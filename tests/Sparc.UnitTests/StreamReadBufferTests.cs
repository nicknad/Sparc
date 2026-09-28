using Sparc;
using Sparc.Core;
using Sparc.InMemory;
using Sparc.Serialization;

namespace Sparc.UnitTests;

public class StreamReadBufferTests
{
    private const int Capacity = 8;
    private const int SlotSize = 64;

    [Fact]
    public async Task StreamsAllBytesAcrossChunks()
    {
        using StreamPair pair = new();
        byte[] message = NewMessage(1000);
        Task producer = Task.Run(
            () => WriteMessage(pair.Writer, type: 7, message),
            TestContext.Current.CancellationToken);

        byte[] received = new byte[message.Length];
        byte[] scratch = new byte[64];
        SparcStreamReadBuffer buffer = pair.Reader.BeginMessage(
            scratch, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            int offset = 0;
            while (!buffer.IsMessageComplete)
            {
                ReadOnlySpan<byte> span = buffer.GetUnreadSpan();
                span.CopyTo(received.AsSpan(offset));
                buffer.Advance(span.Length);
                offset += span.Length;
            }

            Assert.Equal(7, buffer.Type);
            Assert.Equal(message.Length, offset);
            Assert.Equal(message.Length, buffer.BytesConsumed);
        }
        finally
        {
            buffer.Dispose();
        }

        await producer;
        Assert.True(message.AsSpan().SequenceEqual(received));
    }

    [Fact]
    public void TryGetSpanStitchesAcrossChunkSeams()
    {
        using StreamPair pair = new();
        byte[] message = NewMessage(200);
        WriteMessage(pair.Writer, type: 2, message);

        byte[] scratch = new byte[256];
        SparcStreamReadBuffer buffer = pair.Reader.BeginMessage(
            scratch, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            Assert.True(buffer.TryGetSpan(message.Length, out ReadOnlySpan<byte> span));
            Assert.True(span.Length >= message.Length);
            Assert.True(message.AsSpan().SequenceEqual(span[..message.Length]));
            Assert.Equal(0, buffer.BytesConsumed);

            buffer.Advance(message.Length);
            while (!buffer.IsMessageComplete)
            {
                ReadOnlySpan<byte> raw = buffer.GetUnreadSpan();
                buffer.Advance(raw.Length);
            }

            Assert.Equal(message.Length, buffer.BytesConsumed);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Fact]
    public void CopyToDoesNotConsume()
    {
        using StreamPair pair = new();
        byte[] message = NewMessage(120);
        WriteMessage(pair.Writer, type: 3, message);

        byte[] scratch = new byte[128];
        SparcStreamReadBuffer buffer = pair.Reader.BeginMessage(
            scratch, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            byte[] destination = new byte[message.Length];
            buffer.CopyTo(destination);

            Assert.Equal(0, buffer.BytesConsumed);
            Assert.True(message.AsSpan().SequenceEqual(destination));

            buffer.Advance(message.Length);
            Assert.True(buffer.IsMessageComplete);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Fact]
    public void CopyToLongerThanTheMessageThrowsWithoutConsuming()
    {
        using StreamPair pair = new();
        WriteMessage(pair.Writer, type: 0, NewMessage(20));

        byte[] scratch = new byte[64];
        SparcStreamReadBuffer buffer = pair.Reader.BeginMessage(
            scratch, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            bool threw = false;
            try
            {
                buffer.CopyTo(new byte[40]);
            }
            catch (InvalidOperationException)
            {
                threw = true;
            }

            Assert.True(threw);
            Assert.Equal(0, buffer.BytesConsumed);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Fact]
    public void TryGetSpanBeyondScratchThrows()
    {
        using StreamPair pair = new();
        WriteMessage(pair.Writer, type: 0, NewMessage(100));

        byte[] scratch = new byte[16];
        SparcStreamReadBuffer buffer = pair.Reader.BeginMessage(
            scratch, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            bool threw = false;
            try
            {
                buffer.TryGetSpan(40, out _);
            }
            catch (InvalidOperationException)
            {
                threw = true;
            }

            Assert.True(threw);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Fact]
    public void TryGetSpanReturnsFalseAtTheMessageEnd()
    {
        using StreamPair pair = new();
        byte[] message = NewMessage(20);
        WriteMessage(pair.Writer, type: 0, message);

        byte[] scratch = new byte[64];
        SparcStreamReadBuffer buffer = pair.Reader.BeginMessage(
            scratch, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            Assert.False(buffer.TryGetSpan(message.Length + 1, out _));
            Assert.True(buffer.TryGetSpan(message.Length, out ReadOnlySpan<byte> span));
            Assert.True(message.AsSpan().SequenceEqual(span[..message.Length]));
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Fact]
    public void BeginMessageSkipsTheInterruptedTail()
    {
        using StreamPair pair = new();
        byte[] interrupted = NewMessage(200);
        byte[] next = NewMessage(30);
        WriteMessage(pair.Writer, type: 1, interrupted);
        WriteMessage(pair.Writer, type: 2, next);

        Assert.True(pair.Reader.Endpoint.TryBeginRead(out ReadLease lease));
        lease.Dispose();

        byte[] scratch = new byte[64];
        SparcStreamReadBuffer buffer = pair.Reader.BeginMessage(
            scratch, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            Assert.Equal(2, buffer.Type);
            byte[] received = new byte[next.Length];
            int offset = 0;
            while (!buffer.IsMessageComplete)
            {
                ReadOnlySpan<byte> span = buffer.GetUnreadSpan();
                span.CopyTo(received.AsSpan(offset));
                buffer.Advance(span.Length);
                offset += span.Length;
            }

            Assert.Equal(next.Length, offset);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Fact]
    public void EmptyMessageIsImmediatelyComplete()
    {
        using StreamPair pair = new();
        pair.Writer.BeginMessage(9).Dispose();

        byte[] scratch = new byte[16];
        SparcStreamReadBuffer buffer = pair.Reader.BeginMessage(
            scratch, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            Assert.True(buffer.IsMessageComplete);
            Assert.Equal(9, buffer.Type);
            Assert.True(buffer.GetUnreadSpan().IsEmpty);
            Assert.Equal(0, buffer.BytesConsumed);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Fact]
    public void TryBeginMessageOnAnEmptyRingReturnsFalse()
    {
        using StreamPair pair = new();

        Assert.False(pair.Reader.TryBeginMessage(
            new byte[16], out _, cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public void ProducerRestartMidMessageIsReportedAsCorruption()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = "spsc-stream-" + Guid.NewGuid().ToString("N");
        using IProducerEndpoint producer = SparcRing.OpenProducer(
            factory, name, Capacity, SlotSize, cancellationToken: TestContext.Current.CancellationToken);
        using SparcStreamReader reader = new(SparcRing.OpenConsumer(
            factory, name, Capacity, SlotSize, cancellationToken: TestContext.Current.CancellationToken));

        PublishChunk(producer, type: 1, first: true, last: false, payload: [0xAA]);
        PublishChunk(producer, type: 2, first: true, last: true, payload: [0xBB]);

        byte[] scratch = new byte[16];
        SparcStreamReadBuffer buffer = reader.BeginMessage(
            scratch, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            ReadOnlySpan<byte> first = buffer.GetUnreadSpan();
            Assert.Equal(0xAA, first[0]);
            buffer.Advance(first.Length);

            bool threw = false;
            try
            {
                buffer.GetUnreadSpan();
            }
            catch (RingBufferCorruptedException)
            {
                threw = true;
            }

            Assert.True(threw);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Fact]
    public void ContiguousWindowsNeedNoScratch()
    {
        using StreamPair pair = new();
        byte[] message = NewMessage(30);
        WriteMessage(pair.Writer, type: 4, message);

        SparcStreamReadBuffer buffer = pair.Reader.BeginMessage(
            default, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            byte[] destination = new byte[message.Length];
            buffer.CopyTo(destination);
            Assert.Equal(0, buffer.BytesConsumed);
            Assert.True(message.AsSpan().SequenceEqual(destination));

            Assert.True(buffer.TryGetSpan(message.Length, out ReadOnlySpan<byte> span));
            Assert.True(message.AsSpan().SequenceEqual(span[..message.Length]));

            buffer.Advance(message.Length);
            Assert.True(buffer.IsMessageComplete);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Fact]
    public void EndOfMessageIsReportedBeforeScratchLimits()
    {
        using StreamPair pair = new();
        WriteMessage(pair.Writer, type: 0, NewMessage(20));

        byte[] scratch = new byte[4];
        SparcStreamReadBuffer buffer = pair.Reader.BeginMessage(
            scratch, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            ReadOnlySpan<byte> span = buffer.GetUnreadSpan();
            buffer.Advance(span.Length);
            Assert.True(buffer.IsMessageComplete);

            Assert.False(buffer.TryGetSpan(64, out _));

            bool threw = false;
            string? message = null;
            try
            {
                buffer.CopyTo(new byte[64]);
            }
            catch (InvalidOperationException exception)
            {
                threw = true;
                message = exception.Message;
            }

            Assert.True(threw);
            Assert.Contains("fewer than", message);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Fact]
    public void EmptyNonLastChunkIsRejected()
    {
        using StreamPair pair = new();
        PublishChunk(pair.Writer.Endpoint, type: 1, first: true, last: false, payload: []);

        byte[] scratch = new byte[16];
        bool threw = false;
        try
        {
            pair.Reader.BeginMessage(scratch, cancellationToken: TestContext.Current.CancellationToken);
        }
        catch (RingBufferCorruptedException)
        {
            threw = true;
        }

        Assert.True(threw);
    }

    [Fact]
    public void ReadBufferAllocatesNothing()
    {
        using StreamPair pair = new();
        byte[] message = NewMessage(200);
        byte[] scratch = new byte[256];
        byte[] received = new byte[message.Length];
        for (int i = 0; i < 20; i++)
        {
            WriteMessage(pair.Writer, type: i, message);
            Drain(pair.Reader, scratch, received);
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 200; i++)
        {
            WriteMessage(pair.Writer, type: i, message);
            Drain(pair.Reader, scratch, received);
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(allocated < 64, $"Expected near-zero allocations, saw {allocated} bytes.");
    }

    private static void Drain(SparcStreamReader reader, byte[] scratch, byte[] received)
    {
        SparcStreamReadBuffer buffer = reader.BeginMessage(
            scratch, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            int offset = 0;
            while (!buffer.IsMessageComplete)
            {
                ReadOnlySpan<byte> span = buffer.GetUnreadSpan();
                span.CopyTo(received.AsSpan(offset));
                buffer.Advance(span.Length);
                offset += span.Length;
            }

            if (offset != received.Length)
            {
                throw new InvalidOperationException("Round-trip mismatch.");
            }
        }
        finally
        {
            buffer.Dispose();
        }
    }

    private static void WriteMessage(SparcStreamWriter writer, int type, byte[] message)
    {
        SparcStreamWriteBuffer buffer = writer.BeginMessage(type);
        int offset = 0;
        while (offset < message.Length)
        {
            int length = Math.Min(32, message.Length - offset);
            Span<byte> span = buffer.GetSpan(length);
            message.AsSpan(offset, length).CopyTo(span);
            buffer.Advance(length);
            offset += length;
        }

        buffer.Dispose();
    }

    private static void PublishChunk(IProducerEndpoint producer, int type, bool first, bool last, byte[] payload)
    {
        Assert.True(producer.TryReserveWrite(type, out Span<byte> chunk));
        unsafe
        {
            fixed (byte* pointer = chunk)
            {
                ChunkFraming.WriteFlags(pointer, first, last);
            }
        }

        payload.CopyTo(chunk[ChunkFraming.HeaderSize..]);
        producer.CommitWrite(ChunkFraming.HeaderSize + payload.Length);
    }

    private static byte[] NewMessage(int length)
    {
        byte[] message = new byte[length];
        Random.Shared.NextBytes(message);
        return message;
    }

    private sealed class StreamPair : IDisposable
    {
        public StreamPair()
        {
            InMemoryMemoryRegionFactory factory = new();
            string name = "spsc-stream-" + Guid.NewGuid().ToString("N");
            Writer = SparcStreamWriter.Open(
                factory, name, Capacity, SlotSize, cancellationToken: TestContext.Current.CancellationToken);
            Reader = SparcStreamReader.Open(
                factory, name, Capacity, SlotSize, cancellationToken: TestContext.Current.CancellationToken);
        }

        public SparcStreamWriter Writer { get; }

        public SparcStreamReader Reader { get; }

        public void Dispose()
        {
            Writer.Dispose();
            Reader.Dispose();
        }
    }
}
