using Sparc;
using Sparc.Core;
using Sparc.InMemory;
using Sparc.Serialization;

namespace Sparc.UnitTests;

public class StreamMessageTests
{
    private const int Capacity = 8;
    private const int SlotSize = 64;
    private const int ChunkDataCapacity = SlotSize - RingBufferLayout.MessageHeaderSize - 4;

    [Fact]
    public void SingleChunkMessageRoundTrips()
    {
        using StreamPair pair = new();

        byte[] message = NewMessage(20);
        WriteMessage(pair.Writer, type: 5, message);

        byte[] destination = new byte[message.Length];
        int length = pair.Reader.ReadMessage(destination, out int type, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(message.Length, length);
        Assert.Equal(5, type);
        Assert.True(message.AsSpan().SequenceEqual(destination));
    }

    [Fact]
    public void EmptyMessageRoundTrips()
    {
        using StreamPair pair = new();

        pair.Writer.BeginMessage(9).Dispose();

        int length = pair.Reader.ReadMessage(new byte[1], out int type, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(0, length);
        Assert.Equal(9, type);
    }

    [Fact]
    public void ChunkBoundarySizesRoundTrip()
    {
        using StreamPair pair = new();

        byte[] exactChunk = NewMessage(ChunkDataCapacity);
        byte[] exactTwoChunks = NewMessage(ChunkDataCapacity * 2);

        WriteMessage(pair.Writer, type: 1, exactChunk);
        WriteMessage(pair.Writer, type: 2, exactTwoChunks);

        byte[] destination = new byte[exactTwoChunks.Length];
        Assert.Equal(
            exactChunk.Length,
            pair.Reader.ReadMessage(destination, out int firstType, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(1, firstType);
        Assert.True(exactChunk.AsSpan().SequenceEqual(destination.AsSpan(0, exactChunk.Length)));

        Assert.Equal(
            exactTwoChunks.Length,
            pair.Reader.ReadMessage(destination, out int secondType, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(2, secondType);
        Assert.True(exactTwoChunks.AsSpan().SequenceEqual(destination));
    }

    [Fact]
    public async Task MessageLargerThanTheRingStreams()
    {
        using StreamPair pair = new();

        byte[] message = NewMessage(200_000);
        Task producer = Task.Run(
            () => WriteMessage(pair.Writer, type: 5, message),
            TestContext.Current.CancellationToken);

        byte[] destination = new byte[message.Length];
        int length = pair.Reader.ReadMessage(destination, out int type, cancellationToken: TestContext.Current.CancellationToken);
        await producer;

        Assert.Equal(message.Length, length);
        Assert.Equal(5, type);
        Assert.True(message.AsSpan().SequenceEqual(destination));
    }

    [Fact]
    public void FlushPublishesAPartialChunkWithoutEndingTheMessage()
    {
        using StreamPair pair = new();

        SparcStreamWriteBuffer buffer = pair.Writer.BeginMessage(3);
        Write(ref buffer, NewMessage(10));
        buffer.Flush();
        Write(ref buffer, NewMessage(10));
        buffer.Dispose();

        byte[] destination = new byte[20];
        int length = pair.Reader.ReadMessage(destination, out int type, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(20, length);
        Assert.Equal(3, type);
    }

    [Fact]
    public void SpanLargerThanAChunkThrows()
    {
        using StreamPair pair = new();

        SparcStreamWriteBuffer buffer = pair.Writer.BeginMessage(0);

        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.GetSpan(ChunkDataCapacity + 1));

        buffer.Dispose();
    }

    [Fact]
    public void OversizedDestinationAbandonsTheMessage()
    {
        using StreamPair pair = new();

        WriteMessage(pair.Writer, type: 1, NewMessage(200));
        Assert.Throws<InvalidOperationException>(
            () => pair.Reader.ReadMessage(new byte[10], out _, cancellationToken: TestContext.Current.CancellationToken));

        byte[] message = NewMessage(30);
        WriteMessage(pair.Writer, type: 2, message);
        byte[] destination = new byte[message.Length];
        Assert.Equal(
            message.Length,
            pair.Reader.ReadMessage(destination, out int type, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(2, type);
        Assert.True(message.AsSpan().SequenceEqual(destination));
    }

    [Fact]
    public void ReaderSkipsTheInterruptedTailAfterAConsumerRestart()
    {
        using StreamPair pair = new();

        byte[] interrupted = NewMessage(200);
        byte[] next = NewMessage(30);
        WriteMessage(pair.Writer, type: 1, interrupted);
        WriteMessage(pair.Writer, type: 2, next);

        Assert.True(pair.Reader.Endpoint.TryBeginRead(out ReadLease lease));
        lease.Dispose();

        byte[] destination = new byte[next.Length];
        int length = pair.Reader.ReadMessage(destination, out int type, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(next.Length, length);
        Assert.Equal(2, type);
        Assert.True(next.AsSpan().SequenceEqual(destination));
    }

    [Fact]
    public void ProducerRestartMidMessageDeliversTheNextMessage()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = "spsc-stream-" + Guid.NewGuid().ToString("N");
        using IProducerEndpoint producer = SparcRing.OpenProducer(
            factory, name, Capacity, SlotSize, cancellationToken: TestContext.Current.CancellationToken);
        using SparcStreamReader reader = new(SparcRing.OpenConsumer(
            factory, name, Capacity, SlotSize, cancellationToken: TestContext.Current.CancellationToken));

        PublishChunk(producer, type: 1, first: true, last: false, payload: [0xAA]);
        PublishChunk(producer, type: 2, first: true, last: true, payload: [0xBB]);

        byte[] destination = new byte[8];
        int length = reader.ReadMessage(destination, out int type, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(1, length);
        Assert.Equal(2, type);
        Assert.Equal(0xBB, destination[0]);
    }

    [Fact]
    public void EmptyRingTimesOut()
    {
        using StreamPair pair = new();

        Assert.Throws<RingBufferTimeoutException>(
            () => pair.Reader.ReadMessage(
                new byte[8], out _, TimeSpan.FromMilliseconds(10), TestContext.Current.CancellationToken));
    }

    [Fact]
    public void WriterRejectsSlotPayloadsTooSmallForAChunkHeader()
    {
        InMemoryMemoryRegionFactory factory = new();
        using IProducerEndpoint producer = SparcRing.OpenProducer(
            factory, "spsc-stream-" + Guid.NewGuid().ToString("N"), 4, 12,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Throws<InvalidOperationException>(() => new SparcStreamWriter(producer));
    }

    [Fact]
    public void WriteAndReadAllocateNothing()
    {
        using StreamPair pair = new();

        byte[] message = NewMessage(200);
        byte[] destination = new byte[message.Length];
        for (int i = 0; i < 20; i++)
        {
            WriteMessage(pair.Writer, type: i, message);
            pair.Reader.ReadMessage(destination, out _, cancellationToken: TestContext.Current.CancellationToken);
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 100; i++)
        {
            WriteMessage(pair.Writer, type: i, message);
            int length = pair.Reader.ReadMessage(destination, out _, cancellationToken: TestContext.Current.CancellationToken);
            if (length != message.Length)
            {
                throw new InvalidOperationException("Round-trip mismatch.");
            }
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(allocated < 64, $"Expected near-zero allocations, saw {allocated} bytes.");
    }

    private static void WriteMessage(SparcStreamWriter writer, int type, byte[] message)
    {
        SparcStreamWriteBuffer buffer = writer.BeginMessage(type);
        Write(ref buffer, message);
        buffer.Dispose();
    }

    private static void Write(ref SparcStreamWriteBuffer buffer, byte[] message)
    {
        int offset = 0;
        while (offset < message.Length)
        {
            int length = Math.Min(32, message.Length - offset);
            Span<byte> span = buffer.GetSpan(length);
            message.AsSpan(offset, length).CopyTo(span);
            buffer.Advance(length);
            offset += length;
        }
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
