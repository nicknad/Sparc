using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Sparc;
using Sparc.Core;
using Sparc.InMemory;
using Sparc.Serialization;

namespace Sparc.UnitTests;

public class SfStreamCodecTests
{
    private const int Capacity = 16;
    private const int SlotSize = 64;

    [Fact]
    public async Task RoundTripsValuesLargerThanTheRing()
    {
        using Pair pair = new();
        SfStreamCodec<byte[]> codec = CreateByteArrayCodec();
        byte[] value = new byte[25_000];
        Random.Shared.NextBytes(value);

        Task producer = Task.Run(
            () => codec.Write(pair.Writer, value),
            TestContext.Current.CancellationToken);
        byte[] received = codec.Read(
            pair.Reader, new byte[64], cancellationToken: TestContext.Current.CancellationToken);
        await producer;

        Assert.True(value.AsSpan().SequenceEqual(received));
    }

    [Fact]
    public void SerializationFailureBeforeAnyBytesPublishesNothing()
    {
        using Pair pair = new();
        SfStreamCodec<byte[]> failing = new(
            1,
            (ref SparcStreamWriteBuffer buffer, byte[] _) => throw new InvalidOperationException("boom"),
            ReadBytes);

        bool threw = false;
        try
        {
            failing.Write(pair.Writer, []);
        }
        catch (InvalidOperationException)
        {
            threw = true;
        }

        Assert.True(threw);
        Assert.Throws<RingBufferTimeoutException>(
            () => pair.Reader.ReadMessage(
                new byte[16], out _, TimeSpan.FromMilliseconds(10), TestContext.Current.CancellationToken));

        SfStreamCodec<byte[]> good = CreateByteArrayCodec();
        byte[] value = NewMessage(40);
        good.Write(pair.Writer, value);
        byte[] received = good.Read(pair.Reader, new byte[64], cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(value.AsSpan().SequenceEqual(received));
    }

    [Fact]
    public void SerializationFailureAfterBytesAbortsTheMessage()
    {
        using Pair pair = new();
        SfStreamCodec<byte[]> failing = new(1, FailAfterBytes, ReadBytes);

        bool threw = false;
        try
        {
            failing.Write(pair.Writer, []);
        }
        catch (InvalidOperationException)
        {
            threw = true;
        }

        Assert.True(threw);
        Assert.Throws<RingBufferCorruptedException>(
            () => pair.Reader.ReadMessage(
                new byte[512], out _, TimeSpan.FromMilliseconds(10), TestContext.Current.CancellationToken));

        SfStreamCodec<byte[]> good = CreateByteArrayCodec();
        byte[] value = NewMessage(40);
        good.Write(pair.Writer, value);
        byte[] received = good.Read(pair.Reader, new byte[64], cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(value.AsSpan().SequenceEqual(received));
    }

    [Fact]
    public void DeserializerThatLeavesBytesIsRejected()
    {
        using Pair pair = new();
        SfStreamCodec<Point> codec = new(
            2,
            WritePoint,
            (ref SparcStreamReadBuffer buffer) =>
            {
                _ = ReadInt32(ref buffer);
                return new Point(0, 0);
            });

        codec.Write(pair.Writer, new Point(1, 2));

        bool threw = false;
        try
        {
            codec.Read(pair.Reader, new byte[16], cancellationToken: TestContext.Current.CancellationToken);
        }
        catch (RingBufferCorruptedException)
        {
            threw = true;
        }

        Assert.True(threw);
    }

    [Fact]
    public void PointRoundTripAllocatesNothing()
    {
        using Pair pair = new();
        SfStreamCodec<Point> codec = new(3, WritePoint, ReadPoint);
        byte[] scratch = new byte[16];
        for (int i = 0; i < 20; i++)
        {
            codec.Write(pair.Writer, new Point(i, i));
            _ = codec.Read(pair.Reader, scratch, cancellationToken: TestContext.Current.CancellationToken);
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 500; i++)
        {
            codec.Write(pair.Writer, new Point(i, -i));
            Point point = codec.Read(pair.Reader, scratch, cancellationToken: TestContext.Current.CancellationToken);
            if (point.X != i || point.Y != -i)
            {
                throw new InvalidOperationException("Round-trip mismatch.");
            }
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(allocated < 64, $"Expected near-zero allocations, saw {allocated} bytes.");
    }

    [Fact]
    public void AbortFailureDoesNotMaskTheSerializerException()
    {
        FailingAbortProducerEndpoint endpoint = new();
        using SparcStreamWriter writer = new(endpoint);
        SfStreamCodec<byte[]> codec = new(1, FailAfterPublishingAChunk, ReadBytes);

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => codec.Write(writer, []));

        Assert.Equal("serializer boom", exception.Message);
    }

    private static void FailAfterPublishingAChunk(ref SparcStreamWriteBuffer buffer, byte[] value)
    {
        Span<byte> span = buffer.GetSpan(56);
        span.Clear();
        buffer.Advance(56);
        buffer.Flush(); // publish the chunk so Abort needs a new reservation
        throw new InvalidOperationException("serializer boom");
    }

    /// <summary>
    /// Succeeds the first reservation; every later reservation throws, which is
    /// what <c>Abort</c> triggers for an already-published message.
    /// </summary>
    private sealed class FailingAbortProducerEndpoint : IProducerEndpoint
    {
        private readonly byte[] _slot = new byte[64];
        private int _reservations;

        public string Name => "failing-abort";

        public int Capacity => 16;

        public int SlotSize => 64;

        public int MaxPayloadSize => 60;

        public long HeadSequence => 0;

        public long TailSequence => 0;

        public bool IsEmpty => true;

        public int Count => 0;

        public RingBufferEndpointState ProducerState => RingBufferEndpointState.Running;

        public RingBufferEndpointState ConsumerState => RingBufferEndpointState.Running;

        public bool IsPeerWaiting() => false;

        public void SetWaiting(bool waiting)
        {
        }

        public void Abort()
        {
        }

        public void Connect(bool takeover = false, CancellationToken cancellationToken = default)
        {
        }

        public bool TryPublish(int type, ReadOnlySpan<byte> payload) => throw new NotSupportedException();

        public void Publish(int type, ReadOnlySpan<byte> payload, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public bool TryReserveWrite(int type, int length, out Span<byte> payload) =>
            throw new NotSupportedException();

        public bool TryReserveWrite(int type, out Span<byte> payload)
        {
            if (Interlocked.Increment(ref _reservations) > 1)
            {
                throw new AbortReservationException();
            }

            payload = _slot;
            return true;
        }

        public void CommitWrite()
        {
        }

        public void CommitWrite(int length)
        {
        }

        public void AbandonWrite()
        {
        }

        public bool TryBeginWrite(int type, int length, out WriteLease lease) =>
            throw new NotSupportedException();

        public WriteLease BeginWrite(int type, int length, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public void Dispose()
        {
        }
    }

    private sealed class AbortReservationException : Exception;

    private static SfStreamCodec<byte[]> CreateByteArrayCodec() => new(1, WriteBytes, ReadBytes);

    private static void FailAfterBytes(ref SparcStreamWriteBuffer buffer, byte[] value)
    {
        Span<byte> first = buffer.GetSpan(32);
        first[..32].Clear();
        buffer.Advance(32);

        // Force the first chunk to be published so the message is visible.
        Span<byte> second = buffer.GetSpan(32);
        second[..32].Clear();
        buffer.Advance(32);

        throw new InvalidOperationException("boom");
    }

    private static void WriteBytes(ref SparcStreamWriteBuffer buffer, byte[] value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buffer.GetSpan(sizeof(int)), value.Length);
        buffer.Advance(sizeof(int));

        int offset = 0;
        while (offset < value.Length)
        {
            int length = Math.Min(32, value.Length - offset);
            Span<byte> span = buffer.GetSpan(length);
            value.AsSpan(offset, length).CopyTo(span);
            buffer.Advance(length);
            offset += length;
        }
    }

    private static byte[] ReadBytes(ref SparcStreamReadBuffer buffer)
    {
        int length = ReadInt32(ref buffer);
        byte[] value = new byte[length];

        int offset = 0;
        while (offset < length)
        {
            ReadOnlySpan<byte> span = buffer.GetUnreadSpan();
            int take = Math.Min(span.Length, length - offset);
            span[..take].CopyTo(value.AsSpan(offset));
            buffer.Advance(take);
            offset += take;
        }

        return value;
    }

    private static int ReadInt32(ref SparcStreamReadBuffer buffer)
    {
        if (!buffer.TryGetSpan(sizeof(int), out ReadOnlySpan<byte> span))
        {
            throw new EndOfStreamException();
        }

        int value = BinaryPrimitives.ReadInt32LittleEndian(span);
        buffer.Advance(sizeof(int));
        return value;
    }

    private static void WritePoint(ref SparcStreamWriteBuffer buffer, Point value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buffer.GetSpan(sizeof(int)), value.X);
        buffer.Advance(sizeof(int));
        BinaryPrimitives.WriteInt32LittleEndian(buffer.GetSpan(sizeof(int)), value.Y);
        buffer.Advance(sizeof(int));
    }

    private static Point ReadPoint(ref SparcStreamReadBuffer buffer) =>
        new(ReadInt32(ref buffer), ReadInt32(ref buffer));

    private static byte[] NewMessage(int length)
    {
        byte[] message = new byte[length];
        Random.Shared.NextBytes(message);
        return message;
    }

    [StructLayout(LayoutKind.Auto)]
    private readonly record struct Point(int X, int Y);

    private sealed class Pair : IDisposable
    {
        public Pair()
        {
            InMemoryMemoryRegionFactory factory = new();
            string name = "sf-stream-" + Guid.NewGuid().ToString("N");
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
