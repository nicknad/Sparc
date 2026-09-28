using System.Buffers.Binary;
using System.Runtime.InteropServices;
using SerializerFoundation;
using Sparc.Channels;
using Sparc.InMemory;
using Sparc.Serialization;

namespace Sparc.UnitTests;

public class SerializerFoundationCodecTests
{
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct Point(int X, int Y);

    [Fact]
    public void EncodeDecodeRoundTrip()
    {
        SfCodec<Point> codec = CreatePointCodec();
        byte[] destination = new byte[codec.MaxSize];

        int written = codec.Encode(new Point(7, -3), destination);

        Assert.Equal(8, written);
        Assert.Equal(new Point(7, -3), codec.Decode(destination.AsSpan(0, written)));
    }

    [Fact]
    public void RoundTripsThroughTypedChannel()
    {
        using SparcChannel<Point> channel = SparcChannel<Point>.CreateInProcess(
            new InMemoryMemoryRegionFactory(),
            "sf-codec-roundtrip",
            CreatePointCodec(),
            capacity: 8,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(channel.Writer.TryWrite(new Point(11, 22)));
        Assert.True(channel.Reader.TryRead(out Point point));
        Assert.Equal(new Point(11, 22), point);
    }

    [Fact]
    public void DirectEncodeDecodeAllocatesNothing()
    {
        SfCodec<Point> codec = CreatePointCodec();
        byte[] destination = new byte[codec.MaxSize];
        for (int i = 0; i < 100; i++)
        {
            int length = codec.Encode(new Point(i, i), destination);
            _ = codec.Decode(destination.AsSpan(0, length));
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 10_000; i++)
        {
            int length = codec.Encode(new Point(i, -i), destination);
            Point point = codec.Decode(destination.AsSpan(0, length));
            if (point.X != i || point.Y != -i)
            {
                throw new InvalidOperationException("Round-trip mismatch.");
            }
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(allocated < 64, $"Expected near-zero allocations, saw {allocated} bytes.");
    }

    [Fact]
    public void ChannelRoundTripAllocatesNothing()
    {
        using SparcChannel<Point> channel = SparcChannel<Point>.CreateInProcess(
            new InMemoryMemoryRegionFactory(),
            "sf-codec-noalloc",
            CreatePointCodec(),
            capacity: 8,
            cancellationToken: TestContext.Current.CancellationToken);

        for (int i = 0; i < 100; i++)
        {
            channel.Writer.TryWrite(new Point(i, i));
            channel.Reader.TryRead(out _);
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++)
        {
            if (!channel.Writer.TryWrite(new Point(i, -i)))
            {
                throw new InvalidOperationException("Ring reported full.");
            }

            if (!channel.Reader.TryRead(out Point point) || point.X != i || point.Y != -i)
            {
                throw new InvalidOperationException("Round-trip mismatch.");
            }
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(allocated < 64, $"Expected near-zero allocations, saw {allocated} bytes.");
    }

    [Fact]
    public void OversizedMessageThrows()
    {
        SfCodec<Point> codec = CreatePointCodec(maxSize: 4);

        Assert.Throws<InvalidOperationException>(() => codec.Encode(new Point(1, 2), new byte[4]));
    }

    [Fact]
    public void OversizedMessageThrowsEvenWithLargerDestination()
    {
        SfCodec<Point> codec = CreatePointCodec(maxSize: 4);

        Assert.Throws<InvalidOperationException>(() => codec.Encode(new Point(1, 2), new byte[64]));
    }

    [Fact]
    public void ConstructorValidatesArguments()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SfCodec<Point>(0, SerializePoint, DeserializePoint));
        Assert.Throws<ArgumentNullException>(() => new SfCodec<Point>(8, null!, DeserializePoint));
        Assert.Throws<ArgumentNullException>(() => new SfCodec<Point>(8, SerializePoint, null!));
    }

    private static SfCodec<Point> CreatePointCodec(int maxSize = 16) =>
        new(maxSize, SerializePoint, DeserializePoint);

    private static void SerializePoint(ref CompatibleSpanWriteBuffer buffer, Point value)
    {
        WriteInt32(ref buffer, value.X);
        WriteInt32(ref buffer, value.Y);
    }

    private static Point DeserializePoint(ref CompatibleReadOnlySpanReadBuffer buffer) =>
        new(ReadInt32(ref buffer), ReadInt32(ref buffer));

    private static void WriteInt32(ref CompatibleSpanWriteBuffer buffer, int value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buffer.GetSpan(sizeof(int)), value);
        buffer.Advance(sizeof(int));
    }

    private static int ReadInt32(ref CompatibleReadOnlySpanReadBuffer buffer)
    {
        ReadOnlySpan<byte> span = buffer.GetUnreadSpan();
        if (span.Length < sizeof(int) && !buffer.TryGetSpan(sizeof(int), out span))
        {
            throw new EndOfStreamException();
        }

        int value = BinaryPrimitives.ReadInt32LittleEndian(span);
        buffer.Advance(sizeof(int));
        return value;
    }
}
