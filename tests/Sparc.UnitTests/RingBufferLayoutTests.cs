using System.Text;
using Sparc.Core;

namespace Sparc.UnitTests;

public class RingBufferLayoutTests
{
    [Fact]
    public void MagicEncodesAsciiName()
    {
        Span<byte> bytes = stackalloc byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(bytes, RingBufferLayout.Magic);

        Assert.Equal("SPSCRING", Encoding.ASCII.GetString(bytes));
    }

    [Fact]
    public void RequiredSizeMatchesLayout()
    {
        Assert.Equal(192, RingBufferLayout.HeaderSize);
        Assert.Equal(192 + 4 * 64, RingBufferLayout.RequiredSize(4, 64));
        Assert.Equal(192 + 1024 * 256L, RingBufferLayout.RequiredSize(1024, 256));
    }

    [Fact]
    public void HeadAndTailLiveOnSeparateCacheLines()
    {
        Assert.Equal(0, RingBufferLayout.HeadOffset % RingBufferLayout.CacheLineSize);
        Assert.Equal(0, RingBufferLayout.TailOffset % RingBufferLayout.CacheLineSize);
        Assert.Equal(0, RingBufferLayout.HeaderSize % RingBufferLayout.CacheLineSize);
        Assert.NotEqual(
            RingBufferLayout.HeadOffset / RingBufferLayout.CacheLineSize,
            RingBufferLayout.TailOffset / RingBufferLayout.CacheLineSize);
        Assert.True(RingBufferLayout.ProducerStateOffset < RingBufferLayout.CacheLineSize);
        Assert.True(RingBufferLayout.ConsumerStateOffset < RingBufferLayout.CacheLineSize);
    }

    [Fact]
    public void GeometryValidationRejectsBadInput()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => RingBufferLayout.ValidateGeometry(3, 64));
        Assert.Throws<ArgumentOutOfRangeException>(() => RingBufferLayout.ValidateGeometry(4, 8));
        RingBufferLayout.ValidateGeometry(4, 9);
    }
}

public class RingBufferHeaderTests
{
    [Fact]
    public void RoundTripsThroughBytes()
    {
        RingBufferHeader original = RingBufferHeader.CreateNew(8, 64);
        original.Head = 123;
        original.Tail = 456;
        original.ProducerState = RingBufferEndpointState.Running;

        byte[] bytes = new byte[RingBufferLayout.HeaderSize];
        original.WriteTo(bytes);
        RingBufferHeader parsed = RingBufferHeader.Read(bytes);

        Assert.Equal(original.Magic, parsed.Magic);
        Assert.Equal(original.Version, parsed.Version);
        Assert.Equal(original.Capacity, parsed.Capacity);
        Assert.Equal(original.SlotSize, parsed.SlotSize);
        Assert.Equal(original.MaxPayloadSize, parsed.MaxPayloadSize);
        Assert.Equal(original.Head, parsed.Head);
        Assert.Equal(original.Tail, parsed.Tail);
        Assert.Equal(RingBufferEndpointState.Running, parsed.ProducerState);
        parsed.Validate("unit");
    }

    [Fact]
    public void WriteToWithoutMagicLeavesMagicAlone()
    {
        byte[] bytes = new byte[RingBufferLayout.HeaderSize];
        RingBufferHeader header = RingBufferHeader.CreateNew(8, 64);
        header.WriteTo(bytes, includeMagic: false);

        Assert.Equal(0UL, RingBufferHeader.Read(bytes).Magic);

        header.WriteTo(bytes, includeMagic: true);
        Assert.Equal(RingBufferLayout.Magic, RingBufferHeader.Read(bytes).Magic);
    }

    [Fact]
    public void ValidateRejectsBadMagicAndVersion()
    {
        RingBufferHeader header = RingBufferHeader.CreateNew(8, 64);

        header.Magic = 0;
        Assert.Throws<RingBufferCorruptedException>(() => header.Validate("unit"));

        header = RingBufferHeader.CreateNew(8, 64);
        header.Version = 999;
        Assert.Throws<RingBufferVersionMismatchException>(() => header.Validate("unit"));
    }

    [Fact]
    public void ValidateRejectsInconsistentGeometry()
    {
        RingBufferHeader header = RingBufferHeader.CreateNew(8, 64);
        header.MaxPayloadSize = 12;
        Assert.Throws<RingBufferCorruptedException>(() => header.Validate("unit"));

        header = RingBufferHeader.CreateNew(8, 64);
        header.Capacity = 3;
        Assert.Throws<RingBufferCorruptedException>(() => header.Validate("unit"));
    }

    [Fact]
    public void ValidateRequestedGeometryDetectsMismatch()
    {
        RingBufferHeader header = RingBufferHeader.CreateNew(8, 64);
        header.ValidateRequestedGeometry("unit", 8, 64);
        Assert.Throws<RingBufferGeometryMismatchException>(
            () => header.ValidateRequestedGeometry("unit", 16, 64));
    }
}
