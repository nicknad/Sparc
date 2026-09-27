using System.Buffers.Binary;
using Sparc.Core;

namespace Sparc.UnitTests;

public class ProtocolGoldenTests
{
    [Fact]
    public void HeaderFieldOffsetsAreStable()
    {
        Assert.Equal(0, RingBufferLayout.MagicOffset);
        Assert.Equal(8, RingBufferLayout.VersionOffset);
        Assert.Equal(12, RingBufferLayout.CapacityOffset);
        Assert.Equal(16, RingBufferLayout.SlotSizeOffset);
        Assert.Equal(20, RingBufferLayout.HeaderSizeOffset);
        Assert.Equal(24, RingBufferLayout.MaxPayloadSizeOffset);
        Assert.Equal(28, RingBufferLayout.FlagsOffset);
        Assert.Equal(32, RingBufferLayout.ProducerStateOffset);
        Assert.Equal(36, RingBufferLayout.ConsumerStateOffset);
        Assert.Equal(40, RingBufferLayout.ConsumerWaitingOffset);
        Assert.Equal(44, RingBufferLayout.ProducerWaitingOffset);
        Assert.Equal(64, RingBufferLayout.HeadOffset);
        Assert.Equal(128, RingBufferLayout.TailOffset);
        Assert.Equal(192, RingBufferLayout.HeaderSize);
        Assert.Equal(8, RingBufferLayout.MessageHeaderSize);
        Assert.Equal(1, RingBufferLayout.Version);
    }

    [Fact]
    public void HeaderSerializesLittleEndianAtFixedOffsets()
    {
        RingBufferHeader header = RingBufferHeader.CreateNew(8, 64);
        header.Head = 0x0102030405060708;
        header.Tail = 0x1112131415161718;
        header.ProducerState = RingBufferEndpointState.Running;
        header.ConsumerState = RingBufferEndpointState.Stopped;

        byte[] bytes = new byte[RingBufferLayout.HeaderSize];
        header.WriteTo(bytes);

        Assert.Equal(
            RingBufferLayout.Magic,
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(RingBufferLayout.MagicOffset)));
        Assert.Equal(
            RingBufferLayout.Version,
            BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(RingBufferLayout.VersionOffset)));
        Assert.Equal(8, BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(RingBufferLayout.CapacityOffset)));
        Assert.Equal(64, BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(RingBufferLayout.SlotSizeOffset)));
        Assert.Equal(192, BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(RingBufferLayout.HeaderSizeOffset)));
        Assert.Equal(56, BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(RingBufferLayout.MaxPayloadSizeOffset)));
        Assert.Equal(0, BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(RingBufferLayout.FlagsOffset)));
        Assert.Equal(
            (int)RingBufferEndpointState.Running,
            BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(RingBufferLayout.ProducerStateOffset)));
        Assert.Equal(
            (int)RingBufferEndpointState.Stopped,
            BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(RingBufferLayout.ConsumerStateOffset)));
        Assert.Equal(
            0x0102030405060708L,
            BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(RingBufferLayout.HeadOffset)));
        Assert.Equal(
            0x1112131415161718L,
            BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(RingBufferLayout.TailOffset)));
    }

    [Fact]
    public void ReservedAreaBetweenWaitingFlagsAndHeadStaysZero()
    {
        byte[] bytes = new byte[RingBufferLayout.HeaderSize];
        RingBufferHeader.CreateNew(8, 64).WriteTo(bytes);

        for (int offset = 48; offset < RingBufferLayout.HeadOffset; offset++)
        {
            Assert.Equal(0, bytes[offset]);
        }
    }

    [Fact]
    public void SlotFramingIsLengthThenTypeThenPayload()
    {
        byte[] slot = new byte[64];
        byte[] payload = [1, 2, 3, 4, 5];
        SlotFraming.Write(slot, type: 0x11223344, payload);

        Assert.Equal(payload.Length, BinaryPrimitives.ReadInt32LittleEndian(slot));
        Assert.Equal(0x11223344, BinaryPrimitives.ReadInt32LittleEndian(slot.AsSpan(sizeof(int))));
        Assert.Equal(payload, slot.AsSpan(RingBufferLayout.MessageHeaderSize, payload.Length).ToArray());

        Span<byte> destination = stackalloc byte[RingBufferLayout.MaxPayloadSizeFor(slot.Length)];
        SlotFraming.Read(slot, destination, out int bytesRead, out int type);

        Assert.Equal(payload.Length, bytesRead);
        Assert.Equal(0x11223344, type);
        Assert.Equal(payload, destination[..bytesRead].ToArray());
    }

    [Fact]
    public void SlotFramingRejectsLengthBeyondSlot()
    {
        byte[] slot = new byte[64];
        BinaryPrimitives.WriteInt32LittleEndian(slot, RingBufferLayout.MaxPayloadSizeFor(slot.Length) + 1);

        Assert.Throws<RingBufferCorruptedException>(
            () => SlotFraming.Read(slot, new byte[56], out _, out _));
    }
}
