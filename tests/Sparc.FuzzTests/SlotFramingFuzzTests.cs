using System.Buffers.Binary;
using CsCheck;
using Sparc.Core;

namespace Sparc.FuzzTests;

public class SlotFramingFuzzTests
{
    private const int MaxPayloadSize = RingBufferLayout.DefaultSlotSize - RingBufferLayout.MessageHeaderSize;

    private static readonly Gen<byte[]> ValidSlots =
        Gen.Select(Gen.Byte.Array[RingBufferLayout.DefaultSlotSize], Gen.Int[0, MaxPayloadSize])
            .Select(input =>
            {
                (byte[] slot, int length) = input;
                BinaryPrimitives.WriteInt32LittleEndian(slot, length);
                return slot;
            });

    private static readonly Gen<int> InvalidLengths =
        Gen.Int.Select(value => value < 0 ? value : (value % (MaxPayloadSize + 1)) + MaxPayloadSize + 1);

    private static readonly Gen<byte[]> InvalidSlots =
        Gen.Select(Gen.Byte.Array[RingBufferLayout.DefaultSlotSize], InvalidLengths)
            .Select(input =>
            {
                (byte[] slot, int length) = input;
                BinaryPrimitives.WriteInt32LittleEndian(slot, length);
                return slot;
            });

    [Fact]
    public void ValidSlotHeadersDecodeWithinBounds()
    {
        ValidSlots.Sample(
            slot =>
            {
                int length = SlotFraming.ReadHeader(slot, out int type);
                Assert.InRange(length, 0, slot.Length - RingBufferLayout.MessageHeaderSize);
                Assert.Equal(BinaryPrimitives.ReadInt32LittleEndian(slot.AsSpan(sizeof(int))), type);
            },
            iter: 10_000);
    }

    [Fact]
    public void InvalidSlotLengthsAreRejected()
    {
        InvalidSlots.Sample(
            slot => Assert.Throws<RingBufferCorruptedException>(() => SlotFraming.ReadHeader(slot, out _)),
            iter: 10_000);
    }

    [Fact]
    public void WrittenSlotsRoundTrip()
    {
        Gen.Select(Gen.Int, Gen.Byte.Array[0, RingBufferLayout.DefaultSlotSize - RingBufferLayout.MessageHeaderSize])
            .Sample(
                pair =>
                {
                    (int type, byte[] payload) = pair;
                    byte[] slot = new byte[RingBufferLayout.DefaultSlotSize];
                    SlotFraming.Write(slot, type, payload);

                    Assert.Equal(payload.Length, SlotFraming.ReadHeader(slot, out int readType));
                    Assert.Equal(type, readType);

                    Span<byte> destination = new byte[RingBufferLayout.MaxPayloadSizeFor(slot.Length)];
                    SlotFraming.Read(slot, destination, out int bytesRead, out int copiedType);
                    Assert.Equal(payload.Length, bytesRead);
                    Assert.Equal(type, copiedType);
                    Assert.True(payload.AsSpan().SequenceEqual(destination[..bytesRead]));
                },
                iter: 5_000);
    }
}
