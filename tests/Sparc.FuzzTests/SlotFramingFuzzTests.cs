using System.Buffers.Binary;
using CsCheck;
using Sparc.Core;

namespace Sparc.FuzzTests;

public class SlotFramingFuzzTests
{
    private static readonly Gen<byte[]> Slots = Gen.Byte.Array[RingBufferLayout.DefaultSlotSize];

    [Fact]
    public void ArbitrarySlotBytesEitherThrowCorruptionOrDecodeWithinBounds()
    {
        Slots.Sample(
            slot =>
            {
                try
                {
                    int length = SlotFraming.ReadHeader(slot, out int type);
                    Assert.InRange(length, 0, slot.Length - RingBufferLayout.MessageHeaderSize);
                    Assert.Equal(BinaryPrimitives.ReadInt32LittleEndian(slot.AsSpan(sizeof(int))), type);
                }
                catch (RingBufferCorruptedException)
                {
                    return;
                }
            },
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
