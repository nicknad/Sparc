using System.Buffers.Binary;
using CsCheck;
using Sparc.Core;

namespace Sparc.FuzzTests;

public class HeaderFuzzTests
{
    private static readonly Gen<byte[]> HeaderBytes = Gen.Byte.Array[RingBufferLayout.HeaderSize];

    private static readonly Gen<byte[]> InvalidMagicHeaders = HeaderBytes.Select(bytes =>
    {
        BinaryPrimitives.WriteUInt64LittleEndian(
            bytes.AsSpan(RingBufferLayout.MagicOffset), RingBufferLayout.Magic + 1);
        return bytes;
    });

    private static readonly Gen<byte[]> ValidHeaders =
        Gen.Select(
            Gen.Int[0, 20].Select(exponent => 1 << exponent),
            Gen.Int[RingBufferLayout.MessageHeaderSize + 1, 4096])
        .Select(geometry =>
        {
            (int capacity, int slotSize) = geometry;
            byte[] bytes = new byte[RingBufferLayout.HeaderSize];
            RingBufferHeader.CreateNew(capacity, slotSize).WriteTo(bytes);
            return bytes;
        });

    private static readonly Gen<byte[]> MutatedValidHeaders =
        Gen.Select(ValidHeaders, Gen.Int[0, RingBufferLayout.HeaderSize - 1], Gen.Byte)
            .Select(input =>
            {
                (byte[] template, int offset, byte value) = input;
                byte[] bytes = (byte[])template.Clone();
                bytes[offset] = value;
                return bytes;
            });

    [Fact]
    public void MutatedValidHeadersEitherFailValidationOrDescribeUsableGeometry()
    {
        MutatedValidHeaders.Sample(
            bytes =>
            {
                RingBufferHeader header = RingBufferHeader.Read(bytes);
                try
                {
                    header.Validate("fuzz");
                }
                catch (RingBufferException)
                {
                    return;
                }

                Assert.True(RingBufferLayout.IsPowerOfTwo(header.Capacity));
                Assert.Equal(RingBufferLayout.HeaderSize, header.HeaderSize);
                Assert.True(header.SlotSize > RingBufferLayout.MessageHeaderSize);
                Assert.True(header.MaxPayloadSize >= 0);
                Assert.Equal(RingBufferLayout.MaxPayloadSizeFor(header.SlotSize), header.MaxPayloadSize);
                Assert.True(RingBufferLayout.RequiredSize(header.Capacity, header.SlotSize) > 0);
            },
            iter: 10_000);
    }

    [Fact]
    public void ArbitraryBytesFailValidation()
    {
        InvalidMagicHeaders.Sample(
            bytes =>
            {
                RingBufferHeader header = RingBufferHeader.Read(bytes);
                Assert.Throws<RingBufferCorruptedException>(() => header.Validate("fuzz"));
            },
            iter: 10_000);
    }

    [Fact]
    public void SlotSizeAtOrBelowTheMessageHeaderIsRejected()
    {
        byte[] bytes = new byte[RingBufferLayout.HeaderSize];
        RingBufferHeader.CreateNew(4, 16).WriteTo(bytes);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(RingBufferLayout.SlotSizeOffset), 4);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(RingBufferLayout.MaxPayloadSizeOffset), -4);

        RingBufferHeader header = RingBufferHeader.Read(bytes);

        Assert.Throws<RingBufferCorruptedException>(() => header.Validate("fuzz"));
    }
}
