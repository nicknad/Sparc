using System.Buffers.Binary;
using CsCheck;
using Sparc.Core;

namespace Sparc.FuzzTests;

public class HeaderFuzzTests
{
    private static readonly Gen<byte[]> HeaderBytes = Gen.Byte.Array[RingBufferLayout.HeaderSize];

    [Fact]
    public void ArbitraryBytesEitherFailValidationOrDescribeUsableGeometry()
    {
        HeaderBytes.Sample(
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
