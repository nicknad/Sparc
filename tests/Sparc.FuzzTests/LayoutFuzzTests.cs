using CsCheck;
using Sparc.Core;

namespace Sparc.FuzzTests;

public class LayoutFuzzTests
{
    [Fact]
    public void ArbitraryGeometryEitherThrowsOrIsUsable()
    {
        Gen.Select(Gen.Int, Gen.Int).Sample(
            pair =>
            {
                (int capacity, int slotSize) = pair;
                try
                {
                    RingBufferLayout.ValidateGeometry(capacity, slotSize);
                }
                catch (ArgumentOutOfRangeException)
                {
                    return;
                }

                Assert.True(RingBufferLayout.IsPowerOfTwo(capacity));
                Assert.True(slotSize > RingBufferLayout.MessageHeaderSize);
                Assert.Equal(
                    RingBufferLayout.HeaderSize + (long)capacity * slotSize,
                    RingBufferLayout.RequiredSize(capacity, slotSize));
            },
            iter: 10_000);
    }

    [Fact]
    public void RoundedSlotSizeIsTheNextCacheLineMultiple()
    {
        Gen.Int[1, int.MaxValue - RingBufferLayout.CacheLineSize].Sample(
            slotSize =>
            {
                int rounded = RingBufferLayout.RoundSlotSizeToCacheLine(slotSize);
                Assert.True(rounded >= slotSize);
                Assert.Equal(0, rounded % RingBufferLayout.CacheLineSize);
                Assert.True(rounded < slotSize + RingBufferLayout.CacheLineSize);
            },
            iter: 5_000);
    }
}
