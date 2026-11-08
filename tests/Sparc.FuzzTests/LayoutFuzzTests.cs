using CsCheck;
using Sparc.Core;

namespace Sparc.FuzzTests;

public class LayoutFuzzTests
{
    private static readonly Gen<(int Capacity, int SlotSize)> ValidGeometry =
        Gen.Select(
            Gen.Int[0, 20].Select(exponent => 1 << exponent),
            Gen.Int[RingBufferLayout.MessageHeaderSize + 1, 4096]);

    private static readonly Gen<(int Capacity, int SlotSize)> InvalidCapacity =
        Gen.Select(
            Gen.Int[1, 20].Select(exponent => (1 << exponent) + 1),
            Gen.Int[RingBufferLayout.MessageHeaderSize + 1, 4096]);

    private static readonly Gen<(int Capacity, int SlotSize)> InvalidSlotSize =
        Gen.Select(
            Gen.Int[0, 20].Select(exponent => 1 << exponent),
            Gen.Int[-1_000_000, RingBufferLayout.MessageHeaderSize]);

    [Fact]
    public void ValidGeometryIsUsable()
    {
        ValidGeometry.Sample(
            pair =>
            {
                (int capacity, int slotSize) = pair;
                RingBufferLayout.ValidateGeometry(capacity, slotSize);

                Assert.True(RingBufferLayout.IsPowerOfTwo(capacity));
                Assert.True(slotSize > RingBufferLayout.MessageHeaderSize);
                Assert.Equal(
                    RingBufferLayout.HeaderSize + (long)capacity * slotSize,
                    RingBufferLayout.RequiredSize(capacity, slotSize));
            },
            iter: 10_000);
    }

    [Fact]
    public void NonPowerOfTwoCapacityIsRejected()
    {
        InvalidCapacity.Sample(
            pair =>
            {
                (int capacity, int slotSize) = pair;
                Assert.Throws<ArgumentOutOfRangeException>(
                    () => RingBufferLayout.ValidateGeometry(capacity, slotSize));
            },
            iter: 10_000);
    }

    [Fact]
    public void SlotSizeAtOrBelowTheMessageHeaderIsRejected()
    {
        InvalidSlotSize.Sample(
            pair =>
            {
                (int capacity, int slotSize) = pair;
                Assert.Throws<ArgumentOutOfRangeException>(
                    () => RingBufferLayout.ValidateGeometry(capacity, slotSize));
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
