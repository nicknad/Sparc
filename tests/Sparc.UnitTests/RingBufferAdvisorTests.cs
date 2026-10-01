using Sparc.Core;
using Sparc.Hosting;

namespace Sparc.UnitTests;

public class RingBufferAdvisorTests
{
    [Theory]
    [InlineData(16, 256)]
    [InlineData(64, 256)]
    [InlineData(248, 256)]
    [InlineData(249, 320)]
    [InlineData(1024, 1088)]
    [InlineData(4096, 4160)]
    public void SlotSizeMatchesCliAndPumpPolicy(int payloadSize, int expectedSlotSize)
    {
        Assert.Equal(expectedSlotSize, RingBufferAdvisor.SlotSizeFor(payloadSize));
    }

    [Fact]
    public void ExplicitSlotSizeOverrideWins()
    {
        Assert.Equal(100, RingBufferAdvisor.SlotSizeFor(64, slotSizeOverride: 100));
    }

    [Fact]
    public void AdviseComputesFootprint()
    {
        RingBufferAdvice advice = RingBufferAdvisor.Advise(64, capacity: 1024);

        Assert.Equal(256, advice.SlotSize);
        Assert.Equal(192L + 1024 * 256, advice.RegionBytes);
        Assert.Equal(advice.RegionBytes / (1024.0 * 1024.0), advice.RegionMiB);
        Assert.Null(advice.AlignmentWarning);
        Assert.StartsWith("fits in L2", advice.Residency, StringComparison.Ordinal);
    }

    [Fact]
    public void AdviseFlagsOversizeFootprint()
    {
        // 16 KB payloads at the default capacity exceed a typical LLC.
        RingBufferAdvice advice = RingBufferAdvisor.Advise(16384, capacity: 1024);

        Assert.Equal(16448, advice.SlotSize);
        Assert.True(advice.RegionBytes > RingBufferAdvisor.OversizeBytes);
        Assert.StartsWith("large", advice.Residency, StringComparison.Ordinal);
        Assert.NotEmpty(RingBufferAdvisor.GetWarnings(advice.Capacity, advice.SlotSize));
    }

    [Fact]
    public void AdviseReportsMessageRateForTarget()
    {
        RingBufferAdvice advice = RingBufferAdvisor.Advise(4096, capacity: 1024, targetGiBs: 5);

        Assert.NotNull(advice.MessagesPerSecondAtTarget);
        Assert.Equal(5.0 * 1024 * 1024 * 1024 / 4096, advice.MessagesPerSecondAtTarget.Value, precision: 3);
    }

    [Fact]
    public void AdviseRejectsPayloadThatDoesNotFit()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => RingBufferAdvisor.Advise(1000, capacity: 1024, slotSizeOverride: 100));
    }

    [Fact]
    public void WarningsAreEmptyForDefaultGeometry()
    {
        Assert.Empty(RingBufferAdvisor.GetWarnings(1024, 256));
    }

    [Fact]
    public void WarningsCatchAlignmentAndBadCapacity()
    {
        Assert.Contains(
            RingBufferAdvisor.GetWarnings(1024, 100),
            warning => warning.Contains("cache line", StringComparison.Ordinal));
        Assert.Contains(
            RingBufferAdvisor.GetWarnings(1000, 256),
            warning => warning.Contains("power of two", StringComparison.Ordinal));
        Assert.Contains(
            RingBufferAdvisor.GetWarnings(1024, 8),
            warning => warning.Contains("header", StringComparison.Ordinal));
    }

    [Fact]
    public void ChannelOptionsSurfaceTheSameWarnings()
    {
        SparcChannelOptions aligned = new() { Name = "orders", Capacity = 1024, SlotSize = 256 };
        Assert.Empty(aligned.GetWarnings());

        SparcChannelOptions skewed = new() { Name = "orders", Capacity = 1024, SlotSize = 100 };
        Assert.NotEmpty(skewed.GetWarnings());
    }
}
