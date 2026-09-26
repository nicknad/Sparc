using Sparc.Client.Diagnostics;

namespace Sparc.UnitTests;

public class LatencyHistogramTests
{
    [Fact]
    public void EmptyHistogramReportsNoSamples()
    {
        LatencyHistogram histogram = new();

        Assert.Equal(0, histogram.Count);
        Assert.Equal(0, histogram.PercentileTicks(0.5));
        Assert.Equal(0, histogram.MinTicks);
        Assert.Contains("no samples", histogram.ToMicrosecondsReport(10_000_000), StringComparison.Ordinal);
    }

    [Fact]
    public void PercentilesAreApproximateButOrdered()
    {
        LatencyHistogram histogram = new();

        for (int i = 1; i <= 1000; i++)
        {
            histogram.Record(i * 100); // 100..100_000 ticks
        }

        Assert.Equal(1000, histogram.Count);
        Assert.Equal(100, histogram.MinTicks);
        Assert.Equal(100_000, histogram.MaxTicks);

        double p50 = histogram.PercentileTicks(0.50);
        double p95 = histogram.PercentileTicks(0.95);
        double p99 = histogram.PercentileTicks(0.99);

        Assert.True(p50 <= p95);
        Assert.True(p95 <= p99);
        Assert.InRange(p50, 32_768, 65_536);
        Assert.InRange(p99, 32_768, 65_536);
    }

    [Fact]
    public void ResetClearsEverything()
    {
        LatencyHistogram histogram = new();
        histogram.Record(1234);
        histogram.Record(5678);
        histogram.Reset();

        Assert.Equal(0, histogram.Count);
        Assert.Equal(0, histogram.MaxTicks);
        Assert.Equal(0, histogram.MinTicks);
    }

    [Fact]
    public void NegativeValuesAreClamped()
    {
        LatencyHistogram histogram = new();
        histogram.Record(-5);
        Assert.Equal(0, histogram.MinTicks);
    }
}
