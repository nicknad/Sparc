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
        double p90 = histogram.PercentileTicks(0.90);
        double p95 = histogram.PercentileTicks(0.95);
        double p99 = histogram.PercentileTicks(0.99);
        double p999 = histogram.PercentileTicks(0.999);

        Assert.True(p50 <= p90);
        Assert.True(p90 <= p95);
        Assert.True(p95 <= p99);
        Assert.True(p99 <= p999);
        Assert.InRange(p50, 45_000, 56_000);
        Assert.InRange(p99, 90_000, 101_000);
        Assert.True(p999 <= histogram.MaxTicks);
    }

    [Fact]
    public void ReportContainsTheDocumentedPercentiles()
    {
        LatencyHistogram histogram = new();
        for (int i = 1; i <= 100; i++)
        {
            histogram.Record(i * 10_000);
        }

        string report = histogram.ToMicrosecondsReport(10_000_000);

        Assert.StartsWith("latency(us): min=", report, StringComparison.Ordinal);
        Assert.Contains(" mean=", report, StringComparison.Ordinal);
        Assert.Contains(" p50=", report, StringComparison.Ordinal);
        Assert.Contains(" p90=", report, StringComparison.Ordinal);
        Assert.Contains(" p95=", report, StringComparison.Ordinal);
        Assert.Contains(" p99=", report, StringComparison.Ordinal);
        Assert.Contains(" p99.9=", report, StringComparison.Ordinal);
        Assert.Contains(" max=", report, StringComparison.Ordinal);
        Assert.EndsWith("(n=100)", report, StringComparison.Ordinal);
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
