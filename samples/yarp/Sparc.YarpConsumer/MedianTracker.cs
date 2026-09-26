using System.Globalization;

namespace Sparc.YarpConsumer;

/// <summary>Median/p95 summary over the sampled header values of one run.</summary>
internal readonly record struct MedianReport(int Samples, double Median, double P95, double Max);

/// <summary>
/// Collects the numeric header values seen by the consumer and computes the
/// median (plus p95/max) on demand. The sample list is capped so memory stays
/// bounded even on a long run.
/// </summary>
internal sealed class MedianTracker(int maxSamples)
{
    private readonly List<double> _values = new();

    public long Received { get; private set; }

    public long MissingHeader { get; private set; }

    public long InvalidValue { get; private set; }

    public long Undecodable { get; private set; }

    public void CountMessage() => Received++;

    public void CountMissingHeader() => MissingHeader++;

    public void CountInvalidValue() => InvalidValue++;

    public void CountUndecodable() => Undecodable++;

    public void Add(double value)
    {
        if (_values.Count < maxSamples)
        {
            _values.Add(value);
        }
    }

    public MedianReport Compute()
    {
        double[] sorted = _values.ToArray();
        Array.Sort(sorted);
        return new MedianReport(
            sorted.Length,
            Percentile(sorted, 0.50),
            Percentile(sorted, 0.95),
            sorted.Length == 0 ? 0 : sorted[^1]);
    }

    private static double Percentile(double[] sorted, double percentile)
    {
        if (sorted.Length == 0)
        {
            return 0;
        }

        int index = (int)Math.Ceiling(percentile * sorted.Length) - 1;
        return sorted[Math.Clamp(index, 0, sorted.Length - 1)];
    }
}
