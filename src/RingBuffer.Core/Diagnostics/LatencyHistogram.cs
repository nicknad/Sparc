using System.Numerics;
using System.Text;

namespace RingBuffer.Core.Diagnostics;

/// <summary>
/// Allocation-free log2-bucketed latency histogram. Values are recorded in
/// <see cref="System.Diagnostics.Stopwatch"/> ticks and reported in microseconds.
/// </summary>
/// <remarks>
/// Bucket <c>i</c> covers <c>[2^(i-1), 2^i)</c> ticks, so percentiles are
/// approximate by at most a factor of two — plenty for p50/p95/p99 tracking
/// without storing every sample.
/// </remarks>
public sealed class LatencyHistogram
{
    private const int BucketCount = 64;

    private readonly long[] _buckets = new long[BucketCount];
    private long _total;
    private long _sum;
    private long _min = long.MaxValue;
    private long _max;

    public long Count => _total;

    public long MinTicks => _total == 0 ? 0 : _min;

    public long MaxTicks => _max;

    public double MeanTicks => _total == 0 ? 0 : (double)_sum / _total;

    public void Reset()
    {
        Array.Clear(_buckets);
        _total = 0;
        _sum = 0;
        _min = long.MaxValue;
        _max = 0;
    }

    public void Record(long ticks)
    {
        if (ticks < 0)
        {
            ticks = 0;
        }

        int bucket = ticks == 0
            ? 0
            : (int)Math.Min(BitOperations.Log2((ulong)ticks) + 1, BucketCount - 1);

        _buckets[bucket]++;
        _total++;
        _sum += ticks;
        if (ticks < _min)
        {
            _min = ticks;
        }

        if (ticks > _max)
        {
            _max = ticks;
        }
    }

    /// <summary>Approximate percentile in ticks; <paramref name="percentile"/> is 0..1.</summary>
    public double PercentileTicks(double percentile)
    {
        if (_total == 0)
        {
            return 0;
        }

        long target = (long)Math.Ceiling(percentile * _total);
        long cumulative = 0;
        for (int i = 0; i < BucketCount; i++)
        {
            cumulative += _buckets[i];
            if (cumulative >= target)
            {
                return i == 0 ? 0 : 1L << (i - 1);
            }
        }

        return _max;
    }

    /// <summary>Formats a one-line report, converting ticks to microseconds.</summary>
    public string ToMicrosecondsReport(double stopwatchFrequency)
    {
        if (_total == 0)
        {
            return "latency: no samples";
        }

        double toMicros = 1_000_000.0 / stopwatchFrequency;
        StringBuilder sb = new(160);
        sb.Append("latency(us): ");
        sb.Append("min=").Append(Format(MinTicks * toMicros));
        sb.Append(" mean=").Append(Format(MeanTicks * toMicros));
        sb.Append(" p50=").Append(Format(PercentileTicks(0.50) * toMicros));
        sb.Append(" p95=").Append(Format(PercentileTicks(0.95) * toMicros));
        sb.Append(" p99=").Append(Format(PercentileTicks(0.99) * toMicros));
        sb.Append(" max=").Append(Format(MaxTicks * toMicros));
        sb.Append(" (n=").Append(_total).Append(')');
        return sb.ToString();

        static string Format(double value) => value.ToString("F2", System.Globalization.CultureInfo.InvariantCulture);
    }
}
