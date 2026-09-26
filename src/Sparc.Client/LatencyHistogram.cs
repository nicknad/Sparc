using System.Numerics;
using System.Text;

namespace Sparc.Client.Diagnostics;

/// <summary>
/// Allocation-free latency histogram. Values are recorded in
/// <see cref="System.Diagnostics.Stopwatch"/> ticks and reported in microseconds.
/// </summary>
/// <remarks>
/// Each power-of-two range <c>[2^e, 2^(e+1))</c> is split into
/// <see cref="SubBucketCount"/> equal sub-buckets, so percentiles are
/// approximate by at most <c>1/16</c> of the value (about 6%) instead of the
/// 2× error of pure log2 buckets — enough for p50/p90/p99/p99.9 tracking
/// without storing every sample.
/// </remarks>
public sealed class LatencyHistogram
{
    private const int MagnitudeCount = 64;
    private const int SubBucketCount = 16;

    private readonly long[] _buckets = new long[MagnitudeCount * SubBucketCount];
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

        _buckets[IndexFor(ticks)]++;
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
        for (int i = 0; i < _buckets.Length; i++)
        {
            cumulative += _buckets[i];
            if (cumulative >= target)
            {
                return LowerBoundTicks(i);
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
        StringBuilder sb = new(200);
        sb.Append("latency(us): ");
        sb.Append("min=").Append(Format(MinTicks * toMicros));
        sb.Append(" mean=").Append(Format(MeanTicks * toMicros));
        sb.Append(" p50=").Append(Format(PercentileTicks(0.50) * toMicros));
        sb.Append(" p90=").Append(Format(PercentileTicks(0.90) * toMicros));
        sb.Append(" p95=").Append(Format(PercentileTicks(0.95) * toMicros));
        sb.Append(" p99=").Append(Format(PercentileTicks(0.99) * toMicros));
        sb.Append(" p99.9=").Append(Format(PercentileTicks(0.999) * toMicros));
        sb.Append(" max=").Append(Format(MaxTicks * toMicros));
        sb.Append(" (n=").Append(_total).Append(')');
        return sb.ToString();

        static string Format(double value) => value.ToString("F2", System.Globalization.CultureInfo.InvariantCulture);
    }

    private static int IndexFor(long ticks)
    {
        if (ticks <= 0)
        {
            return 0;
        }

        int magnitude = Math.Min(BitOperations.Log2((ulong)ticks), MagnitudeCount - 2);
        long power = 1L << magnitude;
        int sub = magnitude >= 4
            ? (int)((ticks - power) >> (magnitude - 4))
            : (int)((ticks - power) * SubBucketCount / power);
        if (sub >= SubBucketCount)
        {
            sub = SubBucketCount - 1;
        }

        return magnitude * SubBucketCount + sub;
    }

    private static long LowerBoundTicks(int index)
    {
        if (index == 0)
        {
            return 0;
        }

        int magnitude = index / SubBucketCount;
        int sub = index % SubBucketCount;
        long power = 1L << magnitude;

        // power + power * sub / 16, computed in a way that cannot overflow
        // even for the largest magnitude (2^62 * 15 does not fit in a long).
        return power + power / SubBucketCount * sub + power % SubBucketCount * sub / SubBucketCount;
    }
}
