using Sparc.Client.Diagnostics;

namespace Sparc.Client;

/// <summary>Options for <see cref="ConsumerSession"/>.</summary>
public sealed class ConsumerSessionOptions
{
    /// <summary>Stop after this many messages; 0 means "until the producer stops".</summary>
    public long Count { get; init; }

    /// <summary>Expected type tag.</summary>
    public int ExpectedType { get; init; } = 1;

    /// <summary>Validate sequence numbers, type and payload fill bytes.</summary>
    public bool Verify { get; init; } = true;

    /// <summary>Give up when no messages arrive for this long.</summary>
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Optional pause between consumed messages; <see cref="TimeSpan.Zero"/>
    /// disables pacing. Intended for tests/benchmarks that simulate a slow
    /// consumer (backpressure), not for production rate limiting.
    /// </summary>
    public TimeSpan PerMessageDelay { get; init; }

    /// <summary>Claim the consumer role from a crashed peer.</summary>
    public bool Takeover { get; init; }
}

/// <summary>Progress notification emitted every ~10% of the requested count.</summary>
public readonly record struct ConsumerProgress(long Received, long ReceivedBytes, TimeSpan Elapsed);

/// <summary>Outcome of a consumer session.</summary>
public readonly record struct ConsumerRunResult(
    long Received,
    long ReceivedBytes,
    TimeSpan Elapsed,
    SessionStopReason Reason,
    LatencyHistogram Latency,
    string? FailureMessage);
