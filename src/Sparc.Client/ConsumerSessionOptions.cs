using Sparc.Client.Diagnostics;

namespace Sparc.Client;

/// <summary>Options for <see cref="ConsumerSession"/>.</summary>
public sealed class ConsumerSessionOptions
{
    /// <summary>Stop after this many messages; 0 means "until the producer stops".</summary>
    public long Count { get; init; }

    /// <summary>Expected type tag.</summary>
    public int ExpectedType { get; init; } = 1;

    /// <summary>Validate sequence numbers and the message type.</summary>
    public bool Verify { get; init; } = true;

    /// <summary>
    /// Scan every payload for the fill pattern. Only has an effect when
    /// <see cref="Verify"/> is true. Disable to isolate the O(payload) scan
    /// from the transfer cost (benchmarks, throughput tuning); sequence and
    /// type checks still run.
    /// </summary>
    public bool VerifyPayload { get; init; } = true;

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
    string? FailureMessage)
{
    /// <summary>
    /// Wall-clock time from the start of <c>Run</c> to its end, including
    /// waiting for the first message. <see cref="Elapsed"/> is the narrower
    /// first-to-last-message window used for the stream rate.
    /// </summary>
    public TimeSpan RunElapsed { get; init; }
}
