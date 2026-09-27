using System.Runtime.InteropServices;
using Sparc.Client.Diagnostics;

namespace Sparc.Client;

/// <summary>Options for <see cref="ConsumerSession"/>.</summary>
public sealed class ConsumerSessionOptions
{
    /// <summary>Stop after this many messages; 0 means "until the producer stops".</summary>
    public long Count { get; set; }

    /// <summary>Expected type tag.</summary>
    public int ExpectedType { get; set; } = 1;

    /// <summary>
    /// When true (default), the payload carries the session's
    /// <c>[sequence:int64][timestamp:int64][fill...]</c> protocol, so sequence,
    /// payload fill and one-way latency are verified/sampled. Set to false when
    /// the producer owns the whole payload
    /// (<c>ProducerSessionOptions.IncludeSessionHeader = false</c>); sequence,
    /// fill and latency checks are then skipped and the type tag is still checked.
    /// </summary>
    public bool IncludeSessionHeader { get; set; } = true;

    /// <summary>Validate sequence numbers and the message type.</summary>
    public bool Verify { get; set; } = true;

    /// <summary>
    /// Scan every payload for the fill pattern. Only has an effect when
    /// <see cref="Verify"/> is true. Disable to isolate the O(payload) scan
    /// from the transfer cost (benchmarks, throughput tuning); sequence and
    /// type checks still run.
    /// </summary>
    public bool VerifyPayload { get; set; } = true;

    /// <summary>Give up when no messages arrive for this long.</summary>
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Optional pause between consumed messages; <see cref="TimeSpan.Zero"/>
    /// disables pacing. Intended for tests/benchmarks that simulate a slow
    /// consumer (backpressure), not for production rate limiting.
    /// </summary>
    public TimeSpan PerMessageDelay { get; set; }

    /// <summary>Claim the consumer role from a crashed peer.</summary>
    public bool Takeover { get; set; }

    /// <summary>
    /// How to wait while the buffer stays empty. <see cref="SessionWaitMode.SpinOnly"/>
    /// removes the millisecond sleep granularity at the cost of a busy core;
    /// <see cref="SessionWaitMode.Notification"/> blocks on an OS signal and
    /// costs no CPU while idle.
    /// </summary>
    public SessionWaitMode WaitMode { get; set; } = SessionWaitMode.SpinThenSleep;

    /// <summary>
    /// Notification latches; required when <see cref="WaitMode"/> is
    /// <see cref="SessionWaitMode.Notification"/>. The producer endpoint must be
    /// given the same pair (same process) or a pair created from the same region
    /// name (<see cref="SessionNotification.CreateNamed"/>). The host owns it.
    /// </summary>
    public SessionNotification? Notification { get; set; }
}

/// <summary>Progress notification emitted every ~10% of the requested count.</summary>
[StructLayout(LayoutKind.Auto)]
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
