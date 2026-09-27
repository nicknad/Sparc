using Sparc.Core;

namespace Sparc.Client;

/// <summary>Options for <see cref="ProducerSession"/>.</summary>
public sealed class ProducerSessionOptions
{
    /// <summary>Messages to write. Must be positive.</summary>
    public long Count { get; init; } = 1_000_000;

    /// <summary>Payload bytes per message, including the 16-byte protocol header.</summary>
    public int PayloadSize { get; init; } = 64;

    /// <summary>Type tag stored in each slot.</summary>
    public int MessageType { get; init; } = 1;

    /// <summary>Abort when the buffer stays full this long (the consumer may be gone).</summary>
    public TimeSpan FullTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Optional pause between published messages; <see cref="TimeSpan.Zero"/>
    /// disables pacing. Intended for tests/benchmarks that simulate a slow
    /// producer; production pacing belongs in the caller's own scheduling.
    /// </summary>
    public TimeSpan PerMessageDelay { get; init; }

    /// <summary>Claim the producer role from a crashed peer.</summary>
    public bool Takeover { get; init; }

    internal void Validate(int maxPayloadSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(Count, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(PayloadSize, RingBufferMessage.HeaderSize);
        ArgumentOutOfRangeException.ThrowIfLessThan(PerMessageDelay, TimeSpan.Zero);
        if (PayloadSize > maxPayloadSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(PayloadSize), PayloadSize,
                $"Payload size exceeds the buffer maximum of {maxPayloadSize} bytes.");
        }
    }
}

/// <summary>Progress notification emitted every ~10% of the requested count.</summary>
public readonly record struct ProducerProgress(long Produced, TimeSpan Elapsed);

/// <summary>Outcome of a producer session.</summary>
public readonly record struct ProducerRunResult(
    long Produced,
    int PayloadSize,
    TimeSpan Elapsed,
    SessionStopReason Reason,
    long Unsent,
    RingBufferEndpointState PeerState,
    string? FailureMessage)
{
    /// <summary>
    /// Time from the first successfully published message to the end of the
    /// run. Excludes waiting for the peer to attach, so this is the window a
    /// send-rate number should use. <see cref="TimeSpan.Zero"/> when nothing
    /// was published.
    /// </summary>
    public TimeSpan ActiveElapsed { get; init; }
}
