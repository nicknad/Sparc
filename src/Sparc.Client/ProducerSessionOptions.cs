using System.Runtime.InteropServices;
using Sparc.Core;

namespace Sparc.Client;

/// <summary>Options for <see cref="ProducerSession"/>.</summary>
public sealed class ProducerSessionOptions
{
    /// <summary>Messages to write; 0 means "until cancelled or the peer stops".</summary>
    public long Count { get; set; } = 1_000_000;

    /// <summary>Payload bytes per message, including the 16-byte protocol header.</summary>
    public int PayloadSize { get; set; } = 64;

    /// <summary>Type tag stored in each slot.</summary>
    public int MessageType { get; set; } = 1;

    /// <summary>
    /// When true (default), the session stamps every payload with its
    /// <c>[sequence:int64][timestamp:int64][fill...]</c> protocol, which is what
    /// the consumer verifies and samples latency from. The sequence is the
    /// slot's stream position (<see cref="IEndpoint.TailSequence"/>), so
    /// verification survives producer and consumer restarts and takeovers. Set
    /// to false to own the whole payload through <see cref="PayloadWriter"/>;
    /// the consumer must then also be configured with
    /// <c>IncludeSessionHeader = false</c>.
    /// </summary>
    public bool IncludeSessionHeader { get; set; } = true;

    /// <summary>
    /// Optional payload writer, called once per message with the full payload
    /// span. When <see cref="IncludeSessionHeader"/> is true the header is
    /// already written and the writer fills the remainder; when false the writer
    /// owns the whole span (and the payload is zero-filled when no writer is set).
    /// </summary>
    public SessionPayloadWriter? PayloadWriter { get; set; }

    /// <summary>Abort when the buffer stays full this long (the consumer may be gone).</summary>
    public TimeSpan FullTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Optional pause between published messages; <see cref="TimeSpan.Zero"/>
    /// disables pacing. Intended for tests/benchmarks that simulate a slow
    /// producer; production pacing belongs in the caller's own scheduling.
    /// </summary>
    public TimeSpan PerMessageDelay { get; set; }

    /// <summary>Claim the producer role from a crashed peer.</summary>
    public bool Takeover { get; set; }

    /// <summary>
    /// How to wait while the buffer stays full. <see cref="SessionWaitMode.SpinOnly"/>
    /// removes the millisecond sleep granularity at the cost of a busy core.
    /// </summary>
    public SessionWaitMode WaitMode { get; set; } = SessionWaitMode.SpinThenSleep;

    /// <summary>
    /// Notification latches; required when <see cref="WaitMode"/> is
    /// <see cref="SessionWaitMode.Notification"/>. The consumer endpoint must be
    /// given the same pair (same process) or a pair created from the same region
    /// name (<see cref="SessionNotification.CreateNamed"/>). The host owns it.
    /// </summary>
    public SessionNotification? Notification { get; set; }

    internal static void Validate(long count, int payloadSize, TimeSpan perMessageDelay, int maxPayloadSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 0);
        ArgumentOutOfRangeException.ThrowIfLessThan(payloadSize, RingBufferMessage.HeaderSize);
        ArgumentOutOfRangeException.ThrowIfLessThan(perMessageDelay, TimeSpan.Zero);
        if (payloadSize > maxPayloadSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(payloadSize), payloadSize,
                $"Payload size exceeds the buffer maximum of {maxPayloadSize} bytes.");
        }
    }
}

/// <summary>
/// Writes the payload of one session message. The session owns the slot and
/// publishes it after the delegate returns; the span is only valid for the
/// duration of the call.
/// </summary>
/// <param name="index">Zero-based message index within the run.</param>
/// <param name="payload">Full payload span for this message.</param>
public delegate void SessionPayloadWriter(long index, Span<byte> payload);

/// <summary>Progress notification emitted every ~10% of the requested count.</summary>
[StructLayout(LayoutKind.Auto)]
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
