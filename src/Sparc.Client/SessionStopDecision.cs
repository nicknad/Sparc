using Sparc.Core;

namespace Sparc.Client;

/// <summary>Why a session run is deciding to stop.</summary>
internal enum SessionTerminalEvent
{
    /// <summary>The requested message count was reached, or the stream drained cleanly.</summary>
    CountReached = 0,

    /// <summary>The caller cancelled the run.</summary>
    Cancelled = 1,

    /// <summary>The peer reported a terminal endpoint state.</summary>
    PeerStopped = 2,

    /// <summary>The buffer stayed full (producer) or empty (consumer) past its timeout.</summary>
    Timeout = 3,

    /// <summary>A received message failed verification.</summary>
    VerificationFailed = 4,
}

/// <summary>Which half of the region a session run drives.</summary>
internal enum SessionRole
{
    /// <summary>Publishes messages.</summary>
    Producer = 0,

    /// <summary>Reads messages.</summary>
    Consumer = 1,
}

/// <summary>
/// The facts a session run holds when a terminal event fires: the event, the
/// role, how far the run got, and the peer's state. Carries no cursor, clock or
/// logger, which is what keeps the decision below pure.
/// </summary>
internal readonly record struct SessionStopInput
{
    /// <summary>The event that ended the run.</summary>
    public SessionTerminalEvent Event { get; init; }

    /// <summary>The role the run drove.</summary>
    public SessionRole Role { get; init; }

    /// <summary>Messages the run moved: produced for a producer, received for a consumer.</summary>
    public long Delivered { get; init; }

    /// <summary>Requested message count; 0 means "until the peer stops or the caller cancels".</summary>
    public long Requested { get; init; }

    /// <summary>The full/empty timeout that was in force for the role.</summary>
    public TimeSpan Timeout { get; init; }

    /// <summary>The peer's advisory endpoint state when the event fired.</summary>
    public RingBufferEndpointState PeerState { get; init; }

    /// <summary>Verification detail; only read for <see cref="SessionTerminalEvent.VerificationFailed"/>.</summary>
    public string? Detail { get; init; }
}

/// <summary>
/// The outcome of a session run: its public stop reason, the human-readable
/// failure text (null when the run ended cleanly) and how many messages the run
/// left outstanding.
/// </summary>
/// <remarks>
/// This is the one place where a terminal event becomes a stop reason and its
/// wording. It is pure — no ring, no clock, no logger — so the whole table is
/// exercised without opening a region or waiting out a timeout.
/// </remarks>
internal readonly record struct SessionStopDecision(
    SessionStopReason Reason,
    string? Message,
    long Outstanding)
{
    /// <summary>Maps a terminal event to an outcome. Same input, same output.</summary>
    public static SessionStopDecision Resolve(in SessionStopInput input)
    {
        long outstanding = input.Requested > 0 && input.Delivered < input.Requested
            ? input.Requested - input.Delivered
            : 0;

        return input.Event switch
        {
            SessionTerminalEvent.CountReached => Create(SessionStopReason.Completed, null, outstanding),
            SessionTerminalEvent.Cancelled => Create(SessionStopReason.Cancelled, null, outstanding),
            SessionTerminalEvent.VerificationFailed => Create(
                SessionStopReason.VerificationFailed, input.Detail, outstanding),
            SessionTerminalEvent.PeerStopped => PeerStopped(input, outstanding),
            SessionTerminalEvent.Timeout => TimedOut(input, outstanding),
            _ => throw new ArgumentOutOfRangeException(nameof(input), input.Event, "Unknown terminal event."),
        };
    }

    private static SessionStopDecision Create(
        SessionStopReason reason, string? message, long outstanding) =>
        new(reason, message, outstanding);

    /// <summary>
    /// A producer treats a vanished consumer as a failure that names the unsent
    /// messages; a consumer that already received the requested count is done,
    /// and one that fell short names how far it got.
    /// </summary>
    private static SessionStopDecision PeerStopped(in SessionStopInput input, long outstanding) =>
        input.Role switch
        {
            SessionRole.Producer when input.Requested > 0 => Create(
                SessionStopReason.PeerStopped,
                $"consumer is gone (state={input.PeerState}) with {outstanding} messages unsent.",
                outstanding),
            SessionRole.Producer => Create(
                SessionStopReason.PeerStopped,
                $"consumer is gone (state={input.PeerState}) after {input.Delivered} messages.",
                outstanding),
            SessionRole.Consumer when input.Requested > 0 && input.Delivered < input.Requested => Create(
                SessionStopReason.PeerStopped,
                $"producer stopped after {input.Delivered} of {input.Requested} messages.",
                outstanding),
            SessionRole.Consumer => Create(SessionStopReason.Completed, null, outstanding),
            _ => throw new ArgumentOutOfRangeException(nameof(input), input.Role, "Unknown session role."),
        };

    /// <summary>A producer reports a full buffer, a consumer a quiet one.</summary>
    private static SessionStopDecision TimedOut(in SessionStopInput input, long outstanding) =>
        input.Role switch
        {
            SessionRole.Producer when input.Requested > 0 => Create(
                SessionStopReason.Timeout,
                $"buffer stayed full for {input.Timeout.TotalSeconds:F1}s " +
                $"({outstanding} messages unsent, consumer state={input.PeerState}).",
                outstanding),
            SessionRole.Producer => Create(
                SessionStopReason.Timeout,
                $"buffer stayed full for {input.Timeout.TotalSeconds:F1}s " +
                $"(produced={input.Delivered}, consumer state={input.PeerState}).",
                outstanding),
            SessionRole.Consumer => Create(
                SessionStopReason.Timeout,
                $"no messages for {input.Timeout.TotalSeconds:F1}s " +
                $"(producer state={input.PeerState}, received={input.Delivered}).",
                outstanding),
            _ => throw new ArgumentOutOfRangeException(nameof(input), input.Role, "Unknown session role."),
        };
}
