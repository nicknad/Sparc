namespace Sparc.Client;

/// <summary>
/// How a session waits while the buffer is full (producer) or empty (consumer).
/// </summary>
public enum SessionWaitMode
{
    /// <summary>
    /// <see cref="SpinWait"/> behavior: spin briefly, then yield and sleep in
    /// increasing steps. Low CPU use, but an idle endpoint only notices new work
    /// at timer granularity (milliseconds).
    /// </summary>
    SpinThenSleep = 0,

    /// <summary>
    /// Never sleep: keep spinning with <see cref="Thread.SpinWait(int)"/>.
    /// Lowest wake-up latency, at the cost of burning a core while the peer is
    /// idle. Intended for latency-sensitive consumers.
    /// </summary>
    SpinOnly = 1,

    /// <summary>
    /// Block on an OS notification (<see cref="SessionNotification"/>) that the
    /// peer raises when it publishes or consumes. Near-spin wake-up latency
    /// without a pinned core: the blocked endpoint costs no CPU. Both endpoints
    /// must use this mode and share the same <see cref="SessionNotification"/>.
    /// </summary>
    Notification = 2,
}
