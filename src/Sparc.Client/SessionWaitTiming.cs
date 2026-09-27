namespace Sparc.Client;

/// <summary>Timing constants shared by the session wait paths.</summary>
internal static class SessionWaitTiming
{
    /// <summary>
    /// Maximum time a notification wait blocks before the session re-checks the
    /// buffer, peer state and timeouts. Bounds the damage from a peer that never
    /// signals (for example one running a different wait mode).
    /// </summary>
    internal static readonly TimeSpan WaitSlice = TimeSpan.FromMilliseconds(50);
}
