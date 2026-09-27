namespace Sparc.Client;

/// <summary>
/// A one-deep "wake up the peer" latch used by
/// <see cref="SessionWaitMode.Notification"/>. Implementations may be local to
/// a process (<see cref="InProcessSessionSignal"/>) or backed by an OS object
/// shared between processes (<see cref="NamedSessionSignal"/>).
/// </summary>
/// <remarks>
/// A raise is only a hint: the waiting session always re-checks the ring buffer
/// after waking, and the raising session only raises when the peer declared
/// itself waiting (<see cref="Core.IRingBufferEndpoint.IsPeerWaiting"/>).
/// Losing a raise is therefore harmless; it can only cause the waiter to rely
/// on its bounded timeout instead of waking immediately.
/// </remarks>
public interface ISessionSignal : IDisposable
{
    /// <summary>Raises the latch. Safe to call when the latch is already raised.</summary>
    void Signal();

    /// <summary>
    /// Waits up to <paramref name="timeout"/> for the latch. Returns true when
    /// raised, false on timeout. Returns false (never throws) when
    /// <paramref name="cancellationToken"/> fires.
    /// </summary>
    bool Wait(TimeSpan timeout, CancellationToken cancellationToken);
}
