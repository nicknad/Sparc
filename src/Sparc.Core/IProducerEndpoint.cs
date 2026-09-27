namespace Sparc.Core;

/// <summary>
/// The producer half of a shared ring region. Open one with
/// <see cref="SparcRing.OpenProducer"/>; exactly one producer may exist per
/// region and exactly one thread may publish on it.
/// </summary>
public interface IProducerEndpoint : IEndpoint
{
    /// <summary>
    /// Claims the producer role. <see cref="SparcRing"/> already does this, so
    /// calling it again on the same endpoint is a no-op. Pass
    /// <paramref name="takeover"/> to reclaim the role from a crashed peer.
    /// </summary>
    void Connect(bool takeover = false, CancellationToken cancellationToken = default);

    /// <summary>Attempts to copy one message into the buffer. Returns false when the buffer is full.</summary>
    bool TryPublish(int type, ReadOnlySpan<byte> payload);

    /// <summary>Convenience overload for messages that do not use a type tag.</summary>
    bool TryPublish(ReadOnlySpan<byte> payload) => TryPublish(0, payload);

    /// <summary>
    /// Blocking convenience wrapper: spins until the message fits or the caller
    /// cancels, and throws <see cref="RingBufferTimeoutException"/> when the
    /// buffer stays full for 30 seconds.
    /// </summary>
    void Publish(int type, ReadOnlySpan<byte> payload, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reserves the next slot and returns a writable view of its payload area,
    /// so a message can be written without an intermediate copy. Returns false
    /// when the buffer is full. Commit or abandon through <see cref="CommitWrite"/> /
    /// <see cref="AbandonWrite"/>, or use <see cref="TryBeginWrite"/> for a
    /// scope-based lease.
    /// </summary>
    bool TryReserveWrite(int type, int length, out Span<byte> payload);

    /// <summary>Publishes the slot reserved by <see cref="TryReserveWrite"/>.</summary>
    void CommitWrite();

    /// <summary>Discards the slot reserved by <see cref="TryReserveWrite"/> without publishing it.</summary>
    void AbandonWrite();

    /// <summary>
    /// Reserves the next slot and returns a lease that commits when disposed.
    /// Returns false when the buffer is full.
    /// </summary>
    bool TryBeginWrite(int type, int length, out WriteLease lease);

    /// <summary>
    /// Blocking convenience wrapper around <see cref="TryBeginWrite"/> that
    /// spins until a slot is free, the caller cancels, or the buffer stays full
    /// for 30 seconds.
    /// </summary>
    WriteLease BeginWrite(int type, int length, CancellationToken cancellationToken = default);
}
