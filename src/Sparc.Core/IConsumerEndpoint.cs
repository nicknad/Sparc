namespace Sparc.Core;

/// <summary>
/// The consumer half of a shared ring region. Open one with
/// <see cref="SparcRing"/>; exactly one consumer may exist per
/// region and exactly one thread may read on it.
/// </summary>
public interface IConsumerEndpoint : IEndpoint
{
    /// <summary>
    /// Claims the consumer role. <see cref="SparcRing"/> already does this, so
    /// calling it again on the same endpoint is a no-op. Pass
    /// <paramref name="takeover"/> to reclaim the role from a crashed peer.
    /// </summary>
    void Connect(bool takeover = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to copy the oldest message out of the buffer. Returns false when
    /// the buffer is empty.
    /// </summary>
    /// <param name="destination">
    /// Receives the payload. Must be at least <see cref="IEndpoint.MaxPayloadSize"/>
    /// bytes, because the reader commits to the read before the length is known.
    /// </param>
    /// <param name="bytesRead">Number of payload bytes copied.</param>
    /// <param name="type">Type tag stored with the message.</param>
    bool TryRead(Span<byte> destination, out int bytesRead, out int type);

    /// <summary>Convenience overload that discards the type tag.</summary>
    bool TryRead(Span<byte> destination, out int bytesRead) => TryRead(destination, out bytesRead, out _);

    /// <summary>Convenience overload that discards length and type.</summary>
    bool TryRead(Span<byte> destination) => TryRead(destination, out _, out _);

    /// <summary>
    /// Returns a read-only view of the oldest published message without copying
    /// it out. Call <see cref="AdvanceRead"/> when done with the view. Returns
    /// false when the buffer is empty.
    /// </summary>
    bool TryPeek(out ReadOnlySpan<byte> payload, out int length, out int type);

    /// <summary>Releases the slot returned by <see cref="TryPeek"/> to the producer.</summary>
    void AdvanceRead();

    /// <summary>
    /// Returns a lease over the oldest message that releases the slot when
    /// disposed. Returns false when the buffer is empty.
    /// </summary>
    bool TryBeginRead(out ReadLease lease);

    /// <summary>
    /// Blocking convenience wrapper around <see cref="TryBeginRead"/> that spins
    /// until a message arrives, the caller cancels, or the buffer stays empty
    /// for 30 seconds.
    /// </summary>
    ReadLease Read();

    /// <summary>Blocking wrapper with a cancellation token, 30-second empty-buffer timeout.</summary>
    ReadLease Read(CancellationToken cancellationToken);

    /// <summary>
    /// Blocking convenience wrapper with an explicit empty-buffer timeout.
    /// Throws <see cref="RingBufferTimeoutException"/> when the buffer stays
    /// empty for <paramref name="timeout"/>.
    /// </summary>
    ReadLease Read(TimeSpan timeout, CancellationToken cancellationToken = default);
}
