namespace Sparc.Core;

/// <summary>
/// An <see cref="IRingBuffer"/> that is one endpoint of a shared region: it can
/// claim a role and report the peer's advisory state. The session layer depends
/// on this contract instead of the concrete <c>SharedRingBuffer</c>, so another
/// endpoint implementation can be hosted without changing the sessions.
/// </summary>
public interface IRingBufferEndpoint : IRingBuffer
{
    /// <summary>Advisory state reported by the producer endpoint.</summary>
    RingBufferEndpointState ProducerState { get; }

    /// <summary>Advisory state reported by the consumer endpoint.</summary>
    RingBufferEndpointState ConsumerState { get; }

    /// <summary>
    /// Claims the given role. Throws <see cref="RingBufferRoleConflictException"/>
    /// when the role is already claimed by a live instance; pass
    /// <paramref name="takeover"/> to reclaim it from a crashed peer.
    /// </summary>
    /// <param name="role">Role to claim: producer or consumer.</param>
    /// <param name="takeover">When true, reclaim the role from a crashed peer.</param>
    /// <param name="cancellationToken">
    /// Bounds the CAS retry loop; pass <see cref="CancellationToken.None"/> to
    /// retry indefinitely.
    /// </param>
    void Connect(RingBufferEndpointRole role, bool takeover = false, CancellationToken cancellationToken = default);
}
