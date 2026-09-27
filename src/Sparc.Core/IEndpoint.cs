namespace Sparc.Core;

/// <summary>
/// Shared surface of one end of a ring buffer region. A host receives an
/// <see cref="IProducerEndpoint"/> or an <see cref="IConsumerEndpoint"/> from
/// <see cref="SparcRing"/>; the role is fixed by the factory that created it, so
/// a consumer cannot publish and a producer cannot read.
/// </summary>
public interface IEndpoint : IDisposable
{
    /// <summary>The region name this endpoint is attached to.</summary>
    string Name { get; }

    /// <summary>Number of slots.</summary>
    int Capacity { get; }

    /// <summary>Total bytes per slot, including the per-slot header.</summary>
    int SlotSize { get; }

    /// <summary>Maximum payload accepted per message.</summary>
    int MaxPayloadSize { get; }

    /// <summary>Sequence number the consumer will use for its next read.</summary>
    long HeadSequence { get; }

    /// <summary>Sequence number the producer will use for its next write.</summary>
    long TailSequence { get; }

    /// <summary>True when there is nothing to read.</summary>
    bool IsEmpty { get; }

    /// <summary>Number of messages currently buffered.</summary>
    int Count { get; }

    /// <summary>Advisory state reported by the producer endpoint.</summary>
    RingBufferEndpointState ProducerState { get; }

    /// <summary>Advisory state reported by the consumer endpoint.</summary>
    RingBufferEndpointState ConsumerState { get; }

    /// <summary>
    /// True when the peer declared that it is blocked waiting for this endpoint
    /// (producer reads the consumer's flag, consumer reads the producer's).
    /// </summary>
    bool IsPeerWaiting();

    /// <summary>
    /// Declares whether this endpoint is currently blocked waiting for the peer.
    /// The peer reads it before raising a notification, so it only pays the OS
    /// signal when it can help.
    /// </summary>
    void SetWaiting(bool waiting);

    /// <summary>Marks this endpoint as faulted so the peer can stop waiting for it.</summary>
    void Abort();
}
