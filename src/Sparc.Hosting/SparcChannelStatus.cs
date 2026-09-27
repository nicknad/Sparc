using Sparc.Client;
using Sparc.Core;

namespace Sparc.Hosting;

/// <summary>
/// Live view of the channel's endpoints and the last structured results, shared
/// between the hosted services and the health check.
/// </summary>
/// <remarks>
/// Endpoint states come from the shared header and are advisory: a hard-killed
/// peer stays <see cref="RingBufferEndpointState.Running"/> until the region is
/// recreated or the role is taken over.
/// </remarks>
public sealed class SparcChannelStatus(string name)
{
    private IProducerEndpoint? _producer;
    private IConsumerEndpoint? _consumer;
    private string? _failure;

    /// <summary>Region name of the default channel.</summary>
    public string Name { get; } = name;

    /// <summary>Result of the last producer session, when the session hosted one.</summary>
    public ProducerRunResult? ProducerResult { get; internal set; }

    /// <summary>Result of the last consumer session, when the session hosted one.</summary>
    public ConsumerRunResult? ConsumerResult { get; internal set; }

    /// <summary>Last failure description reported by a hosted service, if any.</summary>
    public string? FailureMessage => Volatile.Read(ref _failure);

    /// <summary>Advisory producer state, or <see cref="RingBufferEndpointState.NotPresent"/> when not open.</summary>
    public RingBufferEndpointState ProducerState =>
        Volatile.Read(ref _producer)?.ProducerState ?? RingBufferEndpointState.NotPresent;

    /// <summary>Advisory consumer state, or <see cref="RingBufferEndpointState.NotPresent"/> when not open.</summary>
    public RingBufferEndpointState ConsumerState =>
        Volatile.Read(ref _consumer)?.ConsumerState ?? RingBufferEndpointState.NotPresent;

    internal IProducerEndpoint? ProducerEndpoint => Volatile.Read(ref _producer);

    internal IConsumerEndpoint? ConsumerEndpoint => Volatile.Read(ref _consumer);

    internal void AttachProducer(IProducerEndpoint endpoint) => Volatile.Write(ref _producer, endpoint);

    internal void DetachProducer() => Volatile.Write(ref _producer, null);

    internal void AttachConsumer(IConsumerEndpoint endpoint) => Volatile.Write(ref _consumer, endpoint);

    internal void DetachConsumer() => Volatile.Write(ref _consumer, null);

    internal void Fail(string message) => Volatile.Write(ref _failure, message);
}
