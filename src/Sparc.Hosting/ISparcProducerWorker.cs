using Sparc.Core;

namespace Sparc.Hosting;

/// <summary>
/// Custom producer endpoint hosted by <c>AddSparcProducerWorker</c>. The worker
/// receives an open, role-claimed endpoint and runs until it returns or the host
/// stops; the hosted service disposes the endpoint afterwards, which publishes
/// the graceful stop to the peer.
/// </summary>
public interface ISparcProducerWorker
{
    /// <summary>Publishes messages through <paramref name="producer"/> until cancelled.</summary>
    Task RunAsync(IProducerEndpoint producer, CancellationToken cancellationToken);
}
