using Sparc.Core;

namespace Sparc.Hosting;

/// <summary>
/// Custom consumer endpoint hosted by <c>AddSparcConsumerWorker</c>. The worker
/// receives an open, role-claimed endpoint and runs until it returns or the host
/// stops; the hosted service disposes the endpoint afterwards.
/// </summary>
public interface ISparcConsumerWorker
{
    /// <summary>Reads messages from <paramref name="consumer"/> until cancelled.</summary>
    Task RunAsync(IConsumerEndpoint consumer, CancellationToken cancellationToken);
}
