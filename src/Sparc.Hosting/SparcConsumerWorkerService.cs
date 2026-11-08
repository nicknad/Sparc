using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Sparc.Core;

namespace Sparc.Hosting;

/// <summary>
/// Hosted service for a custom <typeparamref name="TWorker"/> consumer: opens
/// the default channel, claims the consumer role, hands the endpoint to the
/// worker and disposes it when the worker returns or the host stops.
/// </summary>
/// <typeparam name="TWorker">Custom consumer worker resolved from DI.</typeparam>
public sealed class SparcConsumerWorkerService<TWorker> : BackgroundService
    where TWorker : class, ISparcConsumerWorker
{
    private readonly TWorker _worker;
    private readonly IIpcMemoryRegionFactory _factory;
    private readonly SparcChannelOptions _channel;
    private readonly SparcChannelStatus _status;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<SparcConsumerWorkerService<TWorker>> _logger;
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public SparcConsumerWorkerService(
        TWorker worker,
        IIpcMemoryRegionFactory factory,
        SparcChannelOptions channel,
        SparcChannelStatus status,
        TimeProvider timeProvider,
        ILogger<SparcConsumerWorkerService<TWorker>> logger)
    {
        _worker = worker;
        _factory = factory;
        _channel = channel;
        _status = status;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    /// <summary>Completes when the service has stopped, successfully or not.</summary>
    public Task Completion => _completion.Task;

    /// <inheritdoc />
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        SparcHostedServiceRunner.ExecuteAsync(
            _status,
            _channel.Name,
            _logger,
            _completion,
            token => SparcRing.OpenConsumer(
                _factory,
                _channel.Name,
                _channel.Capacity,
                _channel.SlotSize,
                _channel.ToRingOptions(_timeProvider),
                token),
            _status.AttachConsumer,
            _ => _logger.LogInformation(
                "SPARC consumer worker ready: name={Name} worker={Worker}",
                _channel.Name, typeof(TWorker).Name),
            _status.DetachConsumer,
            async (endpoint, token) =>
            {
                await _worker.RunAsync(endpoint, token).ConfigureAwait(false);
                _logger.LogInformation("SPARC consumer worker finished: worker={Worker}", typeof(TWorker).Name);
            },
            "SPARC consumer worker failed: {Message}",
            stoppingToken);
}
