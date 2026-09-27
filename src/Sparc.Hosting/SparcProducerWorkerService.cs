using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Sparc.Core;

namespace Sparc.Hosting;

/// <summary>
/// Hosted service for a custom <typeparamref name="TWorker"/> producer: opens
/// the default channel, claims the producer role, hands the endpoint to the
/// worker and disposes it when the worker returns or the host stops.
/// </summary>
/// <typeparam name="TWorker">Custom producer worker resolved from DI.</typeparam>
public sealed class SparcProducerWorkerService<TWorker> : BackgroundService
    where TWorker : class, ISparcProducerWorker
{
    private readonly TWorker _worker;
    private readonly IIpcMemoryRegionFactory _factory;
    private readonly SparcChannelOptions _channel;
    private readonly SparcChannelStatus _status;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<SparcProducerWorkerService<TWorker>> _logger;
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public SparcProducerWorkerService(
        TWorker worker,
        IIpcMemoryRegionFactory factory,
        SparcChannelOptions channel,
        SparcChannelStatus status,
        TimeProvider timeProvider,
        ILogger<SparcProducerWorkerService<TWorker>> logger)
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
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        IProducerEndpoint? endpoint = null;
        try
        {
            endpoint = await Task.Run(
                () => SparcRing.OpenProducer(
                    _factory,
                    _channel.Name,
                    _channel.Capacity,
                    _channel.SlotSize,
                    _channel.ToRingOptions(_timeProvider),
                    stoppingToken),
                stoppingToken).ConfigureAwait(false);

            _status.AttachProducer(endpoint);
            _logger.LogInformation(
                "SPARC producer worker ready: name={Name} worker={Worker}",
                _channel.Name, typeof(TWorker).Name);

            await _worker.RunAsync(endpoint, stoppingToken).ConfigureAwait(false);
            _logger.LogInformation("SPARC producer worker finished: worker={Worker}", typeof(TWorker).Name);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown.
        }
        catch (Exception exception)
        {
            _status.Fail(exception.Message);
            SparcMetrics.RecordFailure(_channel.Name);
            _logger.LogError(exception, "SPARC producer worker failed: {Message}", exception.Message);
            throw;
        }
        finally
        {
            _status.DetachProducer();
            endpoint?.Dispose();
            _completion.TrySetResult();
        }
    }
}
