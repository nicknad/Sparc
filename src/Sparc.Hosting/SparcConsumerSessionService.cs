using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Sparc.Client;
using Sparc.Core;

namespace Sparc.Hosting;

/// <summary>
/// Hosted consumer that opens the default channel, claims the consumer role and
/// runs a <see cref="ConsumerSession"/> to completion (or until the host stops).
/// </summary>
/// <remarks>
/// An open failure (missing region, role conflict, incompatible geometry) is
/// logged, recorded in <see cref="SparcChannelStatus"/> and rethrown, so the
/// default host behavior stops the application instead of running blind.
/// </remarks>
public sealed class SparcConsumerSessionService : BackgroundService
{
    private readonly IIpcMemoryRegionFactory _factory;
    private readonly SparcChannelOptions _channel;
    private readonly ConsumerSessionOptions _session;
    private readonly SparcChannelStatus _status;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<SparcConsumerSessionService> _logger;
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public SparcConsumerSessionService(
        IIpcMemoryRegionFactory factory,
        SparcChannelOptions channel,
        ConsumerSessionOptions session,
        SparcChannelStatus status,
        TimeProvider timeProvider,
        ILogger<SparcConsumerSessionService> logger)
    {
        _factory = factory;
        _channel = channel;
        _session = session;
        _status = status;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    /// <summary>Completes when the service has stopped, successfully or not.</summary>
    public Task Completion => _completion.Task;

    /// <summary>The session result, once the run finished.</summary>
    public ConsumerRunResult? Result { get; private set; }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        IConsumerEndpoint? endpoint = null;
        try
        {
            endpoint = await Task.Run(
                () => SparcRing.OpenConsumer(
                    _factory,
                    _channel.Name,
                    _channel.Capacity,
                    _channel.SlotSize,
                    _channel.ToRingOptions(_timeProvider),
                    stoppingToken),
                stoppingToken).ConfigureAwait(false);

            _status.AttachConsumer(endpoint);
            _logger.LogInformation(
                "SPARC consumer ready: name={Name} capacity={Capacity} slotSize={SlotSize}",
                _channel.Name, endpoint.Capacity, endpoint.SlotSize);

            ConsumerRunResult result = await new ConsumerSession(endpoint, _session, _timeProvider, _logger)
                .RunAsync(stoppingToken)
                .ConfigureAwait(false);

            Result = result;
            _status.ConsumerResult = result;
            SparcMetrics.RecordConsumerRun(_channel.Name, result);
            _logger.LogInformation(
                "SPARC consumer stopped: received={Received} reason={Reason} failure={Failure}",
                result.Received, result.Reason, result.FailureMessage);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown; the session reports stop reasons through its result.
        }
        catch (Exception exception)
        {
            _status.Fail(exception.Message);
            SparcMetrics.RecordFailure(_channel.Name);
            _logger.LogError(exception, "SPARC consumer failed: {Message}", exception.Message);
            throw;
        }
        finally
        {
            _status.DetachConsumer();
            endpoint?.Dispose();
            _completion.TrySetResult();
        }
    }
}
