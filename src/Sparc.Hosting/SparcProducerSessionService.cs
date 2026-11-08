using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Sparc.Client;
using Sparc.Core;

namespace Sparc.Hosting;

/// <summary>
/// Hosted producer that opens the default channel, claims the producer role and
/// runs a <see cref="ProducerSession"/> to completion (or until the host stops).
/// The endpoint is disposed afterwards, which publishes the graceful stop to
/// the consumer.
/// </summary>
/// <remarks>
/// An open failure (missing region, role conflict, incompatible geometry) is
/// logged, recorded in <see cref="SparcChannelStatus"/> and rethrown, so the
/// default host behavior stops the application instead of running blind.
/// </remarks>
public sealed class SparcProducerSessionService : BackgroundService
{
    private readonly IIpcMemoryRegionFactory _factory;
    private readonly SparcChannelOptions _channel;
    private readonly ProducerSessionOptions _session;
    private readonly SparcChannelStatus _status;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<SparcProducerSessionService> _logger;
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public SparcProducerSessionService(
        IIpcMemoryRegionFactory factory,
        SparcChannelOptions channel,
        ProducerSessionOptions session,
        SparcChannelStatus status,
        TimeProvider timeProvider,
        ILogger<SparcProducerSessionService> logger)
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
    public ProducerRunResult? Result { get; private set; }

    /// <inheritdoc />
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        SparcHostedServiceRunner.ExecuteAsync(
            _status,
            _channel.Name,
            _logger,
            _completion,
            token => SparcRing.OpenProducer(
                _factory,
                _channel.Name,
                _channel.Capacity,
                _channel.SlotSize,
                _channel.ToRingOptions(_timeProvider, _session.Takeover),
                token),
            _status.AttachProducer,
            endpoint => _logger.LogInformation(
                "SPARC producer ready: name={Name} capacity={Capacity} slotSize={SlotSize}",
                _channel.Name, endpoint.Capacity, endpoint.SlotSize),
            _status.DetachProducer,
            async (endpoint, token) =>
            {
                ProducerRunResult result = await new ProducerSession(endpoint, _session, _timeProvider, _logger)
                    .RunAsync(token)
                    .ConfigureAwait(false);

                Result = result;
                _status.ProducerResult = result;
                SparcMetrics.RecordProducerRun(_channel.Name, result);
                _logger.LogInformation(
                    "SPARC producer stopped: produced={Produced} reason={Reason} failure={Failure}",
                    result.Produced, result.Reason, result.FailureMessage);

                if (result.FailureMessage is { } failure)
                {
                    // A structured session failure must not end the host silently.
                    throw new InvalidOperationException($"SPARC producer session failed: {failure}");
                }
            },
            "SPARC producer failed: {Message}",
            stoppingToken);
}
