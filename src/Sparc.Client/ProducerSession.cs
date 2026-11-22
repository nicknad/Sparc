using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Sparc.Core;

namespace Sparc.Client;

/// <summary>
/// Reusable producer endpoint: claims the producer role, writes
/// <see cref="RingBufferMessage"/>-framed messages with timestamps, and reports
/// a structured outcome. Contains no console, argument parsing or process
/// concerns, so CLIs, web apps and worker services can all host it.
/// </summary>
public sealed class ProducerSession
{
    private readonly IProducerEndpoint _buffer;
    private readonly ProducerSessionOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public ProducerSession(
        IProducerEndpoint buffer,
        ProducerSessionOptions options,
        TimeProvider? timeProvider = null,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentNullException.ThrowIfNull(options);
        if (options.WaitMode == SessionWaitMode.Notification && options.Notification is null)
        {
            throw new ArgumentException(
                "WaitMode.Notification requires a SessionNotification instance.", nameof(options));
        }

        ProducerSessionOptions.Validate(
            options.Count, options.PayloadSize, options.PerMessageDelay, buffer.MaxPayloadSize);
        if (options.FullTimeout < TimeSpan.Zero && options.FullTimeout != Timeout.InfiniteTimeSpan)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options), "FullTimeout must be non-negative or Timeout.InfiniteTimeSpan.");
        }

        _buffer = buffer;
        _options = options;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger.Instance;
    }

    public ProducerRunResult Run(
        CancellationToken cancellationToken = default,
        IProgress<ProducerProgress>? progress = null)
    {
        SessionRunContext context = new(
            _buffer,
            _options.Count,
            _options.PerMessageDelay,
            _options.WaitMode,
            _options.Notification,
            WaitOnDataSignal: false);
        ProducerRole role = new(_buffer, _options, _timeProvider, progress);

        SessionRunOutcome outcome = SessionRunEngine.Run(ref role, in context, _timeProvider, cancellationToken);

        _logger.LogDebug(
            "Producer session finished: produced={Produced} reason={Reason} elapsed={Elapsed} activeElapsed={ActiveElapsed}",
            outcome.Delivered, outcome.Decision.Reason, outcome.RunElapsed, outcome.ActiveElapsed);

        return new ProducerRunResult(
            outcome.Delivered,
            _options.PayloadSize,
            outcome.RunElapsed,
            outcome.Decision.Reason,
            outcome.Decision.Outstanding,
            _buffer.ConsumerState,
            outcome.Decision.Message)
        {
            ActiveElapsed = outcome.ActiveElapsed,
        };
    }

    /// <summary>
    /// Runs the blocking session on a dedicated thread so hosts can await it.
    /// Cancellation is reported through the result, never thrown.
    /// </summary>
    public Task<ProducerRunResult> RunAsync(
        CancellationToken cancellationToken = default,
        IProgress<ProducerProgress>? progress = null) =>
        SessionRuntime.RunOnDedicatedThread(() => Run(cancellationToken, progress));

    /// <summary>
    /// Waits until the consumer claims its role (<see cref="RingBufferEndpointState.Running"/>)
    /// or <paramref name="timeout"/> elapses. Returns false when the consumer did
    /// not appear or has already stopped.
    /// </summary>
    public Task<bool> WaitForPeerAsync(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        SessionRuntime.WaitForPeerAsync(
            () => _buffer.ConsumerState,
            _timeProvider,
            timeout,
            cancellationToken);
}
