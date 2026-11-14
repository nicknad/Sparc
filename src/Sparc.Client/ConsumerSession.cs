using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Sparc.Core;

namespace Sparc.Client;

/// <summary>
/// Reusable consumer endpoint: claims the consumer role, reads and verifies
/// <see cref="RingBufferMessage"/>-framed messages, collects one-way latency
/// samples, and drains the buffer when it observes a terminal producer state.
/// Contains no console, argument parsing or process concerns.
/// </summary>
public sealed class ConsumerSession
{
    private readonly IConsumerEndpoint _buffer;
    private readonly ConsumerSessionOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public ConsumerSession(
        IConsumerEndpoint buffer,
        ConsumerSessionOptions options,
        TimeProvider? timeProvider = null,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.PerMessageDelay, TimeSpan.Zero);
        if (options.IdleTimeout < TimeSpan.Zero && options.IdleTimeout != Timeout.InfiniteTimeSpan)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options), "IdleTimeout must be non-negative or Timeout.InfiniteTimeSpan.");
        }
        if (options.WaitMode == SessionWaitMode.Notification && options.Notification is null)
        {
            throw new ArgumentException(
                "WaitMode.Notification requires a SessionNotification instance.", nameof(options));
        }

        _buffer = buffer;
        _options = options;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger.Instance;
    }

    public ConsumerRunResult Run(
        CancellationToken cancellationToken = default,
        IProgress<ConsumerProgress>? progress = null)
    {
        SessionRunContext context = new(
            _buffer,
            _options.Count,
            _options.PerMessageDelay,
            _options.WaitMode,
            _options.Notification,
            WaitOnDataSignal: true);
        ConsumerRole role = new(_buffer, _options, _timeProvider, progress);

        SessionRunOutcome outcome = SessionRunEngine.Run(ref role, in context, _timeProvider, cancellationToken);

        _logger.LogDebug(
            "Consumer session finished: received={Received} reason={Reason} elapsed={Elapsed} runElapsed={RunElapsed}",
            outcome.Delivered, outcome.Decision.Reason, outcome.StreamElapsed, outcome.RunElapsed);

        return new ConsumerRunResult(
            outcome.Delivered,
            role.ReceivedBytes,
            outcome.StreamElapsed,
            outcome.Decision.Reason,
            role.Histogram,
            outcome.Decision.Message)
        {
            RunElapsed = outcome.RunElapsed,
        };
    }

    /// <summary>
    /// Runs the blocking session on a dedicated thread so hosts can await it.
    /// Cancellation is reported through the result, never thrown.
    /// </summary>
    public Task<ConsumerRunResult> RunAsync(
        CancellationToken cancellationToken = default,
        IProgress<ConsumerProgress>? progress = null) =>
        SessionRuntime.RunOnDedicatedThread(() => Run(cancellationToken, progress));

    /// <summary>
    /// Waits until the producer claims its role (<see cref="RingBufferEndpointState.Running"/>)
    /// or <paramref name="timeout"/> elapses. Returns false when the producer did
    /// not appear or has already stopped.
    /// </summary>
    public Task<bool> WaitForPeerAsync(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        SessionRuntime.WaitForPeerAsync(
            () => _buffer.ProducerState,
            _timeProvider,
            timeout,
            cancellationToken);
}
