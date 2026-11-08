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
        try
        {
            _buffer.Connect(_options.Takeover, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Sessions report cancellation as a structured result, never as an
            // exception (the role was not claimed).
            return new ProducerRunResult(
                0, _options.PayloadSize, TimeSpan.Zero, SessionStopReason.Cancelled,
                _options.Count, _buffer.ConsumerState, null);
        }

        long fullTimeoutTicks = (long)(_options.FullTimeout.TotalSeconds * _timeProvider.TimestampFrequency);
        bool fullTimeoutEnabled = _options.FullTimeout != Timeout.InfiniteTimeSpan;
        long pacingTicks = MessagePacer.TicksFor(_timeProvider, _options.PerMessageDelay);
        long progressInterval = SessionRuntime.ProgressInterval(_options.Count);
        SpinWait spin = new();
        long produced = 0;
        long fullSince = 0;
        long cancellationCounter = 0;
        SessionStopReason reason = SessionStopReason.Completed;
        string? failure = null;
        long startTimestamp = _timeProvider.GetTimestamp();
        long firstPublishTimestamp = 0;

        while (_options.Count == 0 || produced < _options.Count)
        {
            // Zero copy: stamp the protocol header and fill inside the shared
            // slot itself, then publish with one release store.
            if (_buffer.TryReserveWrite(_options.MessageType, _options.PayloadSize, out Span<byte> slot))
            {
                WritePayload(slot, produced);
                _buffer.CommitWrite();

                // Notification mode needs a full fence between our release store
                // (tail) and the peer's waiting-flag load: without it a StoreLoad
                // reorder on weak memory models lets both sides miss each other
                // and the consumer sleeps a full wait slice. Spin modes never set
                // the flag, so they can skip the fence entirely.
                if (_options.WaitMode == SessionWaitMode.Notification)
                {
                    Interlocked.MemoryBarrier();
                    SessionRuntime.SignalIfPeerWaiting(
                        _buffer, _options.WaitMode, _options.Notification, signalData: true);
                }

                if (firstPublishTimestamp == 0)
                {
                    firstPublishTimestamp = _timeProvider.GetTimestamp();
                }

                produced++;
                fullSince = 0;
                spin.Reset();
                if (_options.Count == 0 || produced < _options.Count)
                {
                    // Never pace after the final message: it would inflate the
                    // active window by one full delay.
                    MessagePacer.Wait(_timeProvider, startTimestamp, produced, pacingTicks, cancellationToken);
                }

                if (progress is not null && progressInterval > 0 && produced % progressInterval == 0)
                {
                    progress.Report(new ProducerProgress(produced, _timeProvider.GetElapsedTime(startTimestamp)));
                }

                // Paced runs can spend minutes between mask boundaries, so poll
                // every message once pacing is on; unpaced runs keep the batched
                // fast-path check.
                if (SessionRuntime.ShouldCheckCancellation(ref cancellationCounter, pacingTicks)
                    && cancellationToken.IsCancellationRequested)
                {
                    reason = SessionStopReason.Cancelled;
                    break;
                }

                continue;
            }

            if (cancellationToken.IsCancellationRequested)
            {
                reason = SessionStopReason.Cancelled;
                break;
            }

            long now = _timeProvider.GetTimestamp();
            if (fullSince == 0)
            {
                fullSince = now;
            }
            else
            {
                // Check the peer on every full iteration (a single volatile
                // read): a stopped consumer should not cost a whole FullTimeout.
                RingBufferEndpointState peer = _buffer.ConsumerState;
                if (peer is RingBufferEndpointState.Stopped or RingBufferEndpointState.Faulted)
                {
                    reason = SessionStopReason.PeerStopped;
                    failure = _options.Count > 0
                        ? $"consumer is gone (state={peer}) with {_options.Count - produced} messages unsent."
                        : $"consumer is gone (state={peer}) after {produced} messages.";
                    break;
                }

                if (fullTimeoutEnabled && now - fullSince >= fullTimeoutTicks)
                {
                    reason = SessionStopReason.Timeout;
                    failure = _options.Count > 0
                        ? $"buffer stayed full for {_options.FullTimeout.TotalSeconds:F1}s " +
                          $"({_options.Count - produced} messages unsent, consumer state={peer})."
                        : $"buffer stayed full for {_options.FullTimeout.TotalSeconds:F1}s " +
                          $"(produced={produced}, consumer state={peer}).";
                    break;
                }
            }

            SessionRuntime.WaitForNotification(
                _buffer,
                _options.WaitMode,
                _options.Notification,
                waitOnDataSignal: false,
                ref spin,
                cancellationToken);
        }

        long endTimestamp = _timeProvider.GetTimestamp();
        TimeSpan elapsed = _timeProvider.GetElapsedTime(startTimestamp, endTimestamp);
        TimeSpan activeElapsed = firstPublishTimestamp == 0
            ? TimeSpan.Zero
            : _timeProvider.GetElapsedTime(firstPublishTimestamp, endTimestamp);
        _logger.LogDebug(
            "Producer session finished: produced={Produced} reason={Reason} elapsed={Elapsed} activeElapsed={ActiveElapsed}",
            produced, reason, elapsed, activeElapsed);

        return new ProducerRunResult(
            produced,
            _options.PayloadSize,
            elapsed,
            reason,
            _options.Count > 0 ? _options.Count - produced : 0,
            _buffer.ConsumerState,
            failure)
        {
            ActiveElapsed = activeElapsed,
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

    /// <summary>
    /// Writes one payload. The default path stamps the session protocol; a
    /// custom <see cref="ProducerSessionOptions.PayloadWriter"/> writes either
    /// behind the header or instead of it.
    /// </summary>
    private void WritePayload(Span<byte> slot, long index)
    {
        if (_options.IncludeSessionHeader)
        {
            // Stream position of this slot. The shared tail still equals the
            // reserved slot's sequence here (only this thread commits, and it
            // commits after this call), and the consumer seeds its expectation
            // from HeadSequence — so verification holds across producer and
            // consumer restarts and takeovers, where run-local indices always
            // mismatched.
            RingBufferMessage.Write(slot, _buffer.TailSequence, _timeProvider.GetTimestamp());
        }

        if (_options.PayloadWriter is { } writer)
        {
            try
            {
                writer(index, slot);
            }
            catch
            {
                // Keep the endpoint usable: a throwing writer must not leave the
                // reservation pending, or every later TryReserveWrite would fail.
                _buffer.AbandonWrite();
                throw;
            }

            return;
        }

        if (_options.IncludeSessionHeader)
        {
            RingBufferMessage.FillPayload(slot);
        }
        else
        {
            slot.Clear();
        }
    }
}
