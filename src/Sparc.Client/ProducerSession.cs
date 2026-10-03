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
    private const int ProgressReports = 10;
    private const long CancellationCheckMask = 0xFFFF;

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
        long progressInterval = _options.Count > 0 ? Math.Max(1, _options.Count / ProgressReports) : 0;
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

                // Full fence between our release store (tail) and the peer's
                // waiting-flag load: without it a StoreLoad reorder on weak
                // memory models lets both sides miss each other and the consumer
                // sleeps a full wait slice.
                Interlocked.MemoryBarrier();
                SignalDataIfPeerWaiting();

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
                if ((pacingTicks > 0 || (++cancellationCounter & CancellationCheckMask) == 0)
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

            WaitWhileFull(ref spin, cancellationToken);
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
        Task.Factory.StartNew(
            () => Run(cancellationToken, progress),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

    /// <summary>
    /// Waits until the consumer claims its role (<see cref="RingBufferEndpointState.Running"/>)
    /// or <paramref name="timeout"/> elapses. Returns false when the consumer did
    /// not appear or has already stopped.
    /// </summary>
    public async Task<bool> WaitForPeerAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        long startTimestamp = _timeProvider.GetTimestamp();
        using PeriodicTimer timer = new(TimeSpan.FromMilliseconds(5), _timeProvider);
        while (true)
        {
            RingBufferEndpointState peer = _buffer.ConsumerState;
            if (peer is RingBufferEndpointState.Running)
            {
                return true;
            }

            if (peer is RingBufferEndpointState.Stopped or RingBufferEndpointState.Faulted)
            {
                return false;
            }

            if (_timeProvider.GetElapsedTime(startTimestamp) >= timeout)
            {
                return false;
            }

            await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false);
        }
    }

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

    private void SignalDataIfPeerWaiting()
    {
        if (_options.WaitMode != SessionWaitMode.Notification)
        {
            return;
        }

        try
        {
            if (_buffer.IsPeerWaiting())
            {
                _options.Notification!.Data.Signal();
            }
        }
        catch (ObjectDisposedException)
        {
            // The endpoint was disposed concurrently (host shutdown); the
            // data signal is advisory and the run loop reports the stop.
        }
    }

    private void WaitWhileFull(ref SpinWait spin, CancellationToken cancellationToken)
    {
        if (_options.WaitMode == SessionWaitMode.Notification)
        {
            try
            {
                // Declare the wait before the re-check: if the consumer frees a slot
                // after this point it reads the flag and raises the latch, so the
                // wait below cannot miss it.
                _buffer.SetWaiting(true);

                // Full fence between our waiting-flag store and the count load so
                // the flag cannot be delayed past the check on weak memory models.
                Interlocked.MemoryBarrier();
                try
                {
                    if (_buffer.Count < _buffer.Capacity)
                    {
                        return; // space appeared before the wait; retry the publish
                    }

                    _options.Notification!.Space.Wait(SessionWaitTiming.WaitSlice, cancellationToken);
                }
                finally
                {
                    _buffer.SetWaiting(false);
                }
            }
            catch (ObjectDisposedException)
            {
                // The endpoint was disposed concurrently (host shutdown); the
                // next loop iteration observes the cancellation.
            }

            return;
        }

        if (_options.WaitMode == SessionWaitMode.SpinOnly)
        {
            Thread.SpinWait(64);
        }
        else
        {
            spin.SpinOnce();
        }
    }
}
