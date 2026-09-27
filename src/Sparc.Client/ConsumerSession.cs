using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Sparc.Client.Diagnostics;
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
    private const int ProgressReports = 10;
    private const long CancellationCheckMask = 0xFFFF;

    private readonly IRingBufferEndpoint _buffer;
    private readonly ConsumerSessionOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public ConsumerSession(
        IRingBufferEndpoint buffer,
        ConsumerSessionOptions options,
        TimeProvider? timeProvider = null,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.PerMessageDelay, TimeSpan.Zero);

        _buffer = buffer;
        _options = options;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger.Instance;
    }

    public ConsumerRunResult Run(
        CancellationToken cancellationToken = default,
        IProgress<ConsumerProgress>? progress = null)
    {
        try
        {
            _buffer.Connect(RingBufferEndpointRole.Consumer, _options.Takeover, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Sessions report cancellation as a structured result, never as an
            // exception (the role was not claimed).
            return Cancelled();
        }

        ReadState state = new();
        SpinWait spin = new();
        long idleSince = 0;
        long cancellationCounter = 0;
        long idleTimeoutTicks = (long)(_options.IdleTimeout.TotalSeconds * _timeProvider.TimestampFrequency);
        long pacingTicks = MessagePacer.TicksFor(_timeProvider, _options.PerMessageDelay);
        long progressInterval = _options.Count > 0 ? Math.Max(1, _options.Count / ProgressReports) : 0;
        long runStartTimestamp = _timeProvider.GetTimestamp();
        SessionStopReason reason = SessionStopReason.Completed;

        while (_options.Count == 0 || state.Received < _options.Count)
        {
            int result = ProcessOne(state);
            if (result == 1)
            {
                spin.Reset();
                idleSince = 0;
                MessagePacer.Wait(_timeProvider, runStartTimestamp, state.Received, pacingTicks);

                if (progress is not null && progressInterval > 0 && state.Received % progressInterval == 0)
                {
                    progress.Report(new ConsumerProgress(
                        state.Received, state.ReceivedBytes, _timeProvider.GetElapsedTime(runStartTimestamp)));
                }

                if ((++cancellationCounter & CancellationCheckMask) == 0 && cancellationToken.IsCancellationRequested)
                {
                    reason = SessionStopReason.Cancelled;
                    break;
                }

                continue;
            }

            if (result == -1)
            {
                reason = SessionStopReason.VerificationFailed;
                break;
            }

            // Empty: has the producer stopped?
            RingBufferEndpointState producer = _buffer.ProducerState;
            if (producer is RingBufferEndpointState.Stopped or RingBufferEndpointState.Faulted)
            {
                reason = StopAfterPeerStopped(state);
                break;
            }

            if (cancellationToken.IsCancellationRequested)
            {
                reason = SessionStopReason.Cancelled;
                break;
            }

            long now = _timeProvider.GetTimestamp();
            if (idleSince == 0)
            {
                idleSince = now;
            }
            else if (now - idleSince >= idleTimeoutTicks)
            {
                reason = SessionStopReason.Timeout;
                state.Failure =
                    $"no messages for {_options.IdleTimeout.TotalSeconds:F1}s " +
                    $"(producer state={producer}, received={state.Received}).";
                break;
            }

            WaitWhileEmpty(ref spin);
        }

        TimeSpan elapsed = ComputeElapsed(state, runStartTimestamp);
        TimeSpan runElapsed = _timeProvider.GetElapsedTime(runStartTimestamp);

        _logger.LogDebug(
            "Consumer session finished: received={Received} reason={Reason} elapsed={Elapsed} runElapsed={RunElapsed}",
            state.Received, reason, elapsed, runElapsed);

        return new ConsumerRunResult(
            state.Received, state.ReceivedBytes, elapsed, reason, state.Histogram, state.Failure)
        {
            RunElapsed = runElapsed,
        };
    }

    /// <summary>
    /// Reads and verifies one message. Returns 1 when a message was consumed,
    /// 0 when the buffer is empty, and -1 when verification failed (the reason
    /// is stored in <c>ReadState.Failure</c>).
    /// </summary>
    private int ProcessOne(ReadState state)
    {
        // Zero copy: verify and sample straight out of the shared slot; the
        // producer cannot reuse it until AdvanceRead publishes the new head.
        if (!_buffer.TryPeek(out ReadOnlySpan<byte> payload, out int length, out int type))
        {
            return 0;
        }

        long now = _timeProvider.GetTimestamp();

        if (_options.Verify)
        {
            string? failure = Verify(state, payload, length, type);
            if (failure is not null)
            {
                state.Failure = failure;

                // Consume the invalid message (the copy path advanced head before
                // verifying) so the session stops instead of re-reading it.
                _buffer.AdvanceRead();
                return -1;
            }
        }

        // Latency sampling is independent of verification: the producer session
        // always stamps sequence and timestamp, so the sample is valid even when
        // the caller disabled structural checks.
        if (length >= RingBufferMessage.HeaderSize)
        {
            long sentTimestamp = RingBufferMessage.ReadTimestamp(payload);
            if (sentTimestamp > 0)
            {
                state.Histogram.Record(now - sentTimestamp);
            }
        }

        _buffer.AdvanceRead();

        state.Expected++;
        state.Received++;
        state.ReceivedBytes += length;
        if (state.FirstMessageTimestamp == 0)
        {
            state.FirstMessageTimestamp = now;
        }

        state.LastMessageTimestamp = now;
        return 1;
    }

    /// <summary>Returns a failure description, or null when the message is valid.</summary>
    private string? Verify(ReadState state, ReadOnlySpan<byte> payload, int length, int type)
    {
        if (length < RingBufferMessage.HeaderSize)
        {
            return $"message {state.Received} is only {length} bytes.";
        }

        long sequence = RingBufferMessage.ReadSequence(payload);
        if (sequence != state.Expected)
        {
            return $"sequence mismatch at message {state.Received}: expected {state.Expected}, received {sequence}.";
        }

        if (type != _options.ExpectedType)
        {
            return $"message {state.Received} has type {type}, expected {_options.ExpectedType}.";
        }

        if (_options.VerifyPayload && !RingBufferMessage.IsPayloadIntact(payload, length))
        {
            return $"message {state.Received} payload is corrupted.";
        }

        return null;
    }

    /// <summary>
    /// Drains messages published before the producer went away and classifies
    /// the stop reason. Seeing a terminal producer state has acquire semantics,
    /// so the drain cannot miss messages.
    /// </summary>
    private SessionStopReason StopAfterPeerStopped(ReadState state)
    {
        int drain;
        while ((drain = ProcessOne(state)) == 1)
        {
        }

        if (drain == -1)
        {
            return SessionStopReason.VerificationFailed;
        }

        if (_options.Count > 0 && state.Received < _options.Count)
        {
            state.Failure = $"producer stopped after {state.Received} of {_options.Count} messages.";
            return SessionStopReason.PeerStopped;
        }

        return SessionStopReason.Completed;
    }

    private TimeSpan ComputeElapsed(ReadState state, long runStartTimestamp) =>
        state.Received > 1 && state.FirstMessageTimestamp != 0
            ? _timeProvider.GetElapsedTime(state.FirstMessageTimestamp, state.LastMessageTimestamp)
            : _timeProvider.GetElapsedTime(runStartTimestamp);

    private void WaitWhileEmpty(ref SpinWait spin)
    {
        if (_options.WaitMode == SessionWaitMode.SpinOnly)
        {
            Thread.SpinWait(64);
        }
        else
        {
            spin.SpinOnce();
        }
    }

    private static ConsumerRunResult Cancelled() =>
        new(0, 0, TimeSpan.Zero, SessionStopReason.Cancelled, new LatencyHistogram(), null);

    /// <summary>Mutable counters for one run, kept out of the main loop body.</summary>
    private sealed class ReadState
    {
        public LatencyHistogram Histogram { get; } = new();

        public long Received { get; set; }

        public long ReceivedBytes { get; set; }

        public long Expected { get; set; }

        public long FirstMessageTimestamp { get; set; }

        public long LastMessageTimestamp { get; set; }

        public string? Failure { get; set; }
    }
}
