using Sparc.Client.Diagnostics;
using Sparc.Core;

namespace Sparc.Client;

/// <summary>
/// The consumer's half of a session run: peeks, verifies, latency-samples and
/// advances one message per step, drains after a terminal producer, and runs the
/// consumer stall order — peer, drain, cancel, stamp, timeout — inside
/// <see cref="Step"/>. The verification detail and the drain's precedence live
/// here because they are consumer knowledge.
/// </summary>
internal struct ConsumerRole : ISessionRunRole
{
    private readonly IConsumerEndpoint _buffer;
    private readonly ConsumerSessionOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly IProgress<ConsumerProgress>? _progress;
    private SessionStallClock _stall;
    private long _expected;
    private long _received;
    private long _receivedBytes;
    private long _firstMessageTimestamp;
    private long _lastMessageTimestamp;
    private RingBufferEndpointState _observedPeer;
    private string? _failure;

    internal ConsumerRole(
        IConsumerEndpoint buffer,
        ConsumerSessionOptions options,
        TimeProvider timeProvider,
        IProgress<ConsumerProgress>? progress)
    {
        _buffer = buffer;
        _options = options;
        _timeProvider = timeProvider;
        _progress = progress;
        _stall = new SessionStallClock(
            options.IdleTimeout != Timeout.InfiniteTimeSpan,
            (long)(options.IdleTimeout.TotalSeconds * timeProvider.TimestampFrequency));
        _expected = 0;
        _received = 0;
        _receivedBytes = 0;
        _firstMessageTimestamp = -1;
        _lastMessageTimestamp = -1;
        _observedPeer = default;
        _failure = null;
        Histogram = new LatencyHistogram();
    }

    public readonly long Delivered => _received;

    public readonly long FirstTimestamp => _firstMessageTimestamp;

    public readonly long LastTimestamp => _lastMessageTimestamp;

    /// <summary>Bytes decoded for this run, including drained messages.</summary>
    public readonly long ReceivedBytes => _receivedBytes;

    /// <summary>The run's latency samples.</summary>
    public readonly LatencyHistogram Histogram { get; }

    public bool TryConnect(CancellationToken cancellationToken)
    {
        try
        {
            _buffer.Connect(_options.Takeover, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // The role was not claimed; the engine shapes the cancelled result.
            return false;
        }

        // Seed from the stream position of the next slot to read so verification
        // matches the producer's stream-position stamps even after a restart or
        // takeover (head == the sequence of the oldest unread message; this thread
        // is the only consumer, so it cannot move before the first read).
        _expected = _buffer.HeadSequence;
        return true;
    }

    public SessionRunStep Step(CancellationToken cancellationToken)
    {
        int result = ProcessOne();
        if (result == 1)
        {
            _stall.Reset();
            return SessionRunStep.Progressed;
        }

        if (result == -1)
        {
            return SessionRunStep.Stop(SessionTerminalEvent.VerificationFailed);
        }

        // Empty: has the producer stopped?
        RingBufferEndpointState producer = _buffer.ProducerState;
        if (producer is RingBufferEndpointState.Stopped or RingBufferEndpointState.Faulted)
        {
            _observedPeer = producer;
            return Drain(cancellationToken);
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return SessionRunStep.Stop(SessionTerminalEvent.Cancelled);
        }

        long now = _timeProvider.GetTimestamp();
        if (!_stall.Elapsed(now, out bool expired))
        {
            // First empty iteration only records the window, exactly as before.
            return SessionRunStep.Stalled;
        }

        return expired
            ? SessionRunStep.Stop(SessionTerminalEvent.Timeout)
            : SessionRunStep.Stalled;
    }

    public readonly void ReportProgress(TimeSpan elapsed) =>
        _progress?.Report(new ConsumerProgress(_received, _receivedBytes, elapsed));

    public readonly SessionStopInput BuildStopInput(in SessionRunStep terminal) =>
        new()
        {
            Event = terminal.Terminal,
            Role = SessionRole.Consumer,
            Delivered = _received,
            Requested = _options.Count,
            Timeout = _options.IdleTimeout,
            PeerState = _observedPeer,
            Detail = terminal.Terminal == SessionTerminalEvent.VerificationFailed ? _failure : null,
        };

    /// <summary>
    /// Reads and verifies one message. Returns 1 when a message was consumed,
    /// 0 when the buffer is empty, and -1 when verification failed (the reason
    /// is stored in <see cref="_failure"/>).
    /// </summary>
    private int ProcessOne()
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
            string? failure = Verify(payload, length, type);
            if (failure is not null)
            {
                _failure = failure;

                // Consume the invalid message (TryPeek does not advance head) so the
                // session stops instead of re-reading it.
                _buffer.AdvanceRead();
                if (_options.WaitMode == SessionWaitMode.Notification)
                {
                    Interlocked.MemoryBarrier();
                    SessionRuntime.SignalIfPeerWaiting(
                        _buffer, _options.WaitMode, _options.Notification, signalData: false);
                }

                return -1;
            }
        }

        // Latency sampling is independent of verification: the producer session
        // always stamps sequence and timestamp, so the sample is valid even when
        // the caller disabled structural checks.
        if (_options.IncludeSessionHeader && length >= RingBufferMessage.HeaderSize)
        {
            long sentTimestamp = RingBufferMessage.ReadTimestamp(payload);
            if (sentTimestamp > 0)
            {
                Histogram.Record(now - sentTimestamp);
            }
        }

        _buffer.AdvanceRead();

        // Full fence between our release store (head) and the peer's
        // waiting-flag load: see ProducerSession for the missed-wakeup race.
        // Spin modes never set the flag, so they skip the fence entirely.
        if (_options.WaitMode == SessionWaitMode.Notification)
        {
            Interlocked.MemoryBarrier();
            SessionRuntime.SignalIfPeerWaiting(
                _buffer, _options.WaitMode, _options.Notification, signalData: false);
        }

        _expected++;
        _received++;
        _receivedBytes += length;
        if (_firstMessageTimestamp < 0)
        {
            _firstMessageTimestamp = now;
        }

        _lastMessageTimestamp = now;
        return 1;
    }

    /// <summary>Returns a failure description, or null when the message is valid.</summary>
    private readonly string? Verify(ReadOnlySpan<byte> payload, int length, int type)
    {
        if (_options.IncludeSessionHeader)
        {
            if (length < RingBufferMessage.HeaderSize)
            {
                return $"message {_received} is only {length} bytes.";
            }

            long sequence = RingBufferMessage.ReadSequence(payload);
            if (sequence != _expected)
            {
                return $"sequence mismatch at message {_received}: expected {_expected}, received {sequence}.";
            }
        }

        if (type != _options.ExpectedType)
        {
            return $"message {_received} has type {type}, expected {_options.ExpectedType}.";
        }

        if (_options.IncludeSessionHeader
            && _options.VerifyPayload
            && !RingBufferMessage.IsPayloadIntact(payload, length))
        {
            return $"message {_received} payload is corrupted.";
        }

        return null;
    }

    /// <summary>
    /// Drains messages published before the producer went away. Seeing a
    /// terminal producer state has acquire semantics, so the drain cannot miss a
    /// message. Cancellation is checked only after a successful message, and a
    /// verification failure outranks it, exactly as the old inline drain did.
    /// </summary>
    private SessionRunStep Drain(CancellationToken cancellationToken)
    {
        while (true)
        {
            int result = ProcessOne();
            if (result == 0)
            {
                // Drain clean: the table turns a met count into Completed and a
                // shortfall into PeerStopped.
                return SessionRunStep.Stop(SessionTerminalEvent.PeerStopped);
            }

            if (result == -1)
            {
                return SessionRunStep.Stop(SessionTerminalEvent.VerificationFailed);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return SessionRunStep.Stop(SessionTerminalEvent.Cancelled);
            }
        }
    }
}
