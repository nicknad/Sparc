using Sparc.Core;

namespace Sparc.Client;

/// <summary>
/// The producer's half of a session run: reserves, stamps, fills and commits one
/// slot per step, and runs the producer stall order — cancel, stamp, peer,
/// timeout — inside <see cref="Step"/>. The notification fence stays adjacent
/// to the release store, exactly where it was before the engine existed.
/// </summary>
internal struct ProducerRole : ISessionRunRole
{
    private readonly IProducerEndpoint _buffer;
    private readonly ProducerSessionOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly IProgress<ProducerProgress>? _progress;
    private SessionStallClock _stall;
    private long _produced;
    private long _firstPublishTimestamp;
    private RingBufferEndpointState _observedPeer;

    internal ProducerRole(
        IProducerEndpoint buffer,
        ProducerSessionOptions options,
        TimeProvider timeProvider,
        IProgress<ProducerProgress>? progress)
    {
        _buffer = buffer;
        _options = options;
        _timeProvider = timeProvider;
        _progress = progress;
        _stall = new SessionStallClock(
            options.FullTimeout != Timeout.InfiniteTimeSpan,
            (long)(options.FullTimeout.TotalSeconds * timeProvider.TimestampFrequency));
        _produced = 0;
        _firstPublishTimestamp = -1;
        _observedPeer = default;
    }

    public readonly long Delivered => _produced;

    public readonly long FirstTimestamp => _firstPublishTimestamp;

    public readonly long LastTimestamp => -1;

    public bool TryConnect(CancellationToken cancellationToken)
    {
        try
        {
            _buffer.Connect(_options.Takeover, cancellationToken);
            return true;
        }
        catch (OperationCanceledException)
        {
            // The role was not claimed; the engine shapes the cancelled result.
            return false;
        }
    }

    public SessionRunStep Step(CancellationToken cancellationToken)
    {
        // Zero copy: stamp the protocol header and fill inside the shared slot
        // itself, then publish with one release store.
        if (_buffer.TryReserveWrite(_options.MessageType, _options.PayloadSize, out Span<byte> slot))
        {
            WritePayload(slot, _produced);
            _buffer.CommitWrite();

            // Notification mode needs a full fence between our release store
            // (tail, in CommitWrite) and the peer's waiting-flag load: without it
            // a StoreLoad reorder on weak memory models lets both sides miss each
            // other and the consumer sleeps a full wait slice. Spin modes never
            // set the flag, so they skip the fence entirely.
            if (_options.WaitMode == SessionWaitMode.Notification)
            {
                Interlocked.MemoryBarrier();
                SessionRuntime.SignalIfPeerWaiting(
                    _buffer, _options.WaitMode, _options.Notification, signalData: true);
            }

            if (_firstPublishTimestamp < 0)
            {
                _firstPublishTimestamp = _timeProvider.GetTimestamp();
            }

            _produced++;
            _stall.Reset();
            return SessionRunStep.Progressed;
        }

        // Producer stall order, literal: cancel -> stamp -> peer -> timeout.
        if (cancellationToken.IsCancellationRequested)
        {
            return SessionRunStep.Stop(SessionTerminalEvent.Cancelled);
        }

        long now = _timeProvider.GetTimestamp();
        if (!_stall.Elapsed(now, out bool expired))
        {
            // First full iteration only records the window, exactly as before.
            return SessionRunStep.Stalled;
        }

        // Check the peer on every later full iteration (a single volatile read):
        // a stopped consumer should not cost a whole FullTimeout.
        RingBufferEndpointState peer = _buffer.ConsumerState;
        _observedPeer = peer;
        if (peer is RingBufferEndpointState.Stopped or RingBufferEndpointState.Faulted)
        {
            return SessionRunStep.Stop(SessionTerminalEvent.PeerStopped);
        }

        return expired
            ? SessionRunStep.Stop(SessionTerminalEvent.Timeout)
            : SessionRunStep.Stalled;
    }

    public readonly void ReportProgress(TimeSpan elapsed) =>
        _progress?.Report(new ProducerProgress(_produced, elapsed));

    public readonly SessionStopInput BuildStopInput(in SessionRunStep terminal) =>
        new()
        {
            Event = terminal.Terminal,
            Role = SessionRole.Producer,
            Delivered = _produced,
            Requested = _options.Count,
            Timeout = _options.FullTimeout,
            PeerState = _observedPeer,
        };

    /// <summary>
    /// Writes one payload. The default path stamps the session protocol; a
    /// custom <see cref="ProducerSessionOptions.PayloadWriter"/> writes either
    /// behind the header or instead of it.
    /// </summary>
    private readonly void WritePayload(Span<byte> slot, long index)
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
