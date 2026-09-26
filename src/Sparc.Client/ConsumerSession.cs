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

    private readonly SharedRingBuffer _buffer;
    private readonly ConsumerSessionOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public ConsumerSession(
        SharedRingBuffer buffer,
        ConsumerSessionOptions options,
        TimeProvider? timeProvider = null,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentNullException.ThrowIfNull(options);

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
            return new ConsumerRunResult(
                0, 0, TimeSpan.Zero, SessionStopReason.Cancelled, new LatencyHistogram(), null);
        }

        byte[] destination = new byte[_buffer.MaxPayloadSize];
        LatencyHistogram histogram = new();
        SpinWait spin = new();
        long received = 0;
        long receivedBytes = 0;
        long expected = 0;
        long idleSince = 0;
        long cancellationCounter = 0;
        long idleTimeoutTicks = (long)(_options.IdleTimeout.TotalSeconds * _timeProvider.TimestampFrequency);
        long progressInterval = _options.Count > 0 ? Math.Max(1, _options.Count / ProgressReports) : 0;
        long firstMessageTimestamp = 0;
        long lastMessageTimestamp = 0;
        long runStartTimestamp = _timeProvider.GetTimestamp();
        SessionStopReason reason = SessionStopReason.Completed;
        string? failure = null;

        // 0 = empty, 1 = received, -1 = verification failure (details in `failure`).
        int ProcessOne()
        {
            if (!_buffer.TryRead(destination, out int length, out int type))
            {
                return 0;
            }

            long now = _timeProvider.GetTimestamp();

            if (_options.Verify)
            {
                if (length < RingBufferMessage.HeaderSize)
                {
                    failure = $"message {received} is only {length} bytes.";
                    return -1;
                }

                long sequence = RingBufferMessage.ReadSequence(destination);
                if (sequence != expected)
                {
                    failure = $"sequence mismatch at message {received}: expected {expected}, received {sequence}.";
                    return -1;
                }

                if (type != _options.ExpectedType)
                {
                    failure = $"message {received} has type {type}, expected {_options.ExpectedType}.";
                    return -1;
                }

                if (!RingBufferMessage.IsPayloadIntact(destination, length))
                {
                    failure = $"message {received} payload is corrupted.";
                    return -1;
                }

                long sentTimestamp = RingBufferMessage.ReadTimestamp(destination);
                if (sentTimestamp > 0)
                {
                    histogram.Record(now - sentTimestamp);
                }
            }

            expected++;
            received++;
            receivedBytes += length;
            if (firstMessageTimestamp == 0)
            {
                firstMessageTimestamp = now;
            }

            lastMessageTimestamp = now;
            return 1;
        }

        while (_options.Count == 0 || received < _options.Count)
        {
            int result = ProcessOne();
            if (result == 1)
            {
                spin.Reset();
                idleSince = 0;

                if (progress is not null && progressInterval > 0 && received % progressInterval == 0)
                {
                    progress.Report(new ConsumerProgress(received, receivedBytes, _timeProvider.GetElapsedTime(runStartTimestamp)));
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
                // Seeing a terminal state has acquire semantics, so any message the
                // producer published before going away is now visible. Drain before
                // declaring the stream finished.
                int drain;
                while ((drain = ProcessOne()) == 1)
                {
                }

                if (drain == -1)
                {
                    reason = SessionStopReason.VerificationFailed;
                }
                else if (_options.Count > 0 && received < _options.Count)
                {
                    reason = SessionStopReason.PeerStopped;
                    failure = $"producer stopped after {received} of {_options.Count} messages.";
                }

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
                failure =
                    $"no messages for {_options.IdleTimeout.TotalSeconds:F1}s " +
                    $"(producer state={producer}, received={received}).";
                break;
            }

            spin.SpinOnce();
        }

        TimeSpan elapsed = received > 1 && firstMessageTimestamp != 0
            ? _timeProvider.GetElapsedTime(firstMessageTimestamp, lastMessageTimestamp)
            : _timeProvider.GetElapsedTime(runStartTimestamp);

        _logger.LogDebug(
            "Consumer session finished: received={Received} reason={Reason} elapsed={Elapsed}",
            received, reason, elapsed);

        return new ConsumerRunResult(received, receivedBytes, elapsed, reason, histogram, failure);
    }
}
