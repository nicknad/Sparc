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

    private readonly IRingBufferEndpoint _buffer;
    private readonly ProducerSessionOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public ProducerSession(
        IRingBufferEndpoint buffer,
        ProducerSessionOptions options,
        TimeProvider? timeProvider = null,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentNullException.ThrowIfNull(options);
        ProducerSessionOptions.Validate(
            options.Count, options.PayloadSize, options.PerMessageDelay, buffer.MaxPayloadSize);

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
            _buffer.Connect(RingBufferEndpointRole.Producer, _options.Takeover, cancellationToken);
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
        long pacingTicks = MessagePacer.TicksFor(_timeProvider, _options.PerMessageDelay);
        long progressInterval = Math.Max(1, _options.Count / ProgressReports);
        SpinWait spin = new();
        long produced = 0;
        long fullSince = 0;
        long cancellationCounter = 0;
        SessionStopReason reason = SessionStopReason.Completed;
        string? failure = null;
        long startTimestamp = _timeProvider.GetTimestamp();
        long firstPublishTimestamp = 0;

        while (produced < _options.Count)
        {
            // Zero copy: stamp the protocol header and fill inside the shared
            // slot itself, then publish with one release store.
            if (_buffer.TryReserveWrite(_options.MessageType, _options.PayloadSize, out Span<byte> slot))
            {
                RingBufferMessage.Write(slot, produced, _timeProvider.GetTimestamp());
                RingBufferMessage.FillPayload(slot);
                _buffer.CommitWrite();

                if (firstPublishTimestamp == 0)
                {
                    firstPublishTimestamp = _timeProvider.GetTimestamp();
                }

                produced++;
                fullSince = 0;
                spin.Reset();
                MessagePacer.Wait(_timeProvider, startTimestamp, produced, pacingTicks);

                if (progress is not null && produced % progressInterval == 0)
                {
                    progress.Report(new ProducerProgress(produced, _timeProvider.GetElapsedTime(startTimestamp)));
                }

                if ((++cancellationCounter & CancellationCheckMask) == 0 && cancellationToken.IsCancellationRequested)
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
            else if (now - fullSince >= fullTimeoutTicks)
            {
                RingBufferEndpointState peer = _buffer.ConsumerState;
                if (peer is RingBufferEndpointState.Stopped or RingBufferEndpointState.Faulted)
                {
                    reason = SessionStopReason.PeerStopped;
                    failure = $"consumer is gone (state={peer}) with {_options.Count - produced} messages unsent.";
                }
                else
                {
                    reason = SessionStopReason.Timeout;
                    failure =
                        $"buffer stayed full for {_options.FullTimeout.TotalSeconds:F1}s " +
                        $"({_options.Count - produced} messages unsent, consumer state={peer}).";
                }

                break;
            }

            WaitWhileFull(ref spin);
        }

        TimeSpan elapsed = _timeProvider.GetElapsedTime(startTimestamp);
        TimeSpan activeElapsed = firstPublishTimestamp == 0
            ? TimeSpan.Zero
            : _timeProvider.GetElapsedTime(firstPublishTimestamp);
        _logger.LogDebug(
            "Producer session finished: produced={Produced} reason={Reason} elapsed={Elapsed} activeElapsed={ActiveElapsed}",
            produced, reason, elapsed, activeElapsed);

        return new ProducerRunResult(
            produced,
            _options.PayloadSize,
            elapsed,
            reason,
            _options.Count - produced,
            _buffer.ConsumerState,
            failure)
        {
            ActiveElapsed = activeElapsed,
        };
    }

    private void WaitWhileFull(ref SpinWait spin)
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
}
