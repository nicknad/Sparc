using System.Threading.Channels;
using Sparc.Client;

namespace Sparc.WebApp;

/// <summary>
/// Decouples HTTP requests from the blocking SPARC sessions: the API enqueues a
/// transfer request, the hosted service picks it up, runs the synchronous
/// sessions on thread-pool threads and publishes a status snapshot.
/// </summary>
public sealed class TransferCoordinator
{
    private readonly Channel<int> _requests = Channel.CreateUnbounded<int>();
    private readonly TimeProvider _timeProvider;
    private readonly Lock _gate = new();

    private string _phase = "idle";
    private ProducerRunResult? _producer;
    private ConsumerRunResult? _consumer;

    public TransferCoordinator(TimeProvider timeProvider) => _timeProvider = timeProvider;

    /// <summary>Latest status snapshot; safe to call while a transfer is running.</summary>
    public TransferStatus GetStatus()
    {
        lock (_gate)
        {
            return new TransferStatus(
                _phase,
                _producer is { } producer ? ToStatus(producer) : null,
                _consumer is { } consumer ? ToStatus(consumer) : null);
        }
    }

    /// <summary>Queues one transfer; rejected while another one is running.</summary>
    public bool TryRequestTransfer(int count)
    {
        lock (_gate)
        {
            if (string.Equals(_phase, "running", StringComparison.Ordinal))
            {
                return false;
            }

            _phase = "running";
            _producer = null;
            _consumer = null;
        }

        return _requests.Writer.TryWrite(count);
    }

    public ValueTask<int> WaitForRequestAsync(CancellationToken cancellationToken) =>
        _requests.Reader.ReadAsync(cancellationToken);

    public void Complete(ProducerRunResult producer, ConsumerRunResult consumer)
    {
        lock (_gate)
        {
            _producer = producer;
            _consumer = consumer;
            _phase = "completed";
        }
    }

    private ProducerStatus ToStatus(ProducerRunResult result) => new(
        result.Produced,
        result.Unsent,
        result.Reason.ToString(),
        result.FailureMessage,
        Math.Round(result.Elapsed.TotalSeconds, 3),
        result.Elapsed.TotalSeconds > 0 ? Math.Round(result.Produced / result.Elapsed.TotalSeconds) : 0);

    private ConsumerStatus ToStatus(ConsumerRunResult result) => new(
        result.Received,
        result.ReceivedBytes,
        result.Reason.ToString(),
        result.FailureMessage,
        Math.Round(result.Elapsed.TotalSeconds, 3),
        result.Latency.ToMicrosecondsReport(_timeProvider.TimestampFrequency));
}

/// <summary>Point-in-time view returned by <c>GET /</c>.</summary>
public sealed record TransferStatus(string Phase, ProducerStatus? Producer, ConsumerStatus? Consumer);

/// <summary>Producer half of a completed transfer.</summary>
public sealed record ProducerStatus(
    long Produced,
    long Unsent,
    string Reason,
    string? Failure,
    double ElapsedSeconds,
    double MessagesPerSecond);

/// <summary>Consumer half of a completed transfer, including the latency report.</summary>
public sealed record ConsumerStatus(
    long Received,
    long ReceivedBytes,
    string Reason,
    string? Failure,
    double ElapsedSeconds,
    string Latency);
