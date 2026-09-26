using System.Buffers.Binary;
using System.Diagnostics;
using RingBuffer.Core.Diagnostics;

namespace RingBuffer.Benchmarks;

/// <summary>
/// Base class for the throughput/latency baselines. Two long-lived threads own
/// one endpoint each; the benchmark thread only starts and joins a run, so
/// thread creation is not part of the measurement.
/// </summary>
public abstract class TwoThreadPump : IDisposable
{
    protected const int Capacity = 1024;

    private readonly Thread _producerThread;
    private readonly Thread _consumerThread;
    private readonly ManualResetEventSlim _producerGo = new(false);
    private readonly ManualResetEventSlim _consumerGo = new(false);
    private readonly ManualResetEventSlim _producerDone = new(false);
    private readonly ManualResetEventSlim _consumerDone = new(false);

    private volatile bool _shutdown;
    private volatile bool _latencyMode;
    private long _messageCount;
    private int _payloadSize;
    private byte[] _payload = [];
    private LatencyHistogram? _histogram;

    protected TwoThreadPump()
    {
        _producerThread = new Thread(ProducerLoop) { IsBackground = true, Name = "pump-producer" };
        _consumerThread = new Thread(ConsumerLoop) { IsBackground = true, Name = "pump-consumer" };
        _producerThread.Start();
        _consumerThread.Start();
    }

    /// <summary>Attempts to publish one message; false means the transport is full.</summary>
    public abstract bool TryPublish(ReadOnlySpan<byte> payload);

    /// <summary>Attempts to receive one message into <paramref name="destination"/>.</summary>
    public abstract bool TryConsume(Span<byte> destination);

    /// <summary>
    /// Size of the consumer's receive buffer. Ring buffers require a buffer that
    /// can hold any slot payload (the length is only known after the read has
    /// been committed to); stream transports only need the message size.
    /// </summary>
    public virtual int RequiredDestinationSize(int payloadSize) => payloadSize;

    /// <summary>Transfers <paramref name="count"/> messages and returns elapsed seconds.</summary>
    public double Run(long count, int payloadSize)
    {
        Prepare(count, payloadSize);
        _latencyMode = false;

        Stopwatch stopwatch = Stopwatch.StartNew();
        _producerGo.Set();
        _consumerGo.Set();
        _producerDone.Wait();
        _consumerDone.Wait();
        stopwatch.Stop();

        return stopwatch.Elapsed.TotalSeconds;
    }

    /// <summary>
    /// Transfers <paramref name="count"/> timestamped messages and returns the
    /// one-way latency histogram (samples collected by the consumer thread).
    /// </summary>
    public LatencyHistogram RunLatency(long count, int payloadSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(payloadSize, sizeof(long));

        LatencyHistogram histogram = new();
        Prepare(count, payloadSize);
        _histogram = histogram;
        _latencyMode = true;

        _producerGo.Set();
        _consumerGo.Set();
        _producerDone.Wait();
        _consumerDone.Wait();

        return histogram;
    }

    public virtual void Dispose()
    {
        _shutdown = true;
        _producerGo.Set();
        _consumerGo.Set();

        _producerThread.Join(TimeSpan.FromSeconds(5));
        _consumerThread.Join(TimeSpan.FromSeconds(5));

        _producerGo.Dispose();
        _consumerGo.Dispose();
        _producerDone.Dispose();
        _consumerDone.Dispose();
        GC.SuppressFinalize(this);
    }

    private void Prepare(long count, int payloadSize)
    {
        _messageCount = count;
        _payloadSize = payloadSize;
        _payload = new byte[payloadSize];
        _payload.AsSpan().Fill(0xA5);
        _producerDone.Reset();
        _consumerDone.Reset();
    }

    private void ProducerLoop()
    {
        while (true)
        {
            _producerGo.Wait();
            if (_shutdown)
            {
                return;
            }

            _producerGo.Reset();

            try
            {
                byte[] payload = _payload;
                SpinWait spin = new();

                for (long i = 0; i < _messageCount; i++)
                {
                    if (_latencyMode)
                    {
                        BinaryPrimitives.WriteInt64LittleEndian(payload, Stopwatch.GetTimestamp());
                    }

                    while (!TryPublish(payload))
                    {
                        spin.SpinOnce();
                    }

                    spin.Reset();
                }
            }
            finally
            {
                _producerDone.Set();
            }
        }
    }

    private void ConsumerLoop()
    {
        while (true)
        {
            _consumerGo.Wait();
            if (_shutdown)
            {
                return;
            }

            _consumerGo.Reset();

            try
            {
                byte[] destination = new byte[RequiredDestinationSize(_payloadSize)];
                SpinWait spin = new();
                LatencyHistogram? histogram = _histogram;
                bool measure = _latencyMode;

                for (long i = 0; i < _messageCount; i++)
                {
                    while (!TryConsume(destination))
                    {
                        spin.SpinOnce();
                    }

                    spin.Reset();

                    if (measure && histogram is not null)
                    {
                        long sent = BinaryPrimitives.ReadInt64LittleEndian(destination);
                        histogram.Record(Stopwatch.GetTimestamp() - sent);
                    }
                }
            }
            finally
            {
                _consumerDone.Set();
            }
        }
    }
}
