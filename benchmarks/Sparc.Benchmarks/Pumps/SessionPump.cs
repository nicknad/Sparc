using System.Diagnostics;
using Sparc.Client;
using Sparc.Core;
using Sparc.UnixMemoryMapped;
using Sparc.WindowsMemoryMapped;

namespace Sparc.Benchmarks.Pumps;

/// <summary>
/// Runs one <see cref="ProducerSession"/> and one <see cref="ConsumerSession"/>
/// over one shared region and times the pair end to end.
/// </summary>
/// <remarks>
/// <para>
/// This is the baseline for the session layer: the pump scenarios time the
/// endpoints' copy or lease protocol, while this one also pays the session
/// protocol on every message (header stamp, payload fill, sequence and fill
/// verification, latency sampling, progress and cancellation cadence).
/// </para>
/// <para>
/// Endpoints live for the whole harness run so region creation stays outside the
/// measured window, but a fresh session pair is created per run because a
/// session is a one-shot driver. Both sides run for a fixed count, so a run does
/// not depend on the peer's stop state and a StopReason is a failure here.
/// </para>
/// </remarks>
public sealed class SessionPump : IScenarioPump
{
    private readonly IProducerEndpoint _producer;
    private readonly IConsumerEndpoint _consumer;
    private readonly Thread _producerThread;
    private readonly Thread _consumerThread;
    private readonly ManualResetEventSlim _go = new(false);
    private readonly ManualResetEventSlim _producerFinished = new(false);
    private readonly ManualResetEventSlim _consumerFinished = new(false);

    private volatile bool _shutdown;
    private string? _producerFailure; // written only by the producer thread
    private string? _consumerFailure; // written only by the consumer thread
    private long _messageCount;
    private int _payloadSize;

    public SessionPump(int payloadSize)
    {
        string name = "spsc-session-" + Guid.NewGuid().ToString("N");
        int slotSize = SpscArrayPump.SlotSizeFor(payloadSize);
        IIpcMemoryRegionFactory factory = OperatingSystem.IsWindows()
            ? new WindowsNamedMemoryMappedRegionFactory()
            : new UnixFileMemoryMappedRegionFactory();

        _producer = SparcRing.OpenProducer(factory, name, RingBufferLayout.DefaultCapacity, slotSize);
        _consumer = SparcRing.OpenConsumer(factory, name, RingBufferLayout.DefaultCapacity, slotSize);

        _producerThread = new Thread(ProducerLoop) { IsBackground = true, Name = "session-producer" };
        _consumerThread = new Thread(ConsumerLoop) { IsBackground = true, Name = "session-consumer" };
        _producerThread.Start();
        _consumerThread.Start();
    }

    /// <inheritdoc />
    public double Run(long count, int payloadSize)
    {
        _messageCount = count;
        _payloadSize = payloadSize;
        _producerFailure = null;
        _consumerFailure = null;
        _producerFinished.Reset();
        _consumerFinished.Reset();

        long started = Stopwatch.GetTimestamp();
        _go.Set();
        _producerFinished.Wait();
        _consumerFinished.Wait();
        double seconds = Stopwatch.GetElapsedTime(started).TotalSeconds;

        // Both threads are past their session by now (the finished events are
        // set after the run returns), so re-arming the gate cannot race a wakeup.
        _go.Reset();

        if ((_producerFailure ?? _consumerFailure) is { } failure)
        {
            throw new InvalidOperationException($"session pump failed: {failure}");
        }

        return seconds;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _shutdown = true;
        _go.Set();

        _producerThread.Join(TimeSpan.FromSeconds(5));
        _consumerThread.Join(TimeSpan.FromSeconds(5));

        _producer.Dispose();
        _consumer.Dispose();
        _go.Dispose();
        _producerFinished.Dispose();
        _consumerFinished.Dispose();
        GC.SuppressFinalize(this);
    }

    private void ProducerLoop()
    {
        while (true)
        {
            _go.Wait();
            if (_shutdown)
            {
                return;
            }

            try
            {
                ProducerRunResult result = new ProducerSession(_producer, new ProducerSessionOptions
                {
                    Count = _messageCount,
                    PayloadSize = _payloadSize,
                    FullTimeout = TimeSpan.FromSeconds(30),
                }).Run();

                _producerFailure = result.FailureMessage;
            }
            catch (Exception exception)
            {
                _producerFailure = exception.Message;
            }
            finally
            {
                _producerFinished.Set();
            }
        }
    }

    private void ConsumerLoop()
    {
        while (true)
        {
            _go.Wait();
            if (_shutdown)
            {
                return;
            }

            try
            {
                ConsumerRunResult result = new ConsumerSession(_consumer, new ConsumerSessionOptions
                {
                    Count = _messageCount,
                    IdleTimeout = TimeSpan.FromSeconds(30),
                }).Run();

                _consumerFailure = result.FailureMessage;
            }
            catch (Exception exception)
            {
                _consumerFailure = exception.Message;
            }
            finally
            {
                _consumerFinished.Set();
            }
        }
    }
}
