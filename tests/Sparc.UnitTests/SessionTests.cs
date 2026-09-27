using System.Diagnostics;
using Sparc.Client;
using Sparc.Core;
using Sparc.InMemory;

namespace Sparc.UnitTests;

public class SessionTests
{
    private static string NewName() => "spsc-session-" + Guid.NewGuid().ToString("N");

    [Fact]
    public async Task ProducerAndConsumerCompleteTogether()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 128, 64);
        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 128, 64);

        ConsumerSession consumer = new(consumerBuffer, new ConsumerSessionOptions
        {
            Count = 10_000,
            IdleTimeout = TimeSpan.FromSeconds(10),
        });

        Task<ConsumerRunResult> consumerTask = Task.Run(
            () => consumer.Run(TestContext.Current.CancellationToken),
            TestContext.Current.CancellationToken);

        ProducerRunResult producerResult = new ProducerSession(producerBuffer, new ProducerSessionOptions
        {
            Count = 10_000,
            PayloadSize = 32,
            FullTimeout = TimeSpan.FromSeconds(10),
        }).Run(TestContext.Current.CancellationToken);

        ConsumerRunResult consumerResult = await consumerTask;

        Assert.Equal(SessionStopReason.Completed, producerResult.Reason);
        Assert.Equal(10_000, producerResult.Produced);
        Assert.Equal(0, producerResult.Unsent);
        Assert.Null(producerResult.FailureMessage);
        Assert.True(producerResult.ActiveElapsed > TimeSpan.Zero);
        Assert.True(producerResult.ActiveElapsed <= producerResult.Elapsed);

        Assert.Equal(SessionStopReason.Completed, consumerResult.Reason);
        Assert.Equal(10_000, consumerResult.Received);
        Assert.Equal(10_000 * 32, consumerResult.ReceivedBytes);
        Assert.Null(consumerResult.FailureMessage);
        Assert.Equal(10_000, consumerResult.Latency.Count);
        Assert.True(consumerResult.RunElapsed >= consumerResult.Elapsed);
    }

    [Fact]
    public void ProducerTimesOutWhenBufferStaysFull()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer buffer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);

        ProducerRunResult result = new ProducerSession(buffer, new ProducerSessionOptions
        {
            Count = 100,
            PayloadSize = 32,
            FullTimeout = TimeSpan.FromMilliseconds(100),
        }).Run(TestContext.Current.CancellationToken);

        Assert.Equal(SessionStopReason.Timeout, result.Reason);
        Assert.Equal(4, result.Produced);
        Assert.Equal(96, result.Unsent);
        Assert.Equal(RingBufferEndpointState.NotPresent, result.PeerState);
        Assert.NotNull(result.FailureMessage);
    }

    [Fact]
    public void ProducerDetectsStoppedConsumer()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);

        SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        consumerBuffer.Connect(RingBufferEndpointRole.Consumer, cancellationToken: TestContext.Current.CancellationToken);
        consumerBuffer.Dispose();

        ProducerRunResult result = new ProducerSession(producerBuffer, new ProducerSessionOptions
        {
            Count = 100,
            PayloadSize = 32,
            FullTimeout = TimeSpan.FromMilliseconds(100),
        }).Run(TestContext.Current.CancellationToken);

        Assert.Equal(SessionStopReason.PeerStopped, result.Reason);
        Assert.Equal(RingBufferEndpointState.Stopped, result.PeerState);
        Assert.Contains("consumer is gone", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ConsumerReportsVerificationFailure()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        producerBuffer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);

        byte[] payload = new byte[32];
        RingBufferMessage.Write(payload, sequence: 5, Stopwatch.GetTimestamp());
        RingBufferMessage.FillPayload(payload);
        Assert.True(producerBuffer.TryWrite(1, payload));

        ConsumerRunResult result = new ConsumerSession(consumerBuffer, new ConsumerSessionOptions
        {
            IdleTimeout = TimeSpan.FromMilliseconds(200),
        }).Run(TestContext.Current.CancellationToken);

        Assert.Equal(SessionStopReason.VerificationFailed, result.Reason);
        Assert.Contains("sequence mismatch", result.FailureMessage, StringComparison.Ordinal);
        Assert.Equal(0, result.Received);
    }

    [Fact]
    public void ConsumerReportsCorruptedPayloadByDefault()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        producerBuffer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(producerBuffer.TryWrite(1, CorruptedPayload()));

        ConsumerRunResult result = new ConsumerSession(consumerBuffer, new ConsumerSessionOptions
        {
            Count = 1,
            IdleTimeout = TimeSpan.FromMilliseconds(200),
        }).Run(TestContext.Current.CancellationToken);

        Assert.Equal(SessionStopReason.VerificationFailed, result.Reason);
        Assert.Contains("payload is corrupted", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ConsumerCanSkipThePayloadScan()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        producerBuffer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(producerBuffer.TryWrite(1, CorruptedPayload()));

        ConsumerRunResult result = new ConsumerSession(consumerBuffer, new ConsumerSessionOptions
        {
            Count = 1,
            VerifyPayload = false,
            IdleTimeout = TimeSpan.FromMilliseconds(200),
        }).Run(TestContext.Current.CancellationToken);

        Assert.Equal(SessionStopReason.Completed, result.Reason);
        Assert.Equal(1, result.Received);
        Assert.Equal(1, result.Latency.Count);
    }

    [Fact]
    public void ConsumerSamplesLatencyWithoutVerification()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        producerBuffer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(producerBuffer.TryWrite(1, CorruptedPayload()));

        ConsumerRunResult result = new ConsumerSession(consumerBuffer, new ConsumerSessionOptions
        {
            Count = 1,
            Verify = false,
            IdleTimeout = TimeSpan.FromMilliseconds(200),
        }).Run(TestContext.Current.CancellationToken);

        Assert.Equal(SessionStopReason.Completed, result.Reason);
        Assert.Equal(1, result.Received);
        Assert.Equal(1, result.Latency.Count);
    }

    [Fact]
    public void ConsumerDrainsUntilProducerStops()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 16, 64);

        producerBuffer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);
        byte[] payload = new byte[24];
        RingBufferMessage.FillPayload(payload);
        for (int sequence = 0; sequence < 5; sequence++)
        {
            RingBufferMessage.Write(payload, sequence, Stopwatch.GetTimestamp());
            Assert.True(producerBuffer.TryWrite(1, payload));
        }

        producerBuffer.Dispose();

        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 16, 64);
        ConsumerRunResult result = new ConsumerSession(consumerBuffer, new ConsumerSessionOptions
        {
            Count = 0,
            IdleTimeout = TimeSpan.FromSeconds(5),
        }).Run(TestContext.Current.CancellationToken);

        Assert.Equal(SessionStopReason.Completed, result.Reason);
        Assert.Equal(5, result.Received);
        Assert.Null(result.FailureMessage);
    }

    [Fact]
    public void ConsumerTimesOutWhenProducerProducesNothing()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 16, 64);
        producerBuffer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);

        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 16, 64);
        ConsumerRunResult result = new ConsumerSession(consumerBuffer, new ConsumerSessionOptions
        {
            Count = 0,
            IdleTimeout = TimeSpan.FromMilliseconds(150),
        }).Run(TestContext.Current.CancellationToken);

        Assert.Equal(SessionStopReason.Timeout, result.Reason);
        Assert.Equal(0, result.Received);
        Assert.Contains("no messages for", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PacedProducerAndConsumerStillComplete()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 64, 64);
        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 64, 64);

        ConsumerSession consumer = new(consumerBuffer, new ConsumerSessionOptions
        {
            Count = 100,
            IdleTimeout = TimeSpan.FromSeconds(10),
        });

        Task<ConsumerRunResult> consumerTask = Task.Run(
            () => consumer.Run(TestContext.Current.CancellationToken),
            TestContext.Current.CancellationToken);

        ProducerRunResult producerResult = new ProducerSession(producerBuffer, new ProducerSessionOptions
        {
            Count = 100,
            PayloadSize = 32,
            PerMessageDelay = TimeSpan.FromMilliseconds(1),
            FullTimeout = TimeSpan.FromSeconds(10),
        }).Run(TestContext.Current.CancellationToken);

        ConsumerRunResult consumerResult = await consumerTask;

        Assert.Equal(SessionStopReason.Completed, producerResult.Reason);
        Assert.Equal(100, producerResult.Produced);
        Assert.Equal(SessionStopReason.Completed, consumerResult.Reason);
        Assert.Equal(100, consumerResult.Received);
    }

    [Fact]
    public async Task SpinOnlySessionsComplete()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 128, 64);
        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 128, 64);

        ConsumerSession consumer = new(consumerBuffer, new ConsumerSessionOptions
        {
            Count = 5_000,
            IdleTimeout = TimeSpan.FromSeconds(10),
            WaitMode = SessionWaitMode.SpinOnly,
        });

        Task<ConsumerRunResult> consumerTask = Task.Run(
            () => consumer.Run(TestContext.Current.CancellationToken),
            TestContext.Current.CancellationToken);

        ProducerRunResult producerResult = new ProducerSession(producerBuffer, new ProducerSessionOptions
        {
            Count = 5_000,
            PayloadSize = 32,
            FullTimeout = TimeSpan.FromSeconds(10),
            WaitMode = SessionWaitMode.SpinOnly,
        }).Run(TestContext.Current.CancellationToken);

        ConsumerRunResult consumerResult = await consumerTask;

        Assert.Equal(SessionStopReason.Completed, producerResult.Reason);
        Assert.Equal(5_000, producerResult.Produced);
        Assert.Equal(SessionStopReason.Completed, consumerResult.Reason);
        Assert.Equal(5_000, consumerResult.Received);
    }

    [Fact]
    public void SpinOnlyConsumerStillTimesOutWhenIdle()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 16, 64);
        producerBuffer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);

        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 16, 64);
        ConsumerRunResult result = new ConsumerSession(consumerBuffer, new ConsumerSessionOptions
        {
            Count = 0,
            IdleTimeout = TimeSpan.FromMilliseconds(150),
            WaitMode = SessionWaitMode.SpinOnly,
        }).Run(TestContext.Current.CancellationToken);

        Assert.Equal(SessionStopReason.Timeout, result.Reason);
        Assert.Equal(0, result.Received);
    }

    [Fact]
    public async Task NotificationSessionsComplete()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 128, 64);
        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 128, 64);
        using SessionNotification notification = SessionNotification.CreateInProcess();

        ConsumerSession consumer = new(consumerBuffer, new ConsumerSessionOptions
        {
            Count = 5_000,
            IdleTimeout = TimeSpan.FromSeconds(10),
            WaitMode = SessionWaitMode.Notification,
            Notification = notification,
        });

        Task<ConsumerRunResult> consumerTask = Task.Run(
            () => consumer.Run(TestContext.Current.CancellationToken),
            TestContext.Current.CancellationToken);

        ProducerRunResult producerResult = new ProducerSession(producerBuffer, new ProducerSessionOptions
        {
            Count = 5_000,
            PayloadSize = 32,
            FullTimeout = TimeSpan.FromSeconds(10),
            WaitMode = SessionWaitMode.Notification,
            Notification = notification,
        }).Run(TestContext.Current.CancellationToken);

        ConsumerRunResult consumerResult = await consumerTask;

        Assert.Equal(SessionStopReason.Completed, producerResult.Reason);
        Assert.Equal(5_000, producerResult.Produced);
        Assert.Equal(SessionStopReason.Completed, consumerResult.Reason);
        Assert.Equal(5_000, consumerResult.Received);
    }

    [Fact]
    public void NotificationModeRequiresNotification()
    {
        InMemoryMemoryRegionFactory factory = new();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, NewName(), 16, 64);
        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, NewName(), 16, 64);

        Assert.Throws<ArgumentException>(() => new ProducerSession(producerBuffer, new ProducerSessionOptions
        {
            PayloadSize = 32,
            WaitMode = SessionWaitMode.Notification,
        }));

        Assert.Throws<ArgumentException>(() => new ConsumerSession(consumerBuffer, new ConsumerSessionOptions
        {
            WaitMode = SessionWaitMode.Notification,
        }));
    }

    [Fact]
    public async Task NotificationRaisesThePeerLatchForAPacedProducer()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 16, 64);
        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 16, 64);

        CountingSignal data = new();
        CountingSignal space = new();
        using SessionNotification notification = new(data, space);

        ConsumerSession consumer = new(consumerBuffer, new ConsumerSessionOptions
        {
            Count = 5,
            IdleTimeout = TimeSpan.FromSeconds(10),
            WaitMode = SessionWaitMode.Notification,
            Notification = notification,
        });

        Task<ConsumerRunResult> consumerTask = Task.Run(
            () => consumer.Run(TestContext.Current.CancellationToken),
            TestContext.Current.CancellationToken);

        ProducerRunResult producerResult = new ProducerSession(producerBuffer, new ProducerSessionOptions
        {
            Count = 5,
            PayloadSize = 32,
            PerMessageDelay = TimeSpan.FromMilliseconds(20),
            FullTimeout = TimeSpan.FromSeconds(10),
            WaitMode = SessionWaitMode.Notification,
            Notification = notification,
        }).Run(TestContext.Current.CancellationToken);

        ConsumerRunResult consumerResult = await consumerTask;

        Assert.Equal(SessionStopReason.Completed, producerResult.Reason);
        Assert.Equal(SessionStopReason.Completed, consumerResult.Reason);
        Assert.Equal(5, consumerResult.Received);

        // The consumer is idle between the paced messages, so the producer must
        // have seen its waiting flag and raised the data latch.
        Assert.True(data.Signals >= 3, $"data latch was raised only {data.Signals} times");
    }

    [Fact]
    public void SessionsRejectNegativePacing()
    {
        InMemoryMemoryRegionFactory factory = new();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, NewName(), 16, 64);
        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, NewName(), 16, 64);

        Assert.Throws<ArgumentOutOfRangeException>(() => new ProducerSession(producerBuffer, new ProducerSessionOptions
        {
            PayloadSize = 32,
            PerMessageDelay = TimeSpan.FromMilliseconds(-1),
        }));

        Assert.Throws<ArgumentOutOfRangeException>(() => new ConsumerSession(consumerBuffer, new ConsumerSessionOptions
        {
            PerMessageDelay = TimeSpan.FromMilliseconds(-1),
        }));
    }

    [Fact]
    public void ConsumerHonoursCancellation()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 16, 64);
        producerBuffer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);

        using SharedRingBuffer consumerBuffer = SharedRingBuffer.OpenOrCreate(factory, name, 16, 64);
        using CancellationTokenSource cancellation = new();
        cancellation.Cancel();

        ConsumerRunResult result = new ConsumerSession(consumerBuffer, new ConsumerSessionOptions
        {
            Count = 0,
            IdleTimeout = TimeSpan.FromSeconds(30),
        }).Run(cancellation.Token);

        Assert.Equal(SessionStopReason.Cancelled, result.Reason);
    }

    /// <summary>A structurally valid session message whose payload is not the fill pattern.</summary>
    private static byte[] CorruptedPayload()
    {
        byte[] payload = new byte[32];
        RingBufferMessage.Write(payload, sequence: 0, Stopwatch.GetTimestamp());
        payload.AsSpan(RingBufferMessage.HeaderSize).Fill(0x00);
        return payload;
    }

    /// <summary>An in-process latch that counts raises, to observe the notification protocol.</summary>
    private sealed class CountingSignal : ISessionSignal
    {
        private readonly InProcessSessionSignal _inner = new();

        public int Signals;

        public void Signal()
        {
            Interlocked.Increment(ref Signals);
            _inner.Signal();
        }

        public bool Wait(TimeSpan timeout, CancellationToken cancellationToken) =>
            _inner.Wait(timeout, cancellationToken);

        public void Dispose() => _inner.Dispose();
    }
}
