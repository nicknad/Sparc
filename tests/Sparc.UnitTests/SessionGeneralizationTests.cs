using Sparc.Client;
using Sparc.Core;
using Sparc.InMemory;

namespace Sparc.UnitTests;

public class SessionGeneralizationTests
{
    private static string NewName() => "spsc-session-gen-" + Guid.NewGuid().ToString("N");

    [Fact]
    public async Task ProducerRunsUntilCancelled()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using IProducerEndpoint producerBuffer = SparcRing.OpenProducer(
            factory, name, 64, 64, cancellationToken: TestContext.Current.CancellationToken);
        using IConsumerEndpoint consumerBuffer = SparcRing.OpenConsumer(
            factory, name, 64, 64, cancellationToken: TestContext.Current.CancellationToken);
        using CancellationTokenSource cancellation = new();

        ProducerSession producer = new(producerBuffer, new ProducerSessionOptions
        {
            Count = 0,
            PayloadSize = 32,
            FullTimeout = TimeSpan.FromSeconds(10),
        });
        ConsumerSession consumer = new(consumerBuffer, new ConsumerSessionOptions
        {
            Count = 5,
            IdleTimeout = TimeSpan.FromSeconds(10),
        });

        Task<ProducerRunResult> producerTask = producer.RunAsync(cancellation.Token);
        ConsumerRunResult consumerResult = await consumer.RunAsync(TestContext.Current.CancellationToken);
        await cancellation.CancelAsync();
        ProducerRunResult producerResult = await producerTask;

        Assert.Equal(SessionStopReason.Completed, consumerResult.Reason);
        Assert.Equal(5, consumerResult.Received);
        Assert.Equal(SessionStopReason.Cancelled, producerResult.Reason);
        Assert.True(producerResult.Produced > 0);
        Assert.Equal(0, producerResult.Unsent);
    }

    [Fact]
    public void CustomPayloadWriterOwnsThePayload()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using IProducerEndpoint producerBuffer = SparcRing.OpenProducer(
            factory, name, 16, 64, cancellationToken: TestContext.Current.CancellationToken);
        using IConsumerEndpoint consumerBuffer = SparcRing.OpenConsumer(
            factory, name, 16, 64, cancellationToken: TestContext.Current.CancellationToken);

        ProducerRunResult result = new ProducerSession(producerBuffer, new ProducerSessionOptions
        {
            Count = 3,
            PayloadSize = 32,
            IncludeSessionHeader = false,
            PayloadWriter = (index, payload) => payload.Fill((byte)(index + 1)),
        }).Run(TestContext.Current.CancellationToken);

        Assert.Equal(SessionStopReason.Completed, result.Reason);
        Assert.Equal(3, result.Produced);

        int expected = 1;
        while (consumerBuffer.TryBeginRead(out ReadLease lease))
        {
            using (lease)
            {
                Assert.Equal(32, lease.Length);
                Assert.True(lease.Payload.IndexOfAnyExcept((byte)expected) < 0);
            }

            expected++;
        }

        Assert.Equal(4, expected);
    }

    [Fact]
    public async Task HeaderlessSessionsCompleteWithoutStructuralChecks()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using IProducerEndpoint producerBuffer = SparcRing.OpenProducer(
            factory, name, 64, 64, cancellationToken: TestContext.Current.CancellationToken);
        using IConsumerEndpoint consumerBuffer = SparcRing.OpenConsumer(
            factory, name, 64, 64, cancellationToken: TestContext.Current.CancellationToken);

        ProducerSession producer = new(producerBuffer, new ProducerSessionOptions
        {
            Count = 100,
            PayloadSize = 32,
            IncludeSessionHeader = false,
            PayloadWriter = (_, payload) => payload.Fill(0x42),
            FullTimeout = TimeSpan.FromSeconds(10),
        });
        ConsumerSession consumer = new(consumerBuffer, new ConsumerSessionOptions
        {
            Count = 100,
            IncludeSessionHeader = false,
            IdleTimeout = TimeSpan.FromSeconds(10),
        });

        Task<ProducerRunResult> producerTask = producer.RunAsync(TestContext.Current.CancellationToken);
        ConsumerRunResult consumerResult = await consumer.RunAsync(TestContext.Current.CancellationToken);
        ProducerRunResult producerResult = await producerTask;

        Assert.Equal(SessionStopReason.Completed, producerResult.Reason);
        Assert.Equal(100, producerResult.Produced);
        Assert.Equal(SessionStopReason.Completed, consumerResult.Reason);
        Assert.Equal(100, consumerResult.Received);
        Assert.Equal(0, consumerResult.Latency.Count);
    }

    [Fact]
    public async Task WaitForPeerAsyncDetectsThePeerRole()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using IProducerEndpoint producerBuffer = SparcRing.OpenProducer(
            factory, name, 16, 64, cancellationToken: TestContext.Current.CancellationToken);

        ProducerSession producer = new(producerBuffer, new ProducerSessionOptions { Count = 10, PayloadSize = 32 });
        Assert.False(await producer.WaitForPeerAsync(
            TimeSpan.FromMilliseconds(100), TestContext.Current.CancellationToken));

        using IConsumerEndpoint consumerBuffer = SparcRing.OpenConsumer(
            factory, name, 16, 64, cancellationToken: TestContext.Current.CancellationToken);
        ConsumerSession consumer = new(consumerBuffer, new ConsumerSessionOptions { IdleTimeout = TimeSpan.FromSeconds(5) });

        Assert.True(await producer.WaitForPeerAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken));
        Assert.True(await consumer.WaitForPeerAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken));
    }
}
