using Sparc;
using Sparc.Client;
using Sparc.Core;

namespace Sparc.WebApp;

/// <summary>
/// Runs one <see cref="ProducerSession"/> and one <see cref="ConsumerSession"/>
/// over two views of the same shared region, mirroring what two independent
/// processes would do. Sessions are blocking, so they run on thread-pool
/// threads; the worker itself stays async.
/// </summary>
/// <remarks>
/// In a real deployment each process would host a single role (the README's
/// <c>OrderProducerWorker</c> pattern); this sample hosts both so a single
/// <c>dotnet run</c> demonstrates the full channel.
/// </remarks>
public sealed class DemoTransferWorker(
    IIpcMemoryRegionFactory factory,
    TransferCoordinator coordinator,
    TimeProvider timeProvider,
    ILogger<DemoTransferWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!factory.IsSupported)
        {
            logger.LogError(
                "{Factory} is not supported on this platform; the demo stays idle. " +
                "This sample requires Windows named memory-mapped files.",
                factory.GetType().Name);
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            int count = await coordinator.WaitForRequestAsync(stoppingToken).ConfigureAwait(false);
            await RunTransferAsync(count, stoppingToken).ConfigureAwait(false);
        }
    }

    private async Task RunTransferAsync(int count, CancellationToken cancellationToken)
    {
        SharedRingBuffer producerView = await OpenAsync(cancellationToken).ConfigureAwait(false);
        SharedRingBuffer consumerView = await OpenAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ProducerSession producer = new(
                producerView,
                new ProducerSessionOptions { Count = count, PayloadSize = DemoRing.PayloadSize },
                timeProvider,
                logger);

            ConsumerSession consumer = new(
                consumerView,
                new ConsumerSessionOptions { Count = 0, IdleTimeout = TimeSpan.FromSeconds(30) },
                timeProvider,
                logger);

            Task<ProducerRunResult> producerTask = Task.Run(() => producer.Run(cancellationToken), cancellationToken);
            Task<ConsumerRunResult> consumerTask = Task.Run(() => consumer.Run(cancellationToken), cancellationToken);

            ProducerRunResult producerResult = await producerTask.ConfigureAwait(false);

            // Disposing the producer view publishes "producer stopped", which is
            // how the consumer learns that the stream is complete and can drain.
            producerView.Dispose();

            ConsumerRunResult consumerResult = await consumerTask.ConfigureAwait(false);
            coordinator.Complete(producerResult, consumerResult);

            logger.LogInformation(
                "Transfer finished: produced={Produced} received={Received} reasons={ProducerReason}/{ConsumerReason} elapsed={Elapsed:F3}s",
                producerResult.Produced,
                consumerResult.Received,
                producerResult.Reason,
                consumerResult.Reason,
                consumerResult.Elapsed.TotalSeconds);
        }
        finally
        {
            producerView.Dispose();
            consumerView.Dispose();
        }
    }

    private Task<SharedRingBuffer> OpenAsync(CancellationToken cancellationToken) => Task.Run(
        () => SharedRingBuffer.OpenOrCreate(
            factory,
            DemoRing.Name,
            DemoRing.Capacity,
            DemoRing.SlotSize,
            new SharedRingBufferOptions { OpenTimeout = TimeSpan.FromSeconds(30) }),
        cancellationToken);
}
