using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Sparc;
using Sparc.Client;
using Sparc.Core;
using Sparc.Hosting;
using Sparc.InMemory;
using Sparc.UnixMemoryMapped;
using Sparc.WindowsMemoryMapped;

namespace Sparc.UnitTests;

public class HostingTests
{
    private static string NewName() => "sparc-hosting-" + Guid.NewGuid().ToString("N");

    private static ServiceCollection CreateServices(string name, Action<IServiceCollection>? configure = null)
    {
        ServiceCollection services = new();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IIpcMemoryRegionFactory>(new InMemoryMemoryRegionFactory());
        services.AddSparcIpc();
        services.AddSparcChannel(options =>
        {
            options.Name = name;
            options.Capacity = 64;
            options.SlotSize = 64;
        });
        configure?.Invoke(services);
        return services;
    }

    [Fact]
    public void AddSparcIpcKeepsAnExistingFactoryRegistration()
    {
        ServiceCollection services = new();
        InMemoryMemoryRegionFactory custom = new();
        services.AddSingleton<IIpcMemoryRegionFactory>(custom);
        services.AddSparcIpc();

        using ServiceProvider provider = services.BuildServiceProvider();
        Assert.Same(custom, provider.GetRequiredService<IIpcMemoryRegionFactory>());
        Assert.NotNull(provider.GetRequiredService<TimeProvider>());
    }

    [Fact]
    public void AddSparcIpcSelectsTheTransportForTheCurrentPlatform()
    {
        ServiceCollection services = new();
        services.AddSparcIpc();

        using ServiceProvider provider = services.BuildServiceProvider();
        IIpcMemoryRegionFactory factory = provider.GetRequiredService<IIpcMemoryRegionFactory>();
        if (OperatingSystem.IsWindows())
        {
            Assert.IsType<WindowsNamedMemoryMappedRegionFactory>(factory);
        }
        else
        {
            Assert.IsType<UnixFileMemoryMappedRegionFactory>(factory);
        }
    }

    [Fact]
    public void AddSparcChannelRequiresARegisteredTransport()
    {
        ServiceCollection services = new();
        Assert.Throws<InvalidOperationException>(() => services.AddSparcChannel(options => options.Name = NewName()));
    }

    [Fact]
    public async Task SessionServicesCompleteATransfer()
    {
        string name = NewName();
        ServiceCollection services = CreateServices(name, registered =>
        {
            registered.AddSparcProducerSession(options =>
            {
                options.Count = 100;
                options.PayloadSize = 32;
                options.FullTimeout = TimeSpan.FromSeconds(10);
            });
            registered.AddSparcConsumerSession(options =>
            {
                options.Count = 100;
                options.IdleTimeout = TimeSpan.FromSeconds(10);
            });
        });

        using ServiceProvider provider = services.BuildServiceProvider();
        SparcProducerSessionService producer = provider.GetRequiredService<SparcProducerSessionService>();
        SparcConsumerSessionService consumer = provider.GetRequiredService<SparcConsumerSessionService>();

        await producer.StartAsync(TestContext.Current.CancellationToken);
        await consumer.StartAsync(TestContext.Current.CancellationToken);
        await Task.WhenAll(producer.Completion, consumer.Completion)
            .WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        await producer.StopAsync(TestContext.Current.CancellationToken);
        await consumer.StopAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(producer.Result);
        Assert.Equal(SessionStopReason.Completed, producer.Result.Value.Reason);
        Assert.Equal(100, producer.Result.Value.Produced);
        Assert.NotNull(consumer.Result);
        Assert.Equal(SessionStopReason.Completed, consumer.Result.Value.Reason);
        Assert.Equal(100, consumer.Result.Value.Received);

        SparcChannelStatus status = provider.GetRequiredService<SparcChannelStatus>();
        Assert.Equal(100, status.ProducerResult!.Value.Produced);
        Assert.Equal(100, status.ConsumerResult!.Value.Received);
    }

    [Fact]
    public async Task CustomWorkersPublishAndConsume()
    {
        string name = NewName();
        ServiceCollection services = CreateServices(name, registered =>
        {
            registered.AddSingleton<WorkerSignals>();
            registered.AddSparcProducerWorker<TestProducerWorker>();
            registered.AddSparcConsumerWorker<TestConsumerWorker>();
        });

        using ServiceProvider provider = services.BuildServiceProvider();
        WorkerSignals signals = provider.GetRequiredService<WorkerSignals>();
        SparcProducerWorkerService<TestProducerWorker> producer =
            provider.GetRequiredService<SparcProducerWorkerService<TestProducerWorker>>();
        SparcConsumerWorkerService<TestConsumerWorker> consumer =
            provider.GetRequiredService<SparcConsumerWorkerService<TestConsumerWorker>>();

        await producer.StartAsync(TestContext.Current.CancellationToken);
        await consumer.StartAsync(TestContext.Current.CancellationToken);
        await Task.WhenAll(signals.ProducerDone.Task, signals.ConsumerDone.Task)
            .WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        await producer.StopAsync(TestContext.Current.CancellationToken);
        await consumer.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(3, signals.Received);
    }

    [Fact]
    public async Task HealthCheckTracksTheChannel()
    {
        string name = NewName();
        ServiceCollection services = CreateServices(name, registered =>
        {
            registered.AddSingleton<WorkerSignals>();
            registered.AddSparcProducerWorker<BlockingProducerWorker>();
            registered.AddSparcConsumerWorker<BlockingConsumerWorker>();
            registered.AddSparcHealthChecks(options =>
            {
                options.RequireProducer = true;
                options.RequireConsumer = true;
            });
        });

        using ServiceProvider provider = services.BuildServiceProvider();
        HealthCheckService health = provider.GetRequiredService<HealthCheckService>();
        HealthReport report = await health.CheckHealthAsync(TestContext.Current.CancellationToken);
        Assert.Equal(HealthStatus.Unhealthy, report.Status);

        WorkerSignals signals = provider.GetRequiredService<WorkerSignals>();
        SparcProducerWorkerService<BlockingProducerWorker> producer =
            provider.GetRequiredService<SparcProducerWorkerService<BlockingProducerWorker>>();
        SparcConsumerWorkerService<BlockingConsumerWorker> consumer =
            provider.GetRequiredService<SparcConsumerWorkerService<BlockingConsumerWorker>>();

        await producer.StartAsync(TestContext.Current.CancellationToken);
        await consumer.StartAsync(TestContext.Current.CancellationToken);
        await Task.WhenAll(signals.ProducerDone.Task, signals.ConsumerDone.Task)
            .WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        report = await health.CheckHealthAsync(TestContext.Current.CancellationToken);
        Assert.Equal(HealthStatus.Healthy, report.Status);

        signals.Release.TrySetResult();
        await Task.WhenAll(producer.Completion, consumer.Completion)
            .WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        report = await health.CheckHealthAsync(TestContext.Current.CancellationToken);
        Assert.Equal(HealthStatus.Unhealthy, report.Status);
    }

    [Fact]
    public void AddSparcChannelDefaultsToUniqueSecureNames()
    {
        static string RegisterDefault()
        {
            ServiceCollection services = new();
            services.AddSingleton<IIpcMemoryRegionFactory>(new InMemoryMemoryRegionFactory());
            services.AddSparcIpc();
            services.AddSparcChannel();

            using ServiceProvider provider = services.BuildServiceProvider();
            return provider.GetRequiredService<SparcChannelOptions>().Name;
        }

        string first = RegisterDefault();
        string second = RegisterDefault();

        Assert.NotEqual(first, second, StringComparer.Ordinal);
        Assert.Equal(first, RegionName.Validate(first));
        Assert.Equal(second, RegionName.Validate(second));
    }

    [Fact]
    public void AddSparcChannelBindsFromConfiguration()
    {
        IConfigurationRoot configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sparc:Channel:Name"] = "cfg-channel",
                ["Sparc:Channel:Capacity"] = "128",
                ["Sparc:Channel:SlotSize"] = "512",
            })
            .Build();

        ServiceCollection services = new();
        services.AddSingleton<IIpcMemoryRegionFactory>(new InMemoryMemoryRegionFactory());
        services.AddSparcIpc();
        services.AddSparcChannel(configuration.GetSection("Sparc:Channel"));

        using ServiceProvider provider = services.BuildServiceProvider();
        SparcChannelOptions options = provider.GetRequiredService<SparcChannelOptions>();
        Assert.Equal("cfg-channel", options.Name);
        Assert.Equal(128, options.Capacity);
        Assert.Equal(512, options.SlotSize);
    }

    private sealed class WorkerSignals
    {
        public TaskCompletionSource ProducerDone { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource ConsumerDone { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Received;
    }

    private sealed class TestProducerWorker(WorkerSignals signals) : ISparcProducerWorker
    {
        public async Task RunAsync(IProducerEndpoint producer, CancellationToken cancellationToken)
        {
            for (int i = 0; i < 3; i++)
            {
                while (!producer.TryPublish(new byte[] { (byte)i }))
                {
                    await Task.Delay(1, cancellationToken).ConfigureAwait(false);
                }
            }

            signals.ProducerDone.TrySetResult();
        }
    }

    private sealed class TestConsumerWorker(WorkerSignals signals) : ISparcConsumerWorker
    {
        public Task RunAsync(IConsumerEndpoint consumer, CancellationToken cancellationToken)
        {
            using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(20));

            Span<byte> buffer = stackalloc byte[consumer.MaxPayloadSize];
            int received = 0;
            while (received < 3)
            {
                if (consumer.TryRead(buffer, out _, out _))
                {
                    received++;
                    continue;
                }

                Thread.Sleep(1);
                timeout.Token.ThrowIfCancellationRequested();
            }

            signals.Received = received;
            signals.ConsumerDone.TrySetResult();
            return Task.CompletedTask;
        }
    }

    private sealed class BlockingProducerWorker(WorkerSignals signals) : ISparcProducerWorker
    {
        public async Task RunAsync(IProducerEndpoint producer, CancellationToken cancellationToken)
        {
            signals.ProducerDone.TrySetResult();
            await signals.Release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class BlockingConsumerWorker(WorkerSignals signals) : ISparcConsumerWorker
    {
        public async Task RunAsync(IConsumerEndpoint consumer, CancellationToken cancellationToken)
        {
            signals.ConsumerDone.TrySetResult();
            await signals.Release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
