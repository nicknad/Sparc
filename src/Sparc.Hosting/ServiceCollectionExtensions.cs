using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Sparc;
using Sparc.Client;
using Sparc.Core;
using Sparc.Hosting;
using Sparc.UnixMemoryMapped;
using Sparc.WindowsMemoryMapped;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Host integration for SPARC: transport selection, the default channel,
/// background services for sessions or custom workers, health checks and
/// metrics.
/// </summary>
public static class SparcServiceCollectionExtensions
{
    /// <summary>
    /// Selects the memory-mapped transport for the current OS and registers it
    /// as the singleton <see cref="IIpcMemoryRegionFactory"/> (Windows: named
    /// memory-mapped files; elsewhere: file-backed regions), plus a
    /// <see cref="TimeProvider"/>. An existing factory registration wins, so
    /// tests can register <c>InMemoryMemoryRegionFactory</c> first.
    /// </summary>
    public static IServiceCollection AddSparcIpc(this IServiceCollection services) =>
        services.AddSparcIpc(static _ => { });

    /// <summary>Configures and registers the transport selected for the current OS.</summary>
    public static IServiceCollection AddSparcIpc(
        this IServiceCollection services,
        Action<SparcIpcOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        SparcIpcOptions options = new();
        configure(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IIpcMemoryRegionFactory>(
            provider => CreatePlatformFactory(provider.GetRequiredService<SparcIpcOptions>()));
        return services;
    }

    /// <summary>
    /// Registers a custom <see cref="IIpcMemoryRegionFactory"/> (tests, custom
    /// transports) plus a <see cref="TimeProvider"/>.
    /// </summary>
    public static IServiceCollection AddSparcIpc(
        this IServiceCollection services,
        Func<IServiceProvider, IIpcMemoryRegionFactory> factoryFactory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(factoryFactory);

        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton(factoryFactory);
        return services;
    }

    /// <summary>
    /// Registers the default channel: name, geometry and how the region is
    /// created or joined. Requires <see cref="AddSparcIpc(IServiceCollection)"/>.
    /// </summary>
    public static IServiceCollection AddSparcChannel(this IServiceCollection services) =>
        services.AddSparcChannel(static _ => { });

    /// <summary>Configures and registers the default channel.</summary>
    public static IServiceCollection AddSparcChannel(
        this IServiceCollection services,
        Action<SparcChannelOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        SparcChannelOptions options = new();
        configure(options);
        return AddSparcChannelCore(services, options);
    }

    /// <summary>
    /// Binds the default channel from configuration (for example
    /// <c>Sparc:Channel</c> with a <c>Name</c> key). Requires
    /// <see cref="AddSparcIpc(IServiceCollection)"/>.
    /// </summary>
    public static IServiceCollection AddSparcChannel(this IServiceCollection services, IConfigurationSection section)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);
        SparcChannelOptions options = section.Get<SparcChannelOptions>() ?? new SparcChannelOptions();
        return AddSparcChannelCore(services, options);
    }

    /// <summary>Hosts a <see cref="ProducerSession"/> on the default channel.</summary>
    public static IServiceCollection AddSparcProducerSession(
        this IServiceCollection services,
        Action<ProducerSessionOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        RequireIpc(services);
        ProducerSessionOptions options = new();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton<SparcProducerSessionService>();
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredService<SparcProducerSessionService>());
        return services;
    }

    /// <summary>Hosts a <see cref="ConsumerSession"/> on the default channel.</summary>
    public static IServiceCollection AddSparcConsumerSession(
        this IServiceCollection services,
        Action<ConsumerSessionOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        RequireIpc(services);
        ConsumerSessionOptions options = new();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton<SparcConsumerSessionService>();
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredService<SparcConsumerSessionService>());
        return services;
    }

    /// <summary>Hosts a custom <typeparamref name="TWorker"/> producer on the default channel.</summary>
    public static IServiceCollection AddSparcProducerWorker<TWorker>(this IServiceCollection services)
        where TWorker : class, ISparcProducerWorker
    {
        ArgumentNullException.ThrowIfNull(services);
        RequireIpc(services);

        services.TryAddSingleton<TWorker>();
        services.TryAddSingleton<SparcProducerWorkerService<TWorker>>();
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredService<SparcProducerWorkerService<TWorker>>());
        return services;
    }

    /// <summary>Hosts a custom <typeparamref name="TWorker"/> consumer on the default channel.</summary>
    public static IServiceCollection AddSparcConsumerWorker<TWorker>(this IServiceCollection services)
        where TWorker : class, ISparcConsumerWorker
    {
        ArgumentNullException.ThrowIfNull(services);
        RequireIpc(services);

        services.TryAddSingleton<TWorker>();
        services.TryAddSingleton<SparcConsumerWorkerService<TWorker>>();
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredService<SparcConsumerWorkerService<TWorker>>());
        return services;
    }

    /// <summary>Registers the channel health check (see <see cref="SparcHealthOptions"/>).</summary>
    public static IServiceCollection AddSparcHealthChecks(
        this IServiceCollection services,
        Action<SparcHealthOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        SparcHealthOptions options = new();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.AddHealthChecks().AddCheck<SparcChannelHealthCheck>("sparc");
        return services;
    }

    private static IServiceCollection AddSparcChannelCore(IServiceCollection services, SparcChannelOptions options)
    {
        RequireIpc(services);
        options.Validate();

        services.TryAddSingleton(options);
        services.TryAddSingleton(new SparcChannelStatus(options.Name));
        return services;
    }

    private static void RequireIpc(IServiceCollection services)
    {
        foreach (ServiceDescriptor descriptor in services)
        {
            if (descriptor.ServiceType == typeof(IIpcMemoryRegionFactory))
            {
                return;
            }
        }

        throw new InvalidOperationException(
            "No IIpcMemoryRegionFactory is registered; call AddSparcIpc(...) first.");
    }

    private static IIpcMemoryRegionFactory CreatePlatformFactory(SparcIpcOptions options)
    {
        if (OperatingSystem.IsWindows())
        {
            return new WindowsNamedMemoryMappedRegionFactory();
        }

        if (OperatingSystem.IsBrowser() || OperatingSystem.IsWasi())
        {
            throw new IpcPlatformNotSupportedException(
                "SPARC hosting needs a memory-mapped transport; browser and WASI targets are not supported.");
        }

        return new UnixFileMemoryMappedRegionFactory(options.UnixDirectory);
    }
}
