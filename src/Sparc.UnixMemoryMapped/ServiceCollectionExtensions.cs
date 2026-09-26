using Sparc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Sparc.UnixMemoryMapped;

/// <summary>Dependency injection helpers for the Unix file-backed transport.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="UnixFileMemoryMappedRegionFactory"/> as the
    /// singleton <see cref="IIpcMemoryRegionFactory"/>.
    /// </summary>
    public static IServiceCollection AddUnixFileMemoryMappedIpc(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<IIpcMemoryRegionFactory, UnixFileMemoryMappedRegionFactory>();
        return services;
    }
}
