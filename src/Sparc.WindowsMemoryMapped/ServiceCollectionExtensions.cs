using Sparc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Sparc.WindowsMemoryMapped;

/// <summary>Dependency injection helpers for the Windows named memory-mapped transport.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="WindowsNamedMemoryMappedRegionFactory"/> as the
    /// singleton <see cref="IIpcMemoryRegionFactory"/>.
    /// </summary>
    public static IServiceCollection AddWindowsNamedMemoryMappedIpc(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<IIpcMemoryRegionFactory, WindowsNamedMemoryMappedRegionFactory>();
        return services;
    }
}
