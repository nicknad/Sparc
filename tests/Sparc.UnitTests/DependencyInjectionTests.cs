using Microsoft.Extensions.DependencyInjection;
using Sparc.InMemory;
using Sparc.WindowsMemoryMapped;

namespace Sparc.UnitTests;

public class DependencyInjectionTests
{
    [Fact]
    public void AddWindowsNamedMemoryMappedIpcRegistersSingletonFactory()
    {
        ServiceCollection services = new();

        IServiceCollection returned = services.AddWindowsNamedMemoryMappedIpc();

        Assert.Same(services, returned);
        using ServiceProvider provider = services.BuildServiceProvider();
        IIpcMemoryRegionFactory factory = provider.GetRequiredService<IIpcMemoryRegionFactory>();
        Assert.IsType<WindowsNamedMemoryMappedRegionFactory>(factory);
        Assert.Same(factory, provider.GetRequiredService<IIpcMemoryRegionFactory>());
    }

    [Fact]
    public void AddWindowsNamedMemoryMappedIpcKeepsExistingRegistration()
    {
        ServiceCollection services = new();
        InMemoryMemoryRegionFactory existing = new();
        services.AddSingleton<IIpcMemoryRegionFactory>(existing);

        services.AddWindowsNamedMemoryMappedIpc();

        using ServiceProvider provider = services.BuildServiceProvider();
        Assert.Same(existing, provider.GetRequiredService<IIpcMemoryRegionFactory>());
    }
}
