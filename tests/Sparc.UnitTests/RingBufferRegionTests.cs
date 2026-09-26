using System.IO.MemoryMappedFiles;
using Sparc;
using Sparc.WindowsMemoryMapped;
using Sparc.Core;
using Sparc.UnitTests.Support;

namespace Sparc.UnitTests;

public class RingBufferRegionTests
{
    private static string NewName() => "spsc-unit-" + Guid.NewGuid().ToString("N");

    [Fact]
    public void CreateOrOpenCreatesAndInitializes()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using RingBufferRegion region = RingBufferRegion.CreateOrOpen(factory, name, 8, 64);

        Assert.True(region.IsCreator);
        Assert.Equal(8, region.Capacity);
        Assert.Equal(64, region.SlotSize);
        Assert.Equal(56, region.MaxPayloadSize);
        Assert.Equal(RingBufferLayout.RequiredSize(8, 64), region.Size);
        Assert.Equal(RingBufferLayout.Magic, region.Header.Magic);
        Assert.Equal(0, region.Header.Head);
        Assert.Equal(0, region.Header.Tail);
        Assert.Equal(RingBufferEndpointState.NotPresent, region.ReadEndpointState(RingBufferEndpointRole.Producer));
    }

    [Fact]
    public void SecondOpenJoinsAndSharesMemory()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using RingBufferRegion creator = RingBufferRegion.CreateOrOpen(factory, name, 4, 32);
        using RingBufferRegion joiner = RingBufferRegion.CreateOrOpen(factory, name, 4, 32);

        Assert.True(creator.IsCreator);
        Assert.False(joiner.IsCreator);
        Assert.Equal(creator.Header.Capacity, joiner.Header.Capacity);

        // Writes through one mapping are visible through the other.
        Volatile.Write(ref creator.HeadRef, 42);
        Assert.Equal(42, Volatile.Read(ref joiner.HeadRef));

        creator.WriteEndpointState(RingBufferEndpointRole.Producer, RingBufferEndpointState.Running);
        Assert.Equal(
            RingBufferEndpointState.Running,
            joiner.ReadEndpointState(RingBufferEndpointRole.Producer));
    }

    [Fact]
    public void GeometryMismatchThrows()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using RingBufferRegion region = RingBufferRegion.CreateOrOpen(factory, name, 4, 64);

        RingBufferGeometryMismatchException exception = Assert.Throws<RingBufferGeometryMismatchException>(
            () => RingBufferRegion.CreateOrOpen(factory, name, 8, 64));
        Assert.Contains("capacity=4", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public unsafe void VersionMismatchThrows()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using RingBufferRegion region = RingBufferRegion.CreateOrOpen(factory, name, 4, 64);

        *(int*)(region.Pointer + RingBufferLayout.VersionOffset) = 999;

        Assert.Throws<RingBufferVersionMismatchException>(
            () => RingBufferRegion.CreateOrOpen(factory, name, 4, 64));
    }

    [Fact]
    public void UninitializedRegionThrowsCorrupted()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using IIpcMemoryRegion raw = factory.CreateOrOpen(name, RingBufferLayout.RequiredSize(4, 64));

        SharedRingBufferOptions options = new() { OpenTimeout = TimeSpan.FromMilliseconds(200) };
        Assert.Throws<RingBufferCorruptedException>(
            () => RingBufferRegion.CreateOrOpen(factory, name, 4, 64, options));
    }

    [Fact]
    public void RecreateIfStaleReclaimsWhenNoOtherHandleHoldsTheRegion()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using IIpcMemoryRegion raw = factory.CreateOrOpen(name, RingBufferLayout.RequiredSize(4, 64));

        using RingBufferRegion region = RingBufferRegion.CreateOrOpen(factory, name, 4, 64, new SharedRingBufferOptions
        {
            OpenTimeout = TimeSpan.FromMilliseconds(200),
            RecreateIfStale = true,
        });

        Assert.True(region.IsCreator);
        Assert.Equal(RingBufferLayout.Magic, region.Header.Magic);
    }

    [Fact]
    public void AdoptExistingGeometrySkipsMismatchValidation()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using RingBufferRegion creator = RingBufferRegion.CreateOrOpen(factory, name, 4, 32);
        using RingBufferRegion joiner = RingBufferRegion.CreateOrOpen(factory, name, 1024, 256, new SharedRingBufferOptions
        {
            AdoptExistingGeometry = true,
        });

        Assert.Equal(4, joiner.Capacity);
        Assert.Equal(32, joiner.SlotSize);
    }

    [Fact]
    public void RequireExistingFailsWithoutCreating()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        Assert.Throws<RingBufferTimeoutException>(() => RingBufferRegion.CreateOrOpen(
            factory, name, 4, 64, new SharedRingBufferOptions
            {
                RequireExisting = true,
                OpenTimeout = TimeSpan.FromMilliseconds(100),
            }));
    }

    [Fact]
    public void DisposeIsIdempotent()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        RingBufferRegion region = RingBufferRegion.CreateOrOpen(factory, name, 4, 64);
        region.Dispose();
        region.Dispose();
        Assert.Throws<ObjectDisposedException>(() => region.ReadHeader());
    }
}

public class WindowsNamedMemoryMappedRegionFactoryTests
{
    private static readonly WindowsNamedMemoryMappedRegionFactory Factory = new();

    private static string NewName() => "spsc-unit-" + Guid.NewGuid().ToString("N");

    [Fact]
    public unsafe void CreatesAndJoinsSharedMapping()
    {
        Assert.Equal(OperatingSystem.IsWindows(), Factory.IsSupported);
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        string name = NewName();
        using IIpcMemoryRegion creator = Factory.CreateOrOpen(name, 4096);
        using IIpcMemoryRegion joiner = Factory.CreateOrOpen(name, 4096);

        Assert.True(creator.IsCreator);
        Assert.False(joiner.IsCreator);
        Assert.Equal(4096, creator.Size);

        *(long*)creator.Pointer = 0x1234;
        Assert.Equal(0x1234, *(long*)joiner.Pointer);
        Assert.False(Factory.TryReset(name));
    }

    [Fact]
    public void StaleRegionCannotBeReclaimedWhileAnotherHandleHoldsIt()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        string name = NewName();
        using MemoryMappedFile raw = MemoryMappedFile.CreateNew(name, RingBufferLayout.RequiredSize(4, 64));

        SharedRingBufferOptions options = new()
        {
            OpenTimeout = TimeSpan.FromMilliseconds(200),
            RecreateIfStale = true,
        };

        Assert.Throws<RingBufferCorruptedException>(
            () => RingBufferRegion.CreateOrOpen(Factory, name, 4, 64, options));
    }

    [Fact]
    public void RecreateIfStaleReclaimsAfterLastHandleIsClosed()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        string name = NewName();
        using (MemoryMappedFile raw = MemoryMappedFile.CreateNew(name, RingBufferLayout.RequiredSize(4, 64)))
        {
            // Never initialized; releasing the last handle destroys the object on Windows.
        }

        using RingBufferRegion region = RingBufferRegion.CreateOrOpen(Factory, name, 4, 64, new SharedRingBufferOptions
        {
            OpenTimeout = TimeSpan.FromMilliseconds(200),
            RecreateIfStale = true,
        });

        Assert.True(region.IsCreator);
        Assert.Equal(RingBufferLayout.Magic, region.Header.Magic);
    }

    [Fact]
    public void OpenExistingFailsForMissingRegion()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        string name = NewName();
        Assert.Throws<IpcTimeoutException>(() => Factory.OpenExisting(name, new IpcRegionOptions
        {
            OpenTimeout = TimeSpan.FromMilliseconds(100),
        }));
    }
}
