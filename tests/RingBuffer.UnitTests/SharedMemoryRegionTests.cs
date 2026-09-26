using System.IO.MemoryMappedFiles;
using RingBuffer.Core;
using RingBuffer.SharedMemory;

namespace RingBuffer.UnitTests;

public class SharedMemoryRegionTests
{
    private static string NewName() => "spsc-unit-" + Guid.NewGuid().ToString("N");

    [Fact]
    public void CreateOrOpenCreatesAndInitializes()
    {
        string name = NewName();
        using SharedMemoryRegion region = SharedMemoryRegion.CreateOrOpen(name, 8, 64);

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
        string name = NewName();
        using SharedMemoryRegion creator = SharedMemoryRegion.CreateOrOpen(name, 4, 32);
        using SharedMemoryRegion joiner = SharedMemoryRegion.CreateOrOpen(name, 4, 32);

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
        string name = NewName();
        using SharedMemoryRegion region = SharedMemoryRegion.CreateOrOpen(name, 4, 64);

        RingBufferGeometryMismatchException exception = Assert.Throws<RingBufferGeometryMismatchException>(
            () => SharedMemoryRegion.CreateOrOpen(name, 8, 64));
        Assert.Contains("capacity=4", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public unsafe void VersionMismatchThrows()
    {
        string name = NewName();
        using SharedMemoryRegion region = SharedMemoryRegion.CreateOrOpen(name, 4, 64);

        *(int*)(region.Pointer + RingBufferLayout.VersionOffset) = 999;

        Assert.Throws<RingBufferVersionMismatchException>(
            () => SharedMemoryRegion.CreateOrOpen(name, 4, 64));
    }

    [Fact]
    public void UninitializedRegionThrowsCorrupted()
    {
        string name = NewName();
        using MemoryMappedFile raw = MemoryMappedFile.CreateNew(name, RingBufferLayout.RequiredSize(4, 64));

        SharedMemoryOptions options = new() { OpenTimeout = TimeSpan.FromMilliseconds(200) };
        Assert.Throws<RingBufferCorruptedException>(
            () => SharedMemoryRegion.CreateOrOpen(name, 4, 64, options));
    }

    [Fact]
    public void RecreateIfStaleReclaimsWhenNoOtherHandleHoldsTheRegion()
    {
        string name = NewName();
        using (MemoryMappedFile raw = MemoryMappedFile.CreateNew(name, RingBufferLayout.RequiredSize(4, 64)))
        {
            // Never initialized; releasing the last handle destroys the object on Windows.
        }

        using SharedMemoryRegion region = SharedMemoryRegion.CreateOrOpen(name, 4, 64, new SharedMemoryOptions
        {
            OpenTimeout = TimeSpan.FromMilliseconds(200),
            RecreateIfStale = true,
        });

        Assert.True(region.IsCreator);
        Assert.Equal(RingBufferLayout.Magic, region.Header.Magic);
    }

    [Fact]
    public void RecreateIfStaleFailsWhileAnotherLiveHandleHoldsTheRegion()
    {
        string name = NewName();
        using MemoryMappedFile raw = MemoryMappedFile.CreateNew(name, RingBufferLayout.RequiredSize(4, 64));

        SharedMemoryOptions options = new()
        {
            OpenTimeout = TimeSpan.FromMilliseconds(200),
            RecreateIfStale = true,
        };

        // A second handle keeps the kernel object alive, so the stale region
        // cannot be reclaimed; the caller gets a clear error instead.
        Assert.Throws<RingBufferCorruptedException>(
            () => SharedMemoryRegion.CreateOrOpen(name, 4, 64, options));
    }

    [Fact]
    public void AdoptExistingGeometrySkipsMismatchValidation()
    {
        string name = NewName();
        using SharedMemoryRegion creator = SharedMemoryRegion.CreateOrOpen(name, 4, 32);
        using SharedMemoryRegion joiner = SharedMemoryRegion.CreateOrOpen(name, 1024, 256, new SharedMemoryOptions
        {
            AdoptExistingGeometry = true,
        });

        Assert.Equal(4, joiner.Capacity);
        Assert.Equal(32, joiner.SlotSize);
    }

    [Fact]
    public void RequireExistingFailsWithoutCreating()
    {
        string name = NewName();
        Assert.Throws<RingBufferTimeoutException>(() => SharedMemoryRegion.CreateOrOpen(
            name, 4, 64, new SharedMemoryOptions
            {
                RequireExisting = true,
                OpenTimeout = TimeSpan.FromMilliseconds(100),
            }));
    }

    [Fact]
    public void DisposeIsIdempotent()
    {
        string name = NewName();
        SharedMemoryRegion region = SharedMemoryRegion.CreateOrOpen(name, 4, 64);
        region.Dispose();
        region.Dispose();
        Assert.Throws<ObjectDisposedException>(() => region.ReadHeader());
    }
}
