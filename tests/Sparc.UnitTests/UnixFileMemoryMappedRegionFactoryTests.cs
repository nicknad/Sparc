using Sparc.Core;
using Sparc.UnixMemoryMapped;

namespace Sparc.UnitTests;

public class UnixFileMemoryMappedRegionFactoryTests
{
    private static string NewName() => "spsc-unit-" + Guid.NewGuid().ToString("N");

    private static UnixFileMemoryMappedRegionFactory NewFactory(out string directory)
    {
        directory = Path.Combine(Path.GetTempPath(), "sparc-tests-" + Guid.NewGuid().ToString("N"));
        return new UnixFileMemoryMappedRegionFactory(directory);
    }

    private static void Cleanup(string directory)
    {
        if (Directory.Exists(directory))
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public unsafe void CreatesAndJoinsSharedFileMapping()
    {
        UnixFileMemoryMappedRegionFactory factory = NewFactory(out string directory);
        Assert.Equal(!OperatingSystem.IsWindows(), factory.IsSupported);
        if (!factory.IsSupported)
        {
            return;
        }

        try
        {
            string name = NewName();
            using IIpcMemoryRegion creator = factory.CreateOrOpen(name, 4096);
            using IIpcMemoryRegion joiner = factory.CreateOrOpen(name, 4096);

            Assert.True(creator.IsCreator);
            Assert.False(joiner.IsCreator);
            Assert.Equal(4096, creator.Size);
            Assert.True(File.Exists(Path.Combine(factory.DirectoryPath, name)));

            *(long*)creator.Pointer = 0x1234;
            Assert.Equal(0x1234, *(long*)joiner.Pointer);
        }
        finally
        {
            Cleanup(directory);
        }
    }

    [Fact]
    public void RegionFilesAndDirectoryAreOwnerOnly()
    {
        UnixFileMemoryMappedRegionFactory factory = NewFactory(out string directory);
        Assert.Equal(!OperatingSystem.IsWindows(), factory.IsSupported);
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        try
        {
            string name = NewName();
            using (factory.CreateOrOpen(name, 4096))
            {
            }

            const UnixFileMode OtherBits =
                UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute |
                UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute;

            Assert.Equal(UnixFileMode.None, File.GetUnixFileMode(factory.DirectoryPath) & OtherBits);
            Assert.Equal(UnixFileMode.None, File.GetUnixFileMode(Path.Combine(factory.DirectoryPath, name)) & OtherBits);
        }
        finally
        {
            Cleanup(directory);
        }
    }

    [Fact]
    public void TryResetUnlinksTheBackingFile()
    {
        UnixFileMemoryMappedRegionFactory factory = NewFactory(out string directory);
        Assert.Equal(!OperatingSystem.IsWindows(), factory.IsSupported);
        if (!factory.IsSupported)
        {
            return;
        }

        try
        {
            string name = NewName();
            using (IIpcMemoryRegion creator = factory.CreateOrOpen(name, 4096))
            {
                Assert.True(creator.IsCreator);
            }

            Assert.True(File.Exists(Path.Combine(factory.DirectoryPath, name)));
            Assert.True(factory.TryReset(name));
            Assert.False(File.Exists(Path.Combine(factory.DirectoryPath, name)));
            Assert.False(factory.TryReset(name));

            using IIpcMemoryRegion recreated = factory.CreateOrOpen(name, 4096);
            Assert.True(recreated.IsCreator);
        }
        finally
        {
            Cleanup(directory);
        }
    }

    [Fact]
    public void OpenExistingFailsForMissingRegion()
    {
        UnixFileMemoryMappedRegionFactory factory = NewFactory(out string directory);
        Assert.Equal(!OperatingSystem.IsWindows(), factory.IsSupported);
        if (!factory.IsSupported)
        {
            return;
        }

        try
        {
            Assert.Throws<IpcTimeoutException>(() => factory.OpenExisting(NewName(), new IpcRegionOptions
            {
                OpenTimeout = TimeSpan.FromMilliseconds(100),
            }));
        }
        finally
        {
            Cleanup(directory);
        }
    }

    [Fact]
    public void RequireExistingNeverCreatesTheFile()
    {
        UnixFileMemoryMappedRegionFactory factory = NewFactory(out string directory);
        Assert.Equal(!OperatingSystem.IsWindows(), factory.IsSupported);
        if (!factory.IsSupported)
        {
            return;
        }

        try
        {
            string name = NewName();
            Assert.Throws<IpcTimeoutException>(() => factory.CreateOrOpen(name, 4096, new IpcRegionOptions
            {
                RequireExisting = true,
                OpenTimeout = TimeSpan.FromMilliseconds(100),
            }));
            Assert.False(File.Exists(Path.Combine(factory.DirectoryPath, name)));
        }
        finally
        {
            Cleanup(directory);
        }
    }

    [Fact]
    public void RecreateIfStaleReclaimsAnUninitializedFile()
    {
        UnixFileMemoryMappedRegionFactory factory = NewFactory(out string directory);
        Assert.Equal(!OperatingSystem.IsWindows(), factory.IsSupported);
        if (!factory.IsSupported)
        {
            return;
        }

        try
        {
            string name = NewName();
            File.WriteAllBytes(
                Path.Combine(factory.DirectoryPath, name),
                new byte[RingBufferLayout.RequiredSize(4, 64)]);

            using RingBufferRegion region = RingBufferRegion.CreateOrOpen(factory, name, 4, 64, new SharedRingBufferOptions
            {
                OpenTimeout = TimeSpan.FromMilliseconds(200),
                RecreateIfStale = true,
            });

            Assert.True(region.IsCreator);
            Assert.Equal(RingBufferLayout.Magic, region.Header.Magic);
        }
        finally
        {
            Cleanup(directory);
        }
    }

    [Fact]
    public void SharedRingBufferRoundTripsOverTheFileFactory()
    {
        UnixFileMemoryMappedRegionFactory factory = NewFactory(out string directory);
        Assert.Equal(!OperatingSystem.IsWindows(), factory.IsSupported);
        if (!factory.IsSupported)
        {
            return;
        }

        try
        {
            string name = NewName();
            using SharedRingBuffer producer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
            using SharedRingBuffer consumer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
            producer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);
            consumer.Connect(RingBufferEndpointRole.Consumer, cancellationToken: TestContext.Current.CancellationToken);

            byte[] payload = new byte[32];
            payload.AsSpan().Fill(0x5A);
            Assert.True(producer.TryWrite(7, payload));

            byte[] destination = new byte[consumer.MaxPayloadSize];
            Assert.True(consumer.TryRead(destination, out int bytesRead, out int type));
            Assert.Equal(32, bytesRead);
            Assert.Equal(7, type);
            Assert.Equal(payload, destination.AsSpan(0, bytesRead).ToArray());
        }
        finally
        {
            Cleanup(directory);
        }
    }

    [Fact]
    public void SecurityConfigurationIsRejectedInsteadOfSilentlyIgnored()
    {
        UnixFileMemoryMappedRegionFactory factory = NewFactory(out string directory);
        Assert.Equal(!OperatingSystem.IsWindows(), factory.IsSupported);
        if (!factory.IsSupported)
        {
            return;
        }

        try
        {
            IpcRegionOptions options = new() { Security = new UnsupportedSecurity() };
            Assert.Throws<IpcPlatformNotSupportedException>(
                () => factory.CreateOrOpen(NewName(), 4096, options));
            Assert.Throws<IpcPlatformNotSupportedException>(
                () => factory.OpenExisting(NewName(), options));
        }
        finally
        {
            Cleanup(directory);
        }
    }

    private sealed class UnsupportedSecurity : IpcMemoryRegionSecurity;

    [Fact]
    public void NamesWithDirectorySeparatorsAreRejected()
    {
        UnixFileMemoryMappedRegionFactory factory = NewFactory(out string directory);
        Assert.Equal(!OperatingSystem.IsWindows(), factory.IsSupported);
        if (!factory.IsSupported)
        {
            return;
        }

        try
        {
            // Backslash is a legal file-name character on Unix; only path
            // separators and relative directory names are rejected.
            Assert.Throws<ArgumentException>(() => factory.CreateOrOpen("nested/region", 4096));
            Assert.Throws<ArgumentException>(() => factory.CreateOrOpen("..", 4096));
            Assert.Throws<ArgumentException>(() => factory.OpenExisting("nested/region"));
            Assert.Throws<ArgumentException>(() => factory.TryReset("nested/region"));
        }
        finally
        {
            Cleanup(directory);
        }
    }
}
