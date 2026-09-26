using System.IO.MemoryMappedFiles;
using Ipc;

namespace Ipc.WindowsMemoryMapped;

/// <summary>
/// <see cref="IIpcMemoryRegionFactory"/> implementation for Windows named
/// memory-mapped files.
/// </summary>
/// <remarks>
/// <para>
/// .NET supports named memory-mapped files on Windows only. A named map is a
/// kernel object: it disappears when the last handle closes, so a crashed
/// creator leaves nothing behind and <see cref="TryReset"/> is a no-op (closing
/// our handle is enough for the next <see cref="CreateOrOpen"/> to succeed).
/// </para>
/// <para>
/// The factory is thread-safe and intended to be registered once as a singleton.
/// </para>
/// </remarks>
public sealed class WindowsNamedMemoryMappedRegionFactory : IIpcMemoryRegionFactory
{
    /// <inheritdoc />
    public bool IsSupported => OperatingSystem.IsWindows();

    /// <inheritdoc />
    public IIpcMemoryRegion CreateOrOpen(string name, long size, IpcRegionOptions? options = null)
    {
        EnsureSupported();
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentOutOfRangeException.ThrowIfLessThan(size, 1);
        options ??= IpcRegionOptions.Default;

        long startTimestamp = options.TimeProvider.GetTimestamp();

        if (!options.RequireExisting)
        {
            try
            {
#pragma warning disable CA1416 // Windows-only API; EnsureSupported guards every entry point.
                return new NamedMemoryMappedRegion(name, MemoryMappedFile.CreateNew(name, size), isCreator: true);
#pragma warning restore CA1416
            }
            catch (IOException)
            {
                // Already exists: fall through to OpenExisting.
            }
        }

        return new NamedMemoryMappedRegion(name, OpenExistingWithTimeout(name, options, startTimestamp), isCreator: false);
    }

    /// <inheritdoc />
    public IIpcMemoryRegion OpenExisting(string name, IpcRegionOptions? options = null)
    {
        EnsureSupported();
        ArgumentException.ThrowIfNullOrEmpty(name);
        options ??= IpcRegionOptions.Default;

        long startTimestamp = options.TimeProvider.GetTimestamp();
        return new NamedMemoryMappedRegion(name, OpenExistingWithTimeout(name, options, startTimestamp), isCreator: false);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Always returns false: on Windows the underlying kernel object is
    /// destroyed as soon as its last handle is closed, so there is no backing
    /// store for the factory to remove.
    /// </remarks>
    public bool TryReset(string name)
    {
        EnsureSupported();
        return false;
    }

    private static void EnsureSupported()
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new IpcPlatformNotSupportedException(
                "Ipc.WindowsMemoryMapped uses named memory-mapped files, which .NET supports on Windows only. " +
                "On Unix, provide a file-backed IIpcMemoryRegionFactory (MemoryMappedFile.CreateFromFile) instead.");
        }
    }

    private static MemoryMappedFile OpenExistingWithTimeout(string name, IpcRegionOptions options, long startTimestamp)
    {
        while (true)
        {
            try
            {
#pragma warning disable CA1416 // Windows-only API; EnsureSupported guards every entry point.
                return MemoryMappedFile.OpenExisting(name, MemoryMappedFileRights.ReadWrite);
#pragma warning restore CA1416
            }
            catch (Exception exception) when (exception is FileNotFoundException or DirectoryNotFoundException)
            {
                if (options.TimeProvider.GetElapsedTime(startTimestamp) >= options.OpenTimeout)
                {
                    throw new IpcTimeoutException(
                        $"Region '{name}' did not exist within {options.OpenTimeout.TotalMilliseconds:F0} ms.");
                }

                Thread.Sleep(2);
            }
        }
    }
}
