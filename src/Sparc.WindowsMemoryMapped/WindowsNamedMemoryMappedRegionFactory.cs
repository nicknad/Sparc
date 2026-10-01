using System.ComponentModel;
using System.IO.MemoryMappedFiles;
using Sparc;

namespace Sparc.WindowsMemoryMapped;

/// <summary>
/// <see cref="IIpcMemoryRegionFactory"/> implementation for Windows named
/// memory-mapped sections.
/// </summary>
/// <remarks>
/// <para>
/// .NET supports named memory-mapped files on Windows only. A named map is a
/// kernel object: it disappears when the last handle closes, so a crashed
/// creator leaves nothing behind and <see cref="TryReset"/> is a no-op (closing
/// our handle is enough for the next <see cref="CreateOrOpen"/> to succeed).
/// </para>
/// <para>
/// When <see cref="IpcRegionOptions.Security"/> supplies a
/// <see cref="WindowsSectionSecurity"/>, the section is created with that
/// DACL through <c>CreateFileMapping</c>; opening an existing map is a plain
/// read/write section open, which the OS access-checks against the creator's
/// DACL. Security is therefore enforced during region establishment only: the
/// message path never touches it.
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
    /// <remarks>
    /// When joining an existing region the supplied
    /// <see cref="IpcRegionOptions.Security"/> is not needed: access is
    /// governed by the DACL the creator applied.
    /// </remarks>
    public IIpcMemoryRegion CreateOrOpen(string name, long size, IpcRegionOptions? options = null)
    {
        EnsureSupported();
        RegionName.Validate(name);
        ArgumentOutOfRangeException.ThrowIfLessThan(size, 1);
        options ??= IpcRegionOptions.Default;

        WindowsSectionSecurity? security = options.Security as WindowsSectionSecurity;
        if (options.Security is not null && security is null)
        {
            throw new ArgumentException(
                $"Sparc.WindowsMemoryMapped understands {nameof(WindowsSectionSecurity)} only, " +
                $"not {options.Security.GetType().FullName}.",
                nameof(options));
        }

        long startTimestamp = options.TimeProvider.GetTimestamp();

        if (!options.RequireExisting)
        {
            try
            {
                return WindowsSectionMemoryRegion.CreateNamed(name, size, security);
            }
            catch (Win32Exception exception) when (exception.NativeErrorCode == WindowsSectionApi.ErrorAlreadyExists)
            {
                // Already exists: fall through to the plain open.
            }
            catch (Win32Exception exception) when (exception.NativeErrorCode == WindowsSectionApi.ErrorAccessDenied)
            {
                // CreateFileMapping asks for broader section access than a
                // restrictive DACL grants; a read/write open may still be
                // allowed. If the region is actually absent, surface the
                // denial instead of waiting for the open timeout.
                return OpenExistingAfterCreateDenied(name, exception);
            }
        }

        return new NamedMemoryMappedRegion(
            name, OpenExistingWithTimeout(name, options, startTimestamp), isCreator: false);
    }

    /// <inheritdoc />
    public IIpcMemoryRegion OpenExisting(string name, IpcRegionOptions? options = null)
    {
        EnsureSupported();
        RegionName.Validate(name);
        options ??= IpcRegionOptions.Default;

        long startTimestamp = options.TimeProvider.GetTimestamp();
        return new NamedMemoryMappedRegion(
            name, OpenExistingWithTimeout(name, options, startTimestamp), isCreator: false);
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
                "Sparc.WindowsMemoryMapped uses named memory-mapped files, which .NET supports on Windows only. " +
                "On Unix, provide a file-backed IIpcMemoryRegionFactory (MemoryMappedFile.CreateFromFile) instead.");
        }
    }

    private static NamedMemoryMappedRegion OpenExistingAfterCreateDenied(string name, Win32Exception denial)
    {
        try
        {
#pragma warning disable CA1416 // Windows-only API; EnsureSupported guards every entry point.
            return new NamedMemoryMappedRegion(
                name, MemoryMappedFile.OpenExisting(name, MemoryMappedFileRights.ReadWrite), isCreator: false);
#pragma warning restore CA1416
        }
        catch (Exception exception) when (exception is FileNotFoundException or DirectoryNotFoundException)
        {
            throw new UnauthorizedAccessException(
                $"Region '{name}' does not exist and creating it was denied: {denial.Message}", denial);
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
