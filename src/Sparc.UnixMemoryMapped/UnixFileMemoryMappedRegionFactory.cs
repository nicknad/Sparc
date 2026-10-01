using System.IO.MemoryMappedFiles;
using Sparc;

namespace Sparc.UnixMemoryMapped;

/// <summary>
/// <see cref="IIpcMemoryRegionFactory"/> implementation for Unix-like systems,
/// backed by one file per region mapped with
/// <see cref="MemoryMappedFile.CreateFromFile(string, FileMode, string?, long, MemoryMappedFileAccess)"/>.
/// </summary>
/// <remarks>
/// <para>
/// Unlike Windows named maps (kernel objects that disappear with their last
/// handle), a file-backed region is persistent: the backing file stays until
/// <see cref="TryReset"/> unlinks it, an operator deletes it, or the temp
/// directory is cleaned up. A crashed creator therefore leaves a stale file
/// behind; the ring layer detects the bad magic and reclaims it when
/// stale-region recreation is enabled.
/// </para>
/// <para>
/// Regions live in <see cref="DirectoryPath"/>, by default a <c>sparc</c>
/// directory under <see cref="Path.GetTempPath"/> (tmpfs on most Linux
/// systems). Pass a different directory to place the files on a specific
/// filesystem, for example <c>/dev/shm</c>.
/// </para>
/// <para>
/// <see cref="TryReset"/> deletes the file while other processes may still have
/// it mapped. That is safe on POSIX: an unlinked file keeps its pages until the
/// last mapping closes, and the next <see cref="CreateOrOpen"/> starts from a
/// fresh, zero-filled file.
/// </para>
/// <para>The factory is thread-safe and intended to be registered once as a singleton.</para>
/// </remarks>
public sealed class UnixFileMemoryMappedRegionFactory : IIpcMemoryRegionFactory
{
    private static readonly char[] InvalidNameCharacters = Path.GetInvalidFileNameChars()
        .Append(Path.DirectorySeparatorChar)
        .Append(Path.AltDirectorySeparatorChar)
        .Distinct()
        .ToArray();

    /// <summary>Creates a factory rooted at <paramref name="directory"/>.</summary>
    /// <param name="directory">
    /// Directory for the backing files; when null or empty, a <c>sparc</c>
    /// directory under the system temp path is used and created if needed.
    /// </param>
    public UnixFileMemoryMappedRegionFactory(string? directory = null)
    {
        DirectoryPath = string.IsNullOrWhiteSpace(directory)
            ? Path.Combine(Path.GetTempPath(), "sparc")
            : Path.GetFullPath(directory);

        if (OperatingSystem.IsWindows())
        {
            Directory.CreateDirectory(DirectoryPath);
        }
        else
        {
            // 0700: region files carry process data, so group/other access is
            // removed when the directory is created. An existing directory keeps
            // whatever mode its owner chose.
            Directory.CreateDirectory(DirectoryPath, OwnerOnlyDirectoryMode);
        }
    }

    private const UnixFileMode OwnerOnlyDirectoryMode =
        UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute;

    private const UnixFileMode OwnerOnlyFileMode =
        UnixFileMode.UserRead | UnixFileMode.UserWrite;

    /// <summary>Directory that holds one backing file per region.</summary>
    public string DirectoryPath { get; }

    /// <inheritdoc />
    public bool IsSupported =>
        !OperatingSystem.IsWindows() && !OperatingSystem.IsBrowser() && !OperatingSystem.IsWasi();

    /// <inheritdoc />
    public IIpcMemoryRegion CreateOrOpen(string name, long size, IpcRegionOptions? options = null)
    {
        EnsureSupported();
        RegionName.Validate(name);
        ArgumentOutOfRangeException.ThrowIfLessThan(size, 1);
        options ??= IpcRegionOptions.Default;
        EnsureNoSecurity(options);

        string path = PathFor(name);
        long startTimestamp = options.TimeProvider.GetTimestamp();

        if (!options.RequireExisting)
        {
            try
            {
                return new FileMemoryMappedRegion(name, CreateNew(path, size), isCreator: true);
            }
            catch (IOException)
            {
                // Already exists (or is being deleted): fall through to open.
            }
        }

        return new FileMemoryMappedRegion(
            name, OpenExistingWithTimeout(name, path, options, startTimestamp), isCreator: false);
    }

    /// <inheritdoc />
    public IIpcMemoryRegion OpenExisting(string name, IpcRegionOptions? options = null)
    {
        EnsureSupported();
        RegionName.Validate(name);
        options ??= IpcRegionOptions.Default;
        EnsureNoSecurity(options);

        string path = PathFor(name);
        long startTimestamp = options.TimeProvider.GetTimestamp();
        return new FileMemoryMappedRegion(
            name, OpenExistingWithTimeout(name, path, options, startTimestamp), isCreator: false);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Unlinks the backing file. Existing mappings keep their pages until they
    /// are closed, so callers must still be sure no live participant is using
    /// the region.
    /// </remarks>
    public bool TryReset(string name)
    {
        EnsureSupported();
        string path = PathFor(name);
        if (!File.Exists(path))
        {
            return false;
        }

        File.Delete(path);
        return true;
    }

    private static MemoryMappedFile CreateNew(string path, long size)
    {
        FileStreamOptions options = new()
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.ReadWrite,
            Share = FileShare.ReadWrite,
        };
        if (!OperatingSystem.IsWindows())
        {
            options.UnixCreateMode = OwnerOnlyFileMode;
        }

        FileStream stream = new(path, options);
        try
        {
            // CreateNew is atomic, and SetLength publishes the final length in
            // one step, so an opener never maps a half-sized file. Mapping from
            // this handle (instead of reopening the path) closes the window in
            // which another process could unlink or replace the file between
            // creation and mapping.
            stream.SetLength(size);
            return MemoryMappedFile.CreateFromFile(
                stream,
                mapName: null,
                capacity: 0,
                MemoryMappedFileAccess.ReadWrite,
                HandleInheritability.None,
                leaveOpen: false);
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    private static MemoryMappedFile OpenExistingWithTimeout(
        string name, string path, IpcRegionOptions options, long startTimestamp)
    {
        while (true)
        {
            try
            {
                if (File.Exists(path) && new FileInfo(path).Length > 0)
                {
                    // Capacity 0 means "use the file's current length"; the ring
                    // layer validates that the mapping covers the declared geometry.
                    return MapFile(path);
                }
            }
            catch (IOException)
            {
                // Transient (the file is being created or unlinked): retry.
            }

            if (options.TimeProvider.GetElapsedTime(startTimestamp) >= options.OpenTimeout)
            {
                throw new IpcTimeoutException(
                    $"Region '{name}' did not exist within {options.OpenTimeout.TotalMilliseconds:F0} ms.");
            }

            Thread.Sleep(2);
        }
    }

    private static MemoryMappedFile MapFile(string path) => MemoryMappedFile.CreateFromFile(
        path, FileMode.Open, mapName: null, capacity: 0, MemoryMappedFileAccess.ReadWrite);

    private string PathFor(string name)
    {
        if (name.IndexOfAny(InvalidNameCharacters) >= 0 || name is "." or "..")
        {
            throw new ArgumentException(
                $"Region name '{name}' must be a plain file name without directory separators.", nameof(name));
        }

        return Path.Combine(DirectoryPath, name);
    }

    private static void EnsureSupported()
    {
        if (OperatingSystem.IsWindows() || OperatingSystem.IsBrowser() || OperatingSystem.IsWasi())
        {
            throw new IpcPlatformNotSupportedException(
                "Sparc.UnixMemoryMapped uses file-backed memory-mapped files and is meant for Unix-like systems. " +
                "On Windows, use Sparc.WindowsMemoryMapped (named memory-mapped files) or Sparc.InMemory instead.");
        }
    }

    private static void EnsureNoSecurity(IpcRegionOptions options)
    {
        if (options.Security is not null)
        {
            // Fail instead of silently dropping the caller's access-control
            // request. Unix regions are already owner-only files (0600) in a
            // private directory, which is the transport-level boundary here.
            throw new IpcPlatformNotSupportedException(
                "Sparc.UnixMemoryMapped does not interpret IpcRegionOptions.Security; " +
                "Unix region files are protected by their owner-only mode (0600) and the region directory. " +
                "Use a Windows section security configuration only with Sparc.WindowsMemoryMapped.");
        }
    }
}
