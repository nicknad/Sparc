namespace Sparc;

/// <summary>
/// Creates or opens <see cref="IIpcMemoryRegion"/> instances. This is the single
/// seam through which the ring buffer code reaches the operating system, so an
/// application (CLI, web app, service) chooses the implementation once and the
/// rest of the stack stays platform-agnostic.
/// </summary>
public interface IIpcMemoryRegionFactory
{
    /// <summary>
    /// True when this factory can operate on the current platform. Callers can
    /// fail fast or select a different factory instead of catching a
    /// <see cref="IpcPlatformNotSupportedException"/> mid-operation.
    /// </summary>
    bool IsSupported { get; }

    /// <summary>
    /// Creates the region if it does not exist, otherwise opens the existing
    /// one. Implementations must be safe against concurrent creation by another
    /// process and must wait up to
    /// <see cref="IpcRegionOptions.OpenTimeout"/> for a region to appear.
    /// </summary>
    IIpcMemoryRegion CreateOrOpen(string name, long size, IpcRegionOptions? options = null);

    /// <summary>Opens an existing region; never creates one.</summary>
    IIpcMemoryRegion OpenExisting(string name, IpcRegionOptions? options = null);

    /// <summary>
    /// Best-effort removal of a stale region so it can be recreated: on Windows
    /// a named map disappears with its last handle (no-op), on Unix-like
    /// implementations this would unlink the backing store. Callers must be sure
    /// no live participant is using the region.
    /// </summary>
    /// <returns>True when a backing store was actually removed.</returns>
    bool TryReset(string name);
}
