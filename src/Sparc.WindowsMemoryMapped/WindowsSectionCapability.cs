using Microsoft.Win32.SafeHandles;

namespace Sparc.WindowsMemoryMapped;

/// <summary>
/// Capability-style access to a Windows section: the section is created
/// unnamed, so no other process can discover or open it by name. The only way
/// to join is to receive the section <see cref="Handle"/> (handle inheritance
/// or <c>DuplicateHandle</c>) and call <see cref="WindowsUnnamedSection.MapHandle"/>.
/// </summary>
/// <remarks>
/// <para>
/// Knowledge of a channel identifier alone never grants access in this mode:
/// there is no name in the OS namespace to guess. The DACL from an optional
/// <see cref="WindowsSectionSecurity"/> still applies as defence in depth.
/// </para>
/// <para>Dispose the capability after the transfer; it closes the handle and releases a local view that was not taken.</para>
/// </remarks>
public sealed class WindowsSectionCapability : IDisposable
{
    private readonly SafeFileHandle _handle;
    private WindowsSectionMemoryRegion? _region;
    private int _disposed;

    internal WindowsSectionCapability(SafeFileHandle handle, WindowsSectionMemoryRegion region)
    {
        _handle = handle;
        _region = region;
    }

    /// <summary>
    /// The unnamed section handle to transfer to the peer (handle inheritance
    /// or <c>DuplicateHandle</c>). The capability owns it until disposed.
    /// </summary>
    public SafeFileHandle Handle => _handle;

    /// <summary>A read/write view of the section for this process.</summary>
    public IIpcMemoryRegion Region =>
        Volatile.Read(ref _region) ?? throw new ObjectDisposedException(nameof(WindowsSectionCapability));

    /// <summary>
    /// Transfers ownership of the local view to the caller. The capability will
    /// no longer release it on <see cref="Dispose"/>; only the section handle is
    /// still owned.
    /// </summary>
    public IIpcMemoryRegion TakeRegion() =>
        Interlocked.Exchange(ref _region, null)
            ?? throw new ObjectDisposedException(nameof(WindowsSectionCapability));

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        Interlocked.Exchange(ref _region, null)?.Dispose();
        _handle.Dispose();
    }
}
