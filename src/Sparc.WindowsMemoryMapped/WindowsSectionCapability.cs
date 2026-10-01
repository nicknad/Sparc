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
/// <para>Dispose the capability after the transfer; it closes the handle and releases the local view.</para>
/// </remarks>
public sealed class WindowsSectionCapability : IDisposable
{
    private readonly SafeFileHandle _handle;
    private readonly WindowsSectionMemoryRegion _region;
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

    /// <summary>
    /// A read/write view of the section for this process. Adopting it into an
    /// endpoint transfers ownership of the view; the capability still closes
    /// the section handle on <see cref="Dispose"/>.
    /// </summary>
    public IIpcMemoryRegion Region => _region;

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _region.Dispose();
        _handle.Dispose();
    }
}
