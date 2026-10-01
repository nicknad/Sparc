using Microsoft.Win32.SafeHandles;

namespace Sparc.WindowsMemoryMapped;

/// <summary>
/// Creates unnamed Windows sections and maps views of transferred section
/// handles. This is the capability-style alternative to
/// <see cref="WindowsNamedMemoryMappedRegionFactory"/>: nothing is published
/// in the OS namespace, so only a process that received the HANDLE can map the
/// region.
/// </summary>
/// <remarks>
/// <para>
/// Handle transfer is OS-specific and deliberately not hidden behind the
/// cross-platform <see cref="IIpcMemoryRegionFactory"/> abstraction. A parent
/// process can create the section with <c>inheritHandle: true</c> and pass the
/// handle value to a child, or duplicate the handle into an already running
/// process with <c>DuplicateHandle</c>. The receiving process calls
/// <see cref="MapHandle"/> and adopts the returned region with the
/// <c>SparcRing.OpenProducer(IIpcMemoryRegion, ...)</c> / <c>OpenConsumer</c>
/// overloads in <c>Sparc.Core</c>.
/// </para>
/// <para>
/// The section handle keeps the section alive until the last handle and view
/// close, so the capability must stay undisposed until the peer has mapped it
/// (or another handle/view is outstanding).
/// </para>
/// </remarks>
public static class WindowsUnnamedSection
{
    /// <summary>Display name used by unnamed regions; unnamed sections have no OS name.</summary>
    public const string UnnamedRegionName = "(unnamed section)";

    /// <summary>
    /// Creates an unnamed section with the optional security descriptor and
    /// maps it read/write. The returned capability owns both the handle and
    /// the local view.
    /// </summary>
    /// <param name="size">Section size in bytes.</param>
    /// <param name="security">
    /// Optional restrictive DACL. Unnamed sections are already unreachable by
    /// name; the descriptor adds defence in depth against handle guessing.
    /// </param>
    /// <param name="inheritHandle">
    /// When true the handle is inheritable, so a child process created with
    /// handle inheritance receives it without an explicit <c>DuplicateHandle</c>.
    /// </param>
    public static WindowsSectionCapability Create(
        long size,
        WindowsSectionSecurity? security = null,
        bool inheritHandle = false)
    {
        EnsureSupported();
        ArgumentOutOfRangeException.ThrowIfLessThan(size, 1);

        SafeFileHandle handle = WindowsSectionApi.CreateUnnamedSection(size, security, inheritHandle);
        try
        {
            unsafe
            {
                byte* view = WindowsSectionApi.MapReadWriteView(handle);
                WindowsSectionMemoryRegion region = new(
                    UnnamedRegionName,
                    sectionHandle: null,
                    ownsHandle: false,
                    view,
                    size,
                    isCreator: true);
                return new WindowsSectionCapability(handle, region);
            }
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Maps a read/write view of a section handle received from another
    /// process. The caller keeps ownership of <paramref name="sectionHandle"/>:
    /// the view stays valid after the handle is closed.
    /// </summary>
    /// <param name="sectionHandle">Section handle received via inheritance or <c>DuplicateHandle</c>.</param>
    public static IIpcMemoryRegion MapHandle(SafeFileHandle sectionHandle)
    {
        EnsureSupported();
        ArgumentNullException.ThrowIfNull(sectionHandle);
        if (sectionHandle.IsInvalid || sectionHandle.IsClosed)
        {
            throw new ArgumentException("The section handle is invalid or closed.", nameof(sectionHandle));
        }

        unsafe
        {
            byte* view = WindowsSectionApi.MapReadWriteView(sectionHandle);
            try
            {
                long size = WindowsSectionApi.QueryViewSize(view);
                return new WindowsSectionMemoryRegion(
                    UnnamedRegionName,
                    sectionHandle: null,
                    ownsHandle: false,
                    view,
                    size,
                    isCreator: false);
            }
            catch
            {
                WindowsSectionApi.UnmapView(view);
                throw;
            }
        }
    }

    private static void EnsureSupported()
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new IpcPlatformNotSupportedException(
                "Unnamed sections and handle transfer are Windows-only; on Unix, use a private directory " +
                "of file-backed regions (Sparc.UnixMemoryMapped) instead.");
        }
    }
}
