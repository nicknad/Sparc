using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Sparc.WindowsMemoryMapped;

/// <summary>
/// Direct kernel32 access to Windows section objects (memory-mapped files):
/// creation with an explicit security descriptor, view mapping, and unmapping.
/// </summary>
/// <remarks>
/// The factory uses these calls instead of <see cref="System.IO.MemoryMappedFiles.MemoryMappedFile"/>
/// for <b>creation</b> because the BCL offers no way to attach a
/// <c>SECURITY_ATTRIBUTES</c> descriptor to a new named map. Opening an
/// existing map still goes through the BCL, which requests only
/// read/write-section access and therefore works under a restrictive DACL.
/// </remarks>
internal static class WindowsSectionApi
{
    internal const int ErrorAccessDenied = 5;
    internal const int ErrorAlreadyExists = 183;

    private const uint PageReadWrite = 0x04;
    private const uint FileMapWrite = 0x0002;
    private const uint FileMapRead = 0x0004;

    private static readonly IntPtr InvalidHandleValue = new(-1);

    /// <summary>
    /// Creates a named section, or throws <see cref="Win32Exception"/> with
    /// <see cref="ErrorAlreadyExists"/> when the name is taken (the handle is
    /// closed before the exception) or <see cref="ErrorAccessDenied"/> when an
    /// existing section's DACL does not grant the access creation requests.
    /// </summary>
    internal static SafeFileHandle CreateNamedSection(string name, long size, WindowsSectionSecurity? security)
    {
        return CreateSectionCore(name, size, security, inheritHandle: false);
    }

    /// <summary>Creates an unnamed section reachable only through its handle.</summary>
    internal static SafeFileHandle CreateUnnamedSection(long size, WindowsSectionSecurity? security, bool inheritHandle)
    {
        return CreateSectionCore(name: null, size, security, inheritHandle);
    }

    /// <summary>Maps the whole section read/write and returns the view base address.</summary>
    internal static unsafe byte* MapReadWriteView(SafeFileHandle sectionHandle)
    {
        bool referenced = false;
        try
        {
            sectionHandle.DangerousAddRef(ref referenced);
            IntPtr view = MapViewOfFile(
                sectionHandle.DangerousGetHandle(), FileMapRead | FileMapWrite, 0, 0, 0);
            if (view == IntPtr.Zero)
            {
                throw new Win32Exception(
                    Marshal.GetLastWin32Error(), "MapViewOfFile failed for the section handle.");
            }

            return (byte*)view;
        }
        finally
        {
            if (referenced)
            {
                sectionHandle.DangerousRelease();
            }
        }
    }

    /// <summary>
    /// Size of the mapped region as the OS reports it. For an opened section
    /// this is page-rounded (the bytes are genuinely accessible).
    /// </summary>
    internal static unsafe long QueryViewSize(byte* view)
    {
        nuint result = VirtualQuery(
            (IntPtr)view, out MemoryBasicInformation information, (nuint)sizeof(MemoryBasicInformation));
        if (result == 0)
        {
            throw new Win32Exception(Marshal.GetLastWin32Error(), "VirtualQuery failed for the section view.");
        }

        return (long)information.RegionSize;
    }

    /// <summary>Releases a view. Best effort on dispose; the OS reclaims it regardless.</summary>
    internal static unsafe void UnmapView(byte* view)
    {
        if (!UnmapViewOfFile((IntPtr)view))
        {
            Debug.Fail($"UnmapViewOfFile failed with error {Marshal.GetLastWin32Error()}.");
        }
    }

    private static unsafe SafeFileHandle CreateSectionCore(
        string? name, long size, WindowsSectionSecurity? security, bool inheritHandle)
    {
        if (security is null)
        {
            return CreateWithDescriptor(name, size, IntPtr.Zero, inheritHandle);
        }

        // The descriptor is pinned for the duration of CreateFileMapping only;
        // the kernel copies the SECURITY_ATTRIBUTES descriptor into the object.
        fixed (byte* descriptor = security.DescriptorBytes)
        {
            return CreateWithDescriptor(name, size, (IntPtr)descriptor, inheritHandle);
        }
    }

    private static SafeFileHandle CreateWithDescriptor(
        string? name, long size, IntPtr securityDescriptor, bool inheritHandle)
    {
        SecurityAttributes attributes = new()
        {
            Length = Marshal.SizeOf<SecurityAttributes>(),
            SecurityDescriptor = securityDescriptor,
            InheritHandle = inheritHandle ? 1 : 0,
        };

        IntPtr handle = CreateFileMappingW(
            InvalidHandleValue,
            ref attributes,
            PageReadWrite,
            (uint)((ulong)size >> 32),
            (uint)size,
            name);

        int error = Marshal.GetLastWin32Error();
        if (handle == IntPtr.Zero)
        {
            throw new Win32Exception(error, $"CreateFileMapping('{name ?? "<unnamed>"}') failed.");
        }

        if (error == ErrorAlreadyExists)
        {
            // The name is taken; close the joined handle so callers can fall
            // through to the plain read/write open path.
            CloseHandle(handle);
            throw new Win32Exception(error, $"Section '{name}' already exists.");
        }

        return new SafeFileHandle(handle, ownsHandle: true);
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct SecurityAttributes
    {
        public int Length;
        public IntPtr SecurityDescriptor;
        public int InheritHandle;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct MemoryBasicInformation
    {
        public IntPtr BaseAddress;
        public IntPtr AllocationBase;
        public uint AllocationProtect;
        public nuint RegionSize;
        public uint State;
        public uint Protect;
        public uint Type;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern IntPtr CreateFileMappingW(
        IntPtr hFile,
        ref SecurityAttributes attributes,
        uint protect,
        uint maximumSizeHigh,
        uint maximumSizeLow,
        string? name);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern IntPtr MapViewOfFile(
        IntPtr mapping, uint desiredAccess, uint offsetHigh, uint offsetLow, nuint numberOfBytesToMap);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool UnmapViewOfFile(IntPtr baseAddress);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern nuint VirtualQuery(
        IntPtr address, out MemoryBasicInformation information, nuint length);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool CloseHandle(IntPtr handle);
}
