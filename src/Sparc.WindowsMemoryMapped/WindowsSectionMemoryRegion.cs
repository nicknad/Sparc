using Microsoft.Win32.SafeHandles;

namespace Sparc.WindowsMemoryMapped;

/// <summary>
/// An <see cref="IIpcMemoryRegion"/> over a raw Windows section object: a
/// direct <c>MapViewOfFile</c> view, optionally holding the section handle that
/// keeps a named section alive.
/// </summary>
/// <remarks>
/// The data path is identical to the BCL-backed region: the ring protocol only
/// ever touches <see cref="Pointer"/>. Security is applied when the section is
/// created; nothing on this type consults it afterwards.
/// </remarks>
internal sealed unsafe class WindowsSectionMemoryRegion : IIpcMemoryRegion
{
    private readonly SafeFileHandle? _sectionHandle;
    private readonly bool _ownsHandle;
    private byte* _view;
    private int _disposed;

    internal WindowsSectionMemoryRegion(
        string name,
        SafeFileHandle? sectionHandle,
        bool ownsHandle,
        byte* view,
        long size,
        bool isCreator)
    {
        Name = name;
        _sectionHandle = sectionHandle;
        _ownsHandle = ownsHandle;
        _view = view;
        Size = size;
        IsCreator = isCreator;
    }

    /// <summary>
    /// Creates a named section with the optional security descriptor and maps
    /// it. Throws <see cref="System.ComponentModel.Win32Exception"/> with
    /// <c>ERROR_ALREADY_EXISTS</c> when the name is taken.
    /// </summary>
    internal static WindowsSectionMemoryRegion CreateNamed(
        string name, long size, WindowsSectionSecurity? security)
    {
        SafeFileHandle handle = WindowsSectionApi.CreateNamedSection(name, size, security);
        try
        {
            byte* view = WindowsSectionApi.MapReadWriteView(handle);
            return new WindowsSectionMemoryRegion(
                name, handle, ownsHandle: true, view, size, isCreator: true);
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }

    public string Name { get; }

    public bool IsCreator { get; }

    public long Size { get; }

    public byte* Pointer
    {
        get
        {
            // Matches InMemoryMemoryRegionFactory.Region: fail loudly rather
            // than hand out a pointer into an unmapped view.
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            return _view;
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        byte* view = _view;
        _view = null;
        if (view is not null)
        {
            WindowsSectionApi.UnmapView(view);
        }

        if (_ownsHandle)
        {
            _sectionHandle?.Dispose();
        }
    }
}
