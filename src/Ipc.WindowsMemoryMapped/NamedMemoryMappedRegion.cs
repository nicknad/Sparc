using System.IO.MemoryMappedFiles;
using Ipc;

namespace Ipc.WindowsMemoryMapped;

/// <summary>An <see cref="IIpcMemoryRegion"/> backed by a Windows named memory-mapped file.</summary>
internal sealed class NamedMemoryMappedRegion : IIpcMemoryRegion
{
    private readonly MemoryMappedFile _file;
    private readonly MemoryMappedViewAccessor _accessor;
    private readonly bool _pointerAcquired;
    private int _disposed;

    internal NamedMemoryMappedRegion(string name, MemoryMappedFile file, bool isCreator)
    {
        Name = name;
        IsCreator = isCreator;
        _file = file;

        _accessor = file.CreateViewAccessor(0, 0, MemoryMappedFileAccess.ReadWrite);
        unsafe
        {
            byte* pointer = null;
            _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
            _pointerAcquired = true;
            Pointer = pointer + _accessor.PointerOffset;
        }

        Size = _accessor.Capacity;
    }

    public string Name { get; }

    public bool IsCreator { get; }

    public long Size { get; }

    public unsafe byte* Pointer { get; }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        unsafe
        {
            if (_pointerAcquired)
            {
                _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }

        _accessor.Dispose();
        _file.Dispose();
    }
}
