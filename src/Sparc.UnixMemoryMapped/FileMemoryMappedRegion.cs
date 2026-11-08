using System.IO.MemoryMappedFiles;
using Sparc;

namespace Sparc.UnixMemoryMapped;

/// <summary>An <see cref="IIpcMemoryRegion"/> backed by a file-backed memory mapping.</summary>
internal sealed class FileMemoryMappedRegion : IIpcMemoryRegion
{
    private readonly MemoryMappedFile _file;
    private readonly MemoryMappedViewAccessor _accessor;
    private int _disposed;

    internal FileMemoryMappedRegion(string name, MemoryMappedFile file, bool isCreator)
    {
        Name = name;
        IsCreator = isCreator;
        _file = file;

        MemoryMappedViewAccessor? accessor = null;
        try
        {
            accessor = file.CreateViewAccessor(0, 0, MemoryMappedFileAccess.ReadWrite);
            unsafe
            {
                byte* pointer = null;
                accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
                Pointer = pointer + accessor.PointerOffset;
            }

            Size = accessor.Capacity;
        }
        catch
        {
            accessor?.Dispose();
            file.Dispose();
            throw;
        }

        _accessor = accessor;
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
            _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }

        _accessor.Dispose();
        _file.Dispose();
    }
}
