namespace Ipc;

/// <summary>
/// A region of memory shared between independent processes. Implementations are
/// provided per operating system (for example named memory-mapped files on
/// Windows); the ring buffer protocol itself only depends on this contract.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Pointer"/> is intentionally an unmanaged <c>byte*</c>: cross-process
/// atomic operations require stable raw addresses, which managed spans and
/// <c>MemoryMappedViewAccessor.Read</c> cannot express.
/// </para>
/// <para>
/// A region instance owns one mapping/binding. Dispose it exactly once; the
/// underlying kernel object or file may outlive the instance while other
/// processes or views still reference it.
/// </para>
/// </remarks>
public interface IIpcMemoryRegion : IDisposable
{
    /// <summary>The name the region was opened/created under.</summary>
    string Name { get; }

    /// <summary>True when this instance created (rather than joined) the region.</summary>
    bool IsCreator { get; }

    /// <summary>Number of bytes accessible through <see cref="Pointer"/>.</summary>
    long Size { get; }

    /// <summary>Raw pointer to the first byte of the mapping.</summary>
    unsafe byte* Pointer { get; }
}
