using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparc;

namespace Sparc.InMemory;

/// <summary>
/// <see cref="IIpcMemoryRegionFactory"/> implementation backed by pinned
/// managed arrays instead of an operating-system mapping. It lets the ring
/// protocol and the session layer be exercised on any OS without
/// memory-mapped files.
/// </summary>
/// <remarks>
/// <para>
/// Regions are process-local and scoped to one factory instance: another
/// factory instance (in this or any other process) does not see them, and
/// nothing survives the process. Use it for tests, samples and
/// single-process development; it is not a transport between processes.
/// <see cref="IpcRegionOptions.Security"/> is accepted and has no effect:
/// there is no peer that could be denied access.
/// </para>
/// <para>
/// Backing arrays are pinned for the lifetime of the process so a
/// <see cref="IIpcMemoryRegion.Pointer"/> never goes stale, including across
/// <see cref="TryReset"/>. The retained memory is bounded by the number and
/// size of the regions a process creates.
/// </para>
/// <para>
/// The factory is thread-safe and intended to be registered once as a
/// singleton.
/// </para>
/// </remarks>
public sealed class InMemoryMemoryRegionFactory : IIpcMemoryRegionFactory
{
    private readonly ConcurrentDictionary<string, byte[]> _regions = new(StringComparer.Ordinal);
    private readonly ConcurrentBag<GCHandle> _pins = [];

    /// <inheritdoc />
    public bool IsSupported => true;

    /// <inheritdoc />
    public IIpcMemoryRegion CreateOrOpen(string name, long size, IpcRegionOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentOutOfRangeException.ThrowIfLessThan(size, 1);
        options ??= IpcRegionOptions.Default;

        if (options.RequireExisting)
        {
            return OpenExisting(name, options);
        }

        while (true)
        {
            if (_regions.TryGetValue(name, out byte[]? existing))
            {
                return new Region(name, existing, isCreator: false);
            }

            byte[] fresh = Allocate(size);
            if (_regions.TryAdd(name, fresh))
            {
                return new Region(name, fresh, isCreator: true);
            }

            // Lost the create race: join the winner on the next loop; if that
            // winner was concurrently reset, create again rather than ever
            // constructing a region over a null buffer.
        }
    }

    /// <inheritdoc />
    public IIpcMemoryRegion OpenExisting(string name, IpcRegionOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        options ??= IpcRegionOptions.Default;

        long startTimestamp = options.TimeProvider.GetTimestamp();
        byte[]? buffer;
        while (!_regions.TryGetValue(name, out buffer))
        {
            if (options.TimeProvider.GetElapsedTime(startTimestamp) >= options.OpenTimeout)
            {
                throw new IpcTimeoutException(
                    $"Region '{name}' did not exist within {options.OpenTimeout.TotalMilliseconds:F0} ms.");
            }

            Thread.Sleep(1);
        }

        return new Region(name, buffer, isCreator: false);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Removes the region so the next <see cref="CreateOrOpen"/> starts from a
    /// zero-filled buffer. The removed buffer stays pinned: existing region
    /// instances keep valid pointers, and callers must still be sure no live
    /// participant is using the region.
    /// </remarks>
    public bool TryReset(string name) => _regions.TryRemove(name, out _);

    /// <summary>
    /// Frees every pinned backing array. Only safe once no region instance is
    /// in use; the test harness calls this when a per-scenario factory is torn
    /// down. The public API never unpins, so a region pointer stays valid for
    /// the process lifetime.
    /// </summary>
    internal void ReleasePinnedBuffers()
    {
        while (_pins.TryTake(out GCHandle handle))
        {
            handle.Free();
        }
    }

    private byte[] Allocate(long size)
    {
        byte[] buffer = new byte[size];
        _pins.Add(GCHandle.Alloc(buffer, GCHandleType.Pinned));
        return buffer;
    }

    private sealed class Region : IIpcMemoryRegion
    {
        private readonly byte[] _buffer;
        private int _disposed;

        internal Region(string name, byte[] buffer, bool isCreator)
        {
            Name = name;
            _buffer = buffer;
            IsCreator = isCreator;
        }

        public string Name { get; }

        public bool IsCreator { get; }

        public long Size => _buffer.Length;

        public unsafe byte* Pointer
        {
            get
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
                return (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buffer));
            }
        }

        public void Dispose() => Interlocked.Exchange(ref _disposed, 1);
    }
}
