using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparc;

namespace Sparc.UnitTests.Support;

/// <summary>
/// Test-only <see cref="IIpcMemoryRegionFactory"/> backed by pinned managed
/// arrays. It lets the ring protocol layer be exercised on any OS without
/// memory-mapped files; it deliberately has no persistence and no cross-process
/// visibility.
/// </summary>
internal sealed class InMemoryMemoryRegionFactory : IIpcMemoryRegionFactory
{
    private readonly ConcurrentDictionary<string, byte[]> _regions = new(StringComparer.Ordinal);
    private readonly ConcurrentBag<GCHandle> _pins = [];

    public bool IsSupported => true;

    public IIpcMemoryRegion CreateOrOpen(string name, long size, IpcRegionOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentOutOfRangeException.ThrowIfLessThan(size, 1);
        options ??= IpcRegionOptions.Default;

        if (options.RequireExisting)
        {
            return OpenExisting(name, options);
        }

        if (_regions.TryGetValue(name, out byte[]? existing))
        {
            return new Region(name, existing, isCreator: false);
        }

        byte[] fresh = Allocate(size);
        if (_regions.TryAdd(name, fresh))
        {
            return new Region(name, fresh, isCreator: true);
        }

        _regions.TryGetValue(name, out byte[]? winner);
        return new Region(name, winner!, isCreator: false);
    }

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

    public bool TryReset(string name) => _regions.TryRemove(name, out _);

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

        public unsafe byte* Pointer =>
            (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buffer));

        public void Dispose()
        {
            Interlocked.Exchange(ref _disposed, 1);
        }
    }
}
