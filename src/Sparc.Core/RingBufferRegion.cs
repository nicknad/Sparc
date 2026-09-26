using System.Runtime.CompilerServices;
using Sparc;

namespace Sparc.Core;

/// <summary>
/// Ring-buffer protocol layer on top of an <see cref="IIpcMemoryRegion"/>: owns
/// the region instance, initializes or validates the header, and exposes the
/// atomic cursor/state accessors used by <see cref="SharedRingBuffer"/>.
/// </summary>
/// <remarks>
/// This type is platform-agnostic; the operating-system specifics live in an
/// <see cref="IIpcMemoryRegionFactory"/> implementation (for example
/// named memory-mapped files on Windows).
/// </remarks>
public sealed class RingBufferRegion : IDisposable
{
    private readonly IIpcMemoryRegion _region;
    private int _disposed;

    private RingBufferRegion(
        IIpcMemoryRegion region,
        int capacity,
        int slotSize,
        bool adoptExistingGeometry,
        TimeSpan openTimeout,
        TimeProvider timeProvider)
    {
        _region = region;

        try
        {
            if (region.IsCreator)
            {
                Initialize(capacity, slotSize);
            }
            else
            {
                OpenAndValidate(capacity, slotSize, adoptExistingGeometry, openTimeout, timeProvider);
            }

            Header = ReadHeader();
            Capacity = Header.Capacity;
            SlotSize = Header.SlotSize;
            MaxPayloadSize = Header.MaxPayloadSize;
            Size = RingBufferLayout.RequiredSize(Capacity, SlotSize);
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <summary>The region name.</summary>
    public string Name => _region.Name;

    /// <summary>True when this process created the region.</summary>
    public bool IsCreator => _region.IsCreator;

    /// <summary>Bytes accessible in the region.</summary>
    public long Size { get; }

    /// <summary>Number of slots.</summary>
    public int Capacity { get; }

    /// <summary>Bytes per slot.</summary>
    public int SlotSize { get; }

    /// <summary>Maximum payload per slot.</summary>
    public int MaxPayloadSize { get; }

    /// <summary>A managed snapshot of the header taken when the region was opened.</summary>
    public RingBufferHeader Header { get; }

    /// <summary>Raw pointer to the first byte of the region.</summary>
    public unsafe byte* Pointer => _region.Pointer;

    /// <summary>Producer-owned write cursor. Access with atomics.</summary>
    public unsafe ref long TailRef => ref Unsafe.AsRef<long>(_region.Pointer + RingBufferLayout.TailOffset);

    /// <summary>Consumer-owned read cursor. Access with atomics.</summary>
    public unsafe ref long HeadRef => ref Unsafe.AsRef<long>(_region.Pointer + RingBufferLayout.HeadOffset);

    /// <summary>
    /// Creates the region if needed, otherwise opens and validates the existing
    /// one. Either endpoint may call this, so start order does not matter.
    /// </summary>
    public static RingBufferRegion CreateOrOpen(
        IIpcMemoryRegionFactory factory,
        string name,
        int capacity = RingBufferLayout.DefaultCapacity,
        int slotSize = RingBufferLayout.DefaultSlotSize,
        SharedRingBufferOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentException.ThrowIfNullOrEmpty(name);
        RingBufferLayout.ValidateGeometry(capacity, slotSize);
        options ??= SharedRingBufferOptions.Default;

        IpcRegionOptions regionOptions = new()
        {
            OpenTimeout = options.OpenTimeout,
            RequireExisting = options.RequireExisting,
            TimeProvider = options.TimeProvider,
        };

        long requiredSize = RingBufferLayout.RequiredSize(capacity, slotSize);
        bool recreateAttempted = false;

        while (true)
        {
            IIpcMemoryRegion region;
            try
            {
                region = factory.CreateOrOpen(name, requiredSize, regionOptions);
            }
            catch (IpcTimeoutException exception)
            {
                throw new RingBufferTimeoutException(exception.Message, exception);
            }
            catch (IpcPlatformNotSupportedException exception)
            {
                throw new RingBufferPlatformNotSupportedException(exception.Message, exception);
            }

            try
            {
                return new RingBufferRegion(
                    region, capacity, slotSize, options.AdoptExistingGeometry, options.OpenTimeout, options.TimeProvider);
            }
            catch (RingBufferCorruptedException) when (options.RecreateIfStale && !recreateAttempted)
            {
                // Drop our handle, let the factory reclaim any backing store,
                // then create from scratch on the next iteration.
                region.Dispose();
                recreateAttempted = true;
                factory.TryReset(name);
            }
        }
    }

    /// <summary>Opens an existing region; never creates one.</summary>
    public static RingBufferRegion OpenExisting(
        IIpcMemoryRegionFactory factory,
        string name,
        int capacity = RingBufferLayout.DefaultCapacity,
        int slotSize = RingBufferLayout.DefaultSlotSize,
        SharedRingBufferOptions? options = null)
    {
        options ??= SharedRingBufferOptions.Default;
        return CreateOrOpen(factory, name, capacity, slotSize, new SharedRingBufferOptions
        {
            OpenTimeout = options.OpenTimeout,
            RecreateIfStale = false,
            RequireExisting = true,
            AdoptExistingGeometry = options.AdoptExistingGeometry,
            TimeProvider = options.TimeProvider,
        });
    }

    /// <summary>Reads the current header snapshot from the region.</summary>
    public RingBufferHeader ReadHeader()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        unsafe
        {
            return RingBufferHeader.Read(new ReadOnlySpan<byte>(_region.Pointer, RingBufferLayout.HeaderSize));
        }
    }

    /// <summary>Reads an endpoint state with acquire semantics.</summary>
    public RingBufferEndpointState ReadEndpointState(RingBufferEndpointRole role)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        unsafe
        {
            return (RingBufferEndpointState)Volatile.Read(ref Unsafe.AsRef<int>(_region.Pointer + StateOffset(role)));
        }
    }

    /// <summary>Writes an endpoint state with release semantics.</summary>
    public void WriteEndpointState(RingBufferEndpointRole role, RingBufferEndpointState state)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        unsafe
        {
            Volatile.Write(ref Unsafe.AsRef<int>(_region.Pointer + StateOffset(role)), (int)state);
        }
    }

    /// <summary>
    /// Atomically transitions an endpoint state if it currently equals
    /// <paramref name="expected"/>, returning the previous value.
    /// </summary>
    public RingBufferEndpointState CompareExchangeEndpointState(
        RingBufferEndpointRole role, RingBufferEndpointState desired, RingBufferEndpointState expected)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        unsafe
        {
            return (RingBufferEndpointState)Interlocked.CompareExchange(
                ref Unsafe.AsRef<int>(_region.Pointer + StateOffset(role)), (int)desired, (int)expected);
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _region.Dispose();
    }

    private static int StateOffset(RingBufferEndpointRole role) => role switch
    {
        RingBufferEndpointRole.Producer => RingBufferLayout.ProducerStateOffset,
        RingBufferEndpointRole.Consumer => RingBufferLayout.ConsumerStateOffset,
        _ => throw new ArgumentOutOfRangeException(nameof(role), role, null),
    };

    private unsafe void Initialize(int capacity, int slotSize)
    {
        RingBufferHeader header = RingBufferHeader.CreateNew(capacity, slotSize);
        Span<byte> span = new(_region.Pointer, RingBufferLayout.HeaderSize);

        // Write everything except the magic, then publish the magic with release
        // semantics. An opener that observes the magic is guaranteed to observe
        // the geometry and the zeroed cursors.
        header.WriteTo(span, includeMagic: false);
        Volatile.Write(ref Unsafe.AsRef<ulong>(_region.Pointer + RingBufferLayout.MagicOffset), RingBufferLayout.Magic);
    }

    private unsafe void OpenAndValidate(
        int capacity, int slotSize, bool adoptExistingGeometry, TimeSpan openTimeout, TimeProvider timeProvider)
    {
        if (_region.Size < RingBufferLayout.HeaderSize)
        {
            throw new RingBufferCorruptedException(
                $"Region '{Name}' is only {_region.Size} bytes; too small to hold a ring buffer header.");
        }

        long startTimestamp = timeProvider.GetTimestamp();
        while (true)
        {
            ulong magic = Volatile.Read(ref Unsafe.AsRef<ulong>(_region.Pointer + RingBufferLayout.MagicOffset));
            if (magic == RingBufferLayout.Magic)
            {
                break;
            }

            if (timeProvider.GetElapsedTime(startTimestamp) >= openTimeout)
            {
                throw new RingBufferCorruptedException(
                    $"Region '{Name}' exists but was not initialized within " +
                    $"{openTimeout.TotalMilliseconds:F0} ms (bad magic 0x{magic:X16}). " +
                    "The creating process probably crashed during startup, or the region is stale. " +
                    "Re-run with stale-region recreation enabled to reclaim it.");
            }

            Thread.Sleep(2);
        }

        // The acquire above orders all subsequent header reads after the creator's
        // release of the magic value.
        RingBufferHeader header = RingBufferHeader.Read(new ReadOnlySpan<byte>(_region.Pointer, RingBufferLayout.HeaderSize));
        header.Validate(Name);
        if (!adoptExistingGeometry)
        {
            header.ValidateRequestedGeometry(Name, capacity, slotSize);
        }

        if (_region.Size < RingBufferLayout.RequiredSize(header.Capacity, header.SlotSize))
        {
            throw new RingBufferCorruptedException(
                $"Region '{Name}' maps {_region.Size} bytes but its header declares " +
                $"{RingBufferLayout.RequiredSize(header.Capacity, header.SlotSize)} bytes.");
        }
    }
}
