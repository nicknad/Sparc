using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using RingBuffer.Core;

namespace RingBuffer.SharedMemory;

/// <summary>
/// Owns a named <see cref="MemoryMappedFile"/> and the raw pointer to its first
/// byte, exposing strongly-typed <c>ref</c> accessors for the atomic header
/// fields.
/// </summary>
/// <remarks>
/// <para>
/// The region is created on first use and opened by everyone else. Creation and
/// initialization are separated so that a process that opens a half-initialized
/// region simply waits for the magic value, which the creator publishes last
/// with release semantics.
/// </para>
/// <para>
/// <b>Platform:</b> this implementation uses named memory-mapped files. .NET
/// supports named maps on Windows only (on Unix <c>CreateNew(name, ...)</c> and
/// <c>OpenExisting(name)</c> throw <see cref="PlatformNotSupportedException"/>;
/// see dotnet/runtime MemoryMappedFile.Unix.cs). A named map is a kernel object:
/// it disappears when the last handle closes, so a crashed creator leaves
/// nothing behind, and <see cref="SharedMemoryOptions.RecreateIfStale"/> can
/// reclaim a half-initialized region simply by closing our handle and creating a
/// new one.
/// </para>
/// </remarks>
public sealed class SharedMemoryRegion : IDisposable
{
    private readonly MemoryMappedFile _file;
    private readonly MemoryMappedViewAccessor _accessor;
    private readonly bool _pointerAcquired;
    private int _disposed;

    private unsafe SharedMemoryRegion(
        string name,
        MemoryMappedFile file,
        bool isCreator,
        int capacity,
        int slotSize,
        SharedMemoryOptions options,
        long startTimestamp)
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

        try
        {
            if (isCreator)
            {
                Initialize(capacity, slotSize);
            }
            else
            {
                OpenAndValidate(capacity, slotSize, options, startTimestamp);
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

    /// <summary>The OS-level name of the region.</summary>
    public string Name { get; }

    /// <summary>True when this instance created (and initialized) the region.</summary>
    public bool IsCreator { get; }

    /// <summary>Raw pointer to the first byte of the mapped region.</summary>
    public unsafe byte* Pointer { get; }

    /// <summary>Total bytes of the region.</summary>
    public long Size { get; }

    /// <summary>Number of slots.</summary>
    public int Capacity { get; }

    /// <summary>Bytes per slot.</summary>
    public int SlotSize { get; }

    /// <summary>Maximum payload per slot.</summary>
    public int MaxPayloadSize { get; }

    /// <summary>A managed snapshot of the header taken when the region was opened.</summary>
    public RingBufferHeader Header { get; }

    /// <summary>Producer-owned write cursor in the shared memory. Use atomics to access it.</summary>
    public unsafe ref long TailRef => ref Unsafe.AsRef<long>(Pointer + RingBufferLayout.TailOffset);

    /// <summary>Consumer-owned read cursor in the shared memory. Use atomics to access it.</summary>
    public unsafe ref long HeadRef => ref Unsafe.AsRef<long>(Pointer + RingBufferLayout.HeadOffset);

    /// <summary>Reads the current header snapshot from shared memory.</summary>
    public RingBufferHeader ReadHeader()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        unsafe
        {
            return RingBufferHeader.Read(new ReadOnlySpan<byte>(Pointer, RingBufferLayout.HeaderSize));
        }
    }

    /// <summary>Reads an endpoint state with acquire semantics.</summary>
    public RingBufferEndpointState ReadEndpointState(RingBufferEndpointRole role)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        unsafe
        {
            return (RingBufferEndpointState)Volatile.Read(ref Unsafe.AsRef<int>(Pointer + StateOffset(role)));
        }
    }

    /// <summary>Writes an endpoint state with release semantics.</summary>
    public void WriteEndpointState(RingBufferEndpointRole role, RingBufferEndpointState state)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        unsafe
        {
            Volatile.Write(ref Unsafe.AsRef<int>(Pointer + StateOffset(role)), (int)state);
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
                ref Unsafe.AsRef<int>(Pointer + StateOffset(role)), (int)desired, (int)expected);
        }
    }

    /// <summary>
    /// Creates the region if needed, otherwise opens and validates the existing
    /// one. Either endpoint may call this, so start order does not matter.
    /// </summary>
    public static SharedMemoryRegion CreateOrOpen(
        string name,
        int capacity = RingBufferLayout.DefaultCapacity,
        int slotSize = RingBufferLayout.DefaultSlotSize,
        SharedMemoryOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        EnsurePlatformSupported();
        RingBufferLayout.ValidateGeometry(capacity, slotSize);
        options ??= SharedMemoryOptions.Default;

        long requiredSize = RingBufferLayout.RequiredSize(capacity, slotSize);
        long start = Stopwatch.GetTimestamp();
        bool recreateAttempted = false;

        while (true)
        {
            MemoryMappedFile? file = null;
            try
            {
                bool created = false;

                if (!options.RequireExisting)
                {
                    try
                    {
                        file = MemoryMappedFile.CreateNew(name, requiredSize);
                        created = true;
                    }
                    catch (IOException)
                    {
                        // Already exists: fall through to OpenExisting.
                    }
                }

                if (!created)
                {
                    file = OpenExistingWithTimeout(name, options, start);
                }

                return new SharedMemoryRegion(name, file!, created, capacity, slotSize, options, start);
            }
            catch (RingBufferCorruptedException) when (options.RecreateIfStale && !recreateAttempted)
            {
                // Drop every handle we hold; on Windows the kernel object is
                // destroyed if the crashed creator was the only other owner, so
                // the next CreateNew attempt succeeds.
                file?.Dispose();
                recreateAttempted = true;
                // Loop around and recreate from scratch.
            }
        }
    }

    /// <summary>Opens an existing region; does not create one.</summary>
    public static SharedMemoryRegion OpenExisting(
        string name,
        int capacity = RingBufferLayout.DefaultCapacity,
        int slotSize = RingBufferLayout.DefaultSlotSize,
        SharedMemoryOptions? options = null)
    {
        options ??= SharedMemoryOptions.Default;
        return CreateOrOpen(name, capacity, slotSize, new SharedMemoryOptions
        {
            OpenTimeout = options.OpenTimeout,
            RecreateIfStale = false,
            RequireExisting = true,
            AdoptExistingGeometry = options.AdoptExistingGeometry,
        });
    }

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

    private static int StateOffset(RingBufferEndpointRole role) => role switch
    {
        RingBufferEndpointRole.Producer => RingBufferLayout.ProducerStateOffset,
        RingBufferEndpointRole.Consumer => RingBufferLayout.ConsumerStateOffset,
        _ => throw new ArgumentOutOfRangeException(nameof(role), role, null),
    };

    private static void EnsurePlatformSupported()
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException(
                "RingBuffer.SharedMemory uses named memory-mapped files, which .NET supports on Windows only. " +
                "On Unix, use a file-backed map (MemoryMappedFile.CreateFromFile) instead.");
        }
    }

    private static MemoryMappedFile OpenExistingWithTimeout(string name, SharedMemoryOptions options, long startTimestamp)
    {
        while (true)
        {
            try
            {
#pragma warning disable CA1416 // Windows-only API; EnsurePlatformSupported guards every entry point.
                return MemoryMappedFile.OpenExisting(name, MemoryMappedFileRights.ReadWrite);
#pragma warning restore CA1416
            }
            catch (Exception exception) when (exception is FileNotFoundException or DirectoryNotFoundException)
            {
                if (Stopwatch.GetElapsedTime(startTimestamp) >= options.OpenTimeout)
                {
                    throw new RingBufferTimeoutException(
                        $"Region '{name}' did not exist within {options.OpenTimeout.TotalMilliseconds:F0} ms.");
                }

                Thread.Sleep(2);
            }
        }
    }

    private unsafe void Initialize(int capacity, int slotSize)
    {
        RingBufferHeader header = RingBufferHeader.CreateNew(capacity, slotSize);
        Span<byte> span = new(Pointer, RingBufferLayout.HeaderSize);

        // Write everything except the magic, then publish the magic with release
        // semantics. An opener that observes the magic is guaranteed to observe
        // the geometry and the zeroed cursors.
        header.WriteTo(span, includeMagic: false);
        Volatile.Write(ref Unsafe.AsRef<ulong>(Pointer + RingBufferLayout.MagicOffset), RingBufferLayout.Magic);
    }

    private unsafe void OpenAndValidate(
        int capacity, int slotSize, SharedMemoryOptions options, long startTimestamp)
    {
        if (_accessor.Capacity < RingBufferLayout.HeaderSize)
        {
            throw new RingBufferCorruptedException(
                $"Region '{Name}' is only {_accessor.Capacity} bytes; too small to hold a ring buffer header.");
        }

        while (true)
        {
            ulong magic = Volatile.Read(ref Unsafe.AsRef<ulong>(Pointer + RingBufferLayout.MagicOffset));
            if (magic == RingBufferLayout.Magic)
            {
                break;
            }

            if (Stopwatch.GetElapsedTime(startTimestamp) >= options.OpenTimeout)
            {
                throw new RingBufferCorruptedException(
                    $"Region '{Name}' exists but was not initialized within " +
                    $"{options.OpenTimeout.TotalMilliseconds:F0} ms (bad magic 0x{magic:X16}). " +
                    "The creating process probably crashed during startup, or the region is stale. " +
                    "Re-run with stale-region recreation enabled to reclaim it.");
            }

            Thread.Sleep(2);
        }

        // The acquire above orders all subsequent header reads after the creator's
        // release of the magic value.
        RingBufferHeader header = ReadHeader();
        header.Validate(Name);
        if (!options.AdoptExistingGeometry)
        {
            header.ValidateRequestedGeometry(Name, capacity, slotSize);
        }

        if (_accessor.Capacity < RingBufferLayout.RequiredSize(header.Capacity, header.SlotSize))
        {
            throw new RingBufferCorruptedException(
                $"Region '{Name}' maps {_accessor.Capacity} bytes but its header declares " +
                $"{RingBufferLayout.RequiredSize(header.Capacity, header.SlotSize)} bytes.");
        }
    }
}
