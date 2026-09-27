using System.Buffers.Binary;
using System.Diagnostics;
using Sparc;

namespace Sparc.Core;

/// <summary>
/// Single-producer/single-consumer lock-free ring buffer whose state lives in a
/// cross-process <see cref="RingBufferRegion"/>.
/// </summary>
/// <remarks>
/// <para>
/// This is the shared-memory incarnation of <see cref="SpscRingBuffer"/>: the
/// same algorithm, but <c>head</c>, <c>tail</c> and the slots live in memory
/// mapped by two independent processes. All ordering guarantees are provided by
/// <see cref="Volatile"/>/<see cref="Interlocked"/> operations on the mapped
/// addresses, not by the OS. Like the in-process version, each side caches the
/// peer's cursor privately and only refreshes it when the cached value says
/// full/empty, which avoids fetching a cache line the peer writes on every
/// message.
/// </para>
/// <para>
/// <b>Threading contract:</b> one thread calls <see cref="TryWrite"/>, one
/// thread calls <see cref="TryRead"/>, per process. Nothing else may touch the
/// mapped region while the buffer is in use.
/// </para>
/// </remarks>
public sealed class SharedRingBuffer : IRingBufferEndpoint, IDisposable
{
    private readonly RingBufferRegion _region;
    private readonly bool _ownsRegion;
    private readonly int _mask;
    private readonly unsafe byte* _slots;
    private RingBufferEndpointRole? _role;
    private bool _aborted;
    private int _disposed;

    // Private caches of the peer's cursor. The peer writes its cursor every
    // message, which would otherwise force a coherence transfer on every read;
    // each side only refreshes when the stale copy says full/empty. The cached
    // value is never newer than the real cursor, so correctness is unaffected.
    private long _cachedHead; // producer-private
    private long _cachedTail; // consumer-private

    // State for the zero-copy lease API. The producer and consumer contracts are
    // single-threaded, so per-instance pending state is safe.
    private long _pendingTail;
    private int _pendingLength;
    private int _pendingType;
    private bool _hasPendingWrite;
    private long _peekedHead;
    private bool _hasPeekedRead;

    public SharedRingBuffer(RingBufferRegion region, bool ownsRegion = true)
    {
        ArgumentNullException.ThrowIfNull(region);
        _region = region;
        _ownsRegion = ownsRegion;
        Capacity = region.Capacity;
        SlotSize = region.SlotSize;
        MaxPayloadSize = region.MaxPayloadSize;
        _mask = Capacity - 1;
        unsafe
        {
            _slots = region.Pointer + RingBufferLayout.HeaderSize;
        }

        // Seed the private cursor snapshots from the live region. This matters
        // when attaching to a region whose cursors are already non-zero (e.g. a
        // consumer restarting while head == tail > 0): a zero-initialized cache
        // would look like "data available" and read an unpublished slot. After
        // seeding, the consumer invariant cachedTail >= head and the producer
        // invariant cachedHead <= head both hold.
        _cachedHead = Volatile.Read(ref region.HeadRef);
        _cachedTail = Volatile.Read(ref region.TailRef);
        Debug.Assert(_cachedTail >= _cachedHead);
    }

    /// <summary>Creates the region if needed, otherwise joins the existing one.</summary>
    public static SharedRingBuffer OpenOrCreate(
        IIpcMemoryRegionFactory factory,
        string name,
        int capacity = RingBufferLayout.DefaultCapacity,
        int slotSize = RingBufferLayout.DefaultSlotSize,
        SharedRingBufferOptions? options = null)
    {
        return new SharedRingBuffer(
            RingBufferRegion.CreateOrOpen(factory, name, capacity, slotSize, options),
            ownsRegion: true);
    }

    /// <summary>Joins an existing region; fails if it does not exist.</summary>
    public static SharedRingBuffer OpenExisting(
        IIpcMemoryRegionFactory factory,
        string name,
        int capacity = RingBufferLayout.DefaultCapacity,
        int slotSize = RingBufferLayout.DefaultSlotSize,
        SharedRingBufferOptions? options = null)
    {
        return new SharedRingBuffer(
            RingBufferRegion.OpenExisting(factory, name, capacity, slotSize, options),
            ownsRegion: true);
    }

    /// <summary>The region name this buffer is attached to.</summary>
    public string Name => _region.Name;

    /// <inheritdoc />
    public int Capacity { get; }

    /// <inheritdoc />
    public int SlotSize { get; }

    /// <inheritdoc />
    public int MaxPayloadSize { get; }

    /// <summary>Sequence number the producer will use for its next write.</summary>
    public long TailSequence => Volatile.Read(ref _region.TailRef);

    /// <summary>Sequence number the consumer will use for its next read.</summary>
    public long HeadSequence => Volatile.Read(ref _region.HeadRef);

    /// <inheritdoc />
    public bool IsEmpty => HeadSequence == TailSequence;

    /// <inheritdoc />
    public int Count => (int)(TailSequence - HeadSequence);

    /// <summary>Advisory state reported by the producer.</summary>
    public RingBufferEndpointState ProducerState => _region.ReadEndpointState(RingBufferEndpointRole.Producer);

    /// <summary>Advisory state reported by the consumer.</summary>
    public RingBufferEndpointState ConsumerState => _region.ReadEndpointState(RingBufferEndpointRole.Consumer);

    /// <summary>The underlying protocol region. Protocol-level; not part of the public surface.</summary>
    internal RingBufferRegion Region => _region;

    /// <summary>
    /// Claims the given role. Throws <see cref="RingBufferRoleConflictException"/>
    /// when the role is already claimed by a live instance; pass
    /// <paramref name="takeover"/> to reclaim it from a crashed peer.
    /// </summary>
    /// <param name="role">Role to claim: producer or consumer.</param>
    /// <param name="takeover">When true, reclaim the role from a crashed peer.</param>
    /// <param name="cancellationToken">
    /// Bounds the CAS retry loop; pass <see cref="CancellationToken.None"/> to
    /// retry indefinitely.
    /// </param>
    public void Connect(RingBufferEndpointRole role, bool takeover = false, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RingBufferEndpointState current = _region.ReadEndpointState(role);

            if (!takeover && current is RingBufferEndpointState.Starting or RingBufferEndpointState.Running)
            {
                throw new RingBufferRoleConflictException(
                    $"Region '{Name}' already has a {role.ToString().ToLowerInvariant()} " +
                    $"(state={current}). Only one instance per role is allowed.");
            }

            RingBufferEndpointState previous = _region.CompareExchangeEndpointState(
                role, RingBufferEndpointState.Starting, current);

            if (previous == current)
            {
                _region.WriteEndpointState(role, RingBufferEndpointState.Running);
                _role = role;
                return;
            }
        }
    }

    /// <inheritdoc />
    public bool TryWrite(int type, ReadOnlySpan<byte> payload)
    {
        ThrowIfDisposed();

        if ((uint)payload.Length > (uint)MaxPayloadSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(payload), payload.Length,
                $"Payload of {payload.Length} bytes exceeds the maximum of {MaxPayloadSize} bytes.");
        }

        // Producer-owned cursor: this process is the only writer.
        long tail = Volatile.Read(ref _region.TailRef);

        // Acquire (only when the cached peer cursor claims the buffer is full):
        // observes the consumer's release of head, so the slot we are about to
        // overwrite is guaranteed to be fully consumed.
        long head = _cachedHead;
        if (tail - head >= Capacity)
        {
            head = Volatile.Read(ref _region.HeadRef);
            _cachedHead = head;
            if (tail - head >= Capacity)
            {
                return false; // full
            }
        }

        // Invariants: cursors are monotonic, the cached peer cursor is never
        // ahead of the real one, and the target slot is inside the region.
        Debug.Assert(tail >= head);
        Debug.Assert(tail - head < Capacity);
        Debug.Assert(_cachedHead <= Volatile.Read(ref _region.HeadRef));
        int offset = (int)(tail & _mask) * SlotSize;
        Debug.Assert((uint)(tail & _mask) < (uint)Capacity);
        Debug.Assert(offset >= 0);
        Debug.Assert((long)offset + SlotSize <= _region.Size - RingBufferLayout.HeaderSize);

        unsafe
        {
            SlotFraming.Write(new Span<byte>(_slots + offset, SlotSize), type, payload);
        }

        // Release: slot bytes must be globally visible before the consumer can
        // observe the advanced tail.
        Volatile.Write(ref _region.TailRef, tail + 1);
        return true;
    }

    /// <inheritdoc />
    public bool TryRead(Span<byte> destination, out int bytesRead, out int type)
    {
        ThrowIfDisposed();

        if (destination.Length < MaxPayloadSize)
        {
            throw new ArgumentException(
                $"Destination must be at least {MaxPayloadSize} bytes (MaxPayloadSize).", nameof(destination));
        }

        // Consumer-owned cursor.
        long head = Volatile.Read(ref _region.HeadRef);

        // Acquire (only when the cached peer cursor claims the buffer is empty):
        // observes the producer's release of the slot contents.
        long tail = _cachedTail;
        if (tail == head)
        {
            tail = Volatile.Read(ref _region.TailRef);
            _cachedTail = tail;
            if (tail == head)
            {
                bytesRead = 0;
                type = 0;
                return false; // empty
            }
        }

        // Invariants: data is available, the cached peer cursor is never ahead
        // of the real one, and the source slot is inside the region.
        Debug.Assert(tail > head);
        Debug.Assert(_cachedTail <= Volatile.Read(ref _region.TailRef));
        int offset = (int)(head & _mask) * SlotSize;
        Debug.Assert((uint)(head & _mask) < (uint)Capacity);
        Debug.Assert(offset >= 0);
        Debug.Assert((long)offset + SlotSize <= _region.Size - RingBufferLayout.HeaderSize);

        unsafe
        {
            SlotFraming.Read(new ReadOnlySpan<byte>(_slots + offset, SlotSize), destination, out bytesRead, out type);
        }

        // Release: publishes "slot consumed" to the producer. If this process
        // dies before the store, the message is redelivered after a restart.
        Volatile.Write(ref _region.HeadRef, head + 1);
        return true;
    }

    /// <inheritdoc />
    public bool TryReserveWrite(int type, int length, out Span<byte> payload)
    {
        ThrowIfDisposed();

        if (_hasPendingWrite)
        {
            throw new InvalidOperationException("A write reservation is already active.");
        }

        if ((uint)length > (uint)MaxPayloadSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(length), length,
                $"Payload of {length} bytes exceeds the maximum of {MaxPayloadSize} bytes.");
        }

        long tail = Volatile.Read(ref _region.TailRef);
        long head = _cachedHead;
        if (tail - head >= Capacity)
        {
            head = Volatile.Read(ref _region.HeadRef);
            _cachedHead = head;
            if (tail - head >= Capacity)
            {
                payload = default;
                return false; // full
            }
        }

        Debug.Assert(tail >= head);
        Debug.Assert(tail - head < Capacity);
        int offset = (int)(tail & _mask) * SlotSize;
        Debug.Assert(offset >= 0);
        Debug.Assert((long)offset + SlotSize <= _region.Size - RingBufferLayout.HeaderSize);

        unsafe
        {
            payload = new Span<byte>(_slots + offset + RingBufferLayout.MessageHeaderSize, length);
        }

        _pendingTail = tail;
        _pendingLength = length;
        _pendingType = type;
        _hasPendingWrite = true;
        return true;
    }

    /// <inheritdoc />
    public void CommitWrite()
    {
        ThrowIfDisposed();

        if (!_hasPendingWrite)
        {
            throw new InvalidOperationException("No write reservation is active.");
        }

        _hasPendingWrite = false;
        int offset = (int)(_pendingTail & _mask) * SlotSize;
        unsafe
        {
            Span<byte> slot = new(_slots + offset, SlotSize);
            BinaryPrimitives.WriteInt32LittleEndian(slot, _pendingLength);
            BinaryPrimitives.WriteInt32LittleEndian(slot[sizeof(int)..], _pendingType);
        }

        // Release: the caller's payload writes and the framing above must be
        // visible before the consumer can observe the advanced tail.
        Volatile.Write(ref _region.TailRef, _pendingTail + 1);
    }

    /// <inheritdoc />
    public void AbandonWrite()
    {
        ThrowIfDisposed();

        if (!_hasPendingWrite)
        {
            throw new InvalidOperationException("No write reservation is active.");
        }

        _hasPendingWrite = false;
    }

    /// <inheritdoc />
    public bool TryPeek(out ReadOnlySpan<byte> payload, out int length, out int type)
    {
        ThrowIfDisposed();

        if (_hasPeekedRead)
        {
            throw new InvalidOperationException("A read view is already active.");
        }

        long head = Volatile.Read(ref _region.HeadRef);
        long tail = _cachedTail;
        if (tail == head)
        {
            tail = Volatile.Read(ref _region.TailRef);
            _cachedTail = tail;
            if (tail == head)
            {
                payload = default;
                length = 0;
                type = 0;
                return false; // empty
            }
        }

        Debug.Assert(tail > head);
        int offset = (int)(head & _mask) * SlotSize;
        Debug.Assert(offset >= 0);
        Debug.Assert((long)offset + SlotSize <= _region.Size - RingBufferLayout.HeaderSize);

        unsafe
        {
            ReadOnlySpan<byte> slot = new(_slots + offset, SlotSize);
            length = BinaryPrimitives.ReadInt32LittleEndian(slot);
            if ((uint)length > (uint)MaxPayloadSize)
            {
                throw new RingBufferCorruptedException(
                    $"Slot declares a payload of {length} bytes but only {MaxPayloadSize} are available.");
            }

            type = BinaryPrimitives.ReadInt32LittleEndian(slot[sizeof(int)..]);
            payload = slot.Slice(RingBufferLayout.MessageHeaderSize, length);
        }

        _peekedHead = head;
        _hasPeekedRead = true;
        return true;
    }

    /// <inheritdoc />
    public void AdvanceRead()
    {
        ThrowIfDisposed();

        if (!_hasPeekedRead)
        {
            throw new InvalidOperationException("No read view is active.");
        }

        _hasPeekedRead = false;

        // Release: publishes "slot consumed" to the producer once the caller is
        // done with the view. A crash before this store redelivers the message.
        Volatile.Write(ref _region.HeadRef, _peekedHead + 1);
    }

    /// <summary>
    /// Blocking convenience wrapper. Respects a cancellation token so callers
    /// can stop a stalled producer.
    /// </summary>
    public void Write(ReadOnlySpan<byte> payload, int type = 0, CancellationToken cancellationToken = default)
    {
        SpinWait spin = new();
        while (!TryWrite(type, payload))
        {
            cancellationToken.ThrowIfCancellationRequested();
            spin.SpinOnce();
        }
    }

    /// <summary>Marks this endpoint as faulted so the peer can stop waiting for it.</summary>
    public void Abort()
    {
        ThrowIfDisposed();
        if (!_aborted && _role is { } role)
        {
            _aborted = true;
            _region.WriteEndpointState(role, RingBufferEndpointState.Faulted);
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        if (!_aborted && _role is { } role)
        {
            try
            {
                _region.WriteEndpointState(role, RingBufferEndpointState.Stopped);
            }
            catch (ObjectDisposedException)
            {
                // Region already gone; nothing to update.
            }
        }

        if (_ownsRegion)
        {
            _region.Dispose();
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
    }
}
