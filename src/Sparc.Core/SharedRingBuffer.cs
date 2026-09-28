using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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
internal sealed class SharedRingBuffer : IProducerEndpoint, IConsumerEndpoint
{
    private const int WriteTimeoutSeconds = 30;
    private const int ReadTimeoutSeconds = 30;
    private const int MaxRoleClaimAttempts = 1 << 20;

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
    private int _pendingOffset;
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
    public int Count
    {
        get
        {
            // Read head before tail: head is monotonic and never exceeds tail, so
            // this order can never produce a transient negative count.
            long head = Volatile.Read(ref _region.HeadRef);
            long tail = Volatile.Read(ref _region.TailRef);
            return (int)(tail - head);
        }
    }

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

        if (_role == role)
        {
            return; // already claimed by this instance (SparcRing connects at open)
        }

        for (int attempt = 0; attempt < MaxRoleClaimAttempts; attempt++)
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

        throw new RingBufferRoleConflictException(
            $"Could not claim the {role} role in region '{Name}' after {MaxRoleClaimAttempts} attempts; " +
            "another process keeps changing the endpoint state.");
    }

    void IProducerEndpoint.Connect(bool takeover, CancellationToken cancellationToken) =>
        Connect(RingBufferEndpointRole.Producer, takeover, cancellationToken);

    void IConsumerEndpoint.Connect(bool takeover, CancellationToken cancellationToken) =>
        Connect(RingBufferEndpointRole.Consumer, takeover, cancellationToken);

    /// <summary>Publishes a message; alias of <see cref="TryWrite"/> for the producer role.</summary>
    public bool TryPublish(int type, ReadOnlySpan<byte> payload) => TryWrite(type, payload);

    /// <summary>Blocking publish; alias of <see cref="Write"/> for the producer role.</summary>
    public void Publish(int type, ReadOnlySpan<byte> payload, CancellationToken cancellationToken = default) =>
        Write(payload, type, cancellationToken);

    /// <summary>
    /// Attempts to copy one message into the buffer. Returns false when the
    /// buffer is full. Concrete counterpart of <see cref="TryPublish"/>.
    /// </summary>
    public bool TryWrite(int type, ReadOnlySpan<byte> payload)
    {
        ThrowIfDisposed();

        if ((uint)payload.Length > (uint)MaxPayloadSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(payload), payload.Length,
                $"Payload of {payload.Length} bytes exceeds the maximum of {MaxPayloadSize} bytes.");
        }

        if (!TryAcquireWriteSlot(out long tail, out int offset))
        {
            return false; // full
        }

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

        if (!TryAcquireReadSlot(out long head, out int offset))
        {
            bytesRead = 0;
            type = 0;
            return false; // empty
        }

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

        if (!TryAcquireWriteSlot(out long tail, out int offset))
        {
            payload = default;
            return false; // full
        }

        unsafe
        {
            payload = new Span<byte>(_slots + offset + RingBufferLayout.MessageHeaderSize, length);
        }

        _pendingOffset = offset;
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
        Debug.Assert((long)_pendingOffset + SlotSize <= _region.Size - RingBufferLayout.HeaderSize);
        unsafe
        {
            Span<byte> slot = new(_slots + _pendingOffset, SlotSize);
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

        Debug.Assert((long)_pendingOffset + SlotSize <= _region.Size - RingBufferLayout.HeaderSize);
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

        if (!TryAcquireReadSlot(out long head, out int offset))
        {
            payload = default;
            length = 0;
            type = 0;
            return false; // empty
        }

        Debug.Assert((long)offset + SlotSize <= _region.Size - RingBufferLayout.HeaderSize);

        unsafe
        {
            ReadOnlySpan<byte> slot = new(_slots + offset, SlotSize);
            length = SlotFraming.ReadHeader(slot, out type);
            Debug.Assert(length <= MaxPayloadSize);
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
        Debug.Assert(_peekedHead >= 0);

        // Release: publishes "slot consumed" to the producer once the caller is
        // done with the view. A crash before this store redelivers the message.
        Volatile.Write(ref _region.HeadRef, _peekedHead + 1);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryAcquireWriteSlot(out long tail, out int offset)
    {
        // Producer-owned cursor: this process is the only writer.
        tail = Volatile.Read(ref _region.TailRef);

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
                offset = 0;
                return false;
            }
        }

        // Invariants: cursors are monotonic, the cached peer cursor is never
        // ahead of the real one, and the target slot is inside the region.
        Debug.Assert(tail >= head);
        Debug.Assert(tail - head < Capacity);
        Debug.Assert(_cachedHead <= Volatile.Read(ref _region.HeadRef));
        Debug.Assert((uint)(tail & _mask) < (uint)Capacity);
        offset = (int)(tail & _mask) * SlotSize;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryAcquireReadSlot(out long head, out int offset)
    {
        // Consumer-owned cursor.
        head = Volatile.Read(ref _region.HeadRef);

        // Acquire (only when the cached peer cursor claims the buffer is empty):
        // observes the producer's release of the slot contents.
        long tail = _cachedTail;
        if (tail == head)
        {
            tail = Volatile.Read(ref _region.TailRef);
            _cachedTail = tail;
            if (tail == head)
            {
                offset = 0;
                return false;
            }
        }

        // Invariants: data is available, the cached peer cursor is never ahead
        // of the real one, and the source slot is inside the region.
        Debug.Assert(tail > head);
        Debug.Assert(_cachedTail <= Volatile.Read(ref _region.TailRef));
        Debug.Assert((uint)(head & _mask) < (uint)Capacity);
        offset = (int)(head & _mask) * SlotSize;
        return true;
    }

    /// <summary>
    /// Blocking convenience wrapper. Respects a cancellation token so callers
    /// can stop a stalled producer, and throws <see cref="RingBufferTimeoutException"/>
    /// when the buffer stays full for <see cref="WriteTimeoutSeconds"/> seconds.
    /// </summary>
    public void Write(ReadOnlySpan<byte> payload, int type = 0, CancellationToken cancellationToken = default)
    {
        Debug.Assert(payload.Length <= MaxPayloadSize);
        long startTimestamp = Stopwatch.GetTimestamp();
        long timeoutTicks = WriteTimeoutSeconds * Stopwatch.Frequency;
        SpinWait spin = new();
        while (!TryWrite(type, payload))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Stopwatch.GetTimestamp() - startTimestamp >= timeoutTicks)
            {
                throw new RingBufferTimeoutException(
                    $"Buffer stayed full for {WriteTimeoutSeconds} seconds; the peer is not consuming.");
            }

            spin.SpinOnce();
        }
    }

    /// <summary>
    /// Reserves the next slot and returns a lease that publishes on dispose.
    /// Returns false when the buffer is full.
    /// </summary>
    public bool TryBeginWrite(int type, int length, out WriteLease lease)
    {
        if (TryReserveWrite(type, length, out Span<byte> payload))
        {
            lease = new WriteLease(this, payload);
            return true;
        }

        lease = default;
        return false;
    }

    /// <summary>
    /// Blocking variant of <see cref="TryBeginWrite"/>: spins until a slot is
    /// free, the caller cancels, or the buffer stays full for
    /// <see cref="WriteTimeoutSeconds"/> seconds.
    /// </summary>
    public WriteLease BeginWrite(int type, int length, CancellationToken cancellationToken = default)
    {
        long startTimestamp = Stopwatch.GetTimestamp();
        long timeoutTicks = WriteTimeoutSeconds * Stopwatch.Frequency;
        SpinWait spin = new();
        Span<byte> payload;
        while (!TryReserveWrite(type, length, out payload))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Stopwatch.GetTimestamp() - startTimestamp >= timeoutTicks)
            {
                throw new RingBufferTimeoutException(
                    $"Buffer stayed full for {WriteTimeoutSeconds} seconds; the peer is not consuming.");
            }

            spin.SpinOnce();
        }

        return new WriteLease(this, payload);
    }

    /// <summary>
    /// Returns a lease over the oldest message that releases the slot on
    /// dispose. Returns false when the buffer is empty.
    /// </summary>
    public bool TryBeginRead(out ReadLease lease)
    {
        if (TryPeek(out ReadOnlySpan<byte> payload, out int length, out int type))
        {
            lease = new ReadLease(this, payload, length, type);
            return true;
        }

        lease = default;
        return false;
    }

    /// <summary>
    /// Blocking variant of <see cref="TryBeginRead"/>: spins until a message
    /// arrives, the caller cancels, or the buffer stays empty for
    /// <see cref="ReadTimeoutSeconds"/> seconds.
    /// </summary>
    public ReadLease Read() => Read(TimeSpan.FromSeconds(ReadTimeoutSeconds), CancellationToken.None);

    /// <summary>Blocking wrapper with a cancellation token, 30-second empty-buffer timeout.</summary>
    public ReadLease Read(CancellationToken cancellationToken) =>
        Read(TimeSpan.FromSeconds(ReadTimeoutSeconds), cancellationToken);

    /// <summary>
    /// Blocking variant of <see cref="TryBeginRead"/> with an explicit
    /// empty-buffer timeout.
    /// </summary>
    public ReadLease Read(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(timeout, TimeSpan.Zero);

        long startTimestamp = Stopwatch.GetTimestamp();
        double timeoutSeconds = timeout.TotalSeconds;
        long timeoutTicks = (long)(timeoutSeconds * Stopwatch.Frequency);
        SpinWait spin = new();
        ReadOnlySpan<byte> payload;
        int length;
        int type;
        while (!TryPeek(out payload, out length, out type))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Stopwatch.GetTimestamp() - startTimestamp >= timeoutTicks)
            {
                throw new RingBufferTimeoutException(
                    $"Buffer stayed empty for {timeoutSeconds:F1} seconds; the peer is not producing.");
            }

            spin.SpinOnce();
        }

        return new ReadLease(this, payload, length, type);
    }

    /// <inheritdoc />
    public void SetWaiting(bool waiting)
    {
        ThrowIfDisposed();
        if (_role is not { } role)
        {
            throw new InvalidOperationException(
                "Connect must be called before an endpoint can declare that it is waiting.");
        }

        _region.WriteWaiting(role, waiting ? 1 : 0);
    }

    /// <inheritdoc />
    public bool IsPeerWaiting()
    {
        ThrowIfDisposed();
        if (_role is not { } role)
        {
            throw new InvalidOperationException(
                "Connect must be called before an endpoint can read the peer's waiting flag.");
        }

        RingBufferEndpointRole peer = role == RingBufferEndpointRole.Producer
            ? RingBufferEndpointRole.Consumer
            : RingBufferEndpointRole.Producer;
        return _region.ReadWaiting(peer) != 0;
    }

    /// <summary>Marks this endpoint as faulted so the peer can stop waiting for it.</summary>
    public void Abort()
    {
        ThrowIfDisposed();
        if (!_aborted && _role is { } role)
        {
            _aborted = true;
            _region.WriteWaiting(role, 0);
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
                _region.WriteWaiting(role, 0);
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
