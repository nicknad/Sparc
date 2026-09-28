using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Sparc.Core;

/// <summary>
/// Single-producer/single-consumer lock-free ring buffer backed by a managed
/// array (Phase 1/2 of the project).
/// </summary>
/// <remarks>
/// <para>
/// <b>Parity with <see cref="SharedRingBuffer"/>.</b> The two buffers implement
/// the same algorithm with deliberately duplicated hot paths: a shared
/// abstraction over array vs unmanaged storage would sit on every message.
/// Keep changes in sync; <c>SpscRingBufferTests</c> and
/// <c>SharedRingBufferTests</c> cover the same scenarios for both types.
/// </para>
/// <para><b>Slot layout:</b> <c>[int32 length][int32 type][payload...]</c>.</para>
/// <para>
/// <b>Algorithm.</b> <c>tail</c> (next write sequence) is owned by the producer,
/// <c>head</c> (next read sequence) by the consumer. Both counters increase
/// monotonically; the physical slot is <c>sequence &amp; (Capacity - 1)</c>.
/// </para>
/// <list type="bullet">
///   <item><description>Producer: read <c>head</c> (acquire) → check space → write slot → publish <c>tail</c> (release).</description></item>
///   <item><description>Consumer: read <c>tail</c> (acquire) → check data → read slot → publish <c>head</c> (release).</description></item>
/// </list>
/// <para>
/// The release/acquire pair on <c>tail</c> makes the slot bytes written before
/// the publish visible to the consumer, and the release/acquire pair on
/// <c>head</c> guarantees the producer never overwrites a slot the consumer is
/// still reading (or has not yet finished reading).
/// </para>
/// <para>
/// <b>Cached peer cursor.</b> Each side keeps a private, possibly stale copy of
/// the peer's counter and only re-reads the shared value when the stale copy
/// says "full" (producer) or "empty" (consumer). The cached value can never be
/// newer than the real cursor, so the worst case is one redundant refresh; the
/// win is that a line written by the peer on every message is not fetched on
/// every message (this is the usual SPSC queue optimization).
/// </para>
/// <para>
/// The counters are 64-bit. On a 64-bit process aligned 64-bit loads/stores are
/// atomic; on 32-bit processes they are not guaranteed to be, so 64-bit is
/// required for cross-process use.
/// </para>
/// </remarks>
public sealed class SpscRingBuffer : IRingBuffer
{
    private const int WriteTimeoutSeconds = 30;

    private readonly byte[] _buffer;
    private readonly int _mask;

    // Padding keeps the producer-owned and consumer-owned counters on separate
    // cache lines, avoiding false sharing when the two ends run on different cores.
    private PaddedLong _head;
    private PaddedLong _tail;

    // Private caches of the *peer's* cursor. Reading the peer's cache line costs a
    // coherence transfer every time it was written, so each side only refreshes
    // its cache when the stale copy says "full" (producer) or "empty" (consumer).
    // The cached value can only ever be older, never newer, than the peer's real
    // cursor, so the worst case is a redundant refresh.
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

    /// <summary>Creates a buffer with the project defaults (1024 slots × 256 bytes).</summary>
    public SpscRingBuffer()
        : this(RingBufferLayout.DefaultCapacity, RingBufferLayout.DefaultSlotSize)
    {
    }

    /// <summary>Creates a buffer with explicit geometry.</summary>
    /// <param name="capacity">Number of slots; must be a power of two.</param>
    /// <param name="slotSize">Bytes per slot; must exceed <see cref="RingBufferLayout.MessageHeaderSize"/>.</param>
    public SpscRingBuffer(int capacity, int slotSize)
    {
        RingBufferLayout.ValidateGeometry(capacity, slotSize);
        Capacity = capacity;
        SlotSize = slotSize;
        MaxPayloadSize = RingBufferLayout.MaxPayloadSizeFor(slotSize);
        _mask = capacity - 1;
        _buffer = GC.AllocateArray<byte>(checked(capacity * slotSize), pinned: true);
        Debug.Assert(_buffer.Length == capacity * slotSize);
    }

    /// <inheritdoc />
    public int Capacity { get; }

    /// <inheritdoc />
    public int SlotSize { get; }

    /// <inheritdoc />
    public int MaxPayloadSize { get; }

    /// <inheritdoc />
    public bool IsEmpty => Volatile.Read(ref _head.Value) == Volatile.Read(ref _tail.Value);

    /// <inheritdoc />
    public int Count
    {
        get
        {
            long head = Volatile.Read(ref _head.Value);
            long tail = Volatile.Read(ref _tail.Value);
            return (int)(tail - head);
        }
    }

    /// <inheritdoc />
    public bool TryWrite(int type, ReadOnlySpan<byte> payload)
    {
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

        Debug.Assert((long)offset + SlotSize <= _buffer.Length);
        Span<byte> slot = _buffer.AsSpan(offset, SlotSize);
        SlotFraming.Write(slot, type, payload);

        // Release: the slot bytes above must be visible before the consumer can
        // observe the advanced tail.
        Volatile.Write(ref _tail.Value, tail + 1);
        return true;
    }

    /// <inheritdoc />
    public bool TryRead(Span<byte> destination, out int bytesRead, out int type)
    {
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

        Debug.Assert((long)offset + SlotSize <= _buffer.Length);
        ReadOnlySpan<byte> slot = _buffer.AsSpan(offset, SlotSize);
        SlotFraming.Read(slot, destination, out bytesRead, out type);

        // Release: the producer must not overwrite this slot before the copy above
        // is complete. A crash between the copy and this store may redeliver the
        // message (at-least-once across crashes).
        Volatile.Write(ref _head.Value, head + 1);
        return true;
    }

    /// <inheritdoc />
    public bool TryReserveWrite(int type, int length, out Span<byte> payload)
    {
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

        _pendingOffset = offset;
        _pendingTail = tail;
        _pendingLength = length;
        _pendingType = type;
        _hasPendingWrite = true;
        payload = _buffer.AsSpan(offset + RingBufferLayout.MessageHeaderSize, length);
        return true;
    }

    /// <inheritdoc />
    public void CommitWrite()
    {
        if (!_hasPendingWrite)
        {
            throw new InvalidOperationException("No write reservation is active.");
        }

        _hasPendingWrite = false;
        Debug.Assert((long)_pendingOffset + SlotSize <= _buffer.Length);
        Span<byte> slot = _buffer.AsSpan(_pendingOffset, SlotSize);
        BinaryPrimitives.WriteInt32LittleEndian(slot, _pendingLength);
        BinaryPrimitives.WriteInt32LittleEndian(slot[sizeof(int)..], _pendingType);

        // Release: the caller's payload writes and the framing above must be
        // visible before the consumer can observe the advanced tail.
        Volatile.Write(ref _tail.Value, _pendingTail + 1);
    }

    /// <inheritdoc />
    public void AbandonWrite()
    {
        if (!_hasPendingWrite)
        {
            throw new InvalidOperationException("No write reservation is active.");
        }

        Debug.Assert((long)_pendingOffset + SlotSize <= _buffer.Length);
        _hasPendingWrite = false;
    }

    /// <inheritdoc />
    public bool TryPeek(out ReadOnlySpan<byte> payload, out int length, out int type)
    {
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

        Debug.Assert((long)offset + SlotSize <= _buffer.Length);
        ReadOnlySpan<byte> slot = _buffer.AsSpan(offset, SlotSize);
        length = SlotFraming.ReadHeader(slot, out type);
        Debug.Assert(length <= MaxPayloadSize);
        payload = slot.Slice(RingBufferLayout.MessageHeaderSize, length);
        _peekedHead = head;
        _hasPeekedRead = true;
        return true;
    }

    /// <inheritdoc />
    public void AdvanceRead()
    {
        if (!_hasPeekedRead)
        {
            throw new InvalidOperationException("No read view is active.");
        }

        _hasPeekedRead = false;

        // Release: publishes "slot consumed" to the producer once the caller is
        // done with the view.
        Volatile.Write(ref _head.Value, _peekedHead + 1);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryAcquireWriteSlot(out long tail, out int offset)
    {
        // This thread owns _tail; the value is only changed here.
        tail = _tail.Value;

        // Acquire (only when the cached peer cursor claims the buffer is full):
        // never overwrite the slot the consumer is currently reading.
        long head = _cachedHead;
        if (tail - head >= Capacity)
        {
            head = Volatile.Read(ref _head.Value);
            _cachedHead = head;
            if (tail - head >= Capacity)
            {
                offset = 0;
                return false;
            }
        }

        // Invariants: cursors are monotonic, the cached peer cursor is never
        // ahead of the real one, and the target slot is inside the array.
        Debug.Assert(tail >= head);
        Debug.Assert(tail - head < Capacity);
        Debug.Assert(_cachedHead <= Volatile.Read(ref _head.Value));
        Debug.Assert((uint)(tail & _mask) < (uint)Capacity);
        offset = (int)(tail & _mask) * SlotSize;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryAcquireReadSlot(out long head, out int offset)
    {
        // This thread owns _head.
        head = _head.Value;

        // Acquire (only when the cached peer cursor claims the buffer is empty):
        // observe the producer's release of the slot contents.
        long tail = _cachedTail;
        if (tail == head)
        {
            tail = Volatile.Read(ref _tail.Value);
            _cachedTail = tail;
            if (tail == head)
            {
                offset = 0;
                return false;
            }
        }

        // Invariants: data is available, the cached peer cursor is never ahead
        // of the real one, and the source slot is inside the array.
        Debug.Assert(tail > head);
        Debug.Assert(_cachedTail <= Volatile.Read(ref _tail.Value));
        Debug.Assert((uint)(head & _mask) < (uint)Capacity);
        offset = (int)(head & _mask) * SlotSize;
        return true;
    }

    /// <summary>
    /// Blocking convenience wrapper: spins until the message fits or the caller
    /// cancels, and throws <see cref="RingBufferTimeoutException"/> when the
    /// buffer stays full for <see cref="WriteTimeoutSeconds"/> seconds. For
    /// latency sensitive callers prefer <see cref="TryWrite(int, ReadOnlySpan{byte})"/>.
    /// </summary>
    /// <param name="payload">Payload bytes to copy into the next slot.</param>
    /// <param name="type">Caller-defined message tag.</param>
    /// <param name="cancellationToken">
    /// Bounds the wait; observed only while the buffer stays full.
    /// </param>
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

    /// <summary>Resets cursors. Only valid while no endpoint is active.</summary>
    public void Clear()
    {
        Volatile.Write(ref _head.Value, 0);
        Volatile.Write(ref _tail.Value, 0);
        _cachedHead = 0;
        _cachedTail = 0;
        _hasPendingWrite = false;
        _hasPeekedRead = false;
    }

    /// <summary>
    /// A padded 64-bit field so two counters never share a cache line. The
    /// 128-byte size makes the separation independent of the object's heap
    /// alignment: two adjacent fields of this size cannot touch one line.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = RingBufferLayout.CacheLineSize * 2)]
    private struct PaddedLong
    {
        [FieldOffset(0)]
        public long Value;
    }
}
