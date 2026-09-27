namespace Sparc.Core;

/// <summary>
/// Describes the binary layout of a ring buffer region.
/// </summary>
/// <remarks>
/// <para>
/// The region is a single contiguous block of memory:
/// </para>
/// <code>
/// ┌──────────────────────────────┐ offset 0
/// │ Header (192 bytes)           │
/// ├──────────────────────────────┤ offset 192
/// │ Slot 0       (SlotSize)      │
/// │ Slot 1       (SlotSize)      │
/// │ ...                          │
/// │ Slot (Capacity - 1)          │
/// └──────────────────────────────┘ offset 192 + Capacity*SlotSize
/// </code>
/// <para>
/// The header holds immutable geometry plus two monotonically increasing
/// 64-bit sequence counters:
/// </para>
/// <list type="bullet">
///   <item><description><c>Tail</c> — sequence number of the next slot the producer will write. Written only by the producer.</description></item>
///   <item><description><c>Head</c> — sequence number of the next slot the consumer will read. Written only by the consumer.</description></item>
/// </list>
/// <para>
/// Physical slot index for sequence <c>s</c>: <c>HeaderSize + (s % Capacity) * SlotSize</c>.
/// </para>
/// <para>
/// Head and tail live on separate 64-byte cache lines so the two processes do not
/// bounce the same line between cores (false sharing is deliberately avoided).
/// </para>
/// </remarks>
public static class RingBufferLayout
{
    /// <summary>Magic value <c>"SPSCRING"</c> (ASCII), stored little-endian at offset 0.</summary>
    public const ulong Magic = 0x474E_4952_4353_5053UL;

    /// <summary>Layout/protocol version written to the header.</summary>
    public const int Version = 1;

    /// <summary>Bytes of per-slot metadata in front of every payload: 4-byte length, 4-byte type.</summary>
    public const int MessageHeaderSize = 8;

    /// <summary>Default number of slots. Must be a power of two.</summary>
    public const int DefaultCapacity = 1024;

    /// <summary>Default size of one slot in bytes.</summary>
    public const int DefaultSlotSize = 256;

    /// <summary>Typical x86-64/ARM64 cache line size, used for padding.</summary>
    public const int CacheLineSize = 64;

    // ---- Header field offsets -------------------------------------------------

    public const int MagicOffset = 0;
    public const int VersionOffset = 8;
    public const int CapacityOffset = 12;
    public const int SlotSizeOffset = 16;
    public const int HeaderSizeOffset = 20;
    public const int MaxPayloadSizeOffset = 24;
    public const int FlagsOffset = 28;
    public const int ProducerStateOffset = 32;
    public const int ConsumerStateOffset = 36;

    /// <summary>Consumer-owned read cursor. Own cache line.</summary>
    public const int HeadOffset = 64;

    /// <summary>Producer-owned write cursor. Own cache line.</summary>
    public const int TailOffset = 128;

    /// <summary>Slots begin at this offset (cache-line aligned).</summary>
    public const int HeaderSize = 192;

    /// <summary>Maximum payload for a slot, given the slot size.</summary>
    public static int MaxPayloadSizeFor(int slotSize) => slotSize - MessageHeaderSize;

    /// <summary>
    /// Rounds a slot size up to a multiple of <see cref="CacheLineSize"/>.
    /// </summary>
    /// <remarks>
    /// Slots start at <see cref="HeaderSize"/> (cache-line aligned). When the
    /// slot size is not a multiple of the cache line, the last line of one slot
    /// and the first line of the next share a line; with a full ring the
    /// producer writes slot N while the consumer reads slot N+1, so that shared
    /// line bounces between the two cores. Aligning the slot size keeps every
    /// slot boundary on a line boundary.
    /// </remarks>
    public static int RoundSlotSizeToCacheLine(int slotSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(slotSize, 1);
        long rounded = ((long)slotSize + CacheLineSize - 1) / CacheLineSize * CacheLineSize;
        return checked((int)rounded);
    }

    /// <summary>Total bytes required for a region with the given geometry.</summary>
    public static long RequiredSize(int capacity, int slotSize) =>
        checked(HeaderSize + (long)capacity * slotSize);

    /// <summary>True when <paramref name="value"/> is a positive power of two.</summary>
    public static bool IsPowerOfTwo(int value) => value > 0 && (value & (value - 1)) == 0;

    /// <summary>Validates a capacity/slot-size pair, throwing <see cref="ArgumentOutOfRangeException"/> otherwise.</summary>
    public static void ValidateGeometry(int capacity, int slotSize)
    {
        if (!IsPowerOfTwo(capacity))
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity), capacity, "Capacity must be a positive power of two.");
        }

        if (slotSize <= MessageHeaderSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(slotSize), slotSize,
                $"Slot size must be greater than the {MessageHeaderSize}-byte message header.");
        }

        RequiredSize(capacity, slotSize); // verifies no arithmetic overflow
    }
}
