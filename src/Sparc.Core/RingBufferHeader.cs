using System.Buffers.Binary;

namespace Sparc.Core;

/// <summary>
/// Managed snapshot of the fixed-size region header. The authoritative copy lives
/// in the mapped region; this struct is used to read/validate/populate it.
/// </summary>
public struct RingBufferHeader
{
    public ulong Magic;
    public int Version;
    public int Capacity;
    public int SlotSize;
    public int HeaderSize;
    public int MaxPayloadSize;
    public int Flags;
    public RingBufferEndpointState ProducerState;
    public RingBufferEndpointState ConsumerState;

    /// <summary>
    /// Sequence number of the next slot the consumer will read. This value must
    /// only be accessed atomically when it lives in shared memory.
    /// </summary>
    public long Head;

    /// <summary>
    /// Sequence number of the next slot the producer will write. This value must
    /// only be accessed atomically when it lives in shared memory.
    /// </summary>
    public long Tail;

    /// <summary>Reads a header (and validates none of it) from a byte span.</summary>
    public static RingBufferHeader Read(ReadOnlySpan<byte> source)
    {
        if (source.Length < RingBufferLayout.HeaderSize)
        {
            throw new ArgumentException(
                $"Source must contain at least {RingBufferLayout.HeaderSize} bytes.", nameof(source));
        }

        return new RingBufferHeader
        {
            Magic = BinaryPrimitives.ReadUInt64LittleEndian(source[RingBufferLayout.MagicOffset..]),
            Version = BinaryPrimitives.ReadInt32LittleEndian(source[RingBufferLayout.VersionOffset..]),
            Capacity = BinaryPrimitives.ReadInt32LittleEndian(source[RingBufferLayout.CapacityOffset..]),
            SlotSize = BinaryPrimitives.ReadInt32LittleEndian(source[RingBufferLayout.SlotSizeOffset..]),
            HeaderSize = BinaryPrimitives.ReadInt32LittleEndian(source[RingBufferLayout.HeaderSizeOffset..]),
            MaxPayloadSize = BinaryPrimitives.ReadInt32LittleEndian(source[RingBufferLayout.MaxPayloadSizeOffset..]),
            Flags = BinaryPrimitives.ReadInt32LittleEndian(source[RingBufferLayout.FlagsOffset..]),
            ProducerState = (RingBufferEndpointState)BinaryPrimitives.ReadInt32LittleEndian(source[RingBufferLayout.ProducerStateOffset..]),
            ConsumerState = (RingBufferEndpointState)BinaryPrimitives.ReadInt32LittleEndian(source[RingBufferLayout.ConsumerStateOffset..]),
            Head = BinaryPrimitives.ReadInt64LittleEndian(source[RingBufferLayout.HeadOffset..]),
            Tail = BinaryPrimitives.ReadInt64LittleEndian(source[RingBufferLayout.TailOffset..]),
        };
    }

    /// <summary>
    /// Writes metadata to a destination span. When <paramref name="includeMagic"/>
    /// is false the magic field is left untouched so the caller can publish it
    /// last, with release semantics.
    /// </summary>
    public readonly void WriteTo(Span<byte> destination, bool includeMagic = true)
    {
        if (destination.Length < RingBufferLayout.HeaderSize)
        {
            throw new ArgumentException(
                $"Destination must contain at least {RingBufferLayout.HeaderSize} bytes.", nameof(destination));
        }

        if (includeMagic)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(destination[RingBufferLayout.MagicOffset..], Magic);
        }

        BinaryPrimitives.WriteInt32LittleEndian(destination[RingBufferLayout.VersionOffset..], Version);
        BinaryPrimitives.WriteInt32LittleEndian(destination[RingBufferLayout.CapacityOffset..], Capacity);
        BinaryPrimitives.WriteInt32LittleEndian(destination[RingBufferLayout.SlotSizeOffset..], SlotSize);
        BinaryPrimitives.WriteInt32LittleEndian(destination[RingBufferLayout.HeaderSizeOffset..], HeaderSize);
        BinaryPrimitives.WriteInt32LittleEndian(destination[RingBufferLayout.MaxPayloadSizeOffset..], MaxPayloadSize);
        BinaryPrimitives.WriteInt32LittleEndian(destination[RingBufferLayout.FlagsOffset..], Flags);
        BinaryPrimitives.WriteInt32LittleEndian(destination[RingBufferLayout.ProducerStateOffset..], (int)ProducerState);
        BinaryPrimitives.WriteInt32LittleEndian(destination[RingBufferLayout.ConsumerStateOffset..], (int)ConsumerState);
        BinaryPrimitives.WriteInt64LittleEndian(destination[RingBufferLayout.HeadOffset..], Head);
        BinaryPrimitives.WriteInt64LittleEndian(destination[RingBufferLayout.TailOffset..], Tail);
    }

    /// <summary>Builds a validated header for a brand new region.</summary>
    public static RingBufferHeader CreateNew(int capacity, int slotSize)
    {
        RingBufferLayout.ValidateGeometry(capacity, slotSize);

        return new RingBufferHeader
        {
            Magic = RingBufferLayout.Magic,
            Version = RingBufferLayout.Version,
            Capacity = capacity,
            SlotSize = slotSize,
            HeaderSize = RingBufferLayout.HeaderSize,
            MaxPayloadSize = RingBufferLayout.MaxPayloadSizeFor(slotSize),
            Flags = 0,
            ProducerState = RingBufferEndpointState.NotPresent,
            ConsumerState = RingBufferEndpointState.NotPresent,
            Head = 0,
            Tail = 0,
        };
    }

    /// <summary>
    /// Validates magic/version/geometry. Throws a specific exception describing
    /// exactly what is wrong, including <paramref name="regionName"/>.
    /// </summary>
    public readonly void Validate(string regionName)
    {
        if (Magic != RingBufferLayout.Magic)
        {
            throw new RingBufferCorruptedException(
                $"Region '{regionName}' does not contain a ring buffer (bad magic 0x{Magic:X16}).");
        }

        if (Version != RingBufferLayout.Version)
        {
            throw new RingBufferVersionMismatchException(
                $"Region '{regionName}' uses layout version {Version}, this build supports version {RingBufferLayout.Version}.");
        }

        if (HeaderSize != RingBufferLayout.HeaderSize)
        {
            throw new RingBufferCorruptedException(
                $"Region '{regionName}' declares header size {HeaderSize}, expected {RingBufferLayout.HeaderSize}.");
        }

        if (!RingBufferLayout.IsPowerOfTwo(Capacity) || Capacity <= 0 || RingBufferLayout.MaxPayloadSizeFor(SlotSize) != MaxPayloadSize)
        {
            throw new RingBufferCorruptedException(
                $"Region '{regionName}' contains inconsistent geometry (capacity={Capacity}, slotSize={SlotSize}, maxPayload={MaxPayloadSize}).");
        }
    }

    /// <summary>Validates that the region geometry matches what the caller asked for.</summary>
    public readonly void ValidateRequestedGeometry(string regionName, int capacity, int slotSize)
    {
        if (Capacity != capacity || SlotSize != slotSize)
        {
            throw new RingBufferGeometryMismatchException(
                $"Region '{regionName}' has capacity={Capacity}, slotSize={SlotSize}; " +
                $"requested capacity={capacity}, slotSize={slotSize}. " +
                "The geometry is fixed by whichever process created the region.");
        }
    }
}
