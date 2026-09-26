using System.Buffers.Binary;

namespace RingBuffer.Core;

/// <summary>
/// Per-slot framing stored in front of every payload:
/// <c>[int32 length][int32 type][payload...]</c> (little-endian).
/// </summary>
public static class SlotFraming
{
    public static void Write(Span<byte> slot, int type, ReadOnlySpan<byte> payload)
    {
        BinaryPrimitives.WriteInt32LittleEndian(slot, payload.Length);
        BinaryPrimitives.WriteInt32LittleEndian(slot[4..], type);
        payload.CopyTo(slot[RingBufferLayout.MessageHeaderSize..]);
    }

    public static void Read(ReadOnlySpan<byte> slot, Span<byte> destination, out int bytesRead, out int type)
    {
        int length = BinaryPrimitives.ReadInt32LittleEndian(slot);
        if ((uint)length > (uint)(slot.Length - RingBufferLayout.MessageHeaderSize))
        {
            throw new RingBufferCorruptedException(
                $"Slot declares a payload of {length} bytes but only " +
                $"{slot.Length - RingBufferLayout.MessageHeaderSize} are available.");
        }

        type = BinaryPrimitives.ReadInt32LittleEndian(slot[4..]);
        slot.Slice(RingBufferLayout.MessageHeaderSize, length).CopyTo(destination);
        bytesRead = length;
    }
}
