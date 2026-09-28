using System.Buffers.Binary;
using System.Diagnostics;

namespace Sparc.Core;

/// <summary>
/// Per-slot framing stored in front of every payload:
/// <c>[int32 length][int32 type][payload...]</c> (little-endian).
/// </summary>
internal static class SlotFraming
{
    private const int TypeOffset = sizeof(int);

    public static void Write(Span<byte> slot, int type, ReadOnlySpan<byte> payload)
    {
        Debug.Assert(slot.Length >= RingBufferLayout.MessageHeaderSize);
        Debug.Assert(payload.Length <= slot.Length - RingBufferLayout.MessageHeaderSize);

        BinaryPrimitives.WriteInt32LittleEndian(slot, payload.Length);
        BinaryPrimitives.WriteInt32LittleEndian(slot[TypeOffset..], type);
        payload.CopyTo(slot[RingBufferLayout.MessageHeaderSize..]);
    }

    public static int ReadHeader(ReadOnlySpan<byte> slot, out int type)
    {
        Debug.Assert(slot.Length >= RingBufferLayout.MessageHeaderSize);

        int length = BinaryPrimitives.ReadInt32LittleEndian(slot);
        if ((uint)length > (uint)(slot.Length - RingBufferLayout.MessageHeaderSize))
        {
            throw new RingBufferCorruptedException(
                $"Slot declares a payload of {length} bytes but only " +
                $"{slot.Length - RingBufferLayout.MessageHeaderSize} are available.");
        }

        type = BinaryPrimitives.ReadInt32LittleEndian(slot[TypeOffset..]);
        return length;
    }

    public static void Read(ReadOnlySpan<byte> slot, Span<byte> destination, out int bytesRead, out int type)
    {
        Debug.Assert(destination.Length >= slot.Length - RingBufferLayout.MessageHeaderSize);

        int length = ReadHeader(slot, out type);
        slot.Slice(RingBufferLayout.MessageHeaderSize, length).CopyTo(destination);
        bytesRead = length;
    }
}
