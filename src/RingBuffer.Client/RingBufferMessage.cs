using System.Buffers.Binary;

namespace RingBuffer.Client;

/// <summary>
/// Sample message protocol layered on top of the byte-oriented ring buffer:
/// <c>[sequence:int64][timestamp:int64][fill...]</c> (little-endian).
/// </summary>
/// <remarks>
/// The ring buffer itself is payload-agnostic; this type defines the convention
/// used by the producer/consumer sessions so sequence, ordering and corruption
/// can be verified end to end.
/// </remarks>
public static class RingBufferMessage
{
    /// <summary>Bytes occupied by sequence + timestamp.</summary>
    public const int HeaderSize = sizeof(long) + sizeof(long);

    /// <summary>Byte used to fill unused payload space so corruption is detectable.</summary>
    public const byte FillByte = 0xA5;

    /// <summary>Writes the sequence/timestamp header; the caller fills the rest.</summary>
    public static void Write(Span<byte> payload, long sequence, long timestamp)
    {
        BinaryPrimitives.WriteInt64LittleEndian(payload, sequence);
        BinaryPrimitives.WriteInt64LittleEndian(payload[sizeof(long)..], timestamp);
    }

    public static long ReadSequence(ReadOnlySpan<byte> payload) =>
        BinaryPrimitives.ReadInt64LittleEndian(payload);

    public static long ReadTimestamp(ReadOnlySpan<byte> payload) =>
        BinaryPrimitives.ReadInt64LittleEndian(payload[sizeof(long)..]);

    /// <summary>Fills the payload area behind the header with <see cref="FillByte"/>.</summary>
    public static void FillPayload(Span<byte> payload) => payload[HeaderSize..].Fill(FillByte);

    /// <summary>True when the first <paramref name="length"/> bytes contain only the fill pattern.</summary>
    public static bool IsPayloadIntact(ReadOnlySpan<byte> payload, int length) =>
        payload[HeaderSize..length].IndexOfAnyExcept(FillByte) < 0;
}
