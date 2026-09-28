using System.Buffers;
using System.Text;
using SerializerFoundation;
using Sparc.Core;

namespace Sparc.Serialization.Sample;

/// <summary>
/// MessagePack-style primitives: LEB128 varints for lengths and zigzagged
/// integers, length-prefixed UTF-8 for strings. The write side is generic over
/// the SerializerFoundation write buffer, so the same code serves a
/// <c>byte[]</c>, a pipe or a SPARC chunk stream; the read side is written
/// against <see cref="SparcStreamReadBuffer"/>, because a streamed message's
/// total length is unknown (see docs/streaming.md).
/// </summary>
internal static class BufferExtensions
{
    public static void WriteVarUInt64<TWriter>(ref TWriter writer, ulong value)
        where TWriter : struct, IWriteBuffer, allows ref struct
    {
        Span<byte> span = writer.GetSpan(10);
        int count = 0;
        while (value >= 0x80)
        {
            span[count++] = (byte)(value | 0x80);
            value >>= 7;
        }

        span[count++] = (byte)value;
        writer.Advance(count);
    }

    public static void WriteInt32<TWriter>(ref TWriter writer, int value)
        where TWriter : struct, IWriteBuffer, allows ref struct =>
        WriteVarUInt64(ref writer, unchecked((ulong)((value << 1) ^ (value >> 31))));

    public static void WriteString<TWriter>(ref TWriter writer, string value)
        where TWriter : struct, IWriteBuffer, allows ref struct
    {
        int byteCount = Encoding.UTF8.GetByteCount(value);
        WriteVarUInt64(ref writer, (ulong)byteCount);
        if (byteCount == 0)
        {
            return;
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            ReadOnlySpan<byte> bytes = rented.AsSpan(0, Encoding.UTF8.GetBytes(value, rented));
            while (!bytes.IsEmpty)
            {
                // Bulk writes must fit the destination's largest contiguous
                // window, so encode once and wave the bytes through in slices.
                const int SliceSize = 200;
                int length = Math.Min(bytes.Length, SliceSize);
                bytes[..length].CopyTo(writer.GetSpan(length));
                writer.Advance(length);
                bytes = bytes[length..];
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    public static ulong ReadVarUInt64(ref SparcStreamReadBuffer reader)
    {
        ulong value = 0;
        for (int shift = 0; shift < 64; shift += 7)
        {
            ReadOnlySpan<byte> span = reader.GetUnreadSpan();
            if (span.IsEmpty)
            {
                throw new EndOfStreamException("The message ended inside a varint.");
            }

            byte current = span[0];
            reader.Advance(1);
            value |= (ulong)(current & 0x7F) << shift;
            if ((current & 0x80) == 0)
            {
                return value;
            }
        }

        throw new RingBufferCorruptedException("Varint is longer than 64 bits.");
    }

    public static int ReadInt32(ref SparcStreamReadBuffer reader)
    {
        ulong value = ReadVarUInt64(ref reader);
        return unchecked((int)(value >> 1) ^ -(int)(value & 1));
    }

    public static string ReadString(ref SparcStreamReadBuffer reader)
    {
        int length = checked((int)ReadVarUInt64(ref reader));
        if (length == 0)
        {
            return string.Empty;
        }

        byte[] bytes = new byte[length];
        int offset = 0;
        while (offset < length)
        {
            ReadOnlySpan<byte> span = reader.GetUnreadSpan();
            if (span.IsEmpty)
            {
                throw new EndOfStreamException("The message ended inside a string.");
            }

            int take = Math.Min(span.Length, length - offset);
            span[..take].CopyTo(bytes.AsSpan(offset));
            reader.Advance(take);
            offset += take;
        }

        return Encoding.UTF8.GetString(bytes);
    }
}
