using System.Buffers.Binary;
using System.Diagnostics;
using Sparc.Core;

namespace Sparc.Serialization;

/// <summary>
/// Per-chunk header inside the slot payload of a chunked stream message:
/// <c>[int32 flags][data...]</c>, little-endian. <c>First</c> marks the start of
/// a message, <c>Last</c> marks its end; both are set on a single-chunk
/// message.
/// </summary>
internal static class ChunkFraming
{
    public const int HeaderSize = sizeof(int);
    public const int FirstFlag = 1;
    public const int LastFlag = 2;

    private const int KnownFlags = FirstFlag | LastFlag;

    public static int ReadFlags(ReadOnlySpan<byte> chunk)
    {
        if (chunk.Length < HeaderSize)
        {
            throw new RingBufferCorruptedException(
                $"Chunk payload of {chunk.Length} bytes is smaller than the {HeaderSize}-byte chunk header.");
        }

        int flags = BinaryPrimitives.ReadInt32LittleEndian(chunk);
        if ((flags & ~KnownFlags) != 0)
        {
            throw new RingBufferCorruptedException($"Chunk declares unknown flags 0x{flags:X8}.");
        }

        if ((flags & LastFlag) == 0 && chunk.Length == HeaderSize)
        {
            throw new RingBufferCorruptedException("Chunk has no payload but is not the message's last chunk.");
        }

        return flags;
    }

    public static unsafe void WriteFlags(byte* chunk, bool first, bool last)
    {
        Debug.Assert(chunk != null);
        BinaryPrimitives.WriteInt32LittleEndian(
            new Span<byte>(chunk, HeaderSize), (first ? FirstFlag : 0) | (last ? LastFlag : 0));
    }
}
