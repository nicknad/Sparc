using System.Diagnostics;
using SerializerFoundation;
using Sparc.Channels;

namespace Sparc.Serialization;

/// <summary>
/// Serializes one value through a SerializerFoundation write buffer, for
/// example by calling <c>GetSpan</c>/<c>Advance</c> or a serializer method
/// generic over <see cref="IWriteBuffer"/>.
/// </summary>
/// <typeparam name="T">Message type.</typeparam>
/// <param name="buffer">
/// Destination window over the codec's span; the serializer must not write
/// more than the buffer accepts.
/// </param>
/// <param name="value">Value to encode.</param>
public delegate void SfSerializer<T>(ref CompatibleSpanWriteBuffer buffer, T value);

/// <summary>
/// Deserializes one value through a SerializerFoundation read buffer, for
/// example by calling <c>GetUnreadSpan</c>/<c>TryGetSpan</c> and
/// <c>Advance</c>.
/// </summary>
/// <typeparam name="T">Message type.</typeparam>
/// <param name="buffer">Source window over the slot payload.</param>
/// <returns>The decoded value.</returns>
public delegate T SfDeserializer<T>(ref CompatibleReadOnlySpanReadBuffer buffer);

/// <summary>
/// Adapts a SerializerFoundation-based serializer to
/// <see cref="ISparcCodec{T}"/>: the serializer writes through
/// <see cref="IWriteBuffer"/> into the caller's destination span and reads
/// through <see cref="IReadBuffer"/> from a slot's payload span, with no
/// intermediate serializer copy. The channel itself still stages encoded bytes
/// in a <see cref="MaxSize"/> scratch buffer and publishes one copy into the
/// slot.
/// </summary>
/// <typeparam name="T">Message type.</typeparam>
/// <remarks>
/// Uses the <c>Compatible*</c> buffer structs so the same codec works on every
/// target framework and with serializers whose formatters are generic over
/// <c>struct, IWriteBuffer, allows ref struct</c>. The delegates run
/// synchronously on the calling thread and must be stateless, like the codec
/// itself.
/// </remarks>
public sealed class SfCodec<T> : ISparcCodec<T>
{
    private readonly SfSerializer<T> _serialize;
    private readonly SfDeserializer<T> _deserialize;

    /// <summary>Creates the codec.</summary>
    /// <param name="maxSize">
    /// Upper bound in bytes of one encoded message; the channel derives its
    /// slot size from this when none is given.
    /// </param>
    /// <param name="serialize">Writes one value into the destination window.</param>
    /// <param name="deserialize">Reads one value from the source window.</param>
    public SfCodec(int maxSize, SfSerializer<T> serialize, SfDeserializer<T> deserialize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxSize, 1);
        ArgumentNullException.ThrowIfNull(serialize);
        ArgumentNullException.ThrowIfNull(deserialize);
        MaxSize = maxSize;
        _serialize = serialize;
        _deserialize = deserialize;
    }

    /// <inheritdoc />
    public int MaxSize { get; }

    /// <inheritdoc />
    public unsafe int Encode(T item, Span<byte> destination)
    {
        Debug.Assert(
            destination.Length >= MaxSize,
            "ISparcCodec<T>.Encode requires a destination of at least MaxSize bytes.");

        fixed (byte* pointer = destination)
        {
            CompatibleSpanWriteBuffer buffer = new(pointer, Math.Min(destination.Length, MaxSize));
            _serialize(ref buffer, item);
            Debug.Assert(buffer.BytesWritten <= MaxSize);
            return (int)buffer.BytesWritten;
        }
    }

    /// <inheritdoc />
    public unsafe T Decode(ReadOnlySpan<byte> source)
    {
        fixed (byte* pointer = source)
        {
            CompatibleReadOnlySpanReadBuffer buffer = new(pointer, source.Length);
            return _deserialize(ref buffer);
        }
    }
}
