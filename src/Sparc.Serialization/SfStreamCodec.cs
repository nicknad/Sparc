using Sparc.Core;

namespace Sparc.Serialization;

/// <summary>
/// Serializes one value through a chunked message writer, for example by
/// calling <c>GetSpan</c>/<c>Advance</c> or a serializer method generic over
/// <see cref="SerializerFoundation.IWriteBuffer"/>.
/// </summary>
/// <typeparam name="T">Message type.</typeparam>
/// <param name="buffer">The chunked destination buffer for one message.</param>
/// <param name="value">Value to encode.</param>
public delegate void SfStreamSerializer<T>(ref SparcStreamWriteBuffer buffer, T value);

/// <summary>
/// Deserializes one value from a chunked message reader, for example by
/// calling <c>GetUnreadSpan</c>/<c>TryGetSpan</c> and <c>Advance</c>.
/// </summary>
/// <typeparam name="T">Message type.</typeparam>
/// <param name="buffer">The chunked source buffer for one message.</param>
/// <returns>The decoded value.</returns>
public delegate T SfStreamDeserializer<T>(ref SparcStreamReadBuffer buffer);

/// <summary>
/// Ties a chunked stream writer and reader together with a serializer and a
/// deserializer, the streaming counterpart of
/// <see cref="SfCodec{T}"/>: values of any size get one chunked message each,
/// with serialization failures published as aborted messages instead of
/// truncated ones.
/// </summary>
/// <typeparam name="T">Message type.</typeparam>
/// <remarks>
/// <see cref="Read"/> requires the deserializer to consume the whole message;
/// trailing bytes mean the two ends disagree about the value and are reported
/// as <see cref="RingBufferCorruptedException"/>. The delegates run
/// synchronously and must be stateless, like the codec itself.
/// </remarks>
public sealed class SfStreamCodec<T>
{
    private readonly SfStreamSerializer<T> _serialize;
    private readonly SfStreamDeserializer<T> _deserialize;

    /// <summary>Creates the codec.</summary>
    /// <param name="messageType">Type tag stored on every chunk of the message.</param>
    /// <param name="serialize">Writes one value into the chunked buffer.</param>
    /// <param name="deserialize">Reads one value from the chunked buffer.</param>
    public SfStreamCodec(int messageType, SfStreamSerializer<T> serialize, SfStreamDeserializer<T> deserialize)
    {
        ArgumentNullException.ThrowIfNull(serialize);
        ArgumentNullException.ThrowIfNull(deserialize);
        MessageType = messageType;
        _serialize = serialize;
        _deserialize = deserialize;
    }

    /// <summary>Type tag stored on every chunk of the message.</summary>
    public int MessageType { get; }

    /// <summary>
    /// Serializes and publishes one chunked message. When the configured
    /// serializer throws, the partially published message is aborted and the
    /// consumer reports corruption instead of a truncated value.
    /// </summary>
    /// <param name="writer">Producer end of the stream.</param>
    /// <param name="value">Value to encode.</param>
    public void Write(SparcStreamWriter writer, T value)
    {
        ArgumentNullException.ThrowIfNull(writer);
        SparcStreamWriteBuffer buffer = writer.BeginMessage(MessageType);
        try
        {
            _serialize(ref buffer, value);
        }
        catch
        {
            buffer.Abort();
            throw;
        }

        buffer.Dispose();
    }

    /// <summary>
    /// Reads one complete chunked message and deserializes it.
    /// </summary>
    /// <param name="reader">Consumer end of the stream.</param>
    /// <param name="scratch">
    /// Caller-owned scratch for windows that cross chunk seams; see
    /// <see cref="SparcStreamReader.BeginMessage"/>.
    /// </param>
    /// <param name="timeout">How long to wait for the next chunk; defaults to 30 seconds.</param>
    /// <param name="cancellationToken">Cancels waits for chunks.</param>
    public T Read(
        SparcStreamReader reader,
        Span<byte> scratch,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        SparcStreamReadBuffer buffer = reader.BeginMessage(scratch, timeout, cancellationToken);
        try
        {
            T value = _deserialize(ref buffer);
            if (!buffer.IsMessageComplete)
            {
                throw new RingBufferCorruptedException(
                    "The deserializer did not consume the whole stream message.");
            }

            return value;
        }
        finally
        {
            buffer.Dispose();
        }
    }
}
