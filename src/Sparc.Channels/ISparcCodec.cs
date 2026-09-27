namespace Sparc.Channels;

/// <summary>
/// Serializes <typeparamref name="T"/> to and from the channel's payload bytes.
/// Implementations must be stateless and safe to call from one producer thread
/// and one consumer thread at the same time.
/// </summary>
public interface ISparcCodec<T>
{
    /// <summary>
    /// Upper bound in bytes of one encoded message. The channel derives its
    /// slot size from this when no explicit size is given.
    /// </summary>
    int MaxSize { get; }

    /// <summary>
    /// Encodes <paramref name="item"/> into <paramref name="destination"/>
    /// (length is at least <see cref="MaxSize"/>) and returns the bytes written.
    /// </summary>
    int Encode(T item, Span<byte> destination);

    /// <summary>Decodes one message from the bytes carried by a slot.</summary>
    T Decode(ReadOnlySpan<byte> source);
}
