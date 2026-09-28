using System.Text.Json;
using Sparc.Core;

namespace Sparc.Channels;

/// <summary>
/// JSON codec for convenience. Encoding allocates (it goes through
/// <see cref="JsonSerializer"/>), so hot paths should use a hand-written
/// <see cref="ISparcCodec{T}"/> instead.
/// </summary>
/// <typeparam name="T">Message type.</typeparam>
public sealed class JsonCodec<T> : ISparcCodec<T>
{
    private readonly JsonSerializerOptions? _options;

    /// <param name="maxSize">
    /// Maximum encoded size; the channel derives its slot size from this when
    /// none is given. Messages that encode larger throw.
    /// </param>
    /// <param name="options">Optional serializer options.</param>
    public JsonCodec(int maxSize = 4096, JsonSerializerOptions? options = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxSize, 1);
        MaxSize = maxSize;
        _options = options;
    }

    /// <inheritdoc />
    public int MaxSize { get; }

    /// <inheritdoc />
    public int Encode(T item, Span<byte> destination)
    {
        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(item, _options);
        if (bytes.Length > Math.Min(destination.Length, MaxSize))
        {
            throw new InvalidOperationException(
                $"Encoded message of {bytes.Length} bytes exceeds the codec maximum of {MaxSize} bytes.");
        }

        bytes.CopyTo(destination);
        return bytes.Length;
    }

    /// <inheritdoc />
    public T Decode(ReadOnlySpan<byte> source) =>
        JsonSerializer.Deserialize<T>(source, _options)
        ?? throw new RingBufferCorruptedException("JSON payload deserialized to null.");
}
