using System.Buffers;
using System.Text.Json;
using Sparc.Core;

namespace Sparc.Channels;

/// <summary>
/// JSON codec for convenience. Encoding serializes into a pooled buffer and
/// copies once into the caller's destination span: no per-message
/// <c>byte[]</c> allocation and no second copy.
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
        int limit = Math.Min(destination.Length, MaxSize);
        if (limit <= 0)
        {
            throw new InvalidOperationException(
                $"Encoded message exceeds the codec maximum of {MaxSize} bytes.");
        }

        JsonWriterOptions writerOptions = _options is null
            ? default
            : new JsonWriterOptions
            {
                Indented = _options.WriteIndented,
                Encoder = _options.Encoder,
            };

        PooledBufferWriter output = new(limit);
        try
        {
            try
            {
                // Dispose flushes, so the overflow check must wrap the using.
                using Utf8JsonWriter writer = new(output, writerOptions);
                JsonSerializer.Serialize(writer, item, _options);
                writer.Flush();
            }
            catch (EncodeOverflowException exception)
            {
                throw new InvalidOperationException(
                    $"Encoded message exceeds the codec maximum of {MaxSize} bytes.", exception);
            }

            output.WrittenSpan.CopyTo(destination);
            return output.Written;
        }
        finally
        {
            output.Dispose();
        }
    }

    /// <inheritdoc />
    public T Decode(ReadOnlySpan<byte> source) =>
        JsonSerializer.Deserialize<T>(source, _options)
        ?? throw new RingBufferCorruptedException("JSON payload deserialized to null.");

    /// <summary>Thrown when the encoded JSON does not fit the codec's maximum.</summary>
    private sealed class EncodeOverflowException : Exception;

    /// <summary>
    /// A growable <see cref="IBufferWriter{T}"/> over an
    /// <see cref="ArrayPool{T}"/> buffer that fails instead of growing once the
    /// codec's maximum is exceeded. The serializer's minimum first window is
    /// larger than some slots, so the capacity may exceed the limit while the
    /// committed bytes never do.
    /// </summary>
    private sealed class PooledBufferWriter(int limit) : IBufferWriter<byte>, IDisposable
    {
        private byte[] _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(limit, 256));
        private int _written;

        public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _written);

        public int Written => _written;

        public void Advance(int count)
        {
            if (count < 0 || _written + count > limit)
            {
                throw new EncodeOverflowException();
            }

            _written += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            Ensure(sizeHint);
            return _buffer.AsMemory(_written);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            Ensure(sizeHint);
            return _buffer.AsSpan(_written);
        }

        public void Dispose() => ArrayPool<byte>.Shared.Return(_buffer);

        private void Ensure(int sizeHint)
        {
            if (sizeHint < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(sizeHint));
            }

            int required = _written + Math.Max(sizeHint, 1);
            if (required <= _buffer.Length)
            {
                return;
            }

            byte[] next = ArrayPool<byte>.Shared.Rent(Math.Max(required, _buffer.Length * 2));
            _buffer.AsSpan(0, _written).CopyTo(next);
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = next;
        }
    }
}
