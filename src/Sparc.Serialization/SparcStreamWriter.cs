using Sparc.Core;

namespace Sparc.Serialization;

/// <summary>
/// Producer end of a chunked message stream: hands out one
/// <see cref="SparcStreamWriteBuffer"/> per message, so a serializer can
/// publish messages larger than one slot (or larger than the whole ring) with
/// no intermediate buffer.
/// </summary>
/// <remarks>
/// Exactly one thread may write to one writer at a time (the endpoint
/// contract). The writer owns the endpoint and disposes it, which publishes the
/// graceful stop to the consumer.
/// </remarks>
public sealed class SparcStreamWriter : IDisposable
{
    private readonly IProducerEndpoint _endpoint;
    private int _disposed;

    /// <summary>Wraps an open producer endpoint, which the writer owns.</summary>
    /// <param name="endpoint">
    /// Producer endpoint whose slot payload is large enough for a chunk header.
    /// </param>
    public SparcStreamWriter(IProducerEndpoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        if (endpoint.MaxPayloadSize <= ChunkFraming.HeaderSize)
        {
            throw new InvalidOperationException(
                $"Slot payload of {endpoint.MaxPayloadSize} bytes is too small for chunked messages; " +
                $"the payload must exceed the {ChunkFraming.HeaderSize}-byte chunk header.");
        }

        _endpoint = endpoint;
    }

    /// <summary>The underlying role-typed endpoint.</summary>
    public IProducerEndpoint Endpoint => _endpoint;

    /// <summary>Region name.</summary>
    public string Name => _endpoint.Name;

    /// <summary>
    /// Creates the region if needed, claims the producer role and returns the
    /// writer.
    /// </summary>
    /// <param name="factory">Transport that creates or opens the shared region.</param>
    /// <param name="name">Region name both processes share.</param>
    /// <param name="capacity">Number of slots; must be a power of two.</param>
    /// <param name="slotSize">Bytes per slot; larger slots mean fewer chunks.</param>
    /// <param name="options">Open timeout, existing-region policy, takeover.</param>
    /// <param name="cancellationToken">Bounds the role-claim retry loop.</param>
    public static SparcStreamWriter Open(
        IIpcMemoryRegionFactory factory,
        string name,
        int capacity = SparcRing.DefaultCapacity,
        int slotSize = SparcRing.DefaultSlotSize,
        SharedRingBufferOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        return new SparcStreamWriter(
            SparcRing.OpenProducer(factory, name, capacity, slotSize, options, cancellationToken));
    }

    /// <summary>
    /// Starts a chunked message. Dispose the returned buffer to publish the
    /// final chunk; nothing is visible to the consumer before the first chunk
    /// is committed.
    /// </summary>
    /// <param name="type">Caller-defined message tag stored on every chunk.</param>
    public SparcStreamWriteBuffer BeginMessage(int type)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        return new SparcStreamWriteBuffer(_endpoint, type);
    }

    /// <summary>Disposes the endpoint, which publishes the graceful stop to the consumer.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _endpoint.Dispose();
        }
    }
}
