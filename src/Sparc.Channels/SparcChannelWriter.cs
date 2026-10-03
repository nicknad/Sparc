using Sparc.Core;

namespace Sparc.Channels;

/// <summary>
/// Typed producer end of a SPARC channel: encodes <typeparamref name="T"/> and
/// publishes it through a role-typed producer endpoint.
/// </summary>
/// <remarks>
/// Exactly one thread may write to one writer at a time (the endpoint contract).
/// <see cref="WriteAsync"/> waits for space without blocking a thread; the
/// publish itself is a bounded-memory, allocation-free encode directly into the
/// reserved slot.
/// </remarks>
public sealed class SparcChannelWriter<T> : IDisposable
{
    /// <summary>Type tag stored with every message written by this writer.</summary>
    public const int DefaultMessageType = 1;

    private readonly IProducerEndpoint _endpoint;
    private readonly ISparcCodec<T> _codec;
    private int _disposed;

    private SparcChannelWriter(IProducerEndpoint endpoint, ISparcCodec<T> codec)
    {
        if (endpoint.MaxPayloadSize < codec.MaxSize)
        {
            throw new ArgumentException(
                $"Slot payload of {endpoint.MaxPayloadSize} bytes cannot hold the codec maximum of {codec.MaxSize} bytes.",
                nameof(codec));
        }

        _endpoint = endpoint;
        _codec = codec;
    }

    /// <summary>The underlying role-typed endpoint (for states, leases and diagnostics).</summary>
    public IProducerEndpoint Endpoint => _endpoint;

    /// <summary>Region name.</summary>
    public string Name => _endpoint.Name;

    /// <summary>
    /// Creates the region if needed, claims the producer role and returns the
    /// writer.
    /// </summary>
    /// <param name="factory">Transport that creates or opens the region.</param>
    /// <param name="name">Region name both processes share.</param>
    /// <param name="codec">Message codec.</param>
    /// <param name="capacity">Number of slots; must be a power of two.</param>
    /// <param name="slotSize">
    /// Bytes per slot; 0 derives a cache-line-rounded size from
    /// <see cref="ISparcCodec{T}.MaxSize"/>.
    /// </param>
    /// <param name="options">Open timeout, existing-region policy, takeover.</param>
    /// <param name="cancellationToken">Bounds the role-claim retry loop.</param>
    public static SparcChannelWriter<T> Open(
        IIpcMemoryRegionFactory factory,
        string name,
        ISparcCodec<T> codec,
        int capacity = SparcRing.DefaultCapacity,
        int slotSize = 0,
        SharedRingBufferOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(codec);

        IProducerEndpoint endpoint = SparcRing.OpenProducer(
            factory, name, capacity, ResolveSlotSize(codec, slotSize), options, cancellationToken);
        return new SparcChannelWriter<T>(endpoint, codec);
    }

    /// <summary>Encodes and publishes one message; false when the ring is full.</summary>
    public bool TryWrite(T item)
    {
        ThrowIfDisposed();

        // Reserve first so the codec encodes straight into the slot: no scratch,
        // one copy, and a full ring never pays for an encode that is discarded.
        if (!_endpoint.TryReserveWrite(DefaultMessageType, out Span<byte> payload))
        {
            return false;
        }

        int length;
        try
        {
            length = _codec.Encode(item, payload);
        }
        catch
        {
            // A throwing codec must not leave the reservation pending.
            _endpoint.AbandonWrite();
            throw;
        }

        _endpoint.CommitWrite(length);
        return true;
    }

    /// <summary>
    /// Waits for space and publishes one message. Throws
    /// <see cref="InvalidOperationException"/> when the consumer has stopped.
    /// </summary>
    public async ValueTask WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        while (!TryWrite(item))
        {
            if (!await WaitToWriteAsync(cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidOperationException(
                    $"Channel '{Name}' consumer is gone; the message cannot be published.");
            }
        }
    }

    /// <summary>
    /// Completes when the ring has space or returns false when the consumer has
    /// stopped.
    /// </summary>
    public async ValueTask<bool> WaitToWriteAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        // Fast path: let a draining consumer free a slot across a few yields
        // before paying the ~1 ms Task.Delay timer, which otherwise dominates
        // short-lived backpressure.
        for (int i = 0; i < 32 && _endpoint.Count >= _endpoint.Capacity; i++)
        {
            if (_endpoint.ConsumerState is RingBufferEndpointState.Stopped or RingBufferEndpointState.Faulted)
            {
                return false;
            }

            await Task.Yield();
        }

        while (_endpoint.Count >= _endpoint.Capacity)
        {
            if (_endpoint.ConsumerState is RingBufferEndpointState.Stopped or RingBufferEndpointState.Faulted)
            {
                return false;
            }

            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>Disposes the endpoint, which publishes the graceful stop to the consumer.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _endpoint.Dispose();
        }
    }

    internal static int ResolveSlotSize(ISparcCodec<T> codec, int slotSize)
    {
        if (slotSize > 0)
        {
            return slotSize;
        }

        return RingBufferLayout.RoundSlotSizeToCacheLine(
            codec.MaxSize + RingBufferLayout.MessageHeaderSize);
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
    }
}
