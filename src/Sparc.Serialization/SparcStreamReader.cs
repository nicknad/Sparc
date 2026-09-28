using Sparc.Core;

namespace Sparc.Serialization;

/// <summary>
/// Consumer end of a chunked message stream: reassembles one complete message
/// per call into a caller-provided destination span.
/// </summary>
/// <remarks>
/// <para>
/// Chunks carry <c>First</c>/<c>Last</c> flags. A reader that starts (or
/// restarts) mid-message discards the interrupted tail until the next chunk
/// with the <c>First</c> flag, so a consumer crash never delivers a torn
/// message: single-chunk messages stay at-least-once, while a multi-chunk
/// message interrupted by a consumer or producer crash is abandoned
/// (at-most-once) and the next complete message is delivered.
/// </para>
/// <para>
/// Continuation chunks are awaited up to <c>timeout</c> each; when
/// the producer stops mid-message the partial message is discarded and
/// <see cref="RingBufferTimeoutException"/> is thrown. A message larger than
/// the destination span is abandoned and reported as
/// <see cref="InvalidOperationException"/>.
/// </para>
/// <para>
/// Exactly one thread may read from one reader at a time (the endpoint
/// contract). The reader owns the endpoint and disposes it.
/// </para>
/// </remarks>
public sealed class SparcStreamReader : IDisposable
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    private readonly IConsumerEndpoint _endpoint;
    private int _disposed;

    /// <summary>Wraps an open consumer endpoint, which the reader owns.</summary>
    public SparcStreamReader(IConsumerEndpoint endpoint)
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
    public IConsumerEndpoint Endpoint => _endpoint;

    /// <summary>Region name.</summary>
    public string Name => _endpoint.Name;

    /// <summary>
    /// Creates the region if needed, claims the consumer role and returns the
    /// reader.
    /// </summary>
    /// <param name="factory">Transport that creates or opens the shared region.</param>
    /// <param name="name">Region name both processes share.</param>
    /// <param name="capacity">Slots to request when this process creates the region.</param>
    /// <param name="slotSize">Bytes per slot to request when this process creates the region.</param>
    /// <param name="options">Open timeout, existing-region policy, takeover.</param>
    /// <param name="cancellationToken">Bounds the role-claim retry loop.</param>
    public static SparcStreamReader Open(
        IIpcMemoryRegionFactory factory,
        string name,
        int capacity = SparcRing.DefaultCapacity,
        int slotSize = SparcRing.DefaultSlotSize,
        SharedRingBufferOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        return new SparcStreamReader(
            SparcRing.OpenConsumer(factory, name, capacity, slotSize, options, cancellationToken));
    }

    /// <summary>
    /// Reads one complete message into <paramref name="destination"/>, skipping
    /// the tail of any interrupted message first. Returns the bytes written and
    /// the message type.
    /// </summary>
    /// <param name="destination">Receives the message bytes; must fit the message.</param>
    /// <param name="type">The type tag stored with the message's chunks.</param>
    /// <param name="timeout">
    /// How long to wait for the next chunk while the ring is empty; defaults to
    /// 30 seconds.
    /// </param>
    /// <param name="cancellationToken">Cancels the wait for the next chunk.</param>
    public int ReadMessage(
        Span<byte> destination,
        out int type,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        TimeSpan wait = timeout ?? DefaultTimeout;

        ReadLease lease = LeaseNext(wait, cancellationToken);
        while (true)
        {
            int flags = ChunkFraming.ReadFlags(lease.Payload);
            if ((flags & ChunkFraming.FirstFlag) == 0)
            {
                lease.Dispose();
                lease = LeaseNext(wait, cancellationToken);
                continue; // interrupted tail: skip to the next message start
            }

            type = lease.Type;
            int total = 0;
            while (true)
            {
                ReadOnlySpan<byte> data = lease.Payload[ChunkFraming.HeaderSize..];
                if (total + data.Length > destination.Length)
                {
                    lease.Dispose();
                    throw new InvalidOperationException(
                        $"The stream message is larger than the {destination.Length}-byte destination; " +
                        "the message was abandoned.");
                }

                data.CopyTo(destination[total..]);
                total += data.Length;
                bool last = (flags & ChunkFraming.LastFlag) != 0;
                lease.Dispose();
                if (last)
                {
                    return total;
                }

                lease = LeaseNext(wait, cancellationToken);
                flags = ChunkFraming.ReadFlags(lease.Payload);
                if ((flags & ChunkFraming.FirstFlag) != 0)
                {
                    break; // producer restarted mid-message: abandon the partial tail
                }
            }
        }
    }

    /// <summary>Disposes the endpoint, which publishes the graceful stop to the producer.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _endpoint.Dispose();
        }
    }

    private ReadLease LeaseNext(TimeSpan timeout, CancellationToken cancellationToken) =>
        _endpoint.TryBeginRead(out ReadLease lease) ? lease : _endpoint.Read(timeout, cancellationToken);
}
