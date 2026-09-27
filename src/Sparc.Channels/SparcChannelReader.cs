using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Sparc.Core;

namespace Sparc.Channels;

/// <summary>
/// Typed consumer end of a SPARC channel: decodes <typeparamref name="T"/> from
/// a role-typed consumer endpoint.
/// </summary>
/// <remarks>
/// <para>
/// The first asynchronous read starts a dedicated pump thread that blocks on the
/// endpoint and queues decoded messages into an in-process channel, so
/// <see cref="ReadAsync"/> and <see cref="ReadAllAsync"/> never block a
/// thread-pool thread. The pump completes the queue when the producer stops
/// (endpoint state <c>Stopped</c>/<c>Faulted</c>) or fails.
/// </para>
/// <para>
/// While only <see cref="TryRead"/> is used, no thread is started: it reads the
/// endpoint directly. Do not mix direct <see cref="Endpoint"/> reads with this
/// reader; the pump is the single reader once it starts.
/// </para>
/// </remarks>
public sealed class SparcChannelReader<T> : IDisposable
{
    private static readonly TimeSpan DefaultIdleTimeout = TimeSpan.FromMilliseconds(250);

    private readonly IConsumerEndpoint _endpoint;
    private readonly ISparcCodec<T> _codec;
    private readonly Channel<T> _inbox = Channel.CreateUnbounded<T>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
    private readonly CancellationTokenSource _cancellation = new();
    private readonly TimeSpan _idleTimeout;
    private Task? _pump;
    private int _pumpStarted;
    private int _disposed;

    private SparcChannelReader(IConsumerEndpoint endpoint, ISparcCodec<T> codec, TimeSpan idleTimeout)
    {
        _endpoint = endpoint;
        _codec = codec;
        _idleTimeout = idleTimeout;
    }

    /// <summary>The underlying role-typed endpoint (for states and diagnostics).</summary>
    public IConsumerEndpoint Endpoint => _endpoint;

    /// <summary>Region name.</summary>
    public string Name => _endpoint.Name;

    /// <summary>
    /// Creates the region if needed, claims the consumer role and returns the
    /// reader.
    /// </summary>
    /// <param name="factory">Transport that creates or opens the region.</param>
    /// <param name="name">Region name both processes share.</param>
    /// <param name="codec">Message codec.</param>
    /// <param name="capacity">Slots to request when this process creates the region.</param>
    /// <param name="slotSize">
    /// Bytes per slot to request when this process creates the region; 0 derives
    /// it from <see cref="ISparcCodec{T}.MaxSize"/>.
    /// </param>
    /// <param name="options">Open timeout, existing-region policy, takeover.</param>
    /// <param name="idleTimeout">
    /// How long the pump waits on an empty ring before re-checking the producer
    /// state; lower values end <see cref="ReadAllAsync"/> sooner after the
    /// producer stops.
    /// </param>
    /// <param name="cancellationToken">Bounds the role-claim retry loop.</param>
    public static SparcChannelReader<T> Open(
        IIpcMemoryRegionFactory factory,
        string name,
        ISparcCodec<T> codec,
        int capacity = SparcRing.DefaultCapacity,
        int slotSize = 0,
        SharedRingBufferOptions? options = null,
        TimeSpan? idleTimeout = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(codec);

        IConsumerEndpoint endpoint = SparcRing.OpenConsumer(
            factory, name, capacity, SparcChannelWriter<T>.ResolveSlotSize(codec, slotSize), options, cancellationToken);
        return new SparcChannelReader<T>(endpoint, codec, idleTimeout ?? DefaultIdleTimeout);
    }

    /// <summary>
    /// Attempts to decode one message without waiting. Returns false when
    /// nothing is available. Does not start the pump thread.
    /// </summary>
    public bool TryRead([MaybeNullWhen(false)] out T item)
    {
        ThrowIfDisposed();

        if (Volatile.Read(ref _pumpStarted) != 0)
        {
            return _inbox.Reader.TryRead(out item);
        }

        if (_endpoint.TryBeginRead(out ReadLease lease))
        {
            using (lease)
            {
                item = _codec.Decode(lease.Payload);
                return true;
            }
        }

        item = default;
        return false;
    }

    /// <summary>Waits for the next message; throws when the producer stops first.</summary>
    public async ValueTask<T> ReadAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (TryRead(out T? item))
        {
            return item;
        }

        EnsurePump();
        return await _inbox.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Completes when a message is available or returns false when the channel completed.</summary>
    public async ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        EnsurePump();
        return await _inbox.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Streams messages until the producer stops and the ring is drained, the
    /// caller cancels, or the pump fails.
    /// </summary>
    public async IAsyncEnumerable<T> ReadAllAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        EnsurePump();

        while (await _inbox.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (_inbox.Reader.TryRead(out T? item))
            {
                yield return item;
            }
        }
    }

    /// <summary>Stops the pump and disposes the endpoint.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _cancellation.Cancel();
        _endpoint.Dispose();
        _inbox.Writer.TryComplete();

        if (_pump is null || _pump.IsCompleted)
        {
            _cancellation.Dispose();
        }
    }

    private void EnsurePump()
    {
        ThrowIfDisposed();

        if (Interlocked.CompareExchange(ref _pumpStarted, 1, 0) == 0)
        {
            _pump = Task.Factory.StartNew(
                PumpLoop,
                _cancellation.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }
    }

    private void PumpLoop()
    {
        try
        {
            while (!_cancellation.IsCancellationRequested)
            {
                try
                {
                    using ReadLease lease = _endpoint.Read(_idleTimeout, _cancellation.Token);
                    _inbox.Writer.TryWrite(_codec.Decode(lease.Payload));
                }
                catch (RingBufferTimeoutException)
                {
                    if (_endpoint.ProducerState is RingBufferEndpointState.Stopped or RingBufferEndpointState.Faulted)
                    {
                        break;
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Disposal or host shutdown.
        }
        catch (Exception exception)
        {
            _inbox.Writer.TryComplete(exception);
            return;
        }

        _inbox.Writer.TryComplete();
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
    }
}
