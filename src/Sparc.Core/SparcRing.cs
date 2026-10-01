namespace Sparc.Core;

/// <summary>
/// Entry point for opening a role-typed endpoint over a shared region. The
/// transport is chosen by the <see cref="IIpcMemoryRegionFactory"/> passed in;
/// the role is claimed before the endpoint is returned, so callers get a
/// ready-to-use producer or consumer.
/// </summary>
/// <remarks>
/// <para>
/// Either process may open first: the first one creates and initializes the
/// region, the other joins it. Exactly one producer and one consumer may exist
/// per region; a second live endpoint for the same role fails with
/// <see cref="RingBufferRoleConflictException"/> unless
/// <see cref="SharedRingBufferOptions.Takeover"/> is set.
/// </para>
/// <para>
/// The returned endpoint owns the region mapping and publishes its endpoint
/// state on <see cref="IDisposable.Dispose"/> (graceful stop), which is how the
/// peer learns the stream ended.
/// </para>
/// </remarks>
public static class SparcRing
{
    /// <summary>Default number of slots. Must be a power of two.</summary>
    public const int DefaultCapacity = 1024;

    /// <summary>Default size of one slot in bytes.</summary>
    public const int DefaultSlotSize = 256;

    /// <summary>
    /// Creates the region if needed, joins it otherwise, claims the producer
    /// role, and returns the endpoint.
    /// </summary>
    /// <param name="factory">Transport that creates or opens the shared region.</param>
    /// <param name="name">Region name both processes share.</param>
    /// <param name="capacity">Number of slots; must be a power of two.</param>
    /// <param name="slotSize">Bytes per slot; must exceed the message header.</param>
    /// <param name="options">Open timeout, existing-region policy, takeover.</param>
    /// <param name="cancellationToken">Bounds the role-claim retry loop.</param>
    public static IProducerEndpoint OpenProducer(
        IIpcMemoryRegionFactory factory,
        string name,
        int capacity = DefaultCapacity,
        int slotSize = DefaultSlotSize,
        SharedRingBufferOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        options ??= SharedRingBufferOptions.Default;
        SharedRingBuffer buffer = SharedRingBuffer.OpenOrCreate(factory, name, capacity, slotSize, options);
        return ConnectProducer(buffer, options, cancellationToken);
    }

    /// <summary>
    /// Creates the region if needed, joins it otherwise, claims the consumer
    /// role, and returns the endpoint.
    /// </summary>
    public static IConsumerEndpoint OpenConsumer(
        IIpcMemoryRegionFactory factory,
        string name,
        int capacity = DefaultCapacity,
        int slotSize = DefaultSlotSize,
        SharedRingBufferOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        options ??= SharedRingBufferOptions.Default;
        SharedRingBuffer buffer = SharedRingBuffer.OpenOrCreate(factory, name, capacity, slotSize, options);
        return ConnectConsumer(buffer, options, cancellationToken);
    }

    /// <summary>
    /// Attaches to an already-mapped region (for example a Windows section
    /// mapped from a transferred HANDLE), claims the producer role, and returns
    /// the endpoint. Takes ownership of <paramref name="region"/>: disposing
    /// the endpoint releases the mapping.
    /// </summary>
    public static IProducerEndpoint OpenProducer(
        IIpcMemoryRegion region,
        int capacity,
        int slotSize,
        SharedRingBufferOptions? options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(region);
        options ??= SharedRingBufferOptions.Default;
        SharedRingBuffer buffer = SharedRingBuffer.FromRegion(region, capacity, slotSize, options);
        return ConnectProducer(buffer, options, cancellationToken);
    }

    /// <summary>
    /// Attaches to an already-mapped region (for example a Windows section
    /// mapped from a transferred HANDLE), claims the consumer role, and returns
    /// the endpoint. Takes ownership of <paramref name="region"/>: disposing
    /// the endpoint releases the mapping.
    /// </summary>
    public static IConsumerEndpoint OpenConsumer(
        IIpcMemoryRegion region,
        int capacity,
        int slotSize,
        SharedRingBufferOptions? options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(region);
        options ??= SharedRingBufferOptions.Default;
        SharedRingBuffer buffer = SharedRingBuffer.FromRegion(region, capacity, slotSize, options);
        return ConnectConsumer(buffer, options, cancellationToken);
    }

    private static IProducerEndpoint ConnectProducer(
        SharedRingBuffer buffer, SharedRingBufferOptions options, CancellationToken cancellationToken)
    {
        try
        {
            ((IProducerEndpoint)buffer).Connect(options.Takeover, cancellationToken);
            return buffer;
        }
        catch
        {
            buffer.Dispose();
            throw;
        }
    }

    private static IConsumerEndpoint ConnectConsumer(
        SharedRingBuffer buffer, SharedRingBufferOptions options, CancellationToken cancellationToken)
    {
        try
        {
            ((IConsumerEndpoint)buffer).Connect(options.Takeover, cancellationToken);
            return buffer;
        }
        catch
        {
            buffer.Dispose();
            throw;
        }
    }
}
