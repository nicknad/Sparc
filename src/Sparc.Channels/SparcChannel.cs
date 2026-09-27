using Sparc.Core;

namespace Sparc.Channels;

/// <summary>
/// A typed writer and reader over one region, for tests, samples and
/// single-process hosts that need both ends in the same process. In a real
/// deployment each process opens only its own role
/// (<see cref="SparcChannelWriter{T}.Open"/> or <see cref="SparcChannelReader{T}.Open"/>).
/// </summary>
public sealed class SparcChannel<T> : IDisposable
{
    private SparcChannel(SparcChannelWriter<T> writer, SparcChannelReader<T> reader)
    {
        Writer = writer;
        Reader = reader;
    }

    /// <summary>Producer end.</summary>
    public SparcChannelWriter<T> Writer { get; }

    /// <summary>Consumer end.</summary>
    public SparcChannelReader<T> Reader { get; }

    /// <summary>Region name shared with the peer.</summary>
    public string Name => Writer.Name;

    /// <summary>
    /// Opens both endpoints over one region created through
    /// <paramref name="factory"/> (typically
    /// <c>Sparc.InMemory.InMemoryMemoryRegionFactory</c>).
    /// </summary>
    public static SparcChannel<T> CreateInProcess(
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
        options ??= SharedRingBufferOptions.Default;

        SparcChannelWriter<T> writer = SparcChannelWriter<T>.Open(
            factory, name, codec, capacity, slotSize, options, cancellationToken);
        try
        {
            SharedRingBufferOptions readerOptions = new()
            {
                OpenTimeout = options.OpenTimeout,
                RequireExisting = options.RequireExisting,
                RecreateIfStale = options.RecreateIfStale,
                AdoptExistingGeometry = true,
                Takeover = options.Takeover,
                TimeProvider = options.TimeProvider,
            };

            SparcChannelReader<T> reader = SparcChannelReader<T>.Open(
                factory, name, codec, capacity, slotSize, readerOptions, idleTimeout, cancellationToken);
            return new SparcChannel<T>(writer, reader);
        }
        catch
        {
            writer.Dispose();
            throw;
        }
    }

    /// <summary>Disposes both endpoints (each publishes its graceful stop).</summary>
    public void Dispose()
    {
        Writer.Dispose();
        Reader.Dispose();
    }
}
