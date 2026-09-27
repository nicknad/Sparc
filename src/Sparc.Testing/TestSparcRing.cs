using Sparc.Client;
using Sparc.Core;
using Sparc.InMemory;

namespace Sparc.Testing;

/// <summary>
/// A paired producer/consumer endpoint over one in-process region, for tests
/// that exercise the ring/session stack without the OS. The optional
/// <see cref="TimeProvider"/> is what the session factories pass to the
/// sessions, so a <c>FakeTimeProvider</c> makes timeout and pacing behavior
/// deterministic.
/// </summary>
public sealed class TestSparcRing : IDisposable
{
    private TestSparcRing(string name, IProducerEndpoint producer, IConsumerEndpoint consumer, TimeProvider timeProvider)
    {
        Name = name;
        Producer = producer;
        Consumer = consumer;
        TimeProvider = timeProvider;
    }

    /// <summary>Region name.</summary>
    public string Name { get; }

    /// <summary>Claimed producer endpoint.</summary>
    public IProducerEndpoint Producer { get; }

    /// <summary>Claimed consumer endpoint.</summary>
    public IConsumerEndpoint Consumer { get; }

    /// <summary>Clock handed to sessions created by this harness.</summary>
    public TimeProvider TimeProvider { get; }

    /// <summary>
    /// Creates a region in a fresh <see cref="InMemoryMemoryRegionFactory"/>
    /// and opens both roles.
    /// </summary>
    /// <param name="capacity">Slots; must be a power of two.</param>
    /// <param name="slotSize">Bytes per slot.</param>
    /// <param name="timeProvider">Clock for sessions; <c>FakeTimeProvider</c> recommended.</param>
    /// <param name="name">Region name; a unique name is generated when omitted.</param>
    public static TestSparcRing Create(
        int capacity = 1024,
        int slotSize = 256,
        TimeProvider? timeProvider = null,
        string? name = null)
    {
        InMemoryMemoryRegionFactory factory = new();
        string regionName = name ?? "sparc-test-" + Guid.NewGuid().ToString("N");
        IProducerEndpoint producer = SparcRing.OpenProducer(factory, regionName, capacity, slotSize);
        try
        {
            SharedRingBufferOptions consumerOptions = new() { AdoptExistingGeometry = true };
            IConsumerEndpoint consumer = SparcRing.OpenConsumer(factory, regionName, capacity, slotSize, consumerOptions);
            return new TestSparcRing(regionName, producer, consumer, timeProvider ?? TimeProvider.System);
        }
        catch
        {
            producer.Dispose();
            throw;
        }
    }

    /// <summary>Creates a producer session over <see cref="Producer"/> with this harness's clock.</summary>
    public ProducerSession CreateProducerSession(ProducerSessionOptions? options = null) =>
        new(Producer, options ?? new ProducerSessionOptions(), TimeProvider);

    /// <summary>Creates a consumer session over <see cref="Consumer"/> with this harness's clock.</summary>
    public ConsumerSession CreateConsumerSession(ConsumerSessionOptions? options = null) =>
        new(Consumer, options ?? new ConsumerSessionOptions(), TimeProvider);

    /// <summary>Disposes both endpoints (each publishes its graceful stop).</summary>
    public void Dispose()
    {
        Producer.Dispose();
        Consumer.Dispose();
    }
}
