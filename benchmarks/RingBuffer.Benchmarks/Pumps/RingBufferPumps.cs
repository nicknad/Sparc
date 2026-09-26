using RingBuffer.Core;
using RingBuffer.SharedMemory;

namespace RingBuffer.Benchmarks.Pumps;

public sealed class SpscArrayPump : TwoThreadPump
{
    private readonly SpscRingBuffer _buffer;

    public SpscArrayPump(int payloadSize)
    {
        _buffer = new SpscRingBuffer(Capacity, SlotSizeFor(payloadSize));
    }

    public override bool TryPublish(ReadOnlySpan<byte> payload) => _buffer.TryWrite(1, payload);

    public override bool TryConsume(Span<byte> destination) => _buffer.TryRead(destination, out _, out _);

    public override int RequiredDestinationSize(int payloadSize) => _buffer.MaxPayloadSize;

    /// <summary>
    /// Rounds the slot up to a cache line so adjacent slots do not share one.
    /// Tightly packed slots would make producer and consumer false-share, which
    /// distorts small-message benchmarks.
    /// </summary>
    internal static int SlotSizeFor(int payloadSize)
    {
        int raw = payloadSize + RingBufferLayout.MessageHeaderSize;
        int rounded = (raw + RingBufferLayout.CacheLineSize - 1) / RingBufferLayout.CacheLineSize * RingBufferLayout.CacheLineSize;
        return Math.Max(RingBufferLayout.CacheLineSize, rounded);
    }
}

public sealed class SpscSharedMemoryPump : TwoThreadPump
{
    private readonly SharedMemoryRegion _producerRegion;
    private readonly SharedMemoryRegion _consumerRegion;
    private readonly SharedRingBuffer _producer;
    private readonly SharedRingBuffer _consumer;

    public SpscSharedMemoryPump(int payloadSize)
    {
        string name = "spsc-bench-" + Guid.NewGuid().ToString("N");
        int slotSize = SpscArrayPump.SlotSizeFor(payloadSize);
        _producerRegion = SharedMemoryRegion.CreateOrOpen(name, Capacity, slotSize);
        _consumerRegion = SharedMemoryRegion.CreateOrOpen(name, Capacity, slotSize);
        _producer = new SharedRingBuffer(_producerRegion);
        _consumer = new SharedRingBuffer(_consumerRegion);
        _producer.Connect(RingBufferEndpointRole.Producer);
        _consumer.Connect(RingBufferEndpointRole.Consumer);
    }

    public override bool TryPublish(ReadOnlySpan<byte> payload) => _producer.TryWrite(1, payload);

    public override bool TryConsume(Span<byte> destination) => _consumer.TryRead(destination, out _, out _);

    public override int RequiredDestinationSize(int payloadSize) => _producer.MaxPayloadSize;

    public override void Dispose()
    {
        base.Dispose();
        _producer.Dispose();
        _consumer.Dispose();
        _producerRegion.Dispose();
        _consumerRegion.Dispose();
    }
}
