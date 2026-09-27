using Sparc.UnixMemoryMapped;
using Sparc.WindowsMemoryMapped;
using Sparc.Core;

namespace Sparc.Benchmarks.Pumps;

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
    /// distorts small-message benchmarks. Delegates to the Core policy so the
    /// in-process pumps and the CLIs use identical geometry.
    /// </summary>
    internal static int SlotSizeFor(int payloadSize) =>
        Math.Max(
            RingBufferLayout.CacheLineSize,
            RingBufferLayout.RoundSlotSizeToCacheLine(payloadSize + RingBufferLayout.MessageHeaderSize));
}

public sealed class SpscSharedMemoryPump : TwoThreadPump
{
    private readonly SharedRingBuffer _producer;
    private readonly SharedRingBuffer _consumer;

    public SpscSharedMemoryPump(int payloadSize)
    {
        string name = "spsc-bench-" + Guid.NewGuid().ToString("N");
        int slotSize = SpscArrayPump.SlotSizeFor(payloadSize);
        IIpcMemoryRegionFactory factory = OperatingSystem.IsWindows()
            ? new WindowsNamedMemoryMappedRegionFactory()
            : new UnixFileMemoryMappedRegionFactory();
        _producer = SharedRingBuffer.OpenOrCreate(factory, name, Capacity, slotSize);
        _consumer = SharedRingBuffer.OpenOrCreate(factory, name, Capacity, slotSize);
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
    }
}
