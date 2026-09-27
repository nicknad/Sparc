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

/// <summary>
/// Lease (zero-copy) API variant of <see cref="SpscArrayPump"/>: the producer
/// writes into the reserved slot and the consumer peeks/advances. Both still
/// copy through the pump's payload/destination spans so the comparison against
/// the copy API isolates the lease protocol itself.
/// </summary>
public sealed class SpscLeasePump : TwoThreadPump
{
    private readonly SpscRingBuffer _buffer;

    public SpscLeasePump(int payloadSize)
    {
        _buffer = new SpscRingBuffer(Capacity, SpscArrayPump.SlotSizeFor(payloadSize));
    }

    public override bool TryPublish(ReadOnlySpan<byte> payload)
    {
        if (!_buffer.TryReserveWrite(1, payload.Length, out Span<byte> slot))
        {
            return false;
        }

        payload.CopyTo(slot);
        _buffer.CommitWrite();
        return true;
    }

    public override bool TryConsume(Span<byte> destination)
    {
        if (!_buffer.TryPeek(out ReadOnlySpan<byte> payload, out _, out _))
        {
            return false;
        }

        payload.CopyTo(destination);
        _buffer.AdvanceRead();
        return true;
    }

    public override int RequiredDestinationSize(int payloadSize) => _buffer.MaxPayloadSize;
}

public sealed class SpscSharedMemoryPump : TwoThreadPump
{
    private readonly SharedRingBuffer _producer;
    private readonly SharedRingBuffer _consumer;
    private readonly bool _lease;

    public SpscSharedMemoryPump(int payloadSize)
        : this(payloadSize, lease: false)
    {
    }

    private SpscSharedMemoryPump(int payloadSize, bool lease)
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
        _lease = lease;
    }

    /// <summary>Shared-memory variant that uses the lease API instead of TryWrite/TryRead.</summary>
    public static SpscSharedMemoryPump CreateLease(int payloadSize) => new(payloadSize, lease: true);

    public override bool TryPublish(ReadOnlySpan<byte> payload)
    {
        if (!_lease)
        {
            return _producer.TryWrite(1, payload);
        }

        if (!_producer.TryReserveWrite(1, payload.Length, out Span<byte> slot))
        {
            return false;
        }

        payload.CopyTo(slot);
        _producer.CommitWrite();
        return true;
    }

    public override bool TryConsume(Span<byte> destination)
    {
        if (!_lease)
        {
            return _consumer.TryRead(destination, out _, out _);
        }

        if (!_consumer.TryPeek(out ReadOnlySpan<byte> payload, out _, out _))
        {
            return false;
        }

        payload.CopyTo(destination);
        _consumer.AdvanceRead();
        return true;
    }

    public override int RequiredDestinationSize(int payloadSize) => _producer.MaxPayloadSize;

    public override void Dispose()
    {
        base.Dispose();
        _producer.Dispose();
        _consumer.Dispose();
    }
}
