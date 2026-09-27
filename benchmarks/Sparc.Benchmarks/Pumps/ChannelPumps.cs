using System.Buffers.Binary;
using Sparc.Channels;
using Sparc.InMemory;

namespace Sparc.Benchmarks.Pumps;

/// <summary>
/// Pump over the typed channel layer (encode/decode of a <see cref="long"/>
/// through <see cref="ISparcCodec{T}"/>) so the wrapper overhead is measured
/// against the raw endpoint pumps. The first 8 bytes carry the timestamp in
/// latency mode, so latency runs are comparable too.
/// </summary>
public sealed class SpscChannelPump : TwoThreadPump
{
    private readonly SparcChannel<long> _channel;

    public SpscChannelPump(int payloadSize)
    {
        _ = payloadSize; // geometry is fixed by the codec; the scenario size only scales reporting
        _channel = SparcChannel<long>.CreateInProcess(
            new InMemoryMemoryRegionFactory(),
            "spsc-channel-bench-" + Guid.NewGuid().ToString("N"),
            new Int64Codec(),
            Capacity,
            SpscArrayPump.SlotSizeFor(sizeof(long)));
    }

    public override bool TryPublish(ReadOnlySpan<byte> payload) =>
        _channel.Writer.TryWrite(BinaryPrimitives.ReadInt64LittleEndian(payload));

    public override bool TryConsume(Span<byte> destination)
    {
        if (!_channel.Reader.TryRead(out long value))
        {
            return false;
        }

        BinaryPrimitives.WriteInt64LittleEndian(destination, value);
        return true;
    }

    public override int RequiredDestinationSize(int payloadSize) => sizeof(long);

    public override void Dispose()
    {
        base.Dispose();
        _channel.Dispose();
    }

    private sealed class Int64Codec : ISparcCodec<long>
    {
        public int MaxSize => sizeof(long);

        public int Encode(long item, Span<byte> destination)
        {
            BinaryPrimitives.WriteInt64LittleEndian(destination, item);
            return sizeof(long);
        }

        public long Decode(ReadOnlySpan<byte> source) => BinaryPrimitives.ReadInt64LittleEndian(source);
    }
}
