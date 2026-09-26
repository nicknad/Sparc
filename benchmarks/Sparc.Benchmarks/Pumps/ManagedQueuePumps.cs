using System.Threading.Channels;

namespace Sparc.Benchmarks.Pumps;

public sealed class LockQueuePump : TwoThreadPump
{
    private readonly object _gate = new();
    private readonly Queue<byte[]> _queue = new();

    public override bool TryPublish(ReadOnlySpan<byte> payload)
    {
        lock (_gate)
        {
            if (_queue.Count >= Capacity)
            {
                return false;
            }

            _queue.Enqueue(payload.ToArray());
            return true;
        }
    }

    public override bool TryConsume(Span<byte> destination)
    {
        byte[] item;
        lock (_gate)
        {
            if (_queue.Count == 0)
            {
                return false;
            }

            item = _queue.Dequeue();
        }

        item.CopyTo(destination);
        return true;
    }
}

public sealed class ChannelPump : TwoThreadPump
{
    private readonly Channel<byte[]> _channel = Channel.CreateBounded<byte[]>(
        new BoundedChannelOptions(Capacity)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
            AllowSynchronousContinuations = false,
        });

    public override bool TryPublish(ReadOnlySpan<byte> payload) => _channel.Writer.TryWrite(payload.ToArray());

    public override bool TryConsume(Span<byte> destination)
    {
        if (!_channel.Reader.TryRead(out byte[]? item))
        {
            return false;
        }

        item.CopyTo(destination);
        return true;
    }
}
