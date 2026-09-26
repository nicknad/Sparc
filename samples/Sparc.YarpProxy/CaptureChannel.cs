using System.Threading.Channels;
using Sparc.YarpSample;

namespace Sparc.YarpProxy;

/// <summary>
/// Bounded in-process hand-off between the YARP request pipeline and the SPARC
/// producer worker. Publishing never blocks a proxied request: when the queue is
/// full the capture is dropped and counted.
/// </summary>
internal sealed class CaptureChannel
{
    private readonly Channel<CapturedRequest> _channel = Channel.CreateBounded<CapturedRequest>(
        new BoundedChannelOptions(YarpCaptureProtocol.MaxQueuedCaptures)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });

    private long _dropped;

    public ChannelReader<CapturedRequest> Reader => _channel.Reader;

    /// <summary>Captures dropped because the queue was full.</summary>
    public long Dropped => Interlocked.Read(ref _dropped);

    public bool TryPublish(CapturedRequest request)
    {
        if (_channel.Writer.TryWrite(request))
        {
            return true;
        }

        Interlocked.Increment(ref _dropped);
        return false;
    }
}
