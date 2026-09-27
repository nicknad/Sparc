using Sparc;
using Sparc.Core;
using Sparc.YarpSample;

namespace Sparc.YarpProxy;

/// <summary>
/// The channel consumer that acts as the SPARC producer: it drains
/// <see cref="CaptureChannel"/>, JSON-encodes each capture and try-writes it into
/// the shared ring. Ring writes never block the proxy; when the ring is full the
/// capture is dropped and counted.
/// </summary>
internal sealed class CaptureToRingWorker(
    IIpcMemoryRegionFactory factory,
    CaptureChannel channel,
    IConfiguration configuration,
    ILogger<CaptureToRingWorker> logger) : BackgroundService
{
    private long _written;
    private long _ringDropped;
    private long _tooLarge;

    public string RingName { get; } =
        configuration.GetValue("Sparc:RingName", YarpCaptureProtocol.RingName) ?? YarpCaptureProtocol.RingName;

    /// <summary>Captures successfully written to the ring.</summary>
    public long Written => Interlocked.Read(ref _written);

    /// <summary>Captures dropped because the ring was full.</summary>
    public long RingDropped => Interlocked.Read(ref _ringDropped);

    /// <summary>Captures whose encoded form exceeded the ring's maximum payload.</summary>
    public long TooLarge => Interlocked.Read(ref _tooLarge);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!factory.IsSupported)
        {
            logger.LogError(
                "{Factory} is not supported on this platform; no captures will reach the SPARC ring.",
                factory.GetType().Name);
            return;
        }

        int capacity = configuration.GetValue("Sparc:RingCapacity", YarpCaptureProtocol.Capacity);
        int slotSize = configuration.GetValue("Sparc:RingSlotSize", YarpCaptureProtocol.SlotSize);

        using IProducerEndpoint buffer = await Task.Run(
            () => SparcRing.OpenProducer(factory, RingName, capacity, slotSize),
            stoppingToken).ConfigureAwait(false);

        logger.LogInformation(
            "SPARC producer ready: name={Name} capacity={Capacity} slotSize={SlotSize} maxPayload={MaxPayload}",
            buffer.Name, buffer.Capacity, buffer.SlotSize, buffer.MaxPayloadSize);

        await foreach (CapturedRequest request in channel.Reader.ReadAllAsync(stoppingToken).ConfigureAwait(false))
        {
            byte[] payload = YarpCaptureProtocol.Encode(request);
            if (payload.Length > buffer.MaxPayloadSize)
            {
                Interlocked.Increment(ref _tooLarge);
                continue;
            }

            if (buffer.TryPublish(type: 0, payload))
            {
                Interlocked.Increment(ref _written);
            }
            else
            {
                Interlocked.Increment(ref _ringDropped);
            }
        }
    }
}
