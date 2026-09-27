namespace Sparc.Core;

/// <summary>
/// A scope-based read view: the payload is read directly from the shared slot,
/// and disposing the lease releases the slot to the producer.
/// </summary>
public ref struct ReadLease
{
    private readonly IConsumerEndpoint _endpoint;
    private readonly ReadOnlySpan<byte> _payload;
    private bool _done;

    internal ReadLease(IConsumerEndpoint endpoint, ReadOnlySpan<byte> payload, int length, int type)
    {
        _endpoint = endpoint;
        _payload = payload;
        Length = length;
        Type = type;
        _done = false;
    }

    /// <summary>Read-only view of the payload, valid until the lease is disposed.</summary>
    public ReadOnlySpan<byte> Payload => _payload;

    /// <summary>Payload length in bytes.</summary>
    public int Length { get; }

    /// <summary>Type tag stored with the message.</summary>
    public int Type { get; }

    /// <summary>Releases the slot to the producer.</summary>
    public void Dispose()
    {
        if (_endpoint is null || _done)
        {
            return;
        }

        _done = true;
        _endpoint.AdvanceRead();
    }
}
