namespace Sparc.Core;

/// <summary>
/// A scope-based write reservation: the payload is written directly into the
/// shared slot, and disposing the lease publishes the message. Call
/// <see cref="Abandon"/> to leave the slot unpublished instead.
/// </summary>
public ref struct WriteLease
{
    private readonly IProducerEndpoint _endpoint;
    private readonly Span<byte> _payload;
    private bool _abandoned;
    private bool _done;

    internal WriteLease(IProducerEndpoint endpoint, Span<byte> payload)
    {
        _endpoint = endpoint;
        _payload = payload;
        _abandoned = false;
        _done = false;
    }

    /// <summary>Writable view of exactly the reserved length inside the slot.</summary>
    public Span<byte> Payload => _payload;

    /// <summary>Discards the reservation; the slot is not published.</summary>
    public void Abandon() => _abandoned = true;

    /// <summary>Publishes the message unless <see cref="Abandon"/> was called.</summary>
    public void Dispose()
    {
        if (_endpoint is null || _done)
        {
            return;
        }

        _done = true;
        if (_abandoned)
        {
            _endpoint.AbandonWrite();
        }
        else
        {
            _endpoint.CommitWrite();
        }
    }
}
