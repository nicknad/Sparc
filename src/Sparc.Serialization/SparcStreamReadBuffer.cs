using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparc.Core;

namespace Sparc.Serialization;

/// <summary>
/// Zero-copy reader over one chunked message: hands out the message bytes as
/// they arrive, pulls the next chunk slot when the current one is exhausted,
/// and stitches chunk seams into caller-provided scratch when a contiguous
/// window larger than one chunk is requested.
/// </summary>
/// <remarks>
/// <para>
/// Create one with <see cref="SparcStreamReader.BeginMessage"/> and dispose it
/// when done; disposing before the message is fully consumed abandons the
/// remaining tail (the next message starts at the next <c>First</c> chunk).
/// <see cref="GetUnreadSpan"/> and <see cref="TryGetSpan"/> return windows
/// valid until the next such call or <see cref="Dispose"/>; <see cref="Advance"/>
/// and <see cref="CopyTo"/> do not invalidate a held window.
/// </para>
/// <para>
/// This is a ref struct rather than a SerializerFoundation
/// <see cref="SerializerFoundation.IReadBuffer"/> because
/// <c>IReadBuffer.BytesRemaining</c> cannot be known for a message whose end has
/// not been reached, and the endpoint allows only one active lease at a time.
/// A producer that restarts mid-message truncates the stream:
/// <see cref="RingBufferCorruptedException"/> is thrown instead of silently
/// mixing the messages (use <see cref="SparcStreamReader.ReadMessage"/> for
/// restart-resilient reads).
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Auto)]
public ref struct SparcStreamReadBuffer
{
    private readonly IConsumerEndpoint _endpoint;
    private readonly Span<byte> _scratch;
    private readonly TimeSpan _timeout;
    private readonly CancellationToken _cancellationToken;
    private ReadLease _lease;
    private int _leaseOffset;
    private int _scratchStart;
    private int _scratchEnd;
    private long _bytesConsumed;
    private bool _hasLease;
    private bool _leaseIsLast;
    private bool _endReached;
    private bool _disposed;

    private SparcStreamReadBuffer(
        IConsumerEndpoint endpoint,
        ReadLease lease,
        int flags,
        Span<byte> scratch,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        _endpoint = endpoint;
        _lease = lease;
        _leaseOffset = ChunkFraming.HeaderSize;
        _scratch = scratch;
        _scratchStart = 0;
        _scratchEnd = 0;
        _bytesConsumed = 0;
        _timeout = timeout;
        _cancellationToken = cancellationToken;
        _hasLease = true;
        _leaseIsLast = (flags & ChunkFraming.LastFlag) != 0;
        _endReached = false;
        _disposed = false;
        Type = lease.Type;
        FinishIfEmpty();
    }

    /// <summary>The message's type tag, from the first chunk's slot framing.</summary>
    public int Type { get; }

    /// <summary>Total bytes consumed through <see cref="Advance"/>.</summary>
    public long BytesConsumed => _bytesConsumed;

    /// <summary>True when every byte of the message has been consumed.</summary>
    public bool IsMessageComplete => _endReached && _scratchStart == _scratchEnd;

    internal static SparcStreamReadBuffer Begin(
        IConsumerEndpoint endpoint, Span<byte> scratch, TimeSpan timeout, CancellationToken cancellationToken)
    {
        ReadLease lease = LeaseNext(endpoint, timeout, cancellationToken);
        while (true)
        {
            int flags = ChunkFraming.ReadFlags(lease.Payload);
            if ((flags & ChunkFraming.FirstFlag) != 0)
            {
                return new SparcStreamReadBuffer(endpoint, lease, flags, scratch, timeout, cancellationToken);
            }

            lease.Dispose(); // interrupted tail: skip to the next message start
            lease = LeaseNext(endpoint, timeout, cancellationToken);
        }
    }

    internal static bool TryBegin(
        IConsumerEndpoint endpoint, Span<byte> scratch, TimeSpan timeout, CancellationToken cancellationToken, out SparcStreamReadBuffer buffer)
    {
        while (endpoint.TryBeginRead(out ReadLease lease))
        {
            int flags = ChunkFraming.ReadFlags(lease.Payload);
            if ((flags & ChunkFraming.FirstFlag) != 0)
            {
                buffer = new SparcStreamReadBuffer(endpoint, lease, flags, scratch, timeout, cancellationToken);
                return true;
            }

            lease.Dispose();
        }

        buffer = default;
        return false;
    }

    /// <summary>
    /// The unread part of the current contiguous window: unconsumed stitched
    /// bytes first, otherwise the current chunk's remainder. Blocks for the
    /// next chunk when the current one is exhausted; empty only at the end of
    /// the message.
    /// </summary>
    public ReadOnlySpan<byte> GetUnreadSpan()
    {
        ThrowIfDisposed();
        if (_scratchStart < _scratchEnd)
        {
            return _scratch[_scratchStart.._scratchEnd];
        }

        EnsureLease();
        return _hasLease ? _lease.Payload[_leaseOffset..] : default;
    }

    /// <summary>
    /// Returns a contiguous window of at least <paramref name="sizeHint"/> bytes,
    /// stitching chunk seams into the scratch buffer as needed, or false when
    /// the message ends first. Never consumes. The window must fit the scratch
    /// buffer.
    /// </summary>
    public bool TryGetSpan(int sizeHint, out ReadOnlySpan<byte> span)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(sizeHint);

        if (sizeHint == 0)
        {
            span = GetUnreadSpan();
            return true;
        }

        if (_scratchEnd - _scratchStart >= sizeHint)
        {
            span = _scratch[_scratchStart.._scratchEnd];
            return true;
        }

        if (sizeHint > _scratch.Length)
        {
            throw new InvalidOperationException(
                $"sizeHint {sizeHint} exceeds the {_scratch.Length}-byte scratch buffer.");
        }

        if (!TryFillScratch(sizeHint))
        {
            span = default;
            return false;
        }

        span = _scratch[_scratchStart.._scratchEnd];
        return true;
    }

    /// <summary>Consumes bytes previously exposed by a span; the bytes must be available.</summary>
    public void Advance(int bytesConsumed)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(bytesConsumed);

        int scratchAvailable = _scratchEnd - _scratchStart;
        int leaseAvailable = _hasLease ? _lease.Payload.Length - _leaseOffset : 0;
        if (bytesConsumed > scratchAvailable + leaseAvailable)
        {
            throw new InvalidOperationException(
                $"Cannot advance {bytesConsumed} bytes; only {scratchAvailable + leaseAvailable} are available.");
        }

        int fromScratch = Math.Min(bytesConsumed, scratchAvailable);
        _scratchStart += fromScratch;
        if (_scratchStart == _scratchEnd)
        {
            _scratchStart = 0;
            _scratchEnd = 0;
        }

        int fromLease = bytesConsumed - fromScratch;
        if (fromLease > 0)
        {
            _leaseOffset += fromLease;
            if (_leaseOffset == _lease.Payload.Length)
            {
                FinishLease();
            }
        }

        _bytesConsumed += bytesConsumed;
    }

    /// <summary>
    /// Copies the next <paramref name="destination"/>.Length bytes without
    /// consuming them, stitching chunk seams into scratch as needed. Throws when
    /// the message has fewer bytes left.
    /// </summary>
    public void CopyTo(Span<byte> destination)
    {
        ThrowIfDisposed();
        if (destination.Length == 0)
        {
            return;
        }

        if (destination.Length > _scratch.Length)
        {
            throw new InvalidOperationException(
                $"destination of {destination.Length} bytes exceeds the {_scratch.Length}-byte scratch buffer.");
        }

        if (!TryFillScratch(destination.Length))
        {
            throw new InvalidOperationException(
                $"The message has fewer than {destination.Length} bytes left; the copy was not started.");
        }

        _scratch.Slice(_scratchStart, destination.Length).CopyTo(destination);
    }

    /// <summary>Abandons an unread tail and releases the current chunk.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        if (_hasLease)
        {
            _lease.Dispose();
            _hasLease = false;
        }

        _scratchStart = 0;
        _scratchEnd = 0;
        _endReached = true;
    }

    private static ReadLease LeaseNext(IConsumerEndpoint endpoint, TimeSpan timeout, CancellationToken cancellationToken) =>
        endpoint.TryBeginRead(out ReadLease lease) ? lease : endpoint.Read(timeout, cancellationToken);

    private void FinishIfEmpty()
    {
        if (_lease.Payload.Length == ChunkFraming.HeaderSize && _leaseIsLast)
        {
            FinishLease();
        }
    }

    private void EnsureLease()
    {
        ThrowIfDisposed();
        if (_hasLease || _endReached)
        {
            return;
        }

        if (_endpoint is null)
        {
            throw new InvalidOperationException("No stream message is active.");
        }

        ReadLease lease = LeaseNext(_endpoint, _timeout, _cancellationToken);
        int flags = ChunkFraming.ReadFlags(lease.Payload);
        if ((flags & ChunkFraming.FirstFlag) != 0)
        {
            lease.Dispose();
            throw new RingBufferCorruptedException(
                "A new stream message started before the previous one ended; the stream was truncated.");
        }

        _lease = lease;
        _leaseOffset = ChunkFraming.HeaderSize;
        _leaseIsLast = (flags & ChunkFraming.LastFlag) != 0;
        _hasLease = true;
        FinishIfEmpty();
    }

    private void FinishLease()
    {
        Debug.Assert(_hasLease);
        _lease.Dispose();
        _hasLease = false;
        if (_leaseIsLast)
        {
            _endReached = true;
        }
    }

    private bool TryFillScratch(int needed)
    {
        Debug.Assert(needed <= _scratch.Length);

        int count = _scratchEnd - _scratchStart;
        if (count > 0 && _scratchStart > 0)
        {
            _scratch.Slice(_scratchStart, count).CopyTo(_scratch);
        }

        _scratchStart = 0;
        _scratchEnd = count;

        while (_scratchEnd < needed)
        {
            EnsureLease();
            if (!_hasLease)
            {
                return false; // the message ended before the requested window
            }

            ReadOnlySpan<byte> data = _lease.Payload[_leaseOffset..];
            int take = Math.Min(needed - _scratchEnd, data.Length);
            data[..take].CopyTo(_scratch[_scratchEnd..]);
            _scratchEnd += take;
            _leaseOffset += take;
            if (_leaseOffset == _lease.Payload.Length)
            {
                FinishLease();
            }
        }

        return true;
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(SparcStreamReadBuffer));
    }
}
