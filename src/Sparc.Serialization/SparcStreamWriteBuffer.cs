using System.Diagnostics;
using System.Runtime.InteropServices;
using SerializerFoundation;
using Sparc.Core;

namespace Sparc.Serialization;

/// <summary>
/// An <see cref="IWriteBuffer"/> that publishes one message as a chain of
/// chunk slots, so a serializer can stream a message larger than one slot
/// (larger than the ring, even) without staging the whole message anywhere.
/// Create one with <see cref="SparcStreamWriter.BeginMessage"/> and dispose it
/// to publish the final chunk.
/// </summary>
/// <remarks>
/// <para>
/// Each chunk payload is <c>[int32 flags][data]</c>; the writer sets the
/// <c>First</c> flag on the first chunk of the message and the <c>Last</c> flag
/// on the final one. <see cref="Abort"/> marks a failed message as aborted so
/// consumers report corruption instead of a truncated value. The ring's own
/// slot framing carries the message type on every chunk.
/// </para>
/// <para>
/// <see cref="GetSpan"/> returns a contiguous window inside the current chunk;
/// a <c>sizeHint</c> larger than one chunk's data capacity
/// (<c>MaxPayloadSize - 4</c>) cannot be satisfied and throws. When the ring is
/// full the writer spins until the consumer frees a slot, up to 30 seconds,
/// then throws <see cref="RingBufferTimeoutException"/>.
/// </para>
/// <para>
/// This is a mutable single-owner buffer: callers must pass it by
/// <c>ref</c> and must not copy it (SerializerFoundation's SF002 analyzer flags
/// copies by mistake).
/// </para>
/// <para>
/// The current chunk is held as a raw pointer because a mutable struct cannot
/// store a <see cref="Span{T}"/>; the endpoint's reservation therefore has to
/// keep the slot memory fixed until the chunk is committed or abandoned (see
/// <see cref="IProducerEndpoint.TryReserveWrite(int, out Span{byte})"/>).
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Auto)]
public unsafe struct SparcStreamWriteBuffer : IWriteBuffer
{
    private const int WriteTimeoutSeconds = 30;

    private readonly IProducerEndpoint _endpoint;
    private readonly int _type;
    private byte* _chunk;
    private int _used;
    private long _bytesWritten;
    private bool _hasChunk;
    private bool _publishedAny;
    private bool _firstOfMessage;
    private bool _completed;

    internal SparcStreamWriteBuffer(IProducerEndpoint endpoint, int type)
    {
        _endpoint = endpoint;
        _type = type;
        _chunk = null;
        _used = 0;
        _bytesWritten = 0;
        _hasChunk = false;
        _publishedAny = false;
        _firstOfMessage = true;
        _completed = false;
    }

    /// <summary>Data bytes committed across all chunks of the message so far.</summary>
    public long BytesWritten => _bytesWritten;

    private int ChunkDataCapacity => _endpoint.MaxPayloadSize - ChunkFraming.HeaderSize;

    /// <summary>
    /// Returns a writable window inside the current chunk. When the current
    /// chunk cannot hold <paramref name="sizeHint"/> more bytes it is published
    /// and the next chunk is reserved.
    /// </summary>
    public Span<byte> GetSpan(int sizeHint = 0)
    {
        if (_completed)
        {
            throw new InvalidOperationException("The message is already complete.");
        }

        // Unconditional, including the mid-chunk case: returning a shorter span
        // would violate the IWriteBuffer contract (callers may write sizeHint
        // bytes without re-checking Length) and spill into the next slot's
        // framing. Unsigned compare also rejects negative hints.
        if ((uint)sizeHint > (uint)ChunkDataCapacity)
        {
            throw new ArgumentOutOfRangeException(
                nameof(sizeHint), sizeHint,
                $"A contiguous span cannot exceed the chunk data capacity of {ChunkDataCapacity} bytes.");
        }

        if (!_hasChunk)
        {
            ReserveChunk();
        }

        int remaining = ChunkDataCapacity - _used;
        if (sizeHint > remaining || (sizeHint == 0 && remaining == 0))
        {
            CommitChunk(last: false);
            ReserveChunk();
            remaining = ChunkDataCapacity;
        }

        return new Span<byte>(_chunk + ChunkFraming.HeaderSize + _used, remaining);
    }

    /// <summary>Reports the bytes written into the span returned by <see cref="GetSpan"/>.</summary>
    public void Advance(int bytesWritten)
    {
        int remaining = ChunkDataCapacity - _used;
        if (bytesWritten < 0 || bytesWritten > remaining)
        {
            throw new InvalidOperationException(
                $"Cannot advance {bytesWritten} bytes; the current span has {remaining} bytes left.");
        }

        _used += bytesWritten;
        _bytesWritten += bytesWritten;
    }

    /// <summary>Publishes the current chunk without ending the message.</summary>
    public void Flush()
    {
        if (_hasChunk && _used > 0)
        {
            CommitChunk(last: false);
        }
    }

    /// <summary>Publishes the final chunk and ends the message.</summary>
    public void Dispose()
    {
        if (_completed)
        {
            return;
        }

        if (!_hasChunk)
        {
            ReserveChunk();
        }

        CommitChunk(last: true);
        _completed = true;
    }

    /// <summary>
    /// Ends the message as aborted: the consumer reports
    /// <see cref="RingBufferCorruptedException"/> instead of delivering a
    /// truncated value. A message whose chunks were never published is simply
    /// abandoned.
    /// </summary>
    public void Abort()
    {
        if (_completed)
        {
            return;
        }

        if (!_publishedAny)
        {
            if (_hasChunk)
            {
                _endpoint.AbandonWrite();
                _hasChunk = false;
                ReleaseChunk();
            }

            _completed = true;
            return;
        }

        if (!_hasChunk)
        {
            ReserveChunk();
        }

        CommitChunk(last: true, abort: true);
        _completed = true;
    }

    private void ReserveChunk()
    {
        long startTimestamp = Stopwatch.GetTimestamp();
        long timeoutTicks = WriteTimeoutSeconds * Stopwatch.Frequency;
        SpinWait spin = new();
        Span<byte> payload;
        while (!_endpoint.TryReserveWrite(_type, out payload))
        {
            if (Stopwatch.GetTimestamp() - startTimestamp >= timeoutTicks)
            {
                throw new RingBufferTimeoutException(
                    $"Buffer stayed full for {WriteTimeoutSeconds} seconds; the peer is not consuming.");
            }

            spin.SpinOnce();
        }

        fixed (byte* pointer = payload)
        {
            _chunk = pointer;
        }

        _used = 0;
        _hasChunk = true;
    }

    private void CommitChunk(bool last, bool abort = false)
    {
        ChunkFraming.WriteFlags(_chunk, _firstOfMessage, last, abort);
        _endpoint.CommitWrite(ChunkFraming.HeaderSize + _used);
        _firstOfMessage = false;
        _hasChunk = false;
        _publishedAny = true;
        ReleaseChunk();
    }

    private void ReleaseChunk()
    {
        _chunk = null;
        _used = 0;
    }
}
