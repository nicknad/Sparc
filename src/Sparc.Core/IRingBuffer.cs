namespace Sparc.Core;

/// <summary>
/// Common surface for the in-process and shared-memory SPSC buffers.
/// </summary>
/// <remarks>
/// <para>
/// A message is <c>[payload]</c> plus a caller-chosen 32-bit <c>type</c> tag. The
/// buffer adds a small per-slot header, so the maximum payload is
/// <see cref="MaxPayloadSize"/> bytes; larger payloads are rejected.
/// </para>
/// <para>
/// <b>Threading contract:</b> exactly one thread may call <see cref="TryWrite(int, ReadOnlySpan{byte})"/>
/// and exactly one (different) thread may call <see cref="TryRead(Span{byte}, out int, out int)"/> for the
/// lifetime of the buffer. That guarantee is what makes the algorithm wait-free
/// on the producer side: no compare-and-swap is needed because each sequence
/// counter has a single writer.
/// </para>
/// </remarks>
public interface IRingBuffer
{
    /// <summary>Number of slots.</summary>
    int Capacity { get; }

    /// <summary>Total bytes per slot, including the per-slot header.</summary>
    int SlotSize { get; }

    /// <summary>Maximum payload accepted by <see cref="TryWrite(int, ReadOnlySpan{byte})"/>.</summary>
    int MaxPayloadSize { get; }

    /// <summary>True when there is nothing to read.</summary>
    bool IsEmpty { get; }

    /// <summary>Number of messages currently buffered.</summary>
    int Count { get; }

    /// <summary>Attempts to copy one message into the buffer. Returns false when the buffer is full.</summary>
    bool TryWrite(int type, ReadOnlySpan<byte> payload);

    /// <summary>
    /// Attempts to copy the oldest message out of the buffer. Returns false when
    /// the buffer is empty.
    /// </summary>
    /// <param name="destination">
    /// Receives the payload. Must be at least <see cref="MaxPayloadSize"/> bytes,
    /// because the reader commits to the read before the length is known.
    /// </param>
    /// <param name="bytesRead">Number of payload bytes copied.</param>
    /// <param name="type">Type tag stored with the message.</param>
    bool TryRead(Span<byte> destination, out int bytesRead, out int type);

    /// <summary>
    /// Reserves the next slot and returns a writable view of its payload area,
    /// so a producer can write a message without an intermediate copy. Call
    /// <see cref="CommitWrite"/> to publish the message, or
    /// <see cref="AbandonWrite"/> to leave the slot unpublished. Returns false
    /// when the buffer is full.
    /// </summary>
    /// <param name="type">Type tag to store with the message.</param>
    /// <param name="length">Payload bytes the caller will write (at most <see cref="MaxPayloadSize"/>).</param>
    /// <param name="payload">Writable view of exactly <paramref name="length"/> bytes inside the reserved slot.</param>
    /// <remarks>
    /// The view is only valid until the reservation is committed or abandoned.
    /// At most one reservation may be active at a time (the producer contract is
    /// a single thread).
    /// </remarks>
    bool TryReserveWrite(int type, int length, out Span<byte> payload);

    /// <summary>Publishes the slot reserved by <see cref="TryReserveWrite"/>.</summary>
    void CommitWrite();

    /// <summary>Discards the slot reserved by <see cref="TryReserveWrite"/> without publishing it.</summary>
    void AbandonWrite();

    /// <summary>
    /// Returns a read-only view of the oldest published message without copying
    /// it out. Call <see cref="AdvanceRead"/> when done with the view. Returns
    /// false when the buffer is empty.
    /// </summary>
    /// <param name="payload">Read-only view of the payload, valid until <see cref="AdvanceRead"/>.</param>
    /// <param name="length">Payload length in bytes.</param>
    /// <param name="type">Type tag stored with the message.</param>
    /// <remarks>
    /// The producer cannot reuse the slot before <see cref="AdvanceRead"/> is
    /// called, so the view stays valid for the duration of the processing.
    /// </remarks>
    bool TryPeek(out ReadOnlySpan<byte> payload, out int length, out int type);

    /// <summary>Releases the slot returned by <see cref="TryPeek"/> to the producer.</summary>
    void AdvanceRead();

    /// <summary>Convenience overload for messages that do not use a type tag.</summary>
    bool TryWrite(ReadOnlySpan<byte> payload) => TryWrite(0, payload);

    /// <summary>Convenience overload that discards the type tag.</summary>
    bool TryRead(Span<byte> destination, out int bytesRead) => TryRead(destination, out bytesRead, out _);

    /// <summary>Convenience overload that discards length and type.</summary>
    bool TryRead(Span<byte> destination) => TryRead(destination, out _, out _);
}
