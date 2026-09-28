# Chunked message streaming (Sparc.Serialization)

`SparcStreamWriter`/`SparcStreamReader` carry **messages larger than one slot
— larger than the whole ring, even** — by splitting each message into chunk
slots. The writer exposes a SerializerFoundation
[`IWriteBuffer`](https://github.com/Cysharp/SerializerFoundation) over the
ring, so a serializer streams straight into slots with no intermediate
buffer; the reader reassembles one complete message into a caller-provided
destination. (Single-slot messages use [`SfCodec<T>`](channels.md) instead,
which decodes directly from one slot.)

```csharp
// producer process: serialize a large value straight into chunk slots
using SparcStreamWriter writer = SparcStreamWriter.Open(factory, "events", capacity: 64, slotSize: 4096);

SparcStreamWriteBuffer buffer = writer.BeginMessage(type: 1);
try
{
    serializer.Serialize(ref buffer, value);   // any serializer over IWriteBuffer
}
finally
{
    buffer.Dispose();                          // publishes the final chunk
}

// consumer process: reassemble into a destination that fits the message
using SparcStreamReader reader = SparcStreamReader.Open(factory, "events", capacity: 64, slotSize: 4096);

byte[] destination = new byte[maxMessageSize];
int length = reader.ReadMessage(destination, out int type);
```

`IProducerEndpoint` gained the raw reservation the writer builds on:
`TryReserveWrite(type, out Span<byte> payload)` reserves the full payload
window, and `CommitWrite(length)` publishes only the bytes actually used.

## Wire format

Each chunk payload is `[int32 flags][data...]`, little-endian; the ring's own
slot framing carries the message type on every chunk.

| Flag | Meaning |
|---|---|
| `First` | First chunk of a message |
| `Last` | Final chunk of a message (a single-chunk message sets both) |

## Semantics and limits

* **Bounded memory, unbounded messages.** The producer blocks (spinning up to
  30 seconds, then `RingBufferTimeoutException`) when the ring is full; the
  consumer frees slots as it reads chunks. A message larger than the ring only
  requires the reader to keep reading — never a bigger region.
* **Interrupted messages are abandoned.** The reader always resynchronizes on
  the next `First` chunk, so a consumer crash mid-message drops the remaining
  tail instead of delivering a torn message. Single-chunk messages stay
  at-least-once; multi-chunk messages interrupted by a crash are at-most-once.
  A producer that stops mid-message makes the reader time out (per continuation
  chunk, default 30 s) and discard the partial message.
* **`GetSpan` is contiguous within one chunk.** A `sizeHint` larger than
  `MaxPayloadSize - 4` cannot be satisfied and throws; serializers that emit a
  single huge span must chunk it themselves.
* **The destination must fit the message.** `ReadMessage` throws
  `InvalidOperationException` and abandons the message when it does not; size
  the destination for your largest message.
* **One writer thread, one reader thread** per endpoint, as everywhere in
  SPARC. The buffers are single-owner: pass them by `ref`, never copy them
  (SerializerFoundation's SF002 analyzer enforces this).
