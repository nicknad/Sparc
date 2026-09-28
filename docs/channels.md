# Typed channels (Sparc.Channels)

`SparcChannel<T>` is the ergonomic layer over the role-typed endpoints: encode
`T` once, publish bytes zero-copy through the ring, decode on the other side.
It exists for the common case where hand-writing the endpoint loop is not
interesting; the endpoint API stays available for zero-copy control and the
sessions stay available for fixed-protocol transfers.

## Two processes

Producer:

```csharp
using Sparc.Channels;

using SparcChannelWriter<Order> writer = SparcChannelWriter<Order>.Open(
    factory, "orders", OrderCodec.Default);

while (running)
{
    Order order = await queue.Reader.ReadAsync(stoppingToken);
    await writer.WriteAsync(order, stoppingToken);   // waits while the ring is full
}

// Disposing the writer publishes the graceful stop (endpoint state Stopped).
```

Consumer:

```csharp
using SparcChannelReader<Order> reader = SparcChannelReader<Order>.Open(
    factory, "orders", OrderCodec.Default);

await foreach (Order order in reader.ReadAllAsync(stoppingToken))
{
    Process(order);
}
```

Single process (tests, samples):

```csharp
using SparcChannel<Order> channel = SparcChannel<Order>.CreateInProcess(
    new InMemoryMemoryRegionFactory(), "orders", OrderCodec.Default);

channel.Writer.TryWrite(new Order(...));
if (channel.Reader.TryRead(out Order? order)) { ... }
```

## Codecs

```csharp
public interface ISparcCodec<T>
{
    int MaxSize { get; }
    int Encode(T item, Span<byte> destination);   // returns bytes written
    T Decode(ReadOnlySpan<byte> source);
}
```

`JsonCodec<T>` is provided for convenience; it allocates per message, so hot
paths should hand-write a codec (a length-prefixed UTF-8 string, a
`MemoryPack`-style record, and so on). When no explicit `slotSize` is passed,
the channel derives it from `MaxSize`, rounded up to a 64-byte cache line, so
adjacent slots never share one.

Serializers built on
[SerializerFoundation](https://github.com/Cysharp/SerializerFoundation) can be
plugged in through `SfCodec<T>` from the optional `Sparc.Serialization`
package: the serializer encodes straight into the writer's scratch span and
decodes straight from a slot's payload span through
`IWriteBuffer`/`IReadBuffer`, with no codec-side buffers. Measured on this
machine for an 8-byte record, encode+decode, 2M iterations, single-threaded:
`SfCodec<T>` ~81 ns/op and 0 B allocated versus `JsonCodec<T>` ~661 ns/op and
88 B per message; through the typed channel, ~165 ns/op and 0 B. A value that
needs more than `MaxSize` throws `InvalidOperationException`; values larger
than a slot use chunked streaming ([streaming.md](streaming.md)) instead.

## API

| Member | Behaviour |
|---|---|
| `writer.TryWrite(item)` | Encodes and publishes; `false` when the ring is full |
| `writer.WriteAsync(item, ct)` | Encodes, waits for a slot, throws when the consumer stopped |
| `writer.WaitToWriteAsync(ct)` | Completes when there is space; `false` when the consumer stopped |
| `reader.TryRead(out item)` | Decodes one message if buffered; no thread started |
| `reader.ReadAsync(ct)` | Waits for the next message; throws when the stream ends |
| `reader.WaitToReadAsync(ct)` | `false` once the stream completed |
| `reader.ReadAllAsync(ct)` | `IAsyncEnumerable<T>` that ends when the producer stops and the ring drains |
| `reader.Endpoint` / `writer.Endpoint` | The underlying role-typed endpoint (states, leases, `Count`) |

The first asynchronous read starts one dedicated pump thread per reader that
blocks on the endpoint and queues decoded messages into an in-process channel,
so async awaits never occupy a thread-pool thread. `idleTimeout` on
`SparcChannelReader<T>.Open` bounds how long the pump waits on an empty ring
before re-checking the producer state (default 250 ms); it is what ends
`ReadAllAsync` after the producer stops. While only `TryRead` is used, no
thread is started and reads hit the endpoint directly — do not mix direct
endpoint reads with the reader once the pump is running.

## Semantics (unchanged from the ring)

* Exactly one producer and one consumer per region; exactly one thread may call
  the writer/reader at a time.
* Bounded: the ring never grows. `TryWrite` reports the drop; `WriteAsync`
  blocks the caller asynchronously (no thread) until space appears.
* At-least-once across consumer crashes: a message is redelivered if the process
  dies between decoding and publishing `head`. Never torn, never silently lost.
* Not durable, not MPMC, no heartbeats: a hard-killed producer leaves the
  endpoint state `Running`; the consumer's `ReadAllAsync` then ends through the
  idle-timeout/producer-state check only if the state was published (graceful
  stop), otherwise keep your own liveness policy.
* The typed layer trades one copy (encode into the writer's scratch buffer) for
  ergonomics. For true zero-copy, publish into `writer.Endpoint` with
  `TryReserveWrite`/`WriteLease`, or use the endpoint API directly; for values
  larger than a slot, use chunked streaming ([streaming.md](streaming.md)).

## Overhead

The regression harness includes a `channel-8` scenario (encode/decode of a
`long` through the typed layer, in process, two dedicated threads) next to the
raw `array-copy-64` and `array-lease-64` scenarios, so wrapper overhead stays
visible. Run:

```powershell
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --regression
```
