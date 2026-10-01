# SPARC — Shared Process Atomic Ring Channel (.NET)

**SPARC** is a single-producer / single-consumer lock-free ring buffer in C# that lets
**two independent processes exchange fixed-size messages through the same physical
memory without locks**.

## 1. Architecture

```
Producer process                          Consumer process
      │                                         │
      │ ProducerSession.Run()                   │ ConsumerSession.Run()
      ▼                                         ▼
┌──────────────────────────────────────────────────────────────┐
│              IIpcMemoryRegionFactory (per-OS transport)      │
│                      │  shared region                        │
│  [header][slot 0][slot 1][slot 2] … [slot N-1]               │
│              ▲                     ▲                         │
│            head                  tail                        │
└──────────────────────────────────────────────────────────────┘
```

* Exactly one producer and one consumer (SPSC). Each counter has one writer —
  the producer only writes `tail`, the consumer only writes `head` — so the data
  path needs no mutex and no compare-and-swap, just `Volatile` acquire/release
  ordering (correct on x86-64 and ARM64).
* Fixed-size slots (`slot index = sequence % capacity`), bounded region
  (`192 + capacity × slotSize` bytes), zero allocations after initialization.
  Values larger than a slot are split into a chunk chain by the optional
  `Sparc.Serialization` package.
* OS abstraction: the ring protocol never references an OS type. `IIpcMemoryRegionFactory`
  selects the transport — named sections on Windows, file-backed regions on
  Unix, pinned arrays via `Sparc.InMemory` for tests — and transport access
  control (Windows section DACLs, Unix owner-only files) is checked once at
  open, never per message.
* Reusable session layer (`ProducerSession` / `ConsumerSession`) over role-typed
  endpoints (`SparcRing.OpenProducer` / `OpenConsumer`): `TimeProvider`,
  `CancellationToken`, `ILogger`, structured stop reasons, explicit crash
  semantics (at-least-once across consumer crashes, never torn).

See `docs/concept.md` and `docs/how-it-works.md` for guarantees, layout, and handshake.

## 2. Usage in code

Libraries multi-target `net8.0;net11.0`. Pick `Sparc.Hosting` for DI, `Sparc.Channels`
for a typed API, or `Sparc.Core` for raw zero-copy endpoints.

Producer process (hosted session):

```csharp
builder.Services.AddSparcIpc(); // picks the OS transport
builder.Services.AddSparcChannel(options =>
{
    options.Name = "orders";
    options.Capacity = 1024; // power of two
    options.SlotSize = 256;  // >= payload + 8; 64-byte multiples avoid false sharing
});
builder.Services.AddSparcProducerSession(options =>
{
    options.Count = 1_000_000;
    options.PayloadSize = 64;
});
```

Consumer process: same first two lines, then drain until the producer stops:

```csharp
builder.Services.AddSparcConsumerSession(options => options.Count = 0);
```

Raw endpoints (full control, no host):

```csharp
using IProducerEndpoint producer = SparcRing.OpenProducer(factory, "orders", capacity: 1024, slotSize: 256);
producer.TryPublish(payload);

using IConsumerEndpoint consumer = SparcRing.OpenConsumer(factory, "orders");
consumer.TryConsume(destination, out int bytesRead, out int messageType);
```

Typed channel (`Channel<T>`-style async API with span-based codecs):

```csharp
// producer
using SparcChannelWriter<Order> writer = SparcChannelWriter<Order>.Open(factory, "orders", new JsonCodec<Order>(maxSize: 2048));
await writer.WriteAsync(order, stoppingToken); // waits while full; TryWrite is the non-blocking path

// consumer
using SparcChannelReader<Order> reader = SparcChannelReader<Order>.Open(factory, "orders", new JsonCodec<Order>(maxSize: 2048));
await foreach (Order order in reader.ReadAllAsync(stoppingToken)) { Process(order); }
```

Sessions are synchronous and blocking — run them on a background thread. See
`docs/hosting.md`, `docs/channels.md`, and `docs/streaming.md`.

## 3. Performance

Reproduce (Release):

```powershell
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --filter *
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport shared-xproc --sizes 16,64,256,1024,4096,16384 --count 500000 --repeats 3
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --regression
```

In-process throughput (BenchmarkDotNet, 2 dedicated threads, 65,536 msg/batch, means):

| Transport | 16 B | 64 B | 256 B | 1 KB | 4 KB | Alloc/op @64 B |
|---|---:|---:|---:|---:|---:|---:|
| SPSC shared memory | 686 ns | 520 ns | 457 ns | 241 ns | 578 ns | 0 |
| SPSC ring buffer (array) | 970 ns | 734 ns | 1,069 ns | 658 ns | 893 ns | 0 |
| `Channel<T>` | 2,425 ns | 1,626 ns | 2,030 ns | 2,214 ns | 2,615 ns | 88 B |
| Concurrent queue + lock | 3,161 ns | 2,578 ns | 4,145 ns | 3,007 ns | 11,582 ns | 88 B |
| Named pipe | 40.9 µs | 30.8 µs | 30.7 µs | 37.7 µs | 35.5 µs | 0 |
| TCP loopback | 53.9 µs | 44.7 µs | 74.8 µs | 48.3 µs | 45.4 µs | 0 |

Cross-process Producer → Consumer (500k msg/size × 3 runs, consumer end-to-end rate):

| Message | Region | Throughput | Data | p50 | p99 | CPU prod/cons |
|---|---:|---:|---:|---:|---:|---:|
| 16 B | 0.3 MiB | 1.20M msg/s | 18.3 MiB/s | 86.40 µs | 15564.80 µs | 6/13 % |
| 64 B | 0.3 MiB | 1.86M msg/s | 113.2 MiB/s | 147.20 µs | 9830.40 µs | 9/11 % |
| 256 B | 0.3 MiB | 1.30M msg/s | 316.6 MiB/s | 230.40 µs | 14745.60 µs | 9/20 % |
| 1 KB | 1.1 MiB | 1.69M msg/s | 1,646.8 MiB/s | 230.40 µs | 9420.80 µs | 12/29 % |
| 4 KB | 4.1 MiB | 1.92M msg/s | 7,508.9 MiB/s | 115.20 µs | 4710.40 µs | 18/45 % |
| 16 KB | 16.1 MiB | 280k msg/s | 4,379.6 MiB/s | 1331.20 µs | 15564.80 µs | 11/28 % |

Notes:

* Numbers are from a shared Windows 11 VM (i7-1260P) — treat as directional, not
  absolute. Queue/channel baselines allocate per message by design; SPARC allocates
  nothing after setup.
* Payload size and region footprint are separate variables: at fixed footprint the
  per-byte cost is linear; the 16 KB row drops because a 16 MiB ring leaves the CPU
  caches plus the optional verification scan adds a second pass.
* Idle latency is a wait-mode choice (64 B, producer-paced): default `SpinThenSleep`
  ~5 ms at ~2 % CPU, `--spin-only` ~3.9 µs at one busy core, `--notify` (OS latch)
  ~7.6 µs at ~2 % CPU.
* p99 ≈ 15.6 ms is the Windows timer tick when the consumer catches up; p99.9/max
  ≈ 0.3 s are host VM stalls, not ring behavior.

See `docs/benchmarking.md` and `docs/performance-invariants.md`.

## 4. Scope boundary

Not implemented (deliberately): MPSC/MPMC, dynamic resizing, persistence,
networking, compression, multiple producers/consumers, heartbeats or automatic
crash detection, message authentication/encryption in the transport.

Fixed slots stay the unit of storage; larger values are chunked above the ring.
Access control stops at the OS primitive — a process with valid map access can
read and modify the region, so untrusted peers need an authenticated/encrypted
payload layered above the transport.

Learning objective this project exercises end to end:

> **Two independent processes can safely exchange data through the same memory using only
> atomic state transitions and memory-ordering guarantees.**
