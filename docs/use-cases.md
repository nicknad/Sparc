# Use cases

## When SPARC fits

Use SPARC when **all** of these hold:

* Exactly one producer and one consumer, in different processes (or two views in
  one process).
* Messages are fixed-size or can be packed into a fixed slot.
* Both processes run on the same machine under the same user.
* The hot path must avoid locks, scheduler wake-ups, and GC pauses.
* Losing a message under crash conditions is acceptable (at-least-once
  redelivery across a consumer crash is the strongest guarantee).
* Bounded memory and predictable latency matter more than flexible queueing
  semantics.

If any of these fail, see [Anti-patterns](#anti-patterns) for alternatives.

## 1. Local telemetry pipeline (application -> agent)

The classic fit: an application process samples counters, timings, or events and
a separate collector/agent process aggregates, forwards, or writes them.

* Producer: the application, on a dedicated thread (never the request thread).
* Consumer: the agent; use `SessionWaitMode.Notification` or
  `SpinOnly` if the agent must react in microseconds, otherwise the default.
* Slot: `[sequence][timestamp][sample bytes]`; the timestamp enables one-way
  latency percentiles via the built-in histogram.
* Backpressure: if the agent falls behind, the ring fills and the producer's
  write fails; decide the policy at the producer (drop, block, or shrink the
  sample rate). The CLI's `--delay-us` simulates the slow side for tests.

Why not a socket: the payload never enters the kernel, the agent's wake-up is
either immediate (spin/notification) or configurable, and the pipeline has no
allocator or GC involvement, so pause times do not disturb the sample stream.

## 2. Reverse-proxy request capture (YARP sample)

`samples/yarp` captures request metadata in a YARP proxy and analyzes it in a
separate process without blocking proxied requests:

```
request pipeline -> capture middleware -> bounded Channel<T> -> background worker
                                                                     |
                                                            SPARC producer -> ring
                                                                     |
                                              consumer process: decode JSON, median of a header
```

Details worth copying:

* The middleware only captures bounded data (32 headers, 256 chars per value,
  keys lower-cased) and never blocks the request: captures go into a bounded
  in-process `Channel<T>`, and a `BackgroundService` drains it.
* Captures are JSON with variable length, so the ring is used directly via
  `SharedRingBuffer`-backed role endpoints (not the sessions, which speak the fixed
  `[sequence][timestamp][fill]` protocol). Each JSON capture is packed into one
  fixed slot; a capture that does not fit is dropped and counted.
* The consumer process decodes captures, parses `--header` as a number, and
  turns `--expected-median` into a PASS/FAIL exit code.

This is the pattern for "instrument production without changing its timing":
bounded buffering at the source, best-effort transfer, analysis elsewhere.

## 3. Hosting in a web app or worker service

The `samples/Sparc.WebApp` sample hosts both roles in one process to demonstrate
the full channel; a real deployment hosts one role per process.

* Register the transport once: `builder.Services.AddWindowsNamedMemoryMappedIpc()`
  (Windows) or `AddUnixFileMemoryMappedIpc()` (Unix-like).
* Host the session in a `BackgroundService`, dispatching the blocking `Run` to a
  thread with `Task.Run` (`DemoTransferWorker`), or use the minimal shape in the
  top-level README.
* Sessions are synchronous and thread-affine: one thread per role for the whole
  run. Never `Task.Run` per message.
* `SparcRing.OpenProducer`/`OpenConsumer` may block while it waits for the region;
  offload it too (the sample's open helpers do).

## 4. Latency-sensitive consumer

The wait mode is a first-class knob (`SessionWaitMode`, CLI `--spin-only` /
`--notify`):

| Mode | Idle CPU | Wake-up latency | Use when |
|---|---|---|---|
| `SpinThenSleep` (default) | ~0 | ~5 ms | Throughput pipelines, batch consumers |
| `SpinOnly` | 1 core | ~microseconds | Dedicated core available, lowest latency |
| `Notification` | ~0 | ~microseconds | Lowest latency without pinning a core |

Measured on the paced-producer scenario in this repo: p50 goes from 6.45 ms
(default) to 3.9 us (`SpinOnly`) and 7.6 us (`Notification`). See
[benchmarking.md](benchmarking.md).

## 5. Development, tests, and benchmarks without the OS

`Sparc.InMemory` provides the whole ring/session stack over pinned managed
arrays. The unit tests, the WebApp sample's first iteration, and single-process
development all run on it; only the concurrency and process tests need real
named mappings. Swapping the factory is the only change:

```csharp
IIpcMemoryRegionFactory factory = new InMemoryMemoryRegionFactory();
using IProducerEndpoint producer = SparcRing.OpenProducer(factory, "dev", 1024, 256);
```

## 6. Bridging an unbounded source into a bounded channel

When the source cannot be paused (network, user input), put a bounded
`Channel<T>` in front of the producer and drain it on a background thread. The
channel absorbs bursts; the ring provides the cross-process hop; the producer
decides the drop policy when the ring is full. The YARP sample is exactly this
shape.

## Anti-patterns

| Situation | Better tool |
|---|---|
| Multiple producers or consumers | `Channel<T>` / `ConcurrentQueue<T>`, or shard into multiple SPSC rings (one per producer/consumer pair) |
| Records much larger than a cache line and truly variable | Sockets/pipes, or split into chunks; fixed slots waste memory and add a copy |
| Cross-machine transport | TCP/QUIC/gRPC |
| Durability or replay after both processes die | A log/broker (Kafka, files); SPARC memory dies with its mappings |
| Untrusted peer process | No isolation: both sides can corrupt the region. Use IPC with kernel-enforced boundaries |
| Request/response RPC with retries | gRPC or a message broker; SPARC has no acknowledgements |
| Need to know the peer is alive without traffic | Add a heartbeat message type over the ring; states are advisory |

## Sizing guide

* **Slot size**: `max(256, roundUp(payload + 8, 64))`. The CLI derives this
  automatically. The 64-byte rounding keeps slot boundaries on cache lines; the
  8-byte frame is `[length:int32][type:int32]`.
* **Capacity**: power of two. It sets both the maximum messages in flight and
  the worst-case queueing delay `capacity x consumer period` when the consumer
  is the slower side. 1024 is the default.
* **Footprint**: `192 + capacity x slotSize`. This is the number that decides
  cache residency: a 4 MiB region streams at cache speed on the machines tested
  here, a 16 MiB region falls back to DRAM and loses roughly 1.8x. Choose the
  smallest capacity that absorbs your burst, and measure the footprint, not just
  the payload size ([performance-invariants.md](performance-invariants.md),
  section C).
* **Throughput vs latency**: high throughput wants a larger ring (absorbs
  jitter); low latency wants a smaller ring (less queueing) plus `SpinOnly` or
  `Notification`.
