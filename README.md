# SPARC — Shared Process Atomic Ring Channel (.NET)

**SPARC** is a single-producer / single-consumer lock-free ring buffer in C#, with an
operating-system abstraction and a reusable session layer, letting **two
independent processes exchange fixed-size messages through the same physical
memory without locks**.

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

* exactly one producer and one consumer (SPSC)
* no mutexes, no OS synchronization, no CAS on the data path
* fixed-size slots, bounded memory, zero allocations after initialization
* `Volatile` acquire/release ordering (works on x86-64 and ARM64, not just "x86 is strong")
* OS-specific mapping isolated behind `IIpcMemoryRegionFactory`
* reusable sessions for CLIs, web apps and worker services (`TimeProvider`,
  `CancellationToken`, `ILogger`, structured results — no console coupling)
* explicit, documented crash semantics

Built and tested against **.NET 11 RC1** (`11.0.100-rc.1.26425.128`, pinned in
`global.json`). Libraries multi-target **`net8.0;net11.0`**; apps/tests target
`net11.0`.

---

## AI assistance

The implementation and documentation in this repository were generated with AI
assistance (**DeepSeek Flash 4.1**) under the maintainer's direction and then
tested and benchmarked. The full test suite (107 unit, concurrency and process
tests) and the benchmark harness ship with the repository so the result can be
verified independently — it is nonetheless AI-authored code: read it with that
in mind, and open an issue for anything that does not hold up.

---

## Documentation

`docs/` goes deeper than this file:

* [docs/concept.md](docs/concept.md) — the problem, the core idea, guarantees, and how it compares to pipes/sockets/queues.
* [docs/use-cases.md](docs/use-cases.md) — when to use it, sample patterns, anti-patterns, sizing.
* [docs/how-it-works.md](docs/how-it-works.md) — handshake, layout, algorithm, memory ordering, lease API, sessions, wait modes, crash semantics, platforms, tests.
* [docs/performance-invariants.md](docs/performance-invariants.md) — every invariant behind the measured performance, with mechanism, code location, payoff, and failure mode.
* [docs/benchmarking.md](docs/benchmarking.md) — harnesses, commands, current numbers, measurement traps, regression checks.

---

## Repository layout

```
.
├── Sparc.slnx                library solution: src, tests, benchmarks
├── Sparc.Samples.slnx        samples solution: samples/ (intentionally not in Sparc.slnx)
├── docs/                     concept, use cases, mechanics, performance invariants, benchmarking
├── src/
│   ├── Sparc.Abstractions/           IIpcMemoryRegionFactory / IIpcMemoryRegion, options, exceptions
│   ├── Sparc.InMemory/               pinned managed-array factory for tests and single-process development
│   ├── Sparc.UnixMemoryMapped/       Unix file-backed implementation + DI registration
│   ├── Sparc.WindowsMemoryMapped/    Windows named memory-mapped implementation + DI registration
│   ├── Sparc.Core/            ring protocol: layout/header/framing, SpscRingBuffer, SharedRingBuffer
│   ├── Sparc.Client/          ProducerSession/ConsumerSession, message protocol, latency histogram
│   ├── Sparc.Producer/        producer CLI (args → session → summary → exit code)
│   └── Sparc.Consumer/        consumer CLI (args → session → summary → exit code)
├── samples/
│   ├── Sparc.WebApp/          minimal API + BackgroundService hosting both session roles
│   └── yarp/
│       ├── Sparc.YarpProxy/      YARP reverse proxy → bounded channel → SPARC producer
│       ├── Sparc.YarpConsumer/   SPARC consumer that checks a captured header's median
│       └── Sparc.YarpShared/     capture protocol shared by the two YARP samples
├── tests/
│   ├── Sparc.UnitTests/       algorithm, region protocol, sessions (on Sparc.InMemory)
│   ├── Sparc.ConcurrencyTests/ 2 × 10,000,000 message two-thread verification
│   └── Sparc.ProcessTests/    two real processes: lifecycle, conflicts, kill tests
└── benchmarks/
    └── Sparc.Benchmarks/      BenchmarkDotNet throughput suite + custom latency harness
```

Dependency graph (arrows = project reference):

```
Sparc.Abstractions
      ▲                ▲               ▲
Sparc.Core      Sparc.InMemory   Sparc.UnixMemoryMapped
      ▲            ▲
Sparc.Client  Sparc.WindowsMemoryMapped
      ▲                   ▲
Sparc.Producer / Sparc.Consumer / samples/* (hosts)
```

`Sparc.WindowsMemoryMapped`, `Sparc.UnixMemoryMapped` and `Sparc.InMemory` do not
reference `Sparc.Core`; the ring protocol does not reference any OS type.

---

## Quick start (CLI)

```powershell
dotnet build Sparc.slnx -c Release
dotnet test  Sparc.slnx -c Release
dotnet build Sparc.Samples.slnx -c Release    # samples live in a separate solution

# terminal 1
dotnet run -c Release --project src/Sparc.Consumer -- --name demo --count 1000000

# terminal 2
dotnet run -c Release --project src/Sparc.Producer  -- --name demo --count 1000000 --size 64
```

Builds treat warnings as errors (`TreatWarningsAsErrors` in `Directory.Build.props`).
Samples are intentionally not part of the library solution: they build from
`Sparc.Samples.slnx` (same repo, separate solution). A GitHub Actions workflow
(`.github/workflows/ci.yml`) is ready to build + test the library solution and build
the samples on `windows-latest` once a git remote is configured.

The consumer may also be started first (it creates the region; the producer joins).
Example output:

```
ready: role=consumer name=demo capacity=1024 slotSize=256 maxPayload=248
consumed=1000000 bytes=64000000 elapsed=0.61s throughput=1639344 msg/s dataThroughput=100.0 MiB/s wallElapsed=0.65s
producer=Stopped consumer=Running
latency(us): min=1.20 mean=122.44 p50=51.20 p90=102.40 p95=204.80 p99=409.60 p99.9=2048.00 max=20480.00 (n=1000000)
```

### CLI

`producer --name <name> [--count n] [--size bytes] [--capacity slots] [--slot-size bytes]`
`[--type int] [--open-timeout ms] [--full-timeout ms] [--delay-us us] [--takeover] [--spin-only] [--notify] [--recreate-stale] [--require-existing] [--quiet]`

`consumer --name <name> [--count n] [--capacity slots] [--slot-size bytes] [--type int]`
`[--open-timeout ms] [--idle-timeout ms] [--delay-us us] [--takeover] [--spin-only] [--notify] [--recreate-stale] [--require-existing] [--no-verify] [--no-verify-payload] [--quiet]`

`--count 0` on the consumer means "consume until the producer stops".
`--size` is the payload size; layout is `[sequence:int64][timestamp:int64][fill…]` (min 16 bytes).
A slot size derived from `--size` is rounded up to a 64-byte cache line (pass `--slot-size`
to override); this keeps slot boundaries on cache-line boundaries so adjacent slots never
share one.
`--delay-us` inserts a pause between messages on that endpoint (slow-producer/backpressure
simulation for tests and benchmark scenarios; 0 = off). The CLIs select the transport by OS:
named memory-mapped files on Windows, file-backed regions elsewhere. `Ctrl+C` cancels the
session gracefully.

### Exit codes (both apps)

| Code | Meaning |
|---:|---|
| 0 | success |
| 2 | timeout (region never appeared, or buffer stayed full too long) |
| 3 | peer vanished / stream incomplete |
| 4 | role conflict (another live instance owns the role) |
| 5 | incompatible version/geometry/platform |
| 6 | corrupt / half-initialized region |
| 7 | message verification failure |
| 64 | usage error |
| 70 | internal error (unexpected exception) |

---

## Using the libraries

Reference the projects (or packages once published) you need:

| Library | Use when |
|---|---|
| `Sparc.Abstractions` | you only need the OS abstraction contracts |
| `Sparc.InMemory` | you want the whole ring/session stack without the OS (tests, samples, single-process development) |
| `Sparc.UnixMemoryMapped` | you run on Linux/macOS and want file-backed regions (+ DI) |
| `Sparc.WindowsMemoryMapped` | you run on Windows and want named memory-mapped regions (+ DI) |
| `Sparc.Core` | you need the buffer (`SpscRingBuffer`, `SharedRingBuffer`) |
| `Sparc.Client` | you need producer/consumer sessions and verification |

### Web app / worker service / generic host

A runnable version of this pattern lives in `samples/Sparc.WebApp` (see below);
the snippets show the shape of a production worker.

```csharp
using Sparc;
using Sparc.WindowsMemoryMapped;
using Sparc.Client;
using Sparc.Core;

var builder = WebApplication.CreateBuilder(args);

// Chooses the OS transport once. Web app code never sees MemoryMappedFile.
builder.Services.AddWindowsNamedMemoryMappedIpc();   // Windows
// builder.Services.AddUnixFileMemoryMappedIpc();     // Linux/macOS

// e.g. builder.Services.AddHostedService<OrderProducerWorker>();
```

```csharp
sealed class OrderProducerWorker(
    IIpcMemoryRegionFactory factory,
    ILogger<OrderProducerWorker> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(() =>
    {
        using SharedRingBuffer buffer = SharedRingBuffer.OpenOrCreate(
            factory, "orders", capacity: 1024, slotSize: 256,
            new SharedRingBufferOptions { OpenTimeout = TimeSpan.FromSeconds(30) });

        ProducerSession session = new(buffer, new ProducerSessionOptions
        {
            Count = 1_000_000,
            PayloadSize = 64,
        }, logger: logger);

        ProducerRunResult result = session.Run(stoppingToken);
        logger.LogInformation("Sent {Count} messages, stopped because {Reason}",
            result.Produced, result.Reason);
    }, stoppingToken);
}
```

### Runnable sample (`samples/Sparc.WebApp`)

```powershell
dotnet run -c Release --project samples/Sparc.WebApp -- --urls http://127.0.0.1:5199
```

The sample registers the transport with `AddWindowsNamedMemoryMappedIpc()`, then a
single `BackgroundService` hosts both roles over two views of the same region
(in a real deployment each process would host one role). `GET /` returns the
transport and transfer status; `POST /transfer?count=n` starts another transfer.
One 100,000-message transfer runs at startup; set `Sparc:AutoStartCount=0` to
disable it.

Sessions are intentionally **synchronous and blocking**; hosts should invoke them on a
background thread (`Task.Run`, `BackgroundService`). How an endpoint waits while the buffer
is full/empty is `SessionWaitMode` (CLI: `--spin-only`, `--notify`):

| Mode | Wait behavior | Idle cost | Typical idle wake-up |
|---|---|---|---|
| `SpinThenSleep` (default) | `SpinWait`: spin, yield, sleep | ~0 % of a core | ~5 ms (timer) |
| `SpinOnly` | `Thread.SpinWait`, never sleeps | 1 core | ~µs |
| `Notification` | blocks on an OS latch the peer raises | ~0 % of a core | ~µs (measured p50 7.6 µs) |

`Notification` needs a `SessionNotification` shared by both endpoints
(`CreateInProcess()` when both roles share a process, `CreateNamed(regionName)` across
processes). Named latches use named semaphores, which .NET supports on Windows only; on
Unix-like systems use the other modes for now. The peer only raises the latch when the
other side has declared itself waiting (the `ConsumerWaiting`/`ProducerWaiting` header
flags), so a running peer pays nothing; the waiter always re-checks the buffer, so a lost
raise only means falling back to a bounded poll slice.

Cancellation is checked on the full/empty paths and periodically on the hot path;
`SharedRingBuffer.Connect` and `SpscRingBuffer.Write` also accept a `CancellationToken`
that bounds their spin loops, and the sessions report cancellation during role claiming as
`SessionStopReason.Cancelled` rather than throwing. `ProducerSessionOptions`/
`ConsumerSessionOptions` also accept a `PerMessageDelay` (CLI: `--delay-us`) to pace one
endpoint and simulate a producer/consumer speed mismatch; see the benchmark section for
measured effects.

Structured outcomes replace console/exit-code decisions:

```csharp
ProducerRunResult  { Produced, PayloadSize, Elapsed, Reason, Unsent, PeerState, FailureMessage }
ConsumerRunResult  { Received, ReceivedBytes, Elapsed, Reason, Latency, FailureMessage }
SessionStopReason  { Completed, PeerStopped, Timeout, VerificationFailed, Cancelled }
```

Testability: pass a custom `TimeProvider` and/or `ILogger` into the sessions, and
swap `IIpcMemoryRegionFactory` for `Sparc.InMemory.InMemoryMemoryRegionFactory`
to run the whole stack without the OS.

### YARP reverse-proxy sample (`samples/yarp/Sparc.YarpProxy` + `samples/yarp/Sparc.YarpConsumer`)

```powershell
# terminal 1: SPARC consumer checking the median of a captured header
dotnet run -c Release --project samples/yarp/Sparc.YarpConsumer -- --expected-median 999.5 --tolerance 50

# terminal 2: YARP proxy + capture channel + SPARC producer (+ 2000 demo requests)
dotnet run -c Release --project samples/yarp/Sparc.YarpProxy
```

```
YARP proxy process                                     consumer process
┌─────────────────────────────┐                        ┌──────────────────────────┐
│ request pipeline            │                        │ SharedRingBuffer         │
│   capture route + headers   │                        │   decode JSON captures   │
│        │                    │                        │   parse --header values  │
│        ▼                    │                        │   median / p95 / max     │
│ bounded Channel<T>          │                        │        ▲                 │
│        │                    │                        │        │                 │
│        ▼                    │      shared ring       │        │                 │
│ SPARC producer worker ──────┼────────────────────────┼────────┘                 │
└─────────────────────────────┘                        └──────────────────────────┘
```

The proxy registers the transport with `AddWindowsNamedMemoryMappedIpc()` and adds a
step to the YARP proxy pipeline (before session affinity/load balancing) that captures
the matched route and the request headers: keys are lower-cased and both the header
count (32) and each value's length (256 chars) are bounded. Captures go to a bounded
in-process `Channel<T>` without blocking the proxied request; a `BackgroundService`
drains it and try-writes JSON captures into the shared ring, counting captures dropped
because the ring was full. An embedded `/echo` backend means no external service is
needed, `GET /status` reports the capture/ring counters, and `--Sparc:DemoRequestCount=0`
disables the built-in load generator so you can drive `/proxy/{**}` yourself.

The consumer decodes each capture, parses the configured `--header` (default
`x-sample-value`) as a number, keeps a capped sample list and prints median/p95/max
every 5 s and at the end. `--expected-median` turns the final report into a PASS/FAIL
check (exit code 1 on failure). Both samples use `SharedRingBuffer` directly rather than
the session classes, because captures are variable-length JSON payloads; the sessions
speak the fixed `[sequence][timestamp][fill]` protocol.

---

## 1. The ring algorithm

`head` and `tail` are **monotonically increasing 64-bit sequence numbers**, not array
indices:

```
slot index = sequence % capacity            (capacity is a power of two → sequence & (capacity-1))

empty:  head == tail
full:   tail - head == capacity
count:  tail - head
```

The producer only ever writes `tail`, the consumer only ever writes `head`. Because each
counter has exactly one writer, the data path needs **no compare-and-swap and no lock** —
just ordered loads and stores.

```
Producer TryWrite(type, payload):                Consumer TryRead(destination):
  tail = self.tail                                 head = self.head
  head = acquire(shared.head)                      tail = acquire(shared.tail)
  if tail - head >= capacity → full                if tail == head → empty
  write slot at (tail & mask)                      read  slot at (head & mask)
  slot = [len][type][payload]                      copy payload/type out
  release(shared.tail, tail + 1)                   release(shared.head, head + 1)
```

### Cached peer cursor (a real optimization, not a hack)

A peer that writes its counter on every message forces a cache-line transfer on every
read. Each side therefore keeps a private, possibly stale copy of the peer's counter and
only re-reads the shared value when the stale copy says **full** (producer) or **empty**
(consumer). The cached value can never be newer than the real one, so the worst case is a
redundant refresh; measured effect on this VM: **1.3M → 13M messages/s** (`slotSize=256`).

The caches are seeded when the buffer is constructed (`_cachedHead` / `_cachedTail`).
That matters when attaching to a region with non-zero cursors: a zero-initialized
consumer cache would treat `head == tail > 0` as "data available" and read an
unpublished slot. `SharedRingBufferTests.FreshConsumerAttachedToDrainedNonZeroRegionReportsEmpty`
is the regression test for exactly that.

---

## 2. Memory ordering (the part that actually matters)

The correctness of the algorithm rests on two release/acquire pairs:

```
PRODUCER                                    CONSUMER
────────                                    ────────
write [len][type][payload]                  acquire load  tail
release store tail ───────────────────────▶ ────────────────────────
                                            read [len][type][payload]
                                            release store head
acquire load  head ◀─────────────────────── ────────────────────────
(now the slot is free to overwrite)         (slot is published as consumed)
```

* **Producer → Consumer (`tail`).** The release store of `tail` makes the slot bytes
  written *before* it visible to any thread that performs an acquire load of `tail` and
  observes the new value. Without this pair the consumer could read `tail` first and the
  payload bytes after (or read stale payload bytes), especially on ARM64 or after JIT
  reordering.

* **Consumer → Producer (`head`).** The release store of `head` publishes "this slot is
  consumed"; the producer's acquire load of `head` guarantees it neither overwrites a slot
  the consumer is still reading nor reuses a slot before the copy completed.

### Why `Volatile` is required even on x86-64

x86-64's TSO model orders ordinary stores after ordinary loads/stores sufficiently for
this algorithm, but that is a property of the *hardware*, not of the C# memory model.
The C# compiler and JIT are free to reorder non-volatile accesses (and do, e.g. hoisting
the peer cursor read out of a loop). `Volatile.Read`/`Volatile.Write` emit the required
compiler barriers and, on ARM64, real `ldar`/`stlr` instructions. Relying on x86 TSO is
the classic way to write a buffer that only breaks when ported.

### Cross-process visibility

Both processes map the *same physical pages*. Coherence between the two mappings is
provided by the hardware cache-coherence protocol; a store eventually becomes visible to
the other core, and the release/acquire pair establishes the ordering. No explicit
`Flush()` is needed (`Flush` only matters for file durability, not visibility between
mappings of the same object). Two additional rules make this work:

* **64-bit processes.** `head`/`tail` are aligned 64-bit fields (offsets 64 and 128, on
  their own cache lines). Aligned 64-bit loads/stores are atomic on x86-64/ARM64; on
  32-bit processes .NET does not guarantee atomic 64-bit accesses.
* **Fixed offsets, no struct layout surprises.** The header is written field-by-field at
  fixed little-endian offsets, never by blitting a struct.

---

## 3. Binary layout

```
Offset  Size  Field
0       8     Magic            0x474E_4952_4353_5053  ("SPSCRING" little-endian)
8       4     Version          1
12      4     Capacity         number of slots (power of two)
16      4     SlotSize         bytes per slot
20      4     HeaderSize       192
24      4     MaxPayloadSize   SlotSize - 8
28      4     Flags            reserved (0)
32      4     ProducerState    advisory endpoint state
36      4     ConsumerState    advisory endpoint state
40      4     ConsumerWaiting  1 while the consumer blocks on a notification
44      4     ProducerWaiting  1 while the producer blocks on a notification
48      16    Reserved         zero
64      8     Head             consumer-owned sequence (own cache line)
72      56    Padding
128     8     Tail             producer-owned sequence (own cache line)
136     56    Padding
192     ...   Slots            Capacity × SlotSize
```

Each slot:

```
0       4     Length   payload bytes (little-endian, ≤ SlotSize - 8)
4       4     Type     caller-defined tag (little-endian)
8       ...   Payload
```

* `head` and `tail` share no cache line (false sharing would otherwise serialize the two
  ends).
* `HeaderSize = 192` is cache-line aligned, so slot starts stay aligned when `SlotSize`
  is a multiple of 64 (the default 256 is).
* Slot sizes derived from a payload size are rounded up to a cache line, in the CLIs and in
  the benchmark pumps alike: tightly packed small slots make the producer's writes and the
  consumer's reads **false-share** and can slow a buffer down several-fold. The sweep also
  reports the region footprint, because a bigger ring leaves the CPU caches and the measured
  cost per byte then includes a DRAM round trip (see §6).

Region size is exactly `192 + Capacity × SlotSize` bytes (1024 × 256 → 262,336 bytes).

### Initialization handshake and platform abstraction

`SharedRingBuffer.OpenOrCreate(factory, …)` asks the factory for the region and then
performs the protocol handshake in `RingBufferRegion`:

* `factory.CreateOrOpen(name, size, options)` either creates a region or joins the
  existing one. On Windows this is `MemoryMappedFile.CreateNew` / `OpenExisting`; the
  factory retries until `IpcRegionOptions.OpenTimeout` elapses.
* The creator writes every header field **except the magic**, then publishes the magic
  with a release store. An opener polls the magic with an acquire load and only then
  reads the geometry, so it can never observe a half-written header.
* Openers validate version, geometry and declared size, producing specific exceptions
  (`RingBufferVersionMismatchException`, `RingBufferGeometryMismatchException`,
  `RingBufferCorruptedException`).
* Stale-region recovery is split: `RingBufferRegion` detects the bad magic, asks the
  factory to reclaim the backing store (`TryReset`; a no-op on Windows, where dropping
  the last handle is enough), and retries once — only when `RecreateIfStale` is set.

**Platform note.** .NET supports named memory-mapped files on **Windows only**
(`MemoryMappedFile.CreateNew(name, …)` / `OpenExisting(name)` throw
`PlatformNotSupportedException` on Unix), so `WindowsNamedMemoryMappedRegionFactory`
checks this and fails with a clear `IpcPlatformNotSupportedException`. Unix-like
systems use `UnixFileMemoryMappedRegionFactory` instead: one file per region under a
directory (default `<temp>/sparc`, tmpfs on most Linux systems), mapped with
`MemoryMappedFile.CreateFromFile`. Those files are persistent, so a crashed creator
leaves a stale file behind; `TryReset` unlinks it (safe on POSIX while other
processes still have it mapped) and the next `CreateOrOpen` starts from a fresh,
zero-filled file. Nothing above the abstraction changes — both transports implement
the same `IIpcMemoryRegionFactory`.

---

## 4. Process lifecycle and crash semantics

A lock-free data structure is **not** automatically crash-safe. The semantics here are
deliberately explicit:

| Situation | Behaviour |
|---|---|
| Consumer starts first | It creates and initializes the region; the producer joins later. |
| Producer starts first | Same, mirrored. The producer simply fills the buffer and spins until the consumer appears. |
| Region missing | First process creates it; geometry is fixed by whoever creates it (the consumer adopts existing geometry when joining). |
| Second producer/consumer starts | `Connect(role)` CASes the role state; a live `Starting`/`Running` peer causes exit code 4. `--takeover` forcibly reclaims a role. |
| Stale/half-initialized region | Bad magic after `--open-timeout` → exit 6, unless `--recreate-stale` is given: drop handles, reclaim, create again. On Windows a named map dies with its last handle, so a dead creator leaves nothing behind; on Unix the backing file persists until `--recreate-stale` unlinks and recreates it. |
| Incompatible version/geometry | Exit 5 with a message naming the mismatch. |
| **Producer killed mid-run** | Unpublished slot writes are invisible (tail is only published after the full slot write). Messages already published stay valid. The consumer blocks on empty, detects the idle timeout and exits 3. The producer's `ProducerState` remains `Running` forever — a hard kill cannot update it, and no heartbeat is implemented (see scope). |
| **Consumer killed mid-run** | The producer eventually fills the buffer and exits 2 after `--full-timeout`. If a consumer died gracefully it sets `Stopped`, which the producer treats as "consumer gone" (exit 3) when it observes it. |
| Consumer crash during a read | The payload copy happens **before** the release store of `head`. A crash in between leaves `head` unmoved → the message is **redelivered after restart**. Delivery is therefore **at-least-once across consumer crashes**, never torn and never lost. |
| Producer restart | A restarted producer appends at the existing `tail`; the sample protocol's sequence numbers restart at 0, so run with `--no-verify` or a fresh name if you resume an old region. |
| Endpoint state | `NotPresent → Starting → Running → Stopped/Faulted` is advisory: it enables fast graceful-shutdown detection and role-conflict rejection, but a hard kill leaves `Running`. Liveness detection would need heartbeats, which are out of scope. |

---

## 5. Testing

```powershell
dotnet test Sparc.slnx -c Release        # .NET 11 SDK + Microsoft.Testing.Platform (xunit v3)
```

* **UnitTests.** Empty/one/full/wraparound, arbitrary bytes, zero-length and maximum-sized
  payloads, 200k randomized operations against a `Queue<byte[]>` oracle, header/layout
  validation, region creation/join/version/geometry/corruption, role conflicts and
  takeover, endpoint states, histogram math (percentile ordering, p50/p90/p95/p99/p99.9
  report), DI registration, cancellation while a blocking write is full, and session
  semantics (completion, timeouts, peer-stopped drain, verification failure, cancellation,
  per-message pacing). Region protocol tests run against the
  `Sparc.InMemory` factory, so they are OS-independent; Windows factory tests are
  guarded by `OperatingSystem.IsWindows()` and Unix factory tests by
  `!OperatingSystem.IsWindows()` (the Unix tests were verified under WSL Debian 13
  with the pinned SDK while CI itself stays Windows-only).
* **ConcurrencyTests.** Two dedicated threads move **10,000,000 messages** per transport,
  with sequence + checksum + fill-byte verification on every message, for both the
  in-process array buffer and the shared-memory buffer (two views of one region). A
  tiny-capacity (2-slot) torture test (1,000,000 messages) and a no-allocation assertion
  (writes+reads allocate < 4 KiB total).
* **ProcessTests.** Real `dotnet` child processes: both start orders, unlimited consumer
  drain, role conflict, geometry mismatch, killed consumer → producer times out, killed
  producer → consumer exits incomplete, `--require-existing` timeout.

The *full* suite targets Windows: the shared-memory concurrency test and the process
tests need named memory-mapped files.

Crash tests kill processes with `Process.Kill(entireProcessTree: true)`; the assertions
check exit codes and message counts, not wall-clock timing.

---

## 6. Benchmarks

```powershell
# throughput matrix (BenchmarkDotNet; 8 transports × 5 message sizes)
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --filter *

# in-process regression check against benchmarks/Sparc.Benchmarks/perf-baseline.json
# (capture once per machine with --save-baseline; exits 1 when a scenario is below tolerance)
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --regression
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --regression --save-baseline

# one-way latency percentiles, all transports in-process
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport all --count 200000 --size 64

# the real cross-process measurement (spawns the producer and consumer executables)
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport shared-xproc --count 500000 --size 64

# cross-process Producer → Consumer sweep: latency percentiles + throughput per size
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport shared-xproc --sizes 16,64,256,1024,4096,16384 --count 500000 --repeats 3

# speed-mismatch scenarios: a consumer paced to 10k msg/s, then a producer paced to 20k
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport shared-xproc --sizes 64 --count 50000 --repeats 3 --consumer-delay-us 100
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport shared-xproc --sizes 64 --count 50000 --repeats 3 --producer-delay-us 50
```

Transports in the BDN matrix (all in one process): **in-process SPSC ring buffer**
(`SpscRingBuffer`, managed array), **in-process SPSC shared memory** (`SharedRingBuffer`,
two views of one memory-mapped region in the same process), **concurrent queue + lock**,
`Channel<T>`, **named pipe**, **TCP loopback**. Each transport runs two dedicated threads
pumping `Batch = 65,536` messages per measured invocation. The cross-process numbers come
from a different setup: separate producer/consumer executables over a real OS-backed region.

Example run on this machine (Windows 11 VM 22621.4317, i7-1260P 2.50 GHz, 12 physical /
16 logical cores, 15.69 GB RAM, BDN 0.16 preview, .NET 11 RC1,
`IterationCount=3 WarmupCount=1`, captured after the SPARC rename and the safety-hardening
commits; the VM was shared with light background load, so **confidence intervals are wide —
treat as directional**):

| Transport | 16 B | 64 B | 256 B | 1 KB | 4 KB | Alloc/op @64 B |
|---|---:|---:|---:|---:|---:|---:|
| In-process SPSC ring buffer | 970 ns | 734 ns | 1,069 ns | 658 ns | 893 ns | 0 |
| In-process SPSC shared memory | **686 ns** | **520 ns** | **457 ns** | **241 ns** | **578 ns** | 0 |
| Concurrent queue + lock | 3,161 ns | 2,578 ns | 4,145 ns | 3,007 ns | 11,582 ns | 88 B |
| `Channel<T>` | 2,425 ns | 1,626 ns | 2,030 ns | 2,214 ns | 2,615 ns | 88 B |
| Named pipe | 40.9 µs | 30.8 µs | 30.7 µs | 37.7 µs | 35.5 µs | 0 |
| TCP loopback | 53.9 µs | 44.7 µs | 74.8 µs | 48.3 µs | 45.4 µs | 0 |

**Keep the comparison fair.** For this fixed-size SPSC workload, the in-process shared
memory ring reached ~3.1× the throughput of the tested `Channel<T>` configuration and ~5×
the lock+queue configuration at 64 B (means from the run above). `Channel<T>` and
`ConcurrentQueue` provide very different semantics — async waiting, backpressure,
cancellation, scheduling integration, MPMC configurations — so this is not a general
"SPSC shared memory is N× faster than X" claim; it is one specialized workload measured
with the same two dedicated threads, the same 65,536-message batch, equivalent
length-prefixed framing and no per-message flush (TCP runs with `NoDelay`). The
queue/channel baselines allocate a `byte[]` per message because their APIs carry reference
types; that allocation is part of their design and shows up in the `Alloc/op` column.

### Producer → Consumer (cross-process) latency and throughput

500,000 messages per size, three runs each, interleaved size by size (a full block per
size biases the last size on a shared VM); medians, throughput spread is min–max across
runs. `Message` is the payload size; the slot size is derived (rounded to a cache line) and
the region footprint is reported because it determines cache residency. Latencies in
microseconds:

| Message | Region | Throughput | Spread | Data | p50 | p90 | p95 | p99 | p99.9 | max | CPU prod/cons |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 16 B | 0.3 MiB | 1.20M msg/s | 712,250–1.34M | 18.3 MiB/s | 86.40 | 256.00 | 614.40 | 15564.80 | 353894.40 | 357103.60 | 6/13 % |
| 64 B | 0.3 MiB | 1.86M msg/s | 1.05M–1.88M | 113.2 MiB/s | 147.20 | 320.00 | 512.00 | 9830.40 | 314572.80 | 322671.20 | 9/11 % |
| 256 B | 0.3 MiB | 1.30M msg/s | 1.23M–1.64M | 316.6 MiB/s | 230.40 | 345.60 | 486.40 | 14745.60 | 340787.20 | 352744.10 | 9/20 % |
| 1 KB | 1.1 MiB | 1.69M msg/s | 1.64M–2.17M | 1,646.8 MiB/s | 230.40 | 435.20 | 716.80 | 9420.80 | 353894.40 | 367094.00 | 12/29 % |
| 4 KB | 4.1 MiB | 1.92M msg/s | 1.79M–2.04M | 7,508.9 MiB/s | 115.20 | 537.60 | 819.20 | 4710.40 | 340787.20 | 349837.30 | 18/45 % |
| 16 KB | 16.1 MiB | 280,291 msg/s | 186,319–285,317 | 4,379.6 MiB/s | 1331.20 | 4300.80 | 12288.00 | 15564.80 | 353894.40 | 359796.80 | 11/28 % |

* Throughput is the **consumer's** rate end to end. The producer reports its own
  `activeElapsed` window (first publish to end), so its rate no longer includes waiting for
  the consumer to attach. Each endpoint burns 6–45 % of one core depending on size.
* **Why 16 KB falls off (and why the old table said 140k).** Changing `--size` at a fixed
  capacity also changes the region footprint (`192 + capacity × slotSize`): 4 KB → 4 MiB,
  16 KB → 16 MiB. Measured with the fixed-footprint variant of the sweep, the data rate is
  flat across 4/8/16 KB (~5.3 GiB/s), i.e. the cost per byte is linear. Once the ring
  exceeds the CPU caches, every slot line is written and fetched through DRAM with
  write-allocate (~1.8×), and the payload verification scan is another full pass (~1.8×
  when enabled). The two losses stack, which is what made the 16 KB row look like a cliff.
  Scale the footprint with `--capacity`, drop the scan with `--no-verify-payload`, and use
  the printed `Region` column to compare like with like.
* `Alloc/msg` is 0 B after setup: BDN's `MemoryDiagnoser` shows 0 B/op for both SPSC
  transports, and the concurrency test asserts 10M writes+reads allocate < 4 KiB in total.
  The sessions are zero-copy now (see the lease API), so the consumer no longer allocates a
  `MaxPayloadSize` destination buffer.
* p99.9/max (~0.3 s) are host VM scheduling stalls that recur in every capture on this
  machine, not ring behavior. p99 ≈ 15.6 ms is the Windows timer tick that `SpinWait`
  falls back to when the consumer catches up and the buffer goes empty; `--spin-only` and
  `--notify` remove it.
* Latencies come from a 16-sub-bucket log histogram, so percentiles are approximate by at
  most 1/16 of the value.

### Producer vs consumer speed mismatch (64 B)

Same harness with one endpoint paced (`--delay-us`); rates in msg/s, latencies in µs:

| Scenario | Producer | Consumer | p50 | p99 | CPU prod/cons |
|---|---:|---:|---:|---:|---:|
| Producer ≈ consumer (both paced to 10k) | 9,541 | 10,001 | 91750.40 | 327680.00 | 1 % / 34 % |
| Producer >> consumer (`--consumer-delay-us 100`) | 9,508 | 10,001 | 91750.40 | 393216.00 | 0 % / 29 % |
| Producer << consumer (`--producer-delay-us 50`) | 20,000 | 23,167 | 5120.00 | 314572.80 | 5 % / 1 % |

* **Producer >> consumer**: the buffer stays full; each message queues behind up to
  `capacity` messages, so p50 ≈ capacity × consumer period = 1024 × 100 µs ≈ 102 ms and
  throughput equals the consumer's rate. The consumer burns ~30 % of a core; the blocked
  producer burns none.
* **Producer << consumer**: the buffer stays empty and latency is the consumer's
  *idle-detection* latency — 5.1 ms p50 — because `SpinWait` yields and then sleeps rather
  than busy-spinning. Both alternatives were measured on the same scenario: `--spin-only`
  cuts p50 to 3.9 µs at the cost of a busy core while the peer is idle, and `--notify`
  (Windows) blocks on an OS latch for a p50 of 7.6 µs at ~2 % consumer CPU. This is the
  number that matters most for telemetry consumers.
* **Producer ≈ consumer**: throughput tracks the slower side, and latency sits at the queue
  operating point (the startup backlog fills the 1024-slot buffer before both ends settle).

Caveats worth knowing before quoting any of this:

* The host is a VM whose cross-core coherence transfer measured **~1.5 µs per handoff**
  (a two-thread ping-pong test managed only ~640k round trips/s). On bare metal the SPSC
  numbers are typically 10–50× higher.
* `MemoryDiagnoser` attributes process-wide allocations, so the per-message `byte[]`
  allocations of the queue/channel baselines do show up (the SPSC transports allocate
  nothing after setup).
* BenchmarkDotNet 0.15.8 cannot identify the .NET 11 RC runtime; the project uses
  `BenchmarkDotNet 0.16.0-preview.2`.
* Crash/restart and zero-consumer behavior are covered by tests rather than benchmark
  numbers: `Sparc.ProcessTests` uses killed producer/consumer processes, `--require-existing`
  timeouts, role conflicts and geometry mismatches.

---

## 7. Scope boundary

Not implemented (deliberately): MPSC/MPMC, dynamic resizing, variable-sized records,
persistence, networking, compression, encryption, multiple consumers/producers, heartbeats
or automatic crash detection.

The learning objective is the one this project exercises end to end:

> **Two independent processes can safely exchange data through the same memory using only
> atomic state transitions and memory-ordering guarantees.**

Concepts covered: cache coherence, false sharing, acquire/release ordering, lock-free
algorithms, memory mapping, process isolation, binary memory layouts, crash semantics,
OS abstraction behind interfaces, and reusable session design for multiple host types.
