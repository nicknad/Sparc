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

## Repository layout

```
Sparc.slnx
├── src/
│   ├── Sparc.Abstractions/           IIpcMemoryRegionFactory / IIpcMemoryRegion, options, exceptions
│   ├── Sparc.WindowsMemoryMapped/    Windows named memory-mapped implementation + DI registration
│   ├── Sparc.Core/            ring protocol: layout/header/framing, SpscRingBuffer, SharedRingBuffer
│   ├── Sparc.Client/          ProducerSession/ConsumerSession, message protocol, latency histogram
│   ├── Sparc.Producer/        producer CLI (args → session → summary → exit code)
│   └── Sparc.Consumer/        consumer CLI (args → session → summary → exit code)
├── samples/
│   └── Sparc.WebApp/          minimal API + BackgroundService hosting both session roles
├── tests/
│   ├── Sparc.UnitTests/       algorithm, region protocol, sessions (in-memory region factory)
│   ├── Sparc.ConcurrencyTests/ 2 × 10,000,000 message two-thread verification
│   └── Sparc.ProcessTests/    two real processes: lifecycle, conflicts, kill tests
└── benchmarks/
    └── Sparc.Benchmarks/      BenchmarkDotNet throughput suite + custom latency harness
```

Dependency graph (arrows = project reference):

```
Sparc.Abstractions
      ▲
Sparc.Core
      ▲            ▲
Sparc.Client  Sparc.WindowsMemoryMapped
      ▲                   ▲
Sparc.Producer / Sparc.Consumer / samples/Sparc.WebApp (hosts)
```

`Sparc.WindowsMemoryMapped` does not reference `Sparc.Core`; the ring protocol
does not reference any OS type.

---

## Quick start (CLI)

```powershell
dotnet build Sparc.slnx -c Release
dotnet test  Sparc.slnx -c Release

# terminal 1
dotnet run -c Release --project src/Sparc.Consumer -- --name demo --count 1000000

# terminal 2
dotnet run -c Release --project src/Sparc.Producer  -- --name demo --count 1000000 --size 64
```

Builds treat warnings as errors (`TreatWarningsAsErrors` in `Directory.Build.props`).
A GitHub Actions workflow (`.github/workflows/ci.yml`) is ready to run build + test on
`windows-latest` once a git remote is configured.

The consumer may also be started first (it creates the region; the producer joins).
Example output:

```
ready: role=consumer name=demo capacity=1024 slotSize=256 maxPayload=248
consumed=1000000 bytes=64000000 elapsed=0.61s throughput=1639344 msg/s dataThroughput=100.0 MiB/s
producer=Stopped consumer=Running
latency(us): min=1.20 mean=122.44 p50=51.20 p95=204.80 p99=409.60 max=20480.00 (n=1000000)
```

### CLI

`producer --name <name> [--count n] [--size bytes] [--capacity slots] [--slot-size bytes]`
`[--type int] [--open-timeout ms] [--full-timeout ms] [--takeover] [--recreate-stale] [--require-existing] [--quiet]`

`consumer --name <name> [--count n] [--capacity slots] [--slot-size bytes] [--type int]`
`[--open-timeout ms] [--idle-timeout ms] [--takeover] [--recreate-stale] [--require-existing] [--no-verify] [--quiet]`

`--count 0` on the consumer means "consume until the producer stops".
`--size` is the payload size; layout is `[sequence:int64][timestamp:int64][fill…]` (min 16 bytes).
`Ctrl+C` cancels the session gracefully.

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
builder.Services.AddWindowsNamedMemoryMappedIpc();

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

Sessions are intentionally **synchronous and blocking** (they spin on the lock-free
buffer); hosts should invoke them on a background thread (`Task.Run`,
`BackgroundService`). Cancellation is checked on the full/empty paths and
periodically on the hot path; `SharedRingBuffer.Connect` and `SpscRingBuffer.Write`
also accept a `CancellationToken` that bounds their spin loops, and the sessions
report cancellation during role claiming as `SessionStopReason.Cancelled` rather
than throwing.

Structured outcomes replace console/exit-code decisions:

```csharp
ProducerRunResult  { Produced, PayloadSize, Elapsed, Reason, Unsent, PeerState, FailureMessage }
ConsumerRunResult  { Received, ReceivedBytes, Elapsed, Reason, Latency, FailureMessage }
SessionStopReason  { Completed, PeerStopped, Timeout, VerificationFailed, Cancelled }
```

Testability: pass a custom `TimeProvider` and/or `ILogger` into the sessions, and
swap `IIpcMemoryRegionFactory` for the in-memory test double to run the whole
stack without the OS (see `tests/Sparc.UnitTests/Support`).

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
40      24    Reserved         zero
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
* Benchmarks intentionally round slot sizes up to a cache line: tightly packed small slots
  make the producer's writes and the consumer's reads **false-share** and can slow a buffer
  down several-fold. This is visible in the project's own perf experiments.

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
`PlatformNotSupportedException` on Unix). `WindowsNamedMemoryMappedRegionFactory`
checks this and fails with a clear `IpcPlatformNotSupportedException`. A Unix
implementation would provide a file-backed `IIpcMemoryRegionFactory`
(`MemoryMappedFile.CreateFromFile` + unlink on `TryReset`); nothing above the
abstraction changes.

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
| Stale/half-initialized region | Bad magic after `--open-timeout` → exit 6, unless `--recreate-stale` is given: drop handles, reclaim, create again. On Windows a named map dies with its last handle, so a dead creator leaves nothing behind. |
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
  takeover, endpoint states, histogram math, DI registration, cancellation while a
  blocking write is full, and session semantics (completion, timeouts, peer-stopped
  drain, verification failure, cancellation). Region protocol tests run against an
  in-memory `IIpcMemoryRegionFactory`, so they are OS-independent; Windows factory
  tests are guarded by `OperatingSystem.IsWindows()`.
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
# throughput matrix (BenchmarkDotNet; 6 transports × 5 message sizes)
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --filter *

# one-way latency percentiles, all transports in-process
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport all --count 200000 --size 64

# the real cross-process measurement (spawns the producer and consumer executables)
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport shared-xproc --count 500000 --size 64
```

Transports compared: `SpscRingBuffer` (managed array), `SharedRingBuffer`
(`MemoryMappedFile`), `lock + Queue<byte[]>`, bounded `Channel<byte[]>`,
`NamedPipeServerStream/ClientStream`, `TcpListener/TcpClient` loopback. Each transport
runs two dedicated threads pumping `Batch = 65,536` messages per measured invocation.

Example run on this machine (Windows 11 VM, i7-1260P, BDN 0.16 preview, .NET 11 RC1,
`IterationCount=3 WarmupCount=1`; captured before the SPARC rename and the
safety-hardening commits, which do not touch the measured per-message paths; **wide
confidence intervals — treat as directional**):

| Transport | 16 B | 64 B | 256 B | 1 KB | 4 KB | Alloc/op @64 B |
|---|---:|---:|---:|---:|---:|---:|
| SPSC array | 1,068 ns | 1,660 ns | 938 ns | 1,027 ns | 648 ns | 0 |
| SPSC shared memory | **375 ns** | **735 ns** | **451 ns** | **783 ns** | **252 ns** | 0 |
| lock + Queue | 664 ns | 1,062 ns | 1,475 ns | 4,673 ns | 9,152 ns | 88 B |
| `Channel<T>` | 946 ns | 1,351 ns | 698 ns | 1,227 ns | 605 ns | 88 B |
| Named pipe | 16.4 µs | 14.6 µs | 13.7 µs | 14.1 µs | 17.5 µs | 0 |
| TCP loopback | 27.7 µs | 28.9 µs | 25.5 µs | 26.6 µs | 27.2 µs | 0 |

Cross-process latency (producer + consumer executables, 500k × 64 B):

```
consumed=500000 elapsed=0.598s throughput=836736 msg/s dataThroughput=51.1 MiB/s
latency(us): min=7.70 mean=1331.58 p50=204.80 p95=409.60 p99=13107.20 max=337048.50
```

Caveats worth knowing before quoting any of this:

* The host is a VM whose cross-core coherence transfer measured **~1.5 µs per handoff**
  (a two-thread ping-pong test managed only ~640k round trips/s). On bare metal the SPSC
  numbers are typically 10–50× higher.
* Latency measured while the pipeline is saturated includes queueing behind up to
  `capacity` messages; run with a smaller `--count`/buffer or a throttled producer for
  unloaded latency.
* `MemoryDiagnoser` attributes process-wide allocations, so the per-message `byte[]`
  allocations of the queue/channel baselines do show up (the SPSC transports allocate
  nothing after setup).
* BenchmarkDotNet 0.15.8 cannot identify the .NET 11 RC runtime; the project uses
  `BenchmarkDotNet 0.16.0-preview.2`.

---

## 7. Scope boundary

Not implemented (deliberately): MPSC/MPMC, dynamic resizing, variable-sized records,
persistence, networking, compression, encryption, multiple consumers/producers, heartbeats
or automatic crash detection, and Unix file-backed regions (only the abstraction and the
Windows implementation exist today; an in-memory factory ships in the test project).

The learning objective is the one this project exercises end to end:

> **Two independent processes can safely exchange data through the same memory using only
> atomic state transitions and memory-ordering guarantees.**

Concepts covered: cache coherence, false sharing, acquire/release ordering, lock-free
algorithms, memory mapping, process isolation, binary memory layouts, crash semantics,
OS abstraction behind interfaces, and reusable session design for multiple host types.
