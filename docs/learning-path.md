# Learning path

A guided route through SPARC for a new contributor or a curious user. Budget two
to three focused days; each session is one to three hours. Read a doc section,
then the test that proves it, then the code — in that order.

## The mental model

Five sentences; everything else in the repository is detail around them.

1. Two processes map the same physical pages; the ring is a fixed array of
   equal-size slots in that mapping.
2. `head` and `tail` are 64-bit **sequence numbers**, not indices: slot is
   `sequence & (capacity - 1)`, full is `tail - head == capacity`, empty is
   `head == tail`.
3. Each counter has exactly **one writer** (producer owns `tail`, consumer owns
   `head`), so advancing a counter needs a release store, never CAS or a lock.
4. **Publish-then-advance**: write the slot, then release-store the cursor; the
   peer's acquire load makes the bytes visible. That is the entire
   synchronization protocol.
5. Everything above (endpoints, leases, sessions, channels, streaming, hosting)
   is ergonomics over those sentences; everything below (transports) is just
   "give me a `byte*` into mapped memory".

Security in one sentence: **region access** is a transport concern decided at
creation time — a Windows section DACL, or an unnamed section whose HANDLE is
the only capability to join. SPARC never inspects it and never authenticates
messages; see [SECURITY.md](../SECURITY.md).

## How to study

| Source | What it is | When it is authoritative |
|---|---|---|
| `docs/*.md` | the *why* and the guarantees | intent; can lag |
| `src/**` | the *what actually runs* | behavior |
| `tests/**` | executable spec; every guarantee has a test | when docs and code disagree, read the test |

## The stack

```
host (CLI / web app / worker / sample)
  ProducerSession / ConsumerSession            Sparc.Client
  SparcChannel<T> / SfCodec<T> / SfStreamCodec<T>   Sparc.Channels, Sparc.Serialization
  IProducerEndpoint / IConsumerEndpoint        Sparc.Core
  SharedRingBuffer / SpscRingBuffer            Sparc.Core
  RingBufferRegion / RingBufferHeader / SlotFraming / RingBufferLayout
  IIpcMemoryRegionFactory                      Sparc.Abstractions
    Windows named map | Unix file-backed | In-memory pinned array
    access control: DACL (named) | unnamed section + HANDLE (capability)
```

---

## Session 0 — Prerequisites

If the words *cache line*, *store buffer*, *acquire/release* and *memory-mapped
file* are new, skim the relevant parts of a systems textbook first. Then read
[concept.md](concept.md) and [how-it-works.md](how-it-works.md) section 5.

**Checkpoint:** why is `Volatile` not the same as `Interlocked`?

## Session 1 — The ring algorithm (the heart)

* **Read:** [how-it-works.md](how-it-works.md) sections 1 and 4, then
  `src/Sparc.Core/SpscRingBuffer.cs` end to end.
* **Then:** `src/Sparc.Core/SharedRingBuffer.cs` — the same algorithm over raw
  memory, deliberately duplicated for the hot path (the class docs explain why).
* **Tests:** `tests/Sparc.UnitTests/SpscRingBufferTests.cs`
  (`RandomOperationsMatchReferenceQueue` is the model-based spec),
  `tests/Sparc.ConcurrencyTests/ConcurrencyTests.cs`.
* **Run:** `dotnet test Sparc.slnx -c Release` once, so the suite is green
  before you start poking at it.

Common confusion: the cached peer cursor (`_cachedHead`, `_cachedTail`) is
*intentionally stale* and refreshed only when it claims full or empty.

**Checkpoint:** why is there no CAS on the hot path, and where is there a CAS?

## Session 2 — The binary protocol

* **Read:** `RingBufferLayout.cs` (offsets, magic, geometry rules),
  `RingBufferHeader.cs` (validation), `SlotFraming.cs`
  (`[length][type][payload]`), `RingBufferRegion.cs` (create/join handshake:
  write every field, publish the magic last).
* **Tests:** `ProtocolGoldenTests.cs` (locks the wire format),
  `RingBufferLayoutTests.cs`, `RingBufferRegionTests.cs`.

This is the cross-process, cross-version ABI; the PublicAPI files and golden
tests are how it is frozen.

**Checkpoint:** why is the magic published last, and why is that enough?

## Session 3 — The OS seam, transports and access control

### 3a. Transports and lifecycle

* **Read:** `src/Sparc.Abstractions/*` (`IIpcMemoryRegionFactory`,
  `IIpcMemoryRegion.Pointer`, `IpcRegionOptions`), then
  `InMemoryMemoryRegionFactory`, `UnixFileMemoryMappedRegionFactory`,
  `WindowsNamedMemoryMappedRegionFactory`.
* Compare lifecycles: Windows named maps die with the last handle; Unix files
  persist (`TryReset` unlinks; 0700 directory, 0600 files); in-memory arrays are
  pinned for the process.
* **Read:** [SECURITY.md](../SECURITY.md) for the name/permission threat model.

**Checkpoint:** which transports are process-local, ephemeral and persistent,
and what does each do about a stale region?

### 3b. Access control and capability regions (Windows)

* **Read:** `IpcMemoryRegionSecurity.cs` in Abstractions, then
  `WindowsSectionSecurity.cs` (`CurrentUserOnly`, `ForSids`,
  `ForAccountNames`, `FromSddl`), `WindowsUnnamedSection.cs`,
  `WindowsSectionCapability.cs`, `WindowsSectionApi.cs`.
* The rules: security is applied **only when this process creates** the region;
  joining is governed by what the creator applied; a non-null security object on
  a transport that cannot interpret it fails establishment instead of being
  silently dropped. It is transport access control, never message
  authentication.
* Two Windows shapes: a named section with a restrictive DACL, or an **unnamed
  section** with no OS-visible name at all — the HANDLE is the capability,
  transferred by inheritance or `DuplicateHandle`.
* The join side of capability mode: `WindowsUnnamedSection.MapHandle(handle)`
  plus the `SparcRing.OpenProducer(region, …)` / `OpenConsumer(region, …)`
  adoption overloads (the endpoint takes ownership of the mapped region).
* Tools wiring: `--security current-user`, `--section-handle <h>` (`-` reads the
  handle value from stdin; `--notify` is rejected in this mode).
* **Tests:** `WindowsSectionSecurityTests.cs` (DACL construction and parsing,
  and the end-to-end "identity outside the DACL cannot open or join");

**Checkpoint:** why does an unnamed section eliminate name squatting, and what
must still be protected for the capability to stay a capability?

## Session 4 — Endpoints, roles and leases

* **Read:** `SparcRing.cs`, `IEndpoint.cs`, `IProducerEndpoint.cs`,
  `IConsumerEndpoint.cs`, `ReadLease.cs`, `WriteLease.cs`.
* Two ways to attach: by name through a factory
  (`OpenProducer(factory, name, …)`), or by adopting an already-mapped region
  (`OpenProducer(region, capacity, slotSize, …)`) as in the Windows capability
  path from Session 3b.
* Three write styles: copy (`TryPublish`), sized reservation
  (`TryReserveWrite(type, length, …)`), full-window reservation
  (`TryReserveWrite(type, out span)`) plus `CommitWrite(length)`.
* Two read styles: copy (`TryRead`) and lease (`TryPeek` / `TryBeginRead`).
* **Tests:** `EndpointApiTests.cs`, `SharedRingBufferTests.cs`.

The full-window reservation is the primitive that makes chunked streaming
possible; learn it here, before any streaming code. Only one lease may be
active per endpoint — that constraint explains later design choices.

**Checkpoint:** when do you use each of the three write styles?

## Session 5 — Sessions and wait modes

* **Read:** `ProducerSession.cs`, `ConsumerSession.cs`, `RingBufferMessage.cs`
  (`[sequence][timestamp][fill]`), `LatencyHistogram.cs`,
  `SessionWaitMode.cs`.
* Wait modes: `SpinThenSleep`, `SpinOnly`, `Notification`; then
  `ISessionSignal`, `InProcessSessionSignal`, `NamedSessionSignal`,
  `PosixSemaphore` (P/Invoke `sem_timedwait`).
* **Tests:** `SessionTests.cs`, `SessionGeneralizationTests.cs`,
  `NamedSignalTests.cs`.

**Checkpoint:** what does a hard-killed process leave behind, and what ends the
peer's wait?

## Session 6 — The high-level layers (pick by use case)

The layer stack has four rungs; teach them in this order:

1. **Endpoints** — raw bytes, synchronous, one slot.
2. **Channels** (`Sparc.Channels`) — typed, async, one slot.
   Read: [channels.md](channels.md), `ISparcCodec.cs`,
   `SparcChannelWriter.cs`, `SparcChannelReader.cs`; tests `ChannelTests.cs`.
3. **Codecs** (`Sparc.Serialization`) — typed, synchronous, one slot.
   Read: `SfCodec.cs`; test `SerializerFoundationCodecTests.cs`.
4. **Chunked streaming** — typed, any size, synchronous.
   Read in this order, test pair in brackets:
   `ChunkFraming.cs` -> `SparcStreamWriteBuffer.cs` ->
   `SparcStreamWriter.cs` [`StreamMessageTests.cs`] ->
   `SparcStreamReadBuffer.cs` -> `SparcStreamReader.cs`
   [`StreamReadBufferTests.cs`] -> `SfStreamCodec.cs`
   [`SfStreamCodecTests.cs`] -> `ChunkStreamFuzzTests.cs`.

The two genuinely hard parts of streaming: the read buffer's scratch stitching
with a single active lease, and the resynchronization rules (skip non-first
chunks; First mid-message is truncation; `Abort` is corruption; an empty
non-last chunk is corruption so loops cannot spin).

Read [streaming.md](streaming.md) before the code. The write side is a
SerializerFoundation formatter generic over `IWriteBuffer`; the read side is
deliberately not an `IReadBuffer` because `BytesRemaining` is unknowable for a
live stream and only one lease can be active.

**Checkpoint:** why does streaming trade at-least-once for at-most-once, and
what does the `Abort` flag buy?

## Session 7 — Hosting, testing harness, analyzer

* **Read:** [hosting.md](hosting.md), `ServiceCollectionExtensions.cs`, the
  worker/session services, `SparcChannelHealthCheck.cs`, `SparcMetrics.cs`;
  tests `HostingTests.cs`, `DependencyInjectionTests.cs`.
* **Read:** [testing.md](testing.md), `TestSparcRing.cs`,
  `TestSparcChannel.cs`, `ChannelTestExtensions.cs`;
  test `TestingPackageTests.cs`.
* **Read:** `NotificationModeAnalyzer.cs`; test `AnalyzerTests.cs`.

**Checkpoint:** what wins when both a custom factory and `AddSparcIpc()` are
registered, and why is that the right default for tests?

## Session 8 — Performance

* **Read:** [performance-invariants.md](performance-invariants.md) like a book;
  each item is rule -> mechanism -> payoff -> what breaks.
* **Run and study:**

```powershell
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --filter *
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport shared-xproc --count 500000 --size 64
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --regression
```

Learn the traps in [benchmarking.md](benchmarking.md): payload is not
footprint, percentiles are approximate, the p99 floor is the timer tick, the
regression gate is best-of-5 with a 50% tolerance.

**Checkpoint:** why can a payload sweep at fixed capacity look like a cliff?

## Session 9 — Verification and repo engineering

* Four test kinds and what each can catch: `Sparc.UnitTests` (logic),
  `Sparc.ConcurrencyTests` (two threads, 10M messages), `Sparc.ProcessTests`
  (real kills and exit codes), `Sparc.FuzzTests` (hostile bytes, chunk
  reassembly, corrupted peers).
* Packaging engineering: `Directory.Build.props/targets`, PublicAPI
  `Shipped`/`Unshipped` files, `InternalsVisibleTo`, `.github/workflows/ci.yml`
  and `release.yml`, warnings-as-errors analyzers.
* [SECURITY.md](../SECURITY.md), [CONTRIBUTING.md](../CONTRIBUTING.md), and
  `git log` (the history is a readable design log).

**Checkpoint:** which test kind would catch a lost wakeup, a torn slot, a
name-format regression, and a hostile length field?

---

## Capstone: stream a large value

Run the worked example, then shrink its ring and run it again:

```powershell
dotnet run -c Release --project samples/Sparc.Serialization.Sample
```

Then edit `Program.cs` in that sample to `capacity: 4, slotSize: 64` and watch
the 1.2 MB order still flow. That one experiment teaches the streaming layer
better than any paragraph.

## Hands-on experiments

1. **Trace a message.** Two-terminal CLI demo from the top-level README; set
   breakpoints in `TryAcquireWriteSlot`/`TryAcquireReadSlot` and watch the
   cursors move.
2. **Break it deliberately.** Replace `Volatile.Write(ref _tail.Value, …)` with
   a plain write in `SpscRingBuffer` and run `ConcurrencyTests`. On x86 it may
   still pass — explain why, then read how-it-works section 5. Revert.
3. **Violate geometry.** Open a region with capacity `1000` and follow the
   exception through `ValidateGeometry`.
4. **Stream something big.** The capstone above.
5. **Reproduce a fuzz failure.** Run `Sparc.FuzzTests`; when CsCheck reports a
   seed, replay it with
   `$env:CsCheck_Seed="<seed>"; dotnet test tests/Sparc.FuzzTests/Sparc.FuzzTests.csproj -c Release`.
6. **Measure.** Run `--regression`, then `--save-baseline`, then `--regression`
   again; understand best-of-5 and the tolerance gate.
7. **Share without a name.** Create an unnamed section with
   `WindowsUnnamedSection.Create`, pass the HANDLE to a child process (or use
   the CLIs' `--section-handle -` mode) and adopt it with
   `SparcRing.OpenProducer(region, …)`; then try to open the same region by name
   and observe that there is nothing to open. `WindowsSectionSecurityTests` and
   `tests/Sparc.ProcessTests` are the worked examples.

## Checkpoints (all fifteen)

1. Why is there no CAS on the hot path, and where is there a CAS?
2. What does the release store of `tail` guarantee, and why is `Volatile`
   required even on x86-64?
3. What breaks if the cached peer cursor were too new instead of too stale?
4. What are the four region-header areas, and why is the magic published last?
5. How does at-least-once redelivery across a consumer crash fall out of
   publish-then-advance?
6. When do you use `TryPublish`, sized `TryReserveWrite`, or full-window
   `TryReserveWrite` plus `CommitWrite(length)`?
7. Why can `SparcStreamReadBuffer` not implement SerializerFoundation's
   `IReadBuffer`?
8. Which transports are process-local, ephemeral and persistent, and what does
   each do on a stale region?
9. Why is footprint a separate benchmark variable from payload size?
10. Which test kind would catch a lost wakeup, a torn slot, a name-format
    regression, and a hostile length field?
11. Where does chunk framing sit relative to slot framing, and why did it not
    bump the layout version?
12. Why can `SfStreamCodec` be delegate-based when SerializerFoundation ships
    no formatter interface?
13. What are the two hard blockers for an `IReadBuffer` over a live stream?
14. Why does streaming trade at-least-once for at-most-once, and what does
    `Abort` buy?
15. What bounds the reader's resync and stitch loops, and which validation
    makes that bound true?
16. What does `IpcRegionOptions.Security` protect, and what does it explicitly
    not protect?
17. When is security applied — create or join — and what happens when the
    selected transport cannot interpret it?
18. Why does an unnamed section remove name squatting, and what must stay
    secret for capability mode to remain a capability?

## Reading map

| Topic | Docs | Code | Tests |
|---|---|---|---|
| Algorithm | concept, how-it-works 1/4 | `SpscRingBuffer`, `SharedRingBuffer` | `SpscRingBufferTests`, `ConcurrencyTests` |
| Protocol/layout | how-it-works 2/3 | `RingBufferLayout`, `RingBufferHeader`, `SlotFraming` | `ProtocolGoldenTests`, `RingBufferLayoutTests` |
| Region lifecycle | how-it-works 2 | `RingBufferRegion`, `SparcRing` | `RingBufferRegionTests` |
| Transports/security | SECURITY, how-it-works 8 | `Sparc.*MemoryMapped`, `Sparc.InMemory` | transport factory tests |
| Region access control | SECURITY, concept | `IpcMemoryRegionSecurity`, `WindowsSectionSecurity`, `WindowsUnnamedSection`, `WindowsSectionCapability` | `WindowsSectionSecurityTests`, `ProcessTests` |
| Endpoints/leases | how-it-works 6 | `IProducerEndpoint`, `IConsumerEndpoint`, leases | `EndpointApiTests`, `SharedRingBufferTests` |
| Sessions/wait modes | how-it-works 7 | `Sparc.Client` | `SessionTests`, `NamedSignalTests` |
| Channels | channels | `Sparc.Channels` | `ChannelTests` |
| Serialization/streaming | streaming | `Sparc.Serialization` | `StreamMessageTests`, `StreamReadBufferTests`, `SfStreamCodecTests`, `ChunkStreamFuzzTests` |
| Hosting | hosting | `Sparc.Hosting` | `HostingTests`, `DependencyInjectionTests` |
| Testing harness | testing | `Sparc.Testing` | `TestingPackageTests` |
| Analyzer | channels (note) | `Sparc.Analyzers` | `AnalyzerTests` |
| Performance | performance-invariants | hot paths | `ConcurrencyTests` allocation assertion |
| Benchmarking | benchmarking | `benchmarks/Sparc.Benchmarks` | `--regression` |

## If you only have an hour

Read [concept.md](concept.md), the README sections 1 and 2, then
`SpscRingBuffer.cs`, then run the two-terminal CLI demo. That covers the idea;
the rest is layers.
