# How it works

This document follows one message from `TryReserveWrite` to `AdvanceRead`, then
explains the surrounding machinery: region setup, memory ordering, sessions,
wait modes, platform implementations, and crash semantics.

## 1. Layers

```
host (CLI / BackgroundService / sample)
  |  ProducerSession / ConsumerSession          Sparc.Client
  |  SparcChannel<T> / SfCodec / chunk streams  Sparc.Channels / Sparc.Serialization (optional)
  |  IEndpoint / IProducerEndpoint / IConsumerEndpoint: role, states, waiting flags
  |  SparcRing / SharedRingBuffer / SpscRingBuffer          Sparc.Core
  |  RingBufferRegion (handshake, header, validation)
  |  SlotFraming / RingBufferLayout (binary protocol)
  |  IIpcMemoryRegionFactory / IIpcMemoryRegion Sparc.Abstractions
  |
  +-- WindowsNamedMemoryMappedRegionFactory     Sparc.WindowsMemoryMapped
  +-- UnixFileMemoryMappedRegionFactory         Sparc.UnixMemoryMapped
  +-- InMemoryMemoryRegionFactory               Sparc.InMemory
```

`Sparc.Core` references no OS type; the transport implementations reference no
ring code. The seam is `IIpcMemoryRegionFactory` (create/open a region by name
and size, `IsSupported`, `TryReset`).

## 2. Region lifecycle

`SparcRing.OpenProducer` / `SparcRing.OpenConsumer` (factory, name, capacity,
slotSize, options) delegate to `RingBufferRegion.CreateOrOpen`:

1. Ask the factory for the region. The first caller creates it; others join.
   On Windows creation is a direct `CreateFileMapping` (so an optional
   `WindowsSectionSecurity` DACL can be attached) followed by `MapViewOfFile`,
   and joining is `MemoryMappedFile.OpenExisting`; on Unix a file plus
   `CreateFromFile`; in memory a pinned `byte[]`. A region can also be adopted
   from an already-mapped `IIpcMemoryRegion` (unnamed-section HANDLE transfer)
   through the `SparcRing.OpenProducer`/`OpenConsumer` overloads.
2. **If this process is the creator**, `Initialize` writes every header field
   *except the magic*, then publishes the magic with a release store:
   `Volatile.Write(ref *(ulong*)(pointer + MagicOffset), Magic)`. An opener that
   observes the magic is guaranteed to observe the geometry and the zeroed
   cursors behind it.
3. **If this process is a joiner**, `OpenAndValidate` polls the magic with an
   acquire load (2 ms sleeps, bounded by `OpenTimeout`), then reads and
   validates the header: version, header size, geometry consistency, declared
   region size. Specific failures map to specific exceptions
   (`RingBufferVersionMismatchException`, `RingBufferGeometryMismatchException`,
   `RingBufferCorruptedException`).
4. **Stale recovery** (opt-in `RecreateIfStale`): a bad magic after the timeout
   means the creator died during startup. The region is dropped, the factory is
   asked to reclaim the backing store (`TryReset`; a no-op on Windows where the
   last handle owns the mapping), and creation is retried once.

Joining with `AdoptExistingGeometry: true` (the consumer CLI) skips the
requested-geometry check and adopts whatever the creator fixed. Without it, a
geometry mismatch is an error: the creator wins.

## 3. Binary layout

The region is one contiguous block; the header is 192 bytes so slots start on a
cache-line boundary.

```
Offset  Size  Field
0       8     Magic            0x474E_4952_4353_5053 ("SPSCRING" little-endian)
8       4     Version          1
12      4     Capacity         slots (power of two)
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
192     ...   Slots            Capacity x SlotSize
```

Each slot:

```
0   4   Length   payload bytes (little-endian, <= SlotSize - 8)
4   4   Type     caller-defined tag (little-endian)
8   ... Payload
```

The header is written field-by-field at fixed offsets with
`BinaryPrimitives` - never by blitting a struct - so layout is stable across
compilers, JITs, and platforms. `head` and `tail` never share a cache line.
Waiting flags use the reserved area, so the layout version stays 1.

## 4. The ring algorithm

`head` and `tail` are monotonically increasing 64-bit sequence numbers, not
indices. Physical slot for sequence `s` is `s & (Capacity - 1)`.

```
Producer TryReserveWrite(type, length):        Consumer TryPeek():
  tail = self.tail (own)                         head = self.head (own)
  head = cached peer cursor                      tail = cached peer cursor
  if tail - head >= Capacity:                    if tail == head:
      refresh head (acquire)                         refresh tail (acquire)
      if still full -> false                         if still empty -> false
  payload = slot(tail).payload                   read frame, bounds-check length
  (caller writes)                                payload = slot(head).payload
  CommitWrite: write frame; release tail+1       (caller reads/verifies)
                                                 AdvanceRead: release head+1
```

* **No CAS, no lock.** Each cursor has exactly one writer, so an ordinary
  ordered store is enough. The producer is wait-free while the buffer has space;
  the consumer while it has data.
* **Full/empty from the difference.** No separate count field, no shared counter
  to update.
* **Cached peer cursor.** A peer that writes its counter on every message forces
  a cache-line transfer on every read. Each side keeps a private, possibly stale
  copy of the peer's cursor and only re-reads the shared value when the stale
  copy says *full* (producer) or *empty* (consumer). The cached value can never
  be newer than the real one, so the worst case is a redundant refresh. The
  project README reports 1.3M -> 13M messages/s from this optimization on the
  test VM.
* **Cache seeding.** The caches are initialized from the live region in the
  constructor. That matters when attaching to a region with non-zero cursors: a
  zero-initialized consumer cache would read an unpublished slot
  (`FreshConsumerAttachedToDrainedNonZeroRegionReportsEmpty` is the regression
  test).

## 5. Memory ordering

Correctness rests on two release/acquire pairs:

```
PRODUCER                                     CONSUMER
write frame + payload                        acquire load tail
release store tail  -----------------------> read frame + payload
                                             release store head
acquire load head  <----------------------- (slot is now reusable)
```

* `tail` release/acquire makes the slot bytes visible before the consumer can
  observe the new `tail`.
* `head` release/acquire guarantees the producer never overwrites a slot the
  consumer is still reading, and never reuses a slot before the copy/scan is
  complete.

`Volatile.Read`/`Volatile.Write` are required even on x86-64: the hardware's TSO
model would be sufficient for these particular accesses, but the C# memory model
lets the compiler and JIT reorder non-volatile accesses (for example hoisting a
peer cursor read out of a loop). On ARM64 `Volatile` emits real `ldar`/`stlr`.

Cross-process visibility needs no `Flush()`: both processes map the same
physical pages, and the hardware cache-coherence protocol makes a store
eventually visible; the release/acquire pair provides the ordering. Two extra
rules make 64-bit atomics work:

* `head`/`tail` are aligned 64-bit fields (offsets 64 and 128, each on its own
  cache line). Aligned 64-bit loads/stores are atomic on x86-64/ARM64; 32-bit
  processes are not supported for cross-process use.
* The header is fixed little-endian offsets, never a blitted struct.

## 6. The zero-copy lease API

The role-typed endpoints expose both copy and lease operations:

| Producer (`IProducerEndpoint`) | Consumer (`IConsumerEndpoint`) |
|---|---|
| `TryPublish(type, payload)` copies in; `Publish` blocks | `TryRead(destination, ...)` copies out; `Read` blocks |
| `TryReserveWrite(type, length, out Span<byte> payload)` reserves a known size | `TryPeek(out ReadOnlySpan<byte> payload, ...)` views the oldest message |
| `TryReserveWrite(type, out Span<byte> payload)` reserves the full payload window | `TryBeginRead(out ReadLease lease)` views it with scope-based release |
| `CommitWrite()` / `CommitWrite(length)` publishes | `AdvanceRead()` releases the slot |
| `TryBeginWrite(...)` / `WriteLease` releases on dispose | |
| `AbandonWrite()` discards the reservation | |

Properties:

* The payload view points into the slot itself (managed array or mapped memory);
  writing/reading it is a direct memory access, no intermediate buffer.
* At most one reservation/view may be active per endpoint; invalid transitions
  throw `InvalidOperationException`. This is safe because the endpoint contract
  is single-threaded.
* `CommitWrite` writes the 8-byte frame, then release-stores `tail`. A crash
  before the store leaves the slot unpublished.
* `AdvanceRead` release-stores `head` only after the caller is done with the
  view, so the producer cannot overwrite it in the meantime.
* The full-window reservation plus `CommitWrite(length)` is what makes chunked
  streaming possible: a serializer fills the window and publishes only the bytes
  it used (`Sparc.Serialization`, [streaming.md](streaming.md)).
* The sessions use the lease path: `ProducerSession` stamps
  `[sequence][timestamp]` and the fill bytes into the reserved slot;
  `ConsumerSession` verifies and samples latency in place.

## 7. Sessions

`ProducerSession` and `ConsumerSession` turn the buffer into a reusable endpoint
with structured outcomes.

* **Role claiming.** `Connect(role)` CASes the advisory state word
  `NotPresent -> Starting -> Running`; a live `Starting`/`Running` peer for the
  same role throws `RingBufferRoleConflictException` (exit code 4), unless
  `takeover` reclaims it. `Dispose` publishes `Stopped`; `Abort` publishes
  `Faulted`.
* **Protocol.** Payload layout `[sequence:int64][timestamp:int64][fill...]`,
  `FillByte = 0xA5`. Sequence and timestamp make ordering and one-way latency
  verifiable; the fill makes corruption detectable.
* **Producer loop.** Reserve, stamp, fill, commit, pace (`PerMessageDelay`),
  report progress, check cancellation every 65536 messages. When the buffer is
  full it tracks how long it has been full and fails with `Timeout` or
  `PeerStopped` after `FullTimeout`.
* **Consumer loop.** Peek, verify (`Verify` gates sequence/type, `VerifyPayload`
  gates the O(payload) fill scan), record latency when the timestamp is present,
  advance, pace, report progress. On empty it checks the producer state (drain
  and finish if `Stopped`/`Faulted`) and the idle timeout.
* **Results.** `ProducerRunResult`/`ConsumerRunResult` carry counts, bytes,
  reasons (`Completed`, `PeerStopped`, `Timeout`, `VerificationFailed`,
  `Cancelled`), the latency histogram, and failure text. `ActiveElapsed` (first
  publish to end) and `RunElapsed` (wall clock) exist because the total elapsed
  window and the stream window measure different things.
* **Latency histogram.** 16 sub-buckets per power-of-two magnitude, preallocated
  `long[]`, allocation-free `Record`. Percentiles report the bucket midpoint
  (clamped to min/max): approximate by at most 1/32 of the value. `Merge`
  pools per-run histograms into one global percentile.

### Wait modes

| Mode | Behavior |
|---|---|
| `SpinThenSleep` | `SpinWait.SpinOnce()`: spin, yield, then sleep in increasing steps. Low CPU, millisecond wake-ups. |
| `SpinOnly` | `Thread.SpinWait(64)`: never sleeps. Microsecond wake-ups, burns a core while idle. |
| `Notification` | Blocks on a latch the peer raises. Microsecond wake-ups, ~0 CPU while idle. |

`Notification` protocol (per direction):

* Two one-deep latches form a `SessionNotification`: `Data` (producer raises
  after publishing, consumer waits) and `Space` (consumer raises after
  consuming, producer waits).
* Before blocking, the waiter sets its flag in the region header
  (`ConsumerWaiting`/`ProducerWaiting`), re-checks the buffer (so a publish that
  happened between the failed attempt and the flag write cannot be missed), and
  only then waits.
* The peer raises the latch only if `IsPeerWaiting()` - a running peer pays
  nothing, and the latch is never raised speculatively.
* Waits use a 50 ms slice so timeouts, peer-state changes, and cancellation stay
  observable even if the other side runs a different mode. A lost or spurious
  raise only costs one slice, because the waiter always re-checks the buffer.
* Implementations: `InProcessSessionSignal` (auto-reset event, any platform,
  same-process hosts) and `NamedSessionSignal` (named OS semaphore: Windows
  semaphores, POSIX named semaphores on Unix-like systems).
  The CLIs wire `--notify` through `SessionNotification.CreateNamed(regionName)`.

## 8. Platform implementations

| Implementation | Backing | Notes |
|---|---|---|
| `WindowsNamedMemoryMappedRegionFactory` | Named pagefile-backed section | Created with `CreateFileMapping` (optional `WindowsSectionSecurity` DACL) and mapped with `MapViewOfFile`; joined with `MemoryMappedFile.OpenExisting`, which requests read/write section access and therefore works under a restrictive DACL. Kernel object dies with the last handle, so a dead creator leaves nothing; `TryReset` is a no-op. `WindowsUnnamedSection` provides capability-mode sections with no OS name (HANDLE transfer only). |
| `UnixFileMemoryMappedRegionFactory` | File under `<temp>/sparc` (tmpfs on most Linux systems) | Files persist, so stale regions are reclaimed by `TryReset` (unlink; safe while mapped). Created from the same handle it was created with, 0600 files in a 0700 directory. A non-null `IpcRegionOptions.Security` is rejected rather than ignored. |
| `InMemoryMemoryRegionFactory` | Pinned managed arrays in a `ConcurrentDictionary` | Process-local, factory-scoped; for tests, samples, single-process development. Removed regions stay pinned so existing pointers stay valid. `Security` has no effect (no peer can reach the region). |

All three expose a raw `byte*` through `IIpcMemoryRegion`. That is deliberate:
cross-process atomics need stable addresses, which `MemoryMappedViewAccessor`
methods and managed spans alone cannot express. The pointer is obtained once —
`MapViewOfFile` on the created Windows path, `AcquirePointer` on the BCL-backed
open path — and released on dispose.

## 9. Crash semantics

| Situation | Behavior |
|---|---|
| Consumer starts first | It creates and initializes the region; the producer joins. |
| Producer starts first | Mirrored; the producer fills the ring and waits. |
| Second producer/consumer | Role conflict (exit 4); `--takeover` reclaims. |
| Stale/half-initialized region | Bad magic after `OpenTimeout` -> exit 6; `--recreate-stale` reclaims and recreates. |
| Producer killed | Published messages stay valid; the consumer drains, then times out and exits 3. The producer state stays `Running` (no heartbeats). |
| Consumer killed | The producer fills the ring and exits 2 after `--full-timeout`; a graceful stop sets `Stopped`, which the producer reports as peer-gone. |
| Consumer crash mid-read | The payload is copied before `head` advances, so the message is redelivered after restart: at-least-once across consumer crashes. |
| Producer restart | Appends at the existing `tail`; the session protocol's sequence numbers restart at 0, so use `--no-verify` or a fresh name. |
| Endpoint state | Advisory only; hard kills leave `Running`. Liveness detection would need heartbeats, which are out of scope. |

## 10. Testing

* `Sparc.UnitTests` - algorithm (empty/full/wraparound, random ops against a
  `Queue<byte[]>` oracle), layout and header validation, region
  create/join/version/geometry/corruption/stale recovery, role conflicts,
  waiting flags, lease state machines, session semantics (timeouts, peer
  stopped, verification, pacing, wait modes), histogram math, DI registration.
* `Sparc.ConcurrencyTests` - 10,000,000 messages with sequence + checksum + fill
  verification on every message for both the array and shared-memory buffers, a
  2-slot torture run, and an allocation assertion (< 4 KiB for 1,000,000
  write+read pairs after warm-up).
* `Sparc.ProcessTests` - real `dotnet` child processes: both start orders, role
  conflicts, geometry mismatch, killed consumer -> producer times out, killed
  producer -> consumer exits incomplete, `--require-existing`, and a
  `--notify` cross-process round trip.
* `Sparc.FuzzTests` - property tests for the untrusted-input paths: arbitrary
  header/slot/chunk bytes, geometry and region names, mixed copy/lease
  operation streams against a queue oracle, chunk reassembly through both
  readers, and hostile peers corrupting a published slot or chunk flags.

The full suite targets Windows because the shared-memory concurrency test and
the process tests need named mappings; the OS-independent suites (unit,
concurrency, fuzz) also run on the Ubuntu CI lane, and the Unix factory has its
own guarded tests.
