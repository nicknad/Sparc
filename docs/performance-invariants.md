# Performance invariants

This is the catalogue of properties that make SPARC's measured performance
possible. Each invariant is stated as a rule, followed by the mechanism, where
it lives in the code, what it buys, and what breaks if it is violated.

Numbers quoted here come from this repository's benchmarks on a shared Windows
11 VM (12th Gen i7-1260P, 16 logical cores); treat them as directional. The
measurement methodology is in [benchmarking.md](benchmarking.md).

---

## A. Algorithmic invariants

### A1. Exactly one producer and one consumer, per region

**Rule.** `tail` has one writer, `head` has one writer, and each endpoint is
driven by one thread for the lifetime of the buffer.

**Mechanism.** Role claiming CASes an advisory state word (`Connect`), and the
buffer contract in `IRingBuffer` documents single-threaded endpoints.

**Buys.** Advancing a cursor needs an ordered store, never a compare-and-swap.
The producer is wait-free while there is space, the consumer while there is
data. No lock, no contention manager, no fairness machinery.

**Breaks if violated.** Two producers both read the same `tail` and publish
over each other: lost messages, torn slots, cursors that disagree with reality.
There is no detection - the algorithm assumes the contract.

### A2. Cursors are monotonically increasing 64-bit sequences, capacity is a power of two

**Mechanism.** Slot index is `sequence & (capacity - 1)`; full is
`tail - head == capacity`; empty is `head == tail`.

**Buys.** No modulo, no wrap branch, no ambiguity between full and empty, and
the counters work for an unlimited number of messages. 64-bit counters also
mean the "sequence wraps" case never occurs in practice.

**Breaks if violated.** A non-power-of-two capacity forces division and can
make `tail - head == capacity` collide with other states; 32-bit counters wrap
in minutes at high rates.

### A3. Full/empty is derived from the cursor difference, not from a shared count

**Mechanism.** No count field exists in the header. `Count` is computed from
two volatile reads.

**Buys.** One less shared value to update, and no CAS to update it (both sides
would write a count).

**Breaks if violated.** A shared counter needs atomic increment/decrement by
both sides, reintroducing read-modify-write on the hot path.

### A4. Slots are fixed-size; memory is bounded and allocated once

**Mechanism.** Region size is `192 + capacity x slotSize`, fixed at creation
and validated on join (`RingBufferLayout.RequiredSize`).

**Buys.** O(1) slot addressing, no allocator, no fragmentation, no GC, and a
worst-case memory footprint that is known before the first message.

**Breaks if violated.** Variable-size records need per-message metadata and
defragmentation, and the region can no longer be a flat array of equal slots.

### A5. Publish-then-advance on both sides

**Rule.** The producer writes the frame and payload before the release store of
`tail`; the consumer finishes copying/scanning before the release store of
`head`.

**Mechanism.** `CommitWrite` writes the frame then `Volatile.Write(tail)`;
`AdvanceRead` runs only after the caller is done with the peeked view.

**Buys.** Never-torn messages, and at-least-once redelivery across a consumer
crash (a crash between the read and `AdvanceRead` leaves `head` unmoved, so the
message is re-read after restart).

**Breaks if violated.** Advancing `head` before the read completes lets the
producer overwrite the slot mid-copy; publishing `tail` before the write
completes exposes a half-written slot.

---

## B. Memory-ordering invariants

### B1. Release/acquire pairs exist only at the two publication points

**Mechanism.** `Volatile.Write` of `tail` / `Volatile.Read` of `tail`, and the
mirrored pair on `head` (`SharedRingBuffer.TryReserveWrite/TryPeek/CommitWrite/AdvanceRead`).

**Buys.** No fences or locked instructions per byte. The payload bytes travel
on the same coherence transaction as the cursor store; ordering costs one
release store per message and one acquire load per message (often elided by the
cached cursor, B2/C1).

### B2. `Volatile` is required even on x86-64

**Mechanism.** All shared cursor and state accesses use
`Volatile.Read`/`Volatile.Write` (or `Interlocked` for the role CAS).

**Buys.** The compiler and JIT are forbidden from reordering or hoisting the
accesses, and ARM64 gets real `ldar`/`stlr`. Relying on x86 TSO is the classic
way to ship a buffer that only breaks when ported.

**Breaks if violated.** JIT reordering can hoist a peer-cursor read out of a
loop, or move a payload read before the acquire load; the buffer then reads
stale or unpublished bytes.

### B3. Shared fields are aligned 64-bit values, and only 64-bit processes are supported

**Mechanism.** `head` at offset 64 and `tail` at offset 128, each on its own
cache line; accesses go through `ref long`.

**Buys.** Aligned 64-bit loads/stores are atomic on x86-64 and ARM64, which is
what makes a lock-free cursor possible without a wider primitive.

**Breaks if violated.** A misaligned or 32-bit access can tear, producing a
cursor that never existed.

### B4. The header is written field-by-field at fixed little-endian offsets

**Mechanism.** `RingBufferHeader.WriteTo`/`Read` use `BinaryPrimitives`; no
struct blitting anywhere.

**Buys.** Layout is stable across runtimes and compilers, and a joiner can
never observe a half-written field from a struct store.

### B5. Cross-process visibility relies on coherence, not on `Flush`

**Mechanism.** Both processes map the same physical pages; the release/acquire
pair orders accesses. `Flush` is only relevant for file durability.

**Buys.** No per-message syscall or cache flush.

### B6. Endpoint state is thread-affine

**Mechanism.** Cached cursors, lease reservations (`_pendingTail`, `_hasPendingWrite`),
and peek state (`_peekedHead`, `_hasPeekedRead`) are plain instance fields, not
atomics.

**Buys.** No synchronization for private state; the only atomics are the two
shared cursors, the role state, and the optional waiting flags.

**Breaks if violated.** Two threads sharing one endpoint would race on the
pending/lease state and on the "own" cursor.

---

## C. Cache and coherence invariants

### C1. The peer cursor is cached privately and refreshed only when it claims full/empty

**Mechanism.** `_cachedHead`/`_cachedTail`; the shared value is re-read only
when the stale copy says the buffer is full (producer) or empty (consumer).

**Buys.** Avoids fetching a line the peer writes on every message. The project
README reports **1.3M -> 13M messages/s** from this optimization on the test
VM. The cached value can only be older, never newer, so correctness is
unaffected and the worst case is a redundant refresh.

**Breaks if violated.** Reading the peer's cursor on every message costs a
coherence transfer per message; that is the single largest software-level
penalty in a naive SPSC queue.

### C2. Cached cursors are seeded from the live region on attach

**Mechanism.** The `SharedRingBuffer` constructor reads `HeadRef`/`TailRef`.

**Buys.** A consumer attaching to a drained region with non-zero cursors does
not mistake stale zeros for pending data
(`FreshConsumerAttachedToDrainedNonZeroRegionReportsEmpty`).

### C3. `head` and `tail` never share a cache line

**Mechanism.** Offsets 64 and 128 with 56 bytes of padding each.

**Buys.** The two writers never bounce the same line between cores. Without
this, every producer store invalidates the consumer's line and vice versa.

### C4. Slots start on cache-line boundaries

**Mechanism.** `HeaderSize = 192` (three cache lines), and the CLI rounds
derived slot sizes up to 64 bytes (`RingBufferLayout.RoundSlotSizeToCacheLine`).

**Buys.** No slot straddles the header, and every slot boundary is a line
boundary.

### C5. Adjacent slots never share a boundary line

**Rule.** Slot size should be a multiple of 64.

**Why it matters.** When the ring is full, the producer writes slot `N` while
the consumer reads slot `N+1`. With an unaligned slot size, the last line of
slot `N` also holds the first bytes of slot `N+1`, so the producer's write
invalidates the consumer's read.

**Measured.** Neutral on the test VM at 256 B (2.04M vs 1.97M msg/s), because
the cursor handoff dominates there; kept as a structural fix and to align the
CLIs with the benchmark pumps.

### C6. Region footprint decides cache residency, and it is not the same variable as payload size

**Rule.** `footprint = 192 + capacity x slotSize`. Keep it inside the CPU
caches when you want streaming throughput.

**Measured (200k messages per run).**

| Configuration | Data rate | Message rate |
|---|---:|---:|
| 4 KB payload, 4 MiB region | 5.2 GiB/s | 1.34M msg/s |
| 8 KB payload, 4 MiB region | 5.3 GiB/s | 675k msg/s |
| 16 KB payload, 4 MiB region | 5.5 GiB/s | 354k msg/s |
| 16 KB payload, 16 MiB region | 3.1 GiB/s | 197k msg/s |
| 16 KB payload, 16 MiB region, no payload scan | 5.4 GiB/s | 348k msg/s |

At a fixed 4 MiB footprint the **data rate is flat across 4/8/16 KB**: the cost
per byte is linear and the message rate scales as `1/size`. Once the ring
exceeds the caches, every slot line is fetched and written through DRAM and the
rate drops ~1.8x; the payload verification scan costs another ~1.8x when
enabled. Those two losses stack, which is what makes a payload sweep at fixed
capacity look like a cliff.

**Breaks if violated (methodologically).** A benchmark that changes payload size
at a fixed capacity changes payload *and* footprint *and* bytes-in-flight at
once. The harness now exposes `--capacity` and prints the footprint per row.

### C7. Writes to non-resident lines pay read-for-ownership

**Mechanism.** Ordinary copies/memsets write-allocate: the line is read from
DRAM before the store, then written back. `memcpy`/`Span.CopyTo` do not use
non-temporal stores.

**Buys (when resident).** L1/L2/L3 bandwidth instead of DRAM bandwidth.

**Remaining cost.** Even with the zero-copy lease, the producer's payload write
still pays RFO when the region is not cache-resident. A non-temporal store path
is an open follow-up.

### C8. The ring is a bounded, sequentially streamed circular buffer

**Mechanism.** Slots are revisited in order `seq & (capacity-1)`; the working
set is exactly `capacity` slots.

**Buys.** Hardware prefetchers see sequential addresses; there is no pointer
chasing, no tree, no hash. Memory behavior is predictable and testable.

---

## D. Allocation and GC invariants

### D1. Allocate storage once

* `SpscRingBuffer`: one pinned array,
  `GC.AllocateArray<byte>(capacity * slotSize, pinned: true)`.
* `SharedRingBuffer`: no managed buffer; a `byte*` into the mapped region,
  acquired once at open.

### D2. Use stack-only spans and value types on the hot path

`Span<byte>`/`ReadOnlySpan<byte>` are `ref struct`s (cannot be boxed or put on
the heap). Framing uses `BinaryPrimitives` over spans. Cursors are `long`
fields in a padded struct. `SpinWait` is a struct. `Debug.Assert` compiles out
in Release.

### D3. Zero-copy lease removes both intermediate copies

The producer writes directly into the slot and the consumer verifies in place,
so neither side needs an intermediate payload buffer and the consumer no longer
allocates a destination at all.

**Measured.** Cross-process 4 KB: **2.0M msg/s / 7.8 GiB/s** with the lease vs
1.16M / 4.5 GiB/s with copies.

### D4. Support structures are preallocated and allocation-free per operation

The latency histogram preallocates `64 x 16` buckets and `Record` only
increments. Progress reporting is off unless a callback is supplied. No LINQ,
no closures, and no string formatting on the message path.

### D5. Evidence

`SteadyStateDoesNotAllocate` warms up 100k iterations, then asserts that
1,000,000 write+read pairs allocate **< 4 KiB** in total
(`tests/Sparc.ConcurrencyTests`). BDN's `MemoryDiagnoser` reports 0 B/op for all
SPSC pumps. The 10M-message concurrency runs also allocate nothing measurable
per message.

---

## E. Cost-model invariants

### E1. Per-message cost = fixed protocol cost + per-byte transfer cost

The fixed part is dominated by the cross-core cursor handoff (the README's
two-thread ping-pong measured ~1.5 us per handoff on this VM; bare metal is
typically 10-50x better). The per-byte part is the payload passes:

| Pass | Copy API | Lease API |
|---|---|---|
| Producer writes payload into the slot | yes (reads source) | yes (direct) |
| Producer reads source array | yes | no |
| Consumer reads payload out of the slot | yes | in-place scan only |
| Consumer writes destination | yes | no |
| Optional verification scan | yes (over destination) | yes (over slot) |

That is why small messages are handoff-bound and large messages are
bandwidth-bound, and why the zero-copy change helps the consumer side most.

### E2. Verification is O(payload) and optional

`IsPayloadIntact` scans every payload byte for the fill pattern.
`ConsumerSessionOptions.VerifyPayload = false` (CLI `--no-verify-payload`) keeps
the cheap sequence/type checks and skips the scan. Measured at 16 KB / 16 MiB:
3.1 -> 5.4 GiB/s when the scan is removed.

### E3. Wait mode trades CPU for latency explicitly

| Mode | Idle CPU | Measured p50 (paced producer) |
|---|---|---|
| `SpinThenSleep` | ~0 | 6.45 ms |
| `SpinOnly` | 1 core | 3.9 us |
| `Notification` | ~0 | 7.6 us |

The default sleep path is why the p99 of an idle consumer equals the Windows
timer tick (~15.6 ms); spin and notification remove that floor.

### E4. Backpressure delay is bounded by capacity

With the consumer slower, each message queues behind up to `capacity` messages:
`p50 ~= capacity x consumer period`. 1024 slots at 100 us/message gives ~102 ms.
The same bound is the burst absorber when the producer is bursty.

### E5. Zero-copy is not free for the producer when the region is not resident

The producer still writes the whole payload each message (the session protocol
fills `[fill...]` for corruption detection). At 16 KB / 16 MiB that write is
the bottleneck: the consumer is nearly free with `--no-verify-payload`
(p50 2.3 us, ~2% CPU) while the producer sets the rate. A fill-once session
mode (write the constant fill once per slot, stamp only the header) is an open
follow-up.

---

## F. Operational invariants

### F1. Bounded capacity means an explicit drop/block policy

The ring never grows. When it is full the producer must choose: block with a
timeout, drop, or slow the source. The samples bridge an unbounded source
through a bounded in-process channel and count drops.

### F2. Endpoint states are advisory, not liveness detection

`Stopped`/`Faulted` enable fast graceful shutdown and role-conflict rejection,
but a hard kill leaves `Running`; the peer falls back to its timeout. No
heartbeats, by design.

### F3. Geometry is fixed by the creator

Joiners either adopt it (`AdoptExistingGeometry`) or fail with a specific
exception. This keeps the region layout immutable for the lifetime of the
mapping - no renegotiation, no resize.

### F4. No OS synchronization on the data path

The OS is involved at setup (mapping) and, optionally, in `Notification` mode
(a named semaphore wait). `SpinThenSleep` and `SpinOnly` never enter the kernel
on a message.

### F5. Same trust domain only

Both processes can write every byte of the region. There is no capability or
memory protection between endpoints; do not share a region with an untrusted
process.

---

## G. Measurement invariants (how to not fool yourself)

### G1. Payload size and footprint are separate variables

Use `--capacity` and the printed `Region` column; compare like with like (C6).
A payload sweep at fixed capacity silently changes cache residency.

### G2. Interleave runs and repeats

The latency sweep round-robins sizes so a busy period hits every size equally
instead of biasing the last one. The regression runner interleaves rounds for
the same reason.

### G3. Report the right elapsed window

The producer's `Elapsed` includes waiting for the consumer to attach;
`ActiveElapsed` (first publish to end) is the send-rate window. The consumer's
`Elapsed` is the first-to-last-message stream window; `RunElapsed` is wall
clock. Mixing them produces numbers that cannot be compared.

### G4. Know your floors

p99 ~= 15.6 ms is the Windows timer tick on the default sleep path, not ring
behavior. p99.9/max ~= 0.3 s are VM scheduling stalls that recur in every
capture on this host. Use spin/notification to remove the tick floor, and do
not attribute host stalls to the algorithm.

### G5. Percentiles are approximate by design

The histogram splits each power-of-two range into 16 sub-buckets, so a reported
percentile is within 1/16 of the true value. It trades exactness for
allocation-free recording.

### G6. Shared-host swings are real

On this VM, individual in-process scenarios can swing 2-5x between sessions
even with best-of-5. The regression harness defaults to a 50% tolerance and
says so; use BenchmarkDotNet on a quiet machine for authoritative numbers
([benchmarking.md](benchmarking.md)).

---

## Summary: invariant -> mechanism -> payoff

| Invariant | Mechanism | Payoff |
|---|---|---|
| One writer per cursor | SPSC contract + role CAS | no CAS/lock; wait-free ends |
| Sequence cursors + power-of-two capacity | `seq & mask`, difference checks | O(1) addressing, unambiguous full/empty |
| Fixed slots, bounded region | flat slot array, validated geometry | no allocator, known footprint |
| Publish-then-advance | release/acquire at `tail`/`head` | no torn messages; at-least-once redelivery |
| Cached peer cursors | refresh only on full/empty | 1.3M -> 13M msg/s (README) |
| Cache-line separation | offsets 64/128 + padding, 64-byte slots | no false sharing |
| Cache residency discipline | footprint vs caches; harness reports it | flat ~5.3 GiB/s at 4 MiB; ~1.8x cliff when exceeded |
| Zero-copy lease | reserve/peek into the slot | 4 KB: 2.0M/7.8 GiB/s vs 1.16M/4.5 GiB/s |
| Allocation-free hot path | pinned/mapped storage, spans, structs | 0 B/op; no GC pauses |
| Optional verification | `VerifyPayload` / `--no-verify-payload` | removes an O(payload) pass (~1.8x at 16 KB) |
| Wait-mode choice | sleep / spin / OS latch | 6.45 ms -> 3.9 us / 7.6 us idle latency |
| Bounded capacity | fixed ring | predictable queueing and memory |
