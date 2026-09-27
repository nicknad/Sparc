# Concept

## The problem

Two processes on the same machine often need to move a high rate of small,
fixed-size messages: telemetry samples, request metadata, market data, frame
descriptors, metrics. The usual transports all pay for semantics SPARC does not
need:

* **Named pipes / Unix sockets / TCP loopback** copy the payload into kernel
  buffers, cross the user/kernel boundary twice, and wake the peer through the
  scheduler. Round trips land in the tens of microseconds.
* **`ConcurrentQueue<T>` / `Channel<T>`** are in-process only. To cross a process
  boundary you still need a socket or pipe underneath.
* **Locks and condition variables** serialize on cache-line ownership and put the
  waiting thread to sleep; wake-up latency becomes timer-bound (milliseconds).

SPARC's answer is the oldest trick in low-latency systems: put the queue in
memory that both processes already map, and synchronize with two atomic counters
instead of locks.

## The core idea

A ring buffer of fixed-size slots lives in a memory-mapped region. Two 64-bit
sequence numbers, `head` and `tail`, live in the region header:

```
slot index = sequence % capacity          (capacity is a power of two)
empty:  head == tail
full:   tail - head == capacity
```

The producer only ever writes `tail`; the consumer only ever writes `head`.
Because each counter has exactly one writer, advancing them needs only an
ordered load/store pair, never a compare-and-swap. The payload itself is copied
(or written in place) into the slot *before* the release store that publishes
the counter, and the peer's acquire load of that counter orders the payload read
after it. That is the entire synchronization protocol.

```
Producer process                              Consumer process
      |                                             |
      | TryReserveWrite -> write slot -> CommitWrite|
      |   release store tail  --------------------->| acquire load tail
      |                                             | TryPeek -> verify -> AdvanceRead
      |   acquire load head   <---------------------|  release store head
      v                                             v
              same physical pages, one region
```

## What the transport guarantees

| Property | Guarantee |
|---|---|
| Message integrity | A message is never torn: the consumer observes the whole slot only after the producer's release store of `tail`. |
| Ordering | Messages are delivered in the order they were published. |
| Bounded memory | The region is `192 + capacity x slotSize` bytes, allocated once. No growth, no allocator, no GC. |
| No locks / no CAS | The data path uses only `Volatile` loads/stores and raw pointer arithmetic. |
| Exactly one producer, one consumer | `Connect` claims a role with a CAS on an advisory state word; a second live endpoint for the same role is rejected. |
| Consumer crash | The payload copy happens before `head` advances, so a message is redelivered after a restart: **at-least-once across consumer crashes**, never torn, never silently lost. |
| Producer crash | Unpublished slots are invisible. Already published messages stay valid; the consumer detects the stall through its idle timeout. |
| Liveness detection | Advisory only: endpoint states are updated on graceful start/stop, but a hard kill leaves `Running` (no heartbeat, by design). |
| Payload size | Fixed per region (slot size minus an 8-byte frame). Variable-size records must be packed by the caller (the YARP sample does exactly that). |
| Security boundary | None. Both processes map the same region; use it between processes of the same trust domain and user. |

## What SPARC is not

* Not MPMC: one producer and one consumer, per region.
* Not persistent: memory dies with the last mapping (Windows) or the backing
  file (Unix).
* Not networked: both endpoints must run on the same machine and share a user.
* Not variable-sized: every slot has the same size; larger records must be split
  or dropped by the caller.
* Not a workflow engine: no acknowledgements, no retries, no heartbeats.

## How it compares

Directional numbers from this repository's own benchmarks (shared VM; see
[benchmarking.md](benchmarking.md)); the point is the order of magnitude, not the
exact figures.

| Transport | Semantics | Measured ballpark (64 B, two threads, in-process) |
|---|---|---|
| SPSC ring over shared memory | fixed-size, SPSC, lock-free | ~0.5 us/message |
| SPSC ring over a pinned array | same, same process | ~0.7 us/message |
| `Channel<T>` | async waiting, MPMC-ish, allocates per message | ~1.6 us/message |
| `ConcurrentQueue<T>` + lock | MPMC, blocking, allocates | ~2.6 us/message |
| Named pipe | byte stream, kernel copies, wake-ups | ~30 us/message |
| TCP loopback | stream, kernel + network stack | ~45 us/message |

The gap is not "shared memory is faster" in the abstract; it is that SPARC
removes three costs the alternatives pay by design: kernel copies, scheduler
wake-ups on every message, and per-message allocation.

## Design principles

1. **One writer per piece of state.** `tail` is producer-owned, `head` is
   consumer-owned, and every private cache/lease state is single-threaded. This
   is what eliminates CAS and locks.
2. **Publish with a release store, observe with an acquire load.** Only the two
   cursors need ordering; the payload bytes ride along.
3. **Keep the hot path allocation-free and cache-friendly.** Fixed slots,
   pinned/unmanaged storage, stack-only spans, cache-line-separated counters,
   zero-copy leases.
4. **Isolate the operating system.** The ring protocol does not reference any OS
   type; `IIpcMemoryRegionFactory` is the only seam (`src/Sparc.Abstractions`).
5. **Make the failure semantics explicit.** Crash behavior is documented and
   tested rather than assumed (`README.md` section 4, `tests/Sparc.ProcessTests`).
6. **Measure with the right experiment.** Payload size and region footprint are
   separate variables; the harness can vary them independently
   ([performance-invariants.md](performance-invariants.md), section G).
