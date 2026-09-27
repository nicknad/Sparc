# SPARC documentation

SPARC (Shared Process Atomic Ring Channel) is a single-producer/single-consumer
lock-free ring buffer in C# that lets two independent processes exchange
fixed-size messages through the same physical memory without locks, without
compare-and-swap on the data path, and without allocations after startup.

This folder goes deeper than the top-level `README.md`:

| Document | What it covers |
|---|---|
| [concept.md](concept.md) | The problem, the core idea, what the transport guarantees and what it does not, and how it compares to pipes/sockets/queues. |
| [use-cases.md](use-cases.md) | Where an SPSC shared-memory ring is the right tool, where it is not, and sizing guidance. |
| [how-it-works.md](how-it-works.md) | The full mechanism: region handshake, binary layout, ring algorithm, memory ordering, lease API, sessions, wait modes, crash semantics, platform implementations, tests. |
| [performance-invariants.md](performance-invariants.md) | Every invariant that makes the measured performance possible, grouped by kind, each with its mechanism, where it lives in code, what it buys, and what breaks if violated. |
| [benchmarking.md](benchmarking.md) | The benchmark harnesses, how to run them, measurement traps, current numbers, and the regression check. |

Suggested reading order for a newcomer: `concept.md` -> `use-cases.md` ->
`how-it-works.md` -> `performance-invariants.md` -> `benchmarking.md`.

## Source map

| Area | Projects |
|---|---|
| OS abstraction | `src/Sparc.Abstractions` |
| Windows named memory-mapped transport | `src/Sparc.WindowsMemoryMapped` |
| Unix file-backed transport | `src/Sparc.UnixMemoryMapped` |
| Pinned-array transport for tests/dev | `src/Sparc.InMemory` |
| Ring protocol and buffers | `src/Sparc.Core` |
| Producer/consumer sessions, latency histogram | `src/Sparc.Client` |
| Producer/consumer CLIs | `src/Sparc.Producer`, `src/Sparc.Consumer` |
| Hosts | `samples/Sparc.WebApp`, `samples/yarp/*` |
| Verification | `tests/Sparc.UnitTests`, `tests/Sparc.ConcurrencyTests`, `tests/Sparc.ProcessTests` |
| Measurement | `benchmarks/Sparc.Benchmarks` |
