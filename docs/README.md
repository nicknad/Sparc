# SPARC documentation

SPARC (Shared Process Atomic Ring Channel) is a single-producer/single-consumer
lock-free ring buffer in C# that lets two independent processes exchange
messages through the same physical memory without locks, without
compare-and-swap on the data path, and without allocations after startup.
Messages are fixed-size slots; anything larger is streamed as a chunk chain
(see [streaming.md](streaming.md)).

This folder goes deeper than the top-level `README.md`:

| Document | What it covers |
|---|---|
| [learning-path.md](learning-path.md) | A guided route through these docs and the code: the mental model, nine sessions, hands-on experiments and checkpoints. |
| [concept.md](concept.md) | The problem, the core idea, what the transport guarantees and what it does not, and how it compares to pipes/sockets/queues. |
| [use-cases.md](use-cases.md) | Where an SPSC shared-memory ring is the right tool, where it is not, and sizing guidance. |
| [hosting.md](hosting.md) | `Sparc.Hosting`: DI registration, hosted session/worker services, health checks, metrics, configuration, testing. |
| [channels.md](channels.md) | The typed `SparcChannel<T>` layer: codecs, async API, semantics, overhead. |
| [streaming.md](streaming.md) | Chunked messages larger than a slot (or the ring): wire format, `SparcStreamWriter`/`SparcStreamReader`, `SfStreamCodec<T>`, abort semantics. |
| [testing.md](testing.md) | The `Sparc.Testing` harness: paired endpoints, fake time, timeout helpers. |
| [how-it-works.md](how-it-works.md) | The full mechanism: region handshake, binary layout, ring algorithm, memory ordering, lease API, sessions, wait modes, crash semantics, platform implementations, tests. |
| [performance-invariants.md](performance-invariants.md) | Every invariant that makes the measured performance possible, grouped by kind, each with its mechanism, where it lives in code, what it buys, and what breaks if violated. |
| [benchmarking.md](benchmarking.md) | The benchmark harnesses, how to run them, measurement traps, current numbers, and the regression check. |

Transport security and the threat model live in the top-level
[README.md §7](../README.md#7-transport-security-and-isolation) and
[SECURITY.md](../SECURITY.md); the ring protocol never inspects them.

New here? Start with `learning-path.md`. Reading order for the mechanics:
`concept.md` -> `use-cases.md` -> `how-it-works.md` ->
`performance-invariants.md` -> `benchmarking.md`. If you are wiring a host,
start with `hosting.md`; for message typing, `channels.md`; for large values,
`streaming.md`; for writing tests, `testing.md`.

## Source map

| Area | Projects |
|---|---|
| OS abstraction | `src/Sparc.Abstractions` |
| Windows named memory-mapped transport | `src/Sparc.WindowsMemoryMapped` |
| Unix file-backed transport | `src/Sparc.UnixMemoryMapped` |
| Pinned-array transport for tests/dev | `src/Sparc.InMemory` |
| Ring protocol and buffers | `src/Sparc.Core` |
| Producer/consumer sessions, latency histogram | `src/Sparc.Client` |
| Typed channel layer (`SparcChannel<T>`, codecs) | `src/Sparc.Channels` |
| SerializerFoundation codecs and chunked streaming | `src/Sparc.Serialization` |
| Host integration: DI, hosted services, health, metrics | `src/Sparc.Hosting` |
| Test harness (paired endpoints, fake time, timeouts) | `src/Sparc.Testing` |
| Roslyn analyzer (option combinations) | `src/Sparc.Analyzers` |
| Producer/consumer CLIs | `tools/Sparc.Producer`, `tools/Sparc.Consumer` |
| Hosts | `samples/Sparc.WebApp`, `samples/Sparc.Serialization.Sample`, `samples/yarp/*` |
| Verification | `tests/Sparc.UnitTests`, `tests/Sparc.ConcurrencyTests`, `tests/Sparc.FuzzTests`, `tests/Sparc.ProcessTests` |
| Measurement | `benchmarks/Sparc.Benchmarks` |
