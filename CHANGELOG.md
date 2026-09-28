# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0]

### Added

- **Role-typed endpoints.** `SparcRing.OpenProducer`/`OpenConsumer` return
  `IProducerEndpoint`/`IConsumerEndpoint` (compile-time producer/consumer
  separation), with `WriteLease`/`ReadLease` scope-based reservations and
  blocking `Publish`/`BeginWrite`/`Read` wrappers. `SharedRingBuffer`,
  `RingBufferRegion`, `RingBufferHeader`, `SlotFraming` and the role enum are
  internal; the wire format is locked by golden tests and PublicAPI files.
- **Unified error model.** `SparcException`/`SparcErrorCode` base for the
  transport and protocol exception families, plus one `RegionName.Validate`
  for every transport.
- **Generalized sessions.** `PayloadWriter`/`IncludeSessionHeader` for custom
  payloads, `Count = 0` (run until cancelled/peer stopped), `RunAsync` and
  `WaitForPeerAsync`.
- **`Sparc.Hosting`.** `AddSparcIpc` (OS transport selection), `AddSparcChannel`
  (options or configuration section), hosted session services and custom
  `ISparcProducerWorker`/`ISparcConsumerWorker` services, `SparcChannelStatus`,
  `AddSparcHealthChecks` and the `Sparc` meter.
- **`Sparc.Channels`.** Typed `SparcChannelWriter<T>`/`SparcChannelReader<T>`
  and paired `SparcChannel<T>` with span-based `ISparcCodec<T>`, `JsonCodec<T>`,
  `TryWrite` fast path, `WriteAsync` backpressure and
  `ReadAsync`/`ReadAllAsync` streaming that ends when the producer stops.
- **`Sparc.Testing`.** `TestSparcRing`/`TestSparcChannel`,
  `FakeTimeProvider`-driven deterministic sessions and fail-fast timeout
  helpers.
- **`Sparc.Analyzers`.** SPARC0001: `SessionWaitMode.Notification` without a
  `SessionNotification` is a compile-time error.
- **Unix notification parity.** `SessionWaitMode.Notification` and `--notify`
  now work on Unix-like systems through POSIX named semaphores.
- Documentation: `docs/hosting.md`, `docs/channels.md`, `docs/testing.md`,
  library quick starts, plus protocol golden tests and a `channel-8` regression
  scenario.

### Changed

- Breaking (pre-1.0): the `IRingBufferEndpoint` seam is replaced by role-typed
  endpoint interfaces; session option properties are settable for delegate and
  configuration binding; the public surface of every package is locked by
  `PublicAPI.Shipped.txt`.

## [Unreleased]

### Added

- `docs/`: concept, use cases, how-it-works, performance invariants and
  benchmarking documentation.
- Zero-copy lease API (`TryReserveWrite`/`CommitWrite`/`AbandonWrite`,
  `TryPeek`/`AdvanceRead`) and sessions rewritten on top of it; the consumer no
  longer allocates a destination buffer.
- `SessionWaitMode.Notification` with `ISessionSignal`,
  `InProcessSessionSignal`, `NamedSessionSignal` and `SessionNotification`;
  CLIs expose `--notify`.
- Unix file-backed transport (`Sparc.UnixMemoryMapped`) with owner-only region
  files and `AddUnixFileMemoryMappedIpc()`.
- In-process performance regression harness (`--regression`) and lease-API
  BenchmarkDotNet variants.
- `ActiveElapsed`/`RunElapsed` on session results; `--no-verify-payload` and
  `--spin-only` CLI flags.
- Open-source project files: MIT license, security policy, contribution guide,
  code of conduct, issue/PR templates, Dependabot configuration, release
  workflow, NuGet package metadata and SourceLink.

### Changed

- Derived slot sizes are rounded up to a 64-byte cache line in the CLIs as well
  as the benchmark pumps.
- Benchmark harness separates payload size from region footprint (`--capacity`),
  interleaves repeats, and reports the region size per row.
- README benchmark tables refreshed with post-fix numbers and the cache-residency
  explanation for the 16 KB drop.

### Fixed

- Session result timestamps (`Elapsed`/`ActiveElapsed`, `Elapsed`/`RunElapsed`)
  are now derived from a single end timestamp, removing a sampling race.

## [0.2.0]

Initial development line: lock-free SPSC ring protocol with explicit
acquire/release ordering, Windows named memory-mapped and pinned-array
transports, producer/consumer sessions with verification and latency
histograms, CLIs, samples, and the unit/concurrency/process test suites.
