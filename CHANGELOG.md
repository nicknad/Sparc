# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Transport-level security for Windows sections.** `IpcMemoryRegionSecurity`
  (plus `IpcRegionOptions.Security` / `SharedRingBufferOptions.Security`) lets a
  transport apply OS access control when a region is created.
  `WindowsSectionSecurity` builds restrictive section DACLs (current user,
  explicit SIDs/accounts, or SDDL) applied through `CreateFileMapping`; opening
  an existing region is access-checked by the OS. Security is enforced during
  region establishment only — the message path is unchanged.
- **Capability-style unnamed sections.** `WindowsUnnamedSection.Create` creates
  an unnamed section and returns a `WindowsSectionCapability` exposing the
  section HANDLE for inheritance/`DuplicateHandle`; `MapHandle` maps a
  transferred handle. `SparcRing.OpenProducer`/`OpenConsumer` gained
  `IIpcMemoryRegion` overloads so an endpoint can adopt such a mapping. A
  channel identifier alone grants nothing: the section has no name.
- **CLI security flags.** `--security current-user` creates named regions with a
  current-user-only DACL; `--section-handle <h>|-` joins an unnamed section by a
  transferred HANDLE (`-` reads the value from stdin).
- **Security tests and benchmarks.** Windows unit tests (allowed/denied access,
  SDDL validation, unnamed-handle mappings), a secured shared-memory concurrency
  run, process tests for secure named regions and cross-process `DuplicateHandle`
  transfer, plus `shared-secured-copy/lease` regression scenarios and BDN
  benchmarks.
- **Threat model documentation.** README/`SECURITY.md`/`docs/concept.md` now
  distinguish transport access control from message authentication and
  confidentiality, and document how an optional authenticated/encrypted payload
  layer fits above SPARC.
- **Chunked message streaming.** `SparcStreamWriter`/`SparcStreamReader` and
  `SparcStreamWriteBuffer` (`IWriteBuffer`) publish and reassemble messages
  larger than one slot — larger than the ring, even — as `First`/`Last` chunk
  chains, with reader resynchronization after interrupted messages. The raw
  reservation behind it is public: `IProducerEndpoint.TryReserveWrite(int,
  out Span<byte>)` and `CommitWrite(int length)`.
- **Zero-copy stream reads.** `SparcStreamReader.BeginMessage`/`TryBeginMessage`
  return a `SparcStreamReadBuffer` (ref struct) that streams a chunked
  message's bytes without a destination copy and stitches chunk seams into
  caller scratch for `TryGetSpan`/`CopyTo`.
- **Abortable stream messages.** `SparcStreamWriteBuffer.Abort()` and the
  `Abort` chunk flag mark a failed message so consumers report
  `RingBufferCorruptedException` instead of a truncated value;
  `SfStreamCodec<T>` ties a stream writer/reader to serializer/deserializer
  delegates and uses it when serialization fails.
- **Chunk-stream fuzzing.** `Sparc.FuzzTests` now covers arbitrary chunk bytes
  and flags, chunked round-trips through both readers, and corrupted chunk
  flags in a mapped region.
- **Streaming sample.** `samples/Sparc.Serialization.Sample` streams a
  MessagePack-style formatter through `SfStreamCodec<T>`, including a value
  larger than the ring and a simulated serialization failure.
- **`Sparc.Serialization`.** Optional `ISparcCodec<T>` adapter (`SfCodec<T>`)
  that runs SerializerFoundation-based serializers directly over the channel
  writer's destination span and a slot's payload span, with no per-message
  allocations for span serializers.
- **Fuzz tests.** `Sparc.FuzzTests` property tests for the untrusted-input
  paths: arbitrary header/slot bytes, geometry and region-name inputs, mixed
  copy/lease operation streams against a queue oracle, and a hostile peer
  corrupting a published slot.

### Fixed

- `SparcStreamWriteBuffer.GetSpan` now rejects a `sizeHint` larger than one
  chunk's data capacity even when the current chunk is partially filled;
  previously it returned a shorter span, letting a contract-trusting serializer
  write into the next slot's framing.
- `SparcChannelReader` now serializes concurrent `TryRead` calls against the
  async pump and drains messages committed before the producer stopped, so a
  timeout that observes `Stopped`/`Faulted` no longer drops the tail of the
  ring.
- Session sequence verification now stamps and expects the slot's stream
  position (`IEndpoint.TailSequence` / `IEndpoint.HeadSequence`) instead of the
  run-local message index, so producer and consumer restarts and takeovers
  verify instead of failing on the first message.
- A custom `ProducerSessionOptions.PayloadWriter` that throws no longer leaves
  the write reservation pending, which previously made every later
  `TryReserveWrite` fail with `InvalidOperationException`.
- `JsonCodec<T>` and `SfCodec<T>` now reject messages larger than the declared
  `MaxSize` even when the caller passes a larger destination span; previously
  the bound was only enforced when the destination happened to be exactly
  `MaxSize`.
- `RingBufferHeader.Validate` now rejects slot sizes at or below the message
  header (found by the new header fuzz oracle); such a header passed
  validation and failed later during span construction.

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
