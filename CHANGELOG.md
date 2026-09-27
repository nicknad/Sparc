# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Pre-1.0, minor versions may contain breaking changes.

## [Unreleased]

### Added

- `docs/`: concept, use cases, how-it-works, performance invariants and
  benchmarking documentation.
- Zero-copy lease API (`TryReserveWrite`/`CommitWrite`/`AbandonWrite`,
  `TryPeek`/`AdvanceRead`) and sessions rewritten on top of it; the consumer no
  longer allocates a destination buffer.
- `SessionWaitMode.Notification` with `ISessionSignal`,
  `InProcessSessionSignal`, `NamedSessionSignal` and `SessionNotification`;
  CLIs expose `--notify` (Windows).
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
