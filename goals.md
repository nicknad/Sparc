# Goals — SPARC backlog

Backlog for the next maintainer or agent. Consolidated 2026-11-14 from the architecture review at
`88f7c4f` and the local `WORKLIST.md`; it replaces both. Completed work is summarized once and
never repeated as an item.

## Pointers

- Session-engine design (done, still the reference): `docs/architecture/session-run-engine.md`
- Architecture review, 2026-11-14 05:55, working tree `88f7c4f` (ephemeral HTML):
  `C:\Users\nadol\AppData\Local\Temp\dsh-k7LBCu\architecture-review-20261114-055540.html`
- Decision log with evidence: `decisions.tsv` · Terms: `GLOSSARY.md` · Perf rules:
  `docs/performance-invariants.md`

## Recently completed (closed — do not re-open)

- **Session run engine.** `SessionRunEngine` + `ProducerRole`/`ConsumerRole`; one
  `SessionStopDecision.Resolve` call site; public API unchanged. Verified 2026-11-14: build
  0 warnings/0 errors; Unit 257 (9 skipped), Concurrency 5, Process 12, Fuzz 21 pass. Design:
  `docs/architecture/session-run-engine.md`; still uncommitted at `88f7c4f`.
- **Payload-size cliff.** Cause split between cache residency and the verify scan; payload
  decoupled from footprint (`--capacity`, round-robin), interleaved sizes, `--no-verify-payload`,
  `Region` column — `035fc6b`.
- **Lease / zero-copy path** plus `TryPeek` without a max-size destination — `c4c0a23` (4 KB:
  2.0M msg/s, 7.8 GiB/s vs 1.16M, 4.5 GiB/s).
- **Slot rounding + Core geometry policy** in Core with CLI/BDN call sites, plus regression tests —
  `fcf9b1a` (neutral at 256 B; kept as structural). The BDN pump floor still differs from the
  advisor; see open item 10.
- **Seams.** Sessions take role-typed endpoints (`IProducerEndpoint`/`IConsumerEndpoint`),
  internals via `InternalsVisibleTo`, the duplicated ring algorithm documented as deliberate with
  parallel tests — `e8f2d30`.
- **Wait modes.** `SpinOnly` — `69f23b4` (p50 6.45 ms → 3.9 µs); `Notification` — p50 7.6 µs at
  ~2% consumer CPU, cross-process `NotifyMode_FullRoundTrip`; POSIX latch — `fa5c811`; async
  wrappers — `552c97c`.
- **Regression harness.** `--regression`: 12 scenarios, best of 5, `perf-baseline.json`, 50% gate;
  BDN lease pumps; `IScenarioPump` + `SessionPump`.
- **Hygiene.** Unix TOCTOU/permissions and stale-region tests — `cc72d33`; `.vscode/` ignored,
  `.gitattributes`, no tracked build outputs — `7d68296`; package ignores deferred.

## Open work (26 items)

### Session follow-ups (4)

1. **Channel reader stop reason — a public API decision.** `SparcChannelReader` owns its drain
   after producer stop and reports no stop reason (`SparcChannelReader.cs:247-256,287`). It is
   outside the engine on purpose: its pump blocks in `Read`, has no count, and changing it would
   alter latency semantics (design doc, `Tradeoffs accepted` and `Open work`). Decide the public shape
   first.
2. **Producer's extra per-message `Step` call.** One devirtualized `Step` per message is
   accepted; `session-64` is the gate (`RegressionRunner.cs:74`; design doc, `Tradeoffs accepted`).
   Measure against a captured baseline; split `Step` into an inlined fast path only if it clears
   the noise floor.
3. **Failure-text culture formatting.** Failure text is culture-sensitive today (`{…:F1}`) and
   the table preserves that. Decide invariant vs preserved.
   `SessionTests` assert `stayed full`, `consumer is gone`, `producer stopped after`,
   `no messages for`.
4. **Async session wrappers — confirm residue, don't re-add.** `RunAsync` on both sessions
   delegates to `SessionRuntime.RunOnDedicatedThread` (`ProducerSession.cs:84-87`,
   `SessionRuntime.cs:7-12`, `552c97c`), yet `docs/use-cases.md:74` still teaches `Task.Run`.
   Align the docs; add no second async layer.

### Data path and platform (3)

5. **Producer fill-once session option.** Beyond the 16-byte header the session payload is
   constant fill (`ProducerRole.cs:169`, `RingBufferMessage.cs:22,38`). Filling each slot once and
   stamping only the header per message would remove most of the 16 KB producer cost. It changes
   what the benchmark measures, so it must be explicit, documented, never the default.
6. **Non-temporal/streaming store for the copy path.** The lease path writes straight into the
   slot; the copy API still does a normal cached copy of large payloads. Add the streaming path
   only to the copy API and only behind measurement.
7. **Unix notification latch — verify, don't re-implement.** POSIX named semaphores shipped
   (`NamedSessionSignal.cs:49-50`, `PosixSemaphore.cs`, `fa5c811`) and unit tests run on
   `ubuntu-latest` (`ci.yml:14,38-43`). Residue: the cross-process test is Windows-only
   (`ci.yml:35`, `tests/Sparc.ProcessTests/ProcessLifecycleTests.cs:105`). Add a Unix
   cross-process run or record the latch as unit-verified only; no pipe/socket latch needed.

### Architecture candidates (5)

8. **Benchmark scenario catalog.** One scenario is defined four times: 11 BDN classes
   (`ThroughputBenchmarks.cs:29-99`), 8 `TwoThreadPump` subclasses + `SessionPump` (`Pumps/*`), a
   12-row tuple table (`RegressionRunner.cs:69-82`), a 6-arm switch (`LatencyRunner.cs:127-134`),
   in three naming vocabularies. Coverage already diverges: the channel pump discards the size
   because the codec fixes geometry (`ChannelPumps.cs:19`); no lease/secured latency arms.
   Pumps must keep measuring the same work — no public API change to unify.
9. **Region-open policy.** Three retry loops, three backoffs, one timeout text: Windows 2→50 ms
   (`WindowsNamedMemoryMappedRegionFactory.cs:167-195`), Unix fixed 2 ms
   (`UnixFileMemoryMappedRegionFactory.cs:186-218`), in-memory 1 ms
   (`InMemoryMemoryRegionFactory.cs:80-95`). `IIpcMemoryRegionFactory` declares no error modes, so
   `RingBufferRegion.cs:122-131` re-wraps by concrete exception type. Declare the error-mode
   contract before unifying; retry semantics differ by platform.
10. **Geometry policy.** Payload-based slot derivation exists in three forms with three floors:
    `RingBufferAdvisor.SlotSizeFor` floors at `DefaultSlotSize` (256,
    `RingBufferAdvisor.cs:26-38`); the benchmark pumps floor at `CacheLineSize` (64) while their
    comment claims CLI parity (`Pumps/RingBufferPumps.cs:28-31`); `SparcChannelWriter.ResolveSlotSize`
    uses the codec's max with no floor (`SparcChannelWriter.cs:170-178`). The analyzer restates
    power-of-two and `<= 8` for `SparcRing.Open*` and the `SpscRingBuffer` constructor only
    (`GeometryAnalyzer.cs:52-78,96-122`); the codec path is not analyzed and the overflow check is
    absent. The analyzer is netstandard2.0-pinned (`decisions.tsv`), so shared constants must be a
    source file, not a project reference.
11. **Hosting lifecycle.** The 10-argument runner (`SparcHostedServiceRunner.ExecuteAsync`,
    `SparcHostedServiceRunner.cs:7-19` — 11 parameters counting the stopping token, 5 delegates)
    is marshalled by two session services and two worker services. Speculative: act only when a
    third service shape appears.
12. **Endpoint surface.** `IRingBuffer` advertises a common surface, has one implementer and no
    caller (`src/Sparc.Core/IRingBuffer.cs:20`, `SpscRingBuffer.cs:49`) — remove or use it. A
    full-window reserve sets `_pendingLength = MaxPayloadSize` (`SharedRingBuffer.cs:354-359`)
    and the parameterless `CommitWrite()` publishes it (`SharedRingBuffer.cs:366-369`); only short
    commits are tested (`EndpointApiTests.cs:42-47,56-73`) — decide legal-and-documented or guard
    it. The producer stamps `Buffer.TailSequence` per message (`ProducerRole.cs:147`), a volatile
    shared read that defeats the cursor cache (`SharedRingBuffer.cs:46-54`) — measure first.

### Benchmark-skill gaps (9)

13. **No `[Benchmark(Baseline = true)]`** anywhere in `ThroughputBenchmarks.cs`: no comparison
    has a ratio anchor.
14. **Job settings below the `Short` preset.** Every class pins
    `[SimpleJob(warmupCount: 1, iterationCount: 3)]` (`ThroughputBenchmarks.cs:28,35,42,...`),
    below `Short` (5-8 s). `Program.cs:20` passes args to `BenchmarkSwitcher`, but `--job` *adds*
    a job rather than replacing the attribute, so retuning means editing eleven classes. Pick and
    document one preset in code so runs are comparable.
15. **`[Params]` on the base class** (`ThroughputBenchmarks.cs:12`, `Params(16, 64, 256, 1024,
    4096)`) forces 5 sizes × 11 classes. Move params to the classes that need them.
16. **Preview package pinned.** `BenchmarkDotNet 0.16.0-preview.2`
    (`Sparc.Benchmarks.csproj:15`). Resolve a stable version before quoting numbers.
17. **Best-of-5 without a range.** Regression keeps the minimum of 5 runs and prints one number
    (`RegressionRunner.cs:29,107-125`); the checklist wants 5 alternating runs with median +
    range. The latency runner prints spread, the regression runner does not.
18. **CI never runs the benchmarks.** `ci.yml` builds, tests, builds samples and packs, with no
    benchmark or regression gate (`ci.yml:31-50`).
19. **Multi-runtime claim not reproducible.** `docs/csharp15-adoption.md:44-46` says BDN ran with
    `--runtimes net8.0 net11.0`, but the project targets `net11.0` only and nothing re-creates the
    retarget (`Sparc.Benchmarks.csproj:4`).
20. **3 of 12 scenarios have no baseline key at HEAD** — `session-64`,
    `shared-secured-copy-64`, `shared-secured-lease-64` — and silently pass as `no baseline`
    (`perf-baseline.json` at HEAD has 9 keys, `RegressionRunner.cs:215`). The working copy already
    holds a 12-key re-capture; commit it and close this.
21. **Latency mode branched per message on the producer.** `if (_latencyMode)` sits inside the
    produce loop (`TwoThreadPump.cs:134`); the consumer already hoists it (`:171`). Select the
    pump per case instead.

### Measurement and reporting (5)

22. **In-process per-op `Stopwatch` harness.** The cross-process matrices already separated the
    cache and verify-scan effects, so this is only for finer attribution. Timing today is per run
    (`LatencyRunner.cs:136`) and per message in the pump (`TwoThreadPump.cs:136,185`). Keep it in
    `Sparc.Benchmarks`; no instrumentation in production types.
23. **Re-capture the perf baseline per machine.** Baselines are machine-specific
    (`RegressionRunner.cs:15`) and this host swings 2-5x, so the default gate is 50%
    (`RegressionRunner.cs:40`). The working copy's 12-key capture is from this host; commit it and
    treat any other machine as needing its own capture.
24. **In-process BDN params for 8/16/64 KB.** `ThroughputBenchmarks.cs:12` stops at 4 KB; the
    8 KB, 16 KB and 64 KB rows exist only in the cross-process matrices.
25. **Histogram report qualifier in runner output.** README explains the tail (p99 ≈ 15.6 ms is
    the Windows timer tick, `README.md:138-141`), but the printed histogram
    (`LatencyRunner.cs:144`) carries no qualifier.
26. **Deeper protocol-cost doc page.** README §3 has one bullet ("per-byte cost is linear",
    `README.md:136`); per-message O(payload) costs — header, fill, verify scan — have no page.

## Constraints / decisions that constrain new work

- **Public API frozen.** `ProducerSession`, `ConsumerSession`, their options/results and
  `SparcChannelReader`; `PublicAPI.*.txt` must not need new public entries. Adding a public
  member (stop reason, fill-once switch, async layer) is a decision, not a cleanup.
- **Perf invariants** (`docs/performance-invariants.md`): A5 publish-then-advance, B1
  release/acquire only at the two publication points, D2 stack-only spans and value types, D4
  preallocated allocation-free support structures per operation, D5 evidence.
- **Per message on the hot path:** no allocation, no virtual dispatch, no new branch; role
  structs pass by `ref`, never `in`.
- **Notification fence:** one full fence before the flag load in both sessions; no second fence,
  no relaxed variant.
- **Deliberate duplication.** The ring algorithm is duplicated on purpose with parallel tests,
  not abstracted (`e8f2d30`); keep the coverage if it is ever unified.
- **Channel reader stays outside the run engine:** it owns its drain and has no
  `SessionStopReason` (design doc, `Tradeoffs accepted`).
- **Failure text** stays recognizable: `stayed full`, `consumer is gone`,
  `producer stopped after`, `no messages for` (`SessionTests`).
- **Cross-process semantics:** drain after peer stop, at-least-once redelivery, structured
  results instead of exceptions.
- **This pass changed no code.** Consolidation is documentation only; each item needs its own
  change and evidence.
