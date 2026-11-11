# C# 15 and net11.0 adoption

Status: decided 2026-11-11. Applies to the 2.0 line.

## Decision

1. **Libraries target `net11.0` only.** `net8.0` was removed because .NET 8
   reached end of support on 2026-11-10, and the repository already built
   `net11.0` on every run.
2. **The language version is pinned by the target framework.** `net11.0`
   implies C# 15; `LangVersion=latest` was removed so the compiler cannot drift
   with the machine. The `netstandard2.0` analyzer project pins `15.0`
   explicitly, because the SDK default for `netstandard2.0` is C# 7.3.
3. **No C# 15 language feature was adopted.** None of the seven features
   improved performance or readability in this codebase without a public API
   change.
4. **The memory-safety preview was piloted and rejected.** It is opt-in and was
   allowed, but it needs 31 call-site changes in the lock-free path for no
   codegen benefit.

## Feature inventory

Probed with the shipped compiler (Roslyn 5.12.0, language 15.0) against the
`net11.0` reference pack; `net8.0` results use the `8.0.22` pack.

| Feature | net11.0 | net8.0 | Sites in SPARC | Decision |
|---|---|---|---|---|
| `union` | accepted | **fails** (CS0518 `IUnion`, CS0656 `UnionAttribute`) | `TryRead`/`TryPublish`/`TryPeek` return `bool` + `out` | Reject: generated unions store `object?`, so value cases box. A 24-byte box per message is ~24 MB per 1M messages against D5's 4 KiB budget. |
| `closed` | accepted; exhaustive switch compiles | **fails** (CS0656 `IsClosedTypeAttribute`) | `SessionStopReason`, `RingBufferEndpointState`, `SparcErrorCode`, `SessionWaitMode` | Reject: all four are public enums; replacing them is a public API break for no per-message gain. |
| `[with(...)]` | accepted | accepted | `List<string>` in `SparcChannelHealthCheck`; `ConcurrentBag` in `SparcInMemory` | Reject: no capacity/comparer argument is needed at these sites, so it is cosmetic. |
| Extension indexers | accepted | accepted | none found | Reject: no receiver and no need. |
| Labeled jumps | accepted | accepted | none; no `goto`, no found-flag loop, no multi-level exit | Reject: nothing to replace. |
| Static interface members | accepted | accepted | none; the only `netstandard2.0` project is the analyzer, which has no helper interfaces | Reject. |
| Memory-safety preview | accepted (`preview` + rules) | not applicable | 41 unsafe matches; 3 explicit-layout structs | Reject after pilot; see below. |

The generated-union rule is not a guess: `union IntOrString(int, string)`
compiles, and the generated struct carries `public object? Value`. A `bool`
result would box exactly like an `int`.

## Performance comparison

### Method

- Same source commit (working tree at the adopted changes minus the TFM drop),
  benchmark project and tools temporarily multi-targeted `net8.0;net11.0`.
- BenchmarkDotNet, `--runtimes net8.0 net11.0`, shared-memory copy pump,
  `[SimpleJob(warmupCount: 1, iterationCount: 3)]`, 65,536 messages per
  invocation. `net8.0` is a **control**, not a shipping target.
- Cross-process path measured on `net11.0` only, because
  `LatencyRunner` hard-codes the `net11.0` tool paths; 200,000 messages, 3
  interleaved repeats, 64 B and 16 KB.
- Decision rule: a 10% minimum detectable effect. A delta inside the confidence
  interval is reported as inconclusive, not as a win.

### In-process (ns per message, mean ± BDN 99.9% CI half-width)

| Size | .NET 8.0 | .NET 11.0 | net11 / net8 |
|---:|---:|---:|---:|
| 16 B | 2,166.9 ± 294.8 | 1,749.5 ± 246.9 | 0.81 |
| 64 B | 1,292.2 ± 273.2 | 1,132.5 ± 264.9 | 0.88 |
| 256 B | 2,074.5 ± 228.2 | 754.0 ± 170.5 | 0.36* |
| 1 KB | 1,357.8 ± 201.2 | 1,298.3 ± 229.7 | 0.96 |
| 4 KB | 1,102.6 ± 172.3 | 955.4 ± 149.9 | 0.87 |

\* The 256 B row is an outlier: .NET 8 is slower at 256 B than at 1 KB, which
inverts the expected shape and points at one disturbed iteration. With 3
iterations on a shared VM, the intervals overlap parity for 16 B, 64 B, 1 KB and
4 KB; only 256 B excludes 1.0.

**Conclusion: inconclusive at a 10% MDE.** There is no evidence that `net11.0`
regresses the ring, and no reliable evidence of a speedup. The host's
run-to-run spread is larger than the effect.

### Cross-process, net11.0

| Message | Region | Throughput | p50 | p99 | CPU prod/cons |
|---:|---:|---:|---:|---:|---:|
| 64 B | 0.3 MiB | 855,753 msg/s | 211.20 µs | 15,974.40 µs | 7/13 % |
| 16 KB | 16.1 MiB | 132,759 msg/s | 1,792.00 µs | 16,793.60 µs | 11/18 % |

These are ~46–47% of the stored README capture (1.86M / 280k msg/s) with no
source change, which is host state, not code.

### Why the regression baseline was not re-captured

A capture and the immediately following pass, on the same build, disagreed by
4×:

| Scenario | Capture | Next pass | Ratio |
|---|---:|---:|---:|
| array-copy-64 | 8,867,261 | 1,663,402 | 0.19 |
| shared-copy-64 | 13,509,242 | 3,112,247 | 0.23 |
| channel-8 | 4,344,322 | 8,170,078 | 1.88 |
| shared-secured-lease-64 | 36,715,235 | 1,938,342 | 0.05 |

8 of 11 scenarios fell below the 50% gate with no code change. Committing that
capture would encode host noise as a gate. The tracked
`benchmarks/Sparc.Benchmarks/perf-baseline.json` is unchanged; the capture is
kept at `artifacts/phase3/perf-baseline.captured.json` (git-ignored). Re-capture
on a quiet machine, as `docs/benchmarking.md` already says.

## Memory-safety pilot

On `Sparc.Core` with `LangVersion=preview` and
`Features=updated-memory-safety-rules`:

1. 8 × CS9392 on the three explicit-layout structs in `SpscRingBuffer`
   (`PaddedLong`, `PendingWriteState`, `PeekedReadState`), fixed mechanically
   with `safe`.
2. With those fixed, **31 propagation errors**: 23 × CS9362 and 8 × CS9363.
   The patterns are 7 `RingBufferRegion.TailRef`, 7 `HeadRef`,
   8 `IIpcMemoryRegion.Pointer`, 5 `Unsafe.AsRef`/span constructors, and 2
   `Initialize`/`OpenAndValidate` calls.
3. Every fix is an `unsafe { }` wrapper at a call site in the lock-free path, or
   a rewrite of an unsafe member into a safe member with an inner block.

No codegen change is possible from these rules; the diff is pure annotation
churn in the code where a mistake is most expensive. The pilot was reverted.
If the model stabilizes, revisit it as its own PR with a codemod, not as part of
a language-version change.

## Verification

- `dotnet build Sparc.slnx -c Release`: 0 warnings, 0 errors.
- `dotnet build Sparc.Samples.slnx -c Release`: 0 warnings, 0 errors.
- `dotnet msbuild -getProperty:LangVersion`: `15.0` for `Sparc.Core` and
  `Sparc.Analyzers`.
- Tests on fresh `net11.0` binaries: Unit 241 (232 passed, 9 Unix-only skipped
  on Windows), Concurrency 5/5, Fuzz 21/21, Process 12/12.
- `PublicAPI.Shipped.txt` unchanged; the PublicApiAnalyzers run with warnings as
  errors, so no public surface moved.
- No C# 15 feature was adopted, so there is no source-level IL diff to show; the
  `net11.0` source is unchanged by the TFM drop.

## What was not measured

- The lease and array BDN matrices: BenchmarkDotNet's generated build was
  blocked by a NuGet audit cache write (`NU1900` as error) in the measurement
  sandbox. The shared-memory copy matrix is the representative in-process cell.
- `net8.0` cross-process: `LatencyRunner` hard-codes `net11.0` tool paths.
- `net8.0` tests: no test project targeted `net8.0`, which is one reason the TFM
  was dropped.

## Falsifiers

- A quiet host showing a >10% `net8.0` vs `net11.0` delta at 64 B would change
  the runtime conclusion.
- A real hot-path site where a hand-written non-boxing union removes an
  allocation (none exists today) would justify a union.
- A closed hierarchy that enables measured devirtualization in a loop (none
  exists today) would justify `closed`.
- A codemod that reduces the preview's 31 sites to a handful would make the
  memory-safety model reviewable.

## Follow-ups

- Re-capture the regression baseline on a quiet machine and commit it there.
- Pin `global.json` to the resolved `11.0.100` final instead of the `rc.1`
  entry that `rollForward: latestFeature` overrides. Left unchanged here because
  it changes what CI installs.
- If the memory-safety preview is wanted anyway, do it as a standalone PR with
  the `unsafe` boundary made safe-per-member, not per-call-site.
