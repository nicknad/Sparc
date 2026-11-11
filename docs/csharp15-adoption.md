# C# 15 and net11.0 adoption

This decision is dated 2026-11-11 and applies to the 2.0 line.

## Decision

1. The libraries target `net11.0` only. This release removes `net8.0` because
   .NET 8 reached end of support on 2026-11-10, and every build produced
   `net11.0`.
2. The target framework sets the language version. `net11.0` implies C# 15.
   This release removes `LangVersion=latest` so the compiler cannot change with
   the machine. The `netstandard2.0` analyzer project pins `15.0` because the
   SDK default for `netstandard2.0` is C# 7.3.
3. This release adopts no C# 15 language feature. None of the seven features
   improved performance or readability here without a public API change.
4. The project piloted the memory-safety preview and rejected it. The feature is
   opt-in, but it needs 31 call-site changes in the lock-free path and cannot
   change codegen.

## Feature inventory

The probe used the shipped compiler, Roslyn 5.12.0 at language 15.0, against the
`net11.0` reference pack. The `net8.0` results use the `8.0.22` pack.

| Feature | net11.0 | net8.0 | Sites in SPARC | Decision |
|---|---|---|---|---|
| `union` | accepted | fails with CS0518 for `IUnion` and CS0656 for `UnionAttribute` | `TryRead`, `TryPublish`, and `TryPeek` return `bool` and use `out` parameters | Rejected. Generated unions store `object?`, so value cases box. A 24-byte box per message is about 24 MB per 1 million messages, against the 4 KiB budget in D5. |
| `closed` | accepted. The exhaustive switch compiles. | fails with CS0656 for `IsClosedTypeAttribute` | `SessionStopReason`, `RingBufferEndpointState`, `SparcErrorCode`, and `SessionWaitMode` | Rejected. All four are public enums. Replacing them would break the public API and save nothing per message. |
| `[with(...)]` | accepted | accepted | `List<string>` in `SparcChannelHealthCheck` and `ConcurrentBag` in `Sparc.InMemory` | Rejected. No site needs a capacity or comparer argument, so the change is cosmetic. |
| Extension indexers | accepted | accepted | none found | Rejected. No type here needs an indexer. |
| Labeled jumps | accepted | accepted | none. No `goto`, no found-flag loop, no multi-level exit. | Rejected. Nothing to replace. |
| Static interface members | accepted | accepted | none. The only `netstandard2.0` project is the analyzer, and it has no helper interfaces. | Rejected. |
| Memory-safety preview | accepted with `preview` and the `updated-memory-safety-rules` flag | not tested | 41 unsafe matches and 3 explicit-layout structs | Rejected after the pilot. See below. |

`union IntOrString(int, string)` compiles, and the generated struct has
`public object? Value`. A `bool` result boxes the same way an `int` does.

## Performance comparison

### Method

The measurement used the same source commit. The working tree had the adopted
changes but not the target framework drop. The benchmark project and tools
targeted both `net8.0` and `net11.0` for the run.

The benchmark ran BenchmarkDotNet with `--runtimes net8.0 net11.0`, the
shared-memory copy pump, and `[SimpleJob(warmupCount: 1, iterationCount: 3)]`.
Each invocation moved 65,536 messages. `net8.0` was a control, not a shipping
target.

The cross-process run covered `net11.0` only because `LatencyRunner` hard-codes
the `net11.0` tool paths. It moved 200,000 messages in 3 interleaved repeats at
64 B and 16 KB.

The decision rule was a 10% minimum detectable effect, called MDE below. A delta
inside the confidence interval is inconclusive, not a win.

### In-process results

Values are nanoseconds per message. The table gives the mean and the 99.9%
confidence-interval half-width that BenchmarkDotNet reports. The last column
divides the `net11.0` mean by the `net8.0` mean.

| Size | .NET 8.0 | .NET 11.0 | Ratio |
|---:|---:|---:|---:|
| 16 B | 2,166.9 ± 294.8 | 1,749.5 ± 246.9 | 0.81 |
| 64 B | 1,292.2 ± 273.2 | 1,132.5 ± 264.9 | 0.88 |
| 256 B | 2,074.5 ± 228.2 | 754.0 ± 170.5 | 0.36* |
| 1 KB | 1,357.8 ± 201.2 | 1,298.3 ± 229.7 | 0.96 |
| 4 KB | 1,102.6 ± 172.3 | 955.4 ± 149.9 | 0.87 |

\* The 256 B row is an outlier. .NET 8 is slower at 256 B than at 1 KB. That is
the opposite of the expected shape and suggests one disturbed iteration. The
intervals include 1.0 for 16 B, 64 B, 1 KB, and 4 KB. Only 256 B excludes 1.0.

The result is inconclusive at a 10% MDE. The data show no regression and no
speedup. The host's run-to-run spread is larger than the effect.

### Cross-process, net11.0

Latencies are microseconds.

| Message | Region | Throughput | 50th percentile | 99th percentile | CPU producer/consumer |
|---:|---:|---:|---:|---:|---:|
| 64 B | 0.3 MiB | 855,753 msg/s | 211.20 | 15,974.40 | 7/13% |
| 16 KB | 16.1 MiB | 132,759 msg/s | 1,792.00 | 16,793.60 | 11/18% |

These rates are about 46% to 47% of the rates in the README, 1.86 million and
280,000 msg/s. The source did not change, so the difference is host state.

### Why the regression baseline was not re-captured

A capture and the next pass on the same build differed by a factor of about 4.

| Scenario | Capture | Next pass | Ratio |
|---|---:|---:|---:|
| array-copy-64 | 8,867,261 | 1,663,402 | 0.19 |
| shared-copy-64 | 13,509,242 | 3,112,247 | 0.23 |
| channel-8 | 4,344,322 | 8,170,078 | 1.88 |
| shared-secured-lease-64 | 36,715,235 | 1,938,342 | 0.05 |

8 of 11 scenarios fell below the 50% threshold with no code change. Committing
that capture would make host noise the baseline. The tracked
`benchmarks/Sparc.Benchmarks/perf-baseline.json` is unchanged. The capture is in
the git-ignored `artifacts/phase3/perf-baseline.captured.json`. Re-capture the
baseline on a quiet machine. `docs/benchmarking.md` says the same.

## Memory-safety pilot

On `Sparc.Core` with `LangVersion=preview` and
`Features=updated-memory-safety-rules`:

1. The compiler reported 8 CS9392 errors on the three explicit-layout structs
   in `SpscRingBuffer`: `PaddedLong`, `PendingWriteState`, and `PeekedReadState`.
   Adding `safe` fixed them.
2. With those fixed, it reported 31 propagation errors: 23 CS9362 and 8 CS9363.
   The patterns are 7 `RingBufferRegion.TailRef`, 7 `HeadRef`,
   8 `IIpcMemoryRegion.Pointer`, 5 `Unsafe.AsRef` or span constructors, and 2
   `Initialize` or `OpenAndValidate` calls.
3. Every fix is an `unsafe { }` wrapper at a call site in the lock-free path, or
   a rewrite of an unsafe member into a safe member with an inner block.

These rules cannot change codegen. The diff stays in the lock-free path and adds
annotations, wrappers, or safe-member rewrites. The project reverted the pilot.
If the model stabilizes, revisit it in a separate PR with a codemod. Keep it out
of a language-version change.

## Verification

- `dotnet build Sparc.slnx -c Release` reports 0 warnings and 0 errors.
- `dotnet build Sparc.Samples.slnx -c Release` reports 0 warnings and 0 errors.
- `dotnet msbuild -getProperty:LangVersion` returns `15.0` for `Sparc.Core` and
  `Sparc.Analyzers`.
- Unit reports 241 tests, with 232 passed and 9 Unix-only tests skipped on
  Windows. Concurrency passes all 5 tests, Fuzz passes all 21, and Process passes
  all 12.
- `PublicAPI.Shipped.txt` is unchanged. The PublicApiAnalyzers run with warnings
  as errors, so the public API did not change.
- Because this release adopts no C# 15 language feature, there is no IL diff in
  the compiled output. The target framework drop did not change the `net11.0`
  source.

## What this report does not cover

- This report leaves out the lease and array BenchmarkDotNet matrices. A NuGet
  audit cache write blocked BenchmarkDotNet's generated build, and the sandbox
  reported NU1900 as an error. The shared-memory copy matrix is the in-process
  case this report covers.
- This report leaves out `net8.0` cross-process because `LatencyRunner`
  hard-codes the `net11.0` tool paths.
- No test project targeted `net8.0`, which is one reason the target framework
  was dropped.

## Falsifiers

- A quiet host showing a difference larger than 10% between `net8.0` and
  `net11.0` at 64 B would change the runtime conclusion.
- A hot-path site where a hand-written non-boxing union removes an allocation
  would justify a union. None exists today.
- A closed hierarchy that enables measured devirtualization in a loop would
  justify `closed`. None exists today.
- A codemod that reduces the preview's 31 sites to a handful would make the
  memory-safety model reviewable.

## Follow-ups

- Re-capture the regression baseline on a quiet machine and commit it there.
- Pin `global.json` to the resolved `11.0.100` final instead of the `rc.1` entry
  that `rollForward: latestFeature` overrides. The change stays out because it
  alters what CI installs.
- If you want the memory-safety preview anyway, do it in a separate PR. Make
  each member safe with an inner `unsafe` block instead of wrapping each call
  site.
