# Contributing

Thanks for taking a look at SPARC. This document covers the practical parts:
building, testing, and what a good change looks like.

## Prerequisites

* The .NET SDK pinned in `global.json` (currently .NET 11 RC1). `dotnet`
  downloads it automatically if `rollForward` can satisfy the version.
* Windows for the full test suite. `Sparc.ConcurrencyTests` (shared-memory half)
  and `Sparc.ProcessTests` need named memory-mapped files; the Unix transport
  tests need Linux/macOS and run in CI's Ubuntu lane.

## Build and test

```powershell
dotnet build Sparc.slnx -c Release          # warnings are errors
dotnet test  Sparc.slnx -c Release          # unit + concurrency + fuzz + process tests
dotnet build Sparc.Samples.slnx -c Release  # samples live in a separate solution
```

The library targets `net8.0;net11.0`; apps, tests and benchmarks target
`net11.0`. Warnings are errors everywhere, and `Microsoft.CodeAnalysis.NetAnalyzers`
plus `Meziantou.Analyzer` run on every build, so a clean local build is the
first quality gate.

## Benchmarks

```powershell
# BenchmarkDotNet matrix (9 transports x 5 sizes)
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --filter *

# cross-process latency/throughput sweep
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport shared-xproc --sizes 16,64,256,1024,4096,16384 --count 500000 --repeats 3

# in-process regression check (exits non-zero on regression)
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --regression
```

Benchmark baselines are machine-specific: re-capture with `--save-baseline`
before trusting a comparison on a new host, and read
[docs/benchmarking.md](docs/benchmarking.md) for the measurement traps.

## What a good change looks like

* **Tests come with the change.** Algorithm or protocol changes need unit tests;
  changes to the shared-memory path should keep `Sparc.ConcurrencyTests` green;
  lifecycle changes belong in `Sparc.ProcessTests`.
* **Respect the invariants.** `docs/performance-invariants.md` explains why the
  hot path is shaped the way it is. If a change violates an invariant, the
  change needs a measurement and an update to that document.
* **Keep the hot path allocation-free.** After warm-up, write/read must not
  allocate; `SteadyStateDoesNotAllocate` enforces a < 4 KiB budget.
* **Document public API changes.** Packable projects generate XML docs and the
  docs in `docs/` are part of the deliverable.
* **One logical change per pull request.** Keep refactors separate from
  behaviour changes and from benchmark refreshes.
* **Commit messages** are imperative, explain the why, and include measured
  numbers when the change is performance-related (see `git log` for examples).
* **No new dependencies without a reason.** The core libraries intentionally
  depend only on `Microsoft.Extensions.*` abstractions; optional integration
  packages may add one, for example `Sparc.Serialization` on
  `SerializerFoundation`.

## Reporting bugs and vulnerabilities

* Bugs and feature requests: use the issue templates.
* Security: follow [SECURITY.md](SECURITY.md); do not open a public issue.

## License

By contributing you agree that your contribution is licensed under the
[MIT License](LICENSE) that covers this repository.
