# Benchmarking

Three harnesses live in `benchmarks/Sparc.Benchmarks`. They answer different
questions and have different noise characteristics.

| Harness | Question | Command |
|---|---|---|
| BenchmarkDotNet matrix | How fast is each transport in-process, with statistics? | `dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --filter *` |
| Latency runner | What are the one-way percentiles and cross-process rates? | `... -- --latency ...` |
| Regression runner | Did a change regress the in-process pumps? | `... -- --regression` |

All three run against the release build. The host used for the numbers below is
a shared Windows 11 VM (12th Gen i7-1260P, 12 physical / 16 logical cores,
15.69 GB RAM); the README itself warns that confidence intervals are wide, so
treat everything as directional.

---

## 1. BenchmarkDotNet matrix

Eleven transports, five message sizes (16 B, 64 B, 256 B, 1 KB, 4 KB), each
measured with two dedicated threads pumping 65,536 messages per invocation:

* in-process SPSC ring buffer (`SpscArrayBenchmarks`)
* in-process SPSC shared memory (`SpscSharedMemoryBenchmarks`)
* lease-API variants of both (`SpscArrayLeaseBenchmarks`,
  `SpscSharedMemoryLeaseBenchmarks`)
* DACL-secured shared memory (`SpscSecuredSharedMemoryBenchmarks`,
  `SpscSecuredSharedMemoryLeaseBenchmarks`; the region is created with
  `WindowsSectionSecurity.CurrentUserOnly`, on non-Windows these fall back to
  the plain pump because there is no equivalent at this layer)
* typed channel layer over the ring (`SpscChannelBenchmarks`)
* `ConcurrentQueue<T>` + lock, `Channel<T>`, named pipe, TCP loopback

```powershell
# everything
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --filter *

# just the ring transports
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --filter "*Spsc*"

# plain vs secured shared memory only
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --filter "*Spsc*SharedMemory*"
```

Representative copy-API results from the README (means, ns per message):

| Transport | 16 B | 64 B | 256 B | 1 KB | 4 KB | Alloc/op @64 B |
|---|---:|---:|---:|---:|---:|---:|
| In-process SPSC ring buffer | 970 | 734 | 1,069 | 658 | 893 | 0 |
| In-process SPSC shared memory | 686 | 520 | 457 | 241 | 578 | 0 |
| `ConcurrentQueue` + lock | 3,161 | 2,578 | 4,145 | 3,007 | 11,582 | 88 B |
| `Channel<T>` | 2,425 | 1,626 | 2,030 | 2,214 | 2,615 | 88 B |
| Named pipe | 40.9 us | 30.8 us | 30.7 us | 37.7 us | 35.5 us | 0 |
| TCP loopback | 53.9 us | 44.7 us | 74.8 us | 48.3 us | 45.4 us | 0 |

The queue/channel baselines allocate a `byte[]` per message because their APIs
carry reference types; that is part of their design, not an implementation
accident. The comparison is a fixed-size SPSC workload with the same two
threads, the same batch, and equivalent length-prefixed framing.

The lease variants were added so the zero-copy path is measured with the same
machinery. On the shared VM the copy/lease medians overlap (they land on both
sides of each other across sizes), which is consistent with the cross-process
result that zero-copy mainly removes consumer-side passes rather than the
cursor handoff.

The secured variants exist to show that transport security is a **setup-time**
cost: the DACL is attached once in `CreateFileMapping`, and the measured loop is
the identical ring code. On this VM the plain/secured pairs land inside the
documented 2-5x scheduling noise and flip sign between captures; see the README
benchmark section for the capture caveat.

---

## 2. Latency runner

`--latency` mode has two flavors: in-process transports and the real
cross-process measurement that spawns the producer/consumer executables.

```powershell
# in-process one-way percentiles, all transports
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport all --count 200000 --size 64

# real cross-process (spawns the CLIs)
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport shared-xproc --count 500000 --size 64

# payload sweep: latency percentiles + throughput per size, interleaved repeats
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport shared-xproc --sizes 16,64,256,1024,4096,16384 --count 500000 --repeats 3

# speed-mismatch scenarios
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport shared-xproc --sizes 64 --count 50000 --repeats 3 --consumer-delay-us 100
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --latency --transport shared-xproc --sizes 64 --count 50000 --repeats 3 --producer-delay-us 50
```

Knobs:

| Flag | Effect |
|---|---|
| `--sizes a,b,c` | Sweep payload sizes (cross-process only) |
| `--repeats n` | Rounds, round-robin across sizes |
| `--capacity n` | Slot count; **this is what separates payload from footprint** |
| `--slot-size n` | Override the derived slot size (alignment experiments) |
| `--no-verify` / `--no-verify-payload` | Remove structural checks / the O(payload) scan |
| `--spin-only` / `--notify` | Change the endpoint wait mode |
| `--producer-delay-us` / `--consumer-delay-us` | Pace one side |

### Cross-process results (500k messages, 3 runs, interleaved)

| Message | Region | Throughput | Data | p50 (us) | p99 (us) | CPU prod/cons |
|---|---|---:|---:|---:|---:|---:|
| 16 B | 0.3 MiB | 1.20M msg/s | 18.3 MiB/s | 86.40 | 15564.80 | 6/13 % |
| 64 B | 0.3 MiB | 1.86M msg/s | 113.2 MiB/s | 147.20 | 9830.40 | 9/11 % |
| 256 B | 0.3 MiB | 1.30M msg/s | 316.6 MiB/s | 230.40 | 14745.60 | 9/20 % |
| 1 KB | 1.1 MiB | 1.69M msg/s | 1,646.8 MiB/s | 230.40 | 9420.80 | 12/29 % |
| 4 KB | 4.1 MiB | 1.92M msg/s | 7,508.9 MiB/s | 115.20 | 4710.40 | 18/45 % |
| 16 KB | 16.1 MiB | 280k msg/s | 4,379.6 MiB/s | 1331.20 | 15564.80 | 11/28 % |

Read the throughput as the consumer's end-to-end rate; the producer's
`activeElapsed` window excludes waiting for the consumer to attach.

### The 16 KB "cliff" explained

Changing `--size` at the default capacity also changes the region footprint.
Measured with `--capacity` as a separate variable:

| Configuration | Data rate | Message rate |
|---|---:|---:|
| 4 KB, 4 MiB region | 5.2 GiB/s | 1.34M msg/s |
| 8 KB, 4 MiB region | 5.3 GiB/s | 675k msg/s |
| 16 KB, 4 MiB region | 5.5 GiB/s | 354k msg/s |
| 16 KB, 16 MiB region | 3.1 GiB/s | 197k msg/s |
| 16 KB, 16 MiB region, `--no-verify-payload` | 5.4 GiB/s | 348k msg/s |

At a fixed footprint the data rate is flat: the per-byte cost is linear. The
16 KB row loses ~1.8x when the region leaves the caches and another ~1.8x when
the verification scan runs. See
[performance-invariants.md](performance-invariants.md), section C6.

### Speed mismatch and wait modes

One endpoint paced (`--delay-us`), 64 B:

| Scenario | Producer | Consumer | p50 | Notes |
|---|---:|---:|---:|---|
| Both paced to 10k | 9,541 | 10,001 | 91.8 ms | queue operating point |
| Producer >> consumer (`--consumer-delay-us 100`) | 9,508 | 10,001 | 91.8 ms | full ring; latency = capacity x period |
| Producer << consumer (`--producer-delay-us 50`) | 20,000 | 23,167 | 5.1 ms | empty ring; idle-detection latency |

The last row is the one the wait modes target:

| Wait mode | p50 | Consumer CPU |
|---|---:|---:|
| `SpinThenSleep` (default) | 6.45 ms | ~2% |
| `--spin-only` | 3.9 us | pinned core |
| `--notify` | 7.6 us | ~2% |

`--notify` uses named OS semaphores (Windows semaphores, POSIX named semaphores on Unix).

---

## 3. Regression runner

```powershell
# compare against the stored baseline (exits 1 on regression)
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --regression

# (re)capture the baseline for this machine
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --regression --save-baseline

# loosen the gate
dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --regression --tolerance 0.8
```

What it does:

* Eleven scenarios: copy and lease APIs for the array and shared-memory buffers
  at 64 B, 4 KB and 16 KB, the same copy/lease pair over a DACL-secured
  shared-memory region at 64 B (`shared-secured-copy-64`,
  `shared-secured-lease-64`), plus the typed channel layer at 8 B.
* Message counts are chosen so each measured run lasts at least a few hundred
  milliseconds (2M/500k/150k messages).
* All pumps live for the whole run and rounds interleave scenarios, so host
  drift affects every scenario equally.
* The metric is **best of 5 runs** (the least-disturbed sample), compared to
  `benchmarks/Sparc.Benchmarks/perf-baseline.json`.
* The shipped baseline predates the two secured scenarios; until it is
  re-captured with `--save-baseline`, they are measured and reported as
  `no baseline` (which passes) side by side with their plain twins.
* Default tolerance is 50%; the output explicitly warns that a shared VM can
  swing individual scenarios 2-5x and points at BDN for authoritative numbers.

The baseline is machine-specific. Re-capture it after a hardware, OS, or
runtime change, and treat failures on another host as directional.

---

## 4. Measurement traps

1. **Payload is not footprint.** Vary one at a time; use `--capacity` and the
   `Region` column.
2. **The timer tick is not the ring.** Default consumers that catch up fall
   into `SpinWait` sleep and show p99 ~= 15.6 ms on Windows. Use
   `--spin-only`/`--notify` if that floor matters.
3. **VM stalls are not the ring.** p99.9/max ~= 0.3 s recur in every capture on
   this host.
4. **Producer elapsed includes attach wait.** Use `activeElapsed` (printed) for
   send rates.
5. **Consumer stream window vs wall clock.** `elapsed` is first-to-last
   message; `wallElapsed` includes idle time.
6. **Percentiles are approximate.** The histogram reports the midpoint of the
   sub-bucket holding the rank (clamped to min/max), bounding the error at
   1/32 of the value. Use `Merge` to pool per-run histograms instead of taking
   the median of per-run percentiles.
7. **Allocations are measured after warm-up.** The concurrency test warms up
   100k iterations before the < 4 KiB assertion; BDN attributes allocations
   per operation, not per process.
8. **Don't compare across harnesses.** BDN pumps and the cross-process CLI
   path differ in geometry rounding, verification and process count; the
   numbers are only meaningful within one harness.

## 5. Adding a scenario

* In-process transport: subclass `TwoThreadPump` (implement `TryPublish`,
  `TryConsume`, `RequiredDestinationSize`), add a `ThroughputBenchmarkBase`
  subclass, and optionally add it to the regression matrix in
  `RegressionRunner.cs`.
* Cross-process scenario: add a flag to `LatencyRunner` and pass it through to
  the producer/consumer argument lists.
* Keep the invariants from [performance-invariants.md](performance-invariants.md)
  section G in mind: separate the variables, interleave, and report the right
  window.
