using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;
using Sparc.Benchmarks.Pumps;

namespace Sparc.Benchmarks;

/// <summary>
/// In-process performance regression check: runs a fixed matrix of two-thread
/// pumps, compares throughput against a stored baseline and exits non-zero when
/// a scenario drops below the tolerance.
/// </summary>
/// <remarks>
/// <para>
/// The baseline is machine-specific (captured with <c>--save-baseline</c>), so
/// treat failures on a different host as directional and re-capture. The matrix
/// covers the copy and lease APIs for the managed array and shared-memory
/// buffers at 64 B, 4 KB and 16 KB.
/// </para>
/// <code>
/// dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --regression
/// dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --regression --save-baseline
/// dotnet run -c Release --project benchmarks/Sparc.Benchmarks -- --regression --tolerance 0.8
/// </code>
/// </remarks>
internal static class RegressionRunner
{
    private const int MeasuredRuns = 5;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public static int Run(string[] args)
    {
        bool save = args.Contains("--save-baseline");
        double tolerance = 0.50;
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] is "--tolerance" && i + 1 < args.Length)
            {
                tolerance = double.Parse(args[++i], CultureInfo.InvariantCulture);
            }
        }

        if (tolerance is <= 0 or > 1)
        {
            Console.Error.WriteLine("--tolerance must be in (0, 1].");
            return 2;
        }

        string baselinePath = Path.Combine(
            FindRepoRoot(), "benchmarks", "Sparc.Benchmarks", "perf-baseline.json");

        // Message counts are chosen so each measured run lasts a few hundred
        // milliseconds at minimum: shorter batches on a shared VM are dominated
        // by scheduling noise.
        (string Name, int Size, int Messages, Func<TwoThreadPump> Create)[] scenarios =
        [
            ("array-copy-64", 64, 2_000_000, () => new SpscArrayPump(64)),
            ("array-lease-64", 64, 2_000_000, () => new SpscLeasePump(64)),
            ("channel-8", 8, 2_000_000, () => new SpscChannelPump(8)),
            ("shared-copy-64", 64, 2_000_000, () => new SpscSharedMemoryPump(64)),
            ("shared-lease-64", 64, 2_000_000, () => SpscSharedMemoryPump.CreateLease(64)),
            ("shared-secured-copy-64", 64, 2_000_000, () => SpscSharedMemoryPump.CreateSecured(64)),
            ("shared-secured-lease-64", 64, 2_000_000, () => SpscSharedMemoryPump.CreateSecuredLease(64)),
            ("shared-copy-4k", 4096, 500_000, () => new SpscSharedMemoryPump(4096)),
            ("shared-lease-4k", 4096, 500_000, () => SpscSharedMemoryPump.CreateLease(4096)),
            ("shared-copy-16k", 16384, 150_000, () => new SpscSharedMemoryPump(16384)),
            ("shared-lease-16k", 16384, 150_000, () => SpscSharedMemoryPump.CreateLease(16384)),
        ];

        Console.WriteLine(
            $"regression run: {scenarios.Length} scenarios, runs={MeasuredRuns} (best of), " +
            $"machine=\"{RuntimeInformation.OSDescription}\", cores={Environment.ProcessorCount}");
        Console.WriteLine();

        // All pumps live for the whole run and rounds interleave the scenarios,
        // so CPU/VM drift affects every scenario the same way instead of
        // biasing whichever one happens to run during a busy period.
        TwoThreadPump[] pumps = scenarios.Select(s => s.Create()).ToArray();
        try
        {
            for (int i = 0; i < pumps.Length; i++)
            {
                pumps[i].Run(scenarios[i].Messages, scenarios[i].Size); // warmup, not measured
            }

            double[][] seconds = new double[scenarios.Length][];
            for (int i = 0; i < scenarios.Length; i++)
            {
                seconds[i] = new double[MeasuredRuns];
            }

            for (int round = 0; round < MeasuredRuns; round++)
            {
                for (int i = 0; i < scenarios.Length; i++)
                {
                    seconds[i][round] = pumps[i].Run(scenarios[i].Messages, scenarios[i].Size);
                }
            }

            Dictionary<string, ScenarioResult> results = new(StringComparer.Ordinal);
            for (int i = 0; i < scenarios.Length; i++)
            {
                // Best of N: the fastest run is the one least disturbed by VM
                // scheduling, which makes comparisons across sessions stabler
                // than a mean or median on a shared host.
                double best = seconds[i].Min();
                results[scenarios[i].Name] = new ScenarioResult(
                    MessagesPerSecond: scenarios[i].Messages / best,
                    NanosecondsPerMessage: best / scenarios[i].Messages * 1_000_000_000.0,
                    MebibytesPerSecond: scenarios[i].Messages * (double)scenarios[i].Size / best / (1024 * 1024));
            }

            return SaveOrCompare(save, tolerance, baselinePath, results);
        }
        finally
        {
            foreach (TwoThreadPump pump in pumps)
            {
                pump.Dispose();
            }
        }
    }

    private static int SaveOrCompare(
        bool save,
        double tolerance,
        string baselinePath,
        Dictionary<string, ScenarioResult> results)
    {
        if (save)
        {
            Baseline baseline = new()
            {
                Machine = $"{RuntimeInformation.OSDescription} / {Environment.ProcessorCount} cores",
                CapturedUtc = DateTimeOffset.UtcNow,
                Scenarios = results,
            };
            File.WriteAllText(baselinePath, JsonSerializer.Serialize(baseline, JsonOptions));
            Console.WriteLine($"baseline saved: {baselinePath}");
            Console.WriteLine();
            Print(results, baseline: null, tolerance);
            return 0;
        }

        if (!File.Exists(baselinePath))
        {
            Console.Error.WriteLine($"no baseline at {baselinePath}; run with --save-baseline first.");
            Console.WriteLine();
            Print(results, baseline: null, tolerance);
            return 2;
        }

        Baseline? saved = JsonSerializer.Deserialize<Baseline>(File.ReadAllText(baselinePath), JsonOptions);
        if (saved?.Scenarios is null || saved.Scenarios.Count == 0)
        {
            Console.Error.WriteLine($"baseline at {baselinePath} is empty or invalid.");
            return 2;
        }

        Console.WriteLine($"baseline: \"{saved.Machine}\", captured {saved.CapturedUtc:u}");
        Console.WriteLine();

        int failures = Print(results, saved.Scenarios, tolerance);
        return failures == 0 ? 0 : 1;
    }

    private static int Print(
        Dictionary<string, ScenarioResult> current,
        Dictionary<string, ScenarioResult>? baseline,
        double tolerance)
    {
        Console.WriteLine(
            $"{"scenario",-18} {"msg/s",13} {"ns/msg",9} {"MiB/s",10} {"ratio",7}  verdict");
        Console.WriteLine(new string('-', 72));

        int failures = 0;
        foreach ((string name, ScenarioResult result) in current)
        {
            string ratio = "-";
            string verdict = string.Empty;

            if (baseline is not null)
            {
                if (baseline.TryGetValue(name, out ScenarioResult? reference) && reference.MessagesPerSecond > 0)
                {
                    double value = result.MessagesPerSecond / reference.MessagesPerSecond;
                    ratio = value.ToString("F2", CultureInfo.InvariantCulture);
                    if (value < tolerance)
                    {
                        verdict = "REGRESSION";
                        failures++;
                    }
                    else
                    {
                        verdict = value > 1.25 ? "faster" : "ok";
                    }
                }
                else
                {
                    verdict = "no baseline";
                }
            }

            Console.WriteLine(
                $"{name,-18} {result.MessagesPerSecond,13:N0} {result.NanosecondsPerMessage,9:F0} " +
                $"{result.MebibytesPerSecond,10:F1} {ratio,7}  {verdict}");
        }

        Console.WriteLine();
        Console.WriteLine($"throughput must stay >= {tolerance:P0} of the baseline for a pass.");
        Console.WriteLine(
            "note: on a shared/virtualized host, scheduling stalls can swing individual scenarios " +
            "by 2-5x; treat borderline ratios as noise and re-capture with --save-baseline on a quiet machine. " +
            "The BenchmarkDotNet suite is the authoritative measurement.");

        if (baseline is not null)
        {
            Console.WriteLine(failures == 0
                ? "PASS"
                : $"FAIL: {failures} scenario(s) below tolerance");
        }

        return failures;
    }

    private static string FindRepoRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);

        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "Sparc.slnx")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new InvalidOperationException("Could not locate the repository root (Sparc.slnx).");
    }

    private sealed class Baseline
    {
        public string Machine { get; set; } = string.Empty;

        public DateTimeOffset CapturedUtc { get; set; }

        public Dictionary<string, ScenarioResult> Scenarios { get; set; } = new(StringComparer.Ordinal);
    }

    private sealed record ScenarioResult(
        double MessagesPerSecond,
        double NanosecondsPerMessage,
        double MebibytesPerSecond);
}
