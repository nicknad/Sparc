using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using Sparc.Benchmarks.Pumps;
using Sparc.Core;

namespace Sparc.Benchmarks;

/// <summary>
/// Small custom harness for one-way latency percentiles (p50/p90/p95/p99/p99.9).
/// For the real cross-process measurement use <c>--transport shared-xproc</c>,
/// which runs the Producer and Consumer executables against the same region.
/// <c>--sizes 16,64,256,1024,4096,16384</c> sweeps message sizes and prints a
/// latency/throughput table (medians over <c>--repeats</c> runs), and
/// <c>--producer-delay-us</c>/<c>--consumer-delay-us</c> simulate a slow
/// endpoint so producer-vs-consumer speed mismatches can be measured.
/// </summary>
internal static class LatencyRunner
{
    private static readonly TimeSpan RegexTimeout = TimeSpan.FromSeconds(1);

    private static readonly Regex ProducerPattern = new(
        @"produced=(?<count>\d+) .* throughput=(?<throughput>\d+) msg/s dataThroughput=(?<mib>[\d.]+) MiB/s",
        RegexOptions.Compiled,
        RegexTimeout);

    private static readonly Regex ConsumerPattern = new(
        @"consumed=(?<count>\d+) bytes=(?<bytes>\d+) .* throughput=(?<throughput>\d+) msg/s dataThroughput=(?<mib>[\d.]+) MiB/s",
        RegexOptions.Compiled,
        RegexTimeout);

    private static readonly Regex ProducerReadyPattern = new(
        @"capacity=(?<capacity>\d+) slotSize=(?<slot>\d+)",
        RegexOptions.Compiled,
        RegexTimeout);

    private static readonly Regex LatencyPattern = new(
        @"min=(?<min>[\d.]+) mean=(?<mean>[\d.]+) p50=(?<p50>[\d.]+) p90=(?<p90>[\d.]+) " +
        @"p95=(?<p95>[\d.]+) p99=(?<p99>[\d.]+) p99\.9=(?<p99_9>[\d.]+) max=(?<max>[\d.]+) " +
        @"\(n=(?<n>\d+)\)",
        RegexOptions.Compiled,
        RegexTimeout);

    public static void Run(string[] args)
    {
        string transport = "array";
        int size = 64;
        long count = 200_000;
        int[]? sizes = null;
        int repeats = 1;
        int capacity = RingBufferLayout.DefaultCapacity;
        int slotSize = 0;
        int producerDelayUs = 0;
        int consumerDelayUs = 0;
        bool verify = true;
        bool verifyPayload = true;
        bool spinOnly = false;
        bool notify = false;

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--latency":
                    break; // Mode selector consumed by Program.Main.
                case "--transport":
                    transport = args[++i];
                    break;
                case "--size":
                    size = int.Parse(args[++i], CultureInfo.InvariantCulture);
                    break;
                case "--count":
                    count = long.Parse(args[++i], CultureInfo.InvariantCulture);
                    break;
                case "--sizes":
                    sizes = args[++i].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                        .Select(part => int.Parse(part, CultureInfo.InvariantCulture))
                        .ToArray();
                    break;
                case "--repeats":
                    repeats = int.Parse(args[++i], CultureInfo.InvariantCulture);
                    break;
                case "--capacity":
                    capacity = int.Parse(args[++i], CultureInfo.InvariantCulture);
                    break;
                case "--slot-size":
                    slotSize = int.Parse(args[++i], CultureInfo.InvariantCulture);
                    break;
                case "--no-verify":
                    verify = false;
                    break;
                case "--no-verify-payload":
                    verifyPayload = false;
                    break;
                case "--spin-only":
                    spinOnly = true;
                    break;
                case "--notify":
                    notify = true;
                    break;
                case "--producer-delay-us":
                    producerDelayUs = int.Parse(args[++i], CultureInfo.InvariantCulture);
                    break;
                case "--consumer-delay-us":
                    consumerDelayUs = int.Parse(args[++i], CultureInfo.InvariantCulture);
                    break;
                default:
                    throw new ArgumentException($"Unknown argument '{args[i]}'.", nameof(args));
            }
        }

        if (sizes is not null)
        {
            if (transport is not "shared-xproc")
            {
                throw new ArgumentException(
                    "--sizes is only supported for --transport shared-xproc.", nameof(args));
            }

            ValidateRepeats(repeats);
            RunCrossProcessSweep(
                sizes, count, repeats, capacity, slotSize, producerDelayUs, consumerDelayUs,
                verify, verifyPayload, spinOnly, notify);
            return;
        }

        if (transport is "shared-xproc")
        {
            RunCrossProcess(
                size, count, capacity, slotSize, producerDelayUs, consumerDelayUs,
                verify, verifyPayload, spinOnly, notify, echo: true);
            return;
        }

        string[] transports = transport is "all"
            ? ["array", "shared", "lock", "channel", "pipe", "tcp"]
            : [transport];

        foreach (string name in transports)
        {
            RunInProcess(name, size, count);
        }
    }

    private static void RunInProcess(string transport, int size, long count)
    {
        using TwoThreadPump pump = transport switch
        {
            "array" => new SpscArrayPump(size),
            "shared" => new SpscSharedMemoryPump(size),
            "lock" => new LockQueuePump(),
            "channel" => new ChannelPump(),
            "pipe" => new NamedPipePump(),
            "tcp" => new TcpLoopbackPump(),
            _ => throw new ArgumentException($"Unknown transport '{transport}'.", nameof(transport)),
        };

        Stopwatch stopwatch = Stopwatch.StartNew();
        var histogram = pump.RunLatency(count, size);
        stopwatch.Stop();

        double perSecond = stopwatch.Elapsed.TotalSeconds > 0 ? count / stopwatch.Elapsed.TotalSeconds : 0;
        Console.WriteLine(
            $"transport={transport} size={size} count={count} elapsed={stopwatch.Elapsed.TotalSeconds:F3}s " +
            $"throughput={perSecond:F0} msg/s (both ends in one process)");
        Console.WriteLine(histogram.ToMicrosecondsReport(Stopwatch.Frequency));
        Console.WriteLine();
    }

    private static void RunCrossProcessSweep(
        int[] sizes,
        long count,
        int repeats,
        int capacity,
        int slotSize,
        int producerDelayUs,
        int consumerDelayUs,
        bool verify,
        bool verifyPayload,
        bool spinOnly,
        bool notify)
    {
        Console.WriteLine(
            $"transport=shared-xproc sweep sizes=[{string.Join(", ", sizes.Select(FormatSize))}] " +
            $"count={count} repeats={repeats} capacity={capacity} " +
            (slotSize > 0 ? $"slotSize={slotSize}" : "slotSize=auto") +
            (producerDelayUs > 0 ? $" producer-delay={producerDelayUs}us" : string.Empty) +
            (consumerDelayUs > 0 ? $" consumer-delay={consumerDelayUs}us" : string.Empty) +
            (!verify ? " verify=off" : !verifyPayload ? " payloadScan=off" : string.Empty) +
            (spinOnly ? " spin-only" : string.Empty) +
            (notify ? " notify" : string.Empty));
        Console.WriteLine();

        // Round-robin over sizes so every size sees the same machine conditions;
        // running all repeats of one size back to back biases the last size.
        List<CrossProcessRun>[] runsBySize = sizes.Select(_ => new List<CrossProcessRun>()).ToArray();
        for (int round = 1; round <= repeats; round++)
        {
            for (int i = 0; i < sizes.Length; i++)
            {
                CrossProcessRun run = RunCrossProcess(
                    sizes[i], count, capacity, slotSize, producerDelayUs, consumerDelayUs,
                    verify, verifyPayload, spinOnly, notify, echo: false)
                    ?? throw new InvalidOperationException($"Run for size {sizes[i]} did not complete.");
                runsBySize[i].Add(run);

                Console.WriteLine(
                    $"  round={round}/{repeats} size={FormatSize(sizes[i]),-6} region={run.RegionMib,6:F1}MiB " +
                    $"producer={run.ProducerMsgPerSec,-12:N0} consumer={run.ConsumerMsgPerSec,-12:N0} msg/s " +
                    $"p50={run.Latency.P50:F2}us p99={run.Latency.P99:F2}us");
            }
        }

        Console.WriteLine();
        Console.WriteLine("| Message | Region    | Throughput  | Spread          | Data       | p50      | p90      | p95      | p99      | p99.9    | max        | CPU prod/cons |");
        Console.WriteLine("|---------|-----------|-------------|-----------------|------------|----------|----------|----------|----------|----------|------------|---------------|");
        for (int i = 0; i < sizes.Length; i++)
        {
            Console.WriteLine(FormatSweepRow(sizes[i], runsBySize[i]));
        }

        Console.WriteLine();
        Console.WriteLine("Throughput is the consumer's; latencies are the median of each run's percentile; CPU is of one core.");
        Console.WriteLine("Region = 192 + capacity x slotSize; throughput compares only at equal region size for cache-residency reasons.");
    }

    private static string FormatSweepRow(int size, List<CrossProcessRun> runs)
    {
        double throughput = Median(runs.Select(r => r.ConsumerMsgPerSec));
        double throughputMin = runs.Min(r => r.ConsumerMsgPerSec);
        double throughputMax = runs.Max(r => r.ConsumerMsgPerSec);
        double data = Median(runs.Select(r => r.ConsumerMibPerSec));
        double producerCpu = Median(runs.Select(r => r.ProducerCpuPercent));
        double consumerCpu = Median(runs.Select(r => r.ConsumerCpuPercent));

        double regionMib = Median(runs.Select(r => r.RegionMib));

        StringBuilder sb = new(260);
        sb.Append("| ").Append(FormatSize(size).PadRight(7));
        sb.Append("| ").Append($"{regionMib:F1} MiB".PadRight(9));
        sb.Append("| ").Append(FormatCount(throughput).PadRight(10)).Append("msg/s ");
        sb.Append("| ").Append($"{FormatCount(throughputMin)}-{FormatCount(throughputMax)}".PadRight(15));
        sb.Append("| ").Append(data.ToString("F1", CultureInfo.InvariantCulture).PadLeft(8)).Append(" MiB/s ");
        sb.Append("| ").Append(Median(runs.Select(r => r.Latency.P50)).ToString("F2", CultureInfo.InvariantCulture).PadLeft(8));
        sb.Append(" | ").Append(Median(runs.Select(r => r.Latency.P90)).ToString("F2", CultureInfo.InvariantCulture).PadLeft(8));
        sb.Append(" | ").Append(Median(runs.Select(r => r.Latency.P95)).ToString("F2", CultureInfo.InvariantCulture).PadLeft(8));
        sb.Append(" | ").Append(Median(runs.Select(r => r.Latency.P99)).ToString("F2", CultureInfo.InvariantCulture).PadLeft(8));
        sb.Append(" | ").Append(Median(runs.Select(r => r.Latency.P999)).ToString("F2", CultureInfo.InvariantCulture).PadLeft(8));
        sb.Append(" | ").Append(Median(runs.Select(r => r.Latency.Max)).ToString("F2", CultureInfo.InvariantCulture).PadLeft(10));
        sb.Append(" | ").Append(FormattableString.Invariant($"{producerCpu:F0}/{consumerCpu:F0} %"));
        sb.Append(" |");
        return sb.ToString();
    }

    private static void ValidateRepeats(int repeats) =>
        ArgumentOutOfRangeException.ThrowIfLessThan(repeats, 1);

    private static CrossProcessRun? RunCrossProcess(
        int size,
        long count,
        int capacity,
        int slotSize,
        int producerDelayUs,
        int consumerDelayUs,
        bool verify,
        bool verifyPayload,
        bool spinOnly,
        bool notify,
        bool echo)
    {
        string name = "spsc-latency-" + Guid.NewGuid().ToString("N");
        string repoRoot = FindRepoRoot();

#if DEBUG
        const string configuration = "Debug";
#else
        const string configuration = "Release";
#endif

        string producerDll = Path.Combine(repoRoot, "src", "Sparc.Producer", "bin", configuration, "net11.0", "Sparc.Producer.dll");
        string consumerDll = Path.Combine(repoRoot, "src", "Sparc.Consumer", "bin", configuration, "net11.0", "Sparc.Consumer.dll");

        List<string> producerArgs =
        [
            "--name", name,
            "--count", count.ToString(CultureInfo.InvariantCulture),
            "--size", size.ToString(CultureInfo.InvariantCulture),
            "--capacity", capacity.ToString(CultureInfo.InvariantCulture),
            "--delay-us", producerDelayUs.ToString(CultureInfo.InvariantCulture),
        ];
        if (slotSize > 0)
        {
            producerArgs.Add("--slot-size");
            producerArgs.Add(slotSize.ToString(CultureInfo.InvariantCulture));
        }

        List<string> consumerArgs =
        [
            "--name", name,
            "--count", count.ToString(CultureInfo.InvariantCulture),
            "--delay-us", consumerDelayUs.ToString(CultureInfo.InvariantCulture),
        ];
        if (!verify)
        {
            consumerArgs.Add("--no-verify");
        }
        else if (!verifyPayload)
        {
            consumerArgs.Add("--no-verify-payload");
        }

        if (spinOnly)
        {
            producerArgs.Add("--spin-only");
            consumerArgs.Add("--spin-only");
        }

        if (notify)
        {
            producerArgs.Add("--notify");
            consumerArgs.Add("--notify");
        }

        // The producer goes first: it defines the geometry (the consumer adopts it).
        using Process producer = StartProcess(producerDll, repoRoot, producerArgs.ToArray());
        Thread.Sleep(300);
        using Process consumer = StartProcess(consumerDll, repoRoot, consumerArgs.ToArray());

        Task<string> producerOut = producer.StandardOutput.ReadToEndAsync();
        Task<string> producerErr = producer.StandardError.ReadToEndAsync();
        Task<string> consumerOut = consumer.StandardOutput.ReadToEndAsync();
        Task<string> consumerErr = consumer.StandardError.ReadToEndAsync();

        bool producerExited = producer.WaitForExit(120_000);
        bool consumerExited = consumer.WaitForExit(120_000);
        bool bothExited = producerExited && consumerExited;
        if (!bothExited)
        {
            if (!producerExited)
            {
                producer.Kill(entireProcessTree: true);
            }

            if (!consumerExited)
            {
                consumer.Kill(entireProcessTree: true);
            }
        }

        Task.WaitAll(producerOut, producerErr, consumerOut, consumerErr);

        if (echo)
        {
            Console.WriteLine($"transport=shared-xproc size={size} count={count} (two processes, one region)");
            Console.WriteLine(producerOut.Result.Trim());
            Console.WriteLine(consumerOut.Result.Trim());
        }

        if (!bothExited)
        {
            Console.Error.WriteLine("timed out waiting for producer/consumer");
            return null;
        }

        if (!string.IsNullOrWhiteSpace(producerErr.Result))
        {
            Console.Error.WriteLine(producerErr.Result.Trim());
        }

        if (!string.IsNullOrWhiteSpace(consumerErr.Result))
        {
            Console.Error.WriteLine(consumerErr.Result.Trim());
        }

        return new CrossProcessRun(
            ParseProducerOutput(producerOut.Result, capacity, slotSize, size),
            ParseConsumerOutput(consumerOut.Result),
            CpuPercent(producer),
            CpuPercent(consumer));
    }

    private static ProducerSample ParseProducerOutput(string output, int capacity, int slotSize, int payloadSize)
    {
        Match match = ProducerPattern.Match(output);
        if (!match.Success)
        {
            throw new InvalidOperationException($"Could not parse producer output:{Environment.NewLine}{output}");
        }

        // The producer prints the geometry it actually created; fall back to the
        // requested values if the line is missing (for example a quiet build).
        Match ready = ProducerReadyPattern.Match(output);
        int actualCapacity = ready.Success &&
            int.TryParse(ready.Groups["capacity"].Value, CultureInfo.InvariantCulture, out int parsedCapacity)
                ? parsedCapacity
                : capacity;
        int actualSlotSize = ready.Success &&
            int.TryParse(ready.Groups["slot"].Value, CultureInfo.InvariantCulture, out int parsedSlot)
                ? parsedSlot
                : slotSize > 0 ? slotSize : Math.Max(RingBufferLayout.DefaultSlotSize, payloadSize + RingBufferLayout.MessageHeaderSize);

        return new ProducerSample(
            long.Parse(match.Groups["count"].Value, CultureInfo.InvariantCulture),
            double.Parse(match.Groups["throughput"].Value, CultureInfo.InvariantCulture),
            double.Parse(match.Groups["mib"].Value, CultureInfo.InvariantCulture),
            actualCapacity,
            actualSlotSize);
    }

    private static ConsumerSample ParseConsumerOutput(string output)
    {
        Match match = ConsumerPattern.Match(output);
        Match latency = LatencyPattern.Match(output);
        if (!match.Success || !latency.Success)
        {
            throw new InvalidOperationException($"Could not parse consumer output:{Environment.NewLine}{output}");
        }

        return new ConsumerSample(
            long.Parse(match.Groups["count"].Value, CultureInfo.InvariantCulture),
            double.Parse(match.Groups["throughput"].Value, CultureInfo.InvariantCulture),
            double.Parse(match.Groups["mib"].Value, CultureInfo.InvariantCulture),
            new LatencyStats(
                long.Parse(latency.Groups["n"].Value, CultureInfo.InvariantCulture),
                ParseDouble(latency, "min"),
                ParseDouble(latency, "mean"),
                ParseDouble(latency, "p50"),
                ParseDouble(latency, "p90"),
                ParseDouble(latency, "p95"),
                ParseDouble(latency, "p99"),
                ParseDouble(latency, "p99_9"),
                ParseDouble(latency, "max")));
    }

    private static double ParseDouble(Match match, string group) =>
        double.Parse(match.Groups[group].Value, CultureInfo.InvariantCulture);

    private static double CpuPercent(Process process)
    {
        try
        {
            TimeSpan wall = process.ExitTime - process.StartTime;
            return wall.TotalSeconds > 0 ? process.TotalProcessorTime.TotalSeconds / wall.TotalSeconds * 100 : 0;
        }
        catch (InvalidOperationException)
        {
            return 0;
        }
    }

    private static double Median(IEnumerable<double> values)
    {
        double[] sorted = values.OrderBy(v => v).ToArray();
        if (sorted.Length == 0)
        {
            return 0;
        }

        int middle = sorted.Length / 2;
        return sorted.Length % 2 == 1 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
    }

    private static string FormatSize(int size) => size >= 1024 ? $"{size / 1024} KB" : $"{size} B";

    private static string FormatCount(double value) =>
        value >= 1_000_000
            ? (value / 1_000_000).ToString("F2", CultureInfo.InvariantCulture) + "M"
            : value.ToString("N0", CultureInfo.InvariantCulture);

    private static Process StartProcess(string dll, string workingDirectory, params string[] args)
    {
        ProcessStartInfo startInfo = new("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = workingDirectory,
        };

        startInfo.ArgumentList.Add(dll);
        foreach (string arg in args)
        {
            startInfo.ArgumentList.Add(arg);
        }

        return Process.Start(startInfo) ?? throw new InvalidOperationException($"Failed to start {dll}.");
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

        throw new InvalidOperationException("Could not locate the repository root.");
    }

    [StructLayout(LayoutKind.Auto)]
    private readonly record struct ProducerSample(long Count, double MsgPerSec, double MibPerSec, int Capacity, int SlotSize);

    [StructLayout(LayoutKind.Auto)]
    private readonly record struct ConsumerSample(long Count, double MsgPerSec, double MibPerSec, LatencyStats Latency);

    [StructLayout(LayoutKind.Auto)]
    private readonly record struct LatencyStats(
        long Count, double Min, double Mean, double P50, double P90, double P95, double P99, double P999, double Max);

    private sealed record CrossProcessRun(
        ProducerSample Producer, ConsumerSample Consumer, double ProducerCpuPercent, double ConsumerCpuPercent)
    {
        public double ProducerMsgPerSec => Producer.MsgPerSec;

        public double ConsumerMsgPerSec => Consumer.MsgPerSec;

        public double ConsumerMibPerSec => Consumer.MibPerSec;

        public LatencyStats Latency => Consumer.Latency;

        /// <summary>Bytes of the shared region (header + all slots).</summary>
        public double RegionMib =>
            (RingBufferLayout.HeaderSize + Producer.Capacity * (double)Producer.SlotSize) / (1024 * 1024);
    }
}
