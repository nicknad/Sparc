using System.Diagnostics;
using RingBuffer.Benchmarks.Pumps;

namespace RingBuffer.Benchmarks;

/// <summary>
/// Small custom harness for one-way latency percentiles (p50/p95/p99). For the
/// real cross-process measurement use <c>--transport shared-xproc</c>, which
/// runs the Producer and Consumer executables against the same region.
/// </summary>
internal static class LatencyRunner
{
    public static void Run(string[] args)
    {
        string transport = "array";
        int size = 64;
        long count = 200_000;

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--transport":
                    transport = args[++i];
                    break;
                case "--size":
                    size = int.Parse(args[++i]);
                    break;
                case "--count":
                    count = long.Parse(args[++i]);
                    break;
            }
        }

        if (transport is "shared-xproc")
        {
            RunCrossProcess(size, count);
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

    private static void RunCrossProcess(int size, long count)
    {
        string name = "spsc-latency-" + Guid.NewGuid().ToString("N");
        string repoRoot = FindRepoRoot();

#if DEBUG
        const string configuration = "Debug";
#else
        const string configuration = "Release";
#endif

        string producerDll = Path.Combine(repoRoot, "src", "RingBuffer.Producer", "bin", configuration, "net11.0", "RingBuffer.Producer.dll");
        string consumerDll = Path.Combine(repoRoot, "src", "RingBuffer.Consumer", "bin", configuration, "net11.0", "RingBuffer.Consumer.dll");

        // The producer goes first: it defines the geometry (the consumer adopts it).
        using Process producer = StartProcess(producerDll, repoRoot, "--name", name, "--count", count.ToString(), "--size", size.ToString());
        Thread.Sleep(300);
        using Process consumer = StartProcess(consumerDll, repoRoot, "--name", name, "--count", count.ToString());

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

        Console.WriteLine($"transport=shared-xproc size={size} count={count} (two processes, one region)");
        Console.WriteLine(producerOut.Result.Trim());
        Console.WriteLine(consumerOut.Result.Trim());

        if (!bothExited)
        {
            Console.Error.WriteLine("timed out waiting for producer/consumer");
        }

        if (!string.IsNullOrWhiteSpace(producerErr.Result))
        {
            Console.Error.WriteLine(producerErr.Result.Trim());
        }

        if (!string.IsNullOrWhiteSpace(consumerErr.Result))
        {
            Console.Error.WriteLine(consumerErr.Result.Trim());
        }
    }

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
            if (File.Exists(Path.Combine(directory.FullName, "SpscRingBuffer.slnx")) ||
                File.Exists(Path.Combine(directory.FullName, "SpscRingBuffer.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new InvalidOperationException("Could not locate the repository root.");
    }
}
