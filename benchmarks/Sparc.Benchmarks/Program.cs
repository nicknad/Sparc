using BenchmarkDotNet.Running;

namespace Sparc.Benchmarks;

public static class Program
{
    public static void Main(string[] args)
    {
        if (args.Contains("--latency"))
        {
            LatencyRunner.Run(args);
            return;
        }

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
