using BenchmarkDotNet.Running;

namespace Sparc.Benchmarks;

public static class Program
{
    public static int Main(string[] args)
    {
        if (args.Contains("--latency"))
        {
            LatencyRunner.Run(args);
            return 0;
        }

        if (args.Contains("--regression"))
        {
            return RegressionRunner.Run(args);
        }

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        return 0;
    }
}
