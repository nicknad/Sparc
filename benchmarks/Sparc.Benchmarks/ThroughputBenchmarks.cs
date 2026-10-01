using BenchmarkDotNet.Jobs;
using Sparc.Benchmarks.Pumps;

namespace Sparc.Benchmarks;

public abstract class ThroughputBenchmarkBase
{
    public const int BatchSize = 65_536;

    protected TwoThreadPump Pump = null!;

    [Params(16, 64, 256, 1024, 4096)]
    public int MessageSize { get; set; }

    protected abstract TwoThreadPump CreatePump();

    [GlobalSetup]
    public void Setup() => Pump = CreatePump();

    [GlobalCleanup]
    public void Cleanup() => Pump.Dispose();

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public void Transfer() => Pump.Run(BatchSize, MessageSize);
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class SpscArrayBenchmarks : ThroughputBenchmarkBase
{
    protected override TwoThreadPump CreatePump() => new SpscArrayPump(MessageSize);
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class SpscSharedMemoryBenchmarks : ThroughputBenchmarkBase
{
    protected override TwoThreadPump CreatePump() => new SpscSharedMemoryPump(MessageSize);
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class SpscArrayLeaseBenchmarks : ThroughputBenchmarkBase
{
    protected override TwoThreadPump CreatePump() => new SpscLeasePump(MessageSize);
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class SpscSharedMemoryLeaseBenchmarks : ThroughputBenchmarkBase
{
    protected override TwoThreadPump CreatePump() => SpscSharedMemoryPump.CreateLease(MessageSize);
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class SpscSecuredSharedMemoryBenchmarks : ThroughputBenchmarkBase
{
    protected override TwoThreadPump CreatePump() => SpscSharedMemoryPump.CreateSecured(MessageSize);
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class SpscSecuredSharedMemoryLeaseBenchmarks : ThroughputBenchmarkBase
{
    protected override TwoThreadPump CreatePump() => SpscSharedMemoryPump.CreateSecuredLease(MessageSize);
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class SpscChannelBenchmarks : ThroughputBenchmarkBase
{
    protected override TwoThreadPump CreatePump() => new SpscChannelPump(MessageSize);
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class LockQueueBenchmarks : ThroughputBenchmarkBase
{
    protected override TwoThreadPump CreatePump() => new LockQueuePump();
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class ChannelBenchmarks : ThroughputBenchmarkBase
{
    protected override TwoThreadPump CreatePump() => new ChannelPump();
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class NamedPipeBenchmarks : ThroughputBenchmarkBase
{
    protected override TwoThreadPump CreatePump() => new NamedPipePump();
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class TcpLoopbackBenchmarks : ThroughputBenchmarkBase
{
    protected override TwoThreadPump CreatePump() => new TcpLoopbackPump();
}
