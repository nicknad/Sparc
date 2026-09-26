using System.Diagnostics;

namespace RingBuffer.ProcessTests;

public class ProcessTests(ITestOutputHelper output)
{
    private const int ProcessTimeoutMs = 60_000;

    [Fact]
    public async Task ConsumerFirst_FullRoundTrip()
    {
        string name = RingBufferProcesses.NewName();
        Process? consumer = null;
        Process? producer = null;

        try
        {
            consumer = RingBufferProcesses.StartConsumer("--name", name, "--count", "300000");
            await Task.Delay(300, TestContext.Current.CancellationToken);

            producer = RingBufferProcesses.StartProducer("--name", name, "--count", "300000");

            ProcessResult producerResult = await RingBufferProcesses.WaitAsync(producer, ProcessTimeoutMs);
            ProcessResult consumerResult = await RingBufferProcesses.WaitAsync(consumer, ProcessTimeoutMs);
            output.WriteLine(consumerResult.StdOut);

            Assert.Equal(0, producerResult.ExitCode);
            Assert.Equal(0, consumerResult.ExitCode);
            Assert.Contains("consumed=300000", consumerResult.StdOut, StringComparison.Ordinal);
        }
        finally
        {
            Kill(producer);
            Kill(consumer);
        }
    }

    [Fact]
    public async Task ProducerFirst_FullRoundTrip()
    {
        string name = RingBufferProcesses.NewName();
        Process? consumer = null;
        Process? producer = null;

        try
        {
            producer = RingBufferProcesses.StartProducer("--name", name, "--count", "200000", "--size", "256");
            await RingBufferProcesses.WaitForReadyAsync(producer);

            consumer = RingBufferProcesses.StartConsumer("--name", name, "--count", "200000");

            ProcessResult producerResult = await RingBufferProcesses.WaitAsync(producer, ProcessTimeoutMs);
            ProcessResult consumerResult = await RingBufferProcesses.WaitAsync(consumer, ProcessTimeoutMs);
            output.WriteLine(consumerResult.StdOut);

            Assert.Equal(0, producerResult.ExitCode);
            Assert.Equal(0, consumerResult.ExitCode);
            Assert.Contains("consumed=200000", consumerResult.StdOut, StringComparison.Ordinal);
        }
        finally
        {
            Kill(producer);
            Kill(consumer);
        }
    }

    [Fact]
    public async Task UnlimitedConsumer_DrainsUntilProducerStops()
    {
        string name = RingBufferProcesses.NewName();
        Process? consumer = null;
        Process? producer = null;

        try
        {
            consumer = RingBufferProcesses.StartConsumer("--name", name, "--count", "0", "--idle-timeout", "15000");
            await Task.Delay(200, TestContext.Current.CancellationToken);

            producer = RingBufferProcesses.StartProducer("--name", name, "--count", "100000");

            ProcessResult producerResult = await RingBufferProcesses.WaitAsync(producer, ProcessTimeoutMs);
            ProcessResult consumerResult = await RingBufferProcesses.WaitAsync(consumer, ProcessTimeoutMs);
            output.WriteLine(consumerResult.StdOut);

            Assert.Equal(0, producerResult.ExitCode);
            Assert.Equal(0, consumerResult.ExitCode);
            Assert.Contains("consumed=100000", consumerResult.StdOut, StringComparison.Ordinal);
        }
        finally
        {
            Kill(producer);
            Kill(consumer);
        }
    }

    [Fact]
    public async Task SecondProducer_ExitsWithRoleConflict()
    {
        string name = RingBufferProcesses.NewName();
        Process? first = null;
        Process? second = null;

        try
        {
            first = RingBufferProcesses.StartProducer(
                "--name", name, "--count", "100000000", "--full-timeout", "120000");
            await RingBufferProcesses.WaitForReadyAsync(first);

            second = RingBufferProcesses.StartProducer("--name", name, "--count", "10");
            ProcessResult result = await RingBufferProcesses.WaitAsync(second, 15_000);
            output.WriteLine(result.StdErr);

            Assert.Equal(4, result.ExitCode);
            Assert.Contains("already has a producer", result.StdErr, StringComparison.Ordinal);
        }
        finally
        {
            Kill(first);
            Kill(second);
        }
    }

    [Fact]
    public async Task SecondProducer_WithDifferentGeometry_ExitsIncompatible()
    {
        string name = RingBufferProcesses.NewName();
        Process? first = null;
        Process? second = null;

        try
        {
            first = RingBufferProcesses.StartProducer(
                "--name", name, "--count", "100000000", "--slot-size", "512", "--full-timeout", "120000");
            await RingBufferProcesses.WaitForReadyAsync(first);

            second = RingBufferProcesses.StartProducer("--name", name, "--count", "10", "--slot-size", "256");
            ProcessResult result = await RingBufferProcesses.WaitAsync(second, 15_000);
            output.WriteLine(result.StdErr);

            Assert.Equal(5, result.ExitCode);
            Assert.Contains("slotSize=512", result.StdErr, StringComparison.Ordinal);
        }
        finally
        {
            Kill(first);
            Kill(second);
        }
    }

    [Fact]
    public async Task KilledConsumer_ProducerGivesUpAfterFullTimeout()
    {
        string name = RingBufferProcesses.NewName();
        Process? consumer = null;
        Process? producer = null;

        try
        {
            consumer = RingBufferProcesses.StartConsumer("--name", name, "--count", "50000000");
            await Task.Delay(300, TestContext.Current.CancellationToken);

            producer = RingBufferProcesses.StartProducer(
                "--name", name, "--count", "50000000", "--full-timeout", "3000");
            await RingBufferProcesses.WaitForReadyAsync(producer);
            await Task.Delay(500, TestContext.Current.CancellationToken);

            RingBufferProcesses.Kill(consumer);

            ProcessResult producerResult = await RingBufferProcesses.WaitAsync(producer, 30_000);
            output.WriteLine(producerResult.StdErr);

            Assert.Equal(2, producerResult.ExitCode);
            Assert.Contains("stayed full", producerResult.StdErr, StringComparison.Ordinal);
        }
        finally
        {
            Kill(producer);
            Kill(consumer);
        }
    }

    [Fact]
    public async Task KilledProducer_ConsumerExitsIncomplete()
    {
        string name = RingBufferProcesses.NewName();
        Process? consumer = null;
        Process? producer = null;

        try
        {
            consumer = RingBufferProcesses.StartConsumer(
                "--name", name, "--count", "50000000", "--idle-timeout", "2000");
            await Task.Delay(300, TestContext.Current.CancellationToken);

            producer = RingBufferProcesses.StartProducer("--name", name, "--count", "50000000");
            await RingBufferProcesses.WaitForReadyAsync(producer);
            await Task.Delay(500, TestContext.Current.CancellationToken);

            RingBufferProcesses.Kill(producer);

            ProcessResult consumerResult = await RingBufferProcesses.WaitAsync(consumer, 30_000);
            output.WriteLine(consumerResult.StdErr);

            Assert.Equal(3, consumerResult.ExitCode);
            Assert.Contains("no messages for", consumerResult.StdErr, StringComparison.Ordinal);
        }
        finally
        {
            Kill(producer);
            Kill(consumer);
        }
    }

    [Fact]
    public async Task RequireExisting_WithoutRegion_TimesOut()
    {
        string name = RingBufferProcesses.NewName();
        using Process consumer = RingBufferProcesses.StartConsumer(
            "--name", name, "--require-existing", "--open-timeout", "300");

        ProcessResult result = await RingBufferProcesses.WaitAsync(consumer, 15_000);
        output.WriteLine(result.StdErr);

        Assert.Equal(2, result.ExitCode);
        Assert.Contains("did not exist", result.StdErr, StringComparison.Ordinal);
    }

    private static void Kill(Process? process)
    {
        if (process is not null)
        {
            RingBufferProcesses.Kill(process);
        }
    }
}
