using System.Diagnostics;

namespace Sparc.ProcessTests;

public class ProcessLifecycleTests(ITestOutputHelper output)
{
    private const int ProcessTimeoutMs = 60_000;

    [Fact]
    public async Task ConsumerFirst_FullRoundTrip()
    {
        string name = SparcProcesses.NewName();
        Process? consumer = null;
        Process? producer = null;

        try
        {
            consumer = SparcProcesses.StartConsumer("--name", name, "--count", "300000");
            await Task.Delay(300, TestContext.Current.CancellationToken);

            producer = SparcProcesses.StartProducer("--name", name, "--count", "300000");

            ProcessResult producerResult = await SparcProcesses.WaitAsync(producer, ProcessTimeoutMs);
            ProcessResult consumerResult = await SparcProcesses.WaitAsync(consumer, ProcessTimeoutMs);
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
        string name = SparcProcesses.NewName();
        Process? consumer = null;
        Process? producer = null;

        try
        {
            producer = SparcProcesses.StartProducer("--name", name, "--count", "200000", "--size", "256");
            await SparcProcesses.WaitForReadyAsync(producer);

            consumer = SparcProcesses.StartConsumer("--name", name, "--count", "200000");

            ProcessResult producerResult = await SparcProcesses.WaitAsync(producer, ProcessTimeoutMs);
            ProcessResult consumerResult = await SparcProcesses.WaitAsync(consumer, ProcessTimeoutMs);
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
        string name = SparcProcesses.NewName();
        Process? consumer = null;
        Process? producer = null;

        try
        {
            consumer = SparcProcesses.StartConsumer("--name", name, "--count", "0", "--idle-timeout", "15000");
            await Task.Delay(200, TestContext.Current.CancellationToken);

            producer = SparcProcesses.StartProducer("--name", name, "--count", "100000");

            ProcessResult producerResult = await SparcProcesses.WaitAsync(producer, ProcessTimeoutMs);
            ProcessResult consumerResult = await SparcProcesses.WaitAsync(consumer, ProcessTimeoutMs);
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
    public async Task NotifyMode_FullRoundTrip()
    {
        string name = SparcProcesses.NewName();
        Process? consumer = null;
        Process? producer = null;

        try
        {
            producer = SparcProcesses.StartProducer("--name", name, "--count", "100000", "--notify");
            await SparcProcesses.WaitForReadyAsync(producer);

            consumer = SparcProcesses.StartConsumer("--name", name, "--count", "100000", "--notify");

            ProcessResult producerResult = await SparcProcesses.WaitAsync(producer, ProcessTimeoutMs);
            ProcessResult consumerResult = await SparcProcesses.WaitAsync(consumer, ProcessTimeoutMs);
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
        string name = SparcProcesses.NewName();
        Process? first = null;
        Process? second = null;

        try
        {
            first = SparcProcesses.StartProducer(
                "--name", name, "--count", "100000000", "--full-timeout", "120000");
            await SparcProcesses.WaitForReadyAsync(first);

            second = SparcProcesses.StartProducer("--name", name, "--count", "10");
            ProcessResult result = await SparcProcesses.WaitAsync(second, 15_000);
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
        string name = SparcProcesses.NewName();
        Process? first = null;
        Process? second = null;

        try
        {
            first = SparcProcesses.StartProducer(
                "--name", name, "--count", "100000000", "--slot-size", "512", "--full-timeout", "120000");
            await SparcProcesses.WaitForReadyAsync(first);

            second = SparcProcesses.StartProducer("--name", name, "--count", "10", "--slot-size", "256");
            ProcessResult result = await SparcProcesses.WaitAsync(second, 15_000);
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
        string name = SparcProcesses.NewName();
        Process? consumer = null;
        Process? producer = null;

        try
        {
            consumer = SparcProcesses.StartConsumer("--name", name, "--count", "50000000");
            await Task.Delay(300, TestContext.Current.CancellationToken);

            producer = SparcProcesses.StartProducer(
                "--name", name, "--count", "50000000", "--full-timeout", "3000");
            await SparcProcesses.WaitForReadyAsync(producer);
            await Task.Delay(500, TestContext.Current.CancellationToken);

            SparcProcesses.Kill(consumer);

            ProcessResult producerResult = await SparcProcesses.WaitAsync(producer, 30_000);
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
        string name = SparcProcesses.NewName();
        Process? consumer = null;
        Process? producer = null;

        try
        {
            consumer = SparcProcesses.StartConsumer(
                "--name", name, "--count", "50000000", "--idle-timeout", "2000");
            await Task.Delay(300, TestContext.Current.CancellationToken);

            producer = SparcProcesses.StartProducer("--name", name, "--count", "50000000");
            await SparcProcesses.WaitForReadyAsync(producer);
            await Task.Delay(500, TestContext.Current.CancellationToken);

            SparcProcesses.Kill(producer);

            ProcessResult consumerResult = await SparcProcesses.WaitAsync(consumer, 30_000);
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
        string name = SparcProcesses.NewName();
        using Process consumer = SparcProcesses.StartConsumer(
            "--name", name, "--require-existing", "--open-timeout", "300");

        ProcessResult result = await SparcProcesses.WaitAsync(consumer, 15_000);
        output.WriteLine(result.StdErr);

        Assert.Equal(2, result.ExitCode);
        Assert.Contains("did not exist", result.StdErr, StringComparison.Ordinal);
    }

    private static void Kill(Process? process)
    {
        if (process is not null)
        {
            SparcProcesses.Kill(process);
        }
    }
}
