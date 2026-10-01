using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using Sparc;
using Sparc.Client;
using Sparc.Core;
using Sparc.WindowsMemoryMapped;

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

    [Fact]
    public async Task SecuredRegion_FullRoundTrip()
    {
        if (!OperatingSystem.IsWindows())
        {
            return; // section DACLs are Windows-only
        }

        string name = SparcProcesses.NewName();
        Process? consumer = null;
        Process? producer = null;

        try
        {
            // The producer creates the secured section; the consumer joins it
            // with the same DACL configuration. The ring protocol is unchanged.
            producer = SparcProcesses.StartProducer(
                "--name", name, "--count", "200000", "--size", "256", "--security", "current-user");
            await SparcProcesses.WaitForReadyAsync(producer);

            consumer = SparcProcesses.StartConsumer(
                "--name", name, "--count", "200000", "--security", "current-user");

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
    public async Task SecuredRegion_KilledProducer_ConsumerExitsIncomplete()
    {
        if (!OperatingSystem.IsWindows())
        {
            return; // section DACLs are Windows-only
        }

        string name = SparcProcesses.NewName();
        Process? consumer = null;
        Process? producer = null;

        try
        {
            consumer = SparcProcesses.StartConsumer(
                "--name", name, "--count", "50000000", "--idle-timeout", "2000",
                "--security", "current-user");
            await Task.Delay(300, TestContext.Current.CancellationToken);

            producer = SparcProcesses.StartProducer(
                "--name", name, "--count", "50000000", "--security", "current-user");
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
    public async Task UnnamedSection_TransferredHandle_FullRoundTrip()
    {
        if (!OperatingSystem.IsWindows())
        {
            return; // unnamed sections and handle transfer are Windows-only
        }

        const int capacity = 1024;
        const int slotSize = 256;
        const int count = 100_000;
        long size = RingBufferLayout.RequiredSize(capacity, slotSize);

        // The test process plays the creator: it owns the unnamed section and
        // the producer endpoint. The consumer child receives a duplicated
        // HANDLE and maps it without ever seeing a name.
        using WindowsSectionCapability capability = WindowsUnnamedSection.Create(
            size, WindowsSectionSecurity.CurrentUserOnly);
        using IProducerEndpoint producer = SparcRing.OpenProducer(
            capability.Region, capacity, slotSize, options: null, TestContext.Current.CancellationToken);

        Process? consumer = null;
        try
        {
            consumer = SparcProcesses.StartConsumer(
                "--section-handle", "-",
                "--count", count.ToString(CultureInfo.InvariantCulture),
                "--capacity", capacity.ToString(CultureInfo.InvariantCulture),
                "--slot-size", slotSize.ToString(CultureInfo.InvariantCulture));

            IntPtr duplicated = DuplicateHandleInto(consumer, capability.Handle);
            await consumer.StandardInput.WriteLineAsync(
                "0x" + duplicated.ToString("X", CultureInfo.InvariantCulture));
            await consumer.StandardInput.FlushAsync(TestContext.Current.CancellationToken);

            await SparcProcesses.WaitForReadyAsync(consumer);

            ProducerSession session = new(producer, new ProducerSessionOptions
            {
                Count = count,
                PayloadSize = 64,
            });

            ProducerRunResult result = session.Run(TestContext.Current.CancellationToken);
            ProcessResult consumerResult = await SparcProcesses.WaitAsync(consumer, ProcessTimeoutMs);
            output.WriteLine(consumerResult.StdOut);

            Assert.Equal(count, result.Produced);
            Assert.Equal(0, consumerResult.ExitCode);
            Assert.Contains($"consumed={count}", consumerResult.StdOut, StringComparison.Ordinal);
        }
        finally
        {
            Kill(consumer);
        }
    }

    private static IntPtr DuplicateHandleInto(Process child, SafeFileHandle source)
    {
        const uint ProcessDupHandle = 0x0040;
        const uint DuplicateSameAccess = 0x0002;

        IntPtr processHandle = OpenProcess(ProcessDupHandle, bInheritHandle: false, child.Id);
        if (processHandle == IntPtr.Zero)
        {
            throw new Win32Exception(Marshal.GetLastWin32Error(), "OpenProcess(PROCESS_DUP_HANDLE) failed.");
        }

        try
        {
            if (!DuplicateHandle(
                GetCurrentProcess(),
                source.DangerousGetHandle(),
                processHandle,
                out IntPtr target,
                desiredAccess: 0,
                bInheritHandle: false,
                options: DuplicateSameAccess))
            {
                throw new Win32Exception(Marshal.GetLastWin32Error(), "DuplicateHandle failed.");
            }

            return target;
        }
        finally
        {
            CloseHandle(processHandle);
        }
    }

    private static void Kill(Process? process)
    {
        if (process is not null)
        {
            SparcProcesses.Kill(process);
        }
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern IntPtr OpenProcess(uint desiredAccess, bool bInheritHandle, int processId);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool DuplicateHandle(
        IntPtr sourceProcess,
        IntPtr sourceHandle,
        IntPtr targetProcess,
        out IntPtr targetHandle,
        uint desiredAccess,
        bool bInheritHandle,
        uint options);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool CloseHandle(IntPtr handle);

    [DllImport("kernel32.dll")]
    private static extern IntPtr GetCurrentProcess();
}
