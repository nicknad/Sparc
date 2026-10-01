using System.Globalization;
using Microsoft.Win32.SafeHandles;
using Sparc.UnixMemoryMapped;
using Sparc.WindowsMemoryMapped;
using Sparc.Cli;
using Sparc.Client;
using Sparc.Core;

namespace Sparc.Producer;

internal static class Program
{
    private static int Main(string[] args)
    {
        ProducerOptions options;
        try
        {
            options = ProducerOptions.Parse(args);
        }
        catch (UsageException exception)
        {
            Console.Error.WriteLine($"error: {exception.Message}");
            Console.Error.WriteLine();
            Console.Error.WriteLine(ProducerOptions.Usage);
            return RingBufferExitCodes.UsageError;
        }

        if (options.ShowHelp)
        {
            Console.WriteLine(ProducerOptions.Usage);
            return RingBufferExitCodes.Success;
        }

        if (options.Advise)
        {
            PrintAdvice(options);
            return RingBufferExitCodes.Success;
        }

        try
        {
            return Run(options);
        }
        catch (UsageException exception)
        {
            // Raised while resolving a --section-handle value at runtime.
            Console.Error.WriteLine($"error: {exception.Message}");
            return RingBufferExitCodes.UsageError;
        }
        catch (RingBufferException exception)
        {
            Console.Error.WriteLine($"error: {exception.Message}");
            return RingBufferExitCodes.FromException(exception);
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"fatal: {exception}");
            return RingBufferExitCodes.InternalError;
        }
    }

    private static int Run(ProducerOptions options)
    {
        IIpcMemoryRegionFactory factory = OperatingSystem.IsWindows()
            ? new WindowsNamedMemoryMappedRegionFactory()
            : new UnixFileMemoryMappedRegionFactory();

        SharedRingBufferOptions ringOptions = new()
        {
            OpenTimeout = options.OpenTimeout,
            RecreateIfStale = options.RecreateStale,
            RequireExisting = options.RequireExisting,
            Takeover = options.Takeover,
            Security = options.SecurityCurrentUser ? WindowsSectionSecurity.CurrentUserOnly : null,
        };

        using SafeFileHandle? sectionHandle = options.SectionHandle is null
            ? null
            : SectionHandleTransfer.Acquire(options.SectionHandle);
        using IProducerEndpoint buffer = sectionHandle is null
            ? SparcRing.OpenProducer(
                factory, options.Name, options.Capacity, options.EffectiveSlotSize, ringOptions)
            : SparcRing.OpenProducer(
                WindowsUnnamedSection.MapHandle(sectionHandle),
                options.Capacity,
                options.EffectiveSlotSize,
                ringOptions,
                CancellationToken.None);

        if (!options.Quiet)
        {
            Console.WriteLine(
                $"ready: role=producer name={buffer.Name} capacity={buffer.Capacity} " +
                $"slotSize={buffer.SlotSize} maxPayload={buffer.MaxPayloadSize}");
        }

        using CancellationTokenSource cancellation = new();
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cancellation.Cancel();
        };

        using SessionNotification? notification = options.Notify ? CreateNotification(options.Name) : null;
        ProducerSession session = new(buffer, new ProducerSessionOptions
        {
            Count = options.Count,
            PayloadSize = options.Size,
            MessageType = options.Type,
            FullTimeout = options.FullTimeout,
            PerMessageDelay = options.Delay,
            Takeover = options.Takeover,
            WaitMode = options.Notify
                ? SessionWaitMode.Notification
                : options.SpinOnly ? SessionWaitMode.SpinOnly : SessionWaitMode.SpinThenSleep,
            Notification = notification,
        });

        ProducerRunResult result = session.Run(cancellation.Token);
        PrintSummary(result, buffer, options);

        if (result.FailureMessage is not null)
        {
            Console.Error.WriteLine($"error: {result.FailureMessage}");
        }

        return MapReason(result.Reason);
    }

    private static void PrintAdvice(ProducerOptions options)
    {
        RingBufferAdvice advice = RingBufferAdvisor.Advise(
            options.Size, options.Capacity, options.SlotSize, options.TargetGiB);

        if (options.JsonFormat)
        {
            Console.WriteLine(SessionSummaries.AdviceJson(advice));
            return;
        }

        Console.WriteLine(
            $"advise: payload={advice.PayloadSize} capacity={advice.Capacity} slotSize={advice.SlotSize} " +
            $"region={advice.RegionBytes} bytes ({advice.RegionMiB:F1} MiB)");
        Console.WriteLine($"advise: {advice.Residency}");
        if (advice.AlignmentWarning is not null)
        {
            Console.WriteLine($"advise: warning: {advice.AlignmentWarning}");
        }

        foreach (string warning in RingBufferAdvisor.GetWarnings(advice.Capacity, advice.SlotSize))
        {
            if (!string.Equals(warning, advice.AlignmentWarning, StringComparison.Ordinal))
            {
                Console.WriteLine($"advise: warning: {warning}");
            }
        }

        if (advice.MessagesPerSecondAtTarget.HasValue)
        {
            Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"advise: {options.TargetGiB:F1} GiB/s needs {advice.MessagesPerSecondAtTarget:F0} msg/s " +
                $"at {advice.PayloadSize} bytes/message"));
        }
    }

    private static SessionNotification CreateNotification(string regionName) =>
        SessionNotification.CreateNamed(regionName);

    private static int MapReason(SessionStopReason reason) => reason switch
    {
        SessionStopReason.Completed => RingBufferExitCodes.Success,
        SessionStopReason.PeerStopped => RingBufferExitCodes.Incomplete,
        SessionStopReason.Timeout => RingBufferExitCodes.Timeout,
        SessionStopReason.Cancelled => RingBufferExitCodes.Incomplete,
        _ => RingBufferExitCodes.InternalError,
    };

    private static void PrintSummary(ProducerRunResult result, IProducerEndpoint buffer, ProducerOptions options)
    {
        if (options.JsonFormat)
        {
            Console.WriteLine(SessionSummaries.ProducerJson(
                buffer.Name, buffer.Capacity, buffer.SlotSize, buffer.MaxPayloadSize, result));
            return;
        }

        // The send rate uses the active window (first publish to end); the total
        // elapsed time also contains waiting for the consumer to attach.
        TimeSpan active = result.ActiveElapsed > TimeSpan.Zero ? result.ActiveElapsed : result.Elapsed;
        double seconds = active.TotalSeconds;
        double messagesPerSecond = seconds > 0 ? result.Produced / seconds : 0;
        double megabytesPerSecond = messagesPerSecond * result.PayloadSize / (1024.0 * 1024.0);

        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"produced={result.Produced} payloadSize={result.PayloadSize} elapsed={result.Elapsed.TotalSeconds:F3}s " +
            $"activeElapsed={result.ActiveElapsed.TotalSeconds:F3}s " +
            $"throughput={messagesPerSecond:F0} msg/s dataThroughput={megabytesPerSecond:F1} MiB/s"));
    }
}
