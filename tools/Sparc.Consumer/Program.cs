using System.Globalization;
using Sparc.UnixMemoryMapped;
using Sparc.WindowsMemoryMapped;
using Sparc.Cli;
using Sparc.Client;
using Sparc.Core;

namespace Sparc.Consumer;

internal static class Program
{
    private static int Main(string[] args)
    {
        ConsumerOptions options;
        try
        {
            options = ConsumerOptions.Parse(args);
        }
        catch (UsageException exception)
        {
            Console.Error.WriteLine($"error: {exception.Message}");
            Console.Error.WriteLine();
            Console.Error.WriteLine(ConsumerOptions.Usage);
            return RingBufferExitCodes.UsageError;
        }

        if (options.ShowHelp)
        {
            Console.WriteLine(ConsumerOptions.Usage);
            return RingBufferExitCodes.Success;
        }

        try
        {
            return Run(options);
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

    private static int Run(ConsumerOptions options)
    {
        IIpcMemoryRegionFactory factory = OperatingSystem.IsWindows()
            ? new WindowsNamedMemoryMappedRegionFactory()
            : new UnixFileMemoryMappedRegionFactory();
        using IConsumerEndpoint buffer = SparcRing.OpenConsumer(
            factory,
            options.Name,
            options.Capacity,
            options.SlotSize,
            new SharedRingBufferOptions
            {
                OpenTimeout = options.OpenTimeout,
                RecreateIfStale = options.RecreateStale,
                RequireExisting = options.RequireExisting,
                AdoptExistingGeometry = true,
                Takeover = options.Takeover,
            });

        if (!options.Quiet)
        {
            Console.WriteLine(
                $"ready: role=consumer name={options.Name} capacity={buffer.Capacity} " +
                $"slotSize={buffer.SlotSize} maxPayload={buffer.MaxPayloadSize}");
        }

        using CancellationTokenSource cancellation = new();
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cancellation.Cancel();
        };

        using SessionNotification? notification = options.Notify ? CreateNotification(options.Name) : null;
        ConsumerSession session = new(buffer, new ConsumerSessionOptions
        {
            Count = options.Count,
            ExpectedType = options.Type,
            Verify = options.Verify,
            VerifyPayload = options.VerifyPayload,
            IdleTimeout = options.IdleTimeout,
            PerMessageDelay = options.Delay,
            Takeover = options.Takeover,
            WaitMode = options.Notify
                ? SessionWaitMode.Notification
                : options.SpinOnly ? SessionWaitMode.SpinOnly : SessionWaitMode.SpinThenSleep,
            Notification = notification,
        });

        ConsumerRunResult result = session.Run(cancellation.Token);
        PrintSummary(result, buffer);

        if (result.FailureMessage is not null)
        {
            Console.Error.WriteLine($"error: {result.FailureMessage}");
        }

        return MapReason(result.Reason);
    }

    private static SessionNotification CreateNotification(string regionName) =>
        SessionNotification.CreateNamed(regionName);

    private static int MapReason(SessionStopReason reason) => reason switch
    {
        SessionStopReason.Completed => RingBufferExitCodes.Success,
        SessionStopReason.PeerStopped => RingBufferExitCodes.Incomplete,
        SessionStopReason.Timeout => RingBufferExitCodes.Incomplete,
        SessionStopReason.VerificationFailed => RingBufferExitCodes.VerificationFailed,
        SessionStopReason.Cancelled => RingBufferExitCodes.Incomplete,
        _ => RingBufferExitCodes.InternalError,
    };

    private static void PrintSummary(ConsumerRunResult result, IConsumerEndpoint buffer)
    {
        double seconds = result.Elapsed.TotalSeconds;
        double messagesPerSecond = seconds > 0 ? result.Received / seconds : 0;
        double megabytesPerSecond = seconds > 0 ? result.ReceivedBytes / seconds / (1024.0 * 1024.0) : 0;

        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"consumed={result.Received} bytes={result.ReceivedBytes} elapsed={seconds:F3}s " +
            $"throughput={messagesPerSecond:F0} msg/s dataThroughput={megabytesPerSecond:F1} MiB/s " +
            $"wallElapsed={result.RunElapsed.TotalSeconds:F3}s"));
        Console.WriteLine($"producer={buffer.ProducerState} consumer={buffer.ConsumerState}");
        Console.WriteLine(result.Latency.ToMicrosecondsReport(TimeProvider.System.TimestampFrequency));
    }
}
