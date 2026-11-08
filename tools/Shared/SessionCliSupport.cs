using Sparc.Client;
using Sparc.UnixMemoryMapped;
using Sparc.WindowsMemoryMapped;

namespace Sparc.Cli;

internal static class SessionCliSupport
{
    public static IIpcMemoryRegionFactory CreateFactory() =>
        OperatingSystem.IsWindows()
            ? new WindowsNamedMemoryMappedRegionFactory()
            : new UnixFileMemoryMappedRegionFactory();

    public static CancellationTokenSource CreateCancellationSource()
    {
        CancellationTokenSource cancellation = new();
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cancellation.Cancel();
        };

        return cancellation;
    }

    public static SessionNotification? CreateNotification(bool notify, string name) =>
        notify ? SessionNotification.CreateNamed(name) : null;

    public static SessionWaitMode SelectWaitMode(bool notify, bool spinOnly) =>
        notify
            ? SessionWaitMode.Notification
            : spinOnly ? SessionWaitMode.SpinOnly : SessionWaitMode.SpinThenSleep;

    public static int MapProducerReason(SessionStopReason reason) =>
        MapReason(reason, timeoutIsTimeout: true, verificationFailureIsError: false);

    public static int MapConsumerReason(SessionStopReason reason) =>
        MapReason(reason, timeoutIsTimeout: false, verificationFailureIsError: true);

    private static int MapReason(
        SessionStopReason reason,
        bool timeoutIsTimeout,
        bool verificationFailureIsError) => reason switch
    {
        SessionStopReason.Completed => RingBufferExitCodes.Success,
        SessionStopReason.PeerStopped => RingBufferExitCodes.Incomplete,
        SessionStopReason.Timeout => timeoutIsTimeout ? RingBufferExitCodes.Timeout : RingBufferExitCodes.Incomplete,
        SessionStopReason.VerificationFailed when verificationFailureIsError => RingBufferExitCodes.VerificationFailed,
        SessionStopReason.Cancelled => RingBufferExitCodes.Incomplete,
        _ => RingBufferExitCodes.InternalError,
    };
}
