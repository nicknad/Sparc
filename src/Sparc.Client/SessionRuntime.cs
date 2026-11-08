using Sparc.Core;

namespace Sparc.Client;

internal static class SessionRuntime
{
    internal const int ProgressReports = 10;
    internal const long CancellationCheckMask = 0xFFFF;

    internal static long ProgressInterval(long count) =>
        count > 0 ? Math.Max(1, count / ProgressReports) : 0;

    internal static bool ShouldCheckCancellation(ref long counter, long pacingTicks) =>
        pacingTicks > 0 || (++counter & CancellationCheckMask) == 0;

    internal static Task<TResult> RunOnDedicatedThread<TResult>(Func<TResult> run) =>
        Task.Factory.StartNew(
            run,
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

    internal static async Task<bool> WaitForPeerAsync(
        Func<RingBufferEndpointState> peerState,
        TimeProvider timeProvider,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        long startTimestamp = timeProvider.GetTimestamp();
        bool infinite = timeout == Timeout.InfiniteTimeSpan;
        using PeriodicTimer timer = new(TimeSpan.FromMilliseconds(5), timeProvider);
        while (true)
        {
            RingBufferEndpointState peer = peerState();
            if (peer is RingBufferEndpointState.Running)
            {
                return true;
            }

            if (peer is RingBufferEndpointState.Stopped or RingBufferEndpointState.Faulted)
            {
                return false;
            }

            if (!infinite && timeProvider.GetElapsedTime(startTimestamp) >= timeout)
            {
                return false;
            }

            await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    internal static void WaitForNotification(
        IEndpoint buffer,
        SessionWaitMode waitMode,
        SessionNotification? notification,
        bool waitOnDataSignal,
        ref SpinWait spin,
        CancellationToken cancellationToken)
    {
        if (waitMode == SessionWaitMode.Notification)
        {
            try
            {
                // Declare the wait before the re-check: if the peer frees/provides
                // data after this point it reads the flag and raises the latch, so
                // the wait below cannot miss it.
                buffer.SetWaiting(true);

                // Full fence between our waiting-flag store and the count load so
                // the flag cannot be delayed past the check on weak memory models.
                Interlocked.MemoryBarrier();
                try
                {
                    if (waitOnDataSignal ? buffer.Count > 0 : buffer.Count < buffer.Capacity)
                    {
                        return;
                    }

                    ISessionSignal signal = waitOnDataSignal ? notification!.Data : notification!.Space;
                    signal.Wait(SessionWaitTiming.WaitSlice, cancellationToken);
                }
                finally
                {
                    buffer.SetWaiting(false);
                }
            }
            catch (ObjectDisposedException)
            {
            }

            return;
        }

        if (waitMode == SessionWaitMode.SpinOnly)
        {
            Thread.SpinWait(64);
        }
        else
        {
            spin.SpinOnce();
        }
    }

    internal static void SignalIfPeerWaiting(
        IEndpoint buffer,
        SessionWaitMode waitMode,
        SessionNotification? notification,
        bool signalData)
    {
        if (waitMode != SessionWaitMode.Notification)
        {
            return;
        }

        try
        {
            if (buffer.IsPeerWaiting())
            {
                ISessionSignal signal = signalData ? notification!.Data : notification!.Space;
                signal.Signal();
            }
        }
        catch (ObjectDisposedException)
        {
        }
    }
}
