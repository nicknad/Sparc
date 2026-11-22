using System.Runtime.InteropServices;
using Sparc.Core;

namespace Sparc.Client;

/// <summary>
/// The per-run data the engine needs but does not own: the endpoint to wait on,
/// the requested count, and the wait wiring. Kept off <see cref="ISessionRunRole"/>
/// so the role seam carries behaviour, not configuration.
/// </summary>
internal readonly record struct SessionRunContext(
    IEndpoint Endpoint,
    long Requested,
    TimeSpan PerMessageDelay,
    SessionWaitMode WaitMode,
    SessionNotification? Notification,
    bool WaitOnDataSignal);

/// <summary>What one role step did.</summary>
internal enum SessionRunStepKind : byte
{
    /// <summary>The role moved one message; the engine owns pacing/progress/cancel.</summary>
    Progressed = 0,

    /// <summary>The role has no work and has already run its own stall checks.</summary>
    Stalled = 1,

    /// <summary>The run must stop; <see cref="SessionRunStep.Terminal"/> names the event.</summary>
    Terminal = 2,
}

/// <summary>
/// One step result. Small on purpose: the hot loop returns this per message.
/// <see cref="Terminal"/> is only meaningful when <see cref="Kind"/> is
/// <see cref="SessionRunStepKind.Terminal"/>.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal readonly struct SessionRunStep
{
    private SessionRunStep(SessionRunStepKind kind, SessionTerminalEvent terminal)
    {
        Kind = kind;
        Terminal = terminal;
    }

    internal SessionRunStepKind Kind { get; }

    internal SessionTerminalEvent Terminal { get; }

    internal static SessionRunStep Progressed => new(SessionRunStepKind.Progressed, default);

    internal static SessionRunStep Stalled => new(SessionRunStepKind.Stalled, default);

    internal static SessionRunStep Stop(SessionTerminalEvent terminal) =>
        new(SessionRunStepKind.Terminal, terminal);
}

/// <summary>
/// Everything a finished run hands back to a session: the table's decision and
/// the three windows the public results need.
/// </summary>
internal readonly record struct SessionRunOutcome(
    SessionStopDecision Decision,
    long Delivered,
    TimeSpan RunElapsed,
    TimeSpan StreamElapsed,
    TimeSpan ActiveElapsed);

/// <summary>
/// What the engine needs from a role. Implemented only by the two internal role
/// structs, so the engine is generic over it and the calls devirtualize: no
/// boxing and no interface dispatch on the message path.
/// </summary>
internal interface ISessionRunRole
{
    /// <summary>Messages this run has moved so far.</summary>
    long Delivered { get; }

    /// <summary>Timestamp of the first moved message, or -1 when nothing moved.</summary>
    long FirstTimestamp { get; }

    /// <summary>Timestamp of the last moved message, or -1 when nothing moved.</summary>
    long LastTimestamp { get; }

    /// <summary>Claims the role. False means the claim was cancelled.</summary>
    bool TryConnect(CancellationToken cancellationToken);

    /// <summary>
    /// One message attempt, including the role's own no-progress checks in the
    /// role's own order. Never throws for a normal stop.
    /// </summary>
    SessionRunStep Step(CancellationToken cancellationToken);

    /// <summary>Reports progress for the current delivered count.</summary>
    void ReportProgress(TimeSpan elapsed);

    /// <summary>Fills the pure table's input from the role's state and a terminal step.</summary>
    SessionStopInput BuildStopInput(in SessionRunStep terminal);
}

/// <summary>
/// The one run loop. It owns the skeleton both sessions used to duplicate:
/// connect-cancel shaping, the loop bound, pacing, progress cadence, the
/// cancellation mask, the wait, the run clock, and the single call to
/// <see cref="SessionStopDecision.Resolve"/>. The role owns the message step,
/// the stall order, the drain, and the outcome fields only it can know.
/// </summary>
internal static class SessionRunEngine
{
    private const int ProgressReports = 10;
    private const long CancellationCheckMask = 0xFFFF;

    internal static SessionRunOutcome Run<TRole>(
        ref TRole role,
        in SessionRunContext context,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
        where TRole : struct, ISessionRunRole
    {
        if (!role.TryConnect(cancellationToken))
        {
            // The role was not claimed: the table turns this into Cancelled and
            // the session shapes its zero result.
            return FinishCancelled(ref role);
        }

        long runStartTimestamp = timeProvider.GetTimestamp();
        long pacingTicks = MessagePacer.TicksFor(timeProvider, context.PerMessageDelay);
        long progressInterval = ProgressInterval(context.Requested);
        long cancellationCounter = 0;
        SpinWait spin = default;
        SessionRunStep terminal = SessionRunStep.Stop(SessionTerminalEvent.CountReached);

        while (context.Requested == 0 || role.Delivered < context.Requested)
        {
            SessionRunStep step = role.Step(cancellationToken);

            if (step.Kind == SessionRunStepKind.Terminal)
            {
                terminal = step;
                break;
            }

            if (step.Kind == SessionRunStepKind.Stalled)
            {
                SessionRuntime.WaitForNotification(
                    context.Endpoint,
                    context.WaitMode,
                    context.Notification,
                    context.WaitOnDataSignal,
                    ref spin,
                    cancellationToken);
                continue;
            }

            long delivered = role.Delivered;
            spin.Reset();

            // Never pace after the final message: it would inflate the active
            // window by one full delay.
            if (context.Requested == 0 || delivered < context.Requested)
            {
                MessagePacer.Wait(timeProvider, runStartTimestamp, delivered, pacingTicks, cancellationToken);
            }

            if (progressInterval > 0 && delivered % progressInterval == 0)
            {
                role.ReportProgress(timeProvider.GetElapsedTime(runStartTimestamp));
            }

            // Paced runs can spend minutes between mask boundaries, so poll every
            // message once pacing is on; unpaced runs keep the batched fast path.
            if (ShouldCheckCancellation(ref cancellationCounter, pacingTicks)
                && cancellationToken.IsCancellationRequested)
            {
                terminal = SessionRunStep.Stop(SessionTerminalEvent.Cancelled);
                break;
            }
        }

        long endTimestamp = timeProvider.GetTimestamp();
        return Finish(
            ref role,
            in terminal,
            timeProvider.GetElapsedTime(runStartTimestamp, endTimestamp),
            endTimestamp,
            timeProvider);
    }

    /// <summary>The connect-cancel shape: no message moved, every window is zero.</summary>
    private static SessionRunOutcome FinishCancelled<TRole>(ref TRole role)
        where TRole : struct, ISessionRunRole
    {
        SessionStopInput input = role.BuildStopInput(SessionRunStep.Stop(SessionTerminalEvent.Cancelled));
        SessionStopDecision decision = SessionStopDecision.Resolve(in input);
        return new SessionRunOutcome(decision, input.Delivered, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero);
    }

    /// <summary>The one place a terminal event becomes a public outcome.</summary>
    private static SessionRunOutcome Finish<TRole>(
        ref TRole role,
        in SessionRunStep terminal,
        TimeSpan runElapsed,
        long endTimestamp,
        TimeProvider timeProvider)
        where TRole : struct, ISessionRunRole
    {
        SessionStopInput input = role.BuildStopInput(in terminal);
        SessionStopDecision decision = SessionStopDecision.Resolve(in input);

        long first = role.FirstTimestamp;
        long last = role.LastTimestamp;
        TimeSpan active = first < 0 ? TimeSpan.Zero : timeProvider.GetElapsedTime(first, endTimestamp);
        TimeSpan stream = input.Delivered > 1 && first >= 0 && last >= 0
            ? timeProvider.GetElapsedTime(first, last)
            : runElapsed;

        return new SessionRunOutcome(decision, input.Delivered, runElapsed, stream, active);
    }

    private static long ProgressInterval(long count) =>
        count > 0 ? Math.Max(1, count / ProgressReports) : 0;

    private static bool ShouldCheckCancellation(ref long counter, long pacingTicks) =>
        pacingTicks > 0 || (++counter & CancellationCheckMask) == 0;
}

/// <summary>
/// The stamp-once / elapsed-window arithmetic both roles share. It deliberately
/// does not decide when to consult the peer: each role calls
/// <see cref="Elapsed"/> at its own point in its own order. The stamped flag is
/// explicit so a clock whose first timestamp is zero still times out.
/// </summary>
internal struct SessionStallClock
{
    private readonly bool _enabled;
    private readonly long _ticks;
    private long _since;
    private bool _stamped;

    internal SessionStallClock(bool enabled, long ticks)
    {
        _enabled = enabled;
        _ticks = ticks;
        _since = 0;
        _stamped = false;
    }

    /// <summary>Clears the window; called by the role on the progress path.</summary>
    internal void Reset()
    {
        _stamped = false;
        _since = 0;
    }

    /// <summary>
    /// First call stamps the window and returns false. Later calls return true
    /// and report whether the enabled window has elapsed.
    /// </summary>
    internal bool Elapsed(long now, out bool expired)
    {
        if (!_stamped)
        {
            _stamped = true;
            _since = now;
            expired = false;
            return false;
        }

        expired = _enabled && now - _since >= _ticks;
        return true;
    }
}
