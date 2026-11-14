using System.Runtime.InteropServices;
using Sparc.Client;
using Sparc.Core;
using Sparc.InMemory;

namespace Sparc.UnitTests;

/// <summary>
/// Guards the engine seam: one table path, the connect-cancel shape, and the two
/// role-specific stall orders that the loops used to encode inline.
/// </summary>
public class SessionRunEngineTests
{
    private static string NewName() => "spsc-engine-" + Guid.NewGuid().ToString("N");

    [Fact]
    public void CompletesWhenTheRequestedCountIsReached()
    {
        var role = new ScriptedRole { Requested = 2, ConnectResult = true };
        SessionRunContext context = new(new NoopEndpoint(), 2, TimeSpan.Zero, SessionWaitMode.SpinThenSleep, null, false);

        SessionRunOutcome outcome = SessionRunEngine.Run(ref role, in context, TimeProvider.System, TestContext.Current.CancellationToken);

        Assert.Equal(SessionStopReason.Completed, outcome.Decision.Reason);
        Assert.Equal(2L, outcome.Delivered);
        Assert.Null(outcome.Decision.Message);
    }

    [Fact]
    public void ConnectCancelIsZeroShapedThroughTheTable()
    {
        var role = new ScriptedRole { ConnectResult = false, Requested = 5 };
        SessionRunContext context = new(new NoopEndpoint(), 5, TimeSpan.Zero, SessionWaitMode.SpinThenSleep, null, false);

        SessionRunOutcome outcome = SessionRunEngine.Run(ref role, in context, TimeProvider.System, TestContext.Current.CancellationToken);

        Assert.Equal(SessionStopReason.Cancelled, outcome.Decision.Reason);
        Assert.Equal(0L, outcome.Delivered);
        Assert.Equal(TimeSpan.Zero, outcome.RunElapsed);
        Assert.Equal(5L, outcome.Decision.Outstanding);
    }

    [Fact]
    public void VerificationFailureCarriesTheDetail()
    {
        var role = new ScriptedRole { ConnectResult = true, StopWith = SessionTerminalEvent.VerificationFailed, Detail = "message 3 has type 2, expected 1." };
        SessionRunContext context = new(new NoopEndpoint(), 5, TimeSpan.Zero, SessionWaitMode.SpinThenSleep, null, false);

        SessionRunOutcome outcome = SessionRunEngine.Run(ref role, in context, TimeProvider.System, TestContext.Current.CancellationToken);

        Assert.Equal(SessionStopReason.VerificationFailed, outcome.Decision.Reason);
        Assert.Equal("message 3 has type 2, expected 1.", outcome.Decision.Message);
    }

    [Fact]
    public void ProducerChecksCancellationBeforeThePeer()
    {
        InMemoryMemoryRegionFactory factory = new();
        using SharedRingBuffer producer = SharedRingBuffer.OpenOrCreate(factory, NewName(), 2, 64);
        producer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);

        ProducerRole role = new(
            producer,
            new ProducerSessionOptions { Count = 10, PayloadSize = 32, FullTimeout = TimeSpan.FromMilliseconds(100) },
            TimeProvider.System,
            progress: null);

        Assert.Equal(SessionRunStepKind.Progressed, role.Step(TestContext.Current.CancellationToken).Kind);
        Assert.Equal(SessionRunStepKind.Progressed, role.Step(TestContext.Current.CancellationToken).Kind);

        using CancellationTokenSource cancelled = new();
        cancelled.Cancel();
        SessionRunStep step = role.Step(cancelled.Token);

        // The producer polls cancellation before it reads the peer state.
        Assert.Equal(SessionRunStepKind.Terminal, step.Kind);
        Assert.Equal(SessionTerminalEvent.Cancelled, step.Terminal);
    }

    [Fact]
    public void ConsumerClassifiesAPeerStopBeforeCancellation()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using (SharedRingBuffer producer = SharedRingBuffer.OpenOrCreate(factory, name, 2, 64))
        {
            producer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);
        }

        using SharedRingBuffer consumer = SharedRingBuffer.OpenOrCreate(factory, name, 2, 64);
        consumer.Connect(RingBufferEndpointRole.Consumer, cancellationToken: TestContext.Current.CancellationToken);

        ConsumerRole role = new(
            consumer,
            new ConsumerSessionOptions { Count = 10, IdleTimeout = TimeSpan.FromMilliseconds(100) },
            TimeProvider.System,
            progress: null);

        using CancellationTokenSource cancelled = new();
        cancelled.Cancel();
        SessionRunStep step = role.Step(cancelled.Token);

        // The consumer observes the terminal producer (and drains) before it
        // looks at the token, exactly as the inline loop did.
        Assert.Equal(SessionRunStepKind.Terminal, step.Kind);
        Assert.Equal(SessionTerminalEvent.PeerStopped, step.Terminal);

        SessionStopInput input = role.BuildStopInput(in step);
        Assert.Equal(0L, input.Delivered);
        Assert.Equal(RingBufferEndpointState.Stopped, input.PeerState);
    }

    /// <summary>A role with no message work, for engine-shape tests.</summary>
    [StructLayout(LayoutKind.Auto)]
    private struct ScriptedRole : ISessionRunRole
    {
        private long _delivered;

        public long Requested;
        public bool ConnectResult;
        public SessionTerminalEvent StopWith;
        public string? Detail;

        public readonly long Delivered => _delivered;

        public readonly long FirstTimestamp => -1;

        public readonly long LastTimestamp => -1;

        public readonly bool TryConnect(CancellationToken cancellationToken) => ConnectResult;

        public SessionRunStep Step(CancellationToken cancellationToken)
        {
            if (StopWith != SessionTerminalEvent.CountReached)
            {
                return SessionRunStep.Stop(StopWith);
            }

            _delivered++;
            return SessionRunStep.Progressed;
        }

        public readonly void ReportProgress(TimeSpan elapsed)
        {
        }

        public readonly SessionStopInput BuildStopInput(in SessionRunStep terminal) =>
            new()
            {
                Event = terminal.Terminal,
                Role = SessionRole.Producer,
                Delivered = Delivered,
                Requested = Requested,
                Detail = Detail,
            };
    }

    /// <summary>The engine only touches an endpoint on notification waits.</summary>
    [StructLayout(LayoutKind.Auto)]
    private struct NoopEndpoint : IEndpoint
    {
        public readonly string Name => "noop";

        public readonly int Capacity => 2;

        public readonly int SlotSize => 64;

        public readonly int MaxPayloadSize => 56;

        public readonly long HeadSequence => 0;

        public readonly long TailSequence => 0;

        public readonly bool IsEmpty => true;

        public readonly int Count => 0;

        public readonly RingBufferEndpointState ProducerState => RingBufferEndpointState.NotPresent;

        public readonly RingBufferEndpointState ConsumerState => RingBufferEndpointState.NotPresent;

        public readonly bool IsPeerWaiting() => false;

        public readonly void SetWaiting(bool waiting)
        {
        }

        public readonly void Abort()
        {
        }

        public readonly void Dispose()
        {
        }
    }
}
