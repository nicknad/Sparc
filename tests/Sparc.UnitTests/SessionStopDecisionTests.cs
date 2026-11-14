using Sparc.Client;
using Sparc.Core;

namespace Sparc.UnitTests;

/// <summary>
/// The stop-decision table is pure, so these tests drive every terminal event
/// without a region, a clock or a timeout.
/// </summary>
public class SessionStopDecisionTests
{
    [Fact]
    public void ReachingTheRequestedCountCompletesCleanly()
    {
        SessionStopDecision decision = SessionStopDecision.Resolve(new SessionStopInput
        {
            Event = SessionTerminalEvent.CountReached,
            Role = SessionRole.Producer,
            Delivered = 10_000,
            Requested = 10_000,
        });

        Assert.Equal(SessionStopReason.Completed, decision.Reason);
        Assert.Null(decision.Message);
        Assert.Equal(0L, decision.Outstanding);
    }

    [Fact]
    public void CancellationCarriesNoTextAndKeepsWhatIsOutstanding()
    {
        SessionStopDecision decision = SessionStopDecision.Resolve(new SessionStopInput
        {
            Event = SessionTerminalEvent.Cancelled,
            Role = SessionRole.Consumer,
            Delivered = 3,
            Requested = 10,
        });

        Assert.Equal(SessionStopReason.Cancelled, decision.Reason);
        Assert.Null(decision.Message);
        Assert.Equal(7L, decision.Outstanding);
    }

    [Fact]
    public void ProducerPeerStopNamesTheUnsentMessages()
    {
        SessionStopDecision decision = SessionStopDecision.Resolve(new SessionStopInput
        {
            Event = SessionTerminalEvent.PeerStopped,
            Role = SessionRole.Producer,
            Delivered = 4,
            Requested = 100,
            PeerState = RingBufferEndpointState.Stopped,
        });

        Assert.Equal(SessionStopReason.PeerStopped, decision.Reason);
        Assert.Equal("consumer is gone (state=Stopped) with 96 messages unsent.", decision.Message);
        Assert.Equal(96L, decision.Outstanding);
    }

    [Fact]
    public void ProducerPeerStopWithoutACountReportsWhatWasProduced()
    {
        SessionStopDecision decision = SessionStopDecision.Resolve(new SessionStopInput
        {
            Event = SessionTerminalEvent.PeerStopped,
            Role = SessionRole.Producer,
            Delivered = 7,
            Requested = 0,
            PeerState = RingBufferEndpointState.Faulted,
        });

        Assert.Equal(SessionStopReason.PeerStopped, decision.Reason);
        Assert.Equal("consumer is gone (state=Faulted) after 7 messages.", decision.Message);
        Assert.Equal(0L, decision.Outstanding);
    }

    [Fact]
    public void ConsumerPeerStopShortOfTheCountNamesHowFarItGot()
    {
        SessionStopDecision decision = SessionStopDecision.Resolve(new SessionStopInput
        {
            Event = SessionTerminalEvent.PeerStopped,
            Role = SessionRole.Consumer,
            Delivered = 3,
            Requested = 10,
            PeerState = RingBufferEndpointState.Stopped,
        });

        Assert.Equal(SessionStopReason.PeerStopped, decision.Reason);
        Assert.Equal("producer stopped after 3 of 10 messages.", decision.Message);
        Assert.Equal(7L, decision.Outstanding);
    }

    [Fact]
    public void ConsumerPeerStopAfterTheExpectedCountIsClean()
    {
        SessionStopDecision decision = SessionStopDecision.Resolve(new SessionStopInput
        {
            Event = SessionTerminalEvent.PeerStopped,
            Role = SessionRole.Consumer,
            Delivered = 10,
            Requested = 10,
            PeerState = RingBufferEndpointState.Stopped,
        });

        Assert.Equal(SessionStopReason.Completed, decision.Reason);
        Assert.Null(decision.Message);
        Assert.Equal(0L, decision.Outstanding);
    }

    [Fact]
    public void ProducerTimeoutNamesTheUnsentMessagesAndThePeerState()
    {
        SessionStopDecision decision = SessionStopDecision.Resolve(new SessionStopInput
        {
            Event = SessionTerminalEvent.Timeout,
            Role = SessionRole.Producer,
            Delivered = 4,
            Requested = 100,
            Timeout = TimeSpan.FromSeconds(30),
            PeerState = RingBufferEndpointState.Running,
        });

        Assert.Equal(SessionStopReason.Timeout, decision.Reason);
        Assert.Equal(
            "buffer stayed full for 30.0s (96 messages unsent, consumer state=Running).",
            decision.Message);
        Assert.Equal(96L, decision.Outstanding);
    }

    [Fact]
    public void ProducerTimeoutWithoutACountReportsWhatWasProduced()
    {
        SessionStopDecision decision = SessionStopDecision.Resolve(new SessionStopInput
        {
            Event = SessionTerminalEvent.Timeout,
            Role = SessionRole.Producer,
            Delivered = 7,
            Requested = 0,
            Timeout = TimeSpan.FromSeconds(2.5),
            PeerState = RingBufferEndpointState.NotPresent,
        });

        Assert.Equal(SessionStopReason.Timeout, decision.Reason);
        Assert.Equal(
            "buffer stayed full for 2.5s (produced=7, consumer state=NotPresent).",
            decision.Message);
    }

    [Fact]
    public void ConsumerTimeoutUsesTheIdleWording()
    {
        SessionStopDecision decision = SessionStopDecision.Resolve(new SessionStopInput
        {
            Event = SessionTerminalEvent.Timeout,
            Role = SessionRole.Consumer,
            Delivered = 12,
            Timeout = TimeSpan.FromSeconds(5),
            PeerState = RingBufferEndpointState.Running,
        });

        Assert.Equal(SessionStopReason.Timeout, decision.Reason);
        Assert.Equal(
            "no messages for 5.0s (producer state=Running, received=12).",
            decision.Message);
    }

    [Fact]
    public void VerificationFailurePassesTheDetailThrough()
    {
        SessionStopDecision decision = SessionStopDecision.Resolve(new SessionStopInput
        {
            Event = SessionTerminalEvent.VerificationFailed,
            Role = SessionRole.Consumer,
            Delivered = 3,
            Requested = 10,
            Detail = "message 3 has type 2, expected 1.",
        });

        Assert.Equal(SessionStopReason.VerificationFailed, decision.Reason);
        Assert.Equal("message 3 has type 2, expected 1.", decision.Message);
        Assert.Equal(7L, decision.Outstanding);
    }

    [Fact]
    public void EveryTerminalEventResolves()
    {
        foreach (SessionTerminalEvent terminal in Enum.GetValues<SessionTerminalEvent>())
        {
            SessionStopDecision decision = SessionStopDecision.Resolve(new SessionStopInput
            {
                Event = terminal,
                Role = SessionRole.Producer,
                Delivered = 1,
                Requested = 2,
            });

            Assert.True(Enum.IsDefined(decision.Reason));
        }
    }
}
