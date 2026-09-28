using Sparc.Client;

namespace Sparc.UnitTests;

public class NamedSignalTests
{
    [Fact]
    public void NamedSignalRoundTripsBetweenInstances()
    {
        string name = "sparc-signal-" + Guid.NewGuid().ToString("N");
        using NamedSessionSignal signal = new(name);
        using NamedSessionSignal peer = new(name);

        Assert.False(peer.Wait(TimeSpan.FromMilliseconds(50), TestContext.Current.CancellationToken));

        signal.Signal();
        Assert.True(peer.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        // The raise is consumed exactly once.
        Assert.False(peer.Wait(TimeSpan.FromMilliseconds(50), TestContext.Current.CancellationToken));
    }

    [Fact]
    public void SessionNotificationCreateNamedProvidesBothLatches()
    {
        string region = "sparc-signal-" + Guid.NewGuid().ToString("N");
        using SessionNotification first = SessionNotification.CreateNamed(region);
        using SessionNotification second = SessionNotification.CreateNamed(region);

        first.Data.Signal();
        Assert.True(second.Data.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        second.Space.Signal();
        Assert.True(first.Space.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
    }

    [Fact]
    public void NamedWaitHonorsCancellation()
    {
        using NamedSessionSignal signal = new("sparc-signal-" + Guid.NewGuid().ToString("N"));
        using CancellationTokenSource cancellation = new();
        cancellation.Cancel();

        Assert.False(signal.Wait(TimeSpan.FromSeconds(5), cancellation.Token));
    }
}
