using System.Buffers.Binary;
using Microsoft.Extensions.Time.Testing;
using Sparc.Channels;
using Sparc.Client;
using Sparc.Testing;

namespace Sparc.UnitTests;

public class TestingPackageTests
{
    [Fact]
    public void TestRingRoundTripsThroughBothEndpoints()
    {
        using TestSparcRing ring = TestSparcRing.Create(capacity: 4, slotSize: 64);

        Assert.True(ring.Producer.TryPublish(7, new byte[] { 1, 2, 3 }));

        Span<byte> destination = new byte[ring.Consumer.MaxPayloadSize];
        Assert.True(ring.Consumer.TryRead(destination, out int bytesRead, out int type));
        Assert.Equal(3, bytesRead);
        Assert.Equal(7, type);
    }

    [Fact]
    public async Task FakeTimeProviderMakesSessionTimeoutsDeterministic()
    {
        FakeTimeProvider time = new();
        using TestSparcRing ring = TestSparcRing.Create(capacity: 4, slotSize: 64, timeProvider: time);

        ProducerSession session = ring.CreateProducerSession(new ProducerSessionOptions
        {
            Count = 100,
            PayloadSize = 32,
            FullTimeout = TimeSpan.FromMilliseconds(100),
        });

        Task<ProducerRunResult> run = session.RunAsync(TestContext.Current.CancellationToken);

        await Task.Run(
            () =>
            {
                while (ring.Producer.Count < 4)
                {
                    Thread.Sleep(1);
                }
            },
            TestContext.Current.CancellationToken);

        time.Advance(TimeSpan.FromMilliseconds(100));

        ProducerRunResult result = await run.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.Equal(SessionStopReason.Timeout, result.Reason);
        Assert.Equal(4, result.Produced);
    }

    [Fact]
    public async Task ChannelReadWithTimeoutReturnsQueuedMessages()
    {
        using SparcChannel<int> channel = TestSparcChannel.Create(Int32Codec.Instance, capacity: 2);

        Assert.True(channel.Writer.TryWrite(1));
        int value = await channel.Reader.ReadWithTimeoutAsync(
            TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(1, value);
    }

    [Fact]
    public async Task ChannelReadWithTimeoutFailsWhenNothingArrives()
    {
        using SparcChannel<int> channel = TestSparcChannel.Create(
            Int32Codec.Instance, capacity: 2, idleTimeout: TimeSpan.FromMilliseconds(50));

        await Assert.ThrowsAsync<TimeoutException>(
            () => channel.Reader.ReadWithTimeoutAsync(TimeSpan.FromMilliseconds(100), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ChannelWriteWithTimeoutFailsWhileTheRingStaysFull()
    {
        using SparcChannel<int> channel = TestSparcChannel.Create(Int32Codec.Instance, capacity: 2);

        Assert.True(channel.Writer.TryWrite(2));
        Assert.True(channel.Writer.TryWrite(3));
        await Assert.ThrowsAsync<TimeoutException>(
            () => channel.Writer.WriteWithTimeoutAsync(4, TimeSpan.FromMilliseconds(100), TestContext.Current.CancellationToken));
    }

    private sealed class Int32Codec : ISparcCodec<int>
    {
        public static Int32Codec Instance { get; } = new();

        public int MaxSize => sizeof(int);

        public int Encode(int item, Span<byte> destination)
        {
            BinaryPrimitives.WriteInt32LittleEndian(destination, item);
            return sizeof(int);
        }

        public int Decode(ReadOnlySpan<byte> source) => BinaryPrimitives.ReadInt32LittleEndian(source);
    }
}
