using System.Buffers.Binary;
using Sparc.Channels;
using Sparc.InMemory;

namespace Sparc.UnitTests;

public class ChannelTests
{
    private static string NewName() => "sparc-channel-" + Guid.NewGuid().ToString("N");

    [Fact]
    public void CreateInProcessRoundTripsValuesInOrder()
    {
        using SparcChannel<int> channel = SparcChannel<int>.CreateInProcess(
            new InMemoryMemoryRegionFactory(), NewName(), Int32Codec.Instance, capacity: 128,
            cancellationToken: TestContext.Current.CancellationToken);

        for (int i = 0; i < 100; i++)
        {
            Assert.True(channel.Writer.TryWrite(i));
        }

        for (int i = 0; i < 100; i++)
        {
            Assert.True(channel.Reader.TryRead(out int value));
            Assert.Equal(i, value);
        }

        Assert.False(channel.Reader.TryRead(out _));
    }

    [Fact]
    public void TryWriteReturnsFalseWhenTheRingIsFull()
    {
        using SparcChannel<int> channel = SparcChannel<int>.CreateInProcess(
            new InMemoryMemoryRegionFactory(), NewName(), Int32Codec.Instance, capacity: 4,
            cancellationToken: TestContext.Current.CancellationToken);

        for (int i = 0; i < 4; i++)
        {
            Assert.True(channel.Writer.TryWrite(i));
        }

        Assert.False(channel.Writer.TryWrite(99));
        Assert.True(channel.Reader.TryRead(out int first));
        Assert.Equal(0, first);
        Assert.True(channel.Writer.TryWrite(99));
    }

    [Fact]
    public async Task AsyncReadWriteAndReadAllCompleteAtEndOfStream()
    {
        using SparcChannel<int> channel = SparcChannel<int>.CreateInProcess(
            new InMemoryMemoryRegionFactory(), NewName(), Int32Codec.Instance, capacity: 16,
            idleTimeout: TimeSpan.FromMilliseconds(50),
            cancellationToken: TestContext.Current.CancellationToken);

        for (int i = 0; i < 10; i++)
        {
            await channel.Writer.WriteAsync(i, TestContext.Current.CancellationToken);
        }

        Assert.Equal(0, await channel.Reader.ReadAsync(TestContext.Current.CancellationToken));

        channel.Writer.Dispose();

        List<int> rest = [];
        await foreach (int value in channel.Reader.ReadAllAsync(TestContext.Current.CancellationToken))
        {
            rest.Add(value);
        }

        Assert.Equal(Enumerable.Range(1, 9), rest);
    }

    [Fact]
    public async Task WriteAsyncWaitsForSpace()
    {
        using SparcChannel<int> channel = SparcChannel<int>.CreateInProcess(
            new InMemoryMemoryRegionFactory(), NewName(), Int32Codec.Instance, capacity: 2,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(channel.Writer.TryWrite(0));
        Assert.True(channel.Writer.TryWrite(1));

        Task write = channel.Writer.WriteAsync(2, TestContext.Current.CancellationToken).AsTask();
        Assert.False(write.IsCompleted);

        Assert.True(channel.Reader.TryRead(out int first));
        Assert.Equal(0, first);
        await write.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.True(channel.Reader.TryRead(out int second));
        Assert.True(channel.Reader.TryRead(out int third));
        Assert.Equal(1, second);
        Assert.Equal(2, third);
    }

    [Fact]
    public async Task WriteAsyncFailsWhenTheConsumerStopped()
    {
        using SparcChannel<int> channel = SparcChannel<int>.CreateInProcess(
            new InMemoryMemoryRegionFactory(), NewName(), Int32Codec.Instance, capacity: 2,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(channel.Writer.TryWrite(0));
        Assert.True(channel.Writer.TryWrite(1));
        channel.Reader.Dispose();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => channel.Writer.WriteAsync(2, TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task JsonCodecRoundTripsRecords()
    {
        using SparcChannel<Order> channel = SparcChannel<Order>.CreateInProcess(
            new InMemoryMemoryRegionFactory(), NewName(), new JsonCodec<Order>(128), capacity: 4,
            cancellationToken: TestContext.Current.CancellationToken);

        Order sent = new(7, "widget");
        await channel.Writer.WriteAsync(sent, TestContext.Current.CancellationToken);
        Order received = await channel.Reader.ReadAsync(TestContext.Current.CancellationToken);

        Assert.Equal(sent, received);
    }

    [Fact]
    public async Task OversizedMessagesThrow()
    {
        using SparcChannel<Order> channel = SparcChannel<Order>.CreateInProcess(
            new InMemoryMemoryRegionFactory(), NewName(), new JsonCodec<Order>(8), capacity: 4,
            cancellationToken: TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => channel.Writer.WriteAsync(new Order(1, "much too large"), TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public void JsonCodecRejectsMessagesBeyondMaxSizeEvenWithLargerDestination()
    {
        JsonCodec<Order> codec = new(8);

        Assert.Throws<InvalidOperationException>(
            () => codec.Encode(new Order(1, "much too large"), new byte[4096]));
    }

    public sealed record Order(int Id, string Sku);

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
