using System.Buffers.Binary;
using Sparc.Channels;
using Sparc.Core;
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

    [Fact]
    public async Task PumpDrainsMessagesCommittedBeforeTheProducerStops()
    {
        FakeConsumerEndpoint endpoint = new(
            blockReads: false, (1, [1, 0, 0, 0]), (2, [2, 0, 0, 0]))
        {
            ProducerState = RingBufferEndpointState.Stopped,
        };
        using SparcChannelReader<int> reader = new(endpoint, Int32Codec.Instance, TimeSpan.FromMilliseconds(20));

        List<int> received = [];
        await foreach (int value in reader.ReadAllAsync(TestContext.Current.CancellationToken))
        {
            received.Add(value);
        }

        Assert.Equal([1, 2], received);
    }

    [Fact]
    public async Task ConcurrentTryReadDeliversEachMessageExactlyOnce()
    {
        using SparcChannel<int> channel = SparcChannel<int>.CreateInProcess(
            new InMemoryMemoryRegionFactory(), NewName(), Int32Codec.Instance, capacity: 256,
            cancellationToken: TestContext.Current.CancellationToken);

        const int Messages = 512;
        int[] deliveries = new int[Messages];
        int duplicates = 0;
        int produced = 0;

        Task asyncConsumer = Task.Run(
            async () =>
            {
                await foreach (int value in channel.Reader.ReadAllAsync(TestContext.Current.CancellationToken)
                    .ConfigureAwait(false))
                {
                    if (Interlocked.Increment(ref deliveries[value]) != 1)
                    {
                        Interlocked.Increment(ref duplicates);
                    }
                }
            },
            TestContext.Current.CancellationToken);

        Task[] directReaders = new Task[3];
        for (int r = 0; r < directReaders.Length; r++)
        {
            directReaders[r] = Task.Run(
                () =>
                {
                    while (true)
                    {
                        if (channel.Reader.TryRead(out int value))
                        {
                            if (Interlocked.Increment(ref deliveries[value]) != 1)
                            {
                                Interlocked.Increment(ref duplicates);
                            }
                        }
                        else if (Volatile.Read(ref produced) == Messages)
                        {
                            break;
                        }
                        else
                        {
                            Thread.Yield();
                        }
                    }
                },
                TestContext.Current.CancellationToken);
        }

        for (int i = 0; i < Messages; i++)
        {
            while (!channel.Writer.TryWrite(i))
            {
                Thread.Yield();
            }
        }

        Volatile.Write(ref produced, Messages);
        channel.Writer.Dispose();
        await Task.WhenAll(directReaders);
        await asyncConsumer;

        Assert.Equal(0, duplicates);
        Assert.Equal(Messages, deliveries.Count(static count => count == 1));
    }

    [Fact]
    public async Task DisposingDuringABlockedReadCompletesTheChannelCleanly()
    {
        FakeConsumerEndpoint endpoint = new(blockReads: true);
        SparcChannelReader<int> reader = new(endpoint, Int32Codec.Instance, TimeSpan.FromSeconds(30));

        Task drain = Task.Run(
            async () =>
            {
                await foreach (int _ in reader.ReadAllAsync(TestContext.Current.CancellationToken)
                    .ConfigureAwait(false))
                {
                }
            },
            TestContext.Current.CancellationToken);

        await endpoint.ReadStarted.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        reader.Dispose();
        await drain.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    [Fact]
    public void ThrowingCodecLeavesTheWriterUsable()
    {
        using SparcChannel<Order> channel = SparcChannel<Order>.CreateInProcess(
            new InMemoryMemoryRegionFactory(), NewName(), new ThrowingCodec(), capacity: 4,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Throws<InvalidOperationException>(() => channel.Writer.TryWrite(new Order(1, "boom")));
        Assert.True(channel.Writer.TryWrite(new Order(2, "ok")));
        Assert.True(channel.Reader.TryRead(out Order? value));
        Assert.Equal(2, value!.Id);
    }

    public sealed record Order(int Id, string Sku);

    /// <summary>
    /// Scripted endpoint for reader tests: either throws a
    /// <see cref="RingBufferTimeoutException"/> per read (idle) or blocks until
    /// disposed, and serves pre-queued messages through <c>TryBeginRead</c>.
    /// </summary>
    private sealed class FakeConsumerEndpoint : IConsumerEndpoint
    {
        private readonly Queue<(int Type, byte[] Payload)> _messages = new();
        private readonly ManualResetEventSlim _unblockReads = new(false);
        private readonly bool _blockReads;

        public FakeConsumerEndpoint(bool blockReads, params (int Type, byte[] Payload)[] messages)
        {
            _blockReads = blockReads;
            foreach ((int type, byte[] payload) in messages)
            {
                _messages.Enqueue((type, payload));
            }
        }

        public SemaphoreSlim ReadStarted { get; } = new(0);

        public RingBufferEndpointState ProducerState { get; set; } = RingBufferEndpointState.Running;

        public string Name => "fake-consumer";

        public int Capacity => 16;

        public int SlotSize => 64;

        public int MaxPayloadSize => 48;

        public long HeadSequence => 0;

        public long TailSequence => 0;

        public bool IsEmpty => _messages.Count == 0;

        public int Count => _messages.Count;

        public RingBufferEndpointState ConsumerState => RingBufferEndpointState.Running;

        public bool IsPeerWaiting() => false;

        public void SetWaiting(bool waiting)
        {
        }

        public void Abort()
        {
        }

        public void Connect(bool takeover = false, CancellationToken cancellationToken = default)
        {
        }

        public bool TryRead(Span<byte> destination, out int bytesRead, out int type) =>
            throw new NotSupportedException();

        public bool TryPeek(out ReadOnlySpan<byte> payload, out int length, out int type) =>
            throw new NotSupportedException();

        public void AdvanceRead()
        {
        }

        public bool TryBeginRead(out ReadLease lease)
        {
            if (_messages.Count > 0)
            {
                (int type, byte[] payload) = _messages.Dequeue();
                lease = new ReadLease(this, payload, payload.Length, type);
                return true;
            }

            lease = default;
            return false;
        }

        public ReadLease Read() => throw new NotSupportedException();

        public ReadLease Read(CancellationToken cancellationToken) => throw new NotSupportedException();

        public ReadLease Read(TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            ReadStarted.Release();
            if (!_blockReads)
            {
                throw new RingBufferTimeoutException("scripted empty ring");
            }

            _unblockReads.Wait(cancellationToken);
            throw new ObjectDisposedException(nameof(FakeConsumerEndpoint));
        }

        public void Dispose() => _unblockReads.Set();
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

    private sealed class ThrowingCodec : ISparcCodec<Order>
    {
        public int MaxSize => sizeof(int);

        public int Encode(Order item, Span<byte> destination)
        {
            if (item.Id == 1)
            {
                throw new InvalidOperationException("codec failure");
            }

            BinaryPrimitives.WriteInt32LittleEndian(destination, item.Id);
            return sizeof(int);
        }

        public Order Decode(ReadOnlySpan<byte> source) =>
            new(BinaryPrimitives.ReadInt32LittleEndian(source), string.Empty);
    }
}
