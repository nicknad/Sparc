using System.Buffers.Binary;
using RingBuffer.Core;
using RingBuffer.UnitTests.Support;

namespace RingBuffer.UnitTests;

public class SharedRingBufferTests
{
    private static string NewName() => "spsc-unit-" + Guid.NewGuid().ToString("N");

    [Fact]
    public void TwoEndpointsExchangeMessagesInOrder()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        using SharedRingBuffer consumer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);

        producer.Connect(RingBufferEndpointRole.Producer);
        consumer.Connect(RingBufferEndpointRole.Consumer);

        Assert.Equal(4, producer.Capacity);
        Assert.Equal(consumer.Capacity, producer.Capacity);
        Assert.Equal(consumer.SlotSize, producer.SlotSize);

        byte[] payload = new byte[producer.MaxPayloadSize];
        Span<byte> destination = new byte[consumer.MaxPayloadSize];
        int read = 0;

        // Write continuously, draining as we go: forces wraparound and proves
        // full/empty detection across two independent mappings.
        for (int sequence = 0; sequence < 10; sequence++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(payload, sequence);
            Assert.True(producer.TryWrite(sequence, payload));

            if (sequence >= 3)
            {
                Assert.True(consumer.TryRead(destination, out int bytesRead, out int type));
                Assert.Equal(payload.Length, bytesRead);
                Assert.Equal(read, type);
                Assert.Equal(read, BinaryPrimitives.ReadInt32LittleEndian(destination));
                read++;
            }
        }

        while (read < 10)
        {
            Assert.True(consumer.TryRead(destination, out int bytesRead, out int type));
            Assert.Equal(payload.Length, bytesRead);
            Assert.Equal(read, type);
            Assert.Equal(read, BinaryPrimitives.ReadInt32LittleEndian(destination));
            read++;
        }

        Assert.True(consumer.IsEmpty);
    }

    [Fact]
    public void FreshConsumerAttachedToDrainedNonZeroRegionReportsEmpty()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using RingBufferRegion region = RingBufferRegion.CreateOrOpen(factory, name, 4, 64);

        using (SharedRingBuffer producer = new(region, ownsRegion: false))
        using (SharedRingBuffer firstConsumer = new(region, ownsRegion: false))
        {
            producer.Connect(RingBufferEndpointRole.Producer);
            firstConsumer.Connect(RingBufferEndpointRole.Consumer);

            byte[] payload = new byte[8];
            Span<byte> destination = new byte[firstConsumer.MaxPayloadSize];

            for (int i = 0; i < 3; i++)
            {
                Assert.True(producer.TryWrite(i, payload));
            }

            for (int i = 0; i < 3; i++)
            {
                Assert.True(firstConsumer.TryRead(destination, out _, out _));
            }

            Assert.Equal(3, firstConsumer.HeadSequence);
            Assert.Equal(3, firstConsumer.TailSequence);
        }

        // head == tail == 3, the previous consumer said goodbye; a fresh view
        // must not mistake the stale cached cursor for pending data.
        using SharedRingBuffer restarted = new(region, ownsRegion: false);
        restarted.Connect(RingBufferEndpointRole.Consumer, takeover: true);

        Span<byte> buffer = new byte[restarted.MaxPayloadSize];
        Assert.False(restarted.TryRead(buffer, out _, out _));
    }

    [Fact]
    public void RoleConflictIsRejectedUnlessTakeover()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer first = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        using SharedRingBuffer second = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);

        first.Connect(RingBufferEndpointRole.Producer);

        Assert.Throws<RingBufferRoleConflictException>(
            () => second.Connect(RingBufferEndpointRole.Producer));

        second.Connect(RingBufferEndpointRole.Producer, takeover: true);
        Assert.Equal(RingBufferEndpointState.Running, second.ProducerState);
    }

    [Fact]
    public void GracefulDisposeSetsStoppedAndAbortSetsFaulted()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer stateReader = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);

        SharedRingBuffer producer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        producer.Connect(RingBufferEndpointRole.Producer);
        producer.Dispose();
        Assert.Equal(RingBufferEndpointState.Stopped, stateReader.ProducerState);

        SharedRingBuffer aborted = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        aborted.Connect(RingBufferEndpointRole.Consumer);
        aborted.Abort();
        aborted.Dispose();
        Assert.Equal(RingBufferEndpointState.Faulted, stateReader.ConsumerState);
    }

    [Fact]
    public void UseAfterDisposeThrows()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using RingBufferRegion region = RingBufferRegion.CreateOrOpen(factory, name, 4, 64);
        SharedRingBuffer buffer = new(region, ownsRegion: false);
        buffer.Connect(RingBufferEndpointRole.Producer);
        buffer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => buffer.TryWrite(0, new byte[4]));
        Assert.Throws<ObjectDisposedException>(() => buffer.TryRead(new byte[buffer.MaxPayloadSize], out _, out _));
    }

    [Fact]
    public void PayloadLargerThanSlotThrows()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer buffer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        buffer.Connect(RingBufferEndpointRole.Producer);

        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.TryWrite(0, new byte[buffer.MaxPayloadSize + 1]));
    }

    [Fact]
    public void OpenExistingFailsForMissingRegion()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        Assert.Throws<RingBufferTimeoutException>(() => SharedRingBuffer.OpenExisting(
            factory, name, 4, 64, new SharedRingBufferOptions
            {
                OpenTimeout = TimeSpan.FromMilliseconds(100),
            }));
    }
}
