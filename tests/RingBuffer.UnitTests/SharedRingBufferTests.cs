using System.Buffers.Binary;
using RingBuffer.Core;
using RingBuffer.SharedMemory;

namespace RingBuffer.UnitTests;

public class SharedRingBufferTests
{
    private static string NewName() => "spsc-unit-" + Guid.NewGuid().ToString("N");

    [Fact]
    public void TwoEndpointsExchangeMessagesInOrder()
    {
        string name = NewName();
        using SharedMemoryRegion producerRegion = SharedMemoryRegion.CreateOrOpen(name, 4, 64);
        using SharedMemoryRegion consumerRegion = SharedMemoryRegion.CreateOrOpen(name, 4, 64);
        using SharedRingBuffer producer = new(producerRegion);
        using SharedRingBuffer consumer = new(consumerRegion);

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
        string name = NewName();
        using SharedMemoryRegion region = SharedMemoryRegion.CreateOrOpen(name, 4, 64);

        using (SharedRingBuffer producer = new(region))
        using (SharedRingBuffer firstConsumer = new(region))
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
        using SharedRingBuffer restarted = new(region);
        restarted.Connect(RingBufferEndpointRole.Consumer, takeover: true);

        Span<byte> buffer = new byte[restarted.MaxPayloadSize];
        Assert.False(restarted.TryRead(buffer, out _, out _));
    }

    [Fact]
    public void RoleConflictIsRejectedUnlessTakeover()
    {
        string name = NewName();
        using SharedMemoryRegion region = SharedMemoryRegion.CreateOrOpen(name, 4, 64);
        using SharedRingBuffer first = new(region);
        using SharedRingBuffer second = new(region);

        first.Connect(RingBufferEndpointRole.Producer);

        Assert.Throws<RingBufferRoleConflictException>(
            () => second.Connect(RingBufferEndpointRole.Producer));

        second.Connect(RingBufferEndpointRole.Producer, takeover: true);
        Assert.Equal(RingBufferEndpointState.Running, region.ReadEndpointState(RingBufferEndpointRole.Producer));
    }

    [Fact]
    public void GracefulDisposeSetsStoppedAndAbortSetsFaulted()
    {
        string name = NewName();
        using SharedMemoryRegion region = SharedMemoryRegion.CreateOrOpen(name, 4, 64);

        SharedRingBuffer buffer = new(region);
        buffer.Connect(RingBufferEndpointRole.Producer);
        buffer.Dispose();
        Assert.Equal(RingBufferEndpointState.Stopped, region.ReadEndpointState(RingBufferEndpointRole.Producer));

        SharedRingBuffer aborted = new(region);
        aborted.Connect(RingBufferEndpointRole.Consumer);
        aborted.Abort();
        aborted.Dispose();
        Assert.Equal(RingBufferEndpointState.Faulted, region.ReadEndpointState(RingBufferEndpointRole.Consumer));
    }

    [Fact]
    public void UseAfterDisposeThrows()
    {
        string name = NewName();
        using SharedMemoryRegion region = SharedMemoryRegion.CreateOrOpen(name, 4, 64);
        SharedRingBuffer buffer = new(region);
        buffer.Connect(RingBufferEndpointRole.Producer);
        buffer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => buffer.TryWrite(0, new byte[4]));
        Assert.Throws<ObjectDisposedException>(() => buffer.TryRead(new byte[buffer.MaxPayloadSize], out _, out _));
    }

    [Fact]
    public void PayloadLargerThanSlotThrows()
    {
        string name = NewName();
        using SharedRingBuffer buffer = SharedRingBuffer.OpenOrCreate(name, 4, 64);
        buffer.Connect(RingBufferEndpointRole.Producer);

        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.TryWrite(0, new byte[buffer.MaxPayloadSize + 1]));
    }

    [Fact]
    public void OpenExistingFailsForMissingRegion()
    {
        string name = NewName();
        Assert.Throws<RingBufferTimeoutException>(() => SharedRingBuffer.OpenExisting(name, 4, 64, new SharedMemoryOptions
        {
            OpenTimeout = TimeSpan.FromMilliseconds(100),
        }));
    }
}
