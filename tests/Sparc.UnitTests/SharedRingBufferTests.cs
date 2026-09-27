using System.Buffers.Binary;
using Sparc.Core;
using Sparc.InMemory;

namespace Sparc.UnitTests;

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

        producer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);
        consumer.Connect(RingBufferEndpointRole.Consumer, cancellationToken: TestContext.Current.CancellationToken);

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
            producer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);
            firstConsumer.Connect(RingBufferEndpointRole.Consumer, cancellationToken: TestContext.Current.CancellationToken);

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
        restarted.Connect(RingBufferEndpointRole.Consumer, takeover: true, cancellationToken: TestContext.Current.CancellationToken);

        Span<byte> buffer = new byte[restarted.MaxPayloadSize];
        Assert.False(restarted.TryRead(buffer, out _, out _));
    }

    [Fact]
    public void LeaseWriteAndPeekRoundTripAcrossTwoViews()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        using SharedRingBuffer consumer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        producer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);
        consumer.Connect(RingBufferEndpointRole.Consumer, cancellationToken: TestContext.Current.CancellationToken);

        byte[] payload = "shared lease"u8.ToArray();
        Assert.True(producer.TryReserveWrite(5, payload.Length, out Span<byte> slot));
        payload.AsSpan().CopyTo(slot);
        producer.CommitWrite();

        Assert.True(consumer.TryPeek(out ReadOnlySpan<byte> peeked, out int length, out int type));
        Assert.Equal(payload.Length, length);
        Assert.Equal(5, type);
        Assert.True(payload.AsSpan().SequenceEqual(peeked));
        consumer.AdvanceRead();
        Assert.True(consumer.IsEmpty);
    }

    [Fact]
    public void LeaseAndCopyApisInteroperate()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        using SharedRingBuffer consumer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        producer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);
        consumer.Connect(RingBufferEndpointRole.Consumer, cancellationToken: TestContext.Current.CancellationToken);

        // Lease write -> copy read.
        Assert.True(producer.TryReserveWrite(1, 4, out Span<byte> slot));
        slot.Fill(0x2A);
        producer.CommitWrite();
        Span<byte> destination = new byte[consumer.MaxPayloadSize];
        Assert.True(consumer.TryRead(destination, out int bytesRead, out int type));
        Assert.Equal(4, bytesRead);
        Assert.Equal(1, type);
        Assert.True(destination[..bytesRead].IndexOfAnyExcept((byte)0x2A) < 0);

        // Copy write -> peek read.
        Assert.True(producer.TryWrite(2, new byte[] { 7, 7, 7 }));
        Assert.True(consumer.TryPeek(out ReadOnlySpan<byte> peeked, out int length, out int peekedType));
        Assert.Equal(3, length);
        Assert.Equal(2, peekedType);
        Assert.True(peeked.IndexOfAnyExcept((byte)7) < 0);
        consumer.AdvanceRead();
    }

    [Fact]
    public void LeaseReservationOnFullBufferReturnsFalse()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer buffer = SharedRingBuffer.OpenOrCreate(factory, name, 2, 32);
        buffer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(buffer.TryReserveWrite(0, 4, out _));
        buffer.CommitWrite();
        Assert.True(buffer.TryReserveWrite(0, 4, out _));
        buffer.CommitWrite();

        Assert.False(buffer.TryReserveWrite(0, 4, out Span<byte> slot));
        Assert.True(slot.IsEmpty);
    }

    [Fact]
    public void LeaseStateMachinesRejectInvalidTransitions()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer buffer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        buffer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Throws<InvalidOperationException>(() => buffer.CommitWrite());
        Assert.Throws<InvalidOperationException>(() => buffer.AbandonWrite());
        Assert.Throws<InvalidOperationException>(() => buffer.AdvanceRead());

        Assert.True(buffer.TryReserveWrite(0, 4, out _));
        Assert.Throws<InvalidOperationException>(() => buffer.TryReserveWrite(0, 4, out _));
        buffer.AbandonWrite();
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.TryReserveWrite(0, buffer.MaxPayloadSize + 1, out _));
    }

    [Fact]
    public void WaitingFlagsRoundTripBetweenRoles()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer producer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        using SharedRingBuffer consumer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        producer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);
        consumer.Connect(RingBufferEndpointRole.Consumer, cancellationToken: TestContext.Current.CancellationToken);

        Assert.False(producer.IsPeerWaiting());
        Assert.False(consumer.IsPeerWaiting());

        consumer.SetWaiting(true);
        Assert.True(producer.IsPeerWaiting());
        Assert.False(consumer.IsPeerWaiting());

        consumer.SetWaiting(false);
        Assert.False(producer.IsPeerWaiting());

        producer.SetWaiting(true);
        Assert.True(consumer.IsPeerWaiting());
        producer.SetWaiting(false);
        Assert.False(consumer.IsPeerWaiting());
    }

    [Fact]
    public void WaitingFlagsRequireConnect()
    {
        InMemoryMemoryRegionFactory factory = new();
        using SharedRingBuffer buffer = SharedRingBuffer.OpenOrCreate(factory, NewName(), 4, 64);

        Assert.Throws<InvalidOperationException>(() => buffer.SetWaiting(true));
        Assert.Throws<InvalidOperationException>(() => buffer.IsPeerWaiting());
    }

    [Fact]
    public void RoleConflictIsRejectedUnlessTakeover()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer first = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        using SharedRingBuffer second = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);

        first.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Throws<RingBufferRoleConflictException>(
            () => second.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken));

        second.Connect(RingBufferEndpointRole.Producer, takeover: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(RingBufferEndpointState.Running, second.ProducerState);
    }

    [Fact]
    public void GracefulDisposeSetsStoppedAndAbortSetsFaulted()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer stateReader = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);

        SharedRingBuffer producer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        producer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);
        producer.Dispose();
        Assert.Equal(RingBufferEndpointState.Stopped, stateReader.ProducerState);

        SharedRingBuffer aborted = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);
        aborted.Connect(RingBufferEndpointRole.Consumer, cancellationToken: TestContext.Current.CancellationToken);
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
        buffer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);
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
        buffer.Connect(RingBufferEndpointRole.Producer, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.TryWrite(0, new byte[buffer.MaxPayloadSize + 1]));
    }

    [Fact]
    public void ConnectHonorsCancellation()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using SharedRingBuffer buffer = SharedRingBuffer.OpenOrCreate(factory, name, 4, 64);

        using CancellationTokenSource cancellation = new();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(
            () => buffer.Connect(RingBufferEndpointRole.Producer, cancellationToken: cancellation.Token));
        Assert.Equal(RingBufferEndpointState.NotPresent, buffer.ProducerState);
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
