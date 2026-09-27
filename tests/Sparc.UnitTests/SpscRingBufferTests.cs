using System.Text;
using Sparc.Core;

namespace Sparc.UnitTests;

public class SpscRingBufferTests
{
    [Fact]
    public void ConstructorRejectsInvalidGeometry()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SpscRingBuffer(1000, 256));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SpscRingBuffer(0, 256));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SpscRingBuffer(4, RingBufferLayout.MessageHeaderSize));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SpscRingBuffer(4, RingBufferLayout.MessageHeaderSize - 1));
        Assert.NotNull(new SpscRingBuffer(4, RingBufferLayout.MessageHeaderSize + 1));
    }

    [Fact]
    public void EmptyBufferReturnsFalse()
    {
        SpscRingBuffer buffer = new(4, 64);
        Span<byte> destination = new byte[buffer.MaxPayloadSize];

        Assert.True(buffer.IsEmpty);
        Assert.Equal(0, buffer.Count);
        Assert.False(buffer.TryRead(destination, out int bytesRead, out int type));
        Assert.Equal(0, bytesRead);
        Assert.Equal(0, type);
    }

    [Fact]
    public void SingleMessageRoundTrips()
    {
        SpscRingBuffer buffer = new(4, 64);
        byte[] payload = "hello ring buffer"u8.ToArray();
        Span<byte> destination = new byte[buffer.MaxPayloadSize];

        Assert.True(buffer.TryWrite(type: 42, payload));
        Assert.False(buffer.IsEmpty);
        Assert.Equal(1, buffer.Count);

        Assert.True(buffer.TryRead(destination, out int bytesRead, out int type));
        Assert.Equal(payload.Length, bytesRead);
        Assert.Equal(42, type);
        Assert.True(payload.AsSpan().SequenceEqual(destination[..bytesRead]));
        Assert.True(buffer.IsEmpty);
    }

    [Fact]
    public void ZeroLengthPayloadRoundTrips()
    {
        SpscRingBuffer buffer = new(4, 64);
        Span<byte> destination = new byte[buffer.MaxPayloadSize];

        Assert.True(buffer.TryWrite(7, ReadOnlySpan<byte>.Empty));
        Assert.True(buffer.TryRead(destination, out int bytesRead, out int type));
        Assert.Equal(0, bytesRead);
        Assert.Equal(7, type);
    }

    [Fact]
    public void ExactlyFullBufferAcceptsNoMoreUntilConsumed()
    {
        SpscRingBuffer buffer = new(4, 64);
        Span<byte> destination = new byte[buffer.MaxPayloadSize];
        byte[] payload = [1, 2, 3, 4];

        for (int i = 0; i < buffer.Capacity; i++)
        {
            Assert.True(buffer.TryWrite(i, payload));
        }

        Assert.False(buffer.TryWrite(99, payload));
        Assert.Equal(buffer.Capacity, buffer.Count);

        Assert.True(buffer.TryRead(destination, out int bytesRead, out int type));
        Assert.Equal(0, type);
        Assert.Equal(payload.Length, bytesRead);

        Assert.True(buffer.TryWrite(99, payload));
        Assert.False(buffer.TryWrite(99, payload));
    }

    [Fact]
    public void WraparoundPreservesOrder()
    {
        const int capacity = 4;
        SpscRingBuffer buffer = new(capacity, 32);
        Span<byte> destination = new byte[buffer.MaxPayloadSize];

        const int total = 10_000;
        int sequence = 0;
        int read = 0;
        byte[] payload = new byte[8];

        while (read < total)
        {
            for (int i = 0; i < capacity && sequence < total; i++, sequence++)
            {
                BitConverter.TryWriteBytes(payload, sequence);
                Assert.True(buffer.TryWrite(sequence, payload));
            }

            while (buffer.TryRead(destination, out int bytesRead, out int type))
            {
                Assert.Equal(8, bytesRead);
                Assert.Equal(read, type);
                Assert.Equal(read, BitConverter.ToInt32(destination));
                read++;
            }
        }

        Assert.Equal(total, read);
        Assert.True(buffer.IsEmpty);
    }

    [Fact]
    public void PayloadLargerThanSlotThrows()
    {
        SpscRingBuffer buffer = new(4, 32);
        byte[] payload = new byte[buffer.MaxPayloadSize + 1];

        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.TryWrite(0, payload));
    }

    [Fact]
    public void DestinationSmallerThanMaxPayloadThrows()
    {
        SpscRingBuffer buffer = new(4, 64);
        byte[] destination = new byte[buffer.MaxPayloadSize - 1];

        Assert.Throws<ArgumentException>(() => buffer.TryRead(destination, out _, out _));
    }

    [Fact]
    public void RandomOperationsMatchReferenceQueue()
    {
        const int capacity = 16;
        const int slotSize = 48;
        SpscRingBuffer buffer = new(capacity, slotSize);
        Queue<byte[]> reference = new();
        Random random = new(20240926);
        Span<byte> destination = new byte[buffer.MaxPayloadSize];

        for (int operation = 0; operation < 200_000; operation++)
        {
            if (reference.Count == 0 || random.Next(2) == 0)
            {
                byte[] payload = new byte[random.Next(buffer.MaxPayloadSize + 1)];
                random.NextBytes(payload);
                int type = random.Next();

                bool written = buffer.TryWrite(type, payload);
                Assert.Equal(reference.Count < capacity, written);
                if (written)
                {
                    reference.Enqueue(payload);
                }
            }
            else
            {
                bool read = buffer.TryRead(destination, out int bytesRead, out int type);
                Assert.True(read);
                byte[] expected = reference.Dequeue();
                Assert.Equal(expected.Length, bytesRead);
                Assert.True(expected.AsSpan().SequenceEqual(destination[..bytesRead]));
            }

            Assert.Equal(reference.Count, buffer.Count);
        }
    }

    [Fact]
    public void LeaseWriteRoundTripsThroughCopyRead()
    {
        SpscRingBuffer buffer = new(4, 64);
        byte[] payload = "leased payload"u8.ToArray();

        Assert.True(buffer.TryReserveWrite(42, payload.Length, out Span<byte> slot));
        payload.AsSpan().CopyTo(slot);
        buffer.CommitWrite();

        Span<byte> destination = new byte[buffer.MaxPayloadSize];
        Assert.True(buffer.TryRead(destination, out int bytesRead, out int type));
        Assert.Equal(payload.Length, bytesRead);
        Assert.Equal(42, type);
        Assert.True(payload.AsSpan().SequenceEqual(destination[..bytesRead]));
    }

    [Fact]
    public void CopyWriteRoundTripsThroughPeekRead()
    {
        SpscRingBuffer buffer = new(4, 64);
        byte[] payload = [9, 8, 7];
        Assert.True(buffer.TryWrite(3, payload));

        Assert.True(buffer.TryPeek(out ReadOnlySpan<byte> peeked, out int length, out int type));
        Assert.Equal(3, length);
        Assert.Equal(3, type);
        Assert.True(payload.AsSpan().SequenceEqual(peeked));

        buffer.AdvanceRead();
        Assert.True(buffer.IsEmpty);
    }

    [Fact]
    public void AbandonedLeaseDoesNotPublish()
    {
        SpscRingBuffer buffer = new(4, 64);
        Assert.True(buffer.TryReserveWrite(1, 4, out _));
        buffer.AbandonWrite();
        Assert.True(buffer.IsEmpty);

        Assert.True(buffer.TryReserveWrite(2, 4, out Span<byte> slot));
        slot.Fill(0x11);
        buffer.CommitWrite();
        Assert.Equal(1, buffer.Count);
    }

    [Fact]
    public void LeaseReservationOnFullBufferReturnsFalse()
    {
        SpscRingBuffer buffer = new(2, 32);
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
        SpscRingBuffer buffer = new(4, 64);
        Assert.Throws<InvalidOperationException>(() => buffer.CommitWrite());
        Assert.Throws<InvalidOperationException>(() => buffer.AbandonWrite());
        Assert.Throws<InvalidOperationException>(() => buffer.AdvanceRead());

        Assert.True(buffer.TryReserveWrite(0, 4, out _));
        Assert.Throws<InvalidOperationException>(() => buffer.TryReserveWrite(0, 4, out _));
        buffer.CommitWrite();

        Assert.True(buffer.TryPeek(out _, out _, out _));
        Assert.Throws<InvalidOperationException>(() => buffer.TryPeek(out _, out _, out _));
        buffer.AdvanceRead();
    }

    [Fact]
    public void LeaseReserveRejectsOversizedPayload()
    {
        SpscRingBuffer buffer = new(4, 32);
        Assert.Throws<ArgumentOutOfRangeException>(
            () => buffer.TryReserveWrite(0, buffer.MaxPayloadSize + 1, out _));
    }

    [Fact]
    public void LeaseWraparoundPreservesOrder()
    {
        const int capacity = 4;
        SpscRingBuffer buffer = new(capacity, 32);

        const int total = 10_000;
        int sequence = 0;
        int read = 0;

        while (read < total)
        {
            for (int i = 0; i < capacity && sequence < total; i++, sequence++)
            {
                Assert.True(buffer.TryReserveWrite(sequence, sizeof(int), out Span<byte> slot));
                BitConverter.TryWriteBytes(slot, sequence);
                buffer.CommitWrite();
            }

            while (buffer.TryPeek(out ReadOnlySpan<byte> peeked, out int length, out int type))
            {
                Assert.Equal(sizeof(int), length);
                Assert.Equal(read, type);
                Assert.Equal(read, BitConverter.ToInt32(peeked));
                buffer.AdvanceRead();
                read++;
            }
        }

        Assert.True(buffer.IsEmpty);
    }

    [Fact]
    public void BlockingWriteSucceedsWhenConsumerDrains()
    {
        SpscRingBuffer buffer = new(2, 32);
        byte[] payload = new byte[4];
        Span<byte> destination = new byte[buffer.MaxPayloadSize];

        buffer.Write(payload, 1, TestContext.Current.CancellationToken);
        buffer.Write(payload, 2, TestContext.Current.CancellationToken);
        Assert.False(buffer.TryWrite(0, payload));

        Assert.True(buffer.TryRead(destination, out _, out int first));
        Assert.Equal(1, first);

        buffer.Write(payload, 3, TestContext.Current.CancellationToken); // slot freed above, must not block

        Assert.True(buffer.TryRead(destination, out _, out int second));
        Assert.True(buffer.TryRead(destination, out _, out int third));
        Assert.Equal(2, second);
        Assert.Equal(3, third);
    }

    [Fact]
    public void BlockingWriteHonorsCancellationWhileFull()
    {
        SpscRingBuffer buffer = new(2, 32);
        byte[] payload = new byte[4];
        buffer.Write(payload, 1, TestContext.Current.CancellationToken);
        buffer.Write(payload, 2, TestContext.Current.CancellationToken);

        using CancellationTokenSource cancellation = new();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(
            () => buffer.Write(payload, 3, cancellation.Token));
        Assert.Equal(2, buffer.Count);
    }
}
