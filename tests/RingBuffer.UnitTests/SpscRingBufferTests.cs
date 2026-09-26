using System.Text;
using RingBuffer.Core;

namespace RingBuffer.UnitTests;

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
    public void BlockingWriteSucceedsWhenConsumerDrains()
    {
        SpscRingBuffer buffer = new(2, 32);
        byte[] payload = new byte[4];
        Span<byte> destination = new byte[buffer.MaxPayloadSize];

        buffer.Write(payload, 1);
        buffer.Write(payload, 2);
        Assert.False(buffer.TryWrite(0, payload));

        Assert.True(buffer.TryRead(destination, out _, out int first));
        Assert.Equal(1, first);

        buffer.Write(payload, 3); // slot freed above, must not block

        Assert.True(buffer.TryRead(destination, out _, out int second));
        Assert.True(buffer.TryRead(destination, out _, out int third));
        Assert.Equal(2, second);
        Assert.Equal(3, third);
    }
}
