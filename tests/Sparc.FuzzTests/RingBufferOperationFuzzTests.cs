using Sparc.Core;

namespace Sparc.FuzzTests;

public class RingBufferOperationFuzzTests
{
    private const int Capacity = 4;
    private const int SlotSize = 48;

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(42)]
    [InlineData(20240926)]
    public void MixedOperationsMatchReferenceQueue(int seed)
    {
        SpscRingBuffer buffer = new(Capacity, SlotSize);
        Queue<(int Type, byte[] Payload)> reference = new();
        Random random = new(seed);
        byte[] destination = new byte[buffer.MaxPayloadSize];

        for (int operation = 0; operation < 50_000; operation++)
        {
            switch (random.Next(10))
            {
                case < 4:
                    CopyWrite();
                    break;
                case < 7:
                    CopyRead();
                    break;
                case < 9:
                    LeaseWrite();
                    break;
                default:
                    LeaseRead();
                    break;
            }

            Assert.Equal(reference.Count, buffer.Count);
            Assert.Equal(reference.Count == 0, buffer.IsEmpty);
        }

        void CopyWrite()
        {
            byte[] payload = NewPayload();
            int type = random.Next();
            bool written = buffer.TryWrite(type, payload);
            Assert.Equal(reference.Count < Capacity, written);
            if (written)
            {
                reference.Enqueue((type, payload));
            }
        }

        void CopyRead()
        {
            bool read = buffer.TryRead(destination, out int bytesRead, out int type);
            if (reference.Count == 0)
            {
                Assert.False(read);
                return;
            }

            Assert.True(read);
            (int expectedType, byte[] expected) = reference.Dequeue();
            Assert.Equal(expectedType, type);
            Assert.Equal(expected.Length, bytesRead);
            Assert.True(expected.AsSpan().SequenceEqual(destination.AsSpan(0, bytesRead)));
        }

        void LeaseWrite()
        {
            byte[] payload = NewPayload();
            int type = random.Next();
            bool reserved = buffer.TryReserveWrite(type, payload.Length, out Span<byte> slot);
            Assert.Equal(reference.Count < Capacity, reserved);
            if (!reserved)
            {
                return;
            }

            payload.CopyTo(slot);
            if (random.Next(2) == 0)
            {
                buffer.CommitWrite();
                reference.Enqueue((type, payload));
            }
            else
            {
                buffer.AbandonWrite();
            }
        }

        void LeaseRead()
        {
            bool peeked = buffer.TryPeek(out ReadOnlySpan<byte> payload, out int length, out int type);
            if (reference.Count == 0)
            {
                Assert.False(peeked);
                return;
            }

            Assert.True(peeked);
            (int expectedType, byte[] expected) = reference.Peek();
            Assert.Equal(expectedType, type);
            Assert.Equal(expected.Length, length);
            Assert.True(expected.AsSpan().SequenceEqual(payload));
            buffer.AdvanceRead();
            reference.Dequeue();
        }

        byte[] NewPayload()
        {
            byte[] payload = new byte[random.Next(buffer.MaxPayloadSize + 1)];
            random.NextBytes(payload);
            return payload;
        }
    }
}
