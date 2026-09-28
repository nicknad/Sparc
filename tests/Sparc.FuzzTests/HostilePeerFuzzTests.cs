using Sparc.Core;
using Sparc.InMemory;

namespace Sparc.FuzzTests;

public class HostilePeerFuzzTests
{
    [Fact]
    public void CorruptedSlotBytesOnlySurfaceAsDocumentedCorruption()
    {
        const int capacity = 4;
        const int slotSize = 64;
        InMemoryMemoryRegionFactory factory = new();
        Random random = new(1729);
        Span<byte> destination = new byte[slotSize];

        for (int round = 0; round < 200; round++)
        {
            string name = $"fuzz-{round}";
            using IProducerEndpoint producer = SparcRing.OpenProducer(
                factory, name, capacity, slotSize, options: null, cancellationToken: TestContext.Current.CancellationToken);
            using IConsumerEndpoint consumer = SparcRing.OpenConsumer(
                factory, name, capacity, slotSize, options: null, cancellationToken: TestContext.Current.CancellationToken);
            using IIpcMemoryRegion view = factory.CreateOrOpen(name, RingBufferLayout.RequiredSize(capacity, slotSize));

            int written = 0;
            byte[] payload = new byte[16];
            while (producer.TryPublish(written, payload))
            {
                written++;
            }

            Assert.Equal(capacity, written);

            int corruptedSlot = random.Next(written);
            unsafe
            {
                byte* slot = view.Pointer + RingBufferLayout.HeaderSize + (long)corruptedSlot * slotSize;
                random.NextBytes(new Span<byte>(slot, slotSize));
            }

            for (int message = 0; message < written; message++)
            {
                try
                {
                    if (!consumer.TryRead(destination, out int bytesRead, out _))
                    {
                        break;
                    }

                    Assert.InRange(bytesRead, 0, consumer.MaxPayloadSize);
                }
                catch (RingBufferCorruptedException)
                {
                    break;
                }
            }
        }
    }
}
