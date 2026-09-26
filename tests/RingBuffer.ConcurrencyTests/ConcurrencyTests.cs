using System.Globalization;
using RingBuffer.Core;
using RingBuffer.SharedMemory;

namespace RingBuffer.ConcurrencyTests;

public class SpscArrayConcurrencyTests(ITestOutputHelper output)
{
    private const long Total = 10_000_000;
    private static readonly TimeSpan Timeout = TimeSpan.FromMinutes(2);

    [Fact]
    public void TenMillionMessagesArriveInOrder()
    {
        SpscRingBuffer buffer = new(1024, 256);

        TransferRunner.Result result = TransferRunner.Run(buffer, buffer, Total, payloadSize: 64, Timeout);

        output.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"array: {result.Consumed:N0} messages in {result.Elapsed.TotalSeconds:F3}s " +
            $"= {result.MessagesPerSecond:N0} msg/s"));

        Assert.Equal(Total, result.Produced);
        Assert.Equal(Total, result.Consumed);
    }

    [Fact]
    public void TinyCapacityTortureTest()
    {
        SpscRingBuffer buffer = new(2, 32);

        TransferRunner.Result result = TransferRunner.Run(
            buffer, buffer, total: 1_000_000, payloadSize: 24, TimeSpan.FromMinutes(2));

        output.WriteLine($"tiny capacity: {result.Consumed:N0} messages in {result.Elapsed.TotalSeconds:F3}s");
        Assert.Equal(1_000_000, result.Consumed);
    }

    [Fact]
    public void SteadyStateDoesNotAllocate()
    {
        SpscRingBuffer buffer = new(1024, 256);
        byte[] payload = new byte[64];
        byte[] destination = new byte[buffer.MaxPayloadSize];

        // Warm up JIT and steady state.
        for (int i = 0; i < 100_000; i++)
        {
            buffer.TryWrite(1, payload);
            buffer.TryRead(destination, out _, out _);
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000_000; i++)
        {
            buffer.TryWrite(1, payload);
            buffer.TryRead(destination, out _, out _);
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        output.WriteLine($"allocated bytes for 1,000,000 write+read pairs: {allocated}");
        Assert.True(allocated < 4096, $"Expected near-zero allocations, saw {allocated} bytes.");
    }
}

public class SharedRingBufferConcurrencyTests(ITestOutputHelper output)
{
    private const long Total = 10_000_000;
    private static readonly TimeSpan Timeout = TimeSpan.FromMinutes(2);

    [Fact]
    public void TenMillionMessagesArriveInOrderAcrossTwoMappings()
    {
        string name = "spsc-concurrency-" + Guid.NewGuid().ToString("N");

        using SharedMemoryRegion producerRegion = SharedMemoryRegion.CreateOrOpen(name, 1024, 256);
        using SharedMemoryRegion consumerRegion = SharedMemoryRegion.CreateOrOpen(name, 1024, 256);
        using SharedRingBuffer producerView = new(producerRegion);
        using SharedRingBuffer consumerView = new(consumerRegion);

        producerView.Connect(RingBufferEndpointRole.Producer);
        consumerView.Connect(RingBufferEndpointRole.Consumer);

        TransferRunner.Result result = TransferRunner.Run(
            producerView, consumerView, Total, payloadSize: 64, Timeout);

        output.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"shared memory: {result.Consumed:N0} messages in {result.Elapsed.TotalSeconds:F3}s " +
            $"= {result.MessagesPerSecond:N0} msg/s"));

        Assert.Equal(Total, result.Consumed);
        Assert.Equal(Total, producerView.TailSequence);
        Assert.Equal(Total, consumerView.HeadSequence);
    }
}
