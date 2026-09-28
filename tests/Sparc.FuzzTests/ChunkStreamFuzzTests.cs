using System.Buffers.Binary;
using CsCheck;
using Sparc;
using Sparc.Core;
using Sparc.InMemory;
using Sparc.Serialization;

namespace Sparc.FuzzTests;

public class ChunkStreamFuzzTests
{
    private const int Capacity = 128;
    private const int SlotSize = 64;
    private const int ChunkDataCapacity = SlotSize - RingBufferLayout.MessageHeaderSize - ChunkFraming.HeaderSize;

    [Fact]
    public void ArbitraryChunkBytesDecodeWithinTheDocumentedContract()
    {
        Gen.Byte.Array[0, 160].Sample(
            bytes =>
            {
                int flags;
                try
                {
                    flags = ChunkFraming.ReadFlags(bytes);
                }
                catch (RingBufferCorruptedException)
                {
                    return;
                }

                Assert.Equal(
                    0,
                    flags & ~(ChunkFraming.FirstFlag | ChunkFraming.LastFlag | ChunkFraming.AbortFlag));
                if (bytes.Length == ChunkFraming.HeaderSize)
                {
                    Assert.NotEqual(0, flags & ChunkFraming.LastFlag);
                }
            },
            iter: 10_000);
    }

    [Fact]
    public void ChunkedStreamsRoundTripThroughBothReaders()
    {
        Gen.Select(Gen.Byte.Array[0, 1500], Gen.Int[16, ChunkDataCapacity]).Sample(
            pair =>
            {
                (byte[] message, int chunkSize) = pair;
                using FuzzPair stream = new();
                PublishChunked(stream.Producer, type: 5, message, chunkSize);

                byte[] destination = new byte[message.Length];
                int length = stream.Reader.ReadMessage(
                    destination, out int type, cancellationToken: TestContext.Current.CancellationToken);
                Assert.Equal(message.Length, length);
                Assert.Equal(5, type);
                Assert.True(message.AsSpan().SequenceEqual(destination));

                PublishChunked(stream.Producer, type: 6, message, chunkSize);
                byte[] scratch = new byte[ChunkDataCapacity];
                SparcStreamReadBuffer buffer = stream.Reader.BeginMessage(
                    scratch, cancellationToken: TestContext.Current.CancellationToken);
                try
                {
                    byte[] received = new byte[message.Length];
                    int offset = 0;
                    while (!buffer.IsMessageComplete)
                    {
                        ReadOnlySpan<byte> span = buffer.GetUnreadSpan();
                        span.CopyTo(received.AsSpan(offset));
                        buffer.Advance(span.Length);
                        offset += span.Length;
                    }

                    Assert.Equal(6, buffer.Type);
                    Assert.Equal(message.Length, offset);
                    Assert.True(message.AsSpan().SequenceEqual(received));
                }
                finally
                {
                    buffer.Dispose();
                }
            },
            iter: 150);
    }

    [Fact]
    public void ArbitraryFirstChunkFlagsOnlySurfaceAsCorruption()
    {
        Gen.Int.Sample(
            flags =>
            {
                using FuzzPair stream = new();
                PublishRawChunk(stream.Producer, type: 1, flags, payload: [1, 2, 3]);
                try
                {
                    bool started = stream.Reader.TryBeginMessage(
                        new byte[16],
                        out SparcStreamReadBuffer buffer,
                        cancellationToken: TestContext.Current.CancellationToken);
                    if (started)
                    {
                        buffer.Dispose();
                    }
                    else
                    {
                        // Without the First flag the chunk is skipped as an
                        // interrupted tail, so no message can have started.
                        Assert.Equal(0, flags & ChunkFraming.FirstFlag);
                    }
                }
                catch (RingBufferCorruptedException)
                {
                    return;
                }
            },
            iter: 2_000);
    }

    [Fact]
    public void CorruptedChunkFlagsOnlySurfaceAsCorruption()
    {
        Random random = new(4242);
        Span<byte> destination = new byte[512];

        for (int round = 0; round < 200; round++)
        {
            using FuzzPair stream = new();
            PublishChunked(stream.Producer, type: 1, new byte[300], chunkSize: 32);

            using IIpcMemoryRegion view = stream.Factory.CreateOrOpen(
                stream.Name, RingBufferLayout.RequiredSize(Capacity, SlotSize));
            int slot = random.Next(6);
            unsafe
            {
                byte* flags = view.Pointer
                    + RingBufferLayout.HeaderSize
                    + ((long)slot * SlotSize)
                    + RingBufferLayout.MessageHeaderSize;
                BinaryPrimitives.WriteInt32LittleEndian(new Span<byte>(flags, ChunkFraming.HeaderSize), random.Next());
            }

            try
            {
                int length = stream.Reader.ReadMessage(
                    destination, out _, cancellationToken: TestContext.Current.CancellationToken);
                Assert.InRange(length, 0, destination.Length);
            }
            catch (RingBufferCorruptedException)
            {
                // documented outcome for a corrupt chunk
            }
        }
    }

    private static void PublishChunked(IProducerEndpoint producer, int type, byte[] message, int chunkSize)
    {
        if (message.Length == 0)
        {
            PublishRawChunk(producer, type, ChunkFraming.FirstFlag | ChunkFraming.LastFlag, []);
            return;
        }

        int offset = 0;
        while (offset < message.Length)
        {
            int length = Math.Min(chunkSize, message.Length - offset);
            int flags = (offset == 0 ? ChunkFraming.FirstFlag : 0)
                | (offset + length == message.Length ? ChunkFraming.LastFlag : 0);
            PublishRawChunk(producer, type, flags, message.AsSpan(offset, length));
            offset += length;
        }
    }

    private static void PublishRawChunk(IProducerEndpoint producer, int type, int flags, ReadOnlySpan<byte> payload)
    {
        Assert.True(producer.TryReserveWrite(type, out Span<byte> chunk));
        unsafe
        {
            fixed (byte* pointer = chunk)
            {
                BinaryPrimitives.WriteInt32LittleEndian(new Span<byte>(pointer, ChunkFraming.HeaderSize), flags);
            }
        }

        payload.CopyTo(chunk[ChunkFraming.HeaderSize..]);
        producer.CommitWrite(ChunkFraming.HeaderSize + payload.Length);
    }

    private sealed class FuzzPair : IDisposable
    {
        public FuzzPair()
        {
            Factory = new InMemoryMemoryRegionFactory();
            Name = "fuzz-chunk-" + Guid.NewGuid().ToString("N");
            Producer = SparcRing.OpenProducer(
                Factory, Name, Capacity, SlotSize, cancellationToken: TestContext.Current.CancellationToken);
            Reader = new SparcStreamReader(SparcRing.OpenConsumer(
                Factory, Name, Capacity, SlotSize, cancellationToken: TestContext.Current.CancellationToken));
        }

        public InMemoryMemoryRegionFactory Factory { get; }

        public string Name { get; }

        public IProducerEndpoint Producer { get; }

        public SparcStreamReader Reader { get; }

        public void Dispose()
        {
            Producer.Dispose();
            Reader.Dispose();
        }
    }
}
