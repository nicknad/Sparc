using Sparc.Core;
using Sparc.InMemory;
using Sparc.Serialization;
using Sparc.Serialization.Sample;

const int Capacity = 16;
const int SlotSize = 256;

InMemoryMemoryRegionFactory factory = new();
using SparcStreamWriter writer = SparcStreamWriter.Open(factory, "orders", Capacity, SlotSize);
using SparcStreamReader reader = SparcStreamReader.Open(factory, "orders", Capacity, SlotSize);

OrderWriter<SparcStreamWriteBuffer> orderWriter = new();
SfStreamCodec<Order> codec = new(messageType: 1, orderWriter.Serialize, OrderReader.Deserialize);
byte[] scratch = new byte[64];

// 1. A small order fits in one chunk.
Order small = new(1, "alice", [new("widget", 2, 1999), new("gadget", 1, 4995)], new string('n', 32));
long smallBytes = codec.Write(writer, small);
Order readSmall = codec.Read(reader, scratch);
if (!OrdersEqual(small, readSmall))
{
    Console.Error.WriteLine("small order mismatched");
    return 1;
}

Console.WriteLine($"small order: {smallBytes} bytes in one chunk");

// 2. A large order does not fit the ring, so the producer streams it while the
//    consumer reads; message size is bounded by consumer liveness, not by the
//    region.
Order large = new(
    2,
    "bob",
    Enumerable.Range(0, 20_000).Select(i => new OrderItem($"sku-{i:D5}", i % 100, 100 + i)).ToArray(),
    new string('x', 1_000_000));
Task<long> producer = Task.Run(() => codec.Write(writer, large));
Order readLarge = codec.Read(reader, scratch);
long largeBytes = await producer.ConfigureAwait(false);
if (!OrdersEqual(large, readLarge))
{
    Console.Error.WriteLine("large order mismatched");
    return 1;
}

Console.WriteLine(
    $"large order: {largeBytes:N0} bytes streamed through the {Capacity * SlotSize:N0}-byte ring " +
    $"({large.Items.Count:N0} items, {large.Notes.Length:N0} notes chars)");

// 3. A serializer that fails halfway aborts the message: the consumer reports
//    corruption instead of decoding a truncated value.
SfStreamCodec<Order> failing = new(
    messageType: 2,
    static (ref SparcStreamWriteBuffer buffer, Order order) =>
    {
        BufferExtensions.WriteVarUInt64(ref buffer, 123);
        Span<byte> first = buffer.GetSpan(200);
        first[..200].Clear();
        buffer.Advance(200);

        Span<byte> second = buffer.GetSpan(200); // publishes the first chunk
        second[..200].Clear();
        buffer.Advance(200);
        throw new InvalidOperationException("simulated serialization failure");
    },
    OrderReader.Deserialize);

try
{
    failing.Write(writer, new Order(3, "carol", [], string.Empty));
}
catch (InvalidOperationException)
{
    Console.WriteLine("producer: serialization failed after one published chunk; message aborted");
}

try
{
    reader.ReadMessage(new byte[4096], out _, TimeSpan.FromSeconds(5));
    Console.Error.WriteLine("the aborted message was delivered");
    return 1;
}
catch (RingBufferCorruptedException)
{
    Console.WriteLine("consumer: aborted message reported as corruption");
}

// 4. The stream stays usable after the abort.
Order recovered = new(4, "dave", [new("thing", 1, 100)], "note");
codec.Write(writer, recovered);
if (!OrdersEqual(recovered, codec.Read(reader, scratch)))
{
    Console.Error.WriteLine("recovered order mismatched");
    return 1;
}

Console.WriteLine("stream healthy after the abort; sample completed");
return 0;

static bool OrdersEqual(Order left, Order right)
{
    if (left.Id != right.Id
        || !string.Equals(left.Customer, right.Customer, StringComparison.Ordinal)
        || !string.Equals(left.Notes, right.Notes, StringComparison.Ordinal)
        || left.Items.Count != right.Items.Count)
    {
        return false;
    }

    for (int i = 0; i < left.Items.Count; i++)
    {
        if (left.Items[i] != right.Items[i])
        {
            return false;
        }
    }

    return true;
}
