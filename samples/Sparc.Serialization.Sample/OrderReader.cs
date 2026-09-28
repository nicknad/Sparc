using Sparc.Core;

namespace Sparc.Serialization.Sample;

/// <summary>The matching reader for <see cref="Order"/>.</summary>
internal static class OrderReader
{
    private const ulong MaxItems = 1_000_000;

    public static Order Deserialize(ref SparcStreamReadBuffer reader)
    {
        int id = BufferExtensions.ReadInt32(ref reader);
        string customer = BufferExtensions.ReadString(ref reader);

        ulong count = BufferExtensions.ReadVarUInt64(ref reader);
        if (count > MaxItems)
        {
            throw new RingBufferCorruptedException($"Order declares {count} items; the limit is {MaxItems}.");
        }

        OrderItem[] items = new OrderItem[(int)count];
        for (int i = 0; i < items.Length; i++)
        {
            items[i] = new OrderItem(
                BufferExtensions.ReadString(ref reader),
                BufferExtensions.ReadInt32(ref reader),
                BufferExtensions.ReadInt32(ref reader));
        }

        string notes = BufferExtensions.ReadString(ref reader);
        return new Order(id, customer, items, notes);
    }
}
