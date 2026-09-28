using SerializerFoundation;

namespace Sparc.Serialization.Sample;

/// <summary>
/// A formatter for <see cref="Order"/> written the way a serializer built on
/// SerializerFoundation writes one: generic over the destination buffer with an
/// <c>allows ref struct</c> constraint, so the JIT specializes it per buffer.
/// Pass <see cref="SparcStreamWriteBuffer"/> to stream the message through
/// chunk slots.
/// </summary>
internal sealed class OrderWriter<TWriter>
    where TWriter : struct, IWriteBuffer, allows ref struct
{
    public void Serialize(ref TWriter writer, Order order)
    {
        ArgumentNullException.ThrowIfNull(order);
        BufferExtensions.WriteInt32(ref writer, order.Id);
        BufferExtensions.WriteString(ref writer, order.Customer);

        BufferExtensions.WriteVarUInt64(ref writer, (ulong)order.Items.Count);
        foreach (OrderItem item in order.Items)
        {
            BufferExtensions.WriteString(ref writer, item.Sku);
            BufferExtensions.WriteInt32(ref writer, item.Quantity);
            BufferExtensions.WriteInt32(ref writer, item.PriceCents);
        }

        BufferExtensions.WriteString(ref writer, order.Notes);
    }
}
