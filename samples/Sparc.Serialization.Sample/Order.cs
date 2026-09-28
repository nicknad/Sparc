using System.Runtime.InteropServices;

namespace Sparc.Serialization.Sample;

internal sealed record Order(int Id, string Customer, IReadOnlyList<OrderItem> Items, string Notes);

[StructLayout(LayoutKind.Auto)]
internal readonly record struct OrderItem(string Sku, int Quantity, int PriceCents);
