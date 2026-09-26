namespace Sparc.WebApp;

/// <summary>
/// Fixed geometry shared by the two endpoints of the demo channel. Both sides
/// must agree on capacity and slot size; in a real deployment they would come
/// from configuration shared by the producer and consumer hosts.
/// </summary>
internal static class DemoRing
{
    public const string Name = "sparc-webapp-demo";
    public const int Capacity = 1024;
    public const int SlotSize = 256;
    public const int PayloadSize = 64;
    public const int DefaultCount = 100_000;
}
