using Sparc.Channels;
using Sparc.InMemory;

namespace Sparc.Testing;

/// <summary>Factory helpers for typed channels over an in-process region.</summary>
public static class TestSparcChannel
{
    /// <summary>
    /// Creates a paired writer/reader over one in-process region with a unique
    /// name, using <see cref="InMemoryMemoryRegionFactory"/>.
    /// </summary>
    public static SparcChannel<T> Create<T>(
        ISparcCodec<T> codec,
        int capacity = 1024,
        int slotSize = 0,
        TimeSpan? idleTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(codec);
        return SparcChannel<T>.CreateInProcess(
            new InMemoryMemoryRegionFactory(),
            "sparc-test-" + Guid.NewGuid().ToString("N"),
            codec,
            capacity,
            slotSize,
            idleTimeout: idleTimeout);
    }
}
