namespace RingBuffer.SharedMemory;

/// <summary>Options controlling how a shared region is opened.</summary>
public sealed class SharedMemoryOptions
{
    public static SharedMemoryOptions Default { get; } = new();

    /// <summary>
    /// How long to wait for a region to appear (or to finish initializing)
    /// before giving up.
    /// </summary>
    public TimeSpan OpenTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// When true, an existing region whose header never became valid (its
    /// creator died before publishing the magic) is released and recreated.
    /// Destructive: the caller must be sure that no live participant is using
    /// the region.
    /// </summary>
    public bool RecreateIfStale { get; init; }

    /// <summary>When true, never create the region; fail if it does not already exist.</summary>
    public bool RequireExisting { get; init; }

    /// <summary>
    /// When true, the capacity/slot size requested by the caller are only used
    /// if this process creates the region; an existing region's geometry is
    /// adopted as-is.
    /// </summary>
    public bool AdoptExistingGeometry { get; init; }
}
