using Sparc.Core;

namespace Sparc.Hosting;

/// <summary>
/// Options for the default channel endpoints opened by the hosted services:
/// name, geometry and how the region is created or joined.
/// </summary>
public sealed class SparcChannelOptions
{
    /// <summary>Region name both processes share. Required.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Number of slots; must be a power of two.</summary>
    public int Capacity { get; set; } = SparcRing.DefaultCapacity;

    /// <summary>Bytes per slot; must exceed the message header.</summary>
    public int SlotSize { get; set; } = SparcRing.DefaultSlotSize;

    /// <summary>How long to wait for the region to appear (or finish initializing).</summary>
    public TimeSpan OpenTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>When true, never create the region; fail if it does not already exist.</summary>
    public bool RequireExisting { get; set; }

    /// <summary>When true, reclaim a stale region whose creator died before publishing it.</summary>
    public bool RecreateIfStale { get; set; }

    /// <summary>When true, adopt an existing region's geometry instead of failing on a mismatch.</summary>
    public bool AdoptExistingGeometry { get; set; }

    /// <summary>When true, reclaim the endpoint role from a crashed peer.</summary>
    public bool Takeover { get; set; }

    /// <summary>Validates the name and geometry; throws <see cref="ArgumentException"/> otherwise.</summary>
    public void Validate()
    {
        RegionName.Validate(Name);
        RingBufferLayout.ValidateGeometry(Capacity, SlotSize);
    }

    internal SharedRingBufferOptions ToRingOptions(TimeProvider timeProvider) => new()
    {
        OpenTimeout = OpenTimeout,
        RequireExisting = RequireExisting,
        RecreateIfStale = RecreateIfStale,
        AdoptExistingGeometry = AdoptExistingGeometry,
        Takeover = Takeover,
        TimeProvider = timeProvider,
    };
}
