namespace Sparc.Core;

/// <summary>Options controlling how a ring buffer region is created or joined.</summary>
public sealed class SharedRingBufferOptions
{
    public static SharedRingBufferOptions Default { get; } = new();

    /// <summary>How long to wait for a region to appear (or to finish initializing).</summary>
    public TimeSpan OpenTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>When true, never create the region; fail if it does not already exist.</summary>
    public bool RequireExisting { get; init; }

    /// <summary>
    /// When true, a region whose header never became valid (its creator died
    /// before publishing the magic) is released and recreated. Destructive: the
    /// caller must be sure that no live participant is using the region.
    /// </summary>
    public bool RecreateIfStale { get; init; }

    /// <summary>
    /// When true, the capacity/slot size requested by the caller are only used
    /// if this process creates the region; an existing region's geometry is
    /// adopted as-is.
    /// </summary>
    public bool AdoptExistingGeometry { get; init; }

    /// <summary>
    /// Optional transport-level access control applied if this process creates
    /// the region (for example a Windows section DACL). It is ignored when
    /// joining an existing region. The ring protocol never inspects it; it is
    /// passed through to the <see cref="IIpcMemoryRegionFactory"/>.
    /// </summary>
    public IpcMemoryRegionSecurity? Security { get; init; }

    /// <summary>
    /// When true, <see cref="SparcRing"/> reclaims the role from a crashed
    /// peer instead of failing with a role conflict. Destructive: the caller
    /// must be sure the previous holder is gone.
    /// </summary>
    public bool Takeover { get; init; }

    /// <summary>Clock used for timeout handling; replace in tests.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}
