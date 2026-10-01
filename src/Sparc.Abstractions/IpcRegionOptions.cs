namespace Sparc;

/// <summary>Options controlling how a shared region is created or opened.</summary>
public sealed class IpcRegionOptions
{
    public static IpcRegionOptions Default { get; } = new();

    /// <summary>How long to wait for a region to appear (or finish initializing).</summary>
    public TimeSpan OpenTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>When true, never create the region; fail if it does not already exist.</summary>
    public bool RequireExisting { get; init; }

    /// <summary>
    /// Optional transport-level access control applied when this process
    /// creates the region. It is ignored when joining an existing region:
    /// access is then governed by the configuration the creator applied.
    /// A non-null value that the selected transport cannot interpret fails the
    /// region establishment instead of being silently dropped.
    /// </summary>
    public IpcMemoryRegionSecurity? Security { get; init; }

    /// <summary>Clock used for timeout handling; replace in tests.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}
