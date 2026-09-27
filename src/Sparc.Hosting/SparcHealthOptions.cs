namespace Sparc.Hosting;

/// <summary>Options for the channel health check.</summary>
public sealed class SparcHealthOptions
{
    /// <summary>When true, a missing or non-running producer makes the check degraded or unhealthy.</summary>
    public bool RequireProducer { get; set; }

    /// <summary>When true, a missing or non-running consumer makes the check degraded or unhealthy.</summary>
    public bool RequireConsumer { get; set; }
}
