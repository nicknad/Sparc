namespace Sparc.Core;

/// <summary>Identifies which side of the buffer an instance owns.</summary>
internal enum RingBufferEndpointRole
{
    Producer = 0,
    Consumer = 1,
}

/// <summary>
/// Advisory lifecycle state of one endpoint, stored in the shared header.
/// </summary>
/// <remarks>
/// The state is advisory: a process that is killed hard cannot update it, so a
/// crashed endpoint stays <see cref="Running"/> forever. It is still useful for
/// graceful shutdown and for detecting "the peer said goodbye" without polling
/// the data path.
/// </remarks>
public enum RingBufferEndpointState
{
    /// <summary>No endpoint has claimed this role yet.</summary>
    NotPresent = 0,

    /// <summary>An endpoint is initializing (mapping, handshakes).</summary>
    Starting = 1,

    /// <summary>An endpoint is live and exchanging messages.</summary>
    Running = 2,

    /// <summary>An endpoint shut down gracefully; no further writes/reads will happen.</summary>
    Stopped = 3,

    /// <summary>An endpoint caught an error and aborted.</summary>
    Faulted = 4,
}
