namespace Sparc.Client;

/// <summary>Why a producer or consumer session stopped.</summary>
public enum SessionStopReason
{
    /// <summary>The requested message count was transferred (or the stream drained cleanly).</summary>
    Completed,

    /// <summary>The peer stopped or faulted, so no further progress is possible.</summary>
    PeerStopped,

    /// <summary>The buffer stayed full (producer) or empty (consumer) past its timeout.</summary>
    Timeout,

    /// <summary>A received message failed verification.</summary>
    VerificationFailed,

    /// <summary>The caller cancelled the session.</summary>
    Cancelled,
}
