namespace Sparc;

/// <summary>
/// Stable classification for errors raised by SPARC, so hosts can react to an
/// error without depending on a specific exception type.
/// </summary>
public enum SparcErrorCode
{
    /// <summary>No specific classification.</summary>
    Unknown = 0,

    /// <summary>A region or message did not arrive within the configured timeout.</summary>
    Timeout = 1,

    /// <summary>The selected transport is not available on the current operating system.</summary>
    PlatformNotSupported = 2,

    /// <summary>The region exists but does not contain a valid ring buffer.</summary>
    Corrupted = 3,

    /// <summary>The region was created by an incompatible layout/protocol version.</summary>
    VersionMismatch = 4,

    /// <summary>The region exists but was created with different geometry.</summary>
    GeometryMismatch = 5,

    /// <summary>Another live endpoint already holds the requested role.</summary>
    RoleConflict = 6,
}

/// <summary>Base type for every error raised by SPARC, transport and protocol alike.</summary>
public class SparcException : Exception
{
    public SparcException(string message) : base(message) { }

    public SparcException(string message, Exception innerException) : base(message, innerException) { }

    /// <summary>Stable classification of this error.</summary>
    public virtual SparcErrorCode Code => SparcErrorCode.Unknown;
}
