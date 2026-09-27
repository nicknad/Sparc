namespace Sparc.Core;

/// <summary>Base type for all ring buffer errors.</summary>
public class RingBufferException : SparcException
{
    public RingBufferException(string message) : base(message) { }

    public RingBufferException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>The region exists but is not a ring buffer (bad magic / inconsistent geometry).</summary>
public sealed class RingBufferCorruptedException : RingBufferException
{
    public RingBufferCorruptedException(string message) : base(message) { }

    public RingBufferCorruptedException(string message, Exception innerException) : base(message, innerException) { }

    /// <inheritdoc />
    public override SparcErrorCode Code => SparcErrorCode.Corrupted;
}

/// <summary>The region was created by an incompatible layout/protocol version.</summary>
public sealed class RingBufferVersionMismatchException : RingBufferException
{
    public RingBufferVersionMismatchException(string message) : base(message) { }

    public RingBufferVersionMismatchException(string message, Exception innerException) : base(message, innerException) { }

    /// <inheritdoc />
    public override SparcErrorCode Code => SparcErrorCode.VersionMismatch;
}

/// <summary>The region exists but was created with a different capacity/slot size.</summary>
public sealed class RingBufferGeometryMismatchException : RingBufferException
{
    public RingBufferGeometryMismatchException(string message) : base(message) { }

    public RingBufferGeometryMismatchException(string message, Exception innerException) : base(message, innerException) { }

    /// <inheritdoc />
    public override SparcErrorCode Code => SparcErrorCode.GeometryMismatch;
}

/// <summary>Another process already holds the requested role.</summary>
public sealed class RingBufferRoleConflictException : RingBufferException
{
    public RingBufferRoleConflictException(string message) : base(message) { }

    public RingBufferRoleConflictException(string message, Exception innerException) : base(message, innerException) { }

    /// <inheritdoc />
    public override SparcErrorCode Code => SparcErrorCode.RoleConflict;
}

/// <summary>A region did not appear (or was not initialized) within the configured timeout.</summary>
public sealed class RingBufferTimeoutException : RingBufferException
{
    public RingBufferTimeoutException(string message) : base(message) { }

    public RingBufferTimeoutException(string message, Exception innerException) : base(message, innerException) { }

    /// <inheritdoc />
    public override SparcErrorCode Code => SparcErrorCode.Timeout;
}

/// <summary>The selected transport is not available on the current operating system.</summary>
public sealed class RingBufferPlatformNotSupportedException : RingBufferException
{
    public RingBufferPlatformNotSupportedException(string message) : base(message) { }

    public RingBufferPlatformNotSupportedException(string message, Exception innerException) : base(message, innerException) { }

    /// <inheritdoc />
    public override SparcErrorCode Code => SparcErrorCode.PlatformNotSupported;
}
