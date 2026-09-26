namespace RingBuffer.Core;

/// <summary>Base type for all ring buffer errors.</summary>
public class RingBufferException : Exception
{
    public RingBufferException(string message) : base(message) { }

    public RingBufferException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>The region exists but is not a ring buffer (bad magic / inconsistent geometry).</summary>
public sealed class RingBufferCorruptedException : RingBufferException
{
    public RingBufferCorruptedException(string message) : base(message) { }
}

/// <summary>The region was created by an incompatible layout/protocol version.</summary>
public sealed class RingBufferVersionMismatchException : RingBufferException
{
    public RingBufferVersionMismatchException(string message) : base(message) { }
}

/// <summary>The region exists but was created with a different capacity/slot size.</summary>
public sealed class RingBufferGeometryMismatchException : RingBufferException
{
    public RingBufferGeometryMismatchException(string message) : base(message) { }
}

/// <summary>Another process already holds the requested role.</summary>
public sealed class RingBufferRoleConflictException : RingBufferException
{
    public RingBufferRoleConflictException(string message) : base(message) { }
}

/// <summary>A region did not appear (or was not initialized) within the configured timeout.</summary>
public sealed class RingBufferTimeoutException : RingBufferException
{
    public RingBufferTimeoutException(string message) : base(message) { }
}

/// <summary>Exit codes shared by the sample producer/consumer applications.</summary>
public static class RingBufferExitCodes
{
    public const int Success = 0;
    public const int Timeout = 2;
    public const int Incomplete = 3;
    public const int RoleConflict = 4;
    public const int Incompatible = 5;
    public const int Corrupted = 6;
    public const int VerificationFailed = 7;
    public const int UsageError = 64;
    public const int InternalError = 70;

    public static int FromException(RingBufferException exception) => exception switch
    {
        RingBufferRoleConflictException => RoleConflict,
        RingBufferVersionMismatchException => Incompatible,
        RingBufferGeometryMismatchException => Incompatible,
        RingBufferCorruptedException => Corrupted,
        RingBufferTimeoutException => Timeout,
        _ => InternalError,
    };
}
