using Sparc.Core;

namespace Sparc.Cli;

/// <summary>
/// Process exit codes shared by the sample CLI hosts. Library layers return
/// structured results/reasons; only the executables translate them to codes.
/// </summary>
internal static class RingBufferExitCodes
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
        RingBufferPlatformNotSupportedException => Incompatible,
        RingBufferCorruptedException => Corrupted,
        RingBufferTimeoutException => Timeout,
        _ => InternalError,
    };
}
