using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;

namespace Sparc.Client;

/// <summary>
/// POSIX named semaphore used by <see cref="NamedSessionSignal"/> on Unix-like
/// systems. One-deep latch semantics are provided by the caller; this type only
/// wraps open/wait/post/close.
/// </summary>
/// <remarks>
/// The semaphore is never unlinked, mirroring the persistent file-backed region:
/// a crashed run leaves the name behind (in the kernel's semaphore namespace),
/// and a stale raise only causes one spurious wake-up that the session absorbs
/// by re-checking the buffer.
/// </remarks>
internal sealed class PosixSemaphore : IDisposable
{
    private const int Eintr = 4;
    private const int EtimeoutLinux = 110;
    private const int EtimeoutMacOs = 60;
    private const uint OwnerOnlyMode = 0x180; // 0o600

    private static readonly IntPtr SemFailed = new(-1);

    private IntPtr _handle;
    private int _disposed;

    private PosixSemaphore(IntPtr handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// Maps an arbitrary latch name to a POSIX semaphore name (leading slash,
    /// no other slashes, length-capped with a stable hash).
    /// </summary>
    internal static string ToName(string name)
    {
        const int MaxLength = 200;

        string sanitized = name.Replace('/', '_');
        if (sanitized.Length > MaxLength)
        {
            byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(name));
            sanitized = Convert.ToHexString(hash.AsSpan(0, 16));
        }

        return "/sparc." + sanitized;
    }

    /// <summary>Opens the named semaphore, creating it with count 0 when absent.</summary>
    internal static PosixSemaphore Open(string name)
    {
        if (!Environment.Is64BitProcess)
        {
            throw new PlatformNotSupportedException(
                "SPARC named latches require a 64-bit process (timespec layout).");
        }

        int openFlags = OperatingSystem.IsMacOS() ? 0x200 : 0x40; // O_CREAT
        IntPtr handle = sem_open(name, openFlags, OwnerOnlyMode, 0);
        if (handle == SemFailed)
        {
            throw new IOException(
                $"sem_open('{name}') failed with errno {Marshal.GetLastPInvokeError()}.");
        }

        return new PosixSemaphore(handle);
    }

    /// <summary>Raises the latch (best effort; a saturated latch is harmless).</summary>
    internal void Post() => _ = sem_post(_handle);

    /// <summary>Waits up to <paramref name="timeout"/>; false on timeout.</summary>
    internal bool Wait(TimeSpan timeout)
    {
        Timespec deadline = Timespec.FromUtcNow(timeout);
        while (true)
        {
            if (sem_timedwait(_handle, ref deadline) == 0)
            {
                return true;
            }

            int error = Marshal.GetLastPInvokeError();
            if (error == (OperatingSystem.IsMacOS() ? EtimeoutMacOs : EtimeoutLinux))
            {
                return false;
            }

            if (error == Eintr)
            {
                continue;
            }

            throw new IOException($"sem_timedwait failed with errno {error}.");
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            IntPtr handle = Interlocked.Exchange(ref _handle, IntPtr.Zero);
            if (handle != IntPtr.Zero)
            {
                _ = sem_close(handle);
            }
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct Timespec
    {
        public long Seconds;
        public long Nanoseconds;

        public static Timespec FromUtcNow(TimeSpan offset)
        {
            DateTime deadline = DateTime.UtcNow + offset;
            long ticksSinceUnixEpoch = deadline.Ticks - DateTime.UnixEpoch.Ticks;
            if (ticksSinceUnixEpoch < 0)
            {
                ticksSinceUnixEpoch = 0;
            }

            return new Timespec
            {
                Seconds = ticksSinceUnixEpoch / TimeSpan.TicksPerSecond,
                Nanoseconds = ticksSinceUnixEpoch % TimeSpan.TicksPerSecond * 100,
            };
        }
    }

    [DllImport("libc", SetLastError = true)]
    private static extern IntPtr sem_open(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name, int oflag, uint mode, uint value);

    [DllImport("libc", SetLastError = true)]
    private static extern int sem_timedwait(IntPtr semaphore, ref Timespec timeout);

    [DllImport("libc", SetLastError = true)]
    private static extern int sem_post(IntPtr semaphore);

    [DllImport("libc", SetLastError = true)]
    private static extern int sem_close(IntPtr semaphore);
}
