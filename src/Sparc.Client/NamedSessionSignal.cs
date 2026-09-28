namespace Sparc.Client;

/// <summary>
/// A cross-process <see cref="ISessionSignal"/> backed by a named OS semaphore:
/// a Windows named semaphore on Windows, a POSIX named semaphore on Unix-like
/// systems.
/// </summary>
/// <remarks>
/// <para>
/// The latch is one-deep: a raise is kept until the peer consumes it, and
/// raising an already-raised latch is ignored. A stale raise only causes one
/// spurious wake-up, which the waiting session absorbs by re-checking the
/// buffer. On Unix, semaphores are not unlinked when disposed, so a crashed run
/// can leave a stale raise behind; it is harmless for the same reason.
/// </para>
/// <para>
/// Named latches require a 64-bit process; on browser/WASI targets the
/// constructor throws <see cref="PlatformNotSupportedException"/>. Use
/// <see cref="InProcessSessionSignal"/> for single-process hosts.
/// </para>
/// </remarks>
public sealed class NamedSessionSignal : ISessionSignal
{
    private const int CancellationPollMilliseconds = 10;

    private readonly Semaphore? _semaphore;
    private readonly PosixSemaphore? _posix;
    private int _disposed;

    /// <summary>Opens or creates the named latch.</summary>
    /// <param name="name">System-wide latch name; must be unique per direction.</param>
    public NamedSessionSignal(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);

        if (OperatingSystem.IsWindows())
        {
            _semaphore = new Semaphore(initialCount: 0, maximumCount: 1, name);
            return;
        }

        if (OperatingSystem.IsBrowser() || OperatingSystem.IsWasi())
        {
            throw new PlatformNotSupportedException(
                "Named latches need OS semaphores, which are not available on this platform. " +
                "Use InProcessSessionSignal for same-process hosts, or a spin wait mode.");
        }

        _posix = PosixSemaphore.Open(PosixSemaphore.ToName(name));
    }

    /// <inheritdoc />
    public void Signal()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        if (_semaphore is not null)
        {
            try
            {
                _semaphore.Release();
            }
            catch (SemaphoreFullException)
            {
                // Already raised; the waiter re-checks the buffer, so one pending
                // raise is enough.
            }

            return;
        }

        _posix!.Post();
    }

    /// <inheritdoc />
    public bool Wait(TimeSpan timeout, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        long deadline = timeout == Timeout.InfiniteTimeSpan
            ? long.MaxValue
            : Environment.TickCount64 + (long)timeout.TotalMilliseconds;

        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return false;
            }

            long remaining = deadline - Environment.TickCount64;
            if (remaining <= 0)
            {
                return false;
            }

            int slice = (int)Math.Min(remaining, CancellationPollMilliseconds);
            if (_semaphore is not null)
            {
                if (_semaphore.WaitOne(slice))
                {
                    return true;
                }
            }
            else if (_posix!.Wait(TimeSpan.FromMilliseconds(slice)))
            {
                return true;
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _semaphore?.Dispose();
            _posix?.Dispose();
        }
    }
}
