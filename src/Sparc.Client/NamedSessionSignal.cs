namespace Sparc.Client;

/// <summary>
/// A cross-process <see cref="ISessionSignal"/> backed by a named OS semaphore.
/// </summary>
/// <remarks>
/// <para>
/// .NET supports named semaphores on Windows only; on other platforms the
/// constructor throws <see cref="PlatformNotSupportedException"/>. Use
/// <see cref="InProcessSessionSignal"/> for single-process hosts or
/// <see cref="SessionWaitMode.SpinThenSleep"/>/<see cref="SessionWaitMode.SpinOnly"/>
/// on Unix-like systems.
/// </para>
/// <para>
/// The semaphore has a maximum count of one, so it behaves as a one-deep latch:
/// a raise is kept until the peer consumes it, and raising an already-raised
/// latch is ignored. A stale raise only causes one spurious wake-up, which the
/// waiting session absorbs by re-checking the buffer.
/// </para>
/// </remarks>
public sealed class NamedSessionSignal : ISessionSignal
{
    private const int CancellationPollMilliseconds = 10;

    private readonly Semaphore _semaphore;
    private int _disposed;

    /// <summary>Opens or creates the named latch.</summary>
    /// <param name="name">System-wide semaphore name; must be unique per direction.</param>
    public NamedSessionSignal(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException(
                "Named semaphores are supported on Windows only. Use InProcessSessionSignal for " +
                "same-process hosts, or a spin wait mode on this platform.");
        }

        _semaphore = new Semaphore(initialCount: 0, maximumCount: 1, name);
    }

    /// <inheritdoc />
    public void Signal()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        try
        {
            _semaphore.Release();
        }
        catch (SemaphoreFullException)
        {
            // Already raised; the waiter re-checks the buffer, so one pending
            // raise is enough.
        }
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

            if (_semaphore.WaitOne((int)Math.Min(remaining, CancellationPollMilliseconds)))
            {
                return true;
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _semaphore.Dispose();
        }
    }
}
