namespace Sparc.Client;

/// <summary>
/// An <see cref="ISessionSignal"/> for endpoints that share a process (tests,
/// the WebApp sample, single-process development). Uses an auto-reset event,
/// which has exactly the one-deep latch semantics the protocol expects.
/// </summary>
public sealed class InProcessSessionSignal : ISessionSignal
{
    private const int CancellationPollMilliseconds = 10;

    private readonly AutoResetEvent _event = new(initialState: false);
    private int _disposed;

    /// <inheritdoc />
    public void Signal()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        _event.Set();
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

            if (_event.WaitOne((int)Math.Min(remaining, CancellationPollMilliseconds)))
            {
                return true;
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _event.Dispose();
        }
    }
}
