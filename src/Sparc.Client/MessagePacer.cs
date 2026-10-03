namespace Sparc.Client;

/// <summary>
/// Pacing helper for <see cref="ProducerSessionOptions.PerMessageDelay"/> and
/// <see cref="ConsumerSessionOptions.PerMessageDelay"/>: keeps a session on an
/// ideal schedule of one message per delay, sleeping for long waits and
/// spinning for the final stretch so a 1 ms delay does not round up to the
/// Windows timer tick. Intended for tests and benchmarks that simulate a slow
/// endpoint, not for production rate limiting.
/// </summary>
internal static class MessagePacer
{
    public static long TicksFor(TimeProvider timeProvider, TimeSpan perMessageDelay) =>
        (long)(perMessageDelay.TotalSeconds * timeProvider.TimestampFrequency);

    public static void Wait(
        TimeProvider timeProvider,
        long startTimestamp,
        long messages,
        long delayTicks,
        CancellationToken cancellationToken)
    {
        if (delayTicks <= 0)
        {
            return;
        }

        long due = startTimestamp + messages * delayTicks;
        long spinThreshold = timeProvider.TimestampFrequency / 500; // 2 ms
        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return; // the run loop reports cancellation through its result
            }

            long now = timeProvider.GetTimestamp();
            if (now >= due)
            {
                return;
            }

            // Spin the last couple of milliseconds (and all delays shorter than
            // that) to avoid the coarse sleep quantization bursting messages
            // after each timer tick.
            if (due - now >= spinThreshold)
            {
                Thread.Sleep(1);
            }
            else
            {
                Thread.SpinWait(64);
            }
        }
    }
}
