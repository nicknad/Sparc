namespace Sparc.Client;

/// <summary>
/// Pacing helper for <see cref="ProducerSessionOptions.PerMessageDelay"/> and
/// <see cref="ConsumerSessionOptions.PerMessageDelay"/>: keeps a session on an
/// ideal schedule of one message per delay, sleeping when the delay is
/// millisecond-scale and spinning otherwise. Intended for tests and benchmarks
/// that simulate a slow endpoint, not for production rate limiting.
/// </summary>
internal static class MessagePacer
{
    public static long TicksFor(TimeProvider timeProvider, TimeSpan perMessageDelay) =>
        (long)(perMessageDelay.TotalSeconds * timeProvider.TimestampFrequency);

    public static void Wait(TimeProvider timeProvider, long startTimestamp, long messages, long delayTicks)
    {
        if (delayTicks <= 0)
        {
            return;
        }

        long due = startTimestamp + messages * delayTicks;
        bool sleep = delayTicks >= timeProvider.TimestampFrequency / 1000;
        while (timeProvider.GetTimestamp() < due)
        {
            if (sleep)
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
