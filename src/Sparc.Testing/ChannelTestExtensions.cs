using Sparc.Channels;

namespace Sparc.Testing;

/// <summary>Timeouts for the async channel API, so tests fail fast instead of hanging.</summary>
public static class ChannelTestExtensions
{
    /// <summary>
    /// Awaits the next message and throws <see cref="TimeoutException"/> rather
    /// than hanging when nothing arrives within <paramref name="timeout"/>.
    /// </summary>
    public static async Task<T> ReadWithTimeoutAsync<T>(
        this SparcChannelReader<T> reader,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);

        return await WithTimeout(
            token => reader.ReadAsync(token),
            timeout,
            cancellationToken,
            $"No message arrived on channel '{reader.Name}' within {timeout.TotalMilliseconds:F0} ms.").ConfigureAwait(false);
    }

    /// <summary>
    /// Awaits a write and throws <see cref="TimeoutException"/> rather than
    /// hanging while the ring stays full.
    /// </summary>
    public static async Task WriteWithTimeoutAsync<T>(
        this SparcChannelWriter<T> writer,
        T item,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);

        await WithTimeout<bool>(
            async token =>
            {
                await writer.WriteAsync(item, token).ConfigureAwait(false);
                return true;
            },
            timeout,
            cancellationToken,
            $"Channel '{writer.Name}' stayed full for {timeout.TotalMilliseconds:F0} ms.").ConfigureAwait(false);
    }

    /// <summary>Waits for a message and throws <see cref="TimeoutException"/> when none arrives.</summary>
    public static async Task<bool> WaitToReadWithTimeoutAsync<T>(
        this SparcChannelReader<T> reader,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);

        return await WithTimeout(
            token => reader.WaitToReadAsync(token),
            timeout,
            cancellationToken,
            $"Channel '{reader.Name}' produced nothing within {timeout.TotalMilliseconds:F0} ms.").ConfigureAwait(false);
    }

    private static async Task<T> WithTimeout<T>(
        Func<CancellationToken, ValueTask<T>> operation,
        TimeSpan timeout,
        CancellationToken cancellationToken,
        string timeoutMessage)
    {
        using CancellationTokenSource timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        try
        {
            return await operation(timeoutSource.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(timeoutMessage, exception);
        }
    }
}
