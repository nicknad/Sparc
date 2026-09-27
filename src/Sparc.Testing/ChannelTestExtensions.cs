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

        using CancellationTokenSource timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        try
        {
            return await reader.ReadAsync(timeoutSource.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"No message arrived on channel '{reader.Name}' within {timeout.TotalMilliseconds:F0} ms.", exception);
        }
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

        using CancellationTokenSource timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        try
        {
            await writer.WriteAsync(item, timeoutSource.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Channel '{writer.Name}' stayed full for {timeout.TotalMilliseconds:F0} ms.", exception);
        }
    }

    /// <summary>Waits for a message and throws <see cref="TimeoutException"/> when none arrives.</summary>
    public static async Task<bool> WaitToReadWithTimeoutAsync<T>(
        this SparcChannelReader<T> reader,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);

        using CancellationTokenSource timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        try
        {
            return await reader.WaitToReadAsync(timeoutSource.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Channel '{reader.Name}' produced nothing within {timeout.TotalMilliseconds:F0} ms.", exception);
        }
    }
}
