using Microsoft.Extensions.Logging;

namespace Sparc.Hosting;

internal static class SparcHostedServiceRunner
{
    internal static async Task ExecuteAsync<TEndpoint>(
        SparcChannelStatus status,
        string channelName,
        ILogger logger,
        TaskCompletionSource completion,
        Func<CancellationToken, TEndpoint> open,
        Action<TEndpoint> attach,
        Action<TEndpoint> ready,
        Action detach,
        Func<TEndpoint, CancellationToken, Task> run,
        string failureLogMessage,
        CancellationToken stoppingToken)
        where TEndpoint : class, IDisposable
    {
        TEndpoint? endpoint = null;
        try
        {
            endpoint = await Task.Run(() => open(stoppingToken), stoppingToken).ConfigureAwait(false);

            attach(endpoint);
            ready(endpoint);

            await run(endpoint, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            status.Fail(exception.Message);
            SparcMetrics.RecordFailure(channelName);
            logger.LogError(exception, failureLogMessage, exception.Message);
            throw;
        }
        finally
        {
            detach();
            endpoint?.Dispose();
            completion.TrySetResult();
        }
    }
}
