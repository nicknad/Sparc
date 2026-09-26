using System.Globalization;
using Sparc.YarpSample;

namespace Sparc.YarpProxy;

/// <summary>
/// Demo-only load generator: sends <c>Sparc:DemoRequestCount</c> requests through
/// the proxy with a deterministic <c>x-sample-value</c> header, so a single
/// <c>dotnet run</c> of the sample produces data for the median consumer. Set the
/// option to 0 to drive the proxy with your own client instead.
/// </summary>
internal sealed class DemoRequestGenerator(
    IConfiguration configuration,
    ILogger<DemoRequestGenerator> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        int count = configuration.GetValue("Sparc:DemoRequestCount", 2_000);
        if (count <= 0)
        {
            logger.LogInformation("Demo load generator disabled (Sparc:DemoRequestCount <= 0).");
            return;
        }

        using HttpClient client = new() { BaseAddress = new Uri(YarpCaptureProtocol.DemoProxyAddress) };

        // The server may not be accepting connections yet; retry the first send.
        await Task.Delay(300, stoppingToken).ConfigureAwait(false);

        logger.LogInformation("Demo load generator sending {Count} requests through the proxy.", count);
        int sent = 0;

        for (int i = 0; i < count && !stoppingToken.IsCancellationRequested; i++)
        {
            try
            {
                using HttpRequestMessage request = new(HttpMethod.Get, $"/proxy/request/{i}");
                request.Headers.TryAddWithoutValidation(
                    YarpCaptureProtocol.DefaultMedianHeader, i.ToString(CultureInfo.InvariantCulture));

                using HttpResponseMessage response = await client
                    .SendAsync(request, stoppingToken)
                    .ConfigureAwait(false);
                sent++;
            }
            catch (HttpRequestException exception)
            {
                logger.LogError(exception, "Demo request {Index} failed; stopping the generator.", i);
                break;
            }
        }

        logger.LogInformation("Demo load generator finished: sent={Sent}", sent);
    }
}
