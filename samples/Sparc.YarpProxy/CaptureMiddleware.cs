using Microsoft.Extensions.Primitives;
using Sparc.YarpSample;
using Yarp.ReverseProxy.Model;

namespace Sparc.YarpProxy;

/// <summary>
/// Runs inside the YARP proxy pipeline, before forwarding: captures the matched
/// route and the inbound request headers and publishes them to
/// <see cref="CaptureChannel"/>. Header keys are normalized to lower case and
/// both the header count and each value's length are bounded so a hostile request
/// cannot produce an oversized ring payload.
/// </summary>
internal sealed class CaptureMiddleware(RequestDelegate next, CaptureChannel channel)
{
    public Task InvokeAsync(HttpContext context)
    {
        IReverseProxyFeature? feature = context.Features.Get<IReverseProxyFeature>();
        string route = feature?.Route.Config.RouteId ?? "unknown";
        channel.TryPublish(Capture(context, route));
        return next(context);
    }

    private static CapturedRequest Capture(HttpContext context, string route)
    {
        Dictionary<string, string> headers = new(StringComparer.Ordinal);
        int copied = 0;

        foreach (KeyValuePair<string, StringValues> header in context.Request.Headers)
        {
            if (copied++ >= YarpCaptureProtocol.MaxHeaderCount)
            {
                break;
            }

            string value = header.Value.ToString();
            if (value.Length > YarpCaptureProtocol.MaxHeaderValueLength)
            {
                value = value[..YarpCaptureProtocol.MaxHeaderValueLength];
            }

            headers[header.Key.ToLowerInvariant()] = value;
        }

        return new CapturedRequest(
            route,
            context.Request.Method,
            context.Request.Path.Value ?? "/",
            context.Request.Host.Value ?? string.Empty,
            headers);
    }
}
