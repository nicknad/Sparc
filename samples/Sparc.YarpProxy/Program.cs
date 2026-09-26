using Sparc.WindowsMemoryMapped;
using Sparc.YarpProxy;
using Sparc.YarpSample;
using Yarp.ReverseProxy.Configuration;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

// Fixed address: the embedded echo destination calls back into this same server.
builder.WebHost.UseUrls(YarpCaptureProtocol.DemoProxyAddress);

builder.Services.AddWindowsNamedMemoryMappedIpc();
builder.Services.AddSingleton<CaptureChannel>();
builder.Services.AddSingleton<CaptureToRingWorker>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<CaptureToRingWorker>());
builder.Services.AddHostedService<DemoRequestGenerator>();

builder.Services
    .AddReverseProxy()
    .LoadFromMemory(
        [
            new RouteConfig
            {
                RouteId = "capture-route",
                ClusterId = "echo-cluster",
                Match = new RouteMatch { Path = "/proxy/{**catch-all}" },
            },
        ],
        [
            new ClusterConfig
            {
                ClusterId = "echo-cluster",
                Destinations = new Dictionary<string, DestinationConfig>(StringComparer.OrdinalIgnoreCase)
                {
                    ["echo"] = new DestinationConfig
                    {
                        Address = YarpCaptureProtocol.DemoProxyAddress + "/echo",
                    },
                },
            },
        ]);

WebApplication app = builder.Build();

// Embedded backend so the sample needs no external service: YARP forwards a
// request path "/proxy/x" to "{proxy}/echo/proxy/x".
app.MapGet("/echo/{**rest}", (HttpContext context) => Results.Json(new
{
    path = context.Request.Path.Value,
    sample = context.Request.Headers[YarpCaptureProtocol.DefaultMedianHeader].ToString(),
}));

app.MapGet("/status", (CaptureToRingWorker worker, CaptureChannel channel) => Results.Ok(new
{
    proxy = YarpCaptureProtocol.DemoProxyAddress,
    ring = new
    {
        name = worker.RingName,
        capacity = builder.Configuration.GetValue("Sparc:RingCapacity", YarpCaptureProtocol.Capacity),
        slotSize = builder.Configuration.GetValue("Sparc:RingSlotSize", YarpCaptureProtocol.SlotSize),
    },
    capture = new
    {
        queueCapacity = YarpCaptureProtocol.MaxQueuedCaptures,
        dropped = channel.Dropped,
    },
    ringWriter = new
    {
        written = worker.Written,
        ringDropped = worker.RingDropped,
        tooLarge = worker.TooLarge,
    },
}));

app.MapReverseProxy(proxyPipeline =>
{
    // Capture in the proxy pipeline, then keep the required default steps.
    proxyPipeline.UseMiddleware<CaptureMiddleware>();
    proxyPipeline.UseSessionAffinity();
    proxyPipeline.UseLoadBalancing();
});

app.Logger.LogInformation(
    "YARP capture proxy listening on {Address}; status at /status, forwarded requests at /proxy/{{**}}.",
    YarpCaptureProtocol.DemoProxyAddress);

app.Run();
