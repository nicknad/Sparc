using Sparc;
using Sparc.WebApp;
using Sparc.WindowsMemoryMapped;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

// Chooses the OS transport once; nothing below this line sees MemoryMappedFile.
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddWindowsNamedMemoryMappedIpc();
builder.Services.AddSingleton<TransferCoordinator>();
builder.Services.AddHostedService<DemoTransferWorker>();

WebApplication app = builder.Build();

app.MapGet("/", (IIpcMemoryRegionFactory factory, TransferCoordinator coordinator) =>
    Results.Ok(new
    {
        product = "SPARC",
        transport = factory.GetType().Name,
        platformSupported = factory.IsSupported,
        region = new
        {
            name = DemoRing.Name,
            capacity = DemoRing.Capacity,
            slotSize = DemoRing.SlotSize,
            payloadSize = DemoRing.PayloadSize,
        },
        transfer = coordinator.GetStatus(),
    }));

app.MapPost("/transfer", (TransferCoordinator coordinator, int? count) =>
{
    int requested = count ?? DemoRing.DefaultCount;
    if (requested < 1)
    {
        return Results.BadRequest(new { error = "count must be positive." });
    }

    return coordinator.TryRequestTransfer(requested)
        ? Results.Accepted(value: new { requested })
        : Results.Conflict(new { error = "a transfer is already running; wait for it to finish." });
});

// Kick off one transfer at startup unless disabled in configuration
// (Sparc:AutoStartCount=0). POST /transfer runs more.
int autoStartCount = builder.Configuration.GetValue("Sparc:AutoStartCount", DemoRing.DefaultCount);
if (autoStartCount > 0)
{
    app.Services.GetRequiredService<TransferCoordinator>().TryRequestTransfer(autoStartCount);
}

app.Logger.LogInformation("SPARC web sample started; GET / for status, POST /transfer?count=n to run a transfer.");
app.Run();
