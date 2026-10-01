# Hosting SPARC (Sparc.Hosting)

`Sparc.Hosting` turns the library into a few registration lines for web apps,
worker services and any `Microsoft.Extensions.Hosting` host: it picks the
transport, opens the channel, claims the role and runs the session or your own
worker as a `BackgroundService`.

## Minimal producer and consumer

Producer process:

```csharp
using Sparc.Client;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSparcIpc();
builder.Services.AddSparcChannel(options =>
{
    options.Name = "orders";
    options.Capacity = 1024;   // power of two
    options.SlotSize = 256;    // >= payload + 8; 64-byte multiples avoid false sharing
});
builder.Services.AddSparcProducerSession(options =>
{
    options.Count = 1_000_000;
    options.PayloadSize = 64;
});

builder.Build().Run();
```

Consumer process: identical, with

```csharp
builder.Services.AddSparcConsumerSession(options =>
{
    options.Count = 0;                  // until the producer stops
    options.IdleTimeout = TimeSpan.FromSeconds(10);
});
```

`AddSparcIpc()` selects `WindowsNamedMemoryMappedRegionFactory` on Windows and
`UnixFileMemoryMappedRegionFactory` elsewhere (pass
`options => options.UnixDirectory = "/dev/shm/sparc"` to relocate the backing
files). An existing `IIpcMemoryRegionFactory` registration wins, so tests and
custom transports can register their own before calling `AddSparcIpc()`.

`AddSparcChannel(options => options.Security = WindowsSectionSecurity.CurrentUserOnly)`
applies transport access control when this process creates the region (Windows
section DACLs; see the README security section). It is ignored on a join and by
transports that do not implement it (the Unix factory rejects it rather than
dropping it silently).

## Registrations

| API | Effect |
|---|---|
| `AddSparcIpc()` | `IIpcMemoryRegionFactory` singleton for the current OS + `TimeProvider` |
| `AddSparcIpc(Func<IServiceProvider, IIpcMemoryRegionFactory>)` | Custom factory (tests, custom transport) |
| `AddSparcChannel(Action<SparcChannelOptions>)` | Default channel: name, geometry, open policy, takeover, transport security |
| `AddSparcChannel(IConfigurationSection)` | Binds the same options from configuration |
| `AddSparcProducerSession(ProducerSessionOptions?)` / `AddSparcConsumerSession(ConsumerSessionOptions?)` | Hosted `ProducerSession`/`ConsumerSession` |
| `AddSparcProducerWorker<TWorker>()` / `AddSparcConsumerWorker<TWorker>()` | Hosted custom `ISparcProducerWorker`/`ISparcConsumerWorker` |
| `AddSparcHealthChecks(Action<SparcHealthOptions>?)` | Channel health check named `sparc` |

Every role registration requires `AddSparcIpc()` (or a factory registration)
first; otherwise `AddSparcChannel` fails immediately with an actionable
`InvalidOperationException`. One channel per service collection is supported;
for multiple regions, build the endpoints yourself with `SparcRing`.

## Lifecycle

Each hosted service, on start:

1. opens the region (create or join, honoring `OpenTimeout`, `RequireExisting`,
   `RecreateIfStale`, `AdoptExistingGeometry`, `Takeover`);
2. claims its role (a second live endpoint for the same role throws
   `RingBufferRoleConflictException`);
3. runs the session or worker;
4. disposes the endpoint on stop, publishing `Stopped` so the peer can drain and
   exit.

Open failures and unexpected exceptions are logged, recorded in
`SparcChannelStatus.FailureMessage` and rethrown, so the default host behavior
stops the application instead of running without a channel. Session stop
reasons (peer stopped, idle timeout, verification failure) are structured
results, not exceptions, and are logged and exposed through
`SparcChannelStatus.ProducerResult`/`ConsumerResult`.

## Custom workers

```csharp
sealed class OrderProducerWorker : ISparcProducerWorker
{
    public async Task RunAsync(IProducerEndpoint producer, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await queue.Reader.WaitToReadAsync(cancellationToken);
            while (queue.Reader.TryRead(out Order order))
            {
                byte[] payload = OrderCodec.Encode(order);
                if (!producer.TryPublish(payload))
                {
                    // ring full: choose your drop/block policy
                }
            }
        }
    }
}

builder.Services.AddSparcProducerWorker<OrderProducerWorker>();
```

The endpoint is already connected and role-claimed; the worker owns the loop and
returns when it is done. `TryPublish`, `TryBeginWrite`/`WriteLease` and
`BeginWrite` are all available; see the endpoint API in the main README.

## Health checks

`AddSparcHealthChecks(options => { options.RequireProducer = true; options.RequireConsumer = true; })`
registers a check named `sparc`:

| Situation | Result |
|---|---|
| Every required endpoint open and `Running` | Healthy |
| Endpoint attached but `Starting`/`NotPresent` | Degraded |
| Endpoint not open, `Stopped` or `Faulted`, or a recorded failure | Unhealthy |

Register it with your health endpoint (`app.MapHealthChecks("/healthz")`).

## Configuration binding

```csharp
// appsettings.json:
// {
//   "Sparc": {
//     "Channel": { "Name": "orders", "Capacity": 1024, "SlotSize": 256 }
//   }
// }
builder.Services.AddSparcChannel(builder.Configuration.GetSection("Sparc:Channel"));
```

## Metrics

`SparcMetrics.Meter` (name `Sparc`) exposes session completion counters tagged
with `sparc.channel` and `sparc.reason`, plus a failure counter. Subscribe with
OpenTelemetry (`AddMeter("Sparc")`) or any `MeterListener`. The hot path itself
is not instrumented, so there is no cost when nothing listens.

## Testing

```csharp
services.AddSingleton<IIpcMemoryRegionFactory>(new InMemoryMemoryRegionFactory());
services.AddSparcIpc();                       // keeps the override
services.AddSparcChannel(options => options.Name = "test");
services.AddSparcProducerSession(new ProducerSessionOptions { Count = 10, PayloadSize = 32 });
```

Hosted services expose `Completion` and `Result`, so tests can start them
directly (`StartAsync`/`StopAsync`) and await the run without a host.
