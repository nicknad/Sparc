# Testing SPARC (Sparc.Testing)

`Sparc.Testing` removes the boilerplate from unit-testing code that talks to a
SPARC channel: in-process endpoint pairs, typed test channels, deterministic
time and fail-fast timeouts. Nothing here needs the OS, a real mapping or a
second process.

## Paired endpoints

```csharp
using Sparc.Testing;

using TestSparcRing ring = TestSparcRing.Create(capacity: 64, slotSize: 64);

ring.Producer.TryPublish(payload);
if (ring.Consumer.TryRead(destination, out int length, out int type)) { ... }

// Sessions over the same pair, already connected:
ProducerSession producer = ring.CreateProducerSession(new ProducerSessionOptions { Count = 100 });
ConsumerSession consumer = ring.CreateConsumerSession(new ConsumerSessionOptions { Count = 100 });
```

## Deterministic time

`TestSparcRing.Create` accepts a `TimeProvider` and hands it to the sessions it
creates. With `FakeTimeProvider` from
`Microsoft.Extensions.TimeProvider.Testing`, timeout behavior is exact and
instant:

```csharp
FakeTimeProvider time = new();
using TestSparcRing ring = TestSparcRing.Create(capacity: 4, slotSize: 64, timeProvider: time);

ProducerSession session = ring.CreateProducerSession(new ProducerSessionOptions
{
    Count = 100,
    PayloadSize = 32,
    FullTimeout = TimeSpan.FromMilliseconds(100),
});

Task<ProducerRunResult> run = session.RunAsync(cancellationToken);

// The ring fills (4 slots), then the session waits:
time.Advance(TimeSpan.FromMilliseconds(100));

ProducerRunResult result = await run;   // Reason == Timeout, Produced == 4
```

Pacing (`PerMessageDelay`) and elapsed windows also use the injected clock, so
the whole session timeline is controllable. Sessions still spin/sleep while the
ring is full/empty; advance time until the state you are testing is reached
rather than sleeping in the test.

## Typed channels

```csharp
using Sparc.Channels;
using Sparc.Testing;

using SparcChannel<int> channel = TestSparcChannel.Create(Int32Codec.Instance, capacity: 8);

Assert.True(channel.Writer.TryWrite(42));
int value = await channel.Reader.ReadWithTimeoutAsync(TimeSpan.FromSeconds(5));
```

`ReadWithTimeoutAsync`, `WriteWithTimeoutAsync` and
`WaitToReadWithTimeoutAsync` throw `TimeoutException` instead of hanging, which
keeps a broken test from stalling the run.

## Hosting tests

Register the in-memory factory before `AddSparcIpc()`; the existing
registration wins, so the same production wiring runs without the OS:

```csharp
services.AddSingleton<IIpcMemoryRegionFactory>(new InMemoryMemoryRegionFactory());
services.AddSparcIpc();
services.AddSparcChannel(options => options.Name = "test");
services.AddSparcProducerSession(new ProducerSessionOptions { Count = 10, PayloadSize = 32 });
```

Hosted services expose `Completion` and `Result`, so a test can call
`StartAsync`/`StopAsync` directly and await the run without a host.
`SparcChannelStatus` reports the live endpoint states and the last failure.

## What the tests in this repository do

`tests/Sparc.UnitTests` uses this package for session and channel tests;
`tests/Sparc.ConcurrencyTests` still drives the raw buffers through dedicated
threads for the 10M-message verification, `tests/Sparc.FuzzTests` property-tests
the untrusted-input paths (headers, slots, chunk framing, reassembly, hostile
peers), and `tests/Sparc.ProcessTests` spawns real CLI processes for lifecycle
and kill scenarios. Use the same split: in-process for logic, real processes only
when the OS or crash semantics are the thing under test.
