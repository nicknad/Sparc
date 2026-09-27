using System.Diagnostics;
using System.Globalization;
using Sparc;
using Sparc.Core;
using Sparc.WindowsMemoryMapped;
using Sparc.YarpConsumer;
using Sparc.YarpSample;

const string Usage = """
    Sparc.YarpConsumer — checks the median of a request header captured by the YARP proxy sample.

    Usage:
      Sparc.YarpConsumer [options]

    Options:
      --name <ring>            SPARC region name (default: sparc-yarp-demo).
      --header <name>          Header to aggregate (default: x-sample-value).
      --count <n>              Stop after n captures (default: 0 = until idle/producer stops).
      --idle-timeout <ms>      Stop after this long without a message (default: 10000).
      --capacity <slots>       Geometry used only if this process creates the region (default: 1024).
      --slot-size <bytes>      Geometry used only if this process creates the region (default: 8192).
      --max-samples <n>        Cap on retained values for the median (default: 500000).
      --expected-median <v>    Expected median; enables a PASS/FAIL check.
      --tolerance <v>          Allowed absolute deviation for the check (default: 1).
      -h, --help               Show this help.

    Exit codes:
      0 success (check passed or no expected median given), 1 median check failed,
      2 no captures received, 5 platform not supported, 64 usage error.
    """;

string name = YarpCaptureProtocol.RingName;
string header = YarpCaptureProtocol.DefaultMedianHeader;
long count = 0;
int idleTimeoutMs = 10_000;
int capacity = YarpCaptureProtocol.Capacity;
int slotSize = YarpCaptureProtocol.SlotSize;
int maxSamples = 500_000;
double? expectedMedian = null;
double tolerance = 1.0;

for (int i = 0; i < args.Length; i++)
{
    switch (args[i])
    {
        case "--name":
            name = args[++i];
            break;
        case "--header":
            header = args[++i];
            break;
        case "--count":
            count = long.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--idle-timeout":
            idleTimeoutMs = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--capacity":
            capacity = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--slot-size":
            slotSize = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--max-samples":
            maxSamples = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--expected-median":
            expectedMedian = double.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--tolerance":
            tolerance = double.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "-h":
        case "--help":
            Console.WriteLine(Usage);
            return 0;
        default:
            Console.Error.WriteLine($"error: unknown argument '{args[i]}'.");
            Console.Error.WriteLine(Usage);
            return 64;
    }
}

header = header.ToLowerInvariant();

WindowsNamedMemoryMappedRegionFactory factory = new();
if (!factory.IsSupported)
{
    Console.Error.WriteLine("error: this sample requires Windows named memory-mapped files.");
    return 5;
}

using CancellationTokenSource cancellation = new();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellation.Cancel();
};

using IConsumerEndpoint buffer = SparcRing.OpenConsumer(
    factory,
    name,
    capacity,
    slotSize,
    new SharedRingBufferOptions
    {
        OpenTimeout = TimeSpan.FromSeconds(30),
        AdoptExistingGeometry = true,
    });

Console.WriteLine(
    $"ready: role=consumer name={buffer.Name} capacity={buffer.Capacity} slotSize={buffer.SlotSize} medianHeader={header}");

byte[] destination = new byte[buffer.MaxPayloadSize];
MedianTracker tracker = new(maxSamples);
Stopwatch stopwatch = Stopwatch.StartNew();
long idleSince = 0;
long lastReport = 0;

while (true)
{
    if (buffer.TryRead(destination, out int length, out _))
    {
        idleSince = 0;
        Track(destination.AsSpan(0, length));

        if (count > 0 && tracker.Received >= count)
        {
            break;
        }

        continue;
    }

    if (buffer.ProducerState is RingBufferEndpointState.Stopped or RingBufferEndpointState.Faulted)
    {
        // The producer is gone; drain anything published before it stopped.
        while (buffer.TryRead(destination, out int remaining, out _))
        {
            Track(destination.AsSpan(0, remaining));
        }

        break;
    }

    if (cancellation.IsCancellationRequested)
    {
        break;
    }

    long now = stopwatch.ElapsedMilliseconds;
    if (idleSince == 0)
    {
        idleSince = now;
    }
    else if (now - idleSince >= idleTimeoutMs)
    {
        break;
    }

    if (now - lastReport >= 5_000)
    {
        lastReport = now;
        Console.WriteLine("progress: " + Format(tracker, header, tracker.Compute()));
    }

    Thread.Sleep(1);
}

MedianReport report = tracker.Compute();
Console.WriteLine("result: " + Format(tracker, header, report));

if (tracker.Received == 0)
{
    Console.Error.WriteLine("error: no captures received (is the proxy running?).");
    return 2;
}

if (expectedMedian is { } expected)
{
    bool passed = Math.Abs(report.Median - expected) <= tolerance;
    Console.WriteLine(string.Create(
        CultureInfo.InvariantCulture,
        $"median-check: expected={expected:F2} tolerance={tolerance:F2} actual={report.Median:F2} result={(passed ? "PASS" : "FAIL")}"));
    return passed ? 0 : 1;
}

return 0;

void Track(ReadOnlySpan<byte> payload)
{
    tracker.CountMessage();
    CapturedRequest? request = YarpCaptureProtocol.Decode(payload);
    if (request is null)
    {
        tracker.CountUndecodable();
        return;
    }

    if (!request.Headers.TryGetValue(header, out string? value))
    {
        tracker.CountMissingHeader();
        return;
    }

    if (!double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out double parsed))
    {
        tracker.CountInvalidValue();
        return;
    }

    tracker.Add(parsed);
}

static string Format(MedianTracker tracker, string header, MedianReport report) => string.Create(
    CultureInfo.InvariantCulture,
    $"header={header} received={tracker.Received} samples={report.Samples} missing={tracker.MissingHeader} " +
    $"invalid={tracker.InvalidValue} undecodable={tracker.Undecodable} median={report.Median:F2} p95={report.P95:F2} max={report.Max:F2}");
