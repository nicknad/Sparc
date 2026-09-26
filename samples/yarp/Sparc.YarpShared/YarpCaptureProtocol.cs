using System.Text.Json;

namespace Sparc.YarpSample;

/// <summary>
/// One proxied request captured by the YARP pipeline: the matched route plus the
/// request headers. This is the payload the SPARC producer ships to the consumer.
/// </summary>
internal sealed record CapturedRequest(
    string Route,
    string Method,
    string Path,
    string Host,
    Dictionary<string, string> Headers);

/// <summary>
/// Shared contract between the YARP proxy sample and its SPARC consumer:
/// region geometry, the header whose values are median-checked, capture limits
/// and the JSON encoding used for ring payloads.
/// </summary>
internal static class YarpCaptureProtocol
{
    public const string RingName = "sparc-yarp-demo";

    public const int Capacity = 1024;

    /// <summary>Slot size leaves room for a route, path and a bounded header set.</summary>
    public const int SlotSize = 8192;

    public const string DefaultMedianHeader = "x-sample-value";

    /// <summary>Address of the sample proxy (also the embedded echo destination).</summary>
    public const string DemoProxyAddress = "http://127.0.0.1:5210";

    public const int MaxHeaderCount = 32;

    public const int MaxHeaderValueLength = 256;

    public const int MaxQueuedCaptures = 10_000;

    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public static byte[] Encode(CapturedRequest request) =>
        JsonSerializer.SerializeToUtf8Bytes(request, Options);

    public static CapturedRequest? Decode(ReadOnlySpan<byte> payload)
    {
        try
        {
            return JsonSerializer.Deserialize<CapturedRequest>(payload, Options);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
