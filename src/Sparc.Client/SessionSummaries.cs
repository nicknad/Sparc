using System.Globalization;
using System.Text.Json;
using Sparc.Client.Diagnostics;

namespace Sparc.Client;

/// <summary>
/// Machine-readable session summaries for the CLIs (<c>--format json</c>) and the
/// cross-process benchmark harness. The human <c>produced=…/consumed=…</c> lines stay
/// the default; JSON is opt-in so scripts do not regex-scrape prose.
/// Schema (all numbers invariant-culture, <c>_s</c> = seconds, <c>_us</c> = microseconds):
/// <c>{"tool":"producer","name","capacity","slotSize","maxPayload","produced","payloadSize",
/// "elapsedS","activeElapsedS","throughputMsgS","dataThroughputMiBS","reason"}</c> and
/// <c>{"tool":"consumer","name","capacity","slotSize","received","receivedBytes","elapsedS",
/// "wallElapsedS","throughputMsgS","dataThroughputMiBS","producerState","consumerState",
/// "latency":{"minUs","meanUs","p50Us","p90Us","p95Us","p99Us","p999Us","maxUs","n"}}</c>.
/// </summary>
public static class SessionSummaries
{
    /// <summary>JSON-escapes <paramref name="value"/> including the surrounding quotes.</summary>
    /// <remarks>Runs once per session summary (CLI exit / harness parse) — never on the message path.</remarks>
    internal static string Quote(string value)
    {
        System.Text.StringBuilder sb = new(value.Length + 2);
        sb.Append('"');
        foreach (char c in value)
        {
            switch (c)
            {
                case '"':
                    sb.Append("\\\"");
                    break;
                case '\\':
                    sb.Append("\\\\");
                    break;
                case '\n':
                    sb.Append("\\n");
                    break;
                case '\r':
                    sb.Append("\\r");
                    break;
                case '\t':
                    sb.Append("\\t");
                    break;
                default:
                    if (char.IsControl(c))
                    {
                        sb.Append("\\u");
                        sb.Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                    }
                    else
                    {
                        sb.Append(c);
                    }

                    break;
            }
        }

        sb.Append('"');
        return sb.ToString();
    }

    /// <summary>Builds the producer summary object for <paramref name="result"/>.</summary>
    public static string ProducerJson(
        string name,
        int capacity,
        int slotSize,
        int maxPayload,
        ProducerRunResult result)
    {
        ArgumentNullException.ThrowIfNull(name);

        TimeSpan active = result.ActiveElapsed > TimeSpan.Zero ? result.ActiveElapsed : result.Elapsed;
        double seconds = active.TotalSeconds;
        double messagesPerSecond = seconds > 0 ? result.Produced / seconds : 0;
        double mebibytesPerSecond = messagesPerSecond * result.PayloadSize / (1024.0 * 1024.0);

        return string.Create(CultureInfo.InvariantCulture,
            $"{{\"tool\":\"producer\",\"name\":{Quote(name)}," +
            $"\"capacity\":{capacity},\"slotSize\":{slotSize},\"maxPayload\":{maxPayload}," +
            $"\"produced\":{result.Produced},\"payloadSize\":{result.PayloadSize}," +
            $"\"elapsedS\":{result.Elapsed.TotalSeconds.ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"activeElapsedS\":{result.ActiveElapsed.TotalSeconds.ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"throughputMsgS\":{messagesPerSecond.ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"dataThroughputMiBS\":{mebibytesPerSecond.ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"reason\":{Quote(result.Reason.ToString())}}}");
    }

    /// <summary>Builds the consumer summary object for <paramref name="result"/>.</summary>
    public static string ConsumerJson(
        string name,
        int capacity,
        int slotSize,
        ConsumerRunResult result,
        double timestampFrequency,
        string producerState,
        string consumerState)
    {
        ArgumentNullException.ThrowIfNull(name);

        double seconds = result.Elapsed.TotalSeconds;
        double messagesPerSecond = seconds > 0 ? result.Received / seconds : 0;
        double mebibytesPerSecond = seconds > 0 ? result.ReceivedBytes / seconds / (1024.0 * 1024.0) : 0;
        double toMicros = 1_000_000.0 / timestampFrequency;
        LatencyHistogram latency = result.Latency;

        return string.Create(CultureInfo.InvariantCulture,
            $"{{\"tool\":\"consumer\",\"name\":{Quote(name)}," +
            $"\"capacity\":{capacity},\"slotSize\":{slotSize}," +
            $"\"received\":{result.Received},\"receivedBytes\":{result.ReceivedBytes}," +
            $"\"elapsedS\":{result.Elapsed.TotalSeconds.ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"wallElapsedS\":{result.RunElapsed.TotalSeconds.ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"throughputMsgS\":{messagesPerSecond.ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"dataThroughputMiBS\":{mebibytesPerSecond.ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"producerState\":{Quote(producerState)}," +
            $"\"consumerState\":{Quote(consumerState)}," +
            $"\"latency\":{{\"minUs\":{(latency.MinTicks * toMicros).ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"meanUs\":{(latency.MeanTicks * toMicros).ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"p50Us\":{(latency.PercentileTicks(0.50) * toMicros).ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"p90Us\":{(latency.PercentileTicks(0.90) * toMicros).ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"p95Us\":{(latency.PercentileTicks(0.95) * toMicros).ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"p99Us\":{(latency.PercentileTicks(0.99) * toMicros).ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"p999Us\":{(latency.PercentileTicks(0.999) * toMicros).ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"maxUs\":{(latency.MaxTicks * toMicros).ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"n\":{latency.Count}}}}}");
    }

    /// <summary>Builds the sizing-advice object for <paramref name="advice"/> (<c>--advise --format json</c>).</summary>
    public static string AdviceJson(Core.RingBufferAdvice advice)
    {
        ArgumentNullException.ThrowIfNull(advice);

        return string.Create(CultureInfo.InvariantCulture,
            $"{{\"tool\":\"advise\",\"payloadSize\":{advice.PayloadSize}," +
            $"\"capacity\":{advice.Capacity},\"slotSize\":{advice.SlotSize}," +
            $"\"regionBytes\":{advice.RegionBytes}," +
            $"\"regionMiB\":{advice.RegionMiB.ToString("G17", CultureInfo.InvariantCulture)}," +
            $"\"residency\":{Quote(advice.Residency)}," +
            $"\"alignmentWarning\":{(advice.AlignmentWarning is null ? "null" : Quote(advice.AlignmentWarning))}," +
            $"\"messagesPerSecondAtTarget\":{(advice.MessagesPerSecondAtTarget.HasValue ? advice.MessagesPerSecondAtTarget.Value.ToString("G17", CultureInfo.InvariantCulture) : "null")}}}");
    }

    /// <summary>Tries to find and parse a producer summary in CLI output (JSON line first, whole text fallback).</summary>
    public static bool TryParseProducer(string text, out ProducerSummary? summary)
    {
        summary = null;
        using JsonDocument? document = TryGetDocument(text, "producer", out JsonElement root);
        if (document is null)
        {
            return false;
        }

        try
        {
            if (!Enum.TryParse<SessionStopReason>(root.GetProperty("reason").GetString(), out SessionStopReason reason))
            {
                return false;
            }

            summary = new ProducerSummary(
                root.GetProperty("name").GetString() ?? string.Empty,
                root.GetProperty("capacity").GetInt32(),
                root.GetProperty("slotSize").GetInt32(),
                root.GetProperty("maxPayload").GetInt32(),
                root.GetProperty("produced").GetInt64(),
                root.GetProperty("payloadSize").GetInt32(),
                root.GetProperty("elapsedS").GetDouble(),
                root.GetProperty("activeElapsedS").GetDouble(),
                root.GetProperty("throughputMsgS").GetDouble(),
                root.GetProperty("dataThroughputMiBS").GetDouble(),
                reason);
            return true;
        }
        catch (Exception exception) when (exception is KeyNotFoundException
            or InvalidOperationException
            or FormatException)
        {
            return false;
        }
    }

    /// <summary>Tries to find and parse a consumer summary in CLI output (JSON line first, whole text fallback).</summary>
    public static bool TryParseConsumer(string text, out ConsumerSummary? summary)
    {
        summary = null;
        using JsonDocument? document = TryGetDocument(text, "consumer", out JsonElement root);
        if (document is null)
        {
            return false;
        }

        try
        {
            JsonElement latency = root.GetProperty("latency");
            summary = new ConsumerSummary(
                root.GetProperty("name").GetString() ?? string.Empty,
                root.GetProperty("capacity").GetInt32(),
                root.GetProperty("slotSize").GetInt32(),
                root.GetProperty("received").GetInt64(),
                root.GetProperty("receivedBytes").GetInt64(),
                root.GetProperty("elapsedS").GetDouble(),
                root.GetProperty("wallElapsedS").GetDouble(),
                root.GetProperty("throughputMsgS").GetDouble(),
                root.GetProperty("dataThroughputMiBS").GetDouble(),
                root.GetProperty("producerState").GetString() ?? string.Empty,
                root.GetProperty("consumerState").GetString() ?? string.Empty,
                new LatencySummary(
                    latency.GetProperty("minUs").GetDouble(),
                    latency.GetProperty("meanUs").GetDouble(),
                    latency.GetProperty("p50Us").GetDouble(),
                    latency.GetProperty("p90Us").GetDouble(),
                    latency.GetProperty("p95Us").GetDouble(),
                    latency.GetProperty("p99Us").GetDouble(),
                    latency.GetProperty("p999Us").GetDouble(),
                    latency.GetProperty("maxUs").GetDouble(),
                    latency.GetProperty("n").GetInt64()));
            return true;
        }
        catch (Exception exception) when (exception is KeyNotFoundException
            or InvalidOperationException
            or FormatException)
        {
            return false;
        }
    }

    /// <summary>
    /// Finds the JSON line in CLI output, parses it and checks its <c>tool</c> tag.
    /// Returns the live document on success (the caller disposes it); null otherwise.
    /// </summary>
    private static JsonDocument? TryGetDocument(string text, string expectedTool, out JsonElement root)
    {
        root = default;
        if (!TryFindJson(text, out string? json) || json is null)
        {
            return null;
        }

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(json);
        }
        catch (JsonException)
        {
            return null;
        }

        try
        {
            root = document.RootElement;
            if (string.Equals(root.GetProperty("tool").GetString(), expectedTool, StringComparison.Ordinal))
            {
                return document;
            }
        }
        catch (Exception exception) when (exception is KeyNotFoundException or InvalidOperationException)
        {
            // Missing "tool" or a non-object root: not our schema.
        }

        document.Dispose();
        root = default;
        return null;
    }

    private static bool TryFindJson(string text, out string? json)
    {
        foreach (string line in text.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries))
        {
            if (line.TrimStart().StartsWith("{", StringComparison.Ordinal))
            {
                json = line.Trim();
                return true;
            }
        }

        json = null;
        return false;
    }
}

/// <summary>Parsed producer summary (<see cref="SessionSummaries.TryParseProducer"/>).</summary>
public sealed class ProducerSummary
{
    public ProducerSummary(
        string name,
        int capacity,
        int slotSize,
        int maxPayload,
        long produced,
        int payloadSize,
        double elapsedS,
        double activeElapsedS,
        double throughputMsgS,
        double dataThroughputMiBS,
        SessionStopReason reason)
    {
        Name = name;
        Capacity = capacity;
        SlotSize = slotSize;
        MaxPayload = maxPayload;
        Produced = produced;
        PayloadSize = payloadSize;
        ElapsedS = elapsedS;
        ActiveElapsedS = activeElapsedS;
        ThroughputMsgS = throughputMsgS;
        DataThroughputMiBS = dataThroughputMiBS;
        Reason = reason;
    }

    public string Name { get; }

    public int Capacity { get; }

    public int SlotSize { get; }

    public int MaxPayload { get; }

    public long Produced { get; }

    public int PayloadSize { get; }

    public double ElapsedS { get; }

    public double ActiveElapsedS { get; }

    public double ThroughputMsgS { get; }

    public double DataThroughputMiBS { get; }

    public SessionStopReason Reason { get; }
}

/// <summary>Parsed consumer summary (<see cref="SessionSummaries.TryParseConsumer"/>).</summary>
public sealed class ConsumerSummary
{
    public ConsumerSummary(
        string name,
        int capacity,
        int slotSize,
        long received,
        long receivedBytes,
        double elapsedS,
        double wallElapsedS,
        double throughputMsgS,
        double dataThroughputMiBS,
        string producerState,
        string consumerState,
        LatencySummary latency)
    {
        Name = name;
        Capacity = capacity;
        SlotSize = slotSize;
        Received = received;
        ReceivedBytes = receivedBytes;
        ElapsedS = elapsedS;
        WallElapsedS = wallElapsedS;
        ThroughputMsgS = throughputMsgS;
        DataThroughputMiBS = dataThroughputMiBS;
        ProducerState = producerState;
        ConsumerState = consumerState;
        Latency = latency;
    }

    public string Name { get; }

    public int Capacity { get; }

    public int SlotSize { get; }

    public long Received { get; }

    public long ReceivedBytes { get; }

    public double ElapsedS { get; }

    public double WallElapsedS { get; }

    public double ThroughputMsgS { get; }

    public double DataThroughputMiBS { get; }

    public string ProducerState { get; }

    public string ConsumerState { get; }

    public LatencySummary Latency { get; }
}

/// <summary>Parsed latency percentiles in microseconds.</summary>
public sealed class LatencySummary
{
    public LatencySummary(
        double minUs,
        double meanUs,
        double p50Us,
        double p90Us,
        double p95Us,
        double p99Us,
        double p999Us,
        double maxUs,
        long n)
    {
        MinUs = minUs;
        MeanUs = meanUs;
        P50Us = p50Us;
        P90Us = p90Us;
        P95Us = p95Us;
        P99Us = p99Us;
        P999Us = p999Us;
        MaxUs = maxUs;
        N = n;
    }

    public double MinUs { get; }

    public double MeanUs { get; }

    public double P50Us { get; }

    public double P90Us { get; }

    public double P95Us { get; }

    public double P99Us { get; }

    public double P999Us { get; }

    public double MaxUs { get; }

    public long N { get; }
}
