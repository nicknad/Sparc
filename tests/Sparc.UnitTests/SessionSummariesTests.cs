using System.Text.Json;
using Sparc.Client;
using Sparc.Client.Diagnostics;
using Sparc.Core;

namespace Sparc.UnitTests;

public class SessionSummariesTests
{
    [Fact]
    public void ProducerJsonRoundTrips()
    {
        ProducerRunResult result = new(
            Produced: 1000,
            PayloadSize: 64,
            Elapsed: TimeSpan.FromSeconds(1),
            Reason: SessionStopReason.Completed,
            Unsent: 0,
            PeerState: RingBufferEndpointState.Stopped,
            FailureMessage: null)
        {
            ActiveElapsed = TimeSpan.FromSeconds(0.5),
        };

        string json = SessionSummaries.ProducerJson("orders", 1024, 256, 248, result);

        Assert.True(SessionSummaries.TryParseProducer(json, out ProducerSummary? summary));
        Assert.NotNull(summary);
        Assert.Equal("orders", summary.Name);
        Assert.Equal(1024, summary.Capacity);
        Assert.Equal(256, summary.SlotSize);
        Assert.Equal(1000, summary.Produced);
        Assert.Equal(2000, summary.ThroughputMsgS, precision: 6);
        Assert.Equal(SessionStopReason.Completed, summary.Reason);
    }

    [Fact]
    public void ConsumerJsonRoundTrips()
    {
        LatencyHistogram histogram = new();
        for (int i = 0; i < 100; i++)
        {
            histogram.Record(1000); // 100 us at 10 MHz
        }

        ConsumerRunResult result = new(
            Received: 100,
            ReceivedBytes: 6400,
            Elapsed: TimeSpan.FromSeconds(1),
            Reason: SessionStopReason.Completed,
            Latency: histogram,
            FailureMessage: null)
        {
            RunElapsed = TimeSpan.FromSeconds(1.1),
        };

        string json = SessionSummaries.ConsumerJson(
            "orders", 1024, 256, result, 10_000_000, "Stopped", "Running");

        Assert.True(SessionSummaries.TryParseConsumer(json, out ConsumerSummary? summary));
        Assert.NotNull(summary);
        Assert.Equal(100, summary.Received);
        Assert.Equal(6400, summary.ReceivedBytes);
        Assert.Equal(100, summary.Latency.N);
        Assert.Equal(100, summary.Latency.P50Us, precision: 3);
        Assert.Equal("Stopped", summary.ProducerState);
    }

    [Fact]
    public void ParserFindsJsonAmongHumanLines()
    {
        ProducerRunResult result = new(
            Produced: 10,
            PayloadSize: 64,
            Elapsed: TimeSpan.FromSeconds(1),
            Reason: SessionStopReason.Completed,
            Unsent: 0,
            PeerState: RingBufferEndpointState.Running,
            FailureMessage: null)
        {
            ActiveElapsed = TimeSpan.FromSeconds(1),
        };

        string mixed = "ready: role=producer name=orders capacity=1024 slotSize=256 maxPayload=248\n"
            + SessionSummaries.ProducerJson("orders", 1024, 256, 248, result);

        Assert.True(SessionSummaries.TryParseProducer(mixed, out ProducerSummary? summary));
        Assert.NotNull(summary);
        Assert.Equal(10, summary.Produced);
    }

    [Fact]
    public void ParserRejectsGarbageAndWrongTool()
    {
        Assert.False(SessionSummaries.TryParseProducer("produced=10 Gazorp", out _));
        Assert.False(SessionSummaries.TryParseConsumer("{\"tool\":\"producer\"}", out _));
    }

    [Fact]
    public void AdviceJsonCarriesTheDocumentedSchema()
    {
        RingBufferAdvice advice = RingBufferAdvisor.Advise(4096, capacity: 1024, targetGiBs: 5);
        using JsonDocument document = JsonDocument.Parse(SessionSummaries.AdviceJson(advice));
        JsonElement root = document.RootElement;

        Assert.Equal("advise", root.GetProperty("tool").GetString());
        Assert.Equal(4160, root.GetProperty("slotSize").GetInt32());
        Assert.Equal(192L + 1024 * 4160, root.GetProperty("regionBytes").GetInt64());
        Assert.True(root.GetProperty("messagesPerSecondAtTarget").GetDouble() > 1_000_000);
    }
}
