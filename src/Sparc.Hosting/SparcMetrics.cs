using System.Diagnostics.Metrics;
using Sparc.Client;

namespace Sparc.Hosting;

/// <summary>
/// The <c>Sparc</c> meter: hosted services record session completions and
/// failures here. The hot path stays uninstrumented, so there is no cost when
/// nothing listens.
/// </summary>
public static class SparcMetrics
{
    /// <summary>Name of the meter, for <c>MeterProviderBuilder.AddMeter("Sparc")</c>.</summary>
    public const string MeterName = "Sparc";

    /// <summary>The meter shared by all SPARC hosting components.</summary>
    public static Meter Meter { get; } = new(MeterName);

    private static readonly Counter<long> ProducerCompletions =
        Meter.CreateCounter<long>("sparc.producer.completions", "{session}", "Producer sessions that finished.");

    private static readonly Counter<long> ConsumerCompletions =
        Meter.CreateCounter<long>("sparc.consumer.completions", "{session}", "Consumer sessions that finished.");

    private static readonly Counter<long> Failures =
        Meter.CreateCounter<long>("sparc.failures", "{failure}", "Hosted SPARC services that failed.");

    internal static void RecordProducerRun(string channel, ProducerRunResult result) =>
        ProducerCompletions.Add(1,
            new KeyValuePair<string, object?>("sparc.channel", channel),
            new KeyValuePair<string, object?>("sparc.reason", result.Reason.ToString()));

    internal static void RecordConsumerRun(string channel, ConsumerRunResult result) =>
        ConsumerCompletions.Add(1,
            new KeyValuePair<string, object?>("sparc.channel", channel),
            new KeyValuePair<string, object?>("sparc.reason", result.Reason.ToString()));

    internal static void RecordFailure(string channel) =>
        Failures.Add(1, new KeyValuePair<string, object?>("sparc.channel", channel));
}
