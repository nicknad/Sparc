using Microsoft.Extensions.Diagnostics.HealthChecks;
using Sparc.Client;
using Sparc.Core;

namespace Sparc.Hosting;

/// <summary>
/// Reports the default channel's endpoint states: healthy when every required
/// role is running, degraded while a required role has not opened yet, and
/// unhealthy when a role stopped or faulted, opening failed, or a hosted
/// session reported a structured failure (timeout, verification, peer loss).
/// </summary>
public sealed class SparcChannelHealthCheck(SparcChannelStatus status, SparcHealthOptions options) : IHealthCheck
{
    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        List<string> unhealthy = [];
        List<string> degraded = [];

        IProducerEndpoint? producer = status.ProducerEndpoint;
        RingBufferEndpointState producerState = producer?.ProducerState ?? RingBufferEndpointState.NotPresent;
        ProducerRunResult? producerResult = status.ProducerResult;

        IConsumerEndpoint? consumer = status.ConsumerEndpoint;
        RingBufferEndpointState consumerState = consumer?.ConsumerState ?? RingBufferEndpointState.NotPresent;
        ConsumerRunResult? consumerResult = status.ConsumerResult;

        Evaluate("producer", options.RequireProducer, producer is not null, producerState, unhealthy, degraded);
        Evaluate("consumer", options.RequireConsumer, consumer is not null, consumerState, unhealthy, degraded);

        Dictionary<string, object> data = new(StringComparer.Ordinal)
        {
            ["channel"] = status.Name,
            ["producerState"] = producerState.ToString(),
            ["consumerState"] = consumerState.ToString(),
        };

        if (status.FailureMessage is { } failure)
        {
            unhealthy.Add(failure);
            data["failure"] = failure;
        }

        AddSessionResult("producer", producerResult?.Reason, producerResult?.FailureMessage, unhealthy, data);
        AddSessionResult("consumer", consumerResult?.Reason, consumerResult?.FailureMessage, unhealthy, data);

        if (unhealthy.Count > 0)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy(string.Join("; ", unhealthy), data: data));
        }

        if (degraded.Count > 0)
        {
            return Task.FromResult(HealthCheckResult.Degraded(string.Join("; ", degraded), data: data));
        }

        return Task.FromResult(HealthCheckResult.Healthy($"channel '{status.Name}' is running", data));
    }

    private static void Evaluate(
        string role,
        bool required,
        bool hasEndpoint,
        RingBufferEndpointState state,
        List<string> unhealthy,
        List<string> degraded)
    {
        if (!required)
        {
            return;
        }

        if (!hasEndpoint)
        {
            // Required but not opened yet: the peer has not appeared, which is a
            // degraded state, not a failure. Open failures record a failure
            // message separately.
            degraded.Add($"{role} endpoint is not open");
            return;
        }

        switch (state)
        {
            case RingBufferEndpointState.Running:
                return;
            case RingBufferEndpointState.NotPresent or RingBufferEndpointState.Starting:
                degraded.Add($"{role} is {state}");
                return;
            default:
                unhealthy.Add($"{role} is {state}");
                return;
        }
    }

    private static void AddSessionResult(
        string role,
        SessionStopReason? reason,
        string? failure,
        List<string> unhealthy,
        Dictionary<string, object> data)
    {
        if (reason is { } stopReason)
        {
            data[role + "Reason"] = stopReason.ToString();
        }

        if (failure is { } message)
        {
            unhealthy.Add($"{role} session failed: {message}");
        }
    }
}
