using Microsoft.Extensions.Diagnostics.HealthChecks;
using Sparc.Core;

namespace Sparc.Hosting;

/// <summary>
/// Reports the default channel's endpoint states: healthy when every required
/// role is running, degraded while a required peer has not appeared yet, and
/// unhealthy when a role stopped, faulted or failed to open.
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

        Evaluate("producer", options.RequireProducer, status.ProducerEndpoint, status.ProducerState, unhealthy, degraded);
        Evaluate("consumer", options.RequireConsumer, status.ConsumerEndpoint, status.ConsumerState, unhealthy, degraded);

        Dictionary<string, object> data = new(StringComparer.Ordinal)
        {
            ["channel"] = status.Name,
            ["producerState"] = status.ProducerState.ToString(),
            ["consumerState"] = status.ConsumerState.ToString(),
        };

        if (status.FailureMessage is { } failure)
        {
            unhealthy.Add(failure);
            data["failure"] = failure;
        }

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
        object? endpoint,
        RingBufferEndpointState state,
        List<string> unhealthy,
        List<string> degraded)
    {
        if (!required)
        {
            return;
        }

        if (endpoint is null)
        {
            unhealthy.Add($"{role} endpoint is not open");
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
}
