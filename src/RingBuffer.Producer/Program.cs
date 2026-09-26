using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using RingBuffer.Cli;
using RingBuffer.Core;
using RingBuffer.SharedMemory;

namespace RingBuffer.Producer;

internal static class Program
{
    private static int Main(string[] args)
    {
        ProducerOptions options;
        try
        {
            options = ProducerOptions.Parse(args);
        }
        catch (UsageException exception)
        {
            Console.Error.WriteLine($"error: {exception.Message}");
            Console.Error.WriteLine();
            Console.Error.WriteLine(ProducerOptions.Usage);
            return RingBufferExitCodes.UsageError;
        }

        if (options.ShowHelp)
        {
            Console.WriteLine(ProducerOptions.Usage);
            return RingBufferExitCodes.Success;
        }

        try
        {
            return Run(options);
        }
        catch (RingBufferException exception)
        {
            Console.Error.WriteLine($"error: {exception.Message}");
            return RingBufferExitCodes.FromException(exception);
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"fatal: {exception}");
            return RingBufferExitCodes.InternalError;
        }
    }

    private static int Run(ProducerOptions options)
    {
        SharedMemoryOptions memoryOptions = new()
        {
            OpenTimeout = options.OpenTimeout,
            RecreateIfStale = options.RecreateStale,
            RequireExisting = options.RequireExisting,
        };

        using SharedRingBuffer buffer = SharedRingBuffer.OpenOrCreate(
            options.Name, options.Capacity, options.EffectiveSlotSize, memoryOptions);
        buffer.Connect(RingBufferEndpointRole.Producer, options.Takeover);

        if (!options.Quiet)
        {
            Console.WriteLine(
                $"ready: role=producer name={options.Name} capacity={buffer.Capacity} " +
                $"slotSize={buffer.SlotSize} maxPayload={buffer.MaxPayloadSize}");
        }

        byte[] payload = new byte[options.Size];
        if (options.Size > ProducerOptions.MessageHeaderBytes)
        {
            payload.AsSpan(ProducerOptions.MessageHeaderBytes).Fill(0xA5);
        }

        long fullTimeoutTicks = (long)(options.FullTimeout.TotalSeconds * Stopwatch.Frequency);
        SpinWait spin = new();
        long produced = 0;
        long fullSince = 0;

        long startTimestamp = Stopwatch.GetTimestamp();
        while (produced < options.Count)
        {
            BinaryPrimitives.WriteInt64LittleEndian(payload, produced);
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(8), Stopwatch.GetTimestamp());

            if (buffer.TryWrite(options.Type, payload))
            {
                produced++;
                fullSince = 0;
                spin.Reset();
                continue;
            }

            long now = Stopwatch.GetTimestamp();
            if (fullSince == 0)
            {
                fullSince = now;
            }
            else if (now - fullSince >= fullTimeoutTicks)
            {
                RingBufferEndpointState consumer = buffer.ConsumerState;
                if (consumer is RingBufferEndpointState.Stopped or RingBufferEndpointState.Faulted)
                {
                    Console.Error.WriteLine(
                        $"error: consumer is gone (state={consumer}) with {options.Count - produced} messages unsent.");
                    return RingBufferExitCodes.Incomplete;
                }

                Console.Error.WriteLine(
                    $"error: buffer stayed full for {options.FullTimeout.TotalSeconds:F1}s " +
                    $"({options.Count - produced} messages unsent, consumer state={consumer}).");
                return RingBufferExitCodes.Timeout;
            }

            spin.SpinOnce();
        }

        double elapsedSeconds = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
        PrintSummary(options, produced, elapsedSeconds);
        return RingBufferExitCodes.Success;
    }

    private static void PrintSummary(ProducerOptions options, long produced, double elapsedSeconds)
    {
        double messagesPerSecond = elapsedSeconds > 0 ? produced / elapsedSeconds : 0;
        double megabytesPerSecond = messagesPerSecond * options.Size / (1024.0 * 1024.0);

        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"produced={produced} payloadSize={options.Size} elapsed={elapsedSeconds:F3}s " +
            $"throughput={messagesPerSecond:F0} msg/s dataThroughput={megabytesPerSecond:F1} MiB/s"));
    }
}
