using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using RingBuffer.Cli;
using RingBuffer.Core;
using RingBuffer.Core.Diagnostics;
using RingBuffer.SharedMemory;

namespace RingBuffer.Consumer;

internal static class Program
{
    private static int Main(string[] args)
    {
        ConsumerOptions options;
        try
        {
            options = ConsumerOptions.Parse(args);
        }
        catch (UsageException exception)
        {
            Console.Error.WriteLine($"error: {exception.Message}");
            Console.Error.WriteLine();
            Console.Error.WriteLine(ConsumerOptions.Usage);
            return RingBufferExitCodes.UsageError;
        }

        if (options.ShowHelp)
        {
            Console.WriteLine(ConsumerOptions.Usage);
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

    private static int Run(ConsumerOptions options)
    {
        SharedMemoryOptions memoryOptions = new()
        {
            OpenTimeout = options.OpenTimeout,
            RecreateIfStale = options.RecreateStale,
            RequireExisting = options.RequireExisting,
            AdoptExistingGeometry = true,
        };

        using SharedRingBuffer buffer = SharedRingBuffer.OpenOrCreate(
            options.Name, options.Capacity, options.SlotSize, memoryOptions);
        buffer.Connect(RingBufferEndpointRole.Consumer, options.Takeover);

        if (!options.Quiet)
        {
            Console.WriteLine(
                $"ready: role=consumer name={options.Name} capacity={buffer.Capacity} " +
                $"slotSize={buffer.SlotSize} maxPayload={buffer.MaxPayloadSize}");
        }

        byte[] destination = new byte[buffer.MaxPayloadSize];
        LatencyHistogram histogram = new();
        SpinWait spin = new();
        long received = 0;
        long receivedBytes = 0;
        long expected = 0;
        long idleSince = 0;
        long idleTimeoutTicks = (long)(options.IdleTimeout.TotalSeconds * Stopwatch.Frequency);
        long firstMessageTimestamp = 0;
        long lastMessageTimestamp = 0;
        long runStartTimestamp = Stopwatch.GetTimestamp();
        int exitCode = RingBufferExitCodes.Success;

        ProcessResult ProcessOne()
        {
            if (!buffer.TryRead(destination, out int length, out int type))
            {
                return ProcessResult.Empty;
            }

            if (options.Verify)
            {
                if (length < ConsumerOptions.MessageHeaderBytes)
                {
                    Console.Error.WriteLine($"error: message {received} is only {length} bytes.");
                    return ProcessResult.VerificationFailed;
                }

                long sequence = BinaryPrimitives.ReadInt64LittleEndian(destination);
                if (sequence != expected)
                {
                    Console.Error.WriteLine(
                        $"error: sequence mismatch at message {received}: expected {expected}, received {sequence}.");
                    return ProcessResult.VerificationFailed;
                }

                if (type != options.Type)
                {
                    Console.Error.WriteLine($"error: message {received} has type {type}, expected {options.Type}.");
                    return ProcessResult.VerificationFailed;
                }

                if (destination.AsSpan(ConsumerOptions.MessageHeaderBytes, length - ConsumerOptions.MessageHeaderBytes)
                        .IndexOfAnyExcept((byte)0xA5) >= 0)
                {
                    Console.Error.WriteLine($"error: message {received} payload is corrupted.");
                    return ProcessResult.VerificationFailed;
                }

                long sentTimestamp = BinaryPrimitives.ReadInt64LittleEndian(destination.AsSpan(8));
                if (sentTimestamp > 0)
                {
                    histogram.Record(Stopwatch.GetTimestamp() - sentTimestamp);
                }
            }

            expected++;
            received++;
            receivedBytes += length;
            long now = Stopwatch.GetTimestamp();
            if (firstMessageTimestamp == 0)
            {
                firstMessageTimestamp = now;
            }

            lastMessageTimestamp = now;
            return ProcessResult.Ok;
        }

        while (options.Count == 0 || received < options.Count)
        {
            ProcessResult result = ProcessOne();
            if (result == ProcessResult.Ok)
            {
                spin.Reset();
                idleSince = 0;
                continue;
            }

            if (result == ProcessResult.VerificationFailed)
            {
                exitCode = RingBufferExitCodes.VerificationFailed;
                break;
            }

            // Empty: has the producer stopped?
            RingBufferEndpointState producer = buffer.ProducerState;
            if (producer is RingBufferEndpointState.Stopped or RingBufferEndpointState.Faulted)
            {
                // Seeing a terminal state has acquire semantics, so any message the
                // producer published before going away is now visible. Drain before
                // declaring the stream finished.
                ProcessResult drain;
                while ((drain = ProcessOne()) == ProcessResult.Ok)
                {
                }

                if (drain == ProcessResult.VerificationFailed)
                {
                    exitCode = RingBufferExitCodes.VerificationFailed;
                }

                break;
            }

            long now = Stopwatch.GetTimestamp();
            if (idleSince == 0)
            {
                idleSince = now;
            }
            else if (now - idleSince >= idleTimeoutTicks)
            {
                Console.Error.WriteLine(
                    $"error: no messages for {options.IdleTimeout.TotalSeconds:F1}s " +
                    $"(producer state={producer}, received={received}).");
                exitCode = RingBufferExitCodes.Incomplete;
                break;
            }

            spin.SpinOnce();
        }

        if (exitCode == RingBufferExitCodes.Success && options.Count > 0 && received < options.Count)
        {
            Console.Error.WriteLine($"error: producer stopped after {received} of {options.Count} messages.");
            exitCode = RingBufferExitCodes.Incomplete;
        }

        double elapsedSeconds = received > 1 && firstMessageTimestamp != 0
            ? Stopwatch.GetElapsedTime(firstMessageTimestamp, lastMessageTimestamp).TotalSeconds
            : Stopwatch.GetElapsedTime(runStartTimestamp).TotalSeconds;
        PrintSummary(received, receivedBytes, elapsedSeconds, histogram, buffer);

        return exitCode;
    }

    private static void PrintSummary(
        long received, long receivedBytes, double elapsedSeconds, LatencyHistogram histogram, SharedRingBuffer buffer)
    {
        double messagesPerSecond = elapsedSeconds > 0 ? received / elapsedSeconds : 0;
        double megabytesPerSecond = elapsedSeconds > 0 ? receivedBytes / elapsedSeconds / (1024.0 * 1024.0) : 0;

        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"consumed={received} bytes={receivedBytes} elapsed={elapsedSeconds:F3}s " +
            $"throughput={messagesPerSecond:F0} msg/s dataThroughput={megabytesPerSecond:F1} MiB/s"));
        Console.WriteLine($"producer={buffer.ProducerState} consumer={buffer.ConsumerState}");
        Console.WriteLine(histogram.ToMicrosecondsReport(Stopwatch.Frequency));
    }

    private enum ProcessResult
    {
        Empty,
        Ok,
        VerificationFailed,
    }
}
