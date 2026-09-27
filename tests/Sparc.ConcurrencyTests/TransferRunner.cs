using System.Buffers.Binary;
using System.Diagnostics;
using Sparc.Core;

namespace Sparc.ConcurrencyTests;

/// <summary>Shared plumbing for two-thread SPSC verification runs.</summary>
internal static class TransferRunner
{
    /// <summary>
    /// Minimal view over either buffer implementation. The tests only need
    /// write/read, and the two types deliberately share no interface (the
    /// shared-memory one is role-typed), so each is adapted here.
    /// </summary>
    internal interface ITransferRing
    {
        int MaxPayloadSize { get; }

        bool TryWrite(int type, ReadOnlySpan<byte> payload);

        bool TryRead(Span<byte> destination, out int bytesRead, out int type);
    }

    internal sealed record Result(long Produced, long Consumed, TimeSpan Elapsed)
    {
        public double MessagesPerSecond => Elapsed.TotalSeconds > 0 ? Consumed / Elapsed.TotalSeconds : 0;
    }

    /// <summary>
    /// Runs one producer thread and one consumer thread against the same buffer.
    /// The producer writes <paramref name="total"/> messages; each message is
    /// <c>[sequence:int64][sequence ^ checksum][fill]</c>. The consumer validates
    /// ordering, length and contents, so losses, duplicates, reorders and
    /// corruption all fail the run.
    /// </summary>
    public static Result Run(ITransferRing producerBuffer, ITransferRing consumerBuffer, long total, int payloadSize, TimeSpan timeout)
    {
        Exception? producerError = null;
        Exception? consumerError = null;
        long produced = 0;
        long consumed = 0;
        Stopwatch stopwatch = new();

        Thread producer = new(() =>
        {
            try
            {
                byte[] payload = new byte[payloadSize];
                payload.AsSpan(16).Fill(0x5A);
                SpinWait spin = new();

                for (long sequence = 0; sequence < total; sequence++)
                {
                    BinaryPrimitives.WriteInt64LittleEndian(payload, sequence);
                    BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(8), sequence ^ 0x5A5A5A5A5A5A5A5A);
                    while (!producerBuffer.TryWrite(1, payload))
                    {
                        spin.SpinOnce();
                    }

                    spin.Reset();
                    produced = sequence + 1;
                }
            }
            catch (Exception exception)
            {
                producerError = exception;
            }
        });

        Thread consumer = new(() =>
        {
            try
            {
                byte[] destination = new byte[consumerBuffer.MaxPayloadSize];
                SpinWait spin = new();
                long expected = 0;

                while (expected < total)
                {
                    if (!consumerBuffer.TryRead(destination, out int bytesRead, out int type))
                    {
                        spin.SpinOnce();
                        continue;
                    }

                    spin.Reset();
                    if (bytesRead != payloadSize)
                    {
                        throw new InvalidOperationException(
                            $"Message {expected}: expected {payloadSize} bytes, got {bytesRead}.");
                    }

                    long sequence = BinaryPrimitives.ReadInt64LittleEndian(destination);
                    if (sequence != expected)
                    {
                        throw new InvalidOperationException(
                            $"Message order violation: expected {expected}, got {sequence}.");
                    }

                    long checksum = BinaryPrimitives.ReadInt64LittleEndian(destination.AsSpan(8));
                    if (checksum != (sequence ^ 0x5A5A5A5A5A5A5A5A))
                    {
                        throw new InvalidOperationException($"Message {sequence}: checksum mismatch.");
                    }

                    if (type != 1)
                    {
                        throw new InvalidOperationException($"Message {sequence}: type {type}, expected 1.");
                    }

                    for (int i = 16; i < bytesRead; i++)
                    {
                        if (destination[i] != 0x5A)
                        {
                            throw new InvalidOperationException($"Message {sequence}: payload byte {i} corrupted.");
                        }
                    }

                    expected++;
                    consumed = expected;
                }
            }
            catch (Exception exception)
            {
                consumerError = exception;
            }
        });

        producer.IsBackground = true;
        consumer.IsBackground = true;

        stopwatch.Start();
        producer.Start();
        consumer.Start();

        bool producerJoined = producer.Join(Remaining(timeout, stopwatch));
        bool consumerJoined = consumer.Join(Remaining(timeout, stopwatch));

        if (producerError is not null)
        {
            throw new Xunit.Sdk.XunitException($"Producer failed: {producerError}");
        }

        if (consumerError is not null)
        {
            throw new Xunit.Sdk.XunitException($"Consumer failed: {consumerError}");
        }

        if (!producerJoined || !consumerJoined)
        {
            throw new Xunit.Sdk.XunitException(
                $"Timed out after {timeout.TotalSeconds:F0}s: produced={produced}, consumed={consumed}.");
        }

        stopwatch.Stop();
        return new Result(produced, consumed, stopwatch.Elapsed);

        static TimeSpan Remaining(TimeSpan timeout, Stopwatch stopwatch)
        {
            TimeSpan remaining = timeout - stopwatch.Elapsed;
            return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
        }
    }
}

internal sealed class SpscTransferRing(SpscRingBuffer buffer) : TransferRunner.ITransferRing
{
    public int MaxPayloadSize => buffer.MaxPayloadSize;

    public bool TryWrite(int type, ReadOnlySpan<byte> payload) => buffer.TryWrite(type, payload);

    public bool TryRead(Span<byte> destination, out int bytesRead, out int type) =>
        buffer.TryRead(destination, out bytesRead, out type);
}

internal sealed class SharedTransferRing(SharedRingBuffer buffer) : TransferRunner.ITransferRing
{
    public int MaxPayloadSize => buffer.MaxPayloadSize;

    public bool TryWrite(int type, ReadOnlySpan<byte> payload) => buffer.TryWrite(type, payload);

    public bool TryRead(Span<byte> destination, out int bytesRead, out int type) =>
        buffer.TryRead(destination, out bytesRead, out type);
}
