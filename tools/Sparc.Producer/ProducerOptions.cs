using Sparc.Cli;
using Sparc.Client;
using Sparc.Core;

namespace Sparc.Producer;

internal sealed class ProducerOptions
{
    public const int MessageHeaderBytes = RingBufferMessage.HeaderSize;

    public string Name { get; private set; } = string.Empty;
    public long Count { get; private set; } = 1_000_000;
    public int Size { get; private set; } = 64;
    public int Capacity { get; private set; } = RingBufferLayout.DefaultCapacity;
    public int SlotSize { get; private set; }
    public int Type { get; private set; } = 1;
    public TimeSpan OpenTimeout { get; private set; } = TimeSpan.FromSeconds(10);
    public TimeSpan FullTimeout { get; private set; } = TimeSpan.FromSeconds(30);
    public TimeSpan Delay { get; private set; }
    public bool Takeover { get; private set; }
    public bool SpinOnly { get; private set; }
    public bool Notify { get; private set; }
    public bool RecreateStale { get; private set; }
    public bool RequireExisting { get; private set; }
    public bool Quiet { get; private set; }
    public bool ShowHelp { get; private set; }

    public int EffectiveSlotSize => SlotSize > 0
        ? SlotSize
        : Math.Max(
            RingBufferLayout.DefaultSlotSize,
            RingBufferLayout.RoundSlotSizeToCacheLine(Size + RingBufferLayout.MessageHeaderSize));

    public const string Usage = """
        Sparc.Producer — writes fixed-size messages into a cross-process SPSC ring buffer.

        Usage:
          producer --name <buffer> [options]

        Required:
          --name <string>          Name of the shared memory region.

        Options:
          --count <n>              Messages to write; 0 = until Ctrl+C (default: 1000000).
          --size <bytes>           Payload bytes per message; must be >= 16 and <= slotSize-8 (default: 64).
                                   Layout: [sequence:int64][timestamp:int64][fill...].
          --capacity <slots>       Slot count; power of two (default: 1024).
          --slot-size <bytes>      Bytes per slot (default: max(256, size+8) rounded up to a
                                   64-byte cache line). The first creator wins.
          --type <int>             Message type tag (default: 1).
          --open-timeout <ms>      Wait for/lock the region (default: 10000).
          --full-timeout <ms>      Abort if the buffer stays full this long (default: 30000).
          --delay-us <us>          Pause between published messages (slow-producer simulation, default: 0).
          --takeover               Claim the producer role from a crashed peer.
          --spin-only              Busy-spin instead of sleeping while the buffer is full.
          --notify                 Block on an OS signal while full (needs a matching
                                   --notify consumer). Near-spin latency, no busy core.
          --recreate-stale         Delete and recreate an incompatible/stale region (destructive).
          --require-existing       Never create the region; fail if it does not exist.
          --quiet                  Suppress progress output.
          -h, --help               Show this help.

        Exit codes:
          0 success, 2 timeout, 3 consumer vanished, 4 role conflict,
          5 incompatible version/geometry, 6 corrupt region, 64 usage error.
        """;

    public static ProducerOptions Parse(string[] args)
    {
        ProducerOptions options = new();
        ArgumentReader reader = new(args);

        while (reader.MoveNext(out string arg, out string? value))
        {
            switch (arg)
            {
                case "-h":
                case "--help":
                    options.ShowHelp = true;
                    return options;
                case "--name":
                    options.Name = reader.RequiredValue(arg, value);
                    break;
                case "--count":
                    options.Count = ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: 0);
                    break;
                case "--size":
                    options.Size = (int)ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: MessageHeaderBytes, max: int.MaxValue);
                    break;
                case "--capacity":
                    options.Capacity = (int)ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: 1, max: int.MaxValue);
                    break;
                case "--slot-size":
                    options.SlotSize = (int)ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: 1, max: int.MaxValue);
                    break;
                case "--type":
                    options.Type = (int)ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: int.MinValue, max: int.MaxValue);
                    break;
                case "--open-timeout":
                    options.OpenTimeout = TimeSpan.FromMilliseconds(ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: 1));
                    break;
                case "--full-timeout":
                    options.FullTimeout = TimeSpan.FromMilliseconds(ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: 1));
                    break;
                case "--delay-us":
                    options.Delay = TimeSpan.FromMicroseconds(ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: 0));
                    break;
                case "--takeover":
                    options.Takeover = true;
                    break;
                case "--spin-only":
                    options.SpinOnly = true;
                    break;
                case "--notify":
                    options.Notify = true;
                    break;
                case "--recreate-stale":
                    options.RecreateStale = true;
                    break;
                case "--require-existing":
                    options.RequireExisting = true;
                    break;
                case "--quiet":
                    options.Quiet = true;
                    break;
                default:
                    throw new UsageException($"Unknown argument '{arg}'.");
            }
        }

        if (options.Name.Length == 0)
        {
            throw new UsageException("--name is required.");
        }

        if (options.SpinOnly && options.Notify)
        {
            throw new UsageException("--spin-only and --notify are mutually exclusive.");
        }

        RingBufferLayout.ValidateGeometry(options.Capacity, options.EffectiveSlotSize);
        if (options.EffectiveSlotSize - RingBufferLayout.MessageHeaderSize < options.Size)
        {
            throw new UsageException(
                $"Slot size {options.EffectiveSlotSize} cannot hold a {options.Size}-byte payload " +
                $"(maximum is {options.EffectiveSlotSize - RingBufferLayout.MessageHeaderSize}).");
        }

        return options;
    }
}
