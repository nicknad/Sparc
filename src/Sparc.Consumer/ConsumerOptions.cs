using Sparc.Cli;
using Sparc.Client;
using Sparc.Core;

namespace Sparc.Consumer;

internal sealed class ConsumerOptions
{
    public const int MessageHeaderBytes = RingBufferMessage.HeaderSize;

    public string Name { get; private set; } = string.Empty;
    public long Count { get; private set; } // 0 = consume until the producer stops
    public int Capacity { get; private set; } = RingBufferLayout.DefaultCapacity;
    public int SlotSize { get; private set; } = RingBufferLayout.DefaultSlotSize;
    public int Type { get; private set; } = 1;
    public TimeSpan OpenTimeout { get; private set; } = TimeSpan.FromSeconds(10);
    public TimeSpan IdleTimeout { get; private set; } = TimeSpan.FromSeconds(5);
    public TimeSpan Delay { get; private set; }
    public bool Takeover { get; private set; }
    public bool SpinOnly { get; private set; }
    public bool Notify { get; private set; }
    public bool RecreateStale { get; private set; }
    public bool RequireExisting { get; private set; }
    public bool Verify { get; private set; } = true;
    public bool VerifyPayload { get; private set; } = true;
    public bool Quiet { get; private set; }
    public bool ShowHelp { get; private set; }

    public const string Usage = """
        Sparc.Consumer — reads fixed-size messages from a cross-process SPSC ring buffer.

        Usage:
          consumer --name <buffer> [options]

        Required:
          --name <string>          Name of the shared memory region.

        Options:
          --count <n>              Stop after n messages (default: 0 = until the producer stops).
          --capacity <slots>       Slot count when this process creates the region (default: 1024).
          --slot-size <bytes>      Slot size when this process creates the region (default: 256).
                                   When the region already exists its geometry is adopted.
          --type <int>             Expected message type tag (default: 1).
          --open-timeout <ms>      Wait for the region to appear (default: 10000).
          --idle-timeout <ms>      Abort when no messages arrive for this long (default: 5000).
          --delay-us <us>          Pause between consumed messages (slow-consumer/backpressure simulation, default: 0).
          --takeover               Claim the consumer role from a crashed peer.
          --spin-only              Busy-spin instead of sleeping while the buffer is empty.
          --notify                 Block on an OS signal while empty (needs a matching
                                   --notify producer). Near-spin latency, no busy core.
          --recreate-stale         Delete and recreate an incompatible/stale region (destructive).
          --require-existing       Never create the region; fail if it does not exist.
          --no-verify              Do not validate sequence numbers and type.
          --no-verify-payload      Keep sequence/type checks, skip the payload fill scan.
          --quiet                  Suppress progress output.
          -h, --help               Show this help.

        Exit codes:
          0 success, 2 timeout, 3 producer vanished/incomplete, 4 role conflict,
          5 incompatible version/geometry, 6 corrupt region, 7 verification failure, 64 usage error.
        """;

    public static ConsumerOptions Parse(string[] args)
    {
        ConsumerOptions options = new();
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
                case "--idle-timeout":
                    options.IdleTimeout = TimeSpan.FromMilliseconds(ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: 1));
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
                case "--no-verify":
                    options.Verify = false;
                    break;
                case "--no-verify-payload":
                    options.VerifyPayload = false;
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

        RingBufferLayout.ValidateGeometry(options.Capacity, options.SlotSize);
        return options;
    }
}
