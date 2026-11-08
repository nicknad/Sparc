using Sparc.Cli;
using Sparc.Core;

namespace Sparc.Consumer;

internal sealed class ConsumerOptions
{
    public CommonOptions Common { get; } = new(slotSize: RingBufferLayout.DefaultSlotSize);

    public string Name => Common.Name;
    public long Count => Common.Count;
    public int Capacity => Common.Capacity;
    public int SlotSize => Common.SlotSize;
    public int Type => Common.Type;
    public TimeSpan OpenTimeout => Common.OpenTimeout;
    public TimeSpan Delay => Common.Delay;
    public bool Takeover => Common.Takeover;
    public bool SpinOnly => Common.SpinOnly;
    public bool Notify => Common.Notify;
    public bool RecreateStale => Common.RecreateStale;
    public bool RequireExisting => Common.RequireExisting;
    public bool SecurityCurrentUser => Common.SecurityCurrentUser;
    public string? SectionHandle => Common.SectionHandle;
    public bool Quiet => Common.Quiet;
    public bool ShowHelp => Common.ShowHelp;
    public string Format => Common.Format;

    public bool JsonFormat => Common.JsonFormat;

    public TimeSpan IdleTimeout { get; private set; } = TimeSpan.FromSeconds(5);
    public bool Verify { get; private set; } = true;
    public bool VerifyPayload { get; private set; } = true;

    public const string Usage = """
        Sparc.Consumer — reads fixed-size messages from a cross-process SPSC ring buffer.

        Usage:
          consumer --name <buffer> [options]
          consumer --section-handle <h> [options]     (Windows capability mode)

        Required:
          --name <string>          Name of the shared memory region. Not used with --section-handle.

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
          --security current-user  Windows only: create the section with a DACL that grants
                                   read/write access to the current user only (no Everyone).
          --section-handle <h>     Windows only: join an unnamed section by transferred HANDLE
                                   instead of a name (capability mode); '-' reads the handle
                                   value from stdin. --notify is not supported in this mode.
          --recreate-stale         Delete and recreate an incompatible/stale region (destructive).
          --require-existing       Never create the region; fail if it does not exist.
          --no-verify              Do not validate sequence numbers and type.
          --no-verify-payload      Keep sequence/type checks, skip the payload fill scan.
          --quiet                  Suppress progress output.
          --format <text|json>     Output format (default: text). json prints one machine-readable
                                   object for the harness instead of the human summary.
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
            if (options.Common.TryParse(ref reader, arg, value))
            {
                if (options.ShowHelp)
                {
                    return options;
                }

                continue;
            }

            switch (arg)
            {
                case "--idle-timeout":
                    options.IdleTimeout = TimeSpan.FromMilliseconds(ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: 1));
                    break;
                case "--no-verify":
                    options.Verify = false;
                    break;
                case "--no-verify-payload":
                    options.VerifyPayload = false;
                    break;
                default:
                    throw new UsageException($"Unknown argument '{arg}'.");
            }
        }

        options.Common.Validate();
        RingBufferLayout.ValidateGeometry(options.Capacity, options.SlotSize);
        return options;
    }
}
