using Sparc.Cli;
using Sparc.Client;
using Sparc.Core;

namespace Sparc.Producer;

internal sealed class ProducerOptions
{
    public const int MessageHeaderBytes = RingBufferMessage.HeaderSize;

    public CommonOptions Common { get; } = new(count: 1_000_000);

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

    public int Size { get; private set; } = 64;
    public TimeSpan FullTimeout { get; private set; } = TimeSpan.FromSeconds(30);
    public bool Advise { get; private set; }
    public double? TargetGiB { get; private set; }

    public int EffectiveSlotSize => RingBufferAdvisor.SlotSizeFor(Size, SlotSize);

    public const string Usage = """
        Sparc.Producer — writes fixed-size messages into a cross-process SPSC ring buffer.

        Usage:
          producer --name <buffer> [options]
          producer --section-handle <h> [options]     (Windows capability mode)

        Required:
          --name <string>          Name of the shared memory region. Not used with --section-handle.

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
          --security current-user  Windows only: create the section with a DACL that grants
                                   read/write access to the current user only (no Everyone).
          --section-handle <h>     Windows only: join an unnamed section by transferred HANDLE
                                   instead of a name (capability mode); '-' reads the handle
                                   value from stdin. --notify is not supported in this mode.
          --recreate-stale         Delete and recreate an incompatible/stale region (destructive).
          --require-existing       Never create the region; fail if it does not exist.
          --quiet                  Suppress progress output.
          --advise                 Print sizing guidance for --size/--capacity/--slot-size and exit
                                   (no --name needed). Add --target-gib <n> for the message rate
                                   needed to hit that data rate.
          --target-gib <n>         Target data rate in GiB/s; only meaningful with --advise.
          --format <text|json>     Output format (default: text). json prints one machine-readable
                                   object for the harness instead of the human summary.
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
                case "--size":
                    options.Size = (int)ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: MessageHeaderBytes, max: int.MaxValue);
                    break;
                case "--full-timeout":
                    options.FullTimeout = TimeSpan.FromMilliseconds(ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: 1));
                    break;
                case "--advise":
                    options.Advise = true;
                    break;
                case "--target-gib":
                    options.TargetGiB = ParsePositiveDouble(reader.RequiredValue(arg, value), arg);
                    break;
                default:
                    throw new UsageException($"Unknown argument '{arg}'.");
            }
        }

        if (options.TargetGiB.HasValue && !options.Advise)
        {
            throw new UsageException("--target-gib is only meaningful with --advise.");
        }

        if (options.Advise)
        {
            options.ValidateGeometry();
            return options;
        }

        options.Common.Validate();
        options.ValidateGeometry();

        return options;
    }

    private void ValidateGeometry()
    {
        RingBufferLayout.ValidateGeometry(Capacity, EffectiveSlotSize);
        if (EffectiveSlotSize - RingBufferLayout.MessageHeaderSize < Size)
        {
            throw new UsageException(
                $"Slot size {EffectiveSlotSize} cannot hold a {Size}-byte payload " +
                $"(maximum is {EffectiveSlotSize - RingBufferLayout.MessageHeaderSize}).");
        }
    }

    private static double ParsePositiveDouble(string text, string name)
    {
        if (!double.TryParse(
                text,
                System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture,
                out double result) || result <= 0)
        {
            throw new UsageException($"Argument '{name}' has invalid value '{text}'.");
        }

        return result;
    }
}
