using Sparc.Core;

namespace Sparc.Cli;

internal sealed class CommonOptions
{
    public CommonOptions(long count = 0, int slotSize = 0)
    {
        Count = count;
        SlotSize = slotSize;
    }

    public string Name { get; private set; } = string.Empty;
    public long Count { get; private set; }
    public int Capacity { get; private set; } = RingBufferLayout.DefaultCapacity;
    public int SlotSize { get; private set; }
    public int Type { get; private set; } = 1;
    public TimeSpan OpenTimeout { get; private set; } = TimeSpan.FromSeconds(10);
    public TimeSpan Delay { get; private set; }
    public bool Takeover { get; private set; }
    public bool SpinOnly { get; private set; }
    public bool Notify { get; private set; }
    public bool RecreateStale { get; private set; }
    public bool RequireExisting { get; private set; }
    public bool SecurityCurrentUser { get; private set; }
    public string? SectionHandle { get; private set; }
    public bool Quiet { get; private set; }
    public bool ShowHelp { get; private set; }
    public string Format { get; private set; } = "text";

    public bool JsonFormat => OutputFormat.IsJson(Format);

    public bool TryParse(ref ArgumentReader reader, string arg, string? value)
    {
        switch (arg)
        {
            case "-h":
            case "--help":
                ShowHelp = true;
                return true;
            case "--name":
                Name = reader.RequiredValue(arg, value);
                return true;
            case "--count":
                Count = ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: 0);
                return true;
            case "--capacity":
                Capacity = (int)ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: 1, max: int.MaxValue);
                return true;
            case "--slot-size":
                SlotSize = (int)ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: 1, max: int.MaxValue);
                return true;
            case "--type":
                Type = (int)ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: int.MinValue, max: int.MaxValue);
                return true;
            case "--open-timeout":
                OpenTimeout = TimeSpan.FromMilliseconds(ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: 1));
                return true;
            case "--delay-us":
                Delay = TimeSpan.FromMicroseconds(ArgumentReader.ParseLong(reader.RequiredValue(arg, value), arg, min: 0));
                return true;
            case "--takeover":
                Takeover = true;
                return true;
            case "--security":
                SecurityCurrentUser = SecurityFlag.ParseCurrentUser(reader.RequiredValue(arg, value), arg);
                return true;
            case "--section-handle":
                SectionHandle = reader.RequiredValue(arg, value);
                return true;
            case "--spin-only":
                SpinOnly = true;
                return true;
            case "--notify":
                Notify = true;
                return true;
            case "--recreate-stale":
                RecreateStale = true;
                return true;
            case "--require-existing":
                RequireExisting = true;
                return true;
            case "--quiet":
                Quiet = true;
                return true;
            case "--format":
                Format = OutputFormat.Parse(reader.RequiredValue(arg, value), arg);
                return true;
            default:
                return false;
        }
    }

    public void Validate()
    {
        if (Name.Length == 0 && SectionHandle is null)
        {
            throw new UsageException("--name is required (or pass --section-handle).");
        }

        if (SectionHandle is not null && Notify)
        {
            throw new UsageException(
                "--notify needs a named region both endpoints can derive a signal name from; " +
                "it cannot be combined with --section-handle.");
        }

        if (SpinOnly && Notify)
        {
            throw new UsageException("--spin-only and --notify are mutually exclusive.");
        }
    }
}
