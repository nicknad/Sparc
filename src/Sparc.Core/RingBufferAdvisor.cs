namespace Sparc.Core;

/// <summary>
/// Sizing guidance for the ring: derives the effective slot size, the region
/// footprint (<c>192 + capacity × slotSize</c>) and a cache-residency note, so
/// callers do not need <c>docs/performance-invariants.md</c> (C6) open to pick
/// a geometry. Heuristics only — the thresholds describe a typical desktop LLC,
/// not this machine's caches.
/// </summary>
public static class RingBufferAdvisor
{
    /// <summary>At or below this footprint the region fits in L2/L3.</summary>
    public const long L2ResidentBytes = 1L * 1024 * 1024;

    /// <summary>At or below this footprint the region fits in a typical LLC.</summary>
    public const long LlcResidentBytes = 4L * 1024 * 1024;

    /// <summary>Above this footprint the region exceeds a typical LLC (DRAM + write-allocate per line).</summary>
    public const long OversizeBytes = 16L * 1024 * 1024;

    /// <summary>
    /// Effective slot size for a payload: an explicit override wins, otherwise
    /// <c>max(256, round(payload + 8) to a 64-byte line)</c> — the same policy the
    /// producer CLI and the benchmark pumps use.
    /// </summary>
    public static int SlotSizeFor(int payloadSize, int slotSizeOverride = 0)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(payloadSize, 0);
        ArgumentOutOfRangeException.ThrowIfLessThan(slotSizeOverride, 0);
        if (slotSizeOverride > 0)
        {
            return slotSizeOverride;
        }

        return Math.Max(
            RingBufferLayout.DefaultSlotSize,
            RingBufferLayout.RoundSlotSizeToCacheLine(payloadSize + RingBufferLayout.MessageHeaderSize));
    }

    /// <summary>
    /// Computes the geometry and footprint for a payload, throwing
    /// <see cref="ArgumentOutOfRangeException"/> when the payload does not fit
    /// or the geometry is invalid (same rules as
    /// <see cref="RingBufferLayout.ValidateGeometry"/>).
    /// </summary>
    /// <param name="payloadSize">Payload bytes per message.</param>
    /// <param name="capacity">Slot count; must be a power of two.</param>
    /// <param name="slotSizeOverride">Explicit slot size, or 0 to derive it.</param>
    /// <param name="targetGiBs">Optional target data rate; reported back as the required message rate.</param>
    public static RingBufferAdvice Advise(
        int payloadSize,
        int capacity = RingBufferLayout.DefaultCapacity,
        int slotSizeOverride = 0,
        double? targetGiBs = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(payloadSize, 0);
        if (targetGiBs is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(targetGiBs), targetGiBs, "Target rate must be positive.");
        }

        int slotSize = SlotSizeFor(payloadSize, slotSizeOverride);
        if (slotSize - RingBufferLayout.MessageHeaderSize < payloadSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(payloadSize), payloadSize,
                $"Payload of {payloadSize} bytes does not fit in slot size {slotSize} " +
                $"(maximum is {slotSize - RingBufferLayout.MessageHeaderSize}).");
        }

        RingBufferLayout.ValidateGeometry(capacity, slotSize);
        long regionBytes = RingBufferLayout.RequiredSize(capacity, slotSize);

        double? messagesPerSecond = null;
        if (targetGiBs.HasValue && payloadSize > 0)
        {
            messagesPerSecond = targetGiBs.Value * 1024 * 1024 * 1024 / payloadSize;
        }

        return new RingBufferAdvice(
            payloadSize,
            capacity,
            slotSize,
            regionBytes,
            ResidencyNote(regionBytes),
            AlignmentWarning(slotSize),
            messagesPerSecond);
    }

    /// <summary>
    /// Non-throwing geometry notes for UI/validation surfaces: power-of-two,
    /// header, cache-line alignment and footprint checks.
    /// </summary>
    public static IReadOnlyList<string> GetWarnings(int capacity, int slotSize)
    {
        List<string> warnings = new(3);
        if (!RingBufferLayout.IsPowerOfTwo(capacity))
        {
            warnings.Add($"Capacity {capacity} is not a positive power of two; slot indexing requires one.");
        }

        if (slotSize <= RingBufferLayout.MessageHeaderSize)
        {
            warnings.Add(
                $"Slot size {slotSize} must exceed the {RingBufferLayout.MessageHeaderSize}-byte message header.");
            return warnings;
        }

        string? alignment = AlignmentWarning(slotSize);
        if (alignment is not null)
        {
            warnings.Add(alignment);
        }

        try
        {
            long regionBytes = RingBufferLayout.RequiredSize(capacity, slotSize);
            if (regionBytes > OversizeBytes)
            {
                warnings.Add(
                    $"Region is {regionBytes / (1024.0 * 1024.0):F1} MiB: it exceeds a typical LLC, so every " +
                    "slot line pays DRAM + read-for-ownership. Shrink --capacity, or pass --no-verify-payload.");
            }
            else if (regionBytes > LlcResidentBytes)
            {
                warnings.Add(
                    $"Region is {regionBytes / (1024.0 * 1024.0):F1} MiB: it exceeds a typical 4 MiB LLC slice. " +
                    "Compare footprints with the Region column before blaming payload size.");
            }
        }
        catch (OverflowException)
        {
            warnings.Add("Region size overflows: capacity × slotSize does not fit in memory.");
        }

        return warnings;
    }

    internal static string ResidencyNote(long regionBytes)
    {
        if (regionBytes <= L2ResidentBytes)
        {
            return "fits in L2/L3: expect cache-resident streaming.";
        }

        if (regionBytes <= LlcResidentBytes)
        {
            return "fits in a typical LLC: per-byte cost stays linear.";
        }

        if (regionBytes <= OversizeBytes)
        {
            return "exceeds a typical LLC: expect DRAM + write-allocate cost per slot line.";
        }

        return "large: every slot line pays DRAM + read-for-ownership; keep payload verification off unless needed.";
    }

    private static string? AlignmentWarning(int slotSize) =>
        slotSize % RingBufferLayout.CacheLineSize != 0
            ? $"Slot size {slotSize} is not a multiple of {RingBufferLayout.CacheLineSize}: adjacent slots share a cache line (see C5)."
            : null;
}

/// <summary>One sizing answer from <see cref="RingBufferAdvisor.Advise"/>.</summary>
public sealed class RingBufferAdvice
{
    public RingBufferAdvice(
        int payloadSize,
        int capacity,
        int slotSize,
        long regionBytes,
        string residency,
        string? alignmentWarning,
        double? messagesPerSecondAtTarget)
    {
        PayloadSize = payloadSize;
        Capacity = capacity;
        SlotSize = slotSize;
        RegionBytes = regionBytes;
        Residency = residency;
        AlignmentWarning = alignmentWarning;
        MessagesPerSecondAtTarget = messagesPerSecondAtTarget;
    }

    /// <summary>Payload bytes per message the advice was computed for.</summary>
    public int PayloadSize { get; }

    /// <summary>Slot count.</summary>
    public int Capacity { get; }

    /// <summary>Effective bytes per slot.</summary>
    public int SlotSize { get; }

    /// <summary>Total region bytes (<c>192 + capacity × slotSize</c>).</summary>
    public long RegionBytes { get; }

    /// <summary>Region footprint in MiB.</summary>
    public double RegionMiB => RegionBytes / (1024.0 * 1024.0);

    /// <summary>Cache-residency heuristic for <see cref="RegionBytes"/>.</summary>
    public string Residency { get; }

    /// <summary>Non-null when the slot size is not cache-line aligned.</summary>
    public string? AlignmentWarning { get; }

    /// <summary>Message rate needed to hit the requested GiB/s target, if any.</summary>
    public double? MessagesPerSecondAtTarget { get; }
}
