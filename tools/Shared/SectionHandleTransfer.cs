using System.Globalization;
using Microsoft.Win32.SafeHandles;

namespace Sparc.Cli;

/// <summary>
/// Resolves the <c>--section-handle</c> value of the sample CLIs. The value is
/// a numeric Windows HANDLE received through handle inheritance or
/// <c>DuplicateHandle</c>; <c>-</c> reads it from a line on stdin, which is how
/// the process tests transfer a duplicated handle into a child.
/// </summary>
internal static class SectionHandleTransfer
{
    public const string StdinSpec = "-";

    public static SafeFileHandle Acquire(string spec)
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new UsageException("--section-handle is Windows-only (unnamed sections).");
        }

        string value = spec;
        if (string.Equals(spec, StdinSpec, StringComparison.Ordinal))
        {
            string? line = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(line))
            {
                throw new UsageException("Expected the section handle value on stdin.");
            }

            value = line.Trim();
        }

        return FromValue(value);
    }

    private static SafeFileHandle FromValue(string value)
    {
        NumberStyles style = NumberStyles.Integer;
        ReadOnlySpan<char> digits = value.AsSpan();
        if (digits.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            style = NumberStyles.HexNumber;
            digits = digits[2..];
        }

        if (!ulong.TryParse(digits, style, CultureInfo.InvariantCulture, out ulong parsed) ||
            parsed is 0 or > long.MaxValue)
        {
            throw new UsageException($"Invalid section handle value '{value}'.");
        }

        return new SafeFileHandle(new IntPtr((long)parsed), ownsHandle: true);
    }
}
