using System.Globalization;

namespace Sparc.Cli;

/// <summary>Thrown when command line arguments are invalid.</summary>
internal sealed class UsageException(string message) : Exception(message);

/// <summary>
/// Minimal argument reader shared by the sample applications. Supports
/// <c>--name value</c> and <c>--name=value</c> forms.
/// </summary>
internal ref struct ArgumentReader(string[] args)
{
    private readonly string[] _args = args;
    private int _index = -1;

    public bool MoveNext(out string name, out string? inlineValue)
    {
        if (++_index >= _args.Length)
        {
            name = string.Empty;
            inlineValue = null;
            return false;
        }

        string argument = _args[_index];
        int equals = argument.IndexOf('=');
        if (equals > 0)
        {
            name = argument[..equals];
            inlineValue = argument[(equals + 1)..];
        }
        else
        {
            name = argument;
            inlineValue = null;
        }

        return true;
    }

    /// <summary>Returns the option value, consuming the next argument when needed.</summary>
    public string RequiredValue(string name, string? inlineValue)
    {
        if (inlineValue is not null)
        {
            return inlineValue;
        }

        if (_index + 1 >= _args.Length)
        {
            throw new UsageException($"Argument '{name}' requires a value.");
        }

        return _args[++_index];
    }

    public static long ParseLong(string text, string name, long min = long.MinValue, long max = long.MaxValue)
    {
        if (!long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long result) ||
            result < min ||
            result > max)
        {
            throw new UsageException($"Argument '{name}' has invalid value '{text}'.");
        }

        return result;
    }
}

/// <summary>Parses the shared <c>--format</c> switch of the sample CLIs.</summary>
internal static class OutputFormat
{
    public const string Text = "text";

    public const string Json = "json";

    public static string Parse(string value, string name)
    {
        if (string.Equals(value, Json, StringComparison.OrdinalIgnoreCase))
        {
            return Json;
        }

        if (string.Equals(value, Text, StringComparison.OrdinalIgnoreCase))
        {
            return Text;
        }

        throw new UsageException($"Argument '{name}' must be 'text' or 'json'.");
    }

    public static bool IsJson(string format) =>
        string.Equals(format, Json, StringComparison.OrdinalIgnoreCase);
}

/// <summary>Parses the shared <c>--security</c> switch of the sample CLIs.</summary>
internal static class SecurityFlag
{
    public static bool ParseCurrentUser(string value, string name)
    {
        if (!string.Equals(value, "current-user", StringComparison.OrdinalIgnoreCase))
        {
            throw new UsageException($"Argument '{name}' must be 'current-user'.");
        }

        if (!OperatingSystem.IsWindows())
        {
            throw new UsageException(
                $"Argument '{name}' is Windows-only (section DACLs); Unix regions are owner-only files.");
        }

        return true;
    }
}
