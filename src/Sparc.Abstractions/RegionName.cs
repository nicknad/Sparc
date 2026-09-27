namespace Sparc;

/// <summary>
/// Validates region names once for every transport, so callers get the same
/// actionable error instead of a platform-specific failure later.
/// </summary>
public static class RegionName
{
    /// <summary>
    /// Returns the name when it is usable as a cross-platform region name;
    /// throws <see cref="ArgumentException"/> otherwise.
    /// </summary>
    public static string Validate(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);

        if (name.AsSpan().IndexOfAny('\\', '/', '\0') >= 0 || name is "." or "..")
        {
            throw new ArgumentException(
                $"Region name '{name}' must be a plain name without directory separators.", nameof(name));
        }

        return name;
    }
}
