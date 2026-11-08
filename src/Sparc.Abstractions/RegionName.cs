namespace Sparc;

/// <summary>
/// Validates region names once for every transport, so callers get the same
/// actionable error instead of a platform-specific failure later.
/// </summary>
public static class RegionName
{
    /// <summary>
    /// Maximum region-name length. Keeps names usable as Windows object names and
    /// Unix file names (255-byte limit) with room for transport suffixes.
    /// </summary>
    public const int MaxLength = 128;

    /// <summary>
    /// Returns the name when it is usable as a cross-platform region name;
    /// throws <see cref="ArgumentException"/> otherwise.
    /// </summary>
    public static string Validate(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);

        if (name.Length > MaxLength)
        {
            throw new ArgumentException(
                $"Region name must be at most {MaxLength} characters (was {name.Length}).", nameof(name));
        }

        if (name.AsSpan().IndexOfAny('\\', '/', '\0') >= 0 || name is "." or "..")
        {
            throw new ArgumentException(
                $"Region name '{name}' must be a plain name without directory separators.", nameof(name));
        }

        foreach (char c in name)
        {
            if (char.IsControl(c))
            {
                throw new ArgumentException(
                    $"Region name must not contain control characters (U+{(int)c:X4}).", nameof(name));
            }
        }

        return name;
    }

    /// <summary>
    /// Generates a deployment-unique region name (<c>prefix + "-" + 32 hex chars</c>)
    /// so two deployments never share a region by accident and the name cannot be
    /// guessed for squatting. The result always passes <see cref="Validate"/>.
    /// </summary>
    public static string GenerateSecureName(string prefix = "sparc")
    {
        if (string.IsNullOrWhiteSpace(prefix))
        {
            prefix = "sparc";
        }

        string clean = Sanitize(prefix.Trim());
        const int suffixLength = 33;
        if (clean.Length + suffixLength > MaxLength)
        {
            clean = clean[..(MaxLength - suffixLength)];
        }

        return Validate($"{clean}-{Guid.NewGuid():N}");
    }

    private static string Sanitize(string prefix) =>
        string.Create(prefix.Length, prefix, static (span, value) =>
        {
            for (int i = 0; i < value.Length; i++)
            {
                char c = value[i];
                span[i] = (c is '\\' or '/' or '\0' || char.IsControl(c)) ? '-' : c;
            }
        });
}
