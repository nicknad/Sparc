namespace Sparc.Hosting;

/// <summary>Transport-level options for <c>AddSparcIpc</c>.</summary>
public sealed class SparcIpcOptions
{
    /// <summary>
    /// Directory for file-backed regions on Unix-like systems; when null or
    /// empty, the transport's default (<c>&lt;temp&gt;/sparc</c>) is used.
    /// </summary>
    public string? UnixDirectory { get; set; }
}
