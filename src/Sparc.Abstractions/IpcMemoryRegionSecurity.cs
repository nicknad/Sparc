namespace Sparc;

/// <summary>
/// Transport-level access control for a shared region, interpreted by the
/// operating-system-specific <see cref="IIpcMemoryRegionFactory"/>.
/// </summary>
/// <remarks>
/// <para>
/// SPARC itself never inspects this value and never consults it on the message
/// path. The factory hands it to the underlying OS primitive when the region is
/// <b>created</b> (for example, a Windows section object's DACL); opening an
/// existing region is then governed by the access control the creator applied.
/// </para>
/// <para>
/// This is transport access control only: it decides which identities may map
/// the region. It provides no message authentication and no confidentiality
/// against a peer that already has valid access. Layer authenticated or
/// encrypted payloads above the transport when the trust model requires it.
/// </para>
/// <para>
/// Implementations are OS-specific and live in the matching transport package;
/// the abstraction exists so <see cref="IpcRegionOptions"/> does not expose
/// Windows ACL types.
/// </para>
/// </remarks>
public abstract class IpcMemoryRegionSecurity
{
    /// <summary>Creates the base security configuration.</summary>
    protected IpcMemoryRegionSecurity()
    {
    }
}
