using System.Security.AccessControl;
using System.Security.Principal;
using System.Text;

namespace Sparc.WindowsMemoryMapped;

/// <summary>
/// Windows transport security: the DACL applied to the section object created
/// for a region. It controls which Windows identities may open and map the
/// shared memory; it is consulted by the OS when the region is created or
/// opened, never per message.
/// </summary>
/// <remarks>
/// <para>
/// The convenience factories build a protected DACL (no inherited ACEs) that
/// grants only the requested identities the section rights needed to map the
/// region read/write, and nothing to everyone else. There is no implicit
/// <c>Everyone</c>, <c>Authenticated Users</c> or group access.
/// </para>
/// <para>
/// A DACL is transport access control only. An identity that is granted access
/// can read and modify every message, and SPARC does not authenticate the
/// peer. Layer authenticated or encrypted payloads above the transport when
/// peers are not equally trusted.
/// </para>
/// </remarks>
public sealed class WindowsSectionSecurity : IpcMemoryRegionSecurity
{
    // SECTION_QUERY | SECTION_MAP_WRITE | SECTION_MAP_READ. Granting the
    // concrete section rights keeps the ACE minimal; generic read/write would
    // also hand out STANDARD_RIGHTS_WRITE (READ_CONTROL) on the section.
    private const string SectionReadWriteRightsSddl = "0x7";

    // Self-relative form of the descriptor, pinned only for the duration of
    // CreateFileMapping. Caching it here avoids re-parsing the SDDL and a
    // LocalAlloc/LocalFree round trip on every region creation.
    private readonly byte[] _descriptor;

    private WindowsSectionSecurity(RawSecurityDescriptor descriptor)
    {
#pragma warning disable CA1416 // Windows-only API; this class lives in the Windows transport package.
        _descriptor = new byte[descriptor.BinaryLength];
        descriptor.GetBinaryForm(_descriptor, 0);
        Sddl = descriptor.GetSddlForm(AccessControlSections.Access);
#pragma warning restore CA1416
    }

    /// <summary>
    /// A DACL that grants read/write section access to the process's current
    /// Windows user and to no one else.
    /// </summary>
    public static WindowsSectionSecurity CurrentUserOnly =>
        ForSids(GetCurrentUserSid());

    /// <summary>The canonical SDDL form of the descriptor (access section only).</summary>
    public string Sddl { get; }

    /// <summary>
    /// A DACL that grants read/write section access to exactly the given
    /// security identifiers.
    /// </summary>
    /// <exception cref="ArgumentException">No identifiers were supplied.</exception>
    public static WindowsSectionSecurity ForSids(params SecurityIdentifier[] securityIdentifiers)
    {
        ArgumentNullException.ThrowIfNull(securityIdentifiers);
        if (securityIdentifiers.Length == 0)
        {
            throw new ArgumentException("At least one identity must be granted access.", nameof(securityIdentifiers));
        }

        StringBuilder builder = new("D:P");
#pragma warning disable CA1416 // Windows-only API; this class lives in the Windows transport package.
        foreach (SecurityIdentifier identifier in securityIdentifiers)
        {
            if (identifier is null)
            {
                throw new ArgumentException(
                    "The identity list must not contain a null entry.", nameof(securityIdentifiers));
            }

            builder.Append("(A;;").Append(SectionReadWriteRightsSddl).Append(";;;")
                .Append(identifier.Value).Append(')');
        }
#pragma warning restore CA1416

        return Parse(builder.ToString());
    }

    /// <summary>
    /// A DACL that grants read/write section access to exactly the given
    /// Windows accounts, for example <c>@"CONTOSO\svc-orders"</c> or
    /// <c>@"NT AUTHORITY\SYSTEM"</c>.
    /// </summary>
    /// <exception cref="ArgumentException">No accounts were supplied.</exception>
    /// <exception cref="System.Security.Principal.IdentityNotMappedException">An account could not be resolved.</exception>
    public static WindowsSectionSecurity ForAccountNames(params string[] accountNames)
    {
        ArgumentNullException.ThrowIfNull(accountNames);
        if (accountNames.Length == 0)
        {
            throw new ArgumentException("At least one account must be granted access.", nameof(accountNames));
        }

        SecurityIdentifier[] identifiers = new SecurityIdentifier[accountNames.Length];
        for (int i = 0; i < accountNames.Length; i++)
        {
            string accountName = accountNames[i];
            if (string.IsNullOrWhiteSpace(accountName))
            {
                throw new ArgumentException(
                    "The account list must not contain an empty entry.", nameof(accountNames));
            }

#pragma warning disable CA1416 // Windows-only API; this class lives in the Windows transport package.
            identifiers[i] = (SecurityIdentifier)new NTAccount(accountName).Translate(typeof(SecurityIdentifier));
#pragma warning restore CA1416
        }

        return ForSids(identifiers);
    }

    /// <summary>
    /// Uses an explicit SDDL security descriptor. The descriptor must contain a
    /// non-null DACL: a null DACL (for example SDDL without a <c>D:</c>
    /// component, or <c>D:NO_ACCESS_CONTROL</c>) grants everyone full access
    /// and is rejected.
    /// </summary>
    /// <exception cref="ArgumentException">The SDDL is invalid or has no DACL.</exception>
    public static WindowsSectionSecurity FromSddl(string sddl)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sddl);
        return Parse(sddl);
    }

    /// <summary>Self-relative descriptor bytes; the caller pins them for the native call only.</summary>
    internal ReadOnlySpan<byte> DescriptorBytes => _descriptor;

    private static SecurityIdentifier GetCurrentUserSid()
    {
#pragma warning disable CA1416 // Windows-only API; this class lives in the Windows transport package.
        SecurityIdentifier? sid = WindowsIdentity.GetCurrent().User;
#pragma warning restore CA1416
        return sid ?? throw new InvalidOperationException("The current Windows token has no user SID.");
    }

    private static WindowsSectionSecurity Parse(string sddl)
    {
#pragma warning disable CA1416 // Windows-only API; this class lives in the Windows transport package.
        RawSecurityDescriptor descriptor = new(sddl);
        if (descriptor.DiscretionaryAcl is null)
        {
            throw new ArgumentException(
                "The security descriptor must contain a non-null DACL; a null DACL grants everyone full access.",
                nameof(sddl));
        }

        return new WindowsSectionSecurity(descriptor);
#pragma warning restore CA1416
    }
}
