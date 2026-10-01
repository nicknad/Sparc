using System.Buffers.Binary;
using System.Security.AccessControl;
using System.Security.Principal;
using Microsoft.Win32.SafeHandles;
using Sparc.Core;
using Sparc.WindowsMemoryMapped;

namespace Sparc.UnitTests;

public class WindowsSectionSecurityTests
{
    private static string NewName() => "spsc-sec-" + Guid.NewGuid().ToString("N");

    private static bool Supported => OperatingSystem.IsWindows();

    private static string RandomNonTokenSid() =>
        $"S-1-5-21-{Random.Shared.Next(1, int.MaxValue)}-{Random.Shared.Next(1, int.MaxValue)}-" +
        $"{Random.Shared.Next(1, int.MaxValue)}-{Random.Shared.Next(1, int.MaxValue)}";

    private static string CurrentUserSid()
    {
        Assert.True(Supported);
#pragma warning disable CA1416 // Windows-only API; the test is guarded by Supported.
        return WindowsIdentity.GetCurrent().User!.Value;
#pragma warning restore CA1416
    }

    // Reads back the DACL as identities. Asserting on the parsed access
    // control entries rather than raw SDDL text keeps the tests correct when
    // the SDDL canonicalizer replaces a SID with its alias (the built-in
    // Administrator becomes LA, SYSTEM becomes SY, and so on).
    private static (bool IsProtected, IReadOnlyList<(AceType Type, string Sid, int Mask)> Aces) ParseDacl(string sddl)
    {
#pragma warning disable CA1416 // Windows-only API; the test is guarded by Supported.
        RawSecurityDescriptor descriptor = new(sddl);
        RawAcl dacl = descriptor.DiscretionaryAcl!;
        List<(AceType Type, string Sid, int Mask)> aces = new(dacl.Count);
        foreach (GenericAce entry in dacl)
        {
            KnownAce ace = Assert.IsAssignableFrom<KnownAce>(entry);
            aces.Add((ace.AceType, ace.SecurityIdentifier.Value, ace.AccessMask));
        }

        bool isProtected = (descriptor.ControlFlags & ControlFlags.DiscretionaryAclProtected) != ControlFlags.None;
#pragma warning restore CA1416
        return (isProtected, aces);
    }

    [Fact]
    public void CurrentUserOnlyBuildsAProtectedDaclForTheCurrentUser()
    {
        if (!Supported)
        {
            return;
        }

#pragma warning disable CA1416 // Windows-only API; the test is guarded by Supported.
        string sddl = WindowsSectionSecurity.CurrentUserOnly.Sddl;

        Assert.StartsWith("D:P", sddl, StringComparison.Ordinal);

        (bool isProtected, IReadOnlyList<(AceType Type, string Sid, int Mask)> aces) = ParseDacl(sddl);
        Assert.True(isProtected);

        // A single allow entry for exactly the current user, with only the
        // section rights needed to map the region: no Everyone, Users,
        // Authenticated Users, or other group access.
        (AceType type, string sid, int mask) = Assert.Single(aces);
        Assert.Equal(AceType.AccessAllowed, type);
        Assert.Equal(CurrentUserSid(), sid);
        Assert.Equal(0x7, mask);
#pragma warning restore CA1416
    }

    [Fact]
    public void ForSidsRejectsAnEmptyList()
    {
        if (!Supported)
        {
            return;
        }

        Assert.Throws<ArgumentException>(() => WindowsSectionSecurity.ForSids());
    }

    [Fact]
    public void ForAccountNamesResolvesTheCurrentAccount()
    {
        if (!Supported)
        {
            return;
        }

#pragma warning disable CA1416 // Windows-only API; the test is guarded by Supported.
        string accountName = WindowsIdentity.GetCurrent().Name;
#pragma warning restore CA1416
        string sddl = WindowsSectionSecurity.ForAccountNames(accountName).Sddl;

#pragma warning disable CA1416 // Windows-only API; the test is guarded by Supported.
        (_, IReadOnlyList<(AceType Type, string Sid, int Mask)> aces) = ParseDacl(sddl);
        (_, string sid, _) = Assert.Single(aces);
        Assert.Equal(CurrentUserSid(), sid);
#pragma warning restore CA1416
    }

    [Fact]
    public void FromSddlRejectsInvalidSyntax()
    {
        if (!Supported)
        {
            return;
        }

        Assert.Throws<ArgumentException>(() => WindowsSectionSecurity.FromSddl("not an sddl"));
    }

    [Fact]
    public void FromSddlRejectsADescriptorWithoutDacl()
    {
        if (!Supported)
        {
            return;
        }

        // Owner/group only: a null DACL would grant everyone full access.
        Assert.Throws<ArgumentException>(() => WindowsSectionSecurity.FromSddl("O:BAG:BA"));
    }

    [Fact]
    public void FromSddlRejectsAnExplicitNullDacl()
    {
        if (!Supported)
        {
            return;
        }

        Assert.Throws<ArgumentException>(() =>
            WindowsSectionSecurity.FromSddl("D:NO_ACCESS_CONTROL"));
    }

    [Fact]
    public unsafe void AllowedUserCanCreateOpenAndMapTheSecuredRegion()
    {
        if (!Supported)
        {
            return;
        }

        string name = NewName();
        WindowsNamedMemoryMappedRegionFactory factory = new();

        using IIpcMemoryRegion creator = factory.CreateOrOpen(name, 4096, new IpcRegionOptions
        {
            Security = WindowsSectionSecurity.CurrentUserOnly,
        });

        // Joining does not need the descriptor: the creator's DACL governs.
        using IIpcMemoryRegion joiner = factory.CreateOrOpen(name, 4096);

        Assert.True(creator.IsCreator);
        Assert.False(joiner.IsCreator);
        Assert.Equal(4096, creator.Size);

        *(long*)creator.Pointer = 0x1234;
        Assert.Equal(0x1234, *(long*)joiner.Pointer);
    }

    [Fact]
    public void IdentityOutsideTheDaclCannotOpenOrJoinTheRegion()
    {
        if (!Supported)
        {
            return;
        }

        string name = NewName();
        WindowsNamedMemoryMappedRegionFactory factory = new();
        WindowsSectionSecurity security = WindowsSectionSecurity.FromSddl(
            $"D:P(A;;0x7;;;{RandomNonTokenSid()})");

        // The creator receives its handle from CreateFileMapping regardless of
        // the DACL, so the region exists and is mapped by this process.
        using IIpcMemoryRegion creator = factory.CreateOrOpen(name, 4096, new IpcRegionOptions
        {
            Security = security,
        });
        Assert.True(creator.IsCreator);

        // Same process, different access check: the token does not match the
        // only ACE, so every open path fails at region establishment.
        IpcRegionOptions openOptions = new() { OpenTimeout = TimeSpan.FromMilliseconds(200) };
        Assert.Throws<UnauthorizedAccessException>(
            () => factory.OpenExisting(name, openOptions));
        Assert.Throws<UnauthorizedAccessException>(
            () => factory.CreateOrOpen(name, 4096, openOptions));
        Assert.Throws<UnauthorizedAccessException>(
            () => factory.CreateOrOpen(name, 4096, new IpcRegionOptions
            {
                Security = security,
                OpenTimeout = TimeSpan.FromMilliseconds(200),
            }));
    }

    [Fact]
    public void SecuredRingExchangesMessagesInOrderAndUntorn()
    {
        if (!Supported)
        {
            return;
        }

        const int capacity = 8;
        const int slotSize = 64;
        const int messageCount = 1_000;
        string name = NewName();
        WindowsNamedMemoryMappedRegionFactory factory = new();

        // The producer creates the region with the restrictive DACL.
        using IProducerEndpoint producer = SparcRing.OpenProducer(
            factory, name, capacity, slotSize, new SharedRingBufferOptions
            {
                Security = WindowsSectionSecurity.CurrentUserOnly,
            }, TestContext.Current.CancellationToken);

        // The consumer joins without the descriptor; it must pass the OS check.
        using IConsumerEndpoint consumer = SparcRing.OpenConsumer(
            factory, name, capacity, slotSize,
            options: null, TestContext.Current.CancellationToken);

        byte[] payload = new byte[32];
        byte[] destination = new byte[consumer.MaxPayloadSize];

        // Interleave writes and reads so the small ring wraps repeatedly:
        // ordering and torn-message checks still apply on every slot reuse.
        for (int sequence = 0; sequence < messageCount; sequence++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(payload, sequence);
            payload.AsSpan(sizeof(long)).Fill((byte)sequence);
            Assert.True(producer.TryPublish(7, payload));

            Assert.True(consumer.TryRead(destination, out int bytesRead, out int type));
            Assert.Equal(payload.Length, bytesRead);
            Assert.Equal(7, type);
            Assert.Equal(sequence, BinaryPrimitives.ReadInt64LittleEndian(destination));
            for (int i = sizeof(long); i < bytesRead; i++)
            {
                Assert.Equal((byte)sequence, destination[i]);
            }
        }

        Assert.False(consumer.TryRead(destination, out _, out _));
    }
}

public class WindowsUnnamedSectionTests
{
    private static bool Supported => OperatingSystem.IsWindows();

    private const int Capacity = 16;
    private const int SlotSize = 64;

    private static long RegionSize => RingBufferLayout.RequiredSize(Capacity, SlotSize);

    [Fact]
    public unsafe void CreatedSectionIsMappedAndShareableThroughTheHandle()
    {
        if (!Supported)
        {
            return;
        }

        using WindowsSectionCapability capability = WindowsUnnamedSection.Create(
            RegionSize, WindowsSectionSecurity.CurrentUserOnly);
        using IIpcMemoryRegion peer = WindowsUnnamedSection.MapHandle(capability.Handle);

        Assert.True(capability.Region.IsCreator);
        Assert.False(peer.IsCreator);
        Assert.Equal(WindowsUnnamedSection.UnnamedRegionName, peer.Name);
        Assert.False(capability.Handle.IsInvalid);

        *(long*)capability.Region.Pointer = 0x5A5A;
        Assert.Equal(0x5A5A, *(long*)peer.Pointer);
    }

    [Fact]
    public void UnnamedSectionCannotBeDiscoveredByItsLabel()
    {
        if (!Supported)
        {
            return;
        }

        using WindowsSectionCapability capability = WindowsUnnamedSection.Create(RegionSize);
        WindowsNamedMemoryMappedRegionFactory factory = new();

        // There is no name in the OS namespace: the region label is display-only.
        Assert.Throws<IpcTimeoutException>(() => factory.OpenExisting(
            WindowsUnnamedSection.UnnamedRegionName,
            new IpcRegionOptions { OpenTimeout = TimeSpan.FromMilliseconds(100) }));
    }

    [Fact]
    public void CapabilityRingExchangesMessagesOverTwoHandleMappings()
    {
        if (!Supported)
        {
            return;
        }

        using WindowsSectionCapability capability = WindowsUnnamedSection.Create(
            RegionSize, WindowsSectionSecurity.CurrentUserOnly);

        using IProducerEndpoint producer = SparcRing.OpenProducer(
            capability.Region, Capacity, SlotSize, options: null, CancellationToken.None);

        // The peer's mapping is produced from the transferred handle, exactly
        // as a receiving process would map a duplicated or inherited handle.
        using IIpcMemoryRegion peerRegion = WindowsUnnamedSection.MapHandle(capability.Handle);
        using IConsumerEndpoint consumer = SparcRing.OpenConsumer(
            peerRegion, Capacity, SlotSize, options: null, CancellationToken.None);

        byte[] payload = new byte[16];
        byte[] destination = new byte[consumer.MaxPayloadSize];
        const int messageCount = 500;

        for (int sequence = 0; sequence < messageCount; sequence++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(payload, sequence);
            payload.AsSpan(sizeof(long)).Fill((byte)sequence);
            Assert.True(producer.TryPublish(3, payload));

            Assert.True(consumer.TryRead(destination, out int bytesRead, out int type));
            Assert.Equal(payload.Length, bytesRead);
            Assert.Equal(3, type);
            Assert.Equal(sequence, BinaryPrimitives.ReadInt64LittleEndian(destination));
            for (int i = sizeof(long); i < bytesRead; i++)
            {
                Assert.Equal((byte)sequence, destination[i]);
            }
        }
    }

    [Fact]
    public void MapHandleRejectsAClosedHandle()
    {
        if (!Supported)
        {
            return;
        }

        SafeFileHandle handle = new(new IntPtr(1234), ownsHandle: true);
        handle.Dispose();
        Assert.Throws<ArgumentException>(() => WindowsUnnamedSection.MapHandle(handle));
    }

    [Fact]
    public void CreateRejectsANonPositiveSize()
    {
        if (!Supported)
        {
            return;
        }

        Assert.Throws<ArgumentOutOfRangeException>(() => WindowsUnnamedSection.Create(0));
    }

    [Fact]
    public void MapHandleRequiresHandleOwnershipToStayWithTheCaller()
    {
        if (!Supported)
        {
            return;
        }

        using WindowsSectionCapability capability = WindowsUnnamedSection.Create(RegionSize);
        IIpcMemoryRegion peer = WindowsUnnamedSection.MapHandle(capability.Handle);

        // Closing the received handle leaves the view valid; the caller keeps
        // ownership of whatever handle it was given.
        capability.Handle.Dispose();
        unsafe
        {
            *(long*)peer.Pointer = 42;
            Assert.Equal(42, *(long*)peer.Pointer);
        }

        peer.Dispose();
    }
}
