# Security policy

## Supported versions

Security fixes are applied to the latest released minor version. The 1.0 line
is prepared but not yet published; until it is, report against `main`.

| Version | Supported |
|---|---|
| 1.0.x | yes |
| < 1.0 | no |

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private
vulnerability reporting for this repository (Security tab -> "Report a
vulnerability") once the repository is hosted. If private reporting is not
available, contact the maintainer through their GitHub profile.

Please include:

* the affected component (ring buffer, session layer, transport, CLI),
* a minimal reproduction or a description of the message sequence that triggers
  the issue,
* the OS, .NET version and commit or package version.

Expect an acknowledgement within a few days and a coordinated disclosure once a
fix is available.

## Trust model

SPARC is an intra-machine transport. Transport-level isolation is provided by
the underlying OS IPC primitive; SPARC itself does not authenticate peers and
does not encrypt messages.

* **No peer authentication.** Any process with access to the region can publish
  well-formed messages, claim a role and complete the handshake. Messages are
  not authenticated: a peer's identity is whoever holds the OS access.
* **Transport access control is the OS layer.** Which identities may open/map
  a region is decided when the region is created or opened by the OS primitive:
  Windows section DACLs (`WindowsSectionSecurity`) or Unix file ownership/mode
  (`0600` plus a private directory). SPARC never checks access per message.
* **Authorized peers are trusted.** A process with valid access can read or
  modify every byte of the region. DACLs do not provide message integrity or
  confidentiality against an authorized peer.
* **Parsed data is treated as untrusted.** Header, geometry, chunk and slot
  frames are validated before use; corrupt or hostile bytes raise
  `RingBufferCorruptedException`, `RingBufferVersionMismatchException` or
  `RingBufferGeometryMismatchException` instead of reading out of bounds
  (`tests/Sparc.FuzzTests` covers this). This protects a reader from a
  scribbling peer; it does not authenticate the writer.
* **No availability guarantee.** A same-user process can fill the ring, steal a
  role or unlink a Unix backing file (denial of service). Endpoint states are
  advisory: a hard-killed process leaves `Running` behind and peers rely on
  timeouts. See the crash-semantics table in the README.

Message authentication, replay protection and message confidentiality between
already-authorized peers are the application's responsibility: layer an
authenticated/encrypted payload above the transport (below). If peers are not
equally trusted, use a transport with the boundary you need instead of SPARC.

## Transport access control

### Windows: section DACLs

`WindowsSectionSecurity` is applied when this process **creates** the region
through `IpcRegionOptions.Security` (or `SharedRingBufferOptions.Security`).
Openers only need the OS-granted access; the descriptor is not passed on a
join.

* **`CurrentUserOnly`** (also CLI `--security current-user`) builds a protected
  DACL that grants the section rights needed to map read/write to the current
  user's SID and nobody else: no `Everyone`, no `Authenticated Users`, no
  implicit group access.
* **`ForSids(...)` / `ForAccountNames(...)`** grant exactly the listed
  identities; `FromSddl(...)` accepts an explicit descriptor. `FromSddl`
  rejects descriptors without a DACL (a null DACL would grant everyone full
  access).
* The DACL is an ACE list on the section object; the kernel evaluates it when
  a process calls `OpenFileMapping`/`CreateFileMapping` or maps a view. Denial
  surfaces as `UnauthorizedAccessException` at region establishment (not after
  the ring is open).
* A join does not need the descriptor. The factory first tries the raw create
  path; `ERROR_ALREADY_EXISTS` or `ERROR_ACCESS_DENIED` falls through to a plain
  read/write open, which succeeds only when the DACL allows it.

### Windows: unnamed sections and HANDLE transfer (capability mode)

An unnamed section has no entry in the OS namespace, so knowledge of a channel
identifier or name grants nothing. `WindowsUnnamedSection.Create` returns a
`WindowsSectionCapability` holding the section HANDLE, which the owner passes
to the peer (handle inheritance or `DuplicateHandle`); the peer maps it with
`WindowsUnnamedSection.MapHandle` and adopts the region with
`SparcRing.OpenProducer`/`OpenConsumer(IIpcMemoryRegion, ...)`. An optional
`WindowsSectionSecurity` still applies as defence in depth.

Handle transfer is inherently OS-specific and stays in the Windows package; the
cross-platform `IIpcMemoryRegionFactory` contract is unchanged.

### Unix

`Sparc.UnixMemoryMapped` does not implement `IpcRegionOptions.Security`; a
non-null value fails region establishment instead of being silently dropped.
Its boundary is the private region directory (created `0700`) and owner-only
`0600` region files, as described under Permissions.

### What DACLs do not provide

* **Not message authentication.** An authorized process can rewrite header
  fields, slots, or published messages without detection.
* **Not confidentiality against authorized peers.** Anyone granted read/map
  access can read the payload.
* **Not protection from replay or role theft.** Access control decides who may
  open the section, not what they do with it afterwards.

## Optional payload authentication / encryption

SPARC deliberately keeps cryptography out of the transport. The intended shape
is:

```text
application message
        |  serialize + optional AEAD (key is yours to provision)
        v
authenticated/encrypted payload bytes
        |  SPARC (fixed-size slot protocol, unchanged)
        v
shared memory
```

Authenticate/encrypt the serialized payload before `TryPublish`/`WriteAsync`
and verify/decrypt after reading. The ring protocol, the hot path, the
benchmarks and the crash semantics do not change. Use established AEAD
primitives from a maintained library (for example `AesGcm` in
`System.Security.Cryptography`) — do not hand-roll cryptography. Key
distribution, rotation, nonces and replay windows are application concerns.

## Region names

The name is the entire rendezvous key: whoever controls it controls the
channel.

* **Guessable and machine-visible.** Names live in an OS-wide namespace (Windows
  objects) or a directory (Unix files). There is no registry, no discovery and
  no naming authority; two deployments that pick the same name share a region.
* **Squatting.** The first process to create a region fixes its geometry, and
  any process with access can claim a role or corrupt the contents. Role claims
  reject a second live endpoint of the same role; `SharedRingBufferOptions.Takeover`
  (CLI `--takeover`) forcibly reclaims it.
* **Takeover is recovery, not authentication.** Anyone who can open the region
  and pass `takeover: true` can displace a live peer; exposing that flag to
  untrusted input turns it into a denial-of-service primitive.
* **Naming rules.** `RegionName.Validate` rejects empty names, `\`, `/`, NUL and
  the exact names `.` and `..`; the Unix factory additionally rejects
  `Path.GetInvalidFileNameChars` and both separators. Treat case sensitivity as
  platform-defined. Names are not a security boundary.
* **Stale-region recreation.** `RecreateIfStale` detects bad magic and asks the
  factory to reclaim the backing store; on Unix that unlinks the file. Enable it
  only where the directory is trusted.
* **Recommendations.** Derive names from deployment-unique values (for example a
  random GUID suffix) to avoid accidental collisions, and never accept a region
  name directly from an untrusted caller.

On Windows the name is an OS object name. Without a security configuration the
region is created with no custom descriptor, so the process's default DACL
applies and any same-default-DACL process can open it. With
`WindowsSectionSecurity` the DACL restricts opening/mapping to the listed
identities; see "Transport access control" above. In capability mode
(`WindowsUnnamedSection`) there is no name to configure at all. SPARC does not
add cross-session isolation on its own; do not assume any unless you configure
it.

## Permissions

**Unix (`Sparc.UnixMemoryMapped`).**

* Regions are files under a directory, by default a `sparc` directory under
  `Path.GetTempPath()`. The directory is created with mode `0700` (owner-only),
  and each region file is created with mode `0600` (`UnixCreateMode`), using
  `FileMode.CreateNew` so an existing path is never truncated, and mapped from
  the open handle so an unlink-and-replace cannot slip between create and map.
* An **existing** directory keeps whatever mode its owner chose. Pointing the
  factory at a shared or world-writable directory (for example `/tmp` itself)
  means other local users can pre-create a file or symlink under a region's
  name; joining follows the path, so a squatted region can expose messages
  placed in it and feed the opener arbitrary bytes (the protocol rejects the
  latter, the former is disclosure). Use a private directory such as
  `/run/user/<uid>/sparc`, `~/.sparc`, or a dedicated tmpfs mount whenever other
  users share the machine.
* Joining a region file that another user created with `0600` fails
  (`UnauthorizedAccessException`); cross-user operation requires deliberately
  widening the file mode, which also makes the region readable by that group.
* `TryReset`/`RecreateIfStale` unlinks by name: write access to the directory is
  enough to delete regions (denial of service).

**Windows (`Sparc.WindowsMemoryMapped`).**

* Regions are named memory-mapped sections. By default they are created with no
  security descriptor and no namespace prefix, so the default DACL and OS
  namespace rules apply. Any process that can open the object can read and
  modify every message; there is no implicit per-region ACL and no SPARC-level
  access check beyond what the OS performs.
* Passing `WindowsSectionSecurity` (for example `CurrentUserOnly`, or the CLI
  `--security current-user`) creates the section with a protected DACL that
  grants only the listed identities the section rights required to map
  read/write. Access denial is reported when the region is opened.
* `WindowsUnnamedSection` creates a section with no name: it cannot be opened
  by any process that does not hold the transferred HANDLE.
* A process that is granted access can still read/tamper with messages; see the
  threat model and "What DACLs do not provide" above.
* Notification uses named semaphores, also with default OS permissions; the
  semaphore name is derived from the region name and is not protected by
  `WindowsSectionSecurity`. POSIX named semaphores are created `0600` and are
  not unlinked on dispose, matching the persistent region files; a stale raise
  is harmless because waiters always re-check the buffer.

**In-memory (`Sparc.InMemory`).** Regions are process-local; nothing is exposed
to other processes.

## Known design constraints (not vulnerabilities)

* Any process that can map the region can read and corrupt every message.
* Names are guessable by design; see above.
* Endpoint states are advisory and a hard kill leaves `Running`.
* Same-user denial of service (filling the ring, stealing the role, corrupting
  the header, unlinking a Unix backing file) is possible.

Reports about those behaviours are welcome as hardening suggestions, but they
are design constraints rather than vulnerabilities.
