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

SPARC is an intra-machine, same-trust-domain transport. It gives no
confidentiality or integrity against any process that can open the region.

* **No peer authentication.** Any process with access to the region can publish
  well-formed messages, claim a role and complete the handshake. Messages are
  not authenticated: a peer's identity is whoever holds the OS access.
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

Payload confidentiality, message authentication, replay protection and
cross-user isolation are the application's responsibility. If peers are not
equally trusted, add your own authentication/encryption on top of the payload.

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

On Windows the name is an OS object name: `MemoryMappedFile.CreateNew(name, …)`
is used with no `Global\`/`Local\` prefix and no custom security descriptor, so
namespace placement and access follow the OS defaults and the process's default
DACL. SPARC does not add cross-user or cross-session isolation; do not assume
any.

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

* Regions are named memory-mapped objects created with no security descriptor
  and no namespace prefix, so the default DACL and OS namespace rules apply.
  Any process that can open the object can read and modify every message; there
  is no per-region ACL and no SPARC-level access check.
* Notification uses named semaphores, also with default OS permissions. POSIX
  named semaphores are created `0600` and are not unlinked on dispose, matching
  the persistent region files; a stale raise is harmless because waiters always
  re-check the buffer.

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
