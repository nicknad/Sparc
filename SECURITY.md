# Security policy

## Supported versions

Security fixes are applied to the latest released minor version. The project is
pre-1.0, so minor releases may contain breaking changes.

| Version | Supported |
|---|---|
| 0.2.x | yes |
| < 0.2 | no |

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

## Threat model notes

SPARC is an intra-machine, same-trust-domain transport. The following are
**known and out of scope**, not vulnerabilities:

* Any process that maps the region can read and corrupt every message; there is
  no isolation between endpoints.
* Named memory-mapped regions and named semaphores are machine-global; names are
  guessable by design. Use names that are unique per deployment.
* Endpoint states are advisory. A hard-killed process leaves `Running`; peers
  rely on timeouts.
* `SessionWaitMode.Notification` uses named OS semaphores (named `Semaphore`
  objects on Windows, POSIX named semaphores on Unix-like systems). The POSIX
  semaphores are created with owner-only permissions (0600) and are not unlinked
  on dispose, matching the persistent region files; a stale raise is harmless
  because waiters always re-check the buffer.
* On Unix, region files are created under `<temp>/sparc` with owner-only
  permissions; a shared directory supplied by the caller keeps its existing
  permissions.
* Denial of service by a same-user process (filling the ring, stealing the role,
  corrupting the header) is possible; see the crash-semantics table in the
  README.

Reports about those behaviours are welcome as hardening suggestions, but they
are design constraints rather than vulnerabilities.
