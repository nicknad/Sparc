# SPARC

SPARC moves fixed-size messages between exactly two processes through one memory-mapped region: one producer publishes, one consumer reads, and neither takes a lock.

## Region and transport

**Region**:
The fixed-size block of shared memory holding one ring: a header with the cursors, then the slots.
_Avoid_: segment, mapping, shared buffer

**Slot**:
One fixed-size cell in the region. A message occupies one slot; a value larger than a slot becomes a chunked stream.
_Avoid_: entry, record, page

**Frame**:
The few bytes at the head of a slot that carry the payload length and its type tag.
_Avoid_: prefix, metadata, envelope

**Geometry**:
The capacity and slot size a region was created with. Every opener of an existing region must agree with it or is rejected.
_Avoid_: layout, shape, format

**Transport**:
The operating-system primitive that creates or maps a region: a named section on Windows, a file-backed region on Unix, pinned memory in tests.
_Avoid_: backend, driver, provider

## Ring protocol

**Endpoint**:
A role-claimed handle on a region. A producer endpoint cannot read and a consumer endpoint cannot publish.
_Avoid_: connection, handle, side

**Producer**:
The single endpoint in a region allowed to publish messages.
_Avoid_: writer, sender

**Consumer**:
The single endpoint in a region allowed to read messages.
_Avoid_: reader, receiver, subscriber

**Head**:
The consumer-owned cursor: the sequence number of the next message to read.
_Avoid_: read index, position

**Tail**:
The producer-owned cursor: the sequence number the producer will write next.
_Avoid_: write index, position

**Sequence**:
A message's position in the stream. Its slot is the sequence modulo the capacity, so cursors grow without bound while slots are reused.
_Avoid_: offset, index

**Lease**:
A held reservation of one slot that publishes it (write) or releases it (read) when the caller is done.
_Avoid_: handle, token, view

**Wait mode**:
How an endpoint waits while it cannot make progress: spin, spin-then-sleep, or block on a notification latch.
_Avoid_: strategy, policy, backoff

**Notification latch**:
A one-deep cross-process wake-up. Raising it is only a hint: the woken endpoint re-checks the region anyway.
_Avoid_: event, semaphore, interrupt

## Sessions

**Session**:
A reusable driver for one role over an endpoint: it connects, transfers messages, and reports a structured outcome instead of throwing.
_Avoid_: client, worker, runner

**Session run**:
One execution of a session, from claiming the role to its stop decision.
_Avoid_: job, batch, transfer

**Stop decision**:
The mapping from the event that ended a session run to its stop reason and, for an unclean end, its failure message.
_Avoid_: status mapping, result handler

**Stop reason**:
The closed set of ways a session run can end: completed, peer stopped, timeout, verification failed, or cancelled.
_Avoid_: status, exit code, result

**Failure message**:
The human-readable text attached to an unclean stop decision. A clean stop has none.
_Avoid_: error, reason text

**Outstanding**:
Messages a session run still owed when it ended.
_Avoid_: remaining, missing

**Drain**:
Consuming the messages already published by a peer that has stopped, before the run finishes.
_Avoid_: flush, empty, finish

**Pacing**:
An artificial delay between messages, used only to simulate a slow endpoint in tests and benchmarks.
_Avoid_: throttling, rate limit

**Session summary**:
The human or machine-readable (JSON) description of a finished run, including its derived rates.
_Avoid_: report, stats

**Chunked stream**:
A message larger than one slot, published as a chain of chunks marked first and last, with the whole message reassembled by the reader.
_Avoid_: fragmentation, splitting, segmentation
