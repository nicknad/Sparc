---- MODULE SparcLifecycle ----
\* An abstract TLA+ model of one SPARC region with one producer session and
\* one consumer session.
\*
\* The model keeps the protocol decisions that the documentation promises:
\*   - the producer reserves a slot before it writes the payload;
\*   - the publish is the release store of tail;
\*   - the consumer views a published message before it advances head;
\*   - a producer crash before the publish leaves the reservation invisible;
\*   - a consumer crash mid-read leaves head alone, so the message is redelivered.
\*
\* Capacity is 1 and the sequence number is bounded by 2, so TLC's state graph
\* stays small enough to draw as a chart. The protocol is the same for any
\* positive power-of-two capacity; the ring index arithmetic is not modelled.
EXTENDS Naturals

Capacity == 1
MaxSeq == 2

VARIABLES producer, consumer, head, tail, reserving, reading
vars == <<producer, consumer, head, tail, reserving, reading>>

TypeOK ==
    /\ producer \in {"absent", "running", "stopped", "crashed"}
    /\ consumer \in {"absent", "running", "stopped", "crashed"}
    /\ head \in 0..MaxSeq
    /\ tail \in 0..MaxSeq
    /\ reserving \in BOOLEAN
    /\ reading \in BOOLEAN

Init ==
    /\ producer = "absent"
    /\ consumer = "absent"
    /\ head = 0
    /\ tail = 0
    /\ reserving = FALSE
    /\ reading = FALSE

\* The producer claims its role and starts the session.
ProducerStarts ==
    /\ producer \in {"absent", "stopped", "crashed"}
    /\ producer' = "running"
    /\ UNCHANGED <<consumer, head, tail, reserving, reading>>

\* The producer reserves the next slot and writes the payload in place.
ProducerReserves ==
    /\ producer = "running"
    /\ ~reserving
    /\ tail - head < Capacity
    /\ reserving' = TRUE
    /\ UNCHANGED <<producer, consumer, head, tail, reading>>

\* The producer writes the frame and release-stores tail.
ProducerCommits ==
    /\ producer = "running"
    /\ reserving
    /\ tail < MaxSeq
    /\ tail' = tail + 1
    /\ reserving' = FALSE
    /\ UNCHANGED <<producer, consumer, head, reading>>

\* A hard kill before the commit leaves the reservation unpublished.
ProducerCrashes ==
    /\ producer = "running"
    /\ producer' = "crashed"
    /\ reserving' = FALSE
    /\ UNCHANGED <<consumer, head, tail, reading>>

\* A graceful stop publishes Stopped in the advisory state word.
ProducerStops ==
    /\ producer = "running"
    /\ ~reserving
    /\ producer' = "stopped"
    /\ UNCHANGED <<consumer, head, tail, reserving, reading>>

\* The consumer claims its role and starts the session.
ConsumerStarts ==
    /\ consumer \in {"absent", "stopped", "crashed"}
    /\ consumer' = "running"
    /\ UNCHANGED <<producer, head, tail, reserving, reading>>

\* The consumer acquire-loads tail and views the oldest message.
ConsumerPeeks ==
    /\ consumer = "running"
    /\ ~reading
    /\ head < tail
    /\ reading' = TRUE
    /\ UNCHANGED <<producer, consumer, head, tail, reserving>>

\* The consumer finishes with the message and release-stores head.
ConsumerAdvances ==
    /\ consumer = "running"
    /\ reading
    /\ head' = head + 1
    /\ reading' = FALSE
    /\ UNCHANGED <<producer, consumer, tail, reserving>>

\* A hard kill mid-read leaves head alone, so the message is redelivered.
ConsumerCrashes ==
    /\ consumer = "running"
    /\ consumer' = "crashed"
    /\ reading' = FALSE
    /\ UNCHANGED <<producer, head, tail, reserving>>

\* A graceful stop publishes Stopped in the advisory state word.
ConsumerStops ==
    /\ consumer = "running"
    /\ ~reading
    /\ consumer' = "stopped"
    /\ UNCHANGED <<producer, head, tail, reserving, reading>>

Next ==
    \/ ProducerStarts \/ ProducerReserves \/ ProducerCommits
    \/ ProducerCrashes \/ ProducerStops
    \/ ConsumerStarts \/ ConsumerPeeks \/ ConsumerAdvances
    \/ ConsumerCrashes \/ ConsumerStops

Spec == Init /\ [][Next]_vars

\* The producer never wraps past the consumer: the ring never overruns.
CapacityRespected == tail - head <= Capacity

\* The consumer never advances past the newest published message, so no
\* message is skipped: a crash redelivers instead of losing a message.
NothingIsSkipped == head <= tail

\* A message is only viewed after it is published, never torn.
NoUnpublishedRead == reading => head < tail

\* A reservation always has a free slot behind it.
ReservationNeedsSpace == reserving => tail - head < Capacity

====
