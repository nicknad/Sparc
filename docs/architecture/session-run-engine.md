# Session run engine — design

Status: synthesized sketch, 2026-11-14. Implementation follows this file.

## Problem

`ProducerSession.Run` and `ConsumerSession.Run` are two copies of one run skeleton: connect-cancel
handling, the loop bound, pacing, progress cadence, the cancellation mask, no-progress
bookkeeping, peer observation, the wait, and the elapsed windows. Each also rebuilds the failure
wording that `SessionStopDecision` already owns, so the pure table is exercised only by its tests
while production drifts next to it. The drain adds a third copy of "how does this end" inside the
consumer and a fourth in `SparcChannelReader`, which has no stop reason at all.

The shape is constrained by four things: the two roles observe a stall in different orders and
that order is observable; the drain must classify *after* it runs; the per-message path may not
gain allocation, dispatch, timestamp reads, or moved fences; and the public session surface is
frozen.

## Usage (caller's view)

No public usage changes. `ProducerSession.Run` and `ConsumerSession.Run` keep their signatures.
The internal usage is:

```csharp
// ProducerSession.Run
var context = new SessionRunContext(
    _buffer, _options.Count, _options.PerMessageDelay,
    _options.WaitMode, _options.Notification, waitOnData: false);
var role = new ProducerRole(_buffer, _options, _timeProvider, progress);
SessionRunOutcome outcome = SessionRunEngine.Run(ref role, in context, _timeProvider, cancellationToken);

return new ProducerRunResult(
    outcome.Delivered, _options.PayloadSize, outcome.RunElapsed,
    outcome.Decision.Reason, outcome.Decision.Outstanding,
    _buffer.ConsumerState, outcome.Decision.Message)
{ ActiveElapsed = outcome.ActiveElapsed };
```

```csharp
// ConsumerSession.Run
var context = new SessionRunContext(
    _buffer, _options.Count, _options.PerMessageDelay,
    _options.WaitMode, _options.Notification, waitOnData: true);
var role = new ConsumerRole(_buffer, _options, _timeProvider, progress);
SessionRunOutcome outcome = SessionRunEngine.Run(ref role, in context, _timeProvider, cancellationToken);

return new ConsumerRunResult(
    outcome.Delivered, role.ReceivedBytes, outcome.StreamElapsed,
    outcome.Decision.Reason, role.Histogram, outcome.Decision.Message)
{ RunElapsed = outcome.RunElapsed };
```

```csharp
// a test can drive the engine with a scripted role: no region, no clock, no wait
var role = new ScriptedRole(steps: [SessionRunStep.Stalled, SessionRunStep.Progressed]);
SessionRunOutcome outcome = SessionRunEngine.Run(ref role, in context, TimeProvider.System, default);
Assert.Equal(SessionStopReason.Completed, outcome.Decision.Reason);
```

## Shape

One engine, two role structs, one decision table.

```csharp
internal readonly record struct SessionRunContext(
    IEndpoint Endpoint,
    long Requested,
    TimeSpan PerMessageDelay,
    SessionWaitMode WaitMode,
    SessionNotification? Notification,
    bool WaitOnDataSignal);

internal interface ISessionRunRole
{
    long Delivered { get; }
    long FirstTimestamp { get; }
    long LastTimestamp { get; }
    bool TryConnect(CancellationToken cancellationToken);
    SessionRunStep Step(CancellationToken cancellationToken);
    void ReportProgress(TimeSpan elapsed);
    SessionStopInput BuildStopInput(in SessionRunStep terminal);
}

internal static class SessionRunEngine
{
    internal static SessionRunOutcome Run<TRole>(
        ref TRole role, in SessionRunContext context, TimeProvider timeProvider, CancellationToken cancellationToken)
        where TRole : struct, ISessionRunRole;
}
```

`SessionRunStep` is an 8-byte value: `Progressed`, `Stalled`, or `Stop(terminal)`. The engine calls
`Step` once per iteration; the role performs its own stall checks inside `Step` in its own order
(producer: cancel → stamp → peer → timeout; consumer: peer → drain → cancel → stamp → timeout),
so the engine owns no role branch and no flag for "who checks first".

The engine owns: connect-cancel shaping, loop bound, `SpinWait`, pacing (never after the last
message), progress interval (~10% of count), the cancellation mask, the wait, the run clock, and
the single `SessionStopDecision.Resolve` call. The role owns: the message step (reserve/stamp/
fill/commit or peek/verify/latency/advance, including the notification fence where it already
sits), the stall clock, the drain, the observed peer state, and the mapping into `SessionStopInput`.

Elapsed windows are engine-computed from the role's first/last timestamps: `RunElapsed` = start→end;
`ActiveElapsed` = end−first step (0 when nothing moved); `StreamElapsed` = last−first step when more
than one message moved, else `RunElapsed`. Producer maps `RunElapsed→Elapsed`,
`ActiveElapsed→ActiveElapsed`; consumer maps `StreamElapsed→Elapsed`, `RunElapsed→RunElapsed`.

Invariants encoded: `SessionRunStep` cannot carry a failure without a terminal; `BuildStopInput`
is the only path to the table; `SessionStallClock` uses an explicit `bool` for "stamped" so a
`FakeTimeProvider` whose first timestamp is 0 still times out. Validation stays at the
constructors; the engine trusts its role.

Depth: the public surface does not change, and the internal seam is six members, four of them
behaviour. Everything the two loops shared moved behind one call.

## Synthesis decision

Base: candidate B. Its `SessionRunStep` vocabulary keeps the hot loop branch-free and puts the two
stall orders inside the roles, which is where the knowledge lives; its outcome is four fields and
its role owns the drain without a second classifier.

Grafted from A: the explicit stall-stamped boolean (A's sentinel finding), the "role structs by
`ref`, never `in`" rule (defensive copies would land per message), and the separation of the
peer state named in the failure text from the fresh `PeerState` read on the result.

Adapted from both: both sketches kept wait/config members on the role interface (12-14 members).
They are per-run data, not role behaviour, so they moved to `SessionRunContext`; the seam is six
members. Rejected from A: a shared `SessionRunState` ledger — the role already owns its counters,
and the ledger forced producer-only dead stores.

## Tradeoffs accepted

- We accept one devirtualized `Step` call per message on the producer (the consumer already paid a
  `ProcessOne` call) in exchange for one loop; the `session-64` regression scenario is the gate.
- We accept that the two stall orders now live in separate role files, guarded by order tests rather
  than by the compiler.
- We accept compute-and-drop for the consumer's `Outstanding`; the table still computes it, but
  `ConsumerRunResult` has no field for it and the public API stays frozen.
- We accept that `SparcChannelReader` keeps its own drain and gets no `SessionStopReason`: its pump
  blocks in `Read`, has no count, and changing it would alter latency semantics.

## Alternatives considered

- **Abstract template base class.** A virtual `Step` over an inheritance hierarchy costs an
  undevirtualisable call per message and a heap allocation per run; the state machinery moves into
  fields. It hides no more than the generic engine and costs more.
- **Delegate callbacks.** `Func<SessionRunStep>` per run cannot inline the role body, needs a
  closure allocation, and makes the drain's state capture awkward.
- **Single loop with a role enum switch.** Interleaves both orders in one method and grows a flag
  for every difference; the loop becomes the place where role knowledge accumulates.
- **Keep two loops, extract only the classifier.** Smallest diff, but leaves the cadence and the
  elapsed windows duplicated — the thing the change exists to remove.

## Open work

The implementation is complete. The three risks this design flagged — the producer's extra `Step`
call versus the `session-64` noise floor, failure-text culture formatting, and whether the channel
reader should ever report a stop reason — are tracked with the rest of the backlog in
[goals.md](../../goals.md) (Session follow-ups, items 1–3).
