[← Index](README.md) | Prev: [Security attribute](04-security-attribute.md) | Next: [Tickets →](06-tickets.md)

# 05 — How Operation Context Gets Lost

This is the practical core of the document set. Every loss vector below is grounded in code currently
on `master`/`IGNITE-28915`.

**The general shape of the failure.** Context loss is almost never an exception. `OperationContext.get`
on an absent attribute returns `attr.initialValue()`, and for the security attribute that means
`dfltSecCtx` — the node's own privileges. So a lost context degrades into *"execute as the local
node"*, which usually succeeds. The bug shows up later as an authorization hole or a wrong audit
record, far from the code that dropped it.

**The corollary.** Assertions are the primary defence, and Ignite production builds run with `-ea`
off. Everything guarded only by `assert` below is, in production, a silent path.

---

## Ranked summary

| # | Vector | Detected by | Severity |
|---|---|---|---|
| [05.1](#51-scope-discipline) | `Scope` closed out of order / not closed | assertion only | high — corrupts the chain for the whole thread |
| [05.2](#52-a-handoff-with-no-restore) | Handoff point that captures but never restores (or never captures) | tests, if any | high — the recurring real-world bug |
| [05.3](#53-re-wrapping-does-not-re-capture) | Already-wrapped delegate re-submitted from a different context | nothing | medium |
| [05.4](#54-capture-time--submit-time) | `wrap()` called long before the work is submitted | nothing | medium |
| [05.5](#55-transports-that-carry-no-carrier-field) | Bytes leaving the node outside `GridIoMessage`/discovery messages | nothing | medium |
| [05.6](#56-escaping-into-raw-jdk-concurrency) | Raw `Thread`/`CompletableFuture`/`ThreadPoolExecutor`/parallel streams | Checkstyle, with suppressions | medium |
| [05.7](#57-conditional-wrapping-on-security-enabled--fixed) | ~~`PoolProcessor` wraps user pools only when security is on~~ | — | **fixed** — wrapping is now unconditional |
| [05.8](#58-asymmetric-attribute-registration) | Nodes disagree on distributed ID → attribute mapping | assertion only | high, but constrained |
| [05.9](#59-capacity-overflow) | >32 local or >8 distributed attributes | assertion only | latent |
| [05.10](#510-subject-no-longer-resolvable) | Received subject ID no longer in discovery history | `IllegalStateException` | low — fails closed |
| [05.11](#511-empty-snapshot-restores-are-a-noop--fixed) | ~~`restoreSnapshot(null)` was a NOOP — default-context messages inherited the executing thread's context~~ | — | **fixed** (IGNITE-28915) — null now resets to defaults; regression test still missing |
| [05.12](#512-derived-messages-inherit-the-in-scope-context) | Messages created while processing another message inherit its context via `addMessage`'s attach — `ServerImpl:3951` stamps a user's context onto `NodeFailedMessage` | nothing | **high — live defect, cheap fix** |
| [05.13](#513-client-side-entry-points-that-never-capture) | `ClientImpl.MessageWorker.addMessage` never captures — client `failNode()` drops the caller's context | nothing | low — attribution loss only |

---

## 5.1 Scope discipline

Every mutating call returns a `Scope`, and the contract is repeated on each one:

> Note, updates must be undone in the **same order** and in the **same thread** they were applied.

The enforcement is two assertions:

```java
private void undo(Update upd) {
    assert lastUpd == upd;                  // OperationContext.undo
    lastUpd = lastUpd.prev;
}

private void changeState(OperationContextSnapshot expState, OperationContextSnapshot newState) {
    assert lastUpd == expState;             // OperationContext.changeState
    lastUpd = (Update)newState;
}
```

### Failure mode A — out-of-order close

```java
Scope a = OperationContext.set(ATTR_1, v1);
Scope b = OperationContext.set(ATTR_2, v2);

a.close();     // WRONG: closes the outer scope first
```

`undo(a)` sets `lastUpd = a.prev`, discarding `b` — so `ATTR_2` is lost *and* `ATTR_1` is lost, in one
step. When `b.close()` runs later it walks the chain further back, corrupting whatever the caller
above had set. With assertions on this trips immediately; with them off the thread's context is
quietly wrong from that point forward. Always use try-with-resources; never store a `Scope` in a field
or pass it across methods.

### Failure mode B — never closed

Not a *loss* but a leak: the `Update` chain grows without bound on a long-lived pool thread, keeping
attribute values (and, for security, whole `SecurityContext` objects) reachable. Every `get` on a
present attribute walks a longer chain. The javadoc calls this out explicitly as a memory leak risk.

### Failure mode C — closed on a different thread

`Scope` closes over the `OperationContext` instance of the thread that created it, but `undo` mutates
that instance's `lastUpd` field with no synchronisation. Closing from another thread is both a data
race and an almost-certain assertion failure. Never let a `Scope` escape its thread.

---

## 5.2 A handoff with no restore

**This is the vector that has actually produced bugs**, repeatedly
([IGNITE-28902](06-tickets.md#ignite-28902--discovery-acknowledgement-messages),
[IGNITE-28915](06-tickets.md#ignite-28915--postponed-discovery-messages--review-driven-refactoring), plus three more instances of
the same shape fixed within IGNITE-28915: ordered communication buffers
([07 · F1](07-loss-candidate-scan.md#f1--ordered-communication-messages--fixed)), client-reconnect
pending-message replay, and local pending-message re-enqueue —
[found_problems #3/#4](found_problems_in_russian.md)).

The mechanism only works where someone wrote the capture/restore pair. Any new asynchronous handoff,
queue, buffer, replay path, or message type is context-lossy **by default**. The framework has no way
to detect that a handoff exists and is unwrapped.

The IGNITE-28915 case is the cleanest example. `ServerImpl` parks custom discovery messages that
arrive while a join is in progress, and the coordinator replays them once `joiningNodes` empties:

```java
// before — msg.opCtxSnp was populated, but nobody read it
while ((msg = pollPendingCustomMessage()) != null)
    processCustomMessage(msg, true);

// after
while ((msg = pollPendingCustomMessage()) != null) {
    try (Scope ignored = operationCtxDispatcher.restoreSnapshot(msg.opCtxSnp)) {
        processCustomMessage(msg, true);
    }
}
```

The message object had carried the originator's context faithfully across the network and through the
pending queue. The replay loop simply ran on the ring worker thread — whose context is empty — and
never consulted it. Every postponed custom message was processed with default privileges.

Note the *shape* of this bug: the "normal" path (`ServerImpl:3296`) had the restore; the
**deferred/retry/replay** path did not. That is the pattern to look for. IGNITE-28915 closed the
three further instances found by that pattern:

- `GridIoManager.unwind` (`:3795`) now restores each buffered ordered message's own snapshot
  ([07 · F1](07-loss-candidate-scan.md#f1--ordered-communication-messages--fixed));
- `ClientImpl.processDiscoveryMessage` (`:2151`) is now itself the restore boundary, so the
  client-reconnect `pendingMessages()` replay restores per message;
- `TcpDiscoveryAbstractMessage.attachOperationContextSnapshot` sets the envelope **only if absent**,
  so `processPendingMessagesLocally` → `addMessage` no longer overwrites a replayed message's original
  context with the replaying handler's.

### Where to look for more of these

- Anything that **buffers and replays**: pending queues, retry loops, message backlogs, reconnect
  replay, deferred/postponed processing.
- Anything that **generates a derived message**: acknowledgements, responses, forwarded messages,
  split/fan-out messages. `TcpDiscoveryAbstractMessage`'s copy constructor propagates `opCtxSnp`
  (line 109) and `ZkOperationContextAwareCustomMessage.ackMessage()` re-wraps — a *new* message type
  that does neither will silently drop it.
- Anything that **takes from a queue** without using `AsyncQueueHandler.takeQueuedElement()` /
  `pollQueuedElement()` and the surrounding `restoreSnapshot`.
- Any new `DiscoverySpi` implementation — TCP and ZK each needed bespoke integration.

---

## 5.3 Re-wrapping does not re-capture

```java
public static <T> T wrap(T delegate, BiFunction<…> wrapper, boolean ignoreEmptyContext) {
    if (delegate == null || delegate instanceof OperationContextAwareWrapper)
        return delegate;                            // ← already wrapped: returned as-is
    …
}
```

The double-wrap guard means **the first capture wins, permanently**. If a `Runnable` is wrapped in
thread A (context X) and then later submitted from thread B (context Y), it still runs with X. Y is
lost, silently, with no assertion.

This is correct and desirable for the intended use — a task submitted through two layers of
context-aware executors should not accumulate nested wrappers. It becomes a bug when a wrapped task is
**cached and reused**: stored in a field, put in a registry of handlers, or re-submitted on retry from
a different context. The task carries the context of whichever thread happened to construct it first.

Rule of thumb: never retain the result of a `wrap` call beyond a single submission.

---

## 5.4 Capture time ≠ submit time

Capture happens when `wrap()` is called, not when the work runs. For executors these coincide
(`execute()` wraps then submits). They diverge when a closure is built early and submitted later:

```java
Runnable r;

try (Scope s = OperationContext.set(ATTR, v)) {
    r = buildTask();                 // context is set here…
}                                    // …and gone here

executor.execute(r);                 // captures the *empty* context
```

The inverse also bites, via `wrapIfContextNotEmpty`:

```java
public static Runnable wrapIfContextNotEmpty(Runnable delegate) {
    return wrap(delegate, OperationContextAwareRunnable::new, true);   // ignoreEmptyContext = true
}
```

If the context is empty at wrap time, **no wrapper is created at all**. If the caller then sets
context and only afterwards submits, nothing restores it — and, worse, the task will inherit whatever
context the *executing* thread happens to have, rather than nothing.

This optimisation is safe under the invariant that Ignite pool/worker threads start with an empty
context — which is exactly why `IgniteThread`'s six-argument constructor deliberately does *not*
capture the parent context:

> **Note**: This constructor creates a thread that does NOT automatically acquire the parent thread's
> Operation Context … It is used in Ignite thread pools and worker threads, which rely on this
> behavior to avoid unnecessary wrapping.

Break that invariant — leave a `Scope` unclosed on a pool thread (§5.1 failure mode B) — and
`wrapIfContextNotEmpty` starts capturing stale leftovers and attaching them to unrelated tasks. §5.1
and §5.4 compound: a leaked scope becomes *cross-operation context contamination*, which is worse than
loss, because it fails **open** in a specific, attributable way.

---

## 5.5 Transports that carry no carrier field

Cross-node propagation exists only where a carrier field was added:

| Path | Carrier | Status |
|---|---|---|
| Communication (`GridIoManager`) | `GridIoMessage.opCtxSnp` | covered: send + receive + buffered ordered drain |
| TCP discovery | `TcpDiscoveryAbstractMessage.opCtxSnp` | covered incl. acks, pending messages, client reconnect replay |
| ZK discovery | `ZkOperationContextAwareCustomMessage` decorator | covered |

Anything else carries nothing:

- **Direct `CommunicationSpi` use.** The context is attached in
  `GridIoManager.createGridIoMessage(…)`. Code that constructs and sends a message without going
  through it has a `null` `opCtxSnp`. (`GridIoManager`'s own listener warns about direct SPI use for
  unrelated reasons — the same anti-pattern also defeats context propagation.)
- **Channel / file transfer.** `onChannelOpened0(rmtNodeId, (GridIoMessage)initMsg, channel)` on the
  receive path is *not* wrapped in a `restoreSnapshot` scope, unlike `onMessage0` right above it —
  even though the sender *does* attach a snapshot to the channel-init message and it arrives intact
  in `initMsg.opCtxSnp`. Channel listeners and `TransmissionHandler`s (snapshot transfer, file-based
  rebalance) run the whole transmission without the initiator's context. Exact mechanics and the fix
  shape: [09 · P1](09-gridio-message-flow.md#p1--the-channel-path-never-restores), registered as
  [F12](07-loss-candidate-scan.md#712-third-sweep-gridiomessage-flow-2026-07-24). Data moving over an opened channel therefore carries no restored context on the receiving
  side.
- **Thin client / JDBC / ODBC protocols.** Separate wire formats entirely; the IEP explicitly exempts
  thin-client modules from the Checkstyle rules. These have their own authentication path.
- **Anything crossing the JVM boundary that isn't an Ignite `Message`** — persisted records, WAL
  entries, files.

---

## 5.6 Escaping into raw JDK concurrency

Any JDK construct that moves work between threads without an Ignite wrapper drops the context. The
Checkstyle rule (`ClassUsageRestrictionRule`, §2.6) bans the common ones — `Thread`,
`ThreadPoolExecutor`, `ScheduledThreadPoolExecutor`, the `Executors` factories,
`ForkJoinPool.commonPool`, and five `CompletableFuture` statics.

The remaining holes:

- **Suppressions.** `checkstyle/checkstyle-suppressions.xml` and `checkstyle-xpath-suppressions.xml`
  each carry an exempt list. Every entry is a file where propagation is not statically guaranteed and
  must be reasoned about by hand.
- **Thin-client and tooling modules**, exempt by design.
- **Constructs the rule does not cover.** The IEP names the canonical one:

  > external libraries (like `java.util.stream.BaseStream#parallel`) lacking context support can still
  > cause data loss.

  `parallelStream()` / `.parallel()` dispatch to the common `ForkJoinPool` from inside stream
  internals — there is no interception point, and the ban on `ForkJoinPool.commonPool` does not catch
  it. Same for `Arrays.parallelSort`, `CompletableFuture` *instance* methods obtained from a raw
  future, third-party libraries with internal pools, and any JNI/native callback thread.
- **Instance methods vs statics.** Only five `CompletableFuture` *static factories* are banned. A raw
  `CompletableFuture` obtained from a third-party API still exposes the full un-wrapped
  `thenApplyAsync` surface; only `IgniteCompletableFuture` wraps those.

---

## 5.7 Conditional wrapping on `security().enabled()` — FIXED

**Status: fixed in IGNITE-28915.** `PoolProcessor` (~line 276) originally
wrapped user-supplied executors only when security was enabled:

```java
// before
extPools[id] = ctx.security().enabled() ? OperationContextAwareIoPool.wrap(ex) : ex;

// now
extPools[id] = OperationContextAwareIoPool.wrap(ex);
```

The old condition was sound only while `SECURITY` was the sole distributed attribute; a second
attribute (tracing, request IDs, tenant IDs, …) would have been silently dropped across custom
executor pools on every cluster running without security. Wrapping is now unconditional, so this
vector is closed. Kept here because the *pattern* — propagation machinery gated on
`security().enabled()` — is still worth rejecting in review.

---

## 5.8 Asymmetric attribute registration

Distributed decoding is **positional and index-based**:

```java
while ((msg.idBitmap & (1 << attrId)) == 0)
    ++attrId;

assert attrId < locRegisteredAttrs.length;

OperationContextAttribute<Message> attr =
    (OperationContextAttribute<Message>)locRegisteredAttrs[attrId++];

assert attr != null;
```

Correctness requires every node in the cluster to have the *same* ID → attribute mapping. If the
sender registered attribute 3 and the receiver did not:

- with assertions on: `assert attrId < locRegisteredAttrs.length` or `assert attr != null` fires;
- with assertions off: `ArrayIndexOutOfBoundsException`, or — worse — the value is applied to whatever
  attribute happens to occupy that slot, with a `ClassCastException` deferred to the eventual `get`.

Three defences exist:

1. **`DistributedAttributeRegistry`** — IDs are hand-assigned constants in one file, not derived from
   registration order, so they are stable across builds and nodes.
2. **`finishRegistration()`** (IGNITE-28808) — called from `IgniteKernal` after component start; any
   later `registerDistributedAttribute` throws
   `"Initialization of distributed operation context attributes has already finished."` This closes
   the window in which a node could add an attribute mid-life and start emitting bits its peers cannot
   decode.
3. **Duplicate detection** — `"Duplicated distributed attribute id [id=…]"` on a collision.

What is *not* defended: **rolling upgrade and heterogeneous plugins.** Nothing validates the
registered attribute set at join time. An old node that predates a new attribute, or a node missing a
plugin that registers one, will receive bits it cannot map. Any second distributed attribute needs a
node-join compatibility check or a feature gate.

The same gap has a **transport-level** face in discovery: an old-version node in the ring has neither
the `opCtxSnp` field nor the attached-flag bit, so forwarding any message through it re-creates the
message without either — the context is silently dropped mid-ring, and downstream new-version nodes
may restamp the now-flagless message with their own (typically empty) context
([08 · P2](08-custom-message-flow.md#p2--mixed-version-ring-rolling-upgrade)).

---

## 5.9 Capacity overflow

Two independent, silently-enforced caps:

```java
static final int  MAX_ATTR_CNT  = Integer.SIZE;   // 32 — local attributes, per JVM
static final byte MAX_ATTRS_CNT = Byte.SIZE;      //  8 — distributed attribute IDs
```

Local: `assert id < MAX_ATTR_CNT` in `newInstance`. With assertions off, the 33rd attribute computes
`1 << 32`, which in Java is `1 << 0` — it **aliases attribute 0**. Since `equals`/`hashCode` are the
bitmask, the two attributes become indistinguishable: writes to one are read by the other, with a
`ClassCastException` at the read site if the types differ. Today only a handful of attributes exist, so
this is latent, but the counter is a static `AtomicInteger` incremented at class-init — attributes
created by plugins or tests count against the same budget.

Distributed: `assert 0 <= id && id < MAX_ATTRS_CNT` in `registerDistributedAttribute`, bounded by the
`byte idBitmap` wire field. Raising it is a wire-format change.

---

## 5.10 Subject no longer resolvable

Not context *loss* — the context arrives intact — but it can fail to rehydrate.
`SecurityContextWrapper` transmits only `subjId`; the receiver resolves it lazily
([04.4](04-security-attribute.md#44-re-resolution-on-the-receiving-node)):

```java
res = secPrc.securityContext(subjId);

if (res == null) {
    res = findNodeSecurityContext(subjId);        // discovery().node() → historicalNode()

    if (res == null)
        throw new IllegalStateException("Failed to find security context for subject with given ID : " + subjId);
}
```

If the subject was a node that has left the topology *and* aged out of the discovery history, the
lookup throws. This is the correct behaviour — it fails **closed** rather than silently substituting
the local default — but it means the effective lifetime of a received security context is bounded by
discovery history depth. Long-running operations that outlive their originator's presence in history
will fail at the point of first authorization check, not at the point of loss.

---

## 5.11 Empty-snapshot restores are a NOOP — FIXED

**Status: fixed in IGNITE-28915 (the null-snapshot fix, 2026-07-24).** This was
the open residual of the four P1 review findings ([found_problems](found_problems_in_russian.md)).

IGNITE-28915's refactoring step had given `restoreSnapshot` full-replacement semantics for a *non-null* snapshot, but
the `null` case kept a `NOOP_SCOPE` shortcut. Since a sender whose distributed attributes are all at
their initial values transmits **no snapshot at all**
([03.1](03-cross-node-propagation.md#31-operationcontextdispatcher)), a NOOP restore meant a
default-context message executed under whatever context the executing thread already had — a real,
valid, **wrong** context whenever that thread was mid-way through another operation (the reviewed
deterministic scenario: two ordered messages in one set, the second sent with default context, was
processed under the first sender's subject).

The fix makes the null case symmetric with the non-null one:

```java
public Scope restoreSnapshot(@Nullable OperationContextSnapshotMessage snp) {
    if (snp == null)
        return Restorer.restoreEmpty();   // swap the whole context to empty for the scope
    …
}
```

`Restorer.restoreEmpty()` performs `restoreSnapshotInternal(null)` — the thread's context is replaced
by the empty chain, so **every** attribute reads `initialValue()` inside the scope (a no-op only when
the thread's context is already empty). This closes all four windows in one place: ordered
communication drain (`GridIoManager.unwind:3795`), pending custom discovery messages
(`ServerImpl.checkPendingCustomMessages:6306`), client reconnect replay (`ClientImpl:2562/2572`), and
plain listener dispatch.

The companion change closed the last envelope hole: `attachOperationContextSnapshot` now records
"attached" in a serialized **flag bit** (`OP_CTX_ATTACHED_FLAG_POS = 3`) rather than inferring it from
field nullness — so a message legitimately carrying an empty context is not restamped with the
replaying handler's context on local re-enqueue
([03.5](03-cross-node-propagation.md#35-transport-integration-point-2-tcp-discovery)).

Two things remain worth knowing:

- **No regression test for the reviewed scenario.** The null-snapshot fix shipped no tests.
  `OperationContextAttributePropagationTest` asserts default-value transmission and per-message
  restore for two *non-default* ordered messages, but has no case sending a default-context message
  *behind* a non-default one into the same ordered set — the exact deterministic case from review is
  unregressed.
- **Remote restores mask local attributes too.** Both the null and non-null restore replace the
  context *wholesale*, so a thread-local (non-distributed) attribute set by the receiving node is
  invisible inside a remote-restore scope. Harmless today (production has no such attribute crossing
  these boundaries), but it is a semantic to keep in mind for future local attributes.

---

## 5.12 Derived messages inherit the in-scope context

Found by the custom-message flow audit ([08](08-custom-message-flow.md#85-remaining-problems)).

`ServerImpl.RingMessageWorker.addMessage` attaches the *current* thread snapshot to every locally
originated message. When the ring worker creates a message **while processing another message**, it
is inside that message's restore scope — so the derived message inherits the processed message's
context and carries it cluster-wide.

Three instances today, one of them a live defect:

- **`ServerImpl:3951` — defect.** `sendMessageAcrossRing`, forwarding user A's custom message, detects
  the next node failed and creates `TcpDiscoveryNodeFailedMessage` inside A's scope. Every node then
  runs node-failure handling — topology update, `EVT_NODE_FAILED` listeners, exchange trigger — under
  user A's subject. Fix: `attachOperationContextSnapshot(null)` at creation (the flag semantics allow
  explicitly pinning "no context").
- **`ServerImpl:6177` — benign.** The `TcpDiscoveryDiscardMessage` for a processed custom message
  carries the originator's context around the ring; discard processing touches no listener and no
  authorization, so no effect today — but it is an unintended smear and fragile against changes.
- **`ServerImpl:6189` — intended.** The custom-event ack deliberately captures the originator's
  context (IGNITE-28902 semantics).

Nothing distinguishes the intended instance from the accidental ones — every new
`addMessage`-inside-a-scope call site silently picks a side. That makes this a *class* of bug, not a
single site; the checklist below gets an item for it.

---

## 5.13 Client-side entry points that never capture

`ClientImpl.MessageWorker.addMessage(Object)` (`:2716`) performs **no** snapshot attach — unlike the
server's `RingMessageWorker.addMessage`. Any locally created discovery message injected through the
client worker queue therefore never captures the calling user's context, and by processing time the
restore boundary has reset the worker thread to an empty context.

Concrete case: `ClientImpl.failNode():539`. The server-side `failNode():1129` captures the caller's
context (attribution of who failed the node); the client-side one silently loses it. Custom events
are unaffected — `sendCustomEvent` captures at `SocketWriter.sendMessage:1314` on the caller thread.

The asymmetry is the trap: the same public SPI operation propagates context from a server node and
drops it from a client node.

---

## 5.14 A review checklist

> For the results of applying this checklist across Ignite's components — eleven findings with code
> references, ranked — see [07 — Component scan](07-loss-candidate-scan.md); for the directed audit
> of the custom discovery message flow, see [08](08-custom-message-flow.md).

When touching any asynchronous or cross-node path:

- [ ] Does this introduce a **thread handoff**? If so, is it through an Ignite context-aware executor,
      future, or `AsyncQueueHandler`? If it uses a raw JDK construct, is there a Checkstyle suppression
      and is it justified?
- [ ] Does this introduce a **queue, buffer, or replay path**? Is the snapshot captured on enqueue and
      restored on dequeue — on *every* dequeue path, including retry, timeout, and shutdown drain?
- [ ] Does this introduce a **new message type or transport**? Does it carry `opCtxSnp` (or the ZK
      decorator), and does the receive side open a `restoreSnapshot` scope?
- [ ] Does it **derive a message from another** (ack, response, forward)? Is `opCtxSnp` copied — via
      the copy constructor or `attachOperationContextSnapshot` (never a bare field write, which would
      clobber a replayed message's envelope and skip the attached-flag)?
- [ ] Does it **create a message while processing another** (inside a restore scope)? Decide
      explicitly whose context the new message should carry: the current scope's (then say so — the
      ack at `ServerImpl:6189` is the model) or none (then pin it with
      `attachOperationContextSnapshot(null)` before handing it to `addMessage`) — §5.12.
- [ ] Does every receive/replay path go through `OperationContextDispatcher.restoreSnapshot` — including
      for `opCtxSnp == null`? The dispatcher resets to defaults on null (§5.11); a hand-rolled
      `if (snp != null)` guard around the restore would silently reintroduce the inherited-context bug.
- [ ] Is every `Scope` in a **try-with-resources**, closed on the same thread, in LIFO order?
- [ ] Is any wrapped closure **stored** rather than submitted immediately (§5.3)?
- [ ] If adding a **distributed attribute**: is the ID in `DistributedAttributeRegistry`, is
      registration before `finishRegistration()`, and is there a rolling-upgrade story (§5.8)?

---

Next: [06 — Tickets and change history →](06-tickets.md)
