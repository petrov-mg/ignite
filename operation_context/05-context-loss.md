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
| [05.7](#57-conditional-wrapping-on-security-enabled) | `PoolProcessor` wraps user pools only when security is on | nothing | latent |
| [05.8](#58-asymmetric-attribute-registration) | Nodes disagree on distributed ID → attribute mapping | assertion only | high, but constrained |
| [05.9](#59-capacity-overflow) | >32 local or >8 distributed attributes | assertion only | latent |
| [05.10](#510-subject-no-longer-resolvable) | Received subject ID no longer in discovery history | `IllegalStateException` | low — fails closed |

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

**This is the vector that has actually produced bugs**, twice in the last month
([IGNITE-28902](06-tickets.md#ignite-28902--discovery-acknowledgement-messages),
[IGNITE-28915](06-tickets.md#ignite-28915--postponed-discovery-messages)).

The mechanism only works where someone wrote the capture/restore pair. Any new asynchronous handoff,
queue, buffer, replay path, or message type is context-lossy **by default**. The framework has no way
to detect that a handoff exists and is unwrapped.

The IGNITE-28915 case is the cleanest example. `ServerImpl` parks custom discovery messages that
arrive while a join is in progress, and the coordinator replays them once `joiningNodes` empties:

```java
// before — msg.opCtxMsg was populated, but nobody read it
while ((msg = pollPendingCustomMessage()) != null)
    processCustomMessage(msg, true);

// after
while ((msg = pollPendingCustomMessage()) != null) {
    try (Scope ignored = operationCtxDispatcher.restoreRemoteAttributeValues(msg.opCtxMsg)) {
        processCustomMessage(msg, true);
    }
}
```

The message object had carried the originator's context faithfully across the network and through the
pending queue. The replay loop simply ran on the ring worker thread — whose context is empty — and
never consulted it. Every postponed custom message was processed with default privileges.

Note the *shape* of this bug: the "normal" path (`ServerImpl:3296`) had the restore; the
**deferred/retry/replay** path did not. That is the pattern to look for.

### Where to look for more of these

- Anything that **buffers and replays**: pending queues, retry loops, message backlogs, reconnect
  replay, deferred/postponed processing.
- Anything that **generates a derived message**: acknowledgements, responses, forwarded messages,
  split/fan-out messages. `TcpDiscoveryAbstractMessage`'s copy constructor propagates `opCtxMsg`
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
| Communication (`GridIoManager`) | `GridIoMessage.opCtxMsg` | covered, send + receive |
| TCP discovery | `TcpDiscoveryAbstractMessage.opCtxMsg` | covered incl. acks and pending messages |
| ZK discovery | `ZkOperationContextAwareCustomMessage` decorator | covered |

Anything else carries nothing:

- **Direct `CommunicationSpi` use.** The context is attached in
  `GridIoManager.createGridIoMessage(…)`. Code that constructs and sends a message without going
  through it has a `null` `opCtxMsg`. (`GridIoManager`'s own listener warns about direct SPI use for
  unrelated reasons — the same anti-pattern also defeats context propagation.)
- **Channel / file transfer.** `onChannelOpened0(rmtNodeId, (GridIoMessage)initMsg, channel)` on the
  receive path is *not* wrapped in a `restoreRemoteAttributeValues` scope, unlike `onMessage0` right
  above it. Data moving over an opened channel therefore carries no restored context on the receiving
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

## 5.7 Conditional wrapping on `security().enabled()`

`PoolProcessor` (~line 276):

```java
extPools[id] = ctx.security().enabled() ? OperationContextAwareIoPool.wrap(ex) : ex;
```

User-supplied executors become context-aware **only when security is enabled**. Today this is sound —
`SECURITY` is the only distributed attribute, so with security off there is nothing to propagate.

It is a landmine for the next attribute. The moment a second distributed attribute is registered
(tracing, request IDs, tenant IDs, …), every cluster running without security loses it across custom
executor pools, with no error and no test failure. The condition should become "any distributed
attribute registered", or simply unconditional, before `DistributedAttributeRegistry` grows a second
constant.

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

## 5.11 A review checklist

> For the results of applying this checklist across Ignite's components — eight candidate sites with
> code references, ranked — see [07 — Component scan](07-loss-candidate-scan.md).

When touching any asynchronous or cross-node path:

- [ ] Does this introduce a **thread handoff**? If so, is it through an Ignite context-aware executor,
      future, or `AsyncQueueHandler`? If it uses a raw JDK construct, is there a Checkstyle suppression
      and is it justified?
- [ ] Does this introduce a **queue, buffer, or replay path**? Is the snapshot captured on enqueue and
      restored on dequeue — on *every* dequeue path, including retry, timeout, and shutdown drain?
- [ ] Does this introduce a **new message type or transport**? Does it carry `opCtxMsg` (or the ZK
      decorator), and does the receive side open a `restoreRemoteAttributeValues` scope?
- [ ] Does it **derive a message from another** (ack, response, forward)? Is `opCtxMsg` copied?
- [ ] Is every `Scope` in a **try-with-resources**, closed on the same thread, in LIFO order?
- [ ] Is any wrapped closure **stored** rather than submitted immediately (§5.3)?
- [ ] If adding a **distributed attribute**: is the ID in `DistributedAttributeRegistry`, is
      registration before `finishRegistration()`, is there a rolling-upgrade story (§5.8), and does
      `PoolProcessor`'s `security().enabled()` condition still hold (§5.7)?

---

Next: [06 — Tickets and change history →](06-tickets.md)
