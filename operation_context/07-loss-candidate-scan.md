[← Index](README.md) | Prev: [Tickets and change history](06-tickets.md)

# 07 — Component Scan: Candidate Loss Sites

A directed scan of Ignite components for the two handoff shapes that historically produced
IGNITE-28902 and IGNITE-28915:

- **Shape A — deferred execution.** Work is enqueued by a thread that *has* context and executed later
  by a worker/pool thread. If the queue element carries no snapshot, the context is gone.
- **Shape B — foreign-thread drain.** The queue is drained by a thread that is itself in the middle of
  *another* operation (finishing a different job, handling a node failure, winning a drain-ownership
  CAS). Here the context is not merely empty — it is *someone else's*. This is worse than loss,
  because it fails **open and attributably**: the deferred work executes with a real, valid, wrong
  security subject.

## 7.0 Method and bounds

Starting points: all 22 `GridWorker` subclasses in `modules/core`, every blocking/concurrent queue
field in core, all 47 production `restoreSnapshot` call sites, and the set of files that import
`org.apache.ignite.internal.thread.context` (85 files across commons/core/zookeeper). A component was
treated as a candidate when it holds a collection of work items across a thread boundary *and* does
not appear in the context-aware set.

**Not covered by this scan:** `modules/indexing`, `modules/calcite`, ML/Spark modules, the thin-client
and REST protocol handlers, service-grid internals beyond `ServiceDeploymentManager`, and snapshot
restore internals. Findings below are ranked by confidence × blast radius.

---

## 7.1 Summary

| # | Site | Shape | Confidence | Severity |
|---|---|---|---|---|
| [F1](#f1--ordered-communication-messages--fixed) | `GridCommunicationMessageSet.unwind` | A + B | **FIXED** (`e88537a142f` + null case in `13b506e028c`) | ~~High~~ — closed; regression test for the null case still missing |
| [F2](#f2--datastreamer-remap-deque) | `DataStreamerImpl.dataToRemap` | B | Confirmed | **High** |
| [F3](#f3--write-behind-store-flush) | `GridCacheWriteBehindStore` | A | Confirmed | **High** |
| [F4](#f4--unewthread-pins-context-for-a-threads-lifetime) | `U.newThread(GridWorker)` — 14 sites | A (sticky) | Confirmed | Medium |
| [F5](#f5--continuous-query-interval-buffer-checker) | `GridContinuousProcessor` buffer checker | A + B | Confirmed | Medium |
| [F6](#f6--compute-jobs-bypass-the-generic-mechanism) | `GridJobWorker` | — (bespoke) | Confirmed | Medium |
| [F7](#f7--collision-driven-job-activation) | `GridJobProcessor.handleCollisions` | B | Confirmed | Latent |
| [F8](#f8--ringmessageworker-task-hook) | `ServerImpl.RingMessageWorker.tasks` | A | Confirmed | Low |

Verified-clean results are in [7.10](#710-negative-results).

> **Re-verified 2026-07-24** against the branch head (`13b506e028c`): F1 is fixed by the
> `IGNITE-28915 Refactoring` commit (`e88537a142f`), and its null-snapshot residual by
> `13b506e028c`; F2–F8 re-checked against current sources and
> unchanged (`DataStreamerImpl:289/1007/1019`, `GridCacheWriteBehindStore` still has zero context
> imports, `CommonUtils.newThread:2671`, `GridContinuousProcessor:1663/2183-2229`,
> `GridJobWorker:255/533/747`, `ServerImpl` task hook `:2961/:2975/:3145`).

---

## F1 — Ordered communication messages — FIXED

**Status: fixed by the `IGNITE-28915 Refactoring` commit** (`e88537a142f`). `unwind`
(`GridIoManager:3795`) now opens a `restoreSnapshot(mc.message.opCtxSnp)` scope per drained message,
exactly the fix proposed below; the regression test is
`OperationContextAttributePropagationTest.testPostponedCommunicationOrderedMessage` (all sender/receiver
pairs, two ordered messages with different contexts on one topic).

The null-snapshot residual (a buffered default-context message inheriting the draining thread's
context) was subsequently closed by `13b506e028c`: `restoreSnapshot(null)` now resets the context to
defaults ([05.11](05-context-loss.md#511-empty-snapshot-restores-are-a-noop--fixed)). The "context seen
by the listener" column of the drain-entry table below describes the **pre-fix** behaviour.

The original finding, kept for the record:

**`GridIoManager` — `GridCommunicationMessageSet` (:3620), `OrderedMessageContainer` (:3865),
`unwind` (:3795).**

This is the same bug as IGNITE-28915, in the communication path instead of discovery, and it is the
strongest finding in the scan.

Ordered messages that arrive before their listener is ready are buffered:

```java
private static class OrderedMessageContainer {
    GridIoMessage message;          // ← carries opCtxSnp
    long addedTime;
    IgniteRunnable closure;
    Span parentSpan;                // ← tracing IS carried across the hop
}
```

The container preserves the tracing span across the handoff but **not** the operation context. Then:

```java
void unwind(GridMessageListener lsnr) {
    assert reserved.get();

    for (OrderedMessageContainer mc = msgs.poll(); mc != null; mc = msgs.poll()) {
        try (TraceSurroundings ignore = support(ctx.tracing().create(
            COMMUNICATION_ORDERED_PROCESS, mc.parentSpan))) {     // ← span restored
            try {
                invokeListener(plc, lsnr, nodeId, mc.message.message());   // ← no Scope
            }
            …
        }
    }
}
```

`GridIoManager:462` restores context around `onMessage0`, so the *enqueueing* thread has the sender's
context — and drops it at `set.add(msg, msgC)`.

There are five ways into `unwind`, and each loses differently:

| Entry | Draining thread | Context seen by the listener |
|---|---|---|
| `:1746` inline after add | whichever thread won `reserve()` | **the other sender's context** (shape B) |
| `:1758` deferred | pool thread | empty |
| `:2473` `addMessageListener` replay via `pools.poolForPolicy(…).execute(…)` | pool thread, wrapped at submit | **the listener-registrar's context** (shape B) |
| `:939-966` timeout worker | `GridTimeoutProcessor` worker | the timeout object's context, not the message's |
| `:1029-1059` disconnect | disconnect handler | unrelated |

The `:1746` case is the serious one: two nodes send ordered messages on the same topic, both arrive,
thread T1 (carrying subject A) reserves the set and unwinds *both* — so B's message is handled as A.

**The fix is cheap and the data is already present.** `mc.message` is a `GridIoMessage`, which has
`opCtxSnp`. Mirror the tracing line:

```java
try (Scope ignored = ctx.operationContextDispatcher().restoreSnapshot(mc.message.opCtxSnp)) {
    invokeListener(plc, lsnr, nodeId, mc.message.message());
}
```

No new field, no wire change, no capture site — same three-line shape as the IGNITE-28915 fix.
*(This is exactly what the refactoring commit implemented.)*

---

## F2 — DataStreamer remap deque

**`DataStreamerImpl` — `dataToRemap` (:289), add (:1007), drain (:1014-1030).**

Pure shape B, caused by a single-owner drain:

```java
dataToRemap.add(r);                                    // plain Runnable, no snapshot

if (!remapOwning.get() && remapOwning.compareAndSet(false, true)) {
    ctx.closure().callLocalSafe(new GPC<Boolean>() {
        @Override public Boolean call() {
            while (locked || !dataToRemap.isEmpty()) {
                …
                Runnable r = dataToRemap.poll();
                if (r != null)
                    r.run();                           // ← foreign context
                …
            }
        }
    });
}
```

The `remapOwning` CAS elects **one** thread to drain the whole deque. `callLocalSafe` submits to a
context-aware pool, so the drainer faithfully carries the context of whichever streamer thread won the
CAS — and then runs *every other* streamer's remap under it. `r` closes over `load0(entriesForNode,
resFut, activeKeys, remaps + 1, node, topVer)`, i.e. an actual cache write.

Data streamers are commonly per-user (one streamer per ingest client), so this is a concrete
cross-subject crossing, not a theoretical one. It also survives the `wrapIfContextNotEmpty`
optimisation entirely: the context is non-empty, just wrong.

**Fix:** wrap at enqueue — `dataToRemap.add(OperationContextAwareRunnable.wrap(r))`. The drain loop
needs no change.

---

## F3 — Write-behind store flush

**`GridCacheWriteBehindStore` — zero context imports.**

Classic shape A across the longest gap in the codebase:

```
user thread (has context)
  → updateCache(key, val, op)            :592
  → putToWriteCache / putToFlusherWriteCache
        … milliseconds to seconds …
  → Flusher.body()                       :951, flushCacheCoalescing :1172 / NonCoalescing :1229
  → applyBatch                           :750
  → updateStore                          :864
  → store.writeAll(vals.values())        :885
    store.deleteAll(vals.keySet())       :890   ← user-supplied CacheStore code
```

`store.writeAll` / `store.deleteAll` is **user code** — typically a JDBC or JPA write. It runs with no
trace of the subject whose `cache.put()` produced the entry. For a `CacheStore` that does per-tenant
routing or writes an audit column, this is silently wrong output rather than a missing check.

Compounding it: the flusher threads are started via `U.newThread(this)` (:998), so per [F4](#f4--unewthread-pins-context-for-a-threads-lifetime)
they permanently carry whatever context the *cache-start* thread had. Coalescing mode makes a
per-entry snapshot semantically awkward anyway — several users' writes to the same key collapse into
one store call — so this one needs a design decision, not a three-line fix.

---

## F4 — `U.newThread` pins context for a thread's lifetime

**`CommonUtils:2671`.**

```java
public static IgniteThread newThread(GridWorker worker) {
    return new IgniteThread(worker.igniteInstanceName(), worker.name(), worker);
}
```

That is the **three-argument** `IgniteThread` constructor — the one that *does* capture, via
`wrapIfContextNotEmpty(r)` ([02.3](02-intra-node-propagation.md#23-ignitethread)). So a `GridWorker`
started through `U.newThread` inherits the context of whichever thread called `start()`, and keeps it
for the thread's entire life.

This is [05.4](05-context-loss.md#54-capture-time--submit-time) in its most durable form: not one task
captured at the wrong moment, but a daemon thread pinned to a stale context for hours.

Fourteen call sites. Sorted by how likely the starting thread is to have had a real user context:

| Started from | Sites |
|---|---|
| **User / session threads** | `SqlClientContext`, `SchemaOperationWorker` (DDL), `HeavyQueriesTracker`, `GridContinuousProcessor` (routine registration — see [F5](#f5--continuous-query-interval-buffer-checker)) |
| Cache/component start | `GridCacheWriteBehindStore`, `GridCacheSharedTtlCleanupManager`, `WalStateManager`, `DurableBackgroundCleanupIndexTreeTaskV2` |
| Node start (context empty — benign) | `GridTimeoutProcessor`, `Checkpointer`, `FileWriteAheadLogManager`, `FileHandleManagerImpl`, `PerformanceStatisticsProcessor`, `FilePerformanceStatisticsWriter` |

The node-start group is fine — empty context in, empty context stays. The first group is where a
long-lived worker silently adopts one user's identity.

Note the asymmetry this creates: a worker in group 1 processes *every* item under one arbitrary
user's context, whereas a worker whose thread was created via the six-argument constructor processes
every item under *no* context. Both are wrong; only the first is dangerous.

---

## F5 — Continuous query interval buffer checker

**`GridContinuousProcessor:1663-1700`.**

```java
IgniteThread checker = U.newThread(new GridWorker(…, "continuous-buffer-checker", log) {
    @Override protected void body() {
        while (!isCancelled()) {
            U.sleep(interval0);

            IgniteBiTuple<GridContinuousBatch, Long> t = info.checkInterval();
            final GridContinuousBatch batch = t.get1();

            if (batch != null && batch.size() > 0) {
                …
                sendNotification(nodeId, routineId, null, toSnd, hnd.orderedTopic(), msg, ackC);
            }
        }
    }
});
```

Both shapes at once:

- **A + F4:** the thread is created at routine-registration time from the registering thread — often a
  user thread — so it is pinned to that user's context forever.
- **B:** the batch it drains was filled by *cache-update* threads (`batch.add(obj)` at :2183/:2212/:2229),
  each with its own context. Every buffered entry is then notified and acknowledged
  (`hnd.onBatchAcknowledged`) under the registrar's context.

Since `sendNotification` goes through `GridIoManager`, the pinned context is also what gets
**collected onto the wire** — so this one propagates the wrong subject to the remote listener node,
not just locally.

---

## F6 — Compute jobs bypass the generic mechanism

**`GridJobWorker:177, 255, 533, 747`.**

```java
private final SecurityContext secCtx;
…
secCtx = ctx.security().securityContext();               // :255, at construction

try (Scope ignored = ctx.security().withContext(secCtx)) {   // :533, :747
    …
}
```

Compute correctly preserves security across the job queue — but by holding a `SecurityContext` field
directly, which is exactly the per-subsystem pattern IEP-143 exists to eliminate. It works today
because `SECURITY` is the only distributed attribute.

The moment a second attribute is registered, compute jobs become the largest blind spot in the
product: nothing else about the operation context reaches job execution. Same class of latent issue as
[05.7](05-context-loss.md#57-conditional-wrapping-on-security-enabled), but with a much bigger surface.

---

## F7 — Collision-driven job activation

**`GridJobProcessor.handleCollisions()` (:890), called from :1355, :1969, :2081, :2280, :2376.**

This is precisely the shape described as *"processed from a thread that executes another operation"*.
Jobs sit in `passiveJobs` (:193) until a collision SPI decision activates them — and that decision is
made on whichever thread happened to trigger it, most tellingly `:2081`, which runs on the thread
**finishing a different job**. Job B is therefore activated from a thread carrying job A's context.

Currently benign — but only *by accident of* [F6](#f6--compute-jobs-bypass-the-generic-mechanism):
`GridJobWorker` re-establishes its own `secCtx` from a field at `:533`, overwriting whatever it
inherited. Migrate compute to the generic mechanism without fixing this first and it becomes a live
cross-subject bug.

---

## F8 — `RingMessageWorker` task hook

**`ServerImpl:2904` (`tasks`), `:2975` (`addTask`), `:3145` (`runTasks`).**

A general "run this on the ring worker thread" hook:

```java
setBeforeEachPollAction(() -> {
    updateHeartbeat();
    onIdle();
    runTasks();          // :2966
});
```

`runTasks()` runs in `beforeEachPollAction`, i.e. *outside* the `restoreSnapshot` scope
that wraps `processMessage` (:3296) — so no contamination from the previous message, which is the
right call. But nothing carries context *in* either.

Benign today: the only caller (`:1210`) is join-time bootstrap with no user context. Flagged because it
is a public-ish extension point on the most context-sensitive thread in the node, with no propagation
story at all.

---

## 7.9 Suggested order of work

1. ~~**F1**~~ — **done** (refactoring commit; see above).
2. ~~**The null-snapshot NOOP**~~ — **done** (`13b506e028c`; dispatcher-level full-state restore —
   [05.11](05-context-loss.md#511-empty-snapshot-restores-are-a-noop--fixed)). Remaining: a regression
   test for the default-behind-non-default ordered scenario.
3. **F2** — one-line fix at the enqueue site.
4. **F4** — audit the four user-thread `U.newThread` sites; likely the right answer is that long-lived
   workers should start with an *empty* context (the six-argument constructor) and take context
   per-item instead.
5. **F5** — falls out of F4 plus a per-batch snapshot.
6. **F3** — needs a design decision on coalescing semantics.
7. **F6/F7** — a prerequisite pair for any second distributed attribute.

---

## 7.10 Negative results

Checked and found correct — worth recording so the next scan does not redo them:

- **`GridCacheIoManager` pending-affinity messages** (:141, :259-290). Messages parked until an
  affinity future completes *are* covered: the replay closure is registered with `fut.listen(…)`, and
  `GridFutureAdapter` wraps listener closures at registration time
  ([02.4](02-intra-node-propagation.md#24-futures)). The registering thread is the message-receiving
  thread, which is inside `GridIoManager`'s restore scope. Correct by construction.
- **`GridNioServer`** (`modules/nio`) — zero context awareness, correctly so. It moves bytes; the
  restore happens one layer up in `GridIoManager`'s listener.
- **`GridClosureProcessor`** — no context imports, but `callLocalSafe`/`runLocalSafe` submit to
  context-aware pools, so capture happens at submit. 81 call sites covered for free.
- **`GridCachePartitionExchangeManager`** (:3004-3042) — exchange worker tasks carry snapshots and are
  restored per item.
- **`GridTimeoutProcessor`** (:96, :426-440) — `TimeoutObjectWrapper` captures at `addTimeoutObject`
  and restores in `onTimeout`.
- **`CommunicationConnectionStateHandler`** (:172-178) — disconnect data restored per queued element.
- **`GridCacheSharedTtlCleanupManager`** — system-initiated expiry; no originating user context exists
  to lose.

---

[← Index](README.md)
