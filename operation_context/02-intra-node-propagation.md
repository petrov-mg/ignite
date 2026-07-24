[← Index](README.md) | Prev: [Concepts](01-concepts.md) | Next: [Cross-node propagation →](03-cross-node-propagation.md)

# 02 — Intra-Node Propagation (Thread to Thread)

Within a single JVM the context travels by the **capture-and-restore** pattern:

1. At the moment work is handed off (submit, listen, enqueue, thread creation), the *submitting*
   thread calls `OperationContext.createSnapshot()`.
2. The snapshot is stored alongside the work item.
3. When the *executing* thread runs the item, it opens
   `try (Scope s = OperationContext.restoreSnapshot(snapshot))` around the call.

Everything in this document is an application of that one pattern.

## 2.1 The wrapper hierarchy

`…/thread/context/function/OperationContextAwareWrapper<T>` is the base:

```java
public static <T> T wrap(T delegate, BiFunction<T, OperationContextSnapshot, T> wrapper,
                         boolean ignoreEmptyContext) {
    if (delegate == null || delegate instanceof OperationContextAwareWrapper)
        return delegate;                          // (A) never double-wrap

    OperationContextSnapshot snapshot = OperationContext.createSnapshot();

    if (ignoreEmptyContext && snapshot == null)
        return delegate;                          // (B) skip wrapper for empty context

    return wrapper.apply(delegate, snapshot);
}
```

Both early-outs are deliberate optimisations and both are loss vectors under the wrong usage —
see [05.3](05-context-loss.md#53-re-wrapping-does-not-re-capture) and
[05.4](05-context-loss.md#54-capture-time--submit-time).

Concrete wrappers, all following the same shape (capture in `wrap`, restore in the functional method):

```
function/
├── OperationContextAwareRunnable      run()
├── OperationContextAwareCallable      call()
├── OperationContextAwareConsumer      accept()
├── OperationContextAwareBiConsumer    accept()
├── OperationContextAwareFunction      apply()
├── OperationContextAwareBiFunction    apply()
├── OperationContextAwareSupplier      get()
├── OperationContextAwareInClosure     apply()      — Ignite's IgniteInClosure
└── OperationContextAwareWrapper       (plain holder, no functional interface)
```

Example, `OperationContextAwareRunnable`:

```java
@Override public void run() {
    try (Scope ignored = OperationContext.restoreSnapshot(snapshot)) {
        delegate.run();
    }
}
```

`OperationContextAwareWrapper` itself is the *non-functional* variant: it just holds
`(delegate, snapshot)` and exposes `delegate()` / `contextSnapshot()`. It is used where the consumer
wants to restore the context itself around a larger block — notably the worker-queue handlers
(§2.5). It implements `IgniteInternalWrapper<T>` so that unwrapping is uniform across the codebase.

Two entry points matter:

- `wrap(delegate)` — always wraps (unless already wrapped / null).
- `wrapIfContextNotEmpty(delegate)` — returns the delegate untouched when the current context is
  empty. Used on hot paths where the submitting thread is usually a pool thread with no context.

## 2.2 Thread pools

`modules/core/…/internal/thread/pool/`:

| Class | Wraps |
|---|---|
| `IgniteThreadPoolExecutor` | extends `OperationContextAwareExecutorService<ThreadPoolExecutor>` |
| `IgniteScheduledThreadPoolExecutor` | extends the scheduled variant |
| `IgniteStripedExecutor` | uses `wrapIfContextNotEmpty` per submitted task |
| `IgniteForkJoinPool` | context-aware `ForkJoinPool` replacement |
| `OperationContextAwareIoPool` | wraps user-supplied executors handed to `PoolProcessor` |

`modules/commons/…/thread/context/concurrent/`:

| Class | Role |
|---|---|
| `OperationContextAwareExecutor` | wraps any `Executor`; `execute()` → `OperationContextAwareRunnable.wrap(command)` |
| `OperationContextAwareExecutorService` | same for the full `ExecutorService` surface (`submit`, `invokeAll`, `invokeAny`, …) |
| `OperationContextAwareScheduledExecutorService` | adds `schedule*` methods |

`OperationContextAwareExecutor` is the minimal illustration:

```java
@Override public void execute(@NotNull Runnable command) {
    delegate.execute(OperationContextAwareRunnable.wrap(command));
}
```

`PoolProcessor` (line ~276) wraps user-supplied executors **unconditionally**:

```java
extPools[id] = OperationContextAwareIoPool.wrap(ex);
```

Earlier revisions guarded this with `ctx.security().enabled()`, which would have silently dropped any
future non-security distributed attribute on clusters running without security. The condition was
removed in the `IGNITE-28915 Refactoring` commit — see the (now historical)
[05.7](05-context-loss.md#57-conditional-wrapping-on-security-enabled--fixed).

## 2.3 `IgniteThread`

`modules/commons/…/thread/IgniteThread` has two behaviours, and the difference is easy to miss:

```java
// Captures the parent thread's context:
public IgniteThread(String igniteInstanceName, String threadName, Runnable r) {
    this(igniteInstanceName, threadName, wrapIfContextNotEmpty(r), GRP_IDX_UNASSIGNED, -1, UNDEFINED);
}

// Does NOT capture — by design:
public IgniteThread(String name, String threadName, Runnable r, int grpIdx, int stripe, byte plc) { … }
```

The javadoc on the six-argument constructor is explicit:

> **Note**: This constructor creates a thread that does NOT automatically acquire the parent thread's
> Operation Context, ensuring that no Operation Context is attached to it at the start of execution.
> It is used in Ignite thread pools and worker threads, which rely on this behavior to avoid
> unnecessary wrapping.

That is the invariant that makes `wrapIfContextNotEmpty` safe: pool and worker threads start with an
empty context, so the `snapshot == null` fast path is the normal case there, and the per-task wrapper
is only allocated when a caller genuinely had context to propagate.

## 2.4 Futures

### `GridFutureAdapter`

Listener registration wraps the closure at the moment `listen()` is called
(`GridFutureAdapter.Node` constructor):

```java
this.val = val instanceof Thread
    ? val
    : OperationContextAwareInClosure.wrap((IgniteInClosure<?>)val);
```

So the context of the thread that *registered* the listener is what the listener sees — regardless of
which thread eventually completes the future. The `instanceof Thread` branch is the parked-waiter
case, which needs no context.

### `IgniteCompletableFuture`

`…/thread/context/concurrent/IgniteCompletableFuture<T>` is a full `Future` + `CompletionStage`
implementation that **delegates** to a private `CompletableFuture` and wraps every user function on
the way in:

```java
@Override public <U> IgniteCompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
    return wrap(delegate.thenApply(OperationContextAwareFunction.wrap(fn)));
}
```

Every `thenApply`/`thenAccept`/`thenCombine`/`whenComplete`/`handle`/… variant — sync, async, and
async-with-executor — receives the same treatment, and the result is re-wrapped in an
`IgniteCompletableFuture` so the chain stays context-aware end to end. This is delegation rather than
inheritance precisely so that no un-wrapped `CompletableFuture` method can leak through.

## 2.5 Asynchronous worker queues

`modules/core/…/internal/util/worker/queue/`:

```java
abstract class AsyncQueueHandler<T, W extends OperationContextAwareWrapper<T>> extends GridWorker
```

> Represents a single-threaded, asynchronous queue elements handler. It automatically captures the
> `OperationContext` attached to the thread that submitted the item for handling and restores it
> before handling actually begins in the worker thread.

Subclasses: `IgniteAsyncObjectHandler` (plain `LinkedBlockingQueue`) and `IgniteDelayedObjectHandler`
(delay queue). Items go in wrapped via `wrapQueueElement(delegate, snapshot)`; the worker takes them
with `takeQueuedElement()` / `pollQueuedElement(timeout, unit)` and restores explicitly.

`GridDiscoveryManager` is the archetypal consumer — three separate worker loops, each restoring
around its dispatch:

```java
OperationContextAwareWrapper<NotificationEvent> contextualEvt = takeQueuedElement();

try (Scope ignored = OperationContext.restoreSnapshot(contextualEvt.contextSnapshot())) {
    …                                    // GridDiscoveryManager:3064-3066
}
```

with the same shape at `:2748` (discovery worker requests) and `:2829-2831` (future notifications).

This queue-handoff pattern is where the two most recent bugs were found: any code path that pulls an
item out of such a queue **without** going through the restoring accessor drops the context. See
[IGNITE-28915](06-tickets.md#ignite-28915--postponed-discovery-messages--review-driven-refactoring).

## 2.6 Static enforcement — Checkstyle

Step 3 of the IEP: make it impossible to *accidentally* use a non-propagating JDK primitive. Landed in
IGNITE-26775 as a custom Checkstyle rule,
`modules/checkstyle/…/ClassUsageRestrictionRule.java`, configured in `checkstyle/checkstyle.xml`:

| Banned | Required substitute |
|---|---|
| `java.lang.Thread` | `org.apache.ignite.thread.IgniteThread` |
| `java.util.concurrent.ThreadPoolExecutor` | `IgniteThreadPoolExecutor` |
| `java.util.concurrent.ScheduledThreadPoolExecutor` | `IgniteScheduledThreadPoolExecutor` |
| `Executors.newFixedThreadPool`, `newWorkStealingPool`, `newSingleThreadExecutor`, `newCachedThreadPool`, `unconfigurableExecutorService` | `IgniteThreadPoolExecutor` |
| `Executors.newSingleThreadScheduledExecutor`, `newScheduledThreadPool`, `unconfigurableScheduledExecutorService` | `IgniteScheduledThreadPoolExecutor` |
| `ForkJoinPool.commonPool` | `IgniteForkJoinPool` |
| `CompletableFuture.allOf`, `anyOf`, `supplyAsync`, `runAsync`, `completedFuture` | `IgniteCompletableFuture` |

The rule supports whole-class bans and factory-method-level bans. Exemptions live in
`checkstyle/checkstyle-suppressions.xml` and `checkstyle/checkstyle-xpath-suppressions.xml` — the IEP
calls out thin-client modules as legitimately exempt, since they have no cluster-side context to carry.
Each suppression is, by construction, a place where propagation is *not* guaranteed
([05.6](05-context-loss.md#56-escaping-into-raw-jdk-concurrency)).

---

Next: [03 — Cross-node propagation →](03-cross-node-propagation.md)
