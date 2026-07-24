[← Index](README.md) | Next: [Intra-node propagation →](02-intra-node-propagation.md)

# 01 — Core Concepts and Data Model

All classes in this document live in
`modules/commons/src/main/java/org/apache/ignite/internal/thread/context/`.

## 1.1 The four types

| Type | Role |
|---|---|
| `OperationContext` | The thread-bound store. Purely static API; the instance is private and reachable only through a `ThreadLocal`. |
| `OperationContextAttribute<T>` | A typed key. Identified by a unique bit, not by name or identity of a field. |
| `Scope` | An `AutoCloseable` undo token returned by every mutating operation. Closing it reverts that one update. |
| `OperationContextSnapshot` | An opaque handle to a captured context state, transferable to another thread. |

## 1.2 `OperationContext` — the store

```java
private static final ThreadLocal<OperationContext> INSTANCE =
    ThreadLocal.withInitial(OperationContext::new);
```

The public API is four static operations:

```java
static <T> T      get(OperationContextAttribute<T> attr);
static <T> Scope  set(OperationContextAttribute<T> attr, T val);   // + 2-arg and 3-arg overloads
static OperationContextSnapshot createSnapshot();
static Scope      restoreSnapshot(OperationContextSnapshot snp);
```

### The `Update` chain

The context's state is **not** a map. It is a singly-linked list of immutable `Update` nodes, and the
context object holds only a reference to the most recent one (`lastUpd`):

```
         +-----------+   +-----------+
         |           |   | A1 -> V2  |
null <---| A1 -> V1  |<--|           |   <-- lastUpd
         |           |   | A2 -> V3  |
         +-----------+   +-----------+
```

Each `Update` carries:

- `attrVals` — the `(attribute, value)` pairs this update changed;
- `updAttrBits` — OR of the bitmasks of attributes **changed by this update**;
- `storedAttrBits` — OR of the bitmasks of **all attributes present** after this update and all
  preceding ones (`prev.storedAttrBits | updAttrBits`);
- `prev` — link to the previous update.

`Update` implements *both* `Scope` and `OperationContextSnapshot`. That single design decision is the
heart of the mechanism:

- As a **`Scope`**, `close()` calls `undo(this)`, which asserts `lastUpd == this` and then sets
  `lastUpd = prev`. O(1), no recomputation — that is what `storedAttrBits` on the surviving node buys.
- As a **`Snapshot`**, `createSnapshot()` simply returns `lastUpd`. Because updates are immutable and
  each links to its predecessor, *a reference to the newest update fully describes the state*. Snapshot
  creation is therefore allocation-free and O(1).

### Read path

```java
if (lastUpd == null || (lastUpd.storedAttrBits & attr.bitmask()) == 0)
    return attr.initialValue();          // fast negative: one AND, no traversal
```

Only if the bit is present does `findAttributeValue` walk the chain backwards to the newest `Update`
that holds the attribute. Within an `Update`, `value()` scans `attrVals` **in reverse** so that if the
same attribute was supplied twice in one multi-attribute update, the last one wins.

The typical depth of this chain is small (a handful of nested scopes), so the linear walk is cheap and
the bitmask check eliminates it entirely for absent attributes — which is the common case, since only
a couple of attributes exist at all.

### Write path and the identity short-circuit

```java
public static <T> Scope set(OperationContextAttribute<T> attr, T val) {
    OperationContext ctx = INSTANCE.get();
    return ctx.getInternal(attr) == val ? NOOP_SCOPE : ctx.applyAttributeUpdates(...);
}
```

Note `==`, **reference** identity, not `equals`. Setting the value that is already there allocates
nothing and returns `NOOP_SCOPE`. Setting an equal-but-distinct instance does create an update — that
is correct but slightly wasteful; it never loses data.

Two package-private batch collectors exist, both built on a shared `AttributeCollector` base:

- **`Updater`** (used by the 2- and 3-arg `set` overloads) batches several attribute writes into
  a *single* `Update` node pushed **on top of** the existing chain, applying the same identity
  short-circuit per attribute and returning `NOOP_SCOPE` if nothing actually changed.
- **`Restorer`** (used by `OperationContextDispatcher.restoreSnapshot`) instead builds a fresh
  `Update` with **`prev == null`** and swaps it in via `restoreSnapshotInternal`:

  ```java
  Scope restore() {
      return ctx.restoreSnapshotInternal(isEmpty() ? null : ctx.new Update(toArray(), null));
  }

  static Scope restoreEmpty() {
      return new Restorer(INSTANCE.get()).restore();
  }
  ```

  That is **full-replacement** semantics: for the duration of the returned `Scope`, the thread's
  context consists of *exactly* the restored attributes — everything else, including local
  non-distributed attributes, reads as `initialValue()`. This is deliberate (see
  [03.1](03-cross-node-propagation.md#31-operationcontextdispatcher)): a remotely received context
  must not be overlaid on top of whatever the receiving thread already had. Since the IGNITE-28915
  null-snapshot fix the empty case is symmetric too:
  restoring an *empty* set swaps the context to `null` (`restoreEmpty()`), resetting every attribute
  to its default rather than no-op'ing — the history of that fix is in
  [05.11](05-context-loss.md#511-empty-snapshot-restores-are-a-noop--fixed).

### `restoreSnapshot` — cross-thread restoration

```java
private Scope restoreSnapshotInternal(OperationContextSnapshot newSnp) {
    OperationContextSnapshot prevSnp = createSnapshotInternal();
    if (newSnp == prevSnp) return NOOP_SCOPE;
    changeState(prevSnp, newSnp);
    return () -> changeState(newSnp, prevSnp);
}

private void changeState(OperationContextSnapshot expState, OperationContextSnapshot newState) {
    assert lastUpd == expState;
    lastUpd = (Update)newState;
}
```

Restoration **replaces** `lastUpd` wholesale — it does not merge. Whatever the target thread had in its
context is set aside for the duration of the scope and put back on close. The `assert lastUpd ==
expState` in `changeState` is the correctness guard: it fires if scopes are closed out of order or if
someone mutated the context underneath the scope. See [05 — context loss](05-context-loss.md#51-scope-discipline)
for what happens when assertions are disabled.

## 1.3 `OperationContextAttribute<T>`

```java
static final AtomicInteger ID_GEN = new AtomicInteger();
static final int MAX_ATTR_CNT = Integer.SIZE;   // 32

public static <T> OperationContextAttribute<T> newInstance(T initVal) {
    int id = ID_GEN.getAndIncrement();
    assert id < MAX_ATTR_CNT;
    return new OperationContextAttribute<>(1 << id, initVal);
}
```

Key properties:

- **Identity is the bitmask.** `equals`/`hashCode` compare `bitmask` only. Two attributes created by
  two calls to `newInstance()` are always distinct; there is no way to "look up an attribute by name".
- **A global, process-wide counter.** IDs come from a static `AtomicInteger`, so attribute identity is
  tied to *class-initialization order within one JVM*. This is fine locally (the attribute object is
  the key) but it is exactly why the distributed layer needs its own, explicitly assigned IDs — see
  [`DistributedAttributeRegistry`](03-cross-node-propagation.md#32-distributed-attribute-ids).
- **Hard cap of 32 attributes per JVM.** The 33rd `newInstance()` trips an assertion (and, with
  assertions off, silently produces `1 << 32 == 1`, colliding with attribute 0 — a genuine
  correctness hazard, listed in [05](05-context-loss.md#59-capacity-overflow)).
- **`initialValue()`** is what `get()` returns when the attribute was never set. Crucially, the
  distributed layer treats "current value `==` initial value" as "nothing to send".

## 1.4 `Scope`

```java
public interface Scope extends AutoCloseable {
    Scope NOOP_SCOPE = () -> {};
    @Override void close();       // narrowed: cannot throw
}
```

`close()` is declared without checked exceptions so `try (Scope ignored = …)` needs no catch block.
The documented contract, repeated on every method that returns a `Scope`:

> Updates must be undone **in the same order and in the same thread** they were applied.

This is a stack discipline. It is enforced only by assertions.

## 1.5 `OperationContextSnapshot`

A marker interface with no methods. The only implementation is the private
`OperationContext.Update`, which means:

- snapshots cannot be constructed, inspected, or serialized by callers;
- a snapshot is a plain reference to an immutable node, so it is safe to hand to another thread;
- `createSnapshot()` on an untouched thread returns **`null`** — this is the "empty context" signal that
  `wrapIfContextNotEmpty` keys off.

## 1.6 Cost model

| Operation | Cost |
|---|---|
| `get` on an absent attribute | one field read + one AND |
| `get` on a present attribute | chain walk, depth = number of nested scopes |
| `set` (new value) | one small object + one array |
| `set` (same reference) | zero allocation |
| `createSnapshot` | zero allocation, returns a reference |
| `restoreSnapshot` | one lambda for the undo `Scope` |

The IEP notes that ubiquitous propagation could affect performance and GC, but that preliminary
testing showed minimal impact. The allocation-free snapshot and the bitmask fast path are the reasons.

---

Next: [02 — Intra-node propagation →](02-intra-node-propagation.md)
