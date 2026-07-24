[← Index](README.md) | Prev: [Intra-node propagation](02-intra-node-propagation.md) | Next: [Security attribute →](04-security-attribute.md)

# 03 — Cross-Node Propagation (Node to Node)

Step 4 of the IEP ("research remote node propagation") became a concrete mechanism: a subset of
attributes are declared **distributed**, are serialized into every outgoing cluster message, and are
restored on the receiving node around message processing.

The shape is identical to the intra-node pattern — capture before handoff, restore before execution —
except the "handoff" is a network hop and the "snapshot" is a `Message`.

## 3.1 `OperationContextDispatcher`

`modules/core/…/internal/thread/context/OperationContextDispatcher.java`. One instance per node,
reachable as `GridKernalContext.operationContextDispatcher()`.

```java
static final byte MAX_ATTRS_CNT = Byte.SIZE;                                     // 8

private volatile OperationContextAttribute<? extends Message>[] registeredAttrs = …;
private boolean regFinished;
```

Note the type bound: a distributed attribute's **value must itself be a `Message`**, because it has to
go on the wire through Ignite's own serialization. That is why `SecurityContext` cannot be a
distributed attribute directly and is boxed in `SecurityContextWrapper`
([04](04-security-attribute.md)).

> **Naming note.** The `IGNITE-28915 Refactoring` commit renamed the dispatcher API:
> `collectDistributedAttributeValues()` → **`createSnapshot()`** and
> `restoreRemoteAttributeValues(…)` → **`restoreSnapshot(…)`**, mirroring the local
> `OperationContext.createSnapshot()`/`restoreSnapshot()` pair. The wire class was renamed
> `OperationContextMessage` → **`OperationContextSnapshotMessage`**, and the carrier fields
> `opCtxMsg` → **`opCtxSnp`**.

### Registration

```java
public synchronized <T extends Message> void registerDistributedAttribute(
        int id, OperationContextAttribute<T> attr) {
    if (regFinished)
        throw new IgniteException("Initialization of distributed operation context attributes has already finished.");

    assert 0 <= id && id < MAX_ATTRS_CNT;
    …
    if (copy[id] != null)
        throw new IgniteException("Duplicated distributed attribute id [id=" + id + ']');
    …
}
```

`registeredAttrs` is a `volatile` copy-on-write array: registration is `synchronized` and rare;
reads are lock-free on the hot send/receive paths.

`finishRegistration()` flips `regFinished` and is called from `IgniteKernal` once components have
started (IGNITE-28808). After that, any attempt to register throws. The reason is
[cluster-wide ID agreement](05-context-loss.md#58-asymmetric-attribute-registration): if node A could
register attribute 3 late while node B never does, the two nodes would disagree on what bit 3 means.

### Collect (sender side)

```java
public @Nullable OperationContextSnapshotMessage createSnapshot() {
    …
    OperationContextSnapshotMessage.Builder snpBuilder = OperationContextSnapshotMessage.Builder.create();

    for (int id = 0; id < locRegisteredAttrs.length; id++) {
        OperationContextAttribute<? extends Message> attr = locRegisteredAttrs[id];

        if (attr == null)
            continue;

        Message curVal = OperationContext.get(attr);

        if (curVal != attr.initialValue())
            snpBuilder.add(id, curVal);                // (!) initial value is never sent
    }

    return snpBuilder.isEmpty() ? null : snpBuilder.build();
}
```

Two things to internalise:

- **The empty context costs nothing on the wire.** No registered attributes, or every attribute at its
  initial value → returns `null` → the carrier message's `opCtxSnp` field stays `null` → one null flag
  on the wire.
- **"Initial value" means "not propagated."** Comparison is `==` against `attr.initialValue()`. An
  attribute deliberately set back to its initial value is indistinguishable from unset, and the remote
  node will fall back to *its own* default. For security this is the intended semantic (see
  [04](04-security-attribute.md)). Since the receive side resets to defaults on a `null` snapshot
  ([05.11](05-context-loss.md#511-empty-snapshot-restores-are-a-noop--fixed)), the two ends are now
  symmetric: "not sent" and "restored as default" mean the same thing.

### Restore (receiver side)

```java
public Scope restoreSnapshot(@Nullable OperationContextSnapshotMessage snp) {
    if (snp == null)
        return Restorer.restoreEmpty();       // reset every attribute to its default
    …
    Restorer ctxRestorer = Restorer.create();

    for (byte valIdx = 0, attrId = 0; valIdx < snp.attrs.length; ++valIdx) {
        Message attrVal = snp.attrs[valIdx];

        while ((snp.idBitmap & (1 << attrId)) == 0)
            ++attrId;

        assert attrId < locRegisteredAttrs.length;

        OperationContextAttribute<Message> attr =
            (OperationContextAttribute<Message>)locRegisteredAttrs[attrId++];

        assert attr != null;

        ctxRestorer.add(attr, attrVal);
    }

    return ctxRestorer.restore();
}
```

The wire format is a **positional** encoding: values are stored densely in `attrs[]`, and `idBitmap`
says which distributed IDs they belong to. The loop walks the bitmap and the value array in lockstep.

**Restore is full replacement, not overlay — including the null case.** `Restorer.restore()`
([01.2](01-concepts.md#12-operationcontext--the-store)) swaps the thread's *entire* context for a
fresh single-`Update` chain containing exactly the received attributes. Within the scope, every
attribute that was **not** in the message — distributed or local — reads its `initialValue()`. This is
the fix for the overlay bug found in review (a message carrying only attribute 1 no longer inherits
attribute 2 from whatever the receiving thread was doing —
[found_problems #1/#2](found_problems_in_russian.md)). The `snp == null` branch got the same treatment
in the follow-up `Fixed null snapshot problem` commit (`13b506e028c`): `Restorer.restoreEmpty()` swaps
the context to empty, so a message whose sender had a *fully default* context no longer inherits the
executing thread's context ([05.11](05-context-loss.md#511-empty-snapshot-restores-are-a-noop--fixed)).

Correctness of the decode depends entirely on the two nodes having *identical* ID→attribute mappings.
The `assert attrId < locRegisteredAttrs.length` is the only guard, and it is an assertion.

## 3.2 Distributed attribute IDs

`DistributedAttributeRegistry` is the cluster-wide allocation table — deliberately a hand-maintained
list of constants, not a dynamic registry:

```java
public class DistributedAttributeRegistry {
    /** Reserved for {@link SecurityContext} propagation. */
    public static final byte SECURITY = 0;
}
```

Currently exactly one distributed attribute exists. The budget is 8 (`Byte.SIZE`), fixed by the
`byte idBitmap` field on the wire.

Contrast with local `OperationContextAttribute` IDs, which come from a JVM-local `AtomicInteger` and
depend on class-init order — meaningless across nodes. The distributed ID is a separate, explicit,
stable number precisely because of that.

## 3.3 The wire format — `OperationContextSnapshotMessage`

```java
public class OperationContextSnapshotMessage implements Message {
    /** Values of operation context attributes. */
    @Order(0)
    Message[] attrs;

    /** Bitmap of effective attributes ids. */
    @Order(1)
    byte idBitmap;
}
```

Standard Ignite `@Order`-annotated message; the serializer is generated
(`OperationContextSnapshotMessageSerializer`) and the type is registered in `CoreMessagesProvider`.
Instances are built via the nested `Builder` (`add(attrId, attrVal)` asserts against duplicate IDs;
`build()` is only reachable when at least one attribute was added, so an instance on the wire is never
empty — `restoreSnapshot` asserts `idBitmap != 0`).

> `@Order` fields must be contiguous `0..n-1` per class — renumber if a field is ever removed.

## 3.4 Transport integration point 1: Communication (`GridIoManager`)

**Send** — every outgoing message is built through one factory method, and the snapshot is now a
constructor argument (`GridIoManager:2041`):

```java
public GridIoMessage createGridIoMessage(Object topic, Message msg, byte plc,
                                         boolean ordered, long timeout, boolean skipOnTimeout) {
    return new GridIoMessage(plc, topic, msg, ordered, timeout, skipOnTimeout,
        ctx.operationContextDispatcher().createSnapshot());
}
```

The carrier field on `GridIoMessage` (package-private since the refactoring):

```java
@Nullable OperationContextSnapshotMessage opCtxSnp;
```

**Receive** — one restore, wrapping all message dispatch (`GridIoManager:462`):

```java
getSpi().setListener(commLsnr = new CommunicationListenerEx<>() {
    @Override public void onMessage(UUID nodeId, Object msg, IgniteRunnable msgC) {
        try {
            GridIoMessage msg0 = (GridIoMessage)msg;

            try (Scope ignored = ctx.operationContextDispatcher().restoreSnapshot(msg0.opCtxSnp)) {
                onMessage0(nodeId, msg0, msgC);
            }
        }
        …
    }
```

Because this sits at the very top of the receive path, every communication message handler on the node
runs with the sender's context restored — and because the pool executors are context-aware
([02](02-intra-node-propagation.md)), the context survives the subsequent hop into a striped or system
pool thread.

**Ordered (buffered) messages** get a second restore point. Ordered messages that cannot be delivered
immediately are parked in a `GridCommunicationMessageSet` and drained later — possibly by a different
thread carrying a different context. Since the refactoring, `unwind` (`GridIoManager:3795`) restores
each buffered message's own snapshot around its listener invocation, mirroring the tracing span:

```java
for (OrderedMessageContainer mc = msgs.poll(); mc != null; mc = msgs.poll()) {
    try (
        Scope ignored0 = ctx.operationContextDispatcher().restoreSnapshot(mc.message.opCtxSnp);
        TraceSurroundings ignore = support(ctx.tracing().create(COMMUNICATION_ORDERED_PROCESS, mc.parentSpan))
    ) {
        …
        invokeListener(plc, lsnr, nodeId, mc.message.message());
    }
    …
}
```

This closed the strongest finding of the component scan
([07 · F1](07-loss-candidate-scan.md#f1--ordered-communication-messages--fixed)); the regression test is
`testPostponedCommunicationOrderedMessage`. A buffered message with `opCtxSnp == null` resets the
context to defaults ([05.11](05-context-loss.md#511-empty-snapshot-restores-are-a-noop--fixed)), so it
does not inherit the drain thread's context either.

`invokeListener` itself adds a security-specific floor (`GridIoManager:1832`): if the thread reaches
listener dispatch with a **default** security context, it runs the listener under the *sender node's*
subject via `withContext(nodeId)` — see [04.5](04-security-attribute.md#45-listener-level-fallback--withremotesecuritycontext).

Historically all of this was done by a dedicated `GridIoSecurityAwareMessage` wrapper class;
IGNITE-28753 deleted it and folded the behaviour into the generic `opCtxSnp` field.

## 3.5 Transport integration point 2: TCP Discovery

Discovery is harder than communication: messages travel around a ring, are re-sent by intermediate
nodes, are buffered and replayed, and generate acknowledgements. The context belongs to the
**originator**, so it must survive all of that.

The carrier is on the common base class, `TcpDiscoveryAbstractMessage`:

```java
public @Nullable OperationContextSnapshotMessage opCtxSnp;
…
opCtxSnp = msg.opCtxSnp;      // line 109 — copy constructor preserves it
```

That copy-constructor line is what keeps the context attached when the ring re-wraps or forwards a
message (the copy constructor also copies `flags`, which matters below). Attachment goes through a
guarded setter (line 240):

```java
public void attachOperationContextSnapshot(@Nullable OperationContextSnapshotMessage opCtxSnp) {
    if (!getFlag(OP_CTX_ATTACHED_FLAG_POS)) {
        this.opCtxSnp = opCtxSnp;

        setFlag(OP_CTX_ATTACHED_FLAG_POS, true);
    }
}
```

The **first-attach-wins guard is load-bearing**: a message that is re-enqueued locally
(pending-message replay, ensured-delivery retry) keeps its *original* envelope instead of being
restamped with whatever context the replaying thread happens to carry — that overwrite was review
finding [#4](found_problems_in_russian.md). The guard is a dedicated **flag bit**
(`OP_CTX_ATTACHED_FLAG_POS = 3`, a previously unused slot in the serialized `flags` word), not a null
check on the field — so "attached a legitimately empty context" is remembered too, survives the wire,
and survives the copy constructor. An earlier if-null version of this guard would have restamped
default-context messages on replay.

`TcpDiscoveryImpl` holds the dispatcher (`protected final OperationContextDispatcher
operationCtxDispatcher`, line 144) so both `ServerImpl` and `ClientImpl` can use it.

### `ServerImpl` — four integration points

| Line | Direction | What |
|---|---|---|
| 3024 | send | `msg.attachOperationContextSnapshot(operationCtxDispatcher.createSnapshot())` in `RingMessageWorker.addMessage`, applied to **every** locally originated message (`!fromSocket`), not just custom events |
| 3296 | receive | restore around ring-message processing (`processMessage`) |
| 6189 | send | `ackMsg.attachOperationContextSnapshot(…)` — **acknowledgement** messages (IGNITE-28902) |
| 6317 | receive | restore around processing of **postponed/pending** custom messages (IGNITE-28915) |

The pending-message restore is the clearest illustration of the failure mode. Before:

```java
while ((msg = pollPendingCustomMessage()) != null)
    processCustomMessage(msg, true);
```

After:

```java
while ((msg = pollPendingCustomMessage()) != null) {
    try (Scope ignored = operationCtxDispatcher.restoreSnapshot(msg.opCtxSnp)) {
        processCustomMessage(msg, true);
    }
}
```

Custom discovery messages received while a node join is in progress are parked in a pending queue and
replayed by the coordinator once `joiningNodes` empties. The message object retained its snapshot
correctly — nobody was *reading* it on the replay path. The context was silently the ring worker's for
every postponed message.

`checkPendingCustomMessages()` is also reached from `processNodeAddFinishedMessage` /
`processNodeLeftMessage` / `processNodeFailedMessage`, i.e. from *inside* the `:3296` scope of the
topology message being processed. With full-replacement restore semantics a pending message carrying a
snapshot cleanly displaces that outer context, and since `13b506e028c` a pending message with
`opCtxSnp == null` resets it to defaults — neither case leaks the topology message's context
([05.11](05-context-loss.md#511-empty-snapshot-restores-are-a-noop--fixed)).

### `ClientImpl` — two integration points

| Line | Direction | What |
|---|---|---|
| 1314 | send | `msg.attachOperationContextSnapshot(operationCtxDispatcher.createSnapshot())` in `SocketWriter.sendMessage` — every client-originated message |
| 2151 | receive | `processDiscoveryMessage` is itself the restore boundary |

The receive side was restructured by the refactoring (review finding
[#3](found_problems_in_russian.md)):

```java
protected void processDiscoveryMessage(TcpDiscoveryAbstractMessage msg) {
    try (Scope ignored = operationCtxDispatcher.restoreSnapshot(msg.opCtxSnp)) {
        processDiscoveryMessage0(msg);
    }
}
```

Making the per-message dispatch method the boundary means the **client-reconnect replay** is covered
for free: `processClientReconnectMessage` iterates `msg.pendingMessages()` (`:2562`, `:2572`) and
routes each pending message through `processDiscoveryMessage`, so each replayed message runs under its
*own* transported context rather than the reconnect container's.

## 3.6 Transport integration point 3: ZooKeeper Discovery

`ZookeeperDiscoverySpi` has no common base class for its messages, so it cannot add a field the way
`TcpDiscoveryAbstractMessage` does. Instead it uses a **decorator**:

```java
public class ZkOperationContextAwareCustomMessage implements DiscoverySpiCustomMessage {
    DiscoverySpiCustomMessage delegate;
    OperationContextSnapshotMessage opCtxSnp;

    @Override public DiscoverySpiCustomMessage ackMessage() {
        return ack == null ? null : new ZkOperationContextAwareCustomMessage(ack, opCtxSnp);
    }
}
```

Note that `ackMessage()` re-wraps the ack with the *same* `opCtxSnp` — the ZK equivalent of the
`ServerImpl:6189` ack fix.

Send side (`ZookeeperDiscoveryImpl:672`):

```java
OperationContextSnapshotMessage opCtxSnp = opCtxDispatcher.createSnapshot();

if (opCtxSnp != null)
    sendCustomMessage(new ZkOperationContextAwareCustomMessage(msg, opCtxSnp));
```

The wrapper is allocated **only when there is context to carry** — the `null` return from
`createSnapshot` means the original message goes out undecorated.

Receive side (`ZookeeperDiscoveryImpl:3531-3551`) unwraps and restores:

```java
OperationContextSnapshotMessage opCtxSnp = null;

if (msg instanceof ZkOperationContextAwareCustomMessage) {
    opCtxSnp = ((ZkOperationContextAwareCustomMessage)msg).opCtxSnp;
    msg = ((ZkOperationContextAwareCustomMessage)msg).delegate;
}
…
try (Scope ignored = opCtxDispatcher.restoreSnapshot(opCtxSnp)) { … }
```

`ZkDiscoveryCustomEventData` carries a comment noting its unmarshalled message holder "can be wrapped
with `ZkOperationContextAwareCustomMessage`" — anything that reads that holder must unwrap.

## 3.7 Summary of the distributed path

```
   sender thread                                       receiver node
   ─────────────                                       ─────────────
   OperationContext (ThreadLocal)
        │
        │ dispatcher.createSnapshot()
        ▼
   OperationContextSnapshotMessage{ idBitmap, Message[] attrs }
        │
        │ attached to carrier:
        │   • GridIoMessage.opCtxSnp                  (communication, incl. buffered ordered msgs)
        │   • TcpDiscoveryAbstractMessage.opCtxSnp    (TCP discovery, incl. acks & pending)
        │   • ZkOperationContextAwareCustomMessage    (ZK discovery, decorator)
        ▼
   ══════════════════ network ══════════════════▶
                                                       dispatcher.restoreSnapshot(snp)
                                                            │  (full replacement; null → reset to defaults)
                                                            ▼
                                                   try (Scope) { process(msg) }
                                                            │
                                                            ▼
                                              context-aware pools/futures carry it onward
```

---

Next: [04 — The Security attribute →](04-security-attribute.md)
