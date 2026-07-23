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
public @Nullable OperationContextMessage collectDistributedAttributeValues() {
    …
    for (int id = 0; id < locRegisteredAttrs.length; id++) {
        OperationContextAttribute<? extends Message> attr = locRegisteredAttrs[id];
        if (attr == null) continue;

        Message curVal = OperationContext.get(attr);

        if (curVal == attr.initialValue()) continue;   // (!) nothing to propagate

        vals.add(curVal);
        bitmap |= (byte)(1 << id);
    }

    return bitmap == 0 ? null : new OperationContextMessage(bitmap, vals.toArray(Message[]::new));
}
```

Two things to internalise:

- **The empty context costs nothing on the wire.** No registered attributes, or every attribute at its
  initial value → returns `null` → the carrier message's `opCtxMsg` field stays `null` → one null flag
  on the wire.
- **"Initial value" means "not propagated."** Comparison is `==` against `attr.initialValue()`. An
  attribute deliberately set back to its initial value is indistinguishable from unset, and the remote
  node will fall back to *its own* default. For security this is the intended semantic (see
  [04](04-security-attribute.md)) but it is a semantic trap for future attributes.

### Restore (receiver side)

```java
public Scope restoreRemoteAttributeValues(@Nullable OperationContextMessage msg) {
    if (msg == null)
        return Scope.NOOP_SCOPE;
    …
    OperationContext.ContextUpdater updater = OperationContext.ContextUpdater.create();

    for (byte valIdx = 0, attrId = 0; valIdx < msg.attrs.length; ++valIdx) {
        Message curVal = msg.attrs[valIdx];

        while ((msg.idBitmap & (1 << attrId)) == 0)
            ++attrId;

        assert attrId < locRegisteredAttrs.length;

        OperationContextAttribute<Message> attr =
            (OperationContextAttribute<Message>)locRegisteredAttrs[attrId++];

        updater.set(attr, curVal);
    }

    return updater.apply();
}
```

The wire format is a **positional** encoding: values are stored densely in `attrs[]`, and `idBitmap`
says which distributed IDs they belong to. The loop walks the bitmap and the value array in lockstep.
All values land in a single `Update` node via `ContextUpdater`, so the returned `Scope` unwinds the
whole remote context in one step.

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

## 3.3 The wire format — `OperationContextMessage`

```java
public class OperationContextMessage implements Message {
    /** Values of operation context attributes. */
    @Order(0)
    Message[] attrs;

    /** Bitmap of effective attributes ids. */
    @Order(1)
    byte idBitmap;
}
```

Standard Ignite `@Order`-annotated message; the serializer is generated
(`OperationContextMessageSerializer`) and the type is registered in `CoreMessagesProvider`.

> `@Order` fields must be contiguous `0..n-1` per class — renumber if a field is ever removed.

## 3.4 Transport integration point 1: Communication (`GridIoManager`)

**Send** — every outgoing message is built through one factory method, which is where collection
happens (`GridIoManager:2054`):

```java
public GridIoMessage createGridIoMessage(Object topic, Message msg, byte plc,
                                         boolean ordered, long timeout, boolean skipOnTimeout) {
    GridIoMessage res = new GridIoMessage(plc, topic, msg, ordered, timeout, skipOnTimeout);

    res.opCtxMsg = ctx.operationContextDispatcher().collectDistributedAttributeValues();

    return res;
}
```

The carrier field on `GridIoMessage`:

```java
public @Nullable OperationContextMessage opCtxMsg;
```

**Receive** — one restore, wrapping all message dispatch (`GridIoManager:462`):

```java
getSpi().setListener(commLsnr = new CommunicationListenerEx<>() {
    @Override public void onMessage(UUID nodeId, Object msg, IgniteRunnable msgC) {
        try {
            GridIoMessage msg0 = (GridIoMessage)msg;

            try (Scope ignored = ctx.operationContextDispatcher()
                                    .restoreRemoteAttributeValues(msg0.opCtxMsg)) {
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

Historically this was done by a dedicated `GridIoSecurityAwareMessage` wrapper class; IGNITE-28753
deleted it and folded the behaviour into the generic `opCtxMsg` field.

## 3.5 Transport integration point 2: TCP Discovery

Discovery is harder than communication: messages travel around a ring, are re-sent by intermediate
nodes, are buffered and replayed, and generate acknowledgements. The context belongs to the
**originator**, so it must survive all of that.

The carrier is on the common base class, `TcpDiscoveryAbstractMessage`:

```java
public @Nullable OperationContextMessage opCtxMsg;
…
opCtxMsg = msg.opCtxMsg;      // line 109 — copy constructor preserves it
```

That copy-constructor line is what keeps the context attached when the ring re-wraps or forwards a
message.

`TcpDiscoveryImpl` holds the dispatcher (`protected final OperationContextDispatcher
operationCtxDispatcher`, line 144) so both `ServerImpl` and `ClientImpl` can use it.

### `ServerImpl` — four integration points

| Line | Direction | What |
|---|---|---|
| 3024 | send | `msg.opCtxMsg = operationCtxDispatcher.collectDistributedAttributeValues();` — outgoing custom message |
| 3296 | receive | restore around custom-message processing |
| 6189 | send | `ackMsg.opCtxMsg = …` — **acknowledgement** messages (IGNITE-28902) |
| 6317 | receive | restore around processing of **postponed/pending** custom messages (IGNITE-28915) |

The last one is the most recent fix and the clearest illustration of the failure mode. Before:

```java
while ((msg = pollPendingCustomMessage()) != null)
    processCustomMessage(msg, true);
```

After:

```java
while ((msg = pollPendingCustomMessage()) != null) {
    try (Scope ignored = operationCtxDispatcher.restoreRemoteAttributeValues(msg.opCtxMsg)) {
        processCustomMessage(msg, true);
    }
}
```

Custom discovery messages received while a node join is in progress are parked in a pending queue and
replayed by the coordinator once `joiningNodes` empties. The message object retained `opCtxMsg`
correctly — nobody was *reading* it on the replay path. The context was silently the ring worker's
(i.e. empty) for every postponed message.

### `ClientImpl` — two integration points

| Line | Direction | What |
|---|---|---|
| 1314 | send | `msg.opCtxMsg = operationCtxDispatcher.collectDistributedAttributeValues();` |
| 1767 | receive | `restoreRemoteAttributeValues(dm == null ? null : dm.opCtxMsg)` |

## 3.6 Transport integration point 3: ZooKeeper Discovery

`ZookeeperDiscoverySpi` has no common base class for its messages, so it cannot add a field the way
`TcpDiscoveryAbstractMessage` does. Instead it uses a **decorator**:

```java
public class ZkOperationContextAwareCustomMessage implements DiscoverySpiCustomMessage {
    DiscoverySpiCustomMessage delegate;
    OperationContextMessage opCtxMsg;

    @Override public DiscoverySpiCustomMessage ackMessage() {
        return ack == null ? null : new ZkOperationContextAwareCustomMessage(ack, opCtxMsg);
    }
}
```

Note that `ackMessage()` re-wraps the ack with the *same* `opCtxMsg` — the ZK equivalent of the
`ServerImpl:6189` ack fix.

Send side (`ZookeeperDiscoveryImpl:672`):

```java
OperationContextMessage opCtx = opCtxDispatcher.collectDistributedAttributeValues();

if (opCtx != null)
    sendCustomMessage(new ZkOperationContextAwareCustomMessage(msg, opCtx));
```

The wrapper is allocated **only when there is context to carry** — the `null` return from `collect`
means the original message goes out undecorated.

Receive side (`ZookeeperDiscoveryImpl:3531-3551`) unwraps and restores:

```java
OperationContextMessage opCtxMsg = null;

if (msg instanceof ZkOperationContextAwareCustomMessage) {
    opCtxMsg = ((ZkOperationContextAwareCustomMessage)msg).opCtxMsg;
    msg = ((ZkOperationContextAwareCustomMessage)msg).delegate;
}
…
try (Scope ignored = opCtxDispatcher.restoreRemoteAttributeValues(opCtxMsg)) { … }
```

`ZkDiscoveryCustomEventData` carries a comment noting its unmarshalled message holder "can be wrapped
with `ZkOperationContextAwareCustomMessage`" — anything that reads that holder must unwrap.

## 3.7 Summary of the distributed path

```
   sender thread                                       receiver node
   ─────────────                                       ─────────────
   OperationContext (ThreadLocal)
        │
        │ collectDistributedAttributeValues()
        ▼
   OperationContextMessage{ idBitmap, Message[] attrs }
        │
        │ attached to carrier:
        │   • GridIoMessage.opCtxMsg                  (communication)
        │   • TcpDiscoveryAbstractMessage.opCtxMsg    (TCP discovery, incl. acks & pending)
        │   • ZkOperationContextAwareCustomMessage    (ZK discovery, decorator)
        ▼
   ══════════════════ network ══════════════════▶
                                                       restoreRemoteAttributeValues(msg)
                                                            │
                                                            ▼
                                                   try (Scope) { process(msg) }
                                                            │
                                                            ▼
                                              context-aware pools/futures carry it onward
```

---

Next: [04 — The Security attribute →](04-security-attribute.md)
