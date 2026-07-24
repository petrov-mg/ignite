[← Index](README.md) | Prev: [Custom message flow](08-custom-message-flow.md)

# 09 — GridIoMessage Flow: Context Audit

A full walkthrough of communication message processing in `GridIoManager`, tracing where the
operation context is captured, carried, restored — and where it can still go wrong. Audited
2026-07-24 against the IGNITE-28915 branch head. Line numbers refer to that revision.

**Verdict up front.** The `GridIoMessage` path is in better shape than discovery was: capture has a
*single* funnel, restore has a *single* boundary, and every buffering/replay/fallback path re-enters
one of the two. Exactly **one live gap** remains — the channel-open path
([9.5 · P1](#p1--the-channel-path-never-restores)) — plus known-class hazards with no new instances.

## 9.1 Lifecycle overview

```
 sender (any node)                             receiver node
 ─────────────────                             ─────────────
 caller thread
   send*() overloads ──┐
   openChannel() ──────┤                       NIO thread
                       ▼                         commLsnr.onMessage():458
        createGridIoMessage():2042                 └─ restoreSnapshot(msg0.opCtxSnp):462
          └─ snapshot captured as a                     └─ onMessage0():1199
             constructor argument                            ├─ P2P_POOL      → processP2PMessage():1300
                       │                                     ├─ regular       → processRegularMessage():1348
        ┌──────────────┴─────────────┐                       ├─ ordered       → processOrderedMessage():1618
        │ local destination?          │                      └─ pre-start     → waitMap stash :1226
        │  yes → process inline on    │                              (replayed through commLsnr.onMessage :1025)
        │  the sender thread :1994    │
        │  no  → SPI → network        │        pool handoff: context-aware executors capture the
        └────────────────────────────┘         NIO thread's restored context at submit
```

Two invariants carry the whole design:

1. **One construction site.** `createGridIoMessage:2050` is the only production
   `new GridIoMessage(...)`; the snapshot is a constructor argument, captured on whatever thread
   calls the public `send*` API. Nothing can send an un-enveloped message.
2. **One receive boundary.** `:462` wraps all of `onMessage0`; every deferred path below either
   re-enters it or restores per item.

## 9.2 Send side

| Path | Capture |
|---|---|
| all `send*` overloads → `send():1992` | `createGridIoMessage` on the caller thread |
| `openChannel():1937` | same — the channel-init message carries a snapshot too |
| responses sent from message handlers | handler thread runs under the restored request context → the response naturally carries the requester's context back (correct: the operation continues) |

**Local-node short-circuit** (`send():1994-2008`). Local destinations bypass the `:462` boundary and
call `processOrderedMessage` / `processRegularMessage` / `processRegularMessage0` directly on the
sender's thread. Sound by construction: at that moment the sender's *live* context equals the
attached snapshot, and every onward hop (context-aware pool submit, ordered set + `unwind`) captures
or restores it.

## 9.3 Receive side — every dispatch and deferral path

| Path | Context handling | Status |
|---|---|---|
| `onMessage:458-462` | `restoreSnapshot(msg0.opCtxSnp)` around all of `onMessage0`; full replacement, `null` → reset to defaults | boundary |
| pre-start stash `waitMap:1226` | messages parked before the manager is `started`; replayed at `:1025` **through `commLsnr.onMessage`** — i.e. back through the full boundary per message | covered |
| P2P pool `:1330`, `poolForPolicy:1429`, custom executors `:1416`, striped `:1391`, data-streamer striped `:1397` | context-aware executors capture the NIO thread's restored context at submit (`IgniteStripedExecutor:834` wraps via `wrapIfContextNotEmpty`) | covered |
| `RejectedExecutionException` fallbacks `:1337/:1436` | `c.run()` inline — still inside the `:462` scope | covered |
| `CALLER_THREAD` policy, `processFromNioThread` | inline execution inside the scope | covered |
| ordered buffering `processOrderedMessage:1618` → `GridCommunicationMessageSet` | the set stores the `GridIoMessage` itself, envelope intact, kept until a listener registers; **every** drain path — inline winner, deferred pool submit, listener-registration replay, timeout worker, disconnect — funnels into `unwind:3798` with a per-message `restoreSnapshot` (F1 fix, incl. null → reset) | covered |
| regular message, no listener `:1450` | dropped — no deferral | no vector |
| `invokeListener:1822` | `withRemoteSecurityContext` floor: a context-less message is attributed to the *sender node's* subject (security only) | covered |
| SPI layer (`TcpCommunicationSpi`) | NIO workers move bytes; recovery/reconnect resends the same serialized message, envelope intact; the only processing entry is the listener behind `:462` | covered |
| channel open `onChannelOpened:478` → `onChannelOpened0:1161` | **no restore anywhere** | **P1 below** |

## 9.4 Contrast with discovery

Where discovery needed the attached-flag to survive replays and forwarding, communication needs
neither: messages are point-to-point (no intermediate re-wrapping), the envelope is set exactly once
in the constructor, and no replay path mutates it. The entire correctness argument reduces to the
two invariants in 9.1 — which is why the audit found only one gap.

## 9.5 Remaining problems

### P1 — The channel path never restores

**`GridIoManager.onChannelOpened:478` / `onChannelOpened0:1161`.** The one live gap; this sharpens
the long-known [05.5](05-context-loss.md#55-transports-that-carry-no-carrier-field) item with the
exact mechanics. The sender *does* attach a snapshot to the channel-init message
(`openChannel` → `createGridIoMessage:1937`) — but the receiver never reads it:

```java
@Override public void onChannelOpened(UUID rmtNodeId, Object initMsg, Channel channel) {
    // no restoreSnapshot here, unlike onMessage right above          :478
    onChannelOpened0(rmtNodeId, (GridIoMessage)initMsg, channel);
}

private void onChannelOpened0(UUID rmtNodeId, GridIoMessage initMsg, Channel channel) {
    …
    pools.poolForPolicy(plc).execute(new Runnable() {                 // :1178
        @Override public void run() {
            processOpenedChannel(initMsg.topic(), rmtNodeId, …);      // initMsg.opCtxSnp ignored
        }
    });
}
```

The submitting NIO thread is **outside any scope**, so the context-aware pool dutifully captures an
*empty* context. Every channel listener and `TransmissionHandler` (snapshot transfer, file-based
rebalance) runs the entire transmission lifetime without the initiator's context.

The data is already on the wire. The fix mirrors `:462` — open the scope inside the submitted
runnable:

```java
@Override public void run() {
    try (Scope ignored = ctx.operationContextDispatcher().restoreSnapshot(initMsg.opCtxSnp)) {
        processOpenedChannel(initMsg.topic(), rmtNodeId, (SessionChannelMessage)initMsg.message(),
            (SocketChannel)channel);
    }
}
```

Registered as [F12](07-loss-candidate-scan.md#712-third-sweep-gridiomessage-flow-2026-07-24).

### P2 — Handler-origin traffic inherits the request context (design hazard, no live instance)

The communication twin of discovery's
[05.12](05-context-loss.md#512-derived-messages-inherit-the-in-scope-context): any message a handler
sends while processing user A's message is stamped with A's context by `createGridIoMessage`'s
capture-current-thread semantics. For **responses** that is exactly right — the operation continues
under its subject (`GridIoManager`'s own `TOPIC_IO_TEST` auto-response intentionally echoes the
requester's context). The hazard is *unrelated* traffic triggered en passant from a handler (e.g. a
rebalance demand). No harmful instance exists inside `GridIoManager` itself; this is the
review-checklist rule ("decide explicitly whose context a derived message carries"), not a defect.

### P3 — `wrapIfContextNotEmpty` invariant reliance (known class)

The striped executor's fast path skips the wrapper when the submitting thread's context is empty
(`IgniteStripedExecutor:834`). Correct only while stripe threads never hold a leaked scope
([05.1](05-context-loss.md#51-scope-discipline) failure mode B compounding
[05.4](05-context-loss.md#54-capture-time--submit-time)). Not a new finding — but the communication
path is where this invariant carries the most traffic (all partitioned `SYSTEM_POOL` cache messages).

### P4 — Mixed-version peer (known class)

An old-version peer has no `opCtxSnp` field on `GridIoMessage`, so the context is dropped on that
hop. Unlike discovery ([08 · P2](08-custom-message-flow.md#p2--mixed-version-ring-rolling-upgrade))
there is no restamping concern — communication is point-to-point with no intermediate re-wrapping —
so it is pure loss, bounded to the rolling-upgrade window
([05.8](05-context-loss.md#58-asymmetric-attribute-registration)).

---

[← Index](README.md)
