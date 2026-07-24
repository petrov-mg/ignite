[← Index](README.md) | Prev: [Component scan](07-loss-candidate-scan.md) | Next: [GridIoMessage flow →](09-gridio-message-flow.md)

# 08 — Custom Discovery Message Flow: Context Audit

A full walkthrough of custom-message processing in `ServerImpl` and `ClientImpl` (TCP discovery),
tracing where the operation context is captured, carried, restored — and where it can still go
wrong. Audited 2026-07-24 against the IGNITE-28915 branch head. Line numbers refer to that revision.

**Verdict up front.** The custom-message pipeline proper is watertight: every place a custom message
is *processed* sits inside a `restoreSnapshot` boundary, and every place one is *created or replayed*
attaches its snapshot exactly once (guarded by the serialized attached-flag). The remaining defects
are at the edges: **derived** messages forged inside a foreign restore scope, **mixed-version** rings,
and one client-side API that never captures. See [8.5](#85-remaining-problems).

## 8.1 Lifecycle overview

```
 originator (server)                     ring                         every node
 ───────────────────                     ────                         ──────────
 user thread
   sendCustomEvent():1091
     └─ msgWorker.addMessage()           each hop:                    processMessage():3296
          └─ attach(createSnapshot())      copy-ctor keeps              └─ restoreSnapshot(opCtxSnp)
             :3024, caller thread          opCtxSnp + flags :109           └─ processCustomMessage():6151
                                                                              ├─ coordinator, unverified:
 originator (client)                                                          │    verify → notify → send on
 ───────────────────                                                          ├─ coordinator, verified:
 user thread                                                                  │    discard :6177 → ack :6182-6191
   sendCustomEvent():493                                                      ├─ non-coordinator:
     └─ sockWriter.sendMessage():521                                          │    notify :6214 → send on
          └─ attach(createSnapshot())                                         └─ join in progress:
             :1314, caller thread                                                  park in pendingCustomMsgs :6239
```

The context is captured **once, on the thread that calls the public API**, sealed into the message by
the attached-flag, and re-established on every node around the whole of message processing.

## 8.2 Server side (`ServerImpl`) — every integration point

### Capture and carriage

| Site | What happens |
|---|---|
| `sendCustomEvent:1111` → `addMessage:3023` | `attach(createSnapshot())` for every `!fromSocket` message — runs on the **caller** thread, so the user's context is captured at the API boundary |
| `TcpDiscoveryAbstractMessage` copy ctor `:107-113` | copies `opCtxSnp` **and** `flags` — forwarding/re-wrapping on the ring preserves both the envelope and the attached-flag |
| `attachOperationContextSnapshot:240` | first-attach-wins via serialized flag bit `OP_CTX_ATTACHED_FLAG_POS = 3` — replays can never restamp, even when the original envelope is legitimately `null` |
| SocketReader `:7276` | `addMessage(msg, false, true)` — `fromSocket=true`, no attach; the wire envelope is authoritative |
| Router re-adds `:7543/:7546/:7563` | two-arg `addMessage` (attach runs) but incoming client messages already carry the flag → no-op |

### Restore boundaries

| Site | What is restored |
|---|---|
| `processMessage:3292-3299` | `restoreSnapshot(msg.opCtxSnp)` around **all** ring message types — full replacement; `null` → reset to defaults |
| `checkPendingCustomMessages:6306-6322` | per-message restore when draining `pendingCustomMsgs` — the original IGNITE-28915 fix; also reached from `processNodeAddFinished/Left/FailedMessage` *inside* the topology message's scope, where full-replace/reset semantics prevent inheritance |
| `notifyDiscoveryListener:6337` | no scope of its own — runs synchronously on the ring worker inside the message's scope; `GridDiscoveryManager`'s notifier queue captures a snapshot per enqueued item, so the context survives the hop to the notifier worker |

### Replay and derivation paths — all verified

- **Postpone/drain** (`postponeUndeliveredMessages:6228` → `:6317`): parked message keeps its envelope;
  drain restores per message.
- **Local replay** (`processPendingMessagesLocally:4069`): re-add through `addMessage`; the
  attached-flag makes the attach a no-op, preserving the original envelope.
- **No-next-node re-add** (`:3438`): same flag protection.
- **NodeAdded embedding** (`prepareNodeAddedMessage:1917`, consumed at `pendingMsgs.reset:5190`):
  embedded pending messages are bookkeeping only on the joining node — never processed directly;
  each carries its own serialized envelope for any later replay.
- **Ack** (`:6182-6191`): the ack for a verified custom message is created *inside* the original
  message's restore scope and `attach(createSnapshot())` deliberately captures the **originator's**
  context — the IGNITE-28902 semantics. Processed recursively under the same (matching) scope, and on
  other nodes under its own envelope.
- **Discard** (`:6177`): created inside the same scope — inherits the originator's context
  (benign today; see [8.5](#85-remaining-problems)).
- **Idle-loop work** (`noMessageLoop:3319-3332` — metrics, status checks, `checkFailedNodesList`):
  runs between messages with a clean context; derived messages get `attach(null)` + flag.

## 8.3 Client side (`ClientImpl`) — every integration point

| Site | What happens |
|---|---|
| `sendCustomEvent:493-524` → `SocketWriter.sendMessage:1314` | `attach(createSnapshot())` on the **caller** thread — the only custom-flow send entry, correctly captured |
| MessageWorker loop `:1976-2008` | every received `TcpDiscoveryAbstractMessage` funnels into `processDiscoveryMessage` |
| `processDiscoveryMessage:2150-2154` | the restore boundary — `restoreSnapshot(msg.opCtxSnp)` around all per-type dispatch |
| `processCustomMessage:2593` → `notifyDiscovery:2687` | synchronous `lsnr.onDiscovery(...).get()` inside the scope; the notifier queue captures per item |
| Reconnect replay `:2562/:2572` | each `pendingMessages()` item routed through `processDiscoveryMessage` — restores its *own* transported context (review finding #3 fix) |
| Derived responses (ping response `:2630`, client acks) | created inside a reset/empty scope — carry no foreign context |

## 8.4 The three mechanisms that make it hold together

1. **Capture at the API boundary** — `attach(createSnapshot())` runs on the thread that calls
   `sendCustomEvent`, not on a worker.
2. **Seal with the attached-flag** — `OP_CTX_ATTACHED_FLAG_POS` is serialized and copied, so exactly
   one capture per message lifetime, across replays, forwards, and the wire; "attached empty" is
   remembered as firmly as "attached user X".
3. **Full-state restore per processing site** — `restoreSnapshot` replaces the thread context
   wholesale (non-null → exactly the received attributes; null → all defaults, since the
   IGNITE-28915 null-snapshot fix),
   so nested and sequential processing can never inherit a neighbour's context.

## 8.5 Remaining problems

Ranked; the first is the only live defect.

### P1 — Derived node-failure messages forged inside a foreign scope

**`ServerImpl.sendMessageAcrossRing` → `:3951`.** When forwarding user A's custom message and the
next ring node fails, the ring worker creates the failure notification *while still inside A's
restore scope*:

```java
for (TcpDiscoveryNode n : failedNodes)
    msgWorker.addMessage(new TcpDiscoveryNodeFailedMessage(locNodeId, n.id(), n.internalOrder()));
```

`addMessage` → `attach(createSnapshot())` stamps **A's snapshot** onto the `NodeFailedMessage`. The
whole cluster then handles that node failure — topology update, `EVT_NODE_FAILED` listeners, exchange
trigger — under user A's security context: wrong attribution at best, failed-closed authorization
inside node-failure handling at worst.

Cheap fix: `attachOperationContextSnapshot(null)` at creation — the flag semantics support explicitly
pinning "no context" so the later `addMessage` attach cannot restamp it.

Same pattern, adjacent instances:

- `:6177` — `TcpDiscoveryDiscardMessage` carries the originator's context ring-wide. Benign today
  (discard processing touches no listener and makes no authorization checks), but misleading for any
  future audit and fragile against changes to discard handling.
- `:6189` — the ack: the **intended** instance of the pattern (acks must run as the originator).

The general rule this exposes: *any message created while processing another silently inherits the
current scope via `addMessage`'s attach*. Correctness currently rests on each instance being either
intended (ack) or harmless (discard) — nothing enforces the distinction. See the
[review checklist](05-context-loss.md#514-a-review-checklist).

### P2 — Mixed-version ring (rolling upgrade)

An old-version node in the ring has neither the `opCtxSnp` field nor flag bit 3. Forwarding through
it re-creates the message without either — the context is silently dropped mid-ring, and downstream
new-version nodes may restamp the now-flagless message (with their own, typically empty, context).
There is still no join-time validation of context capability
([05.8](05-context-loss.md#58-asymmetric-attribute-registration)).

### P3 — Client `MessageWorker.addMessage(Object)` never captures

`ClientImpl.MessageWorker.addMessage:2716` performs no attach — asymmetric with the server's
`RingMessageWorker.addMessage`. Harmless for custom events (captured at `sockWriter.sendMessage` on
the caller thread), but a locally created discovery message injected through the worker queue —
`failNode():539` is the concrete case — never captures the calling user's context; by processing time
the restore boundary has reset the worker to an empty context. Server-side `failNode():1129` *does*
capture. Attribution of client-initiated node failures is lost.

### P4 — Null-snapshot scenarios have no regression tests

The mechanism (`Restorer.restoreEmpty()`, flag-based attach) is correct but unguarded:
no test sends a default-context custom message parked behind a non-default topology message, or a
default-context ordered message behind a non-default one in the same message set — the exact
deterministic scenarios from the review
([05.11](05-context-loss.md#511-empty-snapshot-restores-are-a-noop--fixed)).

### P5 — Reader-thread direct processing is outside any boundary (informational)

`SocketReader` answers handshakes and pings directly on the reader thread, outside any restore scope.
No context semantics are needed there today, but any future message handled directly on the reader
thread would bypass the boundary-per-message design silently. Treat "SocketReader processes a new
message type inline" as a review trigger.

---

[← Index](README.md)
