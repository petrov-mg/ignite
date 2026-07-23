# Operation Context (IEP-143) — Documentation Index

Reference documentation for the **Unified Operation Context Propagation** mechanism
([IEP-143](https://cwiki.apache.org/confluence/spaces/IGNITE/pages/406620460/IEP-143+Unified+Operation+Context+Propagation),
status DRAFT, created 2026-02-04).

## What it is, in one paragraph

Ignite attaches per-operation metadata (most importantly `SecurityContext`) to the thread executing
that operation. Historically each subsystem — Cache, Security, Tracing, Compute — kept its own
`ThreadLocal` and its own ad-hoc propagation code. IEP-143 replaces all of them with a single
thread-bound store, `OperationContext`, plus a set of wrappers that automatically carry that store
across thread boundaries (thread pools, futures, worker queues) and across node boundaries
(communication and discovery messages). The design goal is that a developer never has to think about
propagation: using the Ignite-provided executor / future / thread class is enough.

## Documents

| Document | Contents |
|---|---|
| [01 — Core concepts and data model](01-concepts.md) | `OperationContext`, `OperationContextAttribute`, `Scope`, `OperationContextSnapshot`, the immutable `Update` chain, bitmask lookup, capacity limits |
| [02 — Intra-node propagation](02-intra-node-propagation.md) | Thread pools, `IgniteThread`, futures (`GridFutureAdapter`, `IgniteCompletableFuture`), functional wrappers, async queue handlers, the Checkstyle enforcement rules |
| [03 — Cross-node propagation](03-cross-node-propagation.md) | `OperationContextDispatcher`, `OperationContextMessage`, `DistributedAttributeRegistry`, the communication path, the TCP discovery path, the ZooKeeper discovery path |
| [04 — The Security attribute](04-security-attribute.md) | `SecurityContext` as the first and currently only distributed attribute; `SecurityContextWrapper`; subject re-resolution on the receiving node |
| [05 — How context gets lost](05-context-loss.md) | **The important one.** Every known loss vector, ranked, with the code that causes it and how it is (or is not) defended against |
| [06 — Tickets and change history](06-tickets.md) | IEP-143 subtask breakdown, what each commit landed, current state and remaining work |
| [07 — Component scan: candidate loss sites](07-loss-candidate-scan.md) | Results of a directed sweep of Ignite components for unguarded queue/thread handoffs — eight candidate sites, ranked, plus verified-clean negative results |

## Package layout

```
modules/commons/…/internal/thread/context/
├── OperationContext.java             — thread-bound store (the ThreadLocal)
├── OperationContextAttribute.java    — typed key, bitmask-identified
├── OperationContextSnapshot.java     — opaque marker for a captured state
├── Scope.java                        — AutoCloseable undo token
├── concurrent/                       — executor & CompletableFuture wrappers
└── function/                         — Runnable/Callable/Function/… wrappers

modules/core/…/internal/thread/context/
├── OperationContextDispatcher.java   — distributed attribute registry + collect/restore
├── OperationContextMessage.java      — the wire format
└── DistributedAttributeRegistry.java — reserved cluster-wide attribute IDs
```

Note the module split: the *local* machinery lives in `commons` (available to thin clients and
low-level utilities); the *distributed* machinery lives in `core` because it depends on `Message`
serialization and `GridKernalContext`.
