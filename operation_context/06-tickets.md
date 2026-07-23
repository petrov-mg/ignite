[← Index](README.md) | Prev: [How context gets lost](05-context-loss.md) | Next: [Component scan →](07-loss-candidate-scan.md)

# 06 — Tickets and Change History

IEP-143 maps onto four design steps; the implementation tickets group accordingly.

| IEP step | Tickets |
|---|---|
| 1. Unified `ThreadLocal` storage | IGNITE-26608 |
| 2. Thread pool & future integration | IGNITE-26775, IGNITE-26776, IGNITE-28681 |
| 3. Static analysis rules | IGNITE-26775 (`ClassUsageRestrictionRule`) |
| 4. Remote node propagation | IGNITE-28808, IGNITE-28753, IGNITE-28902, IGNITE-28915 |

---

## Step 1 — Core mechanism

### IGNITE-26608 — Added a unified mechanism for propagating Operation Context Attributes
`6ab4f7009cf` · PR #12429 · 2026-02-24 · 40 files, +1547/−373

Introduced the four core types in `modules/commons/…/thread/context/`:
`OperationContext` (370 lines), `OperationContextAttribute`, `OperationContextSnapshot`, `Scope`.
Established the immutable `Update` chain, bitmask attribute identity, snapshot/restore, and the
`Scope` undo protocol. See [01 — Concepts](01-concepts.md).

---

## Steps 2 & 3 — Intra-node propagation

### IGNITE-26775 — All uses of Thread Pools replaced with Operation Context aware equivalents
`82f622336da` · PR #12435 · 2026-04-23 · 65 files, +1708/−522

Two halves:

- **Enforcement.** New module rule `modules/checkstyle/…/ClassUsageRestrictionRule.java` (268 lines,
  with a 251-line test) plus its wiring in `checkstyle/checkstyle.xml` (+62/−…) and the two
  suppression files. This is IEP step 3.
- **Migration.** Every internal use of a raw JDK pool replaced with the context-aware equivalent
  (`IgniteThreadPoolExecutor`, `IgniteStripedExecutor`, `IgniteScheduledThreadPoolExecutor`,
  `IgniteForkJoinPool`).

Details: [02.2](02-intra-node-propagation.md#22-thread-pools), [02.6](02-intra-node-propagation.md#26-static-enforcement--checkstyle).

### IGNITE-26776 — Operation Context propagation integrated into `GridFutureAdapter` and `CompletableFuture`
`bf1327c6a21` · PR #12455 · 2026-04-28 · 31 files, +1068/−124

Added `IgniteCompletableFuture` (408 lines — a full delegating `Future`/`CompletionStage`) and the
functional wrapper family (`OperationContextAwareFunction`, `…BiFunction`, `…Consumer`,
`…BiConsumer`, …). Extended the Checkstyle config to ban the five `CompletableFuture` static
factories. `GridFutureAdapter` began wrapping listener closures at registration time.

Details: [02.4](02-intra-node-propagation.md#24-futures).

### IGNITE-28681 — Operation Context integrated in Ignite internal async handlers
`963739736a8` · PR #13148 · 2026-05-27 · 59 files, +1145/−828

Generalised the pattern beyond pools and futures to Ignite's own asynchronous worker queues.
Introduced `IgniteInternalWrapper`, reworked `OperationContextAwareWrapper`, added the
`AsyncQueueHandler` / `IgniteAsyncObjectHandler` / `IgniteDelayedObjectHandler` family, and gave
`IgniteThread` its two-constructor capture/no-capture split. `GridDiscoveryManager`'s three worker
loops were converted to restore context per dequeued item.

Details: [02.3](02-intra-node-propagation.md#23-ignitethread), [02.5](02-intra-node-propagation.md#25-asynchronous-worker-queues).

---

## Step 4 — Cross-node propagation

### IGNITE-28808 — Restricted distributed Operation Context attribute registration after node started
`7a61b2d7f6b` · PR #13275 · 2026-06-26 · 14 files, +214/−154 · *(Vladimir Steshin)*

Added `finishRegistration()` on `OperationContextDispatcher`, called from `IgniteKernal` once
components have started; later registration throws. Renamed the wire message to
`OperationContextMessage`. This is the guard that keeps distributed ID → attribute mappings identical
across nodes.

Details: [03.1](03-cross-node-propagation.md#31-operationcontextdispatcher), [05.8](05-context-loss.md#58-asymmetric-attribute-registration).

### IGNITE-28753 — Replaced dedicated Security Context propagation with common distributed Operation Context
`b1cc16f74cb` · PR #13313 · 2026-07-03 · 12 files, +185/−161 · *(Vladimir Steshin)*

The consolidation payoff. Deleted `GridIoSecurityAwareMessage` (68 lines) and ~58 lines of
`GridIoManager` special-casing; `IgniteSecurityProcessor` now registers `SEC_CTX_ATTR` as distributed
attribute `SECURITY = 0` and `withContext(…)` is a thin wrapper over `OperationContext.set`.

Details: [04](04-security-attribute.md).

### IGNITE-28902 — Discovery acknowledgement messages
`47fb44979ef` · PR #13381 · 2026-07-23 · 19 files, +364/−389

Fixed context propagation for **ack** messages sent via TCP Discovery SPI
(`ServerImpl:6189`). Alongside the fix, a significant refactor:

- `ContextAttribute` → `DistributedAttributeRegistry`; `OperationContextMessage` moved into the
  `thread/context` package;
- `OperationContextDispatcher` reworked (+88/−…) into the current collect/restore shape;
- `SecurityContextWrapper` adjusted;
- ZK path added: `ZkOperationContextAwareCustomMessage`, `ZookeeperDiscoveryImpl` integration;
- tests consolidated: `OperationContextSendAttributesTest` (278 lines) and
  `ZkOperationContextSendAttributesTest` deleted in favour of
  `modules/core/src/test/…/thread/context/OperationContextAttributePropagationTest.java` (266 lines).

Details: [03.5](03-cross-node-propagation.md#35-transport-integration-point-2-tcp-discovery), [03.6](03-cross-node-propagation.md#36-transport-integration-point-3-zookeeper-discovery).

### IGNITE-28915 — Postponed discovery messages
`783217635d7` · **current branch, not yet merged** · 2026-07-23 · 2 files, +85/−4

Custom discovery messages parked while a node join is in progress were replayed by the coordinator
without restoring `msg.opCtxMsg`. Three-line fix in `ServerImpl.processCustomMessage`'s pending-drain
loop plus a regression test `testSendAttributesPostponedMessage` (+82 lines in
`OperationContextAttributePropagationTest`).

Details: [03.5](03-cross-node-propagation.md#35-transport-integration-point-2-tcp-discovery), [05.2](05-context-loss.md#52-a-handoff-with-no-restore).

---

## Adjacent

### IGNITE-28910 — Ignite Tracing public API deprecated, internal implementation removed
`af3a6c139ac`

Tracing was one of the subsystems IEP-143 names as maintaining its own context class. Its removal
means `SecurityContext` is currently the only production consumer of the distributed mechanism.

---

## Test coverage

`modules/core/src/test/java/org/apache/ignite/internal/thread/context/`:

| Test | Scope |
|---|---|
| `OperationContextAttributesTest` | Local mechanics: set/get, scopes, snapshots, pools, futures, registration restrictions |
| `OperationContextAttributePropagationTest` | Cross-node: `testSendAttributesByDiscovery`, `testSendAttributesPostponedMessage`, `testSendAttributesByCommunication`, plus a custom test attribute and message type registered through `MessagesPluginProvider` |

The ZK variants run through `ZookeeperDiscoverySpiTestSuite4`; security-side coverage sits in
`SecurityTestSuite` and `IgniteSecurityProcessorTest`.

---

## Current state

**Done:** local mechanism, thread-pool/future/worker-queue integration, static enforcement, TCP and ZK
discovery propagation, communication propagation, security migrated onto the generic mechanism,
registration lifecycle locked down.

**Open, per [05](05-context-loss.md):**

- Channel-open path (`onChannelOpened0`) has no restore scope — [05.5](05-context-loss.md#55-transports-that-carry-no-carrier-field).
- `PoolProcessor`'s `security().enabled()` condition blocks any second distributed attribute —
  [05.7](05-context-loss.md#57-conditional-wrapping-on-security-enabled).
- No join-time validation that nodes agree on the distributed attribute set; rolling upgrade with a
  new attribute is unhandled — [05.8](05-context-loss.md#58-asymmetric-attribute-registration).
- Capacity assertions (32 local / 8 distributed) degrade to silent aliasing without `-ea` —
  [05.9](05-context-loss.md#59-capacity-overflow).
- `BaseStream#parallel` and similar library-internal thread handoffs remain uncoverable — acknowledged
  in the IEP itself.
- IEP status is still **DRAFT**.

**Unticketed candidates** found by the component sweep in
[07 — Component scan](07-loss-candidate-scan.md) — notably ordered communication messages
([F1](07-loss-candidate-scan.md#f1--ordered-communication-messages)), the DataStreamer remap deque
([F2](07-loss-candidate-scan.md#f2--datastreamer-remap-deque)), and the write-behind store flush
([F3](07-loss-candidate-scan.md#f3--write-behind-store-flush)).
