[← Index](README.md) | Prev: [Cross-node propagation](03-cross-node-propagation.md) | Next: [How context gets lost →](05-context-loss.md)

# 04 — The Security Attribute

`SecurityContext` is the reason IEP-143 exists. The IEP's motivation section is explicit:

> This is especially dangerous for `SecurityContext`, which governs authorization and auditing.

Losing a security context does not throw — it silently falls back to the *node's own* default
context, meaning an operation initiated by a restricted user can end up executing with the local
node's privileges. It is the canonical "fails open" bug, and it is why the propagation guarantees in
this IEP are treated as correctness invariants rather than conveniences.

As of IGNITE-28753, security no longer has any bespoke propagation machinery: it is simply the first
registered distributed `OperationContext` attribute.

## 4.1 Registration

`IgniteSecurityProcessor`:

```java
/** @see OperationContextDispatcher */
private static final OperationContextAttribute<SecurityContextWrapper> SEC_CTX_ATTR =
    OperationContextAttribute.newInstance();          // initial value = null

@Override public void start() throws IgniteCheckedException {
    super.start();

    ctx.operationContextDispatcher().registerDistributedAttribute(SECURITY, SEC_CTX_ATTR);
    …
}
```

`SECURITY` is `DistributedAttributeRegistry.SECURITY == 0`. Registration happens in the processor's
`start()`, i.e. before `IgniteKernal` calls `finishRegistration()`.

Because the attribute is only registered when the security processor is active, a cluster with
security disabled has **zero** distributed attributes; `OperationContextDispatcher.createSnapshot()`
returns `null` on its very first check (`locRegisteredAttrs.length == 0`) and the distributed
machinery costs nothing at all.

## 4.2 Setting the context

```java
@Override public Scope withContext(SecurityContext secCtx) {
    return OperationContext.set(SEC_CTX_ATTR,
        secCtx == dfltSecCtx ? null : new SecurityContextWrapper(secCtx));
}

@Override public Scope withContext(UUID subjId) {
    return withContext(securityContext(subjId));
}
```

`withContext` now *is* `OperationContext.set` — it returns the `Scope`, and callers use it in
try-with-resources exactly like any other context update.

**The default-context encoding matters.** The node's own default security context is stored as
`null`, which equals `SEC_CTX_ATTR.initialValue()`. Consequences:

- `isDefaultContext()` is just `OperationContext.get(SEC_CTX_ATTR) == null`;
- `securityContext()` returns `dfltSecCtx` when the attribute is absent;
- `OperationContextDispatcher.createSnapshot()` **skips** it (`curVal == attr.initialValue()`), so no
  security data goes on the wire, and the receiving node uses *its own* default.

That last point is intended: "I am running as myself" should not be transmitted as a subject ID that
the remote node would then have to resolve. But it means an *absent* context and a *default* context
are indistinguishable end-to-end — the receiving node cannot tell "the sender had no context" from
"the sender was the local system subject". Any future audit feature must not rely on that distinction.

## 4.3 `SecurityContextWrapper` — value on the wire

```java
public class SecurityContextWrapper implements Message {
    /** A value of {@link SecuritySubject#id()} */
    @Order(0)
    UUID subjId;

    /** Transient, effective {@link SecurityContext}. */
    private SecurityContext delegate;

    public SecurityContextWrapper(SecurityContext delegate) {
        this.delegate = delegate;
        this.subjId = delegate.subject().id();
    }
}
```

Only the **subject UUID** crosses the network. The full `SecurityContext` — permissions, subject
details, sandbox state — is never serialized; it is re-resolved locally on the receiving node. This
keeps the per-message overhead at 16 bytes and avoids shipping (and trusting) authorization data over
the wire.

`delegate` is a plain non-`@Order` field, so it is transient by construction: populated on the sender,
`null` on arrival, lazily filled in on first use.

## 4.4 Re-resolution on the receiving node

```java
@Override public SecurityContext securityContext() {
    SecurityContextWrapper secCtx = OperationContext.get(SEC_CTX_ATTR);

    if (secCtx == null)
        return dfltSecCtx;

    if (secCtx.delegate() == null)                       // arrived from a remote node
        secCtx.delegate(securityContext(secCtx.subjId));

    return secCtx.delegate();
}

private SecurityContext securityContext(UUID subjId) {
    SecurityContext res = secPrc.securityContext(subjId);

    if (res == null) {
        res = findNodeSecurityContext(subjId);

        if (res == null)
            throw new IllegalStateException("Failed to find security context for subject with given ID : " + subjId);
    }

    return res;
}
```

Resolution is lazy — a node that receives a message but never asks "who is the caller?" pays nothing.
The resolved `delegate` is cached *in the wrapper instance*, which is shared by every thread that
restores that same snapshot, so the lookup happens at most once per received context per node.

Fallback chain, in order:

1. `secPrc.securityContext(subjId)` — the plugin's own subject store (authenticated users, thin
    clients, …).
2. `findNodeSecurityContext(subjId)` — treats the subject ID as a **node** ID:
   ```java
   if (dfltSecCtx.subject().id().equals(subjId))
       return dfltSecCtx;                                     // it's us

   ClusterNode node = ofNullable(ctx.discovery().node(subjId))
       .orElseGet(() -> ctx.discovery().historicalNode(subjId));

   return node == null ? null
       : secCtxs.computeIfAbsent(node.id(),
             uuid -> nodeSecurityContext(marsh, U.resolveClassLoader(ctx.config()), node));
   ```
   Note the `historicalNode` fallback: a node that has already left the topology can still be
   resolved, because in-flight operations it originated may still be completing.
3. Otherwise `IllegalStateException`.

That exception is a *hard* failure, not a fallback to default — deliberately so. A message that
carries a subject ID which cannot be resolved must not execute with the local node's privileges. The
practical failure mode: a client disconnects and ages out of the discovery history while an operation
it started is still running. Discovery history depth is the effective bound on how long a received
context stays resolvable ([05.10](05-context-loss.md#510-subject-no-longer-resolvable)).

## 4.5 Listener-level fallback — `withRemoteSecurityContext`

Because "sender had default context" is transmitted as *nothing* (see 4.2), the framework alone cannot
distinguish it from "no context at all". Two dispatch sites paper over that for security specifically
by substituting the **sender node's** subject when no explicit context arrived:

```java
// GridIoManager.invokeListener (:1832) — same pattern in GridDiscoveryManager (:929)
try (Scope ignored = withRemoteSecurityContext(nodeId)) {
    lsnr.onMessage(nodeId, msg, plc);
}

private Scope withRemoteSecurityContext(UUID nodeId) {
    // No remote Security Context has been attached to the message processing thread so far.
    // This means that the message was sent as part of an operation initiated by the sender node.
    if (ctx.security().isDefaultContext())
        return ctx.security().withContext(nodeId);

    // Verify that the Security Context currently attached to the thread is valid.
    ctx.security().securityContext();

    return Scope.NOOP_SCOPE;
}
```

Semantics: a message with no attached security context is attributed to the *sending node* rather than
to the local node — the right default for node-initiated internal traffic. Two limits worth knowing:

- It engages **only when the thread's context is default**. While `restoreSnapshot(null)` was a NOOP
  this left a hole — a thread that had *inherited* a non-default context from an unrelated operation
  kept it. Since the IGNITE-28915 null-snapshot fix the dispatcher resets the context to defaults before dispatch
  ([05.11](05-context-loss.md#511-empty-snapshot-restores-are-a-noop--fixed)), so by the time this
  check runs, "default" reliably means "no context arrived with the message".
- It is security-only. A second distributed attribute gets no equivalent floor.

## 4.6 What IGNITE-28753 removed

Before this ticket, security had its own parallel propagation stack. The commit deleted
`GridIoSecurityAwareMessage` (68 lines) — a dedicated message wrapper that existed solely to carry a
subject ID alongside a communication message — and cut ~58 lines from `GridIoManager`, replacing it
all with the single generic `GridIoMessage.opCtxSnp` field (named `opCtxMsg` at the time). `GridDiscoveryManager` and
`IgniteAuthenticationProcessor` were adjusted similarly.

This is the payoff the IEP was aiming for: the security subsystem now describes *what* it wants
propagated, and the context framework owns *how*.

---

Next: [05 — How context gets lost →](05-context-loss.md)
