> **Статусы проверены 2026-07-24** по коду ветки IGNITE-28915. В рамках тикета методы
> переименованы:
> `restoreRemoteAttributeValues()` → `restoreSnapshot()`, `collectDistributedAttributeValues()` →
> `createSnapshot()`, поле `opCtxMsg` → `opCtxSnp`.
>
> **Итог: все четыре проблемы исправлены в коде.** Открытым остаётся только отсутствие
> регрессионного теста на детерминированный сценарий из п.1 (второе ordered message с default
> context вслед за non-default в том же message set).

### Блокирующие проблемы

1. P1 — контекст протекает между ordered messages

> **Статус: исправлено** (в два шага).
> — `unwind` (`GridIoManager:3795`) открывает `restoreSnapshot(mc.message.opCtxSnp)` на каждое
> сообщение; regression-тест `testPostponedCommunicationOrderedMessage`.
> — Overlay-семантика для непустого снапшота исправлена: `Restorer` делает **полную замену**
> контекста потока (`Update` с `prev == null`), поэтому атрибуты, отсутствующие в bitmap,
> читаются как `initialValue()`.
> — Null-случай также закрыт в рамках IGNITE-28915: `restoreSnapshot(null)` теперь вызывает
> `Restorer.restoreEmpty()` — контекст потока заменяется на пустой на время scope, т.е. реализован
> предложенный full-state fix. **Открыто:** регрессионного теста на детерминированный сценарий
> (default-context сообщение вслед за non-default в одном message set) по-прежнему нет. См.
> [05.11](05-context-loss.md#511-empty-snapshot-restores-are-a-noop--fixed).

GridIoManager.java:3806

restoreRemoteAttributeValues() не восстанавливает полное состояние: null означает NOOP, а отсутствующие в bitmap атрибуты не сбрасываются в initialValue().

При этом runnable, обрабатывающий GridCommunicationMessageSet, уже исполняется под snapshot одного из сообщений. Если он вычитает несколько сообщений, получается:

1. Первое сообщение содержит USR=A.
2. Второе отправлено с default context, поэтому его opCtxMsg == null.
3. Для второго сообщения новый restore делает NOOP.
4. Listener второго сообщения продолжает видеть USR=A.

Для security attribute это означает потенциальную обработку сообщения B под subject сообщения A: withRemoteSecurityContext() видит non-default context и не переключается на контекст узла.

Это подтверждено детерминированным тестом: первое сообщение отправлено с User[name=outer], второе — с default context и гарантированно помещено в тот же message set. На текущем PR второе сообщение увидело User[name=outer].

Предлагаемый fix: restoreRemoteAttributeValues() должен восстанавливать полное distributed state. Для каждого зарегистрированного атрибута:

- значение из сообщения — если бит установлен;
- attr.initialValue() — если бит отсутствует или msg == null.

Так сохранятся non-distributed атрибуты, но distributed context не будет наследоваться от внешнего scope.

2. P1 — тот же overlay-баг в pending discovery

> **Статус: исправлено** — тем же централизованным изменением семантики restore.
> Pending custom message с непустым снапшотом полностью вытесняет контекст внешнего topology
> message (full-state replace), а с `opCtxSnp == null` — сбрасывает его к defaults
> (`Restorer.restoreEmpty()`). Наследования контекста внешнего scope больше нет. См.
> [05.11](05-context-loss.md#511-empty-snapshot-restores-are-a-noop--fixed).

ServerImpl.java:6317

checkPendingCustomMessages() вызывается не только из чистого noMessageLoop(), но также из:

- processNodeAddFinishedMessage();
- processNodeLeftMessage();
- processNodeFailedMessage().

Эти вызовы происходят внутри внешнего scope из processMessage(). Поэтому default/partial context отложенного custom message накладывается поверх context текущего topology message.

Например, если topology message несёт SEC_CTX=B, а pending custom message отправлен с default security context, restoreRemoteAttributeValues(null) ничего не изменит и custom message будет обработан как B.

Сам try-with-resources здесь корректен; проблема именно в delta/overlay-семантике restore. Централизованный full-state fix в OperationContextDispatcher исправит и этот путь.

3. P1 — context вложенных сообщений теряется при client reconnect

> **Статус: исправлено** (в рамках IGNITE-28915). Реализовано ровно предложенное:
> `ClientImpl.processDiscoveryMessage` (`:2150-2154`) стал context boundary
> (`try (Scope ignored = operationCtxDispatcher.restoreSnapshot(msg.opCtxSnp))`), и replay из
> `msg.pendingMessages()` (`:2562`, `:2572`) идёт через него — каждое pending message
> обрабатывается под собственным контекстом. Null-случай также закрыт
> (reset к defaults вместо NOOP).

ClientImpl.java:2561 и ClientImpl.java:2571

Внешний scope в message worker восстановлен из TcpDiscoveryClientReconnectMessage. Затем каждое сообщение из msg.pendingMessages() передаётся в processDiscoveryMessage(pendingMsg) без восстановления его собственного pendingMsg.opCtxMsg.

Получается, что все накопленные custom discovery events обрабатываются под context reconnect-контейнера, хотя каждый pending message содержит собственный transport context.

Лучше сделать processDiscoveryMessage() самостоятельной context boundary:

try (Scope ignored = operationCtxDispatcher.restoreRemoteAttributeValues(msg.opCtxMsg)) {
    processDiscoveryMessage0(msg);
}

Тогда и обычный dispatch, и reconnect replay будут иметь одинаковую семантику.

4. P1 — local replay перезаписывает исходный context

> **Статус: исправлено** (в рамках IGNITE-28915). Введён
> `TcpDiscoveryAbstractMessage.attachOperationContextSnapshot(...)`; `RingMessageWorker.addMessage`
> вызывает его только для `!fromSocket`-сообщений (`ServerImpl:3023-3024`). Теперь
> «attach уже был» фиксируется отдельным сериализуемым флагом (`OP_CTX_ATTACHED_FLAG_POS = 3`),
> а не проверкой поля на null — поэтому сообщение с легитимно пустым envelope (отправитель с
> default context) при requeue тоже НЕ переписывается снапшотом текущего потока-обработчика.
> Флаг и `opCtxSnp` копируются copy-конструктором и передаются по сети (`flags` — wire-поле).

ServerImpl.java:4069

processPendingMessagesLocally() повторно ставит уже существующий pendingMsg через обычный:

msgWorker.addMessage(pendingMsg);

msg.opCtxMsg = operationCtxDispatcher.collectDistributedAttributeValues();

В результате context исходной операции заменяется context текущего NodeLeft`/`NodeFailed handler. Особенно неприятно, что TcpDiscoveryCustomEventMessage входит в ensured-delivery history и реально может попасть в этот путь.

При requeue нужно сохранить существующий envelope — например, использовать вариант addMessage(..., fromSocket=true) либо отдельный метод replay, который не меняет opCtxMsg.
