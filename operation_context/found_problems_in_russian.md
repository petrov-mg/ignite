### Блокирующие проблемы

1. P1 — контекст протекает между ordered messages

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

ServerImpl.java:6317

checkPendingCustomMessages() вызывается не только из чистого noMessageLoop(), но также из:

- processNodeAddFinishedMessage();
- processNodeLeftMessage();
- processNodeFailedMessage().

Эти вызовы происходят внутри внешнего scope из processMessage(). Поэтому default/partial context отложенного custom message накладывается поверх context текущего topology message.

Например, если topology message несёт SEC_CTX=B, а pending custom message отправлен с default security context, restoreRemoteAttributeValues(null) ничего не изменит и custom message будет обработан как B.

Сам try-with-resources здесь корректен; проблема именно в delta/overlay-семантике restore. Централизованный full-state fix в OperationContextDispatcher исправит и этот путь.

3. P1 — context вложенных сообщений теряется при client reconnect

ClientImpl.java:2561 и ClientImpl.java:2571

Внешний scope в message worker восстановлен из TcpDiscoveryClientReconnectMessage. Затем каждое сообщение из msg.pendingMessages() передаётся в processDiscoveryMessage(pendingMsg) без восстановления его собственного pendingMsg.opCtxMsg.

Получается, что все накопленные custom discovery events обрабатываются под context reconnect-контейнера, хотя каждый pending message содержит собственный transport context.

Лучше сделать processDiscoveryMessage() самостоятельной context boundary:

try (Scope ignored = operationCtxDispatcher.restoreRemoteAttributeValues(msg.opCtxMsg)) {
    processDiscoveryMessage0(msg);
}

Тогда и обычный dispatch, и reconnect replay будут иметь одинаковую семантику.

4. P1 — local replay перезаписывает исходный context

ServerImpl.java:4069

processPendingMessagesLocally() повторно ставит уже существующий pendingMsg через обычный:

msgWorker.addMessage(pendingMsg);

msg.opCtxMsg = operationCtxDispatcher.collectDistributedAttributeValues();

В результате context исходной операции заменяется context текущего NodeLeft`/`NodeFailed handler. Особенно неприятно, что TcpDiscoveryCustomEventMessage входит в ensured-delivery history и реально может попасть в этот путь.

При requeue нужно сохранить существующий envelope — например, использовать вариант addMessage(..., fromSocket=true) либо отдельный метод replay, который не меняет opCtxMsg.
