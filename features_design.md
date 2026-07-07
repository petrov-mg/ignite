<p>
  <ac:structured-macro ac:macro-id="866ed893-d071-4df0-b22b-539e2ed0d487" ac:name="toc" ac:schema-version="1"/>
</p>
<h1>Требования</h1>
<h2>Этап обновления версии узлов</h2>
<p>На этом этапе RU кластер Ignite включает в себя одновременно узлы "старой" и "новой" версии, которые взаимодействуют друг с другом через отправку сообщений.</p>
<p>
  <strong>Требования:</strong>
</p>
<ol>
  <li>Операции, которые были доступны в "старой" версии,  должны корректно выполняться, несмотря на различия в исходном коде задействованных узлов</li>
  <li>API, привнесенное "новой" версии, должно быть недоступным для вызова</li>
  <li>Поведение "новых" узлов должно в точности совпадать с поведением "старых" узлов в кластере. Это включает, как создание/отправку/обработку сообщений другим узлам, так и запись пользовательских/внутренних данных на диск. </li>
</ol>
<h2>Процесс RU завершен</h2>
<p>На этом этапе кластер Ignite включает в себя только узлы "новой" версии.</p>
<p>
  <strong>Требования:</strong>
</p>
<ol>
  <li>логика работы всех узлов кластера должна соответствовать "новой" версии исходного кода</li>
  <li>API, привнесенное "новой" версии, должно быть доступным для вызова</li>
</ol>
<h1>Решение</h1>
<h2>Действия разработчика для поддержания возможности RU</h2>
<p>При внесении изменений в код для очередной версии Ignite разработчик должен определить - приводят ли его изменения к проблемам при взаимодействии узлов "новой" и "старой" версии?<br/>Если приводят, то разработчик</p>
<ol>
  <li>
    <p>Создает новую Ignite Feature, которая описывает изменения, как единое целое </p>
    <ac:structured-macro ac:macro-id="b2ce2d0f-e9b5-41cc-b0f1-6d2bd1457e2d" ac:name="expand" ac:schema-version="1">
      <ac:parameter ac:name="title">КОД</ac:parameter>
      <ac:rich-text-body>
        <ac:structured-macro ac:macro-id="d8c490a0-8078-4fd2-951c-a01388ce8724" ac:name="code" ac:schema-version="1">
          <ac:parameter ac:name="language">java</ac:parameter>
          <ac:plain-text-body><![CDATA[/** */
public class IgniteFeatureManager {
	/** */
	public static IgniteFeature FEATURE_0 = new IgniteKernalFeature(0);  

    /** */
	public static IgniteFeature FEATURE_1 = new IgniteKernalFeature(1);    

/** */
public static IgniteFeature NEW_LOGIC_FEATURE = new IgniteKernalFeature(2);  
}

/** */
public interface IgniteFeature {
/** Требование для значений Feature ID - id каждой новой Ignite Feature БОЛЬШЕ id существующих Ignite Feature. */
int id();
}]]></ac:plain-text-body>
</ac:structured-macro>
</ac:rich-text-body>
</ac:structured-macro>
  </li>
  <li>
    <p>Добавляет каждую часть новой логики по принципу:</p>
    <ac:structured-macro ac:macro-id="06e4678b-f6ca-4cda-994d-36fd0228bf16" ac:name="code" ac:schema-version="1">
      <ac:parameter ac:name="language">java</ac:parameter>
      <ac:plain-text-body><![CDATA[if (isActive(NEW_LOGIC_FEATURE)) {
	<новая логика>
} else {
	<старая логика, если есть>
}]]></ac:plain-text-body>
    </ac:structured-macro>
  </li>
  <li>Анализирует корректность выполнения операций, если статус новой Ignite Feature отличается на участвующих в выполнении операции узлах (см. раздел Активация Ignite Features при завершении RU).<br/>Если по техническим причинам реализовать выполнение операции для указанного случая невозможно - операция должна быть завершена с понятной пользователю ошибкой, а поведение задокументировано.</li>
  <li>Создает и выносит на обсуждение  описание, которое включает<ol>
      <li>мотивацию</li>
      <li>вносимые изменения</li>
      <li>влияние на операции, которые выполняются параллельно с процессом  RU<br/>
        <br/>
      </li>
    </ol>
  </li>
</ol>
<p>На этапе RU, когда кластер содержит одновременно узлы "старой" и "новой" версии - </p>
<p>
  <code>isActive(IGNITE_FEATURE)</code> возвращает <code>true</code>  только для тех Ignite Feature, которые доступны как на "старой", так и на "новой" версии.  Это гаранитрует то, что поведение узлов кластера с различным исходным кодом будет одинаковым.</p>
<p>
  <br/>После завершения RU -</p>
<p>
  <code>isActive(IGNITE_FEATURE)</code> возвращает <span style="font-family: SFMono-Medium , SF Mono , Segoe UI Mono , Roboto Mono , Ubuntu Mono , Menlo , Courier , monospace;">true</span> для всех Ignite Feature, включая те, что были реализованны в "новой" версии. В результате логика работы всех узлов кластера соотвествует "новой" версии исходного кода и "новое"  API становится доступным.  </p>
<h2>Примеры изменений, которые требуют использование Ignite Features</h2>
<table class="relative-table wrapped" style="width: 112.422%;">
  <colgroup> <col style="width: 21.4515%;"/> <col style="width: 28.4442%;"/> <col style="width: 50.1043%;"/> </colgroup>
  <tbody>
    <tr>
      <th scope="col">Изменение</th>
      <th scope="col">Потенциальные проблемы и вытекающие требования </th>
      <th scope="col">Пример безопасной реализации  в "новой" версии кода</th>
    </tr>
    <tr>
      <td>
        <ol>
          <li>Добавление нового поля в существующее сообщение и использование в логике создания/обработки нового поля вместо старого.</li>
          <li>Добавление нового поля в существующие сообщение, которое обрабатывается независимой логикой,</li>
          <li>"Удаление" поля вместе с относящейся логикой </li>
        </ol>
      </td>
      <td>
        <p>
          <ac:inline-comment-marker ac:ref="9d6eaf3e-61d7-4bb2-8013-c4d7f57541cc">"новый" узел отправляет "старому" сообщение</ac:inline-comment-marker>, в котором для нового поля задано значение, а для существующего - нет.<br/>"старый" узел нового поля не увидит, а в существующем, на котором завязана его логика - null. <br/>Обработка сообщения на "старом" узле потенциально  завершится ошибкой.<br/>
          <br/>
          <strong>Требование</strong> - при отправке сообщения от "нового" узла "старому" все существующие поля должны заполняться ожидаемыми "старым" узлом значениями.<br/>
          <br/>"старый" узел отправляет сообщение "новому". Если логика "нового узла" будет завязана только на новое поле (очевидно, что "старый" узел его заполнить не может), то обработка сообщения на "новом" узле потенциально  завершится ошибкой.</p>
        <p>
          <strong>Требование</strong> - "новый" узел должен корректно обрабатывать сообщения  сформированные на "старых" узлах.</p>
      </td>
      <td>
        <div class="content-wrapper">
          <ac:structured-macro ac:macro-id="9c52a5bb-75ce-4313-93de-b22f0664fbdd" ac:name="expand" ac:schema-version="1">
            <ac:parameter ac:name="title">КОД</ac:parameter>
            <ac:rich-text-body>
              <ac:structured-macro ac:macro-id="77e1bd16-cc70-4add-823a-50266ceb02ad" ac:name="code" ac:schema-version="1">
                <ac:parameter ac:name="language">java</ac:parameter>
                <ac:plain-text-body><![CDATA[public class IgniteProcessor {
  	/** */ 
	private void sendMessage() {
		Message msg = new Message();

if (ctx.features().isActive(NEW_FEATURE))
msg.newField(createInteger());
else
msg.oldField(createString());

		send(msg);
	}
	
 	/** */ 
	private void onMessageReceived(Message msg) {
if (msg.sender().features().isActive(NEW_FEATURE))
processInteger(msg.getNewField());
else
processString(msg.oldField());
}
}]]></ac:plain-text-body>
</ac:structured-macro>
</ac:rich-text-body>
</ac:structured-macro>
<p>
<ac:inline-comment-marker ac:ref="f6ce4414-9775-4fd1-a7bc-4fc0de70bd20"> <ac:inline-comment-marker ac:ref="cbdde24f-4754-4a37-be8a-b84cc18fd69b">Обратите</ac:inline-comment-marker> внимание, что </ac:inline-comment-marker> <code>
<ac:inline-comment-marker ac:ref="f6ce4414-9775-4fd1-a7bc-4fc0de70bd20">onMessageReсeived(</ac:inline-comment-marker>)</code> использует в качестве условия активные Ignite Features отправителя сообщения, а не локальные. Более подробно об этом написано в разделе - <code>Активация Ignite Features при завершении RU</code> .</p>
</div>
</td>
</tr>
<tr>
<td>
<p>Добавление нового сообщения и <span style="letter-spacing: 0.0px;">логики его создания для реализации нового API или внутренних механизмов</span>
</p>
</td>
<td>"новый" узел отправляет новое сообщение "старому" узлу в результате <br/>
<ul style="list-style-type: square;">
<li>вызова нового пользовательского API</li>
<li>работы внутренних механизмов, запускаемых автоматически, типа рассылки ClusterMetricsUpdateMessage</li>
</ul>
<p>"старый" узел, очевидно, обработать такое сообщение не может. В результате изначальная операция завершится ошибкой или не завершится вовсе.<br/>
<br/>
<strong>Требование</strong> - "новый" узел <strong> <ac:inline-comment-marker ac:ref="adc6caba-7719-4405-9bd0-7ef6316e9fb3">не</ac:inline-comment-marker> </strong> должен выполнять логику, которая приводит к отправлению на "старые" узлы неизвестных тем сообщений. <ac:inline-comment-marker ac:ref="c5bf188f-f0ae-444b-8589-81ec2d66dcce">Условия на обработку новых сообщений не требуются</ac:inline-comment-marker>.</p>
</td>
<td>
<div class="content-wrapper">
<ac:structured-macro ac:macro-id="61bda8a4-f7d7-4a56-90d4-85929250ce40" ac:name="expand" ac:schema-version="1">
<ac:parameter ac:name="title">КОД</ac:parameter>
<ac:rich-text-body>
<ac:structured-macro ac:macro-id="b3d39e0f-d7b4-4e6e-a48f-2afc6666917e" ac:name="code" ac:schema-version="1">
<ac:parameter ac:name="language">java</ac:parameter>
<ac:plain-text-body><![CDATA[public class IgniteProcessor {
/** Обработка обращения к новому API. */
public void handleUserRequest() {
  		if (!ctx.features().isActive(NEW_API_FEATURE))
throw new IgniteFeatureInactiveException();

		send(new ExecutionRequest());
	}
	
    /** Запуск внутренних механизмов. */
	public void start() {
if (ctx.features().isActive(NEW_INTERNAL_FEATURE))
startScheduledMessageSending();
else
ctx.feature.subscribeOnActivation(NEW_INTERNAL_FEATURE, () -> startScheduledMessageSending())
}
}]]></ac:plain-text-body>
</ac:structured-macro>
</ac:rich-text-body>
</ac:structured-macro>
</div>
</td>
</tr>
<tr>
<td>
<p>Изменение логики обработки существующего сообщения  процессорами Ignite (структура и данные сообщений в "новой" версии не менялись)</p>
</td>
<td>
<p>"новый" узел после получения сообщения от "старого" реагирует неожиданным для "старого" исходного кода способом. Например, после получения сообщения "новый" узел отправляет ответ только координатору, а по логике "старой" версии должен сделать бродкаст на все узлы. В результате изначальная операция может завершиться ошибкой или не быть завершена.</p>
<p>
<br/>
<strong>Требование</strong> - реакция "нового" узла на существующие сообщения должна быть ожидаемой для "старых" узлов в кластере.  <br/>
<br/>
</p>
</td>
<td>
<div class="content-wrapper">
<ac:structured-macro ac:macro-id="dddab4fc-aee0-4c54-9d28-102ab123b238" ac:name="expand" ac:schema-version="1">
<ac:parameter ac:name="title">КОД</ac:parameter>
<ac:rich-text-body>
<ac:structured-macro ac:macro-id="b312bdb1-6811-427e-9ba9-91157aaf210e" ac:name="code" ac:schema-version="1">
<ac:parameter ac:name="language">java</ac:parameter>
<ac:plain-text-body><![CDATA[public class IgniteProcessor {
	/** */
	private void handleInternalRequest() {
  		if (ctx.features().isActive(NEW_INTERNAL_FEATURE))
			sendResponseToCoordinator();
		else 
			broadcastResponseToAllNodes();
	}
}]]></ac:plain-text-body>
</ac:structured-macro>
</ac:rich-text-body>
</ac:structured-macro>
</div>
</td>
</tr>
<tr>
<td>"удаление" кода из продукта</td>
<td>
<p>Разработчик хочет удалить часть функционала из продукта в процессе разработки версии 2.20<br/>В версии 2.20 он ничего не удаляет. Но</p>
<ol>
<li>
<ac:inline-comment-marker ac:ref="444ef9ea-3e02-400b-bbe9-2d9afcbab1e8">Заводит новую Ignite Feature</ac:inline-comment-marker>
</li>
<li>Запрещает вызов публичного API, которое относится к удаляемому функционалу, если фича из п.1 активна.</li>
</ol>
<p>В итоге: пока RU в процессе и в кластере есть узлы 2.19 и 2.20 версии - фича не активна и функционал работает, как на нодах 2.19, так и 2.20.<br/>
<br/>Обновили все узлы до 2.20 и активировали Ignite Features 2.20 - функционал перестал работать. <br/>
<br/>В 2.21 можно окончательно удалить код из продукта.</p>
</td>
<td>
<div class="content-wrapper">
<ac:structured-macro ac:macro-id="cbd8c1ef-40b9-4bdd-a40f-7152c6fe58c1" ac:name="expand" ac:schema-version="1">
<ac:parameter ac:name="title">КОД</ac:parameter>
<ac:rich-text-body>
<ac:structured-macro ac:macro-id="17a691ee-564f-4ea4-8e75-06982f81ceb1" ac:name="code" ac:schema-version="1">
<ac:parameter ac:name="language">java</ac:parameter>
<ac:plain-text-body><![CDATA[public class IgniteProcessor {
/** Обработка обращения к новому API. */
public void handleApiCall() {
  		if (ctx.features().isActive(API_DEPRECATED_FEATURE))
throw new IgniteFeatureDeprecatedException();

		doWork();
	}
}]]></ac:plain-text-body>
</ac:structured-macro>
</ac:rich-text-body>
</ac:structured-macro>
</div>
</td>
</tr>
<tr>
<td>
<p>Изменение формата записи данных на диск</p>
</td>
<td>
<p>Есть кластер из узлов версии 2.19.<br/>1 ноду вывели обновили исходный код до 2.20 и ввели обратно.<br/>Обновленыый узел должен успешно прочитать все данные с диска, которые были записаны предыдущей версией.<br/>
<br/>
<strong>Под вопросом:</strong> <br/>Есть кластер из узлов версии 2.19.<br/>В кластер ввели узел версии 2.20.<br/>До момента активации Ignite Features версии 2.20 - исходный код узла версии 2.20 можно "откатить" до 2.19 без последствий. <br/>
<br/>С одной стороны это защитит пользователя от ошибок - т.к. если мы предоставляем такую гарантию, то ввод узла "новой" версии будет обратимой операцией. Необратимой она станет только после активации Ignite Features новой версии.<br/>
<br/>Но с другой стороны это накладывает жесткие требования на продукт, которые выходят за рамки совместимости PDS/WAL. Мы буквально должны будем следить за структурой папок и файлов, которые пишет Ignite.</p>
<p>
<br/>
<strong>Требование</strong> - если на узлах разной версии активен один и тот же набор Ignite Feature - они должны успешно читать с диска данные записанные друг другом </p>
</td>
<td>
<div class="content-wrapper">
<p>
<br/>
</p>
</div>
</td>
</tr>
  </tbody>
</table>
<h2>Поведение кластера на этапе обновления версии узлов</h2>
<p>Обновление версии узлов становится доступным после вызова команды </p>
<ac:structured-macro ac:macro-id="78cc6a31-dc00-4d2e-8acb-ae97565ccabd" ac:name="code" ac:schema-version="1">
  <ac:parameter ac:name="language">java</ac:parameter>
  <ac:parameter ac:name="title">КОД</ac:parameter>
  <ac:plain-text-body><![CDATA[control.sh|bat --rolling-upgrade enable]]></ac:plain-text-body>
</ac:structured-macro>
<p>
  <br/>После ее завершения, к кластеру могут присоединяться узлы с версией отличной от текущей со следующими ограничениями:<br/>
  <br/>
</p>
<ol>
  <li>Версия присоединяющихся узлов &gt;= текущей версии кластера</li>
  <li>Ignite Features присоединяющегося узла должны быть совместимы с Ignite Features текущей версии кластера. Это означает, что присоединяющийся должен иметь возможность деактиваровать  Ignite Features недоступные версии существующего кластера. Благодаря этому присоединяющийся узел имитирует поведение узла предыдущей версии.   </li>
  <li>В кластере одновременно могут находиться узлы только с ДВУМЯ различыми версиями</li>
</ol>
<p>После того, как версия всех узлов была обновлена (но не финализирована), обновление можно повторить на более высокую совместимую версию.<br/>
  <br/>Откат узлов на более низкую версию возможен только с полной очисткой PDS. Каких то програмных ограничений не планируется. Ограничения должны быть описаны в документации.<br/>
  <br/>
  <strong>Перезагрузка кластера во время RU<br/>
    <br/>
  </strong>Рассматривается ситуация:</p>
<ol>
  <li>RU активирован</li>
  <li>Часть узлов кластера обновлена на новую версию</li>
  <li>все узлы кластера остановлены</li>
  <li>требуется восстановить кластер</li>
</ol>
<p>Проблема: Восстановить кластер, просто запустив все узлы - не получится, т.к. по умолчанию вход узлов различных версий запрещен.</p>
<p>В итоге у пользователя 2 варианта</p>
<ul>
  <li>дообновить все узлы и запустить кластер целиком</li>
  <li>восстановить "старые" узлы. Повторно активировать RU. Ввести обновленные узлы. Продолжить процесс обновления</li>
</ul>
<p>Сохранять состояние RU между запусками связано со следующими проблемами:</p>
<ol>
  <li>Состояние RU должно быть в какой то форме сохранено в PDS.  Формат сохраненных данных должен учитывать обратную совместимость PDS.  </li>
  <li>in memory кластер не имеет механизмов для сохранениня какого то состояния между запусками</li>
</ol>
<p>Сохранять состояние RU между запусками на 1 этапе реализации не планируется. </p>
<h2>Ignite Feature Set</h2>
<p>Ignite Feature Set представляет из себя цепочку Ignite Feature, выстроенных в порядке их появления (в порядке возрастания Feature ID).<br/>
  <br/>Создавая отдельные Ignite Features и добавляя в код условия <code>isActive(IGNITE_FEATURE),</code> мы реализуем возможность активировать только часть фич.<br/>
  <br/>Благодаря такому подходу узлы "новых" версий будут иметь возможность активировать только те фичи, которые поддерживают старые узлы "кластера".</p>
<p>Мы так же сможем легко удалять из кода продукта фичи из начала цепочки и связанные с ними проверки isActive (значения Feature ID при этом <strong>не</strong> должны меняться). После удаления фичи считаются активными по умолчанию и "отключить" их с помощью уже не выйдет.</p>
<p>Благодаря этому мы решаем проблему бесконечного накопления фич в продукте и ограничиваем диапазон версий в рамках которых RU возможен (см. Ignite Features в релизном процессе)<br/>
  <br/>Состояние фич узла версии 2.20 после присоединения к кластеру версии 2.19: <br/>
  <br/>
  <ac:structured-macro ac:macro-id="4f9c29a4-05e6-461c-bb73-9076dd9a82c3" ac:name="drawio" ac:schema-version="1">
    <ac:parameter ac:name="border">true</ac:parameter>
    <ac:parameter ac:name="diagramName">Copy of Copy of Features chain</ac:parameter>
    <ac:parameter ac:name="simpleViewer">false</ac:parameter>
    <ac:parameter ac:name="width"/>
    <ac:parameter ac:name="links">auto</ac:parameter>
    <ac:parameter ac:name="tbstyle">top</ac:parameter>
    <ac:parameter ac:name="lbox">true</ac:parameter>
    <ac:parameter ac:name="diagramWidth">962</ac:parameter>
    <ac:parameter ac:name="revision">2</ac:parameter>
    <ac:parameter ac:name=""/>
  </ac:structured-macro>
</p>
<p>
  <br/>Состояние фич узла версии 2.20 после завершения RU и активации новых фич: </p>
<p>
  <ac:structured-macro ac:macro-id="880fca0a-0e67-4e0d-a7f6-e3b691c32597" ac:name="drawio" ac:schema-version="1">
    <ac:parameter ac:name="border">true</ac:parameter>
    <ac:parameter ac:name="diagramName">Copy of Features chain</ac:parameter>
    <ac:parameter ac:name="simpleViewer">false</ac:parameter>
    <ac:parameter ac:name="width"/>
    <ac:parameter ac:name="links">auto</ac:parameter>
    <ac:parameter ac:name="tbstyle">top</ac:parameter>
    <ac:parameter ac:name="lbox">true</ac:parameter>
    <ac:parameter ac:name="diagramWidth">962</ac:parameter>
    <ac:parameter ac:name="revision">2</ac:parameter>
    <ac:parameter ac:name=""/>
  </ac:structured-macro>
</p>
<p>
  <br/>
</p>
<p>
  <strong>Зачем заводить отдельную фичу на каждое изменение, а не 1 на релиз?</strong>
</p>
<ol>
  <li>Что бы упростить релизы в банке - фичи ничего не знают про версии игнайта. Версии игнайта и их кол-во в банке и апаче отличаются.</li>
  <li>Что бы упростить черепик изменений.</li>
  <li>Что бы иметь возможность тестировать RU для каждой новой фичи отдельно.    </li>
</ol>
<h2>Активация Ignite Features при завершении RU</h2>
<p>
  <ac:inline-comment-marker ac:ref="82d7885d-8505-4ebb-a7d3-450a72c57c36">Предполагается что после того, как исходный код всех узлов кластера был переведен на "новую" версию, администратор Ignite явно вызовет команду для "финализации" активного Ignite Feature Set</ac:inline-comment-marker>.</p>
<ac:structured-macro ac:macro-id="a3d0e0af-f390-4ff5-99f3-9a7d4c624523" ac:name="code" ac:schema-version="1">
  <ac:parameter ac:name="language">bash</ac:parameter>
  <ac:plain-text-body><![CDATA[control.sh|bat --rolling-upgrade finalize]]></ac:plain-text-body>
</ac:structured-macro>
<p>
  <strong>Требование:</strong>
</p>
<ol>
  <li>выполнение команды возможно только тогда, когда версии исходного кода (набор поддерживаемых Ignite Feature) на всех узлов кластера совпадают</li>
  <li>конкурентный вход узлов во время выполнения операции должен быть ограничен (узлы "старой" версии <strong>не</strong> должны иметь возможности войти в топологию)</li>
  <li>команда может дожидаться окончания и блокировать запуск на время своего выполнения операции Ignite (снятие снапшотов?)</li>
</ol>
<p>
  <br/>После выполнения этой команды - все Ignite Features, привнесенные "новой" версией, становятся активными. Как следствие, все узлы кластера начинают использовать новую логику для создания и обработки сообщений. Новое API становится доступным для вызова.   </p>
<h3>Проблемы реализации</h3>
<h4>Атомарность</h4>
<p> Сделать выполнение команды  "финализации" Ignite Feature Set  атомарным для всех узлов кластера - нетривиальная задача. <br/>
  <br/>Поэтому "финализацию" Ignite Feature Set на всех узлах кластера предлагается сделать асинхронной. Это означает, что</p>
<ol>
  <li>в процессе выполнения команды кластер состоит только из узлов "новой" версии, но список активных Ignite Features на разных узлах отличается</li>
  <li>узлы, на которых "финализация" Ignite Feature Set завершилась, могут отправлять сообщения узлам, которые еще не получили запрос "финализации", и наоборот</li>
</ol>
<p>
  <br/>
  <ac:structured-macro ac:macro-id="c6f9f489-900c-42d4-913c-8a747b07b218" ac:name="drawio" ac:schema-version="1">
    <ac:parameter ac:name="border">true</ac:parameter>
    <ac:parameter ac:name="diagramName">RU finalization</ac:parameter>
    <ac:parameter ac:name="simpleViewer">false</ac:parameter>
    <ac:parameter ac:name="width"/>
    <ac:parameter ac:name="links">auto</ac:parameter>
    <ac:parameter ac:name="tbstyle">top</ac:parameter>
    <ac:parameter ac:name="lbox">true</ac:parameter>
    <ac:parameter ac:name="diagramWidth">341</ac:parameter>
    <ac:parameter ac:name="revision">4</ac:parameter>
    <ac:parameter ac:name=""/>
  </ac:structured-macro>   <br/>
  <br/>
</p>
<p>
  <span style="color: rgb(0,0,0);"> В итоге в процессе выполнения команды финализации статус Ignite Feature на узле, который отправляет сообщение, и узле, который его обрабатывает, может отличаться.</span>
</p>
<h4>
  <span style="color: rgb(0,0,0);">Взаимодействие узлов с разным набором активных Ignite Feature</span>
</h4>
<p>
  <span style="color: rgb(0,0,0);"> <ac:structured-macro ac:macro-id="501734d9-340a-4f0f-b121-912e04a41281" ac:name="drawio" ac:schema-version="1">
      <ac:parameter ac:name="border">true</ac:parameter>
      <ac:parameter ac:name="diagramName">Диаграмма без названия</ac:parameter>
      <ac:parameter ac:name="simpleViewer">false</ac:parameter>
      <ac:parameter ac:name="width"/>
      <ac:parameter ac:name="links">auto</ac:parameter>
      <ac:parameter ac:name="tbstyle">top</ac:parameter>
      <ac:parameter ac:name="lbox">true</ac:parameter>
      <ac:parameter ac:name="diagramWidth">291</ac:parameter>
      <ac:parameter ac:name="revision">1</ac:parameter>
      <ac:parameter ac:name=""/>
    </ac:structured-macro> </span>
</p>
<p>
  <span style="color: rgb(0,0,0);">Если узел получил сообщение от узла, на котором Ignite Feature активна, а локально на нем - нет, то он может сделать выводы:</span>
</p>
<ol>
  <li>
    <span style="color: rgb(0,0,0);"> "финализация" уже началась (скоро запрос на изменение статуса Ignite Features получит и сам узел)</span>
  </li>
  <li>
    <span style="color: rgb(0,0,0);"> все узлы кластера уже имеют одинаковую "новую" версию исходного кода. Следовательно и полный список поддерживаемых Ignite Features у них одинаков</span>
  </li>
  <li>
    <span style="color: rgb(0,0,0);"> узел может считать эту Ignite Feature активной при обработке полученного сообщения и создания ответа</span> <span style="color: rgb(0,0,0);"> </span>
  </li>
</ol>
<p>
  <br/>
</p>
<p>
  <ac:structured-macro ac:macro-id="b5bde384-0170-4f60-9c50-81d753a1da11" ac:name="drawio" ac:schema-version="1">
    <ac:parameter ac:name="border">true</ac:parameter>
    <ac:parameter ac:name="diagramName">Copy of Диаграмма без названия</ac:parameter>
    <ac:parameter ac:name="simpleViewer">false</ac:parameter>
    <ac:parameter ac:name="width"/>
    <ac:parameter ac:name="links">auto</ac:parameter>
    <ac:parameter ac:name="tbstyle">top</ac:parameter>
    <ac:parameter ac:name="lbox">true</ac:parameter>
    <ac:parameter ac:name="diagramWidth">391</ac:parameter>
    <ac:parameter ac:name="revision">4</ac:parameter>
    <ac:parameter ac:name=""/>
  </ac:structured-macro>
</p>
<p>Узел, на котором Ignite Feature активна, может получить сообщения от узла, который еще не получил запрос о ее активации. <br/>В результате узел, получивший сообщение, должен считать Ignite Feature неактивной во время обработки сообщения и создания ответа.  </p>
<p>
  <br/>В итоге можно считать, что набор активных фич инициатора операции(она может затрагивать несколько узлов) должен быть известен на протяжении всего времени ее выполнения. И все участвующие в выполнении узлы должны учитывать активные фичи инициатора. <br/>
  <br/>
</p>
<h4>
  <span style="color: rgb(0,0,0);">Изменение статуса Ignite Feature в процессе выполнения операции</span>
</h4>
<p>
  <span style="color: rgb(0,0,0);"> <ac:structured-macro ac:macro-id="97fdfad8-186c-407f-9a15-a4428c683244" ac:name="drawio" ac:schema-version="1">
      <ac:parameter ac:name="border">true</ac:parameter>
      <ac:parameter ac:name="diagramName">Activation between two isActive checks</ac:parameter>
      <ac:parameter ac:name="simpleViewer">false</ac:parameter>
      <ac:parameter ac:name="width"/>
      <ac:parameter ac:name="links">auto</ac:parameter>
      <ac:parameter ac:name="tbstyle">top</ac:parameter>
      <ac:parameter ac:name="lbox">true</ac:parameter>
      <ac:parameter ac:name="diagramWidth">740</ac:parameter>
      <ac:parameter ac:name="revision">1</ac:parameter>
      <ac:parameter ac:name=""/>
    </ac:structured-macro> </span>
</p>
<p>
  <span style="color: rgb(0,0,0);">Код обработки сообщения или его создания может содержать несколько проверок <code>isActive(IGNITE_FEATURE)</code> . Если между ними изменится статус IGNITE_FEATURE - логика операции может сломаться.<br/>Такая ситуация не должна допускаться. Т.е. в итоге на все время обработки/cсоздания сообщения мы должны фиксировать состояние фич.</span>
</p>
<h3>Предлагаемое решение</h3>
<ac:structured-macro ac:macro-id="998642b3-c8e9-42c3-a774-41f2d7fe4edb" ac:name="expand" ac:schema-version="1">
  <ac:parameter ac:name="title">Процесс изменения статуса Ignite Features на всех узлах кластера (Distributed Process)</ac:parameter>
  <ac:rich-text-body>
    <ol>
      <li>Инициатор локально проверяет, что все узлы в топологии поддерживают одинаковый Feature Set, если нет - завершаем операцию ошибкой</li>
      <li>Инициатор отправляет по Discovery сообщение о начале финализации Feature Set</li>
      <li>Каждый узел <ol>
          <li>проверяет, что он поддерживает целевой Feature Set</li>
          <li>выставляется запрет на вход в кластер узлов, который не поддерживает целевой Feature Set</li>
          <li>дожидается окончания конфликтующих операций (снятия снапшота?) и выставляет блокировку на их запуск до окончания выполнения команды</li>
          <li>отправляет single message с результатом  инициатору операции</li>
        </ol>
      </li>
      <li>Инициатор, если все узлы подтвердили возможность обновления, отправляет по Discovery сообщение о завершении "финализации" и все узлы активируют Ignite Features из целевого набора. Иначе операция завершается ошибкой</li>
    </ol>
  </ac:rich-text-body>
</ac:structured-macro>
<p>
  <br/>
</p>
<p>
  <span style="color: rgb(0,0,0);"> <strong>Процесс обработки сообщений</strong>  <br/>
    <strong>Основная концепция:</strong> Сообщения должны содержать список активных Ignite Feature отправителя, который использовался при создании сообщения. На основе него получатель принимает решение о том, как сообщение обрабатывать.<br/>
    <br/>
  </span>
</p>
<p>
  <span style="color: rgb(0,0,0);"> <ac:inline-comment-marker ac:ref="c03cafae-eaa5-4524-b1da-3502b76b14f3">В итоге код обработки сообщений на "новой" версии будет выглядеть примерно так:</ac:inline-comment-marker> </span>
</p>
<ac:structured-macro ac:macro-id="f74a576d-2c10-4521-80b5-7aa5f13ec4f5" ac:name="code" ac:schema-version="1">
  <ac:parameter ac:name="language">java</ac:parameter>
  <ac:plain-text-body><![CDATA[ private void onMessageReceived(Message msg) {
	if (msg.sender().features().isActive(NEW_FEATURE)) // <-- проверяем активные Ignite Features отправителя, а не локальные.
		<новая логика>
	else
		<старая логика>	
}	 ]]></ac:plain-text-body>
</ac:structured-macro>
<p>
  <span style="color: rgb(0,0,0);"> <br/>
    <strong>Комплексным решением</strong> описанных выше проблем может быть использование механизма <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-143+Unified+Operation+Context+Propagation">IEP-143 Unified Operation Context Propagation</a> (НЕ ДОДЕЛАН, т.к. нет приоритета, но большая часть работы уже готова)<br/>В начале выполнения операции к потоку прикрепляется активный Feature Set, который доступен в течении выполнения операции, в том числе и на удаленных узлах. A <code>isActive</code>  проверки будут основываться на Feature Set, прикрепленному к операции.  <br/>
    <strong>Плюсы:</strong> </span>
</p>
<ol>
  <li>
    <span style="color: rgb(0,0,0);">решатся проблемы операций, которые инициируются на узлах, которые еще не получили запрос активации новых фич<br/>
    </span>
  </li>
  <li>
    <span style="color: rgb(0,0,0);">решатся проблемы изменения статуса фич в процессе выполенения операции</span>
  </li>
  <li>
    <span style="color: rgb(0,0,0);">возможность автоматичского захвата и восстановлении  активных фич при выплнении распределенных операций</span>
  </li>
</ol>
<p>
  <span style="color: rgb(0,0,0);"> <strong>Минусы:</strong> <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-143+Unified+Operation+Context+Propagation">IEP-143 Unified Operation Context Propagation</a> еще нужно доделать. А если он и будет доделан, мы будем полагаться на относительно "сырой" механизм.</span>
</p>
<ac:structured-macro ac:macro-id="3df24793-68d3-489f-b932-ffd009a961bb" ac:name="expand" ac:schema-version="1">
  <ac:parameter ac:name="title">Текущий статус по Operation Context </ac:parameter>
  <ac:rich-text-body>
    <p>Сделано: <br/>Контейнер для аттрибутов, возможность прикреплять его потокам. Создавать "снимок" аттрибутов и их значений и его восстановление в произвольном потоке. <br/>(1 пункт из <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=406620460#IEP143UnifiedOperationContextPropagation-Description">IEP 143 Unified Operation Context Propagation Description</a>)<br/>
      <br/>Не сделано:</p>
    <ol>
      <li>Интеграция автоматического механизма "захвата" и "восстановления" аттрибутов в<ol>
          <li>тред пулы (eсть готовый ПР с апрувом от Никиты Амельчева. Но требует "архитектурного согласования" - <a href="https://issues.apache.org/jira/browse/IGNITE-26775">IGNITE-26775</a>)</li>
          <li>фьючи (есть POC)</li>
          <li>внутренние механизмы игнайта, аля Timeout Worker</li>
        </ol>
      </li>
      <li>реализация проверок статического анализатора</li>
      <li>пересылка и восстановление аттрибутов между узлами</li>
    </ol>
    <p>(Все пункты кроме первого - <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=406620460#IEP143UnifiedOperationContextPropagation-Description">IEP 143 Unified Operation Context Propagation Description</a>)</p>
  </ac:rich-text-body>
</ac:structured-macro>
<p>
  <span style="color: rgb(0,0,0);"> <br/>
    <strong>Альтернативное решение:</strong> <br/>Разработчик решает описанные проблемы для каждого случая индивидуально. С учетом того, что в начале фич и их проверок не должно быть большое кол-во, это на какое то время может быть посильной задачей.<br/>
    <strong>Например:</strong> сохраняет статус фич в начале выполнения операций и передает в процессе обработки. Самостоятельно решает какой список Ignite Feature прикрепить к отправляемому сообщению. <br/>
  </span>
</p>
<ac:structured-macro ac:macro-id="c5073332-280b-4982-b5f8-3e70a688c5bb" ac:name="expand" ac:schema-version="1">
  <ac:parameter ac:name="title">Возможный пример реализации, основанный на опыте CockroachDB</ac:parameter>
  <ac:rich-text-body>
    <p>//  At the same time, with requests/RPCs originating at other crdb nodes, the<br/>  //  initiator of the request gets to decide what's supported. A node should<br/>  //  not refuse functionality on the grounds that its view of the version gate<br/>  //  is as yet inactive. Consider the sender:<br/>  //<br/>  //      func invokeSomeRPC(req) {<br/>  //          if (specific-version is active) {<br/>  //              // Like mentioned above, this implies that all nodes in the<br/>  //              // cluster are running binaries that can handle this new<br/>  //              // feature. We may have learned about this fact before the<br/>  //              // node on the other end. This is due to the fact that migration<br/>  //              // manager informs each node about the specific-version being<br/>  //              // activated active concurrently. See BumpClusterVersion for<br/>  //              // where that happens. Still, it's safe for us to enable the new<br/>  //              // feature flags as we trust the recipient to know how to deal<br/>  //              // with it.<br/>  //              req.NewFeatureFlag = true<br/>  //          }<br/>  //          send(req)<br/>  //      }<br/>  //<br/>  //  And consider the recipient:<br/>  //<br/>  //      func someRPC(req) {<br/>  //          if !req.NewFeatureFlag {<br/>  //              // Legacy behavior...<br/>  //          }<br/>  //          // There's no need to even check if the specific-version is active.<br/>  //          // If the flag is enabled, the specific-version must have been<br/>  //          // activated, even if we haven't yet heard about it (we will pretty<br/>  //          // soon).<br/>  //      }</p>
  </ac:rich-text-body>
</ac:structured-macro>
<p>
  <span style="color: rgb(0,0,0);"> <br/>
    <strong>Минусы:</strong> С учетом сложности кодовой базы Ignite, отслеживать описанные проблемы во время разработки крайне сложно.</span>
</p>
<h2>Команды для управления RU  и Ignite Features</h2>
<p>Мы уже имеем команды для обозначения начала  RU и его завершения:<br/>
  <br/>
</p>
<ac:structured-macro ac:macro-id="7b7122e0-8627-4446-9bd6-b5599ae410b6" ac:name="code" ac:schema-version="1">
  <ac:parameter ac:name="language">bash</ac:parameter>
  <ac:plain-text-body><![CDATA[control.sh|bat --rolling-upgrade enable|disable]]></ac:plain-text-body>
</ac:structured-macro>
<p>В текущей реализации комада enable требует явного указания версии Ignite (версии на которую будет происходить обновление). После вызова этой команды вход узлов с версией, отличной от переданной - запрещен. </p>
<p>
  <strong>Предложение по улучшению: </strong>убрать параметр версии у команды enable. В итоге команда enable будет</p>
<ol>
  <li>разрешать вход узлов более высокой версии (ограничение, что в кластере одновременно могут находиться узлы только 2ух различных версий можно реализовать проверками на join'е нового узла)</li>
  <li>взводить "флаг", благодаря которому процесоры смогут определить, что в кластере могут быть узлы разных версий и ограничить выполнение каких то операций</li>
</ol>
<p>
  <strong>Что это даст: </strong>
</p>
<p>RU не будет привязан к определенной версии игнайта: </p>
<ol>
  <li>структура версий в apache и в банке могут отличаться</li>
  <li>администратор может: обновить версии узлов, но не активировать новые фичи → <ac:inline-comment-marker ac:ref="c286ba24-3d28-42d1-b376-886df1a224b3">узнать о проблемах после активации новых фич</ac:inline-comment-marker> →  дождаться выпуска следующей версии игнайта → еще раз обновить узлы → активировать новые фичи<br/>
    <br/>
  </li>
</ol>
<p>
  <strong>Предложение по улучшению: </strong>добавить команду <code>control.sh|bat --rolling-upgrade</code> finalize [имя плагина?]</p>
<p>
  <strong>Что это даст: </strong>С помощью этой команды администратор сможет актиивировать Ignite Feature Set после обновления версии узлов.<br/>Данная команда автоматически вызовет <code>control.sh|bat --rolling-upgrade</code> disable перед успешным завершением и восстановит запрет на вход узлов c версией, отличной от текущей.<br/>
  <br/>
</p>
<p>
  <br/>В итоге с точки зрения пользователя RU будет состоять из 3 шагов:</p>
<ol>
  <li>Вызвать команду <code>control.sh|bat rolling-upgrade enable</code> </li>
  <li>Для каждого узла: вывести узел, обновить его исходный код и ввести узел обратно в кластер</li>
  <li>Активировать новый Ignite Feature Set, вызовом команды <code>control.sh|bat --rolling-upgrade finalize</code>
  </li>
</ol>
<p>Если по каким то причинам администратор передумал обновлять кластер - он может вызвать  <code>control.sh|bat --rolling-upgrade disable</code> при условии, что все версии всех узлов уластера одинаковы.</p>
<h2>Использование Ignite Features в механизмах сериализации сообщений</h2>
<p>Предложенный в <a href="https://cwiki.apache.org/confluence/display/IGNITE/Communication+protocol">IEP-132</a> механизм сериализации сообщений для отправки между узлам различных версий и связанные с ним аннотаций <code>@Since</code> и <code>@Until</code> требует пересмотра.</p>
<ol>
  <li>Структура сообщений не зависит от версии исходного кода взаимодействующих узлов - узлы могут быть "новой" версии, но до "финализации" Ignite Feature Set требовать "старых" полей</li>
  <li>Использование версии Ignite в логике создаст сложности при выпуске релизов в в банке, где сами версии и их кол-во отличается</li>
</ol>
<h3>Предлагаемые измененеия в дизайне</h3>
<p>Предлагается вместо аннотаций @Since(Ver) @Until(Ver) использовать аннотации <ac:inline-comment-marker ac:ref="ac5ed7ab-1e40-47d0-a0a2-bc81471d9a71"> <ac:inline-comment-marker ac:ref="1192f73f-8fe1-46f9-837e-6564da732a16">@IntroducedBy(IGNITE_FEATURE) @DeprecatedBy(IGNITE_FEATURE)</ac:inline-comment-marker> </ac:inline-comment-marker> для подсказок сериализатору, какие поля нужно использовать при десериализации/сериализации сообщений.</p>
<p>Поле сообщения может быть помечено 2 аннотациями одновременно с указанием различных Ignite Features. Такое возможно, если одно и тоже поле сначала было добавлено одной Ignite Feature, а следом задепрекейчено другой.</p>
<p>В итоге, если отправитель сообщения не поддерживает указанную в аннотации Ignite Feature - поле пропускается/сериализуется в зависимости от типа аннотации. Аналогично с десериализацией, но в этом случае используются активные Ignite Features отправителя.</p>
<h2>
  <ac:inline-comment-marker ac:ref="26638181-13de-4a05-924b-b4e491ba604a"> <ac:inline-comment-marker ac:ref="d708607d-c7ef-4030-85e3-6a235f498f12">Ignite Features в релизном процессе</ac:inline-comment-marker> </ac:inline-comment-marker>
</h2>
<p>Для каждой релизной версии определяется диапазон фич, которые могут быть активированы/деактивированы для имитации работы узлов предыдущих версий.</p>
<p>
  <ac:structured-macro ac:macro-id="aa0ca343-a706-4fc7-a127-e9ec869dbf6f" ac:name="drawio" ac:schema-version="1">
    <ac:parameter ac:name="border">true</ac:parameter>
    <ac:parameter ac:name="diagramName">release features</ac:parameter>
    <ac:parameter ac:name="simpleViewer">false</ac:parameter>
    <ac:parameter ac:name="width"/>
    <ac:parameter ac:name="links">auto</ac:parameter>
    <ac:parameter ac:name="tbstyle">top</ac:parameter>
    <ac:parameter ac:name="lbox">true</ac:parameter>
    <ac:parameter ac:name="diagramWidth">962</ac:parameter>
    <ac:parameter ac:name="revision">4</ac:parameter>
    <ac:parameter ac:name=""/>
  </ac:structured-macro>
</p>
<p>
  <br/>
</p>
<p>LOW_UPDATE_BOUND определяет левую границу диапазона фич, "отключение" которых поддерживается.  Используется для задания  мнимальной версии игнайта, с которой возможно обновиться с помощью RU на текущую версию.</p>
<p>HIGH_UPDATE_BOUND определяет правую границу диапазона фич, которые можно активировать.<br/>
  <br/>В итоге если у двух релизов диапазоны фич не пересекаются - обновиться с помощью RU невозможно.<br/>
  <br/>В master ветке id фич должны монотонно и непрерывно возрастать.<br/>
  <br/>Удаление фич из начала цепочки возможно только в master ветке. <br/>Удаление фич в старых релизных ветках  - запрещено.</p>
<h3>Черепик комиитов, которые привносят новые Ignite Features, в старые релизные ветки</h3>
<p>При черепике комитов, которые привносят новые Ignite Features, в другие релизные ветки монотонное возрастание и непрерывность ID фич нарушается.<br/>Например:<br/>Фичи мастера: <code>[2 → 10]</code> <br/>Фичи старой релизной ветки после черепика комита, который привнес фичу c ID=10 : <code>[2 → 6, 10]</code>  <br/>
  <br/>
</p>
<p>
  <strong>Пример RU между старыми релизными ветками:</strong>
  <br/> <br/>Состояние фич ветки 22.1</p>
<p>
  <ac:structured-macro ac:macro-id="cec2313c-981e-40ad-a857-aeb2c93a8245" ac:name="drawio" ac:schema-version="1">
    <ac:parameter ac:name="border">true</ac:parameter>
    <ac:parameter ac:name="diagramName">cherepick old</ac:parameter>
    <ac:parameter ac:name="simpleViewer">false</ac:parameter>
    <ac:parameter ac:name="width"/>
    <ac:parameter ac:name="links">auto</ac:parameter>
    <ac:parameter ac:name="tbstyle">top</ac:parameter>
    <ac:parameter ac:name="lbox">true</ac:parameter>
    <ac:parameter ac:name="diagramWidth">521</ac:parameter>
    <ac:parameter ac:name="revision">2</ac:parameter>
    <ac:parameter ac:name=""/>
  </ac:structured-macro>
</p>
<p>
  <br/>
</p>
<p>Состояние фич ветки 23.5</p>
<p>
  <ac:structured-macro ac:macro-id="83e856c6-ae5f-4fe0-af71-7a161de71f40" ac:name="drawio" ac:schema-version="1">
    <ac:parameter ac:name="border">true</ac:parameter>
    <ac:parameter ac:name="diagramName">Copy of cherepick new</ac:parameter>
    <ac:parameter ac:name="simpleViewer">false</ac:parameter>
    <ac:parameter ac:name="width"/>
    <ac:parameter ac:name="links">auto</ac:parameter>
    <ac:parameter ac:name="tbstyle">top</ac:parameter>
    <ac:parameter ac:name="lbox">true</ac:parameter>
    <ac:parameter ac:name="diagramWidth">861</ac:parameter>
    <ac:parameter ac:name="revision">4</ac:parameter>
    <ac:parameter ac:name=""/>
  </ac:structured-macro>
</p>
<p>
  <br/>
</p>
<p>Состояние фич узла версии 23.5 после присоединения к кластеру версии 22.1: </p>
<p>
  <ac:structured-macro ac:macro-id="f32f9e35-baf8-4215-8e03-5d4bd289ac9e" ac:name="drawio" ac:schema-version="1">
    <ac:parameter ac:name="border">true</ac:parameter>
    <ac:parameter ac:name="diagramName">Copy of Copy of cherepick new</ac:parameter>
    <ac:parameter ac:name="simpleViewer">false</ac:parameter>
    <ac:parameter ac:name="width"/>
    <ac:parameter ac:name="links">auto</ac:parameter>
    <ac:parameter ac:name="tbstyle">top</ac:parameter>
    <ac:parameter ac:name="lbox">true</ac:parameter>
    <ac:parameter ac:name="diagramWidth">861</ac:parameter>
    <ac:parameter ac:name="revision">3</ac:parameter>
    <ac:parameter ac:name=""/>
  </ac:structured-macro>
</p>
<p>
  <br/>
</p>
<p>В итоге: </p>
<ol>
  <li>Ignite Feature Set по своей структуре схож с Partition Update Counter.</li>
  <li>Фичи, находящиеся в середине Ignite Feature Set цепочки, могут быть неактивны в время RU.</li>
  <li>
    <p>Для возможности RU</p>
    <ol>
      <li>
        <p>узел "новой" версии должен иметь возможность "отключить" фичи, не активные на узлах кластера "старой" версии</p>
      </li>
      <li>узел "новой" версии должен поддерживать все фичи "старой" версии ("удаленные" фичи являются поддеживаемыми и активными по умолчанию)</li>
    </ol>
  </li>
</ol>
<p>
  <br/>
</p>
<p>
  <br/>
</p>
<table class="wrapped">
  <colgroup>
    <col/>
    <col/>
    <col/>
  </colgroup>
  <tbody>
    <tr>
      <th scope="col">Доступные фичи <br/>на RU source version</th>
      <th scope="col">Доступные фичи <br/>на RU target version</th>
      <th scope="col">Возможность RU</th>
    </tr>
    <tr>
      <td>
        <p>
          <code>[2→5, 10]</code>
        </p>
      </td>
      <td>
        <code>[2→8]</code>
      </td>
      <td>
        <ac:emoticon ac:name="cross"/>
      </td>
    </tr>
    <tr>
      <td>
        <code>[2→5, 10]</code>
      </td>
      <td>
        <code>[2→8, 10]</code>
      </td>
      <td>
        <ac:emoticon ac:name="tick"/>
      </td>
    </tr>
    <tr>
      <td>
        <code>[2→3, 10]</code>
      </td>
      <td>
        <code>[5→7, 10]</code>
      </td>
      <td>
        <ac:emoticon ac:name="cross"/>
      </td>
    </tr>
    <tr>
      <td>
        <code>[2→4, 8, 11]</code>
      </td>
      <td>
        <code>[3→7, 8, 11]</code>
      </td>
      <td>
        <ac:emoticon ac:name="tick"/>
      </td>
    </tr>
  </tbody>
</table>
<p>
  <br/>
</p>
<h3>Пример реализации</h3>
<p>
  <br/>
</p>
<p>На момент релиза список Ignite Features выглядит так:</p>
<ac:structured-macro ac:macro-id="f9af035e-b0e1-468b-a120-68376a1de260" ac:name="code" ac:schema-version="1">
  <ac:parameter ac:name="language">java</ac:parameter>
  <ac:plain-text-body><![CDATA[public class IgniteReleaseFeatures {

	/** */
	public static IgniteFeature FEATURE_0 = new IgniteKernalFeature(0); // Добавлено в 19.x релизе
    
    /** */
	public static IgniteFeature FEATURE_1 = new IgniteKernalFeature(1); // Добавлено в 19.x релизе   

/** */
public static IgniteFeature FEATURE_2 = new IgniteKernalFeature(2); // Добавлено в 20.x релизе

    /** */
	public static IgniteFeature FEATURE_3 = new IgniteKernalFeature(3); // Добавлено в 20.x релизе

    /** */
	public static IgniteFeature FEATURE_4 = new IgniteKernalFeature(4); // Добавлено в 20.x релизе 

    /** */
	public static IgniteFeature FEATURE_5 = new IgniteKernalFeature(5); // Добавлено во время разработки 21.0 релиза
}]]></ac:plain-text-body>
</ac:structured-macro>
<p>
  <br/>Действия релиз-инженера при выпуске релиза 21.0:</p>
<ol>
  <li>удаляет переменные FEATURE_0 и FEATURE_1 и все связанные с ними проверки <code>isActive</code> из кода проекта ← возможности обновиться с 19.x версии уже не будет</li>
</ol>
<p>После выпуска 21.0 список Ignite Features  выглядит так:<br/>
  <br/>
</p>
<ac:structured-macro ac:macro-id="4e4306ac-3791-4019-874f-719277d069b1" ac:name="code" ac:schema-version="1">
  <ac:parameter ac:name="language">java</ac:parameter>
  <ac:plain-text-body><![CDATA[public class IgniteReleaseFeatures {
    /** */
	public static IgniteFeature FEATURE_2 = new IgniteKernalFeature(2); // Добавлено в 20.x релизе

    /** */
	public static IgniteFeature FEATURE_3 = new IgniteKernalFeature(3); // Добавлено в 20.x релизе

    /** */
	public static IgniteFeature FEATURE_4 = new IgniteKernalFeature(4); // Добавлено в 20.x релизе 

    /** */
	public static IgniteFeature FEATURE_5 = new IgniteKernalFeature(5); // Добавлено в 21.x релизе
}]]></ac:plain-text-body>
</ac:structured-macro>
<p>
  <br/>
</p>
<p>Игнайт во время запуска парсит IgniteReleaseFeatures и создает структуру по типу</p>
<ac:structured-macro ac:macro-id="acd1808d-2f27-48f8-9d5a-a3e1d48217ac" ac:name="code" ac:schema-version="1">
  <ac:parameter ac:name="language">java</ac:parameter>
  <ac:parameter ac:name="title">Пример реализации</ac:parameter>
  <ac:plain-text-body><![CDATA[public class IgniteFeaturesSet {
    /** */
	private int lwm;  // <- Левая граница диапазона поддерживаемых фич

    /** */
	private int hwm; // <-- Правая граница диапазона поддерживаемых фич

    /** */
	private int[] featuresIds; // <-- Список фич, доступных для отключения. На мастере будет содержать непрерывный диапазон значений. На релизах, которые содержат черепики - нет. 
}]]></ac:plain-text-body>
</ac:structured-macro>
<p>и использует ее для выполнения проверок совместимости и статуса фич.<br/>
  <br/>Потенциально может потребоваться утилита для offline проверки совместимости двух релизов. Она должны парсить значения, указаные в IgniteReleaseFeatures для сравниваемых релизов и выдавать результат совместимости. </p>
<h2>Использование Ignite Features в работе Management API</h2>
<h3>Решаемая проблема</h3>
<p>Клиентская часть Management API являеся оберткой над тонким клиентом. Выполнение команд происходит на основе запуска Compute Task, исходный код которой доступен узлам кластера, и аргумента - IDTO, который пересылается в сериализованном виде. <br/>Формат полей IDTO может меняться от релиза к релизу. Это <strong>не</strong> должно приводить к ошибкам выполнения Management команд (проблема аналогична пересылке сообщений между узлами кластера, только теперь сторонами выступают узел и тонкий клиент ).</p>
<h3>Предлагаемое решение</h3>
<p>Control.sh можно представить как узел с фиксированной версии, для которого активированы все доступные ему Ignite Features.<br/>
  <br/>При отправке IDTO control.sh прикрепляет все активные Ignite Features.<br/>
  <br/>Cервер при получении запроса проверяет:</p>
<ol>
  <li>что control.sh еще поддерживаемой версии, иначе завершает запрос ошибкой (убеждается, что Ignite Features из IDTO попадает в его диапазон доступных фич (см. Использование Ignite Features в релизном процессе),</li>
  <li>что все фичи активные на control.sh, активны и на серверном узле </li>
</ol>
<p>
  <ac:inline-comment-marker ac:ref="8458fff7-1a27-49eb-9df2-f35c494bbb3a">Сервер, ориентируясь на Ignite Feature Set отправителя (control.sh), принимает решение как именно обрабатывать IDTO и в каком формате отправлять результат.</ac:inline-comment-marker> <br/>
  <br/>
  <strong>Пример:</strong>
</p>
<p>Есть кластер 2.19<br/>Начался процесс обновления до 2.20<br/>Пока он не завершится и Ignite Feature Set 2.20 версии не будет активирован - к кластеру может подключиться только control.sh версии 2.19<br/>После того как Ignite Feature Set 2.20 версии будет активирован -  к кластеру сможет подключиться control.sh, как версии 2.19 так и 2.20<br/>
  <br/>
  <strong>Минусы: </strong>
</p>
<ol>
  <li>админы смогут обновить либы control.sh только после полного завершения RU. </li>
  <li>
    <ac:inline-comment-marker ac:ref="31e0f1ee-bcf1-461b-b267-ddd39bdac3f7">админы могут игнорировать обновление control.sh на 2.20</ac:inline-comment-marker>.</li>
</ol>
<p>
  <strong>Плюсы</strong>: Не трогаем протокол тонкого клиента<br/>
  <br/>
  <strong>Альтернативное решение:</strong> <br/>
  <br/>Поддерживать на тонком клиенте список активных Ignite Features кластера аналогично механизмам Cluster Discovery/Partition Awareness</p>
<p>В этом случае control.sh сформирует IDTO в соответсвии с активнми Ignite Feature сервера.<br/>
  <br/>
  <strong>Плюсы: </strong>
</p>
<ol>
  <li>админы смогут обновить либы control.sh, не дожидаясь завершения RU</li>
</ol>
<p>
  <strong>Минусы:</strong>
</p>
<ol>
  <li>Меняем протокол тонкого клиента</li>
  <li>Сервер не имеет возможности уведомлять тонкого клиента о смене активных фич напрямую. В итоге нам опять нужно учитывать на сервере Ignite Features, которые были активны на стороне control.sh при создании запроса.  </li>
</ol>
<h2>
  <ac:inline-comment-marker ac:ref="766eed3d-f0ec-46c5-8b9a-d3742642e949">Использование Ignite Features внешними компонентами (плагины, SPIs)</ac:inline-comment-marker>
</h2>
<h3>Решаемая проблема</h3>
<p>Ignite позволяет кастомизировать свое поведение посредством плагинов или SPIs, которые разрабатываются пользователями независимо.</p>
<p>С момента появления в Ignite поддержки RU, разработчики плагинов должны гарантировать, что их код соответствует всем требованиям, описанным в в <a href="https://cwiki.apache.org/confluence/display/IGNITE/Communication+protocol">IEP-132</a>, и <strong>не</strong> приводит к проблемам во время RU.</p>
<p>
  <strong>Примеры кастомной логики, которая может привести к проблемам RU:</strong>
</p>
<ol>
  <li>Реализация кастомных Management Commands (версия классов аргументов на клиентской и серверной части могут отличаться. см. <code>Использование Ignite Features в работе Management API</code> )</li>
  <li>Запись плагинами значений в метастор (при включенном PDS обновление классов значений может привести к проблемам десериализации )</li>
  <li>Создание объектов, которые в дальнейшем пересылаются между узлами механизмами Ignite (проблемы аналогичны тем, что описаны выше для внутренних сообщений Ignite)<br/>и т.д.</li>
</ol>
<p>Предполагается, что администратор Ignite в процессе  RU может обновить на узлах исходный код Ignite + плагинов или только исходный код плагинов. </p>
<p>
  <strong>В итоге плагины должны иметь возможность: </strong>
</p>
<ol>
  <li>Иметь личную реализацию Ignite Feature Set, отражающую несовместимые изменения</li>
  <li>При инициализации сообщить Ignite <ol>
      <li>
        <span style="color: rgb(23,43,77);">диапазон фич, которые могут быть активированы/деактивированы</span>
      </li>
      <li>Уникальное имя компонента</li>
    </ol>
  </li>
  <li>Проверять статус Ignite Feature, как внутренних для Ignite, так и привнесенных самим плагином</li>
  <li>Получать уведомления о смене статуса Ignite Feature</li>
</ol>
<p>
  <strong>Ignite должен гарантировать, что </strong>
</p>
<ol>
  <li>
    <ac:inline-comment-marker ac:ref="6768fcd8-236f-47c4-be7f-15385dba010d">При запуске первого узла кластера все доступные Ignite Features</ac:inline-comment-marker>
    <ac:inline-comment-marker ac:ref="6768fcd8-236f-47c4-be7f-15385dba010d"> плагина будут активны</ac:inline-comment-marker>
  </li>
  <li>При присоединении узла к существующему кластеру на нем будет активированы только Ignite Features активные на кластере. Если это невозможно, узел <strong>не </strong>должен войти в топологию.</li>
  <li>Вызов команды  <code>control.sh|bat <ac:inline-comment-marker ac:ref="ed6d846e-fdf5-4b60-9929-6ed14caa8ea5">--rolling-upgrade</ac:inline-comment-marker> finalize</code> приведет к активации всех доступных Ignite Features плагина на всех узлах кластера.</li>
  <li>Вернет актуальный статус для Ignite Feature, привнесенной плагином.</li>
</ol>
<p>
  <strong>Под вопросом:</strong> Нужно ли давать возможность плагину напрямую участвовать в процессе "финализации" - это даст возможность</p>
<ol>
  <li>дождаться завершения/заблокировать какие то операции</li>
  <li>добавить кастомную логику определения готов ли узел к "финализации" Ignite Feature Set или нет</li>
</ol>
<p>,но  усложнит код и сделает решение более хрупким.</p>
<h3>Предлагаемое решение</h3>
<ac:structured-macro ac:macro-id="0f6e45c8-531b-4cda-ae01-490f66dec4e6" ac:name="expand" ac:schema-version="1">
  <ac:parameter ac:name="title">Пример реализации</ac:parameter>
  <ac:rich-text-body>
    <p>Если разработчику плагина требуется интегрироваться в механизм RU, он наследует реализацию своего кастомного компонента  от следующего интерфейса</p>
    <ac:structured-macro ac:macro-id="77736f2d-a5ca-4c35-abef-5c1a630c641e" ac:name="code" ac:schema-version="1">
      <ac:parameter ac:name="language">java</ac:parameter>
      <ac:plain-text-body><![CDATA[public interface RollingUpgradeAwareComponent {
	/**  */
	public Collection<IgniteFeature> features();


 	/** */
	public String name();
}]]></ac:plain-text-body>
</ac:structured-macro>
<p>
<br/>Ignite, если внешний компонент реализует  RollingUpgradeAwareComponent интерфейс,  заводит структуру, которая хранит </p>
<ol>
<li>Имя компонента</li>
<li>Структуру, хранязую диапазон доступных фич</li>
<li>Список текущих активных Ignite Feature - используется для <code>isActive</code>  проверок и обновляется после "финализации". </li>
</ol>
<p>
<br/>
</p>
<p>Плагин выглядит как то так </p>
<ac:structured-macro ac:macro-id="eb7d793d-e3bd-48c7-aa2d-ba11941f0451" ac:name="code" ac:schema-version="1">
<ac:parameter ac:name="language">java</ac:parameter>
<ac:plain-text-body><![CDATA[public class PluginImplementation implements RollingUpgradeAwareComponent, IgnitePluggableComponent {
/** */
public static IgniteFeature PLUGIN_FEATURE_0 = new PluginFeature(0);

    /** */
	public static IgniteFeature PLUGIN_FEATURE_1 = new PluginFeature(1);

	/** */
	public static IgniteFeature PLUGIN_FEATURE_2 = new PluginFeature(2);

 	/** */
	@IgniteInstanceResource
	private Ignite ignite;     

	/** */
	@Override public Collection<IgniteFeature> features() {
		return Arrays.asList(PLUGIN_FEATURE_0, PLUGIN_FEATURE_1, PLUGIN_FEATURE_2);
	}

	/** */
	@Override public String name() {
		return "my-plugin";
	}

	/** */
	@Override public void doPluginWork() {
		if(!ignite.feature().isActive(PLUGIN_FEATURE_2))
			ignite.feature().subscribeOnActivation(PLUGIN_FEATURE_2, () -> start());

	 	if (ignite.feature().isActive(PLUGIN_FEATURE_1))
// <логика>
else
// <логика>
}
    
}]]></ac:plain-text-body>
</ac:structured-macro>
</ac:rich-text-body>
</ac:structured-macro>
<p>На первом этапе предлагается <strong>не</strong> встраивать фичи напрямую в плагины.<br/>
  <strong>Причины:</strong>
</p>
<ol>
  <li> Принято решение, что пока API RU не выносится в apache.</li>
  <li>API и поведение плагинов требует рефакторинга:<br/>
    <ol>
      <li>Плагины могут быть настроены/отсутствовать на произвольной части узлов.</li>
      <li>Версия плагинов передается, как String и никак не валидируется</li>
    </ol>
  </li>
  <li>Без пункта 2 и с учетом того, что API плагинов публичное, реализация RU сильно осложняется</li>
</ol>
<p>Несмотря на это нам уже сейчас <strong>стоит реализовать внутренний механизм</strong>, который позволял бы регистрировать в Ignite дополнительные "компоненты", участвующие в RU.</p>
<p>Каждый "компонент" предоставляет свое имя, версия и набор поддерживаемых фич.<br/>Решистрация осуществляется через механизм Extensions<br/>Наличие "компонентов" на узлах валидируется:</p>
<ol>
  <li>Все серверные узлы имеют один и тот же набор компонентов, если RU неактивен.</li>
  <li>Все компоненты на клиентских узлах должны быть известны серверным у, но на клиентских узлах часть компонентов может отсутсвовать (плагины могут не требоваться на клиентскиз узлах/в банке конфигурацию клиентских узлов мы не контролируем). </li>
</ol>
<p>
  <br/>
</p>
<p>
  <strong>Зачем делать предложенный мехаизм сейчас?</strong>
</p>
<p>Реализация поддержки множества компонентов приведет к изменению структур данных RU, которые передаются по сети. А это после релиза RU менять будет проблематично.<br/>Это даст возможность сразу встроить плагины для банка в процесс RU, пусть и через внутренне API.</p>
<p>
  <br/>
  <strong>Что будет требоваться в наших плагинах:</strong>
</p>
<ol>
  <li>
    <p>Рализовать интерфейс (интерфейс - INTERNAL) </p>
    <ac:structured-macro ac:macro-id="4c6e70eb-b4e4-4897-8903-d402d012611d" ac:name="code" ac:schema-version="1">
      <ac:parameter ac:name="language">java</ac:parameter>
      <ac:plain-text-body><![CDATA[/**
 * Provides the ability for internal components (e.g., plugins and SPIs) to provide their own independent sets of
 * {@link IgniteFeature}s that will be accounted during Rolling Upgrade.
 *
 * @see IgniteFeature
 */
public interface IgniteComponentFeaturesProvider extends Extension {
    /** The name of the Ignite component that provides its own set of {@link IgniteFeature}s. */
    public String componentName();

    /** The version of the Ignite component with which the provided {@link IgniteFeature}s will be associated. */
    public IgniteProductVersion componentVersion();

    /**
     * The set of features supported by the Ignite component. Note that the {@link IgniteFeature#componentName()} value
     * for all features must match the {@link #componentName()} value.
     */
    public Collection<IgniteFeature> features();
}

]]></ac:plain-text-body>
</ac:structured-macro>
  </li>
  <li>
    <p>Зарегистрировать extensions в registry</p>
    <ac:structured-macro ac:macro-id="da63a523-4a5f-4373-b322-1195eebaf5f9" ac:name="code" ac:schema-version="1">
      <ac:parameter ac:name="language">java</ac:parameter>
      <ac:plain-text-body><![CDATA[ 		AbstractTestPluginProvider() {
           	@Override public String name() {
                return "test-rolling-upgrade-processor-provider";
            }

            /** {@inheritDoc} */
            @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
                registry.registerExtension(
                    IgniteComponentFeaturesProvider.class,
                    new TestPluginComponentFeaturesProvider(versions.pluginVersion()));
            }
        });]]></ac:plain-text-body>
    </ac:structured-macro>
  </li>
  <li>
    <p> Проверка статуса фич и подписка на активацию будет возможна только через KernalContext.</p>
  </li>
</ol>
<p>
  <br/>
</p>
<p>
  <strong>К чему мы будем стремиться</strong>:</p>
<ol>
  <li> Дать возможность пользователю задавать фичи и версию во время имплементации  org.apache.ignite.plugin.PluginProvider<br/>
    <br/>
  </li>
  <li>Дать возможность доступа к состоянию фич через интерфейс Ignite или интерфейс PluginContext.</li>
</ol>
<p>
  <br/>Тикет: <a class="" href="https://issues.apache.org/jira/browse/IGNITE-28837">https://issues.apache.org/jira/browse/IGNITE-28837</a>
</p>
<h1>Ссылки на имплементации в других продуктах</h1>
<p>CockroachDB</p>
<p>
  <a href="https://github.com/cockroachdb/cockroach/blob/master/pkg/clusterversion/cockroach_versions.go">https://github.com/cockroachdb/cockroach/blob/master/pkg/clusterversion/cockroach_versions.go</a> <br/>
  <a href="https://github.com/cockroachdb/cockroach/blob/master/pkg/kv/kvserver/kvstorage/cluster_version.go">https://github.com/cockroachdb/cockroach/blob/master/pkg/kv/kvserver/kvstorage/cluster_version.go</a>
</p>
<p>Kafka</p>
<p>
  <a class="" href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-584%3A+Versioning+scheme+for+features">https://cwiki.apache.org/confluence/display/KAFKA/KIP-584%3A+Versioning+scheme+for+features</a> <br/>
  <a class="" href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-1022%3A+Formatting+and+Updating+Features">https://cwiki.apache.org/confluence/display/KAFKA/KIP-1022%3A+Formatting+and+Updating+Features</a>
</p>
<p>
  <br/>
</p>
<p>
  <br/>
</p>
<p>
  <br/>
</p>
