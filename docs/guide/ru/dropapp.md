# Platform V DropApp

Platform V DropApp (СберТех) — дистрибутив Kubernetes со своими компонентами платформы, входом через Dex и
VictoriaMetrics для метрик. Glafira узнаёт его и добавляет то, чего нет у обычного Kubernetes: вход через OIDC, проекты
из `projects.dropapp.ru`, состояние платформы в обзоре кластера, три правила проверки и историю метрик из
VictoriaMetrics. Всё остальное работает, как на любом кластере Kubernetes.

## Как Glafira узнаёт DropApp {#detection}

Кластер с группой API `config.dropapp.ru` — это DropApp: группа есть у кластеров версии 3.x и новее, установленных
`dactl`. Список групп API может читать любой вошедший пользователь, поэтому DropApp определяется и у пользователя одного
namespace. У DropApp 1.x–2.x такой группы нет, и он работает как Kubernetes.

[[ui:cluster.connection.platform]] показывает версию DropApp из `ClusterMetadata` (`spec.dappVersion`), например
«DropApp 5.1.2». Если `ClusterMetadata` прочитать нельзя (у пользователя namespace прав на него обычно нет), в подписи —
версия Kubernetes: «DropApp (Kubernetes 1.29)». LLM-ассистент видит ту же платформу и подпись в `list_connections` и
`get_cluster_overview`.

## Вход {#sign-in}

DropApp впускает людей через Dex: страница Dex спрашивает имя и пароль каталога (LDAP). Сама Glafira пароль не
спрашивает: вы вводите его на странице Dex в браузере.

- **Kubeconfig от Loginapp.** Loginapp (`https://loginapp.<домен>`) выдаёт kubeconfig с exec-плагином
  `kubectl oidc-login get-token`, в аргументах которого издатель, клиент и его секрет. Импортируйте его пунктом
  [[ui:sidebar.importKubeconfig]] и оставьте у контекста [[ui:kubeconfig.useOidc]]: издателя, клиента, секрет, scopes
  и CA издателя Glafira возьмёт из аргументов.
- **Вручную.** Выберите в форме подключения [[ui:connections.form.auth.oidc]]: издатель —
  `https://auth.<кластер>.<домен>`, клиент — `dapp`, секрет клиента — у администратора или в kubeconfig от Loginapp
  (`--oidc-client-secret`).

Затем нажмите [[ui:cluster.session.loginOidc]] на вкладке подключения. Поля, срок токена и его обновление описаны в
разделе [Вход через OIDC](connections.md#oidc).

**Порты 8000 и 18000.** Dex возвращает только на адреса, зарегистрированные у клиента `dapp`. Glafira использует адреса
kubelogin: `http://localhost:8000`, а если порт 8000 занят — `http://localhost:18000`. Если заняты оба, вход завершится
ошибкой с подсказкой. Обычно их держит kubelogin, который ждёт входа в другом терминале: завершите или закройте его.
Найти программу помогут `lsof -i :8000` в macOS и Linux или `netstat -ano | findstr :8000` в Windows. Если у клиента
`dapp` вашего кластера таких адресов нет, попросите администратора DropApp добавить их клиенту Dex (в конфигурации по
умолчанию они есть).

**CA издателя.** Dex стоит за ingress, и его сертификат часто выпущен другим CA, чем сертификат API-сервера. Kubeconfig
от Loginapp называет этот CA в аргументе плагина `--certificate-authority` (файл) или `--certificate-authority-data`;
импорт копирует его в поле [[ui:connections.form.oidc.issuerCaPem]]. Если файл прочитать не удалось, импорт
предупреждает и создаёт подключение без CA: вставьте сертификат в форме. Без нужного CA вход останавливается с ошибкой
«Сертификат издателя OIDC … не доверен».

**Exec-плагин как запасной путь.** Если вход через OIDC вашему кластеру не подходит, выберите при импорте
[[ui:kubeconfig.useExec]]: Glafira запустит `kubectl oidc-login`, как написано в kubeconfig от Loginapp, и плагин сам
откроет браузер (см. [Exec-плагины](connections.md#exec)). Плагин должен быть установлен, а организация может запретить
exec-плагины. В документации DropApp версия плагина написана как `client.authentication.io/v1beta1`; с ней вариант
exec недоступен, и диалог объясняет почему. Исправьте её в файле на `client.authentication.k8s.io/v1beta1`. Входу через
OIDC это не мешает.

## Проекты {#projects}

В DropApp 5.x список проектов берётся из `projects.dropapp.ru`: этот API отдаёт только проекты, к которым у вас есть
доступ, а имя проекта совпадает с именем namespace. Glafira берёт список из него, если в кластере нет проектов
OpenShift, поэтому пользователь namespace видит свои проекты, не вписывая их вручную. У кластеров 3.x и 4.x такого API
нет: Glafira показывает список namespace, а если его читать нельзя, перечислите свои проекты в
[[ui:connections.form.manualNamespaces]].

## Обзор кластера и проверка {#overview}

У DropApp в [[ui:cluster.overview.title]] появляется часть DropApp:

- имя кластера и версия DropApp (`ClusterMetadata`);
- [[ui:cluster.dropapp.components]] (`Component` и `Tool`): сколько их и какие не в норме — `failed`, `error` или
  `upgrading` — со статусом, числом повторов и последним событием `Warning` по объекту (у первых 20);
- [[ui:cluster.dropapp.nodes]] (`DropAppNode`) не в статусе `ready`;
- [[ui:cluster.dropapp.ingressTls]]: объекты `IngressTLS`, у которых условие `Ready` не `True`. Объект без этого условия
  ещё не обработан и не учитывается.

Незнакомый Glafira статус показывается как есть и проблемой не считается. Часть, которой в кластере нет, пуста; часть,
которую вам нельзя читать, показывается как «—» с причиной: у пользователя namespace здесь обычно прочерки. При входе
через OIDC вкладка подключения показывает и [[ui:cluster.connection.tokenExpires]].

Проверка кластера (см. [Проверка кластера и замечания](health-check.md#rules)) добавляет три правила:

| Правило | Когда | prod, stage | прочие |
|---|---|---|---|
| [[ui:health.ruleNames.cluster.componentFailed]] | `Component` или `Tool` в статусе `failed` или `error` | [[ui:health.severity.critical]] | [[ui:health.severity.warning]] |
| [[ui:health.ruleNames.cluster.dropappNodeNotReady]] | `DropAppNode` не в статусе `ready` | [[ui:health.severity.warning]] | [[ui:health.severity.info]] |
| [[ui:health.ruleNames.cluster.ingressTlsNotReady]] | у `IngressTLS` условие `Ready` не `True` | [[ui:health.severity.warning]] | [[ui:health.severity.info]] |

`upgrading` сбоем не считается: идущее обновление не отличить от застрявшего. Причина сбоя — в событиях объекта:
`kubectl describe tool ИМЯ` (или `component`). Если эти объекты вам читать нельзя, проверка помечается как
[[ui:health.partial]].

## Метрики {#metrics}

DropApp хранит историю метрик в VictoriaMetrics в namespace `dapp-monitoring`. В режиме
[[ui:connections.form.metricsMode.auto]] Glafira сначала пробует
`dapp-monitoring/vmselect-victoria-metrics-k8s-stack:8481` через прокси API-сервера (API Prometheus — по пути
`/select/0/prometheus`), затем обычные кандидаты Kubernetes, затем текущие значения из `metrics.k8s.io`.

- **Администратор.** Прокси требует права `get services/proxy` в `dapp-monitoring`. У администратора DropApp оно есть,
  и история работает без настроек.
- **Пользователь namespace.** VictoriaMetrics не разделяет метрики по namespace, поэтому права на прокси у таких
  пользователей нет. Шаг [[ui:connections.form.step.metrics]] проверки подключения показывает, что VictoriaMetrics
  отвергнут из-за прав, и Glafira берёт текущие значения из `metrics.k8s.io`: без истории правила проверки по ресурсам
  не работают. Для истории укажите свой источник:
  - ingress `metrics.<кластер>.<домен>`: [[ui:connections.form.metricsMode.prometheus]] с адресом
    `https://metrics.<кластер>.<домен>/select/0/prometheus` и авторизацией, которую администратор настроил у ingress;
  - `vmauth`: адрес `vmauth` с путём, который его `url_map` направляет в vmselect, и свой токен (JWT или
    `bearer_token` от администратора). Авторизация по имени и паролю не поддерживается.

  Оба по умолчанию выключены: их включает администратор DropApp в values инструмента `victoria-metrics-k8s-stack`. Без
  фильтров такой источник показывает метрики всего кластера; `vmauth` может сузить их полем `vm_access` токена.

Поля своего источника описаны в разделе [Метрики](connections.md#metrics).

## Чего Glafira на DropApp не показывает {#not-supported}

- `TenantCluster` (vcluster): его kubeconfig хранится в Secret, который Glafira не читает. Импортируйте kubeconfig
  клиентского кластера как отдельное подключение.
- Ссылку на консоль DropApp: её адрес не записан ни в одном объекте. Обычно консоль —
  `https://console.<кластер>.<домен>`.
- Loki: логи подов Glafira читает через API-сервер, как на любом кластере. Логи удалённых подов есть только в Loki
  (Grafana).
- Route и DeploymentConfig: у DropApp их нет, используйте Ingress и Deployment. LLM-ассистенту это тоже сообщается.
- Вход по имени и паролю в окне Glafira: пароль уходит только на страницу Dex.

## Документация DropApp {#docs}

Публичная документация DropApp — https://platformv.sbertech.ru/docs/public/K8S/. Glafira следует версии 5.1.2: группы
API, клиент Dex, адрес VictoriaMetrics и namespace платформы взяты из неё.
