# Glafira for Kubernetes — этап O5 «DropApp»: спека

Уточнения поверх основной спеки (`2026-10-24-glafira-kubernetes-design.md`); где эта спека молчит, действует основная.

## 1. Цель и исходные данные

Адаптер платформы `dropapp` для Platform V DropApp (СберТех): признак для `detect`, вход людей, автопоиск метрик,
обзор и правила уровня кластера, проекты. До O5 DropApp работает как `kubernetes`.

Основная спека считала документацию производителя закрытой и ставила условием начала стенд. 2026-11-01 нашлась
публичная документация — https://platformv.sbertech.ru/docs/public/K8S/ (24 версии, от 1.4.0-fstec до 5.1.2;
корень переадресует на последнюю). Эта спека написана по версии 5.1.2 с проверкой признака по 4.2.5 и 3.2.2; стенда
нет, поэтому код проверяется на имитации DropApp в kind (§8), а смоук на настоящем стенде проводит пользователь
позже (`docs/smoke/o5.md`). Ниже ссылки вида `[ug/access]` — страницы
`https://platformv.sbertech.ru/docs/public/K8S/5.1.2/common/documents/<путь>.html`:
`ug` — `user-guide`, `ag` — `administration-guide`, `ig` — `installation-guide-dactl`, `about` — `about`.

Что выяснилось (документация 5.1.2):

- **Свои группы API** (все `v1alpha1`): `config.dropapp.ru` — `Component`, `Tool`, `ClusterMetadata`
  (`spec.clusterName`, `spec.dappVersion`), `DropAppConfiguration`, `DropAppUpgradeConfiguration`
  [ig/config-components, ig/update]; `nodes.dropapp.ru` — `DropAppNode`, `SSHAccess` [ig/installation-dactl];
  `cluster.dropapp.ru` — `TenantCluster` (vcluster) [ig/tenantcluster]; `tls.dropapp.ru` — `IngressTLS`
  [ig/ingress-tls-cert-rotation]; `projects.dropapp.ru` — `Project`, агрегированный API `dapp-apiserver`: проекция
  Namespace, пользователь видит только свои проекты по RBAC [ag/1-dapp-apiserver]. `config.dropapp.ru` и
  `nodes.dropapp.ru` есть и в 3.2.2 и 4.2.5 (кластеры, установленные `dactl`); `projects.dropapp.ru` — только в 5.x.
  Групп OpenShift нет.
- **Namespace платформы** — с префиксом `dapp-`: `dapp-system`, `dapp-auth`, `dapp-monitoring`, `dapp-logging`,
  `dapp-security`, `dapp-ui` и другие [ig/prepare-production]. Kubernetes: 3.x — 1.27.16, 4.x — 1.28.15, 5.x —
  1.29.15 [about/matching].
- **Вход** — Dex: issuer `https://auth.<cluster>.<домен>`, `passwordConnector: ldap` (страница Dex с именем и паролем,
  её видит пользователь консоли), статический клиент `dapp` с секретом, который генерируется при установке, и
  `redirectURIs` `https://console.<домен>/auth/callback`, `http://127.0.0.1:9000/auth/callback`,
  `http://localhost:9000/auth/callback`, `http://localhost:8000`, `http://localhost:18000`,
  `https://loginapp.<домен>/callback` [ig/instruments/dex]. API-сервер: `--oidc-client-id=dapp`,
  `--oidc-username-claim=email`, `--oidc-groups-claim=groups` [ig/tenantcluster-values]. Loginapp выдаёт kubeconfig с
  exec `kubectl oidc-login get-token --oidc-issuer-url=… --oidc-client-id=… --oidc-client-secret=…`
  [ig/instruments/loginapp, ag/1-Kubelogin]. OAuth-сервера OpenShift нет.
- **Метрики** — VictoriaMetrics (кластерный режим) в `dapp-monitoring`; Prometheus-совместимый API —
  `vmselect-victoria-metrics-k8s-stack:8481`, путь `/select/0/prometheus/` [ig/instruments/grafana]. Изоляции по
  namespace нет; ingress `metrics.<cluster>.<домен>` и `vmauth` (JWT) по умолчанию выключены
  [ig/instruments/victoria-metrics-k8s-stack, ag/1-Victoriametrics-authorization].
- **Состояние платформы** — `Component` и `Tool`: колонка STATUS `success` / `failed` / `upgrading`, RETRY; причину
  `failed` смотрят в событиях (`kubectl describe tool <имя>`); `DropAppNode` (STATUS `ready`); `IngressTLS`
  (`status.conditions`). Аналогов ClusterVersion и ClusterOperator нет.
- **Прочее** — Knative нет; service mesh — Cilium Service Mesh и Hubble (своих объектов для «Сети» нет); IngressClass
  `nginx` и haproxy, Gateway API — через Cilium (всё это показывает общий код); SCC-эмуляции и Kubelatte в 5.x нет —
  Gatekeeper и Pod Security Admission (enforce `baseline`); консоль — OpenShift Console 4.12 на
  `https://console.<cluster>.<домен>` [ig/instruments/console]; роли — группы `k8s-admins`, `k8s-developers`,
  `k8s-auditors` [ig/instruments/role-model].

## 2. Вне объёма

Вход по имени и паролю в окне Glafira (password grant: пароль LDAP не проходит через приложение); `TenantCluster`
(vcluster: отдельный kubeconfig в Secret, который Glafira не читает); ссылка на консоль DropApp (адрес не записан ни
в одном объекте); правило для `upgrading` (без метки времени застрявшее обновление не отличить от идущего); DropApp
1.x–2.x без `config.dropapp.ru` — они остаются `kubernetes`; Loki и Vector (логи читаются через `pods/log`, как
везде); `vmauth` как отдельный вид авторизации метрик (JWT вставляется как токен своего URL).

## 3. Определение платформы

- `PlatformKind::Dropapp` (`"dropapp"` в JSON, интерфейсе и MCP).
- `detect`, признаки по порядку: группа `config.openshift.io` → `openshift`; группа `config.dropapp.ru` →
  `dropapp`; иначе `kubernetes`. Discovery читает любой вошедший пользователь, поэтому признак работает и у
  пользователя одного namespace. `distribution` у `dropapp` нет, как у `openshift`.
- Версия DropApp — как строка ClusterVersion у OpenShift: если группа `config.dropapp.ru` есть, снимок discovery
  читает список `clustermetadatas.config.dropapp.ru` (имя объекта документация не называет, область — тоже: список
  по всем namespace подходит к обеим) и берёт `spec.dappVersion` первого объекта. Нет прав, объекта или поля — версии
  нет, это не ошибка подключения.
- Подпись: «DropApp 5.1.2» (версия как есть); без версии — «DropApp (Kubernetes 1.29)».

## 4. Проекты

Правило по группе API, а не по платформе (§4.3 основной спеки): список проектов берётся из
`projects.project.openshift.io/v1`, иначе из `projects.projects.dropapp.ru/v1alpha1`, иначе из `namespaces`. Порядок
действует везде, где сейчас выбирается между проектами OpenShift и namespace: список и число проектов
(`k8s/cluster.rs`), шаг прав проверки подключения (`k8s/connectivity.rs`), выбор namespace для метрик
(`metrics/select.rs`). У Project DropApp имя совпадает с именем namespace.

## 5. Вход через OIDC

### 5.1 Способ `AuthConfig::Oidc`

Новый способ входа — вход через браузер в OIDC-провайдер, к которому подключён API-сервер. Он не привязан к платформе
(так же заработают Keycloak и Dex на любом кластере с `--oidc-*`), но значения по умолчанию — DropApp.

| Поле профиля | По умолчанию | Где хранится |
|---|---|---|
| `issuerUrl` | — (у DropApp `https://auth.<cluster>.<домен>`) | профиль |
| `clientId` | `dapp` | профиль |
| `clientSecret` | — (может быть пустым у публичного клиента) | keychain, `glafira-kubernetes/<id>/oidcClientSecret` |
| `scopes` | `openid email groups profile offline_access` | профиль |
| `issuerCaPem` | — (CA сертификата издателя, PEM) | профиль |

`id_token` и `refresh_token` — в keychain (`…/oidcIdToken`, `…/oidcRefreshToken`). Экспорт профиля пишет поля без
секрета, как у токена.

Токены выданы издателю и клиенту, которому доверяли по CA издателя: смена `issuerUrl` (без завершающего `/`),
`clientId` или `issuerCaPem` (`oidc_identity_changed`) удаляет `id_token` и `refresh_token` при сохранении формы,
а при импорте файла настроек — ещё и секрет клиента; нужно войти заново. Импорт файла настроек считает такую смену
сменой направления, как адрес, прокси и CA: при сохранённых секретах подключения (токен, ключ, секрет клиента,
`id_token` или `refresh_token`) — предупреждение `messages.import.oidcChanged`; секреты удаляются до записи
хранилища, сбой keychain останавливает импорт.

### 5.2 Поток

1. `GET <issuerUrl>/.well-known/openid-configuration` → `authorization_endpoint`, `token_endpoint`; `issuer` ответа
   должен совпасть с `issuerUrl` (без завершающего `/`), иначе `AUTH_FAILED` «адрес издателя не совпадает». Оба
   адреса — только `https`: http-адрес — `AUTH_FAILED` `errors.oidc.insecureEndpoint`. Исключение — локальный
   http-издатель (`127.0.0.1`, `localhost`: тесты и имитация), и то лишь для адресов на `127.0.0.1` и `localhost`.
2. Сервер возврата на фиксированном порте: Dex сверяет `redirect_uri` точно, вместе с портом. Сначала
   `127.0.0.1:8000` с `redirect_uri` `http://localhost:8000`, если порт занят — `127.0.0.1:18000` с
   `http://localhost:18000` (адреса kubelogin, они есть у клиента `dapp`). Сервер слушает и `[::1]` на том же порте,
   если адрес IPv6 доступен: браузер может разрешить `localhost` в него; порт, занятый на `[::1]`, тоже считается
   занятым (иначе ответ мог бы уйти чужой программе). Оба порта заняты — `AUTH_FAILED` с
   подсказкой «освободите порт 8000 или 18000 (часто его держит kubelogin)». Путь возврата — `/`.
3. Authorization Code + PKCE (S256), `state`, `nonce`; браузер — системный; ожидание до 5 минут, отмена —
   `cancel_oauth_login`, как у входа OpenShift.
4. Обмен кода на токены: `client_secret_post` (у DropApp клиент конфиденциальный; пустой секрет — без него).
   Проверяется `nonce` в `id_token`; подпись `id_token` проверяет API-сервер, Glafira читает только `exp`. Ответ
   `token_endpoint` читается не больше 1 МиБ; больше — `AUTH_FAILED` `errors.oidc.badTokenResponse` без тела ответа.
5. В API-сервер уходит `id_token` (`Authorization: Bearer`), как у kubelogin.

Сеть: CA из профиля добавляется к системным корням для запросов к издателю, как для OAuth-сервера OpenShift, а к ним —
`issuerCaPem` (сертификат Dex за ingress DropApp часто выпущен не CA API-сервера); прокси и `allowInsecureTls` — как у
остальных запросов подключения. Переадресации издателя не выполняются: форма с кодом и секретом уходит только на
`token_endpoint`.

### 5.3 Срок и обновление

- Срок — `exp` из `id_token`; он показывается в обзоре подключения, с тем же предупреждением за 1 час и «Срок токена
  истёк», что у OpenShift. Строка «Срок токена» таблицы платформ касается способа `oidc` на любой платформе.
- Токен обновляется по `exp`, отдельной обработки 401 нет: срок `exp` становится сроком учётных данных подключения,
  и за минуту до него подключение открывается заново (как у exec-плагина, §5.2 основной спеки); при открытии
  `id_token`, которому осталось больше минуты, берётся из keychain, иначе токены обновляются
  (`grant_type=refresh_token`) — одной попыткой на всех ожидающих (`ClientPool::connect_lock`). Новый
  `refresh_token` заменяет старый, без него остаётся прежний. Нет `refresh_token` или провайдер отказал в обновлении
  — `SESSION_EXPIRED` «Войти снова» (`errors.connections.oidcLoginRequired`, кнопка «Войти через OIDC»); сеть и TLS
  остаются своими ошибками.
- `id_token` без `exp` считается действующим, срока в обзоре нет.
- Новый вход без `refresh_token` стирает прежний `refresh_token`: он от другого входа.

### 5.4 Импорт kubeconfig

Контекст с exec `kubectl oidc-login get-token` (или `kubelogin get-token`) и аргументами `--oidc-issuer-url`,
`--oidc-client-id`, `--oidc-client-secret`, `--oidc-extra-scope` предлагается импортировать двумя способами: «Вход
через браузер (OIDC)» — по умолчанию, поля берутся из аргументов (`--oidc-extra-scope` добавляется к `openid`, а
`offline_access` — всегда), и
«Exec-плагин» — как сейчас. `apiVersion` exec, отличный от `client.authentication.k8s.io/v1` и `v1beta1` (в
документации DropApp опечатка `client.authentication.io/v1beta1`), при выборе OIDC не мешает: exec не запускается.

Предпросмотр отдаёт вариант `oidc` (издатель, клиент, scopes, CA издателя) с признаком секрета, без самого секрета:
применение читает секрет из файла заново. CA издателя — из `--certificate-authority-data` или файла
`--certificate-authority` (относительный путь — от каталога kubeconfig; `*-data` важнее); файл не прочитан, не
PEM-сертификат, содержит приватный ключ, больше 1 МиБ или не обычный файл — предупреждение
`errors.kubeconfig.issuerCaUnreadable` (ни ключ, ни его текст в предпросмотр не попадают), контекст импортируется
без CA. Без `--oidc-extra-scope` —
scopes DropApp; с ними — `openid`, они и `offline_access` (kubelogin обходится без refresh-токена, Glafira — нет).
Exec-вариант, которому нельзя быть (команда не найдена, неизвестный `apiVersion`, запрет политикой), отдаётся
недоступным с причиной; OIDC остаётся. Проверка подключения в форме для OIDC берёт только сохранённый `id_token`,
которому осталось больше 60 с, без обновления (хранилище проверки — только чтение); его нет — «Войти через OIDC».
Токен другой идентичности проверка не берёт: если в несохранённой форме издатель, client id или CA издателя не
такие, как в сохранённом профиле, шаг входа сразу требует «Войти через OIDC», keychain за токенами не читается.

### 5.5 Ошибки

Издатель недоступен — сетевая ошибка (`CONNECTION_FAILED`, `TIMEOUT`) с адресом в деталях; недоверенный сертификат
издателя — `TLS_ERROR` `errors.oidc.issuerCaUntrusted` «добавьте CA издателя (PEM)» (общий `errors.k8s.caUntrusted`
советовал бы CA кластера). Отказ в redirect (`error=access_denied` и другие `error` возврата) — прежняя ошибка входа
через браузер `errors.oauth.denied`; отказ token endpoint (обмен кода или обновление) — `AUTH_FAILED`
`errors.oidc.providerError` с `error` и `error_description` провайдера; ответ больше 1 МиБ или не JSON —
`errors.oidc.badTokenResponse`. http-адрес авторизации или токенов у не локального издателя —
`errors.oidc.insecureEndpoint` (§5.2). Отдельного текста для 401 API-сервера нет — общий `errors.k8s.unauthorized`.
Секрет и токены в ошибки и журнал не попадают.

## 6. Метрики

- `metrics::discovery::candidates` принимает платформу. У `dropapp` первым идёт кандидат
  `dapp-monitoring/vmselect-victoria-metrics-k8s-stack:8481` с префиксом пути `/select/0/prometheus`, за ним —
  кандидаты `kubernetes`. Подпись источника — `dapp-monitoring/vmselect-victoria-metrics-k8s-stack:8481`.
- `ReadClient` пропускает через прокси GET к `<префикс>/api/v1/query` и `<префикс>/api/v1/query_range`, где префикс
  — пустой или `/select/0/prometheus`; префикс проверяется так же, как пути API (`..`, `?`, `#` и путь без ведущего
  `/` не проходят).
- 403 на прокси — общий текст прокси `messages.metrics.proxyForbidden`: он уже называет источник
  (`dapp-monitoring/vmselect-victoria-metrics-k8s-stack:8481`) и право `get services/proxy`, отдельного текста для
  VictoriaMetrics нет. У пользователя namespace права обычно нет, и выбор идёт дальше (`metrics.k8s.io` или свой URL
  профиля). Руководство объясняет, какой адрес вписать в профиль, если администратор включил ingress
  `metrics.<cluster>.<домен>` или `vmauth`.

## 7. Обзор, правила, MCP

### 7.1 Обзор кластера

`PlatformOverview::Dropapp(DropAppOverview)`; части — `Part<…>`: нет CRD (404) — пустая часть, нет прав —
`not_allowed`.

- `metadata` — `clusterName` и `dappVersion` из `ClusterMetadata`;
- `components` — `Component` и `Tool`: всего и проблемные (`failed`, `error`, `upgrading`), по каждому — вид, имя,
  namespace (если есть), статус, RETRY и последнее событие `Warning` по объекту (`involvedObject.uid`); список
  проблемных полный, событие читается только у первых 20 (`MAX_EVENT_LOOKUPS`), ошибка чтения событий — события нет;
  читаются по всем namespace (область CRD документация не называет);
- `nodes` — `DropAppNode` со статусом не `ready` и всего; без статуса — не проблема;
- `ingressTls` — `IngressTLS` с условием `Ready` ≠ `True` и всего; без условия `Ready` (объект ещё не обработан) — не
  проблема.

Поля статуса берутся из `status.phase`, а если его нет — из `status.status` (имя поля за колонкой STATUS документация
не показывает); значения сравниваются без учёта регистра (в метриках `Ready`/`Error`, в колонке `success`/`failed`).
Неизвестное значение — показывается как есть и проблемой не считается.

### 7.2 Правила уровня кластера

| `ruleId` | Объект | Условие | prod | stage | прочие |
|---|---|---|---|---|---|
| `cluster.componentFailed` | cluster | `Component` или `Tool` в статусе `failed` (или `error`); список (до 20) с последним событием `Warning` | К | К | П |
| `cluster.dropappNodeNotReady` | cluster | `DropAppNode` не `ready`; список | П | П | И |
| `cluster.ingressTlsNotReady` | cluster | у `IngressTLS` `Ready` ≠ `True`; список | П | П | И |

Только у платформы `dropapp`; без прав — пометка в `check.partial`. `cluster.componentFailed` не срабатывает на
`upgrading` (проблема обзора, но не сбой, §2); список находки — до 20, событие — у тех, у кого его прочитал сбор;
текст события в params — не длиннее 1024 символов (обрезается по символу, с «…»): его пишет кто угодно с доступом к
кластеру. Тексты находок на en и ru подсказывают, где причина: «события объекта, `kubectl describe tool NAME (or
component NAME)`» (ru — `ИМЯ`).

### 7.3 MCP

`PlatformKind` крейтов `glafira-mcp-*` получает `Dropapp`; `list_connections` и `get_cluster_overview` отдают
`"dropapp"` и подпись, `get_cluster_overview` — часть обзора §7.1 (`metadata`, проблемные компоненты, узлы,
`IngressTLS`); новые правила приходят через `health_check`. В инструкции сервера — фраза: «On DropApp, Routes and
DeploymentConfigs are not available: use Ingress and Deployment». Фрагменты компактного набора (O4b) не меняются.

## 8. Имитация DropApp в kind

`dev/kind/dropapp/` (`README.md`): скрипт `up.sh` / `down.sh`, кластер `glafira-dropapp`, KUBECONFIG — в
`dev/kind/dropapp/out/` (вне git), не в `~/.kube/config`; `out/kubeconfig` — контексты `admin` (сертификат kind),
`oidc-admin` и `oidc-dev` (`kubectl oidc-login get-token` с издателем, `--oidc-client-id=dapp`, секретом,
`--oidc-extra-scope=email`, `--oidc-extra-scope=groups` и `--certificate-authority=<абсолютный путь>/out/dex-ca.crt`,
как kubeconfig от Loginapp).

- **Dex** (`ghcr.io/dexidp/dex:v2.45.1`) в `dapp-auth` по HTTPS с самоподписанным CA (CA, сертификат и ключ пишет
  `up.sh`); издатель `https://dex.glafira-dropapp.localhost:5556`: под с `hostNetwork` слушает порт 5556 ноды,
  `kind.yaml` пробрасывает его на `127.0.0.1:5556` машины разработчика, где `*.localhost` разрешается в `127.0.0.1`
  сам (macOS; Linux — nss-myhostname или systemd-resolved); API-серверу имя даёт `hostAliases` из патча kubeadm
  `patches/kube-apiserver+strategic.yaml`, `/etc/hosts` не меняются. Клиент `dapp` с секретом
  `glafira-dropapp-secret` и `redirectURIs` DropApp, экран согласия пропускается; `enablePasswordDB` с пользователями
  `admin@dropapp.test` (`cluster-admin`) и `dev@dropapp.test` (`admin` в `team-a`), RBAC — по `email`. API-сервер —
  `--oidc-issuer-url`, `--oidc-client-id=dapp`, `--oidc-username-claim=email`, `--oidc-groups-claim=groups`,
  `--oidc-ca-file` (CA через `extraMounts`), `--event-ttl=720h` (событие `Warning` стенда не пропадает через час).
- **CRD** групп `config.dropapp.ru`, `nodes.dropapp.ru`, `tls.dropapp.ru` и `projects.dropapp.ru` с именами из
  констант `k8s/dropapp.rs` (`ClusterMetadata`, `Component`, `Tool` — уровня кластера, `IngressTLS` — в namespace;
  область выбрана стендом); CRD вместо агрегированного API: `dev@dropapp.test` получает 403 на список проектов.
  Объекты: `ClusterMetadata` (`glafira-dropapp`, `5.1.2`), `Component` `auth`, `cni`, `monitoring` (`success`) и
  `ingress-controller` (`upgrading`), `Tool` `dex`, `grafana` (`success`) и `console` (`failed`, `retry: 3`, событие
  `Warning` `InstallFailed`), `DropAppNode` `glafira-dropapp-control-plane` (`ready`) и `worker-2` (`provisioning`),
  `IngressTLS` `default-cert` и `apps-cert` (`Ready=False`) в `dapp-ingress`, `Project` и namespace `team-a` с
  Deployment `web`. Статусы — те поля, что разбирает приложение (`status.phase`, `status.retry`, `status.status`,
  условие `Ready`).
- **VictoriaMetrics** (`v1.148.0`) в `dapp-monitoring`: `vmstorage`, `vminsert`, `vmselect` за Service
  `vmselect-victoria-metrics-k8s-stack:8481` (`/select/0/prometheus`) и `vmagent`, собирающий kubelet и cAdvisor
  через прокси API-сервера; kube-state-metrics нет.
- Образы скрипт не скачивает: они должны быть у Docker заранее (`kind load docker-image`, а для многоплатформенных —
  `docker save --platform` и `ctr images import`).
- Интеграционный тест `tests/it_dropapp.rs` запускается вручную с `GO_IT_DROPAPP_KUBECONFIG`; CI его не запускает
  (второй кластер с OIDC в CI не поднимается), без переменной тест пропускается. Вход через браузер он не проверяет —
  это смоук (`docs/smoke/o5.md`, часть A).

## 9. Интерфейс и документация

- Обзор кластера — карточка «DropApp» (версия, компоненты, узлы, `IngressTLS`) по образцу карточки OpenShift.
- Форма подключения — способ «Вход через OIDC (Dex)»: `issuerUrl`, `clientId`, секрет (скрыт до «Показать»),
  `scopes`; импорт kubeconfig — выбор из §5.4; обзор подключения — срок токена.
- Строки интерфейса — en и ru.
- Руководство (en, ru): раздел о DropApp — как Glafira его узнаёт и какие версии (3.x и новее, установленные
  `dactl`), вход через Dex с kubeconfig от Loginapp, проекты, доступ к метрикам администратора и пользователя
  namespace, чего Glafira на DropApp не показывает; способ «Вход через OIDC» в главе «Подключения».
- Основная спека: §4.3 (таблица платформ и раздел DropApp), §5.2, §6.2, §7.1, §8.2, строка O5 в §19.
- `docs/smoke/o5.md` — смоук на стенде DropApp с пунктами, которые имитация не покрывает: LDAP-коннектор Dex,
  фильтрация `projects.dropapp.ru` по RBAC, настоящие поля статуса и область `Component`/`Tool`, имя и область
  `ClusterMetadata`, метрики через ingress или `vmauth`.

## 10. Тесты

- Модульные: `detect` (таблица: OpenShift, DropApp с версией и без, обычный Kubernetes); список проектов по трём
  группам; кандидаты метрик и префикс прокси; обзор с подставными ответами (CRD нет, прав нет, проблемы есть,
  `status.phase` и `status.status`); три правила; вход OIDC с подставным издателем (метаданные, порт 8000 занят →
  18000, оба заняты, `state`, `nonce`, ошибки Dex, обновление по `refresh_token`); импорт kubeconfig с `oidc-login`.
- Интеграционный на имитации kind (§8), рядом с существующими kind-тестами: `detect` → `dropapp` с версией, обзор и
  правила, проекты, метрики через vmselect у администратора и отказ 403 у пользователя namespace.
- Вход через браузер в окне — смоук на имитации.
