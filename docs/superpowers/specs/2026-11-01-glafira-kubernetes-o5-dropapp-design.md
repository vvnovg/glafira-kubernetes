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

`id_token` и `refresh_token` — в keychain (`…/oidcIdToken`, `…/oidcRefreshToken`). Экспорт профиля пишет поля без
секрета, как у токена.

### 5.2 Поток

1. `GET <issuerUrl>/.well-known/openid-configuration` → `authorization_endpoint`, `token_endpoint`; `issuer` ответа
   должен совпасть с `issuerUrl` (без завершающего `/`), иначе `AUTH_FAILED` «адрес издателя не совпадает».
2. Сервер возврата на фиксированном порте: Dex сверяет `redirect_uri` точно, вместе с портом. Сначала
   `127.0.0.1:8000` с `redirect_uri` `http://localhost:8000`, если порт занят — `127.0.0.1:18000` с
   `http://localhost:18000` (адреса kubelogin, они есть у клиента `dapp`). Сервер слушает и `[::1]` на том же порте,
   если адрес IPv6 доступен: браузер может разрешить `localhost` в него. Оба порта заняты — `AUTH_FAILED` с
   подсказкой «освободите порт 8000 или 18000 (часто его держит kubelogin)». Путь возврата — `/`.
3. Authorization Code + PKCE (S256), `state`, `nonce`; браузер — системный; ожидание до 5 минут, отмена —
   `cancel_oauth_login`, как у входа OpenShift.
4. Обмен кода на токены: `client_secret_post` (у DropApp клиент конфиденциальный; пустой секрет — без него).
   Проверяется `nonce` в `id_token`; подпись `id_token` проверяет API-сервер, Glafira читает только `exp`.
5. В API-сервер уходит `id_token` (`Authorization: Bearer`), как у kubelogin.

Сеть: CA из профиля добавляется к системным корням для запросов к издателю, как для OAuth-сервера OpenShift; прокси
и `allowInsecureTls` — как у остальных запросов подключения.

### 5.3 Срок и обновление

- Срок — `exp` из `id_token`; он показывается в обзоре подключения, с тем же предупреждением за 1 час и «Срок токена
  истёк», что у OpenShift. Строка «Срок токена» таблицы платформ касается способа `oidc` на любой платформе.
- За минуту до `exp` или по ответу 401 Glafira один раз обновляет токены (`grant_type=refresh_token`) — одной
  попыткой на всех ожидающих, как выдача учётных данных exec-плагина (§5.2 основной спеки); новый `refresh_token`
  заменяет старый. Нет `refresh_token` или отказ — «Войти снова» (вход через браузер заново).

### 5.4 Импорт kubeconfig

Контекст с exec `kubectl oidc-login get-token` (или `kubelogin get-token`) и аргументами `--oidc-issuer-url`,
`--oidc-client-id`, `--oidc-client-secret`, `--oidc-extra-scope` предлагается импортировать двумя способами: «Вход
через браузер (OIDC)» — по умолчанию, поля берутся из аргументов (`--oidc-extra-scope` добавляется к `openid`), и
«Exec-плагин» — как сейчас. `apiVersion` exec, отличный от `client.authentication.k8s.io/v1` и `v1beta1` (в
документации DropApp опечатка `client.authentication.io/v1beta1`), при выборе OIDC не мешает: exec не запускается.

### 5.5 Ошибки

Издатель недоступен или TLS — `CONNECTION_FAILED` с адресом издателя; Dex отклонил redirect, клиента или секрет
(`error` в возврате или ответе токена) — `AUTH_FAILED` с текстом Dex; API-сервер ответил 401 на свежий токен —
`AUTH_FAILED` «API-сервер не принял токен: проверьте client id и scope `groups`». Секрет и токены в ошибки и журнал
не попадают.

## 6. Метрики

- `metrics::discovery::candidates` принимает платформу. У `dropapp` первым идёт кандидат
  `dapp-monitoring/vmselect-victoria-metrics-k8s-stack:8481` с префиксом пути `/select/0/prometheus`, за ним —
  кандидаты `kubernetes`. Подпись источника — `dapp-monitoring/vmselect-victoria-metrics-k8s-stack:8481`.
- `ReadClient` пропускает через прокси GET к `<префикс>/api/v1/query` и `<префикс>/api/v1/query_range`, где префикс
  — пустой или `/select/0/prometheus`.
- 403 на прокси — «нет прав на прокси к VictoriaMetrics (`get services/proxy` в `dapp-monitoring`)»: у пользователя
  namespace их обычно нет, и выбор идёт дальше (`metrics.k8s.io` или свой URL профиля). Руководство объясняет, какой
  адрес вписать в профиль, если администратор включил ingress `metrics.<cluster>.<домен>` или `vmauth`.

## 7. Обзор, правила, MCP

### 7.1 Обзор кластера

`PlatformOverview::Dropapp(DropAppOverview)`; части — `Part<…>`: нет CRD (404) — пустая часть, нет прав —
`not_allowed`.

- `metadata` — `clusterName` и `dappVersion` из `ClusterMetadata`;
- `components` — `Component` и `Tool`: всего и проблемные (`failed`, `upgrading`), по каждому — вид, имя, namespace
  (если есть), статус, RETRY и последнее событие `Warning` по объекту (`involvedObject.uid`); читаются по всем
  namespace (область CRD документация не называет);
- `nodes` — `DropAppNode` со статусом не `ready` и всего;
- `ingressTls` — `IngressTLS` с условием `Ready` ≠ `True` и всего.

Поля статуса берутся из `status.phase`, а если его нет — из `status.status` (имя поля за колонкой STATUS документация
не показывает); значения сравниваются без учёта регистра (в метриках `Ready`/`Error`, в колонке `success`/`failed`).
Неизвестное значение — показывается как есть и проблемой не считается.

### 7.2 Правила уровня кластера

| `ruleId` | Объект | Условие | prod | stage | прочие |
|---|---|---|---|---|---|
| `cluster.componentFailed` | cluster | `Component` или `Tool` в статусе `failed` (или `error`); список (до 20) с последним событием `Warning` | К | К | П |
| `cluster.dropappNodeNotReady` | cluster | `DropAppNode` не `ready`; список | П | П | И |
| `cluster.ingressTlsNotReady` | cluster | у `IngressTLS` `Ready` ≠ `True`; список | П | П | И |

Только у платформы `dropapp`; без прав — пометка в `check.partial`. Тексты находок на en и ru подсказывают, где
причина: «события объекта, `kubectl describe tool <имя>`».

### 7.3 MCP

`PlatformKind` крейтов `glafira-mcp-*` получает `Dropapp`; `list_connections` и `get_cluster_overview` отдают
`"dropapp"` и подпись, `get_cluster_overview` — часть обзора §7.1 (`metadata`, проблемные компоненты, узлы,
`IngressTLS`); новые правила приходят через `health_check`. В инструкции сервера — фраза: «On DropApp, Routes and
DeploymentConfigs are not available: use Ingress and Deployment». Фрагменты компактного набора (O4b) не меняются.

## 8. Имитация DropApp в kind

`dev/kind/dropapp/`: скрипт `up.sh` / `down.sh`, кластер `glafira-dropapp`, KUBECONFIG — в каталоге вывода, не в
`~/.kube/config`.

- **Dex** в namespace `dapp-auth` по HTTPS с самоподписанным CA; клиент `dapp` с секретом и `redirectURIs` из §1;
  `enablePasswordDB` со статическими пользователями: администратор и пользователь одного namespace (группы Dex со
  статическими паролями не даёт — RBAC привязывается к `email`). API-сервер kind — с `--oidc-issuer-url`,
  `--oidc-client-id=dapp`, `--oidc-username-claim=email`, `--oidc-groups-claim=groups`, `--oidc-ca-file` (CA через
  `extraMounts`); имя издателя разрешается и с узла kind, и с машины разработчика.
- **CRD** групп `config.dropapp.ru`, `nodes.dropapp.ru`, `tls.dropapp.ru` и `projects.dropapp.ru` (CRD вместо
  агрегированного API: фильтрацию по RBAC имитация не проверяет); объекты `ClusterMetadata` (`dappVersion: 5.1.2`),
  `Component` и `Tool` в разных статусах (один `Tool` в `failed` с событием `Warning`), `DropAppNode` (один не
  `ready`), `IngressTLS` (один не `Ready`), `Project` по namespace.
- **VictoriaMetrics** в `dapp-monitoring`: Service `vmselect-victoria-metrics-k8s-stack:8481`, отвечающий на
  `/select/0/prometheus/api/v1/query*`, и сбор метрик kubelet и cAdvisor — чтобы «Ресурсы» строили графики.
- Образы скачиваются только с согласия пользователя.

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
