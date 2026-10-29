# Glafira for Kubernetes

Desktop-приложение (Tauri 2 + Rust + React) для работы с кластерами Kubernetes и их диагностики; в фокусе —
OpenShift и Platform V DropApp.

Kubernetes is a registered trademark of The Linux Foundation. OpenShift is a trademark of Red Hat, Inc. Platform V
and DropApp are trademarks of their respective owners. This project is not affiliated with The Linux Foundation,
Red Hat or SberTech.

## Установка

Установщики для macOS (`.dmg`, `.pkg`), Windows (`.msi`) и Linux (`.deb`, `.rpm`, `.AppImage`):
https://github.com/vvnovg/glafira-kubernetes-releases/releases

Сборки для macOS подписаны и нотаризованы. Сборки для Windows пока не подписаны: если SmartScreen блокирует
установщик, выберите «Подробнее», затем «Выполнить в любом случае». Новые версии приложение находит само:
«О программе» → «Обновления».

Спецификация: `docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md`. Код, общий с Glafira for Apache Kafka,
перечислен в `PORTED.md`.

## Требования
- Rust stable (`rustup`), Xcode Command Line Tools (macOS)
- Node 22+, pnpm 11
- Для интеграционных тестов — Docker и kind (`dev/kind/README.md`)

## Запуск
```bash
pnpm install
pnpm tauri dev
```

## Тесты
```bash
pnpm test                                                            # фронтенд
pnpm test:scripts                                                    # скрипты scripts/*.mjs
pnpm i18n:check                                                      # словари, ключи Rust, руководство
cargo test --manifest-path src-tauri/Cargo.toml --lib                # Rust unit
cargo test --manifest-path crates/glafira-mcp-core/Cargo.toml        # MCP: транспорт, сессии, лимиты, журнал
cargo test --manifest-path crates/glafira-mcp-kubernetes/Cargo.toml  # MCP: инструменты, область LLM, маскировка
GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --tests  # интеграционные (kind)
```
Без `GO_IT_KUBECONFIG` интеграционные тесты пропускаются.

## Подключения
- Адрес API-сервера: `https://api.<кластер>:6443`; допускается путь (прокси Rancher `…/k8s/clusters/<id>`).
- Способы входа: через браузер (OAuth OpenShift, как `oc login --web`; только OpenShift), токен (`oc whoami -t` или
  токен сервисной учётной записи), клиентский сертификат PEM, exec-плагин из списка разрешённых (`aws`,
  `aws-iam-authenticator`, `gke-gcloud-auth-plugin`, `kubelogin`, `kubectl oidc-login`, `oc`, `yc`, `rancher` —
  `src-tauri/src/auth/exec.rs`). Если на кластере нет OAuth-клиента `openshift-cli-client`, вход через браузер
  недоступен: на вкладке подключения есть кнопка «Получить токен на странице кластера», она открывает страницу токена
  кластера (`/oauth/token/request`).
- Список разрешённых exec-плагинов сверяет имя команды и первые аргументы (`aws eks get-token`). Перед ними допустимы
  только документированные глобальные ключи из таблицы команды (`ALLOWED`): ключ со значением
  (`aws --region eu-west-1 eks get-token`, `--region=eu-west-1`) или флаг без `=` (`aws --no-verify-ssl eks get-token`).
  Любой другой ведущий ключ — отказ с его именем (`errors.exec.unknownOption`), у `kubectl` ведущих ключей нет вовсе
  (`optionBeforePlugin`): плагин kubectl запускает, только если перед его именем нет ключей. Переменные окружения,
  которые подменяют код команды, запрещены (`is_forbidden_env`: `PATH`, `LD_*`, `DYLD_*`, `PYTHON*`, `NODE_OPTIONS` и
  другие) — при сохранении, импорте, в предпросмотре kubeconfig и перед запуском. Переменные, которые направляют
  утилиту на другой конфиг (`AWS_CONFIG_FILE`, `KUBECONFIG`), разрешены: они видны в подтверждаемой строке.
- Exec-плагин запускает Glafira (не `kube`): по абсолютному пути, без оболочки, не дольше 5 минут, с отменой. `PATH`
  плагину всегда задаёт Glafira (каталог команды, PATH оболочки входа и стандартные каталоги), свой `PATH` в профиле
  запрещён. Строку запуска подтверждает человек; подтверждение привязано к точной строке (`launchKey` в
  `src/components/connections/exec.ts`): любое изменение команды, аргументов или переменных требует нового
  подтверждения (сохранение без галочки не переносит прежнее подтверждение на изменённую строку); в форме смена
  способа входа снимает поставленную галочку, а сохранённая подтверждённая строка без правок остаётся подтверждённой.
- Вкладка подключения запускает не больше одного плагина за раз. Пока идёт запуск плагина, вход через браузер или
  сохранение токена, «Подключиться заново» недоступна. У exec-подключения «Запустить exec-плагин снова» на время
  запуска заменена ожиданием, «Отменить» прекращает его. У подключений с токеном и входом через браузер «Сохранить
  токен» и «Войти через браузер» недоступны, пока идёт подключение или другой вход (свой вход заменён ожиданием).
- Импорт из kubeconfig (`KUBECONFIG` или `~/.kube/config`): токены и ключи — в системное хранилище секретов, CA, проект
  контекста и строка запуска exec-плагина — в профиль. Контексты с `auth-provider`, с exec-плагинами вне списка и с
  запрещёнными переменными окружения не импортируются.
- Exec-плагин при импорте: диалог показывает строку запуска, секретные значения в ней скрыты (`••••`), но импорт
  копирует настоящие. Галочка «Разрешить Glafira запускать эту команду» подтверждает строку ровно в показанном виде:
  на импорт уходит отпечаток этой строки (`launch_fingerprint`, `src-tauri/src/k8s/kubeconfig.rs`). Если файл
  kubeconfig изменился между предпросмотром и импортом, плагин импортируется неподтверждённым, и форма подключения
  спросит подтверждение позже.
- CA: системное хранилище ОС или только перечисленные сертификаты PEM; режим без проверки сертификата можно
  запретить политикой `allowInsecureTls`, exec-плагины — политикой `allowExecAuth`.
- Отклоняются при сохранении: PEM с блоком приватного ключа в поле сертификата или CA (ключ — в своё поле) и
  `user:pass@` в адресе API-сервера или прокси.
- Платформа (`src-tauri/src/platform/`): `openshift` по группе `config.openshift.io`, иначе `kubernetes`
  с дистрибутивом (EKS, GKE, AKS, Rancher, Deckhouse, k3s, RKE2); объекты показываются по группам API.
- Проверка подключения по шагам: конфигурация → DNS/TCP → [exec-плагин] → TLS → вход → платформа → проекты. Любой
  ответ сервера на «кто я» (401, 403…) — это TLS ✅ и ❌ на шаге «Вход»; шаг «TLS» падает, в частности, от сетевой
  ошибки, ошибки TLS или таймаута (`src-tauri/src/k8s/connectivity.rs`).

## Чтение кластера
- Всё чтение — в Rust через `ReadClient` (`src-tauri/src/k8s/read.rs`): только get, list, watch, логи и проверки
  прав; списки — страницами по 500, не больше 10 000 объектов (`truncated`). Имя объекта и namespace проверяются до
  запроса (`check_name`, `check_namespace`; namespace — DNS-1123 label, в имени нет `/`, `\`, `%`, `?`, `#`, `;`,
  `..` и не-ASCII): ошибка `INVALID_INPUT`, в кластер ничего не уходит. Каждая команда работает с правами
  пользователя; часть экрана без прав приходит причиной (`Part` в `src-tauri/src/k8s/view.rs`), остальное
  показывается.
- Обзор кластера (`k8s/overview.rs`): `/readyz?verbose`, ноды по ролям, классы Ingress и Gateway, число проектов; у
  OpenShift — адаптер платформы (`platform/adapters.rs`): ClusterVersion (версия — последняя завершённая по истории
  обновлений), Infrastructure и консоль (частями с причиной), ClusterOperators, MachineConfigPools и срок токена
  (`useroauthaccesstokens`). Интерфейс открывает адрес консоли, только если это http или https.
- Проекты, права в проекте (`SelfSubjectRulesReview`, кэш на 5 минут, сброс при 403, переподключении и входе;
  `get_namespace_access(…, force)` обходит кэш — вкладка проекта перечитывает права раз в 5 минут, таймер — срок
  кэша плюс 5 с (`ACCESS_REFRESH_MS` в `src/components/project/useNamespaceAccess.ts`), и по «Обновить»),
  workloads, поды, события, сеть, конфигурация (значения Secret не читаются интерфейсом), PVC, квоты — модули
  `src-tauri/src/k8s/`.
- YAML (`k8s/yaml.rs`, `redact`): объект как `kubectl get -o yaml`, но без `managedFields`, без аннотаций с копией
  объекта (`kubectl.kubernetes.io/last-applied-configuration`, `kapp.k14s.io/original`,
  `objectset.rio.cattle.io/applied`, `openshift.io/encoded-deployment-config`) и без `spec.tls.key` у Route — ключ не
  уходит из Rust. Литеральные значения переменных окружения с секретными именами (`is_secret_name` и `PRIVATE`,
  `AUTHORIZATION`) — `***`, как и `kubernetes.io/change-cause` с таким присваиванием; `valueFrom`, `command`/`args` и
  прочие значения остаются.
- Списки подов, workloads и событий проекта обновляются через watch (`src-tauri/src/watch/`), без прав на watch — опрос
  раз в 10 с. Сессия держит не больше 10 000 строк на все виды (`truncated` у `Reset`), страница начального списка
  ждётся не дольше таймаута запроса, ошибка одного вида workloads не скрывает остальные, ошибка учётных данных или TLS
  завершает сессию (`fatal`). Скрытая вкладка рабочей области не обращается к кластеру (`src/lib/tabActive.ts`): watch
  останавливается и при показе начинается заново, автообновление встаёт на паузу; лог со слежением идёт дальше.
  «Первые 10 000» видны на экранах подов, workloads, событий, PVC, нод, обзора проекта и в дереве проектов; сеть,
  конфигурация, квоты и обзор кластера `truncated` из Rust пока не передают.
- Логи (`src-tauri/src/logs/`): фильтр в Rust, обрезка строк длиннее 64 КБ, байты не в UTF-8 — U+FFFD, счётчики
  уходят и без новых строк; в follow — переподключение до 60 с, а 401, TLS и 403 завершают лог сразу, удалённый под или
  остановленный контейнер — конец лога; `previous` идёт без follow. `save_logs` пишет атомарно (временный файл 0600,
  fsync, rename; `AtomicFile` в `storage/fs.rs`), с таймаутом на строку и останавливается при отключении.
- Сессии останавливаются `stop_session`, при отключении и удалении подключения, при сохранении профиля, новом токене,
  обновлении подключения импортом и перезагрузке окна (`Sessions::stop_all`); интерфейс после этого начинает их
  заново.
- Одновременные чтения одного подключения подключаются один раз: кто ждал очереди, пока чужая попытка не удалась,
  получает ту же ошибку (`AppState::connected`, в том числе через команду `connect`), — exec-плагин не запускается по
  разу на каждую подвкладку. Клиент выбывает из пула, только если упал именно он (`evict_if_same`).

## Проверка кластера и MCP-шлюз
- Проверка (`src-tauri/src/health/`): снимок кластера (`collect.rs`) — `/readyz`, ноды, у OpenShift ClusterOperators,
  ClusterVersion и MachineConfigPools, до 50 проектов и 5000 подов, не больше 4 запросов одновременно (считая `/readyz`)
  и не дольше 45 с; непрочитанное — в `coverage.gaps` и находке `check.partial` (в её `params` список разрывов целиком, в текстах и ответе модели — первые 20: шлюз сначала сужает полный список по области LLM). Ответ 404 на вид объектов значит, что
  вида в кластере нет, и разрывом не считается; у `/readyz` и списка проектов 404 — разрыв. Проект, чей список подов не
  поместился в 5000, и следующие за ним проекты с подами остаются без списка подов: в `check.partial` — только
  `podLimit`, а правила, которым нужны поды, такие проекты пропускают. Правила (`rules/`) — чистые функции над снимком,
  37 правил со строгостью по окружению из таблицы §8.2 спеки (`RULES`, тест `severity_matrix_matches_spec`). Одна
  проверка на подключение: запрос с тем же списком проектов и в том же окружении, пришедший во время проверки, ждёт её,
  иной дожидается её окончания и запускает свою; свежую проверку повторно используют, только если она сделана в текущем
  окружении и охватывает запрошенные проекты (`service.rs`). Результат, скрытые находки, замечания и предложения LLM —
  `health/<id подключения>.json`; изменение шлёт событие `health://changed`. Тексты находок для интерфейса —
  `health.rules.<ruleId>.*` в словарях, для LLM — `health/texts.rs` (по-английски).
- MCP (`crates/glafira-mcp-core`, `crates/glafira-mcp-kubernetes`, `src-tauri/src/mcp/`): шим `glafira-kubernetes mcp`
  соединяется с запущенным приложением через локальный сокет или named pipe по `mcp.json` (адрес и токен, 0600, есть,
  пока сервер работает). Крейты не зависят от kube, k8s-openapi, reqwest, hyper, tauri и самого приложения (тест
  `manifest` в каждом крейте): данные дают `Backend` приложения (`mcp/backend.rs`), чтения идут через `ReadClient` и
  общий пул клиентов, подключение открывается по запросу. Область LLM (`llmAccess`, `llmNamespaces` профиля) проверяется
  в крейте до каждого чтения и ещё раз в бэкенде; чужой, выключенный и несуществующий объект дают одинаковый
  `not_allowed`. Сторожок `mcp_never_reaches_writes_or_the_raw_client` (`mcp/guard.rs`) разбирает `use`-деревья и пути
  всех файлов `mcp/` (псевдонимы, `self`, вложенные группы, без комментариев и строк) и не даёт коду дойти до записи в
  кластер (`ops`, `commands::changes`, `ClusterWriter`), до `kube::Client` и `kube::Api` и до HTTP-клиентов; из `kube`
  разрешены только `ListParams` и `Status`. Тексты ошибок доходят до модели только из списка ключей `LLM_TEXT_KEYS`
  (`mcp/map.rs`), остальные — общей фразой.
- Маскировка (`glafira-mcp-kubernetes/src/mask.rs`): `command`/`args` и аннотации — по именам полей и встроенным
  шаблонам (§10.3 спеки). К тексту из кластера применяются только встроенные шаблоны, без пар `имя=значение`: в прозе
  они портят диагностику. Это сообщения событий и условий, сообщения состояний контейнеров, образы и строки проб,
  сообщения Route, HTTPRoute, GRPCRoute, ClusterOperator и ClusterVersion, `warningEvent` в `observe_workload` и
  `title`, `details`, `params` находок `health_check`. Счётчик `masked` считает замены только в `describe_workload` и
  `describe_pod`, в остальных ответах маскировка молчит.
- Ошибки для модели (`mcp/map.rs`, `mcp/backend.rs`): код остаётся прежним, а тексты `errors.exec.*`, `errors.oauth.*`,
  `errors.connections.*` и `errors.k8s.credentials|transport|forbidden` заменены фиксированными английскими фразами,
  подробности не уходят; остальные ошибки и фиксированные фразы без данных профиля (например,
  `errors.connections.loginRequired`) остаются как есть. Сбой файла проверки и замечаний и паника фоновой задачи —
  `internal` с общей фразой.
- Интеграционные тесты на kind: `tests/it_health.rs` (правила на настоящем API-сервере, частичная проверка без прав) и
  `tests/it_mcp.rs` (клиент rmcp → `mcp.json` → рукопожатие → инструменты в области LLM, ресурсы и предложения;
  значения Secret в ответы не попадают).

## Ресурсы и изменения
- Источник метрик (`src-tauri/src/metrics/`): трейт `MetricsSource` (только GET `/api/v1/query` и `/api/v1/query_range`);
  автопоиск — Thanos Querier у OpenShift, Prometheus через прокси API-сервера (`ReadClient::prometheus`, список
  сервисов — `discovery.rs`), свой URL (`http.rs`), без истории — `metrics.k8s.io`. Выбор живёт в подключении 5 минут;
  403 проб не сбрасывают кэш прав проекта. PromQL — чистые функции `promql.rs` с экранированием меток.
- Статистика и рекомендация (`src-tauri/src/resources/`): худший под за окно 1, 7, 14 или 30 дней, движок
  `recommend.rs` с ограничениями `LimitRange`, квоты (реплики и `maxSurge`), крупнейшей ноды и сдвигом цели HPA,
  уверенность; ресурсы проекта — фоновая сессия с дедлайном 45 с и кэшем 5 минут (`project.rs`).
- Изменения (`src-tauri/src/change/`, `src-tauri/src/ops/`): запросы записи в кластер есть только в `ops`. Предпросмотр
  читает объект, строит «было → станет» и выполняет server-side dry-run (у DELETE `dryRun` — в теле `DeleteOptions`:
  строку запроса API-сервер у DELETE с телом не читает). Версия предпросмотра — `metadata.generation` (у пода — `uid`):
  идущая выкладка её не меняет. Применение после стражей (`guard.rs`) перечитывает объект, сверяет версию
  (`CONFLICT`), пишет с `fieldManager=glafira-kubernetes` и повторяет до 3 раз при ответе 409 из-за статуса;
  `resourceVersion` требует только JSON patch DeploymentConfig. Отказ admission — `ADMISSION_DENIED`.
- Предложения LLM (`health/proposals.rs`, инструменты `propose_change`, `list_proposals`, `withdraw_proposal`) хранятся в
  `health/<id>.json`; при создании проверяются только чтением кластера, применяет их человек формой, отметку
  «применено» ставит `apply_change`.
- Интеграционные тесты на kind: `tests/it_ops.rs` (каждый вид изменения, `CONFLICT`, стражи) и `tests/it_metrics.rs`
  (поддельный Prometheus за прокси API-сервера: автопоиск, статистика, рекомендация, правила `resources.*`).

## Где хранятся данные
- Профили и папки: `connections.json` в каталоге данных (macOS: `~/Library/Application Support/io.github.vvnovg.glafira.kubernetes/`).
- Токены и ключи клиентских сертификатов — в системном хранилище секретов, сервис `glafira-kubernetes`, ключи
  `<id>/token` и `<id>/clientKey`. Исключение — секретные значения переменных окружения и аргументов exec-плагина: они
  лежат в `connections.json` (права 0600, как у kubeconfig). Секретно имя, где есть TOKEN, SECRET, PASS, KEY,
  CREDENTIAL, BEARER или COOKIE: `--client-secret x`, `--oidc-client-secret=x`, `AWS_SECRET_ACCESS_KEY`
  (`is_secret_name` в `src-tauri/src/auth/exec.rs`). В строке запуска такие значения скрыты (`••••`), в отладочном
  выводе — `***`.
- Настройки: `settings.json` (в том числе включённый MCP и подтверждённые MCP-клиенты). Файлы — с правами только для
  владельца (0600 на macOS и Linux).
- Проверка кластера, замечания и предложения LLM: `health/<id>.json` (id из строчных латинских букв, цифр, `-` и `_` — как есть; любой другой,
  в том числе с заглавной буквой (файловые системы macOS и Windows по умолчанию не различают регистр, и `Prod-1` с
  `prod-1` делили бы файл) и начинающийся с `h-`, — `h-<hex>`; у id длиннее 100 байт файла нет); удаляется вместе с подключением. Журнал
  MCP — `mcp-audit.jsonl` (ротация при 5 МБ в `mcp-audit.1.jsonl`): вызовы без содержимого ответов и текстов замечаний.
  `mcp.json` — только пока работает MCP-сервер.
- Доступ LLM (`llmAccess`, `llmNamespaces`) и источник метрик (`metrics`) — локальные поля профиля: экспорт их не
  пишет, импорт не переносит. Свой токен источника метрик — в хранилище секретов, ключ `<id>/metricsToken`.
- Экспорт подключений — файл `*.glafira-kubernetes-connections.json` без токенов и ключей; секретные значения
  exec-плагина в нём пустые (`--oidc-client-secret=`), строка запуска приходит неподтверждённой. Повторный импорт поверх
  существующего подключения сохраняет локальные значения. Если импорт с «Обновлять существующие подключения» меняет
  адрес API, прокси, CA или имя сервера TLS подключения с сохранённым токеном, ключом или подтверждённым exec-плагином,
  предпросмотр предупреждает, что учётные данные уйдут на новый адрес (`destination_warnings` в
  `src-tauri/src/storage/exchange.rs`; ключи `messages.import.addressChanged`, `proxyChanged`, `tlsChanged`).
- Логи: `RUST_LOG=debug pnpm tauri dev`; файлы — в папке логов ОС (путь — в «О программе»).

## Политика администратора
Ключи `updatesEnabled`, `updateUrl`, `language`, `mcpEnabled`, `allowInsecureTls`, `allowExecAuth`; примеры —
`docs/admin/`.

## Языки
Интерфейс и руководство — на английском и русском; остальные 10 языков Kafka-версии добавляет этап O6.
Как добавить строку — `docs/i18n/README.md`.

## Лицензия и поддержка проекта
- Код открыт под [Apache License 2.0](LICENSE). Имя Glafira и значок лицензией не покрываются — см. `TRADEMARK.md`.
- Пункт «Поддержать проект» ведёт на https://github.com/vvnovg/glafira-kubernetes-releases/blob/main/SUPPORT.md.
- Как участвовать — `CONTRIBUTING.md`; об уязвимостях — `SECURITY.md`.
