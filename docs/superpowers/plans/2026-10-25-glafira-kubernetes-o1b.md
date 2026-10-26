# O1b «Чтение кластера» — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Glafira for Kubernetes читает кластер: обзор кластера (API-сервер по `/readyz`, ноды, классы Ingress и Gateway, у OpenShift — версия и обновления, операторы, пулы машин) и ноды, проекты с правами пользователя в проекте, workloads с HPA, PDB и историей выкладки, поды и их контейнеры, события, сеть (Services, Routes, Ingress, Gateway, HTTPRoute, GRPCRoute), конфигурацию (значения Secret — никогда), хранилище, квоты, YAML, логи с фильтром и переподключением, списки, которые обновляются сами (watch), срок токена OpenShift и группы на шаге «Вход»; закрыты долги O1a в подключении, exec-плагине и форме.

**Architecture:** Всё чтение — в Rust через `ReadClient` (newtype над `kube::Client`: get, list страницами, watch, логи, GET путей API-сервера, проверки прав), команды — тонкий слой над модулями `k8s/` (обзор, ноды, проекты, права, workloads, поды, события, сеть, конфигурация, хранилище, квоты, YAML), `watch/` и `logs/` (сессии с `CancellationToken`, данные — каналом Tauri пакетами), поведение по платформе — в трейте `Platform` (`platform/adapters.rs`). Части экрана без прав приходят причиной (`Part`). Фронтенд — вкладки проекта, workload'а, пода и нод, обзор кластера на вкладке подключения, проекты в дереве; списки виртуализированы (`@tanstack/react-virtual`), watch и логи — хуки поверх `Channel`, после входа заново вкладки подключения перечитываются.

**Tech Stack:** Rust 2024 (tauri 2, kube 4.2 с `runtime`, k8s-openapi 0.28 `v1_32`, tokio, tokio-util 0.7, futures 0.3, libc 0.2 на unix, regex, time, pem, sha2, base64), TypeScript (React 19, TanStack Query 5 и Virtual 3, zustand 5, i18next, `@tauri-apps/api` `Channel`, `@tauri-apps/plugin-dialog`), Vitest + Testing Library, pnpm 11, kind v0.33.0 для интеграционных тестов.

**Спецификация:** `docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md` — строка O1b в §19 и разделы §4.1–4.3, §5.2–5.4, §6, §12, §15, §17. Опирается на O1a (`docs/superpowers/plans/2026-10-24-glafira-kubernetes-o1a.md`, ветка `main` @ `eede48f`): его пул и `AppState::connected`, проверку подключения, платформу и группы API, exec-плагины, вкладки. Долги, которые O1a отложил в O1b (раздел «Отступления при выполнении» плана O1a), — задачи 1, 2 и 18. Уточнения спеки по итогам прототипа (задача 19 вносит их в спеку):
- права, группы API, платформа и срок токена приходят в ответе `connect` (`ConnectionInfo`); отдельные `get_capabilities`, `get_project_overview` (обзор проекта собирает интерфейс из списков), `get_rollout_history` (история — в `get_workload`) и `list_hpas` (HPA и PDB — в `get_workload`) не нужны;
- сеть — одна команда `get_network` (каждый вид своей частью), конфигурация — `get_config` и значения ConfigMap по щелчку `get_config_map_values`;
- новая команда `cancel_connection_test`: у проверки подключения свой ключ входа `test:<id>`, проверка в форме и подключение вкладки не отменяют друг друга;
- шаг «Вход» показывает первые 10 групп, остальные — числом;
- `save_logs(params, path, timestamps)` читает лог заново без follow; переподключение follow — с начала секунды последней строки, повторы отбрасываются по времени;
- поды CronJob через watch не читаются — интерфейс опрашивает их раз в 10 с; после входа заново вкладки подключения перечитываются, watch и логи начинаются заново;
- YAML — свой вывод JSON → YAML с ключами по алфавиту, как `kubectl get -o yaml` (без `serde_yaml`);
- интеграция O1b — kind без CRD OpenShift и Gateway API (они нужны правилам O2); объекты проекта запускает образ-помощник local-path-provisioner, образы не скачиваются.

**Проверено до написания плана.** Весь код плана собран и прогнан в отдельной копии репозитория (по коммиту на задачу, база `eede48f`); повторное применение всех блоков плана в чистом клоне `eede48f` даёт то же дерево после каждой задачи побайтно (кроме генерируемых `src-tauri/Cargo.lock` и `pnpm-lock.yaml`), проверки после каждой задачи — зелёные (числа ниже — на момент написания плана; что изменилось при выполнении и каковы числа сейчас — в разделе «Отступления при выполнении»):
- Rust — 352 unit-теста (`cargo test --lib`; было 258 в O1a), clippy с `-D warnings`, `cargo fmt --check`; `cargo deny` — `advisories ok, licenses ok`; в дереве зависимостей нет `aws-lc-rs` и `openssl-sys`;
- интеграционные на настоящем kind v0.33.0 (Kubernetes 1.37): `it_connect` — 4, `it_read` — 6 (стенд `dev/kind/setup.sh` с `fixtures.yaml`; без `GO_IT_KUBECONFIG` пропускаются; на свежем кластере `it_read` иногда идёт 4–5 минут: kubelet 1.37 не всегда сразу пишет CrashLoopBackOff: на двух свежих кластерах из семи — позже чем через 2 минуты, в худшем замере через 263 с, поэтому тест проблемных подов ждёт до 600 с);
- фронтенд — 259 тестов в 48 файлах (было 215 в 32), `tsc --noEmit`, `pnpm build` с проверкой совместимости бандла с WebKit macOS 11, `pnpm i18n:check` (словари en и ru, 136 вызовов из Rust, 4 главы руководства на двух языках);
- скрипты — 54 теста (`pnpm test:scripts`); шаги «Увидеть падение» проверены — тесты фронтенда падают до реализации.
Не проверено: окно приложения (`pnpm tauri dev`) и смоук `docs/smoke/o1b.md`; OpenShift — обзор (ClusterVersion, операторы, пулы машин, консоль), срок токена по `useroauthaccesstokens`, Routes и DeploymentConfig проверены только на моках API-сервера; Gateway API — только на моках (на kind в O1b CRD не ставятся); завершение группы процессов exec-плагина проверено на macOS, на Linux — только в CI, на Windows завершается лишь сам плагин.

## Отступления при выполнении

Блоки кода этого плана воспроизводят состояние **до ревью**: так их собирал и проверял прототип. Поверх задач в ветке `worktree-o1b` лежат fix-коммиты ревью (`git log --oneline 43788e6..HEAD`: `fix:`, `fix(…)`, `test(…)` и сквозной `36d28b6`), **источник истины — ветка**. Блоки заново поверх ветки не применять: они вернут исправленные дефекты. Блоки плана не правились: каждый fix-коммит проверял, что блоки следующих задач по-прежнему применяются (`patch -F0`, `git apply --recount`). Итоговое поведение записано в спеке (§4.2, §4.3, §5.2, §5.4, §6.2–6.7, §12, §17), справке (`docs/guide/`), смоуке и README. Номера строк ниже — в этом файле, с этим разделом; для блоков указан и id. Задачи 2 и 14 прошли ревью без fix-коммитов; задача 19 применила свои блоки дословно, тексты приведены к коду следующим коммитом.

| Задача | Что изменилось | Коммит | Устаревшие места плана |
|---|---|---|---|
| 1 | Команда `connect` идёт через `connection_info`: ошибка самого подключения клиент не сбрасывает, и ожидающие очереди на этом пути тоже получают общий итог (без лишних запусков exec-плагина); `delete_connection` отменяет и идущую проверку `test:<id>` | `962eafa` | патч `t01-src`: `commands/connections.rs` (575–607; `connect` и `delete_connection` в плане не меняются), `state.rs` и тесты ожидания только через `connected` (848–987) |
| 3, follow-up | `AppState::read`: ошибка `connected` возвращается сразу, клиент сбрасывает только ошибка `f` | `9691df0` | `read` в патче `t03-src` (2268–2284) |
| 3 | Выбывает только упавший клиент (`ClientPool::evict_if_same`, `AppState::evict_if_broken_client`); перезагрузка окна останавливает все сессии (`on_page_load` → `Sessions::stop_all`); сохранение профиля, новый токен и обновление импортом останавливают сессии подключения (`stop_for`); `pages` читает не больше `MAX_ITEMS / PAGE_SIZE + EXTRA_PAGES` страниц, бесконечный `continue` даёт `truncated` | `d8cb340` | `pages` (1788–1814), `read` и `evict_if_broken` (2268–2295), `lib.rs` в патче `t03-src` (2218–2237, без `on_page_load`), `sessions.rs` (блок `t03-…-sessions-rs-impl`, 1918, без `stop_all`) |
| 4 | `OpenShiftOverview.infrastructure` и `console_url` — `Part<Option<String>>` (403 — причина, а не пусто); версия ClusterVersion — последняя `Completed` из `status.history` (`current_version`); токен при поиске срока остаётся `SecretString` | `4c1a04b` | поля (3820–3821) и чтение в блоке `t04-…-adapters-rs-impl` (3767), `cluster_version_info` (3262), поиск срока в патче `t04-src` (4096); типы фронтенда (13139–13140) — см. задачу 13 |
| 5, follow-up | `evict_if_broken` и `breaks_client` удалены; `evict_if_broken_client` при выбывании забывает и кэш прав (`forget_connection`) | `0cc3de9` | `evict_if_broken` в патче `t05-src` (4959–4970) и тест `only_credential_errors_evict` |
| 5 | `connected(…, fresh)` сбрасывает и клиент, и кэш прав (`evict_connection`); у `AccessCache` эпоха подключения: `put(id, ns, epoch, access) -> bool`, поздний ответ прежнего клиента в кэш не попадает (`cached_namespace_access`); ответ `SelfSubjectRulesReview` без `status` — `incomplete: true` | `87677ec` | `self.pool.evict(id)` в `connected` (866), `AccessCache::put` (4614), `forget_connection` (4627), `get_namespace_access` (4444), `rules_review` (`unwrap_or_default`, 4898) |
| 6 | Ошибки частей видны `read` и `read_in`: `ReadClient::call` отмечает 403 и ошибки учётных данных и TLS (`observe`, `task_local!`) — 403 в любой части сбрасывает права проекта, 401 и TLS выбивают клиент и при `Ok`; битый DeploymentConfig — ошибка вида с его именем (`errors.k8s.malformedObject`), а не пустая строка; текущая ревизия StatefulSet — по `status.currentRevision`, проблема — по `available`; общие `rollout_problem` и `view::int_or_string` | `eff26a5` | методы `ReadClient` (1738–1902, без `call`), `read` и `read_in` (2271, 4944), `parse_deployment_config` (6164), `stateful_set_row` (6203), `list_kind` (6236), `int_or_string` (6366), `history` (6433–6534); ключа `malformedObject` нет в блоках `locales` |
| 7 | Нативные sidecar-контейнеры (`restartPolicy: Always`) не держат под в `Init:N/M` и входят в готовность и рестарты, как в kubectl; события workload'а — по форме имён детей (`is_child`), а не по префиксу; workload без селектора — без подов; у CronJob `truncated` списка Job | `dfa465e` | `pod_status` (7361), `pod_row` (7510), `list_pods` (7550), `CHILD_KINDS` (7133), `is_workload` и `concerns` (7223–7233), описание задачи (6761: «по префиксу имени») |
| 7, второй fix | Хеш и суффикс сгенерированных имён — алфавит `rand.String` Kubernetes (`GENERATED`), поды хуков DeploymentConfig; пределы (индексированные Job, Job из CronJob вручную, имена длиннее ~46 символов) — в комментариях `is_child` | `f8212e2` | блок `t07-…-events-rs-impl` (7117–7283) |
| 8 | Эндпоинты dual-stack — по семейству адресов, без двойного счёта; родители маршрутов Gateway API сравниваются с namespace маршрута по умолчанию; `port_text` заменён `view::int_or_string` | `a0f3c85` | `port_text` (8335), `endpoints_by_service` (8423), `gateway_route_row` (8720) |
| 9, follow-up | YAML через `redact`: у Route нет `spec.tls.key`, у всех видов нет аннотаций с копией объекта (`OBJECT_COPIES`: `last-applied-configuration`, `kapp.k14s.io/original`, `objectset.rio.cattle.io/applied`, `openshift.io/encoded-deployment-config`); скаляры и кавычки — по правилам YAML 1.2 (свой `quoted`, блок у корня с отступом) | `770fd12` | `object_yaml` (9868–9881, снимает только `managedFields`), `plain` (9887), `to_yaml` (10024), описание задачи (9009) |
| 9, follow-up 2 | Значения env с секретными именами — `***` (`mask_env`, `is_secret_env`), `kubernetes.io/change-cause` с таким присваиванием — `***` (решение пользователя); имена и namespace проверяются до запроса (`check_name`, `check_namespace` в `ReadClient` и `read_in`), ключ `errors.k8s.badName` | `332f5f7` | `object_yaml` (9869–9881), `get_yaml` (10130), методы `ReadClient` (1738–1902) и `rules_review` (4889), `read_in` (4944); глобальное ограничение 4 (188) переписано на месте |
| 10 | Предел 10 000 строк на сессию и `truncated` у `Reset` (`Snapshot`); срок на страницу начального списка с перезапуском потока; любая ошибка списка вида — ошибка вида (`Failure::List`), 410 — перечитывание без ошибки; ошибки учётных данных и TLS — `fatal` и в опросе, `Report` сбрасывает права и клиент; сессия регистрируется до `read_in`; без селектора — пустой `Reset` | `e95d09c` | Produces и описание (10225–10227), `WatchEvent::Reset { items, errors }` (10543–10546, 10745, 10783), `Failure::ListForbidden` и `classify` (10576–10599, 10690–10699), `run` (10709), `poll_loop` (10778), `start_watch_impl` (10946) |
| 11, follow-up | Сессия логов регистрируется до `read_in`; 401 и TLS при переподключении — ошибка сразу; ошибки `open` и `running` идут в `Report` (`Reported`) | `2d37142` | `start_logs_impl` (11935), переподключение в `run` (11730–11832) |
| 11 | 404 при переподключении и от `running()` — `End`, 403 — ошибка сразу, `previous` — без follow; удачное переподключение сбрасывает отсчёт 60 с; строки — `lossy_lines` (U+FFFD, предел памяти на строку); без follow обрыв и таймаут строки — ошибка; `save` — атомарно (`AtomicFile`, 0600), частями, с таймаутом строки; `save_logs` останавливается при отключении (`errors.logs.saveStopped`); `since_time` проверяется (`errors.logs.badSinceTime`) | `ada4c43` | `r.lines()` в `KubeLogs` (11691), `run` (11730–11832), `save` (11851–11875, `std::fs::write`), `save_logs` (11961), ключи `t11-locales` (12043) |
| 12, follow-up | `truncated` у `reset` и в `WatchState`; `restartSessions` после сохранения профиля, токена и импорта; поле затем стало необязательным (`truncated?: boolean`), чтобы тесты задач 14 и 15 из плана проходили без правок | `d38c25f`, `f6c34a2` | тип `WatchEvent` (13590), `useWatchList.ts` (12816), `afterSignIn.ts` (12761), Produces (12110) |
| 12 | `useWatchList` сбрасывает состояние при смене ключа сессии прямо при рендере, тесты гонок; `VirtualTable` не перехватывает Enter и пробел потомков; `YamlView` — итог копирования привязан к тексту, отказ виден (`common.copyFailed`); `Conditions` — акцент по полярности | `f375f7e` | `useWatchList.ts` (12816–12899) и тест (12320), `onKeyDown` в `VirtualTable.tsx` (12673), `YamlView.tsx` (12718–12760), `Conditions.tsx` (12454) |
| 12, второй fix | Состояние `useWatchList` помечено ключом сессии (`{ key, state }`): события прежней сессии не попадают в новую; `PodCompleted` без выделения; область `role="status"` у YAML смонтирована всегда | `c88aea7` | те же блоки (12816, 12454, 12718) |
| 13, follow-up | Инфраструктура и консоль OpenShift — `Part` через `PartView` | `3d0ed63` | типы (13139–13140), `ClusterOverview.tsx` (14291–14297), фикстура теста (13901–13902) |
| 13 | `useNow`: срок токена пересчитывается со временем, у истёкшего — «Подключиться заново»; консоль — ссылкой только для http(s), отказ `openUrl` обработан; «Войти снова» на вкладке подключения сразу запускает вход; `/readyz` без ответа — «—»; `operatorsOk` — «Операторов: N, все в норме.» | `d91b08a` | `ConnectionPanel.tsx` в патче `t13-src` (14513–14582; `Date.now()` 14559), `ClusterOverview.tsx` (14105; `readyz` 14157–14172, консоль 14295–14297, `operatorsOk` 14310), словари (14649, 14721) |
| 15 | Обзор проекта показывает ошибки видов (`KindErrors`) и одинаковую ошибку разделов одним баннером; права — `useNamespaceAccess` с таймером и `refreshNamespaceAccess` по «Обновить»; у `get_namespace_access` параметр `force`; дерево — «Повторить», `sidebar.noProjectsFromProfile`, `sidebar.noMatches`; DeploymentConfig в фильтре — по группе API. После ревью задачи 19 таймер — срок кэша Rust плюс 5 с (`ACCESS_REFRESH_MS = 5 * 60_000 + 5_000`): с ровно 5 минутами тик без `force` заставал ещё живой кэш, и до кластера доходил только каждый второй, то есть права обновлялись раз в 10 минут | `358cac5`, `fix: права проекта перечитываются…` (следующий после `3c447ab`) | `ProjectOverview.tsx` (16369), запрос прав в `ProjectPanel.tsx` (16518), `WorkloadsTable.tsx` (16561), `ConnectionNode.tsx` в патче `t15-src` (16698), команда (4444) и `api.getNamespaceAccess` (12996), Produces задачи 5 (4228), таблица команд в патче спеки (19726–19727) |
| 16 | Срок сертификата Route — `useNow`, «истёк» отдельно от «скоро» (`project.network.certExpired`); PVC `Pending` — `warn`, `Lost` — `problem` (отступление от теста плана); длинные значения ConfigMap не растягивают таблицу (`.config-value`); множественное число размера двоичных данных; значки и заголовки Gateway с текстом | `471238b` | `NetworkView.tsx` (17220; `Date.now()` 17300), `StorageView.tsx` (17596), `ConfigView.tsx` (17103, 17127), тест (16970: `Pending` с `problem`), словари (17720) |
| 17 | Rust шлёт пустой `Lines` со счётчиками и после удачного переподключения (`interval_at`); над списком — сколько строк отброшено (`logs.dropped`); хвост и фильтр применяются после паузы ввода; у выключенного «Следить» — пояснение (`logs.followPrevious`); лог доступен с клавиатуры; длинные строки — горизонтальная прокрутка вместо многоточия; подписи «Начиная с» — `sinceMinutes` и `sinceHours` (`sinceLast` удалён) | `db42d17` | `run` в `logs/mod.rs` (11730), `LogsView.tsx` (18016; `sinceLast` 18180, «Следить» 18186), `buffer.ts` (18242), `.log-line` в патче `t17-src` (18267), словари (18326, 18357), тест (17876) |
| Сквозной, после 17 | «Первые 10 000» (`TruncatedNote`) на экранах подов, workloads, событий, PVC, нод, обзора проекта и в дереве проектов; скрытая вкладка (`TabActiveContext`, `useRefetchInterval`) не запускает watch и автообновление, лог со слежением идёт; один порог квот — 90 % (`quota.ts`, `quotasOk` с `{{percent}}`) | `36d28b6` | `QUOTA_WARN = 0.8` (16384, 16460), `quotasOk` (16462, 16876, 16911), описание задачи 15 (16152: «на 80 %»); `refetchInterval` без учёта вкладки (14136, 14362, 15294, 15470, 15690, 16395); `useWatchList.ts` (12816); `Layout.tsx` в патчах (14480, 15841, 16664); `PodList.tsx` (15367), `EventList.tsx` (15277), `NodesPanel.tsx` (14343); смоук (19577) |
| 18 | Тест логов доказывает слежение (`tail_lines: Some(1)`, строка потока новее хвоста), утечка Secret проверяется по значению и base64 (`assert_no_secret`), `until` с меткой, BackOff ждётся до 300 с, пробный под watch удаляется всегда; README стенда — отдельный `KUBECONFIG`; CI — `timeout-minutes: 20` и вывод подов и событий при сбое | `46d0b04` | `it_read.rs` (18475; `until` 18508, Secret 18709, логи 18743–18764, watch 18823), `fixtures.yaml` (19002), патч `t18-src`: `ci.yml` (19223), `dev/kind/README.md` (19236), `setup.sh` (19261) |
| 19 | Справка, смоук (пункты 2, 5, 7, 9, 11–16 и новые 20–23), README, спека и глоссарий приведены к коду после всех fix-коммитов; после ревью задачи 19 — пункт 14 обрывает поток перезапуском API-сервера (`docker pause` обрыва не даёт), пункт 5 — как получить короткий OAuth-токен, расхождения статуса подов с kubectl, ReplicationControllers в событиях DeploymentConfig, §17 спеки | `582059a`, `3c447ab`, `fix: права проекта перечитываются…` | блоки `t19-docs-…` (19372–19613) и патч `t19-src` (19615–19825): актуальные тексты — в `docs/guide/`, `docs/smoke/o1b.md`, `README.md`, спеке и `docs/i18n/glossary.md` |
| Итоговое ревью ветки | Аннотации вкладок «Обзор» workload'а и пода — через общий `yaml::redact_annotations`, как в YAML (без копий объекта, `change-cause` с секретом — `***`); поток сессии watch и логов, который открывается снова, берёт клиент у `AppState::connected` (`sessions::StreamClient`: истекающие учётные данные exec-плагина — новые, 401 — новый клиент не чаще раза в минуту на поток), опрос — тоже; «Войти снова» в `ReadError` подключается заново (`reconnect`, общий ключ входа) и открывает вкладку подключения, `OverviewError` удалён; таймаут чтения клиента 295 с (`READ_TIMEOUT`), `timeoutSeconds=290` у каждого watch (`WATCH_TIMEOUT`), follow, проживший дольше 60 с и оборванный таймаутом чтения, открывается снова без `Reconnecting`; ошибка до первого `Reset` видна рядом с «Загрузка…»; `ScalingLimited` и `DisruptionTarget` = `True` — `warn`; `pods/*` в правилах RBAC не раскрывается (как `ResourceMatches`); вид и имя объекта событий проверяются до `fieldSelector` (`check_field_value`, без `,` и `=`); временный файл — `create_new`, занятое имя (подложенный symlink) пропускается; `PluginGroup` — только unix; 404 частей обзора OpenShift (пулы машин на ROSA HCP, консоль) — «—», гейтинга по группам API нет; CI — `timeout-minutes: 30`; смоук — пункты 5, 14, 17 и новый 24 | `fix: аннотации в обзорах замаскированы, сессии exec-подключений переживают срок учётных данных, вход из ReadError, таймауты watch и логов` (следующий после `a6c6571`) | `get_workload` (6625), `pod_details` (7740), `resource_matches` (4546), `list_events` (7251), пулы машин в `overview` (3905), `PluginGroup` в патче `t01-src` (359), `sources` и `poll` (10817, 10875), `start_watch_impl` и `start_logs_impl` (10946, 11935), `KubeLogs` (11652), `ReadError.tsx` (12545), `Conditions.tsx` (12454), `ClusterOverview.tsx` (14105), `WatchStatusLine.tsx` (15623), `LogsView.tsx` (18016), `ci.yml` в патче `t18-src` (19223), тексты справки о «Войти снова» в блоках `t19-docs-…` (19405, 19493) |
| Глобальные ограничения | Ограничение 4 (188) переписано на месте: ключ `spec.tls.key` Route не «не читается», а не уходит из Rust (YAML Route читается целиком, ключ вырезает `redact`); «именами и источниками, без значений» — про обзор контейнеров, а YAML — без аннотаций-копий и с маской env по секретным именам | `770fd12`, `332f5f7` | прежняя формулировка осталась в описаниях задач: задача 8 (7943: «ключ `spec.tls.key` не читается» — верно для сети), задача 9 (9009: «YAML — объект без `metadata.managedFields`») |
| Ожидаемые числа | В строках `Expected:` числа до ревью: Rust от 265 до 352, фронтенд от 216 тестов в 32 файлах до 259 в 48, «вызовов из Rust» 133–136 | все fix-коммиты | строки `Expected:` (1043–19842) и шапка (22–25); текущие числа — ниже |

**Числа сейчас** (ветка `worktree-o1b`):
- Rust: `cargo test --lib` — 447 тестов (в плане 352); clippy с `-D warnings` и `cargo fmt --check` чистые; интеграционные на kind v0.33.0 (Kubernetes 1.37) прогнаны исполнителями задач и итоговым fix-коммитом: `it_connect` — 4, `it_read` — 6 (на свежих подах 127,8 с и 68,7 с, на работающих — 1,6–2,1 с; без `GO_IT_KUBECONFIG` пропускаются);
- фронтенд: `pnpm test` — 404 теста в 54 файлах (в плане 259 в 48);
- `pnpm i18n:check`: словари en и ru согласованы, вызовов из Rust: 141 (в плане 136: добавлены, например, `errors.k8s.malformedObject`, `errors.k8s.badName`, проверка namespace `errors.connections.badNamespace`, `errors.logs.badSinceTime`, `errors.logs.saveStopped`), 4 главы руководства на двух языках;
- скрипты: `pnpm test:scripts` — 54 теста;
- `pnpm build` проходит, чанки совместимы с WebKit macOS 11; Vite предупреждает о чанке больше 500 кБ (`index-*.js`, с задачи 13);
- смоук `docs/smoke/o1b.md` (24 пункта) в окне приложения не пройден; пункты 20–24 и правки пунктов 2, 5, 7, 9, 11–17 написаны по коду.

**Решения пользователя во время выполнения** (отступления от плана):
- Задача 9 (2026-10-26): в YAML workloads и подов литеральные значения переменных окружения с секретными именами (слова `is_secret_name` и имена §10.3 по умолчанию) заменяются на `***`, остальные значения видны; правило записано в спеку (§6.4, §12) и в глобальное ограничение 4 (строка 188).
- Правило, действующее с O1a: Important-дефекты кода плана, не противоречащие спеке (надёжность, утечки секретов, неверная классификация), исправляются без вопроса и перечисляются в итоге. Так исправлены: общий итог подключения на пути `connect` и отмена проверки при удалении (задача 1); sidecar-контейнеры, события workload'а без соседей, пустой селектор, `truncated` у CronJob (задача 7); ключ TLS и аннотации-копии в YAML, кавычки YAML 1.2, проверка имён (задача 9); конец лога, строки не в UTF-8, таймаут, отмена, атомарность и 0600 сохранения, `since_time`, гонка регистрации (задача 11); защиты `useWatchList`, клавиатура `VirtualTable`, копирование YAML, акценты условий (задача 12); срок токена со временем, консоль только http(s), вход на вкладке подключения, `/readyz` — «—» (задача 13); ошибки видов в обзоре, перечитывание прав, состояния дерева (задача 15); PVC `Pending` — предупреждение, а не ошибка, вопреки тесту плана (задача 16).

**Отложено** (открытые замечания журнала, которые влияют на поведение; остальное разбирает итоговое ревью):
- правило `pod.crashLoop` (O2): kubelet 1.37 между перезапусками держит контейнер в `Error`, такой под сейчас не считается проблемным;
- `get_network`, `get_config`, `get_quotas`, `get_cluster_overview` (классы, ноды, операторы и пулы машин OpenShift) и `list_cluster_operators` не передают `truncated` — их экраны без пометки «первые 10 000»;
- watch: после `WatchFailed` ошибка видна до следующих `Changes` или `Reset`; предел 10 000 у watch — на все виды сессии, у опроса — на вид (watch, зависший после `InitDone`, обрывает таймаут чтения клиента — итоговое ревью); у `request_timeout_ms` нет верхней границы — запрос, молчащий дольше 295 с, обрывает тот же таймаут чтения (ограничить ≤ 290 с при проверке профиля); поток, сдавшийся после повторного 401, сбрасывает из пула только исходный клиент сессии — взятый позже сломанный клиент живёт до первого чтения вкладки; ошибка замены клиента проглатывается, наружу идёт исходный 401; опрос при пустом пуле снова запускает exec-плагин через 10 с после неудачного входа (с kubelogin — вкладка браузера раз в ~5 минут);
- логи: init- и sidecar-контейнеры в follow заканчиваются `End` вместо переподключения; переподключение без единой строки берёт время с часов рабочего места (тихое переподключение по таймауту чтения делает так раз в ~5 минут у молчащего контейнера); у `Channel` нет обратного давления; нет кнопки «к концу» при переполненном буфере;
- обзор OpenShift читает MachineConfigPools, Infrastructure и Console всегда (на ROSA HCP — до трёх 403 на каждое обновление; 404 с итогового ревью — «—», а не ошибка; гейтить по группам API — в `ApiGroups` нет `machineconfiguration.openshift.io`, а адаптер получает только `Capabilities`); подпись платформы берёт целевую версию (`status.desired`), обзор — последнюю завершённую;
- права: 403 уровня кластера или другого namespace внутри `read_in` тоже сбрасывает права проекта (`Observed.forbidden` — простой флаг);
- маска YAML не трогает `command`/`args` и env с несекретными именами (`DATABASE_URL` с паролем); `watch`, `watch_dynamic` и `get_text` не проверяют имена на уровне `ReadClient` — MCP (O2) проверяет сам; `errors::malformed` кладёт текст serde, который может содержать значение поля, в `details`;
- хранилище: «Кем смонтирован» без прав на поды — «неизвестно» без причины, `mounts()` не учитывает `truncated` подов;
- статус подов расходится с kubectl: нет причины `SchedulingGated`, ephemeral-контейнеры не показываются, рестарты инициализирующегося пода — без обычных init-контейнеров (а `Terminating` перекрывает и завершённые фазы);
- `get_workload` читает HPA и PDB и для Job и CronJob — у пользователя без прав на них лишнее «Нет прав»;
- усечённый список событий берёт первые N в порядке API, а не новейшие (`truncated` при этом передаётся);
- на Windows завершается только сам exec-плагин (Job Object — позже).

## Блоки кода

Код переносится как есть. Перед каждым блоком стоит HTML-комментарий `<!-- block: <вид> <id> step=<задача>.<шаг> [path=…] [part=…] [mode=…] -->`: `shell` — команды bash из корня репозитория; `file` — новый файл по `path` (у Rust-файлов с тестами две части: `part=tests` — модуль тестов, `part=impl` — реализация, итоговый файл = реализация, пустая строка, модуль тестов); `patch` — патч `git diff` (применять `git apply --recount`); `locales` — фрагмент словарей для `scripts/merge-locales.mjs`. Ограждения — четыре и больше обратных кавычек: внутри встречаются тройные. Блоки не перепечатывайте: их применяет скрипт ниже, по шагам, в порядке появления в плане. Патчи рассчитаны на дерево `main` @ `eede48f` с применёнными предыдущими задачами плана; если `main` с тех пор менялся, патч, который не применился, переносится вручную по смыслу.

Скрипт извлекается из этого плана один раз, до задачи 1 (план лежит в `docs/superpowers/plans/` репозитория):

```bash
awk '/^<!-- block: helper o1b-blocks.py -->$/{f=1;next} f&&/^````/{n++; if(n==2) exit; next} f' \
  docs/superpowers/plans/2026-10-25-glafira-kubernetes-o1b.md > /tmp/o1b-blocks.py
python3 /tmp/o1b-blocks.py list | head
```

<!-- block: helper o1b-blocks.py -->
````python
#!/usr/bin/env python3
"""Блоки плана O1b: извлечение и применение по шагам.

Использование (из корня репозитория):
  python3 /tmp/o1b-blocks.py list                 — все блоки: id, вид, шаг, путь
  python3 /tmp/o1b-blocks.py get <id>             — содержимое блока в stdout
  python3 /tmp/o1b-blocks.py step <задача>.<шаг>  — применить блоки шага по порядку
План — $PLAN или docs/superpowers/plans/2026-10-25-glafira-kubernetes-o1b.md.

Виды блоков: shell — команды bash из корня репозитория; file — файл по path (part=tests пишет модуль тестов,
part=impl ставит реализацию перед ним через пустую строку; mode — права); patch — `git apply --recount`;
locales — строки словарей для `scripts/merge-locales.mjs`.
"""
import os
import re
import subprocess
import sys

PLAN = os.environ.get("PLAN", "docs/superpowers/plans/2026-10-25-glafira-kubernetes-o1b.md")
BLOCK = re.compile(r"<!-- block: (\w+) (\S+)((?: \w+=\S+)*) -->\n(`{4,})[\w-]*\n(.*?)\n\4\n", re.S)


def blocks():
    text = open(PLAN, encoding="utf-8").read()
    for m in BLOCK.finditer(text):
        attrs = dict(a.split("=", 1) for a in m.group(3).split())
        yield {"kind": m.group(1), "id": m.group(2), "body": m.group(5) + "\n", **attrs}


def write_file(b):
    path = b["path"]
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    part = b.get("part", "whole")
    body = b["body"]
    if part == "impl" and os.path.exists(path):
        body = body + "\n" + open(path, encoding="utf-8").read()
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    if "mode" in b:
        os.chmod(path, int(b["mode"], 8))


def apply(b):
    kind = b["kind"]
    if kind == "shell":
        subprocess.run(["bash", "-euo", "pipefail", "-c", b["body"]], check=True)
    elif kind == "file":
        write_file(b)
    elif kind == "patch":
        subprocess.run(["git", "apply", "--recount", "--whitespace=nowarn"], input=b["body"].encode(), check=True)
    elif kind == "locales":
        subprocess.run(["node", "scripts/merge-locales.mjs"], input=b["body"].encode(), check=True)
    else:
        sys.exit(f"неизвестный вид блока {kind}")


def main():
    cmd, *args = sys.argv[1:] or ["list"]
    all_blocks = list(blocks())
    if cmd == "list":
        for b in all_blocks:
            print(b["id"], b["kind"], b.get("step", ""), b.get("path", ""))
    elif cmd == "get":
        sys.stdout.write(next(b for b in all_blocks if b["id"] == args[0])["body"])
    elif cmd == "step":
        chosen = [b for b in all_blocks if b.get("step") == args[0]]
        if not chosen:
            sys.exit(f"в шаге {args[0]} нет блоков")
        for b in chosen:
            print(f"→ {b['id']} ({b['kind']}{', ' + b['path'] if 'path' in b else ''})")
            apply(b)
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
````

## Global Constraints

- Кластеры: OpenShift 4.16+ (OCP, OKD, ROSA, ARO, OSD) и любой Kubernetes 1.29+. Какие объекты читать и показывать, решают группы API (`ApiGroups`: Route — `route.openshift.io`, проекты — `project.openshift.io`, DeploymentConfig — `apps.openshift.io`, Gateway API — `gateway.networking.k8s.io/v1`), а не платформа; поведение, которое у платформ разное (обзор кластера, срок токена), — в адаптере `Platform`.
- Зависимости Rust: `kube` 4.2 с фичами `client`, `runtime`, `rustls-tls`, `ring`, `http-proxy` (без `default-features`); `k8s-openapi` 0.28 с `v1_32`; `tokio-util` 0.7, `futures` 0.3; на unix — `libc` 0.2. В дереве зависимостей нет `aws-lc-rs` и OpenSSL. Фронтенд — `@tanstack/react-virtual` ^3.14.18.
- Кластер только читается: все чтения O1b идут через `ReadClient` (get, list, watch, логи, GET путей API-сервера и проверки прав `SelfSubject*Review`); изменяющих запросов в этапе нет.
- Значения Secret кластера не уходят из Rust — ни в списках, ни в YAML (вида Secret в `YamlKind` нет); ключ `spec.tls.key` Route тоже не уходит из Rust (YAML Route читается целиком, ключ вырезается до отправки). Переменные окружения контейнеров в обзоре показываются именами и источниками, без значений; YAML объекта — без `managedFields` и аннотаций с копией объекта, литеральные значения env с секретными именами (слова `is_secret_name` и имена §10.3 по умолчанию) в нём заменяются на `***`, остальные видны (уточнено при выполнении: решение пользователя по задаче 9). Секреты подключения — по правилам O1a: keychain (сервис `glafira-kubernetes`), исключение — секретные значения exec-плагина в `connections.json` (0600), пустые в экспорте и скрытые на экране.
- Списки читаются страницами по 500 и не больше 10 000 объектов (`truncated`); данные сессий watch и логов уходят во фронтенд пакетами — не больше 200 элементов или раз в 100 мс; сессия останавливается `stop_session`, при отключении и при удалении подключения.
- Часть экрана, которую не прочитать из-за прав или ошибки, приходит причиной (`Part`: `data` или `error`), остальной экран показывается (спец. §15); нет прав — «—» с текстом «Нет прав: …».
- Тексты интерфейса и Rust — ключами в `src/locales/en.json` и `ru.json` (en — эталон ключей и английского текста Rust, ru — эталон смысла); строки добавляются фрагментами `node scripts/merge-locales.mjs`; в строковых литералах Rust вне тестов кириллицы нет; подсказка en — не длиннее 200 символов; названия видов ресурсов, `Normal` / `Warning`, taint и endpoint не переводятся (`docs/i18n/glossary.md`, `docs/i18n/same-as-en.json`).
- Проверки после каждой задачи (что применимо): `cargo test --manifest-path src-tauri/Cargo.toml --lib`, `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`, `cargo fmt --manifest-path src-tauri/Cargo.toml --check`; `pnpm test`, `pnpm exec tsc --noEmit`, `pnpm i18n:check`, `pnpm test:scripts`, `pnpm build`.
- Тесты Rust не ходят в сеть, кроме локальных сокетов на `127.0.0.1`: ответы API-сервера — мок `k8s::testing::mock_client` (JSON) и `mock_raw` (тело как есть), exec-плагины — скрипты `sh` во временном каталоге (только unix). Интеграционные тесты (`src-tauri/tests/`, стенд `dev/kind`) без `GO_IT_KUBECONFIG` пропускаются; кластер kind создаётся с отдельным `KUBECONFIG`, `~/.kube/config` не меняется.
- Тесты фронтенда мокают `api` (`vi.mock("../../api/client", …)`) и `useVirtualizer` (`vi.mock("@tanstack/react-virtual", …)`, как в Kafka-версии): в jsdom у списка нет размеров.
- `src-tauri/Cargo.lock` обновляет cargo, `pnpm-lock.yaml` — `pnpm install` (задача 12); оба коммитятся.
- Порт 1420 часто занят Vite Kafka-версии: `pnpm tauri dev --config '{"build":{"devUrl":"http://localhost:1430","beforeDevCommand":"pnpm dev --port 1430"}}'`.
- Комментарии, сообщения коммитов и документы — на русском, в стиле O1a. Коммит — `git add` по путям задачи (не `git add -A` и не `-a`), `git commit -s`, последняя строка сообщения — `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Push — только с согласия пользователя.

## Структура файлов

| Файл | Ответственность |
|---|---|
| `src-tauri/src/state.rs`, `k8s/pool.rs`, `auth/{mod,exec}.rs`, `k8s/connectivity.rs` | долги O1a: общий итог подключения, ключ проверки, группа процессов плагина, группы на шаге «Вход» |
| `src-tauri/src/k8s/read.rs` | `ReadClient`: get, list страницами, watch, логи, GET путей API-сервера, проверки прав |
| `src-tauri/src/sessions.rs`, `commands/sessions.rs` | сессии watch и логов, пакеты, приёмник данных; команды `start_watch`, `start_logs`, `save_logs`, `stop_session` |
| `src-tauri/src/k8s/view.rs` | часть экрана (`Part`), условия объектов, время |
| `src-tauri/src/platform/adapters.rs`, `k8s/openshift.rs` | трейт `Platform` (обзор, срок токена), объекты OpenShift |
| `src-tauri/src/k8s/{overview,nodes}.rs`, `commands/cluster.rs` | обзор кластера и ноды |
| `src-tauri/src/k8s/{projects,access}.rs`, `commands/projects.rs` | проекты, права в проекте и их кэш |
| `src-tauri/src/k8s/{selectors,containers,workloads}.rs` | селекторы меток, контейнеры шаблона, workloads, HPA, PDB, история выкладки |
| `src-tauri/src/k8s/{pods,events}.rs` | поды, их статус и проблемы, события |
| `src-tauri/src/der.rs`, `k8s/network.rs` | DER (перенос), сеть: Services, Routes, Ingress, Gateway API |
| `src-tauri/src/k8s/{quantity,configs,storage,quotas,yaml}.rs` | quantity, ConfigMaps и Secrets, PVC, квоты, YAML |
| `src-tauri/src/commands/objects.rs` | команды чтения объектов проекта |
| `src-tauri/src/watch/mod.rs`, `src-tauri/src/logs/mod.rs` | сессии watch и логов |
| `src-tauri/tests/{common/mod.rs,it_read.rs}`, `dev/kind/{fixtures.yaml,setup.sh}` | интеграция чтения на kind |
| `src/api/`, `src/lib/{age,afterSignIn,signIn,useWatchList}.ts`, `src/state/connectionEpoch.ts` | IPC O1b, возраст, перечитывание после входа, watch-хук |
| `src/components/common/{PartView,ReadError,VirtualTable,RefreshControls,YamlView,Conditions,KeyValues,SubTabs}.tsx` | общие части экранов чтения |
| `src/components/cluster/{ClusterOverview,NodesPanel}.tsx` | обзор кластера, вкладка нод |
| `src/components/objects/` | таблицы подов и событий, вкладки workload'а и пода, карточка контейнера |
| `src/components/project/` | вкладка проекта: обзор, workloads, сеть, конфигурация, хранилище, квоты, права |
| `src/components/logs/` | логи пода и их буфер |
| `src/components/connections/tree/ConnectionNode.tsx` | проекты и ноды в дереве |
| `docs/guide/{en,ru}/{projects-workloads,pods-logs}.md`, `docs/smoke/o1b.md` | справка и смоук |

### Task 1: Долги O1a в подключении: общий итог для ожидающих, ключ проверки, группа процессов exec-плагина, группы на шаге «Вход»

**Files:**
- Modify: `src-tauri/Cargo.toml`, `src-tauri/src/auth/exec.rs`, `src-tauri/src/auth/mod.rs`, `src-tauri/src/commands/auth.rs`, `src-tauri/src/commands/connections.rs`, `src-tauri/src/commands/folders.rs`, `src-tauri/src/k8s/connectivity.rs`, `src-tauri/src/k8s/pool.rs`, `src-tauri/src/lib.rs`, `src-tauri/src/state.rs`, `src-tauri/tests/it_connect.rs`, `src/api/client.ts`, `src/components/connections/ConnectionForm.test.tsx`, `src/components/connections/ConnectionForm.tsx`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: O1a: `AppState::connected`, `ClientPool::{connect_lock, generation, evict}`, `auth::Logins`, `auth::exec::run`, `k8s::connectivity::check_cluster`, `commands::folders::import_from_file`, `storage::exchange::StoredSecrets`.
- Produces: `ClientPool::{finished_attempts(id) -> u64, finish_attempt(id, generation, Option<&AppError>), failure_since(id, seen) -> Option<AppError>}`; `auth::test_key(id) -> String` (`test:<id>`); команда `cancel_connection_test(connectionId) -> bool`, `api.cancelConnectionTest(connectionId)`; ключ `messages.connectionTest.authOkGroups` (`user`, `groups`); unix-зависимость `libc`.

Долги O1a, которые нужно закрыть до того, как O1b добавит параллельных пользователей `AppState::connected` (каждая подвкладка проекта читает кластер сама). Ожидающие `connect_lock` получают итог чужой попытки: если она не удалась или её отменили, они получают ту же ошибку, а не запускают exec-плагин и вход по разу каждый (`ClientPool::failure_since` — ошибка попытки, завершившейся после того, как ожидающий встал в очередь, и только пока профиль не сбрасывали). Отмена ожидания плагина (`cancel_oauth_login`) поэтому отменяет его для всех ожидающих. У проверки подключения в форме свой ключ в `Logins` — `test:<id>`, и своя команда отмены `cancel_connection_test`: проверка и подключение вкладки того же профиля больше не отменяют друг друга. Exec-плагин на unix запускается в своей группе процессов (`process_group(0)`), а при отмене, таймауте и сбое ожидания завершается вся группа (`killpg`) — вместе с процессами, которые плагин запустил; на Windows — только сам плагин. Шаг «Вход» проверки подключения показывает группы пользователя (первые 10, остальные — «+N»; спец. §5.3). Ревью O1a: n1 — из ведущих ключей `aws` убран `--cli-binary-format`; n2 — импорт файла обмена без «Обновлять существующие» не читает keychain вовсе (предупреждать не о чем, лишнего запроса доступа на macOS нет). В `tests/it_connect.rs` — положительная проверка прав сервисной учётной записи: `create deployments` в `team-a` разрешено, в `default` — нет.

- [ ] **Step 1: Тесты.**

<!-- block: patch t01-tests step=1.1 -->
````diff
diff --git a/src-tauri/tests/it_connect.rs b/src-tauri/tests/it_connect.rs
index 1da8e23..048cc10 100644
--- a/src-tauri/tests/it_connect.rs
+++ b/src-tauri/tests/it_connect.rs
@@ -5,6 +5,7 @@ use std::path::{Path, PathBuf};
 use std::sync::Arc;
 
 use glafira_kubernetes_lib::error::ErrorCode;
+use glafira_kubernetes_lib::k8s::cluster::can_i;
 use glafira_kubernetes_lib::k8s::{connectivity, kubeconfig, pool};
 use glafira_kubernetes_lib::platform::PlatformKind;
 use glafira_kubernetes_lib::policy::PolicyStatus;
@@ -78,6 +79,11 @@ async fn admin_certificate_passes_every_step() {
             ("projects", true)
         ]
     );
+    assert_eq!(
+        r.steps[3].message.key, "messages.connectionTest.authOkGroups",
+        "у администратора kind есть группы"
+    );
+    assert!(r.steps[3].message.params["groups"].contains("system:authenticated"));
     assert_eq!(r.steps[4].message.key, "messages.connectionTest.discovery");
     assert!(r.steps[4].message.params["platform"].starts_with("Kubernetes 1."));
     let c = pool::open(profile, secrets, no_cancel()).await.unwrap();
@@ -119,6 +125,12 @@ async fn service_account_sees_only_its_project() {
     assert_eq!(c.info.user, "system:serviceaccount:team-a:glafira-dev");
     assert!(!c.info.capabilities.list_nodes);
     assert!(!c.info.capabilities.list_projects);
+    // Роль edit есть только в team-a: права проверяются и в обе стороны.
+    let limit = std::time::Duration::from_secs(10);
+    let create =
+        |ns: &'static str| can_i(&c.client, limit, "create", "apps", "deployments", Some(ns));
+    assert!(create("team-a").await, "роль edit в team-a");
+    assert!(!create("default").await, "в default прав нет");
 }
 
 #[tokio::test]
diff --git a/src/components/connections/ConnectionForm.test.tsx b/src/components/connections/ConnectionForm.test.tsx
index 79529bc..e9c17d4 100644
--- a/src/components/connections/ConnectionForm.test.tsx
+++ b/src/components/connections/ConnectionForm.test.tsx
@@ -16,7 +16,7 @@ vi.mock("../../api/client", async (importOriginal) => {
       moveItems: vi.fn(),
       getPolicyStatus: vi.fn(),
       resolveExecCommand: vi.fn(),
-      cancelOauthLogin: vi.fn(),
+      cancelConnectionTest: vi.fn(),
     },
   };
 });
@@ -263,6 +263,22 @@ describe("ConnectionForm", () => {
     await waitFor(() => expect(option).toBeDisabled());
   });
 
+  it("exec-плагин: «Отмена» во время проверки отменяет только проверку", async () => {
+    const approved: ConnectionProfile = {
+      ...newProfile(),
+      id: "e1",
+      name: "eks",
+      auth: { method: "exec", command: "/usr/local/bin/aws", args: ["eks", "get-token"], env: [], apiVersion: "client.authentication.k8s.io/v1beta1", provideClusterInfo: false, approved: "fp" },
+    };
+    vi.mocked(api.testConnection).mockReturnValue(new Promise(() => undefined));
+    vi.mocked(api.cancelConnectionTest).mockResolvedValue(true);
+    const user = userEvent.setup();
+    renderWithClient(<ConnectionForm initial={approved} tabId="t" />);
+    await user.click(screen.getByRole("button", { name: "Проверить подключение" }));
+    await user.click(await screen.findByRole("button", { name: "Отмена" }));
+    expect(api.cancelConnectionTest).toHaveBeenCalledWith("e1");
+  });
+
   it("удаление с подтверждением закрывает вкладки подключения", async () => {
     vi.mocked(api.deleteConnection).mockResolvedValue();
     useWorkspace.getState().open({ kind: "cluster", connectionId: "c1" });
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/connections/ConnectionForm.test.tsx`  
Expected: FAIL — тест «Отмена» во время проверки ждёт `api.cancelConnectionTest`, а форма вызывает `cancelOauthLogin`.

- [ ] **Step 3: Реализация.**

<!-- block: patch t01-src step=1.3 -->
````diff
diff --git a/src-tauri/Cargo.toml b/src-tauri/Cargo.toml
index 787ec82..5f54f9d 100644
--- a/src-tauri/Cargo.toml
+++ b/src-tauri/Cargo.toml
@@ -47,6 +47,9 @@ sys-locale = "0.3"
 [target.'cfg(windows)'.dependencies]
 winreg = "0.55"
 
+[target.'cfg(unix)'.dependencies]
+libc = "0.2"
+
 [dev-dependencies]
 tempfile = "3"
 tokio = { version = "1", features = ["rt-multi-thread", "macros", "time", "test-util"] }
diff --git a/src-tauri/src/auth/exec.rs b/src-tauri/src/auth/exec.rs
index d73dd31..56d4828 100644
--- a/src-tauri/src/auth/exec.rs
+++ b/src-tauri/src/auth/exec.rs
@@ -114,7 +114,6 @@ pub const ALLOWED: &[AllowedCommand] = &[
         leading: Some(LeadingOptions {
             values: &[
                 "--ca-bundle",
-                "--cli-binary-format",
                 "--cli-connect-timeout",
                 "--cli-read-timeout",
                 "--color",
@@ -844,8 +843,35 @@ fn plugin_path(command: &Path, dirs: &[PathBuf]) -> Option<String> {
         .map(|p| p.to_string_lossy().to_string())
 }
 
+/// Группа процессов плагина (на unix — своя, `process_group(0)`). Если ожидание прервано
+/// (отмена, таймаут, сбой), завершается вся группа: и плагин, и то, что он запустил (`sleep`,
+/// браузер, вспомогательные процессы), — `kill_on_drop` завершил бы только сам плагин. На
+/// Windows завершается только процесс плагина.
+struct PluginGroup(Option<u32>);
+
+impl PluginGroup {
+    /// Плагин завершился сам: его группу не трогаем.
+    fn release(mut self) {
+        self.0 = None;
+    }
+}
+
+impl Drop for PluginGroup {
+    fn drop(&mut self) {
+        #[cfg(unix)]
+        if let Some(pid) = self.0.and_then(|p| libc::pid_t::try_from(p).ok()) {
+            // SAFETY: killpg только посылает сигнал группе, которую создали для плагина;
+            // если её уже нет, вызов вернёт ESRCH.
+            unsafe {
+                libc::killpg(pid, libc::SIGKILL);
+            }
+        }
+    }
+}
+
 /// Запускает плагин: без оболочки, `stdin` закрыт, не дольше `limit`; `cancel` прерывает
-/// ожидание (как у входа через браузер). Процесс завершается вместе с ожиданием. Профиль могли
+/// ожидание (как у входа через браузер). Процесс завершается вместе с ожиданием — на unix
+/// вместе со всей своей группой (`PluginGroup`). Профиль могли
 /// поправить в обход проверки, поэтому абсолютный путь, список разрешённых команд и запрещённые
 /// переменные окружения проверяются и здесь. `path_env` — PATH процесса плагина.
 pub async fn run(
@@ -877,6 +903,8 @@ pub async fn run(
     if let Some(p) = path_env {
         cmd.env("PATH", p);
     }
+    #[cfg(unix)]
+    cmd.process_group(0);
     #[cfg(windows)]
     {
         // CREATE_NO_WINDOW: без мелькающего окна консоли.
@@ -892,6 +920,7 @@ pub async fn run(
         )
     };
     let child = cmd.spawn().map_err(start_error)?;
+    let group = PluginGroup(child.id());
     let output = tokio::select! {
         out = child.wait_with_output() => out.map_err(start_error)?,
         _ = cancel => return Err(crate::auth::callback::cancelled()),
@@ -905,6 +934,7 @@ pub async fn run(
             ));
         }
     };
+    group.release();
     // Значения секретных переменных и аргументов не попадают в текст ошибок.
     let secrets = secret_values(e);
     let secrets: Vec<&str> = secrets.iter().map(String::as_str).collect();
@@ -1167,6 +1197,17 @@ mod tests {
                 &["--logtostderr", "get-token"][..],
                 "--logtostderr",
             ),
+            // Только у AWS CLI v2, и к получению токена отношения не имеет.
+            (
+                "/usr/local/bin/aws",
+                &[
+                    "--cli-binary-format",
+                    "raw-in-base64-out",
+                    "eks",
+                    "get-token",
+                ][..],
+                "--cli-binary-format",
+            ),
         ];
         for (cmd, a, option) in unknown {
             let e = check_allowed(cmd, &args(a)).unwrap_err();
@@ -1792,6 +1833,7 @@ case "$3" in
   path) printf '{"status":{"token":"%s"}}' "$PATH" ;;
   fail) echo "refresh token eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1In0.c2lnbmF0dXJl expired, key $AWS_SECRET_ACCESS_KEY, args $*" >&2; exit 3 ;;
   slow) sleep 30 ;;
+  orphan) sleep 30 & echo $! > "$PID_FILE"; sleep 30 ;;
   garbage) echo "not json" ;;
   wrongtype) printf '{"status":"opaque-t0ken-value"}' ;;
 esac
@@ -2031,6 +2073,69 @@ esac
             );
         }
 
+        /// Процессы, которые запустил плагин, завершаются вместе с ним — и по таймауту, и по отмене.
+        #[tokio::test]
+        async fn timeout_and_cancel_stop_the_whole_process_group() {
+            /// pid фонового процесса, который записал плагин.
+            async fn started(pid_file: &Path) -> libc::pid_t {
+                let read = async {
+                    loop {
+                        if let Some(pid) = std::fs::read_to_string(pid_file)
+                            .ok()
+                            .and_then(|t| t.trim().parse().ok())
+                        {
+                            return pid;
+                        }
+                        tokio::time::sleep(Duration::from_millis(20)).await;
+                    }
+                };
+                tokio::time::timeout(Duration::from_secs(10), read)
+                    .await
+                    .expect("плагин записал pid")
+            }
+            async fn gone(pid: libc::pid_t) {
+                // SAFETY: kill с сигналом 0 только проверяет, есть ли процесс.
+                let alive = || unsafe { libc::kill(pid, 0) } == 0;
+                let stopped = async {
+                    while alive() {
+                        tokio::time::sleep(Duration::from_millis(20)).await;
+                    }
+                };
+                tokio::time::timeout(Duration::from_secs(5), stopped)
+                    .await
+                    .expect("процесс, запущенный плагином, завершён");
+            }
+            let dir = tempfile::tempdir().unwrap();
+            let pid_file = dir.path().join("pid");
+            let mut e = plugin(dir.path(), "orphan");
+            e.env.push(ExecEnv {
+                name: "PID_FILE".into(),
+                value: pid_file.display().to_string(),
+            });
+            let timed_out = tokio::spawn({
+                let e = e.clone();
+                async move { go(&e, Duration::from_secs(3)).await }
+            });
+            let pid = started(&pid_file).await;
+            let err = timed_out.await.unwrap().unwrap_err();
+            assert_eq!(err.key, "errors.exec.timeout");
+            gone(pid).await;
+
+            std::fs::remove_file(&pid_file).unwrap();
+            let (tx, cancel) = oneshot::channel();
+            let waiting = tokio::spawn({
+                let e = e.clone();
+                async move { run(&e, "{}", None, cancel, Duration::from_secs(60)).await }
+            });
+            let pid = started(&pid_file).await;
+            tx.send(()).unwrap();
+            assert_eq!(
+                waiting.await.unwrap().unwrap_err().key,
+                "errors.oauth.cancelled"
+            );
+            gone(pid).await;
+        }
+
         #[tokio::test]
         async fn unapproved_or_foreign_command_is_not_started() {
             let dir = tempfile::tempdir().unwrap();
diff --git a/src-tauri/src/auth/mod.rs b/src-tauri/src/auth/mod.rs
index ab6ddf8..f45a6a6 100644
--- a/src-tauri/src/auth/mod.rs
+++ b/src-tauri/src/auth/mod.rs
@@ -13,6 +13,12 @@ use tokio::sync::oneshot;
 
 use crate::sync::MutexExt;
 
+/// Ключ входа проверки подключения в `Logins`: проверка в форме и подключение вкладки того же
+/// профиля не отменяют друг друга.
+pub fn test_key(id: &str) -> String {
+    format!("test:{id}")
+}
+
 /// Идущие входы — через браузер и запуском exec-плагина: на подключение — один, новый
 /// отменяет прежний.
 #[derive(Default)]
@@ -76,4 +82,21 @@ mod tests {
         assert!(!l.cancel("a"));
         l.finish("a", n2);
     }
+
+    #[test]
+    fn connection_test_and_connect_do_not_cancel_each_other() {
+        let l = Logins::default();
+        let (_, mut connect) = l.begin("a");
+        let (_, mut test) = l.begin(&test_key("a"));
+        assert!(
+            connect.try_recv().is_err(),
+            "проверка не отменила подключение"
+        );
+        assert!(l.cancel(&test_key("a")));
+        assert!(test.try_recv().is_ok());
+        assert!(
+            connect.try_recv().is_err(),
+            "отмена проверки не задела подключение"
+        );
+    }
 }
diff --git a/src-tauri/src/commands/auth.rs b/src-tauri/src/commands/auth.rs
index 0526f57..ef8a697 100644
--- a/src-tauri/src/commands/auth.rs
+++ b/src-tauri/src/commands/auth.rs
@@ -54,12 +54,21 @@ pub async fn start_oauth_login(
     Ok(c.info.clone())
 }
 
-/// Отменяет вход через браузер или ожидание exec-плагина (подключение и проверка).
+/// Отменяет вход через браузер или ожидание exec-плагина при подключении.
 #[tauri::command]
 pub fn cancel_oauth_login(state: State<'_, AppState>, connection_id: String) -> AppResult<bool> {
     Ok(state.logins.cancel(&connection_id))
 }
 
+/// Отменяет ожидание exec-плагина при проверке подключения в форме.
+#[tauri::command]
+pub fn cancel_connection_test(
+    state: State<'_, AppState>,
+    connection_id: String,
+) -> AppResult<bool> {
+    Ok(state.logins.cancel(&crate::auth::test_key(&connection_id)))
+}
+
 /// Абсолютный путь к команде exec-плагина — кнопка «Найти» в форме: PATH оболочки входа
 /// и стандартные каталоги (спец. §5.2).
 #[tauri::command]
diff --git a/src-tauri/src/commands/connections.rs b/src-tauri/src/commands/connections.rs
index 9ce5949..815f3ee 100644
--- a/src-tauri/src/commands/connections.rs
+++ b/src-tauri/src/commands/connections.rs
@@ -177,7 +177,8 @@ pub async fn delete_connection(state: State<'_, AppState>, id: String) -> AppRes
 
 /// Проверяет и несохранённый профиль: секреты из формы накладываются поверх keychain
 /// (с той же семантикой, что и при сохранении). Ожидание exec-плагина отменяет
-/// `cancel_oauth_login`.
+/// `cancel_connection_test`: у проверки свой ключ в `Logins`, и она не отменяет подключение
+/// вкладки того же профиля, а та — её.
 #[tauri::command]
 pub async fn test_connection(
     state: State<'_, AppState>,
@@ -201,7 +202,8 @@ pub async fn test_connection(
         overrides,
     });
     let exec = matches!(input.profile.auth, AuthConfig::Exec(_));
-    let (n, cancel) = match exec.then(|| state.logins.begin(&id)) {
+    let key = crate::auth::test_key(&id);
+    let (n, cancel) = match exec.then(|| state.logins.begin(&key)) {
         Some((n, rx)) => (Some(n), rx),
         None => (None, tokio::sync::oneshot::channel().1),
     };
@@ -213,7 +215,7 @@ pub async fn test_connection(
     )
     .await;
     if let Some(n) = n {
-        state.logins.finish(&id, n);
+        state.logins.finish(&key, n);
     }
     Ok(report)
 }
diff --git a/src-tauri/src/commands/folders.rs b/src-tauri/src/commands/folders.rs
index 9453169..fa9075d 100644
--- a/src-tauri/src/commands/folders.rs
+++ b/src-tauri/src/commands/folders.rs
@@ -156,7 +156,13 @@ fn import_from_file(
     let snapshot = st.connections.snapshot();
     // Keychain читается до `update`: запрос доступа (на macOS — окно с паролем) не держит
     // блокировку записи хранилища, под которой ждали бы `list_connections` и `connections.get`.
-    let stored = StoredSecrets::read(st.secrets.as_ref(), &snapshot, &parsed);
+    // Без «Обновлять существующие» сохранённые подключения не меняются — предупреждать не о чем,
+    // и keychain не читается вовсе.
+    let stored = if update_existing {
+        StoredSecrets::read(st.secrets.as_ref(), &snapshot, &parsed)
+    } else {
+        StoredSecrets::default()
+    };
     if !apply {
         return Ok(plan_import(&snapshot, &parsed, &opts, &stored)?.preview);
     }
@@ -333,6 +339,24 @@ mod tests {
         assert!(reads.iter().all(|locked| !locked), "{reads:?}");
     }
 
+    /// Без «Обновлять существующие» предупреждать не о чем: ни предпросмотр, ни применение
+    /// keychain не читают (на macOS — без лишнего запроса доступа).
+    #[test]
+    fn import_without_updating_existing_does_not_touch_the_keychain() {
+        let dir = tempfile::tempdir().unwrap();
+        let probe = Arc::new(LockProbe::default());
+        let st = AppState::new(dir.path(), probe.clone()).unwrap();
+        st.connections.upsert(sample_profile("a")).unwrap();
+        probe.reads.lock().unwrap().clear();
+        let path = write_file(dir.path(), "https://api.changed:6443");
+
+        let preview = import_from_file(&st, &path, None, false, false).unwrap();
+        assert!(preview.updated.is_empty() && preview.warnings.is_empty());
+        let applied = import_from_file(&st, &path, None, false, true).unwrap();
+        assert_eq!(applied.added.len(), 1);
+        assert!(probe.reads.lock().unwrap().is_empty());
+    }
+
     #[cfg(unix)]
     #[test]
     fn failed_apply_changes_nothing() {
diff --git a/src-tauri/src/k8s/connectivity.rs b/src-tauri/src/k8s/connectivity.rs
index e995017..31eea07 100644
--- a/src-tauri/src/k8s/connectivity.rs
+++ b/src-tauri/src/k8s/connectivity.rs
@@ -153,6 +153,35 @@ fn discovery_message(d: &Discovered) -> LocalizedText {
     }
 }
 
+/// Сколько групп пользователя показывает шаг «Вход»; остальные — числом.
+const SHOWN_GROUPS: usize = 10;
+
+/// Имя пользователя и его группы (спец. §5.3): первые `SHOWN_GROUPS` через запятую, дальше «+N».
+fn auth_message(user: &str, groups: &[String]) -> LocalizedText {
+    if groups.is_empty() {
+        return ltext!(
+            "messages.connectionTest.authOk",
+            "Signed in as {{user}}",
+            user = user
+        );
+    }
+    let mut list = groups
+        .iter()
+        .take(SHOWN_GROUPS)
+        .map(String::as_str)
+        .collect::<Vec<_>>()
+        .join(", ");
+    if groups.len() > SHOWN_GROUPS {
+        list.push_str(&format!(", +{}", groups.len() - SHOWN_GROUPS));
+    }
+    ltext!(
+        "messages.connectionTest.authOkGroups",
+        "Signed in as {{user}}, groups: {{groups}}",
+        user = user,
+        groups = list
+    )
+}
+
 /// Ошибка транспорта — до ответа API-сервера дело не дошло (TLS, DNS, соединение, таймаут,
 /// сеть). Различаем по ключу, а не по коду: `k8s::errors` отдаёт те же коды и для HTTP-ответов
 /// (429 — `Timeout`, 5xx — `ConnectionFailed`, прочие 4xx — `Internal`), а ответ сервера
@@ -184,16 +213,9 @@ pub async fn check_cluster(
         )
     };
     match whoami(client, limit).await {
-        Ok((user, _)) => {
+        Ok((user, groups)) => {
             steps.push(passed("tls", tls_ok));
-            steps.push(passed(
-                "auth",
-                ltext!(
-                    "messages.connectionTest.authOk",
-                    "Signed in as {{user}}",
-                    user = user
-                ),
-            ));
+            steps.push(passed("auth", auth_message(&user, &groups)));
         }
         // Ошибка транспорта: ответа сервера не было, значит, не прошёл TLS (или связь).
         Err(e) if is_transport_failure(&e) => {
@@ -401,7 +423,12 @@ mod tests {
                 ("projects", true)
             ]
         );
+        assert_eq!(steps[1].message.key, "messages.connectionTest.authOkGroups");
         assert_eq!(steps[1].message.params["user"], "dev1");
+        assert_eq!(
+            steps[1].message.params["groups"],
+            "system:authenticated, team-a"
+        );
         assert_eq!(
             steps[2].message.key,
             "messages.connectionTest.discoveryGroups"
@@ -443,6 +470,21 @@ mod tests {
         );
     }
 
+    #[test]
+    fn sign_in_step_lists_ten_groups_and_counts_the_rest() {
+        assert_eq!(auth_message("u", &[]).key, "messages.connectionTest.authOk");
+        let groups: Vec<String> = (1..=12).map(|i| format!("g{i}")).collect();
+        let m = auth_message("u", &groups);
+        assert_eq!(
+            m.params["groups"],
+            "g1, g2, g3, g4, g5, g6, g7, g8, g9, g10, +2"
+        );
+        assert_eq!(
+            m.text,
+            "Signed in as u, groups: g1, g2, g3, g4, g5, g6, g7, g8, g9, g10, +2"
+        );
+    }
+
     #[tokio::test]
     async fn rejected_token_fails_auth_after_tls() {
         let (client, _server) = mock_client(|_| status(401, "Unauthorized", "Unauthorized"));
diff --git a/src-tauri/src/k8s/pool.rs b/src-tauri/src/k8s/pool.rs
index db9d460..716df44 100644
--- a/src-tauri/src/k8s/pool.rs
+++ b/src-tauri/src/k8s/pool.rs
@@ -45,6 +45,15 @@ pub struct ClientPool {
 struct PoolInner {
     clients: HashMap<String, Arc<Connected>>,
     generations: HashMap<String, u64>,
+    attempts: HashMap<String, Attempts>,
+}
+
+/// Попытки подключения профиля: сколько завершилось и ошибка последней — вместе с поколением,
+/// при котором она началась.
+#[derive(Default)]
+struct Attempts {
+    finished: u64,
+    failure: Option<(u64, AppError)>,
 }
 
 impl ClientPool {
@@ -71,6 +80,36 @@ impl ClientPool {
         true
     }
 
+    /// Сколько попыток подключения `id` завершилось. Ожидающий очереди запоминает число до
+    /// `connect_lock`, чтобы потом узнать, чем кончилась попытка, которую он ждал.
+    pub fn finished_attempts(&self, id: &str) -> u64 {
+        self.inner
+            .lock_safe()
+            .attempts
+            .get(id)
+            .map_or(0, |a| a.finished)
+    }
+
+    /// Записывает итог попытки, начатой при поколении `generation`; `None` — удалась.
+    pub fn finish_attempt(&self, id: &str, generation: u64, failure: Option<&AppError>) {
+        let mut inner = self.inner.lock_safe();
+        let a = inner.attempts.entry(id.to_string()).or_default();
+        a.finished += 1;
+        a.failure = failure.map(|e| (generation, e.clone()));
+    }
+
+    /// Ошибка попытки, завершившейся после `seen`, если профиль с её начала не сбрасывали:
+    /// ожидавшие очереди получают её, а не запускают exec-плагин или вход ещё раз.
+    pub fn failure_since(&self, id: &str, seen: u64) -> Option<AppError> {
+        let inner = self.inner.lock_safe();
+        let a = inner.attempts.get(id)?;
+        let current = inner.generations.get(id).copied().unwrap_or(0);
+        match &a.failure {
+            Some((g, e)) if a.finished > seen && *g == current => Some(e.clone()),
+            _ => None,
+        }
+    }
+
     /// Замок подключения `id` на время подключения.
     pub async fn connect_lock(&self, id: &str) -> tokio::sync::OwnedMutexGuard<()> {
         let lock = self
@@ -160,6 +199,32 @@ mod tests {
         assert!(pool.insert_if_current("a", pool.generation("a"), connected()));
     }
 
+    #[test]
+    fn failure_is_shared_only_with_those_who_waited_for_it() {
+        let pool = ClientPool::default();
+        let e = AppError::internal_with("boom");
+        let seen = pool.finished_attempts("a");
+        assert_eq!(pool.failure_since("a", seen).map(|e| e.key), None);
+        pool.finish_attempt("a", pool.generation("a"), Some(&e));
+        assert_eq!(
+            pool.failure_since("a", seen).and_then(|e| e.details),
+            Some("boom".into()),
+            "ждал эту попытку"
+        );
+        let later = pool.finished_attempts("a");
+        assert!(
+            pool.failure_since("a", later).is_none(),
+            "пришёл после неё — подключается сам"
+        );
+        pool.evict("a");
+        assert!(
+            pool.failure_since("a", seen).is_none(),
+            "профиль сбросили — прежняя ошибка не годится"
+        );
+        pool.finish_attempt("a", pool.generation("a"), None);
+        assert!(pool.failure_since("a", seen).is_none(), "удачная попытка");
+    }
+
     #[tokio::test]
     async fn open_reports_missing_token_without_network() {
         let p = crate::storage::profile::sample_profile("a");
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index 8c845a3..f20646c 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -53,6 +53,7 @@ pub fn run() {
             commands::connections::set_connection_token,
             commands::auth::start_oauth_login,
             commands::auth::cancel_oauth_login,
+            commands::auth::cancel_connection_test,
             commands::auth::oauth_token_page,
             commands::auth::resolve_exec_command,
             commands::kubeconfig::preview_kubeconfig_import,
diff --git a/src-tauri/src/state.rs b/src-tauri/src/state.rs
index 4a33ecb..a688519 100644
--- a/src-tauri/src/state.rs
+++ b/src-tauri/src/state.rs
@@ -42,9 +42,12 @@ impl AppState {
 
     /// Клиент подключения: из пула или новое подключение (`fresh` — всегда новое; истекающие
     /// учётные данные exec-плагина — тоже). Подключения одного профиля идут по очереди, ожидание
-    /// exec-плагина отменяется так же, как вход через браузер (`logins`). Подключение, которое
-    /// запрещено политикой (CA `insecure`, exec-плагин), не открывается.
+    /// exec-плагина отменяется так же, как вход через браузер (`logins`). Кто ждал очереди, пока
+    /// чужая попытка не удалась (или её отменили), получает ту же ошибку: чтения одной вкладки
+    /// не запускают плагин по разу каждое. Подключение, которое запрещено политикой (CA
+    /// `insecure`, exec-плагин), не открывается.
     pub async fn connected(&self, id: &str, fresh: bool) -> AppResult<Arc<Connected>> {
+        let seen = self.pool.finished_attempts(id);
         let _turn = self.pool.connect_lock(id).await;
         if fresh {
             self.pool.evict(id);
@@ -52,6 +55,8 @@ impl AppState {
             && !c.expiring(SystemTime::now())
         {
             return Ok(c);
+        } else if let Some(e) = self.pool.failure_since(id, seen) {
+            return Err(e);
         }
         let generation = self.pool.generation(id);
         let profile = self.connections.get(id)?;
@@ -67,6 +72,8 @@ impl AppState {
         if let Some(n) = n {
             self.logins.finish(id, n);
         }
+        self.pool
+            .finish_attempt(id, generation, result.as_ref().err());
         let c = Arc::new(result?);
         self.pool.insert_if_current(id, generation, c.clone());
         Ok(c)
@@ -148,6 +155,103 @@ mod tests {
         }
     }
 
+    /// Exec-плагин, который считает свои запуски (строка в `COUNT_FILE`) и через 0,3 с падает
+    /// (`fail`) или ждёт отмены (`wait`).
+    #[cfg(unix)]
+    fn counting_plugin(st: &AppState, dir: &Path, mode: &str) -> std::path::PathBuf {
+        use crate::storage::profile::{AuthConfig, ExecEnv, sample_exec};
+        use std::os::unix::fs::PermissionsExt;
+        let script = dir.join("kubectl");
+        std::fs::write(
+            &script,
+            "#!/bin/sh\necho run >> \"$COUNT_FILE\"\ncase \"$3\" in\n  fail) sleep 0.3; exit 1 ;;\n  wait) sleep 30 ;;\nesac\n",
+        )
+        .unwrap();
+        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
+        let count = dir.join("count");
+        let mut exec = sample_exec();
+        exec.command = script.display().to_string();
+        exec.args = vec!["oidc-login".into(), "get-token".into(), mode.into()];
+        exec.env = vec![ExecEnv {
+            name: "COUNT_FILE".into(),
+            value: count.display().to_string(),
+        }];
+        exec.approved = Some(crate::auth::exec::fingerprint(&exec));
+        let mut p = sample_profile("e");
+        p.auth = AuthConfig::Exec(exec);
+        st.connections.upsert(p).unwrap();
+        count
+    }
+
+    /// Три чтения подключаются одновременно: плагин запускается один раз, все получают его ошибку.
+    #[cfg(unix)]
+    async fn concurrent_connects(st: &AppState) -> Vec<AppError> {
+        let first = tokio::spawn({
+            let st = st.clone();
+            async move { st.connected("e", false).await }
+        });
+        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
+        let rest: Vec<_> = (0..2)
+            .map(|_| {
+                let st = st.clone();
+                tokio::spawn(async move { st.connected("e", false).await })
+            })
+            .collect();
+        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
+        let mut errors = Vec::new();
+        for h in std::iter::once(first).chain(rest) {
+            errors.push(h.await.unwrap().err().expect("подключение не удалось"));
+        }
+        errors
+    }
+
+    #[cfg(unix)]
+    #[tokio::test]
+    async fn waiters_share_the_failure_instead_of_rerunning_the_plugin() {
+        let dir = tempfile::tempdir().unwrap();
+        let st = state(dir.path());
+        let count = counting_plugin(&st, dir.path(), "fail");
+        let errors = concurrent_connects(&st).await;
+        assert!(
+            errors.iter().all(|e| e.key == "errors.exec.failed"),
+            "{errors:?}"
+        );
+        assert_eq!(std::fs::read_to_string(&count).unwrap().lines().count(), 1);
+        // Следующее подключение после неудачи — новая попытка.
+        let e = st.connected("e", false).await.err().unwrap();
+        assert_eq!(e.key, "errors.exec.failed");
+        assert_eq!(std::fs::read_to_string(&count).unwrap().lines().count(), 2);
+    }
+
+    #[cfg(unix)]
+    #[tokio::test]
+    async fn cancelling_the_plugin_cancels_every_waiter() {
+        let dir = tempfile::tempdir().unwrap();
+        let st = state(dir.path());
+        let count = counting_plugin(&st, dir.path(), "wait");
+        let waiting = tokio::spawn({
+            let st = st.clone();
+            async move { concurrent_connects(&st).await }
+        });
+        // Плагин запущен (записал строку) — отменяем его ожидание.
+        let started = async {
+            while !count.exists() {
+                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
+            }
+        };
+        tokio::time::timeout(std::time::Duration::from_secs(10), started)
+            .await
+            .expect("плагин запущен");
+        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
+        assert!(st.logins.cancel("e"), "ожидание плагина отменяется");
+        let errors = waiting.await.unwrap();
+        assert!(
+            errors.iter().all(|e| e.key == "errors.oauth.cancelled"),
+            "{errors:?}"
+        );
+        assert_eq!(std::fs::read_to_string(&count).unwrap().lines().count(), 1);
+    }
+
     #[test]
     fn delete_connection_removes_all_secrets() {
         let dir = tempfile::tempdir().unwrap();
diff --git a/src/api/client.ts b/src/api/client.ts
index 2757241..dc7f377 100644
--- a/src/api/client.ts
+++ b/src/api/client.ts
@@ -52,8 +52,10 @@ export const api = {
   setConnectionToken: (connectionId: string, token: string) =>
     call<void>("set_connection_token", { connectionId, token }),
   startOauthLogin: (connectionId: string) => call<ConnectionInfo>("start_oauth_login", { connectionId }),
-  /** Отменяет вход через браузер или ожидание exec-плагина. */
+  /** Отменяет вход через браузер или ожидание exec-плагина при подключении. */
   cancelOauthLogin: (connectionId: string) => call<boolean>("cancel_oauth_login", { connectionId }),
+  /** Отменяет ожидание exec-плагина при проверке подключения в форме; подключение вкладки не трогает. */
+  cancelConnectionTest: (connectionId: string) => call<boolean>("cancel_connection_test", { connectionId }),
   oauthTokenPage: (connectionId: string) => call<string>("oauth_token_page", { connectionId }),
   resolveExecCommand: (command: string) => call<string>("resolve_exec_command", { command }),
   previewKubeconfigImport: (path: string | null) => call<KubeconfigPreview>("preview_kubeconfig_import", { path }),
diff --git a/src/components/connections/ConnectionForm.tsx b/src/components/connections/ConnectionForm.tsx
index 651aa40..2a8c8a5 100644
--- a/src/components/connections/ConnectionForm.tsx
+++ b/src/components/connections/ConnectionForm.tsx
@@ -403,7 +403,7 @@ export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile;
             type="button"
             onClick={() => {
               // Сбой отмены не показываем: ожидание закончится само, а кнопка остаётся для повторной отмены.
-              api.cancelOauthLogin(profile.id).catch(() => undefined);
+              api.cancelConnectionTest(profile.id).catch(() => undefined);
             }}
           >
             {t("common.cancel")}
````

<!-- block: locales t01-locales step=1.3 -->
````json
{
  "en": {
    "messages": {
      "connectionTest": {
        "authOkGroups": "Signed in as {{user}}, groups: {{groups}}"
      }
    }
  },
  "ru": {
    "messages": {
      "connectionTest": {
        "authOkGroups": "Вход выполнен: {{user}}, группы: {{groups}}"
      }
    }
  }
}
````

- [ ] **Step 4: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 265 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `cargo deny --manifest-path src-tauri/Cargo.toml --config deny.toml check licenses advisories`  
Expected: PASS — `advisories ok, licenses ok`

Run: `cargo tree --manifest-path src-tauri/Cargo.toml -i aws-lc-rs`  
Expected: ошибка `did not match any packages` — `aws-lc-rs` в дереве нет (то же для `openssl-sys`)

Run: `pnpm test`  
Expected: PASS — `Test Files 32 passed`, `Tests 216 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 133`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

Стенд kind (если поднят, см. `dev/kind/README.md`; `KUBECONFIG` — отдельный файл, не `~/.kube/config`):  
Run: `GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --test it_connect`  
Expected: PASS — `test result: ok. 4 passed; 0 failed`

- [ ] **Step 5: Commit.**

```bash
git add \
  src-tauri/Cargo.lock \
  src-tauri/Cargo.toml \
  src-tauri/src/auth/exec.rs \
  src-tauri/src/auth/mod.rs \
  src-tauri/src/commands/auth.rs \
  src-tauri/src/commands/connections.rs \
  src-tauri/src/commands/folders.rs \
  src-tauri/src/k8s/connectivity.rs \
  src-tauri/src/k8s/pool.rs \
  src-tauri/src/lib.rs \
  src-tauri/src/state.rs \
  src-tauri/tests/it_connect.rs \
  src/api/client.ts \
  src/components/connections/ConnectionForm.test.tsx \
  src/components/connections/ConnectionForm.tsx \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "fix(connect): общий итог подключения для ожидающих, свой ключ проверки, группа процессов exec-плагина, группы на шаге «Вход»" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: Форма подключения: значения секретов exec-плагина скрыты до «Показать»

**Files:**
- Modify: `docs/guide/en/connections.md`, `docs/guide/ru/connections.md`, `src/components/connections/ConnectionForm.test.tsx`, `src/components/connections/ConnectionForm.tsx`, `src/components/connections/exec.test.ts`, `src/components/connections/exec.ts`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: O1a: `components/connections/exec.ts` (`isSecretName`, `launchLine`), `ConnectionForm`.
- Produces: `exec.ts`: `maskArgs(args) -> string[]`, `maskEnv(env) -> ExecEnv[]`, `hasSecretValues(exec) -> boolean`; ключи `connections.form.exec.secretsHidden`, `connections.form.exec.showSecrets`.

Долг O1a (итоговое ревью, m4): в полях «Аргументы» и «Переменные окружения» формы значения секретных аргументов и переменных показывались открытым текстом. Теперь, если у сохранённого exec-плагина есть такие значения, поля показывают их как `••••` и доступны только для чтения, пока не нажата «Показать» — тогда видны и правятся настоящие. Маскировка — те же правила, что у строки запуска и у Rust (`maskArgs` и `maskEnv` в `exec.ts`, `launchLine` теперь построена на них). Глава «Подключения» справки говорит о скрытых полях.

- [ ] **Step 1: Тесты.**

<!-- block: patch t02-tests step=2.1 -->
````diff
diff --git a/src/components/connections/ConnectionForm.test.tsx b/src/components/connections/ConnectionForm.test.tsx
index e9c17d4..53da547 100644
--- a/src/components/connections/ConnectionForm.test.tsx
+++ b/src/components/connections/ConnectionForm.test.tsx
@@ -279,6 +279,40 @@ describe("ConnectionForm", () => {
     expect(api.cancelConnectionTest).toHaveBeenCalledWith("e1");
   });
 
+  it("exec-плагин: значения секретов скрыты до «Показать», поля до этого только для чтения", async () => {
+    const withSecrets: ConnectionProfile = {
+      ...newProfile(),
+      id: "e2",
+      name: "oidc",
+      auth: {
+        method: "exec",
+        command: "/opt/homebrew/bin/kubectl",
+        args: ["oidc-login", "get-token", "--oidc-client-secret=c1i3nt"],
+        env: [
+          { name: "AWS_PROFILE", value: "dev" },
+          { name: "AWS_SECRET_ACCESS_KEY", value: "s3cr3t" },
+        ],
+        apiVersion: "client.authentication.k8s.io/v1",
+        provideClusterInfo: false,
+        approved: "fp",
+      },
+    };
+    const user = userEvent.setup();
+    renderWithClient(<ConnectionForm initial={withSecrets} tabId="t" />);
+    const args = screen.getByLabelText("Аргументы (по одному в строке)");
+    const env = screen.getByLabelText("Переменные окружения (ИМЯ=значение, по одной в строке)");
+    expect(args).toHaveValue("oidc-login\nget-token\n--oidc-client-secret=••••");
+    expect(env).toHaveValue("AWS_PROFILE=dev\nAWS_SECRET_ACCESS_KEY=••••");
+    expect(args).toHaveAttribute("readonly");
+    expect(screen.queryByDisplayValue(/s3cr3t|c1i3nt/)).not.toBeInTheDocument();
+
+    await user.click(screen.getByRole("button", { name: "Показать" }));
+    expect(env).toHaveValue("AWS_PROFILE=dev\nAWS_SECRET_ACCESS_KEY=s3cr3t");
+    expect(args).toHaveValue("oidc-login\nget-token\n--oidc-client-secret=c1i3nt");
+    expect(args).not.toHaveAttribute("readonly");
+    expect(screen.queryByRole("button", { name: "Показать" })).not.toBeInTheDocument();
+  });
+
   it("удаление с подтверждением закрывает вкладки подключения", async () => {
     vi.mocked(api.deleteConnection).mockResolvedValue();
     useWorkspace.getState().open({ kind: "cluster", connectionId: "c1" });
diff --git a/src/components/connections/exec.test.ts b/src/components/connections/exec.test.ts
index f4adb3c..5316127 100644
--- a/src/components/connections/exec.test.ts
+++ b/src/components/connections/exec.test.ts
@@ -1,6 +1,6 @@
 import { describe, expect, it } from "vitest";
 import type { AuthConfig } from "../../api/types";
-import { formatArgs, formatEnv, isSecretName, launchKey, launchLine, needsExecConfirmation, newExec, parseArgs, parseEnv } from "./exec";
+import { formatArgs, formatEnv, hasSecretValues, isSecretName, launchKey, launchLine, maskArgs, maskEnv, needsExecConfirmation, newExec, parseArgs, parseEnv } from "./exec";
 
 const exec = (patch: Partial<ReturnType<typeof newExec>> = {}): AuthConfig => ({
   method: "exec",
@@ -76,6 +76,27 @@ describe("exec", () => {
     expect(line(["--password", ""])).toBe("plugin --password ''");
   });
 
+  it("поля формы: значения секретов скрыты без кавычек, остальное как есть", () => {
+    expect(maskArgs(["get-token", "--oidc-client-secret=abc", "--client-secret", "x y", "--cluster-name", "c"])).toEqual([
+      "get-token",
+      "--oidc-client-secret=••••",
+      "--client-secret",
+      "••••",
+      "--cluster-name",
+      "c",
+    ]);
+    expect(maskEnv([{ name: "AWS_PROFILE", value: "dev" }, { name: "AWS_SECRET_ACCESS_KEY", value: "s3cr3t" }, { name: "TOKEN", value: "" }])).toEqual([
+      { name: "AWS_PROFILE", value: "dev" },
+      { name: "AWS_SECRET_ACCESS_KEY", value: "••••" },
+      { name: "TOKEN", value: "" },
+    ]);
+    const plain = { ...newExec(), command: "plugin", args: ["get-token"], env: [{ name: "AWS_PROFILE", value: "dev" }] };
+    expect(hasSecretValues(plain)).toBe(false);
+    expect(hasSecretValues({ ...plain, args: ["--password=p"] })).toBe(true);
+    expect(hasSecretValues({ ...plain, env: [{ name: "API_KEY", value: "k" }] })).toBe(true);
+    expect(hasSecretValues({ ...plain, env: [{ name: "API_KEY", value: "" }] })).toBe(false);
+  });
+
   it("подтверждение нужно для новой, изменённой и неподтверждённой строки", () => {
     const approved = exec({ approved: "fp" });
     expect(needsExecConfirmation({ method: "token" }, exec())).toBe(true);
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/connections/ConnectionForm.test.tsx src/components/connections/exec.test.ts`  
Expected: FAIL — нет `maskArgs`, `maskEnv` и `hasSecretValues`; поля формы показывают значения секретов.

- [ ] **Step 3: Реализация.**

<!-- block: patch t02-src step=2.3 -->
````diff
diff --git a/docs/guide/en/connections.md b/docs/guide/en/connections.md
index c9b5705..159003f 100644
--- a/docs/guide/en/connections.md
+++ b/docs/guide/en/connections.md
@@ -48,7 +48,9 @@ contain a command too, so check it before you confirm the line.
 
 The form shows the exact launch line: the path, the arguments and the environment variables. A value is hidden
 (`••••`) when the name of the variable or option contains TOKEN, SECRET, PASS, KEY, CREDENTIAL, BEARER or COOKIE, for
-example `--client-secret x`, `--oidc-client-secret=x` or `AWS_SECRET_ACCESS_KEY`. Glafira runs the line only after you
+example `--client-secret x`, `--oidc-client-secret=x` or `AWS_SECRET_ACCESS_KEY`. The argument and variable fields
+hide such values too and stay read-only until you click [[ui:connections.form.exec.showSecrets]]. Glafira runs the
+line only after you
 tick [[ui:connections.form.exec.confirm]]. The confirmation is bound to the exact line: any change to the command, the
 arguments or the variables needs a new confirmation. In the form, a switch of the sign-in method also drops a tick you
 have set; a line that was saved confirmed and is unchanged stays confirmed. A connection that arrives from an export
diff --git a/docs/guide/ru/connections.md b/docs/guide/ru/connections.md
index 17bcf90..7f9b595 100644
--- a/docs/guide/ru/connections.md
+++ b/docs/guide/ru/connections.md
@@ -47,7 +47,9 @@ Glafira запускает плагин по полному пути, без о
 
 Форма показывает точную строку запуска: путь, аргументы и переменные окружения. Значение скрыто (`••••`), если в имени
 переменной или параметра есть TOKEN, SECRET, PASS, KEY, CREDENTIAL, BEARER или COOKIE, например `--client-secret x`,
-`--oidc-client-secret=x` или `AWS_SECRET_ACCESS_KEY`. Glafira запускает строку, только когда вы отметите
+`--oidc-client-secret=x` или `AWS_SECRET_ACCESS_KEY`. В полях аргументов и переменных такие значения тоже скрыты,
+а поля доступны только для чтения, пока вы не нажмёте [[ui:connections.form.exec.showSecrets]]. Glafira запускает
+строку, только когда вы отметите
 [[ui:connections.form.exec.confirm]]. Подтверждение привязано к точной строке: любое изменение команды, аргументов
 или переменных требует подтвердить заново. В форме смена способа входа снимает и поставленную галочку; сохранённая
 подтверждённая строка без правок остаётся подтверждённой. Подключение, которое приходит из файла экспорта как новое,
diff --git a/src/components/connections/ConnectionForm.tsx b/src/components/connections/ConnectionForm.tsx
index 2a8c8a5..f280a4b 100644
--- a/src/components/connections/ConnectionForm.tsx
+++ b/src/components/connections/ConnectionForm.tsx
@@ -7,7 +7,7 @@ import { tr } from "../../i18n/text";
 import { useWorkspace } from "../../state/workspace";
 import { ErrorBanner } from "../common/ErrorBanner";
 import { Hint } from "../common/Hint";
-import { formatArgs, formatEnv, launchKey, launchLine, needsExecConfirmation, newExec, parseArgs, parseEnv } from "./exec";
+import { formatArgs, formatEnv, hasSecretValues, launchKey, launchLine, maskArgs, maskEnv, needsExecConfirmation, newExec, parseArgs, parseEnv } from "./exec";
 import { formatNamespaces, parseNamespaces, withAuthMethod, withCaMode, withProxyMode } from "./profile";
 import { folderPaths } from "./tree/folderTree";
 
@@ -33,6 +33,8 @@ export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile;
   const [lastExec, setLastExec] = useState(initialExec);
   const [execArgs, setExecArgs] = useState(formatArgs(initialExec.args));
   const [execEnv, setExecEnv] = useState(formatEnv(initialExec.env));
+  // Значения секретных аргументов и переменных скрыты (поля только для чтения), пока не нажата «Показать».
+  const [secretsShown, setSecretsShown] = useState(() => !hasSecretValues(initialExec));
   // Подтверждение привязано к строке: хранится ключ подтверждённой строки. Правка строки не обнуляет ключ, а делает производный флаг ложным (ключ не совпадает); обнуляет его только смена способа входа.
   const [confirmedExec, setConfirmedExec] = useState<string | null>(null);
   const [report, setReport] = useState<TestReport | null>(null);
@@ -268,9 +270,31 @@ export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile;
               </button>
             </span>
             <label htmlFor="cf-exec-args">{t("connections.form.exec.args")}</label>
-            <textarea id="cf-exec-args" rows={3} placeholder="get-token" value={execArgs} onChange={(e) => setExecArgs(e.target.value)} />
+            <textarea
+              id="cf-exec-args"
+              rows={3}
+              placeholder="get-token"
+              readOnly={!secretsShown}
+              value={secretsShown ? execArgs : formatArgs(maskArgs(parseArgs(execArgs)))}
+              onChange={(e) => setExecArgs(e.target.value)}
+            />
             <label htmlFor="cf-exec-env">{t("connections.form.exec.env")}</label>
-            <textarea id="cf-exec-env" rows={2} placeholder="AWS_PROFILE=dev" value={execEnv} onChange={(e) => setExecEnv(e.target.value)} />
+            <textarea
+              id="cf-exec-env"
+              rows={2}
+              placeholder="AWS_PROFILE=dev"
+              readOnly={!secretsShown}
+              value={secretsShown ? execEnv : formatEnv(maskEnv(parseEnv(execEnv)))}
+              onChange={(e) => setExecEnv(e.target.value)}
+            />
+            {!secretsShown && (
+              <p className="muted full">
+                {t("connections.form.exec.secretsHidden")}{" "}
+                <button type="button" className="link" onClick={() => setSecretsShown(true)}>
+                  {t("connections.form.exec.showSecrets")}
+                </button>
+              </p>
+            )}
             <span className="field-label">
               <span>{t("connections.form.exec.line")}</span>
               <Hint k="hints.connections.execLine" label={t("connections.form.exec.line")} />
diff --git a/src/components/connections/exec.ts b/src/components/connections/exec.ts
index 19f70e4..277be6e 100644
--- a/src/components/connections/exec.ts
+++ b/src/components/connections/exec.ts
@@ -46,38 +46,52 @@ function quote(s: string): string {
   return s === "" || /[\s'"\\$`]/.test(s) ? `'${s.replace(/'/g, `'\\''`)}'` : s;
 }
 
+/** Чем заменяются скрытые значения секретов. */
+const MASK = "••••";
+
 /**
- * Аргументы для строки запуска, значения секретных ключей скрыты, как в Rust (`secret_args`).
- * Ключ — аргумент с `-`, чьё имя без ведущих дефисов и до первого `=` подходит под `isSecretName`.
- * `--имя=значение` показывается как `--имя=••••`; в паре `--имя значение` скрывается следующий
+ * Аргументы со скрытыми значениями секретных ключей, как в Rust (`secret_args`). Ключ — аргумент
+ * с `-`, чьё имя без ведущих дефисов и до первого `=` подходит под `isSecretName`.
+ * `--имя=значение` становится `--имя=••••`; в паре `--имя значение` скрывается следующий
  * элемент, каким бы он ни был; ключ без значения в конце списка ничего не скрывает. Пустое
  * значение не скрывается.
  */
-function maskedArgs(args: string[]): string[] {
+export function maskArgs(args: string[]): string[] {
   const out: string[] = [];
   for (let i = 0; i < args.length; i++) {
     const a = args[i];
     const eq = a.indexOf("=");
     const secret = a.startsWith("-") && isSecretName((eq < 0 ? a : a.slice(0, eq)).replace(/^-+/, ""));
     if (!secret) {
-      out.push(quote(a));
+      out.push(a);
     } else if (eq >= 0) {
-      // Имя с пробелом (`--client secret=x`) квотируется вместе с маской, как и открытый аргумент.
-      out.push(a.length > eq + 1 ? quote(`${a.slice(0, eq + 1)}••••`) : quote(a));
+      out.push(a.length > eq + 1 ? `${a.slice(0, eq + 1)}${MASK}` : a);
     } else if (i + 1 < args.length) {
       const value = args[++i];
-      out.push(quote(a), value ? "••••" : quote(value));
+      out.push(a, value ? MASK : value);
     } else {
-      out.push(quote(a));
+      out.push(a);
     }
   }
   return out;
 }
 
+/** Переменные окружения со скрытыми значениями секретных имён (пустое значение не скрывается). */
+export function maskEnv(env: ExecEnv[]): ExecEnv[] {
+  return env.map((v) => (isSecretName(v.name) && v.value ? { ...v, value: MASK } : v));
+}
+
+/** Есть ли у плагина значения, которые форма скрывает до «Показать». */
+export function hasSecretValues(e: ExecAuth): boolean {
+  const masked = maskArgs(e.args);
+  return masked.some((a, i) => a !== e.args[i]) || maskEnv(e.env).some((v, i) => v.value !== e.env[i].value);
+}
+
 /** Строка запуска для подтверждения: переменные и значения секретных ключей скрыты, путь к команде и аргументы. */
 export function launchLine(e: ExecAuth): string {
-  const env = e.env.map((v) => `${v.name}=${isSecretName(v.name) && v.value ? "••••" : quote(v.value)}`);
-  return [...env, quote(e.command), ...maskedArgs(e.args)].join(" ");
+  // Аргумент с пробелом квотируется и замаскированным (`'--client secret=••••'`), как открытый.
+  const env = maskEnv(e.env).map((v) => `${v.name}=${quote(v.value)}`);
+  return [...env, quote(e.command), ...maskArgs(e.args).map(quote)].join(" ");
 }
 
 /**
````

<!-- block: locales t02-locales step=2.3 -->
````json
{
  "en": {
    "connections": {
      "form": {
        "exec": {
          "secretsHidden": "Values of secret arguments and variables are hidden.",
          "showSecrets": "Show"
        }
      }
    }
  },
  "ru": {
    "connections": {
      "form": {
        "exec": {
          "secretsHidden": "Значения секретных аргументов и переменных скрыты.",
          "showSecrets": "Показать"
        }
      }
    }
  }
}
````

- [ ] **Step 4: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 32 passed`, `Tests 218 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 133`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 5: Commit.**

```bash
git add \
  docs/guide/en/connections.md \
  docs/guide/ru/connections.md \
  src/components/connections/ConnectionForm.test.tsx \
  src/components/connections/ConnectionForm.tsx \
  src/components/connections/exec.test.ts \
  src/components/connections/exec.ts \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "fix(ui): значения секретов exec-плагина в полях формы скрыты до «Показать»" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: Чтение кластера через `ReadClient`: страницы списков, сессии watch и логов, пакеты

**Files:**
- Create: `src-tauri/src/commands/sessions.rs`, `src-tauri/src/k8s/read.rs`, `src-tauri/src/sessions.rs`
- Modify: `src-tauri/Cargo.toml`, `src-tauri/src/commands/connections.rs`, `src-tauri/src/commands/mod.rs`, `src-tauri/src/k8s/mod.rs`, `src-tauri/src/k8s/pool.rs`, `src-tauri/src/k8s/testing.rs`, `src-tauri/src/lib.rs`, `src-tauri/src/state.rs`

**Interfaces:**
- Consumes: задача 1; O1a: `k8s::cluster::timed`, `k8s::pool::Connected`, `AppState`.
- Produces: `k8s::read::{ReadClient::{new, timeout, list<K>(ns?, &ListParams), list_cluster<K>, list_dynamic(&ApiResource, ns?, ..), get<K>(ns, name), get_opt<K>, get_cluster<K>, get_dynamic, get_text(path), watch<K>(ns, watcher::Config), watch_dynamic, log_stream(ns, pod, &LogParams)}, Listed<K> { items, truncated }, Listed::map, PAGE_SIZE = 500, MAX_ITEMS = 10_000, by_labels(selector?)}`; `Connected::reader() -> ReadClient`; `sessions::{Sessions::{register(connection_id) -> (String, CancellationToken), stop, stop_for, remove, len, is_empty}, SessionGuard { sessions, id }, Sink<T>::send(T) -> bool (Channel<T> и, в тестах, Collect<T>), Batch<T>::{push, is_due, take, is_empty}, MAX_BATCH = 200, BATCH_WINDOW = 100 мс}`; `AppState::{sessions, read(id, f)}`; `k8s::testing::mock_raw(handler -> (u16, Vec<u8>))`; команда `stop_session(sessionId) -> bool`; зависимости `kube` с фичей `runtime`, `tokio-util`, `futures`.

Спец. §4.1. Всё чтение O1b идёт через `ReadClient` — newtype без доступа к внутреннему `kube::Client`: только get, list, watch (`kube::runtime::watcher`), логи и GET отдельных путей API-сервера (`/readyz?verbose`), каждый запрос — с таймаутом профиля. Им же будет пользоваться MCP-шлюз (O2). Списки читаются страницами по 500 (`limit` / `continue`, спец. §6.7) и не больше 10 000 объектов (`truncated`). `AppState::read` — общий путь команд: клиент из пула (или подключение), чтение, сброс клиента при ошибке учётных данных или TLS. Длительные операции — сессии: id, `CancellationToken`, снятие с учёта при завершении (`SessionGuard`); отключение и удаление подключения останавливают его сессии. Данные уходят во фронтенд каналом пакетами — не больше 200 элементов или раз в 100 мс (`Batch`). Мок API-сервера получает вариант с телом как есть (`mock_raw`) — для текста `/readyz` и строк логов.

- [ ] **Step 1: Тесты.**

<!-- block: file t03-src-tauri-src-k8s-read-rs-tests step=3.1 path=src-tauri/src/k8s/read.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::k8s::testing::{mock_client, mock_raw, status};
    use k8s_openapi::api::core::v1::{ConfigMap, Node, Pod};

    fn reader(client: kube::Client) -> ReadClient {
        ReadClient::new(client, Duration::from_secs(5))
    }

    fn pods(names: &[&str], next: Option<&str>) -> (u16, serde_json::Value) {
        let items: Vec<_> = names
            .iter()
            .map(|n| serde_json::json!({"metadata": {"name": n, "namespace": "team-a"}}))
            .collect();
        (
            200,
            serde_json::json!({"apiVersion": "v1", "kind": "PodList",
                "metadata": {"continue": next.unwrap_or("")}, "items": items}),
        )
    }

    fn names(l: &Listed<Pod>) -> Vec<String> {
        l.items
            .iter()
            .map(|p| p.metadata.name.clone().unwrap())
            .collect()
    }

    #[tokio::test]
    async fn list_reads_every_page() {
        let (client, server) = mock_client(|s| {
            if s.uri.contains("continue=t1") {
                pods(&["c"], None)
            } else {
                pods(&["a", "b"], Some("t1"))
            }
        });
        let l: Listed<Pod> = reader(client)
            .list(Some("team-a"), &by_labels(Some("app=web")))
            .await
            .unwrap();
        assert_eq!(names(&l), ["a", "b", "c"]);
        assert!(!l.truncated);
        let seen = server.await.unwrap();
        assert_eq!(seen.len(), 2);
        for s in &seen {
            assert_eq!(s.path(), "/api/v1/namespaces/team-a/pods");
            assert!(s.uri.contains("limit=500"), "{}", s.uri);
            assert!(s.uri.contains("labelSelector=app%3Dweb"), "{}", s.uri);
        }
    }

    #[tokio::test]
    async fn list_stops_at_the_limit() {
        let (client, server) = mock_client(|s| {
            if s.uri.contains("continue=t1") {
                pods(&["c", "d"], Some("t2"))
            } else {
                pods(&["a", "b"], Some("t1"))
            }
        });
        let api: Api<Pod> = Api::all(client.clone());
        let l = reader(client)
            .pages(&api, &ListParams::default(), 3)
            .await
            .unwrap();
        assert_eq!(names(&l), ["a", "b", "c"]);
        assert!(l.truncated);
        drop(api);
        assert_eq!(server.await.unwrap().len(), 2, "третью страницу не читали");
        let (client, _) = mock_client(|_| pods(&["a", "b", "c"], None));
        let api: Api<Pod> = Api::all(client.clone());
        let l = reader(client)
            .pages(&api, &ListParams::default(), 3)
            .await
            .unwrap();
        assert!(!l.truncated, "ровно столько, сколько можно");
    }

    #[tokio::test]
    async fn cluster_lists_and_missing_objects() {
        let (client, server) = mock_client(|s| match s.path() {
            "/api/v1/nodes" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "NodeList", "metadata": {},
                    "items": [{"metadata": {"name": "n1"}}]}),
            ),
            _ => status(404, "NotFound", "configmaps \"x\" not found"),
        });
        let r = reader(client);
        let nodes: Listed<Node> = r.list_cluster(&ListParams::default()).await.unwrap();
        assert_eq!(nodes.items.len(), 1);
        assert!(
            r.get_opt::<ConfigMap>("team-a", "x")
                .await
                .unwrap()
                .is_none()
        );
        let e = r.get::<ConfigMap>("team-a", "x").await.unwrap_err();
        assert_eq!(e.code, ErrorCode::NotFound);
        drop(r);
        let seen = server.await.unwrap();
        assert_eq!(seen[1].path(), "/api/v1/namespaces/team-a/configmaps/x");
    }

    #[tokio::test]
    async fn dynamic_objects_of_a_namespace_and_of_all() {
        let ar = ApiResource::from_gvk_with_plural(
            &kube::api::GroupVersionKind::gvk("route.openshift.io", "v1", "Route"),
            "routes",
        );
        let (client, server) = mock_client(|_| {
            (
                200,
                serde_json::json!({"apiVersion": "route.openshift.io/v1", "kind": "RouteList",
                    "metadata": {}, "items": [{"metadata": {"name": "web"}}]}),
            )
        });
        let r = reader(client);
        assert_eq!(
            r.list_dynamic(&ar, Some("team-a"), &ListParams::default())
                .await
                .unwrap()
                .items
                .len(),
            1
        );
        r.list_dynamic(&ar, None, &ListParams::default())
            .await
            .unwrap();
        drop(r);
        let seen = server.await.unwrap();
        assert_eq!(
            seen[0].path(),
            "/apis/route.openshift.io/v1/namespaces/team-a/routes"
        );
        assert_eq!(seen[1].path(), "/apis/route.openshift.io/v1/routes");
    }

    #[tokio::test]
    async fn text_of_a_path_and_the_text_of_its_error() {
        let (client, _) = mock_raw(|s| match s.uri.as_str() {
            "/readyz?verbose" => (200, b"[+]ping ok\nreadyz check passed\n".to_vec()),
            _ => (
                500,
                b"[+]ping ok\n[-]etcd failed: reason withheld\n".to_vec(),
            ),
        });
        let r = reader(client);
        assert!(
            r.get_text("/readyz?verbose")
                .await
                .unwrap()
                .contains("passed")
        );
        let e = r.get_text("/livez?verbose").await.unwrap_err();
        assert_eq!(e.code, ErrorCode::ConnectionFailed);
        assert!(e.params["message"].contains("[-]etcd failed"), "{e:?}");
    }
}
````

<!-- block: file t03-src-tauri-src-sessions-rs-tests step=3.1 path=src-tauri/src/sessions.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_stop_and_remove() {
        let s = Sessions::default();
        let (id, cancel) = s.register("c");
        assert_eq!(s.len(), 1);
        assert!(s.stop(&id));
        assert!(cancel.is_cancelled());
        assert!(!s.stop(&id), "уже остановлена");
        let (id, cancel) = s.register("c");
        s.remove(&id);
        assert!(s.is_empty());
        assert!(!cancel.is_cancelled(), "завершилась сама — не отменяем");
    }

    #[test]
    fn stopping_a_connection_stops_only_its_sessions() {
        let s = Sessions::default();
        let (_, a1) = s.register("a");
        let (_, a2) = s.register("a");
        let (_, b) = s.register("b");
        assert_eq!(s.stop_for("a"), 2);
        assert!(a1.is_cancelled() && a2.is_cancelled());
        assert!(!b.is_cancelled());
        assert_eq!(s.len(), 1);
        assert_eq!(s.stop_for("missing"), 0);
    }

    #[test]
    fn guard_removes_the_finished_session() {
        let s = Arc::new(Sessions::default());
        let (id, _) = s.register("a");
        drop(SessionGuard {
            sessions: s.clone(),
            id,
        });
        assert!(s.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn batches_are_full_or_due() {
        let mut b = Batch::default();
        assert!(!b.is_due() && b.is_empty());
        for i in 0..MAX_BATCH - 1 {
            assert!(b.push(i).is_none());
        }
        assert_eq!(b.push(0).map(|v| v.len()), Some(MAX_BATCH), "полный пакет");
        assert!(b.is_empty() && !b.is_due());
        b.push(1);
        tokio::time::advance(Duration::from_millis(50)).await;
        assert!(!b.is_due());
        tokio::time::advance(Duration::from_millis(50)).await;
        assert!(b.is_due());
        assert_eq!(b.take(), vec![1]);
        assert!(!b.is_due());
    }
}
````

- [ ] **Step 2: Реализация.**

<!-- block: file t03-src-tauri-src-commands-sessions-rs step=3.2 path=src-tauri/src/commands/sessions.rs -->
````rust
use tauri::State;

use crate::error::AppResult;
use crate::state::AppState;

/// Останавливает сессию watch или логов. `false` — она уже завершилась.
#[tauri::command]
pub fn stop_session(state: State<'_, AppState>, session_id: String) -> AppResult<bool> {
    Ok(state.sessions.stop(&session_id))
}
````

<!-- block: file t03-src-tauri-src-k8s-read-rs-impl step=3.2 path=src-tauri/src/k8s/read.rs part=impl -->
````rust
//! Чтение кластера (спец. §4.1): только get, list, watch, логи и GET отдельных путей API-сервера.
//! Внутренний `kube::Client` наружу не отдаётся — через `ReadClient` кластер не изменить. Им же
//! будет пользоваться MCP-шлюз (этап O2). Каждый запрос ограничен таймаутом профиля; списки
//! читаются страницами (спец. §6.7).

use std::fmt::Debug;
use std::time::Duration;

use futures::{AsyncBufRead, Stream};
use k8s_openapi::{ClusterResourceScope, NamespaceResourceScope};
use kube::api::{Api, ApiResource, DynamicObject, ListParams, LogParams, ObjectList};
use kube::core::Resource;
use kube::runtime::watcher;
use serde::de::DeserializeOwned;

use crate::error::AppResult;
use crate::k8s::cluster::timed;

/// Сколько объектов на странице списка.
pub const PAGE_SIZE: u32 = 500;
/// Больше стольких объектов один список не читает: остальное — `truncated`.
pub const MAX_ITEMS: usize = 10_000;

/// Прочитанный список: объекты и признак, что прочитано не всё (`MAX_ITEMS`).
#[derive(Debug, Clone)]
pub struct Listed<K> {
    pub items: Vec<K>,
    pub truncated: bool,
}

impl<K> Listed<K> {
    pub fn map<T>(self, f: impl FnMut(K) -> T) -> Listed<T> {
        Listed {
            items: self.items.into_iter().map(f).collect(),
            truncated: self.truncated,
        }
    }
}

/// Клиент только для чтения поверх `kube::Client` подключения.
#[derive(Clone)]
pub struct ReadClient {
    client: kube::Client,
    timeout: Duration,
}

impl ReadClient {
    pub fn new(client: kube::Client, timeout: Duration) -> Self {
        Self { client, timeout }
    }

    /// Таймаут одного запроса (из профиля).
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Объекты в namespace (`Some`) или во всех namespace (`None`).
    pub async fn list<K>(
        &self,
        namespace: Option<&str>,
        params: &ListParams,
    ) -> AppResult<Listed<K>>
    where
        K: Resource<Scope = NamespaceResourceScope, DynamicType = ()>
            + Clone
            + DeserializeOwned
            + Debug,
    {
        let api = match namespace {
            Some(ns) => Api::namespaced(self.client.clone(), ns),
            None => Api::all(self.client.clone()),
        };
        self.pages(&api, params, MAX_ITEMS).await
    }

    /// Объекты уровня кластера (ноды, классы Ingress).
    pub async fn list_cluster<K>(&self, params: &ListParams) -> AppResult<Listed<K>>
    where
        K: Resource<Scope = ClusterResourceScope, DynamicType = ()>
            + Clone
            + DeserializeOwned
            + Debug,
    {
        self.pages(&Api::all(self.client.clone()), params, MAX_ITEMS)
            .await
    }

    /// Объекты вида, которого нет в `k8s-openapi` (OpenShift, Gateway API): в namespace или,
    /// при `None`, во всех namespace или на уровне кластера.
    pub async fn list_dynamic(
        &self,
        ar: &ApiResource,
        namespace: Option<&str>,
        params: &ListParams,
    ) -> AppResult<Listed<DynamicObject>> {
        self.pages(&self.dynamic(ar, namespace), params, MAX_ITEMS)
            .await
    }

    fn dynamic(&self, ar: &ApiResource, namespace: Option<&str>) -> Api<DynamicObject> {
        match namespace {
            Some(ns) => Api::namespaced_with(self.client.clone(), ns, ar),
            None => Api::all_with(self.client.clone(), ar),
        }
    }

    /// Страницы по `PAGE_SIZE`, пока сервер отдаёт `continue` и не набрано `max` объектов.
    async fn pages<K>(&self, api: &Api<K>, params: &ListParams, max: usize) -> AppResult<Listed<K>>
    where
        K: Resource + Clone + DeserializeOwned + Debug,
    {
        let mut items = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut page = params.clone().limit(PAGE_SIZE);
            if let Some(t) = &token {
                page = page.continue_token(t);
            }
            let list: ObjectList<K> = timed(self.timeout, api.list(&page)).await?;
            items.extend(list.items);
            token = list.metadata.continue_.filter(|t| !t.is_empty());
            if items.len() >= max {
                let truncated = items.len() > max || token.is_some();
                items.truncate(max);
                return Ok(Listed { items, truncated });
            }
            if token.is_none() {
                return Ok(Listed {
                    items,
                    truncated: false,
                });
            }
        }
    }

    pub async fn get<K>(&self, namespace: &str, name: &str) -> AppResult<K>
    where
        K: Resource<Scope = NamespaceResourceScope, DynamicType = ()>
            + Clone
            + DeserializeOwned
            + Debug,
    {
        let api: Api<K> = Api::namespaced(self.client.clone(), namespace);
        timed(self.timeout, api.get(name)).await
    }

    /// Объект или `None`, если его нет (404).
    pub async fn get_opt<K>(&self, namespace: &str, name: &str) -> AppResult<Option<K>>
    where
        K: Resource<Scope = NamespaceResourceScope, DynamicType = ()>
            + Clone
            + DeserializeOwned
            + Debug,
    {
        let api: Api<K> = Api::namespaced(self.client.clone(), namespace);
        timed(self.timeout, api.get_opt(name)).await
    }

    pub async fn get_cluster<K>(&self, name: &str) -> AppResult<K>
    where
        K: Resource<Scope = ClusterResourceScope, DynamicType = ()>
            + Clone
            + DeserializeOwned
            + Debug,
    {
        let api: Api<K> = Api::all(self.client.clone());
        timed(self.timeout, api.get(name)).await
    }

    pub async fn get_dynamic(
        &self,
        ar: &ApiResource,
        namespace: Option<&str>,
        name: &str,
    ) -> AppResult<DynamicObject> {
        timed(self.timeout, self.dynamic(ar, namespace).get(name)).await
    }

    /// Тело ответа на GET к пути API-сервера, например `/readyz?verbose`. Ответ с ошибкой
    /// приходит ошибкой, текст тела — в её сообщении.
    pub async fn get_text(&self, path_and_query: &str) -> AppResult<String> {
        let request = http::Request::get(path_and_query)
            .body(Vec::new())
            .map_err(|e| crate::error::AppError::internal_with(e.to_string()))?;
        timed(self.timeout, self.client.request_text(request)).await
    }

    /// Изменения объектов namespace (`kube::runtime::watcher`): список, затем watch; после
    /// разрыва watcher сам читает список заново. Таймаут к потоку не применяется.
    pub fn watch<K>(
        &self,
        namespace: &str,
        config: watcher::Config,
    ) -> impl Stream<Item = watcher::Result<watcher::Event<K>>> + Send + use<K>
    where
        K: Resource<Scope = NamespaceResourceScope, DynamicType = ()>
            + Clone
            + DeserializeOwned
            + Debug
            + Send
            + 'static,
    {
        watcher(Api::namespaced(self.client.clone(), namespace), config)
    }

    /// То же для вида без типа в `k8s-openapi`.
    pub fn watch_dynamic(
        &self,
        ar: &ApiResource,
        namespace: &str,
        config: watcher::Config,
    ) -> impl Stream<Item = watcher::Result<watcher::Event<DynamicObject>>> + Send + use<> {
        watcher(self.dynamic(ar, Some(namespace)), config)
    }

    /// Поток строк лога контейнера; таймаут — только на открытие потока.
    pub async fn log_stream(
        &self,
        namespace: &str,
        pod: &str,
        params: &LogParams,
    ) -> AppResult<impl AsyncBufRead + use<>> {
        let api: Api<k8s_openapi::api::core::v1::Pod> =
            Api::namespaced(self.client.clone(), namespace);
        timed(self.timeout, api.log_stream(pod, params)).await
    }
}

/// Пустой `ListParams` с селектором меток.
pub fn by_labels(selector: Option<&str>) -> ListParams {
    match selector.filter(|s| !s.is_empty()) {
        Some(s) => ListParams::default().labels(s),
        None => ListParams::default(),
    }
}
````

<!-- block: file t03-src-tauri-src-sessions-rs-impl step=3.2 path=src-tauri/src/sessions.rs part=impl -->
````rust
//! Длительные операции — watch открытых списков и логи (спец. §4.1): фоновые задачи tokio с id
//! сессии. Данные уходят во фронтенд каналом пакетами (не больше `MAX_BATCH` элементов или раз
//! в `BATCH_WINDOW`), отмена — `stop_session` через `CancellationToken`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::sync::MutexExt;

/// Идущие сессии по id.
#[derive(Default)]
pub struct Sessions {
    inner: Mutex<HashMap<String, Entry>>,
}

struct Entry {
    connection_id: String,
    cancel: CancellationToken,
}

impl Sessions {
    /// Новая сессия подключения: id и сигнал отмены.
    pub fn register(&self, connection_id: &str) -> (String, CancellationToken) {
        let id = uuid::Uuid::new_v4().to_string();
        let cancel = CancellationToken::new();
        self.inner.lock_safe().insert(
            id.clone(),
            Entry {
                connection_id: connection_id.to_string(),
                cancel: cancel.clone(),
            },
        );
        (id, cancel)
    }

    /// Останавливает сессию. `false` — её уже нет.
    pub fn stop(&self, id: &str) -> bool {
        match self.inner.lock_safe().remove(id) {
            Some(e) => {
                e.cancel.cancel();
                true
            }
            None => false,
        }
    }

    /// Останавливает все сессии подключения (отключение, удаление); возвращает их число.
    pub fn stop_for(&self, connection_id: &str) -> usize {
        let mut inner = self.inner.lock_safe();
        let ids: Vec<String> = inner
            .iter()
            .filter(|(_, e)| e.connection_id == connection_id)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &ids {
            if let Some(e) = inner.remove(id) {
                e.cancel.cancel();
            }
        }
        ids.len()
    }

    /// Снимает с учёта сессию, которая завершилась сама.
    pub fn remove(&self, id: &str) {
        self.inner.lock_safe().remove(id);
    }

    pub fn len(&self) -> usize {
        self.inner.lock_safe().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Снимает сессию с учёта, когда её задача завершилась любым путём.
pub struct SessionGuard {
    pub sessions: Arc<Sessions>,
    pub id: String,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.sessions.remove(&self.id);
    }
}

/// Куда сессия отправляет данные: канал Tauri или, в тестах, вектор. `false` — получателя
/// больше нет (окно закрыто), сессия завершается.
pub trait Sink<T>: Send + Sync + 'static {
    fn send(&self, item: T) -> bool;
}

impl<T: tauri::ipc::IpcResponse + Send + Sync + 'static> Sink<T> for tauri::ipc::Channel<T> {
    fn send(&self, item: T) -> bool {
        tauri::ipc::Channel::send(self, item).is_ok()
    }
}

/// Приёмник для тестов: копит всё, что отправила сессия.
#[cfg(test)]
#[derive(Clone)]
pub struct Collect<T>(pub Arc<Mutex<Vec<T>>>);

#[cfg(test)]
impl<T> Default for Collect<T> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }
}

#[cfg(test)]
impl<T: Clone> Collect<T> {
    pub fn items(&self) -> Vec<T> {
        self.0.lock_safe().clone()
    }
}

#[cfg(test)]
impl<T: Send + 'static> Sink<T> for Collect<T> {
    fn send(&self, item: T) -> bool {
        self.0.lock_safe().push(item);
        true
    }
}

/// Не больше стольких элементов в одном пакете.
pub const MAX_BATCH: usize = 200;
/// Не реже, чем раз в столько, уходит неполный пакет.
pub const BATCH_WINDOW: Duration = Duration::from_millis(100);

/// Накопитель пакета: полный пакет отдаётся сразу, неполный — когда прошло `BATCH_WINDOW` с его
/// первого элемента (`is_due`).
pub struct Batch<T> {
    items: Vec<T>,
    since: Option<Instant>,
}

impl<T> Default for Batch<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            since: None,
        }
    }
}

impl<T> Batch<T> {
    /// Добавляет элемент; если пакет заполнен — возвращает его.
    pub fn push(&mut self, item: T) -> Option<Vec<T>> {
        self.since.get_or_insert_with(Instant::now);
        self.items.push(item);
        (self.items.len() >= MAX_BATCH).then(|| self.take())
    }

    /// Пора отправить неполный пакет.
    pub fn is_due(&self) -> bool {
        self.since.is_some_and(|t| t.elapsed() >= BATCH_WINDOW)
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn take(&mut self) -> Vec<T> {
        self.since = None;
        std::mem::take(&mut self.items)
    }
}
````

<!-- block: patch t03-src step=3.2 -->
````diff
diff --git a/src-tauri/Cargo.toml b/src-tauri/Cargo.toml
index 5f54f9d..da8e51b 100644
--- a/src-tauri/Cargo.toml
+++ b/src-tauri/Cargo.toml
@@ -30,13 +30,15 @@ pem = "3"
 url = "2"
 uuid = { version = "1", features = ["v4"] }
 time = { version = "0.3", features = ["formatting", "parsing"] }
-kube = { version = "4.2", default-features = false, features = ["client", "rustls-tls", "ring", "http-proxy"] }
+kube = { version = "4.2", default-features = false, features = ["client", "runtime", "rustls-tls", "ring", "http-proxy"] }
 k8s-openapi = { version = "0.28", features = ["v1_32"] }
 http = "1"
 secrecy = "0.10"
 base64 = "0.22"
 regex = "1"
 tokio = { version = "1", features = ["rt", "macros", "time", "net", "io-util", "sync", "process"] }
+tokio-util = "0.7"
+futures = "0.3"
 reqwest = { version = "0.13", default-features = false, features = ["rustls-no-provider", "json", "form", "http2", "system-proxy"] }
 rustls = { version = "0.23", default-features = false, features = ["ring", "std", "tls12"] }
 sha2 = "0.11"
diff --git a/src-tauri/src/commands/connections.rs b/src-tauri/src/commands/connections.rs
index 815f3ee..32ef087 100644
--- a/src-tauri/src/commands/connections.rs
+++ b/src-tauri/src/commands/connections.rs
@@ -239,8 +239,10 @@ pub async fn connect(
     }
 }
 
+/// Отключение: клиент сбрасывается, сессии watch и логов подключения останавливаются.
 #[tauri::command]
 pub fn disconnect(state: State<'_, AppState>, connection_id: String) -> AppResult<()> {
+    state.sessions.stop_for(&connection_id);
     state.evict_connection(&connection_id);
     Ok(())
 }
diff --git a/src-tauri/src/commands/mod.rs b/src-tauri/src/commands/mod.rs
index 76f066b..c189876 100644
--- a/src-tauri/src/commands/mod.rs
+++ b/src-tauri/src/commands/mod.rs
@@ -3,6 +3,7 @@ pub mod auth;
 pub mod connections;
 pub mod folders;
 pub mod kubeconfig;
+pub mod sessions;
 pub mod settings;
 
 use crate::error::{AppResult, ErrorCode};
diff --git a/src-tauri/src/k8s/mod.rs b/src-tauri/src/k8s/mod.rs
index d27b691..aa3ee23 100644
--- a/src-tauri/src/k8s/mod.rs
+++ b/src-tauri/src/k8s/mod.rs
@@ -6,5 +6,6 @@ pub mod connectivity;
 pub mod errors;
 pub mod kubeconfig;
 pub mod pool;
+pub mod read;
 #[cfg(test)]
 pub(crate) mod testing;
diff --git a/src-tauri/src/k8s/pool.rs b/src-tauri/src/k8s/pool.rs
index 716df44..dd2af40 100644
--- a/src-tauri/src/k8s/pool.rs
+++ b/src-tauri/src/k8s/pool.rs
@@ -10,6 +10,7 @@ use crate::error::{AppError, AppResult};
 use crate::k8s::cluster::{ConnectionInfo, probe};
 use crate::k8s::config::{apply_exec_credential, build_kube_config, process_env, request_timeout};
 use crate::k8s::errors::from_kube;
+use crate::k8s::read::ReadClient;
 use crate::secrets::SecretStore;
 use crate::storage::profile::{AuthConfig, ConnectionProfile};
 use crate::sync::MutexExt;
@@ -24,6 +25,11 @@ pub struct Connected {
 }
 
 impl Connected {
+    /// Чтение кластера этим подключением.
+    pub fn reader(&self) -> ReadClient {
+        ReadClient::new(self.client.clone(), self.timeout)
+    }
+
     /// Учётные данные истекают в ближайшую минуту: пора запустить плагин снова.
     pub fn expiring(&self, now: SystemTime) -> bool {
         self.expires_at
diff --git a/src-tauri/src/k8s/testing.rs b/src-tauri/src/k8s/testing.rs
index 323abe1..67b3040 100644
--- a/src-tauri/src/k8s/testing.rs
+++ b/src-tauri/src/k8s/testing.rs
@@ -23,6 +23,17 @@ impl Seen {
 /// когда клиент удалён, и возвращает список увиденных запросов.
 pub fn mock_client(
     handler: impl Fn(&Seen) -> (u16, serde_json::Value) + Send + 'static,
+) -> (kube::Client, tokio::task::JoinHandle<Vec<Seen>>) {
+    mock_raw(move |s| {
+        let (code, json) = handler(s);
+        (code, serde_json::to_vec(&json).unwrap())
+    })
+}
+
+/// Как `mock_client`, но тело ответа — байты как есть (текст `/readyz`, строки лога, события
+/// watch по строке на событие).
+pub fn mock_raw(
+    handler: impl Fn(&Seen) -> (u16, Vec<u8>) + Send + 'static,
 ) -> (kube::Client, tokio::task::JoinHandle<Vec<Seen>>) {
     let (svc, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
     let task = tokio::spawn(async move {
@@ -41,12 +52,12 @@ pub fn mock_client(
                 serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
             };
             let s = Seen { method, uri, body };
-            let (code, json) = handler(&s);
+            let (code, bytes) = handler(&s);
             seen.push(s);
             send.send_response(
                 Response::builder()
                     .status(code)
-                    .body(Body::from(serde_json::to_vec(&json).unwrap()))
+                    .body(Body::from(bytes))
                     .unwrap(),
             );
         }
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index f20646c..3ae2d2a 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -10,6 +10,7 @@ pub mod platform;
 pub mod policy;
 pub mod product;
 pub mod secrets;
+pub mod sessions;
 pub mod settings;
 pub mod state;
 pub mod storage;
@@ -69,6 +70,7 @@ pub fn run() {
             commands::settings::get_language_settings,
             commands::settings::set_language,
             commands::settings::get_policy_status,
+            commands::sessions::stop_session,
         ])
         .run(tauri::generate_context!())
         .expect("error while building tauri application");
diff --git a/src-tauri/src/state.rs b/src-tauri/src/state.rs
index a688519..712797a 100644
--- a/src-tauri/src/state.rs
+++ b/src-tauri/src/state.rs
@@ -1,3 +1,4 @@
+use std::future::Future;
 use std::path::Path;
 use std::sync::Arc;
 use std::time::SystemTime;
@@ -20,6 +21,8 @@ pub struct AppState {
     pub pool: Arc<ClientPool>,
     /// Идущие входы: через браузер и запуском exec-плагина.
     pub logins: Arc<crate::auth::Logins>,
+    /// Сессии watch и логов.
+    pub sessions: Arc<crate::sessions::Sessions>,
 }
 
 impl AppState {
@@ -31,6 +34,7 @@ impl AppState {
             settings: Arc::new(crate::settings::SettingsStore::open(data_dir)),
             pool: Arc::new(ClientPool::default()),
             logins: Arc::new(crate::auth::Logins::default()),
+            sessions: Arc::new(crate::sessions::Sessions::default()),
         })
     }
 
@@ -79,6 +83,24 @@ impl AppState {
         Ok(c)
     }
 
+    /// Чтение кластера для команды: клиент из пула (или новое подключение) и `f` с ним. Ошибка
+    /// учётных данных или TLS сбрасывает клиент (`evict_if_broken`): следующее чтение подключится
+    /// заново.
+    pub async fn read<T, F, Fut>(&self, id: &str, f: F) -> AppResult<T>
+    where
+        F: FnOnce(Arc<Connected>) -> Fut,
+        Fut: Future<Output = AppResult<T>>,
+    {
+        let result = match self.connected(id, false).await {
+            Ok(c) => f(c).await,
+            Err(e) => Err(e),
+        };
+        if let Err(e) = &result {
+            self.evict_if_broken(id, e);
+        }
+        result
+    }
+
     /// Сбрасывает клиент, если ошибка значит, что с этими учётными данными или TLS он работать
     /// не будет: следующий вызов подключится заново и подхватит новый токен.
     pub fn evict_if_broken(&self, id: &str, e: &AppError) -> bool {
@@ -96,9 +118,11 @@ impl AppState {
         self.pool.evict(id);
     }
 
-    /// Удаляет подключение целиком: профиль, секреты и клиент. Блокирующий вызов (keychain).
+    /// Удаляет подключение целиком: профиль, секреты, клиент и сессии. Блокирующий вызов
+    /// (keychain).
     pub fn delete_connection_full(&self, id: &str) -> AppResult<()> {
         self.connections.delete(id)?;
+        self.sessions.stop_for(id);
         for field in crate::secrets::CONNECTION_SECRET_FIELDS {
             self.secrets
                 .delete(&crate::secrets::secret_key(id, field))?;
@@ -273,6 +297,25 @@ mod tests {
         }
     }
 
+    #[tokio::test]
+    async fn failed_read_of_an_unknown_connection_reports_not_found() {
+        let dir = tempfile::tempdir().unwrap();
+        let st = state(dir.path());
+        let e = st.read("nope", |_| async { Ok(()) }).await.unwrap_err();
+        assert_eq!(e.code, ErrorCode::NotFound);
+    }
+
+    #[test]
+    fn deleting_a_connection_stops_its_sessions() {
+        let dir = tempfile::tempdir().unwrap();
+        let st = state(dir.path());
+        st.connections.upsert(sample_profile("a")).unwrap();
+        let (_, cancel) = st.sessions.register("a");
+        st.delete_connection_full("a").unwrap();
+        assert!(cancel.is_cancelled());
+        assert!(st.sessions.is_empty());
+    }
+
     #[test]
     fn only_credential_errors_evict() {
         let dir = tempfile::tempdir().unwrap();
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 276 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `cargo deny --manifest-path src-tauri/Cargo.toml --config deny.toml check licenses advisories`  
Expected: PASS — `advisories ok, licenses ok`

Run: `cargo tree --manifest-path src-tauri/Cargo.toml -i aws-lc-rs`  
Expected: ошибка `did not match any packages` — `aws-lc-rs` в дереве нет (то же для `openssl-sys`)

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 133`

- [ ] **Step 4: Commit.**

```bash
git add \
  src-tauri/Cargo.lock \
  src-tauri/Cargo.toml \
  src-tauri/src/commands/connections.rs \
  src-tauri/src/commands/mod.rs \
  src-tauri/src/commands/sessions.rs \
  src-tauri/src/k8s/mod.rs \
  src-tauri/src/k8s/pool.rs \
  src-tauri/src/k8s/read.rs \
  src-tauri/src/k8s/testing.rs \
  src-tauri/src/lib.rs \
  src-tauri/src/sessions.rs \
  src-tauri/src/state.rs
git commit -s -m "feat(k8s): чтение кластера через ReadClient — страницы списков, сессии watch и логов, пакеты для фронтенда" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: Трейт `Platform`, обзор кластера и ноды, срок токена

**Files:**
- Create: `src-tauri/src/commands/cluster.rs`, `src-tauri/src/k8s/nodes.rs`, `src-tauri/src/k8s/openshift.rs`, `src-tauri/src/k8s/overview.rs`, `src-tauri/src/k8s/view.rs`, `src-tauri/src/platform/adapters.rs`
- Modify: `src-tauri/src/commands/mod.rs`, `src-tauri/src/k8s/cluster.rs`, `src-tauri/src/k8s/errors.rs`, `src-tauri/src/k8s/mod.rs`, `src-tauri/src/k8s/pool.rs`, `src-tauri/src/k8s/read.rs`, `src-tauri/src/lib.rs`, `src-tauri/src/platform/mod.rs`

**Interfaces:**
- Consumes: задача 3 (`ReadClient`, `AppState::read`); O1a: `platform::{PlatformKind, PlatformInfo, ApiGroups, detect}`, `k8s::cluster::{Capabilities, ConnectionInfo, list_project_names}`.
- Produces: `platform::adapters::{Platform::{overview(&ReadClient, &Capabilities) -> BoxFuture<Option<PlatformOverview>>, token_expiry(&ReadClient, token) -> BoxFuture<Option<i64>>}, adapter(PlatformKind) -> &'static dyn Platform, PlatformOverview::Openshift(OpenShiftOverview { cluster_version, infrastructure, console_url, operators, machine_config_pools }), OperatorSummary { total, problems }, cluster_operators(&ReadClient)}`; `k8s::openshift::{cluster_versions, cluster_operators, infrastructures, consoles, machine_config_pools, user_oauth_access_tokens, oauth_token_object_name, token_expires_at, ClusterVersionInfo, cluster_version_info, OperatorRow, operator_row, MachineConfigPoolRow, machine_config_pool_row, infrastructure_type, console_url}`; `k8s::view::{Part<T> { data?, error? }, ConditionView { type, status, reason?, message?, lastTransition? }, conditions(json), conditions_of(&Option<Vec<T>>), condition, condition_flag, seconds, created, rfc3339, name}`; `k8s::nodes::{NodeRow, NodeSummary, RoleCount, NodeDetails, node_row, summary, node_details, list_nodes, get_node}`; `k8s::overview::{ClusterOverview { platform, server_version, readyz, nodes, ingress_classes, gateway_classes?, projects, platform_details? }, Readyz { ok, failed, error? }, IngressClassRow, GatewayClassRow, ProjectCount { count, more }, failed_checks, readyz, ingress_classes, cluster_overview}`; `k8s::errors::not_allowed(verb, resource, namespace?)`; `ConnectionInfo.token_expires_at: Option<i64>`; `ReadClient::project_names(projects_api)`; `Listed: Serialize`; команды `get_cluster_overview`, `list_nodes`, `get_node(connectionId, name)`, `list_cluster_operators`.

Спец. §4.3, §5.2, §6.2. Первое поведение, которое зависит от платформы, — значит, появляется трейт `Platform` с адаптерами: `OpenShift` (ClusterVersion `version` — версия, канал, доступные обновления, условия `Failing` и `Progressing`; Infrastructure — тип; Console — адрес веб-консоли; ClusterOperators не в норме; MachineConfigPools; срок токена по объекту `UserOAuthAccessToken`, имя которого — `sha256~` и base64url SHA-256 токена) и `Kubernetes` (своей части нет). Без прав на ClusterVersion и ClusterOperators (`Capabilities`) их не запрашивают. Обзор кластера: `/readyz?verbose` (его читает любой пользователь; не готовый сервер отвечает 5xx со списком проверок — разбираются строки `[-]…`), ноды (готовые из всех и по ролям), классы Ingress (по умолчанию — аннотация `is-default-class`), классы Gateway (при Gateway API), число проектов, часть платформы — всё параллельно; что не читается, приходит причиной (`Part`, спец. §15). Срок токена считается при подключении и уходит во фронтенд в `ConnectionInfo.tokenExpiresAt`. Отдельная команда `get_capabilities` не нужна: права, группы API и платформу уже отдаёт `connect`.

- [ ] **Step 1: Тесты.**

<!-- block: file t04-src-tauri-src-k8s-nodes-rs-tests step=4.1 path=src-tauri/src/k8s/nodes.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::mock_client;

    fn nodes() -> Vec<Node> {
        let list: kube::api::ObjectList<Node> =
            serde_json::from_value(fixtures::node_list()).unwrap();
        list.items
    }

    #[test]
    fn rows_roles_problems_and_summary() {
        let rows: Vec<NodeRow> = nodes().iter().map(node_row).collect();
        assert_eq!(rows[0].roles, ["control-plane", "master"]);
        assert!(rows[0].ready && rows[0].problems.is_empty());
        assert_eq!(rows[0].internal_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(rows[0].cpu.as_deref(), Some("3500m"));
        assert_eq!(rows[0].taints, 2);
        assert_eq!(rows[0].created, Some(1_792_922_400));
        assert_eq!(rows[1].problems, ["MemoryPressure"]);
        assert_eq!(rows[2].problems, ["NotReady"]);
        assert!(rows[3].roles.is_empty());
        let s = summary(&rows);
        assert_eq!((s.total, s.ready), (4, 3));
        assert_eq!(
            s.roles,
            [
                RoleCount {
                    role: "control-plane".into(),
                    total: 1,
                    ready: 1
                },
                RoleCount {
                    role: "master".into(),
                    total: 1,
                    ready: 1
                },
                RoleCount {
                    role: "worker".into(),
                    total: 2,
                    ready: 1
                },
            ]
        );
    }

    #[test]
    fn details_of_a_node() {
        let d = node_details(&nodes()[1]);
        assert_eq!(d.node.name, "w1");
        assert_eq!(
            d.taints,
            [
                "node-role.kubernetes.io/master:NoSchedule",
                "dedicated=db:NoExecute"
            ]
        );
        assert_eq!(d.capacity["memory"], "16Gi");
        assert_eq!(d.addresses, ["InternalIP: 10.0.0.1", "Hostname: w1"]);
        assert_eq!(d.container_runtime.as_deref(), Some("cri-o://1.32"));
        assert_eq!(d.conditions.len(), 2);
    }

    #[tokio::test]
    async fn list_and_get_read_the_cluster() {
        let (client, server) = mock_client(|s| match s.path() {
            "/api/v1/nodes" => (200, fixtures::node_list()),
            _ => (200, fixtures::node("w1", &["worker"], "True", false)),
        });
        let r = ReadClient::new(client, std::time::Duration::from_secs(5));
        assert_eq!(list_nodes(&r).await.unwrap().items.len(), 4);
        assert_eq!(get_node(&r, "w1").await.unwrap().node.roles, ["worker"]);
        drop(r);
        let seen = server.await.unwrap();
        assert_eq!(seen[1].path(), "/api/v1/nodes/w1");
    }
}
````

<!-- block: file t04-src-tauri-src-k8s-openshift-rs-tests step=4.1 path=src-tauri/src/k8s/openshift.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;

    #[test]
    fn token_object_name_and_expiry() {
        assert_eq!(
            oauth_token_object_name("sha256~abc").as_deref(),
            Some("sha256~ungWv48Bz-pBQUDeXa4iI7ADYaOWF3qctBD_YfIAFa0")
        );
        assert_eq!(oauth_token_object_name("eyJhbGciOi.x.y"), None);
        let token = |expires_in: i64| {
            object(serde_json::json!({"apiVersion": "oauth.openshift.io/v1",
                "kind": "UserOAuthAccessToken",
                "metadata": {"name": "sha256~x", "creationTimestamp": "2026-10-25T10:00:00Z"},
                "expiresIn": expires_in}))
        };
        assert_eq!(
            token_expires_at(&token(86_400)),
            Some(1_792_922_400 + 86_400)
        );
        assert_eq!(token_expires_at(&token(0)), None, "бессрочный");
    }

    #[test]
    fn cluster_version_infrastructure_and_console() {
        let cv = cluster_version_info(&object(cluster_version()));
        assert_eq!(cv.version.as_deref(), Some("4.18.3"));
        assert_eq!(cv.channel.as_deref(), Some("stable-4.18"));
        assert_eq!(cv.available_updates, 2);
        assert_eq!(cv.failing.map(|c| c.status), Some("False".into()));
        assert_eq!(
            cv.progressing.and_then(|c| c.message),
            Some("Working towards 4.18.4".into())
        );
        let infra = object(serde_json::json!({"apiVersion": "config.openshift.io/v1",
            "kind": "Infrastructure", "metadata": {"name": "cluster"},
            "status": {"platform": "AWS", "platformStatus": {"type": "AWS"}}}));
        assert_eq!(infrastructure_type(&infra).as_deref(), Some("AWS"));
        let console = object(serde_json::json!({"apiVersion": "config.openshift.io/v1",
            "kind": "Console", "metadata": {"name": "cluster"},
            "status": {"consoleURL": "https://console-openshift-console.apps.c.example.com"}}));
        assert_eq!(
            console_url(&console).as_deref(),
            Some("https://console-openshift-console.apps.c.example.com")
        );
    }

    #[test]
    fn operators_and_pools() {
        let list: kube::api::ObjectList<DynamicObject> =
            serde_json::from_value(operators()).unwrap();
        let rows: Vec<_> = list.items.iter().map(operator_row).collect();
        assert_eq!(rows[0].version.as_deref(), Some("4.18.3"));
        assert!(!rows[0].has_problem());
        assert!(rows[1].has_problem());
        assert_eq!(
            rows[1].message.as_deref(),
            Some("Some ingresscontrollers are degraded")
        );
        assert_eq!(rows[2].available, Some(false));
        assert!(rows[2].has_problem());
        let pools: kube::api::ObjectList<DynamicObject> = serde_json::from_value(pools()).unwrap();
        let rows: Vec<_> = pools.items.iter().map(machine_config_pool_row).collect();
        assert!(!rows[0].updating && !rows[0].degraded);
        assert_eq!(
            (rows[1].updating, rows[1].machines, rows[1].updated),
            (true, 4, 2)
        );
    }
}
````

<!-- block: file t04-src-tauri-src-k8s-overview-rs-tests step=4.1 path=src-tauri/src/k8s/overview.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::cluster::Capabilities;
    use crate::k8s::testing::{Seen, mock_raw};
    use crate::platform::{ApiGroups, Discovery, detect};
    use std::time::Duration;

    fn info(caps: Capabilities, groups: ApiGroups) -> ConnectionInfo {
        ConnectionInfo {
            user: "u".into(),
            groups: vec![],
            server_version: "v1.32.5".into(),
            platform: detect(&Discovery {
                git_version: "v1.32.5".into(),
                ..Default::default()
            }),
            api_groups: groups,
            capabilities: caps,
            token_expires_at: None,
        }
    }

    fn json(v: serde_json::Value) -> (u16, Vec<u8>) {
        (200, serde_json::to_vec(&v).unwrap())
    }

    fn cluster(s: &Seen) -> (u16, Vec<u8>) {
        match s.path() {
            "/readyz" => (
                500,
                b"[+]ping ok\n[-]etcd failed: reason withheld\n[-]poststarthook/x failed\nreadyz check failed\n"
                    .to_vec(),
            ),
            "/api/v1/nodes" => json(crate::k8s::nodes::fixtures::node_list()),
            "/apis/networking.k8s.io/v1/ingressclasses" => json(serde_json::json!({
                "apiVersion": "networking.k8s.io/v1", "kind": "IngressClassList", "metadata": {},
                "items": [
                    {"metadata": {"name": "nginx", "annotations": {"ingressclass.kubernetes.io/is-default-class": "true"}},
                        "spec": {"controller": "k8s.io/ingress-nginx"}},
                    {"metadata": {"name": "traefik"}, "spec": {"controller": "traefik.io/ingress-controller"}}
                ]})),
            "/apis/gateway.networking.k8s.io/v1/gatewayclasses" => json(serde_json::json!({
                "apiVersion": "gateway.networking.k8s.io/v1", "kind": "GatewayClassList", "metadata": {},
                "items": [{"metadata": {"name": "cilium"}, "spec": {"controllerName": "io.cilium/gateway-controller"},
                    "status": {"conditions": [{"type": "Accepted", "status": "True"}]}}]})),
            "/api/v1/namespaces" => json(serde_json::json!({"apiVersion": "v1", "kind": "NamespaceList",
                "metadata": {}, "items": [{"metadata": {"name": "a"}}, {"metadata": {"name": "b"}}]})),
            other => (404, format!("{other} not found").into_bytes()),
        }
    }

    #[test]
    fn failed_checks_of_readyz() {
        assert!(failed_checks("[+]ping ok\nreadyz check passed").is_empty());
        assert_eq!(
            failed_checks("[+]ping ok\n[-]etcd failed: reason withheld\n[-]informer-sync failed"),
            ["etcd", "informer-sync"]
        );
        let many: String = (0..30).map(|i| format!("[-]c{i} failed\n")).collect();
        assert_eq!(failed_checks(&many).len(), 20);
    }

    #[tokio::test]
    async fn overview_of_kubernetes_with_rights() {
        let (client, _server) = mock_raw(cluster);
        let r = ReadClient::new(client, Duration::from_secs(5));
        let caps = Capabilities {
            list_projects: true,
            list_nodes: true,
            ..Default::default()
        };
        let groups = ApiGroups {
            gateway_api: true,
            ..Default::default()
        };
        let o = cluster_overview(&r, &info(caps, groups)).await;
        assert_eq!(o.platform.label, "Kubernetes 1.32");
        assert!(!o.readyz.ok);
        assert_eq!(o.readyz.failed, ["etcd", "poststarthook/x"]);
        assert!(o.readyz.error.is_none());
        assert_eq!(
            o.nodes.data.as_ref().map(|n| (n.total, n.ready)),
            Some((4, 3))
        );
        let classes = o.ingress_classes.data.unwrap();
        assert!(classes[0].default && !classes[1].default);
        let gw = o.gateway_classes.unwrap().data.unwrap();
        assert_eq!(gw[0].accepted, Some(true));
        assert_eq!(
            o.projects.data,
            Some(ProjectCount {
                count: 2,
                more: false
            })
        );
        assert!(o.platform_details.is_none());
    }

    #[tokio::test]
    async fn overview_without_rights_reads_only_what_is_allowed() {
        let (client, server) = mock_raw(cluster);
        let r = ReadClient::new(client, Duration::from_secs(5));
        let o = cluster_overview(&r, &info(Capabilities::default(), ApiGroups::default())).await;
        assert_eq!(o.nodes.error.unwrap().params["resource"], "nodes");
        assert_eq!(o.projects.error.unwrap().params["resource"], "namespaces");
        assert!(o.gateway_classes.is_none(), "Gateway API нет");
        drop(r);
        let seen = server.await.unwrap();
        assert!(
            !seen
                .iter()
                .any(|s| s.path() == "/api/v1/nodes" || s.path() == "/api/v1/namespaces")
        );
    }

    #[tokio::test]
    async fn api_server_that_does_not_answer() {
        let (client, _server) = mock_raw(|_| (403, b"forbidden".to_vec()));
        let r = ReadClient::new(client, Duration::from_secs(5));
        let z = readyz(&r).await;
        assert!(!z.ok && z.failed.is_empty());
        assert_eq!(z.error.unwrap().code, crate::error::ErrorCode::Forbidden);
    }
}
````

<!-- block: file t04-src-tauri-src-k8s-view-rs-tests step=4.1 path=src-tauri/src/k8s/view.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conditions_from_any_object() {
        let v = serde_json::json!([
            {"type": "Ready", "status": "True", "lastTransitionTime": "2026-10-25T10:00:00Z"},
            {"type": "MemoryPressure", "status": "False", "reason": "", "message": "ok"},
            {"status": "True"}
        ]);
        let c = conditions(&v);
        assert_eq!(c.len(), 2, "условие без типа пропускается");
        assert_eq!(c[0].last_transition, Some(1_792_922_400));
        assert!(c[0].is_true());
        assert_eq!(c[1].reason, None, "пустая причина — нет причины");
        assert_eq!(c[1].message.as_deref(), Some("ok"));
        assert_eq!(condition_flag(&c, "Ready"), Some(true));
        assert_eq!(condition_flag(&c, "MemoryPressure"), Some(false));
        assert_eq!(condition_flag(&c, "DiskPressure"), None);
        assert!(conditions(&serde_json::Value::Null).is_empty());
    }

    #[test]
    fn parts_serialize_data_or_error() {
        let ok: Part<u32> = Ok(3).into();
        assert_eq!(
            serde_json::to_value(&ok).unwrap(),
            serde_json::json!({"data": 3})
        );
        let err: Part<u32> = Err(AppError::internal_with("x")).into();
        let v = serde_json::to_value(&err).unwrap();
        assert_eq!(v["error"]["key"], "errors.internal");
        assert!(v.get("data").is_none());
    }

    #[test]
    fn times() {
        assert_eq!(rfc3339("1970-01-01T00:01:00Z"), Some(60));
        assert_eq!(rfc3339("вчера"), None);
        let meta = ObjectMeta {
            creation_timestamp: Some(Time("2026-10-25T10:00:00Z".parse().unwrap())),
            ..Default::default()
        };
        assert_eq!(created(&meta), Some(1_792_922_400));
    }
}
````

<!-- block: file t04-src-tauri-src-platform-adapters-rs-tests step=4.1 path=src-tauri/src/platform/adapters.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::openshift::fixtures;
    use crate::k8s::testing::{mock_client, status};
    use std::time::Duration;

    fn all_rights() -> Capabilities {
        Capabilities {
            list_projects: true,
            list_nodes: true,
            list_all_pods: true,
            read_cluster_operators: true,
            read_cluster_version: true,
        }
    }

    fn openshift_cluster(s: &crate::k8s::testing::Seen) -> (u16, serde_json::Value) {
        match s.path() {
            "/apis/config.openshift.io/v1/clusterversions/version" => {
                (200, fixtures::cluster_version())
            }
            "/apis/config.openshift.io/v1/clusteroperators" => (200, fixtures::operators()),
            "/apis/machineconfiguration.openshift.io/v1/machineconfigpools" => {
                (200, fixtures::pools())
            }
            "/apis/config.openshift.io/v1/infrastructures/cluster" => (
                200,
                serde_json::json!({"apiVersion": "config.openshift.io/v1", "kind": "Infrastructure",
                    "metadata": {"name": "cluster"}, "status": {"platformStatus": {"type": "VSphere"}}}),
            ),
            other => status(403, "Forbidden", &format!("{other} is forbidden")),
        }
    }

    fn reader(client: kube::Client) -> ReadClient {
        ReadClient::new(client, Duration::from_secs(5))
    }

    #[tokio::test]
    async fn openshift_overview_with_rights() {
        let (client, _server) = mock_client(openshift_cluster);
        let r = reader(client);
        let Some(PlatformOverview::Openshift(o)) = adapter(PlatformKind::Openshift)
            .overview(&r, &all_rights())
            .await
        else {
            panic!("у OpenShift есть своя часть обзора");
        };
        let cv = o.cluster_version.data.unwrap();
        assert_eq!(cv.available_updates, 2);
        assert_eq!(o.infrastructure.as_deref(), Some("VSphere"));
        assert_eq!(o.console_url, None, "консоль не читается — адреса нет");
        let ops = o.operators.data.unwrap();
        assert_eq!(ops.total, 3);
        let names: Vec<_> = ops.problems.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["ingress", "monitoring"]);
        assert_eq!(o.machine_config_pools.data.unwrap().len(), 2);
        let json = serde_json::to_value(PlatformOverview::Openshift(OpenShiftOverview {
            cluster_version: Part::err(not_allowed("get", "clusterversions", None)),
            infrastructure: None,
            console_url: None,
            operators: Part::ok(OperatorSummary {
                total: 0,
                problems: vec![],
            }),
            machine_config_pools: Part::ok(vec![]),
        }))
        .unwrap();
        assert_eq!(json["kind"], "openshift");
        assert_eq!(json["operators"]["data"]["total"], 0);
    }

    #[tokio::test]
    async fn openshift_overview_without_rights_does_not_ask() {
        let (client, server) = mock_client(openshift_cluster);
        let r = reader(client);
        let Some(PlatformOverview::Openshift(o)) = adapter(PlatformKind::Openshift)
            .overview(&r, &Capabilities::default())
            .await
        else {
            panic!("OpenShift");
        };
        assert_eq!(
            o.cluster_version.error.unwrap().key,
            "errors.k8s.forbiddenCluster"
        );
        assert_eq!(
            o.operators.error.unwrap().params["resource"],
            "clusteroperators"
        );
        drop(r);
        let seen = server.await.unwrap();
        assert!(
            !seen
                .iter()
                .any(|s| s.path().contains("clusteroperators")
                    || s.path().contains("clusterversions"))
        );
    }

    #[tokio::test]
    async fn kubernetes_has_no_platform_part_and_no_token_expiry() {
        let (client, server) = mock_client(openshift_cluster);
        let r = reader(client);
        let k = adapter(PlatformKind::Kubernetes);
        assert!(k.overview(&r, &all_rights()).await.is_none());
        assert_eq!(k.token_expiry(&r, "sha256~abc").await, None);
        drop(r);
        assert!(server.await.unwrap().is_empty(), "запросов нет");
    }

    #[tokio::test]
    async fn openshift_token_expiry_from_its_object() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis/oauth.openshift.io/v1/useroauthaccesstokens/sha256~ungWv48Bz-pBQUDeXa4iI7ADYaOWF3qctBD_YfIAFa0" => {
                (
                    200,
                    serde_json::json!({"apiVersion": "oauth.openshift.io/v1", "kind": "UserOAuthAccessToken",
                    "metadata": {"name": "sha256~x", "creationTimestamp": "2026-10-25T10:00:00Z"},
                    "expiresIn": 86400}),
                )
            }
            _ => status(404, "NotFound", "not found"),
        });
        let r = reader(client);
        let os = adapter(PlatformKind::Openshift);
        assert_eq!(
            os.token_expiry(&r, "sha256~abc").await,
            Some(1_792_922_400 + 86_400)
        );
        assert_eq!(
            os.token_expiry(&r, "sha256~other").await,
            None,
            "нет объекта"
        );
        assert_eq!(os.token_expiry(&r, "eyJhbGciOi.x.y").await, None);
        drop(r);
        assert_eq!(
            server.await.unwrap().len(),
            2,
            "токен не OAuth — не спрашиваем"
        );
    }
}
````

- [ ] **Step 2: Реализация.**

<!-- block: file t04-src-tauri-src-commands-cluster-rs step=4.2 path=src-tauri/src/commands/cluster.rs -->
````rust
use tauri::State;

use crate::error::AppResult;
use crate::k8s::nodes::{NodeDetails, NodeRow};
use crate::k8s::openshift::OperatorRow;
use crate::k8s::overview::ClusterOverview;
use crate::k8s::read::Listed;
use crate::state::AppState;

/// Обзор кластера (спец. §6.2).
#[tauri::command]
pub async fn get_cluster_overview(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<ClusterOverview> {
    state
        .read(&connection_id, |c| async move {
            Ok(crate::k8s::overview::cluster_overview(&c.reader(), &c.info).await)
        })
        .await
}

#[tauri::command]
pub async fn list_nodes(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<Listed<NodeRow>> {
    state
        .read(&connection_id, |c| async move {
            crate::k8s::nodes::list_nodes(&c.reader()).await
        })
        .await
}

#[tauri::command]
pub async fn get_node(
    state: State<'_, AppState>,
    connection_id: String,
    name: String,
) -> AppResult<NodeDetails> {
    state
        .read(&connection_id, |c| async move {
            crate::k8s::nodes::get_node(&c.reader(), &name).await
        })
        .await
}

/// Все ClusterOperators (OpenShift).
#[tauri::command]
pub async fn list_cluster_operators(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<Vec<OperatorRow>> {
    state
        .read(&connection_id, |c| async move {
            crate::platform::adapters::cluster_operators(&c.reader()).await
        })
        .await
}
````

<!-- block: file t04-src-tauri-src-k8s-nodes-rs-impl step=4.2 path=src-tauri/src/k8s/nodes.rs part=impl -->
````rust
//! Ноды кластера (спец. §6.1–6.2): список, сводка готовых по ролям и сведения о ноде.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Node;
use kube::api::ListParams;
use serde::Serialize;

use crate::error::AppResult;
use crate::k8s::read::{Listed, ReadClient};
use crate::k8s::view::{ConditionView, conditions_of, created, name};

/// Условия ноды, которые при `True` означают проблему.
const PRESSURE: &[&str] = &[
    "MemoryPressure",
    "DiskPressure",
    "PIDPressure",
    "NetworkUnavailable",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeRow {
    pub name: String,
    pub roles: Vec<String>,
    pub ready: bool,
    /// Условия-проблемы: давление на память, диск или PID, нет сети; `Ready` не `True`.
    pub problems: Vec<String>,
    pub unschedulable: bool,
    pub kubelet_version: Option<String>,
    pub internal_ip: Option<String>,
    /// Allocatable: CPU, память, поды.
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub pods: Option<String>,
    pub created: Option<i64>,
    pub taints: usize,
}

/// Роли ноды по меткам `node-role.kubernetes.io/<роль>` и `kubernetes.io/role`.
fn roles(n: &Node) -> Vec<String> {
    let mut roles: Vec<String> = n
        .metadata
        .labels
        .iter()
        .flatten()
        .filter_map(|(k, v)| match k.strip_prefix("node-role.kubernetes.io/") {
            Some(role) if !role.is_empty() => Some(role.to_string()),
            _ if k == "kubernetes.io/role" && !v.is_empty() => Some(v.clone()),
            _ => None,
        })
        .collect();
    roles.sort();
    roles.dedup();
    roles
}

pub fn node_row(n: &Node) -> NodeRow {
    let status = n.status.as_ref();
    let conds = conditions_of(&status.and_then(|s| s.conditions.clone()));
    let ready = conds.iter().any(|c| c.kind == "Ready" && c.is_true());
    let mut problems: Vec<String> = conds
        .iter()
        .filter(|c| PRESSURE.contains(&c.kind.as_str()) && c.is_true())
        .map(|c| c.kind.clone())
        .collect();
    if !ready {
        problems.insert(0, "NotReady".into());
    }
    let allocatable = |k: &str| {
        status
            .and_then(|s| s.allocatable.as_ref())
            .and_then(|a| a.get(k))
            .map(|q| q.0.clone())
    };
    NodeRow {
        name: name(&n.metadata),
        roles: roles(n),
        ready,
        problems,
        unschedulable: n.spec.as_ref().and_then(|s| s.unschedulable) == Some(true),
        kubelet_version: status
            .and_then(|s| s.node_info.as_ref())
            .map(|i| i.kubelet_version.clone()),
        internal_ip: status
            .and_then(|s| s.addresses.as_ref())
            .and_then(|a| a.iter().find(|a| a.type_ == "InternalIP"))
            .map(|a| a.address.clone()),
        cpu: allocatable("cpu"),
        memory: allocatable("memory"),
        pods: allocatable("pods"),
        created: created(&n.metadata),
        taints: n
            .spec
            .as_ref()
            .and_then(|s| s.taints.as_ref())
            .map_or(0, Vec::len),
    }
}

/// Готовые ноды из всех и по ролям (спец. §6.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSummary {
    pub total: usize,
    pub ready: usize,
    pub roles: Vec<RoleCount>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RoleCount {
    pub role: String,
    pub total: usize,
    pub ready: usize,
}

pub fn summary(rows: &[NodeRow]) -> NodeSummary {
    let mut by_role: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for r in rows {
        for role in &r.roles {
            let e = by_role.entry(role).or_default();
            e.0 += 1;
            e.1 += usize::from(r.ready);
        }
    }
    NodeSummary {
        total: rows.len(),
        ready: rows.iter().filter(|r| r.ready).count(),
        roles: by_role
            .into_iter()
            .map(|(role, (total, ready))| RoleCount {
                role: role.to_string(),
                total,
                ready,
            })
            .collect(),
    }
}

/// Сведения о ноде.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeDetails {
    pub node: NodeRow,
    pub conditions: Vec<ConditionView>,
    /// `ключ=значение:эффект`.
    pub taints: Vec<String>,
    pub labels: BTreeMap<String, String>,
    pub capacity: BTreeMap<String, String>,
    pub allocatable: BTreeMap<String, String>,
    /// `тип: адрес`.
    pub addresses: Vec<String>,
    pub os_image: Option<String>,
    pub kernel_version: Option<String>,
    pub container_runtime: Option<String>,
    pub architecture: Option<String>,
}

pub fn node_details(n: &Node) -> NodeDetails {
    let status = n.status.as_ref();
    let info = status.and_then(|s| s.node_info.as_ref());
    let quantities =
        |m: Option<&BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>>| {
            m.map(|m| m.iter().map(|(k, q)| (k.clone(), q.0.clone())).collect())
                .unwrap_or_default()
        };
    NodeDetails {
        node: node_row(n),
        conditions: conditions_of(&status.and_then(|s| s.conditions.clone())),
        taints: n
            .spec
            .as_ref()
            .and_then(|s| s.taints.as_ref())
            .map(|ts| {
                ts.iter()
                    .map(|t| match &t.value {
                        Some(v) if !v.is_empty() => format!("{}={}:{}", t.key, v, t.effect),
                        _ => format!("{}:{}", t.key, t.effect),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        labels: n.metadata.labels.clone().unwrap_or_default(),
        capacity: quantities(status.and_then(|s| s.capacity.as_ref())),
        allocatable: quantities(status.and_then(|s| s.allocatable.as_ref())),
        addresses: status
            .and_then(|s| s.addresses.as_ref())
            .map(|a| {
                a.iter()
                    .map(|a| format!("{}: {}", a.type_, a.address))
                    .collect()
            })
            .unwrap_or_default(),
        os_image: info.map(|i| i.os_image.clone()),
        kernel_version: info.map(|i| i.kernel_version.clone()),
        container_runtime: info.map(|i| i.container_runtime_version.clone()),
        architecture: info.map(|i| i.architecture.clone()),
    }
}

pub async fn list_nodes(r: &ReadClient) -> AppResult<Listed<NodeRow>> {
    let nodes: Listed<Node> = r.list_cluster(&ListParams::default()).await?;
    Ok(nodes.map(|n| node_row(&n)))
}

pub async fn get_node(r: &ReadClient, node: &str) -> AppResult<NodeDetails> {
    let n: Node = r.get_cluster(node).await?;
    Ok(node_details(&n))
}

#[cfg(test)]
pub(crate) mod fixtures {
    pub fn node(name: &str, roles: &[&str], ready: &str, pressure: bool) -> serde_json::Value {
        let mut labels = serde_json::Map::new();
        for r in roles {
            labels.insert(format!("node-role.kubernetes.io/{r}"), "".into());
        }
        labels.insert("kubernetes.io/os".into(), "linux".into());
        serde_json::json!({"metadata": {"name": name, "labels": labels,
                "creationTimestamp": "2026-10-25T10:00:00Z"},
            "spec": {"taints": [{"key": "node-role.kubernetes.io/master", "effect": "NoSchedule"},
                {"key": "dedicated", "value": "db", "effect": "NoExecute"}]},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": ready},
                    {"type": "MemoryPressure", "status": if pressure { "True" } else { "False" }}
                ],
                "addresses": [{"type": "InternalIP", "address": "10.0.0.1"},
                    {"type": "Hostname", "address": name}],
                "allocatable": {"cpu": "3500m", "memory": "15Gi", "pods": "250"},
                "capacity": {"cpu": "4", "memory": "16Gi", "pods": "250"},
                "nodeInfo": {"kubeletVersion": "v1.32.5", "osImage": "RHCOS", "kernelVersion": "5.14",
                    "containerRuntimeVersion": "cri-o://1.32", "architecture": "amd64",
                    "bootID": "", "kubeProxyVersion": "", "machineID": "", "operatingSystem": "linux",
                    "systemUUID": ""}}})
    }

    pub fn node_list() -> serde_json::Value {
        serde_json::json!({"apiVersion": "v1", "kind": "NodeList", "metadata": {}, "items": [
            node("m1", &["control-plane", "master"], "True", false),
            node("w1", &["worker"], "True", true),
            node("w2", &["worker"], "Unknown", false),
            node("x1", &[], "True", false)
        ]})
    }
}
````

<!-- block: file t04-src-tauri-src-k8s-openshift-rs-impl step=4.2 path=src-tauri/src/k8s/openshift.rs part=impl -->
````rust
//! Объекты OpenShift (спец. §2): своих типов в `k8s-openapi` у них нет, поэтому они читаются как
//! `DynamicObject`, а нужные поля берутся из JSON. Здесь — их `ApiResource` и разбор.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};
use serde::Serialize;
use sha2::Digest;

use crate::k8s::view::{ConditionView, conditions};

fn resource(group: &str, kind: &str, plural: &str) -> ApiResource {
    ApiResource::from_gvk_with_plural(&GroupVersionKind::gvk(group, "v1", kind), plural)
}

pub fn cluster_versions() -> ApiResource {
    resource("config.openshift.io", "ClusterVersion", "clusterversions")
}

pub fn cluster_operators() -> ApiResource {
    resource("config.openshift.io", "ClusterOperator", "clusteroperators")
}

pub fn infrastructures() -> ApiResource {
    resource("config.openshift.io", "Infrastructure", "infrastructures")
}

pub fn consoles() -> ApiResource {
    resource("config.openshift.io", "Console", "consoles")
}

pub fn machine_config_pools() -> ApiResource {
    resource(
        "machineconfiguration.openshift.io",
        "MachineConfigPool",
        "machineconfigpools",
    )
}

pub fn user_oauth_access_tokens() -> ApiResource {
    resource(
        "oauth.openshift.io",
        "UserOAuthAccessToken",
        "useroauthaccesstokens",
    )
}

/// Имя объекта `UserOAuthAccessToken` для токена `sha256~…`: `sha256~` и base64url без `=`
/// от SHA-256 части после префикса (так хранит токены OAuth-сервер OpenShift). Токены другого
/// вида (JWT внешнего OIDC, ServiceAccount) в этих объектах не хранятся — `None`.
pub fn oauth_token_object_name(token: &str) -> Option<String> {
    let raw = token.trim().strip_prefix("sha256~")?;
    let digest = sha2::Sha256::digest(raw.as_bytes());
    Some(format!("sha256~{}", URL_SAFE_NO_PAD.encode(digest)))
}

/// Когда истекает токен по его объекту: время создания + `expiresIn` (0 — бессрочный).
pub fn token_expires_at(o: &DynamicObject) -> Option<i64> {
    let created = o.metadata.creation_timestamp.as_ref()?;
    let expires_in = o.data["expiresIn"].as_i64().filter(|s| *s > 0)?;
    Some(crate::k8s::view::seconds(created) + expires_in)
}

/// Версия кластера (`ClusterVersion` `version`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterVersionInfo {
    pub version: Option<String>,
    pub channel: Option<String>,
    pub available_updates: usize,
    pub failing: Option<ConditionView>,
    pub progressing: Option<ConditionView>,
}

pub fn cluster_version_info(o: &DynamicObject) -> ClusterVersionInfo {
    let list = conditions(&o.data["status"]["conditions"]);
    let take = |kind: &str| list.iter().find(|c| c.kind == kind).cloned();
    ClusterVersionInfo {
        version: o.data["status"]["desired"]["version"]
            .as_str()
            .map(String::from),
        channel: o.data["spec"]["channel"]
            .as_str()
            .filter(|c| !c.is_empty())
            .map(String::from),
        available_updates: o.data["status"]["availableUpdates"]
            .as_array()
            .map_or(0, Vec::len),
        failing: take("Failing"),
        progressing: take("Progressing"),
    }
}

/// Инфраструктура кластера (`Infrastructure` `cluster`): `AWS`, `VSphere`, `BareMetal`, `None`…
pub fn infrastructure_type(o: &DynamicObject) -> Option<String> {
    let status = &o.data["status"];
    status["platformStatus"]["type"]
        .as_str()
        .or_else(|| status["platform"].as_str())
        .map(String::from)
}

/// Адрес веб-консоли (`Console` `cluster`).
pub fn console_url(o: &DynamicObject) -> Option<String> {
    o.data["status"]["consoleURL"]
        .as_str()
        .filter(|u| !u.is_empty())
        .map(String::from)
}

/// ClusterOperator: доступен ли, обновляется ли, деградировал ли и почему.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorRow {
    pub name: String,
    pub version: Option<String>,
    pub available: Option<bool>,
    pub progressing: Option<bool>,
    pub degraded: Option<bool>,
    /// Сообщение условия, из-за которого оператор не в норме.
    pub message: Option<String>,
}

impl OperatorRow {
    /// Не в норме: недоступен, деградировал или обновляется.
    pub fn has_problem(&self) -> bool {
        self.available != Some(true)
            || self.degraded == Some(true)
            || self.progressing == Some(true)
    }
}

pub fn operator_row(o: &DynamicObject) -> OperatorRow {
    let list = conditions(&o.data["status"]["conditions"]);
    let flag = |kind: &str| crate::k8s::view::condition_flag(&list, kind);
    let message_of = |kind: &str, status: &str| {
        list.iter()
            .find(|c| c.kind == kind && c.status == status)
            .and_then(|c| c.message.clone())
    };
    let name = o.metadata.name.clone().unwrap_or_default();
    let version = o.data["status"]["versions"].as_array().and_then(|vs| {
        vs.iter()
            .find(|v| v["name"] == "operator")
            .and_then(|v| v["version"].as_str())
            .map(String::from)
    });
    OperatorRow {
        version,
        available: flag("Available"),
        progressing: flag("Progressing"),
        degraded: flag("Degraded"),
        message: message_of("Degraded", "True")
            .or_else(|| message_of("Available", "False"))
            .or_else(|| message_of("Progressing", "True")),
        name,
    }
}

/// MachineConfigPool: деградировал ли, обновляется ли, сколько машин в каком состоянии.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineConfigPoolRow {
    pub name: String,
    pub degraded: bool,
    pub updating: bool,
    pub machines: i64,
    pub ready: i64,
    pub updated: i64,
    pub degraded_machines: i64,
}

pub fn machine_config_pool_row(o: &DynamicObject) -> MachineConfigPoolRow {
    let status = &o.data["status"];
    let list = conditions(&status["conditions"]);
    let count = |k: &str| status[k].as_i64().unwrap_or(0);
    MachineConfigPoolRow {
        name: o.metadata.name.clone().unwrap_or_default(),
        degraded: crate::k8s::view::condition_flag(&list, "Degraded") == Some(true),
        updating: crate::k8s::view::condition_flag(&list, "Updating") == Some(true),
        machines: count("machineCount"),
        ready: count("readyMachineCount"),
        updated: count("updatedMachineCount"),
        degraded_machines: count("degradedMachineCount"),
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    use kube::api::DynamicObject;

    pub fn object(v: serde_json::Value) -> DynamicObject {
        serde_json::from_value(v).unwrap()
    }

    pub fn cluster_version() -> serde_json::Value {
        serde_json::json!({"apiVersion": "config.openshift.io/v1", "kind": "ClusterVersion",
        "metadata": {"name": "version"},
        "spec": {"channel": "stable-4.18"},
        "status": {"desired": {"version": "4.18.3"},
            "availableUpdates": [{"version": "4.18.4"}, {"version": "4.18.5"}],
            "conditions": [
                {"type": "Available", "status": "True"},
                {"type": "Failing", "status": "False"},
                {"type": "Progressing", "status": "True", "message": "Working towards 4.18.4"}
            ]}})
    }

    pub fn operators() -> serde_json::Value {
        let op = |name: &str, available: &str, degraded: &str, message: &str| {
            serde_json::json!({"metadata": {"name": name},
            "status": {"versions": [{"name": "operator", "version": "4.18.3"}],
                "conditions": [
                    {"type": "Available", "status": available},
                    {"type": "Progressing", "status": "False"},
                    {"type": "Degraded", "status": degraded, "message": message}
                ]}})
        };
        serde_json::json!({"apiVersion": "config.openshift.io/v1", "kind": "ClusterOperatorList",
        "metadata": {}, "items": [
            op("dns", "True", "False", ""),
            op("ingress", "True", "True", "Some ingresscontrollers are degraded"),
            op("monitoring", "False", "False", "")
        ]})
    }

    pub fn pools() -> serde_json::Value {
        serde_json::json!({"apiVersion": "machineconfiguration.openshift.io/v1",
        "kind": "MachineConfigPoolList", "metadata": {}, "items": [
            {"metadata": {"name": "master"}, "status": {"machineCount": 3,
                "readyMachineCount": 3, "updatedMachineCount": 3, "degradedMachineCount": 0,
                "conditions": [{"type": "Updating", "status": "False"},
                    {"type": "Degraded", "status": "False"}]}},
            {"metadata": {"name": "worker"}, "status": {"machineCount": 4,
                "readyMachineCount": 3, "updatedMachineCount": 2, "degradedMachineCount": 0,
                "conditions": [{"type": "Updating", "status": "True"},
                    {"type": "Degraded", "status": "False"}]}}
        ]})
    }
}
````

<!-- block: file t04-src-tauri-src-k8s-overview-rs-impl step=4.2 path=src-tauri/src/k8s/overview.rs part=impl -->
````rust
//! Обзор кластера (спец. §6.2): платформа и версия, состояние API-сервера по `/readyz?verbose`,
//! ноды, классы Ingress и Gateway, число проектов и часть платформы. Что не читается из-за прав
//! или ошибки, приходит причиной (`Part`), остальное показывается.

use k8s_openapi::api::networking::v1::IngressClass;
use kube::api::{ApiResource, GroupVersionKind, ListParams};
use serde::Serialize;

use crate::error::{AppError, AppResult};
use crate::k8s::cluster::ConnectionInfo;
use crate::k8s::errors::not_allowed;
use crate::k8s::nodes::{NodeSummary, list_nodes, summary};
use crate::k8s::read::ReadClient;
use crate::k8s::view::{Part, condition_flag, conditions};
use crate::platform::PlatformInfo;
use crate::platform::adapters::{PlatformOverview, adapter};

/// Больше стольких непройденных проверок `/readyz` не показывается.
const MAX_FAILED_CHECKS: usize = 20;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterOverview {
    pub platform: PlatformInfo,
    pub server_version: String,
    pub readyz: Readyz,
    pub nodes: Part<NodeSummary>,
    pub ingress_classes: Part<Vec<IngressClassRow>>,
    /// `None` — в кластере нет Gateway API.
    pub gateway_classes: Option<Part<Vec<GatewayClassRow>>>,
    pub projects: Part<ProjectCount>,
    /// Своя часть платформы (OpenShift: обновления, операторы, пулы машин).
    pub platform_details: Option<PlatformOverview>,
}

/// Состояние API-сервера: готов ли и какие проверки не прошли. `error` — ответа с проверками
/// нет вовсе (сеть, права).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Readyz {
    pub ok: bool,
    pub failed: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AppError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressClassRow {
    pub name: String,
    pub controller: Option<String>,
    /// Класс по умолчанию (аннотация `ingressclass.kubernetes.io/is-default-class`).
    pub default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayClassRow {
    pub name: String,
    pub controller: Option<String>,
    pub accepted: Option<bool>,
}

/// Сколько проектов видит пользователь: первые 500 и признак «есть ещё».
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectCount {
    pub count: usize,
    pub more: bool,
}

/// Имена непройденных проверок: строки `[-]etcd failed: reason withheld` → `etcd`.
pub fn failed_checks(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|l| l.trim().strip_prefix("[-]"))
        .map(|rest| rest.split_whitespace().next().unwrap_or(rest).to_string())
        .take(MAX_FAILED_CHECKS)
        .collect()
}

/// `/readyz?verbose` читает любой пользователь (`system:public-info-viewer`). Не готовый
/// API-сервер отвечает ошибкой 5xx со списком проверок в теле.
pub async fn readyz(r: &ReadClient) -> Readyz {
    match r.get_text("/readyz?verbose").await {
        Ok(text) => {
            let failed = failed_checks(&text);
            Readyz {
                ok: failed.is_empty(),
                failed,
                error: None,
            }
        }
        Err(e) => {
            let failed = e
                .params
                .get("message")
                .map(|m| failed_checks(m))
                .unwrap_or_default();
            Readyz {
                ok: false,
                error: failed.is_empty().then_some(e),
                failed,
            }
        }
    }
}

pub fn ingress_class_row(c: &IngressClass) -> IngressClassRow {
    IngressClassRow {
        name: c.metadata.name.clone().unwrap_or_default(),
        controller: c.spec.as_ref().and_then(|s| s.controller.clone()),
        default: c
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("ingressclass.kubernetes.io/is-default-class"))
            .is_some_and(|v| v == "true"),
    }
}

pub fn gateway_classes() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("gateway.networking.k8s.io", "v1", "GatewayClass"),
        "gatewayclasses",
    )
}

pub async fn ingress_classes(r: &ReadClient) -> AppResult<Vec<IngressClassRow>> {
    let list = r
        .list_cluster::<IngressClass>(&ListParams::default())
        .await?;
    Ok(list.items.iter().map(ingress_class_row).collect())
}

async fn gateway_class_rows(r: &ReadClient) -> AppResult<Vec<GatewayClassRow>> {
    let list = r
        .list_dynamic(&gateway_classes(), None, &ListParams::default())
        .await?;
    Ok(list
        .items
        .iter()
        .map(|o| GatewayClassRow {
            name: o.metadata.name.clone().unwrap_or_default(),
            controller: o.data["spec"]["controllerName"].as_str().map(String::from),
            accepted: condition_flag(&conditions(&o.data["status"]["conditions"]), "Accepted"),
        })
        .collect())
}

/// Обзор кластера; запросы идут параллельно. Ноды и проекты без прав не читаются.
pub async fn cluster_overview(r: &ReadClient, info: &ConnectionInfo) -> ClusterOverview {
    let caps = &info.capabilities;
    let nodes = async {
        if !caps.list_nodes {
            return Part::err(not_allowed("list", "nodes", None));
        }
        list_nodes(r).await.map(|l| summary(&l.items)).into()
    };
    let projects = async {
        if !caps.list_projects {
            let resource = if info.api_groups.projects {
                "projects"
            } else {
                "namespaces"
            };
            return Part::err(not_allowed("list", resource, None));
        }
        r.project_names(info.api_groups.projects)
            .await
            .map(|(names, more)| ProjectCount {
                count: names.len(),
                more,
            })
            .into()
    };
    let gateways = async {
        match info.api_groups.gateway_api {
            true => Some(gateway_class_rows(r).await.into()),
            false => None,
        }
    };
    let (readyz, nodes, ingress_classes, gateway_classes, projects, platform_details) = tokio::join!(
        readyz(r),
        nodes,
        async { ingress_classes(r).await.into() },
        gateways,
        projects,
        adapter(info.platform.kind).overview(r, caps),
    );
    ClusterOverview {
        platform: info.platform.clone(),
        server_version: info.server_version.clone(),
        readyz,
        nodes,
        ingress_classes,
        gateway_classes,
        projects,
        platform_details,
    }
}
````

<!-- block: file t04-src-tauri-src-k8s-view-rs-impl step=4.2 path=src-tauri/src/k8s/view.rs part=impl -->
````rust
//! Общее в ответах чтения: часть экрана (`Part`), условия объектов, время. Время уходит во
//! фронтенд секундами Unix.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use serde::Serialize;

use crate::error::{AppError, AppResult};

/// Часть экрана: данные или причина, почему их нет, — права или ошибка чтения (спец. §15,
/// «Частичные результаты»). Остальной экран показывается.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Part<T> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AppError>,
}

impl<T> Part<T> {
    pub fn ok(data: T) -> Self {
        Self {
            data: Some(data),
            error: None,
        }
    }

    pub fn err(error: AppError) -> Self {
        Self {
            data: None,
            error: Some(error),
        }
    }
}

impl<T> From<AppResult<T>> for Part<T> {
    fn from(r: AppResult<T>) -> Self {
        match r {
            Ok(t) => Self::ok(t),
            Err(e) => Self::err(e),
        }
    }
}

/// Условие объекта (`status.conditions[]`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConditionView {
    #[serde(rename = "type")]
    pub kind: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition: Option<i64>,
}

impl ConditionView {
    pub fn is_true(&self) -> bool {
        self.status == "True"
    }
}

/// Условия из JSON (`status.conditions` любого вида объекта — типизированного или из
/// `DynamicObject`): одинаковые поля у всех.
pub fn conditions(v: &serde_json::Value) -> Vec<ConditionView> {
    let text = |c: &serde_json::Value, k: &str| c[k].as_str().map(String::from);
    v.as_array()
        .map(|list| {
            list.iter()
                .filter_map(|c| {
                    Some(ConditionView {
                        kind: text(c, "type")?,
                        status: text(c, "status").unwrap_or_default(),
                        reason: text(c, "reason").filter(|r| !r.is_empty()),
                        message: text(c, "message").filter(|m| !m.is_empty()),
                        last_transition: c["lastTransitionTime"].as_str().and_then(rfc3339),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Условия типизированного объекта: через JSON, как у `DynamicObject`.
pub fn conditions_of<T: Serialize>(list: &Option<Vec<T>>) -> Vec<ConditionView> {
    match list {
        Some(l) => conditions(&serde_json::to_value(l).unwrap_or_default()),
        None => Vec::new(),
    }
}

/// Условие вида `kind`.
pub fn condition<'a>(list: &'a [ConditionView], kind: &str) -> Option<&'a ConditionView> {
    list.iter().find(|c| c.kind == kind)
}

/// Значение условия как `bool`: `True` / `False`, иначе (`Unknown`, нет условия) — `None`.
pub fn condition_flag(list: &[ConditionView], kind: &str) -> Option<bool> {
    condition(list, kind).and_then(|c| match c.status.as_str() {
        "True" => Some(true),
        "False" => Some(false),
        _ => None,
    })
}

/// Секунды Unix из времени Kubernetes.
pub fn seconds(t: &Time) -> i64 {
    t.0.as_second()
}

/// Когда создан объект.
pub fn created(meta: &ObjectMeta) -> Option<i64> {
    meta.creation_timestamp.as_ref().map(seconds)
}

/// Секунды Unix из строки RFC 3339 (время в JSON объектов без типа).
pub fn rfc3339(s: &str) -> Option<i64> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|t| t.unix_timestamp())
}

/// Имя объекта (пустое, если его нет).
pub fn name(meta: &ObjectMeta) -> String {
    meta.name.clone().unwrap_or_default()
}
````

<!-- block: file t04-src-tauri-src-platform-adapters-rs-impl step=4.2 path=src-tauri/src/platform/adapters.rs part=impl -->
````rust
//! Адаптеры платформы (спец. §4.3): то, что у вендоров устроено по-разному и из групп API не
//! выводится, — обзор уровня кластера и срок токена. Реализация выбирается по `PlatformKind`
//! при подключении; адаптер Platform V DropApp добавит этап O5, автопоиск метрик — этап O3.

use futures::future::BoxFuture;
use kube::api::ListParams;
use serde::Serialize;

use crate::k8s::cluster::Capabilities;
use crate::k8s::errors::not_allowed;
use crate::k8s::openshift::{
    self, ClusterVersionInfo, MachineConfigPoolRow, OperatorRow, cluster_version_info,
    machine_config_pool_row, operator_row,
};
use crate::k8s::read::ReadClient;
use crate::k8s::view::Part;
use crate::platform::PlatformKind;

pub trait Platform: Send + Sync {
    /// Обзор уровня кластера, который есть только у этой платформы.
    fn overview<'a>(
        &'a self,
        r: &'a ReadClient,
        caps: &'a Capabilities,
    ) -> BoxFuture<'a, Option<PlatformOverview>>;

    /// Когда истекает токен входа (секунды Unix), если платформа это сообщает.
    fn token_expiry<'a>(&'a self, r: &'a ReadClient, token: &'a str) -> BoxFuture<'a, Option<i64>>;
}

/// Адаптер платформы.
pub fn adapter(kind: PlatformKind) -> &'static dyn Platform {
    match kind {
        PlatformKind::Openshift => &OpenShift,
        PlatformKind::Kubernetes => &Kubernetes,
    }
}

/// Часть обзора, своя у каждой платформы.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum PlatformOverview {
    Openshift(OpenShiftOverview),
}

/// Обзор кластера OpenShift (спец. §6.2): обновления, инфраструктура, консоль, операторы
/// и пулы машин.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenShiftOverview {
    pub cluster_version: Part<ClusterVersionInfo>,
    pub infrastructure: Option<String>,
    pub console_url: Option<String>,
    pub operators: Part<OperatorSummary>,
    pub machine_config_pools: Part<Vec<MachineConfigPoolRow>>,
}

/// Операторы кластера: сколько всего и какие не в норме.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorSummary {
    pub total: usize,
    pub problems: Vec<OperatorRow>,
}

/// Любой Kubernetes без адаптера: общий обзор без своей части, срока токена нет.
pub struct Kubernetes;

impl Platform for Kubernetes {
    fn overview<'a>(
        &'a self,
        _: &'a ReadClient,
        _: &'a Capabilities,
    ) -> BoxFuture<'a, Option<PlatformOverview>> {
        Box::pin(async { None })
    }

    fn token_expiry<'a>(&'a self, _: &'a ReadClient, _: &'a str) -> BoxFuture<'a, Option<i64>> {
        Box::pin(async { None })
    }
}

pub struct OpenShift;

/// Все ClusterOperators.
pub async fn cluster_operators(r: &ReadClient) -> crate::error::AppResult<Vec<OperatorRow>> {
    let list = r
        .list_dynamic(
            &openshift::cluster_operators(),
            None,
            &ListParams::default(),
        )
        .await?;
    Ok(list.items.iter().map(operator_row).collect())
}

impl Platform for OpenShift {
    fn overview<'a>(
        &'a self,
        r: &'a ReadClient,
        caps: &'a Capabilities,
    ) -> BoxFuture<'a, Option<PlatformOverview>> {
        Box::pin(async move {
            let version = async {
                if !caps.read_cluster_version {
                    return Part::err(not_allowed("get", "clusterversions", None));
                }
                r.get_dynamic(&openshift::cluster_versions(), None, "version")
                    .await
                    .map(|o| cluster_version_info(&o))
                    .into()
            };
            let infrastructure = async {
                r.get_dynamic(&openshift::infrastructures(), None, "cluster")
                    .await
                    .ok()
                    .and_then(|o| openshift::infrastructure_type(&o))
            };
            let console = async {
                r.get_dynamic(&openshift::consoles(), None, "cluster")
                    .await
                    .ok()
                    .and_then(|o| openshift::console_url(&o))
            };
            let operators = async {
                if !caps.read_cluster_operators {
                    return Part::err(not_allowed("list", "clusteroperators", None));
                }
                cluster_operators(r)
                    .await
                    .map(|rows| OperatorSummary {
                        total: rows.len(),
                        problems: rows.into_iter().filter(OperatorRow::has_problem).collect(),
                    })
                    .into()
            };
            let pools = async {
                r.list_dynamic(
                    &openshift::machine_config_pools(),
                    None,
                    &ListParams::default(),
                )
                .await
                .map(|l| l.items.iter().map(machine_config_pool_row).collect())
                .into()
            };
            let (cluster_version, infrastructure, console_url, operators, machine_config_pools) =
                tokio::join!(version, infrastructure, console, operators, pools);
            Some(PlatformOverview::Openshift(OpenShiftOverview {
                cluster_version,
                infrastructure,
                console_url,
                operators,
                machine_config_pools,
            }))
        })
    }

    /// Срок токена OAuth OpenShift — из его объекта `UserOAuthAccessToken` (спец. §5.2). Свои
    /// токены пользователь читать может; нет объекта или прав — срока нет.
    fn token_expiry<'a>(&'a self, r: &'a ReadClient, token: &'a str) -> BoxFuture<'a, Option<i64>> {
        Box::pin(async move {
            let name = openshift::oauth_token_object_name(token)?;
            match r
                .get_dynamic(&openshift::user_oauth_access_tokens(), None, &name)
                .await
            {
                Ok(o) => openshift::token_expires_at(&o),
                Err(e) => {
                    tracing::debug!("token expiry is not readable: {}", e.message);
                    None
                }
            }
        })
    }
}
````

<!-- block: patch t04-src step=4.2 -->
````diff
diff --git a/src-tauri/src/commands/mod.rs b/src-tauri/src/commands/mod.rs
index c189876..b5af943 100644
--- a/src-tauri/src/commands/mod.rs
+++ b/src-tauri/src/commands/mod.rs
@@ -1,5 +1,6 @@
 pub mod app;
 pub mod auth;
+pub mod cluster;
 pub mod connections;
 pub mod folders;
 pub mod kubeconfig;
diff --git a/src-tauri/src/k8s/cluster.rs b/src-tauri/src/k8s/cluster.rs
index c7fc600..8cf7972 100644
--- a/src-tauri/src/k8s/cluster.rs
+++ b/src-tauri/src/k8s/cluster.rs
@@ -39,6 +39,8 @@ pub struct ConnectionInfo {
     pub platform: PlatformInfo,
     pub api_groups: ApiGroups,
     pub capabilities: Capabilities,
+    /// Когда истекает токен входа (секунды Unix), если платформа это сообщает (спец. §5.2).
+    pub token_expires_at: Option<i64>,
 }
 
 /// Итог discovery: версия API-сервера, платформа и группы API.
@@ -276,6 +278,7 @@ pub async fn probe(client: &kube::Client, limit: Duration) -> AppResult<Connecti
         platform: d.platform,
         api_groups: d.api_groups,
         capabilities,
+        token_expires_at: None,
     })
 }
 
@@ -374,6 +377,7 @@ mod tests {
                     read_cluster_operators: false,
                     read_cluster_version: false,
                 },
+                token_expires_at: None,
             }
         );
         drop(client);
diff --git a/src-tauri/src/k8s/errors.rs b/src-tauri/src/k8s/errors.rs
index bb4fc30..a732d47 100644
--- a/src-tauri/src/k8s/errors.rs
+++ b/src-tauri/src/k8s/errors.rs
@@ -49,30 +49,32 @@ static FORBIDDEN: LazyLock<Regex> = LazyLock::new(|| {
     .expect("valid regex")
 });
 
+/// Нет права `verb` на `resource` в проекте `namespace` или на уровне кластера (`None`). Так же
+/// выглядит ответ 403, и так же — часть экрана, которую Glafira не читает, зная, что права нет.
+pub fn not_allowed(verb: &str, resource: &str, namespace: Option<&str>) -> AppError {
+    match namespace {
+        Some(ns) => app_err!(
+            ErrorCode::Forbidden,
+            "errors.k8s.forbiddenIn",
+            "Not allowed: {{verb}} {{resource}} in project “{{namespace}}”",
+            verb = verb,
+            resource = resource,
+            namespace = ns
+        ),
+        None => app_err!(
+            ErrorCode::Forbidden,
+            "errors.k8s.forbiddenCluster",
+            "Not allowed: {{verb}} {{resource}} at the cluster level",
+            verb = verb,
+            resource = resource
+        ),
+    }
+}
+
 /// 403: какого права не хватает — из текста ответа API-сервера.
 pub fn forbidden(message: &str) -> AppError {
     let e = match FORBIDDEN.captures(message) {
-        Some(c) => {
-            let verb = &c["verb"];
-            let resource = &c["resource"];
-            match c.name("ns") {
-                Some(ns) => app_err!(
-                    ErrorCode::Forbidden,
-                    "errors.k8s.forbiddenIn",
-                    "Not allowed: {{verb}} {{resource}} in project “{{namespace}}”",
-                    verb = verb,
-                    resource = resource,
-                    namespace = ns.as_str()
-                ),
-                None => app_err!(
-                    ErrorCode::Forbidden,
-                    "errors.k8s.forbiddenCluster",
-                    "Not allowed: {{verb}} {{resource}} at the cluster level",
-                    verb = verb,
-                    resource = resource
-                ),
-            }
-        }
+        Some(c) => not_allowed(&c["verb"], &c["resource"], c.name("ns").map(|m| m.as_str())),
         None => app_err!(
             ErrorCode::Forbidden,
             "errors.k8s.forbidden",
@@ -258,6 +260,15 @@ mod tests {
         assert_eq!(e.params["verb"], "patch");
         assert_eq!(e.params["resource"], "deployments");
 
+        assert_eq!(
+            not_allowed("list", "nodes", None).message,
+            "Not allowed: list nodes at the cluster level"
+        );
+        assert_eq!(
+            not_allowed("get", "pods/log", Some("a")).params["namespace"],
+            "a"
+        );
+
         let e = forbidden("access denied by webhook");
         assert_eq!(e.key, "errors.k8s.forbidden");
         assert_eq!(e.params["message"], "access denied by webhook");
diff --git a/src-tauri/src/k8s/mod.rs b/src-tauri/src/k8s/mod.rs
index aa3ee23..7419b9f 100644
--- a/src-tauri/src/k8s/mod.rs
+++ b/src-tauri/src/k8s/mod.rs
@@ -5,7 +5,11 @@ pub mod config;
 pub mod connectivity;
 pub mod errors;
 pub mod kubeconfig;
+pub mod nodes;
+pub mod openshift;
+pub mod overview;
 pub mod pool;
 pub mod read;
 #[cfg(test)]
 pub(crate) mod testing;
+pub mod view;
diff --git a/src-tauri/src/k8s/pool.rs b/src-tauri/src/k8s/pool.rs
index dd2af40..e2a0290 100644
--- a/src-tauri/src/k8s/pool.rs
+++ b/src-tauri/src/k8s/pool.rs
@@ -11,6 +11,7 @@ use crate::k8s::cluster::{ConnectionInfo, probe};
 use crate::k8s::config::{apply_exec_credential, build_kube_config, process_env, request_timeout};
 use crate::k8s::errors::from_kube;
 use crate::k8s::read::ReadClient;
+use crate::platform::adapters::adapter;
 use crate::secrets::SecretStore;
 use crate::storage::profile::{AuthConfig, ConnectionProfile};
 use crate::sync::MutexExt;
@@ -148,6 +149,11 @@ pub async fn open(
         tokio::task::spawn_blocking(move || build_kube_config(&p, secrets.as_ref(), &process_env))
             .await
             .map_err(|e| AppError::internal_with(e.to_string()))??;
+    // Токен нужен адаптеру платформы, чтобы узнать срок (OpenShift: `useroauthaccesstokens`).
+    let token = config.auth_info.token.as_ref().map(|t| {
+        use secrecy::ExposeSecret;
+        t.expose_secret().to_string()
+    });
     let mut expires_at = None;
     if matches!(profile.auth, AuthConfig::Exec(_)) {
         let cred =
@@ -157,7 +163,12 @@ pub async fn open(
         apply_exec_credential(&mut config, &cred);
     }
     let client = kube::Client::try_from(config).map_err(|e| from_kube(&e))?;
-    let info = probe(&client, timeout).await?;
+    let mut info = probe(&client, timeout).await?;
+    if let Some(token) = &token {
+        info.token_expires_at = adapter(info.platform.kind)
+            .token_expiry(&ReadClient::new(client.clone(), timeout), token)
+            .await;
+    }
     Ok(Connected {
         client,
         info,
@@ -182,6 +193,7 @@ mod tests {
                 platform: crate::platform::detect(&Default::default()),
                 api_groups: Default::default(),
                 capabilities: Capabilities::default(),
+                token_expires_at: None,
             },
             timeout: Duration::from_secs(1),
             expires_at: None,
diff --git a/src-tauri/src/k8s/read.rs b/src-tauri/src/k8s/read.rs
index 442718b..0c593b7 100644
--- a/src-tauri/src/k8s/read.rs
+++ b/src-tauri/src/k8s/read.rs
@@ -22,7 +22,8 @@ pub const PAGE_SIZE: u32 = 500;
 pub const MAX_ITEMS: usize = 10_000;
 
 /// Прочитанный список: объекты и признак, что прочитано не всё (`MAX_ITEMS`).
-#[derive(Debug, Clone)]
+#[derive(Debug, Clone, serde::Serialize)]
+#[serde(rename_all = "camelCase")]
 pub struct Listed<K> {
     pub items: Vec<K>,
     pub truncated: bool,
@@ -213,6 +214,11 @@ impl ReadClient {
         watcher(self.dynamic(ar, Some(namespace)), config)
     }
 
+    /// Имена проектов (или namespace): первые 500 и признак «есть ещё».
+    pub async fn project_names(&self, projects_api: bool) -> AppResult<(Vec<String>, bool)> {
+        crate::k8s::cluster::list_project_names(&self.client, self.timeout, projects_api).await
+    }
+
     /// Поток строк лога контейнера; таймаут — только на открытие потока.
     pub async fn log_stream(
         &self,
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index 3ae2d2a..5c4caba 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -71,6 +71,10 @@ pub fn run() {
             commands::settings::set_language,
             commands::settings::get_policy_status,
             commands::sessions::stop_session,
+            commands::cluster::get_cluster_overview,
+            commands::cluster::list_nodes,
+            commands::cluster::get_node,
+            commands::cluster::list_cluster_operators,
         ])
         .run(tauri::generate_context!())
         .expect("error while building tauri application");
diff --git a/src-tauri/src/platform/mod.rs b/src-tauri/src/platform/mod.rs
index 91469fc..c3583b7 100644
--- a/src-tauri/src/platform/mod.rs
+++ b/src-tauri/src/platform/mod.rs
@@ -1,8 +1,10 @@
 //! Платформа кластера (спец. §4.3) — то, что у вендоров устроено по-разному и из групп API
 //! не выводится. Определяется чистой функцией `detect` по discovery при подключении; ручного выбора
 //! в профиле нет. Какие объекты читать и показывать, решают группы API (`ApiGroups`), а не
-//! платформа. Поведение, которое зависит от платформы (обзор кластера, автопоиск метрик), добавляют
-//! следующие этапы.
+//! платформа. Поведение, которое зависит от платформы, — в её адаптере (`adapters`): обзор кластера
+//! и срок токена; автопоиск метрик добавит этап O3.
+
+pub mod adapters;
 
 use serde::Serialize;
 
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 293 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 133`

- [ ] **Step 4: Commit.**

```bash
git add \
  src-tauri/src/commands/cluster.rs \
  src-tauri/src/commands/mod.rs \
  src-tauri/src/k8s/cluster.rs \
  src-tauri/src/k8s/errors.rs \
  src-tauri/src/k8s/mod.rs \
  src-tauri/src/k8s/nodes.rs \
  src-tauri/src/k8s/openshift.rs \
  src-tauri/src/k8s/overview.rs \
  src-tauri/src/k8s/pool.rs \
  src-tauri/src/k8s/read.rs \
  src-tauri/src/k8s/view.rs \
  src-tauri/src/lib.rs \
  src-tauri/src/platform/adapters.rs \
  src-tauri/src/platform/mod.rs
git commit -s -m "feat(k8s): трейт Platform, обзор кластера — /readyz, ноды, классы Ingress и Gateway, OpenShift; срок токена" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 5: Проекты и права пользователя в проекте

**Files:**
- Create: `src-tauri/src/commands/projects.rs`, `src-tauri/src/k8s/access.rs`, `src-tauri/src/k8s/projects.rs`
- Modify: `src-tauri/src/commands/mod.rs`, `src-tauri/src/k8s/mod.rs`, `src-tauri/src/k8s/pool.rs`, `src-tauri/src/k8s/read.rs`, `src-tauri/src/lib.rs`, `src-tauri/src/state.rs`

**Interfaces:**
- Consumes: задачи 3–4 (`ReadClient`, `AppState::read`, `pool::Connected`).
- Produces: `k8s::projects::{ProjectRow { name, display_name?, phase?, favorite, from_profile }, ProjectList { items, truncated, error? }, projects(), list_projects(&ReadClient, projects_api, &ConnectionProfile)}`; `k8s::access::{RESOURCES, ACCESS_TTL, Verbs { get, list, watch }, NamespaceAccess { resources, incomplete }, resource_key, allows(&[ResourceRule], verb, group, resource), access_from_rules, namespace_access(&ReadClient, ns), AccessCache::{get, put, forget, forget_connection}}`; `ReadClient::rules_review(ns)`; `AppState::{access, read_in(id, namespace, f)}`; `AppState::evict_connection` сбрасывает и кэш прав; `k8s::pool::test_connected(handler)` (тесты); команды `list_projects(connectionId)`, `get_namespace_access(connectionId, namespace)`.

Спец. §5.1, §5.4, §6.1. Список проектов — объекты `Project` у OpenShift или namespace у остальных; избранные профиля отмечены. Если кластер не даёт список (403), приходят проекты из профиля — ручной список и избранные — вместе с причиной. Права в проекте — `SelfSubjectRulesReview`: по правилам считается get/list/watch для ресурсов, которые читает интерфейс (`RESOURCES`; правила с `resourceNames` не дают доступа к спискам, `*` и подресурсы вида `pods/*` учитываются); `incomplete` — интерфейс ничего не скрывает. Ответ живёт в кэше 5 минут; ответ 403 на чтение в проекте (`AppState::read_in`) и сброс клиента подключения его сбрасывают.

- [ ] **Step 1: Тесты.**

<!-- block: file t05-src-tauri-src-k8s-access-rs-tests step=5.1 path=src-tauri/src/k8s/access.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::mock_client;

    fn rule(verbs: &[&str], groups: &[&str], resources: &[&str]) -> ResourceRule {
        let list = |v: &[&str]| Some(v.iter().map(|s| s.to_string()).collect());
        ResourceRule {
            verbs: verbs.iter().map(|s| s.to_string()).collect(),
            api_groups: list(groups),
            resources: list(resources),
            resource_names: None,
        }
    }

    #[test]
    fn rules_with_wildcards_subresources_and_names() {
        let rules = [
            rule(&["get", "list", "watch"], &[""], &["pods", "pods/log"]),
            rule(&["list"], &["apps"], &["*"]),
            rule(&["*"], &["batch"], &["jobs"]),
            rule(&["get"], &["*"], &["*/status"]),
            ResourceRule {
                resource_names: Some(vec!["only-this".into()]),
                ..rule(&["get", "list"], &[""], &["secrets"])
            },
        ];
        assert!(allows(&rules, "watch", "", "pods"));
        assert!(allows(&rules, "get", "", "pods/log"));
        assert!(allows(&rules, "list", "apps", "deployments"));
        assert!(!allows(&rules, "watch", "apps", "deployments"));
        assert!(allows(&rules, "watch", "batch", "jobs"));
        assert!(allows(&rules, "get", "apps", "deployments/status"));
        assert!(
            !allows(&rules, "list", "", "secrets"),
            "только отдельный объект"
        );
        assert!(!allows(&rules, "list", "", "configmaps"));
        let a = access_from_rules(&rules, false);
        assert_eq!(a.resources.len(), RESOURCES.len());
        assert_eq!(
            a.resources["deployments.apps"],
            Verbs {
                get: false,
                list: true,
                watch: false
            }
        );
        assert!(a.resources["pods/log"].get);
        assert!(!a.resources["secrets"].list);
    }

    #[tokio::test]
    async fn review_of_a_namespace() {
        let (client, server) = mock_client(|_| {
            (
                201,
                serde_json::json!({"apiVersion": "authorization.k8s.io/v1",
                    "kind": "SelfSubjectRulesReview", "metadata": {}, "spec": {"namespace": "team-a"},
                    "status": {"incomplete": true, "nonResourceRules": [],
                        "resourceRules": [{"verbs": ["list"], "apiGroups": [""], "resources": ["pods"]}]}}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let a = namespace_access(&r, "team-a").await.unwrap();
        assert!(a.incomplete);
        assert!(a.resources["pods"].list && !a.resources["pods"].watch);
        drop(r);
        let seen = server.await.unwrap();
        assert_eq!(
            seen[0].path(),
            "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews"
        );
        assert_eq!(seen[0].method, http::Method::POST);
        assert_eq!(seen[0].body["spec"]["namespace"], "team-a");
    }

    #[tokio::test(start_paused = true)]
    async fn cache_lives_five_minutes_and_can_be_forgotten() {
        let c = AccessCache::default();
        let a = NamespaceAccess {
            incomplete: true,
            ..Default::default()
        };
        c.put("c", "team-a", a.clone());
        c.put("c", "team-b", a.clone());
        c.put("d", "team-a", a.clone());
        assert_eq!(c.get("c", "team-a"), Some(a.clone()));
        c.forget("c", "team-b");
        assert_eq!(c.get("c", "team-b"), None);
        tokio::time::advance(ACCESS_TTL).await;
        assert_eq!(c.get("c", "team-a"), None, "устарело");
        c.put("c", "team-a", a.clone());
        c.forget_connection("c");
        assert_eq!(c.get("c", "team-a"), None);
        assert!(
            c.inner.lock_safe().len() == 1,
            "права другого подключения остались"
        );
    }
}
````

<!-- block: file t05-src-tauri-src-k8s-projects-rs-tests step=5.1 path=src-tauri/src/k8s/projects.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use crate::storage::profile::sample_profile;
    use std::time::Duration;

    fn profile() -> ConnectionProfile {
        let mut p = sample_profile("a");
        p.favorite_namespaces = vec!["team-b".into(), "gone".into()];
        p.manual_namespaces = vec!["team-c".into(), "team-b".into()];
        p
    }

    #[tokio::test]
    async fn openshift_projects_with_display_names_and_favorites() {
        let (client, _) = mock_client(|_| {
            (
                200,
                serde_json::json!({"apiVersion": "project.openshift.io/v1", "kind": "ProjectList",
                "metadata": {}, "items": [
                    {"metadata": {"name": "team-b"}, "status": {"phase": "Active"}},
                    {"metadata": {"name": "team-a",
                        "annotations": {"openshift.io/display-name": "Команда A"}},
                        "status": {"phase": "Terminating"}}
                ]}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let l = list_projects(&r, true, &profile()).await.unwrap();
        assert!(l.error.is_none() && !l.truncated);
        let names: Vec<_> = l.items.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(
            names,
            ["team-a", "team-b"],
            "только проекты кластера, по имени"
        );
        assert_eq!(l.items[0].display_name.as_deref(), Some("Команда A"));
        assert_eq!(l.items[0].phase.as_deref(), Some("Terminating"));
        assert!(!l.items[0].favorite && l.items[1].favorite);
    }

    #[tokio::test]
    async fn without_rights_the_projects_of_the_profile() {
        let (client, server) = mock_client(|_| {
            status(
                403,
                "Forbidden",
                r#"namespaces is forbidden: User "u" cannot list resource "namespaces" in API group "" at the cluster scope"#,
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let l = list_projects(&r, false, &profile()).await.unwrap();
        assert_eq!(l.error.unwrap().key, "errors.k8s.forbiddenCluster");
        let names: Vec<_> = l
            .items
            .iter()
            .map(|p| (p.name.as_str(), p.favorite))
            .collect();
        assert_eq!(names, [("gone", true), ("team-b", true), ("team-c", false)]);
        assert!(l.items.iter().all(|p| p.from_profile));
        drop(r);
        assert_eq!(server.await.unwrap()[0].path(), "/api/v1/namespaces");
    }

    #[tokio::test]
    async fn other_errors_are_errors() {
        let (client, _) = mock_client(|_| status(401, "Unauthorized", "Unauthorized"));
        let r = ReadClient::new(client, Duration::from_secs(5));
        let e = list_projects(&r, false, &profile()).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::SessionExpired);
    }
}
````

- [ ] **Step 2: Реализация.**

<!-- block: file t05-src-tauri-src-commands-projects-rs step=5.2 path=src-tauri/src/commands/projects.rs -->
````rust
use tauri::State;

use crate::error::AppResult;
use crate::k8s::access::NamespaceAccess;
use crate::k8s::projects::ProjectList;
use crate::state::AppState;

/// Проекты подключения для дерева (спец. §6.1).
#[tauri::command]
pub async fn list_projects(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<ProjectList> {
    let profile = state.connections.get(&connection_id)?;
    state
        .read(&connection_id, |c| async move {
            crate::k8s::projects::list_projects(&c.reader(), c.info.api_groups.projects, &profile)
                .await
        })
        .await
}

/// Права пользователя в проекте (спец. §5.4): из кэша, если им меньше 5 минут.
#[tauri::command]
pub async fn get_namespace_access(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
) -> AppResult<NamespaceAccess> {
    if let Some(a) = state.access.get(&connection_id, &namespace) {
        return Ok(a);
    }
    let ns = namespace.clone();
    let access = state
        .read(&connection_id, |c| async move {
            crate::k8s::access::namespace_access(&c.reader(), &ns).await
        })
        .await?;
    state.access.put(&connection_id, &namespace, access.clone());
    Ok(access)
}
````

<!-- block: file t05-src-tauri-src-k8s-access-rs-impl step=5.2 path=src-tauri/src/k8s/access.rs part=impl -->
````rust
//! Права пользователя в проекте (спец. §5.4): `SelfSubjectRulesReview` при открытии проекта,
//! кэш на 5 минут, сброс при ответе 403. По ним интерфейс не показывает вкладки и разделы,
//! которые не читаются; окончательно решает API-сервер.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use k8s_openapi::api::authorization::v1::ResourceRule;
use serde::Serialize;
use tokio::time::Instant;

use crate::error::AppResult;
use crate::k8s::read::ReadClient;
use crate::sync::MutexExt;

/// Что интерфейс читает в проекте: группа API и ресурс.
pub const RESOURCES: &[(&str, &str)] = &[
    ("", "pods"),
    ("", "pods/log"),
    ("", "events"),
    ("", "services"),
    ("discovery.k8s.io", "endpointslices"),
    ("route.openshift.io", "routes"),
    ("networking.k8s.io", "ingresses"),
    ("gateway.networking.k8s.io", "gateways"),
    ("gateway.networking.k8s.io", "httproutes"),
    ("gateway.networking.k8s.io", "grpcroutes"),
    ("", "configmaps"),
    ("", "secrets"),
    ("", "persistentvolumeclaims"),
    ("", "resourcequotas"),
    ("", "limitranges"),
    ("autoscaling", "horizontalpodautoscalers"),
    ("policy", "poddisruptionbudgets"),
    ("apps", "deployments"),
    ("apps", "statefulsets"),
    ("apps", "daemonsets"),
    ("apps", "replicasets"),
    ("apps", "controllerrevisions"),
    ("batch", "jobs"),
    ("batch", "cronjobs"),
    ("apps.openshift.io", "deploymentconfigs"),
    ("", "replicationcontrollers"),
];

/// Сколько живут права проекта в кэше.
pub const ACCESS_TTL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Verbs {
    pub get: bool,
    pub list: bool,
    pub watch: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceAccess {
    /// `ресурс.группа`, как в kubectl (`pods`, `deployments.apps`, `pods/log`) → права.
    pub resources: BTreeMap<String, Verbs>,
    /// Сервер перечислил не все права (например, решает вебхук авторизации): интерфейс тогда
    /// ничего не скрывает.
    pub incomplete: bool,
}

/// Ключ ресурса: `pods`, `deployments.apps`.
pub fn resource_key(group: &str, resource: &str) -> String {
    if group.is_empty() {
        resource.to_string()
    } else {
        format!("{resource}.{group}")
    }
}

fn names(list: &Option<Vec<String>>, value: &str) -> bool {
    list.iter().flatten().any(|x| x == "*" || x == value)
}

/// Подходит ли ресурс правила: точное имя, `*` или для подресурса `pods/log` — `pods/*`, `*/log`.
fn resource_matches(list: &Option<Vec<String>>, resource: &str) -> bool {
    list.iter().flatten().any(|x| {
        x == "*"
            || x == resource
            || resource
                .split_once('/')
                .is_some_and(|(main, sub)| *x == format!("{main}/*") || *x == format!("*/{sub}"))
    })
}

/// Разрешено ли `verb` на ресурс по правилам. Правила с `resourceNames` дают доступ только к
/// отдельным объектам — для списков и вкладок они не считаются.
pub fn allows(rules: &[ResourceRule], verb: &str, group: &str, resource: &str) -> bool {
    rules.iter().any(|r| {
        r.resource_names.as_ref().is_none_or(Vec::is_empty)
            && r.verbs.iter().any(|v| v == "*" || v == verb)
            && names(&r.api_groups, group)
            && resource_matches(&r.resources, resource)
    })
}

pub fn access_from_rules(rules: &[ResourceRule], incomplete: bool) -> NamespaceAccess {
    NamespaceAccess {
        resources: RESOURCES
            .iter()
            .map(|(group, resource)| {
                let can = |verb| allows(rules, verb, group, resource);
                (
                    resource_key(group, resource),
                    Verbs {
                        get: can("get"),
                        list: can("list"),
                        watch: can("watch"),
                    },
                )
            })
            .collect(),
        incomplete,
    }
}

/// Права пользователя в проекте.
pub async fn namespace_access(r: &ReadClient, namespace: &str) -> AppResult<NamespaceAccess> {
    let status = r.rules_review(namespace).await?;
    Ok(access_from_rules(&status.resource_rules, status.incomplete))
}

/// Права проектов по подключениям: живут `ACCESS_TTL`, сбрасываются при 403 в проекте и при
/// сбросе клиента подключения.
#[derive(Default)]
pub struct AccessCache {
    inner: Mutex<HashMap<(String, String), (Instant, NamespaceAccess)>>,
}

impl AccessCache {
    pub fn get(&self, connection_id: &str, namespace: &str) -> Option<NamespaceAccess> {
        let key = (connection_id.to_string(), namespace.to_string());
        let mut inner = self.inner.lock_safe();
        match inner.get(&key) {
            Some((at, a)) if at.elapsed() < ACCESS_TTL => Some(a.clone()),
            Some(_) => {
                inner.remove(&key);
                None
            }
            None => None,
        }
    }

    pub fn put(&self, connection_id: &str, namespace: &str, access: NamespaceAccess) {
        self.inner.lock_safe().insert(
            (connection_id.to_string(), namespace.to_string()),
            (Instant::now(), access),
        );
    }

    pub fn forget(&self, connection_id: &str, namespace: &str) {
        self.inner
            .lock_safe()
            .remove(&(connection_id.to_string(), namespace.to_string()));
    }

    pub fn forget_connection(&self, connection_id: &str) {
        self.inner
            .lock_safe()
            .retain(|(c, _), _| c != connection_id);
    }
}
````

<!-- block: file t05-src-tauri-src-k8s-projects-rs-impl step=5.2 path=src-tauri/src/k8s/projects.rs part=impl -->
````rust
//! Проекты (спец. §5.1, §6.1): у OpenShift — объекты `Project`, у остальных — namespace. Если
//! кластер не отдаёт пользователю список, показываются проекты из профиля: ручной список и
//! избранные.

use std::collections::BTreeSet;

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{ApiResource, GroupVersionKind, ListParams};
use serde::Serialize;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::read::ReadClient;
use crate::storage::profile::ConnectionProfile;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectRow {
    pub name: String,
    /// Отображаемое имя проекта OpenShift (`openshift.io/display-name`).
    pub display_name: Option<String>,
    /// `Active` или `Terminating`; у проектов из профиля — неизвестно.
    pub phase: Option<String>,
    pub favorite: bool,
    /// Проект из профиля, а не из списка кластера.
    pub from_profile: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectList {
    pub items: Vec<ProjectRow>,
    pub truncated: bool,
    /// Почему кластер не дал список (обычно нет прав): тогда в списке только проекты из профиля.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AppError>,
}

pub fn projects() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("project.openshift.io", "v1", "Project"),
        "projects",
    )
}

/// Проекты кластера: имя, отображаемое имя и фаза.
async fn listed(r: &ReadClient, projects_api: bool) -> AppResult<(Vec<ProjectRow>, bool)> {
    let row = |name: String, display_name: Option<String>, phase: Option<String>| ProjectRow {
        name,
        display_name,
        phase,
        favorite: false,
        from_profile: false,
    };
    if projects_api {
        let l = r
            .list_dynamic(&projects(), None, &ListParams::default())
            .await?;
        let rows = l
            .items
            .into_iter()
            .map(|p| {
                let display = p
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get("openshift.io/display-name"))
                    .filter(|d| !d.is_empty())
                    .cloned();
                let phase = p.data["status"]["phase"].as_str().map(String::from);
                row(p.metadata.name.unwrap_or_default(), display, phase)
            })
            .collect();
        Ok((rows, l.truncated))
    } else {
        let l = r.list_cluster::<Namespace>(&ListParams::default()).await?;
        let rows = l
            .items
            .into_iter()
            .map(|n| {
                let phase = n.status.and_then(|s| s.phase);
                row(n.metadata.name.unwrap_or_default(), None, phase)
            })
            .collect();
        Ok((rows, l.truncated))
    }
}

/// Список проектов для дерева; избранные отмечены. Нет прав на список — проекты из профиля.
pub async fn list_projects(
    r: &ReadClient,
    projects_api: bool,
    profile: &ConnectionProfile,
) -> AppResult<ProjectList> {
    let favorites: BTreeSet<&str> = profile
        .favorite_namespaces
        .iter()
        .map(String::as_str)
        .collect();
    let (mut items, truncated, error) = match listed(r, projects_api).await {
        Ok((items, truncated)) => (items, truncated, None),
        Err(e) if e.code == ErrorCode::Forbidden => {
            let names: BTreeSet<&str> = profile
                .manual_namespaces
                .iter()
                .chain(&profile.favorite_namespaces)
                .map(String::as_str)
                .collect();
            let items = names
                .into_iter()
                .map(|n| ProjectRow {
                    name: n.to_string(),
                    display_name: None,
                    phase: None,
                    favorite: false,
                    from_profile: true,
                })
                .collect();
            (items, false, Some(e))
        }
        Err(e) => return Err(e),
    };
    for p in &mut items {
        p.favorite = favorites.contains(p.name.as_str());
    }
    items.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(ProjectList {
        items,
        truncated,
        error,
    })
}
````

<!-- block: patch t05-src step=5.2 -->
````diff
diff --git a/src-tauri/src/commands/mod.rs b/src-tauri/src/commands/mod.rs
index b5af943..82e4280 100644
--- a/src-tauri/src/commands/mod.rs
+++ b/src-tauri/src/commands/mod.rs
@@ -4,6 +4,7 @@ pub mod cluster;
 pub mod connections;
 pub mod folders;
 pub mod kubeconfig;
+pub mod projects;
 pub mod sessions;
 pub mod settings;
 
diff --git a/src-tauri/src/k8s/mod.rs b/src-tauri/src/k8s/mod.rs
index 7419b9f..2e802c1 100644
--- a/src-tauri/src/k8s/mod.rs
+++ b/src-tauri/src/k8s/mod.rs
@@ -1,5 +1,6 @@
 //! Работа с API-сервером: сборка клиента из профиля, ошибки, подключение и проверка.
 
+pub mod access;
 pub mod cluster;
 pub mod config;
 pub mod connectivity;
@@ -9,6 +10,7 @@ pub mod nodes;
 pub mod openshift;
 pub mod overview;
 pub mod pool;
+pub mod projects;
 pub mod read;
 #[cfg(test)]
 pub(crate) mod testing;
diff --git a/src-tauri/src/k8s/pool.rs b/src-tauri/src/k8s/pool.rs
index e2a0290..299235d 100644
--- a/src-tauri/src/k8s/pool.rs
+++ b/src-tauri/src/k8s/pool.rs
@@ -177,27 +177,35 @@ pub async fn open(
     })
 }
 
+/// Подключение для тестов: клиент — мок API-сервера с ответами `handler`, платформа — Kubernetes
+/// без прав уровня кластера.
+#[cfg(test)]
+pub(crate) fn test_connected(
+    handler: impl Fn(&crate::k8s::testing::Seen) -> (u16, serde_json::Value) + Send + 'static,
+) -> Arc<Connected> {
+    let (client, _) = crate::k8s::testing::mock_client(handler);
+    Arc::new(Connected {
+        client,
+        info: ConnectionInfo {
+            user: "u".into(),
+            groups: vec![],
+            server_version: "v1.32.0".into(),
+            platform: crate::platform::detect(&Default::default()),
+            api_groups: Default::default(),
+            capabilities: Default::default(),
+            token_expires_at: None,
+        },
+        timeout: Duration::from_secs(1),
+        expires_at: None,
+    })
+}
+
 #[cfg(test)]
 mod tests {
     use super::*;
-    use crate::k8s::cluster::Capabilities;
 
     fn connected() -> Arc<Connected> {
-        let (client, _) = crate::k8s::testing::mock_client(|_| (200, serde_json::json!({})));
-        Arc::new(Connected {
-            client,
-            info: ConnectionInfo {
-                user: "u".into(),
-                groups: vec![],
-                server_version: "v1.32.0".into(),
-                platform: crate::platform::detect(&Default::default()),
-                api_groups: Default::default(),
-                capabilities: Capabilities::default(),
-                token_expires_at: None,
-            },
-            timeout: Duration::from_secs(1),
-            expires_at: None,
-        })
+        test_connected(|_| (200, serde_json::json!({})))
     }
 
     #[tokio::test]
diff --git a/src-tauri/src/k8s/read.rs b/src-tauri/src/k8s/read.rs
index 0c593b7..70e1e97 100644
--- a/src-tauri/src/k8s/read.rs
+++ b/src-tauri/src/k8s/read.rs
@@ -1,4 +1,5 @@
-//! Чтение кластера (спец. §4.1): только get, list, watch, логи и GET отдельных путей API-сервера.
+//! Чтение кластера (спец. §4.1): только get, list, watch, логи, GET отдельных путей API-сервера
+//! и проверки прав.
 //! Внутренний `kube::Client` наружу не отдаётся — через `ReadClient` кластер не изменить. Им же
 //! будет пользоваться MCP-шлюз (этап O2). Каждый запрос ограничен таймаутом профиля; списки
 //! читаются страницами (спец. §6.7).
@@ -7,8 +8,11 @@ use std::fmt::Debug;
 use std::time::Duration;
 
 use futures::{AsyncBufRead, Stream};
+use k8s_openapi::api::authorization::v1::{
+    SelfSubjectRulesReview, SelfSubjectRulesReviewSpec, SubjectRulesReviewStatus,
+};
 use k8s_openapi::{ClusterResourceScope, NamespaceResourceScope};
-use kube::api::{Api, ApiResource, DynamicObject, ListParams, LogParams, ObjectList};
+use kube::api::{Api, ApiResource, DynamicObject, ListParams, LogParams, ObjectList, PostParams};
 use kube::core::Resource;
 use kube::runtime::watcher;
 use serde::de::DeserializeOwned;
@@ -214,6 +218,20 @@ impl ReadClient {
         watcher(self.dynamic(ar, Some(namespace)), config)
     }
 
+    /// Права пользователя в namespace (`SelfSubjectRulesReview`). Проверка создаётся запросом
+    /// POST, но в кластере ничего не меняет.
+    pub async fn rules_review(&self, namespace: &str) -> AppResult<SubjectRulesReviewStatus> {
+        let api: Api<SelfSubjectRulesReview> = Api::all(self.client.clone());
+        let review = SelfSubjectRulesReview {
+            spec: SelfSubjectRulesReviewSpec {
+                namespace: Some(namespace.to_string()),
+            },
+            ..Default::default()
+        };
+        let r = timed(self.timeout, api.create(&PostParams::default(), &review)).await?;
+        Ok(r.status.unwrap_or_default())
+    }
+
     /// Имена проектов (или namespace): первые 500 и признак «есть ещё».
     pub async fn project_names(&self, projects_api: bool) -> AppResult<(Vec<String>, bool)> {
         crate::k8s::cluster::list_project_names(&self.client, self.timeout, projects_api).await
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index 5c4caba..d66cf16 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -75,6 +75,8 @@ pub fn run() {
             commands::cluster::list_nodes,
             commands::cluster::get_node,
             commands::cluster::list_cluster_operators,
+            commands::projects::list_projects,
+            commands::projects::get_namespace_access,
         ])
         .run(tauri::generate_context!())
         .expect("error while building tauri application");
diff --git a/src-tauri/src/state.rs b/src-tauri/src/state.rs
index 712797a..8f4f1ec 100644
--- a/src-tauri/src/state.rs
+++ b/src-tauri/src/state.rs
@@ -23,6 +23,8 @@ pub struct AppState {
     pub logins: Arc<crate::auth::Logins>,
     /// Сессии watch и логов.
     pub sessions: Arc<crate::sessions::Sessions>,
+    /// Права пользователя в проектах (кэш на 5 минут).
+    pub access: Arc<crate::k8s::access::AccessCache>,
 }
 
 impl AppState {
@@ -35,6 +37,7 @@ impl AppState {
             pool: Arc::new(ClientPool::default()),
             logins: Arc::new(crate::auth::Logins::default()),
             sessions: Arc::new(crate::sessions::Sessions::default()),
+            access: Arc::new(crate::k8s::access::AccessCache::default()),
         })
     }
 
@@ -101,6 +104,23 @@ impl AppState {
         result
     }
 
+    /// Чтение в проекте: как `read`, а ответ 403 ещё и сбрасывает кэш прав этого проекта —
+    /// права могли измениться (спец. §5.4).
+    pub async fn read_in<T, F, Fut>(&self, id: &str, namespace: &str, f: F) -> AppResult<T>
+    where
+        F: FnOnce(Arc<Connected>) -> Fut,
+        Fut: Future<Output = AppResult<T>>,
+    {
+        let result = self.read(id, f).await;
+        if result
+            .as_ref()
+            .is_err_and(|e| e.code == ErrorCode::Forbidden)
+        {
+            self.access.forget(id, namespace);
+        }
+        result
+    }
+
     /// Сбрасывает клиент, если ошибка значит, что с этими учётными данными или TLS он работать
     /// не будет: следующий вызов подключится заново и подхватит новый токен.
     pub fn evict_if_broken(&self, id: &str, e: &AppError) -> bool {
@@ -109,13 +129,16 @@ impl AppState {
             ErrorCode::SessionExpired | ErrorCode::AuthFailed | ErrorCode::TlsError
         );
         if broken {
-            self.pool.evict(id);
+            self.evict_connection(id);
         }
         broken
     }
 
+    /// Сбрасывает клиент подключения и кэш его прав в проектах: со следующим подключением
+    /// (новый токен, другой профиль) права могут быть другими.
     pub fn evict_connection(&self, id: &str) {
         self.pool.evict(id);
+        self.access.forget_connection(id);
     }
 
     /// Удаляет подключение целиком: профиль, секреты, клиент и сессии. Блокирующий вызов
@@ -305,6 +328,53 @@ mod tests {
         assert_eq!(e.code, ErrorCode::NotFound);
     }
 
+    #[tokio::test]
+    async fn forbidden_read_in_a_project_forgets_its_rights() {
+        let dir = tempfile::tempdir().unwrap();
+        let st = state(dir.path());
+        let connected = crate::k8s::pool::test_connected(|_| (200, serde_json::json!({})));
+        assert!(
+            st.pool
+                .insert_if_current("a", st.pool.generation("a"), connected)
+        );
+        let a = crate::k8s::access::NamespaceAccess::default();
+        st.access.put("a", "team-a", a.clone());
+        st.access.put("a", "team-b", a.clone());
+        let ok = st.read_in("a", "team-a", |_| async { Ok(1) }).await;
+        assert_eq!(ok.unwrap(), 1);
+        assert!(st.access.get("a", "team-a").is_some());
+        let e = st
+            .read_in("a", "team-a", |_| async {
+                Err::<(), _>(crate::k8s::errors::not_allowed(
+                    "list",
+                    "pods",
+                    Some("team-a"),
+                ))
+            })
+            .await
+            .unwrap_err();
+        assert_eq!(e.code, ErrorCode::Forbidden);
+        assert!(
+            st.access.get("a", "team-a").is_none(),
+            "права проекта перечитаются"
+        );
+        assert!(st.access.get("a", "team-b").is_some());
+        assert!(st.pool.get("a").is_some(), "403 клиент не сбрасывает");
+    }
+
+    #[test]
+    fn evicting_a_client_forgets_the_rights_of_the_connection() {
+        let dir = tempfile::tempdir().unwrap();
+        let st = state(dir.path());
+        st.access.put(
+            "a",
+            "team-a",
+            crate::k8s::access::NamespaceAccess::default(),
+        );
+        st.evict_connection("a");
+        assert!(st.access.get("a", "team-a").is_none());
+    }
+
     #[test]
     fn deleting_a_connection_stops_its_sessions() {
         let dir = tempfile::tempdir().unwrap();
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 301 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 133`

- [ ] **Step 4: Commit.**

```bash
git add \
  src-tauri/src/commands/mod.rs \
  src-tauri/src/commands/projects.rs \
  src-tauri/src/k8s/access.rs \
  src-tauri/src/k8s/mod.rs \
  src-tauri/src/k8s/pool.rs \
  src-tauri/src/k8s/projects.rs \
  src-tauri/src/k8s/read.rs \
  src-tauri/src/lib.rs \
  src-tauri/src/state.rs
git commit -s -m "feat(k8s): проекты с запасным списком из профиля, права пользователя в проекте с кэшем на 5 минут" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6: Workloads: шесть видов, HPA, PDB, история выкладки

**Files:**
- Create: `src-tauri/src/commands/objects.rs`, `src-tauri/src/k8s/containers.rs`, `src-tauri/src/k8s/selectors.rs`, `src-tauri/src/k8s/workloads.rs`
- Modify: `src-tauri/src/commands/mod.rs`, `src-tauri/src/k8s/mod.rs`, `src-tauri/src/lib.rs`

**Interfaces:**
- Consumes: задачи 3–5 (`ReadClient`, `view`, `AppState::read_in`).
- Produces: `k8s::selectors::{selector_string(&LabelSelector), map_selector_string(&BTreeMap), selector_matches}`; `k8s::containers::{ContainerSpecView { name, image, init, requests, limits, ports, liveness?, readiness?, startup?, env }, EnvView { name, source, reference? }, EnvSource, probe_text, container_view, containers_of, images_of}`; `k8s::workloads::{WorkloadKind::{Deployment, StatefulSet, DaemonSet, DeploymentConfig, Job, CronJob} (+ ALL, available(&ApiGroups)), WorkloadProblem::{Unavailable, RolloutStuck, Failed}, WorkloadRow { kind, name, desired, ready, available, updated, images, created, problem, schedule?, suspended?, last_schedule? }, WorkloadRow::key, deployment_row, stateful_set_row, daemon_set_row, job_row, cron_job_row, deployment_config_row, deployment_configs(), KindError { kind, error }, WorkloadList { items, errors, truncated }, list_kind, list_workloads, HpaView, PdbView, Revision, WorkloadDetails { workload, conditions, selector, strategy, containers, labels, annotations, active_jobs, hpa, pdb, history? }, pod_selector, get_workload}`; команды `list_workloads(connectionId, namespace)`, `get_workload(connectionId, namespace, kind, name)`.

Спец. §6.3–6.4. Deployment, StatefulSet, DaemonSet, Job, CronJob и, при группе `apps.openshift.io`, DeploymentConfig (своего типа в `k8s-openapi` нет — нужные поля разбираются из `DynamicObject`). Строка списка: желаемо / готово / доступно / обновлено (у Job — нужно завершений и успешно, у CronJob — расписание, приостановлен ли, последний запуск, идущие Job), образы, возраст и проблема — доступно меньше желаемого, выкладка не уложилась в `progressDeadlineSeconds`, Job завершился ошибкой. Виды читаются параллельно; вид без прав не скрывает остальные (`errors`). Подробности: условия, селектор подов, стратегия, контейнеры шаблона (ресурсы, порты, пробы как в `kubectl describe`, имена переменных и их источники — без значений), метки и аннотации без `last-applied-configuration`, HPA по `scaleTargetRef`, PDB по селектору и меткам шаблона, история выкладки — ReplicaSets владельца с ревизией (Deployment), ControllerRevisions (StatefulSet, DaemonSet), ReplicationControllers с меткой `openshift.io/deployment-config.name` (DeploymentConfig); у Job и CronJob истории нет. Отдельная команда `get_rollout_history` не нужна: история приходит в `get_workload`.

- [ ] **Step 1: Тесты.**

<!-- block: file t06-src-tauri-src-k8s-containers-rs-tests step=6.1 path=src-tauri/src/k8s/containers.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> PodSpec {
        serde_json::from_value(serde_json::json!({
            "initContainers": [{"name": "migrate", "image": "app:1"}],
            "containers": [{
                "name": "web", "image": "registry/app:1.2",
                "ports": [{"containerPort": 8080, "name": "http"}, {"containerPort": 9090, "protocol": "UDP"}],
                "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"memory": "256Mi"}},
                "readinessProbe": {"httpGet": {"path": "/ready", "port": 8080}, "periodSeconds": 5},
                "livenessProbe": {"tcpSocket": {"port": "http"}, "initialDelaySeconds": 10},
                "startupProbe": {"exec": {"command": ["cat", "/tmp/started"]}, "failureThreshold": 30},
                "env": [
                    {"name": "MODE", "value": "prod"},
                    {"name": "LOG_LEVEL", "valueFrom": {"configMapKeyRef": {"name": "cfg", "key": "level"}}},
                    {"name": "DB_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "db", "key": "password"}}},
                    {"name": "POD", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                    {"name": "MEM", "valueFrom": {"resourceFieldRef": {"resource": "limits.memory"}}}
                ],
                "envFrom": [{"configMapRef": {"name": "common"}}, {"prefix": "S_", "secretRef": {"name": "extra"}}]
            }]
        }))
        .unwrap()
    }

    #[test]
    fn containers_with_resources_probes_and_env_sources() {
        let list = containers_of(&spec());
        assert_eq!(list.len(), 2);
        assert!(list[0].init && !list[1].init);
        let web = &list[1];
        assert_eq!(web.requests["cpu"], "100m");
        assert_eq!(web.limits.get("cpu"), None);
        assert_eq!(web.ports, ["8080/TCP (http)", "9090/UDP"]);
        assert_eq!(
            web.readiness.as_deref(),
            Some("http-get http://:8080/ready delay=0s timeout=1s period=5s #success=1 #failure=3")
        );
        assert_eq!(
            web.liveness.as_deref(),
            Some("tcp-socket :http delay=10s timeout=1s period=10s #success=1 #failure=3")
        );
        assert!(
            web.startup
                .as_deref()
                .unwrap()
                .starts_with("exec [cat /tmp/started]")
        );
        let env: Vec<_> = web
            .env
            .iter()
            .map(|e| (e.name.as_str(), e.source, e.reference.as_deref()))
            .collect();
        assert_eq!(
            env,
            [
                ("MODE", EnvSource::Literal, None),
                ("LOG_LEVEL", EnvSource::ConfigMapKeyRef, Some("cfg/level")),
                ("DB_PASSWORD", EnvSource::SecretKeyRef, Some("db/password")),
                ("POD", EnvSource::FieldRef, Some("metadata.name")),
                ("MEM", EnvSource::ResourceFieldRef, Some("limits.memory")),
                ("*", EnvSource::ConfigMapRef, Some("common")),
                ("S_*", EnvSource::SecretRef, Some("extra")),
            ]
        );
        let json = serde_json::to_string(&list).unwrap();
        assert!(
            !json.contains("prod"),
            "значения переменных не уходят: {json}"
        );
        assert_eq!(images_of(&spec()), ["registry/app:1.2"]);
    }
}
````

<!-- block: file t06-src-tauri-src-k8s-selectors-rs-tests step=6.1 path=src-tauri/src/k8s/selectors.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement;

    fn selector() -> LabelSelector {
        let req = |key: &str, op: &str, values: &[&str]| LabelSelectorRequirement {
            key: key.into(),
            operator: op.into(),
            values: (!values.is_empty()).then(|| values.iter().map(|v| v.to_string()).collect()),
        };
        LabelSelector {
            match_labels: Some(BTreeMap::from([("app".to_string(), "web".to_string())])),
            match_expressions: Some(vec![
                req("tier", "In", &["a", "b"]),
                req("env", "NotIn", &["prod"]),
                req("team", "Exists", &[]),
                req("legacy", "DoesNotExist", &[]),
            ]),
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn selector_as_a_string() {
        assert_eq!(
            selector_string(&selector()),
            "app=web,tier in (a,b),env notin (prod),team,!legacy"
        );
        assert_eq!(selector_string(&LabelSelector::default()), "");
        assert_eq!(
            map_selector_string(&labels(&[("app", "web"), ("deploymentconfig", "web")])),
            "app=web,deploymentconfig=web"
        );
    }

    #[test]
    fn labels_against_a_selector() {
        let s = selector();
        assert!(selector_matches(
            &s,
            &labels(&[("app", "web"), ("tier", "a"), ("team", "x")])
        ));
        assert!(!selector_matches(
            &s,
            &labels(&[("app", "web"), ("tier", "c"), ("team", "x")])
        ));
        assert!(!selector_matches(
            &s,
            &labels(&[
                ("app", "web"),
                ("tier", "a"),
                ("team", "x"),
                ("env", "prod")
            ])
        ));
        assert!(!selector_matches(
            &s,
            &labels(&[
                ("app", "web"),
                ("tier", "a"),
                ("team", "x"),
                ("legacy", "1")
            ])
        ));
        assert!(selector_matches(&LabelSelector::default(), &labels(&[])));
    }
}
````

<!-- block: file t06-src-tauri-src-k8s-workloads-rs-tests step=6.1 path=src-tauri/src/k8s/workloads.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use std::time::Duration;

    fn reader(client: kube::Client) -> ReadClient {
        ReadClient::new(client, Duration::from_secs(5))
    }

    fn from<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> T {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn rows_of_every_kind() {
        let d = deployment_row(&from(deployment("web", 3, 2)));
        assert_eq!(
            (d.desired, d.ready, d.available, d.updated),
            (Some(3), Some(2), Some(2), Some(3))
        );
        assert_eq!(d.images, ["registry/web:3"]);
        assert_eq!(d.problem, Some(WorkloadProblem::Unavailable));
        assert_eq!(d.key(), "Deployment/web");
        assert_eq!(deployment_row(&from(deployment("ok", 2, 2))).problem, None);
        assert_eq!(
            deployment_row(&from(deployment("zero", 0, 0))).problem,
            None
        );

        let mut stuck = deployment("stuck", 1, 1);
        stuck["status"]["conditions"] = serde_json::json!([{"type": "Progressing", "status": "False",
            "reason": "ProgressDeadlineExceeded"}]);
        assert_eq!(
            deployment_row(&from(stuck)).problem,
            Some(WorkloadProblem::RolloutStuck)
        );

        let ds: DaemonSet = from(serde_json::json!({"metadata": {"name": "agent"},
            "spec": {"selector": {"matchLabels": {"app": "agent"}}, "template": template("agent", "agent:1")},
            "status": {"desiredNumberScheduled": 4, "numberReady": 3, "numberAvailable": 3,
                "currentNumberScheduled": 4, "numberMisscheduled": 0}}));
        let row = daemon_set_row(&ds);
        assert_eq!((row.desired, row.ready), (Some(4), Some(3)));
        assert_eq!(row.problem, Some(WorkloadProblem::Unavailable));

        let job: Job = from(serde_json::json!({"metadata": {"name": "migrate"},
            "spec": {"template": template("migrate", "app:1")},
            "status": {"failed": 3, "conditions": [{"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded"}]}}));
        let row = job_row(&job);
        assert_eq!((row.desired, row.ready), (Some(1), Some(0)));
        assert_eq!(row.problem, Some(WorkloadProblem::Failed));

        let cron: CronJob = from(serde_json::json!({"metadata": {"name": "report"},
            "spec": {"schedule": "0 3 * * *", "suspend": true,
                "jobTemplate": {"spec": {"template": template("report", "report:2")}}},
            "status": {"active": [{"name": "report-1"}], "lastScheduleTime": "2026-10-25T10:00:00Z"}}));
        let row = cron_job_row(&cron);
        assert_eq!(row.schedule.as_deref(), Some("0 3 * * *"));
        assert_eq!((row.suspended, row.ready), (Some(true), Some(1)));
        assert_eq!(row.last_schedule, Some(1_792_922_400));
        assert_eq!(row.images, ["report:2"]);

        let dc: DynamicObject = from(serde_json::json!({"apiVersion": "apps.openshift.io/v1",
            "kind": "DeploymentConfig", "metadata": {"name": "legacy"},
            "spec": {"replicas": 2, "selector": {"app": "legacy"}, "strategy": {"type": "Rolling"},
                "template": template("legacy", "legacy:7")},
            "status": {"readyReplicas": 2, "availableReplicas": 2, "updatedReplicas": 2, "latestVersion": 7}}));
        let p = parse_deployment_config(&dc);
        assert_eq!(p.row.kind, WorkloadKind::DeploymentConfig);
        assert_eq!((p.row.desired, p.row.available), (Some(2), Some(2)));
        assert_eq!(p.selector.as_deref(), Some("app=legacy"));
        assert_eq!(p.strategy.as_deref(), Some("Rolling"));
        assert_eq!(p.latest_version, Some(7));
    }

    #[tokio::test]
    async fn list_of_kinds_with_a_forbidden_one() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments" => (
                200,
                list(
                    "Deployment",
                    vec![deployment("web", 1, 1), deployment("api", 1, 1)],
                ),
            ),
            "/apis/batch/v1/namespaces/team-a/cronjobs" => status(
                403,
                "Forbidden",
                r#"cronjobs.batch is forbidden: User "u" cannot list resource "cronjobs" in API group "batch" in the namespace "team-a""#,
            ),
            p if p.contains("deploymentconfigs") => panic!("без apps.openshift.io не спрашиваем"),
            _ => (200, list("X", vec![])),
        });
        let l = list_workloads(&reader(client), "team-a", &ApiGroups::default()).await;
        let names: Vec<_> = l.items.iter().map(|w| w.name.as_str()).collect();
        assert_eq!(names, ["api", "web"]);
        assert_eq!(l.errors.len(), 1);
        assert_eq!(l.errors[0].kind, WorkloadKind::CronJob);
        assert_eq!(l.errors[0].error.params["resource"], "cronjobs");
        assert_eq!(server.await.unwrap().len(), 5);
    }

    #[tokio::test]
    async fn deployment_details_with_hpa_pdb_and_history() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => (200, deployment("web", 3, 3)),
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers" => (
                200,
                list(
                    "HorizontalPodAutoscaler",
                    vec![serde_json::json!({"metadata": {"name": "web"},
                    "spec": {"scaleTargetRef": {"kind": "Deployment", "name": "web", "apiVersion": "apps/v1"},
                        "minReplicas": 2, "maxReplicas": 10,
                        "metrics": [{"type": "Resource", "resource": {"name": "cpu",
                            "target": {"type": "Utilization", "averageUtilization": 80}}}]},
                    "status": {"currentReplicas": 3, "desiredReplicas": 3,
                        "conditions": [{"type": "ScalingLimited", "status": "False"}]}})],
                ),
            ),
            "/apis/policy/v1/namespaces/team-a/poddisruptionbudgets" => (
                200,
                list(
                    "PodDisruptionBudget",
                    vec![
                        serde_json::json!({"metadata": {"name": "other"},
                        "spec": {"selector": {"matchLabels": {"app": "api"}}}}),
                        serde_json::json!({"metadata": {"name": "web"},
                        "spec": {"minAvailable": 2, "selector": {"matchLabels": {"app": "web"}}},
                        "status": {"disruptionsAllowed": 1, "currentHealthy": 3, "desiredHealthy": 2,
                            "expectedPods": 3}}),
                    ],
                ),
            ),
            "/apis/apps/v1/namespaces/team-a/replicasets" => {
                let rs = |name: &str, rev: &str, owner: &str, image: &str| {
                    serde_json::json!({"metadata": {"name": name,
                        "annotations": {"deployment.kubernetes.io/revision": rev},
                        "ownerReferences": [{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web",
                            "uid": owner, "controller": true}]},
                        "spec": {"selector": {"matchLabels": {"app": "web"}},
                            "template": template("web", image)},
                        "status": {"replicas": if rev == "3" { 3 } else { 0 }}})
                };
                (
                    200,
                    list(
                        "ReplicaSet",
                        vec![
                            rs("web-a", "2", "uid-web", "registry/web:2"),
                            rs("web-b", "3", "uid-web", "registry/web:3"),
                            rs("web-foreign", "9", "uid-other", "x"),
                        ],
                    ),
                )
            }
            other => status(404, "NotFound", other),
        });
        let d = get_workload(&reader(client), "team-a", WorkloadKind::Deployment, "web")
            .await
            .unwrap();
        assert_eq!(d.selector.as_deref(), Some("app=web"));
        assert_eq!(
            d.strategy.as_deref(),
            Some("RollingUpdate (maxSurge 1, maxUnavailable 0)")
        );
        assert_eq!(d.containers.len(), 1);
        assert!(!d.annotations.contains_key(LAST_APPLIED));
        assert_eq!(d.annotations["deployment.kubernetes.io/revision"], "3");
        let hpa = d.hpa.data.unwrap().unwrap();
        assert_eq!((hpa.min_replicas, hpa.max_replicas), (Some(2), 10));
        assert_eq!(hpa.metrics, ["cpu 80%"]);
        let pdb = d.pdb.data.unwrap().unwrap();
        assert_eq!(pdb.name, "web");
        assert_eq!(pdb.min_available.as_deref(), Some("2"));
        let history = d.history.unwrap().data.unwrap();
        let revs: Vec<_> = history.iter().map(|r| (r.revision, r.current)).collect();
        assert_eq!(
            revs,
            [(3, true), (2, false)],
            "чужой ReplicaSet не в истории"
        );
        assert_eq!(history[1].images, ["registry/web:2"]);
        let seen = server.await.unwrap();
        assert!(seen.iter().any(
            |s| s.path().ends_with("/replicasets") && s.uri.contains("labelSelector=app%3Dweb")
        ));
    }

    #[tokio::test]
    async fn deployment_config_history_and_forbidden_parts() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/apis/apps.openshift.io/v1/namespaces/team-a/deploymentconfigs/legacy" => (
                200,
                serde_json::json!({"apiVersion": "apps.openshift.io/v1", "kind": "DeploymentConfig",
                    "metadata": {"name": "legacy"},
                    "spec": {"replicas": 1, "selector": {"app": "legacy"}, "template": template("legacy", "legacy:7")},
                    "status": {"latestVersion": 7}}),
            ),
            "/api/v1/namespaces/team-a/replicationcontrollers" => {
                assert!(
                    s.uri
                        .contains("openshift.io%2Fdeployment-config.name%3Dlegacy"),
                    "{}",
                    s.uri
                );
                let rc = |n: i64| {
                    serde_json::json!({"metadata": {"name": format!("legacy-{n}"),
                        "annotations": {"openshift.io/deployment-config.latest-version": n.to_string()}},
                        "spec": {"template": template("legacy", &format!("legacy:{n}"))},
                        "status": {"replicas": if n == 7 { 1 } else { 0 }}})
                };
                (200, list("ReplicationController", vec![rc(6), rc(7)]))
            }
            _ => status(
                403,
                "Forbidden",
                r#"x is forbidden: User "u" cannot list resource "horizontalpodautoscalers" in API group "autoscaling" in the namespace "team-a""#,
            ),
        });
        let d = get_workload(
            &reader(client),
            "team-a",
            WorkloadKind::DeploymentConfig,
            "legacy",
        )
        .await
        .unwrap();
        assert!(d.hpa.error.is_some() && d.pdb.error.is_some());
        let history = d.history.unwrap().data.unwrap();
        assert_eq!(
            history
                .iter()
                .map(|r| (r.revision, r.current))
                .collect::<Vec<_>>(),
            [(7, true), (6, false)]
        );
    }

    #[tokio::test]
    async fn jobs_have_no_history_and_missing_workloads_are_not_found() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/apis/batch/v1/namespaces/team-a/jobs/migrate" => (
                200,
                serde_json::json!({"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "migrate"},
                    "spec": {"selector": {"matchLabels": {"batch.kubernetes.io/controller-uid": "u1"}},
                        "template": template("migrate", "app:1")}}),
            ),
            "/apis/apps/v1/namespaces/team-a/statefulsets/db" => {
                status(404, "NotFound", "not found")
            }
            _ => (200, list("X", vec![])),
        });
        let r = reader(client);
        let d = get_workload(&r, "team-a", WorkloadKind::Job, "migrate")
            .await
            .unwrap();
        assert!(d.history.is_none());
        assert_eq!(
            pod_selector(&r, "team-a", WorkloadKind::Job, "migrate")
                .await
                .unwrap()
                .as_deref(),
            Some("batch.kubernetes.io/controller-uid=u1")
        );
        let e = get_workload(&r, "team-a", WorkloadKind::StatefulSet, "db")
            .await
            .unwrap_err();
        assert_eq!(e.code, crate::error::ErrorCode::NotFound);
    }

    #[test]
    fn kinds_serialize_as_in_kubernetes() {
        assert_eq!(
            serde_json::to_value(WorkloadKind::DeploymentConfig).unwrap(),
            "DeploymentConfig"
        );
        let k: WorkloadKind = serde_json::from_value("CronJob".into()).unwrap();
        assert_eq!(k, WorkloadKind::CronJob);
        assert!(!WorkloadKind::DeploymentConfig.available(&ApiGroups::default()));
    }
}
````

- [ ] **Step 2: Реализация.**

<!-- block: file t06-src-tauri-src-commands-objects-rs step=6.2 path=src-tauri/src/commands/objects.rs -->
````rust
//! Объекты проекта (спец. §6.3–6.5): чтение в namespace; ответ 403 сбрасывает кэш прав проекта.

use tauri::State;

use crate::error::AppResult;
use crate::k8s::workloads::{WorkloadDetails, WorkloadKind, WorkloadList};
use crate::state::AppState;

#[tauri::command]
pub async fn list_workloads(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
) -> AppResult<WorkloadList> {
    let ns = namespace.clone();
    state
        .read_in(&connection_id, &namespace, |c| async move {
            Ok(crate::k8s::workloads::list_workloads(&c.reader(), &ns, &c.info.api_groups).await)
        })
        .await
}

#[tauri::command]
pub async fn get_workload(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
    kind: WorkloadKind,
    name: String,
) -> AppResult<WorkloadDetails> {
    let ns = namespace.clone();
    state
        .read_in(&connection_id, &namespace, |c| async move {
            crate::k8s::workloads::get_workload(&c.reader(), &ns, kind, &name).await
        })
        .await
}
````

<!-- block: file t06-src-tauri-src-k8s-containers-rs-impl step=6.2 path=src-tauri/src/k8s/containers.rs part=impl -->
````rust
//! Описание контейнеров шаблона пода (спец. §6.4–6.5): образ, requests и limits, порты, пробы,
//! имена переменных окружения и их источники. Значения переменных не показываются.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Container, PodSpec, Probe};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerSpecView {
    pub name: String,
    pub image: String,
    pub init: bool,
    pub requests: BTreeMap<String, String>,
    pub limits: BTreeMap<String, String>,
    /// `8080/TCP (http)`.
    pub ports: Vec<String>,
    pub liveness: Option<String>,
    pub readiness: Option<String>,
    pub startup: Option<String>,
    pub env: Vec<EnvView>,
}

/// Переменная окружения: имя и откуда берётся значение (само значение не показывается).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvView {
    /// У `envFrom` — `*` (все ключи источника) или префикс с `*`.
    pub name: String,
    pub source: EnvSource,
    /// Объект и ключ источника: `app-config/LOG_LEVEL`, `metadata.name`, `limits.memory`.
    pub reference: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum EnvSource {
    Literal,
    ConfigMapKeyRef,
    SecretKeyRef,
    FieldRef,
    ResourceFieldRef,
    /// `envFrom.configMapRef`: все ключи ConfigMap.
    ConfigMapRef,
    /// `envFrom.secretRef`: все ключи Secret.
    SecretRef,
}

/// Проба как в `kubectl describe`: `http-get :8080/healthz delay=5s timeout=1s period=10s
/// #success=1 #failure=3`.
pub fn probe_text(p: &Probe) -> String {
    let port = |p: &k8s_openapi::apimachinery::pkg::util::intstr::IntOrString| match p {
        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(i) => i.to_string(),
        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(s) => s.clone(),
    };
    let action = if let Some(h) = &p.http_get {
        let scheme = h.scheme.as_deref().unwrap_or("HTTP").to_lowercase();
        format!(
            "http-get {scheme}://{}:{}{}",
            h.host.as_deref().unwrap_or(""),
            port(&h.port),
            h.path.as_deref().unwrap_or("/")
        )
    } else if let Some(t) = &p.tcp_socket {
        format!(
            "tcp-socket {}:{}",
            t.host.as_deref().unwrap_or(""),
            port(&t.port)
        )
    } else if let Some(g) = &p.grpc {
        format!("grpc :{}", g.port)
    } else if let Some(e) = &p.exec {
        format!("exec [{}]", e.command.clone().unwrap_or_default().join(" "))
    } else {
        "unknown".to_string()
    };
    format!(
        "{action} delay={}s timeout={}s period={}s #success={} #failure={}",
        p.initial_delay_seconds.unwrap_or(0),
        p.timeout_seconds.unwrap_or(1),
        p.period_seconds.unwrap_or(10),
        p.success_threshold.unwrap_or(1),
        p.failure_threshold.unwrap_or(3)
    )
}

fn env_views(c: &Container) -> Vec<EnvView> {
    let mut out: Vec<EnvView> = c
        .env
        .iter()
        .flatten()
        .map(|e| {
            let (source, reference) = match &e.value_from {
                None => (EnvSource::Literal, None),
                Some(f) => {
                    if let Some(r) = &f.config_map_key_ref {
                        (
                            EnvSource::ConfigMapKeyRef,
                            Some(format!("{}/{}", r.name, r.key)),
                        )
                    } else if let Some(r) = &f.secret_key_ref {
                        (
                            EnvSource::SecretKeyRef,
                            Some(format!("{}/{}", r.name, r.key)),
                        )
                    } else if let Some(r) = &f.field_ref {
                        (EnvSource::FieldRef, Some(r.field_path.clone()))
                    } else if let Some(r) = &f.resource_field_ref {
                        (EnvSource::ResourceFieldRef, Some(r.resource.clone()))
                    } else {
                        (EnvSource::Literal, None)
                    }
                }
            };
            EnvView {
                name: e.name.clone(),
                source,
                reference,
            }
        })
        .collect();
    for f in c.env_from.iter().flatten() {
        let name = format!("{}*", f.prefix.clone().unwrap_or_default());
        if let Some(r) = &f.config_map_ref {
            out.push(EnvView {
                name: name.clone(),
                source: EnvSource::ConfigMapRef,
                reference: Some(r.name.clone()),
            });
        }
        if let Some(r) = &f.secret_ref {
            out.push(EnvView {
                name,
                source: EnvSource::SecretRef,
                reference: Some(r.name.clone()),
            });
        }
    }
    out
}

pub fn container_view(c: &Container, init: bool) -> ContainerSpecView {
    let quantities =
        |m: Option<&BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>>| {
            m.map(|m| m.iter().map(|(k, q)| (k.clone(), q.0.clone())).collect())
                .unwrap_or_default()
        };
    let resources = c.resources.as_ref();
    ContainerSpecView {
        name: c.name.clone(),
        image: c.image.clone().unwrap_or_default(),
        init,
        requests: quantities(resources.and_then(|r| r.requests.as_ref())),
        limits: quantities(resources.and_then(|r| r.limits.as_ref())),
        ports: c
            .ports
            .iter()
            .flatten()
            .map(|p| {
                let proto = p.protocol.as_deref().unwrap_or("TCP");
                match &p.name {
                    Some(n) => format!("{}/{proto} ({n})", p.container_port),
                    None => format!("{}/{proto}", p.container_port),
                }
            })
            .collect(),
        liveness: c.liveness_probe.as_ref().map(probe_text),
        readiness: c.readiness_probe.as_ref().map(probe_text),
        startup: c.startup_probe.as_ref().map(probe_text),
        env: env_views(c),
    }
}

/// Init-контейнеры, затем обычные.
pub fn containers_of(spec: &PodSpec) -> Vec<ContainerSpecView> {
    spec.init_containers
        .iter()
        .flatten()
        .map(|c| container_view(c, true))
        .chain(spec.containers.iter().map(|c| container_view(c, false)))
        .collect()
}

/// Образы обычных контейнеров (без init).
pub fn images_of(spec: &PodSpec) -> Vec<String> {
    spec.containers
        .iter()
        .filter_map(|c| c.image.clone())
        .collect()
}
````

<!-- block: file t06-src-tauri-src-k8s-selectors-rs-impl step=6.2 path=src-tauri/src/k8s/selectors.rs part=impl -->
````rust
//! Селекторы меток: строкой для `labelSelector` и проверкой меток объекта (PDB, Service).

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;

/// Селектор строкой, как его понимает `labelSelector`: `app=web,tier in (a,b),!legacy`.
/// Пустой селектор — пустая строка (подходит всё).
pub fn selector_string(s: &LabelSelector) -> String {
    let mut parts: Vec<String> = s
        .match_labels
        .iter()
        .flatten()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    for e in s.match_expressions.iter().flatten() {
        let values = e.values.clone().unwrap_or_default().join(",");
        parts.push(match e.operator.as_str() {
            "In" => format!("{} in ({values})", e.key),
            "NotIn" => format!("{} notin ({values})", e.key),
            "Exists" => e.key.clone(),
            "DoesNotExist" => format!("!{}", e.key),
            other => format!("{} {other} ({values})", e.key),
        });
    }
    parts.join(",")
}

/// Селектор из пар `ключ=значение` (Service, ReplicationController, DeploymentConfig).
pub fn map_selector_string(m: &BTreeMap<String, String>) -> String {
    m.iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Подходят ли метки под селектор. Пустой селектор подходит всем; неизвестный оператор — никому.
pub fn selector_matches(s: &LabelSelector, labels: &BTreeMap<String, String>) -> bool {
    let labels_ok = s
        .match_labels
        .iter()
        .flatten()
        .all(|(k, v)| labels.get(k) == Some(v));
    let expressions_ok = s.match_expressions.iter().flatten().all(|e| {
        let values = e.values.clone().unwrap_or_default();
        let value = labels.get(&e.key);
        match e.operator.as_str() {
            "In" => value.is_some_and(|v| values.contains(v)),
            "NotIn" => value.is_none_or(|v| !values.contains(v)),
            "Exists" => value.is_some(),
            "DoesNotExist" => value.is_none(),
            _ => false,
        }
    });
    labels_ok && expressions_ok
}
````

<!-- block: file t06-src-tauri-src-k8s-workloads-rs-impl step=6.2 path=src-tauri/src/k8s/workloads.rs part=impl -->
````rust
//! Workloads проекта (спец. §6.3–6.4): Deployment, StatefulSet, DaemonSet, DeploymentConfig
//! (при группе `apps.openshift.io`), Job и CronJob — строка списка, подробности, HPA и PDB,
//! история rollout. Вид, который не читается, не скрывает остальные: его ошибка — в `errors`.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{
    ControllerRevision, DaemonSet, Deployment, ReplicaSet, StatefulSet,
};
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{PodTemplateSpec, ReplicationController};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ListParams};
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::k8s::containers::{ContainerSpecView, containers_of, images_of};
use crate::k8s::read::{ReadClient, by_labels};
use crate::k8s::selectors::{map_selector_string, selector_matches, selector_string};
use crate::k8s::view::{ConditionView, Part, conditions, conditions_of, created, name, seconds};
use crate::platform::ApiGroups;

/// Аннотация, которую интерфейс не показывает: в ней полная копия объекта.
const LAST_APPLIED: &str = "kubectl.kubernetes.io/last-applied-configuration";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum WorkloadKind {
    Deployment,
    StatefulSet,
    DaemonSet,
    DeploymentConfig,
    Job,
    CronJob,
}

impl WorkloadKind {
    pub const ALL: [WorkloadKind; 6] = [
        WorkloadKind::Deployment,
        WorkloadKind::StatefulSet,
        WorkloadKind::DaemonSet,
        WorkloadKind::DeploymentConfig,
        WorkloadKind::Job,
        WorkloadKind::CronJob,
    ];

    /// Есть ли вид в кластере: DeploymentConfig — только при группе `apps.openshift.io`.
    pub fn available(self, groups: &ApiGroups) -> bool {
        self != WorkloadKind::DeploymentConfig || groups.deployment_configs
    }
}

pub fn deployment_configs() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("apps.openshift.io", "v1", "DeploymentConfig"),
        "deploymentconfigs",
    )
}

/// Почему workload не в норме.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkloadProblem {
    /// Доступно меньше реплик, чем желаемо.
    Unavailable,
    /// Выкладка не уложилась в `progressDeadlineSeconds`.
    RolloutStuck,
    /// Job завершился ошибкой.
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadRow {
    pub kind: WorkloadKind,
    pub name: String,
    /// Желаемо: реплик, подов DaemonSet, завершений Job; у CronJob — нет.
    pub desired: Option<i32>,
    /// Готово; у Job — успешно завершено, у CronJob — идущих Job.
    pub ready: Option<i32>,
    pub available: Option<i32>,
    pub updated: Option<i32>,
    pub images: Vec<String>,
    pub created: Option<i64>,
    pub problem: Option<WorkloadProblem>,
    /// Только у CronJob: расписание, приостановлен ли, когда запускался последний раз.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspended: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_schedule: Option<i64>,
}

impl WorkloadRow {
    /// Ключ строки в списке и в watch: `Deployment/web`.
    pub fn key(&self) -> String {
        format!("{:?}/{}", self.kind, self.name)
    }
}

/// Всё, что нужно и строке, и подробностям: разбирается одинаково для каждого вида.
struct Parsed {
    row: WorkloadRow,
    meta: ObjectMeta,
    conditions: Vec<ConditionView>,
    /// Селектор подов строкой для `labelSelector`.
    selector: Option<String>,
    template: Option<PodTemplateSpec>,
    strategy: Option<String>,
    active_jobs: Vec<String>,
    /// DeploymentConfig: номер текущей версии.
    latest_version: Option<i64>,
}

fn row(kind: WorkloadKind, meta: &ObjectMeta, template: Option<&PodTemplateSpec>) -> WorkloadRow {
    WorkloadRow {
        kind,
        name: name(meta),
        desired: None,
        ready: None,
        available: None,
        updated: None,
        images: template
            .and_then(|t| t.spec.as_ref())
            .map(images_of)
            .unwrap_or_default(),
        created: created(meta),
        problem: None,
        schedule: None,
        suspended: None,
        last_schedule: None,
    }
}

/// Доступно меньше желаемого (при желаемом больше нуля).
fn unavailable(desired: Option<i32>, available: Option<i32>) -> Option<WorkloadProblem> {
    let desired = desired.unwrap_or(0);
    (desired > 0 && available.unwrap_or(0) < desired).then_some(WorkloadProblem::Unavailable)
}

fn rolling(max_surge: Option<&IntOrString>, max_unavailable: Option<&IntOrString>) -> String {
    let v = |x: Option<&IntOrString>| match x {
        Some(IntOrString::Int(i)) => i.to_string(),
        Some(IntOrString::String(s)) => s.clone(),
        None => "25%".into(),
    };
    format!(
        "RollingUpdate (maxSurge {}, maxUnavailable {})",
        v(max_surge),
        v(max_unavailable)
    )
}

fn parse_deployment(d: &Deployment) -> Parsed {
    let spec = d.spec.as_ref();
    let status = d.status.as_ref();
    let template = spec.map(|s| s.template.clone());
    let mut r = row(WorkloadKind::Deployment, &d.metadata, template.as_ref());
    r.desired = Some(spec.and_then(|s| s.replicas).unwrap_or(1));
    r.ready = Some(status.and_then(|s| s.ready_replicas).unwrap_or(0));
    r.available = Some(status.and_then(|s| s.available_replicas).unwrap_or(0));
    r.updated = Some(status.and_then(|s| s.updated_replicas).unwrap_or(0));
    let conds = conditions_of(&status.and_then(|s| s.conditions.clone()));
    let stuck = conds.iter().any(|c| {
        c.kind == "Progressing"
            && c.status == "False"
            && c.reason.as_deref() == Some("ProgressDeadlineExceeded")
    });
    r.problem = if stuck {
        Some(WorkloadProblem::RolloutStuck)
    } else {
        unavailable(r.desired, r.available)
    };
    let strategy = spec.and_then(|s| s.strategy.as_ref()).map(|st| {
        match st.type_.as_deref().unwrap_or("RollingUpdate") {
            "RollingUpdate" => {
                let ru = st.rolling_update.as_ref();
                rolling(
                    ru.and_then(|r| r.max_surge.as_ref()),
                    ru.and_then(|r| r.max_unavailable.as_ref()),
                )
            }
            other => other.to_string(),
        }
    });
    let selector = spec.map(|s| selector_string(&s.selector));
    Parsed {
        row: r,
        meta: d.metadata.clone(),
        conditions: conds,
        selector,
        template,
        strategy,
        active_jobs: vec![],
        latest_version: None,
    }
}

fn parse_stateful_set(s: &StatefulSet) -> Parsed {
    let spec = s.spec.as_ref();
    let status = s.status.as_ref();
    let template = spec.map(|s| s.template.clone());
    let mut r = row(WorkloadKind::StatefulSet, &s.metadata, template.as_ref());
    r.desired = Some(spec.and_then(|s| s.replicas).unwrap_or(1));
    r.ready = Some(status.and_then(|s| s.ready_replicas).unwrap_or(0));
    r.available = Some(status.and_then(|s| s.available_replicas).unwrap_or(0));
    r.updated = Some(status.and_then(|s| s.updated_replicas).unwrap_or(0));
    r.problem = unavailable(r.desired, r.ready);
    let selector = spec.map(|s| selector_string(&s.selector));
    Parsed {
        row: r,
        meta: s.metadata.clone(),
        conditions: conditions_of(&status.and_then(|s| s.conditions.clone())),
        selector,
        template,
        strategy: spec
            .and_then(|s| s.update_strategy.as_ref())
            .and_then(|u| u.type_.clone()),
        active_jobs: vec![],
        latest_version: None,
    }
}

fn parse_daemon_set(d: &DaemonSet) -> Parsed {
    let spec = d.spec.as_ref();
    let status = d.status.as_ref();
    let template = spec.map(|s| s.template.clone());
    let mut r = row(WorkloadKind::DaemonSet, &d.metadata, template.as_ref());
    r.desired = status.map(|s| s.desired_number_scheduled);
    r.ready = status.map(|s| s.number_ready);
    r.available = Some(status.and_then(|s| s.number_available).unwrap_or(0));
    r.updated = Some(status.and_then(|s| s.updated_number_scheduled).unwrap_or(0));
    r.problem = unavailable(r.desired, r.available);
    let selector = spec.map(|s| selector_string(&s.selector));
    Parsed {
        row: r,
        meta: d.metadata.clone(),
        conditions: conditions_of(&status.and_then(|s| s.conditions.clone())),
        selector,
        template,
        strategy: spec
            .and_then(|s| s.update_strategy.as_ref())
            .and_then(|u| u.type_.clone()),
        active_jobs: vec![],
        latest_version: None,
    }
}

fn parse_job(j: &Job) -> Parsed {
    let spec = j.spec.as_ref();
    let status = j.status.as_ref();
    let template = spec.map(|s| s.template.clone());
    let mut r = row(WorkloadKind::Job, &j.metadata, template.as_ref());
    r.desired = Some(spec.and_then(|s| s.completions).unwrap_or(1));
    r.ready = Some(status.and_then(|s| s.succeeded).unwrap_or(0));
    let conds = conditions_of(&status.and_then(|s| s.conditions.clone()));
    if conds.iter().any(|c| c.kind == "Failed" && c.is_true()) {
        r.problem = Some(WorkloadProblem::Failed);
    }
    let selector = spec.and_then(|s| s.selector.as_ref()).map(selector_string);
    Parsed {
        row: r,
        meta: j.metadata.clone(),
        conditions: conds,
        selector,
        template,
        strategy: None,
        active_jobs: vec![],
        latest_version: None,
    }
}

fn parse_cron_job(c: &CronJob) -> Parsed {
    let spec = c.spec.as_ref();
    let status = c.status.as_ref();
    let template = spec.and_then(|s| s.job_template.spec.as_ref().map(|j| j.template.clone()));
    let mut r = row(WorkloadKind::CronJob, &c.metadata, template.as_ref());
    let active: Vec<String> = status
        .and_then(|s| s.active.as_ref())
        .map(|a| a.iter().filter_map(|o| o.name.clone()).collect())
        .unwrap_or_default();
    r.ready = Some(active.len() as i32);
    r.schedule = spec.map(|s| s.schedule.clone());
    r.suspended = Some(spec.and_then(|s| s.suspend) == Some(true));
    r.last_schedule = status
        .and_then(|s| s.last_schedule_time.as_ref())
        .map(seconds);
    Parsed {
        row: r,
        meta: c.metadata.clone(),
        conditions: vec![],
        selector: None,
        template,
        strategy: spec.and_then(|s| s.concurrency_policy.clone()),
        active_jobs: active,
        latest_version: None,
    }
}

/// Поля DeploymentConfig, которые нужны Glafira (своего типа в `k8s-openapi` нет).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeploymentConfigObject {
    #[serde(default)]
    metadata: ObjectMeta,
    #[serde(default)]
    spec: DcSpec,
    #[serde(default)]
    status: DcStatus,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DcSpec {
    replicas: Option<i32>,
    selector: Option<BTreeMap<String, String>>,
    strategy: Option<DcStrategy>,
    template: Option<PodTemplateSpec>,
}

#[derive(Debug, Default, Deserialize)]
struct DcStrategy {
    #[serde(rename = "type")]
    kind: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DcStatus {
    ready_replicas: Option<i32>,
    available_replicas: Option<i32>,
    updated_replicas: Option<i32>,
    latest_version: Option<i64>,
    #[serde(default)]
    conditions: serde_json::Value,
}

fn parse_deployment_config(o: &DynamicObject) -> Parsed {
    let dc: DeploymentConfigObject = serde_json::to_value(o)
        .and_then(serde_json::from_value)
        .unwrap_or_default();
    let mut r = row(
        WorkloadKind::DeploymentConfig,
        &dc.metadata,
        dc.spec.template.as_ref(),
    );
    r.desired = Some(dc.spec.replicas.unwrap_or(1));
    r.ready = Some(dc.status.ready_replicas.unwrap_or(0));
    r.available = Some(dc.status.available_replicas.unwrap_or(0));
    r.updated = Some(dc.status.updated_replicas.unwrap_or(0));
    let conds = conditions(&dc.status.conditions);
    let stuck = conds.iter().any(|c| {
        c.kind == "Progressing"
            && c.status == "False"
            && c.reason.as_deref() == Some("ProgressDeadlineExceeded")
    });
    r.problem = if stuck {
        Some(WorkloadProblem::RolloutStuck)
    } else {
        unavailable(r.desired, r.available)
    };
    Parsed {
        row: r,
        conditions: conds,
        selector: dc.spec.selector.as_ref().map(map_selector_string),
        template: dc.spec.template,
        strategy: dc.spec.strategy.and_then(|s| s.kind),
        active_jobs: vec![],
        latest_version: dc.status.latest_version,
        meta: dc.metadata,
    }
}

pub fn deployment_row(d: &Deployment) -> WorkloadRow {
    parse_deployment(d).row
}
pub fn stateful_set_row(s: &StatefulSet) -> WorkloadRow {
    parse_stateful_set(s).row
}
pub fn daemon_set_row(d: &DaemonSet) -> WorkloadRow {
    parse_daemon_set(d).row
}
pub fn job_row(j: &Job) -> WorkloadRow {
    parse_job(j).row
}
pub fn cron_job_row(c: &CronJob) -> WorkloadRow {
    parse_cron_job(c).row
}
pub fn deployment_config_row(o: &DynamicObject) -> WorkloadRow {
    parse_deployment_config(o).row
}

/// Вид, который не прочитался (например, нет прав на `cronjobs`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KindError {
    pub kind: WorkloadKind,
    pub error: AppError,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadList {
    pub items: Vec<WorkloadRow>,
    pub errors: Vec<KindError>,
    pub truncated: bool,
}

/// Строки одного вида.
pub async fn list_kind(
    r: &ReadClient,
    namespace: &str,
    kind: WorkloadKind,
) -> AppResult<(Vec<WorkloadRow>, bool)> {
    let lp = ListParams::default();
    let ns = Some(namespace);
    macro_rules! typed {
        ($t:ty, $f:ident) => {{
            let l = r.list::<$t>(ns, &lp).await?;
            (l.items.iter().map($f).collect(), l.truncated)
        }};
    }
    Ok(match kind {
        WorkloadKind::Deployment => typed!(Deployment, deployment_row),
        WorkloadKind::StatefulSet => typed!(StatefulSet, stateful_set_row),
        WorkloadKind::DaemonSet => typed!(DaemonSet, daemon_set_row),
        WorkloadKind::Job => typed!(Job, job_row),
        WorkloadKind::CronJob => typed!(CronJob, cron_job_row),
        WorkloadKind::DeploymentConfig => {
            let l = r.list_dynamic(&deployment_configs(), ns, &lp).await?;
            (
                l.items.iter().map(deployment_config_row).collect(),
                l.truncated,
            )
        }
    })
}

/// Все workloads проекта; виды читаются параллельно, ошибка одного вида не скрывает остальные.
pub async fn list_workloads(r: &ReadClient, namespace: &str, groups: &ApiGroups) -> WorkloadList {
    let kinds: Vec<WorkloadKind> = WorkloadKind::ALL
        .into_iter()
        .filter(|k| k.available(groups))
        .collect();
    let results =
        futures::future::join_all(kinds.iter().map(|k| list_kind(r, namespace, *k))).await;
    let mut list = WorkloadList {
        items: Vec::new(),
        errors: Vec::new(),
        truncated: false,
    };
    for (kind, result) in kinds.into_iter().zip(results) {
        match result {
            Ok((rows, truncated)) => {
                list.items.extend(rows);
                list.truncated |= truncated;
            }
            Err(error) => list.errors.push(KindError { kind, error }),
        }
    }
    list.items
        .sort_by(|a, b| a.name.cmp(&b.name).then(a.kind.cmp(&b.kind)));
    list
}

/// HPA, который масштабирует workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HpaView {
    pub name: String,
    pub min_replicas: Option<i32>,
    pub max_replicas: i32,
    pub current_replicas: Option<i32>,
    pub desired_replicas: i32,
    /// `cpu 80%`, `memory 70%`, `pods requests_per_second 100`…
    pub metrics: Vec<String>,
    pub conditions: Vec<ConditionView>,
}

fn hpa_view(h: &HorizontalPodAutoscaler) -> HpaView {
    let spec = h.spec.as_ref();
    let status = h.status.as_ref();
    let target = |t: &k8s_openapi::api::autoscaling::v2::MetricTarget| {
        if let Some(u) = t.average_utilization {
            format!("{u}%")
        } else if let Some(v) = &t.average_value {
            v.0.clone()
        } else if let Some(v) = &t.value {
            v.0.clone()
        } else {
            "—".into()
        }
    };
    let metrics = spec
        .and_then(|s| s.metrics.as_ref())
        .map(|ms| {
            ms.iter()
                .map(|m| {
                    if let Some(r) = &m.resource {
                        format!("{} {}", r.name, target(&r.target))
                    } else if let Some(c) = &m.container_resource {
                        format!("{} {} ({})", c.name, target(&c.target), c.container)
                    } else if let Some(p) = &m.pods {
                        format!("pods {} {}", p.metric.name, target(&p.target))
                    } else if let Some(o) = &m.object {
                        format!("object {} {}", o.metric.name, target(&o.target))
                    } else if let Some(e) = &m.external {
                        format!("external {} {}", e.metric.name, target(&e.target))
                    } else {
                        m.type_.clone()
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    HpaView {
        name: name(&h.metadata),
        min_replicas: spec.and_then(|s| s.min_replicas),
        max_replicas: spec.map_or(0, |s| s.max_replicas),
        current_replicas: status.and_then(|s| s.current_replicas),
        desired_replicas: status.map_or(0, |s| s.desired_replicas),
        metrics,
        conditions: conditions_of(&status.and_then(|s| s.conditions.clone())),
    }
}

/// PDB, который защищает поды workload'а.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PdbView {
    pub name: String,
    pub min_available: Option<String>,
    pub max_unavailable: Option<String>,
    pub allowed_disruptions: i32,
    pub current_healthy: i32,
    pub desired_healthy: i32,
    pub expected_pods: i32,
}

fn int_or_string(v: &IntOrString) -> String {
    match v {
        IntOrString::Int(i) => i.to_string(),
        IntOrString::String(s) => s.clone(),
    }
}

fn pdb_view(p: &PodDisruptionBudget) -> PdbView {
    let spec = p.spec.as_ref();
    let status = p.status.as_ref();
    PdbView {
        name: name(&p.metadata),
        min_available: spec
            .and_then(|s| s.min_available.as_ref())
            .map(int_or_string),
        max_unavailable: spec
            .and_then(|s| s.max_unavailable.as_ref())
            .map(int_or_string),
        allowed_disruptions: status.map_or(0, |s| s.disruptions_allowed),
        current_healthy: status.map_or(0, |s| s.current_healthy),
        desired_healthy: status.map_or(0, |s| s.desired_healthy),
        expected_pods: status.map_or(0, |s| s.expected_pods),
    }
}

/// Ревизия выкладки: ReplicaSet, ControllerRevision или ReplicationController.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Revision {
    pub revision: i64,
    pub name: String,
    pub images: Vec<String>,
    pub created: Option<i64>,
    pub replicas: Option<i32>,
    pub current: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadDetails {
    pub workload: WorkloadRow,
    pub conditions: Vec<ConditionView>,
    pub selector: Option<String>,
    pub strategy: Option<String>,
    pub containers: Vec<ContainerSpecView>,
    pub labels: BTreeMap<String, String>,
    /// Без `kubectl.kubernetes.io/last-applied-configuration`.
    pub annotations: BTreeMap<String, String>,
    /// Только у CronJob: идущие Job.
    pub active_jobs: Vec<String>,
    pub hpa: Part<Option<HpaView>>,
    pub pdb: Part<Option<PdbView>>,
    /// История выкладки; у Job и CronJob её нет.
    pub history: Option<Part<Vec<Revision>>>,
}

fn owned_by(meta: &ObjectMeta, uid: Option<&str>) -> bool {
    uid.is_some_and(|uid| meta.owner_references.iter().flatten().any(|o| o.uid == uid))
}

fn annotation_i64(meta: &ObjectMeta, key: &str) -> Option<i64> {
    meta.annotations
        .as_ref()
        .and_then(|a| a.get(key))
        .and_then(|v| v.parse().ok())
}

async fn history(r: &ReadClient, namespace: &str, p: &Parsed) -> Option<Part<Vec<Revision>>> {
    let uid = p.meta.uid.as_deref();
    let lp = by_labels(p.selector.as_deref());
    let ns = Some(namespace);
    let sorted = |mut v: Vec<Revision>| {
        v.sort_by_key(|r| std::cmp::Reverse(r.revision));
        v
    };
    let result: AppResult<Vec<Revision>> = match p.row.kind {
        WorkloadKind::Deployment => {
            let current = annotation_i64(&p.meta, "deployment.kubernetes.io/revision");
            r.list::<ReplicaSet>(ns, &lp).await.map(|l| {
                sorted(
                    l.items
                        .iter()
                        .filter(|rs| owned_by(&rs.metadata, uid))
                        .map(|rs| {
                            let revision =
                                annotation_i64(&rs.metadata, "deployment.kubernetes.io/revision")
                                    .unwrap_or(0);
                            Revision {
                                revision,
                                name: name(&rs.metadata),
                                images: rs
                                    .spec
                                    .as_ref()
                                    .and_then(|s| s.template.as_ref())
                                    .and_then(|t| t.spec.as_ref())
                                    .map(images_of)
                                    .unwrap_or_default(),
                                created: created(&rs.metadata),
                                replicas: rs.status.as_ref().map(|s| s.replicas),
                                current: Some(revision) == current,
                            }
                        })
                        .collect(),
                )
            })
        }
        WorkloadKind::StatefulSet | WorkloadKind::DaemonSet => {
            r.list::<ControllerRevision>(ns, &lp).await.map(|l| {
                let mut v = sorted(
                    l.items
                        .iter()
                        .filter(|cr| owned_by(&cr.metadata, uid))
                        .map(|cr| Revision {
                            revision: cr.revision,
                            name: name(&cr.metadata),
                            images: cr
                                .data
                                .as_ref()
                                .and_then(|d| {
                                    d.0["spec"]["template"]["spec"]["containers"]
                                        .as_array()
                                        .cloned()
                                })
                                .map(|cs| {
                                    cs.iter()
                                        .filter_map(|c| c["image"].as_str().map(String::from))
                                        .collect()
                                })
                                .unwrap_or_default(),
                            created: created(&cr.metadata),
                            replicas: None,
                            current: false,
                        })
                        .collect(),
                );
                if let Some(first) = v.first_mut() {
                    first.current = true;
                }
                v
            })
        }
        WorkloadKind::DeploymentConfig => {
            let lp = ListParams::default().labels(&format!(
                "openshift.io/deployment-config.name={}",
                p.row.name
            ));
            r.list::<ReplicationController>(ns, &lp).await.map(|l| {
                sorted(
                    l.items
                        .iter()
                        .map(|rc| {
                            let revision = annotation_i64(
                                &rc.metadata,
                                "openshift.io/deployment-config.latest-version",
                            )
                            .unwrap_or(0);
                            Revision {
                                revision,
                                name: name(&rc.metadata),
                                images: rc
                                    .spec
                                    .as_ref()
                                    .and_then(|s| s.template.as_ref())
                                    .and_then(|t| t.spec.as_ref())
                                    .map(images_of)
                                    .unwrap_or_default(),
                                created: created(&rc.metadata),
                                replicas: rc.status.as_ref().map(|s| s.replicas),
                                current: Some(revision) == p.latest_version,
                            }
                        })
                        .collect(),
                )
            })
        }
        WorkloadKind::Job | WorkloadKind::CronJob => return None,
    };
    Some(result.into())
}

async fn parsed(
    r: &ReadClient,
    namespace: &str,
    kind: WorkloadKind,
    workload: &str,
) -> AppResult<Parsed> {
    Ok(match kind {
        WorkloadKind::Deployment => parse_deployment(&r.get(namespace, workload).await?),
        WorkloadKind::StatefulSet => parse_stateful_set(&r.get(namespace, workload).await?),
        WorkloadKind::DaemonSet => parse_daemon_set(&r.get(namespace, workload).await?),
        WorkloadKind::Job => parse_job(&r.get(namespace, workload).await?),
        WorkloadKind::CronJob => parse_cron_job(&r.get(namespace, workload).await?),
        WorkloadKind::DeploymentConfig => parse_deployment_config(
            &r.get_dynamic(&deployment_configs(), Some(namespace), workload)
                .await?,
        ),
    })
}

/// Селектор подов workload'а строкой (для списка его подов).
pub async fn pod_selector(
    r: &ReadClient,
    namespace: &str,
    kind: WorkloadKind,
    workload: &str,
) -> AppResult<Option<String>> {
    Ok(parsed(r, namespace, kind, workload).await?.selector)
}

/// Подробности workload'а: сам объект, затем параллельно HPA, PDB и история.
pub async fn get_workload(
    r: &ReadClient,
    namespace: &str,
    kind: WorkloadKind,
    workload: &str,
) -> AppResult<WorkloadDetails> {
    let p = parsed(r, namespace, kind, workload).await?;
    let ns = Some(namespace);
    let hpa = async {
        r.list::<HorizontalPodAutoscaler>(ns, &ListParams::default())
            .await
            .map(|l| {
                l.items
                    .iter()
                    .find(|h| {
                        h.spec.as_ref().is_some_and(|s| {
                            s.scale_target_ref.kind == format!("{:?}", p.row.kind)
                                && s.scale_target_ref.name == p.row.name
                        })
                    })
                    .map(hpa_view)
            })
            .into()
    };
    let template_labels = p
        .template
        .as_ref()
        .and_then(|t| t.metadata.as_ref())
        .and_then(|m| m.labels.clone())
        .unwrap_or_default();
    let pdb = async {
        r.list::<PodDisruptionBudget>(ns, &ListParams::default())
            .await
            .map(|l| {
                l.items
                    .iter()
                    .find(|b| {
                        !template_labels.is_empty()
                            && b.spec
                                .as_ref()
                                .and_then(|s| s.selector.as_ref())
                                .is_some_and(|s| selector_matches(s, &template_labels))
                    })
                    .map(pdb_view)
            })
            .into()
    };
    let (hpa, pdb, history) = tokio::join!(hpa, pdb, history(r, namespace, &p));
    let mut annotations = p.meta.annotations.clone().unwrap_or_default();
    annotations.remove(LAST_APPLIED);
    Ok(WorkloadDetails {
        conditions: p.conditions.clone(),
        selector: p.selector.clone(),
        strategy: p.strategy.clone(),
        containers: p
            .template
            .as_ref()
            .and_then(|t| t.spec.as_ref())
            .map(containers_of)
            .unwrap_or_default(),
        labels: p.meta.labels.clone().unwrap_or_default(),
        annotations,
        active_jobs: p.active_jobs.clone(),
        hpa,
        pdb,
        history,
        workload: p.row,
    })
}

#[cfg(test)]
pub(crate) mod fixtures {
    pub fn template(app: &str, image: &str) -> serde_json::Value {
        serde_json::json!({"metadata": {"labels": {"app": app}},
            "spec": {"containers": [{"name": app, "image": image}]}})
    }

    pub fn deployment(name: &str, desired: i32, available: i32) -> serde_json::Value {
        serde_json::json!({"apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": name, "namespace": "team-a", "uid": format!("uid-{name}"),
                "creationTimestamp": "2026-10-25T10:00:00Z",
                "annotations": {"deployment.kubernetes.io/revision": "3",
                    "kubectl.kubernetes.io/last-applied-configuration": "{…}"}},
            "spec": {"replicas": desired, "selector": {"matchLabels": {"app": name}},
                "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 1, "maxUnavailable": "0"}},
                "template": template(name, "registry/web:3")},
            "status": {"replicas": desired, "readyReplicas": available, "availableReplicas": available,
                "updatedReplicas": desired,
                "conditions": [{"type": "Available", "status": "True"}]}})
    }

    pub fn list(kind: &str, items: Vec<serde_json::Value>) -> serde_json::Value {
        serde_json::json!({"apiVersion": "v1", "kind": format!("{kind}List"), "metadata": {}, "items": items})
    }
}
````

<!-- block: patch t06-src step=6.2 -->
````diff
diff --git a/src-tauri/src/commands/mod.rs b/src-tauri/src/commands/mod.rs
index 82e4280..722f053 100644
--- a/src-tauri/src/commands/mod.rs
+++ b/src-tauri/src/commands/mod.rs
@@ -4,6 +4,7 @@ pub mod cluster;
 pub mod connections;
 pub mod folders;
 pub mod kubeconfig;
+pub mod objects;
 pub mod projects;
 pub mod sessions;
 pub mod settings;
diff --git a/src-tauri/src/k8s/mod.rs b/src-tauri/src/k8s/mod.rs
index 2e802c1..9ab392d 100644
--- a/src-tauri/src/k8s/mod.rs
+++ b/src-tauri/src/k8s/mod.rs
@@ -4,6 +4,7 @@ pub mod access;
 pub mod cluster;
 pub mod config;
 pub mod connectivity;
+pub mod containers;
 pub mod errors;
 pub mod kubeconfig;
 pub mod nodes;
@@ -12,6 +13,8 @@ pub mod overview;
 pub mod pool;
 pub mod projects;
 pub mod read;
+pub mod selectors;
 #[cfg(test)]
 pub(crate) mod testing;
 pub mod view;
+pub mod workloads;
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index d66cf16..23a6058 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -77,6 +77,8 @@ pub fn run() {
             commands::cluster::list_cluster_operators,
             commands::projects::list_projects,
             commands::projects::get_namespace_access,
+            commands::objects::list_workloads,
+            commands::objects::get_workload,
         ])
         .run(tauri::generate_context!())
         .expect("error while building tauri application");
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 310 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 133`

- [ ] **Step 4: Commit.**

```bash
git add \
  src-tauri/src/commands/mod.rs \
  src-tauri/src/commands/objects.rs \
  src-tauri/src/k8s/containers.rs \
  src-tauri/src/k8s/mod.rs \
  src-tauri/src/k8s/selectors.rs \
  src-tauri/src/k8s/workloads.rs \
  src-tauri/src/lib.rs
git commit -s -m "feat(k8s): workloads проекта — шесть видов, HPA, PDB, история rollout, контейнеры шаблона" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 7: Поды и события

**Files:**
- Create: `src-tauri/src/k8s/events.rs`, `src-tauri/src/k8s/pods.rs`
- Modify: `src-tauri/src/commands/objects.rs`, `src-tauri/src/k8s/mod.rs`, `src-tauri/src/lib.rs`

**Interfaces:**
- Consumes: задачи 3–6 (`ReadClient`, `containers`, `workloads::pod_selector`).
- Produces: `k8s::pods::{PodProblem::{ImagePull, OomKilled, CrashLoop, Pending, Failed}, OwnerRef { kind, name }, PodRow { name, phase, status, problem, ready, containers, restarts, last_restart, node, created, qos, owner }, pod_status, pod_problem, owner_of, pod_row, WorkloadRef { kind, name }, list_pods(&ReadClient, ns, Option<&WorkloadRef>), StateView, ContainerPhase, ContainerStatusView (поля ContainerSpecView + image_id, state, last_state, ready, restarts), VolumeView, PodDetails, pod_details, get_pod}`; `k8s::events::{EventRow { key, type, reason, object, message, count, first_seen, last_seen, source }, event_row, EventObject { kind, name }, concerns, sort_events, list_events(&ReadClient, ns, Option<&EventObject>)}`; команды `list_pods(connectionId, namespace, workload?)`, `get_pod`, `list_events(connectionId, namespace, object?)`.

Спец. §6.3, §6.5. Статус пода — по правилам `kubectl get pods` (init-контейнеры, причина ожидания или завершения, `Terminating`). Проблема — то, что видно сейчас: образ не скачивается, контейнер завершён по памяти сейчас или в прошлый раз, перезапуск в цикле, `Pending`, `Failed`. Владелец — workload, если его можно узнать (ReplicaSet с `pod-template-hash` — Deployment, ReplicationController с меткой DeploymentConfig — DeploymentConfig), иначе прямой владелец. Поды workload'а — по его селектору, а у CronJob — поды его Job. Подробности пода: условия, IP, сервисная учётная запись, тома, и по каждому контейнеру (init тоже) — образ и digest, состояние и прошлое состояние с причиной, кодом выхода и временем, рестарты, пробы, ресурсы, имена переменных окружения и источники. События: время — из серии, `lastTimestamp`, `eventTime` или времени создания; события пода выбираются на сервере (`fieldSelector`), события workload'а — вместе с его подами, ReplicaSet и Job (по префиксу имени); новые сверху.

- [ ] **Step 1: Тесты.**

<!-- block: file t07-src-tauri-src-k8s-events-rs-tests step=7.1 path=src-tauri/src/k8s/events.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;
    use crate::k8s::testing::mock_client;
    use std::time::Duration;

    #[test]
    fn rows_take_time_from_whatever_the_event_has() {
        let e: Event = serde_json::from_value(event(
            "u1",
            "Warning",
            ("Pod", "web-1"),
            "2026-10-25T10:00:00Z",
        ))
        .unwrap();
        let row = event_row(&e);
        assert_eq!(row.kind, "Warning");
        assert_eq!((row.count, row.last_seen), (5, Some(1_792_922_400)));
        assert_eq!(row.first_seen, Some(1_792_922_400 - 1800));
        assert_eq!(row.source.as_deref(), Some("kubelet, w1"));
        let series: Event = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "x", "creationTimestamp": "2026-10-25T09:00:00Z"},
            "involvedObject": {"kind": "Pod", "name": "a"},
            "eventTime": "2026-10-25T09:10:00.000000Z", "reportingComponent": "scheduler",
            "series": {"count": 12, "lastObservedTime": "2026-10-25T09:20:00.000000Z"}}))
        .unwrap();
        let row = event_row(&series);
        assert_eq!(row.count, 12);
        assert_eq!(row.last_seen, Some(1_792_922_400 - 2400));
        assert_eq!(row.first_seen, Some(1_792_922_400 - 3000));
        assert_eq!(row.source.as_deref(), Some("scheduler"));
        assert_eq!(row.kind, "Normal");
        assert_eq!(row.key, "x", "без uid — по имени");
    }

    #[test]
    fn events_of_a_workload_include_its_children() {
        let row = |kind: &str, name: &str| {
            event_row(
                &serde_json::from_value(event("u", "Normal", (kind, name), "2026-10-25T10:00:00Z"))
                    .unwrap(),
            )
        };
        let web = EventObject {
            kind: "Deployment".into(),
            name: "web".into(),
        };
        assert!(concerns(&row("Deployment", "web"), &web));
        assert!(concerns(&row("ReplicaSet", "web-7d9f"), &web));
        assert!(concerns(&row("Pod", "web-7d9f-abc"), &web));
        assert!(
            !concerns(&row("Pod", "webhook-1"), &web),
            "только по префиксу «web-»"
        );
        assert!(!concerns(&row("Service", "web-svc"), &web));
        let pod = EventObject {
            kind: "Pod".into(),
            name: "web-1".into(),
        };
        assert!(
            !concerns(&row("Pod", "web-1-x"), &pod),
            "у пода — только его события"
        );
    }

    #[tokio::test]
    async fn events_of_a_pod_are_selected_by_the_server_and_sorted() {
        let (client, server) = mock_client(|_| {
            (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "EventList", "metadata": {}, "items": [
                    event("old", "Normal", ("Pod", "web-1"), "2026-10-25T09:00:00Z"),
                    event("new", "Warning", ("Pod", "web-1"), "2026-10-25T10:00:00Z")
                ]}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let pod = EventObject {
            kind: "Pod".into(),
            name: "web-1".into(),
        };
        let l = list_events(&r, "team-a", Some(&pod)).await.unwrap();
        let keys: Vec<_> = l.items.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, ["new", "old"]);
        list_events(&r, "team-a", None).await.unwrap();
        drop(r);
        let seen = server.await.unwrap();
        assert!(
            seen[0]
                .uri
                .contains("fieldSelector=involvedObject.kind%3DPod%2CinvolvedObject.name%3Dweb-1"),
            "{}",
            seen[0].uri
        );
        assert!(!seen[1].uri.contains("fieldSelector"));
    }
}
````

<!-- block: file t07-src-tauri-src-k8s-pods-rs-tests step=7.1 path=src-tauri/src/k8s/pods.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;
    use crate::k8s::testing::mock_client;
    use std::time::Duration;

    fn from(v: serde_json::Value) -> Pod {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn status_and_problem_like_kubectl() {
        let cases = [
            (running("a"), "Running", None),
            (
                crash_looping("b"),
                "CrashLoopBackOff",
                Some(PodProblem::CrashLoop),
            ),
            (
                pod(
                    "c",
                    "Pending",
                    serde_json::json!({"waiting": {"reason": "ImagePullBackOff"}}),
                ),
                "ImagePullBackOff",
                Some(PodProblem::ImagePull),
            ),
            (
                pod(
                    "d",
                    "Running",
                    serde_json::json!({"terminated": {"exitCode": 137, "reason": "OOMKilled"}}),
                ),
                "OOMKilled",
                Some(PodProblem::OomKilled),
            ),
            (
                pod(
                    "e",
                    "Succeeded",
                    serde_json::json!({"terminated": {"exitCode": 0, "reason": "Completed"}}),
                ),
                "Completed",
                None,
            ),
            (
                pod(
                    "f",
                    "Failed",
                    serde_json::json!({"terminated": {"exitCode": 2}}),
                ),
                "ExitCode:2",
                Some(PodProblem::Failed),
            ),
        ];
        for (p, status, problem) in cases {
            let p = from(p);
            assert_eq!(pod_status(&p), status, "{:?}", p.metadata.name);
            assert_eq!(pod_problem(&p), problem, "{:?}", p.metadata.name);
        }
        let mut pending = from(pod("g", "Pending", serde_json::json!({})));
        pending.status.as_mut().unwrap().container_statuses = None;
        assert_eq!(pod_status(&pending), "Pending");
        assert_eq!(pod_problem(&pending), Some(PodProblem::Pending));
        let mut deleting = from(running("h"));
        deleting.metadata.deletion_timestamp =
            Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                "2026-10-25T10:00:00Z".parse().unwrap(),
            ));
        assert_eq!(pod_status(&deleting), "Terminating");
    }

    #[test]
    fn oom_in_the_last_state_is_a_problem_even_when_running_again() {
        let mut p = running("a");
        p["status"]["containerStatuses"][0]["lastState"] =
            serde_json::json!({"terminated": {"exitCode": 137, "reason": "OOMKilled"}});
        assert_eq!(pod_problem(&from(p)), Some(PodProblem::OomKilled));
    }

    #[test]
    fn init_containers_like_kubectl() {
        let mut p = running("a");
        p["spec"]["initContainers"] = serde_json::json!([{"name": "i1"}, {"name": "i2"}]);
        p["status"]["phase"] = "Pending".into();
        p["status"]["initContainerStatuses"] = serde_json::json!([
            {"name": "i1", "image": "x", "imageID": "", "ready": true, "restartCount": 0,
                "state": {"terminated": {"exitCode": 0}}},
            {"name": "i2", "image": "x", "imageID": "", "ready": false, "restartCount": 0,
                "state": {"running": {}}}
        ]);
        assert_eq!(pod_status(&from(p.clone())), "Init:1/2");
        p["status"]["initContainerStatuses"][1]["state"] =
            serde_json::json!({"waiting": {"reason": "CrashLoopBackOff"}});
        let pod = from(p);
        assert_eq!(pod_status(&pod), "Init:CrashLoopBackOff");
        assert_eq!(pod_problem(&pod), Some(PodProblem::CrashLoop));
    }

    #[test]
    fn owners_are_workloads_when_known() {
        assert_eq!(
            owner_of(&from(running("web-7d9f-x"))),
            Some(OwnerRef {
                kind: "Deployment".into(),
                name: "web".into()
            })
        );
        let mut dc = running("legacy-7-abc");
        dc["metadata"]["labels"] =
            serde_json::json!({"openshift.io/deployment-config.name": "legacy"});
        dc["metadata"]["ownerReferences"][0]["kind"] = "ReplicationController".into();
        dc["metadata"]["ownerReferences"][0]["name"] = "legacy-7".into();
        assert_eq!(owner_of(&from(dc)).unwrap().kind, "DeploymentConfig");
        let mut ss = running("db-0");
        ss["metadata"]["ownerReferences"][0]["kind"] = "StatefulSet".into();
        ss["metadata"]["ownerReferences"][0]["name"] = "db".into();
        assert_eq!(owner_of(&from(ss)).unwrap().name, "db");
        let mut bare = running("debug");
        bare["metadata"]["ownerReferences"] = serde_json::json!([]);
        assert_eq!(owner_of(&from(bare)), None);
    }

    #[test]
    fn row_counts_and_details() {
        let row = pod_row(&from(crash_looping("web-7d9f-x")));
        assert_eq!((row.ready, row.containers, row.restarts), (0, 1, 7));
        assert_eq!(row.last_restart, Some(1_792_922_700));
        assert_eq!(row.node.as_deref(), Some("w1"));
        assert_eq!(row.qos.as_deref(), Some("Burstable"));
        let mut p = crash_looping("web-7d9f-x");
        p["spec"]["volumes"] = serde_json::json!([
            {"name": "data", "persistentVolumeClaim": {"claimName": "web-data"}},
            {"name": "cfg", "configMap": {"name": "web-config"}},
            {"name": "tmp", "emptyDir": {}}
        ]);
        p["spec"]["containers"][0]["env"] =
            serde_json::json!([{"name": "TOKEN", "value": "t0ps3cret"}]);
        let d = pod_details(&from(p));
        let c = &d.containers[0];
        assert_eq!(c.image_id.as_deref(), Some("web@sha256:1"));
        assert_eq!(
            c.state.as_ref().unwrap().reason.as_deref(),
            Some("CrashLoopBackOff")
        );
        let last = c.last_state.as_ref().unwrap();
        assert_eq!(
            (last.state, last.exit_code),
            (ContainerPhase::Terminated, Some(1))
        );
        assert_eq!(
            d.volumes,
            [
                VolumeView {
                    name: "data".into(),
                    kind: "persistentVolumeClaim".into(),
                    source: Some("web-data".into())
                },
                VolumeView {
                    name: "cfg".into(),
                    kind: "configMap".into(),
                    source: Some("web-config".into())
                },
                VolumeView {
                    name: "tmp".into(),
                    kind: "emptyDir".into(),
                    source: None
                },
            ]
        );
        let json = serde_json::to_string(&d).unwrap();
        assert!(
            json.contains("\"imageId\""),
            "поля контейнера на одном уровне: {json}"
        );
        assert!(!json.contains("t0ps3cret"), "значения env не уходят");
    }

    fn list(items: Vec<serde_json::Value>) -> serde_json::Value {
        serde_json::json!({"apiVersion": "v1", "kind": "List", "metadata": {}, "items": items})
    }

    #[tokio::test]
    async fn pods_of_a_workload_by_its_selector_and_of_a_cron_job_by_its_jobs() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => (
                200,
                crate::k8s::workloads::fixtures::deployment("web", 1, 1),
            ),
            "/apis/batch/v1/namespaces/team-a/jobs" => (
                200,
                list(vec![
                    serde_json::json!({"metadata": {"name": "report-1",
                        "ownerReferences": [{"apiVersion": "batch/v1", "kind": "CronJob", "name": "report",
                            "uid": "c", "controller": true}]}}),
                    serde_json::json!({"metadata": {"name": "manual"}}),
                ]),
            ),
            _ => {
                let job_pod = |name: &str, job: &str| {
                    let mut p = running(name);
                    p["metadata"]["ownerReferences"] = serde_json::json!([{"apiVersion": "batch/v1",
                        "kind": "Job", "name": job, "uid": "j", "controller": true}]);
                    p
                };
                (
                    200,
                    list(vec![
                        running("web-7d9f-a"),
                        job_pod("report-1-x", "report-1"),
                        job_pod("manual-y", "manual"),
                    ]),
                )
            }
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let web = WorkloadRef {
            kind: WorkloadKind::Deployment,
            name: "web".into(),
        };
        list_pods(&r, "team-a", Some(&web)).await.unwrap();
        let cron = WorkloadRef {
            kind: WorkloadKind::CronJob,
            name: "report".into(),
        };
        let pods = list_pods(&r, "team-a", Some(&cron)).await.unwrap();
        let names: Vec<_> = pods.items.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["report-1-x"]);
        assert_eq!(list_pods(&r, "team-a", None).await.unwrap().items.len(), 3);
        drop(r);
        let seen = server.await.unwrap();
        let pods_of_web = seen
            .iter()
            .find(|s| s.path() == "/api/v1/namespaces/team-a/pods")
            .unwrap();
        assert!(
            pods_of_web.uri.contains("labelSelector=app%3Dweb"),
            "{}",
            pods_of_web.uri
        );
    }
}
````

- [ ] **Step 2: Реализация.**

<!-- block: file t07-src-tauri-src-k8s-events-rs-impl step=7.2 path=src-tauri/src/k8s/events.rs part=impl -->
````rust
//! События проекта (спец. §6.3–6.5): все события namespace, события объекта или workload'а
//! вместе с его подами, ReplicaSet и Job. Новые — сверху.

use k8s_openapi::api::core::v1::Event;
use kube::api::ListParams;
use serde::{Deserialize, Serialize};

use crate::error::AppResult;
use crate::k8s::pods::OwnerRef;
use crate::k8s::read::{Listed, ReadClient};
use crate::k8s::view::seconds;
use crate::k8s::workloads::WorkloadKind;

/// Виды объектов, которые создаёт workload: их события показываются вместе с его собственными.
const CHILD_KINDS: &[&str] = &["Pod", "ReplicaSet", "ReplicationController", "Job"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventRow {
    /// Ключ строки в списке и в watch (uid события).
    pub key: String,
    /// `Normal` или `Warning`.
    #[serde(rename = "type")]
    pub kind: String,
    pub reason: String,
    pub object: OwnerRef,
    pub message: String,
    pub count: i32,
    pub first_seen: Option<i64>,
    pub last_seen: Option<i64>,
    /// Кто сообщил: компонент и узел.
    pub source: Option<String>,
}

/// Строка события. Время — из серии (`series.lastObservedTime`), `lastTimestamp`, `eventTime`
/// или времени создания: новые события `events.k8s.io` заполняют не все поля.
pub fn event_row(e: &Event) -> EventRow {
    let created = e.metadata.creation_timestamp.as_ref().map(seconds);
    let event_time = e.event_time.as_ref().map(|t| t.0.as_second());
    let last_seen = e
        .series
        .as_ref()
        .and_then(|s| s.last_observed_time.as_ref())
        .map(|t| t.0.as_second())
        .or_else(|| e.last_timestamp.as_ref().map(seconds))
        .or(event_time)
        .or(created);
    let source = e
        .source
        .as_ref()
        .and_then(|s| s.component.clone())
        .or_else(|| e.reporting_component.clone())
        .filter(|s| !s.is_empty())
        .map(|c| {
            match e
                .source
                .as_ref()
                .and_then(|s| s.host.clone())
                .filter(|h| !h.is_empty())
            {
                Some(h) => format!("{c}, {h}"),
                None => c,
            }
        });
    EventRow {
        key: e
            .metadata
            .uid
            .clone()
            .or_else(|| e.metadata.name.clone())
            .unwrap_or_default(),
        kind: e.type_.clone().unwrap_or_else(|| "Normal".into()),
        reason: e.reason.clone().unwrap_or_default(),
        object: OwnerRef {
            kind: e.involved_object.kind.clone().unwrap_or_default(),
            name: e.involved_object.name.clone().unwrap_or_default(),
        },
        message: e.message.clone().unwrap_or_default(),
        count: e
            .series
            .as_ref()
            .and_then(|s| s.count)
            .or(e.count)
            .unwrap_or(1),
        first_seen: e
            .first_timestamp
            .as_ref()
            .map(seconds)
            .or(event_time)
            .or(created),
        last_seen,
        source,
    }
}

/// Чьи события нужны: объекта (`Pod web-1`) или workload'а — вместе с его подами, ReplicaSet и
/// Job (по префиксу имени `web-`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventObject {
    pub kind: String,
    pub name: String,
}

fn is_workload(kind: &str) -> bool {
    serde_json::from_value::<WorkloadKind>(kind.into()).is_ok()
}

/// Относится ли событие к объекту `o`.
pub fn concerns(row: &EventRow, o: &EventObject) -> bool {
    if row.object.kind == o.kind && row.object.name == o.name {
        return true;
    }
    is_workload(&o.kind)
        && CHILD_KINDS.contains(&row.object.kind.as_str())
        && row.object.name.starts_with(&format!("{}-", o.name))
}

/// Новые сверху.
pub fn sort_events(rows: &mut [EventRow]) {
    rows.sort_by(|a, b| b.last_seen.cmp(&a.last_seen).then(a.key.cmp(&b.key)));
}

/// События проекта или объекта. События пода выбираются на сервере (`fieldSelector`),
/// события workload'а — по объекту и префиксу имени.
pub async fn list_events(
    r: &ReadClient,
    namespace: &str,
    object: Option<&EventObject>,
) -> AppResult<Listed<EventRow>> {
    let lp = match object {
        Some(o) if !is_workload(&o.kind) => ListParams::default().fields(&format!(
            "involvedObject.kind={},involvedObject.name={}",
            o.kind, o.name
        )),
        _ => ListParams::default(),
    };
    let events = r.list::<Event>(Some(namespace), &lp).await?;
    let mut rows: Vec<EventRow> = events
        .items
        .iter()
        .map(event_row)
        .filter(|e| object.is_none_or(|o| concerns(e, o)))
        .collect();
    sort_events(&mut rows);
    Ok(Listed {
        items: rows,
        truncated: events.truncated,
    })
}

#[cfg(test)]
pub(crate) mod fixtures {
    pub fn event(uid: &str, kind: &str, object: (&str, &str), last: &str) -> serde_json::Value {
        serde_json::json!({"apiVersion": "v1", "kind": "Event",
            "metadata": {"name": format!("{}.{uid}", object.1), "uid": uid, "namespace": "team-a",
                "creationTimestamp": "2026-10-25T09:00:00Z"},
            "involvedObject": {"kind": object.0, "name": object.1, "namespace": "team-a"},
            "type": kind, "reason": "BackOff", "message": "Back-off restarting failed container",
            "count": 5, "firstTimestamp": "2026-10-25T09:30:00Z", "lastTimestamp": last,
            "source": {"component": "kubelet", "host": "w1"}})
    }
}
````

<!-- block: file t07-src-tauri-src-k8s-pods-rs-impl step=7.2 path=src-tauri/src/k8s/pods.rs part=impl -->
````rust
//! Поды проекта и workload'а (спец. §6.3, §6.5): статус, как его показывает `kubectl get pods`,
//! проблема (CrashLoopBackOff, ImagePullBackOff, OOMKilled, Pending, Failed), готовность,
//! рестарты, владелец; подробности — состояние каждого контейнера.

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ContainerState, ContainerStatus, Pod};
use kube::api::ListParams;
use serde::{Deserialize, Serialize};

use crate::error::AppResult;
use crate::k8s::containers::{ContainerSpecView, container_view};
use crate::k8s::read::{Listed, ReadClient, by_labels};
use crate::k8s::view::{ConditionView, conditions_of, created, name, seconds};
use crate::k8s::workloads::{WorkloadKind, pod_selector};

/// Причины ожидания контейнера, которые означают, что образ не скачать.
const IMAGE_PULL: &[&str] = &["ErrImagePull", "ImagePullBackOff", "InvalidImageName"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PodProblem {
    ImagePull,
    OomKilled,
    CrashLoop,
    Pending,
    Failed,
}

/// Владелец объекта: workload, если его можно узнать, иначе — прямой владелец.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerRef {
    pub kind: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodRow {
    pub name: String,
    pub phase: String,
    /// Статус, как в `kubectl get pods`: `Running`, `CrashLoopBackOff`, `Init:1/2`, `Completed`…
    pub status: String,
    pub problem: Option<PodProblem>,
    pub ready: i32,
    pub containers: i32,
    pub restarts: i32,
    /// Когда контейнер завершился в последний раз (перед рестартом).
    pub last_restart: Option<i64>,
    pub node: Option<String>,
    pub created: Option<i64>,
    pub qos: Option<String>,
    pub owner: Option<OwnerRef>,
}

fn statuses(p: &Pod, init: bool) -> &[ContainerStatus] {
    let s = p.status.as_ref();
    let list = if init {
        s.and_then(|s| s.init_container_statuses.as_deref())
    } else {
        s.and_then(|s| s.container_statuses.as_deref())
    };
    list.unwrap_or(&[])
}

fn exit_text(t: &k8s_openapi::api::core::v1::ContainerStateTerminated) -> String {
    match t.signal {
        Some(s) if s != 0 => format!("Signal:{s}"),
        _ => format!("ExitCode:{}", t.exit_code),
    }
}

/// Статус пода по правилам `kubectl get pods` (printPod).
pub fn pod_status(p: &Pod) -> String {
    let status = p.status.as_ref();
    let mut reason = status
        .and_then(|s| s.reason.clone())
        .filter(|r| !r.is_empty())
        .or_else(|| status.and_then(|s| s.phase.clone()))
        .unwrap_or_else(|| "Unknown".into());
    let init_total = p
        .spec
        .as_ref()
        .and_then(|s| s.init_containers.as_ref())
        .map_or(0, Vec::len);
    let mut initializing = false;
    for (i, c) in statuses(p, true).iter().enumerate() {
        let state = c.state.as_ref();
        if let Some(t) = state.and_then(|s| s.terminated.as_ref()) {
            if t.exit_code == 0 {
                continue;
            }
            reason = match t.reason.as_deref().filter(|r| !r.is_empty()) {
                Some(r) => format!("Init:{r}"),
                None => format!("Init:{}", exit_text(t)),
            };
        } else if let Some(r) = state
            .and_then(|s| s.waiting.as_ref())
            .and_then(|w| w.reason.as_deref())
            .filter(|r| !r.is_empty() && *r != "PodInitializing")
        {
            reason = format!("Init:{r}");
        } else {
            reason = format!("Init:{i}/{init_total}");
        }
        initializing = true;
        break;
    }
    if !initializing {
        let mut running = false;
        for c in statuses(p, false).iter().rev() {
            let state = c.state.as_ref();
            if let Some(r) = state
                .and_then(|s| s.waiting.as_ref())
                .and_then(|w| w.reason.clone())
                .filter(|r| !r.is_empty())
            {
                reason = r;
            } else if let Some(t) = state.and_then(|s| s.terminated.as_ref()) {
                reason = t
                    .reason
                    .clone()
                    .filter(|r| !r.is_empty())
                    .unwrap_or_else(|| exit_text(t));
            } else if c.ready && state.and_then(|s| s.running.as_ref()).is_some() {
                running = true;
            }
        }
        if reason == "Completed" && running {
            let ready = status
                .and_then(|s| s.conditions.as_ref())
                .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"));
            reason = if ready { "Running" } else { "NotReady" }.into();
        }
    }
    if p.metadata.deletion_timestamp.is_some() {
        reason = if status.and_then(|s| s.reason.as_deref()) == Some("NodeLost") {
            "Unknown".into()
        } else {
            "Terminating".into()
        };
    }
    reason
}

fn waiting_reason(c: &ContainerStatus) -> Option<&str> {
    c.state
        .as_ref()
        .and_then(|s| s.waiting.as_ref())
        .and_then(|w| w.reason.as_deref())
}

fn oom(state: Option<&ContainerState>) -> bool {
    state
        .and_then(|s| s.terminated.as_ref())
        .is_some_and(|t| t.reason.as_deref() == Some("OOMKilled"))
}

/// Проблема пода — то, что видно сейчас: образ не скачивается, контейнер завершён по памяти
/// (сейчас или в последний раз), перезапускается в цикле, под ждёт размещения или упал.
pub fn pod_problem(p: &Pod) -> Option<PodProblem> {
    let all: Vec<&ContainerStatus> = statuses(p, true).iter().chain(statuses(p, false)).collect();
    let phase = p.status.as_ref().and_then(|s| s.phase.as_deref());
    if all
        .iter()
        .any(|c| waiting_reason(c).is_some_and(|r| IMAGE_PULL.contains(&r)))
    {
        Some(PodProblem::ImagePull)
    } else if all
        .iter()
        .any(|c| oom(c.state.as_ref()) || oom(c.last_state.as_ref()))
    {
        Some(PodProblem::OomKilled)
    } else if all
        .iter()
        .any(|c| waiting_reason(c) == Some("CrashLoopBackOff"))
    {
        Some(PodProblem::CrashLoop)
    } else if phase == Some("Pending") {
        Some(PodProblem::Pending)
    } else if phase == Some("Failed") {
        Some(PodProblem::Failed)
    } else {
        None
    }
}

/// Владелец-workload: ReplicaSet с `pod-template-hash` — это Deployment, ReplicationController
/// с меткой DeploymentConfig — DeploymentConfig; иначе — прямой владелец-контроллер.
pub fn owner_of(p: &Pod) -> Option<OwnerRef> {
    let owner = p
        .metadata
        .owner_references
        .iter()
        .flatten()
        .find(|o| o.controller == Some(true))?;
    let labels = p.metadata.labels.clone().unwrap_or_default();
    let workload = |kind: &str, name: &str| {
        Some(OwnerRef {
            kind: kind.into(),
            name: name.into(),
        })
    };
    match owner.kind.as_str() {
        "ReplicaSet" => match labels.get("pod-template-hash") {
            Some(hash) if owner.name.ends_with(&format!("-{hash}")) => workload(
                "Deployment",
                &owner.name[..owner.name.len() - hash.len() - 1],
            ),
            _ => workload("ReplicaSet", &owner.name),
        },
        "ReplicationController" => match labels
            .get("openshift.io/deployment-config.name")
            .or_else(|| labels.get("deploymentconfig"))
        {
            Some(dc) => workload("DeploymentConfig", dc),
            None => workload("ReplicationController", &owner.name),
        },
        kind => workload(kind, &owner.name),
    }
}

pub fn pod_row(p: &Pod) -> PodRow {
    let status = p.status.as_ref();
    let main = statuses(p, false);
    let last_restart = main
        .iter()
        .filter_map(|c| {
            c.last_state
                .as_ref()
                .and_then(|s| s.terminated.as_ref())
                .and_then(|t| t.finished_at.as_ref())
                .map(seconds)
        })
        .max();
    PodRow {
        name: name(&p.metadata),
        phase: status
            .and_then(|s| s.phase.clone())
            .unwrap_or_else(|| "Unknown".into()),
        status: pod_status(p),
        problem: pod_problem(p),
        ready: main.iter().filter(|c| c.ready).count() as i32,
        containers: p.spec.as_ref().map_or(0, |s| s.containers.len() as i32),
        restarts: main.iter().map(|c| c.restart_count).sum(),
        last_restart,
        node: p.spec.as_ref().and_then(|s| s.node_name.clone()),
        created: created(&p.metadata),
        qos: status.and_then(|s| s.qos_class.clone()),
        owner: owner_of(p),
    }
}

/// Workload, поды которого нужны.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadRef {
    pub kind: WorkloadKind,
    pub name: String,
}

/// Поды проекта или workload'а: по селектору workload'а, а у CronJob — поды его Job.
pub async fn list_pods(
    r: &ReadClient,
    namespace: &str,
    workload: Option<&WorkloadRef>,
) -> AppResult<Listed<PodRow>> {
    let ns = Some(namespace);
    match workload {
        None => Ok(r
            .list::<Pod>(ns, &ListParams::default())
            .await?
            .map(|p| pod_row(&p))),
        Some(w) if w.kind == WorkloadKind::CronJob => {
            let jobs: BTreeSet<String> = r
                .list::<Job>(ns, &ListParams::default())
                .await?
                .items
                .into_iter()
                .filter(|j| {
                    j.metadata
                        .owner_references
                        .iter()
                        .flatten()
                        .any(|o| o.kind == "CronJob" && o.name == w.name)
                })
                .filter_map(|j| j.metadata.name)
                .collect();
            let pods = r.list::<Pod>(ns, &ListParams::default()).await?;
            let rows: Vec<PodRow> = pods
                .items
                .iter()
                .map(pod_row)
                .filter(|p| {
                    p.owner
                        .as_ref()
                        .is_some_and(|o| o.kind == "Job" && jobs.contains(&o.name))
                })
                .collect();
            Ok(Listed {
                items: rows,
                truncated: pods.truncated,
            })
        }
        Some(w) => {
            let selector = pod_selector(r, namespace, w.kind, &w.name).await?;
            Ok(r.list::<Pod>(ns, &by_labels(selector.as_deref()))
                .await?
                .map(|p| pod_row(&p)))
        }
    }
}

/// Состояние контейнера: работает, ждёт или завершён — с причиной, кодом выхода и временем.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateView {
    pub state: ContainerPhase,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub exit_code: Option<i32>,
    pub started: Option<i64>,
    pub finished: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ContainerPhase {
    Running,
    Waiting,
    Terminated,
}

fn state_view(s: &ContainerState) -> Option<StateView> {
    if let Some(r) = &s.running {
        Some(StateView {
            state: ContainerPhase::Running,
            reason: None,
            message: None,
            exit_code: None,
            started: r.started_at.as_ref().map(seconds),
            finished: None,
        })
    } else if let Some(w) = &s.waiting {
        Some(StateView {
            state: ContainerPhase::Waiting,
            reason: w.reason.clone(),
            message: w.message.clone(),
            exit_code: None,
            started: None,
            finished: None,
        })
    } else {
        s.terminated.as_ref().map(|t| StateView {
            state: ContainerPhase::Terminated,
            reason: t.reason.clone(),
            message: t.message.clone(),
            exit_code: Some(t.exit_code),
            started: t.started_at.as_ref().map(seconds),
            finished: t.finished_at.as_ref().map(seconds),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerStatusView {
    #[serde(flatten)]
    pub spec: ContainerSpecView,
    /// Образ с digest, который запущен на самом деле.
    pub image_id: Option<String>,
    pub state: Option<StateView>,
    pub last_state: Option<StateView>,
    pub ready: bool,
    pub restarts: i32,
}

/// Том пода: имя, вид (`configMap`, `secret`, `persistentVolumeClaim`, `emptyDir`…) и источник.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeView {
    pub name: String,
    pub kind: String,
    pub source: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodDetails {
    pub pod: PodRow,
    pub conditions: Vec<ConditionView>,
    pub pod_ip: Option<String>,
    pub host_ip: Option<String>,
    pub service_account: Option<String>,
    pub started: Option<i64>,
    pub containers: Vec<ContainerStatusView>,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub volumes: Vec<VolumeView>,
}

/// Вид тома и его источник — первое заданное поле `volumes[]` кроме имени.
fn volume_view(v: &serde_json::Value) -> Option<VolumeView> {
    let obj = v.as_object()?;
    let name = obj.get("name")?.as_str()?.to_string();
    let (kind, src) = obj.iter().find(|(k, _)| *k != "name")?;
    let source = ["claimName", "name", "secretName", "path", "medium"]
        .iter()
        .find_map(|k| src[*k].as_str().filter(|s| !s.is_empty()).map(String::from));
    Some(VolumeView {
        name,
        kind: kind.clone(),
        source,
    })
}

pub fn pod_details(p: &Pod) -> PodDetails {
    let spec = p.spec.as_ref();
    let status = p.status.as_ref();
    let by_name = |init: bool| -> BTreeMap<String, ContainerStatus> {
        statuses(p, init)
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect()
    };
    let (init_status, main_status) = (by_name(true), by_name(false));
    let view = |c: &k8s_openapi::api::core::v1::Container, init: bool| {
        let st = if init {
            init_status.get(&c.name)
        } else {
            main_status.get(&c.name)
        };
        ContainerStatusView {
            spec: container_view(c, init),
            image_id: st.map(|s| s.image_id.clone()).filter(|i| !i.is_empty()),
            state: st.and_then(|s| s.state.as_ref()).and_then(state_view),
            last_state: st.and_then(|s| s.last_state.as_ref()).and_then(state_view),
            ready: st.is_some_and(|s| s.ready),
            restarts: st.map_or(0, |s| s.restart_count),
        }
    };
    let containers = spec
        .map(|s| {
            s.init_containers
                .iter()
                .flatten()
                .map(|c| view(c, true))
                .chain(s.containers.iter().map(|c| view(c, false)))
                .collect()
        })
        .unwrap_or_default();
    let mut annotations = p.metadata.annotations.clone().unwrap_or_default();
    annotations.remove("kubectl.kubernetes.io/last-applied-configuration");
    PodDetails {
        pod: pod_row(p),
        conditions: conditions_of(&status.and_then(|s| s.conditions.clone())),
        pod_ip: status.and_then(|s| s.pod_ip.clone()),
        host_ip: status.and_then(|s| s.host_ip.clone()),
        service_account: spec.and_then(|s| s.service_account_name.clone()),
        started: status.and_then(|s| s.start_time.as_ref()).map(seconds),
        containers,
        labels: p.metadata.labels.clone().unwrap_or_default(),
        annotations,
        volumes: spec
            .and_then(|s| s.volumes.as_ref())
            .map(|vs| {
                vs.iter()
                    .filter_map(|v| serde_json::to_value(v).ok())
                    .filter_map(|v| volume_view(&v))
                    .collect()
            })
            .unwrap_or_default(),
    }
}

pub async fn get_pod(r: &ReadClient, namespace: &str, pod: &str) -> AppResult<PodDetails> {
    Ok(pod_details(&r.get::<Pod>(namespace, pod).await?))
}

#[cfg(test)]
pub(crate) mod fixtures {
    /// Под с одним контейнером в состоянии `state` (JSON `ContainerState`).
    pub fn pod(name: &str, phase: &str, state: serde_json::Value) -> serde_json::Value {
        serde_json::json!({"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": "team-a", "creationTimestamp": "2026-10-25T10:00:00Z",
                "labels": {"app": "web", "pod-template-hash": "7d9f"},
                "ownerReferences": [{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "web-7d9f",
                    "uid": "rs", "controller": true}]},
            "spec": {"nodeName": "w1", "containers": [{"name": "web", "image": "web:1"}]},
            "status": {"phase": phase, "qosClass": "Burstable",
                "containerStatuses": [{"name": "web", "image": "web:1", "imageID": "web@sha256:1",
                    "ready": phase == "Running" && state.get("running").is_some(),
                    "restartCount": 0, "state": state}]}})
    }

    pub fn running(name: &str) -> serde_json::Value {
        pod(
            name,
            "Running",
            serde_json::json!({"running": {"startedAt": "2026-10-25T10:00:00Z"}}),
        )
    }

    pub fn crash_looping(name: &str) -> serde_json::Value {
        let mut p = pod(
            name,
            "Running",
            serde_json::json!({"waiting": {"reason": "CrashLoopBackOff",
            "message": "back-off 5m0s restarting failed container"}}),
        );
        p["status"]["containerStatuses"][0]["restartCount"] = 7.into();
        p["status"]["containerStatuses"][0]["lastState"] = serde_json::json!({"terminated": {
            "exitCode": 1, "reason": "Error", "finishedAt": "2026-10-25T10:05:00Z"}});
        p
    }
}
````

<!-- block: patch t07-src step=7.2 -->
````diff
diff --git a/src-tauri/src/commands/objects.rs b/src-tauri/src/commands/objects.rs
index d5dd027..27f9489 100644
--- a/src-tauri/src/commands/objects.rs
+++ b/src-tauri/src/commands/objects.rs
@@ -3,6 +3,9 @@
 use tauri::State;
 
 use crate::error::AppResult;
+use crate::k8s::events::{EventObject, EventRow};
+use crate::k8s::pods::{PodDetails, PodRow, WorkloadRef};
+use crate::k8s::read::Listed;
 use crate::k8s::workloads::{WorkloadDetails, WorkloadKind, WorkloadList};
 use crate::state::AppState;
 
@@ -35,3 +38,50 @@ pub async fn get_workload(
         })
         .await
 }
+
+/// Поды проекта или, если задан `workload`, его поды.
+#[tauri::command]
+pub async fn list_pods(
+    state: State<'_, AppState>,
+    connection_id: String,
+    namespace: String,
+    workload: Option<WorkloadRef>,
+) -> AppResult<Listed<PodRow>> {
+    let ns = namespace.clone();
+    state
+        .read_in(&connection_id, &namespace, |c| async move {
+            crate::k8s::pods::list_pods(&c.reader(), &ns, workload.as_ref()).await
+        })
+        .await
+}
+
+#[tauri::command]
+pub async fn get_pod(
+    state: State<'_, AppState>,
+    connection_id: String,
+    namespace: String,
+    name: String,
+) -> AppResult<PodDetails> {
+    let ns = namespace.clone();
+    state
+        .read_in(&connection_id, &namespace, |c| async move {
+            crate::k8s::pods::get_pod(&c.reader(), &ns, &name).await
+        })
+        .await
+}
+
+/// События проекта или объекта (`object`): у workload'а — вместе с его подами.
+#[tauri::command]
+pub async fn list_events(
+    state: State<'_, AppState>,
+    connection_id: String,
+    namespace: String,
+    object: Option<EventObject>,
+) -> AppResult<Listed<EventRow>> {
+    let ns = namespace.clone();
+    state
+        .read_in(&connection_id, &namespace, |c| async move {
+            crate::k8s::events::list_events(&c.reader(), &ns, object.as_ref()).await
+        })
+        .await
+}
diff --git a/src-tauri/src/k8s/mod.rs b/src-tauri/src/k8s/mod.rs
index 9ab392d..932ee43 100644
--- a/src-tauri/src/k8s/mod.rs
+++ b/src-tauri/src/k8s/mod.rs
@@ -6,10 +6,12 @@ pub mod config;
 pub mod connectivity;
 pub mod containers;
 pub mod errors;
+pub mod events;
 pub mod kubeconfig;
 pub mod nodes;
 pub mod openshift;
 pub mod overview;
+pub mod pods;
 pub mod pool;
 pub mod projects;
 pub mod read;
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index 23a6058..dac89a4 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -79,6 +79,9 @@ pub fn run() {
             commands::projects::get_namespace_access,
             commands::objects::list_workloads,
             commands::objects::get_workload,
+            commands::objects::list_pods,
+            commands::objects::get_pod,
+            commands::objects::list_events,
         ])
         .run(tauri::generate_context!())
         .expect("error while building tauri application");
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 319 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 133`

- [ ] **Step 4: Commit.**

```bash
git add \
  src-tauri/src/commands/objects.rs \
  src-tauri/src/k8s/events.rs \
  src-tauri/src/k8s/mod.rs \
  src-tauri/src/k8s/pods.rs \
  src-tauri/src/lib.rs
git commit -s -m "feat(k8s): поды — статус как у kubectl, проблемы, владелец, контейнеры; события объекта и workload'а" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 8: Сеть проекта: Services, Routes, Ingress, Gateway API

**Files:**
- Port: `src-tauri/src/der.rs` (с правками)
- Create: `src-tauri/src/k8s/network.rs`, `src-tauri/src/k8s/testdata/route-cert.pem`
- Modify: `PORTED.md`, `src-tauri/src/commands/objects.rs`, `src-tauri/src/k8s/mod.rs`, `src-tauri/src/lib.rs`

**Interfaces:**
- Consumes: задачи 3–7 (`ReadClient`, `view`, `selectors`); Kafka-версия: `tls/der.rs`.
- Produces: `der::read_tlv` (перенос); `k8s::network::{routes(), gateways(), http_routes(), grpc_routes(), certificate_expiry(pem) -> Option<i64>, EndpointCount, ServiceRow, service_row, endpoints_by_service, RouteRow, route_row, IngressPath, IngressTls, IngressRow, ingress_row, ListenerView, GatewayRow, gateway_row, ParentView, GatewayRouteRow, gateway_route_row, NetworkView { services, endpoints_error?, routes?, ingresses, gateways?, http_routes?, grpc_routes? }, get_network}`; тестовый сертификат `k8s/testdata/route-cert.pem`; команда `get_network(connectionId, namespace)`.

Спец. §6.3, §4.3. Services: тип, порты (`80:30080/TCP → 8080`), внешние адреса, готовые endpoints по EndpointSlices (метка `kubernetes.io/service-name`; endpoint без условия `ready` — готов). Routes (при `route.openshift.io`): хост и путь, сервис и порт, TLS-терминация, принят ли роутером (и почему нет), срок встроенного сертификата `spec.tls.certificate` (разбор DER — `read_tlv`, перенесённый из Kafka-версии; UTCTime и GeneralizedTime) и Ingress, из которого OpenShift создал Route; ключ `spec.tls.key` не читается. Ingress: класс (или аннотация `kubernetes.io/ingress.class`), правила хост/путь → сервис и порт, TLS-хосты и имя Secret, адрес из `status.loadBalancer`, созданные из него Routes. Gateway API (при `gateway.networking.k8s.io/v1`): Gateway — класс, listeners, адреса, `Accepted` и `Programmed`; HTTPRoute и GRPCRoute — хосты, родители с условиями `Accepted` и `ResolvedRefs`, бэкенды; GRPCRoute нет в Gateway API до 1.1 — 404 значит «вида нет». Одна команда `get_network` вместо пяти: каждый вид — своей частью, без прав на один остальные видны.

- [ ] **Step 1: Перенос из Kafka-версии.**

Файлы копируются с коммита `8bbd0bd` и правятся патчами (правки — против перенесённого файла).

<!-- block: shell t08-port step=8.1 -->
````bash
scripts/port.sh src-tauri/src/tls/der.rs src-tauri/src/der.rs
````

<!-- block: patch t08-port-src-tauri-src-der-rs step=8.1 -->
````diff
diff --git a/src-tauri/src/der.rs b/src-tauri/src/der.rs
--- a/src-tauri/src/der.rs
+++ b/src-tauri/src/der.rs
@@ -1,4 +1,4 @@
-//! Минимальный разбор DER: тег, содержимое и остаток (для EncryptedPrivateKeyInfo в JKS).
+//! Минимальный разбор DER: тег, содержимое и остаток (для срока сертификата Route).
 
 pub fn read_tlv(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
     let (&tag, rest) = input.split_first()?;
````

- [ ] **Step 2: Тесты.**

<!-- block: file t08-src-tauri-src-k8s-network-rs-tests step=8.2 path=src-tauri/src/k8s/network.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::{Seen, mock_client, status};
    use std::time::Duration;

    const ROUTE_CERT: &str = include_str!("testdata/route-cert.pem");

    fn from<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> T {
        serde_json::from_value(v).unwrap()
    }

    fn list(items: Vec<serde_json::Value>) -> (u16, serde_json::Value) {
        (
            200,
            serde_json::json!({"apiVersion": "v1", "kind": "List", "metadata": {}, "items": items}),
        )
    }

    #[test]
    fn certificate_expiry_in_utc_and_generalized_time() {
        assert_eq!(
            certificate_expiry(ROUTE_CERT),
            Some(1_795_537_583),
            "UTCTime"
        );
        assert_eq!(
            certificate_expiry(crate::storage::profile::TEST_CERT_PEM),
            Some(4_946_471_467),
            "GeneralizedTime после 2049 года"
        );
        assert_eq!(certificate_expiry("not a pem"), None);
    }

    #[test]
    fn services_with_ports_external_addresses_and_endpoints() {
        let svc: Service = from(serde_json::json!({"metadata": {"name": "web"},
            "spec": {"type": "LoadBalancer", "clusterIP": "10.0.0.10", "selector": {"app": "web"},
                "externalIPs": ["192.0.2.1"],
                "ports": [{"port": 80, "targetPort": 8080, "protocol": "TCP", "nodePort": 30080},
                    {"port": 443, "targetPort": "https"}]},
            "status": {"loadBalancer": {"ingress": [{"hostname": "lb.example.com"}]}}}));
        let slices: Vec<EndpointSlice> = vec![
            from(
                serde_json::json!({"metadata": {"name": "web-1", "labels": {"kubernetes.io/service-name": "web"}},
                "addressType": "IPv4", "endpoints": [
                    {"addresses": ["10.1.0.1"], "conditions": {"ready": true}},
                    {"addresses": ["10.1.0.2"], "conditions": {"ready": false}},
                    {"addresses": ["10.1.0.3"]}]}),
            ),
            from(
                serde_json::json!({"metadata": {"name": "x"}, "addressType": "IPv4", "endpoints": []}),
            ),
        ];
        let counts = endpoints_by_service(&slices);
        assert_eq!(counts["web"], EndpointCount { ready: 2, total: 3 });
        let row = service_row(&svc, counts.get("web").copied());
        assert_eq!(row.kind, "LoadBalancer");
        assert_eq!(row.ports, ["80:30080/TCP → 8080", "443/TCP → https"]);
        assert_eq!(row.external, ["lb.example.com", "192.0.2.1"]);
        assert_eq!(row.selector.as_deref(), Some("app=web"));
        assert_eq!(row.endpoints, Some(EndpointCount { ready: 2, total: 3 }));
        let manual: Service = from(serde_json::json!({"metadata": {"name": "db"},
            "spec": {"type": "ExternalName", "externalName": "db.example.com"}}));
        let row = service_row(&manual, Some(EndpointCount { ready: 0, total: 0 }));
        assert_eq!(row.endpoints, None, "без селектора endpoints не считаются");
        assert_eq!(row.external, ["db.example.com"]);
    }

    fn route(name: &str, admitted: Option<&str>, owner: Option<&str>) -> serde_json::Value {
        let mut r = serde_json::json!({"apiVersion": "route.openshift.io/v1", "kind": "Route",
            "metadata": {"name": name},
            "spec": {"host": format!("{name}.apps.example.com"), "path": "/api",
                "to": {"kind": "Service", "name": "web"}, "port": {"targetPort": 8080},
                "tls": {"termination": "edge", "insecureEdgeTerminationPolicy": "Redirect",
                    "certificate": ROUTE_CERT, "key": "-----BEGIN PRIVATE KEY-----"}}});
        if let Some(a) = admitted {
            r["status"] = serde_json::json!({"ingress": [{"routerName": "default",
                "conditions": [{"type": "Admitted", "status": a, "message": "host already claimed"}]}]});
        }
        if let Some(o) = owner {
            r["metadata"]["ownerReferences"] = serde_json::json!([{"apiVersion": "networking.k8s.io/v1",
                "kind": "Ingress", "name": o, "uid": "i"}]);
        }
        r
    }

    #[test]
    fn routes_admission_certificate_and_owner() {
        let ok = route_row(&from(route("web", Some("True"), None)));
        assert_eq!(ok.host.as_deref(), Some("web.apps.example.com"));
        assert_eq!(
            (ok.service.as_deref(), ok.port.as_deref()),
            (Some("web"), Some("8080"))
        );
        assert_eq!(ok.tls.as_deref(), Some("edge"));
        assert_eq!(
            (ok.admitted, ok.admitted_message.as_deref()),
            (Some(true), None)
        );
        assert_eq!(ok.cert_expires, Some(1_795_537_583));
        let json = serde_json::to_string(&ok).unwrap();
        assert!(
            !json.contains("PRIVATE KEY"),
            "ключ Route не уходит: {json}"
        );
        let rejected = route_row(&from(route("dup", Some("False"), Some("web"))));
        assert_eq!(rejected.admitted, Some(false));
        assert_eq!(
            rejected.admitted_message.as_deref(),
            Some("host already claimed")
        );
        assert_eq!(rejected.ingress.as_deref(), Some("web"));
        assert_eq!(route_row(&from(route("new", None, None))).admitted, None);
    }

    #[test]
    fn ingress_rules_tls_class_and_routes() {
        let ing: Ingress = from(serde_json::json!({"metadata": {"name": "web",
                "annotations": {"kubernetes.io/ingress.class": "nginx"}},
            "spec": {"rules": [{"host": "web.example.com", "http": {"paths": [
                    {"path": "/", "pathType": "Prefix", "backend": {"service": {"name": "web", "port": {"number": 80}}}},
                    {"path": "/api", "pathType": "Prefix", "backend": {"service": {"name": "api", "port": {"name": "http"}}}}
                ]}}],
                "defaultBackend": {"service": {"name": "fallback", "port": {"number": 8080}}},
                "tls": [{"hosts": ["web.example.com"], "secretName": "web-tls"}]},
            "status": {"loadBalancer": {"ingress": [{"ip": "203.0.113.5"}]}}}));
        let routes = vec![
            route_row(&from(route("web-abcde", Some("True"), Some("web")))),
            route_row(&from(route("other", Some("True"), None))),
        ];
        let row = ingress_row(&ing, &routes);
        assert_eq!(row.class.as_deref(), Some("nginx"));
        assert_eq!(
            row.rules[1],
            IngressPath {
                host: Some("web.example.com".into()),
                path: Some("/api".into()),
                service: Some("api".into()),
                port: Some("http".into())
            }
        );
        assert_eq!(row.default_backend.as_deref(), Some("fallback:8080"));
        assert_eq!(row.tls[0].secret.as_deref(), Some("web-tls"));
        assert_eq!(row.addresses, ["203.0.113.5"]);
        assert_eq!(row.routes, ["web-abcde"]);
    }

    fn gateway_objects(s: &Seen) -> Option<(u16, serde_json::Value)> {
        Some(match s.path() {
            "/apis/gateway.networking.k8s.io/v1/namespaces/team-a/gateways" => {
                list(vec![serde_json::json!({
                "metadata": {"name": "gw"},
                "spec": {"gatewayClassName": "cilium", "listeners": [
                    {"name": "https", "port": 443, "protocol": "HTTPS", "hostname": "*.example.com"}]},
                "status": {"addresses": [{"type": "IPAddress", "value": "198.51.100.7"}],
                    "conditions": [{"type": "Accepted", "status": "True"},
                        {"type": "Programmed", "status": "False", "reason": "AddressNotAssigned"}]}})])
            }
            "/apis/gateway.networking.k8s.io/v1/namespaces/team-a/httproutes" => {
                list(vec![serde_json::json!({
                "metadata": {"name": "web"},
                "spec": {"hostnames": ["web.example.com"],
                    "parentRefs": [{"name": "gw", "sectionName": "https"}, {"name": "other"}],
                    "rules": [{"backendRefs": [{"name": "web", "port": 80}, {"name": "canary", "port": 80}]}]},
                "status": {"parents": [{"parentRef": {"name": "gw", "sectionName": "https"},
                    "controllerName": "io.cilium/gateway-controller",
                    "conditions": [{"type": "Accepted", "status": "True"},
                        {"type": "ResolvedRefs", "status": "False", "reason": "BackendNotFound"}]}]}})])
            }
            "/apis/gateway.networking.k8s.io/v1/namespaces/team-a/grpcroutes" => status(
                404,
                "NotFound",
                "the server could not find the requested resource",
            ),
            _ => return None,
        })
    }

    #[tokio::test]
    async fn network_of_a_kubernetes_project_with_gateway_api() {
        let (client, server) = mock_client(|s| {
            gateway_objects(s).unwrap_or_else(|| match s.path() {
                "/api/v1/namespaces/team-a/services" => list(vec![serde_json::json!({
                    "metadata": {"name": "web"}, "spec": {"selector": {"app": "web"}, "ports": [{"port": 80}]}})]),
                "/apis/discovery.k8s.io/v1/namespaces/team-a/endpointslices" => status(
                    403,
                    "Forbidden",
                    r#"endpointslices.discovery.k8s.io is forbidden: User "u" cannot list resource "endpointslices" in API group "discovery.k8s.io" in the namespace "team-a""#,
                ),
                "/apis/networking.k8s.io/v1/namespaces/team-a/ingresses" => list(vec![]),
                other => panic!("лишний запрос {other}"),
            })
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let groups = ApiGroups {
            gateway_api: true,
            ..Default::default()
        };
        let n = get_network(&r, "team-a", &groups).await;
        let services = n.services.data.unwrap();
        assert_eq!(services[0].endpoints, None, "EndpointSlices не читаются");
        assert_eq!(
            n.endpoints_error.unwrap().params["resource"],
            "endpointslices"
        );
        assert!(
            n.routes.is_none(),
            "нет route.openshift.io — Routes не спрашиваем"
        );
        assert_eq!(n.ingresses.data.unwrap().len(), 0);
        let gw = &n.gateways.unwrap().data.unwrap()[0];
        assert_eq!((gw.accepted, gw.programmed), (Some(true), Some(false)));
        assert_eq!(gw.listeners[0].port, Some(443));
        assert_eq!(gw.addresses, ["198.51.100.7"]);
        let http = &n.http_routes.unwrap().data.unwrap()[0];
        assert_eq!(http.backends, ["web:80", "canary:80"]);
        assert_eq!(http.parents.len(), 2);
        assert!(http.parents[0].accepted.as_ref().unwrap().is_true());
        assert_eq!(
            http.parents[0]
                .resolved_refs
                .as_ref()
                .unwrap()
                .reason
                .as_deref(),
            Some("BackendNotFound")
        );
        assert!(
            http.parents[1].accepted.is_none(),
            "статуса от родителя нет"
        );
        assert!(
            n.grpc_routes.is_none(),
            "GRPCRoute нет в этой версии Gateway API"
        );
        drop(r);
        assert_eq!(server.await.unwrap().len(), 6);
    }

    #[tokio::test]
    async fn network_of_openshift_without_gateway_api() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis/route.openshift.io/v1/namespaces/team-a/routes" => {
                list(vec![route("web-abcde", Some("True"), Some("web"))])
            }
            "/apis/networking.k8s.io/v1/namespaces/team-a/ingresses" => {
                list(vec![serde_json::json!({
                "metadata": {"name": "web"}, "spec": {"ingressClassName": "openshift-default"}})])
            }
            p if p.contains("gateway.networking.k8s.io") => panic!("Gateway API не спрашиваем"),
            _ => list(vec![]),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let groups = ApiGroups {
            routes: true,
            ..Default::default()
        };
        let n = get_network(&r, "team-a", &groups).await;
        assert_eq!(n.routes.unwrap().data.unwrap().len(), 1);
        assert_eq!(n.ingresses.data.unwrap()[0].routes, ["web-abcde"]);
        assert!(n.gateways.is_none() && n.http_routes.is_none() && n.grpc_routes.is_none());
        assert!(n.endpoints_error.is_none());
        drop(r);
        assert_eq!(server.await.unwrap().len(), 4);
    }
}
````

- [ ] **Step 3: Реализация.**

<!-- block: file t08-src-tauri-src-k8s-network-rs-impl step=8.3 path=src-tauri/src/k8s/network.rs part=impl -->
````rust
//! Сеть проекта (спец. §6.3): Services с готовыми endpoints по EndpointSlices, Routes (при
//! `route.openshift.io`), Ingress, а при Gateway API — Gateway, HTTPRoute и GRPCRoute. Каждый вид
//! — своей частью: без прав на один вид остальные показываются. Ключ TLS из Route не читается.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::networking::v1::{Ingress, IngressBackend};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ListParams};
use serde::Serialize;

use crate::der::read_tlv;
use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::read::ReadClient;
use crate::k8s::selectors::map_selector_string;
use crate::k8s::view::{ConditionView, Part, condition, condition_flag, conditions, created, name};
use crate::platform::ApiGroups;

pub fn routes() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("route.openshift.io", "v1", "Route"),
        "routes",
    )
}

fn gateway_api(kind: &str, plural: &str) -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("gateway.networking.k8s.io", "v1", kind),
        plural,
    )
}

pub fn gateways() -> ApiResource {
    gateway_api("Gateway", "gateways")
}

pub fn http_routes() -> ApiResource {
    gateway_api("HTTPRoute", "httproutes")
}

pub fn grpc_routes() -> ApiResource {
    gateway_api("GRPCRoute", "grpcroutes")
}

/// Срок (`notAfter`, секунды Unix) первого сертификата в PEM — встроенного сертификата Route.
pub fn certificate_expiry(pem_text: &str) -> Option<i64> {
    let block = pem::parse_many(pem_text)
        .ok()?
        .into_iter()
        .find(|p| p.tag() == "CERTIFICATE")?;
    let (_, cert, _) = read_tlv(block.contents())?;
    let (_, tbs, _) = read_tlv(cert)?;
    let mut rest = tbs;
    // Версия — необязательное поле [0].
    if rest.first() == Some(&0xA0) {
        rest = read_tlv(rest)?.2;
    }
    // Серийный номер, алгоритм подписи, издатель.
    for _ in 0..3 {
        rest = read_tlv(rest)?.2;
    }
    let (_, validity, _) = read_tlv(rest)?;
    let (_, _, after) = read_tlv(validity)?;
    let (tag, not_after, _) = read_tlv(after)?;
    asn1_time(tag, not_after)
}

/// UTCTime (`YYMMDDHHMMSSZ`, годы 1950–2049) или GeneralizedTime (`YYYYMMDDHHMMSSZ`).
fn asn1_time(tag: u8, v: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(v).ok()?;
    let (year, rest) = match tag {
        0x17 => {
            let yy: i32 = s.get(0..2)?.parse().ok()?;
            (if yy >= 50 { 1900 + yy } else { 2000 + yy }, s.get(2..)?)
        }
        0x18 => (s.get(0..4)?.parse().ok()?, s.get(4..)?),
        _ => return None,
    };
    let num = |from: usize| rest.get(from..from + 2)?.parse::<u8>().ok();
    let date =
        time::Date::from_calendar_date(year, time::Month::try_from(num(0)?).ok()?, num(2)?).ok()?;
    let t = time::Time::from_hms(num(4)?, num(6)?, num(8)?).ok()?;
    Some(
        time::PrimitiveDateTime::new(date, t)
            .assume_utc()
            .unix_timestamp(),
    )
}

fn port_text(p: &IntOrString) -> String {
    match p {
        IntOrString::Int(i) => i.to_string(),
        IntOrString::String(s) => s.clone(),
    }
}

/// Готовые endpoints и все.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointCount {
    pub ready: i32,
    pub total: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceRow {
    pub name: String,
    /// `ClusterIP`, `NodePort`, `LoadBalancer`, `ExternalName`.
    #[serde(rename = "type")]
    pub kind: String,
    pub cluster_ip: Option<String>,
    /// Внешние адреса: балансировщик, `externalIPs`, `externalName`.
    pub external: Vec<String>,
    /// `80/TCP → 8080`, `80:30080/TCP → http`.
    pub ports: Vec<String>,
    pub selector: Option<String>,
    /// `None` — у сервиса нет селектора или EndpointSlices не читаются.
    pub endpoints: Option<EndpointCount>,
    pub created: Option<i64>,
}

pub fn service_row(s: &Service, endpoints: Option<EndpointCount>) -> ServiceRow {
    let spec = s.spec.as_ref();
    let mut external: Vec<String> = s
        .status
        .as_ref()
        .and_then(|st| st.load_balancer.as_ref())
        .and_then(|lb| lb.ingress.as_ref())
        .map(|ing| {
            ing.iter()
                .filter_map(|i| i.ip.clone().or_else(|| i.hostname.clone()))
                .collect()
        })
        .unwrap_or_default();
    external.extend(
        spec.and_then(|s| s.external_ips.clone())
            .unwrap_or_default(),
    );
    external.extend(spec.and_then(|s| s.external_name.clone()));
    let selector = spec
        .and_then(|s| s.selector.as_ref())
        .filter(|m| !m.is_empty())
        .map(map_selector_string);
    ServiceRow {
        name: name(&s.metadata),
        kind: spec
            .and_then(|s| s.type_.clone())
            .unwrap_or_else(|| "ClusterIP".into()),
        cluster_ip: spec.and_then(|s| s.cluster_ip.clone()),
        external,
        ports: spec
            .and_then(|s| s.ports.as_ref())
            .map(|ps| {
                ps.iter()
                    .map(|p| {
                        let proto = p.protocol.as_deref().unwrap_or("TCP");
                        let port = match p.node_port {
                            Some(n) => format!("{}:{n}/{proto}", p.port),
                            None => format!("{}/{proto}", p.port),
                        };
                        match &p.target_port {
                            Some(t) => format!("{port} → {}", port_text(t)),
                            None => port,
                        }
                    })
                    .collect()
            })
            .unwrap_or_default(),
        endpoints: endpoints.filter(|_| selector.is_some()),
        selector,
        created: created(&s.metadata),
    }
}

/// Endpoints по сервисам: метка `kubernetes.io/service-name` у EndpointSlice. Endpoint без
/// условия `ready` считается готовым (так читает его kube-proxy).
pub fn endpoints_by_service(slices: &[EndpointSlice]) -> BTreeMap<String, EndpointCount> {
    let mut out: BTreeMap<String, EndpointCount> = BTreeMap::new();
    for s in slices {
        let Some(service) = s
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name"))
        else {
            continue;
        };
        let e = out
            .entry(service.clone())
            .or_insert(EndpointCount { ready: 0, total: 0 });
        for ep in &s.endpoints {
            e.total += 1;
            if ep.conditions.as_ref().and_then(|c| c.ready) != Some(false) {
                e.ready += 1;
            }
        }
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteRow {
    pub name: String,
    pub host: Option<String>,
    pub path: Option<String>,
    pub service: Option<String>,
    pub port: Option<String>,
    /// TLS-терминация: `edge`, `passthrough`, `reencrypt`.
    pub tls: Option<String>,
    pub insecure_policy: Option<String>,
    /// Принят ли роутером; `None` — статуса роутера нет.
    pub admitted: Option<bool>,
    /// Почему не принят.
    pub admitted_message: Option<String>,
    /// Срок встроенного сертификата `spec.tls.certificate`.
    pub cert_expires: Option<i64>,
    /// Ingress, из которого OpenShift создал Route.
    pub ingress: Option<String>,
    pub created: Option<i64>,
}

pub fn route_row(o: &DynamicObject) -> RouteRow {
    let spec = &o.data["spec"];
    let text = |v: &serde_json::Value| v.as_str().filter(|s| !s.is_empty()).map(String::from);
    let ingresses = o.data["status"]["ingress"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let admitted: Vec<ConditionView> = ingresses
        .iter()
        .filter_map(|i| {
            conditions(&i["conditions"])
                .into_iter()
                .find(|c| c.kind == "Admitted")
        })
        .collect();
    let accepted = admitted.iter().any(ConditionView::is_true);
    RouteRow {
        name: o.metadata.name.clone().unwrap_or_default(),
        host: text(&spec["host"]),
        path: text(&spec["path"]),
        service: text(&spec["to"]["name"]),
        port: match &spec["port"]["targetPort"] {
            serde_json::Value::Number(n) => Some(n.to_string()),
            v => text(v),
        },
        tls: text(&spec["tls"]["termination"]),
        insecure_policy: text(&spec["tls"]["insecureEdgeTerminationPolicy"]),
        admitted: (!admitted.is_empty()).then_some(accepted),
        admitted_message: if accepted {
            None
        } else {
            admitted.iter().find_map(|c| c.message.clone())
        },
        cert_expires: spec["tls"]["certificate"]
            .as_str()
            .and_then(certificate_expiry),
        ingress: o
            .metadata
            .owner_references
            .iter()
            .flatten()
            .find(|r| r.kind == "Ingress")
            .map(|r| r.name.clone()),
        created: created(&o.metadata),
    }
}

/// Правило Ingress: хост и путь → Service и порт.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressPath {
    pub host: Option<String>,
    pub path: Option<String>,
    pub service: Option<String>,
    pub port: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressTls {
    pub hosts: Vec<String>,
    pub secret: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressRow {
    pub name: String,
    /// `spec.ingressClassName` или аннотация `kubernetes.io/ingress.class`.
    pub class: Option<String>,
    pub rules: Vec<IngressPath>,
    pub default_backend: Option<String>,
    pub tls: Vec<IngressTls>,
    /// Адрес из `status.loadBalancer` (заполняют не все контроллеры).
    pub addresses: Vec<String>,
    /// Routes, которые OpenShift создал из этого Ingress.
    pub routes: Vec<String>,
    pub created: Option<i64>,
}

fn backend(b: &IngressBackend) -> (Option<String>, Option<String>) {
    match &b.service {
        Some(s) => (
            Some(s.name.clone()),
            s.port
                .as_ref()
                .and_then(|p| p.number.map(|n| n.to_string()).or_else(|| p.name.clone())),
        ),
        None => (
            b.resource
                .as_ref()
                .map(|r| format!("{}/{}", r.kind, r.name)),
            None,
        ),
    }
}

pub fn ingress_row(i: &Ingress, routes: &[RouteRow]) -> IngressRow {
    let spec = i.spec.as_ref();
    let name = name(&i.metadata);
    let rules = spec
        .and_then(|s| s.rules.as_ref())
        .map(|rules| {
            rules
                .iter()
                .flat_map(|r| {
                    let host = r.host.clone();
                    r.http
                        .as_ref()
                        .map(|h| h.paths.clone())
                        .unwrap_or_default()
                        .into_iter()
                        .map(move |p| {
                            let (service, port) = backend(&p.backend);
                            IngressPath {
                                host: host.clone(),
                                path: p.path.clone(),
                                service,
                                port,
                            }
                        })
                })
                .collect()
        })
        .unwrap_or_default();
    IngressRow {
        class: spec.and_then(|s| s.ingress_class_name.clone()).or_else(|| {
            i.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("kubernetes.io/ingress.class"))
                .cloned()
        }),
        rules,
        default_backend: spec
            .and_then(|s| s.default_backend.as_ref())
            .map(backend)
            .and_then(|(s, p)| s.map(|s| p.map_or(s.clone(), |p| format!("{s}:{p}")))),
        tls: spec
            .and_then(|s| s.tls.as_ref())
            .map(|ts| {
                ts.iter()
                    .map(|t| IngressTls {
                        hosts: t.hosts.clone().unwrap_or_default(),
                        secret: t.secret_name.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        addresses: i
            .status
            .as_ref()
            .and_then(|s| s.load_balancer.as_ref())
            .and_then(|lb| lb.ingress.as_ref())
            .map(|ing| {
                ing.iter()
                    .filter_map(|x| x.ip.clone().or_else(|| x.hostname.clone()))
                    .collect()
            })
            .unwrap_or_default(),
        routes: routes
            .iter()
            .filter(|r| r.ingress.as_deref() == Some(name.as_str()))
            .map(|r| r.name.clone())
            .collect(),
        created: created(&i.metadata),
        name,
    }
}

/// Listener Gateway.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListenerView {
    pub name: String,
    pub port: Option<i64>,
    pub protocol: Option<String>,
    pub hostname: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayRow {
    pub name: String,
    pub class: Option<String>,
    pub listeners: Vec<ListenerView>,
    pub addresses: Vec<String>,
    pub accepted: Option<bool>,
    pub programmed: Option<bool>,
    pub created: Option<i64>,
}

pub fn gateway_row(o: &DynamicObject) -> GatewayRow {
    let conds = conditions(&o.data["status"]["conditions"]);
    GatewayRow {
        name: o.metadata.name.clone().unwrap_or_default(),
        class: o.data["spec"]["gatewayClassName"]
            .as_str()
            .map(String::from),
        listeners: o.data["spec"]["listeners"]
            .as_array()
            .map(|ls| {
                ls.iter()
                    .map(|l| ListenerView {
                        name: l["name"].as_str().unwrap_or_default().to_string(),
                        port: l["port"].as_i64(),
                        protocol: l["protocol"].as_str().map(String::from),
                        hostname: l["hostname"].as_str().map(String::from),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        addresses: o.data["status"]["addresses"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|x| x["value"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
        accepted: condition_flag(&conds, "Accepted"),
        programmed: condition_flag(&conds, "Programmed"),
        created: created(&o.metadata),
    }
}

/// Родительский Gateway маршрута и его условия для этого маршрута.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParentView {
    pub name: String,
    pub namespace: Option<String>,
    pub section: Option<String>,
    /// `None` — статуса от этого родителя ещё нет.
    pub accepted: Option<ConditionView>,
    pub resolved_refs: Option<ConditionView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayRouteRow {
    /// `HTTPRoute` или `GRPCRoute`.
    pub kind: String,
    pub name: String,
    pub hostnames: Vec<String>,
    pub parents: Vec<ParentView>,
    /// `service:port`.
    pub backends: Vec<String>,
    pub created: Option<i64>,
}

pub fn gateway_route_row(kind: &str, o: &DynamicObject) -> GatewayRouteRow {
    let spec = &o.data["spec"];
    let statuses = o.data["status"]["parents"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let same = |a: &serde_json::Value, b: &serde_json::Value| {
        a["name"] == b["name"]
            && a["namespace"] == b["namespace"]
            && a["sectionName"] == b["sectionName"]
    };
    let parents = spec["parentRefs"]
        .as_array()
        .map(|refs| {
            refs.iter()
                .map(|p| {
                    let conds = statuses
                        .iter()
                        .find(|s| same(&s["parentRef"], p))
                        .map(|s| conditions(&s["conditions"]))
                        .unwrap_or_default();
                    ParentView {
                        name: p["name"].as_str().unwrap_or_default().to_string(),
                        namespace: p["namespace"].as_str().map(String::from),
                        section: p["sectionName"].as_str().map(String::from),
                        accepted: condition(&conds, "Accepted").cloned(),
                        resolved_refs: condition(&conds, "ResolvedRefs").cloned(),
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let backends = spec["rules"]
        .as_array()
        .map(|rules| {
            rules
                .iter()
                .flat_map(|r| r["backendRefs"].as_array().cloned().unwrap_or_default())
                .map(|b| match b["port"].as_i64() {
                    Some(p) => format!("{}:{p}", b["name"].as_str().unwrap_or_default()),
                    None => b["name"].as_str().unwrap_or_default().to_string(),
                })
                .collect()
        })
        .unwrap_or_default();
    GatewayRouteRow {
        kind: kind.into(),
        name: o.metadata.name.clone().unwrap_or_default(),
        hostnames: spec["hostnames"]
            .as_array()
            .map(|h| {
                h.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
        parents,
        backends,
        created: created(&o.metadata),
    }
}

/// Сеть проекта. Вид без группы API в кластере — `None`; GRPCRoute появился в Gateway API 1.1,
/// и у более старой установки его нет (404 — тоже `None`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkView {
    pub services: Part<Vec<ServiceRow>>,
    /// Почему не видно endpoints сервисов (нет прав на EndpointSlices).
    pub endpoints_error: Option<AppError>,
    pub routes: Option<Part<Vec<RouteRow>>>,
    pub ingresses: Part<Vec<IngressRow>>,
    pub gateways: Option<Part<Vec<GatewayRow>>>,
    pub http_routes: Option<Part<Vec<GatewayRouteRow>>>,
    pub grpc_routes: Option<Part<Vec<GatewayRouteRow>>>,
}

pub async fn get_network(r: &ReadClient, namespace: &str, groups: &ApiGroups) -> NetworkView {
    let ns = Some(namespace);
    let lp = ListParams::default();
    let dynamic = |ar: ApiResource| {
        let lp = lp.clone();
        async move { r.list_dynamic(&ar, ns, &lp).await.map(|l| l.items) }
    };
    let routes = async {
        match groups.routes {
            true => Some(
                dynamic(routes())
                    .await
                    .map(|items| items.iter().map(route_row).collect::<Vec<_>>()),
            ),
            false => None,
        }
    };
    let gateway_api = |ar: ApiResource| async move {
        match groups.gateway_api {
            true => Some(dynamic(ar).await),
            false => None,
        }
    };
    let (services, slices, routes, ingresses, gateways, http, grpc) = tokio::join!(
        r.list::<Service>(ns, &lp),
        r.list::<EndpointSlice>(ns, &lp),
        routes,
        r.list::<Ingress>(ns, &lp),
        gateway_api(gateways()),
        gateway_api(http_routes()),
        gateway_api(grpc_routes()),
    );
    let (endpoints, endpoints_error) = match slices {
        Ok(l) => (Some(endpoints_by_service(&l.items)), None),
        Err(e) => (None, Some(e)),
    };
    let services: AppResult<Vec<ServiceRow>> = services.map(|l| {
        l.items
            .iter()
            .map(|s| {
                let count = endpoints.as_ref().map(|m| {
                    m.get(&name(&s.metadata))
                        .copied()
                        .unwrap_or(EndpointCount { ready: 0, total: 0 })
                });
                service_row(s, count)
            })
            .collect()
    });
    let route_rows = routes
        .as_ref()
        .and_then(|r| r.as_ref().ok())
        .cloned()
        .unwrap_or_default();
    let gateway_routes = |kind: &str, r: Option<AppResult<Vec<DynamicObject>>>| match r {
        None => None,
        Some(Err(e)) if e.code == ErrorCode::NotFound => None,
        Some(r) => Some(
            r.map(|items| items.iter().map(|o| gateway_route_row(kind, o)).collect())
                .into(),
        ),
    };
    NetworkView {
        services: services.into(),
        endpoints_error,
        routes: routes.map(Part::from),
        ingresses: ingresses
            .map(|l| {
                l.items
                    .iter()
                    .map(|i| ingress_row(i, &route_rows))
                    .collect()
            })
            .into(),
        gateways: gateways.map(|r| {
            r.map(|items| items.iter().map(gateway_row).collect())
                .into()
        }),
        http_routes: gateway_routes("HTTPRoute", http),
        grpc_routes: gateway_routes("GRPCRoute", grpc),
    }
}
````

<!-- block: file t08-src-tauri-src-k8s-testdata-route-cert-pem step=8.3 path=src-tauri/src/k8s/testdata/route-cert.pem -->
````text
-----BEGIN CERTIFICATE-----
MIIBczCCARigAwIBAgIUYM/uwck+xL8Pm/BGIU3qR0ON5cEwCgYIKoZIzj0EAwIw
HzEdMBsGA1UEAwwUd2ViLmFwcHMuZXhhbXBsZS5jb20wHhcNMjYxMDI1MTYyNjIz
WhcNMjYxMTI0MTYyNjIzWjAfMR0wGwYDVQQDDBR3ZWIuYXBwcy5leGFtcGxlLmNv
bTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABJtbLiM0/jC5Pr3i7ueRbSADYDsn
KX/F5tMaqClWbvCSVS9GHapOGtEWFntTgJogJlFvew9QEd80r10zMHyx5fOjMjAw
MB0GA1UdDgQWBBSu8mRlO7H8GVJe0Cmgp4C/pQSOBjAPBgNVHRMBAf8EBTADAQH/
MAoGCCqGSM49BAMCA0kAMEYCIQDwVLM1sslJn7sQNtryPrtPCXOB75oSTnl3bdYD
Si160QIhAM2bz+sC2zxmf3j+M6a0jDmlgfrWy1uG9LmwWPFBx5Ng
-----END CERTIFICATE-----
````

<!-- block: patch t08-src step=8.3 -->
````diff
diff --git a/PORTED.md b/PORTED.md
index 919fb42..8130550 100644
--- a/PORTED.md
+++ b/PORTED.md
@@ -122,3 +122,4 @@
 | `.github/workflows/ci.yml` | `.github/workflows/ci.yml` | `8bbd0bd` | адаптирован: без Kafka и MCP-крейта, стенд kind |
 | `docs/i18n/README.md` | `docs/i18n/README.md` | `8bbd0bd` | адаптирован: языки en и ru, без Kafka |
 | `docs/i18n/glossary.md` | `docs/i18n/glossary.md` | `8bbd0bd` | адаптирован: термины Kubernetes и OpenShift, en и ru |
+| `src-tauri/src/der.rs` | `src-tauri/src/tls/der.rs` | `8bbd0bd` | адаптирован: комментарий модуля |
diff --git a/src-tauri/src/commands/objects.rs b/src-tauri/src/commands/objects.rs
index 27f9489..264f349 100644
--- a/src-tauri/src/commands/objects.rs
+++ b/src-tauri/src/commands/objects.rs
@@ -4,6 +4,7 @@ use tauri::State;
 
 use crate::error::AppResult;
 use crate::k8s::events::{EventObject, EventRow};
+use crate::k8s::network::NetworkView;
 use crate::k8s::pods::{PodDetails, PodRow, WorkloadRef};
 use crate::k8s::read::Listed;
 use crate::k8s::workloads::{WorkloadDetails, WorkloadKind, WorkloadList};
@@ -85,3 +86,18 @@ pub async fn list_events(
         })
         .await
 }
+
+/// Сеть проекта: Services, Routes, Ingress, Gateway API — по группам API кластера.
+#[tauri::command]
+pub async fn get_network(
+    state: State<'_, AppState>,
+    connection_id: String,
+    namespace: String,
+) -> AppResult<NetworkView> {
+    let ns = namespace.clone();
+    state
+        .read_in(&connection_id, &namespace, |c| async move {
+            Ok(crate::k8s::network::get_network(&c.reader(), &ns, &c.info.api_groups).await)
+        })
+        .await
+}
diff --git a/src-tauri/src/k8s/mod.rs b/src-tauri/src/k8s/mod.rs
index 932ee43..6001445 100644
--- a/src-tauri/src/k8s/mod.rs
+++ b/src-tauri/src/k8s/mod.rs
@@ -8,6 +8,7 @@ pub mod containers;
 pub mod errors;
 pub mod events;
 pub mod kubeconfig;
+pub mod network;
 pub mod nodes;
 pub mod openshift;
 pub mod overview;
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index dac89a4..b0b6ecb 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -2,6 +2,7 @@
 pub mod i18n;
 pub mod auth;
 pub mod commands;
+pub mod der;
 mod dock_name;
 pub mod error;
 pub mod k8s;
@@ -82,6 +83,7 @@ pub fn run() {
             commands::objects::list_pods,
             commands::objects::get_pod,
             commands::objects::list_events,
+            commands::objects::get_network,
         ])
         .run(tauri::generate_context!())
         .expect("error while building tauri application");
````

- [ ] **Step 4: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 326 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 133`

- [ ] **Step 5: Commit.**

```bash
git add \
  PORTED.md \
  src-tauri/src/commands/objects.rs \
  src-tauri/src/der.rs \
  src-tauri/src/k8s/mod.rs \
  src-tauri/src/k8s/network.rs \
  src-tauri/src/k8s/testdata/route-cert.pem \
  src-tauri/src/lib.rs
git commit -s -m "feat(k8s): сеть проекта — Services и EndpointSlices, Routes со сроком сертификата, Ingress, Gateway API" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 9: Конфигурация, хранилище, квоты и YAML

**Files:**
- Create: `src-tauri/src/k8s/configs.rs`, `src-tauri/src/k8s/quantity.rs`, `src-tauri/src/k8s/quotas.rs`, `src-tauri/src/k8s/storage.rs`, `src-tauri/src/k8s/yaml.rs`
- Modify: `src-tauri/src/commands/objects.rs`, `src-tauri/src/k8s/mod.rs`, `src-tauri/src/lib.rs`

**Interfaces:**
- Consumes: задачи 3–8 (`ReadClient`, `view`, `workloads::deployment_configs`, `network::{routes, gateways, http_routes, grpc_routes}`).
- Produces: `k8s::quantity::{parse_quantity(&str) -> Option<f64>, ratio(used, hard)}`; `k8s::configs::{ConfigMapRow, SecretRow, ConfigView { config_maps, secrets }, config_map_row, secret_row, get_config, ConfigValue { text?, binary_size? }, config_map_values}`; `k8s::storage::{PvcRow, pvc_row, mounts, list_pvcs}`; `k8s::quotas::{QuotaEntry, QuotaView, LimitView, LimitRangeView, QuotasView, quota_view, limit_range_view, get_quotas}`; `k8s::yaml::{YamlKind, object_yaml(&ReadClient, ns, YamlKind, name), to_yaml(&serde_json::Value)}`; команды `get_config`, `get_config_map_values(connectionId, namespace, name)`, `list_pvcs`, `get_quotas`, `get_yaml(connectionId, namespace, kind, name)`.

Спец. §6.3–6.5, §12. ConfigMaps — ключи (и двоичные ключи), значения — отдельной командой по щелчку. Secrets — имя, тип и ключи: значения отбрасываются в Rust и в интерфейс не уходят никогда. PVC — фаза, класс, выделено и запрошено, режимы доступа, том и поды, которые его монтируют (если поды читаются). Квоты — каждая ResourceQuota (использовано, лимит и доля — quantity разбирается числом: `500m`, `128Mi`, `2e3`) и LimitRange по ресурсам. YAML — объект без `metadata.managedFields`, ключи по алфавиту, как в `kubectl get -o yaml`; вывод JSON → YAML свой (крейт `serde_yaml` больше не поддерживается), строки, похожие на числа и `true`, — в кавычках, многострочные — блоком `|`. Secret в `YamlKind` нет.

- [ ] **Step 1: Тесты.**

<!-- block: file t09-src-tauri-src-k8s-configs-rs-tests step=9.1 path=src-tauri/src/k8s/configs.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use std::time::Duration;

    #[tokio::test]
    async fn config_maps_with_keys_and_secrets_without_values() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/api/v1/namespaces/team-a/configmaps" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "ConfigMapList", "metadata": {}, "items": [
                    {"metadata": {"name": "web"}, "data": {"LOG_LEVEL": "debug", "app.yaml": "a: 1"},
                        "binaryData": {"logo.png": "iVBORw0K"}}]}),
            ),
            "/api/v1/namespaces/team-a/secrets" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "SecretList", "metadata": {}, "items": [
                    {"metadata": {"name": "db"}, "type": "Opaque",
                        "data": {"password": "czNjcjN0", "user": "YWRtaW4="}},
                    {"metadata": {"name": "web-tls"}, "type": "kubernetes.io/tls",
                        "data": {"tls.crt": "eA==", "tls.key": "eQ=="}}]}),
            ),
            _ => status(404, "NotFound", "x"),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let v = get_config(&r, "team-a").await;
        let cms = v.config_maps.data.unwrap();
        assert_eq!(cms[0].keys, ["LOG_LEVEL", "app.yaml"]);
        assert_eq!(cms[0].binary_keys, ["logo.png"]);
        let secrets = v.secrets.data.unwrap();
        assert_eq!(secrets[0].keys, ["password", "user"]);
        assert_eq!(secrets[1].kind, "kubernetes.io/tls");
        let json = serde_json::to_string(&secrets).unwrap();
        assert!(
            !json.contains("czNjcjN0") && !json.contains("s3cr3t"),
            "{json}"
        );
    }

    #[tokio::test]
    async fn values_of_a_config_map_on_request() {
        let (client, server) = mock_client(|_| {
            (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "web"},
                    "data": {"LOG_LEVEL": "debug"}, "binaryData": {"logo.png": "AAECAw=="}}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let v = config_map_values(&r, "team-a", "web").await.unwrap();
        assert_eq!(v["LOG_LEVEL"].text.as_deref(), Some("debug"));
        assert_eq!(v["logo.png"].binary_size, Some(4));
        drop(r);
        assert_eq!(
            server.await.unwrap()[0].path(),
            "/api/v1/namespaces/team-a/configmaps/web"
        );
    }

    #[tokio::test]
    async fn forbidden_secrets_do_not_hide_config_maps() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/api/v1/namespaces/team-a/secrets" => status(
                403,
                "Forbidden",
                r#"secrets is forbidden: User "u" cannot list resource "secrets" in API group "" in the namespace "team-a""#,
            ),
            _ => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "ConfigMapList", "metadata": {}, "items": []}),
            ),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let v = get_config(&r, "team-a").await;
        assert!(v.config_maps.data.is_some());
        assert_eq!(v.secrets.error.unwrap().params["resource"], "secrets");
    }
}
````

<!-- block: file t09-src-tauri-src-k8s-quantity-rs-tests step=9.1 path=src-tauri/src/k8s/quantity.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantities() {
        for (q, v) in [
            ("1", 1.0),
            ("500m", 0.5),
            ("2.5", 2.5),
            ("128Mi", 134_217_728.0),
            ("1Gi", 1_073_741_824.0),
            ("1k", 1000.0),
            ("2e3", 2000.0),
            ("1.5G", 1.5e9),
            ("100n", 1e-7),
            ("1E", 1e18),
            ("1Ei", 1024f64.powi(6)),
            ("2E3", 2000.0),
        ] {
            let got = parse_quantity(q).unwrap();
            assert!((got - v).abs() < 1e-9 * v.max(1.0), "{q}: {got}");
        }
        for bad in ["", "Mi", "1Xi", "e3", "abc"] {
            assert_eq!(parse_quantity(bad), None, "{bad}");
        }
    }

    #[test]
    fn ratios() {
        assert_eq!(ratio("500m", "1"), Some(0.5));
        assert_eq!(ratio("9", "10"), Some(0.9));
        assert_eq!(ratio("1Gi", "2Gi"), Some(0.5));
        assert_eq!(ratio("1", "0"), None);
        assert_eq!(ratio("x", "1"), None);
    }
}
````

<!-- block: file t09-src-tauri-src-k8s-quotas-rs-tests step=9.1 path=src-tauri/src/k8s/quotas.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::mock_client;
    use std::time::Duration;

    #[tokio::test]
    async fn quotas_with_usage_and_limit_ranges_per_resource() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/api/v1/namespaces/team-a/resourcequotas" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "ResourceQuotaList", "metadata": {}, "items": [
                    {"metadata": {"name": "compute"},
                        "spec": {"hard": {"requests.cpu": "4", "limits.memory": "8Gi", "pods": "20"},
                            "scopes": ["NotTerminating"]},
                        "status": {"hard": {"requests.cpu": "4", "limits.memory": "8Gi", "pods": "20"},
                            "used": {"requests.cpu": "3600m", "limits.memory": "2Gi"}}}]}),
            ),
            _ => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "LimitRangeList", "metadata": {}, "items": [
                {"metadata": {"name": "defaults"}, "spec": {"limits": [
                    {"type": "Container", "default": {"cpu": "500m", "memory": "512Mi"},
                        "defaultRequest": {"cpu": "100m"}, "max": {"memory": "2Gi"},
                        "maxLimitRequestRatio": {"cpu": "10"}},
                    {"type": "PersistentVolumeClaim", "min": {"storage": "1Gi"}}
                ]}}]}),
            ),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let v = get_quotas(&r, "team-a").await;
        let q = &v.quotas.data.unwrap()[0];
        assert_eq!(q.scopes, ["NotTerminating"]);
        let cpu = q
            .entries
            .iter()
            .find(|e| e.resource == "requests.cpu")
            .unwrap();
        assert_eq!(cpu.used.as_deref(), Some("3600m"));
        assert!((cpu.ratio.unwrap() - 0.9).abs() < 1e-9);
        let pods = q.entries.iter().find(|e| e.resource == "pods").unwrap();
        assert_eq!((pods.used.as_deref(), pods.ratio), (None, None));
        let lr = &v.limit_ranges.data.unwrap()[0];
        let views: Vec<_> = lr
            .limits
            .iter()
            .map(|l| (l.kind.as_str(), l.resource.as_str()))
            .collect();
        assert_eq!(
            views,
            [
                ("Container", "cpu"),
                ("Container", "memory"),
                ("PersistentVolumeClaim", "storage")
            ]
        );
        let cpu = &lr.limits[0];
        assert_eq!(
            (
                cpu.default.as_deref(),
                cpu.default_request.as_deref(),
                cpu.max_limit_request_ratio.as_deref()
            ),
            (Some("500m"), Some("100m"), Some("10"))
        );
        assert_eq!(lr.limits[1].max.as_deref(), Some("2Gi"));
    }
}
````

<!-- block: file t09-src-tauri-src-k8s-storage-rs-tests step=9.1 path=src-tauri/src/k8s/storage.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use std::time::Duration;

    fn pvcs() -> serde_json::Value {
        serde_json::json!({"apiVersion": "v1", "kind": "PersistentVolumeClaimList", "metadata": {}, "items": [
            {"metadata": {"name": "data"},
                "spec": {"storageClassName": "standard", "accessModes": ["ReadWriteOnce"], "volumeMode": "Filesystem",
                    "volumeName": "pvc-1", "resources": {"requests": {"storage": "10Gi"}}},
                "status": {"phase": "Bound", "capacity": {"storage": "10Gi"}}},
            {"metadata": {"name": "cache"},
                "spec": {"accessModes": ["ReadWriteMany"], "resources": {"requests": {"storage": "1Gi"}}},
                "status": {"phase": "Pending"}}
        ]})
    }

    #[tokio::test]
    async fn claims_with_the_pods_that_mount_them() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/api/v1/namespaces/team-a/persistentvolumeclaims" => (200, pvcs()),
            _ => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": [
                    {"metadata": {"name": "db-0"}, "spec": {"containers": [],
                        "volumes": [{"name": "d", "persistentVolumeClaim": {"claimName": "data"}}]}},
                    {"metadata": {"name": "web"}, "spec": {"containers": [], "volumes": [{"name": "t", "emptyDir": {}}]}}
                ]}),
            ),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let l = list_pvcs(&r, "team-a").await.unwrap();
        let data = &l.items[0];
        assert_eq!(data.phase.as_deref(), Some("Bound"));
        assert_eq!(data.class.as_deref(), Some("standard"));
        assert_eq!(
            (data.capacity.as_deref(), data.requested.as_deref()),
            (Some("10Gi"), Some("10Gi"))
        );
        assert_eq!(data.access_modes, ["ReadWriteOnce"]);
        assert_eq!(data.mounted_by.as_deref(), Some(&["db-0".to_string()][..]));
        let cache = &l.items[1];
        assert_eq!(
            (cache.phase.as_deref(), cache.capacity.as_deref()),
            (Some("Pending"), None)
        );
        assert_eq!(cache.mounted_by.as_deref(), Some(&[][..]));
    }

    #[tokio::test]
    async fn without_pods_it_is_unknown_who_mounts() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/api/v1/namespaces/team-a/persistentvolumeclaims" => (200, pvcs()),
            _ => status(403, "Forbidden", "pods is forbidden"),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let l = list_pvcs(&r, "team-a").await.unwrap();
        assert!(l.items.iter().all(|p| p.mounted_by.is_none()));
    }
}
````

<!-- block: file t09-src-tauri-src-k8s-yaml-rs-tests step=9.1 path=src-tauri/src/k8s/yaml.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::mock_client;
    use std::time::Duration;

    /// Ключи — по алфавиту, как в `kubectl get -o yaml`.
    #[test]
    fn nested_maps_lists_and_scalars() {
        let v = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "web", "labels": {"app.kubernetes.io/name": "web"}, "annotations": {}},
            "spec": {"containers": [
                {"name": "web", "args": ["--port", "8080"], "ports": [{"containerPort": 8080}], "env": []},
                {"name": "side", "tty": true, "command": null}
            ]}
        });
        assert_eq!(
            to_yaml(&v),
            "apiVersion: v1\nkind: Pod\nmetadata:\n  annotations: {}\n  labels:\n    app.kubernetes.io/name: web\n  name: web\nspec:\n  containers:\n    - args:\n        - --port\n        - \"8080\"\n      env: []\n      name: web\n      ports:\n        - containerPort: 8080\n    - command: null\n      name: side\n      tty: true\n"
        );
    }

    #[test]
    fn strings_that_need_quotes_or_blocks() {
        for (s, yaml) in [
            ("true", "\"true\""),
            ("1.0", "\"1.0\""),
            ("", "\"\""),
            ("a: b", "\"a: b\""),
            ("*", "\"*\""),
            ("- x", "\"- x\""),
            ("-", "\"-\""),
            ("--port", "--port"),
            ("\nx", "\"\\nx\""),
            (" lead", "\" lead\""),
            ("x #y", "\"x #y\""),
            ("tab\there", "\"tab\\there\""),
            ("registry/app:1.2", "registry/app:1.2"),
            ("Running", "Running"),
        ] {
            assert_eq!(scalar(s), yaml, "{s:?}");
        }
        let v = serde_json::json!({"a": "line 1\nline 2\n", "b": "x\n\ny", "c": "keep\n\n"});
        assert_eq!(
            to_yaml(&v),
            "a: |\n  line 1\n  line 2\nb: |-\n  x\n\n  y\nc: |+\n  keep\n\n"
        );
        assert_eq!(to_yaml(&serde_json::json!("x")), "x\n");
    }

    #[tokio::test]
    async fn yaml_of_a_workload_without_managed_fields() {
        let (client, server) = mock_client(|_| {
            (
                200,
                serde_json::json!({"apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": {"name": "web", "namespace": "team-a",
                        "managedFields": [{"manager": "kubectl", "operation": "Apply"}]},
                    "spec": {"replicas": 2}}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let y = object_yaml(&r, "team-a", YamlKind::Deployment, "web")
            .await
            .unwrap();
        assert!(y.contains("replicas: 2") && y.contains("name: web"), "{y}");
        assert!(!y.contains("managedFields"), "{y}");
        drop(r);
        assert_eq!(
            server.await.unwrap()[0].path(),
            "/apis/apps/v1/namespaces/team-a/deployments/web"
        );
    }

    #[test]
    fn secrets_have_no_yaml() {
        assert!(serde_json::from_value::<YamlKind>("Secret".into()).is_err());
        assert_eq!(
            serde_json::from_value::<YamlKind>("HTTPRoute".into()).unwrap(),
            YamlKind::HttpRoute
        );
    }
}
````

- [ ] **Step 2: Реализация.**

<!-- block: file t09-src-tauri-src-k8s-configs-rs-impl step=9.2 path=src-tauri/src/k8s/configs.rs part=impl -->
````rust
//! Конфигурация проекта (спец. §6.3): ConfigMaps — ключи, значения по запросу; Secrets — имя,
//! тип и ключи. Значения Secret из Rust не уходят никогда.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::api::ListParams;
use serde::Serialize;

use crate::error::AppResult;
use crate::k8s::read::ReadClient;
use crate::k8s::view::{Part, created, name};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapRow {
    pub name: String,
    pub keys: Vec<String>,
    /// Ключи `binaryData`.
    pub binary_keys: Vec<String>,
    pub created: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretRow {
    pub name: String,
    /// `Opaque`, `kubernetes.io/tls`, `kubernetes.io/dockerconfigjson`…
    #[serde(rename = "type")]
    pub kind: String,
    pub keys: Vec<String>,
    pub created: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigView {
    pub config_maps: Part<Vec<ConfigMapRow>>,
    pub secrets: Part<Vec<SecretRow>>,
}

pub fn config_map_row(c: &ConfigMap) -> ConfigMapRow {
    ConfigMapRow {
        name: name(&c.metadata),
        keys: c.data.iter().flatten().map(|(k, _)| k.clone()).collect(),
        binary_keys: c
            .binary_data
            .iter()
            .flatten()
            .map(|(k, _)| k.clone())
            .collect(),
        created: created(&c.metadata),
    }
}

/// Строка Secret: только имя, тип и ключи; значения отбрасываются здесь же.
pub fn secret_row(s: &Secret) -> SecretRow {
    SecretRow {
        name: name(&s.metadata),
        kind: s.type_.clone().unwrap_or_else(|| "Opaque".into()),
        keys: s.data.iter().flatten().map(|(k, _)| k.clone()).collect(),
        created: created(&s.metadata),
    }
}

pub async fn get_config(r: &ReadClient, namespace: &str) -> ConfigView {
    let ns = Some(namespace);
    let lp = ListParams::default();
    let (config_maps, secrets) =
        tokio::join!(r.list::<ConfigMap>(ns, &lp), r.list::<Secret>(ns, &lp));
    ConfigView {
        config_maps: config_maps
            .map(|l| l.items.iter().map(config_map_row).collect())
            .into(),
        secrets: secrets
            .map(|l| l.items.iter().map(secret_row).collect())
            .into(),
    }
}

/// Значение ключа ConfigMap: текст или размер двоичных данных.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigValue {
    pub text: Option<String>,
    pub binary_size: Option<usize>,
}

/// Значения ConfigMap — по щелчку в интерфейсе.
pub async fn config_map_values(
    r: &ReadClient,
    namespace: &str,
    config_map: &str,
) -> AppResult<BTreeMap<String, ConfigValue>> {
    let c: ConfigMap = r.get(namespace, config_map).await?;
    let mut out: BTreeMap<String, ConfigValue> = c
        .data
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| {
            (
                k,
                ConfigValue {
                    text: Some(v),
                    binary_size: None,
                },
            )
        })
        .collect();
    for (k, v) in c.binary_data.unwrap_or_default() {
        out.insert(
            k,
            ConfigValue {
                text: None,
                binary_size: Some(v.0.len()),
            },
        );
    }
    Ok(out)
}
````

<!-- block: file t09-src-tauri-src-k8s-quantity-rs-impl step=9.2 path=src-tauri/src/k8s/quantity.rs part=impl -->
````rust
//! Quantity Kubernetes числом: `500m` → 0,5, `128Mi` → 134 217 728, `2e3` → 2000. Нужно,
//! чтобы считать использование квот (и дальше — ресурсы контейнеров, этап O3).

/// Значение quantity или `None`, если строка — не quantity.
pub fn parse_quantity(q: &str) -> Option<f64> {
    let q = q.trim();
    let (number, suffix) = q.split_at(number_end(q.as_bytes()));
    let multiplier = match suffix {
        "" => 1.0,
        "n" => 1e-9,
        "u" => 1e-6,
        "m" => 1e-3,
        "k" => 1e3,
        "M" => 1e6,
        "G" => 1e9,
        "T" => 1e12,
        "P" => 1e15,
        "E" => 1e18,
        "Ki" => 1024.0,
        "Mi" => 1024f64.powi(2),
        "Gi" => 1024f64.powi(3),
        "Ti" => 1024f64.powi(4),
        "Pi" => 1024f64.powi(5),
        "Ei" => 1024f64.powi(6),
        _ => return None,
    };
    if number.is_empty() {
        return None;
    }
    number.parse::<f64>().ok().map(|n| n * multiplier)
}

/// Где кончается число: знак, цифры и точка, затем показатель `e3` (`E` без цифр — суффикс).
fn number_end(b: &[u8]) -> usize {
    let digits = |mut i: usize| {
        while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.') {
            i += 1;
        }
        i
    };
    let mut i = digits(usize::from(
        b.first().is_some_and(|c| *c == b'+' || *c == b'-'),
    ));
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        let mut j = i + 1;
        if j < b.len() && (b[j] == b'+' || b[j] == b'-') {
            j += 1;
        }
        if j < b.len() && b[j].is_ascii_digit() {
            i = digits(j);
        }
    }
    i
}

/// Доля `used` от `hard`; `None`, если что-то не разобрано или лимит нулевой.
pub fn ratio(used: &str, hard: &str) -> Option<f64> {
    let hard = parse_quantity(hard).filter(|h| *h > 0.0)?;
    Some(parse_quantity(used)? / hard)
}
````

<!-- block: file t09-src-tauri-src-k8s-quotas-rs-impl step=9.2 path=src-tauri/src/k8s/quotas.rs part=impl -->
````rust
//! Квоты проекта (спец. §6.3): ResourceQuota — использовано и лимит по каждому ресурсу,
//! LimitRange — границы и значения по умолчанию.

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::core::v1::{LimitRange, ResourceQuota};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::ListParams;
use serde::Serialize;

use crate::k8s::quantity::ratio;
use crate::k8s::read::ReadClient;
use crate::k8s::view::{Part, name};

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaEntry {
    /// `requests.cpu`, `limits.memory`, `pods`, `count/deployments.apps`…
    pub resource: String,
    pub used: Option<String>,
    pub hard: String,
    /// Использовано / лимит (0,9 — 90 %), если оба разобраны.
    pub ratio: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaView {
    pub name: String,
    pub scopes: Vec<String>,
    pub entries: Vec<QuotaEntry>,
}

/// Граница LimitRange для одного ресурса одного вида объектов.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitView {
    /// `Container`, `Pod`, `PersistentVolumeClaim`.
    #[serde(rename = "type")]
    pub kind: String,
    pub resource: String,
    pub min: Option<String>,
    pub max: Option<String>,
    pub default: Option<String>,
    pub default_request: Option<String>,
    pub max_limit_request_ratio: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitRangeView {
    pub name: String,
    pub limits: Vec<LimitView>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotasView {
    pub quotas: Part<Vec<QuotaView>>,
    pub limit_ranges: Part<Vec<LimitRangeView>>,
}

pub fn quota_view(q: &ResourceQuota) -> QuotaView {
    let hard = q
        .status
        .as_ref()
        .and_then(|s| s.hard.clone())
        .or_else(|| q.spec.as_ref().and_then(|s| s.hard.clone()))
        .unwrap_or_default();
    let used = q
        .status
        .as_ref()
        .and_then(|s| s.used.clone())
        .unwrap_or_default();
    QuotaView {
        name: name(&q.metadata),
        scopes: q
            .spec
            .as_ref()
            .and_then(|s| s.scopes.clone())
            .unwrap_or_default(),
        entries: hard
            .iter()
            .map(|(resource, h)| {
                let u = used.get(resource).map(|u| u.0.clone());
                QuotaEntry {
                    resource: resource.clone(),
                    ratio: u.as_deref().and_then(|u| ratio(u, &h.0)),
                    used: u,
                    hard: h.0.clone(),
                }
            })
            .collect(),
    }
}

pub fn limit_range_view(l: &LimitRange) -> LimitRangeView {
    let get = |m: &Option<BTreeMap<String, Quantity>>, r: &str| {
        m.as_ref().and_then(|m| m.get(r)).map(|q| q.0.clone())
    };
    let limits = l
        .spec
        .as_ref()
        .map(|s| {
            s.limits
                .iter()
                .flat_map(|item| {
                    let resources: BTreeSet<String> = [
                        &item.min,
                        &item.max,
                        &item.default,
                        &item.default_request,
                        &item.max_limit_request_ratio,
                    ]
                    .into_iter()
                    .flatten()
                    .flat_map(|m| m.keys().cloned())
                    .collect();
                    resources
                        .into_iter()
                        .map(|r| LimitView {
                            kind: item.type_.clone(),
                            min: get(&item.min, &r),
                            max: get(&item.max, &r),
                            default: get(&item.default, &r),
                            default_request: get(&item.default_request, &r),
                            max_limit_request_ratio: get(&item.max_limit_request_ratio, &r),
                            resource: r,
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        })
        .unwrap_or_default();
    LimitRangeView {
        name: name(&l.metadata),
        limits,
    }
}

pub async fn get_quotas(r: &ReadClient, namespace: &str) -> QuotasView {
    let ns = Some(namespace);
    let lp = ListParams::default();
    let (quotas, ranges) = tokio::join!(
        r.list::<ResourceQuota>(ns, &lp),
        r.list::<LimitRange>(ns, &lp)
    );
    QuotasView {
        quotas: quotas
            .map(|l| l.items.iter().map(quota_view).collect())
            .into(),
        limit_ranges: ranges
            .map(|l| l.items.iter().map(limit_range_view).collect())
            .into(),
    }
}
````

<!-- block: file t09-src-tauri-src-k8s-storage-rs-impl step=9.2 path=src-tauri/src/k8s/storage.rs part=impl -->
````rust
//! Хранилище проекта (спец. §6.3): PVC — статус, класс, ёмкость, режим доступа и кем смонтирован.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
use kube::api::ListParams;
use serde::Serialize;

use crate::error::AppResult;
use crate::k8s::read::{Listed, ReadClient};
use crate::k8s::view::{created, name};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PvcRow {
    pub name: String,
    /// `Pending`, `Bound`, `Lost`.
    pub phase: Option<String>,
    pub class: Option<String>,
    /// Выделено (`status.capacity`) и запрошено (`spec.resources.requests`).
    pub capacity: Option<String>,
    pub requested: Option<String>,
    pub access_modes: Vec<String>,
    pub volume_mode: Option<String>,
    pub volume: Option<String>,
    /// Поды, которые монтируют PVC; `None` — поды не читаются.
    pub mounted_by: Option<Vec<String>>,
    pub created: Option<i64>,
}

pub fn pvc_row(p: &PersistentVolumeClaim, mounted_by: Option<Vec<String>>) -> PvcRow {
    let spec = p.spec.as_ref();
    let status = p.status.as_ref();
    let storage = |m: Option<
        &BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>,
    >| { m.and_then(|m| m.get("storage")).map(|q| q.0.clone()) };
    PvcRow {
        name: name(&p.metadata),
        phase: status.and_then(|s| s.phase.clone()),
        class: spec.and_then(|s| s.storage_class_name.clone()),
        capacity: storage(status.and_then(|s| s.capacity.as_ref())),
        requested: storage(
            spec.and_then(|s| s.resources.as_ref())
                .and_then(|r| r.requests.as_ref()),
        ),
        access_modes: spec
            .and_then(|s| s.access_modes.clone())
            .unwrap_or_default(),
        volume_mode: spec.and_then(|s| s.volume_mode.clone()),
        volume: spec.and_then(|s| s.volume_name.clone()),
        mounted_by,
        created: created(&p.metadata),
    }
}

/// Какие поды монтируют какие PVC.
pub fn mounts(pods: &[Pod]) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for p in pods {
        let claims = p
            .spec
            .as_ref()
            .and_then(|s| s.volumes.as_ref())
            .into_iter()
            .flatten()
            .filter_map(|v| v.persistent_volume_claim.as_ref())
            .map(|c| c.claim_name.clone());
        for claim in claims {
            out.entry(claim).or_default().push(name(&p.metadata));
        }
    }
    out
}

/// PVC проекта; кто их монтирует, видно, если читаются поды.
pub async fn list_pvcs(r: &ReadClient, namespace: &str) -> AppResult<Listed<PvcRow>> {
    let ns = Some(namespace);
    let lp = ListParams::default();
    let (pvcs, pods) = tokio::join!(
        r.list::<PersistentVolumeClaim>(ns, &lp),
        r.list::<Pod>(ns, &lp)
    );
    let mounts = pods.ok().map(|l| mounts(&l.items));
    Ok(pvcs?.map(|p| {
        let by = mounts
            .as_ref()
            .map(|m| m.get(&name(&p.metadata)).cloned().unwrap_or_default());
        pvc_row(&p, by)
    }))
}
````

<!-- block: file t09-src-tauri-src-k8s-yaml-rs-impl step=9.2 path=src-tauri/src/k8s/yaml.rs part=impl -->
````rust
//! YAML объекта только для чтения (спец. §6.4–6.5): объект из API без `metadata.managedFields`,
//! записанный в YAML. Свой вывод JSON → YAML вместо крейта: нужно только записывать, а
//! `serde_yaml` больше не поддерживается. Secret сюда не попадает — его вида нет в `YamlKind`.

use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Pod, Service};
use k8s_openapi::api::networking::v1::Ingress;
use kube::api::ApiResource;
use serde::Deserialize;

use crate::error::AppResult;
use crate::k8s::read::ReadClient;

/// Виды объектов, YAML которых показывает интерфейс.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum YamlKind {
    Pod,
    Deployment,
    StatefulSet,
    DaemonSet,
    DeploymentConfig,
    Job,
    CronJob,
    Service,
    Route,
    Ingress,
    ConfigMap,
    PersistentVolumeClaim,
    Gateway,
    #[serde(rename = "HTTPRoute")]
    HttpRoute,
    #[serde(rename = "GRPCRoute")]
    GrpcRoute,
}

impl YamlKind {
    fn resource(self) -> ApiResource {
        use crate::k8s::network;
        match self {
            YamlKind::Pod => ApiResource::erase::<Pod>(&()),
            YamlKind::Deployment => ApiResource::erase::<Deployment>(&()),
            YamlKind::StatefulSet => ApiResource::erase::<StatefulSet>(&()),
            YamlKind::DaemonSet => ApiResource::erase::<DaemonSet>(&()),
            YamlKind::DeploymentConfig => crate::k8s::workloads::deployment_configs(),
            YamlKind::Job => ApiResource::erase::<Job>(&()),
            YamlKind::CronJob => ApiResource::erase::<CronJob>(&()),
            YamlKind::Service => ApiResource::erase::<Service>(&()),
            YamlKind::Route => network::routes(),
            YamlKind::Ingress => ApiResource::erase::<Ingress>(&()),
            YamlKind::ConfigMap => ApiResource::erase::<ConfigMap>(&()),
            YamlKind::PersistentVolumeClaim => ApiResource::erase::<PersistentVolumeClaim>(&()),
            YamlKind::Gateway => network::gateways(),
            YamlKind::HttpRoute => network::http_routes(),
            YamlKind::GrpcRoute => network::grpc_routes(),
        }
    }
}

/// YAML объекта без `managedFields`.
pub async fn object_yaml(
    r: &ReadClient,
    namespace: &str,
    kind: YamlKind,
    object: &str,
) -> AppResult<String> {
    let o = r
        .get_dynamic(&kind.resource(), Some(namespace), object)
        .await?;
    let mut v = serde_json::to_value(&o)?;
    if let Some(meta) = v.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        meta.remove("managedFields");
    }
    Ok(to_yaml(&v))
}

/// Строку можно записать без кавычек: она не похожа на число, `true`, `null` и не содержит
/// символов, которые YAML понял бы иначе.
fn plain(s: &str) -> bool {
    const RESERVED: &[&str] = &[
        "true", "false", "yes", "no", "on", "off", "y", "n", "null", "~",
    ];
    let Some(first) = s.chars().next() else {
        return false;
    };
    let number_like = s
        .trim_start_matches(['+', '-'])
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_digit() || c == '.');
    // `-`, `?` и `:` — знаки YAML, только если за ними пробел (`--port` — обычная строка).
    let indicator = match first {
        '-' | '?' | ':' => s.chars().nth(1).is_none_or(|c| c == ' '),
        c => ",[]{}#&*!|>'\"%@`".contains(c),
    };
    !RESERVED.contains(&s.to_lowercase().as_str())
        && !number_like
        && !indicator
        && !s.starts_with(' ')
        && !s.ends_with(' ')
        && !s.ends_with(':')
        && !s.contains(": ")
        && !s.contains(" #")
        && !s.chars().any(|c| c.is_control())
}

fn scalar(s: &str) -> String {
    if plain(s) {
        s.to_string()
    } else {
        // Строка JSON — допустимая строка YAML в двойных кавычках.
        serde_json::to_string(s).unwrap_or_default()
    }
}

/// Многострочный текст — блоком `|`: `|-` без перевода строки в конце, `|+`, если их несколько.
fn block(s: &str, indent: usize, out: &mut String) -> bool {
    // Первая строка задаёт отступ блока: она не может быть пустой или начинаться с пробела.
    if !s.contains('\n')
        || s.starts_with(['\n', ' ', '\t'])
        || s.chars().any(|c| c.is_control() && c != '\n')
    {
        return false;
    }
    let body = s.trim_end_matches('\n');
    let chomp = match s.len() - body.len() {
        0 => "-",
        1 => "",
        _ => "+",
    };
    out.push('|');
    out.push_str(chomp);
    out.push('\n');
    let pad = " ".repeat(indent);
    let lines: Vec<&str> = if chomp == "+" {
        s.split('\n').collect::<Vec<_>>()[..s.matches('\n').count()].to_vec()
    } else {
        body.split('\n').collect()
    };
    for line in lines {
        if !line.is_empty() {
            out.push_str(&pad);
            out.push_str(line);
        }
        out.push('\n');
    }
    true
}

fn value(v: &serde_json::Value, indent: usize, out: &mut String) {
    use serde_json::Value;
    let pad = " ".repeat(indent);
    match v {
        Value::Object(m) if !m.is_empty() => {
            out.push('\n');
            for (k, child) in m {
                out.push_str(&pad);
                out.push_str(&scalar(k));
                out.push(':');
                inline(child, indent + 2, out);
            }
        }
        Value::Array(a) if !a.is_empty() => {
            out.push('\n');
            for item in a {
                out.push_str(&pad);
                out.push('-');
                match item {
                    Value::Object(m) if !m.is_empty() => {
                        // Первый ключ — на строке с `-`, остальные — под ним.
                        let mut first = true;
                        for (k, child) in m {
                            if first {
                                out.push(' ');
                                first = false;
                            } else {
                                out.push_str(&pad);
                                out.push_str("  ");
                            }
                            out.push_str(&scalar(k));
                            out.push(':');
                            inline(child, indent + 4, out);
                        }
                    }
                    _ => inline(item, indent + 2, out),
                }
            }
        }
        // Скаляры пишет `inline`.
        _ => unreachable!("scalars are written by inline"),
    }
}

/// Значение после `ключ:` или `-`: скаляр на той же строке, коллекция — с новой.
fn inline(v: &serde_json::Value, indent: usize, out: &mut String) {
    use serde_json::Value;
    match v {
        Value::Object(m) if !m.is_empty() => value(v, indent, out),
        Value::Array(a) if !a.is_empty() => value(v, indent, out),
        Value::Object(_) => out.push_str(" {}\n"),
        Value::Array(_) => out.push_str(" []\n"),
        Value::Null => out.push_str(" null\n"),
        Value::Bool(b) => out.push_str(&format!(" {b}\n")),
        Value::Number(n) => out.push_str(&format!(" {n}\n")),
        Value::String(s) => {
            out.push(' ');
            if !block(s, indent, out) {
                out.push_str(&scalar(s));
                out.push('\n');
            }
        }
    }
}

/// YAML документа JSON.
pub fn to_yaml(v: &serde_json::Value) -> String {
    let mut out = String::new();
    match v {
        serde_json::Value::Object(m) if !m.is_empty() => value(v, 0, &mut out),
        serde_json::Value::Array(a) if !a.is_empty() => value(v, 0, &mut out),
        _ => {
            inline(v, 0, &mut out);
            return out.trim_start().to_string();
        }
    }
    out.trim_start_matches('\n').to_string()
}
````

<!-- block: patch t09-src step=9.2 -->
````diff
diff --git a/src-tauri/src/commands/objects.rs b/src-tauri/src/commands/objects.rs
index 264f349..1664dbf 100644
--- a/src-tauri/src/commands/objects.rs
+++ b/src-tauri/src/commands/objects.rs
@@ -1,13 +1,19 @@
 //! Объекты проекта (спец. §6.3–6.5): чтение в namespace; ответ 403 сбрасывает кэш прав проекта.
 
+use std::collections::BTreeMap;
+
 use tauri::State;
 
 use crate::error::AppResult;
+use crate::k8s::configs::{ConfigValue, ConfigView};
 use crate::k8s::events::{EventObject, EventRow};
 use crate::k8s::network::NetworkView;
 use crate::k8s::pods::{PodDetails, PodRow, WorkloadRef};
+use crate::k8s::quotas::QuotasView;
 use crate::k8s::read::Listed;
+use crate::k8s::storage::PvcRow;
 use crate::k8s::workloads::{WorkloadDetails, WorkloadKind, WorkloadList};
+use crate::k8s::yaml::YamlKind;
 use crate::state::AppState;
 
 #[tauri::command]
@@ -101,3 +107,79 @@ pub async fn get_network(
         })
         .await
 }
+
+/// ConfigMaps (ключи) и Secrets (имена, типы и ключи — без значений).
+#[tauri::command]
+pub async fn get_config(
+    state: State<'_, AppState>,
+    connection_id: String,
+    namespace: String,
+) -> AppResult<ConfigView> {
+    let ns = namespace.clone();
+    state
+        .read_in(&connection_id, &namespace, |c| async move {
+            Ok(crate::k8s::configs::get_config(&c.reader(), &ns).await)
+        })
+        .await
+}
+
+/// Значения ConfigMap — по щелчку.
+#[tauri::command]
+pub async fn get_config_map_values(
+    state: State<'_, AppState>,
+    connection_id: String,
+    namespace: String,
+    name: String,
+) -> AppResult<BTreeMap<String, ConfigValue>> {
+    let ns = namespace.clone();
+    state
+        .read_in(&connection_id, &namespace, |c| async move {
+            crate::k8s::configs::config_map_values(&c.reader(), &ns, &name).await
+        })
+        .await
+}
+
+#[tauri::command]
+pub async fn list_pvcs(
+    state: State<'_, AppState>,
+    connection_id: String,
+    namespace: String,
+) -> AppResult<Listed<PvcRow>> {
+    let ns = namespace.clone();
+    state
+        .read_in(&connection_id, &namespace, |c| async move {
+            crate::k8s::storage::list_pvcs(&c.reader(), &ns).await
+        })
+        .await
+}
+
+#[tauri::command]
+pub async fn get_quotas(
+    state: State<'_, AppState>,
+    connection_id: String,
+    namespace: String,
+) -> AppResult<QuotasView> {
+    let ns = namespace.clone();
+    state
+        .read_in(&connection_id, &namespace, |c| async move {
+            Ok(crate::k8s::quotas::get_quotas(&c.reader(), &ns).await)
+        })
+        .await
+}
+
+/// YAML объекта без `managedFields` (Secret — нельзя).
+#[tauri::command]
+pub async fn get_yaml(
+    state: State<'_, AppState>,
+    connection_id: String,
+    namespace: String,
+    kind: YamlKind,
+    name: String,
+) -> AppResult<String> {
+    let ns = namespace.clone();
+    state
+        .read_in(&connection_id, &namespace, |c| async move {
+            crate::k8s::yaml::object_yaml(&c.reader(), &ns, kind, &name).await
+        })
+        .await
+}
diff --git a/src-tauri/src/k8s/mod.rs b/src-tauri/src/k8s/mod.rs
index 6001445..a22614a 100644
--- a/src-tauri/src/k8s/mod.rs
+++ b/src-tauri/src/k8s/mod.rs
@@ -3,6 +3,7 @@
 pub mod access;
 pub mod cluster;
 pub mod config;
+pub mod configs;
 pub mod connectivity;
 pub mod containers;
 pub mod errors;
@@ -15,9 +16,13 @@ pub mod overview;
 pub mod pods;
 pub mod pool;
 pub mod projects;
+pub mod quantity;
+pub mod quotas;
 pub mod read;
 pub mod selectors;
+pub mod storage;
 #[cfg(test)]
 pub(crate) mod testing;
 pub mod view;
 pub mod workloads;
+pub mod yaml;
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index b0b6ecb..8407c40 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -84,6 +84,11 @@ pub fn run() {
             commands::objects::get_pod,
             commands::objects::list_events,
             commands::objects::get_network,
+            commands::objects::get_config,
+            commands::objects::get_config_map_values,
+            commands::objects::list_pvcs,
+            commands::objects::get_quotas,
+            commands::objects::get_yaml,
         ])
         .run(tauri::generate_context!())
         .expect("error while building tauri application");
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 338 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 133`

- [ ] **Step 4: Commit.**

```bash
git add \
  src-tauri/src/commands/objects.rs \
  src-tauri/src/k8s/configs.rs \
  src-tauri/src/k8s/mod.rs \
  src-tauri/src/k8s/quantity.rs \
  src-tauri/src/k8s/quotas.rs \
  src-tauri/src/k8s/storage.rs \
  src-tauri/src/k8s/yaml.rs \
  src-tauri/src/lib.rs
git commit -s -m "feat(k8s): конфигурация без значений Secret, PVC, квоты и LimitRange, YAML без managedFields" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 10: Watch: поды, workloads и события проекта

**Files:**
- Create: `src-tauri/src/watch/mod.rs`
- Modify: `src-tauri/src/commands/sessions.rs`, `src-tauri/src/lib.rs`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задачи 3, 6, 7 (`ReadClient::watch`, `sessions`, строки `pod_row`, `*_row` workloads, `event_row`, `list_*`).
- Produces: `watch::{POLL_INTERVAL = 10 с, WatchKind::{Pods, Workloads, Events}, WatchParams { connection_id, namespace, kind, workload? }, Row::{Pod, Workload, Event} (+ key), RowChange::{Upsert { row }, Delete { key }}, WatchEvent::{Reset { items, errors }, Changes { changes }, Polling, Error { error, fatal }}, Source { kind?, stream }, Poll, run(sources, poll, sink, cancel), sources(&ReadClient, ns, kind, selector?, &ApiGroups), poll(...)}`; команда `start_watch(params, onEvent: Channel<WatchEvent>) -> String`, `commands::sessions::start_watch_impl`; ключ `errors.watch.cronJobPods`.

Спец. §6.7. Сессия watch открытого списка: `kube::runtime::watcher` с обычной задержкой повторов; строки — те же, что у команд списков. Сначала собирается весь список (`Init` … `InitDone`) и уходит целиком (`Reset`), дальше — изменения пакетами в порядке прихода (`Changes`: `upsert` и `delete`). У workloads — свой поток на каждый вид: `Reset` уходит, когда готовы все, а вид без прав на список не скрывает остальные (`errors`). Список читается, а watch запрещён (403 на watch) — сессия переходит на опрос раз в 10 с (`Polling`, затем `Reset` на каждом опросе). 401 и отказ в списке единственного вида — ошибка с `fatal`, сессия завершается; сеть и сервер — ошибка без `fatal` один раз, watcher пробует снова сам. Поды CronJob (их Job меняются) через watch не читаются — интерфейс опрашивает `list_pods`.

- [ ] **Step 1: Тесты.**

<!-- block: file t10-src-tauri-src-watch-mod-rs-tests step=10.1 path=src-tauri/src/watch/mod.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sessions::Collect;
    use watcher::Event as E;

    fn pod(name: &str, status: &str) -> Row {
        let mut p: Pod = serde_json::from_value(crate::k8s::pods::fixtures::running(name)).unwrap();
        p.status.as_mut().unwrap().phase = Some(status.into());
        Row::Pod(pod_row(&p))
    }

    fn api_error(code: u16) -> kube::Error {
        kube::Error::Api(
            kube::core::Status::failure(
                "denied",
                if code == 403 {
                    "Forbidden"
                } else {
                    "Unauthorized"
                },
            )
            .with_code(code)
            .boxed(),
        )
    }

    /// Поток, который отдаёт события и дальше молчит, как живой watch.
    fn source(
        kind: Option<WorkloadKind>,
        events: Vec<watcher::Result<watcher::Event<Row>>>,
    ) -> Source {
        Source {
            kind,
            stream: futures::stream::iter(events)
                .chain(futures::stream::pending())
                .boxed(),
        }
    }

    fn no_poll() -> Poll {
        Box::new(|| Box::pin(async { panic!("опроса быть не должно") }))
    }

    /// Запускает сессию на `wait` (время на паузе) и возвращает всё, что она отправила.
    async fn session(sources: Vec<Source>, poll: Poll, wait: Duration) -> Vec<WatchEvent> {
        let sink = Collect::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(sources, poll, sink.clone(), cancel.clone()));
        tokio::time::sleep(wait).await;
        cancel.cancel();
        task.await.unwrap();
        sink.items()
    }

    fn summary(events: &[WatchEvent]) -> Vec<String> {
        events
            .iter()
            .map(|e| match e {
                WatchEvent::Reset { items, errors } => format!(
                    "reset {} {}",
                    items.iter().map(Row::key).collect::<Vec<_>>().join(","),
                    errors
                        .iter()
                        .map(|e| format!("{:?}", e.kind))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                WatchEvent::Changes { changes } => format!(
                    "changes {}",
                    changes
                        .iter()
                        .map(|c| match c {
                            RowChange::Upsert { row } => format!("+{}", row.key()),
                            RowChange::Delete { key } => format!("-{key}"),
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                WatchEvent::Polling => "polling".into(),
                WatchEvent::Error { error, fatal } => format!("error {} {fatal}", error.key),
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn list_then_changes_in_order() {
        let s = source(
            None,
            vec![
                Ok(E::Init),
                Ok(E::InitApply(pod("a", "Running"))),
                Ok(E::InitApply(pod("b", "Running"))),
                Ok(E::InitDone),
                Ok(E::Apply(pod("c", "Pending"))),
                Ok(E::Delete(pod("a", "Running"))),
                Ok(E::Apply(pod("a", "Running"))),
            ],
        );
        let events = session(vec![s], no_poll(), Duration::from_secs(1)).await;
        assert_eq!(summary(&events), ["reset a,b ", "changes +c,-a,+a"]);
    }

    #[tokio::test(start_paused = true)]
    async fn workloads_wait_for_every_kind_and_report_forbidden_ones() {
        let row = |name: &str| {
            Row::Workload(deployment_row(
                &serde_json::from_value(crate::k8s::workloads::fixtures::deployment(name, 1, 1))
                    .unwrap(),
            ))
        };
        let deployments = source(
            Some(WorkloadKind::Deployment),
            vec![Ok(E::Init), Ok(E::InitApply(row("web"))), Ok(E::InitDone)],
        );
        let forbidden = || Err(watcher::Error::InitialListFailed(api_error(403)));
        let cron = source(Some(WorkloadKind::CronJob), vec![forbidden(), forbidden()]);
        let events = session(vec![deployments, cron], no_poll(), Duration::from_secs(1)).await;
        assert_eq!(summary(&events), ["reset Deployment/web CronJob"]);
    }

    #[tokio::test(start_paused = true)]
    async fn without_watch_rights_the_list_is_polled() {
        let s = source(
            None,
            vec![
                Ok(E::Init),
                Ok(E::InitDone),
                Err(watcher::Error::WatchStartFailed(api_error(403))),
            ],
        );
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let poll: Poll = {
            let calls = calls.clone();
            Box::new(move || {
                let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async move { Ok((vec![pod(&format!("p{n}"), "Running")], vec![])) })
            })
        };
        let events = session(vec![s], poll, Duration::from_secs(25)).await;
        assert_eq!(
            summary(&events),
            ["reset  ", "polling", "reset p0 ", "reset p1 ", "reset p2 "]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn expired_session_ends_and_transient_errors_are_reported_once() {
        let s = source(
            None,
            vec![
                Err(watcher::Error::InitialListFailed(api_error(500))),
                Err(watcher::Error::InitialListFailed(api_error(500))),
                Ok(E::Init),
                Ok(E::InitDone),
                Err(watcher::Error::WatchFailed(api_error(401))),
                Ok(E::Apply(pod("late", "Running"))),
            ],
        );
        let events = session(vec![s], no_poll(), Duration::from_secs(1)).await;
        assert_eq!(
            summary(&events),
            [
                "error errors.k8s.serverError false",
                "reset  ",
                "error errors.k8s.unauthorized true"
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_single_kind_without_list_rights_is_fatal() {
        let s = source(
            None,
            vec![Err(watcher::Error::InitialListFailed(api_error(403)))],
        );
        let events = session(vec![s], no_poll(), Duration::from_secs(1)).await;
        assert_eq!(summary(&events), ["error errors.k8s.forbidden true"]);
    }

    /// Настоящий watcher против мока: список читается, watch запрещён — опрос.
    #[tokio::test]
    async fn real_watcher_falls_back_to_polling() {
        let (client, _server) = crate::k8s::testing::mock_client(|s| {
            if s.uri.contains("watch=true") {
                crate::k8s::testing::status(
                    403,
                    "Forbidden",
                    r#"pods is forbidden: User "u" cannot watch resource "pods" in API group "" in the namespace "team-a""#,
                )
            } else {
                (
                    200,
                    serde_json::json!({"apiVersion": "v1", "kind": "PodList",
                        "metadata": {"resourceVersion": "7"},
                        "items": [crate::k8s::pods::fixtures::running("web-1")]}),
                )
            }
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let groups = ApiGroups::default();
        let sink = Collect::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(
            sources(&r, "team-a", WatchKind::Pods, None, &groups),
            poll(r.clone(), "team-a".into(), WatchKind::Pods, None, groups),
            sink.clone(),
            cancel.clone(),
        ));
        let polled = async {
            while summary(&sink.items()).len() < 3 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(10), polled)
            .await
            .expect("перешли на опрос");
        cancel.cancel();
        task.await.unwrap();
        assert_eq!(
            summary(&sink.items())[..3],
            ["reset web-1 ", "polling", "reset web-1 "]
        );
    }
}
````

- [ ] **Step 2: Реализация.**

<!-- block: file t10-src-tauri-src-watch-mod-rs-impl step=10.2 path=src-tauri/src/watch/mod.rs part=impl -->
````rust
//! Сессии watch открытых списков проекта (спец. §6.7): поды, workloads и события через
//! `kube::runtime::watcher`; если прав на watch нет — опрос раз в 10 с. Строки — те же, что
//! у команд списков. Изменения уходят во фронтенд пакетами в порядке, в котором пришли.

use std::collections::BTreeMap;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{Event, Pod};
use kube::runtime::{WatchStreamExt, watcher};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::errors::{from_kube, from_status};
use crate::k8s::events::{EventRow, event_row, list_events};
use crate::k8s::pods::{PodRow, WorkloadRef, list_pods, pod_row};
use crate::k8s::read::ReadClient;
use crate::k8s::workloads::{
    KindError, WorkloadKind, WorkloadRow, cron_job_row, daemon_set_row, deployment_config_row,
    deployment_configs, deployment_row, job_row, list_workloads, stateful_set_row,
};
use crate::platform::ApiGroups;
use crate::sessions::{BATCH_WINDOW, Batch, Sink};

/// Как часто перечитывается список, если прав на watch нет.
pub const POLL_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WatchKind {
    Pods,
    Workloads,
    Events,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchParams {
    pub connection_id: String,
    pub namespace: String,
    pub kind: WatchKind,
    /// Только поды этого workload'а (по его селектору); CronJob не поддерживается.
    #[serde(default)]
    pub workload: Option<WorkloadRef>,
}

/// Строка списка: под, workload или событие.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum Row {
    Pod(PodRow),
    Workload(WorkloadRow),
    Event(EventRow),
}

impl Row {
    pub fn key(&self) -> String {
        match self {
            Row::Pod(p) => p.name.clone(),
            Row::Workload(w) => w.key(),
            Row::Event(e) => e.key.clone(),
        }
    }
}

/// Изменение списка; применяются по порядку.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "op", rename_all = "camelCase")]
pub enum RowChange {
    Upsert { row: Row },
    Delete { key: String },
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum WatchEvent {
    /// Весь список: после первого чтения, после каждого перечитывания и при опросе. `errors` —
    /// виды workloads, которые не читаются.
    Reset {
        items: Vec<Row>,
        errors: Vec<KindError>,
    },
    Changes {
        changes: Vec<RowChange>,
    },
    /// Прав на watch нет: список перечитывается раз в 10 с.
    Polling,
    /// Ошибка чтения; `fatal` — сессия завершена (например, истекла сессия входа).
    Error {
        error: AppError,
        fatal: bool,
    },
}

/// Поток изменений одного вида объектов, уже в строках.
pub struct Source {
    /// Вид workload'а (для ошибки вида); у подов и событий — `None`.
    pub kind: Option<WorkloadKind>,
    pub stream: BoxStream<'static, watcher::Result<watcher::Event<Row>>>,
}

/// Полный список для режима опроса.
pub type Poll = Box<dyn Fn() -> BoxFuture<'static, AppResult<(Vec<Row>, Vec<KindError>)>> + Send>;

/// Чем кончилась ошибка watcher'а.
#[derive(Debug)]
enum Failure {
    /// Список не читается (403): вид без прав.
    ListForbidden(AppError),
    /// Список читается, а watch — нет: перейти на опрос.
    WatchForbidden,
    /// Дальше читать нельзя (401): сессия завершается.
    Fatal(AppError),
    /// Сеть, сервер: watcher повторит сам.
    Transient(AppError),
}

fn classify(e: &watcher::Error) -> Failure {
    let (error, listing) = match e {
        watcher::Error::InitialListFailed(k) => (from_kube(k), true),
        watcher::Error::WatchStartFailed(k) | watcher::Error::WatchFailed(k) => {
            (from_kube(k), false)
        }
        watcher::Error::WatchError(s) => (from_status(s), false),
        watcher::Error::NoResourceVersion => (
            AppError::internal_with("no resourceVersion in the list"),
            true,
        ),
    };
    match (error.code, listing) {
        (ErrorCode::SessionExpired, _) => Failure::Fatal(error),
        (ErrorCode::Forbidden, true) => Failure::ListForbidden(error),
        (ErrorCode::Forbidden, false) => Failure::WatchForbidden,
        _ => Failure::Transient(error),
    }
}

/// Что сделать с изменением потока.
#[derive(Debug)]
enum Step {
    Nothing,
    Reset(Vec<Row>, Vec<KindError>),
    Change(RowChange),
    Poll,
    Fatal(AppError),
    Transient(AppError),
}

/// Строки всех потоков: список сначала собирается (`Init` … `InitDone`) и уходит целиком, когда
/// готовы все потоки; дальше — изменения.
struct State {
    kinds: Vec<Option<WorkloadKind>>,
    stores: Vec<BTreeMap<String, Row>>,
    pending: Vec<Option<BTreeMap<String, Row>>>,
    ready: Vec<bool>,
    errors: Vec<Option<KindError>>,
    live: bool,
}

impl State {
    fn new(kinds: Vec<Option<WorkloadKind>>) -> Self {
        let n = kinds.len();
        Self {
            kinds,
            stores: vec![BTreeMap::new(); n],
            pending: vec![None; n],
            ready: vec![false; n],
            errors: vec![None; n],
            live: false,
        }
    }

    fn reset(&mut self) -> Step {
        if !self.ready.iter().all(|r| *r) {
            return Step::Nothing;
        }
        self.live = true;
        Step::Reset(
            self.stores
                .iter()
                .flat_map(|s| s.values().cloned())
                .collect(),
            self.errors.iter().flatten().cloned().collect(),
        )
    }

    fn apply(&mut self, i: usize, event: watcher::Result<watcher::Event<Row>>) -> Step {
        use watcher::Event;
        match event {
            Ok(Event::Init) => {
                self.pending[i] = Some(BTreeMap::new());
                Step::Nothing
            }
            Ok(Event::InitApply(row)) => {
                self.pending[i]
                    .get_or_insert_with(BTreeMap::new)
                    .insert(row.key(), row);
                Step::Nothing
            }
            Ok(Event::InitDone) => {
                self.stores[i] = self.pending[i].take().unwrap_or_default();
                self.ready[i] = true;
                self.errors[i] = None;
                self.reset()
            }
            Ok(Event::Apply(row)) => {
                self.stores[i].insert(row.key(), row.clone());
                match self.live {
                    true => Step::Change(RowChange::Upsert { row }),
                    false => Step::Nothing,
                }
            }
            Ok(Event::Delete(row)) => {
                let key = row.key();
                self.stores[i].remove(&key);
                match self.live {
                    true => Step::Change(RowChange::Delete { key }),
                    false => Step::Nothing,
                }
            }
            Err(e) => match classify(&e) {
                // Вид workload'а без прав не скрывает остальные; повторная ошибка — та же.
                Failure::ListForbidden(error) if self.kinds.len() > 1 => {
                    if self.errors[i].is_some() {
                        return Step::Nothing;
                    }
                    self.stores[i].clear();
                    self.ready[i] = true;
                    self.errors[i] = self.kinds[i].map(|kind| KindError { kind, error });
                    self.reset()
                }
                Failure::ListForbidden(error) | Failure::Fatal(error) => Step::Fatal(error),
                Failure::WatchForbidden => Step::Poll,
                Failure::Transient(error) => Step::Transient(error),
            },
        }
    }
}

/// Сессия: изменения потоков пакетами, а без прав на watch — опрос. Завершается по отмене, при
/// неустранимой ошибке и когда получателя больше нет.
pub async fn run(
    sources: Vec<Source>,
    poll: Poll,
    sink: impl Sink<WatchEvent>,
    cancel: CancellationToken,
) {
    let mut state = State::new(sources.iter().map(|s| s.kind).collect());
    let mut streams = futures::stream::select_all(
        sources
            .into_iter()
            .enumerate()
            .map(|(i, s)| s.stream.map(move |e| (i, e)).boxed()),
    );
    let mut batch: Batch<RowChange> = Batch::default();
    let mut tick = tokio::time::interval(BATCH_WINDOW);
    // Ошибка уже показана: повторы той же ошибки (watcher пробует снова) не шлём.
    let mut failing = false;
    let polling = loop {
        let step = tokio::select! {
            _ = cancel.cancelled() => return,
            next = streams.next() => match next {
                Some((i, event)) => state.apply(i, event),
                None => return,
            },
            _ = tick.tick() => {
                if batch.is_due() && !sink.send(WatchEvent::Changes { changes: batch.take() }) {
                    return;
                }
                continue;
            }
        };
        let sent = match step {
            Step::Nothing => true,
            Step::Reset(items, errors) => {
                failing = false;
                batch.take();
                sink.send(WatchEvent::Reset { items, errors })
            }
            Step::Change(change) => {
                failing = false;
                match batch.push(change) {
                    Some(full) => sink.send(WatchEvent::Changes { changes: full }),
                    None => true,
                }
            }
            Step::Poll => break true,
            Step::Fatal(error) => {
                sink.send(WatchEvent::Error { error, fatal: true });
                return;
            }
            Step::Transient(error) if !failing => {
                failing = true;
                sink.send(WatchEvent::Error {
                    error,
                    fatal: false,
                })
            }
            Step::Transient(_) => true,
        };
        if !sent {
            return;
        }
    };
    if polling && sink.send(WatchEvent::Polling) {
        drop(streams);
        poll_loop(poll, sink, cancel).await;
    }
}

async fn poll_loop(poll: Poll, sink: impl Sink<WatchEvent>, cancel: CancellationToken) {
    loop {
        let sent = tokio::select! {
            _ = cancel.cancelled() => return,
            r = poll() => match r {
                Ok((items, errors)) => sink.send(WatchEvent::Reset { items, errors }),
                Err(error) if error.code == ErrorCode::SessionExpired => {
                    sink.send(WatchEvent::Error { error, fatal: true });
                    return;
                }
                Err(error) => sink.send(WatchEvent::Error { error, fatal: false }),
            },
        };
        if !sent {
            return;
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
        }
    }
}

/// Событие watcher'а со строкой вместо объекта.
fn to_rows<K>(
    e: watcher::Result<watcher::Event<K>>,
    f: impl Fn(&K) -> Row,
) -> watcher::Result<watcher::Event<Row>> {
    use watcher::Event;
    e.map(|e| match e {
        Event::Apply(k) => Event::Apply(f(&k)),
        Event::Delete(k) => Event::Delete(f(&k)),
        Event::Init => Event::Init,
        Event::InitApply(k) => Event::InitApply(f(&k)),
        Event::InitDone => Event::InitDone,
    })
}

/// Потоки вида: поды (по селектору workload'а), workloads всех видов или события.
pub fn sources(
    r: &ReadClient,
    namespace: &str,
    kind: WatchKind,
    selector: Option<&str>,
    groups: &ApiGroups,
) -> Vec<Source> {
    let config = || match selector.filter(|s| !s.is_empty()) {
        Some(s) => watcher::Config::default().labels(s),
        None => watcher::Config::default(),
    };
    macro_rules! typed {
        ($kind:expr, $t:ty, $f:expr) => {
            Source {
                kind: $kind,
                stream: r
                    .watch::<$t>(namespace, config())
                    .default_backoff()
                    .map(|e| to_rows(e, $f))
                    .boxed(),
            }
        };
    }
    match kind {
        WatchKind::Pods => vec![typed!(None, Pod, |p| Row::Pod(pod_row(p)))],
        WatchKind::Events => vec![typed!(None, Event, |e| Row::Event(event_row(e)))],
        WatchKind::Workloads => {
            let mut out = vec![
                typed!(Some(WorkloadKind::Deployment), Deployment, |o| {
                    Row::Workload(deployment_row(o))
                }),
                typed!(Some(WorkloadKind::StatefulSet), StatefulSet, |o| {
                    Row::Workload(stateful_set_row(o))
                }),
                typed!(Some(WorkloadKind::DaemonSet), DaemonSet, |o| {
                    Row::Workload(daemon_set_row(o))
                }),
                typed!(Some(WorkloadKind::Job), Job, |o| Row::Workload(job_row(o))),
                typed!(Some(WorkloadKind::CronJob), CronJob, |o| {
                    Row::Workload(cron_job_row(o))
                }),
            ];
            if WorkloadKind::DeploymentConfig.available(groups) {
                out.push(Source {
                    kind: Some(WorkloadKind::DeploymentConfig),
                    stream: r
                        .watch_dynamic(&deployment_configs(), namespace, config())
                        .default_backoff()
                        .map(|e| to_rows(e, |o| Row::Workload(deployment_config_row(o))))
                        .boxed(),
                });
            }
            out
        }
    }
}

/// Опрос вида: те же чтения, что у команд списков.
pub fn poll(
    r: ReadClient,
    namespace: String,
    kind: WatchKind,
    workload: Option<WorkloadRef>,
    groups: ApiGroups,
) -> Poll {
    Box::new(move || {
        let (r, ns, workload) = (r.clone(), namespace.clone(), workload.clone());
        Box::pin(async move {
            Ok(match kind {
                WatchKind::Pods => (
                    list_pods(&r, &ns, workload.as_ref())
                        .await?
                        .items
                        .into_iter()
                        .map(Row::Pod)
                        .collect(),
                    vec![],
                ),
                WatchKind::Events => (
                    list_events(&r, &ns, None)
                        .await?
                        .items
                        .into_iter()
                        .map(Row::Event)
                        .collect(),
                    vec![],
                ),
                WatchKind::Workloads => {
                    let l = list_workloads(&r, &ns, &groups).await;
                    (l.items.into_iter().map(Row::Workload).collect(), l.errors)
                }
            })
        })
    })
}
````

<!-- block: patch t10-src step=10.2 -->
````diff
diff --git a/src-tauri/src/commands/sessions.rs b/src-tauri/src/commands/sessions.rs
index 944d1a0..fe948a8 100644
--- a/src-tauri/src/commands/sessions.rs
+++ b/src-tauri/src/commands/sessions.rs
@@ -1,10 +1,148 @@
 use tauri::State;
+use tauri::ipc::Channel;
 
-use crate::error::AppResult;
+use crate::error::{AppResult, ErrorCode};
+use crate::sessions::{SessionGuard, Sink};
 use crate::state::AppState;
+use crate::watch::{WatchEvent, WatchKind, WatchParams};
 
 /// Останавливает сессию watch или логов. `false` — она уже завершилась.
 #[tauri::command]
 pub fn stop_session(state: State<'_, AppState>, session_id: String) -> AppResult<bool> {
     Ok(state.sessions.stop(&session_id))
 }
+
+/// Watch списка проекта (спец. §6.7); возвращает id сессии. Данные — в `on_event`.
+#[tauri::command]
+pub async fn start_watch(
+    state: State<'_, AppState>,
+    params: WatchParams,
+    on_event: Channel<WatchEvent>,
+) -> AppResult<String> {
+    start_watch_impl(state.inner(), params, on_event).await
+}
+
+pub async fn start_watch_impl(
+    st: &AppState,
+    params: WatchParams,
+    sink: impl Sink<WatchEvent>,
+) -> AppResult<String> {
+    if params.kind == WatchKind::Pods
+        && params
+            .workload
+            .as_ref()
+            .is_some_and(|w| w.kind == crate::k8s::workloads::WorkloadKind::CronJob)
+    {
+        return Err(app_err!(
+            ErrorCode::InvalidInput,
+            "errors.watch.cronJobPods",
+            "Pods of a CronJob are read by the list, not by watch"
+        ));
+    }
+    let ns = params.namespace.clone();
+    let workload = params.workload.clone();
+    let (reader, groups, selector) = st
+        .read_in(&params.connection_id, &params.namespace, |c| async move {
+            let r = c.reader();
+            let selector = match &workload {
+                Some(w) => crate::k8s::workloads::pod_selector(&r, &ns, w.kind, &w.name).await?,
+                None => None,
+            };
+            Ok((r, c.info.api_groups, selector))
+        })
+        .await?;
+    let sources = crate::watch::sources(
+        &reader,
+        &params.namespace,
+        params.kind,
+        selector.as_deref(),
+        &groups,
+    );
+    let poll = crate::watch::poll(
+        reader,
+        params.namespace.clone(),
+        params.kind,
+        params.workload.clone(),
+        groups,
+    );
+    let (id, cancel) = st.sessions.register(&params.connection_id);
+    let guard = SessionGuard {
+        sessions: st.sessions.clone(),
+        id: id.clone(),
+    };
+    tokio::spawn(async move {
+        let _guard = guard;
+        crate::watch::run(sources, poll, sink, cancel).await;
+    });
+    Ok(id)
+}
+
+#[cfg(test)]
+mod tests {
+    use super::*;
+    use crate::k8s::pods::WorkloadRef;
+    use crate::k8s::workloads::WorkloadKind;
+    use crate::sessions::Collect;
+    use std::sync::Arc;
+
+    fn params(kind: WatchKind, workload: Option<WorkloadRef>) -> WatchParams {
+        WatchParams {
+            connection_id: "a".into(),
+            namespace: "team-a".into(),
+            kind,
+            workload,
+        }
+    }
+
+    #[tokio::test]
+    async fn a_session_is_registered_and_stopped() {
+        let dir = tempfile::tempdir().unwrap();
+        let st =
+            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
+        let connected = crate::k8s::pool::test_connected(|s| {
+            if s.uri.contains("watch=true") {
+                // Watch, который не отвечает, пока сессию не остановят.
+                crate::k8s::testing::status(504, "Timeout", "timeout")
+            } else {
+                (
+                    200,
+                    serde_json::json!({"apiVersion": "v1", "kind": "EventList",
+                        "metadata": {"resourceVersion": "1"}, "items": []}),
+                )
+            }
+        });
+        assert!(
+            st.pool
+                .insert_if_current("a", st.pool.generation("a"), connected)
+        );
+        let sink: Collect<WatchEvent> = Collect::default();
+        let id = start_watch_impl(&st, params(WatchKind::Events, None), sink.clone())
+            .await
+            .unwrap();
+        assert_eq!(st.sessions.len(), 1);
+        assert!(st.sessions.stop(&id));
+        let ended = async {
+            while !st.sessions.is_empty() {
+                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
+            }
+        };
+        tokio::time::timeout(std::time::Duration::from_secs(5), ended)
+            .await
+            .unwrap();
+    }
+
+    #[tokio::test]
+    async fn pods_of_a_cron_job_are_not_watched() {
+        let dir = tempfile::tempdir().unwrap();
+        let st =
+            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
+        let cron = WorkloadRef {
+            kind: WorkloadKind::CronJob,
+            name: "report".into(),
+        };
+        let e = start_watch_impl(&st, params(WatchKind::Pods, Some(cron)), Collect::default())
+            .await
+            .unwrap_err();
+        assert_eq!(e.key, "errors.watch.cronJobPods");
+    }
+}
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index 8407c40..cb3583a 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -17,6 +17,7 @@ pub mod state;
 pub mod storage;
 pub mod sync;
 pub mod tls;
+pub mod watch;
 
 use std::sync::Arc;
 
@@ -72,6 +73,7 @@ pub fn run() {
             commands::settings::set_language,
             commands::settings::get_policy_status,
             commands::sessions::stop_session,
+            commands::sessions::start_watch,
             commands::cluster::get_cluster_overview,
             commands::cluster::list_nodes,
             commands::cluster::get_node,
````

<!-- block: locales t10-locales step=10.2 -->
````json
{
  "en": {
    "errors": {
      "watch": {
        "cronJobPods": "Pods of a CronJob are read by the list, not by watch"
      }
    }
  },
  "ru": {
    "errors": {
      "watch": {
        "cronJobPods": "Поды CronJob читаются списком, а не через watch"
      }
    }
  }
}
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 346 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm test`  
Expected: PASS — `Test Files 32 passed`, `Tests 218 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 134`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 4: Commit.**

```bash
git add \
  src-tauri/src/commands/sessions.rs \
  src-tauri/src/lib.rs \
  src-tauri/src/watch/mod.rs \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "feat(watch): сессии watch подов, workloads и событий проекта; опрос раз в 10 с без прав на watch" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 11: Логи контейнера: фильтр в Rust, обрезка, переподключение, сохранение

**Files:**
- Create: `src-tauri/src/logs/mod.rs`
- Modify: `.gitignore`, `PORTED.md`, `src-tauri/src/commands/sessions.rs`, `src-tauri/src/lib.rs`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задачи 3, 10 (`ReadClient::log_stream`, `sessions`, `commands::sessions`).
- Produces: `logs::{DEFAULT_TAIL = 500, MAX_LINE = 64 КБ, RECONNECT_FOR = 60 с, LogFilter { text, regex, case_sensitive }, LogParams { connection_id, namespace, pod, container?, previous, tail_lines?, since_seconds?, since_time?, follow, filter? }, LogLine { ts?, text, truncated }, LogEvent::{Lines { lines, scanned, shown }, Reconnecting { since? }, End { scanned, shown }, Error { error }}, Matcher::{new(Option<&LogFilter>), matches}, split_line, truncate, LogSource::{open(since?), running()}, KubeLogs { reader, params }, run(source, follow, matcher, sink, cancel), save(&source, &matcher, path, timestamps) -> u64}`; команды `start_logs(params, onEvent: Channel<LogEvent>) -> String`, `save_logs(params, path, timestamps) -> u64`; ключи `errors.logs.badRegex`, `errors.logs.lost`; `.gitignore`: `/logs` только в корне.

Спец. §6.6. Лог читается с `timestamps=true`: время строки нужно для переподключения и показывается по желанию. По умолчанию — последние 500 строк; `previous`, `sinceSeconds` / `sinceTime`, follow. Фильтр — подстрока или regex (с учётом регистра или без; неверный regex — ошибка до начала сессии) — применяется в Rust, счётчик «показано / просмотрено»; строка длиннее 64 КБ обрезается по границе символа. В follow конец потока при работающем контейнере — обрыв: переподключение с начала секунды последней строки (API округляет `sinceTime`), уже показанные строки отбрасываются по времени, паузы 1, 2, 4, 8, 10 с; через 60 с без восстановления — ошибка; контейнер завершился — `End`. Источник лога — трейт `LogSource`, в тестах — заготовленные потоки. `save_logs` читает лог заново с теми же параметрами без follow и пишет подошедшие строки целиком. Корневой `.gitignore` (из Kafka-версии) игнорировал любые каталоги `logs` — теперь только `/logs` в корне, иначе модули `logs/` не попали бы в git.

- [ ] **Step 1: Тесты.**

<!-- block: file t11-src-tauri-src-logs-mod-rs-tests step=11.1 path=src-tauri/src/logs/mod.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sessions::Collect;
    use std::sync::Mutex;

    /// Заготовленные потоки: каждый `open` берёт следующий; `None` — ошибка открытия.
    struct Scripted {
        streams: Mutex<Vec<Option<Vec<std::io::Result<String>>>>>,
        opened: Mutex<Vec<Option<Timestamp>>>,
        running: bool,
        /// Последний поток не кончается (живой follow).
        hold_last: bool,
    }

    impl Scripted {
        fn new(streams: Vec<Option<Vec<&str>>>, running: bool) -> Self {
            Self {
                streams: Mutex::new(
                    streams
                        .into_iter()
                        .rev()
                        .map(|s| s.map(|l| l.into_iter().map(|x| Ok(x.to_string())).collect()))
                        .collect(),
                ),
                opened: Mutex::new(vec![]),
                running,
                hold_last: false,
            }
        }
    }

    impl LogSource for Scripted {
        fn open(
            &self,
            since: Option<Timestamp>,
        ) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>> {
            self.opened.lock().unwrap().push(since);
            let next = self.streams.lock().unwrap().pop();
            let last = self.streams.lock().unwrap().is_empty();
            let hold = self.hold_last && last;
            Box::pin(async move {
                match next {
                    Some(Some(lines)) if hold => Ok(futures::stream::iter(lines)
                        .chain(futures::stream::pending())
                        .boxed()),
                    Some(Some(lines)) => Ok(futures::stream::iter(lines).boxed()),
                    _ => Err(crate::k8s::errors::from_transport_text(
                        "connection refused",
                    )),
                }
            })
        }

        fn running(&self) -> BoxFuture<'_, AppResult<bool>> {
            let running = self.running;
            Box::pin(async move { Ok(running) })
        }
    }

    async fn session(
        source: Scripted,
        follow: bool,
        matcher: Matcher,
        wait: Duration,
    ) -> Vec<LogEvent> {
        let sink = Collect::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(source, follow, matcher, sink.clone(), cancel.clone()));
        tokio::time::sleep(wait).await;
        cancel.cancel();
        task.await.unwrap();
        sink.items()
    }

    fn texts(events: &[LogEvent]) -> Vec<String> {
        events
            .iter()
            .flat_map(|e| match e {
                LogEvent::Lines { lines, .. } => lines.iter().map(|l| l.text.clone()).collect(),
                LogEvent::Reconnecting { .. } => vec!["<reconnecting>".into()],
                LogEvent::End { scanned, shown } => vec![format!("<end {shown}/{scanned}>")],
                LogEvent::Error { error } => vec![format!("<error {}>", error.key)],
            })
            .collect()
    }

    // Без нулей в конце долей секунды: так время печатает и Rust.
    const T1: &str = "2026-10-25T10:00:00.123Z";
    const T2: &str = "2026-10-25T10:00:00.987Z";
    const T3: &str = "2026-10-25T10:00:01.5Z";

    #[test]
    fn filters_lines_and_truncation() {
        let f = |text: &str, regex, case_sensitive| {
            Matcher::new(Some(&LogFilter {
                text: text.into(),
                regex,
                case_sensitive,
            }))
            .unwrap()
        };
        assert!(f("error", false, false).matches("ERROR: boom"));
        assert!(!f("error", false, true).matches("ERROR: boom"));
        assert!(f(r"code=\d{3}", true, false).matches("status code=503"));
        assert!(Matcher::new(None).unwrap().matches("x"));
        let bad = Matcher::new(Some(&LogFilter {
            text: "(".into(),
            regex: true,
            case_sensitive: false,
        }));
        assert_eq!(bad.err().unwrap().key, "errors.logs.badRegex");
        let raw = format!("{T1} hello world");
        let (ts, text) = split_line(&raw);
        assert_eq!(
            (ts.map(|t| t.to_string()), text),
            (Some(T1.into()), "hello world")
        );
        assert_eq!(split_line("no time here").0, None);
        let long = "я".repeat(MAX_LINE);
        let (cut, truncated) = truncate(&long);
        assert!(truncated && cut.len() <= MAX_LINE && cut.len() % 2 == 0);
        assert_eq!(truncate("short"), ("short".into(), false));
    }

    #[tokio::test(start_paused = true)]
    async fn without_follow_the_log_ends_with_counters() {
        let source = Scripted::new(
            vec![Some(vec![
                &format!("{T1} GET /health 200"),
                &format!("{T2} GET /api 500 error"),
                &format!("{T3} GET /api 200"),
            ])],
            true,
        );
        let matcher = Matcher::new(Some(&LogFilter {
            text: "/api".into(),
            regex: false,
            case_sensitive: false,
        }))
        .unwrap();
        let events = session(source, false, matcher, Duration::from_secs(1)).await;
        assert_eq!(
            texts(&events),
            ["GET /api 500 error", "GET /api 200", "<end 2/3>"]
        );
        let LogEvent::Lines {
            lines,
            scanned,
            shown,
        } = &events[0]
        else {
            panic!("строки");
        };
        assert_eq!((*scanned, *shown), (3, 2));
        assert_eq!(lines[0].ts.as_deref(), Some(T2));
    }

    #[tokio::test(start_paused = true)]
    async fn follow_reconnects_from_the_last_line_without_repeats() {
        let mut source = Scripted::new(
            vec![
                Some(vec![&format!("{T1} one"), &format!("{T2} two")]),
                None,
                Some(vec![
                    &format!("{T1} one"),
                    &format!("{T2} two"),
                    &format!("{T3} three"),
                ]),
            ],
            true,
        );
        source.hold_last = true;
        let opened = std::sync::Arc::new(source);
        struct Shared(std::sync::Arc<Scripted>);
        impl LogSource for Shared {
            fn open(
                &self,
                since: Option<Timestamp>,
            ) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>> {
                self.0.open(since)
            }
            fn running(&self) -> BoxFuture<'_, AppResult<bool>> {
                self.0.running()
            }
        }
        let sink = Collect::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(
            Shared(opened.clone()),
            true,
            Matcher::All,
            sink.clone(),
            cancel.clone(),
        ));
        tokio::time::sleep(Duration::from_secs(10)).await;
        cancel.cancel();
        task.await.unwrap();
        assert_eq!(
            texts(&sink.items()),
            ["one", "two", "<reconnecting>", "three"]
        );
        let since = opened.opened.lock().unwrap().clone();
        assert_eq!(since.len(), 3);
        assert_eq!(since[0], None);
        assert_eq!(
            since[1].map(|t| t.to_string()).as_deref(),
            Some("2026-10-25T10:00:00Z"),
            "с начала секунды последней строки"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn follow_gives_up_after_a_minute_and_ends_when_the_container_stops() {
        let source = Scripted::new(
            [Some(vec![&format!("{T1} one") as &str])]
                .into_iter()
                .chain(std::iter::repeat_n(None, 20))
                .collect(),
            true,
        );
        let events = session(source, true, Matcher::All, Duration::from_secs(120)).await;
        assert_eq!(
            texts(&events),
            ["one", "<reconnecting>", "<error errors.logs.lost>"]
        );

        let stopped = Scripted::new(vec![Some(vec![&format!("{T1} bye") as &str])], false);
        let events = session(stopped, true, Matcher::All, Duration::from_secs(1)).await;
        assert_eq!(texts(&events), ["bye", "<end 1/1>"]);

        let refused = Scripted::new(vec![None], true);
        let events = session(refused, true, Matcher::All, Duration::from_secs(1)).await;
        assert_eq!(
            texts(&events),
            ["<error errors.k8s.connect>"],
            "первое открытие не повторяется"
        );
    }

    #[tokio::test]
    async fn kube_logs_ask_for_a_tail_with_timestamps_and_save_writes_a_file() {
        let (client, server) = crate::k8s::testing::mock_raw(|s| {
            if s.path().ends_with("/log") {
                (200, format!("{T1} first\n{T2} second error\n").into_bytes())
            } else {
                let pod = crate::k8s::pods::fixtures::running("web-1");
                (200, serde_json::to_vec(&pod).unwrap())
            }
        });
        let logs = KubeLogs {
            reader: ReadClient::new(client, Duration::from_secs(5)),
            params: LogParams {
                connection_id: "a".into(),
                namespace: "team-a".into(),
                pod: "web-1".into(),
                container: Some("web".into()),
                previous: true,
                tail_lines: None,
                since_seconds: None,
                since_time: None,
                follow: false,
                filter: None,
            },
        };
        assert!(logs.running().await.unwrap());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("web-1.log");
        let matcher = Matcher::new(Some(&LogFilter {
            text: "error".into(),
            regex: false,
            case_sensitive: false,
        }))
        .unwrap();
        assert_eq!(save(&logs, &matcher, &path, true).await.unwrap(), 1);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            format!("{T2} second error\n")
        );
        let since = Timestamp::from_second(1_792_922_400).unwrap();
        let lp = logs.kube_params(Some(since));
        assert_eq!((lp.tail_lines, lp.since_time), (None, Some(since)));
        drop(logs);
        let seen = server.await.unwrap();
        let log = seen.iter().find(|s| s.path().ends_with("/log")).unwrap();
        assert_eq!(log.path(), "/api/v1/namespaces/team-a/pods/web-1/log");
        for q in [
            "container=web",
            "previous=true",
            "tailLines=500",
            "timestamps=true",
        ] {
            assert!(log.uri.contains(q), "{q}: {}", log.uri);
        }
        assert!(!log.uri.contains("follow"));
    }
}
````

- [ ] **Step 2: Реализация.**

<!-- block: file t11-src-tauri-src-logs-mod-rs-impl step=11.2 path=src-tauri/src/logs/mod.rs part=impl -->
````rust
//! Логи контейнера (спец. §6.6): хвост (по умолчанию 500 строк) или с момента, предыдущий
//! контейнер, follow. Фильтр (подстрока или regex) применяется здесь, до отправки в интерфейс;
//! строка длиннее 64 КБ обрезается. В follow при потере соединения — переподключение с
//! `sinceTime` последней строки (повторы отбрасываются), через 60 с без восстановления — ошибка.

use std::path::Path;
use std::time::Duration;

use futures::AsyncBufReadExt;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::jiff::Timestamp;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::read::ReadClient;
use crate::sessions::{BATCH_WINDOW, Batch, Sink};

/// Сколько строк с конца читается, если не задано ни начало, ни число строк.
pub const DEFAULT_TAIL: i64 = 500;
/// Строка длиннее обрезается.
pub const MAX_LINE: usize = 64 * 1024;
/// Сколько follow ждёт восстановления соединения.
pub const RECONNECT_FOR: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogFilter {
    pub text: String,
    #[serde(default)]
    pub regex: bool,
    #[serde(default)]
    pub case_sensitive: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogParams {
    pub connection_id: String,
    pub namespace: String,
    pub pod: String,
    #[serde(default)]
    pub container: Option<String>,
    #[serde(default)]
    pub previous: bool,
    #[serde(default)]
    pub tail_lines: Option<i64>,
    #[serde(default)]
    pub since_seconds: Option<i64>,
    /// RFC 3339.
    #[serde(default)]
    pub since_time: Option<String>,
    #[serde(default)]
    pub follow: bool,
    #[serde(default)]
    pub filter: Option<LogFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogLine {
    /// Время строки (RFC 3339 с долями секунды), если API его отдал.
    pub ts: Option<String>,
    pub text: String,
    /// Строка длиннее 64 КБ показана не целиком.
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum LogEvent {
    /// Строки, подошедшие под фильтр; `scanned` и `shown` — сколько всего просмотрено и показано.
    Lines {
        lines: Vec<LogLine>,
        scanned: u64,
        shown: u64,
    },
    /// Соединение потеряно, идёт переподключение.
    Reconnecting {
        since: Option<String>,
    },
    /// Лог прочитан до конца (без follow или контейнер завершился).
    End {
        scanned: u64,
        shown: u64,
    },
    Error {
        error: AppError,
    },
}

/// Фильтр строк.
pub enum Matcher {
    All,
    Text {
        needle: String,
        case_sensitive: bool,
    },
    Regex(regex::Regex),
}

impl Matcher {
    pub fn new(f: Option<&LogFilter>) -> AppResult<Self> {
        let Some(f) = f.filter(|f| !f.text.is_empty()) else {
            return Ok(Matcher::All);
        };
        if f.regex {
            regex::RegexBuilder::new(&f.text)
                .case_insensitive(!f.case_sensitive)
                .size_limit(1 << 20)
                .build()
                .map(Matcher::Regex)
                .map_err(|e| {
                    app_err!(
                        ErrorCode::InvalidInput,
                        "errors.logs.badRegex",
                        "Invalid regular expression: {{error}}",
                        error = e
                    )
                })
        } else {
            Ok(Matcher::Text {
                needle: if f.case_sensitive {
                    f.text.clone()
                } else {
                    f.text.to_lowercase()
                },
                case_sensitive: f.case_sensitive,
            })
        }
    }

    pub fn matches(&self, line: &str) -> bool {
        match self {
            Matcher::All => true,
            Matcher::Text {
                needle,
                case_sensitive: true,
            } => line.contains(needle.as_str()),
            Matcher::Text { needle, .. } => line.to_lowercase().contains(needle.as_str()),
            Matcher::Regex(r) => r.is_match(line),
        }
    }
}

/// Время и текст строки лога `2026-10-25T10:00:00.123456789Z текст` (API с `timestamps=true`).
pub fn split_line(raw: &str) -> (Option<Timestamp>, &str) {
    match raw.split_once(' ') {
        Some((ts, text)) => match ts.parse::<Timestamp>() {
            Ok(t) => (Some(t), text),
            Err(_) => (None, raw),
        },
        None => match raw.parse::<Timestamp>() {
            Ok(t) => (Some(t), ""),
            Err(_) => (None, raw),
        },
    }
}

/// Строка не длиннее `MAX_LINE` байт (по границе символа) и признак обрезки.
pub fn truncate(text: &str) -> (String, bool) {
    if text.len() <= MAX_LINE {
        return (text.to_string(), false);
    }
    let mut end = MAX_LINE;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (text[..end].to_string(), true)
}

/// Откуда читается лог: в приложении — API-сервер, в тестах — заготовленные строки.
pub trait LogSource: Send + Sync {
    /// Поток строк (с временем в начале): по параметрам сессии или, при переподключении, с `since`.
    fn open(
        &self,
        since: Option<Timestamp>,
    ) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>>;

    /// Работает ли ещё контейнер: если нет, конец потока — конец лога, а не обрыв.
    fn running(&self) -> BoxFuture<'_, AppResult<bool>>;
}

/// Лог контейнера через API-сервер.
pub struct KubeLogs {
    pub reader: ReadClient,
    pub params: LogParams,
}

impl KubeLogs {
    fn kube_params(&self, since: Option<Timestamp>) -> kube::api::LogParams {
        let p = &self.params;
        let mut lp = kube::api::LogParams {
            container: p.container.clone(),
            follow: p.follow,
            previous: p.previous,
            timestamps: true,
            ..Default::default()
        };
        if let Some(t) = since {
            lp.since_time = Some(t);
        } else {
            lp.since_seconds = p.since_seconds;
            lp.since_time = p.since_time.as_deref().and_then(|t| t.parse().ok());
            lp.tail_lines = p.tail_lines.or_else(|| {
                (p.since_seconds.is_none() && p.since_time.is_none()).then_some(DEFAULT_TAIL)
            });
        }
        lp
    }
}

impl LogSource for KubeLogs {
    fn open(
        &self,
        since: Option<Timestamp>,
    ) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>> {
        Box::pin(async move {
            let lp = self.kube_params(since);
            let r = self
                .reader
                .log_stream(&self.params.namespace, &self.params.pod, &lp)
                .await?;
            Ok(r.lines().boxed())
        })
    }

    fn running(&self) -> BoxFuture<'_, AppResult<bool>> {
        Box::pin(async move {
            let pod: Pod = self
                .reader
                .get(&self.params.namespace, &self.params.pod)
                .await?;
            let statuses = pod
                .status
                .and_then(|s| s.container_statuses)
                .unwrap_or_default();
            let status = match &self.params.container {
                Some(c) => statuses.into_iter().find(|s| &s.name == c),
                None => statuses.into_iter().next(),
            };
            Ok(status
                .and_then(|s| s.state)
                .is_some_and(|s| s.running.is_some()))
        })
    }
}

fn lost() -> AppError {
    app_err!(
        ErrorCode::ConnectionFailed,
        "errors.logs.lost",
        "The log stream was lost and did not recover in 60 s"
    )
}

/// Пауза перед попыткой переподключения: 1, 2, 4, 8, затем 10 с.
fn backoff(attempt: u32) -> Duration {
    Duration::from_secs((1u64 << attempt.min(4)).min(10))
}

/// Сессия логов. Завершается по отмене, в конце лога, при ошибке и когда получателя нет.
pub async fn run(
    source: impl LogSource,
    follow: bool,
    matcher: Matcher,
    sink: impl Sink<LogEvent>,
    cancel: CancellationToken,
) {
    let (mut scanned, mut shown) = (0u64, 0u64);
    let mut last: Option<Timestamp> = None;
    let mut since: Option<Timestamp> = None;
    let mut lost_at: Option<tokio::time::Instant> = None;
    let mut attempt = 0;
    loop {
        let opened = tokio::select! {
            _ = cancel.cancelled() => return,
            s = source.open(since) => s,
        };
        let mut stream = match opened {
            Ok(s) => s,
            Err(error) if lost_at.is_none() => {
                sink.send(LogEvent::Error { error });
                return;
            }
            Err(_) => {
                if lost_at.is_some_and(|t| t.elapsed() >= RECONNECT_FOR) {
                    sink.send(LogEvent::Error { error: lost() });
                    return;
                }
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(backoff(attempt)) => {}
                }
                attempt += 1;
                continue;
            }
        };
        let mut batch: Batch<LogLine> = Batch::default();
        let mut tick = tokio::time::interval(BATCH_WINDOW);
        // После переподключения строки до `last` включительно уже показаны.
        let resume_after = since.and(last);
        loop {
            let line = tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tick.tick() => {
                    if batch.is_due()
                        && !sink.send(LogEvent::Lines { lines: batch.take(), scanned, shown })
                    {
                        return;
                    }
                    continue;
                }
                line = stream.next() => line,
            };
            let Some(Ok(raw)) = line else { break };
            let (ts, text) = split_line(&raw);
            if resume_after.is_some_and(|after| ts.is_some_and(|t| t <= after)) {
                continue;
            }
            // Строки приходят — соединение восстановлено.
            lost_at = None;
            attempt = 0;
            if ts.is_some() {
                last = ts;
            }
            scanned += 1;
            if matcher.matches(text) {
                shown += 1;
                let (text, truncated) = truncate(text);
                let line = LogLine {
                    ts: ts.map(|t| t.to_string()),
                    text,
                    truncated,
                };
                if let Some(full) = batch.push(line)
                    && !sink.send(LogEvent::Lines {
                        lines: full,
                        scanned,
                        shown,
                    })
                {
                    return;
                }
            }
        }
        if !batch.is_empty()
            && !sink.send(LogEvent::Lines {
                lines: batch.take(),
                scanned,
                shown,
            })
        {
            return;
        }
        if !follow || matches!(source.running().await, Ok(false)) {
            sink.send(LogEvent::End { scanned, shown });
            return;
        }
        let started = *lost_at.get_or_insert_with(tokio::time::Instant::now);
        if started.elapsed() >= RECONNECT_FOR {
            sink.send(LogEvent::Error { error: lost() });
            return;
        }
        // С начала секунды последней строки: API округляет `sinceTime` до секунд.
        since = Some(last.map_or_else(Timestamp::now, |t| {
            Timestamp::from_second(t.as_second()).unwrap_or(t)
        }));
        if !sink.send(LogEvent::Reconnecting {
            since: last.map(|t| t.to_string()),
        }) {
            return;
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(backoff(attempt)) => {}
        }
        attempt += 1;
    }
}

/// Сохраняет лог (без follow) в файл: подошедшие под фильтр строки целиком, с временем, если
/// `timestamps`. Возвращает число записанных строк.
pub async fn save(
    source: &impl LogSource,
    matcher: &Matcher,
    path: &Path,
    timestamps: bool,
) -> AppResult<u64> {
    let mut stream = source.open(None).await?;
    let mut out = String::new();
    let mut n = 0;
    while let Some(raw) = stream.next().await {
        let raw = raw?;
        let (ts, text) = split_line(&raw);
        if !matcher.matches(text) {
            continue;
        }
        if timestamps && let Some(t) = ts {
            out.push_str(&t.to_string());
            out.push(' ');
        }
        out.push_str(text);
        out.push('\n');
        n += 1;
    }
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || std::fs::write(path, out))
        .await
        .map_err(|e| AppError::internal_with(e.to_string()))??;
    Ok(n)
}
````

<!-- block: patch t11-src step=11.2 -->
````diff
diff --git a/.gitignore b/.gitignore
index 37065e4..7499f26 100644
--- a/.gitignore
+++ b/.gitignore
@@ -1,5 +1,5 @@
-# Logs
-logs
+# Logs (только в корне: в исходниках есть модули logs/)
+/logs
 *.log
 npm-debug.log*
 yarn-debug.log*
diff --git a/PORTED.md b/PORTED.md
index 8130550..c9269da 100644
--- a/PORTED.md
+++ b/PORTED.md
@@ -21,7 +21,7 @@
 | `src-tauri/build.rs` | `src-tauri/build.rs` | `8bbd0bd` | как есть |
 | `src-tauri/capabilities/default.json` | `src-tauri/capabilities/default.json` | `8bbd0bd` | как есть |
 | `index.html` | `index.html` | `8bbd0bd` | адаптирован: заголовок |
-| `.gitignore` | `.gitignore` | `8bbd0bd` | адаптирован: без путей Kafka-версии |
+| `.gitignore` | `.gitignore` | `8bbd0bd` | адаптирован: без путей Kafka-версии; `/logs` — только каталог в корне, не модули `logs/` |
 | `NOTICE` | `NOTICE` | `8bbd0bd` | адаптирован: имя продукта и товарные знаки |
 | `TRADEMARK.md` | `TRADEMARK.md` | `8bbd0bd` | адаптирован: имя продукта и товарные знаки |
 | `SECURITY.md` | `SECURITY.md` | `8bbd0bd` | адаптирован: имя продукта и товарные знаки |
diff --git a/src-tauri/src/commands/sessions.rs b/src-tauri/src/commands/sessions.rs
index fe948a8..fc64772 100644
--- a/src-tauri/src/commands/sessions.rs
+++ b/src-tauri/src/commands/sessions.rs
@@ -2,6 +2,7 @@ use tauri::State;
 use tauri::ipc::Channel;
 
 use crate::error::{AppResult, ErrorCode};
+use crate::logs::{KubeLogs, LogEvent, LogParams, Matcher};
 use crate::sessions::{SessionGuard, Sink};
 use crate::state::AppState;
 use crate::watch::{WatchEvent, WatchKind, WatchParams};
@@ -77,6 +78,63 @@ pub async fn start_watch_impl(
     Ok(id)
 }
 
+/// Логи контейнера (спец. §6.6); возвращает id сессии. Неверный regex — ошибка сразу.
+#[tauri::command]
+pub async fn start_logs(
+    state: State<'_, AppState>,
+    params: LogParams,
+    on_event: Channel<LogEvent>,
+) -> AppResult<String> {
+    start_logs_impl(state.inner(), params, on_event).await
+}
+
+pub async fn start_logs_impl(
+    st: &AppState,
+    params: LogParams,
+    sink: impl Sink<LogEvent>,
+) -> AppResult<String> {
+    let matcher = Matcher::new(params.filter.as_ref())?;
+    let reader = st
+        .read_in(&params.connection_id, &params.namespace, |c| async move {
+            Ok(c.reader())
+        })
+        .await?;
+    let follow = params.follow;
+    let (id, cancel) = st.sessions.register(&params.connection_id);
+    let guard = SessionGuard {
+        sessions: st.sessions.clone(),
+        id: id.clone(),
+    };
+    tokio::spawn(async move {
+        let _guard = guard;
+        crate::logs::run(KubeLogs { reader, params }, follow, matcher, sink, cancel).await;
+    });
+    Ok(id)
+}
+
+/// Сохраняет лог с теми же параметрами (без follow) в файл `path`; возвращает число строк.
+#[tauri::command]
+pub async fn save_logs(
+    state: State<'_, AppState>,
+    params: LogParams,
+    path: String,
+    timestamps: bool,
+) -> AppResult<u64> {
+    let matcher = Matcher::new(params.filter.as_ref())?;
+    let mut params = params;
+    params.follow = false;
+    let (id, ns) = (params.connection_id.clone(), params.namespace.clone());
+    state
+        .read_in(&id, &ns, |c| async move {
+            let logs = KubeLogs {
+                reader: c.reader(),
+                params,
+            };
+            crate::logs::save(&logs, &matcher, std::path::Path::new(&path), timestamps).await
+        })
+        .await
+}
+
 #[cfg(test)]
 mod tests {
     use super::*;
@@ -131,6 +189,34 @@ mod tests {
             .unwrap();
     }
 
+    #[tokio::test]
+    async fn invalid_regex_fails_before_the_session() {
+        let dir = tempfile::tempdir().unwrap();
+        let st =
+            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
+        let params = LogParams {
+            connection_id: "a".into(),
+            namespace: "team-a".into(),
+            pod: "web-1".into(),
+            container: None,
+            previous: false,
+            tail_lines: None,
+            since_seconds: None,
+            since_time: None,
+            follow: true,
+            filter: Some(crate::logs::LogFilter {
+                text: "[".into(),
+                regex: true,
+                case_sensitive: false,
+            }),
+        };
+        let e = start_logs_impl(&st, params, Collect::default())
+            .await
+            .unwrap_err();
+        assert_eq!(e.key, "errors.logs.badRegex");
+        assert!(st.sessions.is_empty());
+    }
+
     #[tokio::test]
     async fn pods_of_a_cron_job_are_not_watched() {
         let dir = tempfile::tempdir().unwrap();
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index cb3583a..0a368c6 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -7,6 +7,7 @@ mod dock_name;
 pub mod error;
 pub mod k8s;
 pub mod logging;
+pub mod logs;
 pub mod platform;
 pub mod policy;
 pub mod product;
@@ -74,6 +75,8 @@ pub fn run() {
             commands::settings::get_policy_status,
             commands::sessions::stop_session,
             commands::sessions::start_watch,
+            commands::sessions::start_logs,
+            commands::sessions::save_logs,
             commands::cluster::get_cluster_overview,
             commands::cluster::list_nodes,
             commands::cluster::get_node,
````

<!-- block: locales t11-locales step=11.2 -->
````json
{
  "en": {
    "errors": {
      "logs": {
        "badRegex": "Invalid regular expression: {{error}}",
        "lost": "The log stream was lost and did not recover in 60 s"
      }
    }
  },
  "ru": {
    "errors": {
      "logs": {
        "badRegex": "Неверное регулярное выражение: {{error}}",
        "lost": "Поток логов прервался и не восстановился за 60 с"
      }
    }
  }
}
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 352 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm test`  
Expected: PASS — `Test Files 32 passed`, `Tests 218 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 136`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 4: Commit.**

```bash
git add \
  .gitignore \
  PORTED.md \
  src-tauri/src/commands/sessions.rs \
  src-tauri/src/lib.rs \
  src-tauri/src/logs/mod.rs \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "feat(logs): логи контейнера — фильтр в Rust, обрезка длинных строк, переподключение в follow, сохранение в файл" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 12: Основа фронтенда O1b: типы и API-клиент, таблица, части экрана, YAML, watch-хук, перечитывание после входа

**Files:**
- Create: `src/app.css`, `src/components/common/Conditions.tsx`, `src/components/common/KeyValues.tsx`, `src/components/common/PartView.test.tsx`, `src/components/common/PartView.tsx`, `src/components/common/ReadError.tsx`, `src/components/common/RefreshControls.tsx`, `src/components/common/SubTabs.tsx`, `src/components/common/VirtualTable.test.tsx`, `src/components/common/VirtualTable.tsx`, `src/components/common/YamlView.test.tsx`, `src/components/common/YamlView.tsx`, `src/lib/afterSignIn.test.ts`, `src/lib/afterSignIn.ts`, `src/lib/age.test.ts`, `src/lib/age.ts`, `src/lib/signIn.ts`, `src/lib/useWatchList.test.tsx`, `src/lib/useWatchList.ts`, `src/state/connectionEpoch.ts`
- Modify: `package.json`, `src/api/client.ts`, `src/api/types.ts`, `src/components/cluster/ConnectionPanel.test.tsx`, `src/components/cluster/ConnectionPanel.tsx`, `src/components/cluster/SessionActions.tsx`, `src/locales/en.json`, `src/locales/ru.json`, `src/main.tsx`

**Interfaces:**
- Consumes: команды задач 1–11; O1a: `api/client.ts`, `api/types.ts`, `ErrorBanner`, `SessionActions`, `ConnectionPanel`.
- Produces: `api/types.ts`: `Part`, `Listed`, `ConditionView`, `ClusterOverview` и всё для него, `NodeRow`, `NodeDetails`, `ProjectRow`, `ProjectList`, `NamespaceAccess`, `WorkloadKind`, `WorkloadRow`, `WorkloadDetails`, `PodRow`, `PodDetails`, `EventRow`, `EventObject`, `NetworkView` и строки сети, `ConfigView`, `PvcRow`, `QuotasView`, `YamlKind`, `WatchParams`, `WatchEvent`, `RowChange`, `LogParams`, `LogEvent`, `ConnectionInfo.tokenExpiresAt`; `api.{getClusterOverview, listNodes, getNode, listClusterOperators, listProjects, getNamespaceAccess, listWorkloads, getWorkload, listPods, getPod, listEvents, getNetwork, getConfig, getConfigMapValues, listPvcs, getQuotas, getYaml, startWatch(params, onEvent), startLogs(params, onEvent), saveLogs, stopSession}`; `lib/age.ts` (`formatAge`, `formatDateTime`); `state/connectionEpoch.ts` (`useConnectionEpoch`, `useEpoch`); `lib/afterSignIn.ts`; `lib/signIn.ts` (`SIGN_IN_KEYS`, `needsSignIn(e, profile?)`); `lib/useWatchList.ts` (`useWatchList(params, keyOf)`, `podKey`, `workloadKey`, `eventKey`, `WatchState`); компоненты `common/{PartView, Unavailable, ReadError, VirtualTable, Column, RefreshControls, useRefreshInterval, YamlView, Conditions, KeyValues, SubTabs}`; стили `src/app.css`; зависимость `@tanstack/react-virtual`.

Типы IPC и клиент `api` для всех команд O1b; `startWatch` и `startLogs` создают `Channel` и отдают события в колбэк. Общие части экранов чтения: `VirtualTable` (виртуализированные строки, шапка, выбор мышью и клавиатурой; в тестах `useVirtualizer` подменяется, как в Kafka-версии), `PartView` / `Unavailable` (нет прав — «—» с причиной, ошибка — баннер), `ReadError` (истёкшая сессия — «Войти снова» на вкладку подключения, удалённый объект — пояснение), `RefreshControls` (обновить и автообновление выкл. / 10 с / 30 с), `YamlView` (копирование в буфер), `Conditions`, `KeyValues`, `SubTabs`, возраст объектов как в kubectl. `useWatchList` держит строки сессии watch (`Reset` — целиком, `Changes` — по порядку), состояние (подключение, живой, опрос, ошибка) и виды без прав; сессия останавливается при размонтировании и перезапускается после входа заново: успешный вход (`reconnect`, вход через браузер) вызывает `afterSignIn` — запросы вкладок подключения перечитываются (спец. §5.2), номер входа (`connectionEpoch`) растёт. `needsSignIn` вынесен из вкладки подключения в `lib/signIn.ts`.

- [ ] **Step 1: Тесты.**

<!-- block: file t12-src-components-common-partview-test-tsx step=12.1 path=src/components/common/PartView.test.tsx -->
````tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it } from "vitest";
import { renderWithClient } from "../../test/render";
import type { AppError } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { PartView } from "./PartView";
import { ReadError } from "./ReadError";

const FORBIDDEN: AppError = {
  code: "FORBIDDEN",
  key: "errors.k8s.forbiddenIn",
  params: { verb: "list", resource: "secrets", namespace: "team-a" },
  message: "Not allowed",
  retryable: false,
};

beforeEach(() => useWorkspace.setState({ tabs: [], activeId: null }));

describe("PartView и ReadError", () => {
  it("часть экрана: данные, «—» с причиной без прав, баннер при ошибке", () => {
    const { rerender } = renderWithClient(<PartView part={{ data: 3 }}>{(n) => <span>всего {n}</span>}</PartView>);
    expect(screen.getByText("всего 3")).toBeInTheDocument();
    rerender(<PartView part={{ error: FORBIDDEN }}>{() => <span>x</span>}</PartView>);
    expect(screen.getByText("— Нет прав: list secrets в проекте «team-a»")).toBeInTheDocument();
    rerender(<PartView part={{ error: { code: "TIMEOUT", key: "errors.k8s.timeout", message: "t", retryable: true } }}>{() => <span>x</span>}</PartView>);
    expect(screen.getByRole("alert")).toHaveTextContent("Сервер не ответил вовремя");
  });

  it("истёкшая сессия ведёт на вкладку подключения, удалённый объект — пояснение", async () => {
    const user = userEvent.setup();
    const { rerender } = renderWithClient(
      <ReadError error={{ code: "SESSION_EXPIRED", key: "errors.k8s.unauthorized", message: "x", retryable: false }} connectionId="c1" />,
    );
    await user.click(screen.getByRole("button", { name: "Войти снова" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "cluster", connectionId: "c1" });
    rerender(<ReadError error={{ code: "NOT_FOUND", key: "errors.k8s.notFound", message: "x", retryable: false }} connectionId="c1" />);
    expect(screen.getByText("Объект удалён или переименован — закройте вкладку")).toBeInTheDocument();
    rerender(<ReadError error={FORBIDDEN} connectionId="c1" onRetry={() => undefined} />);
    expect(screen.queryByRole("button", { name: "Повторить" })).not.toBeInTheDocument();
  });
});
````

<!-- block: file t12-src-components-common-virtualtable-test-tsx step=12.1 path=src/components/common/VirtualTable.test.tsx -->
````tsx
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 28 })),
    getTotalSize: () => o.count * 28,
  }),
}));

import { RefreshControls } from "./RefreshControls";
import { VirtualTable } from "./VirtualTable";

const rows = [
  { name: "web", ready: "1/1" },
  { name: "api", ready: "0/1" },
];
const columns = [
  { key: "name", title: "Имя", width: "1fr", render: (r: (typeof rows)[number]) => r.name },
  { key: "ready", title: "Готово", width: "60px", render: (r: (typeof rows)[number]) => r.ready },
];

describe("VirtualTable", () => {
  it("шапка, строки, выбор мышью и клавиатурой", async () => {
    const onRowClick = vi.fn();
    const user = userEvent.setup();
    render(<VirtualTable label="Поды" rows={rows} columns={columns} rowKey={(r) => r.name} onRowClick={onRowClick} selectedKey="api" />);
    expect(screen.getByRole("table", { name: "Поды" })).toBeInTheDocument();
    expect(screen.getAllByRole("columnheader").map((h) => h.textContent)).toEqual(["Имя", "Готово"]);
    const [header, web, api] = screen.getAllByRole("row");
    expect(header).not.toHaveAttribute("tabindex");
    expect(api).toHaveAttribute("aria-selected", "true");
    await user.click(web);
    expect(onRowClick).toHaveBeenCalledWith(rows[0]);
    api.focus();
    await user.keyboard("{Enter}");
    expect(onRowClick).toHaveBeenLastCalledWith(rows[1]);
  });

  it("пустой список и автообновление", async () => {
    const onInterval = vi.fn();
    const onRefresh = vi.fn();
    const user = userEvent.setup();
    render(
      <>
        <VirtualTable label="Поды" rows={[]} columns={columns} rowKey={(r) => r.name} />
        <RefreshControls interval={0} onInterval={onInterval} onRefresh={onRefresh} busy={false} />
      </>,
    );
    expect(screen.getByText("Пусто")).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Автообновление"), "каждые 10 с");
    expect(onInterval).toHaveBeenCalledWith(10_000);
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    expect(onRefresh).toHaveBeenCalled();
  });
});
````

<!-- block: file t12-src-components-common-yamlview-test-tsx step=12.1 path=src/components/common/YamlView.test.tsx -->
````tsx
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getYaml: vi.fn() } };
});

import { api } from "../../api/client";
import { Conditions } from "./Conditions";
import { KeyValues } from "./KeyValues";
import { SubTabs } from "./SubTabs";
import { YamlView } from "./YamlView";

beforeEach(() => vi.clearAllMocks());

describe("YamlView и мелкие части экранов", () => {
  it("YAML объекта и копирование", async () => {
    vi.mocked(api.getYaml).mockResolvedValue("kind: Deployment\nmetadata:\n  name: web\n");
    const user = userEvent.setup();
    const writeText = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();
    renderWithClient(<YamlView connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    expect(await screen.findByText(/kind: Deployment/)).toBeInTheDocument();
    expect(api.getYaml).toHaveBeenCalledWith("c1", "team-a", "Deployment", "web");
    await user.click(screen.getByRole("button", { name: "Копировать" }));
    expect(writeText).toHaveBeenCalledWith("kind: Deployment\nmetadata:\n  name: web\n");
    await waitFor(() => expect(screen.getByText("Скопировано")).toBeInTheDocument());
  });

  it("условия, метки и подвкладки", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    renderWithClient(
      <>
        <Conditions conditions={[{ type: "Ready", status: "False", reason: "ContainersNotReady", message: "web not ready" }]} />
        <KeyValues values={{ app: "web" }} />
        <SubTabs tabs={[{ id: "a", label: "Обзор" }, { id: "b", label: "YAML" }]} active="a" onChange={onChange} />
      </>,
    );
    expect(screen.getByText("ContainersNotReady")).toBeInTheDocument();
    expect(screen.getByText("app=web")).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Обзор" })).toHaveAttribute("aria-selected", "true");
    await user.click(screen.getByRole("tab", { name: "YAML" }));
    expect(onChange).toHaveBeenCalledWith("b");
  });
});
````

<!-- block: file t12-src-lib-aftersignin-test-ts step=12.1 path=src/lib/afterSignIn.test.ts -->
````ts
import { QueryClient } from "@tanstack/react-query";
import { describe, expect, it } from "vitest";
import { useConnectionEpoch } from "../state/connectionEpoch";
import { afterSignIn } from "./afterSignIn";

describe("afterSignIn", () => {
  it("перечитывает вкладки только этого подключения и перезапускает его сессии", () => {
    const qc = new QueryClient();
    for (const key of [["connection", "c1"], ["pods", "c1", "team-a"], ["pods", "c2", "team-a"]]) qc.setQueryData(key, 1);
    afterSignIn(qc, "c1");
    const stale = (key: string[]) => qc.getQueryState(key)?.isInvalidated;
    expect(stale(["pods", "c1", "team-a"])).toBe(true);
    expect(stale(["pods", "c2", "team-a"])).toBe(false);
    expect(stale(["connection", "c1"])).toBe(false);
    expect(useConnectionEpoch.getState().epochs.c1).toBe(1);
  });
});
````

<!-- block: file t12-src-lib-age-test-ts step=12.1 path=src/lib/age.test.ts -->
````ts
import { describe, expect, it } from "vitest";
import { formatAge, formatDateTime } from "./age";

describe("age", () => {
  it("возраст коротко: секунды, минуты, часы, дни", () => {
    const now = 1_000_000_000_000;
    const ago = (s: number) => formatAge(now / 1000 - s, now);
    expect(ago(45)).toBe("45 с");
    expect(ago(150)).toBe("2 мин");
    expect(ago(3 * 3600)).toBe("3 ч");
    expect(ago(5 * 86_400)).toBe("5 д");
    expect(formatAge(now / 1000 + 30, now)).toBe("0 с");
    expect(formatAge(null)).toBe("—");
  });

  it("дата и время", () => {
    expect(formatDateTime(undefined)).toBe("—");
    expect(formatDateTime(0)).toContain("1970");
  });
});
````

<!-- block: file t12-src-lib-usewatchlist-test-tsx step=12.1 path=src/lib/useWatchList.test.tsx -->
````tsx
import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../api/client")>();
  return { ...orig, api: { startWatch: vi.fn(), stopSession: vi.fn() } };
});

import { api } from "../api/client";
import type { PodRow, WatchEvent, WatchParams } from "../api/types";
import { useConnectionEpoch } from "../state/connectionEpoch";
import { podKey, useWatchList } from "./useWatchList";

const pod = (name: string, status = "Running"): PodRow => ({
  name,
  phase: "Running",
  status,
  problem: null,
  ready: 1,
  containers: 1,
  restarts: 0,
  lastRestart: null,
  node: "w1",
  created: 1,
  qos: null,
  owner: null,
});

const PARAMS: WatchParams = { connectionId: "c1", namespace: "team-a", kind: "pods" };

let emit: (e: WatchEvent) => void = () => undefined;

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.startWatch).mockImplementation(async (_p, onEvent) => {
    emit = onEvent;
    return `s${vi.mocked(api.startWatch).mock.calls.length}`;
  });
  vi.mocked(api.stopSession).mockResolvedValue(true);
});

describe("useWatchList", () => {
  it("список целиком, затем изменения по порядку, опрос и ошибки", async () => {
    const { result } = renderHook(() => useWatchList(PARAMS, podKey));
    expect(result.current.status).toBe("connecting");
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledWith(PARAMS, expect.any(Function)));
    act(() => emit({ type: "reset", items: [pod("a"), pod("b")], errors: [] }));
    expect(result.current.rows.map((r) => r.name)).toEqual(["a", "b"]);
    expect(result.current.status).toBe("live");
    act(() =>
      emit({
        type: "changes",
        changes: [
          { op: "upsert", row: pod("c") },
          { op: "delete", key: "a" },
          { op: "upsert", row: pod("b", "CrashLoopBackOff") },
        ],
      }),
    );
    expect(result.current.rows.map((r) => `${r.name}:${r.status}`)).toEqual(["b:CrashLoopBackOff", "c:Running"]);
    act(() => emit({ type: "polling" }));
    expect(result.current.status).toBe("polling");
    const error = { code: "CONNECTION_FAILED" as const, message: "x", retryable: true };
    act(() => emit({ type: "error", error, fatal: false }));
    expect(result.current.status).toBe("polling");
    expect(result.current.error).toEqual(error);
    act(() => emit({ type: "error", error, fatal: true }));
    expect(result.current.status).toBe("error");
  });

  it("останавливает сессию при размонтировании и перезапускает после входа заново", async () => {
    const { unmount } = renderHook(() => useWatchList(PARAMS, podKey));
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
    await act(async () => {
      useConnectionEpoch.getState().bump("c1");
    });
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(2));
    expect(api.stopSession).toHaveBeenCalledWith("s1");
    unmount();
    expect(api.stopSession).toHaveBeenCalledWith("s2");
  });

  it("без параметров сессии нет", () => {
    renderHook(() => useWatchList(null, podKey));
    expect(api.startWatch).not.toHaveBeenCalled();
  });
});
````

<!-- block: patch t12-tests step=12.1 -->
````diff
diff --git a/src/components/cluster/ConnectionPanel.test.tsx b/src/components/cluster/ConnectionPanel.test.tsx
index 13733e8..6f65baf 100644
--- a/src/components/cluster/ConnectionPanel.test.tsx
+++ b/src/components/cluster/ConnectionPanel.test.tsx
@@ -34,6 +34,7 @@ const INFO: ConnectionInfo = {
   platform: { kind: "openshift", distribution: null, version: "4.18", label: "OpenShift 4.18" },
   apiGroups: { routes: true, projects: true, deploymentConfigs: true, gatewayApi: false },
   capabilities: { listProjects: true, listNodes: false, listAllPods: false, readClusterOperators: true, readClusterVersion: false },
+  tokenExpiresAt: null,
 };
 
 const profile = (auth: ConnectionProfile["auth"]): ConnectionProfile => ({
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/cluster/ConnectionPanel.test.tsx src/components/common/PartView.test.tsx src/components/common/VirtualTable.test.tsx src/components/common/YamlView.test.tsx src/lib/afterSignIn.test.ts src/lib/age.test.ts src/lib/useWatchList.test.tsx`  
Expected: FAIL — нет модулей `lib/age`, `lib/afterSignIn`, `lib/useWatchList`, компонентов `PartView`, `ReadError`, `VirtualTable`, `RefreshControls`, `YamlView`, `Conditions`, `KeyValues`, `SubTabs`.

- [ ] **Step 3: Реализация.**

<!-- block: file t12-src-app-css step=12.3 path=src/app.css -->
````css
/* Экраны чтения кластера (этап O1b); общие стили — в styles.css (перенесён из Kafka-версии). */
.yaml { background: var(--bg-alt); border: 1px solid var(--border); border-radius: 4px; padding: 8px; overflow: auto; white-space: pre; word-break: normal; }
.kv { list-style: none; margin: 0; padding: 0; display: flex; flex-wrap: wrap; gap: 4px; }
.kv code { background: var(--bg-alt); border-radius: 3px; padding: 0 4px; overflow-wrap: anywhere; }
.problem { color: var(--danger); font-weight: 600; }
.split { display: flex; gap: 8px; flex: 1; min-height: 320px; }
.split > .vlist { flex: 3; }
.split > .details-pane { flex: 2; }
.section { display: flex; flex-direction: column; gap: 6px; }
.section h3 { margin: 4px 0 0; font-size: 14px; }
.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 8px; }
.card { border: 1px solid var(--border); border-radius: 6px; padding: 8px; display: flex; flex-direction: column; gap: 4px; min-width: 0; }
.card h4 { margin: 0; font-size: 13px; overflow-wrap: anywhere; }
.facts { display: grid; grid-template-columns: max-content minmax(0, 1fr); gap: 2px 12px; margin: 0; }
.facts dt { color: var(--muted); }
.facts dd { margin: 0; overflow-wrap: anywhere; }
````

<!-- block: file t12-src-components-common-conditions-tsx step=12.3 path=src/components/common/Conditions.tsx -->
````tsx
import { useTranslation } from "react-i18next";
import type { ConditionView } from "../../api/types";
import { formatAge } from "../../lib/age";

/** Условия объекта (`status.conditions`): вид, статус, причина, сообщение, с какого времени. */
export function Conditions({ conditions }: { conditions: ConditionView[] }) {
  const { t } = useTranslation();
  if (conditions.length === 0) return <p className="muted">—</p>;
  return (
    <table>
      <thead>
        <tr>
          <th>{t("objects.conditions.type")}</th>
          <th>{t("objects.conditions.status")}</th>
          <th>{t("objects.conditions.reason")}</th>
          <th>{t("objects.conditions.message")}</th>
          <th>{t("objects.conditions.since")}</th>
        </tr>
      </thead>
      <tbody>
        {conditions.map((c) => (
          <tr key={c.type}>
            <td>{c.type}</td>
            <td className={c.status === "True" ? undefined : "muted"}>{c.status}</td>
            <td>{c.reason ?? ""}</td>
            <td>{c.message ?? ""}</td>
            <td>{formatAge(c.lastTransition ?? null)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
````

<!-- block: file t12-src-components-common-keyvalues-tsx step=12.3 path=src/components/common/KeyValues.tsx -->
````tsx
/** Метки или аннотации: `ключ=значение` по одному в строке; пусто — «—». */
export function KeyValues({ values }: { values: Record<string, string> }) {
  const entries = Object.entries(values);
  if (entries.length === 0) return <p className="muted">—</p>;
  return (
    <ul className="kv">
      {entries.map(([k, v]) => (
        <li key={k}>
          <code>
            {k}={v}
          </code>
        </li>
      ))}
    </ul>
  );
}
````

<!-- block: file t12-src-components-common-partview-tsx step=12.3 path=src/components/common/PartView.tsx -->
````tsx
import type { ReactNode } from "react";
import type { AppError, Part } from "../../api/types";
import { tr } from "../../i18n/text";
import { ErrorBanner } from "./ErrorBanner";

/** Нет прав — «—» с причиной, иная ошибка — баннер (спец. §15, «Частичные результаты»). */
export function Unavailable({ error }: { error: AppError }) {
  if (error.code === "FORBIDDEN") return <p className="muted">— {tr(error)}</p>;
  return <ErrorBanner error={error} />;
}

/** Часть экрана: данные или причина, почему их нет; остальной экран показывается. */
export function PartView<T>({ part, children }: { part: Part<T> | null | undefined; children: (data: T) => ReactNode }) {
  if (!part) return null;
  if (part.error) return <Unavailable error={part.error} />;
  return part.data === undefined ? null : <>{children(part.data)}</>;
}
````

<!-- block: file t12-src-components-common-readerror-tsx step=12.3 path=src/components/common/ReadError.tsx -->
````tsx
import { useTranslation } from "react-i18next";
import { toAppError } from "../../api/client";
import { needsSignIn } from "../../lib/signIn";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "./ErrorBanner";

/**
 * Ошибка чтения на вкладке объекта. Сессия истекла — «Войти снова» открывает вкладку подключения
 * (там вход), после входа вкладки перечитываются сами; объект удалён — пояснение; нет прав — без
 * «Повторить»; прочее — баннер с «Повторить».
 */
export function ReadError({ error, connectionId, onRetry }: { error: unknown; connectionId: string; onRetry?: () => void }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const e = toAppError(error);
  if (needsSignIn(e))
    return (
      <div className="panel">
        <ErrorBanner error={e} />
        <div className="toolbar">
          <button type="button" className="primary" onClick={() => open({ kind: "cluster", connectionId })}>
            {t("objects.signInAgain")}
          </button>
        </div>
      </div>
    );
  if (e.code === "NOT_FOUND") return <p className="empty">{t("objects.gone")}</p>;
  return <ErrorBanner error={e} onRetry={e.code === "FORBIDDEN" ? undefined : onRetry} />;
}
````

<!-- block: file t12-src-components-common-refreshcontrols-tsx step=12.3 path=src/components/common/RefreshControls.tsx -->
````tsx
import { useState } from "react";
import { useTranslation } from "react-i18next";

/** Варианты автообновления (спец. §6.7): выключено, 10 с, 30 с. */
export const REFRESH_INTERVALS = [0, 10_000, 30_000] as const;

/** Интервал автообновления экрана (0 — выключено) для `refetchInterval`. */
export function useRefreshInterval(): [number, (ms: number) => void] {
  return useState<number>(0);
}

/** «Обновить» и автообновление для экранов без watch. */
export function RefreshControls({
  interval,
  onInterval,
  onRefresh,
  busy,
}: {
  interval: number;
  onInterval: (ms: number) => void;
  onRefresh: () => void;
  busy: boolean;
}) {
  const { t } = useTranslation();
  return (
    <span className="toolbar">
      <button type="button" disabled={busy} onClick={onRefresh}>
        {t("common.refresh")}
      </button>
      <label className="inline">
        {t("common.autoRefresh")}
        <select value={interval} onChange={(e) => onInterval(Number(e.target.value))}>
          {REFRESH_INTERVALS.map((ms) => (
            <option key={ms} value={ms}>
              {ms === 0 ? t("common.autoRefreshOff") : t("common.everySeconds", { n: ms / 1000 })}
            </option>
          ))}
        </select>
      </label>
    </span>
  );
}
````

<!-- block: file t12-src-components-common-subtabs-tsx step=12.3 path=src/components/common/SubTabs.tsx -->
````tsx
/** Подвкладки экрана объекта: показывается одна, остальные не смонтированы (их watch не идёт). */
export function SubTabs<T extends string>({ tabs, active, onChange }: { tabs: { id: T; label: string }[]; active: T; onChange: (id: T) => void }) {
  return (
    <div className="subtabs" role="tablist">
      {tabs.map((tab) => (
        <button key={tab.id} type="button" role="tab" aria-selected={tab.id === active} className={tab.id === active ? "active" : undefined} onClick={() => onChange(tab.id)}>
          {tab.label}
        </button>
      ))}
    </div>
  );
}
````

<!-- block: file t12-src-components-common-virtualtable-tsx step=12.3 path=src/components/common/VirtualTable.tsx -->
````tsx
import { useVirtualizer } from "@tanstack/react-virtual";
import { type KeyboardEvent, type ReactNode, useRef } from "react";
import { useTranslation } from "react-i18next";

const ROW_HEIGHT = 28;

export interface Column<T> {
  key: string;
  title: ReactNode;
  /** Ширина в `grid-template-columns`: `120px`, `minmax(160px, 1fr)`. */
  width: string;
  render: (row: T) => ReactNode;
}

/** Таблица большого списка (спец. §6.7): строки виртуализированы, шапка закреплена, строку можно выбрать мышью и с клавиатуры. */
export function VirtualTable<T>({
  rows,
  columns,
  rowKey,
  onRowClick,
  selectedKey,
  label,
  empty,
}: {
  rows: T[];
  columns: Column<T>[];
  rowKey: (row: T) => string;
  onRowClick?: (row: T) => void;
  selectedKey?: string | null;
  label: string;
  empty?: string;
}) {
  const { t } = useTranslation();
  const parentRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
  });
  const grid = { gridTemplateColumns: columns.map((c) => c.width).join(" ") };
  const activate = (row: T) => ({
    tabIndex: 0,
    onClick: () => onRowClick?.(row),
    onKeyDown: (e: KeyboardEvent) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        onRowClick?.(row);
      }
    },
  });
  return (
    <div className="vlist" ref={parentRef} role="table" aria-label={label}>
      <div className="row header" role="row" style={{ ...grid, height: ROW_HEIGHT }}>
        {columns.map((c) => (
          <span key={c.key} role="columnheader">
            {c.title}
          </span>
        ))}
      </div>
      {rows.length === 0 && <p className="empty">{empty ?? t("common.empty")}</p>}
      <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
        {virtualizer.getVirtualItems().map((item) => {
          const row = rows[item.index];
          const key = rowKey(row);
          const selected = key === selectedKey;
          return (
            <div
              key={key}
              role="row"
              className={`row${onRowClick ? " clickable" : ""}${selected ? " selected" : ""}`}
              style={{ ...grid, top: item.start, height: ROW_HEIGHT }}
              aria-selected={onRowClick ? selected : undefined}
              {...(onRowClick ? activate(row) : {})}
            >
              {columns.map((c) => (
                <span key={c.key} role="cell">
                  {c.render(row)}
                </span>
              ))}
            </div>
          );
        })}
      </div>
    </div>
  );
}
````

<!-- block: file t12-src-components-common-yamlview-tsx step=12.3 path=src/components/common/YamlView.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { YamlKind } from "../../api/types";
import { ReadError } from "./ReadError";

/** YAML объекта только для чтения, без `managedFields` (спец. §6.4); копирование в буфер. */
export function YamlView({ connectionId, namespace, kind, name }: { connectionId: string; namespace: string; kind: YamlKind; name: string }) {
  const { t } = useTranslation();
  const q = useQuery({ queryKey: ["yaml", connectionId, namespace, kind, name], queryFn: () => api.getYaml(connectionId, namespace, kind, name) });
  const [copied, setCopied] = useState(false);
  return (
    <div className="panel">
      <div className="toolbar">
        <button type="button" disabled={q.isFetching} onClick={() => void q.refetch()}>
          {t("common.refresh")}
        </button>
        <button
          type="button"
          disabled={!q.data}
          onClick={() => {
            if (!q.data) return;
            navigator.clipboard.writeText(q.data).then(
              () => setCopied(true),
              () => setCopied(false),
            );
          }}
        >
          {t("common.copy")}
        </button>
        {copied && <span className="muted">{t("common.copied")}</span>}
      </div>
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && <pre className="yaml">{q.data}</pre>}
    </div>
  );
}
````

<!-- block: file t12-src-lib-aftersignin-ts step=12.3 path=src/lib/afterSignIn.ts -->
````ts
import type { QueryClient } from "@tanstack/react-query";
import { useConnectionEpoch } from "../state/connectionEpoch";

/**
 * После входа заново открытые вкладки подключения перечитываются (спец. §5.2): запросы с этим
 * подключением (ключ `[вид, connectionId, …]`) помечаются устаревшими, watch и логи перезапускаются.
 * Сведения о подключении (`["connection", id]`) уже свежие — их вход и вернул.
 */
export function afterSignIn(queryClient: QueryClient, connectionId: string): void {
  void queryClient.invalidateQueries({ predicate: (q) => q.queryKey[0] !== "connection" && q.queryKey[1] === connectionId });
  useConnectionEpoch.getState().bump(connectionId);
}
````

<!-- block: file t12-src-lib-age-ts step=12.3 path=src/lib/age.ts -->
````ts
import { currentLocale, i18n } from "../i18n";

/** Возраст объекта коротко, как в kubectl: «45 с», «12 мин», «5 ч», «3 д»; нет времени — «—». */
export function formatAge(createdSec: number | null | undefined, nowMs = Date.now()): string {
  if (createdSec == null) return "—";
  const s = Math.max(0, Math.floor(nowMs / 1000 - createdSec));
  if (s < 120) return i18n.t("common.age.seconds", { n: s });
  if (s < 7200) return i18n.t("common.age.minutes", { n: Math.floor(s / 60) });
  if (s < 172_800) return i18n.t("common.age.hours", { n: Math.floor(s / 3600) });
  return i18n.t("common.age.days", { n: Math.floor(s / 86_400) });
}

/** Дата и время (секунды Unix) в формате языка интерфейса; нет времени — «—». */
export function formatDateTime(sec: number | null | undefined): string {
  if (sec == null) return "—";
  return new Date(sec * 1000).toLocaleString(currentLocale());
}
````

<!-- block: file t12-src-lib-signin-ts step=12.3 path=src/lib/signIn.ts -->
````ts
import type { AppError, ConnectionProfile } from "../api/types";

/** Ошибки, которые лечатся входом заново или новым токеном. */
export const SIGN_IN_KEYS = new Set(["errors.connections.tokenMissing", "errors.connections.loginRequired", "errors.connections.clientKeyMissing"]);

/**
 * Помогает ли войти заново: истёкшая сессия, нет токена или ключа, ошибка exec-плагина (если
 * профиль известен и вход у него — exec-плагином).
 */
export function needsSignIn(e: AppError, profile?: ConnectionProfile): boolean {
  if (e.code === "SESSION_EXPIRED" || SIGN_IN_KEYS.has(e.key ?? "")) return true;
  const exec = profile ? profile.auth.method === "exec" : e.key?.startsWith("errors.exec.");
  return Boolean(exec) && (e.key?.startsWith("errors.exec.") || e.key === "errors.oauth.cancelled");
}
````

<!-- block: file t12-src-lib-usewatchlist-ts step=12.3 path=src/lib/useWatchList.ts -->
````ts
import { useEffect, useState } from "react";
import { api, toAppError } from "../api/client";
import type { AppError, EventRow, KindError, PodRow, WatchParams, WatchRow, WorkloadRow } from "../api/types";
import { useEpoch } from "../state/connectionEpoch";

/** Ключи строк — те же, что у Rust (`Row::key`). */
export const podKey = (p: PodRow) => p.name;
export const workloadKey = (w: WorkloadRow) => `${w.kind}/${w.name}`;
export const eventKey = (e: EventRow) => e.key;

export type WatchStatus = "connecting" | "live" | "polling" | "error";

export interface WatchState<T> {
  rows: T[];
  status: WatchStatus;
  /** Последняя ошибка: при `error` сессия завершена, иначе watcher пробует снова. */
  error: AppError | null;
  /** Виды workloads, которые не читаются. */
  kindErrors: KindError[];
}

const INITIAL = { rows: [], status: "connecting" as const, error: null, kindErrors: [] };

/**
 * Список проекта, который обновляется сам (спец. §6.7): сессия watch в Rust, а без прав на watch —
 * опрос раз в 10 с. Сессия перезапускается при смене параметров и после входа заново и
 * останавливается при размонтировании. `params` null — сессии нет. `keyOf` — функция модуля.
 */
export function useWatchList<T extends WatchRow>(params: WatchParams | null, keyOf: (row: T) => string): WatchState<T> {
  const epoch = useEpoch(params?.connectionId ?? "");
  const paramsKey = params ? JSON.stringify(params) : null;
  const [state, setState] = useState<WatchState<T>>(INITIAL);

  useEffect(() => {
    if (!paramsKey) return;
    const p = JSON.parse(paramsKey) as WatchParams;
    let active = true;
    let sessionId: string | null = null;
    const rows = new Map<string, T>();
    setState(INITIAL);
    api
      .startWatch(p, (e) => {
        if (!active) return;
        switch (e.type) {
          case "reset":
            rows.clear();
            for (const r of e.items as T[]) rows.set(keyOf(r), r);
            setState((s) => ({ rows: [...rows.values()], status: s.status === "polling" ? "polling" : "live", error: null, kindErrors: e.errors }));
            break;
          case "changes":
            for (const c of e.changes) {
              if (c.op === "upsert") rows.set(keyOf(c.row as T), c.row as T);
              else rows.delete(c.key);
            }
            setState((s) => ({ ...s, rows: [...rows.values()], error: null }));
            break;
          case "polling":
            setState((s) => ({ ...s, status: "polling" }));
            break;
          case "error":
            setState((s) => ({ ...s, status: e.fatal ? "error" : s.status, error: e.error }));
            break;
        }
      })
      .then((id) => {
        if (active) sessionId = id;
        else api.stopSession(id).catch(() => undefined);
      })
      .catch((err) => {
        if (active) setState((s) => ({ ...s, status: "error", error: toAppError(err) }));
      });
    return () => {
      active = false;
      if (sessionId) api.stopSession(sessionId).catch(() => undefined);
    };
    // keyOf — функция модуля; сессия зависит только от параметров и входа.
  }, [paramsKey, epoch]);

  return state;
}
````

<!-- block: file t12-src-state-connectionepoch-ts step=12.3 path=src/state/connectionEpoch.ts -->
````ts
import { create } from "zustand";

/**
 * Номер входа подключения: растёт после каждого успешного входа заново (токен, браузер,
 * exec-плагин). По нему открытые вкладки перезапускают watch и логи.
 */
interface EpochState {
  epochs: Record<string, number>;
  bump: (connectionId: string) => void;
}

export const useConnectionEpoch = create<EpochState>((set) => ({
  epochs: {},
  bump: (id) => set((s) => ({ epochs: { ...s.epochs, [id]: (s.epochs[id] ?? 0) + 1 } })),
}));

export function useEpoch(connectionId: string): number {
  return useConnectionEpoch((s) => s.epochs[connectionId] ?? 0);
}
````

<!-- block: patch t12-src step=12.3 -->
````diff
diff --git a/package.json b/package.json
index 2981fed..bc8b69a 100644
--- a/package.json
+++ b/package.json
@@ -18,6 +18,7 @@
   "dependencies": {
     "@floating-ui/react": "^0.27.20",
     "@tanstack/react-query": "^5.104.2",
+    "@tanstack/react-virtual": "^3.14.18",
     "@tauri-apps/api": "^2",
     "@tauri-apps/plugin-dialog": "^2.8.2",
     "@tauri-apps/plugin-opener": "^2",
diff --git a/src/api/client.ts b/src/api/client.ts
index dc7f377..b757b75 100644
--- a/src/api/client.ts
+++ b/src/api/client.ts
@@ -1,18 +1,43 @@
-import { invoke } from "@tauri-apps/api/core";
+import { Channel, invoke } from "@tauri-apps/api/core";
 import type {
   AppError,
   AppInfo,
+  ClusterOverview,
+  ConfigValue,
+  ConfigView,
   ConnectionFolder,
   ConnectionInfo,
   ConnectionProfile,
+  EventObject,
+  EventRow,
   ExportSummary,
   ImportPreview,
   KubeconfigPreview,
   LanguageSettings,
+  Listed,
+  LogEvent,
+  LogParams,
+  NamespaceAccess,
+  NetworkView,
+  NodeDetails,
+  NodeRow,
+  OperatorRow,
+  PodDetails,
+  PodRow,
   PolicyStatus,
+  ProjectList,
+  PvcRow,
+  QuotasView,
   SecretsInput,
   TestReport,
   TreeItemRef,
+  WatchEvent,
+  WatchParams,
+  WorkloadDetails,
+  WorkloadKind,
+  WorkloadList,
+  WorkloadRef,
+  YamlKind,
 } from "./types";
 
 export function isAppError(e: unknown): e is AppError {
@@ -79,4 +104,46 @@ export const api = {
     call<ImportPreview>("preview_import", { path, targetFolderId, updateExisting }),
   applyImport: (path: string, targetFolderId: string | null, updateExisting: boolean) =>
     call<ImportPreview>("apply_import", { path, targetFolderId, updateExisting }),
+
+  getClusterOverview: (connectionId: string) => call<ClusterOverview>("get_cluster_overview", { connectionId }),
+  listNodes: (connectionId: string) => call<Listed<NodeRow>>("list_nodes", { connectionId }),
+  getNode: (connectionId: string, name: string) => call<NodeDetails>("get_node", { connectionId, name }),
+  listClusterOperators: (connectionId: string) => call<OperatorRow[]>("list_cluster_operators", { connectionId }),
+  listProjects: (connectionId: string) => call<ProjectList>("list_projects", { connectionId }),
+  getNamespaceAccess: (connectionId: string, namespace: string) =>
+    call<NamespaceAccess>("get_namespace_access", { connectionId, namespace }),
+  listWorkloads: (connectionId: string, namespace: string) => call<WorkloadList>("list_workloads", { connectionId, namespace }),
+  getWorkload: (connectionId: string, namespace: string, kind: WorkloadKind, name: string) =>
+    call<WorkloadDetails>("get_workload", { connectionId, namespace, kind, name }),
+  /** Поды проекта или, если задан `workload`, его поды. */
+  listPods: (connectionId: string, namespace: string, workload: WorkloadRef | null = null) =>
+    call<Listed<PodRow>>("list_pods", { connectionId, namespace, workload }),
+  getPod: (connectionId: string, namespace: string, name: string) => call<PodDetails>("get_pod", { connectionId, namespace, name }),
+  /** События проекта или объекта; у workload'а — вместе с его подами. */
+  listEvents: (connectionId: string, namespace: string, object: EventObject | null = null) =>
+    call<Listed<EventRow>>("list_events", { connectionId, namespace, object }),
+  getNetwork: (connectionId: string, namespace: string) => call<NetworkView>("get_network", { connectionId, namespace }),
+  getConfig: (connectionId: string, namespace: string) => call<ConfigView>("get_config", { connectionId, namespace }),
+  getConfigMapValues: (connectionId: string, namespace: string, name: string) =>
+    call<Record<string, ConfigValue>>("get_config_map_values", { connectionId, namespace, name }),
+  listPvcs: (connectionId: string, namespace: string) => call<Listed<PvcRow>>("list_pvcs", { connectionId, namespace }),
+  getQuotas: (connectionId: string, namespace: string) => call<QuotasView>("get_quotas", { connectionId, namespace }),
+  getYaml: (connectionId: string, namespace: string, kind: YamlKind, name: string) =>
+    call<string>("get_yaml", { connectionId, namespace, kind, name }),
+
+  /** Watch списка проекта; события приходят в `onEvent`. Возвращает id сессии для `stopSession`. */
+  startWatch: (params: WatchParams, onEvent: (e: WatchEvent) => void) => {
+    const channel = new Channel<WatchEvent>();
+    channel.onmessage = onEvent;
+    return call<string>("start_watch", { params, onEvent: channel });
+  },
+  /** Логи контейнера; события приходят в `onEvent`. Возвращает id сессии для `stopSession`. */
+  startLogs: (params: LogParams, onEvent: (e: LogEvent) => void) => {
+    const channel = new Channel<LogEvent>();
+    channel.onmessage = onEvent;
+    return call<string>("start_logs", { params, onEvent: channel });
+  },
+  /** Сохраняет лог (без follow) в файл; возвращает число строк. */
+  saveLogs: (params: LogParams, path: string, timestamps: boolean) => call<number>("save_logs", { params, path, timestamps }),
+  stopSession: (sessionId: string) => call<boolean>("stop_session", { sessionId }),
 };
diff --git a/src/api/types.ts b/src/api/types.ts
index b6feee9..dab8296 100644
--- a/src/api/types.ts
+++ b/src/api/types.ts
@@ -181,6 +181,8 @@ export interface ConnectionInfo {
   platform: PlatformInfo;
   apiGroups: ApiGroups;
   capabilities: Capabilities;
+  /** Когда истекает токен входа (секунды Unix), если платформа это сообщает (OpenShift). */
+  tokenExpiresAt: number | null;
 }
 
 export type ImportAuth = "token" | "clientCert" | "browser" | "exec";
@@ -252,3 +254,578 @@ export interface LanguageSettings {
   reason: LanguageReason;
   languages: string[];
 }
+
+// --- Чтение кластера (этап O1b). Время — секунды Unix.
+
+/** Часть экрана: данные или причина, почему их нет (права, ошибка чтения). */
+export interface Part<T> {
+  data?: T;
+  error?: AppError;
+}
+
+/** Список; `truncated` — прочитано не всё (больше 10 000 объектов). */
+export interface Listed<T> {
+  items: T[];
+  truncated: boolean;
+}
+
+export interface ConditionView {
+  type: string;
+  status: string;
+  reason?: string;
+  message?: string;
+  lastTransition?: number;
+}
+
+export interface Readyz {
+  ok: boolean;
+  failed: string[];
+  error?: AppError;
+}
+
+export interface RoleCount {
+  role: string;
+  total: number;
+  ready: number;
+}
+
+export interface NodeSummary {
+  total: number;
+  ready: number;
+  roles: RoleCount[];
+}
+
+export interface IngressClassRow {
+  name: string;
+  controller: string | null;
+  default: boolean;
+}
+
+export interface GatewayClassRow {
+  name: string;
+  controller: string | null;
+  accepted: boolean | null;
+}
+
+export interface ProjectCount {
+  count: number;
+  more: boolean;
+}
+
+export interface ClusterVersionInfo {
+  version: string | null;
+  channel: string | null;
+  availableUpdates: number;
+  failing: ConditionView | null;
+  progressing: ConditionView | null;
+}
+
+export interface OperatorRow {
+  name: string;
+  version: string | null;
+  available: boolean | null;
+  progressing: boolean | null;
+  degraded: boolean | null;
+  message: string | null;
+}
+
+export interface MachineConfigPoolRow {
+  name: string;
+  degraded: boolean;
+  updating: boolean;
+  machines: number;
+  ready: number;
+  updated: number;
+  degradedMachines: number;
+}
+
+/** Часть обзора OpenShift. */
+export interface OpenShiftOverview {
+  kind: "openshift";
+  clusterVersion: Part<ClusterVersionInfo>;
+  infrastructure: string | null;
+  consoleUrl: string | null;
+  operators: Part<{ total: number; problems: OperatorRow[] }>;
+  machineConfigPools: Part<MachineConfigPoolRow[]>;
+}
+
+export interface ClusterOverview {
+  platform: PlatformInfo;
+  serverVersion: string;
+  readyz: Readyz;
+  nodes: Part<NodeSummary>;
+  ingressClasses: Part<IngressClassRow[]>;
+  /** null — в кластере нет Gateway API. */
+  gatewayClasses: Part<GatewayClassRow[]> | null;
+  projects: Part<ProjectCount>;
+  platformDetails: OpenShiftOverview | null;
+}
+
+export interface NodeRow {
+  name: string;
+  roles: string[];
+  ready: boolean;
+  problems: string[];
+  unschedulable: boolean;
+  kubeletVersion: string | null;
+  internalIp: string | null;
+  cpu: string | null;
+  memory: string | null;
+  pods: string | null;
+  created: number | null;
+  taints: number;
+}
+
+export interface NodeDetails {
+  node: NodeRow;
+  conditions: ConditionView[];
+  taints: string[];
+  labels: Record<string, string>;
+  capacity: Record<string, string>;
+  allocatable: Record<string, string>;
+  addresses: string[];
+  osImage: string | null;
+  kernelVersion: string | null;
+  containerRuntime: string | null;
+  architecture: string | null;
+}
+
+export interface ProjectRow {
+  name: string;
+  displayName: string | null;
+  phase: string | null;
+  favorite: boolean;
+  /** Проект из профиля (ручной список или избранное), а не из списка кластера. */
+  fromProfile: boolean;
+}
+
+export interface ProjectList {
+  items: ProjectRow[];
+  truncated: boolean;
+  /** Почему кластер не дал список (нет прав): тогда в списке только проекты из профиля. */
+  error?: AppError;
+}
+
+export interface Verbs {
+  get: boolean;
+  list: boolean;
+  watch: boolean;
+}
+
+/** Права в проекте: `ресурс.группа` (`pods`, `deployments.apps`, `pods/log`) → права. */
+export interface NamespaceAccess {
+  resources: Record<string, Verbs>;
+  /** Сервер перечислил не все права — интерфейс ничего не скрывает. */
+  incomplete: boolean;
+}
+
+export type WorkloadKind = "Deployment" | "StatefulSet" | "DaemonSet" | "DeploymentConfig" | "Job" | "CronJob";
+export type WorkloadProblem = "unavailable" | "rolloutStuck" | "failed";
+
+export interface WorkloadRow {
+  kind: WorkloadKind;
+  name: string;
+  desired: number | null;
+  ready: number | null;
+  available: number | null;
+  updated: number | null;
+  images: string[];
+  created: number | null;
+  problem: WorkloadProblem | null;
+  schedule?: string | null;
+  suspended?: boolean | null;
+  lastSchedule?: number | null;
+}
+
+export interface KindError {
+  kind: WorkloadKind;
+  error: AppError;
+}
+
+export interface WorkloadList {
+  items: WorkloadRow[];
+  errors: KindError[];
+  truncated: boolean;
+}
+
+export type EnvSource = "literal" | "configMapKeyRef" | "secretKeyRef" | "fieldRef" | "resourceFieldRef" | "configMapRef" | "secretRef";
+
+/** Переменная окружения: имя и источник; значения нет. */
+export interface EnvView {
+  name: string;
+  source: EnvSource;
+  reference: string | null;
+}
+
+export interface ContainerSpecView {
+  name: string;
+  image: string;
+  init: boolean;
+  requests: Record<string, string>;
+  limits: Record<string, string>;
+  ports: string[];
+  liveness: string | null;
+  readiness: string | null;
+  startup: string | null;
+  env: EnvView[];
+}
+
+export interface HpaView {
+  name: string;
+  minReplicas: number | null;
+  maxReplicas: number;
+  currentReplicas: number | null;
+  desiredReplicas: number;
+  metrics: string[];
+  conditions: ConditionView[];
+}
+
+export interface PdbView {
+  name: string;
+  minAvailable: string | null;
+  maxUnavailable: string | null;
+  allowedDisruptions: number;
+  currentHealthy: number;
+  desiredHealthy: number;
+  expectedPods: number;
+}
+
+export interface Revision {
+  revision: number;
+  name: string;
+  images: string[];
+  created: number | null;
+  replicas: number | null;
+  current: boolean;
+}
+
+export interface WorkloadDetails {
+  workload: WorkloadRow;
+  conditions: ConditionView[];
+  selector: string | null;
+  strategy: string | null;
+  containers: ContainerSpecView[];
+  labels: Record<string, string>;
+  annotations: Record<string, string>;
+  activeJobs: string[];
+  hpa: Part<HpaView | null>;
+  pdb: Part<PdbView | null>;
+  /** null — у Job и CronJob истории нет. */
+  history: Part<Revision[]> | null;
+}
+
+export type PodProblem = "imagePull" | "oomKilled" | "crashLoop" | "pending" | "failed";
+
+export interface OwnerRef {
+  kind: string;
+  name: string;
+}
+
+export interface PodRow {
+  name: string;
+  phase: string;
+  /** Статус, как в `kubectl get pods`. */
+  status: string;
+  problem: PodProblem | null;
+  ready: number;
+  containers: number;
+  restarts: number;
+  lastRestart: number | null;
+  node: string | null;
+  created: number | null;
+  qos: string | null;
+  owner: OwnerRef | null;
+}
+
+export interface StateView {
+  state: "running" | "waiting" | "terminated";
+  reason: string | null;
+  message: string | null;
+  exitCode: number | null;
+  started: number | null;
+  finished: number | null;
+}
+
+export interface ContainerStatusView extends ContainerSpecView {
+  imageId: string | null;
+  state: StateView | null;
+  lastState: StateView | null;
+  ready: boolean;
+  restarts: number;
+}
+
+export interface VolumeView {
+  name: string;
+  kind: string;
+  source: string | null;
+}
+
+export interface PodDetails {
+  pod: PodRow;
+  conditions: ConditionView[];
+  podIp: string | null;
+  hostIp: string | null;
+  serviceAccount: string | null;
+  started: number | null;
+  containers: ContainerStatusView[];
+  labels: Record<string, string>;
+  annotations: Record<string, string>;
+  volumes: VolumeView[];
+}
+
+export interface WorkloadRef {
+  kind: WorkloadKind;
+  name: string;
+}
+
+export interface EventRow {
+  key: string;
+  /** `Normal` или `Warning`. */
+  type: string;
+  reason: string;
+  object: OwnerRef;
+  message: string;
+  count: number;
+  firstSeen: number | null;
+  lastSeen: number | null;
+  source: string | null;
+}
+
+/** Чьи события: объекта или workload'а вместе с его подами. */
+export interface EventObject {
+  kind: string;
+  name: string;
+}
+
+export interface EndpointCount {
+  ready: number;
+  total: number;
+}
+
+export interface ServiceRow {
+  name: string;
+  type: string;
+  clusterIp: string | null;
+  external: string[];
+  ports: string[];
+  selector: string | null;
+  endpoints: EndpointCount | null;
+  created: number | null;
+}
+
+export interface RouteRow {
+  name: string;
+  host: string | null;
+  path: string | null;
+  service: string | null;
+  port: string | null;
+  tls: string | null;
+  insecurePolicy: string | null;
+  admitted: boolean | null;
+  admittedMessage: string | null;
+  certExpires: number | null;
+  ingress: string | null;
+  created: number | null;
+}
+
+export interface IngressPath {
+  host: string | null;
+  path: string | null;
+  service: string | null;
+  port: string | null;
+}
+
+export interface IngressTls {
+  hosts: string[];
+  secret: string | null;
+}
+
+export interface IngressRow {
+  name: string;
+  class: string | null;
+  rules: IngressPath[];
+  defaultBackend: string | null;
+  tls: IngressTls[];
+  addresses: string[];
+  routes: string[];
+  created: number | null;
+}
+
+export interface ListenerView {
+  name: string;
+  port: number | null;
+  protocol: string | null;
+  hostname: string | null;
+}
+
+export interface GatewayRow {
+  name: string;
+  class: string | null;
+  listeners: ListenerView[];
+  addresses: string[];
+  accepted: boolean | null;
+  programmed: boolean | null;
+  created: number | null;
+}
+
+export interface ParentView {
+  name: string;
+  namespace: string | null;
+  section: string | null;
+  accepted: ConditionView | null;
+  resolvedRefs: ConditionView | null;
+}
+
+export interface GatewayRouteRow {
+  kind: "HTTPRoute" | "GRPCRoute";
+  name: string;
+  hostnames: string[];
+  parents: ParentView[];
+  backends: string[];
+  created: number | null;
+}
+
+/** Сеть проекта; null — вида нет в кластере (нет группы API). */
+export interface NetworkView {
+  services: Part<ServiceRow[]>;
+  endpointsError: AppError | null;
+  routes: Part<RouteRow[]> | null;
+  ingresses: Part<IngressRow[]>;
+  gateways: Part<GatewayRow[]> | null;
+  httpRoutes: Part<GatewayRouteRow[]> | null;
+  grpcRoutes: Part<GatewayRouteRow[]> | null;
+}
+
+export interface ConfigMapRow {
+  name: string;
+  keys: string[];
+  binaryKeys: string[];
+  created: number | null;
+}
+
+/** Secret: имя, тип и ключи — значения в интерфейс не приходят. */
+export interface SecretRow {
+  name: string;
+  type: string;
+  keys: string[];
+  created: number | null;
+}
+
+export interface ConfigView {
+  configMaps: Part<ConfigMapRow[]>;
+  secrets: Part<SecretRow[]>;
+}
+
+export interface ConfigValue {
+  text: string | null;
+  binarySize: number | null;
+}
+
+export interface PvcRow {
+  name: string;
+  phase: string | null;
+  class: string | null;
+  capacity: string | null;
+  requested: string | null;
+  accessModes: string[];
+  volumeMode: string | null;
+  volume: string | null;
+  /** null — поды не читаются, неизвестно. */
+  mountedBy: string[] | null;
+  created: number | null;
+}
+
+export interface QuotaEntry {
+  resource: string;
+  used: string | null;
+  hard: string;
+  ratio: number | null;
+}
+
+export interface QuotaView {
+  name: string;
+  scopes: string[];
+  entries: QuotaEntry[];
+}
+
+export interface LimitView {
+  type: string;
+  resource: string;
+  min: string | null;
+  max: string | null;
+  default: string | null;
+  defaultRequest: string | null;
+  maxLimitRequestRatio: string | null;
+}
+
+export interface LimitRangeView {
+  name: string;
+  limits: LimitView[];
+}
+
+export interface QuotasView {
+  quotas: Part<QuotaView[]>;
+  limitRanges: Part<LimitRangeView[]>;
+}
+
+/** Виды объектов с вкладкой YAML. */
+export type YamlKind =
+  | WorkloadKind
+  | "Pod"
+  | "Service"
+  | "Route"
+  | "Ingress"
+  | "ConfigMap"
+  | "PersistentVolumeClaim"
+  | "Gateway"
+  | "HTTPRoute"
+  | "GRPCRoute";
+
+export type WatchKind = "pods" | "workloads" | "events";
+
+export interface WatchParams {
+  connectionId: string;
+  namespace: string;
+  kind: WatchKind;
+  workload?: WorkloadRef | null;
+}
+
+export type WatchRow = PodRow | WorkloadRow | EventRow;
+export type RowChange = { op: "upsert"; row: WatchRow } | { op: "delete"; key: string };
+
+export type WatchEvent =
+  | { type: "reset"; items: WatchRow[]; errors: KindError[] }
+  | { type: "changes"; changes: RowChange[] }
+  | { type: "polling" }
+  | { type: "error"; error: AppError; fatal: boolean };
+
+export interface LogFilter {
+  text: string;
+  regex: boolean;
+  caseSensitive: boolean;
+}
+
+export interface LogParams {
+  connectionId: string;
+  namespace: string;
+  pod: string;
+  container?: string | null;
+  previous?: boolean;
+  tailLines?: number | null;
+  sinceSeconds?: number | null;
+  sinceTime?: string | null;
+  follow?: boolean;
+  filter?: LogFilter | null;
+}
+
+export interface LogLine {
+  ts: string | null;
+  text: string;
+  truncated: boolean;
+}
+
+export type LogEvent =
+  | { type: "lines"; lines: LogLine[]; scanned: number; shown: number }
+  | { type: "reconnecting"; since: string | null }
+  | { type: "end"; scanned: number; shown: number }
+  | { type: "error"; error: AppError };
diff --git a/src/components/cluster/ConnectionPanel.tsx b/src/components/cluster/ConnectionPanel.tsx
index 7de96bc..a1794ea 100644
--- a/src/components/cluster/ConnectionPanel.tsx
+++ b/src/components/cluster/ConnectionPanel.tsx
@@ -2,14 +2,12 @@ import { useIsMutating, useQuery, useQueryClient } from "@tanstack/react-query";
 import { useTranslation } from "react-i18next";
 import { api, toAppError } from "../../api/client";
 import type { ApiGroups, AppError, Capabilities, ConnectionInfo, ConnectionProfile } from "../../api/types";
+import { needsSignIn } from "../../lib/signIn";
 import { useWorkspace } from "../../state/workspace";
 import { ErrorBanner } from "../common/ErrorBanner";
 import { Hint } from "../common/Hint";
 import { LoginWait, reconnect, SessionActions, sessionMutationKey } from "./SessionActions";
 
-/** Ошибки, которые лечатся входом заново или новым токеном. */
-const SIGN_IN_KEYS = new Set(["errors.connections.tokenMissing", "errors.connections.loginRequired", "errors.connections.clientKeyMissing"]);
-
 const CAPABILITIES: (keyof Capabilities)[] = ["listProjects", "listNodes", "listAllPods", "readClusterOperators", "readClusterVersion"];
 /** Права, которые бывают только у OpenShift. */
 const OPENSHIFT_ONLY: (keyof Capabilities)[] = ["readClusterOperators", "readClusterVersion"];
@@ -22,12 +20,6 @@ const API_GROUPS: [keyof ApiGroups, string][] = [
   ["gatewayApi", "Gateway API"],
 ];
 
-/** Помогает ли войти заново: истёкшая сессия, нет токена или ключа, ошибка exec-плагина. */
-function needsSignIn(e: AppError, profile: ConnectionProfile): boolean {
-  if (e.code === "SESSION_EXPIRED" || SIGN_IN_KEYS.has(e.key ?? "")) return true;
-  return profile.auth.method === "exec" && (e.key?.startsWith("errors.exec.") || e.key === "errors.oauth.cancelled");
-}
-
 /** Вкладка подключения: подключается при открытии и показывает, кто вошёл, что за кластер и какие есть права. */
 export function ConnectionPanel({ connectionId }: { connectionId: string }) {
   const { t } = useTranslation();
diff --git a/src/components/cluster/SessionActions.tsx b/src/components/cluster/SessionActions.tsx
index d13d128..a7e8670 100644
--- a/src/components/cluster/SessionActions.tsx
+++ b/src/components/cluster/SessionActions.tsx
@@ -4,6 +4,7 @@ import { useState } from "react";
 import { useTranslation } from "react-i18next";
 import { api, toAppError } from "../../api/client";
 import type { ConnectionInfo, ConnectionProfile } from "../../api/types";
+import { afterSignIn } from "../../lib/afterSignIn";
 import { useWorkspace } from "../../state/workspace";
 import { ErrorBanner } from "../common/ErrorBanner";
 
@@ -32,9 +33,13 @@ export const sessionMutationKey = (connectionId: string) => ["session", connecti
 /**
  * Подключение заново (свежие сведения и права). Идёт через запрос вкладки: тот помечается как загружаемый,
  * и панель не запускает рядом второй exec-плагин (бэкенд ставит подключения одного id в очередь).
+ * После него открытые вкладки подключения перечитываются (`afterSignIn`).
  */
-export const reconnect = (queryClient: QueryClient, connectionId: string) =>
-  queryClient.fetchQuery({ queryKey: ["connection", connectionId], queryFn: () => api.connect(connectionId, true) });
+export async function reconnect(queryClient: QueryClient, connectionId: string): Promise<ConnectionInfo> {
+  const info = await queryClient.fetchQuery({ queryKey: ["connection", connectionId], queryFn: () => api.connect(connectionId, true) });
+  afterSignIn(queryClient, connectionId);
+  return info;
+}
 
 /**
  * Что сделать, если сессия истекла или токена нет: войти через браузер, вставить токен
@@ -47,7 +52,10 @@ export function SessionActions({ profile }: { profile: ConnectionProfile }) {
   const open = useWorkspace((s) => s.open);
   const [token, setToken] = useState("");
   const id = profile.id;
-  const done = (info: ConnectionInfo) => queryClient.setQueryData(["connection", id], info);
+  const done = (info: ConnectionInfo) => {
+    queryClient.setQueryData(["connection", id], info);
+    afterSignIn(queryClient, id);
+  };
   // Подключение этой вкладки (в том числе «Подключиться заново» на панели) тоже может запускать exec-плагин.
   const connecting = useIsFetching({ queryKey: ["connection", id], exact: true }) > 0;
   const mutationKey = sessionMutationKey(id);
diff --git a/src/main.tsx b/src/main.tsx
index b250a45..67c6f14 100644
--- a/src/main.tsx
+++ b/src/main.tsx
@@ -4,6 +4,7 @@ import React from "react";
 import ReactDOM from "react-dom/client";
 import App from "./App";
 import "./styles.css";
+import "./app.css";
 import { installPlainInputs } from "./lib/plainInputs";
 
 installPlainInputs();
````

<!-- block: locales t12-locales step=12.3 -->
````json
{
  "en": {
    "common": {
      "refresh": "Refresh",
      "autoRefresh": "Auto-refresh",
      "autoRefreshOff": "off",
      "everySeconds": "every {{n}} s",
      "copy": "Copy",
      "copied": "Copied",
      "empty": "Nothing here",
      "truncated": "The list is incomplete: only the first 10,000 objects are shown",
      "age": {
        "seconds": "{{n}}s",
        "minutes": "{{n}}m",
        "hours": "{{n}}h",
        "days": "{{n}}d"
      }
    },
    "objects": {
      "signInAgain": "Sign in again",
      "gone": "The object has been deleted or renamed — close the tab",
      "conditions": {
        "type": "Condition",
        "status": "Status",
        "reason": "Reason",
        "message": "Message",
        "since": "Since"
      }
    }
  },
  "ru": {
    "common": {
      "refresh": "Обновить",
      "autoRefresh": "Автообновление",
      "autoRefreshOff": "выкл.",
      "everySeconds": "каждые {{n}} с",
      "copy": "Копировать",
      "copied": "Скопировано",
      "empty": "Пусто",
      "truncated": "Список неполный: показаны первые 10 000 объектов",
      "age": {
        "seconds": "{{n}} с",
        "minutes": "{{n}} мин",
        "hours": "{{n}} ч",
        "days": "{{n}} д"
      }
    },
    "objects": {
      "signInAgain": "Войти снова",
      "gone": "Объект удалён или переименован — закройте вкладку",
      "conditions": {
        "type": "Условие",
        "status": "Статус",
        "reason": "Причина",
        "message": "Сообщение",
        "since": "С момента"
      }
    }
  }
}
````

<!-- block: shell t12-install step=12.3 -->
````bash
pnpm install
````

- [ ] **Step 4: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 38 passed`, `Tests 230 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 136`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 5: Commit.**

```bash
git add \
  package.json \
  pnpm-lock.yaml \
  src/api/client.ts \
  src/api/types.ts \
  src/app.css \
  src/components/cluster/ConnectionPanel.test.tsx \
  src/components/cluster/ConnectionPanel.tsx \
  src/components/cluster/SessionActions.tsx \
  src/components/common/Conditions.tsx \
  src/components/common/KeyValues.tsx \
  src/components/common/PartView.test.tsx \
  src/components/common/PartView.tsx \
  src/components/common/ReadError.tsx \
  src/components/common/RefreshControls.tsx \
  src/components/common/SubTabs.tsx \
  src/components/common/VirtualTable.test.tsx \
  src/components/common/VirtualTable.tsx \
  src/components/common/YamlView.test.tsx \
  src/components/common/YamlView.tsx \
  src/lib/afterSignIn.test.ts \
  src/lib/afterSignIn.ts \
  src/lib/age.test.ts \
  src/lib/age.ts \
  src/lib/signIn.ts \
  src/lib/useWatchList.test.tsx \
  src/lib/useWatchList.ts \
  src/locales/en.json \
  src/locales/ru.json \
  src/main.tsx \
  src/state/connectionEpoch.ts
git commit -s -m "feat(ui): основа чтения кластера — типы и API-клиент, таблица с виртуализацией, части экрана, YAML, watch-хук, перечитывание после входа" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 13: Обзор кластера на вкладке подключения и вкладка нод

**Files:**
- Create: `src/components/cluster/ClusterOverview.test.tsx`, `src/components/cluster/ClusterOverview.tsx`, `src/components/cluster/NodesPanel.test.tsx`, `src/components/cluster/NodesPanel.tsx`
- Modify: `docs/i18n/same-as-en.json`, `src/components/Layout.tsx`, `src/components/TabContent.tsx`, `src/components/cluster/ConnectionPanel.test.tsx`, `src/components/cluster/ConnectionPanel.tsx`, `src/components/help/helpChapterFor.ts`, `src/locales/en.json`, `src/locales/ru.json`, `src/state/workspace.ts`

**Interfaces:**
- Consumes: задача 12; команды задачи 4.
- Produces: `components/cluster/ClusterOverviewView({ connectionId })`, `components/cluster/NodesPanel({ connectionId })`; `TabSpec` `{ kind: "nodes", connectionId }` (id `nodes:<id>`, заголовок «<подключение> · Ноды», справка — «Подключения»); на вкладке подключения — срок токена с предупреждением за час и кнопка «Ноды» при праве на ноды.

Спец. §6.1–6.2, §5.2. Под сведениями о подключении — «Обзор кластера»: готов ли API-сервер (и непройденные проверки `/readyz`), готовые ноды и по ролям, число проектов, классы Ingress (по умолчанию) и Gateway; у OpenShift — версия, канал и обновления, `Failing` и `Progressing`, инфраструктура, ссылка на веб-консоль, операторы не в норме (кнопка «Показать все» читает `list_cluster_operators`) и пулы машин. Части без прав — «—» с причиной и пояснение «Остальное скрыто правами.». Если токен истекает меньше чем через час — предупреждение (спец. §5.2). Вкладка «Ноды»: таблица (готовность и проблемы, роли, kubelet, CPU, память, возраст), щелчок — условия, taints, адреса, ёмкость и доступное подам, метки.

- [ ] **Step 1: Тесты.**

<!-- block: file t13-src-components-cluster-clusteroverview-test-tsx step=13.1 path=src/components/cluster/ClusterOverview.test.tsx -->
````tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getClusterOverview: vi.fn(), listClusterOperators: vi.fn() } };
});

import { openUrl } from "@tauri-apps/plugin-opener";
import { api } from "../../api/client";
import type { AppError, ClusterOverview, OperatorRow } from "../../api/types";
import { ClusterOverviewView } from "./ClusterOverview";

const forbidden = (resource: string): AppError => ({
  code: "FORBIDDEN",
  key: "errors.k8s.forbiddenCluster",
  params: { verb: "list", resource },
  message: "x",
  retryable: false,
});

const op = (name: string, degraded: boolean, message: string | null = null): OperatorRow => ({
  name,
  version: "4.18.3",
  available: true,
  progressing: false,
  degraded,
  message,
});

const OPENSHIFT: ClusterOverview = {
  platform: { kind: "openshift", distribution: null, version: "4.18", label: "OpenShift 4.18" },
  serverVersion: "v1.31.6",
  readyz: { ok: false, failed: ["etcd"] },
  nodes: { data: { total: 6, ready: 5, roles: [{ role: "master", total: 3, ready: 3 }, { role: "worker", total: 3, ready: 2 }] } },
  ingressClasses: { data: [{ name: "openshift-default", controller: "openshift.io/ingress-to-route", default: true }] },
  gatewayClasses: null,
  projects: { data: { count: 500, more: true } },
  platformDetails: {
    kind: "openshift",
    clusterVersion: {
      data: {
        version: "4.18.3",
        channel: "stable-4.18",
        availableUpdates: 2,
        failing: { type: "Failing", status: "False" },
        progressing: { type: "Progressing", status: "True", message: "Working towards 4.18.4" },
      },
    },
    infrastructure: "AWS",
    consoleUrl: "https://console-openshift-console.apps.c.example.com",
    operators: { data: { total: 32, problems: [op("ingress", true, "Some ingresscontrollers are degraded")] } },
    machineConfigPools: {
      data: [{ name: "worker", degraded: false, updating: true, machines: 3, ready: 2, updated: 1, degradedMachines: 0 }],
    },
  },
};

beforeEach(() => vi.clearAllMocks());

describe("ClusterOverviewView", () => {
  it("OpenShift: API-сервер, ноды по ролям, обновления, консоль, операторы и пулы машин", async () => {
    vi.mocked(api.getClusterOverview).mockResolvedValue(OPENSHIFT);
    vi.mocked(api.listClusterOperators).mockResolvedValue([op("dns", false), op("ingress", true)]);
    const user = userEvent.setup();
    renderWithClient(<ClusterOverviewView connectionId="c1" />);
    expect(await screen.findByText("⚠ не готов")).toBeInTheDocument();
    expect(screen.getByText("etcd")).toBeInTheDocument();
    expect(screen.getByText("5 / 6")).toBeInTheDocument();
    expect(screen.getByText("500+")).toBeInTheDocument();
    expect(screen.getByText("По ролям: master 3/3, worker 2/3")).toBeInTheDocument();
    expect(screen.getByText("по умолчанию")).toBeInTheDocument();
    expect(screen.getByText("stable-4.18")).toBeInTheDocument();
    expect(screen.getByText("Working towards 4.18.4")).toBeInTheDocument();
    expect(screen.getByText("AWS")).toBeInTheDocument();
    expect(screen.getByText("Some ingresscontrollers are degraded")).toBeInTheDocument();
    expect(screen.getByText(/Не в норме: 1 из 32/)).toBeInTheDocument();
    expect(screen.getByText(/1\/3 машин обновлено · обновляется/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "https://console-openshift-console.apps.c.example.com" }));
    expect(openUrl).toHaveBeenCalledWith("https://console-openshift-console.apps.c.example.com");
    await user.click(screen.getByRole("button", { name: "Показать все" }));
    expect(await screen.findByText("dns")).toBeInTheDocument();
    expect(api.listClusterOperators).toHaveBeenCalledWith("c1");
    expect(screen.queryByText("Остальное скрыто правами.")).not.toBeInTheDocument();
  });

  it("без прав — «—» с причиной и пояснение; Gateway API — классы Gateway", async () => {
    vi.mocked(api.getClusterOverview).mockResolvedValue({
      ...OPENSHIFT,
      platform: { kind: "kubernetes", distribution: null, version: "1.32", label: "Kubernetes 1.32" },
      readyz: { ok: true, failed: [] },
      nodes: { error: forbidden("nodes") },
      projects: { error: forbidden("namespaces") },
      gatewayClasses: { data: [{ name: "cilium", controller: "io.cilium/gateway-controller", accepted: true }] },
      platformDetails: null,
    });
    renderWithClient(<ClusterOverviewView connectionId="c1" />);
    expect(await screen.findByText("✅ готов")).toBeInTheDocument();
    expect(screen.getByText("— Нет прав: list nodes на уровне кластера")).toBeInTheDocument();
    expect(screen.getByText("cilium")).toBeInTheDocument();
    expect(screen.getByText("Остальное скрыто правами.")).toBeInTheDocument();
    expect(screen.queryByText("OpenShift")).not.toBeInTheDocument();
  });
});
````

<!-- block: file t13-src-components-cluster-nodespanel-test-tsx step=13.1 path=src/components/cluster/NodesPanel.test.tsx -->
````tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 28 })),
    getTotalSize: () => o.count * 28,
  }),
}));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { listNodes: vi.fn(), getNode: vi.fn() } };
});

import { api } from "../../api/client";
import type { NodeRow } from "../../api/types";
import { NodesPanel } from "./NodesPanel";

const node = (name: string, ready: boolean, problems: string[] = []): NodeRow => ({
  name,
  roles: ["worker"],
  ready,
  problems,
  unschedulable: false,
  kubeletVersion: "v1.32.5",
  internalIp: "10.0.0.1",
  cpu: "3500m",
  memory: "15Gi",
  pods: "250",
  created: 1,
  taints: 0,
});

beforeEach(() => vi.clearAllMocks());

describe("NodesPanel", () => {
  it("список нод и сведения о выбранной", async () => {
    vi.mocked(api.listNodes).mockResolvedValue({ items: [node("w1", true), node("w2", false, ["NotReady", "DiskPressure"])], truncated: false });
    vi.mocked(api.getNode).mockResolvedValue({
      node: node("w2", false),
      conditions: [{ type: "DiskPressure", status: "True", reason: "KubeletHasDiskPressure" }],
      taints: ["node.kubernetes.io/disk-pressure:NoSchedule"],
      labels: { "kubernetes.io/os": "linux" },
      capacity: { cpu: "4", memory: "16Gi" },
      allocatable: { cpu: "3500m", memory: "15Gi" },
      addresses: ["InternalIP: 10.0.0.2"],
      osImage: "RHCOS",
      kernelVersion: "5.14",
      containerRuntime: "cri-o://1.32",
      architecture: "amd64",
    });
    const user = userEvent.setup();
    renderWithClient(<NodesPanel connectionId="c1" />);
    expect(await screen.findByText("w1")).toBeInTheDocument();
    expect(screen.getByText("NotReady, DiskPressure")).toBeInTheDocument();
    await user.click(screen.getByText("w2"));
    expect(await screen.findByText("KubeletHasDiskPressure")).toBeInTheDocument();
    expect(api.getNode).toHaveBeenCalledWith("c1", "w2");
    expect(screen.getByText("node.kubernetes.io/disk-pressure:NoSchedule")).toBeInTheDocument();
    expect(screen.getByText("cri-o://1.32")).toBeInTheDocument();
    expect(screen.getByText("16Gi")).toBeInTheDocument();
  });
});
````

<!-- block: patch t13-tests step=13.1 -->
````diff
diff --git a/src/components/cluster/ConnectionPanel.test.tsx b/src/components/cluster/ConnectionPanel.test.tsx
index 6f65baf..e63fe4c 100644
--- a/src/components/cluster/ConnectionPanel.test.tsx
+++ b/src/components/cluster/ConnectionPanel.test.tsx
@@ -16,13 +16,14 @@ vi.mock("../../api/client", async (importOriginal) => {
       cancelOauthLogin: vi.fn(),
       setConnectionToken: vi.fn(),
       oauthTokenPage: vi.fn(),
+      getClusterOverview: vi.fn(),
     },
   };
 });
 
 import { openUrl } from "@tauri-apps/plugin-opener";
 import { api } from "../../api/client";
-import type { ConnectionInfo, ConnectionProfile } from "../../api/types";
+import type { ClusterOverview, ConnectionInfo, ConnectionProfile } from "../../api/types";
 import { useWorkspace } from "../../state/workspace";
 import { newProfile } from "../connections/profile";
 import { ConnectionPanel } from "./ConnectionPanel";
@@ -59,8 +60,21 @@ const EXEC: ConnectionProfile["auth"] = {
 
 const EXEC_FAILED = { code: "AUTH_FAILED", key: "errors.exec.failed", params: { command: "kubelogin", code: "1" }, message: "x", retryable: false };
 
+/** Обзор кластера под сведениями о подключении; его проверяет ClusterOverview.test.tsx. */
+const OVERVIEW: ClusterOverview = {
+  platform: INFO.platform,
+  serverVersion: "v1.32.5",
+  readyz: { ok: true, failed: [] },
+  nodes: { data: { total: 3, ready: 3, roles: [] } },
+  ingressClasses: { data: [] },
+  gatewayClasses: null,
+  projects: { data: { count: 2, more: false } },
+  platformDetails: null,
+};
+
 beforeEach(() => {
   vi.clearAllMocks();
+  vi.mocked(api.getClusterOverview).mockResolvedValue(OVERVIEW);
   vi.mocked(api.cancelOauthLogin).mockResolvedValue(true);
   useWorkspace.setState({ tabs: [], activeId: null });
 });
@@ -81,6 +95,23 @@ describe("ConnectionPanel", () => {
     expect(screen.getByText("2 группы")).toBeInTheDocument();
   });
 
+  it("срок токена и предупреждение за час, кнопка нод при правах на ноды", async () => {
+    const soon = Math.floor(Date.now() / 1000) + 600;
+    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "browser" })]);
+    vi.mocked(api.connect).mockResolvedValue({
+      ...INFO,
+      tokenExpiresAt: soon,
+      capabilities: { ...INFO.capabilities, listNodes: true },
+    });
+    const user = userEvent.setup();
+    renderWithClient(<ConnectionPanel connectionId="c1" />);
+    expect(await screen.findByText("Токен истекает меньше чем через час — скоро нужно будет войти снова")).toBeInTheDocument();
+    expect(screen.getByText("Токен действует до")).toBeInTheDocument();
+    expect(await screen.findByText("Обзор кластера")).toBeInTheDocument();
+    await user.click(screen.getByRole("button", { name: "Ноды" }));
+    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "nodes", connectionId: "c1" });
+  });
+
   it("Kubernetes: платформа с дистрибутивом, Gateway API, без прав OpenShift", async () => {
     vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "token" })]);
     vi.mocked(api.connect).mockResolvedValue({
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/cluster/ClusterOverview.test.tsx src/components/cluster/ConnectionPanel.test.tsx src/components/cluster/NodesPanel.test.tsx`  
Expected: FAIL — нет компонентов `ClusterOverview` и `NodesPanel`; на вкладке подключения нет срока токена и кнопки «Ноды».

- [ ] **Step 3: Реализация.**

<!-- block: file t13-src-components-cluster-clusteroverview-tsx step=13.3 path=src/components/cluster/ClusterOverview.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { openUrl } from "@tauri-apps/plugin-opener";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { ClusterOverview, OpenShiftOverview, OperatorRow, Part } from "../../api/types";
import { tr } from "../../i18n/text";
import { Hint } from "../common/Hint";
import { PartView } from "../common/PartView";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";

/** Какие части обзора скрыты правами (у них ошибка FORBIDDEN). */
function hiddenByRights(o: ClusterOverview): boolean {
  const parts: (Part<unknown> | null | undefined)[] = [o.nodes, o.ingressClasses, o.gatewayClasses, o.projects];
  const os = o.platformDetails;
  if (os) parts.push(os.clusterVersion, os.operators, os.machineConfigPools);
  return parts.some((p) => p?.error?.code === "FORBIDDEN");
}

const flag = (v: boolean | null) => (v === true ? "✅" : v === false ? "⚠" : "—");

/** Обзор кластера (спец. §6.2): API-сервер, ноды, проекты, классы Ingress и Gateway, у OpenShift — обновления, операторы, пулы машин. */
export function ClusterOverviewView({ connectionId }: { connectionId: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const q = useQuery({
    queryKey: ["overview", connectionId],
    queryFn: () => api.getClusterOverview(connectionId),
    refetchInterval: interval || false,
  });
  const o = q.data;
  return (
    <section className="section">
      <div className="toolbar">
        <h3>{t("cluster.overview.title")}</h3>
        <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      </div>
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {o && (
        <>
          <dl className="stats">
            <div>
              <dt>
                <span className="field-label">
                  {t("cluster.overview.apiServer")}
                  <Hint k="hints.cluster.readyz" label={t("cluster.overview.apiServer")} />
                </span>
              </dt>
              <dd>{o.readyz.ok ? `✅ ${t("cluster.overview.ready")}` : `⚠ ${t("cluster.overview.notReady")}`}</dd>
            </div>
            <div>
              <dt>{t("cluster.overview.nodes")}</dt>
              <dd>
                <PartView part={o.nodes}>{(n) => `${n.ready} / ${n.total}`}</PartView>
              </dd>
            </div>
            <div>
              <dt>{t("cluster.overview.projects")}</dt>
              <dd>
                <PartView part={o.projects}>{(p) => `${p.count}${p.more ? "+" : ""}`}</PartView>
              </dd>
            </div>
          </dl>
          {!o.readyz.ok && o.readyz.failed.length > 0 && (
            <p className="warn">
              {t("cluster.overview.failedChecks")}: <code>{o.readyz.failed.join(", ")}</code>
            </p>
          )}
          {o.readyz.error && <p className="muted">— {tr(o.readyz.error)}</p>}
          {o.nodes.data && o.nodes.data.roles.length > 0 && (
            <p>
              {t("cluster.overview.byRole")}: {o.nodes.data.roles.map((r) => `${r.role} ${r.ready}/${r.total}`).join(", ")}
            </p>
          )}
          <h4>{t("cluster.overview.ingressClasses")}</h4>
          <PartView part={o.ingressClasses}>
            {(list) =>
              list.length === 0 ? (
                <p className="muted">—</p>
              ) : (
                <ul>
                  {list.map((c) => (
                    <li key={c.name}>
                      <code>{c.name}</code> {c.default && <strong>{t("cluster.overview.defaultClass")}</strong>}{" "}
                      <span className="muted">{c.controller ?? ""}</span>
                    </li>
                  ))}
                </ul>
              )
            }
          </PartView>
          {o.gatewayClasses && (
            <>
              <h4>{t("cluster.overview.gatewayClasses")}</h4>
              <PartView part={o.gatewayClasses}>
                {(list) =>
                  list.length === 0 ? (
                    <p className="muted">—</p>
                  ) : (
                    <ul>
                      {list.map((c) => (
                        <li key={c.name}>
                          {flag(c.accepted)} <code>{c.name}</code> <span className="muted">{c.controller ?? ""}</span>
                        </li>
                      ))}
                    </ul>
                  )
                }
              </PartView>
            </>
          )}
          {o.platformDetails && <OpenShiftSection connectionId={connectionId} os={o.platformDetails} />}
          {hiddenByRights(o) && <p className="muted">{t("cluster.overview.hiddenByRights")}</p>}
        </>
      )}
    </section>
  );
}

function OperatorList({ rows }: { rows: OperatorRow[] }) {
  const { t } = useTranslation();
  return (
    <table>
      <thead>
        <tr>
          <th>{t("cluster.operators.name")}</th>
          <th>{t("cluster.operators.available")}</th>
          <th>{t("cluster.operators.progressing")}</th>
          <th>{t("cluster.operators.degraded")}</th>
          <th>{t("cluster.operators.message")}</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.name}>
            <td>
              <code>{r.name}</code> <span className="muted">{r.version ?? ""}</span>
            </td>
            <td>{flag(r.available)}</td>
            <td>{r.progressing ? "⏳" : "—"}</td>
            <td className={r.degraded ? "problem" : undefined}>{r.degraded ? "⚠" : "—"}</td>
            <td>{r.message ?? ""}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** Часть обзора OpenShift: версия и обновления, инфраструктура, консоль, операторы и пулы машин не в норме. */
function OpenShiftSection({ connectionId, os }: { connectionId: string; os: OpenShiftOverview }) {
  const { t } = useTranslation();
  const [all, setAll] = useState(false);
  const operators = useQuery({ queryKey: ["clusterOperators", connectionId], queryFn: () => api.listClusterOperators(connectionId), enabled: all });
  return (
    <>
      <h4>OpenShift</h4>
      <PartView part={os.clusterVersion}>
        {(cv) => (
          <dl className="facts">
            <dt>{t("cluster.overview.version")}</dt>
            <dd>{cv.version ?? "—"}</dd>
            <dt>{t("cluster.overview.channel")}</dt>
            <dd>{cv.channel ?? "—"}</dd>
            <dt>{t("cluster.overview.updates")}</dt>
            <dd>{cv.availableUpdates}</dd>
            {cv.failing?.status === "True" && (
              <>
                <dt className="problem">{t("cluster.overview.failing")}</dt>
                <dd>{cv.failing.message ?? cv.failing.reason ?? ""}</dd>
              </>
            )}
            {cv.progressing?.status === "True" && (
              <>
                <dt>{t("cluster.overview.progressing")}</dt>
                <dd>{cv.progressing.message ?? ""}</dd>
              </>
            )}
          </dl>
        )}
      </PartView>
      <dl className="facts">
        <dt>{t("cluster.overview.infrastructure")}</dt>
        <dd>{os.infrastructure ?? "—"}</dd>
        <dt>{t("cluster.overview.console")}</dt>
        <dd>
          {os.consoleUrl ? (
            <button type="button" className="link" onClick={() => void openUrl(os.consoleUrl as string)}>
              {os.consoleUrl}
            </button>
          ) : (
            "—"
          )}
        </dd>
      </dl>
      <h4>{t("cluster.overview.operators")}</h4>
      <PartView part={os.operators}>
        {(ops) => (
          <>
            <p>
              {ops.problems.length === 0
                ? t("cluster.overview.operatorsOk", { total: ops.total })
                : t("cluster.overview.operatorsProblems", { bad: ops.problems.length, total: ops.total })}{" "}
              <button type="button" className="link" onClick={() => setAll((v) => !v)}>
                {all ? t("cluster.overview.operatorsProblemsOnly") : t("cluster.overview.operatorsAll")}
              </button>
            </p>
            {all ? (
              operators.data ? <OperatorList rows={operators.data} /> : operators.error ? <ReadError error={operators.error} connectionId={connectionId} /> : null
            ) : (
              ops.problems.length > 0 && <OperatorList rows={ops.problems} />
            )}
          </>
        )}
      </PartView>
      <h4>{t("cluster.overview.machineConfigPools")}</h4>
      <PartView part={os.machineConfigPools}>
        {(pools) => (
          <ul>
            {pools.map((p) => (
              <li key={p.name} className={p.degraded ? "problem" : undefined}>
                <code>{p.name}</code> {p.updated}/{p.machines} {t("cluster.overview.machinesUpdated")}
                {p.updating && ` · ${t("cluster.overview.updating")}`}
                {p.degraded && ` · ${t("cluster.overview.degraded", { count: p.degradedMachines })}`}
              </li>
            ))}
          </ul>
        )}
      </PartView>
    </>
  );
}
````

<!-- block: file t13-src-components-cluster-nodespanel-tsx step=13.3 path=src/components/cluster/NodesPanel.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { NodeRow } from "../../api/types";
import { formatAge } from "../../lib/age";
import { Conditions } from "../common/Conditions";
import { KeyValues } from "../common/KeyValues";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
import { type Column, VirtualTable } from "../common/VirtualTable";

/** Вкладка «Ноды» (спец. §6.1): список с готовностью и ролями, сведения о выбранной ноде. */
export function NodesPanel({ connectionId }: { connectionId: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const [selected, setSelected] = useState<string | null>(null);
  const q = useQuery({ queryKey: ["nodes", connectionId], queryFn: () => api.listNodes(connectionId), refetchInterval: interval || false });
  const columns: Column<NodeRow>[] = [
    { key: "name", title: t("cluster.nodes.name"), width: "minmax(180px, 2fr)", render: (n) => n.name },
    {
      key: "status",
      title: t("cluster.nodes.status"),
      width: "minmax(120px, 1fr)",
      render: (n) => (
        <span className={n.ready && n.problems.length === 0 ? undefined : "problem"}>
          {n.ready ? "Ready" : "NotReady"}
          {n.problems.filter((p) => p !== "NotReady").map((p) => `, ${p}`)}
          {n.unschedulable && ", SchedulingDisabled"}
        </span>
      ),
    },
    { key: "roles", title: t("cluster.nodes.roles"), width: "minmax(120px, 1fr)", render: (n) => n.roles.join(", ") || "—" },
    { key: "version", title: t("cluster.nodes.version"), width: "110px", render: (n) => n.kubeletVersion ?? "—" },
    { key: "cpu", title: "CPU", width: "70px", render: (n) => n.cpu ?? "—" },
    { key: "memory", title: t("cluster.nodes.memory"), width: "90px", render: (n) => n.memory ?? "—" },
    { key: "age", title: t("cluster.nodes.age"), width: "70px", render: (n) => formatAge(n.created) },
  ];
  return (
    <div className="page">
      <div className="toolbar">
        <h2>{t("cluster.nodes.title")}</h2>
        <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      </div>
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data?.truncated && <p className="warn">{t("common.truncated")}</p>}
      {q.data && (
        <div className="split">
          <VirtualTable label={t("cluster.nodes.title")} rows={q.data.items} columns={columns} rowKey={(n) => n.name} onRowClick={(n) => setSelected(n.name)} selectedKey={selected} />
          {selected && <NodeDetailsPane connectionId={connectionId} name={selected} />}
        </div>
      )}
    </div>
  );
}

function NodeDetailsPane({ connectionId, name }: { connectionId: string; name: string }) {
  const { t } = useTranslation();
  const q = useQuery({ queryKey: ["node", connectionId, name], queryFn: () => api.getNode(connectionId, name) });
  const d = q.data;
  return (
    <div className="details-pane section" aria-label={name}>
      <h3>{name}</h3>
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {d && (
        <>
          <dl className="facts">
            <dt>{t("cluster.nodes.addresses")}</dt>
            <dd>{d.addresses.join(", ") || "—"}</dd>
            <dt>{t("cluster.nodes.os")}</dt>
            <dd>
              {d.osImage ?? "—"} · {d.architecture ?? "—"} · {d.kernelVersion ?? "—"}
            </dd>
            <dt>{t("cluster.nodes.runtime")}</dt>
            <dd>{d.containerRuntime ?? "—"}</dd>
          </dl>
          <h4>{t("cluster.nodes.conditions")}</h4>
          <Conditions conditions={d.conditions} />
          <h4>{t("cluster.nodes.taints")}</h4>
          {d.taints.length === 0 ? (
            <p className="muted">—</p>
          ) : (
            <ul className="kv">
              {d.taints.map((x) => (
                <li key={x}>
                  <code>{x}</code>
                </li>
              ))}
            </ul>
          )}
          <h4>{t("cluster.nodes.resources")}</h4>
          <table>
            <thead>
              <tr>
                <th />
                <th>{t("cluster.nodes.capacity")}</th>
                <th>{t("cluster.nodes.allocatable")}</th>
              </tr>
            </thead>
            <tbody>
              {Object.keys(d.capacity).map((r) => (
                <tr key={r}>
                  <td>{r}</td>
                  <td>{d.capacity[r]}</td>
                  <td>{d.allocatable[r] ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <h4>{t("objects.labels")}</h4>
          <KeyValues values={d.labels} />
        </>
      )}
    </div>
  );
}
````

<!-- block: patch t13-src step=13.3 -->
````diff
diff --git a/docs/i18n/same-as-en.json b/docs/i18n/same-as-en.json
index f4d258a..0679a0b 100644
--- a/docs/i18n/same-as-en.json
+++ b/docs/i18n/same-as-en.json
@@ -1,6 +1,8 @@
 {
   "ru": [
     "connections.form.step.tcp",
-    "connections.form.step.tls"
+    "connections.form.step.tls",
+    "cluster.nodes.taints",
+    "cluster.nodes.version"
   ]
 }
diff --git a/src/components/Layout.tsx b/src/components/Layout.tsx
index a6275bb..c5906b5 100644
--- a/src/components/Layout.tsx
+++ b/src/components/Layout.tsx
@@ -24,6 +24,8 @@ function tabTitle(tab: Tab, byId: Map<string, ConnectionProfile>): string {
       return tab.connectionId ? i18n.t("tabs.connection", { name: conn }) : i18n.t("tabs.newConnection");
     case "cluster":
       return i18n.t("tabs.cluster", { conn });
+    case "nodes":
+      return i18n.t("tabs.nodes", { conn });
   }
 }
 
diff --git a/src/components/TabContent.tsx b/src/components/TabContent.tsx
index f24aff9..88e55a7 100644
--- a/src/components/TabContent.tsx
+++ b/src/components/TabContent.tsx
@@ -3,6 +3,7 @@ import { useTranslation } from "react-i18next";
 import type { Tab } from "../state/workspace";
 import { AboutPanel } from "./about/AboutPanel";
 import { ConnectionPanel } from "./cluster/ConnectionPanel";
+import { NodesPanel } from "./cluster/NodesPanel";
 import { ConnectionFormTab } from "./connections/ConnectionFormTab";
 import { SettingsPanel } from "./settings/SettingsPanel";
 
@@ -43,5 +44,7 @@ export function TabContent({ tab }: { tab: Tab }) {
       return <ConnectionFormTab connectionId={tab.connectionId} folderId={tab.folderId ?? null} tabId={tab.id} />;
     case "cluster":
       return <ConnectionPanel connectionId={tab.connectionId} />;
+    case "nodes":
+      return <NodesPanel connectionId={tab.connectionId} />;
   }
 }
diff --git a/src/components/cluster/ConnectionPanel.tsx b/src/components/cluster/ConnectionPanel.tsx
index a1794ea..6361fca 100644
--- a/src/components/cluster/ConnectionPanel.tsx
+++ b/src/components/cluster/ConnectionPanel.tsx
@@ -2,12 +2,17 @@ import { useIsMutating, useQuery, useQueryClient } from "@tanstack/react-query";
 import { useTranslation } from "react-i18next";
 import { api, toAppError } from "../../api/client";
 import type { ApiGroups, AppError, Capabilities, ConnectionInfo, ConnectionProfile } from "../../api/types";
+import { formatDateTime } from "../../lib/age";
 import { needsSignIn } from "../../lib/signIn";
 import { useWorkspace } from "../../state/workspace";
 import { ErrorBanner } from "../common/ErrorBanner";
 import { Hint } from "../common/Hint";
+import { ClusterOverviewView } from "./ClusterOverview";
 import { LoginWait, reconnect, SessionActions, sessionMutationKey } from "./SessionActions";
 
+/** За сколько до истечения токена показывается предупреждение (спец. §5.2). */
+const TOKEN_WARNING_SEC = 3600;
+
 const CAPABILITIES: (keyof Capabilities)[] = ["listProjects", "listNodes", "listAllPods", "readClusterOperators", "readClusterVersion"];
 /** Права, которые бывают только у OpenShift. */
 const OPENSHIFT_ONLY: (keyof Capabilities)[] = ["readClusterOperators", "readClusterVersion"];
@@ -49,6 +54,11 @@ export function ConnectionPanel({ connectionId }: { connectionId: string }) {
         <button type="button" onClick={() => open({ kind: "connection-form", connectionId })}>
           {t("cluster.connection.edit")}
         </button>
+        {q.data?.capabilities.listNodes && (
+          <button type="button" onClick={() => open({ kind: "nodes", connectionId })}>
+            {t("cluster.nodes.open")}
+          </button>
+        )}
       </div>
       {/* Exec-плагин может ждать входа в браузере до 5 минут, и при первом подключении, и при повторе. Рядом с ошибкой входа то же ожидание показывает SessionActions. */}
       {exec && q.isFetching && !signIn && (
@@ -59,6 +69,7 @@ export function ConnectionPanel({ connectionId }: { connectionId: string }) {
       {!exec && q.isPending && <p className="muted">{t("cluster.connection.connecting")}</p>}
       {error && <ConnectionError error={error} signIn={signIn} onRetry={() => void q.refetch()} profile={profile} />}
       {q.data && <ConnectionSummary info={q.data} />}
+      {q.data && <ClusterOverviewView connectionId={connectionId} />}
     </div>
   );
 }
@@ -82,8 +93,14 @@ function ConnectionSummary({ info }: { info: ConnectionInfo }) {
   const { t } = useTranslation();
   const openShift = info.platform.kind === "openshift";
   const found = API_GROUPS.filter(([k]) => info.apiGroups[k]).map(([, name]) => name);
+  const expiresIn = info.tokenExpiresAt === null ? null : info.tokenExpiresAt - Date.now() / 1000;
   return (
     <>
+      {expiresIn !== null && expiresIn < TOKEN_WARNING_SEC && (
+        <p className="warn" role="status">
+          {expiresIn > 0 ? t("cluster.connection.tokenExpiresSoon") : t("cluster.connection.tokenExpired")}
+        </p>
+      )}
       <dl className="stats">
         <div>
           <dt>{t("cluster.connection.user")}</dt>
@@ -111,6 +128,12 @@ function ConnectionSummary({ info }: { info: ConnectionInfo }) {
           </dt>
           <dd>{found.length > 0 ? found.join(", ") : "—"}</dd>
         </div>
+        {info.tokenExpiresAt !== null && (
+          <div>
+            <dt>{t("cluster.connection.tokenExpires")}</dt>
+            <dd>{formatDateTime(info.tokenExpiresAt)}</dd>
+          </div>
+        )}
       </dl>
       <h3>{t("cluster.connection.capabilities")}</h3>
       <ul className="steps">
diff --git a/src/components/help/helpChapterFor.ts b/src/components/help/helpChapterFor.ts
index 8f823d4..539e05a 100644
--- a/src/components/help/helpChapterFor.ts
+++ b/src/components/help/helpChapterFor.ts
@@ -6,6 +6,7 @@ export function helpChapterFor(tab: TabSpec | null): ChapterId {
   switch (tab?.kind) {
     case "connection-form":
     case "cluster":
+    case "nodes":
       return "connections";
     default:
       return "getting-started";
diff --git a/src/state/workspace.ts b/src/state/workspace.ts
index 356a644..82f9a7e 100644
--- a/src/state/workspace.ts
+++ b/src/state/workspace.ts
@@ -1,8 +1,9 @@
 import { create } from "zustand";
 
 export type TabSpec =
-  /** Обзор подключения: кто вошёл, версия, права (этап O1b добавит обзор кластера). */
+  /** Обзор подключения и кластера: кто вошёл, права, API-сервер, ноды, у OpenShift — обновления и операторы. */
   | { kind: "cluster"; connectionId: string }
+  | { kind: "nodes"; connectionId: string }
   /** folderId — папка нового подключения («Новое подключение здесь»). */
   | { kind: "connection-form"; connectionId: string | null; folderId?: string | null }
   | { kind: "about" }
````

<!-- block: locales t13-locales step=13.3 -->
````json
{
  "en": {
    "tabs": { "nodes": "{{conn}} · Nodes" },
    "objects": { "labels": "Labels", "annotations": "Annotations" },
    "hints": {
      "cluster": {
        "readyz": "The <code>/readyz</code> checks of the API server. Any user can read them; a failed check names the part of the server that is not ready."
      }
    },
    "cluster": {
      "connection": {
        "tokenExpires": "Token valid until",
        "tokenExpiresSoon": "The token expires in less than an hour — sign in again soon",
        "tokenExpired": "The token has expired — sign in again"
      },
      "overview": {
        "title": "Cluster overview",
        "apiServer": "API server",
        "ready": "ready",
        "notReady": "not ready",
        "failedChecks": "Failed checks",
        "nodes": "Ready nodes",
        "byRole": "By role",
        "projects": "Projects",
        "ingressClasses": "Ingress classes",
        "gatewayClasses": "Gateway classes",
        "defaultClass": "default",
        "version": "Version",
        "channel": "Update channel",
        "updates": "Available updates",
        "failing": "Update fails",
        "progressing": "Updating",
        "infrastructure": "Infrastructure",
        "console": "Web console",
        "operators": "Cluster operators",
        "operatorsOk": "All {{total}} operators are fine.",
        "operatorsProblems": "Not fine: {{bad}} of {{total}}.",
        "operatorsAll": "Show all",
        "operatorsProblemsOnly": "Only the problems",
        "machineConfigPools": "Machine config pools",
        "machinesUpdated": "machines updated",
        "updating": "updating",
        "degraded_one": "{{count}} machine degraded",
        "degraded_other": "{{count}} machines degraded",
        "hiddenByRights": "The rest is hidden by your rights."
      },
      "operators": {
        "name": "Operator",
        "available": "Available",
        "progressing": "Progressing",
        "degraded": "Degraded",
        "message": "Message"
      },
      "nodes": {
        "open": "Nodes",
        "title": "Nodes",
        "name": "Name",
        "status": "Status",
        "roles": "Roles",
        "version": "Kubelet",
        "memory": "Memory",
        "age": "Age",
        "addresses": "Addresses",
        "os": "OS",
        "runtime": "Container runtime",
        "conditions": "Conditions",
        "taints": "Taints",
        "resources": "Resources",
        "capacity": "Capacity",
        "allocatable": "Allocatable"
      }
    }
  },
  "ru": {
    "tabs": { "nodes": "{{conn}} · Ноды" },
    "objects": { "labels": "Метки", "annotations": "Аннотации" },
    "hints": {
      "cluster": {
        "readyz": "Проверки <code>/readyz</code> API-сервера. Их может читать любой пользователь; непройденная проверка называет часть сервера, которая не готова."
      }
    },
    "cluster": {
      "connection": {
        "tokenExpires": "Токен действует до",
        "tokenExpiresSoon": "Токен истекает меньше чем через час — скоро нужно будет войти снова",
        "tokenExpired": "Срок токена истёк — войдите снова"
      },
      "overview": {
        "title": "Обзор кластера",
        "apiServer": "API-сервер",
        "ready": "готов",
        "notReady": "не готов",
        "failedChecks": "Не пройдены проверки",
        "nodes": "Готовые ноды",
        "byRole": "По ролям",
        "projects": "Проекты",
        "ingressClasses": "Классы Ingress",
        "gatewayClasses": "Классы Gateway",
        "defaultClass": "по умолчанию",
        "version": "Версия",
        "channel": "Канал обновлений",
        "updates": "Доступно обновлений",
        "failing": "Обновление не удаётся",
        "progressing": "Идёт обновление",
        "infrastructure": "Инфраструктура",
        "console": "Веб-консоль",
        "operators": "Операторы кластера",
        "operatorsOk": "Все {{total}} операторов в норме.",
        "operatorsProblems": "Не в норме: {{bad}} из {{total}}.",
        "operatorsAll": "Показать все",
        "operatorsProblemsOnly": "Только проблемные",
        "machineConfigPools": "Пулы машин (MachineConfigPool)",
        "machinesUpdated": "машин обновлено",
        "updating": "обновляется",
        "degraded_one": "деградировала {{count}} машина",
        "degraded_few": "деградировали {{count}} машины",
        "degraded_many": "деградировали {{count}} машин",
        "degraded_other": "деградировали {{count}} машины",
        "hiddenByRights": "Остальное скрыто правами."
      },
      "operators": {
        "name": "Оператор",
        "available": "Доступен",
        "progressing": "Обновляется",
        "degraded": "Деградировал",
        "message": "Сообщение"
      },
      "nodes": {
        "open": "Ноды",
        "title": "Ноды",
        "name": "Имя",
        "status": "Статус",
        "roles": "Роли",
        "version": "Kubelet",
        "memory": "Память",
        "age": "Возраст",
        "addresses": "Адреса",
        "os": "ОС",
        "runtime": "Среда контейнеров",
        "conditions": "Условия",
        "taints": "Taints",
        "resources": "Ресурсы",
        "capacity": "Ёмкость",
        "allocatable": "Доступно подам"
      }
    }
  }
}
````

- [ ] **Step 4: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 40 passed`, `Tests 234 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 136`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 5: Commit.**

```bash
git add \
  docs/i18n/same-as-en.json \
  src/components/Layout.tsx \
  src/components/TabContent.tsx \
  src/components/cluster/ClusterOverview.test.tsx \
  src/components/cluster/ClusterOverview.tsx \
  src/components/cluster/ConnectionPanel.test.tsx \
  src/components/cluster/ConnectionPanel.tsx \
  src/components/cluster/NodesPanel.test.tsx \
  src/components/cluster/NodesPanel.tsx \
  src/components/help/helpChapterFor.ts \
  src/locales/en.json \
  src/locales/ru.json \
  src/state/workspace.ts
git commit -s -m "feat(ui): обзор кластера на вкладке подключения — API-сервер, ноды, классы Ingress и Gateway, OpenShift; срок токена; вкладка нод" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 14: Вкладки workload'а и пода, таблицы подов и событий

**Files:**
- Create: `src/components/objects/ContainerCard.tsx`, `src/components/objects/EventList.tsx`, `src/components/objects/EventsTable.tsx`, `src/components/objects/PodList.test.tsx`, `src/components/objects/PodList.tsx`, `src/components/objects/PodPanel.test.tsx`, `src/components/objects/PodPanel.tsx`, `src/components/objects/PodsTable.tsx`, `src/components/objects/WatchStatusLine.tsx`, `src/components/objects/WorkloadPanel.test.tsx`, `src/components/objects/WorkloadPanel.tsx`, `src/test/objects.ts`
- Modify: `src/components/Layout.tsx`, `src/components/TabContent.tsx`, `src/locales/en.json`, `src/locales/ru.json`, `src/state/workspace.test.ts`, `src/state/workspace.ts`

**Interfaces:**
- Consumes: задачи 12–13; команды задач 6, 7, 9, 10.
- Produces: `components/objects/{PodsTable, PodList({ connectionId, namespace, workload? }), EventsTable({ rows }), ObjectEvents({ connectionId, namespace, object }), WatchStatusLine, ContainerCard, WorkloadPanel({ connectionId, namespace, kind, name }), PodPanel({ connectionId, namespace, name })}`; `TabSpec` `{ kind: "workload", connectionId, namespace, workloadKind, name }` (id `workload:<id>:<ns>:<вид>:<имя>`) и `{ kind: "pod", connectionId, namespace, name }` (id `pod:<id>:<ns>:<имя>`), заголовок — имя объекта; тестовые данные `src/test/objects.ts` (`podRow`, `workloadRow`, `container`).

Спец. §6.4–6.5. `PodList` — поды через watch (у CronJob — список раз в 10 с), фильтры «Только проблемные» и по имени, щелчок открывает под. `EventsTable` — новые сверху, фильтры по типу, причине и объекту. Вкладка workload'а: «Обзор» (реплики или у Job — завершения, у CronJob — расписание, приостановка и идущие Job; проблема, селектор, стратегия, условия, HPA, PDB, история выкладки с текущей ревизией, контейнеры, метки и аннотации), «Поды», «События» (с подами) и YAML. Вкладка пода: «Обзор» (статус, готовность, рестарты, QoS, фаза, нода, IP, владелец — щелчок открывает workload, сервисная учётная запись, условия, контейнеры с состоянием и прошлым состоянием, тома, метки и аннотации), «События», YAML. Переменные окружения — только имена и источники. Подвкладка показывается одна: watch неоткрытых не идёт.

- [ ] **Step 1: Тесты.**

<!-- block: file t14-src-components-objects-podlist-test-tsx step=14.1 path=src/components/objects/PodList.test.tsx -->
````tsx
import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 28 })),
    getTotalSize: () => o.count * 28,
  }),
}));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { startWatch: vi.fn(), stopSession: vi.fn(), listPods: vi.fn() } };
});

import { api } from "../../api/client";
import type { EventRow, WatchEvent } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { podRow } from "../../test/objects";
import { EventsTable } from "./EventsTable";
import { PodList } from "./PodList";

let emit: (e: WatchEvent) => void = () => undefined;

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspace.setState({ tabs: [], activeId: null });
  vi.mocked(api.startWatch).mockImplementation(async (_p, onEvent) => {
    emit = onEvent;
    return "s1";
  });
  vi.mocked(api.stopSession).mockResolvedValue(true);
});

describe("PodList", () => {
  it("поды workload'а через watch: только проблемные, фильтр по имени, опрос, открытие пода", async () => {
    const user = userEvent.setup();
    renderWithClient(<PodList connectionId="c1" namespace="team-a" workload={{ kind: "Deployment", name: "web" }} />);
    await waitFor(() =>
      expect(api.startWatch).toHaveBeenCalledWith(
        { connectionId: "c1", namespace: "team-a", kind: "pods", workload: { kind: "Deployment", name: "web" } },
        expect.any(Function),
      ),
    );
    act(() =>
      emit({
        type: "reset",
        items: [podRow("web-b"), podRow("web-a", { status: "CrashLoopBackOff", problem: "crashLoop", restarts: 7, lastRestart: 1 })],
        errors: [],
      }),
    );
    expect(screen.getAllByRole("row").slice(1).map((r) => r.firstChild?.textContent)).toEqual(["web-a", "web-b"]);
    await user.click(screen.getByLabelText("Только проблемные (1)"));
    expect(screen.queryByText("web-b")).not.toBeInTheDocument();
    expect(screen.getByText("CrashLoopBackOff")).toHaveClass("problem");
    act(() => emit({ type: "polling" }));
    expect(screen.getByText("Нет прав на watch — список обновляется раз в 10 с")).toBeInTheDocument();
    await user.click(screen.getByText("web-a"));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "pod", connectionId: "c1", namespace: "team-a", name: "web-a" });
  });

  it("поды CronJob читаются списком, без watch", async () => {
    vi.mocked(api.listPods).mockResolvedValue({ items: [podRow("report-1-x", { owner: { kind: "Job", name: "report-1" } })], truncated: false });
    renderWithClient(<PodList connectionId="c1" namespace="team-a" workload={{ kind: "CronJob", name: "report" }} />);
    expect(await screen.findByText("report-1-x")).toBeInTheDocument();
    expect(api.listPods).toHaveBeenCalledWith("c1", "team-a", { kind: "CronJob", name: "report" });
    expect(api.startWatch).not.toHaveBeenCalled();
  });
});

describe("EventsTable", () => {
  const ev = (key: string, type: string, reason: string, name: string, lastSeen: number): EventRow => ({
    key,
    type,
    reason,
    object: { kind: "Pod", name },
    message: `${reason} message`,
    count: 1,
    firstSeen: lastSeen,
    lastSeen,
    source: "kubelet",
  });

  it("новые сверху, фильтры по типу, причине и объекту", async () => {
    const user = userEvent.setup();
    renderWithClient(
      <EventsTable rows={[ev("1", "Normal", "Pulled", "web-a", 10), ev("2", "Warning", "BackOff", "web-a", 20), ev("3", "Warning", "FailedMount", "db-0", 15)]} />,
    );
    const reasons = () => screen.getAllByRole("row").slice(1).map((r) => r.children[2].textContent);
    expect(reasons()).toEqual(["BackOff", "FailedMount", "Pulled"]);
    await user.selectOptions(screen.getByLabelText("Тип"), "Warning");
    expect(reasons()).toEqual(["BackOff", "FailedMount"]);
    await user.type(screen.getByLabelText("Объект"), "db-");
    expect(reasons()).toEqual(["FailedMount"]);
    await user.clear(screen.getByLabelText("Объект"));
    await user.selectOptions(screen.getByLabelText("Причина"), "BackOff");
    expect(reasons()).toEqual(["BackOff"]);
  });
});
````

<!-- block: file t14-src-components-objects-podpanel-test-tsx step=14.1 path=src/components/objects/PodPanel.test.tsx -->
````tsx
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 28 })),
    getTotalSize: () => o.count * 28,
  }),
}));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getPod: vi.fn(), listEvents: vi.fn(), getYaml: vi.fn() } };
});

import { api } from "../../api/client";
import type { PodDetails } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { container, podRow } from "../../test/objects";
import { PodPanel } from "./PodPanel";

const DETAILS: PodDetails = {
  pod: podRow("web-7d9f-a", { status: "CrashLoopBackOff", problem: "crashLoop", ready: 0, restarts: 7 }),
  conditions: [{ type: "Ready", status: "False", reason: "ContainersNotReady" }],
  podIp: "10.1.0.5",
  hostIp: "10.0.0.1",
  serviceAccount: "default",
  started: 1,
  containers: [container({ init: true, name: "migrate", state: { state: "terminated", reason: "Completed", message: null, exitCode: 0, started: 1, finished: 2 }, lastState: null }), container()],
  labels: { app: "web" },
  annotations: { note: "x" },
  volumes: [{ name: "data", kind: "persistentVolumeClaim", source: "web-data" }],
};

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspace.setState({ tabs: [], activeId: null });
  vi.mocked(api.getPod).mockResolvedValue(DETAILS);
  vi.mocked(api.listEvents).mockResolvedValue({ items: [], truncated: false });
});

describe("PodPanel", () => {
  it("обзор: статус, контейнеры с прошлым состоянием, тома; владелец открывает workload", async () => {
    const user = userEvent.setup();
    renderWithClient(<PodPanel connectionId="c1" namespace="team-a" name="web-7d9f-a" />);
    expect(await screen.findByText("CrashLoopBackOff")).toHaveClass("problem");
    expect(screen.getByText("migrate")).toBeInTheDocument();
    expect(screen.getByText("(init-контейнер)")).toBeInTheDocument();
    expect(screen.getByText(/завершён: Error, код 1/)).toBeInTheDocument();
    expect(screen.getAllByText("registry/web@sha256:abc", { selector: "code" })).toHaveLength(2);
    expect(screen.getByText("data: persistentVolumeClaim web-data")).toBeInTheDocument();
    expect(screen.getByText("ContainersNotReady")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Deployment/web" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "workload", workloadKind: "Deployment", name: "web", namespace: "team-a" });
  });

  it("события пода", async () => {
    const user = userEvent.setup();
    renderWithClient(<PodPanel connectionId="c1" namespace="team-a" name="web-7d9f-a" />);
    await user.click(screen.getByRole("tab", { name: "События" }));
    await waitFor(() => expect(api.listEvents).toHaveBeenCalledWith("c1", "team-a", { kind: "Pod", name: "web-7d9f-a" }));
  });
});
````

<!-- block: file t14-src-components-objects-workloadpanel-test-tsx step=14.1 path=src/components/objects/WorkloadPanel.test.tsx -->
````tsx
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 28 })),
    getTotalSize: () => o.count * 28,
  }),
}));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getWorkload: vi.fn(), startWatch: vi.fn(), stopSession: vi.fn(), listEvents: vi.fn(), getYaml: vi.fn() } };
});

import { api } from "../../api/client";
import type { WorkloadDetails } from "../../api/types";
import { container, workloadRow } from "../../test/objects";
import { WorkloadPanel } from "./WorkloadPanel";

const DETAILS: WorkloadDetails = {
  workload: workloadRow("web", { ready: 2, available: 2, problem: "unavailable" }),
  conditions: [{ type: "Available", status: "False", reason: "MinimumReplicasUnavailable" }],
  selector: "app=web",
  strategy: "RollingUpdate (maxSurge 25%, maxUnavailable 25%)",
  containers: [container()],
  labels: { app: "web" },
  annotations: {},
  activeJobs: [],
  hpa: { data: { name: "web", minReplicas: 2, maxReplicas: 10, currentReplicas: 3, desiredReplicas: 3, metrics: ["cpu 80%"], conditions: [] } },
  pdb: { data: null },
  history: {
    data: [
      { revision: 3, name: "web-7d9f", images: ["registry/web:3"], created: 1, replicas: 3, current: true },
      { revision: 2, name: "web-5c4b", images: ["registry/web:2"], created: 1, replicas: 0, current: false },
    ],
  },
};

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.getWorkload).mockResolvedValue(DETAILS);
  vi.mocked(api.startWatch).mockResolvedValue("s1");
  vi.mocked(api.stopSession).mockResolvedValue(true);
  vi.mocked(api.listEvents).mockResolvedValue({ items: [], truncated: false });
  vi.mocked(api.getYaml).mockResolvedValue("kind: Deployment\n");
});

describe("WorkloadPanel", () => {
  it("обзор: реплики, проблема, HPA, история, контейнеры без значений переменных", async () => {
    renderWithClient(<WorkloadPanel connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    expect(await screen.findByText("Доступно меньше реплик, чем желаемо")).toBeInTheDocument();
    expect(api.getWorkload).toHaveBeenCalledWith("c1", "team-a", "Deployment", "web");
    expect(screen.getAllByText("app=web"), "селектор и метка").toHaveLength(2);
    expect(screen.getByText("MinimumReplicasUnavailable")).toBeInTheDocument();
    expect(screen.getByText(/2–10, реплик сейчас 3, нужно 3 · cpu 80%/)).toBeInTheDocument();
    expect(screen.getByText("3 (текущая)")).toBeInTheDocument();
    expect(screen.getByText("registry/web:2")).toBeInTheDocument();
    expect(screen.getByText("ждёт: CrashLoopBackOff — back-off 5m0s")).toBeInTheDocument();
    expect(screen.getByText("DB_PASSWORD ← secretKeyRef db/password")).toBeInTheDocument();
    expect(screen.getByText("100m / —")).toBeInTheDocument();
  });

  it("подвкладки: поды workload'а через watch, события вместе с подами, YAML", async () => {
    const user = userEvent.setup();
    renderWithClient(<WorkloadPanel connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    await user.click(screen.getByRole("tab", { name: "Поды" }));
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledWith(expect.objectContaining({ kind: "pods", workload: { kind: "Deployment", name: "web" } }), expect.any(Function)));
    await user.click(screen.getByRole("tab", { name: "События" }));
    await waitFor(() => expect(api.listEvents).toHaveBeenCalledWith("c1", "team-a", { kind: "Deployment", name: "web" }));
    expect(api.stopSession).toHaveBeenCalledWith("s1");
    await user.click(screen.getByRole("tab", { name: "YAML" }));
    expect(await screen.findByText("kind: Deployment")).toBeInTheDocument();
  });

  it("CronJob: расписание и идущие Job вместо реплик", async () => {
    vi.mocked(api.getWorkload).mockResolvedValue({
      ...DETAILS,
      workload: workloadRow("report", { kind: "CronJob", desired: null, ready: 1, schedule: "0 3 * * *", suspended: true, lastSchedule: 1, problem: null }),
      activeJobs: ["report-1"],
      history: null,
    });
    renderWithClient(<WorkloadPanel connectionId="c1" namespace="team-a" kind="CronJob" name="report" />);
    expect(await screen.findByText("0 3 * * *")).toBeInTheDocument();
    expect(screen.getByText("report-1")).toBeInTheDocument();
    expect(screen.getByText(/приостановлен/)).toBeInTheDocument();
    expect(screen.queryByText("История выкладки")).not.toBeInTheDocument();
  });
});
````

<!-- block: file t14-src-test-objects-ts step=14.1 path=src/test/objects.ts -->
````ts
// Данные для тестов экранов объектов.
import type { ContainerStatusView, PodRow, WorkloadRow } from "../api/types";

export const podRow = (name: string, patch: Partial<PodRow> = {}): PodRow => ({
  name,
  phase: "Running",
  status: "Running",
  problem: null,
  ready: 1,
  containers: 1,
  restarts: 0,
  lastRestart: null,
  node: "w1",
  created: 1,
  qos: "Burstable",
  owner: { kind: "Deployment", name: "web" },
  ...patch,
});

export const workloadRow = (name: string, patch: Partial<WorkloadRow> = {}): WorkloadRow => ({
  kind: "Deployment",
  name,
  desired: 3,
  ready: 3,
  available: 3,
  updated: 3,
  images: ["registry/web:3"],
  created: 1,
  problem: null,
  ...patch,
});

export const container = (patch: Partial<ContainerStatusView> = {}): ContainerStatusView => ({
  name: "web",
  image: "registry/web:3",
  init: false,
  requests: { cpu: "100m", memory: "128Mi" },
  limits: { memory: "256Mi" },
  ports: ["8080/TCP (http)"],
  liveness: null,
  readiness: "http-get http://:8080/ready delay=0s timeout=1s period=5s #success=1 #failure=3",
  startup: null,
  env: [
    { name: "LOG_LEVEL", source: "configMapKeyRef", reference: "web/level" },
    { name: "DB_PASSWORD", source: "secretKeyRef", reference: "db/password" },
  ],
  imageId: "registry/web@sha256:abc",
  state: { state: "waiting", reason: "CrashLoopBackOff", message: "back-off 5m0s", exitCode: null, started: null, finished: null },
  lastState: { state: "terminated", reason: "Error", message: null, exitCode: 1, started: 1, finished: 2 },
  ready: false,
  restarts: 7,
  ...patch,
});
````

<!-- block: patch t14-tests step=14.1 -->
````diff
diff --git a/src/state/workspace.test.ts b/src/state/workspace.test.ts
index f51ab16..4545175 100644
--- a/src/state/workspace.test.ts
+++ b/src/state/workspace.test.ts
@@ -35,6 +35,17 @@ describe("workspace", () => {
     expect(tabId({ kind: "connection-form", connectionId: "c" })).toBe("connection-form:c");
     expect(tabId({ kind: "cluster", connectionId: "c" })).toBe("cluster:c");
     expect(tabId({ kind: "help", chapter: "connections" })).toBe("help");
+    expect(tabId({ kind: "nodes", connectionId: "c" })).toBe("nodes:c");
+    expect(tabId({ kind: "workload", connectionId: "c", namespace: "team-a", workloadKind: "Deployment", name: "web" })).toBe("workload:c:team-a:Deployment:web");
+    expect(tabId({ kind: "pod", connectionId: "c", namespace: "team-a", name: "web-1" })).toBe("pod:c:team-a:web-1");
+  });
+
+  it("вкладки объектов закрываются вместе с подключением", () => {
+    const { open, closeForConnection } = useWorkspace.getState();
+    open({ kind: "pod", connectionId: "a", namespace: "team-a", name: "web-1" });
+    open({ kind: "workload", connectionId: "b", namespace: "team-a", workloadKind: "Job", name: "migrate" });
+    closeForConnection("a");
+    expect(useWorkspace.getState().tabs.map((t) => t.id)).toEqual(["workload:b:team-a:Job:migrate"]);
   });
 
   it("closeForConnection закрывает все вкладки подключения", () => {
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/objects/PodList.test.tsx src/components/objects/PodPanel.test.tsx src/components/objects/WorkloadPanel.test.tsx src/state/workspace.test.ts`  
Expected: FAIL — нет компонентов `PodList`, `EventsTable`, `WorkloadPanel`, `PodPanel` и тестовых данных `src/test/objects.ts`; у `TabSpec` нет видов `workload` и `pod`.

- [ ] **Step 3: Реализация.**

<!-- block: file t14-src-components-objects-containercard-tsx step=14.3 path=src/components/objects/ContainerCard.tsx -->
````tsx
import { Fragment } from "react";
import { useTranslation } from "react-i18next";
import type { ContainerSpecView, ContainerStatusView, StateView } from "../../api/types";
import { formatDateTime } from "../../lib/age";

const isStatus = (c: ContainerSpecView | ContainerStatusView): c is ContainerStatusView => "restarts" in c;

/** Состояние контейнера словами: «работает с …», «ждёт: CrashLoopBackOff», «завершён: Error, код 1». */
function StateText({ s }: { s: StateView }) {
  const { t } = useTranslation();
  const reason = [s.reason, s.message].filter(Boolean).join(" — ");
  switch (s.state) {
    case "running":
      return <>{t("objects.container.running", { since: formatDateTime(s.started) })}</>;
    case "waiting":
      return <span className="problem">{t("objects.container.waiting", { reason: reason || "—" })}</span>;
    case "terminated":
      return (
        <span className={s.exitCode ? "problem" : undefined}>
          {t("objects.container.terminated", { reason: reason || "—", code: s.exitCode ?? "—", at: formatDateTime(s.finished) })}
        </span>
      );
  }
}

/** Контейнер шаблона или пода (спец. §6.5): образ, состояние, ресурсы, пробы, переменные окружения (имена и источники, без значений). */
export function ContainerCard({ c }: { c: ContainerSpecView | ContainerStatusView }) {
  const { t } = useTranslation();
  const resource = (r: "cpu" | "memory") => `${c.requests[r] ?? "—"} / ${c.limits[r] ?? "—"}`;
  return (
    <div className="card">
      <h4>
        {c.name} {c.init && <span className="muted">({t("objects.container.init")})</span>}
      </h4>
      <dl className="facts">
        <dt>{t("objects.container.image")}</dt>
        <dd>
          <code>{c.image}</code>
          {isStatus(c) && c.imageId && (
            <>
              <br />
              <code className="muted">{c.imageId}</code>
            </>
          )}
        </dd>
        {isStatus(c) && (
          <>
            <dt>{t("objects.container.state")}</dt>
            <dd>{c.state ? <StateText s={c.state} /> : "—"}</dd>
            {c.lastState && (
              <>
                <dt>{t("objects.container.lastState")}</dt>
                <dd>
                  <StateText s={c.lastState} />
                </dd>
              </>
            )}
            <dt>{t("objects.container.ready")}</dt>
            <dd>{c.ready ? "✅" : "—"}</dd>
            <dt>{t("objects.container.restarts")}</dt>
            <dd>{c.restarts}</dd>
          </>
        )}
        <dt>CPU</dt>
        <dd>{resource("cpu")}</dd>
        <dt>{t("objects.container.memory")}</dt>
        <dd>{resource("memory")}</dd>
        {c.ports.length > 0 && (
          <>
            <dt>{t("objects.container.ports")}</dt>
            <dd>{c.ports.join(", ")}</dd>
          </>
        )}
        {(["liveness", "readiness", "startup"] as const).map(
          (p) =>
            c[p] && (
              <Fragment key={p}>
                <dt>{t(`objects.container.${p}`)}</dt>
                <dd>
                  <code>{c[p]}</code>
                </dd>
              </Fragment>
            ),
        )}
      </dl>
      {c.env.length > 0 && (
        <details>
          <summary>{t("objects.container.env", { count: c.env.length })}</summary>
          <ul className="kv">
            {c.env.map((e) => (
              <li key={`${e.name}:${e.source}:${e.reference ?? ""}`}>
                <code>
                  {e.name} ← {e.source === "literal" ? t("objects.container.literal") : `${e.source} ${e.reference ?? ""}`}
                </code>
              </li>
            ))}
          </ul>
        </details>
      )}
    </div>
  );
}
````

<!-- block: file t14-src-components-objects-eventlist-tsx step=14.3 path=src/components/objects/EventList.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { EventObject } from "../../api/types";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
import { EventsTable } from "./EventsTable";

/** События объекта (у workload'а — вместе с его подами): «Обновить» и автообновление. */
export function ObjectEvents({ connectionId, namespace, object }: { connectionId: string; namespace: string; object: EventObject }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const q = useQuery({
    queryKey: ["events", connectionId, namespace, object],
    queryFn: () => api.listEvents(connectionId, namespace, object),
    refetchInterval: interval || false,
  });
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && <EventsTable rows={q.data.items} />}
    </div>
  );
}
````

<!-- block: file t14-src-components-objects-eventstable-tsx step=14.3 path=src/components/objects/EventsTable.tsx -->
````tsx
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import type { EventRow } from "../../api/types";
import { formatAge } from "../../lib/age";
import { eventKey } from "../../lib/useWatchList";
import { type Column, VirtualTable } from "../common/VirtualTable";

/** Таблица событий (спец. §6.3) с фильтрами по типу, причине и объекту; новые — сверху. */
export function EventsTable({ rows }: { rows: EventRow[] }) {
  const { t } = useTranslation();
  const [type, setType] = useState("");
  const [reason, setReason] = useState("");
  const [object, setObject] = useState("");
  const reasons = useMemo(() => [...new Set(rows.map((e) => e.reason))].sort(), [rows]);
  const shown = useMemo(() => {
    const o = object.trim().toLowerCase();
    return rows
      .filter((e) => (!type || e.type === type) && (!reason || e.reason === reason) && (!o || `${e.object.kind}/${e.object.name}`.toLowerCase().includes(o)))
      .sort((a, b) => (b.lastSeen ?? 0) - (a.lastSeen ?? 0));
  }, [rows, type, reason, object]);
  const columns: Column<EventRow>[] = [
    { key: "last", title: t("objects.events.lastSeen"), width: "80px", render: (e) => formatAge(e.lastSeen) },
    { key: "type", title: t("objects.events.type"), width: "80px", render: (e) => <span className={e.type === "Warning" ? "problem" : undefined}>{e.type}</span> },
    { key: "reason", title: t("objects.events.reason"), width: "minmax(110px, 1fr)", render: (e) => e.reason },
    { key: "object", title: t("objects.events.object"), width: "minmax(160px, 1.5fr)", render: (e) => `${e.object.kind}/${e.object.name}` },
    { key: "message", title: t("objects.events.message"), width: "minmax(240px, 4fr)", render: (e) => <span title={e.message}>{e.message}</span> },
    { key: "count", title: t("objects.events.count"), width: "60px", render: (e) => e.count },
  ];
  return (
    <>
      <div className="toolbar">
        <label className="inline">
          {t("objects.events.type")}
          <select value={type} onChange={(e) => setType(e.target.value)}>
            <option value="">{t("objects.events.all")}</option>
            <option value="Warning">Warning</option>
            <option value="Normal">Normal</option>
          </select>
        </label>
        <label className="inline">
          {t("objects.events.reason")}
          <select value={reason} onChange={(e) => setReason(e.target.value)}>
            <option value="">{t("objects.events.all")}</option>
            {reasons.map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
          </select>
        </label>
        <input aria-label={t("objects.events.object")} placeholder={t("objects.events.objectFilter")} value={object} onChange={(e) => setObject(e.target.value)} />
      </div>
      <VirtualTable label={t("objects.events.title")} rows={shown} columns={columns} rowKey={eventKey} />
    </>
  );
}
````

<!-- block: file t14-src-components-objects-podlist-tsx step=14.3 path=src/components/objects/PodList.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { PodRow, WorkloadRef } from "../../api/types";
import { podKey, useWatchList } from "../../lib/useWatchList";
import { ReadError } from "../common/ReadError";
import { WatchStatusLine } from "./WatchStatusLine";
import { PodsTable } from "./PodsTable";

/** Поды CronJob не наблюдаются (их Job меняются) — список перечитывается раз в 10 с. */
const CRON_POLL_MS = 10_000;

/** Поды проекта или workload'а с фильтрами «только проблемные» и по имени; обновляются сами (watch или опрос). */
export function PodList({ connectionId, namespace, workload }: { connectionId: string; namespace: string; workload?: WorkloadRef }) {
  const { t } = useTranslation();
  const [problemsOnly, setProblemsOnly] = useState(false);
  const [filter, setFilter] = useState("");
  const cron = workload?.kind === "CronJob";
  const watch = useWatchList<PodRow>(cron ? null : { connectionId, namespace, kind: "pods", workload: workload ?? null }, podKey);
  const listed = useQuery({
    queryKey: ["pods", connectionId, namespace, workload ?? null],
    queryFn: () => api.listPods(connectionId, namespace, workload ?? null),
    enabled: cron,
    refetchInterval: CRON_POLL_MS,
  });
  const all = cron ? (listed.data?.items ?? []) : watch.rows;
  const rows = useMemo(() => {
    const f = filter.trim().toLowerCase();
    return all.filter((p) => (!problemsOnly || p.problem) && (!f || p.name.toLowerCase().includes(f))).sort((a, b) => a.name.localeCompare(b.name));
  }, [all, problemsOnly, filter]);
  const problems = all.filter((p) => p.problem).length;
  return (
    <div className="section">
      <div className="toolbar">
        <label className="inline">
          <input type="checkbox" checked={problemsOnly} onChange={(e) => setProblemsOnly(e.target.checked)} />
          {t("objects.pods.problemsOnly", { count: problems })}
        </label>
        <input aria-label={t("objects.pods.filter")} placeholder={t("objects.pods.filter")} value={filter} onChange={(e) => setFilter(e.target.value)} />
        {!cron && <WatchStatusLine state={watch} />}
      </div>
      {cron && listed.error && <ReadError error={listed.error} connectionId={connectionId} onRetry={() => void listed.refetch()} />}
      {!cron && watch.status === "error" && watch.error && <ReadError error={watch.error} connectionId={connectionId} />}
      <PodsTable connectionId={connectionId} namespace={namespace} rows={rows} />
    </div>
  );
}
````

<!-- block: file t14-src-components-objects-podpanel-tsx step=14.3 path=src/components/objects/PodPanel.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { PodDetails, WorkloadKind } from "../../api/types";
import { formatDateTime } from "../../lib/age";
import { useWorkspace } from "../../state/workspace";
import { Conditions } from "../common/Conditions";
import { KeyValues } from "../common/KeyValues";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
import { SubTabs } from "../common/SubTabs";
import { YamlView } from "../common/YamlView";
import { ContainerCard } from "./ContainerCard";
import { ObjectEvents } from "./EventList";

type Sub = "overview" | "events" | "yaml";

const WORKLOAD_KINDS: string[] = ["Deployment", "StatefulSet", "DaemonSet", "DeploymentConfig", "Job", "CronJob"] satisfies WorkloadKind[];

/** Вкладка пода (спец. §6.5): обзор с состоянием каждого контейнера, события, YAML. */
export function PodPanel({ connectionId, namespace, name }: { connectionId: string; namespace: string; name: string }) {
  const { t } = useTranslation();
  const [sub, setSub] = useState<Sub>("overview");
  const tabs: { id: Sub; label: string }[] = [
    { id: "overview", label: t("objects.tabs.overview") },
    { id: "events", label: t("objects.tabs.events") },
    { id: "yaml", label: "YAML" },
  ];
  return (
    <div className="page">
      <div className="toolbar">
        <h2>Pod {name}</h2>
        <span className="muted">{namespace}</span>
      </div>
      <SubTabs tabs={tabs} active={sub} onChange={setSub} />
      {sub === "overview" && <PodOverview connectionId={connectionId} namespace={namespace} name={name} />}
      {sub === "events" && <ObjectEvents connectionId={connectionId} namespace={namespace} object={{ kind: "Pod", name }} />}
      {sub === "yaml" && <YamlView connectionId={connectionId} namespace={namespace} kind="Pod" name={name} />}
    </div>
  );
}

function PodOverview({ connectionId, namespace, name }: { connectionId: string; namespace: string; name: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const q = useQuery({
    queryKey: ["pod", connectionId, namespace, name],
    queryFn: () => api.getPod(connectionId, namespace, name),
    refetchInterval: interval || false,
  });
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && <PodSummary connectionId={connectionId} namespace={namespace} d={q.data} />}
    </div>
  );
}

function PodSummary({ connectionId, namespace, d }: { connectionId: string; namespace: string; d: PodDetails }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const p = d.pod;
  const owner = p.owner;
  return (
    <>
      <dl className="stats">
        <div>
          <dt>{t("objects.pods.status")}</dt>
          <dd className={p.problem ? "problem" : undefined}>{p.status}</dd>
        </div>
        <div>
          <dt>{t("objects.pods.ready")}</dt>
          <dd>
            {p.ready}/{p.containers}
          </dd>
        </div>
        <div>
          <dt>{t("objects.pods.restarts")}</dt>
          <dd>{p.restarts}</dd>
        </div>
        <div>
          <dt>QoS</dt>
          <dd>{p.qos ?? "—"}</dd>
        </div>
      </dl>
      <dl className="facts">
        <dt>{t("objects.pod.phase")}</dt>
        <dd>{p.phase}</dd>
        <dt>{t("objects.pods.node")}</dt>
        <dd>{p.node ?? "—"}</dd>
        <dt>{t("objects.pod.ip")}</dt>
        <dd>
          {d.podIp ?? "—"} <span className="muted">({t("objects.pod.hostIp", { ip: d.hostIp ?? "—" })})</span>
        </dd>
        <dt>{t("objects.pods.owner")}</dt>
        <dd>
          {owner && WORKLOAD_KINDS.includes(owner.kind) ? (
            <button
              type="button"
              className="link"
              onClick={() => open({ kind: "workload", connectionId, namespace, workloadKind: owner.kind as WorkloadKind, name: owner.name })}
            >
              {owner.kind}/{owner.name}
            </button>
          ) : owner ? (
            `${owner.kind}/${owner.name}`
          ) : (
            "—"
          )}
        </dd>
        <dt>{t("objects.pod.serviceAccount")}</dt>
        <dd>{d.serviceAccount ?? "—"}</dd>
        <dt>{t("objects.pod.started")}</dt>
        <dd>{formatDateTime(d.started)}</dd>
      </dl>
      <h3>{t("objects.workload.conditions")}</h3>
      <Conditions conditions={d.conditions} />
      <h3>{t("objects.workload.containers")}</h3>
      <div className="cards">
        {d.containers.map((c) => (
          <ContainerCard key={`${c.init}:${c.name}`} c={c} />
        ))}
      </div>
      <h3>{t("objects.pod.volumes")}</h3>
      {d.volumes.length === 0 ? (
        <p className="muted">—</p>
      ) : (
        <ul className="kv">
          {d.volumes.map((v) => (
            <li key={v.name}>
              <code>
                {v.name}: {v.kind}
                {v.source ? ` ${v.source}` : ""}
              </code>
            </li>
          ))}
        </ul>
      )}
      <h3>{t("objects.labels")}</h3>
      <KeyValues values={d.labels} />
      <h3>{t("objects.annotations")}</h3>
      <KeyValues values={d.annotations} />
    </>
  );
}
````

<!-- block: file t14-src-components-objects-podstable-tsx step=14.3 path=src/components/objects/PodsTable.tsx -->
````tsx
import { useTranslation } from "react-i18next";
import type { PodRow } from "../../api/types";
import { formatAge } from "../../lib/age";
import { podKey } from "../../lib/useWatchList";
import { useWorkspace } from "../../state/workspace";
import { type Column, VirtualTable } from "../common/VirtualTable";

/** Таблица подов (спец. §6.3): статус с причиной, готовность, рестарты, нода, возраст, QoS, владелец; щелчок открывает под. */
export function PodsTable({ connectionId, namespace, rows }: { connectionId: string; namespace: string; rows: PodRow[] }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const columns: Column<PodRow>[] = [
    { key: "name", title: t("objects.pods.name"), width: "minmax(200px, 2fr)", render: (p) => p.name },
    {
      key: "status",
      title: t("objects.pods.status"),
      width: "minmax(130px, 1fr)",
      render: (p) => <span className={p.problem ? "problem" : undefined}>{p.status}</span>,
    },
    { key: "ready", title: t("objects.pods.ready"), width: "70px", render: (p) => `${p.ready}/${p.containers}` },
    {
      key: "restarts",
      title: t("objects.pods.restarts"),
      width: "110px",
      render: (p) => (p.restarts > 0 && p.lastRestart ? `${p.restarts} (${formatAge(p.lastRestart)})` : String(p.restarts)),
    },
    { key: "node", title: t("objects.pods.node"), width: "minmax(100px, 1fr)", render: (p) => p.node ?? "—" },
    { key: "age", title: t("objects.pods.age"), width: "70px", render: (p) => formatAge(p.created) },
    { key: "qos", title: "QoS", width: "90px", render: (p) => p.qos ?? "—" },
    { key: "owner", title: t("objects.pods.owner"), width: "minmax(120px, 1fr)", render: (p) => (p.owner ? `${p.owner.kind}/${p.owner.name}` : "—") },
  ];
  return (
    <VirtualTable
      label={t("objects.pods.title")}
      rows={rows}
      columns={columns}
      rowKey={podKey}
      onRowClick={(p) => open({ kind: "pod", connectionId, namespace, name: p.name })}
    />
  );
}
````

<!-- block: file t14-src-components-objects-watchstatusline-tsx step=14.3 path=src/components/objects/WatchStatusLine.tsx -->
````tsx
import { useTranslation } from "react-i18next";
import { tr } from "../../i18n/text";
import type { WatchState } from "../../lib/useWatchList";

/** Как обновляется список: загрузка, опрос без прав на watch, временная ошибка (watcher повторит сам). */
export function WatchStatusLine({ state }: { state: WatchState<unknown> }) {
  const { t } = useTranslation();
  if (state.status === "connecting") return <span className="muted">{t("common.loading")}</span>;
  if (state.status === "error") return null;
  return (
    <>
      {state.status === "polling" && <span className="muted">{t("objects.watch.polling")}</span>}
      {state.error && <span className="warn">{tr(state.error)}</span>}
    </>
  );
}
````

<!-- block: file t14-src-components-objects-workloadpanel-tsx step=14.3 path=src/components/objects/WorkloadPanel.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { WorkloadDetails, WorkloadKind } from "../../api/types";
import { formatAge, formatDateTime } from "../../lib/age";
import { Conditions } from "../common/Conditions";
import { KeyValues } from "../common/KeyValues";
import { PartView } from "../common/PartView";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
import { SubTabs } from "../common/SubTabs";
import { YamlView } from "../common/YamlView";
import { ContainerCard } from "./ContainerCard";
import { ObjectEvents } from "./EventList";
import { PodList } from "./PodList";

type Sub = "overview" | "pods" | "events" | "yaml";

/** Вкладка workload'а (спец. §6.4): обзор, его поды, события вместе с подами, YAML. */
export function WorkloadPanel({ connectionId, namespace, kind, name }: { connectionId: string; namespace: string; kind: WorkloadKind; name: string }) {
  const { t } = useTranslation();
  const [sub, setSub] = useState<Sub>("overview");
  const tabs: { id: Sub; label: string }[] = [
    { id: "overview", label: t("objects.tabs.overview") },
    { id: "pods", label: t("objects.tabs.pods") },
    { id: "events", label: t("objects.tabs.events") },
    { id: "yaml", label: "YAML" },
  ];
  return (
    <div className="page">
      <div className="toolbar">
        <h2>
          {kind} {name}
        </h2>
        <span className="muted">{namespace}</span>
      </div>
      <SubTabs tabs={tabs} active={sub} onChange={setSub} />
      {sub === "overview" && <WorkloadOverview connectionId={connectionId} namespace={namespace} kind={kind} name={name} />}
      {sub === "pods" && <PodList connectionId={connectionId} namespace={namespace} workload={{ kind, name }} />}
      {sub === "events" && <ObjectEvents connectionId={connectionId} namespace={namespace} object={{ kind, name }} />}
      {sub === "yaml" && <YamlView connectionId={connectionId} namespace={namespace} kind={kind} name={name} />}
    </div>
  );
}

function WorkloadOverview({ connectionId, namespace, kind, name }: { connectionId: string; namespace: string; kind: WorkloadKind; name: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const q = useQuery({
    queryKey: ["workload", connectionId, namespace, kind, name],
    queryFn: () => api.getWorkload(connectionId, namespace, kind, name),
    refetchInterval: interval || false,
  });
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && <WorkloadSummary d={q.data} />}
    </div>
  );
}

const num = (n: number | null | undefined) => (n == null ? "—" : String(n));

function WorkloadSummary({ d }: { d: WorkloadDetails }) {
  const { t } = useTranslation();
  const w = d.workload;
  return (
    <>
      {w.problem && <p className="problem">{t(`objects.workload.problem.${w.problem}`)}</p>}
      <dl className="stats">
        {w.kind === "CronJob" ? (
          <>
            <div>
              <dt>{t("objects.workload.schedule")}</dt>
              <dd>
                <code>{w.schedule ?? "—"}</code>
                {w.suspended && ` · ${t("objects.workload.suspended")}`}
              </dd>
            </div>
            <div>
              <dt>{t("objects.workload.lastSchedule")}</dt>
              <dd>{formatDateTime(w.lastSchedule)}</dd>
            </div>
            <div>
              <dt>{t("objects.workload.activeJobs")}</dt>
              <dd>{d.activeJobs.join(", ") || "—"}</dd>
            </div>
          </>
        ) : (
          <>
            <div>
              <dt>{w.kind === "Job" ? t("objects.workload.completions") : t("objects.workload.desired")}</dt>
              <dd>{num(w.desired)}</dd>
            </div>
            <div>
              <dt>{w.kind === "Job" ? t("objects.workload.succeeded") : t("objects.workload.ready")}</dt>
              <dd>{num(w.ready)}</dd>
            </div>
            {w.kind !== "Job" && (
              <>
                <div>
                  <dt>{t("objects.workload.available")}</dt>
                  <dd>{num(w.available)}</dd>
                </div>
                <div>
                  <dt>{t("objects.workload.updated")}</dt>
                  <dd>{num(w.updated)}</dd>
                </div>
              </>
            )}
          </>
        )}
      </dl>
      <dl className="facts">
        <dt>{t("objects.workload.selector")}</dt>
        <dd>{d.selector ? <code>{d.selector}</code> : "—"}</dd>
        <dt>{t("objects.workload.strategy")}</dt>
        <dd>{d.strategy ?? "—"}</dd>
        <dt>{t("objects.workload.created")}</dt>
        <dd>{formatDateTime(w.created)}</dd>
      </dl>
      <h3>{t("objects.workload.conditions")}</h3>
      <Conditions conditions={d.conditions} />
      <h3>HPA</h3>
      <PartView part={d.hpa}>
        {(h) =>
          h ? (
            <p>
              <code>{h.name}</code>: {num(h.minReplicas)}–{h.maxReplicas}, {t("objects.workload.hpaReplicas", { current: num(h.currentReplicas), desired: h.desiredReplicas })}
              {h.metrics.length > 0 && ` · ${h.metrics.join(", ")}`}
            </p>
          ) : (
            <p className="muted">—</p>
          )
        }
      </PartView>
      <h3>PodDisruptionBudget</h3>
      <PartView part={d.pdb}>
        {(p) =>
          p ? (
            <p>
              <code>{p.name}</code>: {p.minAvailable ? `minAvailable ${p.minAvailable}` : `maxUnavailable ${p.maxUnavailable ?? "—"}`},{" "}
              {t("objects.workload.pdbState", { allowed: p.allowedDisruptions, healthy: p.currentHealthy, desired: p.desiredHealthy })}
            </p>
          ) : (
            <p className="muted">—</p>
          )
        }
      </PartView>
      {d.history && (
        <>
          <h3>{t("objects.workload.history")}</h3>
          <PartView part={d.history}>
            {(revs) => (
              <table>
                <thead>
                  <tr>
                    <th>{t("objects.workload.revision")}</th>
                    <th>{t("objects.workload.revisionName")}</th>
                    <th>{t("objects.workload.images")}</th>
                    <th>{t("objects.workload.replicas")}</th>
                    <th>{t("objects.workload.age")}</th>
                  </tr>
                </thead>
                <tbody>
                  {revs.map((r) => (
                    <tr key={r.name} className={r.current ? "selected" : undefined}>
                      <td>
                        {r.revision}
                        {r.current && ` (${t("objects.workload.current")})`}
                      </td>
                      <td>{r.name}</td>
                      <td>{r.images.join(", ") || "—"}</td>
                      <td>{num(r.replicas)}</td>
                      <td>{formatAge(r.created)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </PartView>
        </>
      )}
      <h3>{t("objects.workload.containers")}</h3>
      <div className="cards">
        {d.containers.map((c) => (
          <ContainerCard key={`${c.init}:${c.name}`} c={c} />
        ))}
      </div>
      <h3>{t("objects.labels")}</h3>
      <KeyValues values={d.labels} />
      <h3>{t("objects.annotations")}</h3>
      <KeyValues values={d.annotations} />
    </>
  );
}
````

<!-- block: patch t14-src step=14.3 -->
````diff
diff --git a/src/components/Layout.tsx b/src/components/Layout.tsx
index c5906b5..a104be1 100644
--- a/src/components/Layout.tsx
+++ b/src/components/Layout.tsx
@@ -26,6 +26,10 @@ function tabTitle(tab: Tab, byId: Map<string, ConnectionProfile>): string {
       return i18n.t("tabs.cluster", { conn });
     case "nodes":
       return i18n.t("tabs.nodes", { conn });
+    // Имена объектов не переводятся; подключение видно по бейджу окружения.
+    case "workload":
+    case "pod":
+      return tab.name;
   }
 }
 
diff --git a/src/components/TabContent.tsx b/src/components/TabContent.tsx
index 88e55a7..69b1e82 100644
--- a/src/components/TabContent.tsx
+++ b/src/components/TabContent.tsx
@@ -4,6 +4,8 @@ import type { Tab } from "../state/workspace";
 import { AboutPanel } from "./about/AboutPanel";
 import { ConnectionPanel } from "./cluster/ConnectionPanel";
 import { NodesPanel } from "./cluster/NodesPanel";
+import { PodPanel } from "./objects/PodPanel";
+import { WorkloadPanel } from "./objects/WorkloadPanel";
 import { ConnectionFormTab } from "./connections/ConnectionFormTab";
 import { SettingsPanel } from "./settings/SettingsPanel";
 
@@ -46,5 +48,9 @@ export function TabContent({ tab }: { tab: Tab }) {
       return <ConnectionPanel connectionId={tab.connectionId} />;
     case "nodes":
       return <NodesPanel connectionId={tab.connectionId} />;
+    case "workload":
+      return <WorkloadPanel connectionId={tab.connectionId} namespace={tab.namespace} kind={tab.workloadKind} name={tab.name} />;
+    case "pod":
+      return <PodPanel connectionId={tab.connectionId} namespace={tab.namespace} name={tab.name} />;
   }
 }
diff --git a/src/state/workspace.ts b/src/state/workspace.ts
index 82f9a7e..b810342 100644
--- a/src/state/workspace.ts
+++ b/src/state/workspace.ts
@@ -1,9 +1,12 @@
 import { create } from "zustand";
+import type { WorkloadKind } from "../api/types";
 
 export type TabSpec =
   /** Обзор подключения и кластера: кто вошёл, права, API-сервер, ноды, у OpenShift — обновления и операторы. */
   | { kind: "cluster"; connectionId: string }
   | { kind: "nodes"; connectionId: string }
+  | { kind: "workload"; connectionId: string; namespace: string; workloadKind: WorkloadKind; name: string }
+  | { kind: "pod"; connectionId: string; namespace: string; name: string }
   /** folderId — папка нового подключения («Новое подключение здесь»). */
   | { kind: "connection-form"; connectionId: string | null; folderId?: string | null }
   | { kind: "about" }
@@ -23,6 +26,10 @@ export function tabId(spec: TabSpec): string {
       return "help";
     case "settings":
       return "settings";
+    case "workload":
+      return `workload:${spec.connectionId}:${spec.namespace}:${spec.workloadKind}:${spec.name}`;
+    case "pod":
+      return `pod:${spec.connectionId}:${spec.namespace}:${spec.name}`;
     default:
       return `${spec.kind}:${spec.connectionId}`;
   }
````

<!-- block: locales t14-locales step=14.3 -->
````json
{
  "en": {
    "objects": {
      "tabs": {
        "overview": "Overview",
        "pods": "Pods",
        "events": "Events"
      },
      "watch": {
        "polling": "No watch rights — the list refreshes every 10 s"
      },
      "pods": {
        "title": "Pods",
        "name": "Name",
        "status": "Status",
        "ready": "Ready",
        "restarts": "Restarts",
        "node": "Node",
        "age": "Age",
        "owner": "Owner",
        "filter": "Filter by name",
        "problemsOnly": "Problems only ({{count}})"
      },
      "pod": {
        "phase": "Phase",
        "ip": "Pod IP",
        "hostIp": "node {{ip}}",
        "serviceAccount": "Service account",
        "started": "Started",
        "volumes": "Volumes"
      },
      "events": {
        "title": "Events",
        "lastSeen": "Last",
        "type": "Type",
        "reason": "Reason",
        "object": "Object",
        "objectFilter": "Object, e.g. Pod/web",
        "message": "Message",
        "count": "Count",
        "all": "all"
      },
      "workload": {
        "problem": {
          "unavailable": "Fewer replicas are available than desired",
          "rolloutStuck": "The rollout exceeded its progress deadline",
          "failed": "The Job failed"
        },
        "desired": "Desired",
        "ready": "Ready",
        "available": "Available",
        "updated": "Updated",
        "completions": "Completions",
        "succeeded": "Succeeded",
        "schedule": "Schedule",
        "suspended": "suspended",
        "lastSchedule": "Last run",
        "activeJobs": "Running Jobs",
        "selector": "Pod selector",
        "strategy": "Strategy",
        "created": "Created",
        "conditions": "Conditions",
        "hpaReplicas": "replicas now {{current}}, needed {{desired}}",
        "pdbState": "disruptions allowed {{allowed}}, healthy {{healthy}} of {{desired}}",
        "history": "Rollout history",
        "revision": "Revision",
        "revisionName": "Name",
        "images": "Images",
        "replicas": "Replicas",
        "age": "Age",
        "current": "current",
        "containers": "Containers"
      },
      "container": {
        "init": "init container",
        "image": "Image",
        "state": "State",
        "lastState": "Last state",
        "ready": "Ready",
        "restarts": "Restarts",
        "memory": "Memory",
        "ports": "Ports",
        "liveness": "Liveness probe",
        "readiness": "Readiness probe",
        "startup": "Startup probe",
        "running": "running since {{since}}",
        "waiting": "waiting: {{reason}}",
        "terminated": "terminated: {{reason}}, exit code {{code}}, at {{at}}",
        "literal": "value in the spec",
        "env_one": "{{count}} environment variable (names and sources)",
        "env_other": "{{count}} environment variables (names and sources)"
      }
    }
  },
  "ru": {
    "objects": {
      "tabs": {
        "overview": "Обзор",
        "pods": "Поды",
        "events": "События"
      },
      "watch": {
        "polling": "Нет прав на watch — список обновляется раз в 10 с"
      },
      "pods": {
        "title": "Поды",
        "name": "Имя",
        "status": "Статус",
        "ready": "Готово",
        "restarts": "Рестарты",
        "node": "Нода",
        "age": "Возраст",
        "owner": "Владелец",
        "filter": "Фильтр по имени",
        "problemsOnly": "Только проблемные ({{count}})"
      },
      "pod": {
        "phase": "Фаза",
        "ip": "IP пода",
        "hostIp": "нода {{ip}}",
        "serviceAccount": "Сервисная учётная запись",
        "started": "Запущен",
        "volumes": "Тома"
      },
      "events": {
        "title": "События",
        "lastSeen": "Последнее",
        "type": "Тип",
        "reason": "Причина",
        "object": "Объект",
        "objectFilter": "Объект, например Pod/web",
        "message": "Сообщение",
        "count": "Сколько раз",
        "all": "все"
      },
      "workload": {
        "problem": {
          "unavailable": "Доступно меньше реплик, чем желаемо",
          "rolloutStuck": "Выкладка не уложилась в отведённое время (progressDeadlineSeconds)",
          "failed": "Job завершился ошибкой"
        },
        "desired": "Желаемо",
        "ready": "Готово",
        "available": "Доступно",
        "updated": "Обновлено",
        "completions": "Нужно завершений",
        "succeeded": "Успешно",
        "schedule": "Расписание",
        "suspended": "приостановлен",
        "lastSchedule": "Последний запуск",
        "activeJobs": "Идущие Job",
        "selector": "Селектор подов",
        "strategy": "Стратегия",
        "created": "Создан",
        "conditions": "Условия",
        "hpaReplicas": "реплик сейчас {{current}}, нужно {{desired}}",
        "pdbState": "можно прервать {{allowed}}, здоровых {{healthy}} из {{desired}}",
        "history": "История выкладки",
        "revision": "Ревизия",
        "revisionName": "Имя",
        "images": "Образы",
        "replicas": "Реплики",
        "age": "Возраст",
        "current": "текущая",
        "containers": "Контейнеры"
      },
      "container": {
        "init": "init-контейнер",
        "image": "Образ",
        "state": "Состояние",
        "lastState": "Прошлое состояние",
        "ready": "Готов",
        "restarts": "Рестарты",
        "memory": "Память",
        "ports": "Порты",
        "liveness": "Liveness-проба",
        "readiness": "Readiness-проба",
        "startup": "Startup-проба",
        "running": "работает с {{since}}",
        "waiting": "ждёт: {{reason}}",
        "terminated": "завершён: {{reason}}, код {{code}}, {{at}}",
        "literal": "значение в спецификации",
        "env_one": "{{count}} переменная окружения (имена и источники)",
        "env_few": "{{count}} переменные окружения (имена и источники)",
        "env_many": "{{count}} переменных окружения (имена и источники)",
        "env_other": "{{count}} переменной окружения (имена и источники)"
      }
    }
  }
}
````

- [ ] **Step 4: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 43 passed`, `Tests 243 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 136`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 5: Commit.**

```bash
git add \
  src/components/Layout.tsx \
  src/components/TabContent.tsx \
  src/components/objects/ContainerCard.tsx \
  src/components/objects/EventList.tsx \
  src/components/objects/EventsTable.tsx \
  src/components/objects/PodList.test.tsx \
  src/components/objects/PodList.tsx \
  src/components/objects/PodPanel.test.tsx \
  src/components/objects/PodPanel.tsx \
  src/components/objects/PodsTable.tsx \
  src/components/objects/WatchStatusLine.tsx \
  src/components/objects/WorkloadPanel.test.tsx \
  src/components/objects/WorkloadPanel.tsx \
  src/locales/en.json \
  src/locales/ru.json \
  src/state/workspace.test.ts \
  src/state/workspace.ts \
  src/test/objects.ts
git commit -s -m "feat(ui): вкладки workload'а и пода — обзор, контейнеры, HPA, PDB, история выкладки, поды через watch, события, YAML" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 15: Вкладка проекта и проекты в дереве

**Files:**
- Create: `src/components/connections/tree/ConnectionNode.test.tsx`, `src/components/project/ProjectOverview.tsx`, `src/components/project/ProjectPanel.test.tsx`, `src/components/project/ProjectPanel.tsx`, `src/components/project/WorkloadsTable.tsx`, `src/components/project/access.ts`
- Modify: `docs/i18n/same-as-en.json`, `src/components/Layout.tsx`, `src/components/TabContent.tsx`, `src/components/connections/tree/ConnectionNode.tsx`, `src/components/objects/EventList.tsx`, `src/locales/en.json`, `src/locales/ru.json`, `src/state/workspace.test.ts`, `src/state/workspace.ts`

**Interfaces:**
- Consumes: задачи 12–14; команды задач 5–7, 9, 10.
- Produces: `components/project/{ProjectPanel({ connectionId, namespace }), ProjectOverview, WorkloadsTable, canList(access, ...keys), WORKLOAD_RESOURCES}`; `components/objects/ProjectEvents({ connectionId, namespace })`; `TabSpec` `{ kind: "project", connectionId, namespace }` (id `project:<id>:<ns>`, заголовок «<проект> · <подключение>»); в дереве у подключения — «Проекты» (раскрываются по щелчку, избранные ★ сверху, фильтр от 8 проектов, проекты из профиля с пояснением) и «Ноды» (по сведениям вкладки подключения).

Спец. §6.1, §6.3, §5.4. Вкладка проекта читает права в проекте и не показывает подвкладки, на которые прав нет (права неизвестны или `incomplete` — показывает). «Обзор»: поды по фазам и проблемные поды, workloads не в норме, квоты, занятые на 80 % и больше, события `Warning` за час — из тех же списков, что и подвкладки (команда `get_project_overview` не нужна). «Workloads» — через watch, с фильтром по виду и имени и видами без прав отдельной строкой; «Поды» и «События» — через watch. Дерево само не подключается: «Проекты» читают список по щелчку, «Ноды» появляются, когда вкладка подключения уже знает права.

- [ ] **Step 1: Тесты.**

<!-- block: file t15-src-components-connections-tree-connectionnode-test-tsx step=15.1 path=src/components/connections/tree/ConnectionNode.test.tsx -->
````tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../../test/render";

vi.mock("../../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../../api/client")>();
  return { ...orig, api: { listProjects: vi.fn(), connect: vi.fn() } };
});

import { api } from "../../../api/client";
import type { ConnectionInfo, ProjectRow } from "../../../api/types";
import { useWorkspace } from "../../../state/workspace";
import { newProfile } from "../profile";
import { ConnectionNode } from "./ConnectionNode";

const conn = { ...newProfile(), id: "c1", name: "dev" };
const project = (name: string, favorite = false, fromProfile = false): ProjectRow => ({ name, displayName: null, phase: "Active", favorite, fromProfile });

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("ConnectionNode", () => {
  it("проекты по щелчку: избранные сверху, фильтр, открытие проекта", async () => {
    vi.mocked(api.listProjects).mockResolvedValue({
      items: [project("alpha"), project("team-b", true), ...["c", "d", "e", "f", "g", "h"].map((x) => project(`p-${x}`))],
      truncated: false,
    });
    const user = userEvent.setup();
    renderWithClient(
      <ul>
        <ConnectionNode connection={conn} />
      </ul>,
    );
    expect(api.listProjects).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: /Проекты/ }));
    expect(await screen.findByRole("button", { name: "★ team-b" })).toBeInTheDocument();
    const names = screen.getAllByRole("button").map((b) => b.textContent);
    expect(names.indexOf("★ team-b")).toBeLessThan(names.indexOf("alpha"));
    await user.type(screen.getByLabelText("Фильтр проектов"), "alp");
    expect(screen.queryByRole("button", { name: "★ team-b" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "alpha" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "project", connectionId: "c1", namespace: "alpha" });
    expect(api.connect).not.toHaveBeenCalled();
  });

  it("без прав на список — проекты из профиля с пояснением; ноды — при правах из сведений подключения", async () => {
    vi.mocked(api.listProjects).mockResolvedValue({
      items: [project("team-a", true, true)],
      truncated: false,
      error: { code: "FORBIDDEN", key: "errors.k8s.forbiddenCluster", params: { verb: "list", resource: "namespaces" }, message: "x", retryable: false },
    });
    const user = userEvent.setup();
    const { client } = renderWithClient(
      <ul>
        <ConnectionNode connection={conn} />
      </ul>,
    );
    expect(screen.queryByRole("button", { name: "Ноды" })).not.toBeInTheDocument();
    client.setQueryData<Partial<ConnectionInfo>>(["connection", "c1"], {
      capabilities: { listProjects: false, listNodes: true, listAllPods: false, readClusterOperators: false, readClusterVersion: false },
    });
    await user.click(await screen.findByRole("button", { name: "Ноды" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "nodes", connectionId: "c1" });
    await user.click(screen.getByRole("button", { name: /Проекты/ }));
    expect(await screen.findByText("Кластер не даёт список проектов — показаны проекты из профиля")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "★ team-a" })).toBeInTheDocument();
  });
});
````

<!-- block: file t15-src-components-project-projectpanel-test-tsx step=15.1 path=src/components/project/ProjectPanel.test.tsx -->
````tsx
import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 28 })),
    getTotalSize: () => o.count * 28,
  }),
}));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getNamespaceAccess: vi.fn(),
      listPods: vi.fn(),
      listWorkloads: vi.fn(),
      getQuotas: vi.fn(),
      listEvents: vi.fn(),
      startWatch: vi.fn(),
      stopSession: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import type { NamespaceAccess, WatchEvent } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { podRow, workloadRow } from "../../test/objects";
import { ProjectPanel } from "./ProjectPanel";

let emit: (e: WatchEvent) => void = () => undefined;

const ALL: NamespaceAccess = { resources: {}, incomplete: false };
const now = () => Math.floor(Date.now() / 1000);

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspace.setState({ tabs: [], activeId: null });
  vi.mocked(api.getNamespaceAccess).mockResolvedValue(ALL);
  vi.mocked(api.listPods).mockResolvedValue({
    items: [podRow("web-a"), podRow("web-b", { phase: "Pending", status: "ImagePullBackOff", problem: "imagePull" })],
    truncated: false,
  });
  vi.mocked(api.listWorkloads).mockResolvedValue({ items: [workloadRow("web", { problem: "unavailable" }), workloadRow("api")], errors: [], truncated: false });
  vi.mocked(api.getQuotas).mockResolvedValue({
    quotas: { data: [{ name: "compute", scopes: [], entries: [{ resource: "requests.cpu", used: "3600m", hard: "4", ratio: 0.9 }, { resource: "pods", used: "2", hard: "20", ratio: 0.1 }] }] },
    limitRanges: { data: [] },
  });
  vi.mocked(api.listEvents).mockResolvedValue({
    items: [
      { key: "e1", type: "Warning", reason: "Failed", object: { kind: "Pod", name: "web-b" }, message: "image not found", count: 3, firstSeen: now() - 600, lastSeen: now() - 60, source: "kubelet" },
      { key: "e2", type: "Warning", reason: "Old", object: { kind: "Pod", name: "x" }, message: "long ago", count: 1, firstSeen: now() - 7200, lastSeen: now() - 7200, source: null },
    ],
    truncated: false,
  });
  vi.mocked(api.startWatch).mockImplementation(async (_p, onEvent) => {
    emit = onEvent;
    return "s1";
  });
  vi.mocked(api.stopSession).mockResolvedValue(true);
});

describe("ProjectPanel", () => {
  it("обзор: поды по фазам и проблемам, workloads не в норме, квоты у предела, Warning за час", async () => {
    const user = userEvent.setup();
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    expect(await screen.findByRole("button", { name: "web-b" })).toBeInTheDocument();
    expect(screen.getByText("ImagePullBackOff")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Deployment/web" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Deployment/api" })).not.toBeInTheDocument();
    expect(await screen.findByText(/3600m \/ 4 \(90 %\)/)).toBeInTheDocument();
    expect(screen.getByText(/image not found/)).toBeInTheDocument();
    expect(screen.queryByText(/long ago/)).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "web-b" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "pod", name: "web-b" });
  });

  it("workloads через watch, виды без прав — отдельно; открытие workload'а", async () => {
    const user = userEvent.setup();
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    await user.click(await screen.findByRole("tab", { name: "Workloads" }));
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledWith({ connectionId: "c1", namespace: "team-a", kind: "workloads" }, expect.any(Function)));
    act(() =>
      emit({
        type: "reset",
        items: [workloadRow("web"), workloadRow("report", { kind: "CronJob", schedule: "0 3 * * *" })],
        errors: [{ kind: "DaemonSet", error: { code: "FORBIDDEN", key: "errors.k8s.forbiddenIn", params: { verb: "list", resource: "daemonsets", namespace: "team-a" }, message: "x", retryable: false } }],
      }),
    );
    expect(screen.getByText("DaemonSet: — Нет прав: list daemonsets в проекте «team-a»")).toBeInTheDocument();
    expect(screen.getByText("0 3 * * *")).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Вид"), "CronJob");
    expect(screen.queryByText("web")).not.toBeInTheDocument();
    await user.click(screen.getByText("report"));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "workload", workloadKind: "CronJob", name: "report" });
  });

  it("подвкладки без прав на чтение не показываются", async () => {
    vi.mocked(api.getNamespaceAccess).mockResolvedValue({
      resources: { pods: { get: true, list: true, watch: false }, events: { get: false, list: false, watch: false } },
      incomplete: false,
    });
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    await waitFor(() => expect(screen.queryByRole("tab", { name: "События" })).not.toBeInTheDocument());
    expect(screen.getByRole("tab", { name: "Поды" })).toBeInTheDocument();
  });
});
````

<!-- block: patch t15-tests step=15.1 -->
````diff
diff --git a/src/state/workspace.test.ts b/src/state/workspace.test.ts
index 4545175..631a8d0 100644
--- a/src/state/workspace.test.ts
+++ b/src/state/workspace.test.ts
@@ -36,6 +36,7 @@ describe("workspace", () => {
     expect(tabId({ kind: "cluster", connectionId: "c" })).toBe("cluster:c");
     expect(tabId({ kind: "help", chapter: "connections" })).toBe("help");
     expect(tabId({ kind: "nodes", connectionId: "c" })).toBe("nodes:c");
+    expect(tabId({ kind: "project", connectionId: "c", namespace: "team-a" })).toBe("project:c:team-a");
     expect(tabId({ kind: "workload", connectionId: "c", namespace: "team-a", workloadKind: "Deployment", name: "web" })).toBe("workload:c:team-a:Deployment:web");
     expect(tabId({ kind: "pod", connectionId: "c", namespace: "team-a", name: "web-1" })).toBe("pod:c:team-a:web-1");
   });
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/connections/tree/ConnectionNode.test.tsx src/components/project/ProjectPanel.test.tsx src/state/workspace.test.ts`  
Expected: FAIL — нет `ProjectPanel` и проектов в дереве; у `TabSpec` нет вида `project`.

- [ ] **Step 3: Реализация.**

<!-- block: file t15-src-components-project-projectoverview-tsx step=15.3 path=src/components/project/ProjectOverview.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { PodRow } from "../../api/types";
import { formatAge } from "../../lib/age";
import { useWorkspace } from "../../state/workspace";
import { PartView } from "../common/PartView";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";

/** Последние события Warning — за этот срок (спец. §6.3). */
const WARNINGS_FOR_SEC = 3600;
/** Квота показывается, когда занята на столько. */
const QUOTA_WARN = 0.8;
/** Больше стольких строк в каждом списке обзора не показывается. */
const MAX_ITEMS = 20;

const PHASES = ["Running", "Pending", "Succeeded", "Failed", "Unknown"];

/** Обзор проекта: поды по фазам и проблемам, workloads не в норме, квоты у предела, Warning за час. */
export function ProjectOverview({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const [interval, setInterval] = useRefreshInterval();
  const refetchInterval = interval || false;
  const pods = useQuery({ queryKey: ["pods", connectionId, namespace, null], queryFn: () => api.listPods(connectionId, namespace), refetchInterval });
  const workloads = useQuery({ queryKey: ["workloads", connectionId, namespace], queryFn: () => api.listWorkloads(connectionId, namespace), refetchInterval });
  const quotas = useQuery({ queryKey: ["quotas", connectionId, namespace], queryFn: () => api.getQuotas(connectionId, namespace), refetchInterval });
  const events = useQuery({ queryKey: ["events", connectionId, namespace, null], queryFn: () => api.listEvents(connectionId, namespace), refetchInterval });
  const busy = pods.isFetching || workloads.isFetching || quotas.isFetching || events.isFetching;
  const refresh = () => Promise.all([pods.refetch(), workloads.refetch(), quotas.refetch(), events.refetch()]);
  const openPod = (p: PodRow) => open({ kind: "pod", connectionId, namespace, name: p.name });
  const since = Date.now() / 1000 - WARNINGS_FOR_SEC;
  const warnings = (events.data?.items ?? []).filter((e) => e.type === "Warning" && (e.lastSeen ?? 0) >= since).slice(0, MAX_ITEMS);
  const problemPods = (pods.data?.items ?? []).filter((p) => p.problem);
  const problemWorkloads = (workloads.data?.items ?? []).filter((w) => w.problem);
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void refresh()} busy={busy} />
      <h3>{t("project.overview.pods")}</h3>
      {pods.error && <ReadError error={pods.error} connectionId={connectionId} onRetry={() => void pods.refetch()} />}
      {pods.data && (
        <>
          <dl className="stats">
            {PHASES.map((phase) => (
              <div key={phase}>
                <dt>{phase}</dt>
                <dd>{pods.data.items.filter((p) => p.phase === phase).length}</dd>
              </div>
            ))}
          </dl>
          {problemPods.length === 0 ? (
            <p className="muted">{t("project.overview.noProblemPods")}</p>
          ) : (
            <ul>
              {problemPods.slice(0, MAX_ITEMS).map((p) => (
                <li key={p.name}>
                  <button type="button" className="link" onClick={() => openPod(p)}>
                    {p.name}
                  </button>{" "}
                  <span className="problem">{p.status}</span> <span className="muted">{t("project.overview.restarts", { count: p.restarts })}</span>
                </li>
              ))}
            </ul>
          )}
        </>
      )}
      <h3>{t("project.overview.workloads")}</h3>
      {workloads.error && <ReadError error={workloads.error} connectionId={connectionId} onRetry={() => void workloads.refetch()} />}
      {workloads.data &&
        (problemWorkloads.length === 0 ? (
          <p className="muted">{t("project.overview.noProblemWorkloads")}</p>
        ) : (
          <ul>
            {problemWorkloads.slice(0, MAX_ITEMS).map((w) => (
              <li key={`${w.kind}/${w.name}`}>
                <button type="button" className="link" onClick={() => open({ kind: "workload", connectionId, namespace, workloadKind: w.kind, name: w.name })}>
                  {w.kind}/{w.name}
                </button>{" "}
                <span className="problem">{t(`objects.workload.problem.${w.problem}`)}</span>
              </li>
            ))}
          </ul>
        ))}
      <h3>{t("project.overview.quotas")}</h3>
      {quotas.error && <ReadError error={quotas.error} connectionId={connectionId} onRetry={() => void quotas.refetch()} />}
      {quotas.data && (
        <PartView part={quotas.data.quotas}>
          {(list) => {
            const near = list.flatMap((q) => q.entries.filter((e) => (e.ratio ?? 0) >= QUOTA_WARN).map((e) => ({ quota: q.name, ...e })));
            if (list.length === 0) return <p className="muted">{t("project.overview.noQuotas")}</p>;
            if (near.length === 0) return <p className="muted">{t("project.overview.quotasOk")}</p>;
            return (
              <ul>
                {near.map((e) => (
                  <li key={`${e.quota}/${e.resource}`} className="warn">
                    {e.quota}: <code>{e.resource}</code> {e.used} / {e.hard} ({Math.round((e.ratio ?? 0) * 100)} %)
                  </li>
                ))}
              </ul>
            );
          }}
        </PartView>
      )}
      <h3>{t("project.overview.warnings")}</h3>
      {events.error && <ReadError error={events.error} connectionId={connectionId} onRetry={() => void events.refetch()} />}
      {events.data &&
        (warnings.length === 0 ? (
          <p className="muted">{t("project.overview.noWarnings")}</p>
        ) : (
          <ul>
            {warnings.map((e) => (
              <li key={e.key}>
                <span className="muted">{formatAge(e.lastSeen)}</span> <strong>{e.reason}</strong> {e.object.kind}/{e.object.name}: {e.message}
              </li>
            ))}
          </ul>
        ))}
    </div>
  );
}
````

<!-- block: file t15-src-components-project-projectpanel-tsx step=15.3 path=src/components/project/ProjectPanel.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { WorkloadRow } from "../../api/types";
import { tr } from "../../i18n/text";
import { useWatchList, workloadKey } from "../../lib/useWatchList";
import { ReadError } from "../common/ReadError";
import { SubTabs } from "../common/SubTabs";
import { ProjectEvents } from "../objects/EventList";
import { PodList } from "../objects/PodList";
import { WatchStatusLine } from "../objects/WatchStatusLine";
import { WORKLOAD_RESOURCES, canList } from "./access";
import { ProjectOverview } from "./ProjectOverview";
import { WorkloadsTable } from "./WorkloadsTable";

type Sub = "overview" | "workloads" | "pods" | "events";

/** Вкладка проекта (спец. §6.3). Подвкладки без прав на чтение не показываются (§5.4); показывается одна, её watch идёт, пока она открыта. */
export function ProjectPanel({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const { t } = useTranslation();
  const [sub, setSub] = useState<Sub>("overview");
  const access = useQuery({ queryKey: ["access", connectionId, namespace], queryFn: () => api.getNamespaceAccess(connectionId, namespace) });
  const a = access.data;
  const tabs: { id: Sub; label: string; visible: boolean }[] = [
    { id: "overview", label: t("objects.tabs.overview"), visible: true },
    { id: "workloads", label: t("project.tabs.workloads"), visible: canList(a, ...WORKLOAD_RESOURCES) },
    { id: "pods", label: t("objects.tabs.pods"), visible: canList(a, "pods") },
    { id: "events", label: t("objects.tabs.events"), visible: canList(a, "events") },
  ];
  const visible = tabs.filter((x) => x.visible);
  const active = visible.some((x) => x.id === sub) ? sub : "overview";
  return (
    <div className="page">
      <div className="toolbar">
        <h2>{namespace}</h2>
      </div>
      {access.error && <ReadError error={access.error} connectionId={connectionId} onRetry={() => void access.refetch()} />}
      <SubTabs tabs={visible} active={active} onChange={setSub} />
      {active === "overview" && <ProjectOverview connectionId={connectionId} namespace={namespace} />}
      {active === "workloads" && <ProjectWorkloads connectionId={connectionId} namespace={namespace} />}
      {active === "pods" && <PodList connectionId={connectionId} namespace={namespace} />}
      {active === "events" && <ProjectEvents connectionId={connectionId} namespace={namespace} />}
    </div>
  );
}

/** Workloads проекта через watch; виды без прав называются отдельно. */
function ProjectWorkloads({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const watch = useWatchList<WorkloadRow>({ connectionId, namespace, kind: "workloads" }, workloadKey);
  return (
    <div className="section">
      <WatchStatusLine state={watch} />
      {watch.status === "error" && watch.error && <ReadError error={watch.error} connectionId={connectionId} />}
      {watch.kindErrors.map((e) => (
        <p key={e.kind} className="muted">
          {e.kind}: — {tr(e.error)}
        </p>
      ))}
      <WorkloadsTable connectionId={connectionId} namespace={namespace} rows={watch.rows} />
    </div>
  );
}
````

<!-- block: file t15-src-components-project-workloadstable-tsx step=15.3 path=src/components/project/WorkloadsTable.tsx -->
````tsx
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import type { WorkloadKind, WorkloadRow } from "../../api/types";
import { formatAge } from "../../lib/age";
import { workloadKey } from "../../lib/useWatchList";
import { useWorkspace } from "../../state/workspace";
import { type Column, VirtualTable } from "../common/VirtualTable";

const KINDS: WorkloadKind[] = ["Deployment", "StatefulSet", "DaemonSet", "DeploymentConfig", "Job", "CronJob"];

/** Готово / желаемо; у CronJob — расписание, у Job — успешно / нужно. */
function readyText(w: WorkloadRow): string {
  if (w.kind === "CronJob") return w.schedule ?? "—";
  return `${w.ready ?? "—"}/${w.desired ?? "—"}`;
}

/** Таблица workloads проекта (спец. §6.3) с фильтрами по виду и имени; щелчок открывает workload. */
export function WorkloadsTable({ connectionId, namespace, rows }: { connectionId: string; namespace: string; rows: WorkloadRow[] }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const [kind, setKind] = useState<WorkloadKind | "">("");
  const [filter, setFilter] = useState("");
  const shown = useMemo(() => {
    const f = filter.trim().toLowerCase();
    return rows.filter((w) => (!kind || w.kind === kind) && (!f || w.name.toLowerCase().includes(f))).sort((a, b) => a.name.localeCompare(b.name) || a.kind.localeCompare(b.kind));
  }, [rows, kind, filter]);
  const columns: Column<WorkloadRow>[] = [
    { key: "kind", title: t("project.workloads.kind"), width: "130px", render: (w) => w.kind },
    { key: "name", title: t("project.workloads.name"), width: "minmax(180px, 2fr)", render: (w) => w.name },
    { key: "ready", title: t("project.workloads.ready"), width: "110px", render: (w) => <span className={w.problem ? "problem" : undefined}>{readyText(w)}</span> },
    { key: "images", title: t("project.workloads.images"), width: "minmax(200px, 3fr)", render: (w) => <span title={w.images.join(", ")}>{w.images.join(", ")}</span> },
    { key: "age", title: t("project.workloads.age"), width: "70px", render: (w) => formatAge(w.created) },
  ];
  return (
    <>
      <div className="toolbar">
        <label className="inline">
          {t("project.workloads.kind")}
          <select value={kind} onChange={(e) => setKind(e.target.value as WorkloadKind | "")}>
            <option value="">{t("objects.events.all")}</option>
            {KINDS.map((k) => (
              <option key={k} value={k}>
                {k}
              </option>
            ))}
          </select>
        </label>
        <input aria-label={t("objects.pods.filter")} placeholder={t("objects.pods.filter")} value={filter} onChange={(e) => setFilter(e.target.value)} />
      </div>
      <VirtualTable
        label={t("project.workloads.title")}
        rows={shown}
        columns={columns}
        rowKey={workloadKey}
        onRowClick={(w) => open({ kind: "workload", connectionId, namespace, workloadKind: w.kind, name: w.name })}
      />
    </>
  );
}
````

<!-- block: file t15-src-components-project-access-ts step=15.3 path=src/components/project/access.ts -->
````ts
import type { NamespaceAccess } from "../../api/types";

/**
 * Можно ли читать список ресурса в проекте (спец. §5.4): права ещё неизвестны, не перечислены
 * целиком (`incomplete`) или ресурс не из списка — «можно», окончательно решает API-сервер.
 * `key` — `ресурс.группа`: `pods`, `deployments.apps`.
 */
export function canList(access: NamespaceAccess | undefined, ...keys: string[]): boolean {
  if (!access || access.incomplete) return true;
  return keys.some((k) => access.resources[k]?.list ?? true);
}

export const WORKLOAD_RESOURCES = [
  "deployments.apps",
  "statefulsets.apps",
  "daemonsets.apps",
  "jobs.batch",
  "cronjobs.batch",
  "deploymentconfigs.apps.openshift.io",
];
````

<!-- block: patch t15-src step=15.3 -->
````diff
diff --git a/docs/i18n/same-as-en.json b/docs/i18n/same-as-en.json
index 0679a0b..6d6d4c5 100644
--- a/docs/i18n/same-as-en.json
+++ b/docs/i18n/same-as-en.json
@@ -3,6 +3,8 @@
     "connections.form.step.tcp",
     "connections.form.step.tls",
     "cluster.nodes.taints",
-    "cluster.nodes.version"
+    "cluster.nodes.version",
+    "project.tabs.workloads",
+    "project.workloads.title"
   ]
 }
diff --git a/src/components/Layout.tsx b/src/components/Layout.tsx
index a104be1..0acefd8 100644
--- a/src/components/Layout.tsx
+++ b/src/components/Layout.tsx
@@ -26,6 +26,8 @@ function tabTitle(tab: Tab, byId: Map<string, ConnectionProfile>): string {
       return i18n.t("tabs.cluster", { conn });
     case "nodes":
       return i18n.t("tabs.nodes", { conn });
+    case "project":
+      return `${tab.namespace} · ${conn}`;
     // Имена объектов не переводятся; подключение видно по бейджу окружения.
     case "workload":
     case "pod":
diff --git a/src/components/TabContent.tsx b/src/components/TabContent.tsx
index 69b1e82..752efd4 100644
--- a/src/components/TabContent.tsx
+++ b/src/components/TabContent.tsx
@@ -5,6 +5,7 @@ import { AboutPanel } from "./about/AboutPanel";
 import { ConnectionPanel } from "./cluster/ConnectionPanel";
 import { NodesPanel } from "./cluster/NodesPanel";
 import { PodPanel } from "./objects/PodPanel";
+import { ProjectPanel } from "./project/ProjectPanel";
 import { WorkloadPanel } from "./objects/WorkloadPanel";
 import { ConnectionFormTab } from "./connections/ConnectionFormTab";
 import { SettingsPanel } from "./settings/SettingsPanel";
@@ -48,6 +49,8 @@ export function TabContent({ tab }: { tab: Tab }) {
       return <ConnectionPanel connectionId={tab.connectionId} />;
     case "nodes":
       return <NodesPanel connectionId={tab.connectionId} />;
+    case "project":
+      return <ProjectPanel connectionId={tab.connectionId} namespace={tab.namespace} />;
     case "workload":
       return <WorkloadPanel connectionId={tab.connectionId} namespace={tab.namespace} kind={tab.workloadKind} name={tab.name} />;
     case "pod":
diff --git a/src/components/connections/tree/ConnectionNode.tsx b/src/components/connections/tree/ConnectionNode.tsx
index 46d3c80..40877cd 100644
--- a/src/components/connections/tree/ConnectionNode.tsx
+++ b/src/components/connections/tree/ConnectionNode.tsx
@@ -1,16 +1,26 @@
-import type { HTMLAttributes } from "react";
+import { useQuery } from "@tanstack/react-query";
+import { type HTMLAttributes, useMemo, useState } from "react";
 import { useTranslation } from "react-i18next";
+import { api, toAppError } from "../../../api/client";
 import type { ConnectionProfile } from "../../../api/types";
+import { tr } from "../../../i18n/text";
 import { useWorkspace } from "../../../state/workspace";
 import { EnvBadge } from "../../common/EnvBadge";
 
+/** С какого числа проектов показывается фильтр. */
+const FILTER_FROM = 8;
+
 /**
- * Узел подключения в дереве: точка цвета, имя, окружение и пункты подключения
- * (этап O1b добавит «Проекты» и «Ноды»). rowProps — атрибуты строки (перетаскивание).
+ * Узел подключения в дереве: точка цвета, имя, окружение и пункты подключения — «Кластер», «Проекты»
+ * (разворачиваются и читаются по щелчку) и «Ноды», если на них есть права. rowProps — атрибуты строки
+ * (перетаскивание).
  */
 export function ConnectionNode({ connection: c, rowProps }: { connection: ConnectionProfile; rowProps?: HTMLAttributes<HTMLDivElement> }) {
   const { t } = useTranslation();
   const open = useWorkspace((s) => s.open);
+  const [projectsOpen, setProjectsOpen] = useState(false);
+  // Сведения о подключении — из вкладки подключения; само дерево не подключается.
+  const info = useQuery({ queryKey: ["connection", c.id], queryFn: () => api.connect(c.id, false), enabled: false });
 
   return (
     <li>
@@ -28,7 +38,60 @@ export function ConnectionNode({ connection: c, rowProps }: { connection: Connec
             {t("sidebar.cluster")}
           </button>
         </li>
+        <li>
+          <button type="button" className="tree-folder-toggle" aria-expanded={projectsOpen} onClick={() => setProjectsOpen((v) => !v)}>
+            <span className="tree-caret">{projectsOpen ? "▾" : "▸"}</span>
+            {t("sidebar.projects")}
+          </button>
+          {projectsOpen && <ProjectsNode connectionId={c.id} />}
+        </li>
+        {info.data?.capabilities.listNodes && (
+          <li>
+            <button type="button" className="link" onClick={() => open({ kind: "nodes", connectionId: c.id })}>
+              {t("sidebar.nodes")}
+            </button>
+          </li>
+        )}
       </ul>
     </li>
   );
 }
+
+/** Проекты подключения (спец. §6.1): избранные сверху, фильтр; без прав на список — проекты из профиля. */
+function ProjectsNode({ connectionId }: { connectionId: string }) {
+  const { t } = useTranslation();
+  const open = useWorkspace((s) => s.open);
+  const [filter, setFilter] = useState("");
+  const q = useQuery({ queryKey: ["projects", connectionId], queryFn: () => api.listProjects(connectionId) });
+  const all = q.data?.items;
+  const items = useMemo(() => {
+    const f = filter.trim().toLowerCase();
+    return (all ?? [])
+      .filter((p) => !f || p.name.toLowerCase().includes(f) || p.displayName?.toLowerCase().includes(f))
+      .sort((a, b) => Number(b.favorite) - Number(a.favorite) || a.name.localeCompare(b.name));
+  }, [all, filter]);
+  return (
+    <>
+      {q.isPending && <p className="tree-notice">{t("common.loading")}</p>}
+      {q.error && (
+        <p className="tree-error" role="alert">
+          {tr(toAppError(q.error))}
+        </p>
+      )}
+      {q.data?.error && <p className="tree-notice">{t("sidebar.projectsFromProfile")}</p>}
+      {all && all.length >= FILTER_FROM && (
+        <input className="tree-filter" aria-label={t("sidebar.projectsFilter")} placeholder={t("sidebar.projectsFilter")} value={filter} onChange={(e) => setFilter(e.target.value)} />
+      )}
+      {all && all.length === 0 && <p className="tree-notice">{t("sidebar.noProjects")}</p>}
+      <ul>
+        {items.map((p) => (
+          <li key={p.name}>
+            <button type="button" className="link" title={p.displayName ?? undefined} onClick={() => open({ kind: "project", connectionId, namespace: p.name })}>
+              {p.favorite ? `★ ${p.name}` : p.name}
+            </button>
+          </li>
+        ))}
+      </ul>
+    </>
+  );
+}
diff --git a/src/components/objects/EventList.tsx b/src/components/objects/EventList.tsx
index 002ce6b..688d95b 100644
--- a/src/components/objects/EventList.tsx
+++ b/src/components/objects/EventList.tsx
@@ -1,10 +1,24 @@
 import { useQuery } from "@tanstack/react-query";
 import { useTranslation } from "react-i18next";
 import { api } from "../../api/client";
-import type { EventObject } from "../../api/types";
+import type { EventObject, EventRow } from "../../api/types";
+import { eventKey, useWatchList } from "../../lib/useWatchList";
 import { ReadError } from "../common/ReadError";
 import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
 import { EventsTable } from "./EventsTable";
+import { WatchStatusLine } from "./WatchStatusLine";
+
+/** События всего проекта: обновляются сами (watch или опрос). */
+export function ProjectEvents({ connectionId, namespace }: { connectionId: string; namespace: string }) {
+  const watch = useWatchList<EventRow>({ connectionId, namespace, kind: "events" }, eventKey);
+  return (
+    <div className="section">
+      <WatchStatusLine state={watch} />
+      {watch.status === "error" && watch.error && <ReadError error={watch.error} connectionId={connectionId} />}
+      <EventsTable rows={watch.rows} />
+    </div>
+  );
+}
 
 /** События объекта (у workload'а — вместе с его подами): «Обновить» и автообновление. */
 export function ObjectEvents({ connectionId, namespace, object }: { connectionId: string; namespace: string; object: EventObject }) {
diff --git a/src/state/workspace.ts b/src/state/workspace.ts
index b810342..dfde7fe 100644
--- a/src/state/workspace.ts
+++ b/src/state/workspace.ts
@@ -5,6 +5,7 @@ export type TabSpec =
   /** Обзор подключения и кластера: кто вошёл, права, API-сервер, ноды, у OpenShift — обновления и операторы. */
   | { kind: "cluster"; connectionId: string }
   | { kind: "nodes"; connectionId: string }
+  | { kind: "project"; connectionId: string; namespace: string }
   | { kind: "workload"; connectionId: string; namespace: string; workloadKind: WorkloadKind; name: string }
   | { kind: "pod"; connectionId: string; namespace: string; name: string }
   /** folderId — папка нового подключения («Новое подключение здесь»). */
@@ -26,6 +27,8 @@ export function tabId(spec: TabSpec): string {
       return "help";
     case "settings":
       return "settings";
+    case "project":
+      return `project:${spec.connectionId}:${spec.namespace}`;
     case "workload":
       return `workload:${spec.connectionId}:${spec.namespace}:${spec.workloadKind}:${spec.name}`;
     case "pod":
````

<!-- block: locales t15-locales step=15.3 -->
````json
{
  "en": {
    "sidebar": {
      "projects": "Projects",
      "nodes": "Nodes",
      "projectsFilter": "Filter projects",
      "projectsFromProfile": "The cluster does not list projects for you — the projects of the profile are shown",
      "noProjects": "No projects"
    },
    "project": {
      "tabs": { "workloads": "Workloads" },
      "workloads": {
        "title": "Workloads",
        "kind": "Kind",
        "name": "Name",
        "ready": "Ready",
        "images": "Images",
        "age": "Age"
      },
      "overview": {
        "pods": "Pods",
        "noProblemPods": "No problem pods.",
        "restarts_one": "{{count}} restart",
        "restarts_other": "{{count}} restarts",
        "workloads": "Workloads that are not fine",
        "noProblemWorkloads": "All workloads are fine.",
        "quotas": "Quotas",
        "noQuotas": "The project has no quotas.",
        "quotasOk": "Every quota is used by less than 80 %.",
        "warnings": "Warning events in the last hour",
        "noWarnings": "No warnings in the last hour."
      }
    }
  },
  "ru": {
    "sidebar": {
      "projects": "Проекты",
      "nodes": "Ноды",
      "projectsFilter": "Фильтр проектов",
      "projectsFromProfile": "Кластер не даёт список проектов — показаны проекты из профиля",
      "noProjects": "Проектов нет"
    },
    "project": {
      "tabs": { "workloads": "Workloads" },
      "workloads": {
        "title": "Workloads",
        "kind": "Вид",
        "name": "Имя",
        "ready": "Готово",
        "images": "Образы",
        "age": "Возраст"
      },
      "overview": {
        "pods": "Поды",
        "noProblemPods": "Проблемных подов нет.",
        "restarts_one": "{{count}} рестарт",
        "restarts_few": "{{count}} рестарта",
        "restarts_many": "{{count}} рестартов",
        "restarts_other": "{{count}} рестарта",
        "workloads": "Workloads не в норме",
        "noProblemWorkloads": "Все workloads в норме.",
        "quotas": "Квоты",
        "noQuotas": "Квот у проекта нет.",
        "quotasOk": "Каждая квота занята меньше чем на 80 %.",
        "warnings": "События Warning за последний час",
        "noWarnings": "Предупреждений за последний час нет."
      }
    }
  }
}
````

- [ ] **Step 4: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 45 passed`, `Tests 248 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 136`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 5: Commit.**

```bash
git add \
  docs/i18n/same-as-en.json \
  src/components/Layout.tsx \
  src/components/TabContent.tsx \
  src/components/connections/tree/ConnectionNode.test.tsx \
  src/components/connections/tree/ConnectionNode.tsx \
  src/components/objects/EventList.tsx \
  src/components/project/ProjectOverview.tsx \
  src/components/project/ProjectPanel.test.tsx \
  src/components/project/ProjectPanel.tsx \
  src/components/project/WorkloadsTable.tsx \
  src/components/project/access.ts \
  src/locales/en.json \
  src/locales/ru.json \
  src/state/workspace.test.ts \
  src/state/workspace.ts
git commit -s -m "feat(ui): вкладка проекта — обзор, workloads и поды через watch, события, подвкладки по правам; проекты и ноды в дереве" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 16: Сеть, конфигурация, хранилище и квоты проекта

**Files:**
- Create: `src/components/project/ConfigView.tsx`, `src/components/project/NetworkView.tsx`, `src/components/project/ProjectResources.test.tsx`, `src/components/project/QuotasView.tsx`, `src/components/project/StorageView.tsx`
- Modify: `docs/i18n/same-as-en.json`, `src/components/project/ProjectPanel.tsx`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задача 15; команды задач 8, 9.
- Produces: `components/project/{NetworkView, ConfigView, StorageView, QuotasView}` (все — `({ connectionId, namespace })`); подвкладки «Сеть», «Конфигурация», «Хранилище», «Квоты» вкладки проекта (по правам на их ресурсы).

Спец. §6.3. «Сеть»: Services (endpoints 0 из N выделены; если EndpointSlices не читаются — пояснение), Routes («Принят» с причиной отказа, срок сертификата — меньше 30 дней выделен, «из Ingress …»), Ingress (правила, TLS, адрес, Routes из него), Gateway, HTTPRoute и GRPCRoute (условия родителей) — только виды, которые есть в кластере. «Конфигурация»: ConfigMaps с ключами и «Показать значения» (значения читаются только по щелчку), Secrets — имя, тип и ключи с пояснением, что значения не показываются. «Хранилище»: PVC со статусом, классом, ёмкостью (или запрошенным), режимами доступа и подами («неизвестно», если поды не читаются). «Квоты»: ResourceQuota с долями (90 % и больше выделены) и LimitRange.

- [ ] **Step 1: Тесты.**

<!-- block: file t16-src-components-project-projectresources-test-tsx step=16.1 path=src/components/project/ProjectResources.test.tsx -->
````tsx
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getNetwork: vi.fn(), getConfig: vi.fn(), getConfigMapValues: vi.fn(), listPvcs: vi.fn(), getQuotas: vi.fn() } };
});

import { api } from "../../api/client";
import type { AppError, NetworkView as Network } from "../../api/types";
import { ConfigView } from "./ConfigView";
import { NetworkView } from "./NetworkView";
import { QuotasView } from "./QuotasView";
import { StorageView } from "./StorageView";

const forbidden = (resource: string): AppError => ({
  code: "FORBIDDEN",
  key: "errors.k8s.forbiddenIn",
  params: { verb: "list", resource, namespace: "team-a" },
  message: "x",
  retryable: false,
});

const soon = () => Math.floor(Date.now() / 1000) + 86_400;

const NETWORK: Network = {
  services: { data: [{ name: "web", type: "ClusterIP", clusterIp: "10.0.0.10", external: [], ports: ["80/TCP → 8080"], selector: "app=web", endpoints: { ready: 0, total: 2 }, created: 1 }] },
  endpointsError: null,
  routes: {
    data: [
      { name: "web", host: "web.apps.example.com", path: "/", service: "web", port: "8080", tls: "edge", insecurePolicy: "Redirect", admitted: false, admittedMessage: "host already claimed", certExpires: soon(), ingress: null, created: 1 },
    ],
  },
  ingresses: {
    data: [
      {
        name: "web",
        class: "nginx",
        rules: [{ host: "web.example.com", path: "/api", service: "api", port: "http" }],
        defaultBackend: null,
        tls: [{ hosts: ["web.example.com"], secret: "web-tls" }],
        addresses: ["203.0.113.5"],
        routes: ["web-abcde"],
        created: 1,
      },
    ],
  },
  gateways: { data: [{ name: "gw", class: "cilium", listeners: [{ name: "https", port: 443, protocol: "HTTPS", hostname: null }], addresses: ["198.51.100.7"], accepted: true, programmed: false, created: 1 }] },
  httpRoutes: {
    data: [
      {
        kind: "HTTPRoute",
        name: "web",
        hostnames: ["web.example.com"],
        parents: [{ name: "gw", namespace: null, section: "https", accepted: { type: "Accepted", status: "True" }, resolvedRefs: { type: "ResolvedRefs", status: "False", reason: "BackendNotFound" } }],
        backends: ["web:80"],
        created: 1,
      },
    ],
  },
  grpcRoutes: null,
};

beforeEach(() => vi.clearAllMocks());

describe("подвкладки проекта", () => {
  it("сеть: endpoints, Route с отказом и скорым сроком, Ingress с созданными Routes, Gateway API", async () => {
    vi.mocked(api.getNetwork).mockResolvedValue(NETWORK);
    renderWithClient(<NetworkView connectionId="c1" namespace="team-a" />);
    expect(await screen.findByText("0/2")).toHaveClass("problem");
    expect(screen.getByText(/host already claimed/)).toBeInTheDocument();
    expect(screen.getByText("Routes из этого Ingress: web-abcde")).toBeInTheDocument();
    expect(screen.getByText("web-tls")).toBeInTheDocument();
    expect(screen.getByText("https HTTPS:443")).toBeInTheDocument();
    expect(screen.getByText(/BackendNotFound/)).toBeInTheDocument();
    expect(screen.queryByText("GRPCRoute")).not.toBeInTheDocument();
  });

  it("конфигурация: значения ConfigMap по щелчку, Secrets без значений и без прав", async () => {
    vi.mocked(api.getConfig).mockResolvedValue({
      configMaps: { data: [{ name: "web", keys: ["LOG_LEVEL"], binaryKeys: ["logo.png"], created: 1 }] },
      secrets: { error: forbidden("secrets") },
    });
    vi.mocked(api.getConfigMapValues).mockResolvedValue({ LOG_LEVEL: { text: "debug", binarySize: null }, "logo.png": { text: null, binarySize: 4 } });
    const user = userEvent.setup();
    renderWithClient(<ConfigView connectionId="c1" namespace="team-a" />);
    expect(await screen.findByText("LOG_LEVEL, logo.png")).toBeInTheDocument();
    expect(api.getConfigMapValues).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Показать значения" }));
    expect(await screen.findByText("debug")).toBeInTheDocument();
    expect(screen.getByText("двоичные данные, 4 байт")).toBeInTheDocument();
    expect(screen.getByText("— Нет прав: list secrets в проекте «team-a»")).toBeInTheDocument();
  });

  it("хранилище и квоты", async () => {
    vi.mocked(api.listPvcs).mockResolvedValue({
      items: [
        { name: "data", phase: "Bound", class: "standard", capacity: "10Gi", requested: "10Gi", accessModes: ["ReadWriteOnce"], volumeMode: "Filesystem", volume: "pvc-1", mountedBy: ["db-0"], created: 1 },
        { name: "cache", phase: "Pending", class: null, capacity: null, requested: "1Gi", accessModes: [], volumeMode: null, volume: null, mountedBy: null, created: 1 },
      ],
      truncated: false,
    });
    vi.mocked(api.getQuotas).mockResolvedValue({
      quotas: { data: [{ name: "compute", scopes: [], entries: [{ resource: "requests.cpu", used: "3600m", hard: "4", ratio: 0.9 }] }] },
      limitRanges: { data: [{ name: "defaults", limits: [{ type: "Container", resource: "cpu", min: null, max: null, default: "500m", defaultRequest: "100m", maxLimitRequestRatio: null }] }] },
    });
    renderWithClient(
      <>
        <StorageView connectionId="c1" namespace="team-a" />
        <QuotasView connectionId="c1" namespace="team-a" />
      </>,
    );
    expect(await screen.findByText("db-0")).toBeInTheDocument();
    expect(screen.getByText("Pending")).toHaveClass("problem");
    expect(screen.getByText("запрошено 1Gi")).toBeInTheDocument();
    expect(screen.getByText("неизвестно")).toBeInTheDocument();
    await waitFor(() => expect(screen.getByText("3600m").closest("tr")).toHaveClass("warn"));
    expect(screen.getByText("500m")).toBeInTheDocument();
  });
});
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/project/ProjectResources.test.tsx`  
Expected: FAIL — нет компонентов `NetworkView`, `ConfigView`, `StorageView`, `QuotasView`.

- [ ] **Step 3: Реализация.**

<!-- block: file t16-src-components-project-configview-tsx step=16.3 path=src/components/project/ConfigView.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import { formatAge } from "../../lib/age";
import { PartView } from "../common/PartView";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";

/** Значения ConfigMap — по щелчку (спец. §6.3). */
function ConfigMapValues({ connectionId, namespace, name }: { connectionId: string; namespace: string; name: string }) {
  const { t } = useTranslation();
  const q = useQuery({ queryKey: ["configMap", connectionId, namespace, name], queryFn: () => api.getConfigMapValues(connectionId, namespace, name) });
  if (q.error) return <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />;
  if (!q.data) return <p className="muted">{t("common.loading")}</p>;
  return (
    <dl className="facts">
      {Object.entries(q.data).map(([k, v]) => (
        <div key={k} style={{ display: "contents" }}>
          <dt>
            <code>{k}</code>
          </dt>
          <dd>{v.text !== null ? <pre className="yaml">{v.text}</pre> : <span className="muted">{t("project.config.binary", { size: v.binarySize ?? 0 })}</span>}</dd>
        </div>
      ))}
    </dl>
  );
}

/** Конфигурация проекта: ConfigMaps (ключи, значения по щелчку) и Secrets (имя, тип, ключи — значения не показываются). */
export function ConfigView({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const [shown, setShown] = useState<string | null>(null);
  const q = useQuery({ queryKey: ["config", connectionId, namespace], queryFn: () => api.getConfig(connectionId, namespace), refetchInterval: interval || false });
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && (
        <>
          <h3>ConfigMaps</h3>
          <PartView part={q.data.configMaps}>
            {(rows) =>
              rows.length === 0 ? (
                <p className="muted">—</p>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>{t("project.network.name")}</th>
                      <th>{t("project.config.keys")}</th>
                      <th>{t("project.workloads.age")}</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((c) => (
                      <tr key={c.name}>
                        <td>{c.name}</td>
                        <td>
                          {[...c.keys, ...c.binaryKeys].join(", ") || "—"}
                          {shown === c.name && <ConfigMapValues connectionId={connectionId} namespace={namespace} name={c.name} />}
                        </td>
                        <td>{formatAge(c.created)}</td>
                        <td>
                          <button type="button" className="link" onClick={() => setShown((s) => (s === c.name ? null : c.name))}>
                            {shown === c.name ? t("project.config.hideValues") : t("project.config.showValues")}
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )
            }
          </PartView>
          <h3>Secrets</h3>
          <p className="muted">{t("project.config.secretValuesHidden")}</p>
          <PartView part={q.data.secrets}>
            {(rows) =>
              rows.length === 0 ? (
                <p className="muted">—</p>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>{t("project.network.name")}</th>
                      <th>{t("project.network.type")}</th>
                      <th>{t("project.config.keys")}</th>
                      <th>{t("project.workloads.age")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((s) => (
                      <tr key={s.name}>
                        <td>{s.name}</td>
                        <td>{s.type}</td>
                        <td>{s.keys.join(", ") || "—"}</td>
                        <td>{formatAge(s.created)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )
            }
          </PartView>
        </>
      )}
    </div>
  );
}
````

<!-- block: file t16-src-components-project-networkview-tsx step=16.3 path=src/components/project/NetworkView.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { ConditionView, GatewayRouteRow, Part } from "../../api/types";
import { tr } from "../../i18n/text";
import { formatDateTime } from "../../lib/age";
import { PartView } from "../common/PartView";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";

/** Сертификат Route, который истекает раньше, — с предупреждением (спец. §8.2, `route.certExpiring`). */
const CERT_WARN_SEC = 30 * 86_400;

const flag = (v: boolean | null) => (v === true ? "✅" : v === false ? "⚠" : "—");

function Cond({ c }: { c: ConditionView | null }) {
  if (!c) return <>—</>;
  return (
    <span className={c.status === "True" ? undefined : "problem"} title={c.message}>
      {c.status === "True" ? "✅" : "⚠"} {c.reason ?? c.status}
    </span>
  );
}

function GatewayRoutes({ title, part }: { title: string; part: Part<GatewayRouteRow[]> | null }) {
  const { t } = useTranslation();
  if (!part) return null;
  return (
    <>
      <h3>{title}</h3>
      <PartView part={part}>
        {(rows) =>
          rows.length === 0 ? (
            <p className="muted">—</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>{t("project.network.name")}</th>
                  <th>{t("project.network.hosts")}</th>
                  <th>{t("project.network.parents")}</th>
                  <th>{t("project.network.backends")}</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.name}>
                    <td>{r.name}</td>
                    <td>{r.hostnames.join(", ") || "*"}</td>
                    <td>
                      {r.parents.map((p) => (
                        <div key={`${p.namespace ?? ""}/${p.name}/${p.section ?? ""}`}>
                          <code>
                            {p.name}
                            {p.section ? `/${p.section}` : ""}
                          </code>{" "}
                          {t("project.network.accepted")}: <Cond c={p.accepted} />, {t("project.network.resolvedRefs")}: <Cond c={p.resolvedRefs} />
                        </div>
                      ))}
                    </td>
                    <td>{r.backends.join(", ") || "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )
        }
      </PartView>
    </>
  );
}

/** Сеть проекта (спец. §6.3): Services, Routes, Ingress, Gateway API — каждый вид, только если есть его группа API. */
export function NetworkView({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const q = useQuery({ queryKey: ["network", connectionId, namespace], queryFn: () => api.getNetwork(connectionId, namespace), refetchInterval: interval || false });
  const n = q.data;
  const now = Date.now() / 1000;
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {n && (
        <>
          <h3>Services</h3>
          {n.endpointsError && <p className="muted">{t("project.network.endpointsUnknown", { reason: tr(n.endpointsError) })}</p>}
          <PartView part={n.services}>
            {(rows) =>
              rows.length === 0 ? (
                <p className="muted">—</p>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>{t("project.network.name")}</th>
                      <th>{t("project.network.type")}</th>
                      <th>Cluster IP</th>
                      <th>{t("project.network.ports")}</th>
                      <th>{t("project.network.endpoints")}</th>
                      <th>{t("project.network.external")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((s) => (
                      <tr key={s.name}>
                        <td>{s.name}</td>
                        <td>{s.type}</td>
                        <td>{s.clusterIp ?? "—"}</td>
                        <td>{s.ports.join(", ") || "—"}</td>
                        <td className={s.endpoints && s.endpoints.ready === 0 ? "problem" : undefined}>
                          {s.endpoints ? `${s.endpoints.ready}/${s.endpoints.total}` : "—"}
                        </td>
                        <td>{s.external.join(", ") || "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )
            }
          </PartView>
          {n.routes && (
            <>
              <h3>Routes</h3>
              <PartView part={n.routes}>
                {(rows) =>
                  rows.length === 0 ? (
                    <p className="muted">—</p>
                  ) : (
                    <table>
                      <thead>
                        <tr>
                          <th>{t("project.network.name")}</th>
                          <th>{t("project.network.host")}</th>
                          <th>{t("project.network.service")}</th>
                          <th>TLS</th>
                          <th>{t("project.network.admitted")}</th>
                          <th>{t("project.network.certExpires")}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {rows.map((r) => (
                          <tr key={r.name}>
                            <td>
                              {r.name}
                              {r.ingress && <span className="muted"> ({t("project.network.fromIngress", { name: r.ingress })})</span>}
                            </td>
                            <td>
                              {r.host ?? "—"}
                              {r.path ?? ""}
                            </td>
                            <td>
                              {r.service ?? "—"}
                              {r.port ? `:${r.port}` : ""}
                            </td>
                            <td>{r.tls ?? "—"}</td>
                            <td title={r.admittedMessage ?? undefined} className={r.admitted === false ? "problem" : undefined}>
                              {flag(r.admitted)} {r.admittedMessage ?? ""}
                            </td>
                            <td className={r.certExpires !== null && r.certExpires - now < CERT_WARN_SEC ? "warn" : undefined}>{formatDateTime(r.certExpires)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )
                }
              </PartView>
            </>
          )}
          <h3>Ingress</h3>
          <PartView part={n.ingresses}>
            {(rows) =>
              rows.length === 0 ? (
                <p className="muted">—</p>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>{t("project.network.name")}</th>
                      <th>{t("project.network.class")}</th>
                      <th>{t("project.network.rules")}</th>
                      <th>TLS</th>
                      <th>{t("project.network.address")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((i) => (
                      <tr key={i.name}>
                        <td>
                          {i.name}
                          {i.routes.length > 0 && <div className="muted">{t("project.network.routesFromIngress", { names: i.routes.join(", ") })}</div>}
                        </td>
                        <td>{i.class ?? "—"}</td>
                        <td>
                          {i.rules.map((r) => (
                            <div key={`${r.host ?? "*"}${r.path ?? ""}`}>
                              {r.host ?? "*"}
                              {r.path ?? ""} → {r.service ?? "—"}
                              {r.port ? `:${r.port}` : ""}
                            </div>
                          ))}
                          {i.defaultBackend && <div className="muted">{t("project.network.defaultBackend", { backend: i.defaultBackend })}</div>}
                        </td>
                        <td>
                          {i.tls.map((x) => (
                            <div key={x.secret ?? x.hosts.join(",")}>
                              {x.hosts.join(", ")} {x.secret && <code>{x.secret}</code>}
                            </div>
                          ))}
                        </td>
                        <td>{i.addresses.join(", ") || "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )
            }
          </PartView>
          {n.gateways && (
            <>
              <h3>Gateway</h3>
              <PartView part={n.gateways}>
                {(rows) =>
                  rows.length === 0 ? (
                    <p className="muted">—</p>
                  ) : (
                    <table>
                      <thead>
                        <tr>
                          <th>{t("project.network.name")}</th>
                          <th>{t("project.network.class")}</th>
                          <th>Listeners</th>
                          <th>{t("project.network.address")}</th>
                          <th>Accepted</th>
                          <th>Programmed</th>
                        </tr>
                      </thead>
                      <tbody>
                        {rows.map((g) => (
                          <tr key={g.name}>
                            <td>{g.name}</td>
                            <td>{g.class ?? "—"}</td>
                            <td>{g.listeners.map((l) => `${l.name} ${l.protocol ?? ""}:${l.port ?? ""}${l.hostname ? ` ${l.hostname}` : ""}`).join(", ")}</td>
                            <td>{g.addresses.join(", ") || "—"}</td>
                            <td>{flag(g.accepted)}</td>
                            <td>{flag(g.programmed)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )
                }
              </PartView>
            </>
          )}
          <GatewayRoutes title="HTTPRoute" part={n.httpRoutes} />
          <GatewayRoutes title="GRPCRoute" part={n.grpcRoutes} />
        </>
      )}
    </div>
  );
}
````

<!-- block: file t16-src-components-project-quotasview-tsx step=16.3 path=src/components/project/QuotasView.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import { PartView } from "../common/PartView";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";

/** Квота занята на столько и больше — выделяется (как правило `quota.nearLimit`). */
const NEAR_LIMIT = 0.9;

/** Квоты проекта (спец. §6.3): ResourceQuota — использовано и лимит, LimitRange — границы и значения по умолчанию. */
export function QuotasView({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const q = useQuery({ queryKey: ["quotas", connectionId, namespace], queryFn: () => api.getQuotas(connectionId, namespace), refetchInterval: interval || false });
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && (
        <>
          <h3>ResourceQuota</h3>
          <PartView part={q.data.quotas}>
            {(quotas) =>
              quotas.length === 0 ? (
                <p className="muted">—</p>
              ) : (
                quotas.map((quota) => (
                  <div key={quota.name} className="section">
                    <h4>
                      {quota.name}
                      {quota.scopes.length > 0 && <span className="muted"> ({quota.scopes.join(", ")})</span>}
                    </h4>
                    <table>
                      <thead>
                        <tr>
                          <th>{t("project.quotas.resource")}</th>
                          <th>{t("project.quotas.used")}</th>
                          <th>{t("project.quotas.hard")}</th>
                          <th>%</th>
                        </tr>
                      </thead>
                      <tbody>
                        {quota.entries.map((e) => (
                          <tr key={e.resource} className={(e.ratio ?? 0) >= NEAR_LIMIT ? "warn" : undefined}>
                            <td>
                              <code>{e.resource}</code>
                            </td>
                            <td>{e.used ?? "—"}</td>
                            <td>{e.hard}</td>
                            <td>{e.ratio === null ? "—" : Math.round(e.ratio * 100)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ))
              )
            }
          </PartView>
          <h3>LimitRange</h3>
          <PartView part={q.data.limitRanges}>
            {(ranges) =>
              ranges.length === 0 ? (
                <p className="muted">—</p>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>{t("project.network.name")}</th>
                      <th>{t("project.network.type")}</th>
                      <th>{t("project.quotas.resource")}</th>
                      <th>min</th>
                      <th>max</th>
                      <th>default</th>
                      <th>defaultRequest</th>
                      <th>maxLimitRequestRatio</th>
                    </tr>
                  </thead>
                  <tbody>
                    {ranges.flatMap((r) =>
                      r.limits.map((l) => (
                        <tr key={`${r.name}/${l.type}/${l.resource}`}>
                          <td>{r.name}</td>
                          <td>{l.type}</td>
                          <td>{l.resource}</td>
                          <td>{l.min ?? "—"}</td>
                          <td>{l.max ?? "—"}</td>
                          <td>{l.default ?? "—"}</td>
                          <td>{l.defaultRequest ?? "—"}</td>
                          <td>{l.maxLimitRequestRatio ?? "—"}</td>
                        </tr>
                      )),
                    )}
                  </tbody>
                </table>
              )
            }
          </PartView>
        </>
      )}
    </div>
  );
}
````

<!-- block: file t16-src-components-project-storageview-tsx step=16.3 path=src/components/project/StorageView.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import { formatAge } from "../../lib/age";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";

/** Хранилище проекта (спец. §6.3): PVC — статус, класс, ёмкость, режим доступа, кем смонтирован. */
export function StorageView({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const q = useQuery({ queryKey: ["pvcs", connectionId, namespace], queryFn: () => api.listPvcs(connectionId, namespace), refetchInterval: interval || false });
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && q.data.items.length === 0 && <p className="muted">—</p>}
      {q.data && q.data.items.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>{t("project.network.name")}</th>
              <th>{t("objects.pods.status")}</th>
              <th>{t("project.storage.class")}</th>
              <th>{t("project.storage.capacity")}</th>
              <th>{t("project.storage.accessModes")}</th>
              <th>{t("project.storage.mountedBy")}</th>
              <th>{t("project.workloads.age")}</th>
            </tr>
          </thead>
          <tbody>
            {q.data.items.map((p) => (
              <tr key={p.name}>
                <td>{p.name}</td>
                <td className={p.phase === "Bound" ? undefined : "problem"}>{p.phase ?? "—"}</td>
                <td>{p.class ?? "—"}</td>
                <td>{p.capacity ?? t("project.storage.requested", { size: p.requested ?? "—" })}</td>
                <td>{p.accessModes.join(", ") || "—"}</td>
                <td>{p.mountedBy === null ? <span className="muted">{t("project.storage.unknown")}</span> : p.mountedBy.join(", ") || "—"}</td>
                <td>{formatAge(p.created)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
````

<!-- block: patch t16-src step=16.3 -->
````diff
diff --git a/docs/i18n/same-as-en.json b/docs/i18n/same-as-en.json
index 6d6d4c5..8e4944d 100644
--- a/docs/i18n/same-as-en.json
+++ b/docs/i18n/same-as-en.json
@@ -5,6 +5,7 @@
     "cluster.nodes.taints",
     "cluster.nodes.version",
     "project.tabs.workloads",
-    "project.workloads.title"
+    "project.workloads.title",
+    "project.network.service"
   ]
 }
diff --git a/src/components/project/ProjectPanel.tsx b/src/components/project/ProjectPanel.tsx
index a741fec..4bcfca6 100644
--- a/src/components/project/ProjectPanel.tsx
+++ b/src/components/project/ProjectPanel.tsx
@@ -11,10 +11,24 @@ import { ProjectEvents } from "../objects/EventList";
 import { PodList } from "../objects/PodList";
 import { WatchStatusLine } from "../objects/WatchStatusLine";
 import { WORKLOAD_RESOURCES, canList } from "./access";
+import { ConfigView } from "./ConfigView";
+import { NetworkView } from "./NetworkView";
 import { ProjectOverview } from "./ProjectOverview";
+import { QuotasView } from "./QuotasView";
+import { StorageView } from "./StorageView";
 import { WorkloadsTable } from "./WorkloadsTable";
 
-type Sub = "overview" | "workloads" | "pods" | "events";
+type Sub = "overview" | "workloads" | "pods" | "network" | "config" | "storage" | "events" | "quotas";
+
+/** Ресурсы «Сети»: подвкладка видна, если читается хоть один. */
+const NETWORK_RESOURCES = [
+  "services",
+  "ingresses.networking.k8s.io",
+  "routes.route.openshift.io",
+  "gateways.gateway.networking.k8s.io",
+  "httproutes.gateway.networking.k8s.io",
+  "grpcroutes.gateway.networking.k8s.io",
+];
 
 /** Вкладка проекта (спец. §6.3). Подвкладки без прав на чтение не показываются (§5.4); показывается одна, её watch идёт, пока она открыта. */
 export function ProjectPanel({ connectionId, namespace }: { connectionId: string; namespace: string }) {
@@ -26,7 +40,11 @@ export function ProjectPanel({ connectionId, namespace }: { connectionId: string
     { id: "overview", label: t("objects.tabs.overview"), visible: true },
     { id: "workloads", label: t("project.tabs.workloads"), visible: canList(a, ...WORKLOAD_RESOURCES) },
     { id: "pods", label: t("objects.tabs.pods"), visible: canList(a, "pods") },
+    { id: "network", label: t("project.tabs.network"), visible: canList(a, ...NETWORK_RESOURCES) },
+    { id: "config", label: t("project.tabs.config"), visible: canList(a, "configmaps", "secrets") },
+    { id: "storage", label: t("project.tabs.storage"), visible: canList(a, "persistentvolumeclaims") },
     { id: "events", label: t("objects.tabs.events"), visible: canList(a, "events") },
+    { id: "quotas", label: t("project.tabs.quotas"), visible: canList(a, "resourcequotas", "limitranges") },
   ];
   const visible = tabs.filter((x) => x.visible);
   const active = visible.some((x) => x.id === sub) ? sub : "overview";
@@ -40,7 +58,11 @@ export function ProjectPanel({ connectionId, namespace }: { connectionId: string
       {active === "overview" && <ProjectOverview connectionId={connectionId} namespace={namespace} />}
       {active === "workloads" && <ProjectWorkloads connectionId={connectionId} namespace={namespace} />}
       {active === "pods" && <PodList connectionId={connectionId} namespace={namespace} />}
+      {active === "network" && <NetworkView connectionId={connectionId} namespace={namespace} />}
+      {active === "config" && <ConfigView connectionId={connectionId} namespace={namespace} />}
+      {active === "storage" && <StorageView connectionId={connectionId} namespace={namespace} />}
       {active === "events" && <ProjectEvents connectionId={connectionId} namespace={namespace} />}
+      {active === "quotas" && <QuotasView connectionId={connectionId} namespace={namespace} />}
     </div>
   );
 }
````

<!-- block: locales t16-locales step=16.3 -->
````json
{
  "en": {
    "project": {
      "tabs": {
        "network": "Network",
        "config": "Configuration",
        "storage": "Storage",
        "quotas": "Quotas"
      },
      "network": {
        "name": "Name",
        "type": "Type",
        "ports": "Ports",
        "endpoints": "Ready endpoints",
        "endpointsUnknown": "Endpoints are not shown: {{reason}}",
        "external": "External addresses",
        "host": "Host and path",
        "service": "Service",
        "admitted": "Admitted",
        "certExpires": "Certificate expires",
        "fromIngress": "from Ingress {{name}}",
        "class": "Class",
        "rules": "Rules",
        "address": "Address",
        "routesFromIngress": "Routes from this Ingress: {{names}}",
        "defaultBackend": "by default → {{backend}}",
        "hosts": "Hosts",
        "parents": "Gateways",
        "backends": "Backends",
        "accepted": "accepted",
        "resolvedRefs": "references"
      },
      "config": {
        "keys": "Keys",
        "showValues": "Show values",
        "hideValues": "Hide values",
        "binary": "binary data, {{size}} bytes",
        "secretValuesHidden": "Secret values are never shown — only names, types and keys."
      },
      "storage": {
        "class": "Storage class",
        "capacity": "Capacity",
        "requested": "requested {{size}}",
        "accessModes": "Access modes",
        "mountedBy": "Mounted by",
        "unknown": "unknown"
      },
      "quotas": {
        "resource": "Resource",
        "used": "Used",
        "hard": "Limit"
      }
    }
  },
  "ru": {
    "project": {
      "tabs": {
        "network": "Сеть",
        "config": "Конфигурация",
        "storage": "Хранилище",
        "quotas": "Квоты"
      },
      "network": {
        "name": "Имя",
        "type": "Тип",
        "ports": "Порты",
        "endpoints": "Готовые endpoints",
        "endpointsUnknown": "Endpoints не видны: {{reason}}",
        "external": "Внешние адреса",
        "host": "Хост и путь",
        "service": "Service",
        "admitted": "Принят",
        "certExpires": "Сертификат до",
        "fromIngress": "из Ingress {{name}}",
        "class": "Класс",
        "rules": "Правила",
        "address": "Адрес",
        "routesFromIngress": "Routes из этого Ingress: {{names}}",
        "defaultBackend": "по умолчанию → {{backend}}",
        "hosts": "Хосты",
        "parents": "Gateway",
        "backends": "Бэкенды",
        "accepted": "принят",
        "resolvedRefs": "ссылки"
      },
      "config": {
        "keys": "Ключи",
        "showValues": "Показать значения",
        "hideValues": "Скрыть значения",
        "binary": "двоичные данные, {{size}} байт",
        "secretValuesHidden": "Значения Secret не показываются никогда — только имена, типы и ключи."
      },
      "storage": {
        "class": "Класс хранилища",
        "capacity": "Ёмкость",
        "requested": "запрошено {{size}}",
        "accessModes": "Режимы доступа",
        "mountedBy": "Кем смонтирован",
        "unknown": "неизвестно"
      },
      "quotas": {
        "resource": "Ресурс",
        "used": "Использовано",
        "hard": "Лимит"
      }
    }
  }
}
````

- [ ] **Step 4: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 46 passed`, `Tests 251 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 136`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 5: Commit.**

```bash
git add \
  docs/i18n/same-as-en.json \
  src/components/project/ConfigView.tsx \
  src/components/project/NetworkView.tsx \
  src/components/project/ProjectPanel.tsx \
  src/components/project/ProjectResources.test.tsx \
  src/components/project/QuotasView.tsx \
  src/components/project/StorageView.tsx \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "feat(ui): сеть, конфигурация, хранилище и квоты проекта — Routes, Ingress, Gateway API, значения ConfigMap по щелчку" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 17: Логи пода

**Files:**
- Create: `src/components/logs/LogsView.test.tsx`, `src/components/logs/LogsView.tsx`, `src/components/logs/buffer.test.ts`, `src/components/logs/buffer.ts`
- Modify: `src/app.css`, `src/components/objects/PodPanel.tsx`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задачи 12, 14; команды задачи 11.
- Produces: `components/logs/{LogsView({ connectionId, namespace, pod, containers }), appendLines(buffer, lines, max), MAX_LINES = 100 000}`; подвкладка «Логи» вкладки пода; подсказка `hints.logs.filter`.

Спец. §6.6. Параметры: контейнер (по умолчанию первый обычный), «Предыдущий запуск» (тогда слежения нет), «Последних строк» (500), «Начиная с» (всё, 5 мин, 1 ч, 6 ч, 24 ч), «Следить за новыми строками», «Время строк» (без перезапуска сессии), фильтр — подстрока или regex, с учётом регистра или без (применяется в Rust через 400 мс после ввода). Смена параметров перезапускает сессию, вход заново — тоже. Буфер — последние 100 000 строк, список виртуализирован и следит за концом, пока его не прокрутили вверх; обрезанная строка помечена. Состояние: подключение, поток, переподключение, конец, ошибка; счётчик «показано из просмотрено». «Сохранить в файл…» — диалог сохранения и `save_logs` с теми же параметрами без follow.

- [ ] **Step 1: Тесты.**

<!-- block: file t17-src-components-logs-logsview-test-tsx step=17.1 path=src/components/logs/LogsView.test.tsx -->
````tsx
import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 18 })),
    getTotalSize: () => o.count * 18,
    scrollToIndex: vi.fn(),
  }),
}));

vi.mock("@tauri-apps/plugin-dialog", () => ({ save: vi.fn() }));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { startLogs: vi.fn(), stopSession: vi.fn(), saveLogs: vi.fn() } };
});

import { save } from "@tauri-apps/plugin-dialog";
import { api } from "../../api/client";
import type { LogEvent } from "../../api/types";
import { LogsView } from "./LogsView";

let emit: (e: LogEvent) => void = () => undefined;
const CONTAINERS = [
  { name: "migrate", init: true },
  { name: "web", init: false },
];

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.startLogs).mockImplementation(async (_p, onEvent) => {
    emit = onEvent;
    return `s${vi.mocked(api.startLogs).mock.calls.length}`;
  });
  vi.mocked(api.stopSession).mockResolvedValue(true);
});

const lastParams = () => {
  const calls = vi.mocked(api.startLogs).mock.calls;
  return calls[calls.length - 1]?.[0];
};

describe("LogsView", () => {
  it("хвост основного контейнера с follow, строки, счётчики и состояние", async () => {
    renderWithClient(<LogsView connectionId="c1" namespace="team-a" pod="web-1" containers={CONTAINERS} />);
    await waitFor(() =>
      expect(lastParams()).toEqual({
        connectionId: "c1",
        namespace: "team-a",
        pod: "web-1",
        container: "web",
        previous: false,
        follow: true,
        tailLines: 500,
        sinceSeconds: null,
        filter: null,
      }),
    );
    act(() =>
      emit({
        type: "lines",
        lines: [
          { ts: "2026-10-25T10:00:00.1Z", text: "GET /api 500", truncated: false },
          { ts: "2026-10-25T10:00:01Z", text: "x".repeat(10), truncated: true },
        ],
        scanned: 3,
        shown: 2,
      }),
    );
    expect(screen.getByText("GET /api 500")).toBeInTheDocument();
    expect(screen.getByText("(строка обрезана)")).toBeInTheDocument();
    expect(screen.getByText("показано 2 из 3")).toBeInTheDocument();
    expect(screen.getByText("идёт поток")).toBeInTheDocument();
    act(() => emit({ type: "reconnecting", since: "2026-10-25T10:00:01Z" }));
    expect(screen.getByText("соединение потеряно — переподключение…")).toHaveClass("warn");
    act(() => emit({ type: "end", scanned: 3, shown: 2 }));
    expect(screen.getByText("конец лога")).toBeInTheDocument();
  });

  it("фильтр и предыдущий запуск перезапускают сессию; время строк — без перезапуска", async () => {
    const user = userEvent.setup();
    renderWithClient(<LogsView connectionId="c1" namespace="team-a" pod="web-1" containers={CONTAINERS} />);
    await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
    act(() => emit({ type: "lines", lines: [{ ts: "2026-10-25T10:00:00.1Z", text: "hello", truncated: false }], scanned: 1, shown: 1 }));
    await user.click(screen.getByLabelText("Время строк"));
    expect(screen.getByText("2026-10-25T10:00:00.1Z")).toBeInTheDocument();
    expect(api.startLogs).toHaveBeenCalledTimes(1);
    await user.type(screen.getByLabelText("Фильтр"), "err");
    await user.click(screen.getByLabelText("regex"));
    await waitFor(() => expect(lastParams()?.filter).toEqual({ text: "err", regex: true, caseSensitive: false }));
    expect(api.stopSession).toHaveBeenCalledWith("s1");
    await user.click(screen.getByLabelText("Предыдущий запуск"));
    await waitFor(() => expect(lastParams()).toMatchObject({ previous: true, follow: false }));
    expect(screen.getByLabelText("Следить за новыми строками")).toBeDisabled();
  });

  it("ошибка regex показывается, сохранение пишет файл с параметрами без follow", async () => {
    vi.mocked(save).mockResolvedValue("/tmp/web-1.log");
    vi.mocked(api.saveLogs).mockResolvedValue(42);
    const user = userEvent.setup();
    renderWithClient(<LogsView connectionId="c1" namespace="team-a" pod="web-1" containers={CONTAINERS} />);
    await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
    act(() => emit({ type: "error", error: { code: "INVALID_INPUT", key: "errors.logs.badRegex", params: { error: "x" }, message: "x", retryable: false } }));
    expect(screen.getByRole("alert")).toHaveTextContent("Неверное регулярное выражение: x");
    await user.click(screen.getByRole("button", { name: "Сохранить в файл…" }));
    await waitFor(() => expect(api.saveLogs).toHaveBeenCalledWith(expect.objectContaining({ pod: "web-1", follow: false }), "/tmp/web-1.log", false));
    expect(await screen.findByText("Сохранено строк: 42 — /tmp/web-1.log")).toBeInTheDocument();
  });
});
````

<!-- block: file t17-src-components-logs-buffer-test-ts step=17.1 path=src/components/logs/buffer.test.ts -->
````ts
import { describe, expect, it } from "vitest";
import type { LogLine } from "../../api/types";
import { appendLines } from "./buffer";

const line = (text: string): LogLine => ({ ts: null, text, truncated: false });

describe("буфер логов", () => {
  it("держит последние строки", () => {
    const b = appendLines([line("1"), line("2")], [line("3"), line("4")], 3);
    expect(b.map((l) => l.text)).toEqual(["2", "3", "4"]);
    expect(appendLines([], [line("a")], 3)).toHaveLength(1);
  });
});
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/logs/LogsView.test.tsx src/components/logs/buffer.test.ts`  
Expected: FAIL — нет компонента `LogsView` и буфера `appendLines`.

- [ ] **Step 3: Реализация.**

<!-- block: file t17-src-components-logs-logsview-tsx step=17.3 path=src/components/logs/LogsView.tsx -->
````tsx
import { useVirtualizer } from "@tanstack/react-virtual";
import { save } from "@tauri-apps/plugin-dialog";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { api, toAppError } from "../../api/client";
import type { AppError, LogLine, LogParams } from "../../api/types";
import { useDebouncedValue } from "../../lib/useDebouncedValue";
import { useEpoch } from "../../state/connectionEpoch";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { ReadError } from "../common/ReadError";
import { appendLines } from "./buffer";

const ROW_HEIGHT = 18;
/** Начало лога: без ограничения или последние 5 мин, 1 ч, 6 ч, 24 ч (секунды). */
const SINCE = [0, 300, 3600, 21_600, 86_400];
/** Хвост по умолчанию (спец. §6.6). */
const DEFAULT_TAIL = 500;

type Status = "connecting" | "streaming" | "reconnecting" | "ended" | "error";

/**
 * Логи контейнера (спец. §6.6): контейнер, предыдущий запуск, хвост или начало, follow, фильтр
 * (подстрока или regex — применяется в Rust), время строк, буфер до 100 000 строк, сохранение в файл.
 */
export function LogsView({
  connectionId,
  namespace,
  pod,
  containers,
}: {
  connectionId: string;
  namespace: string;
  pod: string;
  containers: { name: string; init: boolean }[];
}) {
  const { t } = useTranslation();
  const epoch = useEpoch(connectionId);
  const firstMain = containers.find((c) => !c.init) ?? containers[0];
  const [container, setContainer] = useState(firstMain?.name ?? "");
  const [previous, setPrevious] = useState(false);
  const [tail, setTail] = useState(String(DEFAULT_TAIL));
  const [since, setSince] = useState(0);
  const [follow, setFollow] = useState(true);
  const [timestamps, setTimestamps] = useState(false);
  const [text, setText] = useState("");
  const [regex, setRegex] = useState(false);
  const [caseSensitive, setCaseSensitive] = useState(false);
  const filterText = useDebouncedValue(text, 400);
  const [lines, setLines] = useState<LogLine[]>([]);
  const [counters, setCounters] = useState({ shown: 0, scanned: 0 });
  const [status, setStatus] = useState<Status>("connecting");
  const [error, setError] = useState<AppError | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<unknown>(null);
  const tailLines = Number.parseInt(tail, 10);
  const params: LogParams = {
    connectionId,
    namespace,
    pod,
    container: container || null,
    previous,
    // Предыдущий запуск контейнера уже завершён — следить не за чем.
    follow: follow && !previous,
    tailLines: Number.isFinite(tailLines) && tailLines > 0 ? tailLines : null,
    sinceSeconds: since || null,
    filter: filterText ? { text: filterText, regex, caseSensitive } : null,
  };
  const paramsKey = JSON.stringify(params);

  useEffect(() => {
    const p = JSON.parse(paramsKey) as LogParams;
    let active = true;
    let sessionId: string | null = null;
    setLines([]);
    setCounters({ shown: 0, scanned: 0 });
    setStatus("connecting");
    setError(null);
    api
      .startLogs(p, (e) => {
        if (!active) return;
        switch (e.type) {
          case "lines":
            setLines((b) => appendLines(b, e.lines));
            setCounters({ shown: e.shown, scanned: e.scanned });
            setStatus("streaming");
            break;
          case "reconnecting":
            setStatus("reconnecting");
            break;
          case "end":
            setCounters({ shown: e.shown, scanned: e.scanned });
            setStatus("ended");
            break;
          case "error":
            setError(e.error);
            setStatus("error");
            break;
        }
      })
      .then((id) => {
        if (active) sessionId = id;
        else api.stopSession(id).catch(() => undefined);
      })
      .catch((err) => {
        if (!active) return;
        setError(toAppError(err));
        setStatus("error");
      });
    return () => {
      active = false;
      if (sessionId) api.stopSession(sessionId).catch(() => undefined);
    };
  }, [paramsKey, epoch]);

  const parentRef = useRef<HTMLDivElement>(null);
  // Внизу списка — следим за новыми строками; прокрутили вверх — не мешаем читать.
  const atBottom = useRef(true);
  const virtualizer = useVirtualizer({ count: lines.length, getScrollElement: () => parentRef.current, estimateSize: () => ROW_HEIGHT, overscan: 40 });
  useEffect(() => {
    if (atBottom.current && lines.length > 0) virtualizer.scrollToIndex(lines.length - 1);
  }, [lines.length, virtualizer]);

  const onSave = async () => {
    setNotice(null);
    setSaveError(null);
    try {
      const path = await save({ defaultPath: `${pod}-${container}.log`, filters: [{ name: "Log", extensions: ["log", "txt"] }] });
      if (!path) return;
      const n = await api.saveLogs({ ...params, follow: false }, path, timestamps);
      setNotice(t("logs.saved", { count: n, path }));
    } catch (e) {
      setSaveError(e);
    }
  };

  return (
    <div className="section logs">
      <div className="toolbar">
        <label className="inline">
          {t("logs.container")}
          <select value={container} onChange={(e) => setContainer(e.target.value)}>
            {containers.map((c) => (
              <option key={c.name} value={c.name}>
                {c.init ? `${c.name} (init)` : c.name}
              </option>
            ))}
          </select>
        </label>
        <label className="inline">
          <input type="checkbox" checked={previous} onChange={(e) => setPrevious(e.target.checked)} />
          {t("logs.previous")}
        </label>
        <label className="inline">
          {t("logs.tail")}
          <input className="narrow" inputMode="numeric" value={tail} onChange={(e) => setTail(e.target.value)} />
        </label>
        <label className="inline">
          {t("logs.since")}
          <select value={since} onChange={(e) => setSince(Number(e.target.value))}>
            {SINCE.map((s) => (
              <option key={s} value={s}>
                {s === 0 ? t("logs.sinceAll") : t("logs.sinceLast", { minutes: s / 60 })}
              </option>
            ))}
          </select>
        </label>
        <label className="inline">
          <input type="checkbox" checked={follow && !previous} disabled={previous} onChange={(e) => setFollow(e.target.checked)} />
          {t("logs.follow")}
        </label>
        <label className="inline">
          <input type="checkbox" checked={timestamps} onChange={(e) => setTimestamps(e.target.checked)} />
          {t("logs.timestamps")}
        </label>
      </div>
      <div className="toolbar">
        <input aria-label={t("logs.filter")} placeholder={t("logs.filter")} value={text} onChange={(e) => setText(e.target.value)} />
        <label className="inline">
          <input type="checkbox" checked={regex} onChange={(e) => setRegex(e.target.checked)} />
          regex
        </label>
        <Hint k="hints.logs.filter" label={t("logs.filter")} />
        <label className="inline">
          <input type="checkbox" checked={caseSensitive} onChange={(e) => setCaseSensitive(e.target.checked)} />
          {t("logs.caseSensitive")}
        </label>
        <span className="muted">{t("logs.counters", { shown: counters.shown, scanned: counters.scanned })}</span>
        <span className={status === "reconnecting" ? "warn" : "muted"}>{t(`logs.status.${status}`)}</span>
        <button type="button" onClick={() => void onSave()}>
          {t("logs.save")}
        </button>
      </div>
      {notice && <p className="muted">{notice}</p>}
      {saveError !== null && <ErrorBanner error={saveError} />}
      {error && <ReadError error={error} connectionId={connectionId} />}
      <div
        className="vlist log-lines"
        ref={parentRef}
        role="log"
        aria-label={t("logs.title")}
        onScroll={(e) => {
          const el = e.currentTarget;
          atBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < ROW_HEIGHT * 2;
        }}
      >
        <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
          {virtualizer.getVirtualItems().map((item) => {
            const l = lines[item.index];
            return (
              <div key={item.index} className="log-line" style={{ top: item.start, height: ROW_HEIGHT }}>
                {timestamps && l.ts && <span className="muted">{l.ts} </span>}
                {l.text}
                {l.truncated && <span className="muted"> {t("logs.truncated")}</span>}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
````

<!-- block: file t17-src-components-logs-buffer-ts step=17.3 path=src/components/logs/buffer.ts -->
````ts
import type { LogLine } from "../../api/types";

/** Больше стольких строк интерфейс не держит (спец. §6.6): в follow старые уходят, как в кольцевом буфере. */
export const MAX_LINES = 100_000;

/** Строки буфера после добавления `lines`: последние `max`. */
export function appendLines(buffer: LogLine[], lines: LogLine[], max = MAX_LINES): LogLine[] {
  const next = buffer.concat(lines);
  return next.length > max ? next.slice(next.length - max) : next;
}
````

<!-- block: patch t17-src step=17.3 -->
````diff
diff --git a/src/app.css b/src/app.css
index 79fe33b..a9065b3 100644
--- a/src/app.css
+++ b/src/app.css
@@ -14,3 +14,6 @@
 .facts { display: grid; grid-template-columns: max-content minmax(0, 1fr); gap: 2px 12px; margin: 0; }
 .facts dt { color: var(--muted); }
 .facts dd { margin: 0; overflow-wrap: anywhere; }
+.log-lines { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; min-height: 360px; }
+.log-line { position: absolute; left: 0; right: 0; padding: 0 8px; white-space: pre; overflow: hidden; text-overflow: ellipsis; }
+input.narrow { width: 70px; }
diff --git a/src/components/objects/PodPanel.tsx b/src/components/objects/PodPanel.tsx
index cc737ab..dbb7dd6 100644
--- a/src/components/objects/PodPanel.tsx
+++ b/src/components/objects/PodPanel.tsx
@@ -11,10 +11,11 @@ import { ReadError } from "../common/ReadError";
 import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
 import { SubTabs } from "../common/SubTabs";
 import { YamlView } from "../common/YamlView";
+import { LogsView } from "../logs/LogsView";
 import { ContainerCard } from "./ContainerCard";
 import { ObjectEvents } from "./EventList";
 
-type Sub = "overview" | "events" | "yaml";
+type Sub = "overview" | "logs" | "events" | "yaml";
 
 const WORKLOAD_KINDS: string[] = ["Deployment", "StatefulSet", "DaemonSet", "DeploymentConfig", "Job", "CronJob"] satisfies WorkloadKind[];
 
@@ -22,8 +23,11 @@ const WORKLOAD_KINDS: string[] = ["Deployment", "StatefulSet", "DaemonSet", "Dep
 export function PodPanel({ connectionId, namespace, name }: { connectionId: string; namespace: string; name: string }) {
   const { t } = useTranslation();
   const [sub, setSub] = useState<Sub>("overview");
+  // Контейнеры для логов — из того же запроса, что и обзор.
+  const details = useQuery({ queryKey: ["pod", connectionId, namespace, name], queryFn: () => api.getPod(connectionId, namespace, name) });
   const tabs: { id: Sub; label: string }[] = [
     { id: "overview", label: t("objects.tabs.overview") },
+    { id: "logs", label: t("logs.title") },
     { id: "events", label: t("objects.tabs.events") },
     { id: "yaml", label: "YAML" },
   ];
@@ -35,6 +39,9 @@ export function PodPanel({ connectionId, namespace, name }: { connectionId: stri
       </div>
       <SubTabs tabs={tabs} active={sub} onChange={setSub} />
       {sub === "overview" && <PodOverview connectionId={connectionId} namespace={namespace} name={name} />}
+      {sub === "logs" && details.isPending && <p className="muted">{t("common.loading")}</p>}
+      {sub === "logs" && details.data && <LogsView connectionId={connectionId} namespace={namespace} pod={name} containers={details.data.containers} />}
+      {sub === "logs" && details.error && <ReadError error={details.error} connectionId={connectionId} onRetry={() => void details.refetch()} />}
       {sub === "events" && <ObjectEvents connectionId={connectionId} namespace={namespace} object={{ kind: "Pod", name }} />}
       {sub === "yaml" && <YamlView connectionId={connectionId} namespace={namespace} kind="Pod" name={name} />}
     </div>
````

<!-- block: locales t17-locales step=17.3 -->
````json
{
  "en": {
    "hints": {
      "logs": {
        "filter": "The filter runs in Glafira before the lines reach the window: a substring or a regular expression (regex). Lines that do not match are counted but not shown."
      }
    },
    "logs": {
      "title": "Logs",
      "container": "Container",
      "previous": "Previous run",
      "tail": "Last lines",
      "since": "Since",
      "sinceAll": "the start",
      "sinceLast": "last {{minutes}} min",
      "follow": "Follow new lines",
      "timestamps": "Line timestamps",
      "filter": "Filter",
      "caseSensitive": "Match case",
      "counters": "shown {{shown}} of {{scanned}}",
      "status": {
        "connecting": "connecting…",
        "streaming": "streaming",
        "reconnecting": "connection lost — reconnecting…",
        "ended": "end of the log",
        "error": "error"
      },
      "save": "Save to file…",
      "saved": "Lines saved: {{count}} — {{path}}",
      "truncated": "(line truncated)"
    }
  },
  "ru": {
    "hints": {
      "logs": {
        "filter": "Фильтр работает в Glafira до того, как строки попадут в окно: подстрока или регулярное выражение (regex). Неподошедшие строки учитываются в счётчике, но не показываются."
      }
    },
    "logs": {
      "title": "Логи",
      "container": "Контейнер",
      "previous": "Предыдущий запуск",
      "tail": "Последних строк",
      "since": "Начиная с",
      "sinceAll": "начала",
      "sinceLast": "последних {{minutes}} мин",
      "follow": "Следить за новыми строками",
      "timestamps": "Время строк",
      "filter": "Фильтр",
      "caseSensitive": "С учётом регистра",
      "counters": "показано {{shown}} из {{scanned}}",
      "status": {
        "connecting": "подключение…",
        "streaming": "идёт поток",
        "reconnecting": "соединение потеряно — переподключение…",
        "ended": "конец лога",
        "error": "ошибка"
      },
      "save": "Сохранить в файл…",
      "saved": "Сохранено строк: {{count}} — {{path}}",
      "truncated": "(строка обрезана)"
    }
  }
}
````

- [ ] **Step 4: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 48 passed`, `Tests 255 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 136`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 5: Commit.**

```bash
git add \
  src/app.css \
  src/components/logs/LogsView.test.tsx \
  src/components/logs/LogsView.tsx \
  src/components/logs/buffer.test.ts \
  src/components/logs/buffer.ts \
  src/components/objects/PodPanel.tsx \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "feat(ui): логи пода — контейнер, предыдущий запуск, хвост, follow, фильтр в Rust, буфер 100 000 строк, сохранение в файл" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 18: Интеграционные тесты чтения на kind

**Files:**
- Create: `dev/kind/fixtures.yaml`, `src-tauri/tests/common/mod.rs`, `src-tauri/tests/it_read.rs`
- Modify: `.github/workflows/ci.yml`, `dev/kind/README.md`, `dev/kind/setup.sh`, `src-tauri/tests/it_connect.rs`

**Interfaces:**
- Consumes: задачи 1–11 (публичные модули `k8s::*`, `watch`, `logs`, `sessions::Sink`).
- Produces: `dev/kind/fixtures.yaml` (объекты проекта `team-a`); `dev/kind/setup.sh` применяет их; `src-tauri/tests/common/mod.rs` (`no_cancel`, `kubeconfig_path`, `import`); `src-tauri/tests/it_read.rs` (6 тестов).

Спец. §17. Стенд kind получает объекты проекта `team-a`: Deployment `web` (2 реплики, переменные из ConfigMap и Secret), `crash` (падает — CrashLoopBackOff), `pull` (образ не скачать), `pending` (не размещается), `logger` (строка в секунду), Job `migrate`, CronJob `report` (приостановлен), Service и Ingress `web`, ConfigMap, Secret, PVC, ResourceQuota, LimitRange. Образы не скачиваются: контейнеры запускает образ-помощник local-path-provisioner, который уже есть на ноде (в нём только bash, mkdir и rm, поэтому ожидание — `read -t` на пустом канале). Тесты `it_read.rs` проверяют на настоящем API-сервере проблемные поды, workloads с историей и подами, события, сеть (endpoints по EndpointSlices), конфигурацию без значений Secret, PVC, квоты, YAML, логи (предыдущий запуск и follow), watch (новый под приходит изменением) и права сервисной учётной записи (обзор без нод, права в `team-a` и `default`, проекты из профиля). Kubelet 1.37 пишет CrashLoopBackOff не всегда сразу: бывает, что между первыми перезапусками контейнер минутами остаётся завершённым (статус Error), а «предыдущий» запуск уже убран (тогда вместо лога приходит строка «unable to retrieve container logs»); поэтому проблемные поды тесты ждут до 600 с, лог предыдущего запуска — до 300 с, и на свежем кластере `it_read` иногда идёт 4–5 минут. Общие помощники вынесены из `it_connect.rs` в `tests/common`. Без `GO_IT_KUBECONFIG` тесты пропускаются.

- [ ] **Step 1: Тесты.**

<!-- block: file t18-src-tauri-tests-common-mod-rs step=18.1 path=src-tauri/tests/common/mod.rs -->
````rust
//! Общее для интеграционных тестов на kind: kubeconfig стенда и подключение из его контекста.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use glafira_kubernetes_lib::k8s::kubeconfig;
use glafira_kubernetes_lib::policy::PolicyStatus;
use glafira_kubernetes_lib::secrets::{CLIENT_KEY, MemoryStore, SecretStore, TOKEN, secret_key};
use glafira_kubernetes_lib::storage::profile::ConnectionProfile;
use tokio::sync::oneshot;

/// Сигнал отмены, который не срабатывает (отправитель живёт до конца теста).
pub fn no_cancel() -> oneshot::Receiver<()> {
    let (tx, rx) = oneshot::channel();
    std::mem::forget(tx);
    rx
}

pub fn kubeconfig_path() -> Option<PathBuf> {
    let p = std::env::var_os("GO_IT_KUBECONFIG").map(PathBuf::from);
    if p.is_none() {
        eprintln!("GO_IT_KUBECONFIG не задан — интеграционный тест пропущен");
    }
    p
}

/// Подключение из контекста kubeconfig — так же, как импорт в приложении.
pub fn import(path: &Path, context: &str) -> (ConnectionProfile, Arc<MemoryStore>) {
    let kc = kubeconfig::read(path).unwrap();
    let preview = kubeconfig::preview(&kc, path, &[], &PolicyStatus::default(), &|_, _| None);
    let ctx = preview
        .contexts
        .iter()
        .find(|c| c.name == context)
        .unwrap_or_else(|| panic!("нет контекста {context}"));
    let imported = kubeconfig::import_context(&kc, context, ctx).unwrap();
    let secrets = Arc::new(MemoryStore::default());
    if let Some(t) = &imported.token {
        secrets
            .set(&secret_key(&imported.profile.id, TOKEN), t)
            .unwrap();
    }
    if let Some(k) = &imported.client_key {
        secrets
            .set(&secret_key(&imported.profile.id, CLIENT_KEY), k)
            .unwrap();
    }
    (imported.profile, secrets)
}
````

<!-- block: file t18-src-tauri-tests-it-read-rs step=18.1 path=src-tauri/tests/it_read.rs -->
````rust
//! Чтение кластера на настоящем API-сервере (kind, `dev/kind/setup.sh` с `fixtures.yaml`): проблемные
//! поды, workloads, события, сеть, конфигурация, PVC, квоты, YAML, логи, watch и права. Нужен
//! `GO_IT_KUBECONFIG`; без переменной тесты пропускаются.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{import, kubeconfig_path, no_cancel};
use glafira_kubernetes_lib::error::ErrorCode;
use glafira_kubernetes_lib::k8s::pods::{PodProblem, PodRow, WorkloadRef, list_pods};
use glafira_kubernetes_lib::k8s::pool::{Connected, open};
use glafira_kubernetes_lib::k8s::workloads::{WorkloadKind, WorkloadProblem};
use glafira_kubernetes_lib::k8s::{
    access, configs, events, network, overview, projects, quotas, storage, workloads, yaml,
};
use glafira_kubernetes_lib::logs::{self, KubeLogs, LogEvent, LogParams, Matcher};
use glafira_kubernetes_lib::sessions::Sink;
use glafira_kubernetes_lib::watch::{self, Row, RowChange, WatchEvent, WatchKind};
use tokio_util::sync::CancellationToken;

const NS: &str = "team-a";

async fn connect(context: &str) -> Option<Connected> {
    let path = kubeconfig_path()?;
    let (profile, secrets) = import(&path, context);
    Some(open(profile, secrets, no_cancel()).await.unwrap())
}

/// Ждёт, пока `check` не вернёт `Some`, не дольше `limit`.
async fn until<T, F, Fut>(limit: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let wait = async {
        loop {
            if let Some(v) = check().await {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };
    tokio::time::timeout(limit, wait)
        .await
        .expect("не дождались")
}

fn by_app<'a>(pods: &'a [PodRow], app: &str) -> Vec<&'a PodRow> {
    pods.iter()
        .filter(|p| p.owner.as_ref().is_some_and(|o| o.name == app))
        .collect()
}

/// Приёмник событий сессии для тестов.
#[derive(Clone)]
struct Collected<T>(Arc<Mutex<Vec<T>>>);

impl<T> Default for Collected<T> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }
}

impl<T: Send + 'static> Sink<T> for Collected<T> {
    fn send(&self, item: T) -> bool {
        self.0.lock().unwrap().push(item);
        true
    }
}

#[tokio::test]
async fn problem_pods_are_recognized() {
    let Some(c) = connect("admin").await else {
        return;
    };
    let r = c.reader();
    // Kubelet 1.37 пишет CrashLoopBackOff не всегда сразу: бывает, что между первыми перезапусками
    // контейнер остаётся завершённым (статус Error), пока пауза перед перезапуском не вырастет до минут.
    let pods = until(Duration::from_secs(600), || async {
        let pods = list_pods(&r, NS, None).await.unwrap().items;
        by_app(&pods, "crash")
            .iter()
            .any(|p| p.status == "CrashLoopBackOff")
            .then_some(pods)
    })
    .await;
    let one = |app: &str| by_app(&pods, app)[0].clone();
    let crash = by_app(&pods, "crash")
        .into_iter()
        .find(|p| p.status == "CrashLoopBackOff")
        .unwrap()
        .clone();
    assert_eq!(crash.problem, Some(PodProblem::CrashLoop));
    assert!(
        crash.restarts >= 1 && crash.last_restart.is_some(),
        "{crash:?}"
    );
    let pull = one("pull");
    assert_eq!(pull.problem, Some(PodProblem::ImagePull), "{pull:?}");
    assert!(
        ["ErrImagePull", "ImagePullBackOff"].contains(&pull.status.as_str()),
        "{pull:?}"
    );
    let pending = one("pending");
    assert_eq!(
        (pending.phase.as_str(), pending.problem),
        ("Pending", Some(PodProblem::Pending))
    );
    let web = by_app(&pods, "web");
    assert_eq!(web.len(), 2);
    assert!(
        web.iter().all(|p| p.problem.is_none() && p.ready == 1),
        "{web:?}"
    );
    assert_eq!(web[0].owner.as_ref().unwrap().kind, "Deployment");
    let details = glafira_kubernetes_lib::k8s::pods::get_pod(&r, NS, &crash.name)
        .await
        .unwrap();
    let last = details.containers[0].last_state.as_ref().unwrap();
    assert_eq!(last.exit_code, Some(1));
}

#[tokio::test]
async fn workloads_with_details_and_their_pods() {
    let Some(c) = connect("admin").await else {
        return;
    };
    let r = c.reader();
    let list = workloads::list_workloads(&r, NS, &c.info.api_groups).await;
    assert!(list.errors.is_empty(), "{:?}", list.errors);
    let find = |kind: WorkloadKind, name: &str| {
        list.items
            .iter()
            .find(|w| w.kind == kind && w.name == name)
            .unwrap_or_else(|| panic!("{kind:?}/{name}"))
            .clone()
    };
    let web = find(WorkloadKind::Deployment, "web");
    assert_eq!((web.desired, web.available), (Some(2), Some(2)));
    assert_eq!(web.problem, None);
    assert_eq!(
        find(WorkloadKind::Deployment, "crash").problem,
        Some(WorkloadProblem::Unavailable)
    );
    assert_eq!(find(WorkloadKind::Job, "migrate").ready, Some(1));
    let report = find(WorkloadKind::CronJob, "report");
    assert_eq!(
        (report.schedule.as_deref(), report.suspended),
        (Some("0 3 * * *"), Some(true))
    );
    let d = workloads::get_workload(&r, NS, WorkloadKind::Deployment, "web")
        .await
        .unwrap();
    assert_eq!(d.selector.as_deref(), Some("app=web"));
    let history = d.history.unwrap().data.unwrap();
    assert!(history.iter().any(|h| h.current), "{history:?}");
    let env: Vec<_> = d.containers[0]
        .env
        .iter()
        .map(|e| e.name.as_str())
        .collect();
    assert_eq!(env, ["LOG_LEVEL", "DB_PASSWORD"]);
    assert!(d.hpa.data.unwrap().is_none() && d.pdb.data.unwrap().is_none());
    let web_ref = WorkloadRef {
        kind: WorkloadKind::Deployment,
        name: "web".into(),
    };
    let pods = list_pods(&r, NS, Some(&web_ref)).await.unwrap();
    assert_eq!(pods.items.len(), 2);
    let y = yaml::object_yaml(&r, NS, yaml::YamlKind::Deployment, "web")
        .await
        .unwrap();
    assert!(
        y.contains("kind: Deployment") && !y.contains("managedFields"),
        "{y}"
    );
}

#[tokio::test]
async fn events_network_config_storage_and_quotas() {
    let Some(c) = connect("admin").await else {
        return;
    };
    let r = c.reader();
    let crash = events::EventObject {
        kind: "Deployment".into(),
        name: "crash".into(),
    };
    let evs = until(Duration::from_secs(60), || async {
        let l = events::list_events(&r, NS, Some(&crash)).await.unwrap();
        l.items
            .iter()
            .any(|e| e.reason == "BackOff" && e.kind == "Warning")
            .then_some(l.items)
    })
    .await;
    assert!(
        evs.iter().all(|e| e.object.name.starts_with("crash")),
        "{evs:?}"
    );

    let net = network::get_network(&r, NS, &c.info.api_groups).await;
    let services = net.services.data.unwrap();
    let web = services.iter().find(|s| s.name == "web").unwrap();
    assert_eq!(web.ports, ["80/TCP → 8080"]);
    assert_eq!(
        web.endpoints.map(|e| (e.ready, e.total)),
        Some((2, 2)),
        "готовые endpoints по EndpointSlices"
    );
    let ing = &net.ingresses.data.unwrap()[0];
    assert_eq!(ing.rules[0].host.as_deref(), Some("web.example.test"));
    assert_eq!(ing.rules[0].service.as_deref(), Some("web"));
    assert!(
        net.routes.is_none() && net.gateways.is_none(),
        "на kind нет Route и Gateway API"
    );

    let cfg = configs::get_config(&r, NS).await;
    let cm = cfg.config_maps.data.unwrap();
    assert!(
        cm.iter()
            .any(|c| c.name == "web-config" && c.keys == ["LOG_LEVEL"])
    );
    let secrets = cfg.secrets.data.unwrap();
    let db = secrets.iter().find(|s| s.name == "db").unwrap();
    assert_eq!(db.keys, ["password"]);
    assert!(
        !serde_json::to_string(&secrets)
            .unwrap()
            .contains("czNjcjN0")
    );
    let values = configs::config_map_values(&r, NS, "web-config")
        .await
        .unwrap();
    assert_eq!(values["LOG_LEVEL"].text.as_deref(), Some("debug"));

    let pvcs = storage::list_pvcs(&r, NS).await.unwrap();
    let data = pvcs.items.iter().find(|p| p.name == "data").unwrap();
    assert_eq!(
        data.phase.as_deref(),
        Some("Pending"),
        "WaitForFirstConsumer"
    );
    assert_eq!(data.requested.as_deref(), Some("1Gi"));
    assert_eq!(data.mounted_by.as_deref(), Some(&[][..]));

    let q = quotas::get_quotas(&r, NS).await;
    let compute = &q.quotas.data.unwrap()[0];
    let pods = compute
        .entries
        .iter()
        .find(|e| e.resource == "pods")
        .unwrap();
    assert!(pods.used.is_some() && pods.ratio.is_some(), "{pods:?}");
    let lr = &q.limit_ranges.data.unwrap()[0];
    assert!(
        lr.limits
            .iter()
            .any(|l| l.resource == "cpu" && l.default.as_deref() == Some("100m"))
    );
}

#[tokio::test]
async fn logs_of_a_crashed_and_of_a_running_container() {
    let Some(c) = connect("admin").await else {
        return;
    };
    let r = c.reader();
    // Под, у которого уже был предыдущий запуск, и работающий под logger.
    let (crash, logger) = until(Duration::from_secs(120), || async {
        let pods = list_pods(&r, NS, None).await.unwrap().items;
        let crash = by_app(&pods, "crash")
            .into_iter()
            .find(|p| p.restarts >= 1)?;
        let logger = by_app(&pods, "logger").into_iter().find(|p| p.ready == 1)?;
        Some((crash.name.clone(), logger.name.clone()))
    })
    .await;
    let params = |pod: &str, previous: bool, follow: bool| LogParams {
        connection_id: "c".into(),
        namespace: NS.into(),
        pod: pod.into(),
        container: None,
        previous,
        tail_lines: None,
        since_seconds: None,
        since_time: None,
        follow,
        filter: None,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("crash.log");
    let source = KubeLogs {
        reader: r.clone(),
        params: params(&crash, true, false),
    };
    // Между перезапусками (статус Error) «предыдущий» запуск kubelet уже мог убрать, и вместо лога
    // приходит одна строка «unable to retrieve container logs»: ждём, пока предыдущий запуск читается.
    until(Duration::from_secs(300), || async {
        let n = logs::save(&source, &Matcher::All, &path, false)
            .await
            .ok()?;
        let text = std::fs::read_to_string(&path).unwrap();
        (n >= 2 && text.contains("starting") && text.contains("boom")).then_some(())
    })
    .await;

    let sink: Collected<LogEvent> = Collected::default();
    let cancel = CancellationToken::new();
    let follow = KubeLogs {
        reader: r.clone(),
        params: params(&logger, false, true),
    };
    let task = tokio::spawn(logs::run(
        follow,
        true,
        Matcher::All,
        sink.clone(),
        cancel.clone(),
    ));
    let shown = until(Duration::from_secs(20), || {
        let sink = sink.clone();
        async move {
            let lines: Vec<String> = sink
                .0
                .lock()
                .unwrap()
                .iter()
                .flat_map(|e| match e {
                    LogEvent::Lines { lines, .. } => lines.iter().map(|l| l.text.clone()).collect(),
                    _ => vec![],
                })
                .collect();
            (lines.len() >= 3).then_some(lines)
        }
    })
    .await;
    cancel.cancel();
    task.await.unwrap();
    assert!(shown.iter().all(|l| l.starts_with("line ")), "{shown:?}");
}

#[tokio::test]
async fn watch_sees_a_new_pod() {
    use k8s_openapi::api::core::v1::Pod;
    let Some(c) = connect("admin").await else {
        return;
    };
    let r = c.reader();
    let sink: Collected<WatchEvent> = Collected::default();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(watch::run(
        watch::sources(&r, NS, WatchKind::Pods, None, &c.info.api_groups),
        watch::poll(
            r.clone(),
            NS.into(),
            WatchKind::Pods,
            None,
            c.info.api_groups,
        ),
        sink.clone(),
        cancel.clone(),
    ));
    until(Duration::from_secs(20), || {
        let sink = sink.clone();
        async move {
            sink.0
                .lock()
                .unwrap()
                .iter()
                .any(|e| matches!(e, WatchEvent::Reset { items, .. } if !items.is_empty()))
                .then_some(())
        }
    })
    .await;
    let name = format!("watch-probe-{}", uuid::Uuid::new_v4().simple());
    let api: kube::Api<Pod> = kube::Api::namespaced(c.client.clone(), NS);
    let pod: Pod = serde_json::from_value(serde_json::json!({
        "metadata": {"name": name},
        "spec": {"nodeSelector": {"glafira/none": "true"}, "containers": [{"name": "x", "image": "registry.invalid/x:1"}]}
    }))
    .unwrap();
    api.create(&Default::default(), &pod).await.unwrap();
    until(Duration::from_secs(20), || {
        let (sink, name) = (sink.clone(), name.clone());
        async move {
            sink.0
                .lock()
                .unwrap()
                .iter()
                .any(|e| match e {
                    WatchEvent::Changes { changes } => changes.iter().any(
                        |c| matches!(c, RowChange::Upsert { row: Row::Pod(p) } if p.name == name),
                    ),
                    _ => false,
                })
                .then_some(())
        }
    })
    .await;
    api.delete(&name, &Default::default()).await.unwrap();
    cancel.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn cluster_overview_and_the_rights_of_a_service_account() {
    let Some(admin) = connect("admin").await else {
        return;
    };
    let o = overview::cluster_overview(&admin.reader(), &admin.info).await;
    assert!(o.readyz.ok, "{:?}", o.readyz);
    let nodes = o.nodes.data.unwrap();
    assert_eq!((nodes.total, nodes.ready), (1, 1));
    assert_eq!(nodes.roles[0].role, "control-plane");
    assert!(o.projects.data.unwrap().count >= 5);
    assert!(o.platform_details.is_none() && o.gateway_classes.is_none());

    let dev = connect("dev-token").await.unwrap();
    let r = dev.reader();
    let o = overview::cluster_overview(&r, &dev.info).await;
    assert!(o.readyz.ok, "readyz читает любой пользователь");
    assert_eq!(o.nodes.error.unwrap().code, ErrorCode::Forbidden);
    let a = access::namespace_access(&r, NS).await.unwrap();
    assert!(a.resources["pods"].watch && a.resources["pods/log"].get);
    assert!(a.resources["deployments.apps"].list);
    let elsewhere = access::namespace_access(&r, "default").await.unwrap();
    assert!(
        !elsewhere.resources["pods"].list,
        "роль edit — только в team-a"
    );
    let mut profile = import(&kubeconfig_path().unwrap(), "dev-token").0;
    profile.manual_namespaces = vec![NS.into()];
    let p = projects::list_projects(&r, dev.info.api_groups.projects, &profile)
        .await
        .unwrap();
    assert_eq!(p.error.unwrap().code, ErrorCode::Forbidden);
    assert_eq!(p.items[0].name, NS);
    assert!(p.items[0].favorite && p.items[0].from_profile);
    let e = list_pods(&r, "default", None).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Forbidden);
    assert_eq!(e.params["namespace"], "default");
}
````

<!-- block: patch t18-tests step=18.1 -->
````diff
diff --git a/src-tauri/tests/it_connect.rs b/src-tauri/tests/it_connect.rs
index 048cc10..3c985c3 100644
--- a/src-tauri/tests/it_connect.rs
+++ b/src-tauri/tests/it_connect.rs
@@ -1,61 +1,23 @@
 //! Интеграция с настоящим API-сервером (kind, `dev/kind/setup.sh`). Нужен `GO_IT_KUBECONFIG` —
 //! kubeconfig с контекстами `admin` и `dev-token`; без переменной тесты пропускаются.
 
-use std::path::{Path, PathBuf};
+mod common;
+
 use std::sync::Arc;
 
+use common::{import, kubeconfig_path, no_cancel};
 use glafira_kubernetes_lib::error::ErrorCode;
 use glafira_kubernetes_lib::k8s::cluster::can_i;
-use glafira_kubernetes_lib::k8s::{connectivity, kubeconfig, pool};
+use glafira_kubernetes_lib::k8s::{connectivity, pool};
 use glafira_kubernetes_lib::platform::PlatformKind;
 use glafira_kubernetes_lib::policy::PolicyStatus;
-use glafira_kubernetes_lib::secrets::{CLIENT_KEY, MemoryStore, SecretStore, TOKEN, secret_key};
+use glafira_kubernetes_lib::secrets::{MemoryStore, SecretStore, TOKEN, secret_key};
 use glafira_kubernetes_lib::storage::profile::ConnectionProfile;
-use tokio::sync::oneshot;
-
-/// Сигнал отмены, который не срабатывает (отправитель живёт до конца теста).
-fn no_cancel() -> oneshot::Receiver<()> {
-    let (tx, rx) = oneshot::channel();
-    std::mem::forget(tx);
-    rx
-}
 
 async fn test(p: &ConnectionProfile, secrets: Arc<MemoryStore>) -> connectivity::TestReport {
     connectivity::test_connection(p, secrets, &PolicyStatus::default(), no_cancel()).await
 }
 
-fn kubeconfig_path() -> Option<PathBuf> {
-    let p = std::env::var_os("GO_IT_KUBECONFIG").map(PathBuf::from);
-    if p.is_none() {
-        eprintln!("GO_IT_KUBECONFIG не задан — интеграционный тест пропущен");
-    }
-    p
-}
-
-/// Подключение из контекста kubeconfig — так же, как импорт в приложении.
-fn import(path: &Path, context: &str) -> (ConnectionProfile, Arc<MemoryStore>) {
-    let kc = kubeconfig::read(path).unwrap();
-    let preview = kubeconfig::preview(&kc, path, &[], &PolicyStatus::default(), &|_, _| None);
-    let ctx = preview
-        .contexts
-        .iter()
-        .find(|c| c.name == context)
-        .unwrap_or_else(|| panic!("нет контекста {context}"));
-    let imported = kubeconfig::import_context(&kc, context, ctx).unwrap();
-    let secrets = Arc::new(MemoryStore::default());
-    if let Some(t) = &imported.token {
-        secrets
-            .set(&secret_key(&imported.profile.id, TOKEN), t)
-            .unwrap();
-    }
-    if let Some(k) = &imported.client_key {
-        secrets
-            .set(&secret_key(&imported.profile.id, CLIENT_KEY), k)
-            .unwrap();
-    }
-    (imported.profile, secrets)
-}
-
 fn steps(r: &connectivity::TestReport) -> Vec<(&str, bool)> {
     r.steps.iter().map(|s| (s.name, s.ok)).collect()
 }
````

- [ ] **Step 2: Реализация.**

<!-- block: file t18-dev-kind-fixtures-yaml step=18.2 path=dev/kind/fixtures.yaml -->
````yaml
# Объекты проекта team-a для интеграционных тестов чтения (tests/it_read.rs). Образ __HELPER_IMAGE__ —
# помощник local-path-provisioner, который уже есть на ноде kind: стенд не скачивает образы. В нём только
# bash, mkdir и rm, поэтому ожидание — `read -t` на пустом канале вместо sleep.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: team-a
spec:
  replicas: 2
  selector:
    matchLabels: { app: web }
  template:
    metadata:
      labels: { app: web }
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: web
          image: __HELPER_IMAGE__
          imagePullPolicy: IfNotPresent
          command: ["bash", "-c", "exec 3<> <(:); read -u 3"]
          resources:
            requests: { cpu: 10m, memory: 16Mi }
          env:
            - name: LOG_LEVEL
              valueFrom: { configMapKeyRef: { name: web-config, key: LOG_LEVEL } }
            - name: DB_PASSWORD
              valueFrom: { secretKeyRef: { name: db, key: password } }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: crash
  namespace: team-a
spec:
  replicas: 1
  selector:
    matchLabels: { app: crash }
  template:
    metadata:
      labels: { app: crash }
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: crash
          image: __HELPER_IMAGE__
          imagePullPolicy: IfNotPresent
          command: ["bash", "-c", "echo starting; echo boom >&2; exit 1"]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pull
  namespace: team-a
spec:
  replicas: 1
  selector:
    matchLabels: { app: pull }
  template:
    metadata:
      labels: { app: pull }
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: pull
          image: registry.invalid/glafira/none:1
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pending
  namespace: team-a
spec:
  replicas: 1
  selector:
    matchLabels: { app: pending }
  template:
    metadata:
      labels: { app: pending }
    spec:
      terminationGracePeriodSeconds: 1
      nodeSelector: { glafira/none: "true" }
      containers:
        - name: pending
          image: __HELPER_IMAGE__
          imagePullPolicy: IfNotPresent
          command: ["bash", "-c", "exec 3<> <(:); read -u 3"]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: logger
  namespace: team-a
spec:
  replicas: 1
  selector:
    matchLabels: { app: logger }
  template:
    metadata:
      labels: { app: logger }
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: logger
          image: __HELPER_IMAGE__
          imagePullPolicy: IfNotPresent
          command: ["bash", "-c", "exec 3<> <(:); i=0; while true; do echo \"line $i\"; i=$((i+1)); read -t 1 -u 3 || :; done"]
---
apiVersion: batch/v1
kind: Job
metadata:
  name: migrate
  namespace: team-a
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: __HELPER_IMAGE__
          imagePullPolicy: IfNotPresent
          command: ["bash", "-c", "echo migrated"]
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: report
  namespace: team-a
spec:
  schedule: "0 3 * * *"
  suspend: true
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: report
              image: __HELPER_IMAGE__
              imagePullPolicy: IfNotPresent
              command: ["bash", "-c", "echo report"]
---
apiVersion: v1
kind: Service
metadata:
  name: web
  namespace: team-a
spec:
  selector: { app: web }
  ports:
    - port: 80
      targetPort: 8080
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: web
  namespace: team-a
spec:
  rules:
    - host: web.example.test
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service: { name: web, port: { number: 80 } }
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: web-config
  namespace: team-a
data:
  LOG_LEVEL: debug
---
apiVersion: v1
kind: Secret
metadata:
  name: db
  namespace: team-a
stringData:
  password: s3cr3t-value
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
  namespace: team-a
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests: { storage: 1Gi }
---
apiVersion: v1
kind: ResourceQuota
metadata:
  name: compute
  namespace: team-a
spec:
  hard:
    pods: "20"
    requests.cpu: "4"
---
apiVersion: v1
kind: LimitRange
metadata:
  name: defaults
  namespace: team-a
spec:
  limits:
    - type: Container
      defaultRequest: { cpu: 10m, memory: 16Mi }
      default: { cpu: 100m, memory: 64Mi }
````

<!-- block: patch t18-src step=18.2 -->
````diff
diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml
index 07aef53..acbde4b 100644
--- a/.github/workflows/ci.yml
+++ b/.github/workflows/ci.yml
@@ -52,7 +52,7 @@ jobs:
         with:
           version: v0.33.0
           cluster_name: glafira
-      - name: Учётные записи стенда
+      - name: Стенд — учётные записи и объекты проекта team-a
         run: dev/kind/setup.sh
       - name: Тесты Rust (unit + интеграционные)
         run: cargo test --manifest-path src-tauri/Cargo.toml --lib --tests
diff --git a/dev/kind/README.md b/dev/kind/README.md
index 209c584..68469ad 100644
--- a/dev/kind/README.md
+++ b/dev/kind/README.md
@@ -2,8 +2,11 @@
 
 Обычный Kubernetes в Docker для интеграционных тестов Rust: Glafira проверяет на нём подключение, вход
 (сертификат, токен и exec-плагин — скрипт, который выдаёт токен сервисной учётной записи), платформу `kubernetes`,
-права и импорт kubeconfig. Особенностей OpenShift (проекты, OAuth, маршруты) на kind нет — их проверяет смоук
-на OpenShift Local или dev-кластере (`docs/smoke/o1a.md`).
+права и импорт kubeconfig (`tests/it_connect.rs`), а также чтение кластера (`tests/it_read.rs`): проблемные поды
+(CrashLoopBackOff, ImagePullBackOff, Pending), workloads, события, сеть, конфигурацию, PVC, квоты, YAML, логи и
+watch. Объекты проекта team-a — в `fixtures.yaml`; образы не скачиваются: контейнеры запускает образ-помощник
+local-path-provisioner (bash), который уже есть на ноде. Особенностей OpenShift (проекты, OAuth, маршруты) на kind нет —
+их проверяет смоук на OpenShift Local или dev-кластере (`docs/smoke/o1a.md`, `docs/smoke/o1b.md`).
 
 ```bash
 brew install kind            # или https://kind.sigs.k8s.io/docs/user/quick-start/
@@ -13,4 +16,6 @@ GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-t
 kind delete cluster --name glafira
 ```
 
+На свежем кластере `it_read` иногда идёт 4–5 минут: kubelet не всегда сразу показывает CrashLoopBackOff пода `crash`.
+
 `dev/kind/out/` в git не попадает: в kubeconfig — ключ администратора кластера и токен.
diff --git a/dev/kind/setup.sh b/dev/kind/setup.sh
index 16bad91..187d901 100755
--- a/dev/kind/setup.sh
+++ b/dev/kind/setup.sh
@@ -1,10 +1,12 @@
 #!/usr/bin/env bash
 # Стенд интеграционных тестов на kind: проект team-a, сервисная учётная запись glafira-dev с ролью edit
-# только в нём и kubeconfig dev/kind/out/kubeconfig с контекстами admin (сертификат kind) и dev-token
-# (токен glafira-dev, проект team-a). Перед запуском: kind create cluster --name glafira
+# только в нём, объекты проекта для тестов чтения (fixtures.yaml) и kubeconfig dev/kind/out/kubeconfig
+# с контекстами admin (сертификат kind) и dev-token (токен glafira-dev, проект team-a).
+# Перед запуском: kind create cluster --name glafira
 set -euo pipefail
 cluster="${KIND_CLUSTER:-glafira}"
-out="$(cd "$(dirname "$0")" && pwd)/out"
+here="$(cd "$(dirname "$0")" && pwd)"
+out="$here/out"
 ctx="kind-$cluster"
 k() { kubectl --context "$ctx" "$@"; }
 mkdir -p "$out"
@@ -12,6 +14,15 @@ k create namespace team-a --dry-run=client -o yaml | k apply -f -
 k -n team-a create serviceaccount glafira-dev --dry-run=client -o yaml | k apply -f -
 k -n team-a create rolebinding glafira-dev-edit --clusterrole=edit --serviceaccount=team-a:glafira-dev \
   --dry-run=client -o yaml | k apply -f -
+# Образ с bash, который уже есть на ноде: помощник local-path-provisioner.
+helper="$(k -n local-path-storage get configmap local-path-config -o jsonpath='{.data.helperPod\.yaml}' |
+  awk '/image:/ { print $2; exit }')"
+# Шаблон Job не меняется после создания: повторный запуск стенда создаёт Job заново.
+k -n team-a delete job migrate --ignore-not-found
+sed "s|__HELPER_IMAGE__|$helper|g" "$here/fixtures.yaml" | k apply -f -
+k -n team-a rollout status deployment/web --timeout=180s
+k -n team-a wait --for=condition=complete job/migrate --timeout=180s
+k -n team-a rollout status deployment/logger --timeout=180s
 token="$(k -n team-a create token glafira-dev --duration=24h)"
 kind get kubeconfig --name "$cluster" > "$out/kubeconfig"
 chmod 600 "$out/kubeconfig"
````

- [ ] **Step 3: Проверки.**

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 136`

Стенд kind (если поднят, см. `dev/kind/README.md`; `KUBECONFIG` — отдельный файл, не `~/.kube/config`):  
Run: `GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --test it_connect`  
Expected: PASS — `test result: ok. 4 passed; 0 failed`

Стенд kind (если поднят, см. `dev/kind/README.md`; `KUBECONFIG` — отдельный файл, не `~/.kube/config`):  
Run: `GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --test it_read`  
Expected: PASS — `test result: ok. 6 passed; 0 failed`

- [ ] **Step 4: Commit.**

```bash
git add \
  .github/workflows/ci.yml \
  dev/kind/README.md \
  dev/kind/fixtures.yaml \
  dev/kind/setup.sh \
  src-tauri/tests/common/mod.rs \
  src-tauri/tests/it_connect.rs \
  src-tauri/tests/it_read.rs
git commit -s -m "test(kind): чтение кластера на настоящем API-сервере — проблемные поды, workloads, события, сеть, конфигурация, логи, watch, права" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 19: Документация: справка, смоук, README, глоссарий, уточнения спеки

**Files:**
- Create: `docs/guide/en/pods-logs.md`, `docs/guide/en/projects-workloads.md`, `docs/guide/ru/pods-logs.md`, `docs/guide/ru/projects-workloads.md`, `docs/smoke/o1b.md`
- Modify: `README.md`, `docs/guide/en/connections.md`, `docs/guide/ru/connections.md`, `docs/i18n/glossary.md`, `docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md`, `scripts/i18n-check.mjs`, `src/components/help/helpChapterFor.test.ts`, `src/components/help/helpChapterFor.ts`, `src/guide/chapters.ts`

**Interfaces:**
- Consumes: задачи 1–18.
- Produces: главы руководства `projects-workloads` и `pods-logs` (en, ru), разделы «Обзор кластера» и «Ноды» в главе «Подключения»; `GUIDE_CHAPTERS` (4 главы) в `src/guide/chapters.ts` и `scripts/i18n-check.mjs`; `helpChapterFor`: проект и workload → `projects-workloads`, под → `pods-logs`, ноды → `connections`; `docs/smoke/o1b.md`; README «Чтение кластера»; термины глоссария; спека §4.2, §5.3, §6.6, §6.7, §19.

Спец. §16. Главы «Проекты и workloads» (проекты в дереве, вкладка проекта и права, списки, которые обновляются сами, workloads, сеть, конфигурация, хранилище, квоты) и «Поды и логи» (список подов и проблемы, вкладка пода, логи, истёкшая сессия) на en и ru; F1 открывает главу текущего экрана. Смоук O1b — 19 пунктов на kind и OpenShift Local. В спеку вносятся уточнения прототипа: состав команд §4.2 (`cancel_connection_test`, права и срок токена — в `connect`, `get_workload` с историей, `get_network`, `get_config`, `get_config_map_values`, `save_logs(params, path, timestamps)`; нет `get_capabilities`, `get_project_overview`, `get_rollout_history`, `list_hpas`), группы на шаге «Вход» (§5.3), сохранение и переподключение логов (§6.6), поды CronJob и перечитывание после входа (§6.7), состав O1b (§19). Если спеку к моменту выполнения правили, патч спеки применяется вручную по смыслу.

- [ ] **Step 1: Тесты.**

<!-- block: patch t19-tests step=19.1 -->
````diff
diff --git a/src/components/help/helpChapterFor.test.ts b/src/components/help/helpChapterFor.test.ts
index 89448d2..62d6326 100644
--- a/src/components/help/helpChapterFor.test.ts
+++ b/src/components/help/helpChapterFor.test.ts
@@ -5,6 +5,10 @@ import { helpChapterFor } from "./helpChapterFor";
 const cases: [TabSpec | null, string][] = [
   [{ kind: "connection-form", connectionId: null }, "connections"],
   [{ kind: "cluster", connectionId: "c" }, "connections"],
+  [{ kind: "nodes", connectionId: "c" }, "connections"],
+  [{ kind: "project", connectionId: "c", namespace: "team-a" }, "projects-workloads"],
+  [{ kind: "workload", connectionId: "c", namespace: "team-a", workloadKind: "Deployment", name: "web" }, "projects-workloads"],
+  [{ kind: "pod", connectionId: "c", namespace: "team-a", name: "web-1" }, "pods-logs"],
   [{ kind: "about" }, "getting-started"],
   [{ kind: "settings" }, "getting-started"],
   [{ kind: "help", chapter: "connections" }, "getting-started"],
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/help/helpChapterFor.test.ts`  
Expected: FAIL — `helpChapterFor` ведёт вкладки проекта, workload'а и пода в «Начало работы»: глав `projects-workloads` и `pods-logs` ещё нет.

- [ ] **Step 3: Реализация.**

<!-- block: file t19-docs-guide-en-pods-logs-md step=19.3 path=docs/guide/en/pods-logs.md -->
````markdown
# Pods and logs

## Pod list {#pods}

The pods of a project or a workload show the status as `kubectl get pods` does (`Running`, `CrashLoopBackOff`,
`Init:1/2`, `Completed`…), the ready containers, the restarts and when the last one was, the node, the age, the QoS
class and the owner. A problem pod — the image cannot be pulled, a container was killed for memory (`OOMKilled`),
restarts in a loop, the pod waits to be scheduled (`Pending`) or has failed — is highlighted; the problems-only
filter leaves just them.

## Pod tab {#pod}

Click a pod to open its tab. [[ui:objects.tabs.overview]] shows the phase, the conditions, the node, the IP, the QoS,
the owner (a click opens the workload) and every container, init containers too: the image and the digest that is
running, the state and the last state with the reason, the exit code and the time, the restarts, the probes, the
requests and limits and the environment variables — names and sources only (a value in the spec, a ConfigMap or
Secret key, a field), never the values. [[ui:objects.tabs.events]] and YAML are there too.

## Logs {#logs}

[[ui:logs.title]] shows the log of a container: choose the [[ui:logs.container]], the [[ui:logs.previous]], the
[[ui:logs.tail]] (500 by default) or where to start from ([[ui:logs.since]]), and [[ui:logs.follow]].
[[ui:logs.timestamps]] adds the time of each line. The [[ui:logs.filter]] — a substring or a regular expression,
[[ui:logs.caseSensitive]] if needed — runs in Glafira before the lines reach the window, and the counter shows how many
lines were shown and how many were read. The window keeps up to 100,000 lines; a line longer than 64 KB is cut. When
the connection is lost while following, Glafira reconnects from the last line it has, without repeats, and reports an
error after a minute without success. [[ui:logs.save]] reads the log again with the same settings (without
following) and saves the whole lines to a file.

## When the session expires {#session}

If the token expires while you read the cluster, the tabs show the error with [[ui:objects.signInAgain]]: it opens the
connection tab, where you sign in again. After that the open tabs of the connection read their data again by
themselves, and the logs and the lists that update themselves start over.
````

<!-- block: file t19-docs-guide-en-projects-workloads-md step=19.3 path=docs/guide/en/projects-workloads.md -->
````markdown
# Projects and workloads

Glafira reads the cluster with your rights. A part of a screen you may not read shows a dash (—) with the reason, and
the rest of the screen is shown.

## Projects {#projects}

In the tree, [[ui:sidebar.projects]] under a connection opens the list of its projects: OpenShift projects or, on
other clusters, namespaces. Favorite projects of the connection are on top (★); with many projects a filter
appears. If the cluster does not list projects for you, the tree shows the projects of the profile — its favorites
and [[ui:connections.form.manualNamespaces]].

## Project tab {#project}

A project opens in its own tab: [[ui:objects.tabs.overview]], [[ui:project.tabs.workloads]],
[[ui:objects.tabs.pods]], [[ui:project.tabs.network]], [[ui:project.tabs.config]], [[ui:project.tabs.storage]],
[[ui:objects.tabs.events]] and [[ui:project.tabs.quotas]]. Glafira asks the cluster what you may read in the project
and hides the subtabs you have no rights for; the answer is kept for 5 minutes. The overview shows the pods by phase
and the problem pods, the workloads that are not fine, the quotas that are almost used up and the `Warning` events of
the last hour.

## Lists that update themselves {#watch}

Workloads, pods and events of a project update by themselves while their subtab is open: Glafira watches the
changes in the cluster. Without the right to watch, the list refreshes every 10 seconds. Other screens have
[[ui:common.refresh]] and [[ui:common.autoRefresh]] (every 10 or 30 seconds).

## Workloads {#workloads}

Deployment, StatefulSet, DaemonSet, DeploymentConfig (OpenShift), Job and CronJob, with a filter by kind and name.
A workload tab shows the replicas (desired, ready, available, updated), the problem if there is one, the conditions,
the pod selector, the strategy, the HPA and the PodDisruptionBudget, the rollout history (ReplicaSets or
ReplicationControllers with the revision and the images) and the containers. [[ui:objects.tabs.pods]] lists its
pods, [[ui:objects.tabs.events]] shows its events together with those of its pods, and YAML shows the object without
`managedFields`.

## Network {#network}

Services with the ready endpoints, Routes (OpenShift) with the router admission and the expiry of the built-in
certificate, Ingress with the rules, the TLS hosts and the Routes OpenShift created from it, and with Gateway API —
Gateway, HTTPRoute and GRPCRoute with the conditions of each parent. Only the kinds your cluster has are shown.

## Configuration, storage and quotas {#config}

ConfigMaps show their keys; [[ui:project.config.showValues]] reads the values. Secrets show only the name, the type
and the keys: Glafira never shows their values. Storage lists the PVCs with the status, the class, the capacity, the
access modes and the pods that mount them. Quotas show each ResourceQuota (used and limit) and the LimitRange limits.
````

<!-- block: file t19-docs-guide-ru-pods-logs-md step=19.3 path=docs/guide/ru/pods-logs.md -->
````markdown
# Поды и логи

## Список подов {#pods}

Поды проекта или workload'а показывают статус так же, как `kubectl get pods` (`Running`, `CrashLoopBackOff`,
`Init:1/2`, `Completed`…), готовые контейнеры, рестарты и когда был последний, ноду, возраст, класс QoS и владельца.
Проблемный под — образ не скачивается, контейнер завершён по памяти (`OOMKilled`), перезапускается в цикле, под ждёт
размещения (`Pending`) или упал — выделен; фильтр «только проблемные» оставляет только такие.

## Вкладка пода {#pod}

Щелчок по поду открывает его вкладку. [[ui:objects.tabs.overview]] — фаза, условия, нода, IP, QoS, владелец (щелчок
открывает workload) и каждый контейнер, в том числе init: образ и digest, который запущен, состояние и прошлое
состояние с причиной, кодом выхода и временем, рестарты, пробы, requests и limits и переменные окружения — только имена
и источники (значение в спецификации, ключ ConfigMap или Secret, поле), значения не показываются. Есть и
[[ui:objects.tabs.events]], и YAML.

## Логи {#logs}

[[ui:logs.title]] показывают лог контейнера: выберите [[ui:logs.container]], [[ui:logs.previous]],
[[ui:logs.tail]] (по умолчанию 500) или откуда начать ([[ui:logs.since]]) и [[ui:logs.follow]].
[[ui:logs.timestamps]] добавляет время каждой строки. [[ui:logs.filter]] — подстрока или регулярное выражение, при
необходимости [[ui:logs.caseSensitive]] — работает в Glafira до того, как строки попадут в окно, а счётчик показывает,
сколько строк показано и сколько прочитано. Окно хранит до 100 000 строк; строка длиннее 64 КБ обрезается. Если в
режиме слежения соединение потеряно, Glafira переподключается с последней полученной строки без повторов, а через
минуту без успеха сообщает об ошибке. [[ui:logs.save]] читает лог заново с теми же настройками (без слежения) и
сохраняет строки целиком в файл.

## Когда сессия истекла {#session}

Если токен истёк, пока вы читаете кластер, вкладки показывают ошибку с кнопкой [[ui:objects.signInAgain]]: она
открывает вкладку подключения, где нужно войти снова. После входа открытые вкладки подключения сами перечитывают
данные, а логи и списки, которые обновляются сами, начинаются заново.
````

<!-- block: file t19-docs-guide-ru-projects-workloads-md step=19.3 path=docs/guide/ru/projects-workloads.md -->
````markdown
# Проекты и workloads

Glafira читает кластер с вашими правами. Часть экрана, которую вам нельзя читать, показывается как «—» с причиной, а
остальной экран виден.

## Проекты {#projects}

В дереве пункт [[ui:sidebar.projects]] под подключением открывает список его проектов: проекты OpenShift или, в других
кластерах, namespace. Избранные проекты подключения — сверху (★); если проектов много, появляется фильтр. Если кластер
не даёт вам список проектов, дерево показывает проекты из профиля — избранные и
[[ui:connections.form.manualNamespaces]].

## Вкладка проекта {#project}

Проект открывается в своей вкладке: [[ui:objects.tabs.overview]], [[ui:project.tabs.workloads]],
[[ui:objects.tabs.pods]], [[ui:project.tabs.network]], [[ui:project.tabs.config]], [[ui:project.tabs.storage]],
[[ui:objects.tabs.events]] и [[ui:project.tabs.quotas]]. Glafira спрашивает у кластера, что вам можно читать в проекте,
и прячет подвкладки, на которые прав нет; ответ хранится 5 минут. Обзор показывает поды по фазам и проблемные поды,
workloads не в норме, почти исчерпанные квоты и события `Warning` за последний час.

## Списки, которые обновляются сами {#watch}

Workloads, поды и события проекта обновляются сами, пока открыта их подвкладка: Glafira следит за изменениями в
кластере. Без права на watch список перечитывается раз в 10 секунд. На остальных экранах есть [[ui:common.refresh]] и
[[ui:common.autoRefresh]] (каждые 10 или 30 секунд).

## Workloads {#workloads}

Deployment, StatefulSet, DaemonSet, DeploymentConfig (OpenShift), Job и CronJob, с фильтром по виду и имени. Вкладка
workload'а показывает реплики (желаемо, готово, доступно, обновлено), проблему, если она есть, условия, селектор подов,
стратегию, HPA и PodDisruptionBudget, историю выкладки (ReplicaSets или ReplicationControllers с ревизией и образами) и
контейнеры. [[ui:objects.tabs.pods]] — его поды, [[ui:objects.tabs.events]] — его события вместе с событиями его подов,
YAML — объект без `managedFields`.

## Сеть {#network}

Services с готовыми endpoints, Routes (OpenShift) с решением роутера и сроком встроенного сертификата, Ingress с
правилами, TLS-хостами и Routes, которые OpenShift создал из него, а при Gateway API — Gateway, HTTPRoute и GRPCRoute с
условиями от каждого родителя. Показываются только те виды, которые есть в вашем кластере.

## Конфигурация, хранилище и квоты {#config}

У ConfigMaps видны ключи; [[ui:project.config.showValues]] читает значения. У Secrets видны только имя, тип и ключи:
их значения Glafira не показывает никогда. Хранилище — PVC со статусом, классом, ёмкостью, режимами доступа и подами,
которые их монтируют. Квоты — каждая ResourceQuota (использовано и лимит) и границы LimitRange.
````

<!-- block: file t19-docs-smoke-o1b-md step=19.3 path=docs/smoke/o1b.md -->
````markdown
# Смоук O1b (чтение кластера)

Сборка: `pnpm tauri dev` (если порт 1420 занят — `pnpm tauri dev --config
'{"build":{"devUrl":"http://localhost:1430","beforeDevCommand":"pnpm dev --port 1430"}}'`). Кластеры: OpenShift Local
(`crc start`, пользователи `kubeadmin` и `developer`) или dev-кластер OpenShift — для пунктов про OpenShift; стенд
kind (`dev/kind/README.md`, объекты проекта `team-a` из `dev/kind/fixtures.yaml`) — для остальных. Подключения из
смоука O1a: `crc` (вход через браузер), `admin` и `dev-token` (импорт `dev/kind/out/kubeconfig`).

1. Шаг «Вход» проверки подключения. Форма `admin` → «Проверить подключение»: «Вход выполнен: kubernetes-admin,
   группы: kubeadm:cluster-admins, system:authenticated». У пользователя с десятками групп — первые 10 и «+N».
2. Обзор кластера (kind, `admin`). Вкладка подключения: под сведениями — «Обзор кластера»: «API-сервер» ✅ готов,
   «Готовые ноды» 1 / 1, «По ролям: control-plane 1/1», «Проекты» — число namespace, «Классы Ingress» — «—».
   «Автообновление» — «каждые 10 с»: обзор перечитывается сам. Кнопка «Ноды» открывает вкладку нод.
3. Ноды. Вкладка «Ноды»: нода kind — Ready, роль control-plane, версия kubelet; щелчок — условия, адреса, ресурсы
   («Ёмкость» и «Доступно подам»), метки.
4. Обзор без прав (kind, `dev-token`): «API-сервер» ✅ (его читает любой), «Готовые ноды» и «Проекты» — «— Нет прав:
   list … на уровне кластера», внизу «Остальное скрыто правами.»; кнопки «Ноды» нет.
5. Обзор OpenShift (`kubeadmin` на OpenShift Local): версия, «Канал обновлений», «Доступно обновлений»,
   «Инфраструктура», «Веб-консоль» (ссылка открывает консоль в браузере), «Операторы кластера» — «Все N операторов в
   норме.» или список не в норме, «Показать все» — таблица всех; «Пулы машин» master и worker. У `developer` — те же
   разделы с «— Нет прав…». Под сведениями о подключении — «Токен действует до …»; если до конца срока меньше часа —
   предупреждение «Токен истекает меньше чем через час…».
6. Дерево. У подключения — «Кластер», «Проекты» (раскрываются по щелчку) и «Ноды» (если вкладка подключения уже
   открыта и права есть). У `dev-token` в «Проекты» — «Кластер не даёт список проектов — показаны проекты из профиля» и
   «★ team-a». У `crc` с десятками проектов — фильтр проектов, избранные (★) сверху.
7. Вкладка проекта (kind, `admin`, проект `team-a`). «Обзор»: Running, Pending и другие фазы; проблемные поды
   `crash-…` (CrashLoopBackOff), `pull-…` (ErrImagePull или ImagePullBackOff), `pending-…` (Pending); «Workloads не в
   норме» — `Deployment/crash`, `Deployment/pull`, `Deployment/pending`; «Квоты» — «Каждая квота занята меньше чем на
   80 %.»; «События Warning за последний час» — BackOff и Failed.
8. Workloads. Подвкладка «Workloads»: web 2/2, crash 0/1 (выделено), migrate (Job) 1/1, report (CronJob) — расписание
   `0 3 * * *`; фильтр по виду «CronJob» оставляет report. В терминале `kubectl -n team-a scale deploy/web
   --replicas=3` — строка web меняется сама, без «Обновить».
9. Поды. «Поды»: «Только проблемные (N)» оставляет crash, pull и pending; фильтр по имени; щелчок открывает под.
   `kubectl -n team-a delete pod <под web>` — под исчезает, новый появляется сам.
10. Права в проекте. `dev-token`: проект `team-a` открывается, подвкладки те же. Пользователь без прав на events в
    проекте (на OpenShift — роль view без events или своя Role) — подвкладки «События» нет.
11. Workload. Щелчок по `web`: «Обзор» — реплики, селектор `app=web`, стратегия RollingUpdate, «История выкладки» с
    текущей ревизией, контейнер с переменными `LOG_LEVEL ← configMapKeyRef web-config/LOG_LEVEL` и `DB_PASSWORD ←
    secretKeyRef db/password` (значения не видны); «Поды» — два пода web; «События» — события Deployment, ReplicaSet и
    подов; YAML — без `managedFields`, «Копировать» кладёт текст в буфер.
12. Под. `crash-…`: статус CrashLoopBackOff, у контейнера «Состояние» — «ждёт: CrashLoopBackOff — …», «Прошлое
    состояние» — «завершён: Error, код 1, …», рестарты; «Владелец» `Deployment/crash` открывает workload.
13. Логи. Под `logger-…` → «Логи»: строки `line N` идут сами («идёт поток»), «Время строк» добавляет время без
    перезапуска; «Фильтр» `line 1` — показываются только подходящие, счётчик «показано N из M»; «regex» и `line \d*5$`.
    Под `crash-…` → «Предыдущий запуск»: «starting» и «boom», «Следить за новыми строками» недоступна. «Сохранить в
    файл…» → файл с теми же строками целиком, сообщение «Сохранено строк: N — путь».
14. Обрыв логов. В режиме слежения выключить сеть (или `docker pause glafira-control-plane`) на 10–20 с: статус
    «соединение потеряно — переподключение…», после восстановления строки продолжаются без повторов. Пауза дольше
    минуты — ошибка «Поток логов прервался и не восстановился за 60 с».
15. Сеть. «Сеть» в `team-a`: Service web — «Готовые endpoints» 2/2; Ingress web — `web.example.test/ → web:80`; Routes
    и Gateway на kind нет. На OpenShift: Routes с «Принят» ✅ и сроком сертификата; Ingress с «Routes из этого Ingress».
    Кластер с Gateway API (CRD standard channel): Gateway, HTTPRoute с условиями родителей.
16. Конфигурация, хранилище, квоты. ConfigMap web-config — ключ LOG_LEVEL, «Показать значения» — `debug`; Secret db —
    тип Opaque, ключ password, значения не показываются нигде. PVC data — Pending (WaitForFirstConsumer), «Кем
    смонтирован» — «—». «Квоты»: compute — pods и requests.cpu с процентами, LimitRange defaults.
17. Истечение сессии. Подключение с токеном: отозвать токен (`oc delete useroauthaccesstoken …` или подождать) —
    вкладки проекта показывают ошибку с «Войти снова»; кнопка открывает вкладку подключения; после «Сохранить токен» с
    новым токеном вкладки проекта перечитываются сами, логи и списки начинаются заново.
18. Exec-плагин (долги O1a). Подключение с exec-плагином (скрипт, который ждёт 30 с): открыть сразу проект — плагин
    запускается один раз (в `ps` один процесс), все подвкладки ждут его. «Отменить» на вкладке подключения отменяет
    ожидание у всех вкладок, а процессы, которые запустил плагин (`sleep` в скрипте), завершаются. Проверка подключения в
    форме и подключение вкладки того же профиля не отменяют друг друга: «Отмена» в форме не трогает вкладку.
19. Форма exec-плагина. Подключение с `AWS_SECRET_ACCESS_KEY=…` в переменных и `--oidc-client-secret=…` в аргументах:
    в полях значения скрыты (`••••`), поля только для чтения, «Показать» открывает настоящие значения для правки.
````

<!-- block: patch t19-src step=19.3 -->
````diff
diff --git a/README.md b/README.md
index 3f4fee5..f505473 100644
--- a/README.md
+++ b/README.md
@@ -75,6 +75,21 @@ GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-t
   ответ сервера на «кто я» (401, 403…) — это TLS ✅ и ❌ на шаге «Вход»; шаг «TLS» падает, в частности, от сетевой
   ошибки, ошибки TLS или таймаута (`src-tauri/src/k8s/connectivity.rs`).
 
+## Чтение кластера
+- Всё чтение — в Rust через `ReadClient` (`src-tauri/src/k8s/read.rs`): только get, list, watch, логи и проверки
+  прав; списки — страницами по 500, не больше 10 000 объектов. Каждая команда работает с правами пользователя; часть
+  экрана без прав приходит причиной (`Part` в `src-tauri/src/k8s/view.rs`), остальное показывается.
+- Обзор кластера (`k8s/overview.rs`): `/readyz?verbose`, ноды по ролям, классы Ingress и Gateway, число проектов; у
+  OpenShift — адаптер платформы (`platform/adapters.rs`): ClusterVersion, Infrastructure, консоль, ClusterOperators,
+  MachineConfigPools и срок токена (`useroauthaccesstokens`).
+- Проекты, права в проекте (`SelfSubjectRulesReview`, кэш на 5 минут, сброс при 403), workloads, поды, события, сеть,
+  конфигурация (значения Secret не читаются интерфейсом), PVC, квоты, YAML без `managedFields` — модули `src-tauri/src/k8s/`.
+- Списки подов, workloads и событий проекта обновляются через watch (`src-tauri/src/watch/`), без прав на watch — опрос
+  раз в 10 с; логи (`src-tauri/src/logs/`) — фильтр в Rust, обрезка строк длиннее 64 КБ, переподключение в follow.
+  Сессии останавливаются `stop_session`, при отключении и удалении подключения.
+- Одновременные чтения одного подключения подключаются один раз: кто ждал очереди, пока чужая попытка не удалась,
+  получает ту же ошибку (`AppState::connected`), — exec-плагин не запускается по разу на каждую подвкладку.
+
 ## Где хранятся данные
 - Профили и папки: `connections.json` в каталоге данных (macOS: `~/Library/Application Support/io.github.vvnovg.glafira.kubernetes/`).
 - Токены и ключи клиентских сертификатов — в системном хранилище секретов, сервис `glafira-kubernetes`, ключи
diff --git a/docs/guide/en/connections.md b/docs/guide/en/connections.md
index 159003f..a80b4da 100644
--- a/docs/guide/en/connections.md
+++ b/docs/guide/en/connections.md
@@ -98,6 +98,22 @@ Glafira works with your rights only. On connection it checks what you may read a
 operators). If the cluster does not let you list projects, enter them in
 [[ui:connections.form.manualNamespaces]].
 
+## Cluster overview {#overview}
+
+Under the connection details, the connection tab shows the [[ui:cluster.overview.title]]: whether the API server is
+ready (the `/readyz` checks, which any user may read, and the checks that failed), ready nodes by role, the number of
+projects, the Ingress classes and, with Gateway API, the Gateway classes. On OpenShift it adds the version, the update
+channel and the available updates, the infrastructure, the web console, the cluster operators that are not fine
+([[ui:cluster.overview.operatorsAll]] lists every one) and the machine config pools. A part you may not read shows
+a dash (—) with the reason; the rest is shown. On OpenShift the tab also shows until when the sign-in token is valid and warns
+an hour before it expires.
+
+## Nodes {#nodes}
+
+With the right to list nodes, the connection tab and the tree have [[ui:cluster.nodes.open]]: the list of nodes with
+their readiness, problems (`MemoryPressure`, `DiskPressure`…), roles and kubelet version. Click a node to see its
+conditions, taints, capacity and allocatable resources, addresses and labels.
+
 ## Environments and read-only {#environments}
 
 [[ui:connections.form.environment]] sets the color of the connection. A new `prod` connection is
diff --git a/docs/guide/ru/connections.md b/docs/guide/ru/connections.md
index 7f9b595..bc12cbe 100644
--- a/docs/guide/ru/connections.md
+++ b/docs/guide/ru/connections.md
@@ -96,6 +96,21 @@ Glafira работает только с вашими правами. При п
 (проекты, ноды, операторы). Если кластер не даёт вам список проектов, перечислите их в
 [[ui:connections.form.manualNamespaces]].
 
+## Обзор кластера {#overview}
+
+Под сведениями о подключении вкладка подключения показывает [[ui:cluster.overview.title]]: готов ли API-сервер
+(проверки `/readyz`, их может читать любой пользователь, и непройденные проверки), готовые ноды по ролям, число
+проектов, классы Ingress, а при Gateway API — классы Gateway. У OpenShift добавляются версия, канал и доступные
+обновления, инфраструктура, веб-консоль, операторы кластера не в норме ([[ui:cluster.overview.operatorsAll]] выводит
+все) и пулы машин. Часть, которую вам нельзя читать, показывается как «—» с причиной, остальное видно. У OpenShift
+вкладка показывает и срок токена входа, а за час до истечения предупреждает.
+
+## Ноды {#nodes}
+
+Если у вас есть право на список нод, на вкладке подключения и в дереве появляется пункт [[ui:cluster.nodes.open]]:
+список нод с готовностью, проблемами (`MemoryPressure`, `DiskPressure`…), ролями и версией kubelet. Щелчок по ноде
+показывает её условия, taints, ёмкость и доступные подам ресурсы, адреса и метки.
+
 ## Окружения и только чтение {#environments}
 
 [[ui:connections.form.environment]] задаёт цвет подключения. Новое подключение `prod` — [[ui:common.readOnly]]:
diff --git a/docs/i18n/glossary.md b/docs/i18n/glossary.md
index ee1848f..2fd742e 100644
--- a/docs/i18n/glossary.md
+++ b/docs/i18n/glossary.md
@@ -39,3 +39,14 @@ HTTPRoute, GRPCRoute, Service, ConfigMap, Secret, PVC, HPA, ResourceQuota, Limit
 | exec plugin | exec-плагин | команда kubeconfig, которая выдаёт учётные данные |
 | launch line | строка запуска | путь, аргументы и переменные exec-плагина |
 | allow (to run a command) | разрешить (запуск) | подтверждение строки запуска |
+| cluster overview | обзор кластера | |
+| condition | условие | `status.conditions` объекта; названия условий (`Ready`, `Available`) не переводятся |
+| ready / desired / available / updated | готово / желаемо / доступно / обновлено | реплики workload'а |
+| rollout history | история выкладки | ReplicaSets или ReplicationControllers |
+| event | событие | `Normal` и `Warning` не переводятся |
+| endpoint | endpoint | готовые endpoints сервиса |
+| taint | taint | не переводится |
+| probe | проба | `Liveness-проба`, `Readiness-проба`, `Startup-проба` |
+| capacity / allocatable | ёмкость / доступно подам | ресурсы ноды |
+| logs, follow | логи, следить за новыми строками | |
+| previous run | предыдущий запуск | логи прошлого запуска контейнера (`previous`) |
diff --git a/docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md b/docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md
index 3bc225f..0b3dc0d 100644
--- a/docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md
+++ b/docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md
@@ -161,13 +161,13 @@ crates/glafira-mcp-kubernetes — инструменты, типы ответо
 
 | Группа | Команды |
 |---|---|
-| Подключения | `list_connections`, `save_connection`, `delete_connection`, `test_connection`, `connect`, `disconnect`, `set_connection_token`, `start_oauth_login`, `cancel_oauth_login` (и ожидание exec-плагина), `resolve_exec_command`, `preview_kubeconfig_import`, `apply_kubeconfig_import` |
+| Подключения | `list_connections`, `save_connection`, `delete_connection`, `test_connection`, `connect`, `disconnect`, `set_connection_token`, `start_oauth_login`, `cancel_oauth_login` (и ожидание exec-плагина при подключении), `cancel_connection_test` (ожидание exec-плагина при проверке в форме: у проверки свой ключ входа `test:<id>`), `resolve_exec_command`, `preview_kubeconfig_import`, `apply_kubeconfig_import` |
 | Папки и обмен | `list_folders`, `create_folder`, `rename_folder`, `delete_folder`, `move_items`, `export_connections`, `preview_import`, `apply_import` |
-| Права и платформа | `get_capabilities(connectionId)` (права, группы API, платформа и дистрибутив, §4.3), `get_namespace_access(connectionId, namespace)` |
+| Права и платформа | права, группы API, платформа и дистрибутив (§4.3) и срок токена — в ответе `connect` (`ConnectionInfo`); `get_namespace_access(connectionId, namespace)` |
 | Кластер | `get_cluster_overview`, `list_nodes`, `get_node`, `list_cluster_operators` |
-| Проекты | `list_projects`, `get_project_overview`, `get_quotas` |
-| Объекты | `list_workloads`, `get_workload`, `get_rollout_history`, `list_pods`, `get_pod`, `list_events`, `list_services`, `list_routes`, `list_ingresses`, `list_gateways`, `list_gateway_routes`, `list_config_maps`, `list_secrets` (имена, типы, ключи), `list_pvcs`, `list_hpas`, `get_yaml(ref)` |
-| Watch и логи | `start_watch(params, channel) -> SessionId`, `start_logs(params, channel) -> SessionId`, `save_logs`, `stop_session` |
+| Проекты | `list_projects`, `get_quotas`; обзор проекта собирает интерфейс из списков подов, workloads, квот и событий |
+| Объекты | `list_workloads`, `get_workload` (с HPA, PDB и историей rollout), `list_pods(…, workload?)`, `get_pod`, `list_events(…, object?)`, `get_network` (Services, Routes, Ingress, Gateway, HTTPRoute, GRPCRoute — каждый вид своей частью), `get_config` (ConfigMaps; Secrets — имена, типы, ключи), `get_config_map_values`, `list_pvcs`, `get_yaml(kind, name)` |
+| Watch и логи | `start_watch(params, channel) -> SessionId`, `start_logs(params, channel) -> SessionId`, `save_logs(params, path, timestamps)`, `stop_session` |
 | Ресурсы | `get_metrics_source_status`, `get_workload_usage(ref, window)`, `get_project_usage(namespace, window, channel) -> SessionId`, `recommend_resources(ref, window)` |
 | Изменения | `preview_change(connectionId, change) -> ChangePreview`, `apply_change(connectionId, change, expectedResourceVersion, confirm?, proposalId?)` |
 | Проверка | `run_health_check`, `get_health`, `set_finding_ignored`, `set_annotation_status`, `delete_annotation`, `delete_all_annotations`, `set_proposal_status`, `delete_proposal`, `delete_all_proposals` |
@@ -356,10 +356,9 @@ oc. В тексте отказа — только имя ключа: значе
 `test_connection` идёт по шагам и показывает, на каком шаге ошибка, с подсказкой:
 1. DNS / TCP до API-сервера (с учётом прокси); у exec-плагина после него — запуск плагина (шаг `exec`);
 2. TLS: цепочка, имя хоста, срок сертификата («сертификат подписан неизвестным CA — укажите CA кластера»);
-3. аутентификация: `SelfSubjectReview` (`authentication.k8s.io/v1`), имя пользователя и группы (в O1a шаг «Вход»
-   показывает только имя, группы видны на вкладке подключения; группы на самом шаге — в O1b); любой ответ сервера на
-   этот запрос (401, 403…) — это TLS ✓ и ошибка на шаге «Вход», а шаг TLS падает только от сетевой ошибки, ошибки TLS
-   и таймаута;
+3. аутентификация: `SelfSubjectReview` (`authentication.k8s.io/v1`), имя пользователя и группы (первые 10, остальные
+   числом); любой ответ сервера на этот запрос (401, 403…) — это TLS ✓ и ошибка на шаге «Вход», а шаг TLS падает
+   только от сетевой ошибки, ошибки TLS и таймаута;
 4. discovery: платформа и дистрибутив (§4.3), версия (`ClusterVersion`, если читается, иначе `/version`), найдены ли
    группы OpenShift и Gateway API;
 5. проекты: число доступных;
@@ -469,15 +468,18 @@ oc. В тексте отказа — только имя ключа: значе
 - **Фильтр** (подстрока или regex, с учётом регистра или без) выполняется в Rust до отправки в интерфейс; счётчик
   «показано / просмотрено».
 - Буфер в интерфейсе — до 100 000 строк (кольцевой в режиме follow); строка длиннее 64 КБ обрезается в превью.
-- Сохранение в файл — `save_logs` (диалог выбора файла).
-- Потеря соединения в follow: переподключение с `sinceTime` последней строки; через 60 с без восстановления — статус
-  «ошибка».
+- Сохранение в файл — `save_logs` (диалог выбора файла): Rust читает лог заново с теми же параметрами без follow и
+  пишет подошедшие под фильтр строки целиком.
+- Потеря соединения в follow: переподключение с `sinceTime` последней строки (с начала её секунды — API округляет
+  `sinceTime`), уже показанные строки отбрасываются по времени; через 60 с без восстановления — статус «ошибка».
 
 ### 6.7 Обновление данных
 
 Открытые списки подов, workloads и событий проекта обновляются через `kube::runtime::watcher` (сессия `start_watch`).
-Если прав на `watch` нет — опрос раз в 10 с. Остальные экраны — ручное обновление и автообновление (выкл. / 10 с /
-30 с). Списки читаются страницами (`limit` 500 / `continue`) и виртуализированы.
+Если прав на `watch` нет — опрос раз в 10 с. Поды CronJob (их Job меняются) читаются списком раз в 10 с. Остальные
+экраны — ручное обновление и автообновление (выкл. / 10 с / 30 с). Списки читаются страницами (`limit` 500 /
+`continue`, не больше 10 000 объектов) и виртуализированы. После входа заново открытые вкладки подключения
+перечитываются, а сессии watch и логов начинаются заново.
 
 ---
 
@@ -1008,7 +1010,7 @@ keychain: сервис glafira-kubernetes, ключи <connection-id>/<field>
 
 | Этап | Содержимое |
 |---|---|
-| **O1. Каркас и чтение** | Два плана. **O1a «Каркас и подключения»:** новый репозиторий, перенос основы (§3) и `PORTED.md`; хранилище, keychain, настройки, политика (включая `allowInsecureTls`), журнал, словари en/ru, подсказки, справка (главы «Начало работы» и «Подключения»), папки, экспорт и импорт; подключения (токен, сертификат, вход через браузер, exec-плагины из списка разрешённых и политика `allowExecAuth`, импорт kubeconfig, адрес API с путём), проверка подключения, права; слой платформы (`openshift`, `kubernetes`) и дистрибутив по discovery (§4.3; трейт `Platform` с адаптерами — в O1b вместе с обзором кластера); дерево, вкладка подключения (кто вошёл, платформа и дистрибутив, права), «О программе». **O1b «Чтение кластера»:** обзор кластера (включая `/readyz`, классы Ingress и Gateway), проекты, workloads, поды, события, сеть (Services, Routes, Ingress, Gateway, HTTPRoute, GRPCRoute), конфигурация, хранилище, квоты, YAML, логи, watch, срок токена, группы пользователя на шаге «Вход» проверки подключения (§5.3) |
+| **O1. Каркас и чтение** | Два плана. **O1a «Каркас и подключения»:** новый репозиторий, перенос основы (§3) и `PORTED.md`; хранилище, keychain, настройки, политика (включая `allowInsecureTls`), журнал, словари en/ru, подсказки, справка (главы «Начало работы» и «Подключения»), папки, экспорт и импорт; подключения (токен, сертификат, вход через браузер, exec-плагины из списка разрешённых и политика `allowExecAuth`, импорт kubeconfig, адрес API с путём), проверка подключения, права; слой платформы (`openshift`, `kubernetes`) и дистрибутив по discovery (§4.3; трейт `Platform` с адаптерами — в O1b вместе с обзором кластера); дерево, вкладка подключения (кто вошёл, платформа и дистрибутив, права), «О программе». **O1b «Чтение кластера»:** обзор кластера (включая `/readyz`, классы Ingress и Gateway), проекты, workloads, поды, события, сеть (Services, Routes, Ingress, Gateway, HTTPRoute, GRPCRoute), конфигурация, хранилище, квоты, YAML, логи, watch, срок токена, группы пользователя на шаге «Вход» проверки подключения (§5.3), трейт `Platform`, `ReadClient`; долги O1a: общий итог подключения для ожидающих очереди, свой ключ входа у проверки подключения, завершение группы процессов exec-плагина, значения секретов exec-плагина в полях формы — до «Показать» |
 | **O2. MCP-шлюз и проверка** | `glafira-mcp-core` (перенос), `glafira-mcp-kubernetes`; уровень `metadata`, `llmNamespaces`, маскировка `command`/`args`/аннотаций; проверка (`health/`, 34 правила без `resources.*`, в том числе `cluster.apiNotReady`, `ingress.*`, `gatewayRoute.*`, `gateway.*`), вкладка «Замечания», значки и блоки; 17 инструментов (все, кроме `get_resource_usage`, `recommend_resources`, предложений и логов) |
 | **O3. Ресурсы, изменения, предложения** | Источники метрик (включая автопоиск Prometheus через прокси API-сервера), PromQL-шаблоны, движок рекомендаций, вкладки «Ресурсы» workload'а и проекта, графики; правила `resources.*`; изменяющие операции §9 с dry-run и guard; `get_resource_usage`, `recommend_resources`, `propose_change`, `list_proposals`, `withdraw_proposal` с заполнением форм (22 инструмента) |
 | **O4. Логи для LLM** | Уровень `logs`, маскировка логов и значений, политика `mcpLogsEnabled`, `get_pod_logs`, `search_workload_logs` (24 инструмента) |
diff --git a/scripts/i18n-check.mjs b/scripts/i18n-check.mjs
index 7f640f3..290c623 100644
--- a/scripts/i18n-check.mjs
+++ b/scripts/i18n-check.mjs
@@ -561,7 +561,7 @@ export function findRustCyrillic(files) {
 }
 
 /** Главы руководства: тот же список, что `GUIDE_CHAPTERS` в `src/guide/chapters.ts`. */
-export const GUIDE_CHAPTERS = ["getting-started", "connections"];
+export const GUIDE_CHAPTERS = ["getting-started", "connections", "projects-workloads", "pods-logs"];
 
 const KEBAB = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
 
diff --git a/src/components/help/helpChapterFor.ts b/src/components/help/helpChapterFor.ts
index 539e05a..8d2f222 100644
--- a/src/components/help/helpChapterFor.ts
+++ b/src/components/help/helpChapterFor.ts
@@ -8,6 +8,11 @@ export function helpChapterFor(tab: TabSpec | null): ChapterId {
     case "cluster":
     case "nodes":
       return "connections";
+    case "project":
+    case "workload":
+      return "projects-workloads";
+    case "pod":
+      return "pods-logs";
     default:
       return "getting-started";
   }
diff --git a/src/guide/chapters.ts b/src/guide/chapters.ts
index ddd416f..2116994 100644
--- a/src/guide/chapters.ts
+++ b/src/guide/chapters.ts
@@ -1,5 +1,5 @@
 /** Главы руководства (спец. §16); каждый этап добавляет свои. Тот же список — в `scripts/i18n-check.mjs`. */
-export const GUIDE_CHAPTERS = ["getting-started", "connections"] as const;
+export const GUIDE_CHAPTERS = ["getting-started", "connections", "projects-workloads", "pods-logs"] as const;
 
 export type ChapterId = (typeof GUIDE_CHAPTERS)[number];
 
````

- [ ] **Step 4: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 48 passed`, `Tests 259 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 136`

Run: `pnpm test:scripts`  
Expected: PASS — `# pass 54`, `# fail 0`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 5: Commit.**

```bash
git add \
  README.md \
  docs/guide/en/connections.md \
  docs/guide/en/pods-logs.md \
  docs/guide/en/projects-workloads.md \
  docs/guide/ru/connections.md \
  docs/guide/ru/pods-logs.md \
  docs/guide/ru/projects-workloads.md \
  docs/i18n/glossary.md \
  docs/smoke/o1b.md \
  docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md \
  scripts/i18n-check.mjs \
  src/components/help/helpChapterFor.test.ts \
  src/components/help/helpChapterFor.ts \
  src/guide/chapters.ts
git commit -s -m "docs: главы «Проекты и workloads» и «Поды и логи», обзор кластера и ноды в справке, смоук O1b, README, глоссарий, уточнения спеки" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
