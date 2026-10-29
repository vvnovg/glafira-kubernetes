# O3 «Ресурсы, изменения, предложения» — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Glafira for Kubernetes показывает фактическое потребление CPU и памяти workload'ов, подов и проектов по истории метрик (Thanos Querier у OpenShift, Prometheus через прокси API-сервера или свой адрес, без них — `metrics.k8s.io`), рекомендует `requests` и `limits` с учётом `LimitRange`, квот, нод и HPA, проверяет кластер ещё тремя правилами `resources.*` и меняет кластер формами §9 — ресурсы контейнеров, реплики, перезапуск, границы HPA, удаление пода — с «было → станет», server-side dry-run и стражами профиля; LLM-ассистент получает пять новых инструментов (22 всего): смотрит ресурсы и рекомендации и предлагает изменения, которые человек применяет формой; граница записи MCP закрыта сторожком, разбирающим пути.

**Architecture:** Rust: `metrics/` (трейт `MetricsSource` только с GET к Prometheus API, автопоиск и выбор источника, PromQL-шаблоны), `resources/` (статистика худшего пода, движок рекомендаций — чистая функция, ресурсы проекта — фоновая сессия), `change/` (модель `Change`, факты и предупреждения — только чтение, им пользуется и MCP), `ops/` (единственное место с запросами записи: `ClusterWriter`, предпросмотр с dry-run, применение после `guard`), `health/` (правила `resources.*`, предложения LLM в файле проверки), `mcp/` (инструменты ресурсов и предложений поверх крейта `glafira-mcp-kubernetes`). Фронтенд: графики uPlot отдельным чанком, вкладки «Ресурсы» workload'а, пода и проекта, формы изменений поверх экрана, предложения во вкладке «Замечания» и блоках объектов. Стенд kind получает объекты для изменений и поддельный Prometheus за прокси API-сервера.

**Tech Stack:** Rust 2024 (tauri 2, kube 4.2, k8s-openapi 0.28 `v1_32`, reqwest 0.13 — HTTP-клиент своего источника метрик, url, time; крейты MCP — rmcp 3.5, schemars 1), TypeScript (React 19, TanStack Query 5, zustand 5, i18next, uPlot 1.6.32), Vitest + Testing Library, pnpm 11, kind v0.33.0 (Kubernetes 1.37) для интеграционных тестов.

**Спецификация:** `docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md` — строка O3 в §19 и разделы §4.1, §5.1, §5.3, §5.4, §7 (источник метрик, сбор статистики, движок рекомендаций, интерфейс), §8.2 (`resources.*`), §9 (изменяющие операции), §10.4–10.6 (инструменты, предложения LLM, инструкции), §13, §15, §17. Опирается на O2 (`docs/superpowers/plans/2026-10-26-glafira-kubernetes-o2.md`, ветка `main` @ `84f8f2f`): `ReadClient`, `AppState::{connected, read, read_in}`, `health/`, крейты MCP, стенд `dev/kind`. Перенос из Glafira for Apache Kafka — с коммита `8bbd0bd` скриптом `scripts/port.sh` (`~/kafka-explorer` или `$KAFKA_REPO`), строки о перенесённых файлах — в `PORTED.md`.

Решения пользователя по вопросам, на которые спека не отвечала:
- форма изменения — диалог поверх экрана (задачи 15, 18);
- метрики на стенде kind — поддельный Prometheus, а не kube-prometheus (задача 13);
- фильтр «только с рекомендациями» у ресурсов проекта — workloads с риском или избытком requests (задачи 7, 17);
- форма «Ресурсы контейнеров» открывается и без рекомендации — кнопкой на обзоре workload'а (задача 15);
- источник метрик подключения — локальная настройка: экспорт её не пишет (задача 2).

Уточнения спеки по итогам прототипа (задача 19 вносит их в спеку):
- версия предпросмотра — `metadata.generation` (у пода — `uid`), а не `resourceVersion`: идущая выкладка меняет `resourceVersion` статусом, и прототип ловил ложный `CONFLICT`; применение перечитывает объект, значения пишутся абсолютными, как у kubectl, `resourceVersion` требует только JSON patch DeploymentConfig, ответ 409 из-за статуса — повтор до 3 раз (и у dry-run);
- у DELETE dry-run и предусловие `uid` — в теле `DeleteOptions`: API-сервер у DELETE с телом не читает строку запроса, и `?dryRun=All` удалял под по-настоящему (нашёл стенд kind);
- `setResources` недоступен для Job (шаблон Job неизменяем); шаг «Метрики» проверки подключения — предупреждение, итог проверки без источника — успех;
- настройка метрик — локальное поле профиля, `http://` — только без авторизации, токен — `<id>/metricsToken`; выбор источника живёт в подключении 5 минут, 403 проб не сбрасывают кэш прав проекта;
- окно `get_resource_usage` и `recommend_resources` — 1, 7, 14 или 30 дней (спека говорила «1–30»);
- сторожок записи MCP — разбор `use`-деревьев и путей; тексты ошибок для модели — по списку ключей `LLM_TEXT_KEYS`;
- стенд kind: прокси API-сервера ведёт только на адреса подов, поэтому поддельный Prometheus — HTTP-сервер теста на рабочей машине за под-заглушкой с hostNetwork и DNAT в ноде.

**Отложенное O2** (раздел «Отступления при выполнении» плана O2, «Отложено»). Закрываются здесь — задача 1:
- сторожок `mcp_never_reaches_writes_or_the_raw_client` искал по подстрокам, и `use kube::{Api, Client}` проскакивал — теперь разбор путей; этап O3 впервые добавляет запись в кластер, так что сторожок нужен до неё;
- `llm_message` пропускал ошибки с неизвестными ключами — теперь список разрешённых ключей и предел длины 500 символов;
- `target_exists` читал через `state.read` и не проверял проект — теперь `read_in` и список проектов пользователя.

Остаются отложенными (к ресурсам и изменениям не относятся): маскировка строк проб и пар `имя=значение` в свободном тексте, частные случаи `Authorization: Digest`, «Bearer …» в прозе и булевых флагов; повтор свежей проверки при `llmNamespaces` больше 50 и после смены `apiUrl`; миграция старых файлов `health/<id>.json`; усечённый список подов у `list_nodes`; последний ответ шима при закрытии stdin; `Scope::default()` как «все проекты»; `null` у `get_network` для видов без группы API; строка PORTED.md о `McpSection.test.tsx`; покрытие стенда kind для `limit_to_scope` и `masked`. Отложенное O1b из шапки плана O2 — тоже без изменений.

Отложено в O3: окно анализа по умолчанию в `settings.json` (§13) — вкладки начинают с 7 дней; F1 на подвкладке «Ресурсы» открывает главу её вкладки, а не «Ресурсы и изменения».

**Проверено до написания плана.** Весь код плана собран и прогнан в отдельной копии репозитория (по коммиту на задачу, база `84f8f2f`); повторное применение всех блоков плана в чистом клоне `84f8f2f` даёт то же дерево после каждой задачи побайтно (кроме генерируемых `src-tauri/Cargo.lock`, `crates/*/Cargo.lock` и `pnpm-lock.yaml`), проверки после каждой задачи — зелёные:
- Rust — unit-тесты приложения: 674 (`cargo test --lib`; было 578 после O2), clippy с `-D warnings`, `cargo fmt --check`; крейт `glafira-mcp-core` — без изменений (16 модульных, `gui_stdio` 1, `manifest` 1, `server` 12); крейт `glafira-mcp-kubernetes` — 24 модульных, `manifest` 1, `server` 35 (было 22 модульных, `manifest` 1, `server` 32);
- интеграционные на настоящем kind v0.33.0 (Kubernetes 1.37): `it_connect` — 4, `it_read` — 6, `it_health` — 2, `it_ops` — 1, `it_metrics` — 1, `it_mcp` — 1 (без `GO_IT_KUBECONFIG` пропускаются; `it_health` ждёт находки стенда до 600 с);
- фронтенд — 517 тестов в 74 файлах (было 475 в 63), `tsc --noEmit`, `pnpm build` с проверкой совместимости бандла с WebKit macOS 11 (uPlot — отдельный чанк), `pnpm i18n:check` (словари en и ru, 198 вызовов из Rust, 8 глав руководства на двух языках);
- скрипты — 86 тестов (`pnpm test:scripts`); шаги «Увидеть падение» проверены — тесты фронтенда падают до реализации.
Не проверено: окно приложения (`pnpm tauri dev`) и смоук `docs/smoke/o3.md` — графики в светлой и тёмной теме, формы на настоящем кластере OpenShift; Thanos Querier и маршрут `thanos-querier`, свой источник по HTTPS с токеном и CA, настоящий Prometheus и kube-state-metrics (на стенде — записанные ответы и подзапросы без вычисления); DeploymentConfig — JSON patch ресурсов и `instantiate` (только unit-тесты); настоящие MCP-клиенты (шлюз проверен клиентом rmcp).

## Отступления при выполнении

План выполняли субагенты, задача за задачей, в ветке `worktree-o3` (база `468e08a`): после каждой задачи — ревью (у задач с записью в кластер, MCP и источниками метрик — на Opus), найденные Critical и Important исправлялись fix-коммитом поверх задачи с повторным ревью, Minor копились в журнале; в конце — итоговое ревью всей ветки, одна волна исправлений и повторное ревью. Блоки плана применялись как есть; где ранний fix-коммит менял код, на который опирались блоки поздних задач, исполнитель вносил согласованные правки после применения блоков (они перечислены в таблице). Каждый fix-коммит проверялся тем, что все блоки следующих задач по-прежнему применяются (`git apply`), поэтому строки и блоки плана ниже — это места, где текст плана теперь расходится с кодом ветки.

**Решение пользователя по ходу выполнения (2026-10-29).** Ревью задачи 9 показало, что dry-run изменения шаблона Deployment, StatefulSet или DeploymentConfig не проверяет ни `LimitRange` (LimitRanger смотрит только поды и PVC), ни квоту CPU и памяти (она считается по подам): поды новой ревизии не создались бы, выкладка встала бы. Решение — «LimitRange — ошибка»: нарушение границы `LimitRange` — ошибка `errors.change.limitRangeMin`/`limitRangeMax` предпросмотра, применения и `propose_change` (`invalid_argument`); нехватка квоты остаётся предупреждением с текстом «поды новой ревизии не создадутся, выкладка встанет». Спека (§9, §10.5), справка, README и смоук приведены к этому.

| Задача | Что изменилось | Коммиты | Устаревшие места плана |
|---|---|---|---|
| 3, 5 | Ряд графика — ровно 200 точек: диапазон начинается на шаг позже начала окна (`start = now − окно + шаг`), тест считает точки, а не интервалы. Контейнер только с `limits`: request пода — подстановка API-сервера (= limit), без пометки «по умолчанию LimitRange», действующий request — limit шаблона. 403 списка подов в режиме metrics.k8s.io не обнуляет потребление при селекторе. `surge_pods` без переполнения | `9d4d5b2` (сообщение без лишней строки соавтора), `89837e2` | `t03-…-promql-rs-*` (2022–2658), `t05-…-current-rs-*` (5481–5714), `t05-…-usage-rs` (5725–6469) |
| 4 | Своя Thanos (tenancy) пробуется с `namespace` проекта пользователя (избранные → ручной список → первый проект кластера); шаг «Метрики» без английских `details` (причины — переведёнными строками); отказ своего источника — свой текст `messages.metrics.ownForbidden`; клиент метрик не следует редиректам | `90e320d` | `t04-…-metrics-{http,prometheus,select}-rs-*` (2847–4608), `t04-src` (4610–5336) |
| 6 | Запас квоты при уменьшении requests учитывает surge-под: `max(0, new − old) × replicas + new × surge`; эквивалентный target HPA не меньше 1 | `06a9500` | `t06-…-recommend-rs-*` (7112–8182) |
| 7 | Кэш ресурсов проекта сбрасывается вместе с кэшем прав (отключение, токен, правка, удаление) и защищён эпохой; результат с повторяемой ошибкой не кэшируется; сбой списка Jobs — в `errors`; резерв без неизвестного потребления | `95d286a`, `3db59df` | `t07-…-project-rs-*` (8335–9114), `t07-src` (9116–9374) |
| 8 | Ошибка выбора источника и 404 источника — пробел проверки (`check.partial`), а не обрыв или тихий пропуск; правило берёт помощники вкладки «Ресурсы»: под CronJob относится к CronJob, `Unknown`-поды считаются; чтение метрик — во втором `join` | `2ba97f2` | `t08-…-rules-resources-rs-*` (9417–9792), `t08-src` (9794–10325) |
| 9 | Количества сравниваются с допуском (`700m` = `0.7`); непрочитанные (не 404) HPA, LimitRange, квоты — предупреждение `Unverified`; формула квоты при уменьшении — как в задаче 6; решение пользователя по LimitRange; все `errors.change.*` — в `LLM_TEXT_KEYS` | `a905da4`, `925813d` | `t09-…-facts-rs-*` (10541–10769, 10992–11507), `t09-…-mod-rs-*` (10771–10988, 11509–11896) |
| 11 | Применение только с версией предпросмотра (без неё — CONFLICT); версия — `g<generation>`, без generation — `s<хэш spec>` (HPA до Kubernetes 1.37, CronJob до 1.22), у пода `u<uid>`; `send` не шлёт dry-run операции без его поддержки (`instantiate`); `DryRun::Rejected` — только FORBIDDEN, ADMISSION_DENIED, INVALID_INPUT, CONFLICT, прочее — ошибка; вид объекта в журнале | `eceffae`, `bbf833d` | `t11-…-ops-mod-rs` (13359–14260), `t11-…-commands-changes-rs-*` (13166–13357) |
| 12 | Согласованные правки после блоков: ветка `Unverified` в `warning()`, текст квоты, тест backend (50m → `invalid_argument` по LimitRange, 150m создаётся), описание `propose_change`, doc-комментарии | в `11bc6ea` | `t12-src` (16304–16894), описание инструмента в `t12-…` |
| 13 | `it_ops` передаёт версию предпросмотра и проверяет её у каждого вида; сброс стенда своим field manager и без `restartedAt`, строгая проверка владения `f:resources`; поддельный Prometheus отвечает только GET на два пути; `setup.sh` — сходящееся правило DNAT | в `055687b`, `56720c6` | `t13-…-it-ops-rs` (17249–17476), `t13-…-prometheus-rs` (16956–17085), `t13-src` (17703–17782) |
| 14, 15 | `ChangeWarning` с `unverified`; комментарий версии; ветка `unverified` в `warningText`; текст квоты в интерфейсе | в `07d5165`, `af10b4b` | `t14-…` (`src/api/types.ts`), `t15-…-changes-ts` (19580–19693), `t15-locales` |
| 16, 17 | Переведённые причины отказа источников (`SourceReasons`, `ResourcesError`), «Обновить» и «Повторить» выбирают источник заново (`refresh` у трёх команд ресурсов и у источника проекта), режим metrics.k8s.io — «только текущие значения», окно отключено; пустое состояние пода; потребление workload'а без работающих подов неизвестно (`None`, «—»); одноразовый `refresh`; различимые пустые состояния; неизвестный резерв в конце сортировки | `8225da5`, в `2737aeb`, `028b242` | `t16-…-workloadresources-tsx`, `t17-…-projectusageview-tsx` (20956–21112), `t17-…-projectrows-ts` (21114–21157), проверка `report.usage.cpu == Some(0.0)` в `t07-…-project-rs-tests` |
| Итоговое ревью ветки | Первый заход — «With fixes» (Critical нет), второй — «Ready to merge: Yes». Исправлено: наибольшая нода кэшируется в подключении (5 минут); без истории метрик нет «избытка»; ошибки проверки предложения идут модели через `llm_message`; числа формы проверяются до «Проверить»; после CONFLICT и без версии «Применить» скрыт; Esc закрывает один раз; фокус возвращается в диалог; граница LimitRange в тексте ошибки округляется в допустимую сторону; удалён мёртвый `Warning::LimitRange`; сторожок: два прохода по псевдонимам, экранированный символьный литерал, запрет `include!` и `#[path]`; строгие поля ресурсов в крейте; ключ находки ограничен и очищен; токен источника читается только при выборе; документы приведены к коду | `4153467`, `18497e9`, `e24d1fe`, `efe6614`, `db45b65`, `0df6928` | — |
| Блоки плана | Код блоков плана — прототип до выполнения и повторно как есть не применяется: в нём ещё есть `Warning::LimitRange` (с ветками `warning()`, `warningText`, `fieldAmount` и ключами `change.warning.limitRange`, `change.bound.*`), старая подсказка «ask for a shorter window», сигнатуры `summarize`, `analyze` и `read_constraints` без истории и кэша нод, `invalid()` без `llm_message`, сторожок без двух проходов по псевдонимам | — | `t09-…-facts-rs-*`, `t12-…-resources-rs`, `t12-…-resource-tools-rs` (15641–15779), `t12-…-mcp-proposals-rs` (15781–15965), `t15-…-changes-ts`, `t06-…-constraints-rs-*`, `t07-…-project-rs-*`, `t01-…-guard-rs` (187–658) |

**Числа сейчас** (ветка `worktree-o3`, `0df6928`):
- Rust: `cargo test --lib` приложения — 732 теста (в плане 674); крейт `glafira-mcp-core` — 16 модульных, `gui_stdio` 1, `manifest` 1, `server` 12 (как в плане); крейт `glafira-mcp-kubernetes` — 25 модульных, `manifest` 1, `server` 35 (в плане 24/1/35);
- интеграционные на kind v0.33.0 (кластер с отдельным `KUBECONFIG`): `it_connect` 4, `it_read` 6, `it_health` 2, `it_ops` 1 (повторяем), `it_metrics` 1, `it_mcp` 1;
- фронтенд: `pnpm test` — 537 тестов в 74 файлах (в плане 517), stderr чистый; `tsc --noEmit`, `pnpm build` (uPlot — отдельный чанк `UsageChart-*.js`, проверка WebKit macOS 11 проходит);
- `pnpm i18n:check`: вызовов из Rust — 201 (в плане 198); `pnpm test:scripts` — 86;
- окно приложения и смоук `docs/smoke/o3.md` не пройдены; правки смоука после fix-коммитов написаны по коду.

**Отложено** (итоговое ревью отнесло к не блокирующим):
- запись: RBAC 403 на dry-run показывается как «API-сервер отклонил изменение» и не сбрасывает кэш прав до истечения TTL; `ADMISSION_DENIED` — только для 403 (ValidatingAdmissionPolicy по умолчанию отвечает 422 → INVALID_INPUT); пути OpenShift (JSON patch DeploymentConfig с `resourceVersion`, `scale` DC, POST `instantiate` с `fieldManager`) не проверены на кластере; LimitRange проверяется только у контейнеров из `setResources` (`rolloutRestart` и неизменённые контейнеры — нет, по замыслу);
- форма: «Открыть в форме» не проверяет права проекта, как другие кнопки (решают Rust и API-сервер); списки подов и workload'ов не перечитываются сразу после применения; «сейчас X» не перечитывается после «Обновить и повторить»; нулевой diff всё равно предлагает «Применить»;
- метрики: дедлайн 45 с проекта покрывает и выбор источника; хэш версии без generation уже generation-семантики (поля HPA новее k8s-openapi `v1_32`, поля CronJob вне шаблона, расписания и `suspend`); проба tenancy берёт один проект (нет `view` в первом избранном → «отказ своего источника» при годном токене); при падающем источнике проверка повторяет 3 запроса на каждый из 50 проектов; ряды графиков — мгновенное значение раз в шаг; рестарты и OOM через `sum_over_time(increase)` приблизительны;
- сторожок MCP: `self::super::…` не разбирается; при урезанном списке проектов `target_exists` принимает любое имя проекта;
- предложения: закрытые ограничены только сроком 7 дней; `MarkApplied::Skipped(&str)` — строка, а не перечисление;
- интерфейс: ось Y памяти с десятичными шагами и двоичными подписями; маркеры без заливки в тёмной теме; цвета графика не меняются при смене темы; `ChunkBoundary` не сбрасывается; итог «requests CPU +20m» не говорит «разница», «1 реплик» без форм множественного числа;
- тестовые пробелы: связка `analyze`/`recommend_resources`, истечение 5-минутных кэшей, дедлайн и отмена сессии проекта, вкладки «Ресурсы» в панелях, подсчёт предложений в значках.

## Блоки кода

Код переносится как есть. Перед каждым блоком стоит HTML-комментарий `<!-- block: <вид> <id> step=<задача>.<шаг> [path=…] [part=…] [mode=…] -->`: `shell` — команды bash из корня репозитория (в том числе `scripts/port.sh` — перенос файла из Kafka-версии); `file` — новый файл по `path` (у Rust-файлов с тестами две части: `part=tests` — модуль тестов, `part=impl` — реализация, итоговый файл = реализация, пустая строка, модуль тестов); `patch` — патч `git diff` (применять `git apply --recount`; патч перенесённого файла — против файла Kafka-версии); `locales` — фрагмент словарей для `scripts/merge-locales.mjs`. Ограждения — четыре и больше обратных кавычек: внутри встречаются тройные. Блоки не перепечатывайте: их применяет скрипт ниже, по шагам, в порядке появления в плане. Патчи рассчитаны на дерево `main` @ `84f8f2f` с применёнными предыдущими задачами плана; если `main` с тех пор менялся, патч, который не применился, переносится вручную по смыслу.

Скрипт извлекается из этого плана один раз, до задачи 1 (план лежит в `docs/superpowers/plans/` репозитория):

```bash
awk '/^<!-- block: helper o3-blocks.py -->$/{f=1;next} f&&/^````/{n++; if(n==2) exit; next} f' \
  docs/superpowers/plans/2026-10-28-glafira-kubernetes-o3.md > /tmp/o3-blocks.py
python3 /tmp/o3-blocks.py list | head
```

<!-- block: helper o3-blocks.py -->
````python
#!/usr/bin/env python3
"""Блоки плана O3: извлечение и применение по шагам.

Использование (из корня репозитория):
  python3 /tmp/o3-blocks.py list                 — все блоки: id, вид, шаг, путь
  python3 /tmp/o3-blocks.py get <id>             — содержимое блока в stdout
  python3 /tmp/o3-blocks.py step <задача>.<шаг>  — применить блоки шага по порядку
План — $PLAN или docs/superpowers/plans/2026-10-28-glafira-kubernetes-o3.md.

Виды блоков: shell — команды bash из корня репозитория; file — файл по path (part=tests пишет модуль тестов,
part=impl ставит реализацию перед ним через пустую строку; mode — права); patch — `git apply --recount`;
locales — строки словарей для `scripts/merge-locales.mjs`.
"""
import os
import re
import subprocess
import sys

PLAN = os.environ.get("PLAN", "docs/superpowers/plans/2026-10-28-glafira-kubernetes-o3.md")
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

- Запросы записи в кластер — только в `src-tauri/src/ops/` (`ClusterWriter`, `KubeWriter`), с `fieldManager=glafira-kubernetes`, после стражей `guard` (профиль «только чтение» — `READ_ONLY`, никакое подтверждение не помогает; prod — имя подключения в `confirm`; под без контроллера — его имя в `confirm_pod`) и server-side dry-run в предпросмотре. Через MCP кластер не меняется: сторожок `mcp_never_reaches_writes_or_the_raw_client` (`src-tauri/src/mcp/guard.rs`) не пускает в `src-tauri/src/mcp/` `crate::ops`, `crate::commands::changes`, `ClusterWriter`, `KubeWriter`, `kube_client`, `Api`/`Client` из `kube`, `Patch`, `PostParams`, `PatchParams`, `DeleteParams`, `apply_change`, `preview_change`, `reqwest`, `hyper`, `http`; предложения LLM проверяются только чтением и пишутся только в файл Glafira `health/<id>.json`; применяет их человек формой.
- Секреты — только в keychain (сервис `glafira-kubernetes`; свой токен источника метрик — `<id>/metricsToken`); значения Secret кластера не покидают Rust: ни в интерфейс, ни модели, ни в снимок проверки. Модель не получает адрес своего источника метрик из профиля, токены и параметры входа; тексты ошибок — только из `LLM_TEXT_KEYS`.
- Источник метрик делает только GET к `/api/v1/query` и `/api/v1/query_range` (Prometheus HTTP API); через прокси API-сервера — только эти пути. Свой источник другим не заменяется. Настройка метрик — локальное поле профиля: экспорт её не пишет.
- Пределы: окно 1, 7, 14 или 30 дней, шаг подзапросов 5 минут, ряд графика — не больше 200 точек; ресурсы проекта — фоновая сессия с дедлайном 45 с и кэшем 5 минут; выбор источника — 5 минут в подключении; правила `resources.*` — окно 1 день; предложений — не больше 50 открытых на подключение, срок 7 дней; ответ MCP — до 100 КБ (ряды опускаются первыми); реплик — 0–1000.
- Зависимости: новых крейтов нет (reqwest, url, time — уже в приложении); фронтенд — `uplot` с точной версией `1.6.32` в `package.json`. В дереве зависимостей приложения нет `aws-lc-rs` и OpenSSL.
- Тексты интерфейса и Rust — ключами в `src/locales/en.json` и `ru.json` (en — эталон ключей, ru — эталон смысла), строки добавляются фрагментами `node scripts/merge-locales.mjs`; в строковых литералах Rust вне тестов кириллицы нет; совпадения ru с en (термины Kubernetes: `CPU request`, `OOM`…) — в `docs/i18n/same-as-en.json`; описания инструментов MCP, полей схем, инструкции сервера и тексты для модели — по-английски.
- Большие future в debug-сборке держатся в куче (`Box::pin`): чтение фактов, сбор статистики, выбор источника и цепочки команд иначе переполняют стек потока tokio (2 МБ).
- Проверки после каждой задачи (что применимо): `cargo test --manifest-path src-tauri/Cargo.toml --lib`, `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`, `cargo fmt --manifest-path src-tauri/Cargo.toml --check`; для крейта `crates/<крейт>` — `cargo test`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check` с `--manifest-path crates/<крейт>/Cargo.toml`; `pnpm test`, `pnpm exec tsc --noEmit`, `pnpm i18n:check`, `pnpm test:scripts`, `pnpm build`.
- Тесты Rust не ходят в сеть, кроме локальных сокетов: API-сервер — мок `k8s::testing::mock_client`, источник метрик — `metrics::prometheus::fake::FakeSource`, запись — `ops::tests::FakeWriter`. Интеграционные тесты (`src-tauri/tests/`, стенд `dev/kind`) без `GO_IT_KUBECONFIG` пропускаются; кластер kind — только с отдельным `KUBECONFIG`, `~/.kube/config` не меняется; поддельный Prometheus слушает порт 19090 рабочей машины. Тесты фронтенда мокают `api` (`vi.mock("../../api/client", …)`) и `uplot` (в jsdom нет canvas и `matchMedia`).
- `src-tauri/Cargo.lock`, `crates/*/Cargo.lock` и `pnpm-lock.yaml` обновляют cargo и pnpm; они коммитятся.
- Порт 1420 часто занят Vite Kafka-версии: `pnpm tauri dev --config '{"build":{"devUrl":"http://localhost:1430","beforeDevCommand":"pnpm dev --port 1430"}}'`.
- Комментарии кода и документы — на русском, в стиле O2; **сообщения коммитов — на английском** (Conventional Commits: `feat(metrics): …`, `feat(ops): …`, `feat(ui): …`, `test(kind): …`, `docs: …`). Коммит — `git add` по путям задачи (не `git add -A` и не `-a`), `git commit -s`, последняя строка сообщения — `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Push — только с согласия пользователя.

## Структура файлов

| Файл | Ответственность |
|---|---|
| `src-tauri/src/mcp/{guard,map,backend}.rs` | сторожок границы записи разбором путей, тексты ошибок для модели по списку ключей, сверка объекта замечания |
| `src-tauri/src/storage/{profile,exchange}.rs`, `secrets.rs`, `src/components/connections/{ConnectionForm.tsx,profile.ts}` | источник метрик подключения, его токен, локальность настройки |
| `src-tauri/src/metrics/{promql,prometheus,http,discovery,pods,select}.rs`, `k8s/{read,pool,connectivity}.rs`, `commands/resources.rs` | PromQL, источники и их выбор, прокси API-сервера, шаг «Метрики», статус источника |
| `src-tauri/src/resources/{current,usage,recommend,constraints,project}.rs`, `state.rs` | статистика workload'а и пода, движок рекомендаций, ограничения кластера, ресурсы проекта |
| `src-tauri/src/health/{collect,snapshot,texts,service}.rs`, `health/rules/resources.rs`, `k8s/quantity.rs` | правила `resources.*` |
| `src-tauri/src/guard.rs`, `src-tauri/src/change/{mod,facts}.rs` | страж изменений, модель `Change`, факты, предупреждения, «было → станет» |
| `src-tauri/src/health/{proposals,store}.rs`, `commands/health.rs` | предложения LLM и их статусы |
| `src-tauri/src/ops/mod.rs`, `commands/changes.rs`, `k8s/access.rs`, `error.rs` | запросы записи, предпросмотр и применение, права `patch`/`delete`/`create`, `ADMISSION_DENIED` |
| `crates/glafira-mcp-kubernetes/src/{resources,proposals,model}.rs`, `server/{resource_tools,proposal_tools,mod}.rs`, `src-tauri/src/mcp/{resources,proposals}.rs` | пять новых инструментов MCP |
| `dev/kind/{ops,metrics}.yaml`, `dev/kind/setup.sh`, `src-tauri/tests/{it_ops,it_metrics,it_mcp}.rs`, `tests/common/prometheus.rs` | стенд изменений и метрик, интеграция |
| `src/api/`, `src/lib/{quantity,useConnection}.ts`, `src/components/resources/{chart.ts,UsageChart.tsx}` | IPC O3, количества, графики |
| `src/components/change/`, `src/components/common/ConfirmDialog.tsx` | формы изменений |
| `src/components/resources/{recommend.ts,WorkloadResources.tsx,projectRows.ts,ProjectUsageView.tsx}` | вкладки «Ресурсы» |
| `src/components/health/{ProposalItem.tsx,proposals.ts,useActivePrefill.ts,findings.ts,FindingsPanel.tsx,HealthBlock.tsx}`, `src/state/workspace.ts` | предложения LLM в интерфейсе |
| `docs/guide/{en,ru}/*.md`, `docs/smoke/o3.md`, `README.md`, спека | справка, смоук, README, уточнения спеки |

### Task 1: Долги O2: граница записи MCP — разбор use-деревьев и путей, тексты ошибок для модели по списку ключей, объект замечания сверяется в проектах пользователя

**Files:**
- Create: `src-tauri/src/mcp/guard.rs`
- Modify: `src-tauri/src/mcp/backend.rs`, `src-tauri/src/mcp/map.rs`, `src-tauri/src/mcp/mod.rs`

**Interfaces:**
- Consumes: O2: сторожок `mcp_never_reaches_writes_or_the_raw_client` в `src-tauri/src/mcp/mod.rs`, `mcp::map::{error_text, llm_message, code}`, `mcp::backend::{AppBackend, target_exists, to_backend}`, `AppState::{read, read_in}`, `k8s::read::check_namespace`.
- Produces: `#[cfg(test)] mod guard` в `src-tauri/src/mcp/guard.rs`: лексер без комментариев, строк и времён жизни, разбор `use`-деревьев (псевдонимы, `self`, `super`, вложенные группы, встроенные модули) и путей; `FORBIDDEN_WORDS`, `FORBIDDEN_PATHS`, `KUBE_ALLOWED` (`kube::api::ListParams`, `kube::core::Status`); тест `mcp_never_reaches_writes_or_the_raw_client` обходит `src/mcp` рекурсивно; `mcp::map::{LLM_TEXT_KEYS, MAX_LLM_ERROR_CHARS = 500}`, `llm_message(&AppError) -> String` — текст модели только у ключей из списка, иначе «The operation failed; the user can see the details in Glafira»; `target_exists` читает объект проекта через `read_in`, проект — через `list_projects`.

Закрывает три пункта «Отложено» плана O2, которые касаются границы записи: этап O3 впервые вводит запись в кластер (`ops`, `ClusterWriter`), и сторожок по подстрокам, который обходился `use kube::{Api, Client}`, перестаёт быть достаточным. Новый сторожок разбирает исходники `mcp/` настоящим лексером (строки, сырые строки, символы и комментарии не мешают), собирает пути из `use`-деревьев и выражений и сверяет их со списками запрещённого: `crate::ops`, `crate::commands::changes`, `reqwest`, `hyper`, `http`, `ClusterWriter`, `KubeWriter`, `kube_client`, `Api` и `Client` из `kube`, `Patch*Params`, `apply_change`, `preview_change`; `extern crate` и `use crate::*` запрещены целиком. Два теста-«ловушки» доказывают, что псевдоним (`use kube::{Api as A}`), пробелы и вложенные группы его не обходят, а комментарии и литералы не дают ложных срабатываний. Сообщения сторожка — по-английски (i18n-проверка запрещает кириллицу в литералах Rust). Тексты ошибок для модели: прежний фильтр по префиксам пропускал новые префиксы с данными профиля, теперь — список разрешённых ключей и предел длины; тест проверяет, что каждый ключ списка есть в `en.json`. Объект замечания LLM сверяется с кластером в проекте через `read_in` (403 сбрасывает кэш прав проекта, как у вкладок), а существование проекта — по списку проектов пользователя.

- [ ] **Step 1: Реализация.**

<!-- block: file t01-src-tauri-src-mcp-guard-rs step=1.1 path=src-tauri/src/mcp/guard.rs -->
````rust
//! Сторожок «через MCP кластер не изменить» (спец. §4.1, §17): код `mcp/` не берёт клиент `kube`
//! мимо `ReadClient`, не знает изменяющих операций (`ops`, команды изменений) и не ходит в сеть
//! сам. Проверка — по разбору кода, а не по подстрокам: комментарии и содержимое литералов
//! вырезаются, `use`-деревья раскрываются (`use kube::{Api, Client}` — это пути `kube::Api` и
//! `kube::Client`), псевдонимы (`as`), `self` и `super` разрешаются, пробелы вокруг `::` не мешают.
//! Строки примеров в тестах этого модуля — литералы, поэтому сам модуль под проверку не попадает.

use std::collections::HashMap;
use std::path::Path;

/// Слова кода, которых в `mcp/` нет ни в каком виде: клиент и `Api` из `kube`, запись в кластер
/// (`ClusterWriter`, `Connected::writer`, `Connected::kube_client`, параметры запросов записи),
/// команды изменений.
const FORBIDDEN_WORDS: &[&str] = &[
    "Api",
    "Client",
    "ClusterWriter",
    "KubeWriter",
    "writer",
    "kube_client",
    "Patch",
    "PostParams",
    "PatchParams",
    "DeleteParams",
    "apply_change",
    "preview_change",
];

/// Пути, начало которых запрещено: изменяющие операции, команды изменений, клиенты HTTP.
const FORBIDDEN_PATHS: &[&str] = &[
    "crate::ops",
    "crate::commands::changes",
    "reqwest",
    "hyper",
    "http",
];

/// Из `kube` разрешено только это (и то, что внутри): параметры списка для `ReadClient` и тело
/// ответа об ошибке API-сервера (моки в тестах).
const KUBE_ALLOWED: &[&str] = &["kube::api::ListParams", "kube::core::Status"];

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Ident(String),
    /// `::`
    Path,
    Punct(char),
}

/// Токены кода без комментариев, строк, символов и времён жизни.
fn tokens(src: &str) -> Vec<Tok> {
    let c: Vec<char> = src.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    let ident_start = |ch: char| ch == '_' || ch.is_alphabetic();
    let ident_char = |ch: char| ch == '_' || ch.is_alphanumeric();
    while i < c.len() {
        let ch = c[i];
        let next = c.get(i + 1).copied();
        if ch == '/' && next == Some('/') {
            while i < c.len() && c[i] != '\n' {
                i += 1;
            }
        } else if ch == '/' && next == Some('*') {
            let mut depth = 0;
            while i < c.len() {
                if c[i] == '/' && c.get(i + 1) == Some(&'*') {
                    depth += 1;
                    i += 2;
                } else if c[i] == '*' && c.get(i + 1) == Some(&'/') {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else if let Some(end) = raw_string_end(&c, i) {
            i = end;
        } else if ch == '"' {
            i += 1;
            while i < c.len() && c[i] != '"' {
                i += if c[i] == '\\' { 2 } else { 1 };
            }
            i += 1;
        } else if ch == '\'' {
            if next == Some('\\') {
                // Символ с экранированием: '\n', '\'', '\u{1F600}'.
                i += 2;
                while i < c.len() && c[i] != '\'' {
                    i += 1;
                }
                i += 1;
            } else if c.get(i + 2) == Some(&'\'') {
                i += 3;
            } else {
                // Время жизни: 'a, 'static.
                i += 1;
                while i < c.len() && ident_char(c[i]) {
                    i += 1;
                }
            }
        } else if ident_start(ch) {
            // Сырой идентификатор r#type.
            if ch == 'r' && next == Some('#') && c.get(i + 2).is_some_and(|x| ident_start(*x)) {
                i += 2;
            }
            let from = i;
            while i < c.len() && ident_char(c[i]) {
                i += 1;
            }
            out.push(Tok::Ident(c[from..i].iter().collect()));
        } else if ch == ':' && next == Some(':') {
            out.push(Tok::Path);
            i += 2;
        } else {
            if !ch.is_whitespace() {
                out.push(Tok::Punct(ch));
            }
            i += 1;
        }
    }
    out
}

/// Конец сырой или байтовой строки (`r"…"`, `r#"…"#`, `b"…"`, `br#"…"#`, `c"…"`), начатой в `i`.
fn raw_string_end(c: &[char], i: usize) -> Option<usize> {
    if i > 0 && (c[i - 1] == '_' || c[i - 1].is_alphanumeric()) {
        return None;
    }
    let mut j = i;
    if matches!(c.get(j), Some('b' | 'c')) {
        j += 1;
    }
    let raw = c.get(j) == Some(&'r');
    if raw {
        j += 1;
    }
    if j == i {
        return None;
    }
    let mut hashes = 0;
    while raw && c.get(j) == Some(&'#') {
        hashes += 1;
        j += 1;
    }
    if c.get(j) != Some(&'"') {
        return None;
    }
    j += 1;
    if !raw {
        while j < c.len() && c[j] != '"' {
            j += if c[j] == '\\' { 2 } else { 1 };
        }
        return Some(j + 1);
    }
    while j < c.len() {
        if c[j] == '"' && (1..=hashes).all(|k| c.get(j + k) == Some(&'#')) {
            return Some(j + 1 + hashes);
        }
        j += 1;
    }
    Some(j)
}

/// Путь из `use`: сегменты, `*` в конце и имя, под которым он виден в файле.
#[derive(Debug)]
struct UsePath {
    segments: Vec<String>,
    glob: bool,
    alias: Option<String>,
}

/// Разбор дерева `use` с позиции `i`: `a::b::{c, d::e as f, g::*}`.
fn use_tree(t: &[Tok], i: &mut usize, prefix: Vec<String>, out: &mut Vec<UsePath>) {
    let mut segs = prefix;
    loop {
        match t.get(*i) {
            Some(Tok::Path) => *i += 1,
            Some(Tok::Ident(s)) if s != "as" => {
                segs.push(s.clone());
                *i += 1;
            }
            Some(Tok::Punct('*')) => {
                *i += 1;
                out.push(UsePath {
                    segments: segs,
                    glob: true,
                    alias: None,
                });
                return;
            }
            Some(Tok::Punct('{')) => {
                *i += 1;
                while !matches!(t.get(*i), Some(Tok::Punct('}')) | None) {
                    use_tree(t, i, segs.clone(), out);
                    if t.get(*i) == Some(&Tok::Punct(',')) {
                        *i += 1;
                    }
                }
                *i += 1;
                return;
            }
            _ => break,
        }
    }
    let mut alias = None;
    if t.get(*i) == Some(&Tok::Ident("as".into())) {
        if let Some(Tok::Ident(a)) = t.get(*i + 1) {
            alias = Some(a.clone());
        }
        *i += 2;
    }
    // `use a::{self}` — сам `a`.
    if segs.last().is_some_and(|s| s == "self") {
        segs.pop();
    }
    out.push(UsePath {
        segments: segs,
        glob: false,
        alias,
    });
}

/// Путь модуля по пути файла внутри `src/`: `mcp/backend.rs` → `crate::mcp::backend`.
fn module_of(rel: &Path) -> Vec<String> {
    let mut m = vec!["crate".to_string()];
    let parts: Vec<String> = rel
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();
    for (k, p) in parts.iter().enumerate() {
        let last = k + 1 == parts.len();
        if !last {
            m.push(p.clone());
        } else if p != "mod.rs" && p != "lib.rs" && p != "main.rs" {
            m.push(p.trim_end_matches(".rs").to_string());
        }
    }
    m
}

/// Полный путь: `self`, `super` и `crate` — от модуля, псевдоним `use` — его путём.
fn resolve(
    path: &[String],
    module: &[String],
    aliases: &HashMap<String, Vec<String>>,
) -> Vec<String> {
    let Some(first) = path.first() else {
        return Vec::new();
    };
    let mut base: Vec<String> = match first.as_str() {
        "crate" => vec!["crate".into()],
        "self" => module.to_vec(),
        "super" => {
            let mut m = module.to_vec();
            let mut k = 0;
            while path.get(k).is_some_and(|s| s == "super") {
                if m.len() > 1 {
                    m.pop();
                }
                k += 1;
            }
            m.extend(path[k..].iter().cloned());
            return m;
        }
        other => match aliases.get(other) {
            Some(full) => full.clone(),
            None => vec![other.to_string()],
        },
    };
    base.extend(path[1..].iter().cloned());
    base
}

fn starts_with(path: &[String], prefix: &str) -> bool {
    let p: Vec<&str> = prefix.split("::").collect();
    path.len() >= p.len() && path.iter().zip(&p).all(|(a, b)| a == b)
}

/// Нарушения в одном файле: слово, путь или `use`, которых в `mcp/` быть не должно.
fn violations(module: &[String], src: &str) -> Vec<String> {
    let t = tokens(src);
    let mut out = Vec::new();
    for tok in &t {
        if let Tok::Ident(s) = tok
            && FORBIDDEN_WORDS.contains(&s.as_str())
        {
            out.push(format!("word {s}"));
        }
        if tok == &Tok::Ident("extern".into()) {
            out.push("extern crate".into());
        }
    }
    // Модули внутри файла (`mod tests { … }`): `super` в них — от их собственного пути.
    let mut uses = Vec::new();
    let mut i = 0;
    let mut depth = 0usize;
    let mut stack: Vec<(String, usize)> = Vec::new();
    let mut paths: Vec<(Vec<String>, Vec<String>)> = Vec::new();
    while i < t.len() {
        let here: Vec<String> = module
            .iter()
            .cloned()
            .chain(stack.iter().map(|(m, _)| m.clone()))
            .collect();
        match &t[i] {
            Tok::Ident(s) if s == "mod" => {
                if let (Some(Tok::Ident(name)), Some(Tok::Punct('{'))) =
                    (t.get(i + 1), t.get(i + 2))
                {
                    depth += 1;
                    stack.push((name.clone(), depth));
                    i += 3;
                    continue;
                }
                i += 1;
            }
            Tok::Ident(s) if s == "use" && t.get(i + 1) != Some(&Tok::Punct('<')) => {
                i += 1;
                let mut found = Vec::new();
                use_tree(&t, &mut i, Vec::new(), &mut found);
                for u in found {
                    uses.push((here.clone(), u));
                }
            }
            Tok::Ident(s) => {
                // Путь в коде: a::b::c (и абсолютный ::a::b).
                let mut segs = vec![s.clone()];
                let mut j = i + 1;
                while t.get(j) == Some(&Tok::Path) {
                    match t.get(j + 1) {
                        Some(Tok::Ident(n)) => {
                            segs.push(n.clone());
                            j += 2;
                        }
                        _ => break,
                    }
                }
                if segs.len() > 1 {
                    paths.push((here.clone(), segs));
                }
                i = j;
            }
            Tok::Punct('{') => {
                depth += 1;
                i += 1;
            }
            Tok::Punct('}') => {
                if stack.last().is_some_and(|(_, d)| *d == depth) {
                    stack.pop();
                }
                depth = depth.saturating_sub(1);
                i += 1;
            }
            _ => i += 1,
        }
    }
    let mut aliases: HashMap<String, Vec<String>> = HashMap::new();
    let mut resolved = Vec::new();
    for (here, u) in &uses {
        let full = resolve(&u.segments, here, &aliases);
        if let Some(name) = u.alias.clone().or_else(|| u.segments.last().cloned())
            && !u.glob
            && name != "_"
        {
            aliases.insert(name, full.clone());
        }
        resolved.push((full, u.glob));
    }
    for (here, p) in &paths {
        resolved.push((resolve(p, here, &aliases), false));
    }
    for (path, glob) in resolved {
        let text = path.join("::") + if glob { "::*" } else { "" };
        if FORBIDDEN_PATHS.iter().any(|f| starts_with(&path, f)) {
            out.push(format!("path {text}"));
        }
        if path.first().is_some_and(|s| s == "kube")
            && (glob || !KUBE_ALLOWED.iter().any(|a| starts_with(&path, a)))
        {
            out.push(format!("path {text}"));
        }
        // `use crate::*` открыл бы и `ops`.
        if glob && path == ["crate"] {
            out.push(format!("path {text}"));
        }
    }
    out
}

/// Все `.rs` каталога и подкаталогов.
fn rust_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn mcp_never_reaches_writes_or_the_raw_client() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&src.join("mcp"), &mut files);
    for path in &files {
        let text = std::fs::read_to_string(path).unwrap();
        let module = module_of(path.strip_prefix(&src).unwrap());
        let found = violations(&module, &text);
        assert!(found.is_empty(), "{}: {found:?}", path.display());
    }
    assert!(files.len() >= 8, "mcp/ files: {}", files.len());
}

#[test]
fn use_trees_aliases_and_spacing_do_not_hide_the_raw_client() {
    let m = module_of(Path::new("mcp/backend.rs"));
    assert_eq!(m, ["crate", "mcp", "backend"]);
    for bad in [
        "use kube::{Api, Client};",
        "use kube::{api::{Api as A}, self};",
        "use kube as k;\nfn f() { let _ = k::Config::infer(); }",
        "fn f() { let _ = kube :: Config :: infer(); }",
        "fn f() { let _ = ::kube::Config::infer(); }",
        "use kube::api::*;",
        "use crate::ops;",
        "use crate::{health, ops::preview};",
        "use crate as c;\nfn f() { c::ops::apply(); }",
        "fn f() { super::super::ops::apply(); }",
        "use crate::*;",
        "use crate::commands::changes::apply;",
        "use reqwest::Client as R;",
        "fn f(c: &Connected) { c.writer(); }",
        "fn f(c: &Connected) { let _ = c.kube_client(); }",
        "extern crate kube;",
        "fn f() -> http::Request<()> { todo!() }",
    ] {
        assert!(!violations(&m, bad).is_empty(), "not found: {bad}");
    }
}

#[test]
fn comments_literals_and_allowed_paths_pass() {
    let m = module_of(Path::new("mcp/mod.rs"));
    assert_eq!(m, ["crate", "mcp"]);
    for good in [
        "use kube::api::ListParams;",
        "use kube::api::{ListParams};\nfn f() { let _ = ListParams::default(); }",
        "// use kube::Client; crate::ops::apply()\nfn f() {}",
        "/* kube::Api /* nested */ ops:: */ fn f() {}",
        "fn f() { let _ = \"kube::Client ops:: writer\"; }",
        "fn f() { let _ = r#\"use crate::ops;\"#; let c = 'a'; let q = '\\''; }",
        "fn f<'a>(x: &'a str) -> impl Iterator + use<'a> { x.chars() }",
        "use crate::k8s::read::ReadClient;\nfn f() { let _ = super::files::load(); }",
        "mod tests {\n use super::*;\n}",
        "fn f() { let stdin_writer = 1; let _ = stdin_writer; }",
    ] {
        assert!(
            violations(&m, good).is_empty(),
            "{good}: {:?}",
            violations(&m, good)
        );
    }
}
````

<!-- block: patch t01-src step=1.1 -->
````diff
diff --git a/src-tauri/src/mcp/backend.rs b/src-tauri/src/mcp/backend.rs
index 3f2e672..ddf5a98 100644
--- a/src-tauri/src/mcp/backend.rs
+++ b/src-tauri/src/mcp/backend.rs
@@ -425,20 +425,26 @@ impl AppBackend {
         }
     }
 
-    /// Объект замечания есть в кластере (защита от придуманных имён). Проект проверяется по
-    /// имени: его самого пользователь без прав на `namespaces` прочитать не может.
+    /// Объект замечания есть в кластере (защита от придуманных имён). Объекты проекта читаются
+    /// через `read_in` (403 сбрасывает кэш прав проекта, как у вкладки); проект — по списку
+    /// проектов пользователя (`project_exists`).
     async fn target_exists(&self, connection_id: &str, t: &Target) -> Result<(), BackendError> {
         let ns = t.namespace.clone().unwrap_or_default();
         let name = t.name.clone().unwrap_or_default();
         let found = match t.kind {
             TargetKind::Cluster => true,
-            TargetKind::Project => is_valid_namespace(&ns),
+            TargetKind::Project => self.project_exists(connection_id, &ns).await?,
             _ => {
-                let t = t.clone();
-                let r = Box::pin(self.state.read(connection_id, |c| async move {
-                    exists(&c.reader(), &t, &ns, &name).await
-                }))
-                .await;
+                let node = t.kind == TargetKind::Node;
+                let (target, in_ns) = (t.clone(), ns.clone());
+                let read = |c: Arc<crate::k8s::pool::Connected>| async move {
+                    exists(&c.reader(), &target, &in_ns, &name).await
+                };
+                let r = if node {
+                    Box::pin(self.state.read(connection_id, read)).await
+                } else {
+                    Box::pin(self.state.read_in(connection_id, &ns, read)).await
+                };
                 match r {
                     Ok(found) => found,
                     Err(e) if e.code == ErrorCode::NotFound => false,
@@ -461,6 +467,22 @@ impl AppBackend {
         }
     }
 
+    /// Проект есть среди проектов пользователя (как в дереве). Без права на их список (403)
+    /// проверить нельзя — тогда достаточно имени: так открывается проект из ручного списка.
+    async fn project_exists(&self, connection_id: &str, ns: &str) -> Result<bool, BackendError> {
+        if !is_valid_namespace(ns) {
+            return Ok(false);
+        }
+        let profile = self.allowed(connection_id)?;
+        let l = Box::pin(self.state.read(connection_id, |c| async move {
+            crate::k8s::projects::list_projects(&c.reader(), c.info.api_groups.projects, &profile)
+                .await
+        }))
+        .await
+        .map_err(to_backend)?;
+        Ok(l.error.is_some() || l.truncated || l.items.iter().any(|p| p.name == ns))
+    }
+
     async fn health_impl(&self, op: mh::HealthOp) -> Result<mh::HealthOutput, BackendError> {
         // Поколение — до проверки доступа: запись в подключение, удалённое за это время, отклоняется.
         let generation = self.state.health.generation(op.connection_id());
@@ -1062,6 +1084,33 @@ mod tests {
         ));
     }
 
+    #[tokio::test]
+    async fn project_notes_need_a_project_of_the_user() {
+        let (_d, b) = backend();
+        let ns = |name: &str| serde_json::json!({"metadata": {"name": name}});
+        pooled(&b, "all", move |s| match s.path() {
+            "/api/v1/namespaces" => list(vec![ns("team-a")]),
+            _ => status(404, "NotFound", "not found"),
+        });
+        let session = view("s1", Some("claude-ai"));
+        b.access(&session);
+        let annotate = |project: &str| {
+            b.health(mh::HealthOp::Annotate {
+                connection_id: "all".into(),
+                session: session.clone(),
+                note: note(Target::project(project), "Project note"),
+            })
+        };
+        assert!(annotate("team-a").await.is_ok());
+        let e = annotate("team-b").await.unwrap_err();
+        assert_eq!(e.code, "not_found", "{e:?}");
+        // Без права на список проектов проверить нельзя: достаточно имени.
+        pooled(&b, "all", |_| {
+            status(403, "Forbidden", "namespaces is forbidden")
+        });
+        assert!(annotate("team-b").await.is_ok());
+    }
+
     #[tokio::test]
     async fn health_ops_need_llm_access() {
         let (_d, b) = backend();
diff --git a/src-tauri/src/mcp/map.rs b/src-tauri/src/mcp/map.rs
index 525565c..8bd6974 100644
--- a/src-tauri/src/mcp/map.rs
+++ b/src-tauri/src/mcp/map.rs
@@ -30,9 +30,13 @@ pub fn error_text(e: &AppError) -> String {
     format!("{}: {}", code(e), llm_message(e))
 }
 
-/// Ошибки подключения, текст которых остаётся как есть: либо постоянная фраза без данных
-/// профиля, либо только то, что модель сама назвала в запросе (проект, id подключения).
-const PLAIN_CONNECTION_KEYS: &[&str] = &[
+/// Ключи ошибок, текст которых модель получает как есть: постоянная фраза без данных профиля
+/// или только то, о чём модель спросила сама (проект, объект, id подключения), и ответ
+/// API-сервера на её запрос (`{{message}}` у 404, 409, 400/422, 5xx). Остальные ключи — и
+/// любой новый — получают фиксированную фразу (`llm_message`): так текст с адресом сервера,
+/// командой exec-плагина, адресом источника метрик или именем пользователя не уйдёт модели,
+/// даже если о новом ключе здесь забыли.
+pub const LLM_TEXT_KEYS: &[&str] = &[
     "errors.exec.notApproved",
     "errors.connections.loginRequired",
     "errors.connections.tokenMissing",
@@ -42,18 +46,43 @@ const PLAIN_CONNECTION_KEYS: &[&str] = &[
     "errors.oauth.cancelled",
     "errors.oauth.timeout",
     "errors.oauth.badTokenResponse",
+    "errors.k8s.notFound",
+    "errors.k8s.badName",
+    "errors.k8s.forbiddenIn",
+    "errors.k8s.forbiddenCluster",
+    "errors.k8s.unauthorized",
+    "errors.k8s.conflict",
+    "errors.k8s.invalid",
+    "errors.k8s.tooManyRequests",
+    "errors.k8s.serverError",
+    "errors.k8s.apiError",
+    "errors.k8s.hostnameMismatch",
+    "errors.k8s.certExpired",
+    "errors.k8s.caUntrusted",
+    "errors.k8s.tlsError",
+    "errors.k8s.dns",
+    "errors.k8s.timeout",
+    "errors.k8s.connect",
+    "errors.k8s.malformedObject",
+    "errors.k8s.unexpectedResponse",
+    "errors.health.annotationNotFound",
+    "errors.internal",
 ];
 
-/// Текст ошибки для LLM. Чтение открывает подключение, если оно ещё не открыто, и тексты этих
-/// ошибок несут данные профиля: команду exec-плагина и параметры её запуска (`errors.exec.*`),
-/// адрес API-сервера и прокси (`errors.connections.*`), ответ OAuth-сервера (`errors.oauth.*`),
-/// цепочку причин kube и сети, имя пользователя из ответа 403. Модель получает вместо них фиксированную фразу без этих данных;
-/// код ошибки остаётся прежним, подробности — только у человека в интерфейсе. Остальные ошибки
-/// несут имена объектов, о которых модель сама спросила, и не меняются.
+/// Длина текста ошибки для модели: ответ API-сервера бывает длинным (сообщение webhook'а).
+pub const MAX_LLM_ERROR_CHARS: usize = 500;
+
+/// Текст ошибки для LLM. Чтение открывает подключение, если оно ещё не открыто, и тексты
+/// ошибок подключения несут данные профиля: команду exec-плагина и параметры её запуска
+/// (`errors.exec.*`), адрес API-сервера и прокси (`errors.connections.*`), ответ OAuth-сервера
+/// (`errors.oauth.*`), цепочку причин kube и сети, имя пользователя из ответа 403. Как есть
+/// уходят только ключи `LLM_TEXT_KEYS` (не длиннее `MAX_LLM_ERROR_CHARS`); остальные получают
+/// фиксированную фразу по группе ключа. Код ошибки остаётся прежним, подробности — только у
+/// человека в интерфейсе.
 pub fn llm_message(e: &AppError) -> String {
     let key = e.key.as_str();
-    if PLAIN_CONNECTION_KEYS.contains(&key) {
-        return e.message.clone();
+    if LLM_TEXT_KEYS.contains(&key) {
+        return cut(&e.message, MAX_LLM_ERROR_CHARS);
     }
     let hidden = if key.starts_with("errors.exec.") {
         "The connection's credential plugin failed; check the connection in Glafira"
@@ -66,12 +95,22 @@ pub fn llm_message(e: &AppError) -> String {
             "errors.k8s.credentials" => "The credentials were rejected; sign in again in Glafira",
             "errors.k8s.transport" => "The cluster could not be reached",
             "errors.k8s.forbidden" => "Access is forbidden",
-            _ => return e.message.clone(),
+            _ => "The operation failed; the user can see the details in Glafira",
         }
     };
     hidden.to_string()
 }
 
+/// Не длиннее `max` символов, с «…» на месте отрезанного.
+fn cut(s: &str, max: usize) -> String {
+    if s.chars().count() <= max {
+        return s.to_string();
+    }
+    let mut out: String = s.chars().take(max - 1).collect();
+    out.push('…');
+    out
+}
+
 pub fn code(e: &AppError) -> String {
     serde_json::to_value(e.code)
         .ok()
@@ -799,4 +838,45 @@ mod tests {
         };
         assert!(error.starts_with("FORBIDDEN: "), "{error}");
     }
+
+    #[test]
+    fn unknown_error_keys_get_a_fixed_phrase() {
+        let e = crate::app_err!(
+            crate::error::ErrorCode::ConnectionFailed,
+            "errors.someday.newKey",
+            "Cannot reach {{url}}",
+            url = "https://marker-host.example/prometheus"
+        );
+        assert_eq!(
+            error_text(&e),
+            "CONNECTION_FAILED: The operation failed; the user can see the details in Glafira"
+        );
+    }
+
+    #[test]
+    fn server_text_is_cut_for_the_model() {
+        let long = "x".repeat(2000);
+        let e = crate::k8s::errors::from_status(
+            &kube::core::Status::failure(&long, "Invalid").with_code(422),
+        );
+        let text = llm_message(&e);
+        assert_eq!(text.chars().count(), MAX_LLM_ERROR_CHARS);
+        assert!(text.ends_with('…'), "{text}");
+        let short = crate::k8s::errors::not_allowed("get", "pods", Some("team-a"));
+        assert_eq!(llm_message(&short), short.message);
+    }
+
+    #[test]
+    fn llm_text_keys_are_dictionary_keys() {
+        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../src/locales/en.json");
+        let dict: serde_json::Value =
+            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
+        for key in LLM_TEXT_KEYS {
+            let found = key
+                .split('.')
+                .try_fold(&dict, |v, part| v.get(part))
+                .is_some_and(serde_json::Value::is_string);
+            assert!(found, "{key}");
+        }
+    }
 }
diff --git a/src-tauri/src/mcp/mod.rs b/src-tauri/src/mcp/mod.rs
index 2c2c195..79864fd 100644
--- a/src-tauri/src/mcp/mod.rs
+++ b/src-tauri/src/mcp/mod.rs
@@ -6,6 +6,8 @@ pub mod approvals;
 pub mod backend;
 pub mod controller;
 pub mod files;
+#[cfg(test)]
+mod guard;
 pub mod map;
 pub mod shim;
 
@@ -133,38 +135,3 @@ mod tests {
         assert_eq!(launch_path(exe, Some("".into())), exe.to_path_buf());
     }
 }
-
-/// Сторожок «через MCP кластер не изменить» (спец. §4.1, §17): код `mcp/` не берёт клиент `kube`
-/// мимо `ReadClient` и не знает изменяющих операций. Шаблоны собраны из частей, чтобы этот файл
-/// сам под них не попадал.
-#[cfg(test)]
-mod guard {
-    #[test]
-    fn mcp_never_reaches_writes_or_the_raw_client() {
-        let forbidden: Vec<String> = [
-            ["ops", "::"],
-            ["Cluster", "Writer"],
-            ["kube_", "client("],
-            ["kube::", "Client"],
-            ["kube::", "Api"],
-            ["kube::api::", "Api<"],
-            ["Post", "Params"],
-            ["Patch", "Params"],
-            ["Delete", "Params"],
-        ]
-        .iter()
-        .map(|p| p.concat())
-        .collect();
-        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/mcp");
-        let mut checked = 0;
-        for entry in std::fs::read_dir(&dir).unwrap() {
-            let path = entry.unwrap().path();
-            let text = std::fs::read_to_string(&path).unwrap();
-            for f in &forbidden {
-                assert!(!text.contains(f.as_str()), "{}: {f}", path.display());
-            }
-            checked += 1;
-        }
-        assert!(checked >= 7, "файлы mcp/: {checked}");
-    }
-}
````

- [ ] **Step 2: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 584 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 160`

- [ ] **Step 3: Commit.**

```bash
git add \
  src-tauri/src/mcp/backend.rs \
  src-tauri/src/mcp/guard.rs \
  src-tauri/src/mcp/map.rs \
  src-tauri/src/mcp/mod.rs
git commit -s -m "fix(mcp): the write-boundary guard parses use trees and paths, error texts reach the model only from an allowlist, note targets are checked against the user's projects" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: Источник метрик в профиле подключения: автоматически, свой Thanos Querier или Prometheus с авторизацией и CA; локальная настройка, токен — в keychain

**Files:**
- Modify: `src-tauri/src/commands/connections.rs`, `src-tauri/src/commands/kubeconfig.rs`, `src-tauri/src/k8s/kubeconfig.rs`, `src-tauri/src/secrets.rs`, `src-tauri/src/storage/exchange.rs`, `src-tauri/src/storage/profile.rs`, `src/api/types.ts`, `src/components/connections/ConnectionForm.test.tsx`, `src/components/connections/ConnectionForm.tsx`, `src/components/connections/profile.test.ts`, `src/components/connections/profile.ts`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: O1a: `storage::profile::{ConnectionProfile, validate}`, `secrets::{secret_key, CONNECTION_SECRET_FIELDS}`, `commands::connections::SecretsInput`, `storage::exchange`, `k8s::kubeconfig::import_context`; форма `ConnectionForm`.
- Produces: `storage::profile::{MetricsConfig (serde tag "mode": Auto | Thanos(MetricsEndpoint) | Prometheus(MetricsEndpoint)), MetricsEndpoint { url, auth, ca }, MetricsAuth (connection | bearer | none), MetricsCa (connection | pem { pem })}`, поле `ConnectionProfile.metrics` (не пишется, пока `auto`), `MetricsConfig::endpoint()`; `secrets::METRICS_TOKEN = "metricsToken"`, `CONNECTION_SECRET_FIELDS: [&str; 3]`; `SecretsInput.metrics_token`; TS: `MetricsConfig`, `MetricsMode`, `MetricsEndpoint`, `MetricsAuth`, `MetricsCa`, `SecretsInput.metricsToken`, `profile.ts::{newMetricsEndpoint, withMetricsMode}`.

Решение пользователя: настройка метрик — локальная, как доступ LLM: экспорт её не пишет, импорт нового подключения даёт `auto`, обновление существующего сохраняет его настройку, а сравнение «то же подключение» её не учитывает. Проверки профиля: адрес — `https://` или `http://` без учётных данных в URL, `http://` — только без авторизации (`errors.connections.metricsHttpsRequired`), PEM — как у CA подключения. Свой токен источника — секрет подключения (`<id>/metricsToken`), форма отправляет его, только когда режим не `auto`, а смена режима не теряет введённый адрес. Импорт kubeconfig даёт `auto`.

- [ ] **Step 1: Тесты.**

<!-- block: patch t02-tests step=2.1 -->
````diff
diff --git a/src/components/connections/ConnectionForm.test.tsx b/src/components/connections/ConnectionForm.test.tsx
index 4196d48..82b0f8f 100644
--- a/src/components/connections/ConnectionForm.test.tsx
+++ b/src/components/connections/ConnectionForm.test.tsx
@@ -377,6 +377,37 @@ describe("ConnectionForm", () => {
     });
   });
 
+  it("источник метрик: свой Prometheus с токеном — адрес в профиле, токен секретом; «Автоматически» — без них", async () => {
+    vi.mocked(api.saveConnection).mockImplementation(async (p) => ({ ...p, id: "c1" }));
+    const user = userEvent.setup();
+    const initial = { ...newProfile(), id: "c1", name: "dev", apiUrl: "https://api.dev:6443" };
+    renderWithClient(<ConnectionForm initial={initial} tabId="t" />);
+    expect(screen.queryByLabelText("Адрес источника")).toBeNull();
+    await user.selectOptions(screen.getByLabelText("Метрики"), "prometheus");
+    await user.type(screen.getByLabelText("Адрес источника"), "https://prometheus.example.com");
+    await user.selectOptions(screen.getByLabelText("Авторизация источника"), "bearer");
+    await user.type(screen.getByLabelText("Токен источника"), "prom-token");
+    await user.selectOptions(screen.getByLabelText("CA источника"), "pem");
+    await user.type(screen.getByLabelText("Сертификаты CA источника (PEM)"), "PEM");
+    await user.click(screen.getByRole("button", { name: "Сохранить" }));
+    await waitFor(() => expect(api.saveConnection).toHaveBeenCalledTimes(1));
+    const [saved, secrets] = vi.mocked(api.saveConnection).mock.calls[0];
+    expect(saved.metrics).toEqual({ mode: "prometheus", url: "https://prometheus.example.com", auth: "bearer", ca: { mode: "pem", pem: "PEM" } });
+    expect(secrets?.metricsToken).toBe("prom-token");
+
+    // Вернуться к «Автоматически» и обратно: введённый адрес не теряется.
+    await user.selectOptions(screen.getByLabelText("Метрики"), "auto");
+    expect(screen.queryByLabelText("Адрес источника")).toBeNull();
+    await user.selectOptions(screen.getByLabelText("Метрики"), "thanos");
+    expect(screen.getByLabelText("Адрес источника")).toHaveValue("https://prometheus.example.com");
+    await user.selectOptions(screen.getByLabelText("Метрики"), "auto");
+    await user.click(screen.getByRole("button", { name: "Сохранить" }));
+    await waitFor(() => expect(api.saveConnection).toHaveBeenCalledTimes(2));
+    const [auto, autoSecrets] = vi.mocked(api.saveConnection).mock.calls[1];
+    expect(auto.metrics).toEqual({ mode: "auto" });
+    expect(autoSecrets).not.toHaveProperty("metricsToken");
+  });
+
   it("поля проектов подсказывают проекты, которые уже прочитало дерево, без нового запроса", async () => {
     const user = userEvent.setup();
     const initial = { ...newProfile(), id: "c1", name: "dev", apiUrl: "https://api.dev:6443", favoriteNamespaces: ["team-a"], llmAccess: "metadata" as const, llmNamespaces: [] };
diff --git a/src/components/connections/profile.test.ts b/src/components/connections/profile.test.ts
index b80321d..c2a1429 100644
--- a/src/components/connections/profile.test.ts
+++ b/src/components/connections/profile.test.ts
@@ -1,5 +1,5 @@
 import { describe, expect, it } from "vitest";
-import { formatNamespaces, newProfile, parseNamespaces, withAuthMethod, withCaMode, withProxyMode } from "./profile";
+import { formatNamespaces, newProfile, parseNamespaces, withAuthMethod, withCaMode, withMetricsMode, withProxyMode } from "./profile";
 
 describe("profile", () => {
   it("новый профиль: вход через браузер, системный CA и прокси, dev", () => {
@@ -32,3 +32,14 @@ describe("profile", () => {
     expect(formatNamespaces(["a", "b"])).toBe("a, b");
   });
 });
+
+describe("withMetricsMode", () => {
+  it("свой адрес не теряется при переключении", () => {
+    const thanos = withMetricsMode({ mode: "auto" }, "thanos");
+    expect(thanos).toEqual({ mode: "thanos", url: "", auth: "connection", ca: { mode: "connection" } });
+    const own = { mode: "thanos" as const, url: "https://t.example.com", auth: "bearer" as const, ca: { mode: "connection" as const } };
+    expect(withMetricsMode(own, "prometheus")).toEqual({ ...own, mode: "prometheus" });
+    expect(withMetricsMode(own, "auto")).toEqual({ mode: "auto" });
+    expect(withMetricsMode({ mode: "auto" }, "prometheus", own)).toEqual({ ...own, mode: "prometheus" });
+  });
+});
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/connections/ConnectionForm.test.tsx src/components/connections/profile.test.ts`  
Expected: FAIL — нет `newMetricsEndpoint` и `withMetricsMode`, в форме нет раздела «Метрики».

- [ ] **Step 3: Реализация.**

<!-- block: patch t02-src step=2.3 -->
````diff
diff --git a/src-tauri/src/commands/connections.rs b/src-tauri/src/commands/connections.rs
index bab4e9f..4dcd746 100644
--- a/src-tauri/src/commands/connections.rs
+++ b/src-tauri/src/commands/connections.rs
@@ -8,9 +8,9 @@ use super::blocking;
 use crate::error::{AppError, AppResult};
 use crate::k8s::cluster::ConnectionInfo;
 use crate::k8s::connectivity::{TestReport, ensure_allowed};
-use crate::secrets::{CLIENT_KEY, OverlayStore, SecretStore, TOKEN, secret_key};
+use crate::secrets::{CLIENT_KEY, METRICS_TOKEN, OverlayStore, SecretStore, TOKEN, secret_key};
 use crate::state::AppState;
-use crate::storage::profile::{AuthConfig, ConnectionProfile, REDACTED};
+use crate::storage::profile::{AuthConfig, ConnectionProfile, MetricsAuth, REDACTED};
 
 /// Секреты из формы. Для каждого: нет или `null` — оставить сохранённый, `""` — удалить,
 /// иное — сохранить. `Debug` печатает вместо значений `***`.
@@ -21,6 +21,9 @@ pub struct SecretsInput {
     pub token: Option<String>,
     #[serde(default)]
     pub client_key: Option<String>,
+    /// Bearer-токен своего источника метрик.
+    #[serde(default)]
+    pub metrics_token: Option<String>,
 }
 
 /// Токен и приватный ключ не печатаются: структура может попасть в лог или сообщение об ошибке.
@@ -30,6 +33,7 @@ impl std::fmt::Debug for SecretsInput {
         f.debug_struct("SecretsInput")
             .field("token", &hide(&self.token))
             .field("client_key", &hide(&self.client_key))
+            .field("metrics_token", &hide(&self.metrics_token))
             .finish()
     }
 }
@@ -89,7 +93,7 @@ enum SecretOp {
 }
 
 /// Используемые профилем секреты — по данным формы, неиспользуемые — удаляются.
-fn secret_ops(profile: &ConnectionProfile, s: &SecretsInput) -> [(&'static str, SecretOp); 2] {
+fn secret_ops(profile: &ConnectionProfile, s: &SecretsInput) -> [(&'static str, SecretOp); 3] {
     let op = |used: bool, v: Option<&String>| match (used, v.map(|x| x.trim())) {
         (false, _) => SecretOp::Delete,
         (true, None) => SecretOp::Keep,
@@ -105,6 +109,16 @@ fn secret_ops(profile: &ConnectionProfile, s: &SecretsInput) -> [(&'static str,
                 s.client_key.as_ref(),
             ),
         ),
+        (
+            METRICS_TOKEN,
+            op(
+                profile
+                    .metrics
+                    .endpoint()
+                    .is_some_and(|e| e.auth == MetricsAuth::Bearer),
+                s.metrics_token.as_ref(),
+            ),
+        ),
     ]
 }
 
@@ -195,6 +209,7 @@ pub async fn test_connection(
     for (field, v) in [
         (TOKEN, &input.secrets.token),
         (CLIENT_KEY, &input.secrets.client_key),
+        (METRICS_TOKEN, &input.secrets.metrics_token),
     ] {
         if let Some(v) = v {
             overrides.insert(secret_key(&id, field), v.trim().to_string());
@@ -289,7 +304,9 @@ mod tests {
     use super::*;
     use crate::error::ErrorCode;
     use crate::secrets::MemoryStore;
-    use crate::storage::profile::{CaConfig, TEST_CERT_PEM, sample_profile};
+    use crate::storage::profile::{
+        CaConfig, MetricsCa, MetricsConfig, MetricsEndpoint, TEST_CERT_PEM, sample_profile,
+    };
 
     fn state(dir: &std::path::Path) -> AppState {
         AppState::new(dir, Arc::new(MemoryStore::default())).unwrap()
@@ -305,6 +322,7 @@ mod tests {
             secrets: SecretsInput {
                 token: token.map(String::from),
                 client_key: key.map(String::from),
+                metrics_token: None,
             },
             confirm_exec: false,
         }
@@ -320,10 +338,15 @@ mod tests {
         let s = |t: Option<&str>, k: Option<&str>| SecretsInput {
             token: t.map(String::from),
             client_key: k.map(String::from),
+            metrics_token: Some("m".into()),
         };
         assert_eq!(
             secret_ops(&token, &s(None, Some("k"))),
-            [(TOKEN, SecretOp::Keep), (CLIENT_KEY, SecretOp::Delete)]
+            [
+                (TOKEN, SecretOp::Keep),
+                (CLIENT_KEY, SecretOp::Delete),
+                (METRICS_TOKEN, SecretOp::Delete)
+            ]
         );
         assert_eq!(
             secret_ops(&token, &s(Some(" t "), None))[0],
@@ -341,9 +364,25 @@ mod tests {
             secret_ops(&cert, &s(Some("t"), Some("KEY"))),
             [
                 (TOKEN, SecretOp::Delete),
-                (CLIENT_KEY, SecretOp::Set("KEY".into()))
+                (CLIENT_KEY, SecretOp::Set("KEY".into())),
+                (METRICS_TOKEN, SecretOp::Delete)
             ]
         );
+        // Токен метрик хранится, только пока у профиля свой источник с авторизацией bearer.
+        let mut bearer = sample_profile("a");
+        bearer.metrics = MetricsConfig::Prometheus(MetricsEndpoint {
+            url: "https://prometheus.example.com".into(),
+            auth: MetricsAuth::Bearer,
+            ca: MetricsCa::Connection,
+        });
+        assert_eq!(
+            secret_ops(&bearer, &s(None, None))[2],
+            (METRICS_TOKEN, SecretOp::Set("m".into()))
+        );
+        assert_eq!(
+            secret_ops(&bearer, &SecretsInput::default())[2],
+            (METRICS_TOKEN, SecretOp::Keep)
+        );
     }
 
     #[test]
@@ -576,6 +615,7 @@ mod tests {
         let secrets = SecretsInput {
             token: Some("sha256~tok-123".into()),
             client_key: Some("KEY-456".into()),
+            metrics_token: Some("METRICS-789".into()),
         };
         let connection = ConnectionInput {
             profile: sample_profile("a"),
@@ -589,7 +629,9 @@ mod tests {
             format!("{connection:#?}"),
         ] {
             assert!(
-                !text.contains("tok-123") && !text.contains("KEY-456"),
+                !text.contains("tok-123")
+                    && !text.contains("KEY-456")
+                    && !text.contains("METRICS-789"),
                 "{text}"
             );
             assert!(text.contains("***"), "{text}");
diff --git a/src-tauri/src/commands/kubeconfig.rs b/src-tauri/src/commands/kubeconfig.rs
index 955ee06..5bf442c 100644
--- a/src-tauri/src/commands/kubeconfig.rs
+++ b/src-tauri/src/commands/kubeconfig.rs
@@ -172,6 +172,7 @@ fn prepare(
         secrets: SecretsInput {
             token,
             client_key: Some(imported.client_key.unwrap_or_default()),
+            metrics_token: None,
         },
         confirm_exec,
     })
diff --git a/src-tauri/src/k8s/kubeconfig.rs b/src-tauri/src/k8s/kubeconfig.rs
index b5e6f8d..832b9f8 100644
--- a/src-tauri/src/k8s/kubeconfig.rs
+++ b/src-tauri/src/k8s/kubeconfig.rs
@@ -425,6 +425,7 @@ pub fn import_context(
         order: 0,
         llm_access: Default::default(),
         llm_namespaces: None,
+        metrics: Default::default(),
         extra: Default::default(),
     };
     normalize_profile(&mut profile);
diff --git a/src-tauri/src/secrets.rs b/src-tauri/src/secrets.rs
index 5c28abd..cc633d5 100644
--- a/src-tauri/src/secrets.rs
+++ b/src-tauri/src/secrets.rs
@@ -15,8 +15,11 @@ pub const TOKEN: &str = "token";
 /// Ключ клиентского сертификата (PEM).
 pub const CLIENT_KEY: &str = "clientKey";
 
+/// Bearer-токен своего источника метрик (`MetricsAuth::Bearer`).
+pub const METRICS_TOKEN: &str = "metricsToken";
+
 /// Все секреты подключения в keychain (для удаления).
-pub const CONNECTION_SECRET_FIELDS: [&str; 2] = [TOKEN, CLIENT_KEY];
+pub const CONNECTION_SECRET_FIELDS: [&str; 3] = [TOKEN, CLIENT_KEY, METRICS_TOKEN];
 
 pub fn secret_key(connection_id: &str, field: &str) -> String {
     format!("{connection_id}/{field}")
diff --git a/src-tauri/src/storage/exchange.rs b/src-tauri/src/storage/exchange.rs
index 5325458..fbbad91 100644
--- a/src-tauri/src/storage/exchange.rs
+++ b/src-tauri/src/storage/exchange.rs
@@ -150,9 +150,11 @@ pub fn build_export(
         .filter(|p| in_scope(p.folder_id.as_deref()))
         .map(|mut profile| {
             profile.extra.clear();
-            // Доступ LLM — локальное решение получателя (спец. §5.1): в файл не пишется.
+            // Доступ LLM и источник метрик — локальные решения получателя (спец. §5.1): в файл
+            // не пишутся.
             profile.llm_access = Default::default();
             profile.llm_namespaces = None;
+            profile.metrics = Default::default();
             strip_exec_for_export(&mut profile);
             strip_pem_keys_for_export(&mut profile);
             let needs_secret = needs_secret(secrets, &profile)?;
@@ -411,6 +413,7 @@ fn same_settings(local: &ConnectionProfile, incoming: &ConnectionProfile) -> boo
             order: 0,
             llm_access: Default::default(),
             llm_namespaces: None,
+            metrics: Default::default(),
             extra: Default::default(),
             ..p.clone()
         };
@@ -622,12 +625,13 @@ pub(crate) fn plan_import(
                 unchanged.push((TreeItemKind::Connection, profile.id.clone()));
                 continue;
             }
-            // Место в дереве, доступ LLM и неизвестные поля — локальные; пароли получателя не
-            // трогаются.
+            // Место в дереве, доступ LLM, источник метрик и неизвестные поля — локальные; пароли
+            // получателя не трогаются.
             profile.folder_id = slot.folder_id.clone();
             profile.order = slot.order;
             profile.llm_access = slot.llm_access;
             profile.llm_namespaces = slot.llm_namespaces.take();
+            profile.metrics = std::mem::take(&mut slot.metrics);
             profile.extra = std::mem::take(&mut slot.extra);
             // Секреты exec-плагина тоже локальные: файл их не несёт. Подтверждение остаётся,
             // только если строка запуска с этими секретами не изменилась (спец. §5.2).
@@ -678,9 +682,11 @@ pub(crate) fn plan_import(
             .unwrap_or(0);
         profile.folder_id = folder;
         profile.name = name;
-        // Файл не открывает кластер модели: доступ LLM нового подключения выключен.
+        // Файл не открывает кластер модели и не задаёт, куда ходить за метриками: доступ LLM
+        // нового подключения выключен, источник метрик — автоматический.
         profile.llm_access = Default::default();
         profile.llm_namespaces = None;
+        profile.metrics = Default::default();
         profile.extra = Default::default();
         if needs.token || needs.client_key {
             preview.needs_secrets += 1;
@@ -880,6 +886,51 @@ mod tests {
         assert!(!serde_json::to_string(&out).unwrap().contains("futureField"));
     }
 
+    /// Источник метрик — локальная настройка: экспорт её не пишет, импорт не переносит (чужой
+    /// файл не направит токен подключения на свой адрес).
+    #[test]
+    fn metrics_source_is_never_exported_or_imported() {
+        use crate::storage::profile::{MetricsAuth, MetricsCa, MetricsConfig, MetricsEndpoint};
+        let (_d, s) = store();
+        let mut own = sample_profile("a");
+        own.metrics = MetricsConfig::Thanos(MetricsEndpoint {
+            url: "https://thanos.example.com".into(),
+            auth: MetricsAuth::Connection,
+            ca: MetricsCa::Connection,
+        });
+        s.upsert(own.clone()).unwrap();
+        let out = build_export(&s, &MemoryStore::default(), None, "1", "now").unwrap();
+        let json = serde_json::to_value(&out).unwrap();
+        assert!(json["connections"][0].get("metrics").is_none(), "{json}");
+
+        let data = StoreData {
+            connections: vec![own.clone()],
+            ..Default::default()
+        };
+        let evil = serde_json::json!({
+            "mode": "prometheus", "url": "https://evil.example.com", "auth": "connection"
+        });
+        let mut incoming_a = conn_json("a", "conn-a", None);
+        incoming_a["apiUrl"] = "https://api.other.example.com:6443".into();
+        incoming_a["metrics"] = evil.clone();
+        let mut incoming_b = conn_json("b", "conn-b", None);
+        incoming_b["metrics"] = evil;
+        let p = plan(
+            &data,
+            &file(vec![], vec![incoming_a, incoming_b]),
+            &opts(None),
+        );
+        assert_eq!(
+            conn(&p.next, "a").metrics,
+            own.metrics,
+            "своя настройка сохраняется"
+        );
+        assert!(
+            conn(&p.next, "b").metrics.is_auto(),
+            "новое подключение — автоматический источник"
+        );
+    }
+
     /// Доступ LLM — локальное решение (спец. §5.1): экспорт его не пишет, импорт не переносит.
     #[test]
     fn llm_access_is_never_exported_or_imported() {
diff --git a/src-tauri/src/storage/profile.rs b/src-tauri/src/storage/profile.rs
index 1956004..569d4a1 100644
--- a/src-tauri/src/storage/profile.rs
+++ b/src-tauri/src/storage/profile.rs
@@ -176,6 +176,70 @@ impl LlmAccess {
     }
 }
 
+/// Источник метрик подключения (спец. §5.1, §7.1). `Auto` — автопоиск платформы, затем
+/// `metrics.k8s.io`; свой URL явно выбранного источника другим не заменяется. Локальная
+/// настройка, как доступ LLM: экспорт её не пишет, импорт не переносит — чужой файл не направит
+/// токен подключения на свой адрес.
+#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
+#[serde(tag = "mode", rename_all = "camelCase")]
+pub enum MetricsConfig {
+    #[default]
+    Auto,
+    /// Свой Thanos Querier (например, tenancy-endpoint, открытый администраторами): запросы по
+    /// проекту идут с параметром `namespace`.
+    Thanos(MetricsEndpoint),
+    /// Свой Prometheus.
+    Prometheus(MetricsEndpoint),
+}
+
+impl MetricsConfig {
+    pub fn is_auto(&self) -> bool {
+        *self == MetricsConfig::Auto
+    }
+
+    pub fn endpoint(&self) -> Option<&MetricsEndpoint> {
+        match self {
+            MetricsConfig::Auto => None,
+            MetricsConfig::Thanos(e) | MetricsConfig::Prometheus(e) => Some(e),
+        }
+    }
+}
+
+/// Адрес своего источника метрик и как к нему ходить.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(rename_all = "camelCase")]
+pub struct MetricsEndpoint {
+    /// `https://thanos.example.com` — без `/api/v1`.
+    pub url: String,
+    #[serde(default)]
+    pub auth: MetricsAuth,
+    #[serde(default)]
+    pub ca: MetricsCa,
+}
+
+/// Авторизация источника метрик: токен подключения, свой bearer-токен (keychain, поле
+/// `metricsToken`) или без авторизации.
+#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
+#[serde(rename_all = "camelCase")]
+pub enum MetricsAuth {
+    #[default]
+    Connection,
+    Bearer,
+    None,
+}
+
+/// CA источника метрик: как у подключения или свой PEM; оба добавляются к системным корням
+/// (сертификат маршрута часто выпущен CA ingress).
+#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
+#[serde(tag = "mode", rename_all = "camelCase")]
+pub enum MetricsCa {
+    #[default]
+    Connection,
+    Pem {
+        pem: String,
+    },
+}
+
 #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
 #[serde(rename_all = "camelCase")]
 pub struct ConnectionProfile {
@@ -216,6 +280,9 @@ pub struct ConnectionProfile {
     /// Проекты, которые видит LLM; `None` — все доступные пользователю. Локальное поле.
     #[serde(default, skip_serializing_if = "Option::is_none")]
     pub llm_namespaces: Option<Vec<String>>,
+    /// Источник метрик. Локальное поле.
+    #[serde(default, skip_serializing_if = "MetricsConfig::is_auto")]
+    pub metrics: MetricsConfig,
     /// Поля из более новой версии Glafira: сохраняются при записи как есть.
     /// `ConnectionStore::upsert` входящий `extra` не принимает: у существующего профиля
     /// остаётся сохранённый, новый получает пустой.
@@ -227,6 +294,9 @@ pub struct ConnectionProfile {
 /// списки проектов без пустых строк и повторов.
 pub fn normalize_profile(p: &mut ConnectionProfile) {
     p.api_url = p.api_url.trim().trim_end_matches('/').to_string();
+    if let MetricsConfig::Thanos(e) | MetricsConfig::Prometheus(e) = &mut p.metrics {
+        e.url = e.url.trim().trim_end_matches('/').to_string();
+    }
     p.tls_server_name = p
         .tls_server_name
         .take()
@@ -468,6 +538,9 @@ pub fn validate_profile(p: &ConnectionProfile) -> AppResult<()> {
             name = ns
         )));
     }
+    if let Some(e) = p.metrics.endpoint() {
+        validate_metrics(e)?;
+    }
     if p.request_timeout_ms < 1_000 || p.connect_timeout_ms < 1_000 {
         return Err(invalid(ltext!(
             "errors.connections.timeoutTooSmall",
@@ -477,6 +550,50 @@ pub fn validate_profile(p: &ConnectionProfile) -> AppResult<()> {
     Ok(())
 }
 
+/// Свой источник метрик: адрес http(s) с хостом, без имени и пароля, без запроса; `http://` —
+/// только без авторизации (токен не уходит открытым текстом); CA — сертификаты PEM без ключа.
+fn validate_metrics(e: &MetricsEndpoint) -> AppResult<()> {
+    let bad = || {
+        invalid(ltext!(
+            "errors.connections.badMetricsUrl",
+            "Invalid metrics source address “{{url}}”: use https://host[:port][/path]",
+            url = redact_url(&e.url)
+        ))
+    };
+    let u = url::Url::parse(e.url.trim()).map_err(|_| bad())?;
+    if !matches!(u.scheme(), "http" | "https")
+        || u.host_str().is_none_or(str::is_empty)
+        || u.query().is_some()
+        || u.fragment().is_some()
+    {
+        return Err(bad());
+    }
+    if has_userinfo(&u) {
+        return Err(credentials_in_url(&u));
+    }
+    if u.scheme() == "http" && e.auth != MetricsAuth::None {
+        return Err(invalid(ltext!(
+            "errors.connections.metricsHttpsRequired",
+            "A metrics source with a token needs an https:// address"
+        )));
+    }
+    if let MetricsCa::Pem { pem } = &e.ca {
+        if pem_has_private_key(pem) {
+            return Err(invalid(ltext!(
+                "errors.connections.pemContainsKey",
+                "A certificate field contains a private key — put the key into the key field"
+            )));
+        }
+        if pem_certificates(pem).is_err() {
+            return Err(invalid(ltext!(
+                "errors.connections.caPemInvalid",
+                "The CA certificate must be in PEM format (-----BEGIN CERTIFICATE-----)"
+            )));
+        }
+    }
+    Ok(())
+}
+
 /// Exec-плагин: абсолютный путь, команда из списка разрешённых, известная версия API,
 /// корректные имена переменных окружения и ни одной запрещённой (`auth::exec::is_forbidden_env`).
 fn validate_exec(e: &ExecAuth) -> AppResult<()> {
@@ -558,6 +675,7 @@ pub fn sample_profile(id: &str) -> ConnectionProfile {
         order: 0,
         llm_access: LlmAccess::Off,
         llm_namespaces: None,
+        metrics: MetricsConfig::Auto,
         extra: Default::default(),
     }
 }
@@ -623,6 +741,126 @@ mod tests {
         );
     }
 
+    #[test]
+    fn metrics_source_is_auto_by_default_and_written_only_when_set() {
+        let v = serde_json::to_value(sample_profile("a")).unwrap();
+        assert!(v.get("metrics").is_none(), "{v}");
+        let mut v = v;
+        v["metrics"] = serde_json::json!({
+            "mode": "thanos", "url": " https://thanos.example.com/ ", "auth": "bearer",
+            "ca": {"mode": "pem", "pem": TEST_CERT_PEM}
+        });
+        let mut p: ConnectionProfile = serde_json::from_value(v).unwrap();
+        normalize_profile(&mut p);
+        let MetricsConfig::Thanos(e) = &p.metrics else {
+            panic!("{:?}", p.metrics)
+        };
+        assert_eq!(
+            (e.url.as_str(), e.auth),
+            ("https://thanos.example.com", MetricsAuth::Bearer)
+        );
+        assert!(validate_profile(&p).is_ok());
+        let back = serde_json::to_value(&p).unwrap();
+        assert_eq!(back["metrics"]["mode"], "thanos");
+        assert_eq!(back["metrics"]["ca"]["mode"], "pem");
+        let p: ConnectionProfile = serde_json::from_value(serde_json::json!({
+            "id": "a", "name": "a", "color": "#000", "environment": "dev", "readOnly": false,
+            "apiUrl": "https://api.example.com:6443", "auth": {"method": "token"},
+            "metrics": {"mode": "prometheus", "url": "https://prometheus.example.com"}
+        }))
+        .unwrap();
+        assert_eq!(
+            p.metrics.endpoint().map(|e| (e.auth, e.ca.clone())),
+            Some((MetricsAuth::Connection, MetricsCa::Connection)),
+            "авторизация и CA по умолчанию — как у подключения"
+        );
+    }
+
+    #[test]
+    fn metrics_source_address_is_checked() {
+        let with = |url: &str, auth: MetricsAuth, ca: MetricsCa| {
+            let mut p = sample_profile("a");
+            p.metrics = MetricsConfig::Prometheus(MetricsEndpoint {
+                url: url.into(),
+                auth,
+                ca,
+            });
+            validate_profile(&p).err().map(|e| e.key)
+        };
+        let conn = MetricsCa::Connection;
+        assert_eq!(
+            with(
+                "https://prom.example.com/prefix",
+                MetricsAuth::Connection,
+                conn.clone()
+            ),
+            None
+        );
+        assert_eq!(
+            with("http://localhost:9090", MetricsAuth::None, conn.clone()),
+            None
+        );
+        for (url, auth, key) in [
+            (
+                "http://prom.example.com",
+                MetricsAuth::Bearer,
+                "errors.connections.metricsHttpsRequired",
+            ),
+            (
+                "http://prom.example.com",
+                MetricsAuth::Connection,
+                "errors.connections.metricsHttpsRequired",
+            ),
+            (
+                "https://u:p@prom.example.com",
+                MetricsAuth::None,
+                "errors.connections.credentialsInUrl",
+            ),
+            (
+                "https://prom.example.com/?x=1",
+                MetricsAuth::None,
+                "errors.connections.badMetricsUrl",
+            ),
+            (
+                "ftp://prom.example.com",
+                MetricsAuth::None,
+                "errors.connections.badMetricsUrl",
+            ),
+            (
+                "prom.example.com",
+                MetricsAuth::None,
+                "errors.connections.badMetricsUrl",
+            ),
+        ] {
+            assert_eq!(with(url, auth, conn.clone()).as_deref(), Some(key), "{url}");
+        }
+        let pem = |pem: &str| MetricsCa::Pem { pem: pem.into() };
+        assert_eq!(
+            with("https://prom.example.com", MetricsAuth::None, pem("nope")).as_deref(),
+            Some("errors.connections.caPemInvalid")
+        );
+        let with_key = format!(
+            "{TEST_CERT_PEM}-----BEGIN PRIVATE KEY-----\nAA==\n-----END PRIVATE KEY-----\n"
+        );
+        assert_eq!(
+            with(
+                "https://prom.example.com",
+                MetricsAuth::None,
+                pem(&with_key)
+            )
+            .as_deref(),
+            Some("errors.connections.pemContainsKey")
+        );
+        let mut p = sample_profile("a");
+        p.metrics = MetricsConfig::Thanos(MetricsEndpoint {
+            url: "https://user:secret@thanos.example.com".into(),
+            auth: MetricsAuth::None,
+            ca: MetricsCa::Connection,
+        });
+        let e = validate_profile(&p).unwrap_err();
+        assert!(!e.message.contains("secret"), "{}", e.message);
+    }
+
     #[test]
     fn json_shape_matches_frontend_contract() {
         let mut p = sample_profile("x");
diff --git a/src/api/types.ts b/src/api/types.ts
index 72640a8..14aa459 100644
--- a/src/api/types.ts
+++ b/src/api/types.ts
@@ -85,8 +85,24 @@ export interface ConnectionProfile {
   llmAccess?: LlmAccess;
   /** Проекты для LLM; null или нет поля — все доступные. */
   llmNamespaces?: string[] | null;
+  /** Источник метрик; локальное поле: экспорт его не пишет. Нет поля — `auto`. */
+  metrics?: MetricsConfig;
 }
 
+/** Источник метрик (спец. §7.1): автопоиск или свой Thanos Querier / Prometheus. */
+export type MetricsConfig = { mode: "auto" } | ({ mode: "thanos" } & MetricsEndpoint) | ({ mode: "prometheus" } & MetricsEndpoint);
+export type MetricsMode = MetricsConfig["mode"];
+
+export interface MetricsEndpoint {
+  url: string;
+  auth: MetricsAuth;
+  ca: MetricsCa;
+}
+
+/** Токен подключения, свой bearer-токен (keychain) или без авторизации. */
+export type MetricsAuth = "connection" | "bearer" | "none";
+export type MetricsCa = { mode: "connection" } | { mode: "pem"; pem: string };
+
 /** Уровень доступа LLM: `logs` — этап O4. */
 export type LlmAccess = "off" | "metadata";
 
@@ -94,6 +110,7 @@ export type LlmAccess = "off" | "metadata";
 export interface SecretsInput {
   token?: string | null;
   clientKey?: string | null;
+  metricsToken?: string | null;
 }
 
 export interface ConnectionFolder {
diff --git a/src/components/connections/ConnectionForm.tsx b/src/components/connections/ConnectionForm.tsx
index 9e23357..ad480c3 100644
--- a/src/components/connections/ConnectionForm.tsx
+++ b/src/components/connections/ConnectionForm.tsx
@@ -2,14 +2,14 @@ import { useMemo, useState } from "react";
 import { useTranslation } from "react-i18next";
 import { useQuery, useQueryClient } from "@tanstack/react-query";
 import { api, toAppError } from "../../api/client";
-import type { AppError, AuthConfig, AuthMethod, CaMode, ConnectionProfile, Environment, ExecAuth, LlmAccess, ProxyMode, SecretsInput, TestReport } from "../../api/types";
+import type { AppError, AuthConfig, AuthMethod, CaMode, ConnectionProfile, Environment, ExecAuth, LlmAccess, MetricsAuth, MetricsConfig, MetricsMode, ProxyMode, SecretsInput, TestReport } from "../../api/types";
 import { tr } from "../../i18n/text";
 import { restartSessions } from "../../lib/afterSignIn";
 import { useWorkspace } from "../../state/workspace";
 import { ErrorBanner } from "../common/ErrorBanner";
 import { Hint } from "../common/Hint";
 import { formatArgs, formatEnv, hasSecretValues, launchKey, launchLine, maskArgs, maskEnv, needsExecConfirmation, newExec, parseArgs, parseEnv } from "./exec";
-import { formatNamespaces, parseNamespaces, withAuthMethod, withCaMode, withProxyMode } from "./profile";
+import { formatNamespaces, newMetricsEndpoint, parseNamespaces, withAuthMethod, withCaMode, withMetricsMode, withProxyMode } from "./profile";
 import { ProjectListInput } from "./ProjectListInput";
 import { folderPaths } from "./tree/folderTree";
 
@@ -18,6 +18,8 @@ const AUTH_METHODS: AuthMethod[] = ["browser", "token", "clientCert", "exec"];
 const CA_MODES: CaMode[] = ["system", "pem", "insecure"];
 const PROXY_MODES: ProxyMode[] = ["system", "none", "url"];
 const LLM_ACCESS: LlmAccess[] = ["off", "metadata"];
+const METRICS_MODES: MetricsMode[] = ["auto", "thanos", "prometheus"];
+const METRICS_AUTH: MetricsAuth[] = ["connection", "bearer", "none"];
 
 export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile; tabId: string }) {
   const { t } = useTranslation();
@@ -25,6 +27,7 @@ export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile;
   // Секреты из формы: пустое поле — оставить сохранённый (у нового подключения его нет).
   const [token, setToken] = useState("");
   const [clientKey, setClientKey] = useState("");
+  const [metricsToken, setMetricsToken] = useState("");
   const [favorites, setFavorites] = useState(formatNamespaces(initial.favoriteNamespaces));
   const [manual, setManual] = useState(formatNamespaces(initial.manualNamespaces));
   // Проекты для LLM: «все доступные» (null) или список; список не теряется при переключении туда и обратно.
@@ -34,6 +37,7 @@ export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile;
   const [lastCert, setLastCert] = useState(initial.auth.method === "clientCert" ? initial.auth.certificatePem : "");
   const [lastCa, setLastCa] = useState(initial.ca.mode === "pem" ? initial.ca.pem : "");
   const [lastProxy, setLastProxy] = useState(initial.proxy.mode === "url" ? initial.proxy.url : "");
+  const [lastMetrics, setLastMetrics] = useState(initial.metrics && initial.metrics.mode !== "auto" ? initial.metrics : newMetricsEndpoint());
   // Exec-плагин: аргументы и переменные редактируются текстом, по строке на значение.
   const initialExec: ExecAuth = initial.auth.method === "exec" ? initial.auth : newExec();
   const [lastExec, setLastExec] = useState(initialExec);
@@ -90,6 +94,8 @@ export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile;
   const secrets = (): SecretsInput => ({
     token: profile.auth.method === "token" && token.trim() ? token : null,
     clientKey: profile.auth.method === "clientCert" && clientKey.trim() ? clientKey : null,
+    // Без своего источника токена метрик нет: Rust удаляет сохранённый сам.
+    ...(metrics.mode !== "auto" && { metricsToken: metrics.auth === "bearer" && metricsToken.trim() ? metricsToken : null }),
   });
   // Строку запуска подтверждает человек: новая, изменённая или неподтверждённая требует галочки.
   const execKey = profile.auth.method === "exec" ? launchKey(currentAuth() as ExecAuth) : null;
@@ -164,6 +170,10 @@ export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile;
   const auth = profile.auth;
   const ca = profile.ca;
   const proxy = profile.proxy;
+  const metrics: MetricsConfig = profile.metrics ?? { mode: "auto" };
+  const updateMetrics = (patch: Partial<Omit<Extract<MetricsConfig, { mode: "thanos" }>, "mode">>) => {
+    if (metrics.mode !== "auto") update({ metrics: { ...metrics, ...patch } });
+  };
 
   return (
     <div className="page">
@@ -442,6 +452,83 @@ export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile;
           onChange={(e) => update({ connectTimeoutMs: Number(e.target.value) })}
         />
 
+        <span className="field-label">
+          <label htmlFor="cf-metrics">{t("connections.form.metrics")}</label>
+          <Hint k="hints.connections.metrics" label={t("connections.form.metrics")} />
+        </span>
+        <select
+          id="cf-metrics"
+          value={metrics.mode}
+          onChange={(e) => {
+            if (metrics.mode !== "auto") setLastMetrics(metrics);
+            update({ metrics: withMetricsMode(metrics, e.target.value as MetricsMode, lastMetrics) });
+          }}
+        >
+          {METRICS_MODES.map((m) => (
+            <option key={m} value={m}>
+              {t(`connections.form.metricsMode.${m}`)}
+            </option>
+          ))}
+        </select>
+        {metrics.mode !== "auto" && (
+          <>
+            <label htmlFor="cf-metrics-url">{t("connections.form.metricsUrl")}</label>
+            <input
+              id="cf-metrics-url"
+              placeholder={metrics.mode === "thanos" ? "https://thanos-querier.apps.example.com" : "https://prometheus.example.com"}
+              value={metrics.url}
+              onChange={(e) => updateMetrics({ url: e.target.value })}
+            />
+            <span className="field-label">
+              <label htmlFor="cf-metrics-auth">{t("connections.form.metricsAuth")}</label>
+              <Hint k="hints.connections.metricsAuth" label={t("connections.form.metricsAuth")} />
+            </span>
+            <select id="cf-metrics-auth" value={metrics.auth} onChange={(e) => updateMetrics({ auth: e.target.value as MetricsAuth })}>
+              {METRICS_AUTH.map((a) => (
+                <option key={a} value={a}>
+                  {t(`connections.form.metricsAuthMode.${a}`)}
+                </option>
+              ))}
+            </select>
+            {metrics.auth === "bearer" && (
+              <>
+                <label htmlFor="cf-metrics-token">{t("connections.form.metricsToken")}</label>
+                <input
+                  id="cf-metrics-token"
+                  type="password"
+                  autoComplete="off"
+                  placeholder={keepPlaceholder}
+                  value={metricsToken}
+                  onChange={(e) => setMetricsToken(e.target.value)}
+                />
+              </>
+            )}
+            <label htmlFor="cf-metrics-ca">{t("connections.form.metricsCa")}</label>
+            <select
+              id="cf-metrics-ca"
+              value={metrics.ca.mode}
+              onChange={(e) =>
+                updateMetrics({
+                  ca: e.target.value === "pem" ? { mode: "pem", pem: lastMetrics.ca.mode === "pem" ? lastMetrics.ca.pem : "" } : { mode: "connection" },
+                })
+              }
+            >
+              <option value="connection">{t("connections.form.metricsCaMode.connection")}</option>
+              <option value="pem">{t("connections.form.metricsCaMode.pem")}</option>
+            </select>
+            {metrics.ca.mode === "pem" && (
+              <textarea
+                className="full"
+                aria-label={t("connections.form.metricsCaPem")}
+                rows={4}
+                placeholder="-----BEGIN CERTIFICATE-----"
+                value={metrics.ca.pem}
+                onChange={(e) => updateMetrics({ ca: { mode: "pem", pem: e.target.value } })}
+              />
+            )}
+          </>
+        )}
+
         <span className="field-label">
           <label htmlFor="cf-llm">{t("connections.form.llmAccess")}</label>
           <Hint k="hints.connections.llmAccess" label={t("connections.form.llmAccess")} />
diff --git a/src/components/connections/profile.ts b/src/components/connections/profile.ts
index 543a625..8a29fb4 100644
--- a/src/components/connections/profile.ts
+++ b/src/components/connections/profile.ts
@@ -1,4 +1,4 @@
-import type { AuthConfig, AuthMethod, CaConfig, CaMode, ConnectionProfile, ExecAuth, ProxyConfig, ProxyMode } from "../../api/types";
+import type { AuthConfig, AuthMethod, CaConfig, CaMode, ConnectionProfile, ExecAuth, MetricsConfig, MetricsEndpoint, MetricsMode, ProxyConfig, ProxyMode } from "../../api/types";
 import { newExec } from "./exec";
 
 export function newProfile(): ConnectionProfile {
@@ -19,9 +19,21 @@ export function newProfile(): ConnectionProfile {
     connectTimeoutMs: 10000,
     llmAccess: "off",
     llmNamespaces: null,
+    metrics: { mode: "auto" },
   };
 }
 
+export function newMetricsEndpoint(): MetricsEndpoint {
+  return { url: "", auth: "connection", ca: { mode: "connection" } };
+}
+
+/** Источник метрик; адрес, авторизация и CA сохраняются при переключении между своими источниками и «Автоматически». */
+export function withMetricsMode(metrics: MetricsConfig, mode: MetricsMode, last: MetricsEndpoint = newMetricsEndpoint()): MetricsConfig {
+  if (mode === "auto") return { mode };
+  const { url, auth, ca } = metrics.mode === "auto" ? last : metrics;
+  return { mode, url, auth, ca };
+}
+
 /** Способ входа; сертификат и exec-плагин сохраняются при переключении туда и обратно. */
 export function withAuthMethod(auth: AuthConfig, method: AuthMethod, lastCertificate = "", lastExec: ExecAuth = newExec()): AuthConfig {
   if (method === "clientCert") return { method, certificatePem: auth.method === "clientCert" ? auth.certificatePem : lastCertificate };
````

<!-- block: locales t02-locales step=2.3 -->
````json
{
  "en": {
    "errors": {
      "connections": {
        "badMetricsUrl": "Invalid metrics source address “{{url}}”: use https://host[:port][/path]",
        "metricsHttpsRequired": "A metrics source with a token needs an https:// address"
      }
    },
    "hints": {
      "connections": {
        "metrics": "Automatic: Thanos Querier on OpenShift, Prometheus through the API server elsewhere, then metrics.k8s.io. Own: a Thanos or Prometheus address. Not exported.",
        "metricsAuth": "Connection token — the token of this connection. Own token — saved in the system secret store. A token is sent only over https."
      }
    },
    "connections": {
      "form": {
        "metrics": "Metrics",
        "metricsMode": {
          "auto": "Automatic",
          "thanos": "Own Thanos Querier",
          "prometheus": "Own Prometheus"
        },
        "metricsUrl": "Source address",
        "metricsAuth": "Source authorization",
        "metricsAuthMode": {
          "connection": "Connection token",
          "bearer": "Own token",
          "none": "No authorization"
        },
        "metricsToken": "Source token",
        "metricsCa": "Source CA",
        "metricsCaMode": {
          "connection": "As the connection",
          "pem": "Certificates (PEM)"
        },
        "metricsCaPem": "Source CA certificates (PEM)"
      }
    }
  },
  "ru": {
    "errors": {
      "connections": {
        "badMetricsUrl": "Неверный адрес источника метрик «{{url}}»: используйте https://хост[:порт][/путь]",
        "metricsHttpsRequired": "Источнику метрик с токеном нужен адрес https://"
      }
    },
    "hints": {
      "connections": {
        "metrics": "Автоматически: Thanos Querier у OpenShift, Prometheus через API-сервер у остальных, затем metrics.k8s.io. Свой — адрес Thanos или Prometheus. В экспорт не попадает.",
        "metricsAuth": "Токен подключения — токен этого подключения. Свой токен хранится в системном хранилище секретов. Токен уходит только по https."
      }
    },
    "connections": {
      "form": {
        "metrics": "Метрики",
        "metricsMode": {
          "auto": "Автоматически",
          "thanos": "Свой Thanos Querier",
          "prometheus": "Свой Prometheus"
        },
        "metricsUrl": "Адрес источника",
        "metricsAuth": "Авторизация источника",
        "metricsAuthMode": {
          "connection": "Токен подключения",
          "bearer": "Свой токен",
          "none": "Без авторизации"
        },
        "metricsToken": "Токен источника",
        "metricsCa": "CA источника",
        "metricsCaMode": {
          "connection": "Как у подключения",
          "pem": "Сертификаты (PEM)"
        },
        "metricsCaPem": "Сертификаты CA источника (PEM)"
      }
    }
  }
}
````

- [ ] **Step 4: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 587 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm test`  
Expected: PASS — `Test Files 63 passed`, `Tests 477 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 164`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 5: Commit.**

```bash
git add \
  src-tauri/src/commands/connections.rs \
  src-tauri/src/commands/kubeconfig.rs \
  src-tauri/src/k8s/kubeconfig.rs \
  src-tauri/src/secrets.rs \
  src-tauri/src/storage/exchange.rs \
  src-tauri/src/storage/profile.rs \
  src/api/types.ts \
  src/components/connections/ConnectionForm.test.tsx \
  src/components/connections/ConnectionForm.tsx \
  src/components/connections/profile.test.ts \
  src/components/connections/profile.ts \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "feat(connections): metrics source of a connection — automatic, own Thanos Querier or Prometheus with its authorization and CA; a local setting that export does not write" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: PromQL-шаблоны статистики workload'а и проекта: худший под за окно, поды по владельцам kube-state-metrics или по имени, экранирование меток

**Files:**
- Create: `src-tauri/src/metrics/mod.rs`, `src-tauri/src/metrics/promql.rs`
- Modify: `src-tauri/src/lib.rs`

**Interfaces:**
- Consumes: O1b: `k8s::workloads::WorkloadKind`.
- Produces: `metrics::promql::{quote, re_escape, window(days), chart_step_seconds(days), STEP = "5m", STEP_SECONDS = 300, Pods::{Owner, Name}, Pods::namespace, pod_name_regex, owned_pods, owner_metrics_present, workload_cpu, workload_memory, workload_throttling, workload_restarts, workload_coverage, workload_cpu_series, workload_memory_series, workload_events_series, pod_now, project_cpu_p95, project_memory_max, project_throttling_mean, project_oom_count}`; модуль `metrics` в `lib.rs`.

Чистые функции (спец. §7.2): перцентили считаются подзапросами `[Nd:5m]` по каждому поду, затем `max by (container)` — «худший под»; показатели одного запроса различаются меткой `stat` (`label_replace`, объединение `or`). Поды workload'а — по `kube_pod_owner` (у Deployment — через `kube_replicaset_owner`, у DeploymentConfig — через ReplicationController) либо, без kube-state-metrics, регэкспом имени по виду workload'а. Значения меток экранируются (`quote`, `re_escape`): тест с кавычками, обратной косой чертой и `.*` в имени доказывает, что выйти из строки нельзя. Шаг графика — окно / 200, не меньше 5 минут.

- [ ] **Step 1: Тесты.**

<!-- block: file t03-src-tauri-src-metrics-promql-rs-tests step=3.1 path=src-tauri/src/metrics/promql.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;

    fn owner(kind: WorkloadKind, name: &str) -> Pods {
        Pods::Owner {
            namespace: "team-a".into(),
            kind,
            name: name.into(),
        }
    }

    fn by_name(kind: WorkloadKind, name: &str) -> Pods {
        Pods::Name {
            namespace: "team-a".into(),
            kind,
            name: name.into(),
        }
    }

    #[test]
    fn values_are_quoted_and_escaped() {
        assert_eq!(quote("web"), "\"web\"");
        assert_eq!(quote("a\"b\\c\nd"), "\"a\\\"b\\\\c\\nd\"");
        assert_eq!(re_escape("web.v2"), "web\\.v2");
        assert_eq!(re_escape("a+b(c)|[d]"), "a\\+b\\(c\\)\\|\\[d\\]");
        // Точка в имени — литерал и в RE2, и в строке PromQL.
        assert_eq!(
            pod_name_regex(WorkloadKind::StatefulSet, "db.v1"),
            "db\\.v1-[0-9]+"
        );
        assert!(
            by_name(WorkloadKind::StatefulSet, "db.v1")
                .matchers()
                .contains("pod=~\"db\\\\.v1-[0-9]+\"")
        );
    }

    #[test]
    fn a_name_cannot_add_its_own_conditions() {
        let evil = "x\"} or vector(1) or up{a=\"";
        for q in [
            workload_cpu(&owner(WorkloadKind::Deployment, evil), 7),
            workload_cpu(&by_name(WorkloadKind::Deployment, evil), 7),
            project_cpu_p95(evil, 1),
        ] {
            // Кавычка из имени экранирована: строка метки не закрывается раньше времени.
            assert!(!q.contains("\"x\"}"), "{q}");
        }
        assert!(
            owned_pods("team-a", WorkloadKind::Job, evil)
                .contains("owner_name=\"x\\\"} or vector(1) or up{a=\\\"\"")
        );
        assert!(
            by_name(WorkloadKind::Job, evil)
                .matchers()
                .contains("pod=~\"x\\\"\\\\} or vector\\\\(1\\\\)")
        );
    }

    #[test]
    fn pod_names_follow_the_workload_kind() {
        let g = GENERATED;
        for (kind, expected) in [
            (
                WorkloadKind::Deployment,
                format!("web-{g}{{5,10}}-{g}{{5}}"),
            ),
            (WorkloadKind::StatefulSet, "web-[0-9]+".to_string()),
            (WorkloadKind::DaemonSet, format!("web-{g}{{5}}")),
            (WorkloadKind::Job, format!("web-{g}{{5}}")),
            (WorkloadKind::CronJob, format!("web-[0-9]+-{g}{{5}}")),
            (
                WorkloadKind::DeploymentConfig,
                format!("web-[0-9]+-{g}{{5}}"),
            ),
        ] {
            assert_eq!(pod_name_regex(kind, "web"), expected, "{kind:?}");
        }
        let re = regex::Regex::new(&format!(
            "^(?:{})$",
            pod_name_regex(WorkloadKind::Deployment, "web")
        ))
        .unwrap();
        assert!(re.is_match("web-7d9f8c6b5d-x2x4z"));
        assert!(
            !re.is_match("web-api-7d9f8c6b5d-x2x4z"),
            "соседи не попадают"
        );
        let dc = regex::Regex::new(&format!(
            "^(?:{})$",
            pod_name_regex(WorkloadKind::DeploymentConfig, "legacy")
        ))
        .unwrap();
        assert!(dc.is_match("legacy-3-b2c4d"));
        assert!(
            !dc.is_match("legacy-3-deploy"),
            "под выкладки — не приложение"
        );
    }

    #[test]
    fn owners_go_through_their_intermediate_objects() {
        let d = owned_pods("team-a", WorkloadKind::Deployment, "web");
        assert_eq!(
            d,
            "max by (namespace, pod) (kube_pod_owner{namespace=\"team-a\", owner_kind=\"ReplicaSet\"} \
             * on (namespace, owner_name) group_left () max by (namespace, owner_name) \
             (label_replace(kube_replicaset_owner{namespace=\"team-a\", owner_kind=\"Deployment\", owner_name=\"web\"}, \
             \"owner_name\", \"$1\", \"replicaset\", \"(.*)\")))"
        );
        assert!(owned_pods("team-a", WorkloadKind::CronJob, "report")
            .contains("kube_job_owner{namespace=\"team-a\", owner_kind=\"CronJob\", owner_name=\"report\"}"));
        assert!(owned_pods("team-a", WorkloadKind::DeploymentConfig, "legacy")
            .contains("kube_replicationcontroller_owner{namespace=\"team-a\", owner_kind=\"DeploymentConfig\""));
        assert_eq!(
            owned_pods("team-a", WorkloadKind::StatefulSet, "db"),
            "max by (namespace, pod) (kube_pod_owner{namespace=\"team-a\", owner_kind=\"StatefulSet\", owner_name=\"db\"})"
        );
        assert_eq!(
            owner_metrics_present("team-a"),
            "count(kube_pod_owner{namespace=\"team-a\"})"
        );
    }

    #[test]
    fn workload_statistics_take_the_worst_pod_over_the_window() {
        let q = workload_cpu(&by_name(WorkloadKind::StatefulSet, "db"), 7);
        let cpu = "max by (namespace, pod, container) (rate(container_cpu_usage_seconds_total{namespace=\"team-a\", pod=~\"db-[0-9]+\", container!=\"\", container!=\"POD\"}[5m]))";
        assert_eq!(
            q,
            format!(
                "label_replace(max by (container) (quantile_over_time(0.5, ({cpu})[7d:5m])), \"stat\", \"cpu_p50\", \"\", \"\") \
                 or label_replace(max by (container) (quantile_over_time(0.95, ({cpu})[7d:5m])), \"stat\", \"cpu_p95\", \"\", \"\") \
                 or label_replace(max by (container) (quantile_over_time(0.99, ({cpu})[7d:5m])), \"stat\", \"cpu_p99\", \"\", \"\") \
                 or label_replace(max by (container) (max_over_time(({cpu})[7d:5m])), \"stat\", \"cpu_max\", \"\", \"\")"
            )
        );
        // С kube-state-metrics поды выбираются внутри подзапроса, на каждом шаге окна.
        let q = workload_memory(&owner(WorkloadKind::StatefulSet, "db"), 14);
        assert!(q.contains(
            "quantile_over_time(0.99, ((max by (namespace, pod, container) (container_memory_working_set_bytes{namespace=\"team-a\", container!=\"\", container!=\"POD\"}) \
             * on (namespace, pod) group_left () max by (namespace, pod) (kube_pod_owner{namespace=\"team-a\", owner_kind=\"StatefulSet\", owner_name=\"db\"})))[14d:5m])"
        ), "{q}");
        for (q, stats) in [
            (
                workload_memory(&owner(WorkloadKind::Job, "j"), 1),
                &["memory_p95", "memory_p99", "memory_max"][..],
            ),
            (
                workload_throttling(&owner(WorkloadKind::Job, "j"), 1),
                &["throttling_mean", "throttling_p95"][..],
            ),
            (
                workload_restarts(&owner(WorkloadKind::Job, "j"), 1),
                &["oom_count", "oom_last", "restarts"][..],
            ),
            (
                workload_coverage(&owner(WorkloadKind::Job, "j"), 1),
                &["steps", "first"][..],
            ),
        ] {
            for s in stats {
                assert!(q.contains(&format!("\"stat\", \"{s}\"")), "{s}: {q}");
            }
            assert_eq!(
                q.matches(" or label_replace(").count(),
                stats.len() - 1,
                "{q}"
            );
        }
        assert!(workload_throttling(&owner(WorkloadKind::Job, "j"), 1)
            .contains("/ (rate(container_cpu_cfs_periods_total{namespace=\"team-a\", container!=\"\", container!=\"POD\"}[5m]) > 0)"));
        assert!(workload_restarts(&owner(WorkloadKind::Job, "j"), 30)
            .contains("kube_pod_container_status_last_terminated_reason{namespace=\"team-a\", reason=\"OOMKilled\"}"));
    }

    #[test]
    fn charts_have_at_most_two_hundred_points_and_a_five_minute_step() {
        assert_eq!(chart_step_seconds(7), 3024);
        assert_eq!(chart_step_seconds(1), 432);
        assert_eq!(chart_step_seconds(30), 12_960);
        for days in [1, 7, 14, 30] {
            let step = chart_step_seconds(days);
            assert!(step >= STEP_SECONDS && u64::from(days) * 86_400 / step <= CHART_POINTS);
        }
        let p = by_name(WorkloadKind::Deployment, "web");
        assert!(
            workload_cpu_series(&p)
                .starts_with("max by (container) (max by (namespace, pod, container) (rate(")
        );
        let e = workload_events_series(&p, 3024);
        assert!(
            e.contains("[3024s]")
                && e.contains("\"event\", \"restart\"")
                && e.contains("\"event\", \"oom\""),
            "{e}"
        );
    }

    #[test]
    fn project_queries_group_by_pod_and_container() {
        assert_eq!(
            project_cpu_p95("team-a", 1),
            "quantile_over_time(0.95, (max by (pod, container) (rate(container_cpu_usage_seconds_total{namespace=\"team-a\", container!=\"\", container!=\"POD\"}[5m])))[1d:5m])"
        );
        assert_eq!(
            project_memory_max("team-a", 7),
            "max_over_time((max by (pod, container) (container_memory_working_set_bytes{namespace=\"team-a\", container!=\"\", container!=\"POD\"}))[7d:5m])"
        );
        for q in [
            project_throttling_mean("team-a", 1),
            project_oom_count("team-a", 1),
        ] {
            assert!(q.contains("max by (pod, container)"), "{q}");
            assert!(
                !q.contains(
                    "max by (namespace, pod, container) (kube_pod_container_status_restarts_total"
                ),
                "{q}"
            );
        }
    }
}
````

- [ ] **Step 2: Реализация.**

<!-- block: file t03-src-tauri-src-metrics-mod-rs step=3.2 path=src-tauri/src/metrics/mod.rs -->
````rust
//! Метрики потребления ресурсов (спец. §7.1, §7.2): PromQL-шаблоны — чистые функции.
//! Источники метрик (Thanos, Prometheus, `metrics.k8s.io`) и их выбор — задача 4 плана O3.

pub mod promql;
````

<!-- block: file t03-src-tauri-src-metrics-promql-rs-impl step=3.2 path=src-tauri/src/metrics/promql.rs part=impl -->
````rust
//! PromQL-шаблоны анализа ресурсов (спец. §7.2) — чистые функции: строка запроса из проекта,
//! workload'а и окна. Значения меток экранируются (`quote`), имена в регулярных выражениях —
//! ещё и как RE2 (`re_escape`): имя объекта не может дописать к запросу свои условия.
//!
//! Статистика — instant-запросы с подзапросом `[окно:5m]`: перцентиль по каждому поду за окно,
//! затем максимум по подам («худший под»). Несколько показателей одного семейства идут одним
//! запросом, у каждого ряда — метка `stat` (`label_replace`). Поды workload'а выбираются внутри
//! подзапроса, на каждом шаге: так в окно попадают и поды, удалённые до его конца.

use crate::k8s::workloads::WorkloadKind;

/// Шаг подзапросов и окно `rate`.
pub const STEP: &str = "5m";
/// Шаг подзапросов в секундах.
pub const STEP_SECONDS: u64 = 300;
/// Точек на графике окна (спец. §7.2): шаг графика — окно / 200, но не меньше `STEP`.
pub const CHART_POINTS: u64 = 200;

/// Значение в кавычках PromQL: `\`, `"` и переводы строк экранируются.
pub fn quote(v: &str) -> String {
    let mut out = String::with_capacity(v.len() + 2);
    out.push('"');
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Литерал для регулярного выражения RE2: метасимволы — через `\`.
pub fn re_escape(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        if "\\.+*?()|[]{}^$".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Окно запроса: `7d`.
pub fn window(days: u32) -> String {
    format!("{days}d")
}

/// Шаг графика окна в секундах: окно / 200, не меньше 5 минут (спец. §7.2, §10.7).
pub fn chart_step_seconds(days: u32) -> u64 {
    (u64::from(days) * 86_400 / CHART_POINTS).max(STEP_SECONDS)
}

/// Алфавит случайных частей имён Kubernetes (`rand.String`): согласные и цифры без 0, 1 и 3.
const GENERATED: &str = "[bcdfghjklmnpqrstvwxz2456789]";

/// Поды workload'а в запросе.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pods {
    /// Через kube-state-metrics (`kube_pod_owner` и владельцы ReplicaSet, Job,
    /// ReplicationController): точный выбор, в том числе подов индексированных Job.
    Owner {
        namespace: String,
        kind: WorkloadKind,
        name: String,
    },
    /// Без kube-state-metrics — по имени пода, которое порождает workload этого вида
    /// (`web-<hash>-<5>`, `db-<n>`…).
    Name {
        namespace: String,
        kind: WorkloadKind,
        name: String,
    },
}

impl Pods {
    pub fn namespace(&self) -> &str {
        match self {
            Pods::Owner { namespace, .. } | Pods::Name { namespace, .. } => namespace,
        }
    }

    /// Матчеры метрики: проект и, без kube-state-metrics, имя пода.
    fn matchers(&self) -> String {
        let ns = format!("namespace={}", quote(self.namespace()));
        match self {
            Pods::Owner { .. } => ns,
            Pods::Name { kind, name, .. } => {
                format!("{ns}, pod=~{}", quote(&pod_name_regex(*kind, name)))
            }
        }
    }

    /// Ограничение выражения с метками `namespace`, `pod` подами workload'а: умножение на
    /// единицы `owned_pods` (kube-state-metrics) или ничего (имя уже в матчерах).
    fn join(&self, expr: &str) -> String {
        match self {
            Pods::Owner {
                namespace,
                kind,
                name,
            } => format!(
                "({expr} * on (namespace, pod) group_left () {})",
                owned_pods(namespace, *kind, name)
            ),
            Pods::Name { .. } => expr.to_string(),
        }
    }
}

/// Регулярное выражение имени пода workload'а — те же формы, по которым находятся дети
/// workload'а в событиях (`k8s::events`). У DeploymentConfig поды выкладки и хуков
/// (`<dc>-<n>-deploy`, `-hook-pre`) не считаются: это не приложение.
pub fn pod_name_regex(kind: WorkloadKind, name: &str) -> String {
    let w = re_escape(name);
    let g = GENERATED;
    match kind {
        WorkloadKind::Deployment => format!("{w}-{g}{{5,10}}-{g}{{5}}"),
        WorkloadKind::StatefulSet => format!("{w}-[0-9]+"),
        WorkloadKind::DaemonSet | WorkloadKind::Job => format!("{w}-{g}{{5}}"),
        WorkloadKind::CronJob | WorkloadKind::DeploymentConfig => {
            format!("{w}-[0-9]+-{g}{{5}}")
        }
    }
}

/// Поды workload'а по kube-state-metrics: ряд со значением 1 и метками `namespace`, `pod`.
/// Deployment — через ReplicaSet (`kube_replicaset_owner`), CronJob — через Job
/// (`kube_job_owner`), DeploymentConfig — через ReplicationController
/// (`kube_replicationcontroller_owner`); `label_replace` кладёт имя посредника в `owner_name`.
pub fn owned_pods(namespace: &str, kind: WorkloadKind, name: &str) -> String {
    let ns = quote(namespace);
    let owner = quote(name);
    let via = |middle: &str, metric: &str, label: &str, parent: &str| {
        format!(
            "max by (namespace, pod) (kube_pod_owner{{namespace={ns}, owner_kind=\"{middle}\"}} \
             * on (namespace, owner_name) group_left () max by (namespace, owner_name) \
             (label_replace({metric}{{namespace={ns}, owner_kind=\"{parent}\", owner_name={owner}}}, \
             \"owner_name\", \"$1\", \"{label}\", \"(.*)\")))"
        )
    };
    match kind {
        WorkloadKind::Deployment => via(
            "ReplicaSet",
            "kube_replicaset_owner",
            "replicaset",
            "Deployment",
        ),
        WorkloadKind::CronJob => via("Job", "kube_job_owner", "job_name", "CronJob"),
        WorkloadKind::DeploymentConfig => via(
            "ReplicationController",
            "kube_replicationcontroller_owner",
            "replicationcontroller",
            "DeploymentConfig",
        ),
        WorkloadKind::StatefulSet | WorkloadKind::DaemonSet | WorkloadKind::Job => format!(
            "max by (namespace, pod) (kube_pod_owner{{namespace={ns}, owner_kind=\"{}\", owner_name={owner}}})",
            kind_name(kind)
        ),
    }
}

fn kind_name(kind: WorkloadKind) -> &'static str {
    match kind {
        WorkloadKind::Deployment => "Deployment",
        WorkloadKind::StatefulSet => "StatefulSet",
        WorkloadKind::DaemonSet => "DaemonSet",
        WorkloadKind::DeploymentConfig => "DeploymentConfig",
        WorkloadKind::Job => "Job",
        WorkloadKind::CronJob => "CronJob",
    }
}

/// Есть ли у проекта метрики kube-state-metrics: от него зависит выбор подов (`Pods`).
pub fn owner_metrics_present(namespace: &str) -> String {
    format!("count(kube_pod_owner{{namespace={}}})", quote(namespace))
}

/// Контейнеры приложения: без пустого имени и без `POD` (pause-контейнер).
const CONTAINERS: &str = "container!=\"\", container!=\"POD\"";

/// Потребление CPU в ядрах по поду и контейнеру (`rate` за 5 минут).
fn cpu(p: &Pods) -> String {
    p.join(&format!(
        "max by (namespace, pod, container) (rate(container_cpu_usage_seconds_total{{{}, {CONTAINERS}}}[{STEP}]))",
        p.matchers()
    ))
}

/// Рабочий набор памяти в байтах по поду и контейнеру.
fn memory(p: &Pods) -> String {
    p.join(&format!(
        "max by (namespace, pod, container) (container_memory_working_set_bytes{{{}, {CONTAINERS}}})",
        p.matchers()
    ))
}

/// Доля периодов CFS с троттлингом (0–1); периоды без работы не считаются.
fn throttling(p: &Pods) -> String {
    let m = p.matchers();
    p.join(&format!(
        "max by (namespace, pod, container) (rate(container_cpu_cfs_throttled_periods_total{{{m}, {CONTAINERS}}}[{STEP}]) \
         / (rate(container_cpu_cfs_periods_total{{{m}, {CONTAINERS}}}[{STEP}]) > 0))"
    ))
}

/// Перезапуски контейнеров за `range` (kube-state-metrics).
fn restarts(p: &Pods, range: &str) -> String {
    p.join(&format!(
        "max by (namespace, pod, container) (increase(kube_pod_container_status_restarts_total{{{}}}[{range}]))",
        p.matchers()
    ))
}

/// Перезапуски за `range`, после которых последняя причина завершения — OOMKilled.
fn ooms(p: &Pods, range: &str) -> String {
    format!(
        "({} > 0) and on (namespace, pod, container) \
         (max by (namespace, pod, container) (kube_pod_container_status_last_terminated_reason{{{}, reason=\"OOMKilled\"}}) == 1)",
        restarts(p, range),
        p.matchers()
    )
}

/// Показатель с меткой `stat`.
fn stat(name: &str, expr: &str) -> String {
    format!("label_replace({expr}, \"stat\", \"{name}\", \"\", \"\")")
}

fn either(parts: &[String]) -> String {
    parts.join(" or ")
}

/// Перцентиль по каждому поду за окно, затем максимум по подам — по контейнеру.
fn worst(expr: &str, f: &str, days: u32) -> String {
    format!(
        "max by (container) ({f}(({expr})[{}:{STEP}]))",
        window(days)
    )
}

fn quantile(expr: &str, q: &str, days: u32) -> String {
    format!(
        "max by (container) (quantile_over_time({q}, ({expr})[{}:{STEP}]))",
        window(days)
    )
}

/// CPU: p50, p95, p99 и максимум (ядра).
pub fn workload_cpu(p: &Pods, days: u32) -> String {
    let e = cpu(p);
    either(&[
        stat("cpu_p50", &quantile(&e, "0.5", days)),
        stat("cpu_p95", &quantile(&e, "0.95", days)),
        stat("cpu_p99", &quantile(&e, "0.99", days)),
        stat("cpu_max", &worst(&e, "max_over_time", days)),
    ])
}

/// Память: p95, p99 и максимум (байты).
pub fn workload_memory(p: &Pods, days: u32) -> String {
    let e = memory(p);
    either(&[
        stat("memory_p95", &quantile(&e, "0.95", days)),
        stat("memory_p99", &quantile(&e, "0.99", days)),
        stat("memory_max", &worst(&e, "max_over_time", days)),
    ])
}

/// Троттлинг: среднее и p95 (доля 0–1).
pub fn workload_throttling(p: &Pods, days: u32) -> String {
    let e = throttling(p);
    either(&[
        stat("throttling_mean", &worst(&e, "avg_over_time", days)),
        stat("throttling_p95", &quantile(&e, "0.95", days)),
    ])
}

/// OOM и рестарты по контейнеру (kube-state-metrics): число эпизодов OOM, время последнего
/// (Unix, секунды) и число рестартов за окно.
pub fn workload_restarts(p: &Pods, days: u32) -> String {
    let w = window(days);
    let oom = ooms(p, STEP);
    let restarts = restarts(p, STEP);
    either(&[
        stat(
            "oom_count",
            &format!("sum by (container) (count_over_time(({oom})[{w}:{STEP}]))"),
        ),
        stat(
            "oom_last",
            &format!("max by (container) (max_over_time(timestamp({oom})[{w}:{STEP}]))"),
        ),
        stat(
            "restarts",
            &format!("sum by (container) (sum_over_time(({restarts})[{w}:{STEP}]))"),
        ),
    ])
}

/// Покрытие окна: число шагов с данными хотя бы по одному поду (`steps`) и время первого такого
/// шага (`first`, Unix-секунды).
pub fn workload_coverage(p: &Pods, days: u32) -> String {
    let w = window(days);
    let any = format!("max({})", cpu(p));
    either(&[
        stat("steps", &format!("count_over_time(({any})[{w}:{STEP}])")),
        stat(
            "first",
            &format!("min_over_time(timestamp({any})[{w}:{STEP}])"),
        ),
    ])
}

/// Ряд графика CPU по контейнеру (range-запрос): худший под на каждом шаге.
pub fn workload_cpu_series(p: &Pods) -> String {
    format!("max by (container) ({})", cpu(p))
}

/// Ряд графика памяти по контейнеру.
pub fn workload_memory_series(p: &Pods) -> String {
    format!("max by (container) ({})", memory(p))
}

/// Отметки графика (range-запрос с шагом `step_seconds`): рестарты (`event="restart"`) и
/// OOM (`event="oom"`) по контейнеру за шаг.
pub fn workload_events_series(p: &Pods, step_seconds: u64) -> String {
    let range = format!("{step_seconds}s");
    let label = |expr: String, event: &str| {
        format!("label_replace({expr}, \"event\", \"{event}\", \"\", \"\")")
    };
    either(&[
        label(
            format!("sum by (container) ({} > 0)", restarts(p, &range)),
            "restart",
        ),
        label(format!("count by (container) ({})", ooms(p, &range)), "oom"),
    ])
}

/// Агрегаты проекта по поду и контейнеру (вкладка «Ресурсы» проекта и правила `resources.*`):
/// без выбора workload'а — поды сопоставляет с workload'ами Rust, по имени и владельцу.
fn by_pod(expr: &str) -> String {
    expr.replace(
        "max by (namespace, pod, container)",
        "max by (pod, container)",
    )
}

fn project_matchers(namespace: &str) -> String {
    format!("namespace={}, {CONTAINERS}", quote(namespace))
}

/// p95 CPU по поду и контейнеру за окно (ядра).
pub fn project_cpu_p95(namespace: &str, days: u32) -> String {
    let m = project_matchers(namespace);
    by_pod(&format!(
        "quantile_over_time(0.95, (max by (namespace, pod, container) (rate(container_cpu_usage_seconds_total{{{m}}}[{STEP}])))[{}:{STEP}])",
        window(days)
    ))
}

/// Максимум памяти по поду и контейнеру за окно (байты).
pub fn project_memory_max(namespace: &str, days: u32) -> String {
    let m = project_matchers(namespace);
    by_pod(&format!(
        "max_over_time((max by (namespace, pod, container) (container_memory_working_set_bytes{{{m}}}))[{}:{STEP}])",
        window(days)
    ))
}

/// Средний троттлинг по поду и контейнеру за окно (доля 0–1).
pub fn project_throttling_mean(namespace: &str, days: u32) -> String {
    let m = project_matchers(namespace);
    by_pod(&format!(
        "avg_over_time((max by (namespace, pod, container) (rate(container_cpu_cfs_throttled_periods_total{{{m}}}[{STEP}]) \
         / (rate(container_cpu_cfs_periods_total{{{m}}}[{STEP}]) > 0)))[{}:{STEP}])",
        window(days)
    ))
}

/// Число эпизодов OOM по поду и контейнеру за окно (kube-state-metrics).
pub fn project_oom_count(namespace: &str, days: u32) -> String {
    let ns = quote(namespace);
    by_pod(&format!(
        "count_over_time(((max by (namespace, pod, container) (increase(kube_pod_container_status_restarts_total{{namespace={ns}}}[{STEP}])) > 0) \
         and on (namespace, pod, container) (max by (namespace, pod, container) \
         (kube_pod_container_status_last_terminated_reason{{namespace={ns}, reason=\"OOMKilled\"}}) == 1))[{}:{STEP}])",
        window(days)
    ))
}
````

<!-- block: patch t03-src step=3.2 -->
````diff
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index 74a935c..4df7791 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -10,6 +10,7 @@ pub mod k8s;
 pub mod logging;
 pub mod logs;
 pub mod mcp;
+pub mod metrics;
 pub mod platform;
 pub mod policy;
 pub mod product;
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 594 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 164`

- [ ] **Step 4: Commit.**

```bash
git add \
  src-tauri/src/lib.rs \
  src-tauri/src/metrics/mod.rs \
  src-tauri/src/metrics/promql.rs
git commit -s -m "feat(metrics): PromQL templates for workload and project resource statistics — the worst pod over the window, pods by kube-state-metrics owners or by name, escaped labels" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: Источники метрик: Thanos Querier у OpenShift, Prometheus через прокси API-сервера, свой адрес, metrics.k8s.io; выбор и отвергнутые — в проверке подключения и статусе источника

**Files:**
- Create: `src-tauri/src/commands/resources.rs`, `src-tauri/src/metrics/discovery.rs`, `src-tauri/src/metrics/http.rs`, `src-tauri/src/metrics/pods.rs`, `src-tauri/src/metrics/prometheus.rs`, `src-tauri/src/metrics/select.rs`
- Modify: `src-tauri/src/commands/mod.rs`, `src-tauri/src/error.rs`, `src-tauri/src/k8s/connectivity.rs`, `src-tauri/src/k8s/pool.rs`, `src-tauri/src/k8s/read.rs`, `src-tauri/src/lib.rs`, `src-tauri/src/metrics/mod.rs`, `src-tauri/src/platform/mod.rs`, `src-tauri/tests/it_connect.rs`, `src/api/client.ts`, `src/api/types.ts`, `src/components/connections/ConnectionForm.test.tsx`, `src/components/connections/ConnectionForm.tsx`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задачи 2–3: `MetricsConfig`, `METRICS_TOKEN`, `promql`; O1b: `ReadClient`, `k8s::pool::Connected`, `connectivity::test_connection`, `platform::{PlatformInfo, Distribution}`, `auth::oauth::http_client`.
- Produces: `metrics::prometheus::{PromApi, SourceKind, SourceOrigin, SourceInfo { kind, origin, location, history } + label(), Reason, Failure, MetricsSource { info, tenancy, get }, parse_vector, parse_matrix, query, query_range, probe, reason_text}`; `metrics::http::{HttpSource, client}`; `metrics::discovery::{PROMETHEUS_SERVICES, candidates, ProxySource, THANOS_ROUTE, thanos_url}`; `metrics::pods::{pods_now, installed, info, PodNow, ContainerNow}`; `metrics::select::{Rejected, Mode, SourceStatus { mode, selected, rejected, pod_metrics, checked_at_ms } + unavailable(), Selected { history, status }, Inputs, select, MetricsCache, for_connection(st, id, c, refresh)}`; `ReadClient::prometheus(ServiceProxy, api, params)`; `Connected::{bearer(), metrics}`; `ErrorCode::MetricsUnavailable` (`METRICS_UNAVAILABLE`); шаг `metrics` проверки подключения (`TestStep.notes`); команда `get_metrics_source_status(connection_id, refresh)`; TS: `MetricsSourceStatus`, `api.getMetricsSourceStatus`.

Спец. §7.1. Трейт `MetricsSource` делает только GET к `/api/v1/query` и `/api/v1/query_range`. У OpenShift — Thanos по маршруту `openshift-monitoring/thanos-querier` (адрес — из Route, иначе из домена приложений или хоста OAuth) с токеном подключения; у остальных — кандидаты из `discovery.rs` (сначала дистрибутива) через прокси API-сервера `services/<имя>:<порт>/proxy`; без истории — `metrics.k8s.io`. Свой источник другим не заменяется. Проба — `vector(1)`; причина отказа — код (`forbidden`, `notFound`, `unavailable`, `timeout`, `badResponse`, `tokenMissing`) и текст с именем источника. Выбор хранится в подключении 5 минут; ответы 403 при пробах собираются отдельным `observe` и не сбрасывают кэш прав проекта, в котором идёт чтение. Шаг «Метрики» проверки подключения — предупреждение: без источника итог проверки — успех (форма рисует ⚠️, `it_connect` проверяет первые шесть шагов). Большие future в debug-сборке — в куче (`Box::pin`).

- [ ] **Step 1: Тесты.**

<!-- block: file t04-src-tauri-src-metrics-discovery-rs-tests step=4.1 path=src-tauri/src/metrics/discovery.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use crate::metrics::prometheus::{Reason, probe};

    fn reader(
        handler: impl Fn(&crate::k8s::testing::Seen) -> (u16, serde_json::Value) + Send + 'static,
    ) -> ReadClient {
        ReadClient::new(mock_client(handler).0, Duration::from_secs(5))
    }

    #[test]
    fn distribution_candidates_go_first() {
        let names = |d| {
            candidates(d)
                .iter()
                .map(ServiceProxy::label)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            names(None),
            [
                "monitoring/prometheus-k8s:9090",
                "monitoring/prometheus-operated:9090",
                "observability/prometheus-operated:9090",
                "cattle-monitoring-system/rancher-monitoring-prometheus:9090"
            ]
        );
        assert_eq!(
            names(Some("rancher"))[0],
            "cattle-monitoring-system/rancher-monitoring-prometheus:9090"
        );
        assert_eq!(names(Some("eks")), names(None));
    }

    #[tokio::test]
    async fn prometheus_is_asked_through_the_service_proxy() {
        let r = reader(|s| {
            assert_eq!(
                s.path(),
                "/api/v1/namespaces/monitoring/services/http:prometheus-k8s:9090/proxy/api/v1/query"
            );
            assert!(s.uri.contains("query=vector%281%29"), "{}", s.uri);
            (
                200,
                serde_json::json!({"status": "success", "data": {"resultType": "vector",
                    "result": [{"metric": {}, "value": [1, "1"]}]}}),
            )
        });
        let src = ProxySource::new(r, candidates(None)[0].clone());
        probe(&src, 1).await.unwrap();
        assert_eq!(src.info().location, "monitoring/prometheus-k8s:9090");
    }

    #[tokio::test]
    async fn proxy_answers_become_reasons() {
        for (code, reason) in [
            (403, Reason::Forbidden),
            (404, Reason::NotFound),
            (503, Reason::Unavailable),
        ] {
            let r = reader(move |_| status(code, "X", "no"));
            let src = ProxySource::new(r, candidates(None)[0].clone());
            assert_eq!(probe(&src, 1).await.unwrap_err().reason, reason, "{code}");
        }
        // Ответ Prometheus с ошибкой запроса — не Status: «ответ не тот».
        let r = reader(|_| {
            (
                400,
                serde_json::json!({"status": "error", "errorType": "bad_data", "error": "parse error"}),
            )
        });
        let src = ProxySource::new(r, candidates(None)[0].clone());
        assert_eq!(
            probe(&src, 1).await.unwrap_err().reason,
            Reason::BadResponse
        );
    }

    #[tokio::test]
    async fn thanos_address_from_the_route_the_domain_or_the_oauth_host() {
        let route = reader(|s| match s.path() {
            "/apis/route.openshift.io/v1/namespaces/openshift-monitoring/routes/thanos-querier" => {
                (
                    200,
                    serde_json::json!({"apiVersion": "route.openshift.io/v1", "kind": "Route",
                    "metadata": {"name": "thanos-querier"},
                    "spec": {"host": "thanos-querier-openshift-monitoring.apps.a.example.com"}}),
                )
            }
            _ => status(404, "NotFound", "no"),
        });
        assert_eq!(
            thanos_url(&route).await.unwrap().as_str(),
            "https://thanos-querier-openshift-monitoring.apps.a.example.com/"
        );
        let domain = reader(|s| match s.path() {
            "/apis/config.openshift.io/v1/ingresses/cluster" => (
                200,
                serde_json::json!({"apiVersion": "config.openshift.io/v1", "kind": "Ingress",
                    "metadata": {"name": "cluster"}, "spec": {"domain": "apps.b.example.com"}}),
            ),
            _ => status(403, "Forbidden", "no"),
        });
        assert_eq!(
            thanos_url(&domain).await.unwrap().host_str(),
            Some("thanos-querier-openshift-monitoring.apps.b.example.com")
        );
        let oauth = reader(|s| match s.path() {
            "/.well-known/oauth-authorization-server" => (
                200,
                serde_json::json!({"issuer": "https://oauth-openshift.apps.c.example.com"}),
            ),
            _ => status(403, "Forbidden", "no"),
        });
        assert_eq!(
            thanos_url(&oauth).await.unwrap().host_str(),
            Some("thanos-querier-openshift-monitoring.apps.c.example.com")
        );
        let nothing = reader(|_| status(403, "Forbidden", "no"));
        assert_eq!(
            thanos_url(&nothing).await.unwrap_err().reason,
            Reason::Forbidden
        );
    }
}
````

<!-- block: file t04-src-tauri-src-metrics-http-rs-tests step=4.1 path=src-tauri/src/metrics/http.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::metrics::prometheus::{SourceKind, SourceOrigin, probe, query};
    use crate::storage::profile::sample_profile;

    /// HTTP-сервер для тестов: ответ `(код, тело)` по первой строке запроса; запросы с
    /// заголовками записываются.
    async fn server(
        answer: impl Fn(&str) -> (u16, String) + Send + Sync + 'static,
    ) -> (url::Url, Arc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base =
            url::Url::parse(&format!("http://{}/prefix", listener.local_addr().unwrap())).unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let (answer, seen) = (Arc::new(answer), log.clone());
        tokio::spawn(async move {
            while let Ok((mut s, _)) = listener.accept().await {
                let (answer, seen) = (answer.clone(), seen.clone());
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    while !String::from_utf8_lossy(&buf).contains("\r\n\r\n") {
                        let Ok(n) = s.read(&mut chunk).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    let head = String::from_utf8_lossy(&buf).to_string();
                    let line = head.lines().next().unwrap_or_default().to_string();
                    seen.lock().unwrap().push(head);
                    let (code, body) = answer(&line);
                    let resp = format!(
                        "HTTP/1.1 {code} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(resp.as_bytes()).await;
                });
            }
        });
        (base, log)
    }

    fn source(base: url::Url, token: Option<&str>, tenancy: bool) -> HttpSource {
        let mut p = sample_profile("a");
        p.proxy = crate::storage::profile::ProxyConfig::None;
        HttpSource::new(
            client(&p, &MetricsCa::Connection).unwrap(),
            base,
            token.map(|t| SecretString::from(t.to_string())),
            SourceInfo {
                kind: SourceKind::Thanos,
                origin: SourceOrigin::Profile,
                location: "x".into(),
                history: true,
            },
            tenancy,
        )
    }

    const ONE: &str = r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"1"]}]}}"#;

    #[tokio::test]
    async fn get_goes_to_the_api_path_with_the_token() {
        let (base, log) = server(|_| (200, ONE.into())).await;
        let s = source(base, Some("sha256~secret"), true);
        probe(&s, 5).await.unwrap();
        query(&s, "team-a", "up{a=\"b\"}", 7).await.unwrap();
        let seen = log.lock().unwrap();
        assert!(
            seen[0].starts_with("GET /prefix/api/v1/query?query=vector%281%29&time=5 "),
            "{}",
            seen[0]
        );
        assert!(
            seen[0].contains("authorization: Bearer sha256~secret"),
            "{}",
            seen[0]
        );
        assert!(
            seen[1].starts_with(
                "GET /prefix/api/v1/query?query=up%7Ba%3D%22b%22%7D&time=7&namespace=team-a "
            ),
            "{}",
            seen[1]
        );
        assert!(!format!("{s:?}").contains("secret"), "{s:?}");
    }

    #[tokio::test]
    async fn answers_become_reasons() {
        let (base, _) = server(|line| {
            if line.contains("time=1") {
                (403, "forbidden".into())
            } else if line.contains("time=2") {
                (404, "nope".into())
            } else if line.contains("time=3") {
                (503, "down".into())
            } else {
                (
                    400,
                    r#"{"status":"error","errorType":"bad_data","error":"parse error"}"#.into(),
                )
            }
        })
        .await;
        let s = source(base.clone(), None, false);
        for (t, reason) in [
            (1, Reason::Forbidden),
            (2, Reason::NotFound),
            (3, Reason::Unavailable),
            (4, Reason::BadResponse),
        ] {
            let f = probe(&s, t).await.unwrap_err();
            assert_eq!(f.reason, reason, "{t}");
        }
        let f = probe(&s, 1).await.unwrap_err();
        assert_eq!(f.error.key, "errors.metrics.httpStatus");
        assert_eq!(f.error.params["status"], "403");
        assert!(f.error.params["url"].ends_with("/prefix"));
        // Порт закрыт — источник недоступен.
        let closed = url::Url::parse("http://127.0.0.1:1").unwrap();
        let f = probe(&source(closed, None, false), 1).await.unwrap_err();
        assert!(
            matches!(f.reason, Reason::Unavailable | Reason::Timeout),
            "{f:?}"
        );
    }
}
````

<!-- block: file t04-src-tauri-src-metrics-pods-rs-tests step=4.1 path=src-tauri/src/metrics/pods.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::k8s::testing::{mock_client, status};

    #[tokio::test]
    async fn pod_metrics_become_cores_and_bytes() {
        let (client, _) = mock_client(|s| {
            assert_eq!(
                s.path(),
                "/apis/metrics.k8s.io/v1beta1/namespaces/team-a/pods"
            );
            assert!(s.uri.contains("labelSelector=app%3Dweb"), "{}", s.uri);
            (
                200,
                serde_json::json!({"apiVersion": "metrics.k8s.io/v1beta1", "kind": "PodMetricsList",
                    "metadata": {}, "items": [{"metadata": {"name": "web-1", "namespace": "team-a"},
                    "timestamp": "2026-10-28T10:00:00Z", "window": "30s",
                    "containers": [{"name": "app", "usage": {"cpu": "250m", "memory": "128Mi"}},
                                   {"name": "sidecar", "usage": {"cpu": "12345n"}}]}]}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let pods = pods_now(&r, "team-a", Some("app=web")).await.unwrap();
        assert_eq!(pods[0].pod, "web-1");
        assert_eq!(
            pods[0].containers[0],
            ContainerNow {
                name: "app".into(),
                cpu: Some(0.25),
                memory: Some(134_217_728.0)
            }
        );
        assert_eq!(pods[0].containers[1].memory, None);
        assert!((pods[0].containers[1].cpu.unwrap() - 0.000_012_345).abs() < 1e-12);
    }

    #[tokio::test]
    async fn missing_group_and_rights_are_reasons() {
        for (code, reason) in [(404, Reason::NotFound), (403, Reason::Forbidden)] {
            let (client, _) = mock_client(move |_| status(code, "X", "no"));
            let r = ReadClient::new(client, Duration::from_secs(5));
            assert_eq!(
                pods_now(&r, "team-a", None).await.unwrap_err().reason,
                reason
            );
            assert_eq!(installed(&r).await.unwrap_err().reason, reason);
        }
    }
}
````

<!-- block: file t04-src-tauri-src-metrics-prometheus-rs-tests step=4.1 path=src-tauri/src/metrics/prometheus.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::fake::{FakeSource, matrix, vector};
    use super::*;

    #[test]
    fn vectors_scalars_and_matrices_are_read() {
        let v = parse_vector(&vector(&[(
            &[("container", "app"), ("stat", "cpu_p95")],
            0.25,
        )]))
        .unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!((v[0].label("container"), v[0].value), ("app", 0.25));
        assert_eq!(v[0].label("missing"), "");
        let body = r#"{"status":"success","data":{"resultType":"vector","result":[
            {"metric":{"a":"1"},"value":[1700000000.5,"NaN"]},
            {"metric":{"a":"2"},"value":[1700000000.5,"+Inf"]}]}}"#;
        let v = parse_vector(body).unwrap();
        assert_eq!(v.len(), 1, "NaN отброшен");
        assert!(v[0].value.is_infinite());
        let s = parse_vector(
            r#"{"status":"success","data":{"resultType":"scalar","result":[1700000000,"1"]}}"#,
        )
        .unwrap();
        assert_eq!(s[0].value, 1.0);
        let m = parse_matrix(&matrix(&[(
            &[("container", "app")],
            &[(10, 1.0), (20, 2.5)],
        )]))
        .unwrap();
        assert_eq!(m[0].points, [(10, 1.0), (20, 2.5)]);
    }

    #[test]
    fn errors_and_strange_bodies_are_bad_responses() {
        for body in [
            r#"{"status":"error","errorType":"bad_data","error":"parse error"}"#,
            "<html>login</html>",
            r#"{"status":"success"}"#,
            r#"{"status":"success","data":{"resultType":"matrix","result":[]}}"#,
        ] {
            let e = parse_vector(body).unwrap_err();
            assert_eq!(e.reason, Reason::BadResponse, "{body}");
            assert_eq!(e.error.key, "errors.metrics.badResponse");
        }
        assert!(
            parse_vector(r#"{"status":"error","errorType":"bad_data","error":"parse error"}"#)
                .unwrap_err()
                .error
                .details
                .unwrap()
                .contains("parse error")
        );
    }

    #[test]
    fn failures_get_their_reason_from_the_error_code() {
        let cases = [
            (
                crate::k8s::errors::not_allowed("get", "services/proxy", Some("monitoring")),
                Reason::Forbidden,
            ),
            (
                crate::k8s::errors::from_status(
                    &kube::core::Status::failure("services \"x\" not found", "NotFound")
                        .with_code(404),
                ),
                Reason::NotFound,
            ),
            (
                crate::k8s::errors::from_status(
                    &kube::core::Status::failure("no endpoints", "ServiceUnavailable")
                        .with_code(503),
                ),
                Reason::Unavailable,
            ),
            (crate::k8s::errors::timeout(), Reason::Timeout),
        ];
        for (e, reason) in cases {
            assert_eq!(Failure::from_error(e.clone()).reason, reason, "{}", e.key);
        }
    }

    #[tokio::test]
    async fn thanos_tenancy_gets_the_namespace() {
        let mut s = FakeSource::new(|_, _| Ok(vector(&[(&[], 1.0)])));
        query(&s, "team-a", "up", 100).await.unwrap();
        s.tenancy = true;
        query_range(&s, "team-a", "up", 0, 600, 300)
            .await
            .unwrap_err();
        let seen = s.seen.lock().unwrap();
        assert!(!seen[0].1.iter().any(|(k, _)| k == "namespace"));
        assert_eq!(seen[0].0, PromApi::Query);
        assert!(seen[1].1.contains(&("namespace".into(), "team-a".into())));
        assert!(seen[1].1.contains(&("step".into(), "300".into())));
    }

    #[tokio::test]
    async fn probe_wants_vector_one() {
        let ok = FakeSource::new(|_, q| {
            assert_eq!(q, "vector(1)");
            Ok(vector(&[(&[], 1.0)]))
        });
        assert!(probe(&ok, 1).await.is_ok());
        let wrong = FakeSource::new(|_, _| Ok(vector(&[])));
        assert_eq!(
            probe(&wrong, 1).await.unwrap_err().reason,
            Reason::BadResponse
        );
    }
}
````

<!-- block: file t04-src-tauri-src-metrics-select-rs-tests step=4.1 path=src-tauri/src/metrics/select.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use std::time::Duration as StdDuration;

    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use crate::storage::profile::{MetricsCa, MetricsEndpoint, sample_profile};

    fn ok_vector() -> (u16, serde_json::Value) {
        (
            200,
            serde_json::json!({"status": "success", "data": {"resultType": "vector",
                "result": [{"metric": {}, "value": [1, "1"]}]}}),
        )
    }

    fn inputs(
        platform: PlatformKind,
        handler: impl Fn(&crate::k8s::testing::Seen) -> (u16, serde_json::Value) + Send + 'static,
    ) -> Inputs {
        let mut info = crate::platform::detect(&Default::default());
        info.kind = platform;
        Inputs {
            reader: ReadClient::new(mock_client(handler).0, StdDuration::from_secs(5)),
            profile: sample_profile("a"),
            platform: info,
            connection_token: None,
            metrics_token: None,
        }
    }

    #[tokio::test]
    async fn kubernetes_takes_the_first_prometheus_that_answers() {
        let i = inputs(PlatformKind::Kubernetes, |s| {
            if s.path().contains("services/http:prometheus-k8s:9090") {
                status(403, "Forbidden", "services \"prometheus-k8s\" is forbidden")
            } else if s
                .path()
                .contains("monitoring/services/http:prometheus-operated")
            {
                ok_vector()
            } else {
                status(404, "NotFound", "no")
            }
        });
        let s = select(&i, 100).await;
        assert_eq!(s.status.mode, Mode::Auto);
        let sel = s.status.selected.unwrap();
        assert_eq!(sel.location, "monitoring/prometheus-operated:9090");
        assert_eq!(sel.kind, SourceKind::Prometheus);
        assert_eq!(s.status.rejected.len(), 1);
        assert_eq!(s.status.rejected[0].reason, Reason::Forbidden);
        assert_eq!(
            s.status.rejected[0].message.key,
            "messages.metrics.proxyForbidden"
        );
        assert!(!s.status.pod_metrics, "с историей metrics.k8s.io не нужен");
        assert!(s.history.is_some());
    }

    #[tokio::test]
    async fn without_history_auto_falls_back_to_pod_metrics() {
        let i = inputs(PlatformKind::Kubernetes, |s| {
            if s.path() == "/apis/metrics.k8s.io/v1beta1" {
                (200, serde_json::json!({"kind": "APIResourceList"}))
            } else {
                status(404, "NotFound", "no")
            }
        });
        let s = select(&i, 100).await;
        assert!(s.history.is_none());
        assert!(s.status.pod_metrics);
        assert_eq!(s.status.rejected.len(), 4, "все кандидаты Prometheus");
        let e = s.status.unavailable();
        assert_eq!(e.code, ErrorCode::MetricsUnavailable);
        assert!(
            e.details
                .unwrap()
                .contains("monitoring/prometheus-k8s:9090")
        );
    }

    #[tokio::test]
    async fn openshift_asks_thanos_and_reports_missing_rights() {
        // Ни маршрута, ни домена приложений, ни OAuth-сервера: адрес Thanos не найти.
        let i = inputs(PlatformKind::Openshift, |s| match s.path() {
            "/apis/metrics.k8s.io/v1beta1" => (200, serde_json::json!({})),
            _ => status(403, "Forbidden", "no"),
        });
        let s = select(&i, 100).await;
        assert!(s.history.is_none());
        assert_eq!(
            s.status.rejected.len(),
            1,
            "прокси Prometheus у OpenShift не пробуется"
        );
        assert_eq!(s.status.rejected[0].source.kind, SourceKind::Thanos);
        assert_eq!(s.status.rejected[0].source.location, THANOS_ROUTE);
        assert_eq!(
            s.status.rejected[0].message.key,
            "messages.metrics.thanosForbidden"
        );
        assert!(s.status.pod_metrics);
    }

    #[tokio::test]
    async fn own_source_is_never_replaced() {
        let mut i = inputs(PlatformKind::Kubernetes, |_| ok_vector());
        i.profile.metrics = MetricsConfig::Prometheus(MetricsEndpoint {
            url: "https://user:pw@prometheus.invalid".into(),
            auth: MetricsAuth::Bearer,
            ca: MetricsCa::Connection,
        });
        let s = select(&i, 100).await;
        assert_eq!(s.status.mode, Mode::Prometheus);
        assert!(s.history.is_none() && !s.status.pod_metrics);
        assert_eq!(
            s.status.rejected.len(),
            1,
            "прокси и metrics.k8s.io не пробовали"
        );
        assert_eq!(s.status.rejected[0].reason, Reason::TokenMissing);
        assert_eq!(s.status.rejected[0].source.origin, SourceOrigin::Profile);
        assert!(!s.status.rejected[0].source.location.contains("pw"));
    }

    #[tokio::test]
    async fn the_choice_is_kept_until_refresh_or_expiry() {
        let cache = MetricsCache::default();
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let pick = || async {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            select(
                &inputs(PlatformKind::Kubernetes, |_| status(404, "NotFound", "no")),
                1,
            )
            .await
        };
        cache.get_or_select(false, pick).await;
        cache.get_or_select(false, pick).await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        cache.get_or_select(true, pick).await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
}
````

<!-- block: patch t04-tests step=4.1 -->
````diff
diff --git a/src-tauri/tests/it_connect.rs b/src-tauri/tests/it_connect.rs
index b7cd6d1..34d2008 100644
--- a/src-tauri/tests/it_connect.rs
+++ b/src-tauri/tests/it_connect.rs
@@ -31,7 +31,7 @@ async fn admin_certificate_passes_every_step() {
     let r = test(&profile, secrets.clone()).await;
     assert!(r.ok, "{:#?}", r.steps);
     assert_eq!(
-        steps(&r),
+        steps(&r)[..6],
         [
             ("config", true),
             ("tcp", true),
@@ -41,6 +41,9 @@ async fn admin_certificate_passes_every_step() {
             ("projects", true)
         ]
     );
+    // «Метрики» — предупреждение: источника на стенде может не быть, итог проверки успешен и без
+    // него.
+    assert_eq!(r.steps[6].name, "metrics");
     assert_eq!(
         r.steps[3].message.key, "messages.connectionTest.authOkGroups",
         "у администратора kind есть группы"
diff --git a/src/components/connections/ConnectionForm.test.tsx b/src/components/connections/ConnectionForm.test.tsx
index 82b0f8f..4900179 100644
--- a/src/components/connections/ConnectionForm.test.tsx
+++ b/src/components/connections/ConnectionForm.test.tsx
@@ -377,6 +377,41 @@ describe("ConnectionForm", () => {
     });
   });
 
+  it("шаг «Метрики»: выбранный источник и причины, по которым не подошли остальные", async () => {
+    vi.mocked(api.testConnection).mockResolvedValue({
+      ok: true,
+      steps: [
+        {
+          name: "metrics",
+          ok: true,
+          message: msg("messages.connectionTest.metricsHistory", "History", { source: "Prometheus monitoring/prometheus-operated:9090" }),
+          notes: [msg("messages.metrics.proxyForbidden", "forbidden", { source: "Prometheus monitoring/prometheus-k8s:9090" })],
+        },
+      ],
+    });
+    const user = userEvent.setup();
+    renderWithClient(<ConnectionForm initial={{ ...newProfile(), name: "dev", apiUrl: "https://api.dev:6443" }} tabId="t" />);
+    await user.click(screen.getByRole("button", { name: "Проверить подключение" }));
+    expect(await screen.findByText("Метрики:")).toBeInTheDocument();
+    expect(screen.getByText("История потребления: Prometheus monitoring/prometheus-operated:9090")).toBeInTheDocument();
+    expect(
+      screen.getByText("Prometheus monitoring/prometheus-k8s:9090: нет прав на источник (через API-сервер — get services/proxy)"),
+    ).toBeInTheDocument();
+  });
+
+  it("шаг «Метрики» без источника — предупреждение, а не ошибка", async () => {
+    vi.mocked(api.testConnection).mockResolvedValue({
+      ok: true,
+      steps: [{ name: "metrics", ok: false, message: msg("errors.metrics.unavailable", "No metrics source is available") }],
+    });
+    const user = userEvent.setup();
+    renderWithClient(<ConnectionForm initial={{ ...newProfile(), name: "dev", apiUrl: "https://api.dev:6443" }} tabId="t" />);
+    await user.click(screen.getByRole("button", { name: "Проверить подключение" }));
+    const step = (await screen.findByText("Метрики:")).closest("li");
+    expect(step).toHaveTextContent("⚠️");
+    expect(step).not.toHaveTextContent("❌");
+  });
+
   it("источник метрик: свой Prometheus с токеном — адрес в профиле, токен секретом; «Автоматически» — без них", async () => {
     vi.mocked(api.saveConnection).mockImplementation(async (p) => ({ ...p, id: "c1" }));
     const user = userEvent.setup();
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/connections/ConnectionForm.test.tsx`  
Expected: FAIL — форма не показывает шаг «Метрики» с причинами и ⚠️ без источника.

- [ ] **Step 3: Реализация.**

<!-- block: file t04-src-tauri-src-commands-resources-rs step=4.3 path=src-tauri/src/commands/resources.rs -->
````rust
//! Ресурсы (спец. §4.2, §7): источник метрик подключения и его статус.

use tauri::State;

use crate::error::AppResult;
use crate::metrics::select::SourceStatus;
use crate::state::AppState;

/// Какой источник метрик выбран и почему отвергнуты остальные; `refresh` — выбрать заново.
#[tauri::command]
pub async fn get_metrics_source_status(
    state: State<'_, AppState>,
    connection_id: String,
    refresh: bool,
) -> AppResult<SourceStatus> {
    let st = state.inner().clone();
    let id = connection_id.clone();
    state
        .read(&connection_id, |c| async move {
            let s = crate::metrics::select::for_connection(&st, &id, &c, refresh).await?;
            Ok(s.status.clone())
        })
        .await
}
````

<!-- block: file t04-src-tauri-src-metrics-discovery-rs-impl step=4.3 path=src-tauri/src/metrics/discovery.rs part=impl -->
````rust
//! Автопоиск источника метрик платформы (спец. §7.1, §4.3). У `openshift` — Thanos Querier:
//! маршрут `thanos-querier` в `openshift-monitoring`, иначе адрес из домена приложений
//! (`ingresses.config.openshift.io/cluster`), иначе из хоста OAuth-сервера. У остальных —
//! Prometheus через прокси API-сервера по списку известных сервисов, кандидаты дистрибутива
//! первыми.

use futures::future::BoxFuture;
use kube::api::{ApiResource, GroupVersionKind};

use super::prometheus::{Failure, MetricsSource, PromApi, SourceInfo, SourceKind, SourceOrigin};
use crate::k8s::read::{ReadClient, ServiceProxy};

/// Известные сервисы Prometheus: проект, имя, порт и дистрибутив, у которого он стоит обычно.
pub const PROMETHEUS_SERVICES: [(&str, &str, u16, Option<&str>); 4] = [
    // Rancher Monitoring.
    (
        "cattle-monitoring-system",
        "rancher-monitoring-prometheus",
        9090,
        Some("rancher"),
    ),
    // kube-prometheus.
    ("monitoring", "prometheus-k8s", 9090, None),
    // Prometheus Operator.
    ("monitoring", "prometheus-operated", 9090, None),
    ("observability", "prometheus-operated", 9090, None),
];

/// Кандидаты по порядку проверки: сервисы дистрибутива, затем общие, затем чужих дистрибутивов.
pub fn candidates(distribution: Option<&str>) -> Vec<ServiceProxy> {
    let rank = |d: Option<&str>| match d {
        Some(x) if Some(x) == distribution => 0,
        None => 1,
        Some(_) => 2,
    };
    let mut list: Vec<_> = PROMETHEUS_SERVICES.iter().enumerate().collect();
    list.sort_by_key(|(i, (_, _, _, d))| (rank(*d), *i));
    list.into_iter()
        .map(|(_, (ns, name, port, _))| ServiceProxy {
            namespace: (*ns).into(),
            name: (*name).into(),
            port: *port,
        })
        .collect()
}

/// Prometheus за прокси API-сервера: запросы — `ReadClient::prometheus`.
pub struct ProxySource {
    reader: ReadClient,
    service: ServiceProxy,
    info: SourceInfo,
}

impl ProxySource {
    pub fn new(reader: ReadClient, service: ServiceProxy) -> Self {
        let info = SourceInfo {
            kind: SourceKind::Prometheus,
            origin: SourceOrigin::Auto,
            location: service.label(),
            history: true,
        };
        Self {
            reader,
            service,
            info,
        }
    }
}

impl MetricsSource for ProxySource {
    fn info(&self) -> &SourceInfo {
        &self.info
    }

    fn get<'a>(
        &'a self,
        api: PromApi,
        params: Vec<(String, String)>,
    ) -> BoxFuture<'a, Result<String, Failure>> {
        Box::pin(async move {
            self.reader
                .prometheus(&self.service, api, &params)
                .await
                .map_err(Failure::from_error)
        })
    }
}

/// Маршрут `thanos-querier` в `openshift-monitoring` — для подписи источника.
pub const THANOS_ROUTE: &str = "openshift-monitoring/thanos-querier";

/// `ingresses.config.openshift.io`: домен приложений кластера.
fn ingress_config() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("config.openshift.io", "v1", "Ingress"),
        "ingresses",
    )
}

/// Адрес Thanos Querier по домену приложений.
fn from_domain(domain: &str) -> Option<url::Url> {
    let domain = domain.trim().trim_end_matches('.');
    if domain.is_empty() {
        return None;
    }
    url::Url::parse(&format!(
        "https://thanos-querier-openshift-monitoring.{domain}"
    ))
    .ok()
}

/// Адрес Thanos Querier OpenShift: маршрут, если его можно прочитать, иначе по домену
/// приложений — из настроек Ingress кластера или из хоста OAuth-сервера
/// (`oauth-openshift.<домен>`). Ошибка — причина последней попытки.
pub async fn thanos_url(r: &ReadClient) -> Result<url::Url, Failure> {
    let route = r
        .get_dynamic(
            &crate::k8s::network::routes(),
            Some("openshift-monitoring"),
            "thanos-querier",
        )
        .await;
    if let Ok(o) = &route
        && let Some(host) = o.data["spec"]["host"].as_str()
        && let Ok(u) = url::Url::parse(&format!("https://{host}"))
    {
        return Ok(u);
    }
    if let Ok(o) = r.get_dynamic(&ingress_config(), None, "cluster").await
        && let Some(u) = o.data["spec"]["domain"].as_str().and_then(from_domain)
    {
        return Ok(u);
    }
    let meta = r
        .get_text("/.well-known/oauth-authorization-server")
        .await
        .map_err(Failure::from_error)?;
    let issuer: serde_json::Value =
        serde_json::from_str(&meta).map_err(|_| Failure::bad_response("oauth metadata"))?;
    issuer["issuer"]
        .as_str()
        .and_then(|i| url::Url::parse(i).ok())
        .and_then(|u| {
            u.host_str()
                .and_then(|h| h.strip_prefix("oauth-openshift."))
                .and_then(from_domain)
        })
        .ok_or_else(|| match route {
            Err(e) => Failure::from_error(e),
            Ok(_) => Failure::bad_response("no thanos-querier address"),
        })
}
````

<!-- block: file t04-src-tauri-src-metrics-http-rs-impl step=4.3 path=src-tauri/src/metrics/http.rs part=impl -->
````rust
//! Прямой HTTP к источнику метрик (спец. §7.1): Thanos Querier по маршруту OpenShift и свой
//! URL из профиля. Только GET к `/api/v1/query` и `/api/v1/query_range` — других запросов
//! клиент не умеет; токен (подключения или свой) уходит заголовком `Authorization`. TLS и
//! прокси — как у подключения (`auth::oauth::http_client`: CA добавляется к системным корням —
//! сертификат маршрута часто выпущен CA ingress), свой CA источника — так же.

use futures::future::BoxFuture;
use secrecy::{ExposeSecret, SecretString};

use super::prometheus::{Failure, MetricsSource, PromApi, Reason, SourceInfo};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::storage::profile::{CaConfig, ConnectionProfile, MetricsCa};

/// Источник метрик по адресу: база без `/api/v1`, токен — по выбранной авторизации.
pub struct HttpSource {
    client: reqwest::Client,
    base: url::Url,
    token: Option<SecretString>,
    info: SourceInfo,
    tenancy: bool,
}

/// `Debug` без токена.
impl std::fmt::Debug for HttpSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSource")
            .field("base", &self.base.as_str())
            .field("token", &self.token.as_ref().map(|_| "***"))
            .field("info", &self.info)
            .finish()
    }
}

/// HTTP-клиент источника: TLS, прокси и таймауты профиля; CA источника — свой или как у
/// подключения.
pub fn client(profile: &ConnectionProfile, ca: &MetricsCa) -> AppResult<reqwest::Client> {
    match ca {
        MetricsCa::Connection => crate::auth::oauth::http_client(profile),
        MetricsCa::Pem { pem } => crate::auth::oauth::http_client(&ConnectionProfile {
            ca: CaConfig::Pem { pem: pem.clone() },
            ..profile.clone()
        }),
    }
}

impl HttpSource {
    pub fn new(
        client: reqwest::Client,
        base: url::Url,
        token: Option<SecretString>,
        info: SourceInfo,
        tenancy: bool,
    ) -> Self {
        Self {
            client,
            base,
            token,
            info,
            tenancy,
        }
    }

    fn url(&self, api: PromApi) -> String {
        format!("{}{}", self.base.as_str().trim_end_matches('/'), api.path())
    }

    async fn fetch(&self, api: PromApi, params: Vec<(String, String)>) -> Result<String, Failure> {
        let mut url = url::Url::parse(&self.url(api))
            .map_err(|e| Failure::from_error(AppError::internal_with(e.to_string())))?;
        url.query_pairs_mut().extend_pairs(&params);
        let mut req = self.client.get(url);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t.expose_secret());
        }
        let resp = req.send().await.map_err(|e| transport(&e))?;
        let status = resp.status().as_u16();
        let body = resp.text().await.map_err(|e| transport(&e))?;
        match status {
            200..=299 => Ok(body),
            // У Prometheus ошибка запроса — 400/422 с телом `{"status":"error",…}`.
            400 | 422 => Err(Failure::bad_response(
                body.chars().take(300).collect::<String>(),
            )),
            _ => Err(http_failure(status, &self.base)),
        }
    }
}

impl MetricsSource for HttpSource {
    fn info(&self) -> &SourceInfo {
        &self.info
    }

    fn tenancy(&self) -> bool {
        self.tenancy
    }

    fn get<'a>(
        &'a self,
        api: PromApi,
        params: Vec<(String, String)>,
    ) -> BoxFuture<'a, Result<String, Failure>> {
        Box::pin(self.fetch(api, params))
    }
}

/// Сеть и TLS: тот же разбор, что у API-сервера.
fn transport(e: &reqwest::Error) -> Failure {
    let error = if e.is_timeout() {
        crate::k8s::errors::timeout()
    } else {
        crate::k8s::errors::from_transport_text(&crate::k8s::errors::chain_text(e))
    };
    Failure::from_error(error)
}

/// Ответ источника с кодом ошибки; адрес — в тексте для человека (без имени и пароля).
fn http_failure(status: u16, base: &url::Url) -> Failure {
    let reason = match status {
        401 | 403 => Reason::Forbidden,
        404 => Reason::NotFound,
        _ => Reason::Unavailable,
    };
    let code = match reason {
        Reason::Forbidden => ErrorCode::Forbidden,
        Reason::NotFound => ErrorCode::NotFound,
        _ => ErrorCode::ConnectionFailed,
    };
    Failure {
        reason,
        error: AppError::localized(
            code,
            crate::ltext!(
                "errors.metrics.httpStatus",
                "The metrics source {{url}} answered with code {{status}}",
                url = crate::storage::profile::without_userinfo(base),
                status = status
            ),
        ),
    }
}
````

<!-- block: file t04-src-tauri-src-metrics-pods-rs-impl step=4.3 path=src-tauri/src/metrics/pods.rs part=impl -->
````rust
//! `metrics.k8s.io` (спец. §7.1): текущее потребление подов (`PodMetrics`) — одна точка на
//! контейнер, без истории. Обычно доступно с ролью `view`.

use kube::api::{ApiResource, GroupVersionKind};
use serde::Serialize;

use super::prometheus::{Failure, Reason, SourceInfo, SourceKind, SourceOrigin};
use crate::k8s::quantity::parse_quantity;
use crate::k8s::read::{ReadClient, by_labels};

/// `pods.metrics.k8s.io/v1beta1`.
pub fn pod_metrics() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "PodMetrics"),
        "pods",
    )
}

/// Источник `metrics.k8s.io` для интерфейса и модели.
pub fn info() -> SourceInfo {
    SourceInfo {
        kind: SourceKind::MetricsServer,
        origin: SourceOrigin::Auto,
        location: "metrics.k8s.io".into(),
        history: false,
    }
}

/// Текущее потребление контейнера: ядра и байты.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerNow {
    pub name: String,
    pub cpu: Option<f64>,
    pub memory: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodNow {
    pub pod: String,
    pub containers: Vec<ContainerNow>,
}

/// Текущее потребление подов проекта (с селектором меток — подов workload'а). Нет группы API —
/// `NotFound`, нет прав — `Forbidden`.
pub async fn pods_now(
    r: &ReadClient,
    namespace: &str,
    selector: Option<&str>,
) -> Result<Vec<PodNow>, Failure> {
    let l = r
        .list_dynamic(&pod_metrics(), Some(namespace), &by_labels(selector))
        .await
        .map_err(Failure::from_error)?;
    Ok(l.items
        .into_iter()
        .filter_map(|o| {
            let pod = o.metadata.name?;
            let containers = o.data["containers"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|c| {
                    let q = |k: &str| c["usage"][k].as_str().and_then(parse_quantity);
                    Some(ContainerNow {
                        name: c["name"].as_str()?.to_string(),
                        cpu: q("cpu"),
                        memory: q("memory"),
                    })
                })
                .collect();
            Some(PodNow { pod, containers })
        })
        .collect())
}

/// Есть ли группа `metrics.k8s.io` в кластере (права в проекте проверит само чтение).
pub async fn installed(r: &ReadClient) -> Result<(), Failure> {
    r.get_text("/apis/metrics.k8s.io/v1beta1")
        .await
        .map(|_| ())
        .map_err(|e| {
            let mut f = Failure::from_error(e);
            if f.reason == Reason::BadResponse {
                f.reason = Reason::NotFound;
            }
            f
        })
}
````

<!-- block: file t04-src-tauri-src-metrics-prometheus-rs-impl step=4.3 path=src-tauri/src/metrics/prometheus.rs part=impl -->
````rust
//! Prometheus HTTP API (спец. §7.1): трейт источника [`MetricsSource`] — только GET к
//! `/api/v1/query` и `/api/v1/query_range`, — разбор ответов и запросы поверх него. Thanos
//! Querier говорит на том же API; у своего Thanos запросы по проекту идут с параметром
//! `namespace` (tenancy-endpoint OpenShift без него отвечает 400).

use std::collections::BTreeMap;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, ErrorCode};
use crate::i18n::LocalizedText;

/// Путь API Prometheus: других запросов источники не делают.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromApi {
    Query,
    QueryRange,
}

impl PromApi {
    pub fn path(self) -> &'static str {
        match self {
            PromApi::Query => "/api/v1/query",
            PromApi::QueryRange => "/api/v1/query_range",
        }
    }
}

/// Вид источника.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SourceKind {
    Thanos,
    Prometheus,
    MetricsServer,
}

/// Откуда источник: найден автопоиском платформы или задан в профиле.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SourceOrigin {
    Auto,
    Profile,
}

/// Источник для интерфейса и модели. `location` — для человека: маршрут или сервис кластера, у
/// своего источника — адрес из профиля (модель его не получает).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceInfo {
    pub kind: SourceKind,
    pub origin: SourceOrigin,
    pub location: String,
    /// Есть история (PromQL); у `metrics.k8s.io` — только текущее значение.
    pub history: bool,
}

/// Почему источник не подошёл: код для интерфейса и модели.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Reason {
    /// 401/403: нет прав на мониторинг или на прокси к сервису.
    Forbidden,
    /// Нет сервиса, маршрута или группы API (404).
    NotFound,
    /// Сервис или сеть не отвечают (5xx, отказ соединения, TLS).
    Unavailable,
    Timeout,
    /// Ответ — не Prometheus API или запрос отвергнут (400).
    BadResponse,
    /// Свой источник требует токен, а его нет в хранилище секретов.
    TokenMissing,
}

/// Отказ источника: причина и ошибка с подробностями для человека.
#[derive(Debug, Clone)]
pub struct Failure {
    pub reason: Reason,
    pub error: AppError,
}

impl Failure {
    /// Причина по ошибке запроса к API-серверу или источнику.
    pub fn from_error(error: AppError) -> Self {
        let reason = match error.code {
            ErrorCode::Forbidden | ErrorCode::SessionExpired => Reason::Forbidden,
            ErrorCode::NotFound => Reason::NotFound,
            ErrorCode::Timeout => Reason::Timeout,
            ErrorCode::InvalidInput => Reason::BadResponse,
            _ if error.key == "errors.k8s.unexpectedResponse" => Reason::BadResponse,
            _ => Reason::Unavailable,
        };
        Self { reason, error }
    }

    pub fn bad_response(details: impl Into<String>) -> Self {
        Self {
            reason: Reason::BadResponse,
            error: crate::app_err!(
                ErrorCode::ConnectionFailed,
                "errors.metrics.badResponse",
                "The metrics source returned an unexpected response"
            )
            .with_details(details),
        }
    }
}

/// Источник метрик с историей: GET к API Prometheus. Реализации — прокси сервиса через
/// API-сервер (`ReadClient`) и прямой HTTP (`metrics::http::MetricsHttp`); в тестах — подмена.
pub trait MetricsSource: Send + Sync {
    fn info(&self) -> &SourceInfo;
    /// Запросы по проекту передают `namespace` (Thanos tenancy).
    fn tenancy(&self) -> bool {
        false
    }
    /// Тело ответа на GET `api` с параметрами `params` (уже без `namespace`).
    fn get<'a>(
        &'a self,
        api: PromApi,
        params: Vec<(String, String)>,
    ) -> BoxFuture<'a, Result<String, Failure>>;
}

/// Значение ряда в момент запроса.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

impl Sample {
    pub fn label(&self, name: &str) -> &str {
        self.labels.get(name).map_or("", String::as_str)
    }
}

/// Ряд range-запроса: точки (Unix-секунды, значение) без `NaN`.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    pub labels: BTreeMap<String, String>,
    pub points: Vec<(i64, f64)>,
}

impl Series {
    pub fn label(&self, name: &str) -> &str {
        self.labels.get(name).map_or("", String::as_str)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Envelope {
    status: String,
    #[serde(default)]
    data: Option<Data>,
    #[serde(default)]
    error_type: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "resultType", content = "result", rename_all = "lowercase")]
enum Data {
    Vector(Vec<VectorItem>),
    Matrix(Vec<MatrixItem>),
    Scalar(Point),
    String(serde::de::IgnoredAny),
}

#[derive(Deserialize)]
struct VectorItem {
    #[serde(default)]
    metric: BTreeMap<String, String>,
    value: Point,
}

#[derive(Deserialize)]
struct MatrixItem {
    #[serde(default)]
    metric: BTreeMap<String, String>,
    #[serde(default)]
    values: Vec<Point>,
}

/// `[1700000000.123, "1.5"]`.
#[derive(Deserialize)]
struct Point(f64, String);

impl Point {
    /// Значение или `None` для `NaN`; `+Inf` остаётся бесконечностью.
    fn value(&self) -> Option<f64> {
        self.1.parse::<f64>().ok().filter(|v| !v.is_nan())
    }
}

fn envelope(body: &str) -> Result<Data, Failure> {
    let e: Envelope = serde_json::from_str(body)
        .map_err(|_| Failure::bad_response(body.chars().take(200).collect::<String>()))?;
    if e.status != "success" {
        return Err(Failure::bad_response(format!(
            "{}: {}",
            e.error_type.unwrap_or_default(),
            e.error.unwrap_or_default()
        )));
    }
    e.data
        .ok_or_else(|| Failure::bad_response("success without data"))
}

/// Ответ instant-запроса: ряды со значением (`NaN` отброшены; скаляр — ряд без меток).
pub fn parse_vector(body: &str) -> Result<Vec<Sample>, Failure> {
    match envelope(body)? {
        Data::Vector(items) => Ok(items
            .into_iter()
            .filter_map(|i| {
                Some(Sample {
                    value: i.value.value()?,
                    labels: i.metric,
                })
            })
            .collect()),
        Data::Scalar(p) => Ok(p
            .value()
            .map(|value| Sample {
                labels: BTreeMap::new(),
                value,
            })
            .into_iter()
            .collect()),
        _ => Err(Failure::bad_response("expected a vector")),
    }
}

/// Ответ range-запроса.
pub fn parse_matrix(body: &str) -> Result<Vec<Series>, Failure> {
    match envelope(body)? {
        Data::Matrix(items) => Ok(items
            .into_iter()
            .map(|i| Series {
                points: i
                    .values
                    .iter()
                    .filter_map(|p| Some((p.0 as i64, p.value()?)))
                    .collect(),
                labels: i.metric,
            })
            .collect()),
        _ => Err(Failure::bad_response("expected a matrix")),
    }
}

fn with_namespace(
    s: &dyn MetricsSource,
    namespace: &str,
    mut params: Vec<(String, String)>,
) -> Vec<(String, String)> {
    if s.tenancy() {
        params.push(("namespace".into(), namespace.into()));
    }
    params
}

/// Instant-запрос в момент `time` (Unix-секунды).
pub async fn query(
    s: &dyn MetricsSource,
    namespace: &str,
    promql: &str,
    time: i64,
) -> Result<Vec<Sample>, Failure> {
    let params = vec![
        ("query".to_string(), promql.to_string()),
        ("time".to_string(), time.to_string()),
    ];
    let body = s
        .get(PromApi::Query, with_namespace(s, namespace, params))
        .await?;
    parse_vector(&body)
}

/// Range-запрос `[start, end]` с шагом `step` секунд.
pub async fn query_range(
    s: &dyn MetricsSource,
    namespace: &str,
    promql: &str,
    start: i64,
    end: i64,
    step: u64,
) -> Result<Vec<Series>, Failure> {
    let params = vec![
        ("query".to_string(), promql.to_string()),
        ("start".to_string(), start.to_string()),
        ("end".to_string(), end.to_string()),
        ("step".to_string(), step.to_string()),
    ];
    let body = s
        .get(PromApi::QueryRange, with_namespace(s, namespace, params))
        .await?;
    parse_matrix(&body)
}

/// Проверка источника: `vector(1)` отвечает единицей.
pub async fn probe(s: &dyn MetricsSource, time: i64) -> Result<(), Failure> {
    let body = s
        .get(
            PromApi::Query,
            vec![
                ("query".into(), "vector(1)".into()),
                ("time".into(), time.to_string()),
            ],
        )
        .await?;
    match parse_vector(&body)?.first() {
        Some(v) if v.value == 1.0 => Ok(()),
        _ => Err(Failure::bad_response("vector(1) did not return 1")),
    }
}

impl SourceInfo {
    /// Название для человека: «Thanos Querier openshift-monitoring/thanos-querier»,
    /// «Prometheus monitoring/prometheus-k8s:9090», «metrics.k8s.io».
    pub fn label(&self) -> String {
        match self.kind {
            SourceKind::Thanos => format!("Thanos Querier {}", self.location),
            SourceKind::Prometheus => format!("Prometheus {}", self.location),
            SourceKind::MetricsServer => self.location.clone(),
        }
    }
}

/// Причина словами, с источником (`{{source}}`): для интерфейса и проверки подключения.
pub fn reason_text(r: Reason, info: &SourceInfo) -> LocalizedText {
    let source = info.label();
    match (r, info.kind) {
        (Reason::Forbidden, SourceKind::Thanos) => crate::ltext!(
            "messages.metrics.thanosForbidden",
            "{{source}}: no rights to cluster monitoring (the cluster-monitoring-view role or similar is needed)",
            source = source
        ),
        (Reason::Forbidden, SourceKind::MetricsServer) => crate::ltext!(
            "messages.metrics.podMetricsForbidden",
            "{{source}}: no rights to read pod metrics",
            source = source
        ),
        (Reason::Forbidden, _) => crate::ltext!(
            "messages.metrics.proxyForbidden",
            "{{source}}: no rights to the source (through the API server — get services/proxy)",
            source = source
        ),
        (Reason::NotFound, SourceKind::MetricsServer) => crate::ltext!(
            "messages.metrics.podMetricsMissing",
            "{{source}}: not installed in the cluster",
            source = source
        ),
        (Reason::NotFound, _) => crate::ltext!(
            "messages.metrics.notFound",
            "{{source}}: not found",
            source = source
        ),
        (Reason::Unavailable, _) => crate::ltext!(
            "messages.metrics.unavailable",
            "{{source}}: does not respond",
            source = source
        ),
        (Reason::Timeout, _) => crate::ltext!(
            "messages.metrics.timeout",
            "{{source}}: did not respond in time",
            source = source
        ),
        (Reason::BadResponse, _) => crate::ltext!(
            "messages.metrics.badResponse",
            "{{source}}: answered, but not as Prometheus",
            source = source
        ),
        (Reason::TokenMissing, _) => crate::ltext!(
            "messages.metrics.tokenMissing",
            "{{source}}: the token is not saved — enter it in the connection settings",
            source = source
        ),
    }
}

/// Подмена источника для тестов: ответ по подстроке запроса, журнал запросов.
#[cfg(test)]
pub mod fake {
    use std::sync::Mutex;

    use super::*;

    type Answer = Box<dyn Fn(PromApi, &str) -> Result<String, Failure> + Send + Sync>;
    /// Запрос: путь API и параметры.
    pub type Request = (PromApi, Vec<(String, String)>);
    /// Метки ряда в ответе.
    pub type Labels<'a> = &'a [(&'a str, &'a str)];

    pub struct FakeSource {
        pub info: SourceInfo,
        pub tenancy: bool,
        answer: Answer,
        pub seen: Mutex<Vec<Request>>,
    }

    impl FakeSource {
        pub fn new(
            answer: impl Fn(PromApi, &str) -> Result<String, Failure> + Send + Sync + 'static,
        ) -> Self {
            Self {
                info: SourceInfo {
                    kind: SourceKind::Prometheus,
                    origin: SourceOrigin::Auto,
                    location: "monitoring/prometheus-k8s:9090".into(),
                    history: true,
                },
                tenancy: false,
                answer: Box::new(answer),
                seen: Mutex::new(Vec::new()),
            }
        }

        pub fn queries(&self) -> Vec<String> {
            self.seen
                .lock()
                .unwrap()
                .iter()
                .filter_map(|(_, p)| p.iter().find(|(k, _)| k == "query").map(|(_, v)| v.clone()))
                .collect()
        }
    }

    impl MetricsSource for FakeSource {
        fn info(&self) -> &SourceInfo {
            &self.info
        }

        fn tenancy(&self) -> bool {
            self.tenancy
        }

        fn get<'a>(
            &'a self,
            api: PromApi,
            params: Vec<(String, String)>,
        ) -> BoxFuture<'a, Result<String, Failure>> {
            let query = params
                .iter()
                .find(|(k, _)| k == "query")
                .map(|(_, v)| v.clone())
                .unwrap_or_default();
            self.seen.lock().unwrap().push((api, params));
            let r = (self.answer)(api, &query);
            Box::pin(async move { r })
        }
    }

    /// Ответ instant-запроса: ряды `(метки, значение)`.
    pub fn vector(rows: &[(Labels<'_>, f64)]) -> String {
        let result: Vec<_> = rows
            .iter()
            .map(|(labels, v)| {
                let metric: BTreeMap<_, _> = labels.iter().copied().collect();
                serde_json::json!({"metric": metric, "value": [1_700_000_000, v.to_string()]})
            })
            .collect();
        serde_json::json!({"status": "success", "data": {"resultType": "vector", "result": result}})
            .to_string()
    }

    /// Ответ range-запроса: ряды `(метки, точки)`.
    pub fn matrix(rows: &[(Labels<'_>, &[(i64, f64)])]) -> String {
        let result: Vec<_> = rows
            .iter()
            .map(|(labels, points)| {
                let metric: BTreeMap<_, _> = labels.iter().copied().collect();
                let values: Vec<_> = points
                    .iter()
                    .map(|(t, v)| serde_json::json!([t, v.to_string()]))
                    .collect();
                serde_json::json!({"metric": metric, "values": values})
            })
            .collect();
        serde_json::json!({"status": "success", "data": {"resultType": "matrix", "result": result}})
            .to_string()
    }
}
````

<!-- block: file t04-src-tauri-src-metrics-select-rs-impl step=4.3 path=src-tauri/src/metrics/select.rs part=impl -->
````rust
//! Выбор источника метрик подключения (спец. §7.1). `auto` берёт первый работающий источник
//! истории: у `openshift` — Thanos Querier, у остальных — Prometheus через прокси API-сервера;
//! без него — `metrics.k8s.io` (только текущее значение). Свой URL из профиля другим не
//! заменяется. Выбор, отвергнутые источники и причины видны в интерфейсе и модели; выбор
//! живёт в подключении (`Connected::metrics`) 5 минут, «Обновить» выбирает заново.

use std::sync::Arc;
use std::time::Duration;

use secrecy::SecretString;
use serde::Serialize;
use tokio::time::Instant;

use super::discovery::{ProxySource, THANOS_ROUTE, candidates, thanos_url};
use super::http::{HttpSource, client};
use super::prometheus::{
    Failure, MetricsSource, Reason, SourceInfo, SourceKind, SourceOrigin, probe, reason_text,
};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::i18n::LocalizedText;
use crate::k8s::read::ReadClient;
use crate::platform::{PlatformInfo, PlatformKind};
use crate::storage::profile::{ConnectionProfile, MetricsAuth, MetricsConfig};

/// Сколько живёт выбор источника.
pub const SELECTION_TTL: Duration = Duration::from_secs(5 * 60);

/// Источник, который не подошёл.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Rejected {
    pub source: SourceInfo,
    pub reason: Reason,
    /// Причина словами (для человека).
    pub message: LocalizedText,
    /// Ошибка с подробностями (для человека: адрес, код ответа).
    pub error: AppError,
}

/// Режим профиля: `auto`, `thanos` или `prometheus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Mode {
    Auto,
    Thanos,
    Prometheus,
}

/// Что выбрано и почему: для «Ресурсов», проверки подключения и модели.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceStatus {
    pub mode: Mode,
    /// Источник истории; `None` — его нет.
    pub selected: Option<SourceInfo>,
    pub rejected: Vec<Rejected>,
    /// Группа `metrics.k8s.io` в кластере есть: без источника истории берутся текущие значения
    /// (только в режиме `auto`).
    pub pod_metrics: bool,
    /// Когда выбирали (мс Unix).
    pub checked_at_ms: i64,
}

impl SourceStatus {
    /// Ошибка «нет ни одного источника» с причинами (спец. §15 `METRICS_UNAVAILABLE`).
    pub fn unavailable(&self) -> AppError {
        let reasons = self
            .rejected
            .iter()
            .map(|r| r.message.text.clone())
            .collect::<Vec<_>>()
            .join("; ");
        crate::app_err!(
            ErrorCode::MetricsUnavailable,
            "errors.metrics.unavailable",
            "No metrics source is available"
        )
        .with_details(reasons)
    }
}

pub struct Selected {
    pub history: Option<Arc<dyn MetricsSource>>,
    pub status: SourceStatus,
}

/// Что нужно для выбора: чтение кластера, профиль, платформа и токены.
pub struct Inputs {
    pub reader: ReadClient,
    pub profile: ConnectionProfile,
    pub platform: PlatformInfo,
    /// Токен подключения (если вход по токену): уходит Thanos и своему источнику.
    pub connection_token: Option<SecretString>,
    /// Свой токен источника из keychain (`metricsToken`).
    pub metrics_token: Option<SecretString>,
}

fn rejected(source: SourceInfo, f: Failure) -> Rejected {
    Rejected {
        message: reason_text(f.reason, &source),
        reason: f.reason,
        error: f.error,
        source,
    }
}

/// Выбирает источник истории и проверяет `metrics.k8s.io`.
pub async fn select(i: &Inputs, now_s: i64) -> Selected {
    let mut tried = Vec::new();
    let (mode, history) = match &i.profile.metrics {
        MetricsConfig::Auto => (Mode::Auto, auto(i, now_s, &mut tried).await),
        MetricsConfig::Thanos(_) => (Mode::Thanos, own(i, now_s, &mut tried).await),
        MetricsConfig::Prometheus(_) => (Mode::Prometheus, own(i, now_s, &mut tried).await),
    };
    // Свой источник другим не заменяется: `metrics.k8s.io` — только у автоматического.
    let pod_metrics = mode == Mode::Auto
        && history.is_none()
        && match super::pods::installed(&i.reader).await {
            Ok(()) => true,
            Err(f) => {
                tried.push(rejected(super::pods::info(), f));
                false
            }
        };
    Selected {
        status: SourceStatus {
            mode,
            selected: history.as_ref().map(|h| h.info().clone()),
            rejected: tried,
            pod_metrics,
            checked_at_ms: now_s * 1000,
        },
        history,
    }
}

async fn auto(i: &Inputs, now_s: i64, tried: &mut Vec<Rejected>) -> Option<Arc<dyn MetricsSource>> {
    if i.platform.kind == PlatformKind::Openshift {
        let info = SourceInfo {
            kind: SourceKind::Thanos,
            origin: SourceOrigin::Auto,
            location: THANOS_ROUTE.into(),
            history: true,
        };
        let found = thanos_url(&i.reader).await.and_then(|url| {
            let http = client(&i.profile, &crate::storage::profile::MetricsCa::Connection)
                .map_err(Failure::from_error)?;
            Ok(HttpSource::new(
                http,
                url,
                i.connection_token.clone(),
                info.clone(),
                false,
            ))
        });
        match found {
            Ok(src) => match probe(&src, now_s).await {
                Ok(()) => return Some(Arc::new(src)),
                Err(f) => tried.push(rejected(info, f)),
            },
            Err(f) => tried.push(rejected(info, f)),
        }
        return None;
    }
    let distribution = i.platform.distribution.map(|d| d.id());
    for svc in candidates(distribution) {
        let src = ProxySource::new(i.reader.clone(), svc);
        match probe(&src, now_s).await {
            Ok(()) => return Some(Arc::new(src)),
            Err(f) => tried.push(rejected(src.info().clone(), f)),
        }
    }
    None
}

/// Свой источник из профиля.
async fn own(i: &Inputs, now_s: i64, tried: &mut Vec<Rejected>) -> Option<Arc<dyn MetricsSource>> {
    let (kind, e) = match &i.profile.metrics {
        MetricsConfig::Thanos(e) => (SourceKind::Thanos, e),
        MetricsConfig::Prometheus(e) => (SourceKind::Prometheus, e),
        MetricsConfig::Auto => return None,
    };
    let info = SourceInfo {
        kind,
        origin: SourceOrigin::Profile,
        location: crate::storage::profile::redact_url(&e.url),
        history: true,
    };
    let token = match e.auth {
        MetricsAuth::Connection => i.connection_token.clone(),
        MetricsAuth::None => None,
        MetricsAuth::Bearer => match &i.metrics_token {
            Some(t) => Some(t.clone()),
            None => {
                tried.push(rejected(
                    info,
                    Failure {
                        reason: Reason::TokenMissing,
                        error: crate::app_err!(
                            ErrorCode::AuthFailed,
                            "errors.metrics.tokenMissing",
                            "The metrics source token is not saved"
                        ),
                    },
                ));
                return None;
            }
        },
    };
    let built = url::Url::parse(&e.url)
        .map_err(|_| Failure::bad_response("metrics url"))
        .and_then(|url| {
            let http = client(&i.profile, &e.ca).map_err(Failure::from_error)?;
            Ok(HttpSource::new(
                http,
                url,
                token,
                info.clone(),
                kind == SourceKind::Thanos,
            ))
        });
    match built {
        Ok(src) => match probe(&src, now_s).await {
            Ok(()) => Some(Arc::new(src)),
            Err(f) => {
                tried.push(rejected(info, f));
                None
            }
        },
        Err(f) => {
            tried.push(rejected(info, f));
            None
        }
    }
}

/// Выбор, который живёт в подключении. Одновременные вызовы ждут один выбор.
#[derive(Default)]
pub struct MetricsCache {
    inner: tokio::sync::Mutex<Option<(Instant, Arc<Selected>)>>,
}

impl MetricsCache {
    pub async fn get_or_select<F: std::future::Future<Output = Selected>>(
        &self,
        refresh: bool,
        select: impl FnOnce() -> F,
    ) -> Arc<Selected> {
        let mut slot = self.inner.lock().await;
        if !refresh
            && let Some((at, s)) = slot.as_ref()
            && at.elapsed() < SELECTION_TTL
        {
            return s.clone();
        }
        let s = Arc::new(select().await);
        *slot = Some((Instant::now(), s.clone()));
        s
    }
}

fn now_s() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Источник подключения `id` для команд и MCP: из подключения или выбор заново. Свой токен
/// источника читается из keychain (на blocking-потоке). Ответы 403 при пробах кандидатов не
/// сбрасывают права проекта, в котором идёт чтение: они собираются отдельно (`observe`).
pub async fn for_connection(
    st: &crate::state::AppState,
    id: &str,
    c: &crate::k8s::pool::Connected,
    refresh: bool,
) -> AppResult<Arc<Selected>> {
    let profile = st.connections.get(id)?;
    let bearer = profile
        .metrics
        .endpoint()
        .is_some_and(|e| e.auth == MetricsAuth::Bearer);
    let metrics_token = if bearer {
        let secrets = st.secrets.clone();
        let key = crate::secrets::secret_key(id, crate::secrets::METRICS_TOKEN);
        crate::commands::blocking(move || secrets.get(&key))
            .await?
            .map(SecretString::from)
    } else {
        None
    };
    let inputs = Inputs {
        reader: c.reader(),
        profile,
        platform: c.info.platform.clone(),
        connection_token: c.bearer().cloned(),
        metrics_token,
    };
    let (s, _) = crate::k8s::read::observe(
        c.metrics
            .get_or_select(refresh, || async { select(&inputs, now_s()).await }),
    )
    .await;
    Ok(s)
}
````

<!-- block: patch t04-src step=4.3 -->
````diff
diff --git a/src-tauri/src/commands/mod.rs b/src-tauri/src/commands/mod.rs
index 3135053..2fad953 100644
--- a/src-tauri/src/commands/mod.rs
+++ b/src-tauri/src/commands/mod.rs
@@ -8,6 +8,7 @@ pub mod kubeconfig;
 pub mod mcp;
 pub mod objects;
 pub mod projects;
+pub mod resources;
 pub mod sessions;
 pub mod settings;
 
diff --git a/src-tauri/src/error.rs b/src-tauri/src/error.rs
index 9046f3c..2125de6 100644
--- a/src-tauri/src/error.rs
+++ b/src-tauri/src/error.rs
@@ -19,6 +19,8 @@ pub enum ErrorCode {
     InvalidInput,
     ReadOnly,
     ConfirmationRequired,
+    /// Ни один источник метрик не подошёл (спец. §7.1).
+    MetricsUnavailable,
     Internal,
 }
 
diff --git a/src-tauri/src/k8s/connectivity.rs b/src-tauri/src/k8s/connectivity.rs
index 07aaec4..cd2da6b 100644
--- a/src-tauri/src/k8s/connectivity.rs
+++ b/src-tauri/src/k8s/connectivity.rs
@@ -1,5 +1,5 @@
 //! Пошаговая проверка подключения (спец. §5.3): конфигурация → TCP → [exec-плагин] → TLS → вход →
-//! discovery → проекты. Останавливается на первом неуспешном шаге и называет его.
+//! discovery → проекты → метрики. Останавливается на первом неуспешном шаге и называет его.
 
 use std::sync::Arc;
 use std::time::Duration;
@@ -24,6 +24,9 @@ pub struct TestStep {
     pub message: LocalizedText,
     #[serde(skip_serializing_if = "Option::is_none")]
     pub error: Option<AppError>,
+    /// Пояснения под шагом: у шага «Метрики» — отвергнутые источники и причины.
+    #[serde(skip_serializing_if = "Vec::is_empty")]
+    pub notes: Vec<LocalizedText>,
 }
 
 #[derive(Debug, Clone, Serialize)]
@@ -39,6 +42,7 @@ fn passed(name: &'static str, message: LocalizedText) -> TestStep {
         ok: true,
         message,
         error: None,
+        notes: Vec::new(),
     }
 }
 
@@ -48,6 +52,7 @@ fn failed_step(name: &'static str, e: AppError) -> TestStep {
         ok: false,
         message: e.text(),
         error: Some(e),
+        notes: Vec::new(),
     }
 }
 
@@ -195,11 +200,12 @@ fn is_transport_failure(e: &AppError) -> bool {
 }
 
 /// Шаги после TCP на готовом клиенте: TLS и вход (одним запросом «кто я»), discovery, проекты.
+/// Всё прошло — сведения discovery (по ним выбирается источник метрик).
 pub async fn check_cluster(
     steps: &mut Vec<TestStep>,
     client: &kube::Client,
     p: &ConnectionProfile,
-) -> bool {
+) -> Option<Discovered> {
     let limit = request_timeout(p);
     let tls_ok = if p.ca == CaConfig::Insecure {
         ltext!(
@@ -220,27 +226,27 @@ pub async fn check_cluster(
         // Ошибка транспорта: ответа сервера не было, значит, не прошёл TLS (или связь).
         Err(e) if is_transport_failure(&e) => {
             steps.push(failed_step("tls", e));
-            return false;
+            return None;
         }
         // Любой ответ сервера (401, 403, 404, 400/422 или не-API страница) доказывает, что
         // TLS-рукопожатие прошло, — виноват вход.
         Err(e) => {
             steps.push(passed("tls", tls_ok));
             steps.push(failed_step("auth", e));
-            return false;
+            return None;
         }
     }
-    let projects_api = match discover(client, limit).await {
+    let discovered = match discover(client, limit).await {
         Ok(d) => {
             steps.push(passed("discovery", discovery_message(&d)));
-            d.api_groups.projects
+            d
         }
         Err(e) => {
             steps.push(failed_step("discovery", e));
-            return false;
+            return None;
         }
     };
-    match list_project_names(client, limit, projects_api).await {
+    match list_project_names(client, limit, discovered.api_groups.projects).await {
         Ok((names, more)) => {
             let count = if more {
                 format!("{}+", names.len())
@@ -255,7 +261,7 @@ pub async fn check_cluster(
                     count = count
                 ),
             ));
-            true
+            Some(discovered)
         }
         Err(e) if e.code == ErrorCode::Forbidden && !p.manual_namespaces.is_empty() => {
             steps.push(passed(
@@ -266,15 +272,50 @@ pub async fn check_cluster(
                     count = p.manual_namespaces.len()
                 ),
             ));
-            true
+            Some(discovered)
         }
         Err(e) => {
             steps.push(failed_step("projects", e));
-            false
+            None
         }
     }
 }
 
+/// Шаг «Метрики»: какой источник выбран бы и почему отвергнуты остальные (спец. §5.3, §7.1).
+/// Без источника истории, но с `metrics.k8s.io` шаг проходит: текущие значения есть.
+pub async fn metrics_step(steps: &mut Vec<TestStep>, inputs: &crate::metrics::select::Inputs) {
+    let now = std::time::SystemTime::now()
+        .duration_since(std::time::UNIX_EPOCH)
+        .map_or(0, |d| d.as_secs() as i64);
+    let s = crate::metrics::select::select(inputs, now).await;
+    let notes = s
+        .status
+        .rejected
+        .iter()
+        .map(|r| r.message.clone())
+        .collect();
+    let mut step = match (&s.status.selected, s.status.pod_metrics) {
+        (Some(info), _) => passed(
+            "metrics",
+            ltext!(
+                "messages.connectionTest.metricsHistory",
+                "History of resource usage: {{source}}",
+                source = info.label()
+            ),
+        ),
+        (None, true) => passed(
+            "metrics",
+            ltext!(
+                "messages.connectionTest.metricsCurrent",
+                "No history source: only current values from metrics.k8s.io"
+            ),
+        ),
+        (None, false) => failed_step("metrics", s.status.unavailable()),
+    };
+    step.notes = notes;
+    steps.push(step);
+}
+
 /// Exec-плагин без подтверждённой строки запуска не запускается и при проверке.
 fn ensure_exec_approved(p: &ConnectionProfile) -> AppResult<()> {
     match &p.auth {
@@ -322,11 +363,12 @@ pub async fn test_connection(
 ) -> TestReport {
     let mut steps = Vec::new();
     let profile = p.clone();
+    let config_secrets = secrets.clone();
     let config = match ensure_allowed(p, policy).and_then(|()| ensure_exec_approved(p)) {
         Err(e) => Err(e),
         Ok(()) => tokio::task::spawn_blocking(move || {
             crate::storage::profile::validate_profile(&profile)?;
-            build_kube_config(&profile, secrets.as_ref(), &process_env)
+            build_kube_config(&profile, config_secrets.as_ref(), &process_env)
         })
         .await
         .unwrap_or_else(|e| Err(AppError::internal_with(e.to_string()))),
@@ -379,6 +421,7 @@ pub async fn test_connection(
         }
     }
 
+    let connection_token = config.auth_info.token.clone();
     let client = match kube::Client::try_from(config) {
         Ok(c) => c,
         Err(e) => {
@@ -386,13 +429,48 @@ pub async fn test_connection(
             return report(steps);
         }
     };
-    check_cluster(&mut steps, &client, p).await;
+    let Some(discovered) = check_cluster(&mut steps, &client, p).await else {
+        return report(steps);
+    };
+    let metrics_token = metrics_token(p, secrets).await;
+    let inputs = crate::metrics::select::Inputs {
+        reader: crate::k8s::read::ReadClient::new(client, request_timeout(p)),
+        profile: p.clone(),
+        platform: discovered.platform,
+        connection_token,
+        metrics_token,
+    };
+    metrics_step(&mut steps, &inputs).await;
     report(steps)
 }
 
+/// Свой токен источника метрик (форма или keychain), если он нужен профилю.
+async fn metrics_token(
+    p: &ConnectionProfile,
+    secrets: Arc<dyn SecretStore>,
+) -> Option<secrecy::SecretString> {
+    use crate::storage::profile::MetricsAuth;
+    if !p
+        .metrics
+        .endpoint()
+        .is_some_and(|e| e.auth == MetricsAuth::Bearer)
+    {
+        return None;
+    }
+    let key = crate::secrets::secret_key(&p.id, crate::secrets::METRICS_TOKEN);
+    tokio::task::spawn_blocking(move || secrets.get(&key))
+        .await
+        .ok()?
+        .ok()?
+        .filter(|t| !t.trim().is_empty())
+        .map(secrecy::SecretString::from)
+}
+
+/// Итог — по шагам, без которых подключение не работает. «Метрики» — предупреждение: без
+/// источника недоступны только история потребления и рекомендации.
 fn report(steps: Vec<TestStep>) -> TestReport {
     TestReport {
-        ok: steps.iter().all(|s| s.ok),
+        ok: steps.iter().all(|s| s.ok || s.name == "metrics"),
         steps,
     }
 }
@@ -413,7 +491,11 @@ mod tests {
     async fn all_cluster_steps_pass_on_openshift() {
         let (client, _server) = mock_client(fixtures::openshift);
         let mut steps = Vec::new();
-        assert!(check_cluster(&mut steps, &client, &sample_profile("a")).await);
+        assert!(
+            check_cluster(&mut steps, &client, &sample_profile("a"))
+                .await
+                .is_some()
+        );
         assert_eq!(
             names(&steps),
             [
@@ -457,7 +539,11 @@ mod tests {
             _ => fixtures::openshift(s),
         });
         let mut steps = Vec::new();
-        assert!(check_cluster(&mut steps, &client, &sample_profile("a")).await);
+        assert!(
+            check_cluster(&mut steps, &client, &sample_profile("a"))
+                .await
+                .is_some()
+        );
         assert_eq!(steps[2].message.key, "messages.connectionTest.discovery");
         assert_eq!(steps[2].message.params["platform"], "Kubernetes 1.32");
         assert_eq!(steps[3].message.params["count"], "2");
@@ -489,7 +575,11 @@ mod tests {
     async fn rejected_token_fails_auth_after_tls() {
         let (client, _server) = mock_client(|_| status(401, "Unauthorized", "Unauthorized"));
         let mut steps = Vec::new();
-        assert!(!check_cluster(&mut steps, &client, &sample_profile("a")).await);
+        assert!(
+            check_cluster(&mut steps, &client, &sample_profile("a"))
+                .await
+                .is_none()
+        );
         assert_eq!(names(&steps), [("tls", true), ("auth", false)]);
         assert_eq!(
             steps[1].error.as_ref().unwrap().code,
@@ -509,14 +599,14 @@ mod tests {
         });
         let mut p = sample_profile("a");
         let mut steps = Vec::new();
-        assert!(!check_cluster(&mut steps, &client, &p).await);
+        assert!(check_cluster(&mut steps, &client, &p).await.is_none());
         assert_eq!(
             steps[3].error.as_ref().unwrap().key,
             "errors.k8s.forbiddenCluster"
         );
         p.manual_namespaces = vec!["team-a".into()];
         let mut steps = Vec::new();
-        assert!(check_cluster(&mut steps, &client, &p).await);
+        assert!(check_cluster(&mut steps, &client, &p).await.is_some());
         assert_eq!(
             steps[3].message.key,
             "messages.connectionTest.projectsManual"
@@ -530,7 +620,7 @@ mod tests {
     ) -> (bool, Vec<TestStep>) {
         let (client, _server) = mock_client(move |_| answer.clone());
         let mut steps = Vec::new();
-        let ok = check_cluster(&mut steps, &client, p).await;
+        let ok = check_cluster(&mut steps, &client, p).await.is_some();
         (ok, steps)
     }
 
@@ -678,7 +768,11 @@ mod tests {
         ] {
             let client = failing_client(text);
             let mut steps = Vec::new();
-            assert!(!check_cluster(&mut steps, &client, &sample_profile("a")).await);
+            assert!(
+                check_cluster(&mut steps, &client, &sample_profile("a"))
+                    .await
+                    .is_none()
+            );
             assert_eq!(names(&steps), [("tls", false)], "{text}");
             let e = steps[0].error.as_ref().unwrap();
             assert_eq!((e.code, e.key.as_str()), (code, key), "{text}");
@@ -787,6 +881,76 @@ mod tests {
         );
     }
 
+    fn metrics_inputs(
+        handler: impl Fn(&crate::k8s::testing::Seen) -> (u16, serde_json::Value) + Send + 'static,
+    ) -> crate::metrics::select::Inputs {
+        let (client, _) = mock_client(handler);
+        crate::metrics::select::Inputs {
+            reader: crate::k8s::read::ReadClient::new(client, Duration::from_secs(5)),
+            profile: sample_profile("a"),
+            platform: crate::platform::detect(&Default::default()),
+            connection_token: None,
+            metrics_token: None,
+        }
+    }
+
+    #[tokio::test]
+    async fn metrics_step_names_the_source_and_the_rejected_ones() {
+        let mut steps = Vec::new();
+        metrics_step(
+            &mut steps,
+            &metrics_inputs(|s| {
+                if s.path().contains("prometheus-operated:9090") {
+                    (
+                        200,
+                        serde_json::json!({"status": "success", "data": {"resultType": "vector",
+                            "result": [{"metric": {}, "value": [1, "1"]}]}}),
+                    )
+                } else {
+                    status(403, "Forbidden", "no")
+                }
+            }),
+        )
+        .await;
+        assert_eq!(names(&steps), [("metrics", true)]);
+        assert_eq!(
+            steps[0].message.params["source"],
+            "Prometheus monitoring/prometheus-operated:9090"
+        );
+        assert_eq!(steps[0].notes.len(), 1);
+        assert_eq!(steps[0].notes[0].key, "messages.metrics.proxyForbidden");
+
+        let mut steps = Vec::new();
+        metrics_step(
+            &mut steps,
+            &metrics_inputs(|s| match s.path() {
+                "/apis/metrics.k8s.io/v1beta1" => (200, serde_json::json!({})),
+                _ => status(404, "NotFound", "no"),
+            }),
+        )
+        .await;
+        assert_eq!(names(&steps), [("metrics", true)]);
+        assert_eq!(
+            steps[0].message.key,
+            "messages.connectionTest.metricsCurrent"
+        );
+        assert_eq!(steps[0].notes.len(), 4);
+
+        let mut steps = Vec::new();
+        metrics_step(
+            &mut steps,
+            &metrics_inputs(|_| status(404, "NotFound", "no")),
+        )
+        .await;
+        assert_eq!(names(&steps), [("metrics", false)]);
+        assert_eq!(
+            steps[0].error.as_ref().unwrap().key,
+            "errors.metrics.unavailable"
+        );
+        assert_eq!(steps[0].notes.len(), 5, "и metrics.k8s.io");
+        assert!(report(steps).ok, "без метрик подключение работает");
+    }
+
     #[tokio::test]
     async fn closed_port_fails_tcp_step() {
         let mut p = sample_profile("a");
diff --git a/src-tauri/src/k8s/pool.rs b/src-tauri/src/k8s/pool.rs
index f10a130..6d99334 100644
--- a/src-tauri/src/k8s/pool.rs
+++ b/src-tauri/src/k8s/pool.rs
@@ -4,7 +4,7 @@ use std::collections::HashMap;
 use std::sync::{Arc, Mutex};
 use std::time::{Duration, SystemTime};
 
-use secrecy::ExposeSecret;
+use secrecy::{ExposeSecret, SecretString};
 use tokio::sync::oneshot;
 
 use crate::error::{AppError, AppResult};
@@ -25,6 +25,11 @@ pub struct Connected {
     pub timeout: Duration,
     /// Когда истекают учётные данные exec-плагина (`expirationTimestamp`).
     pub expires_at: Option<SystemTime>,
+    /// Токен подключения (вход по токену, через браузер или токен exec-плагина): его получают
+    /// Thanos Querier OpenShift и свой источник метрик с авторизацией «токен подключения».
+    bearer: Option<SecretString>,
+    /// Выбранный источник метрик (спец. §7.1): живёт, пока живёт клиент.
+    pub metrics: crate::metrics::select::MetricsCache,
 }
 
 impl Connected {
@@ -39,9 +44,22 @@ impl Connected {
             info,
             timeout,
             expires_at,
+            bearer: None,
+            metrics: Default::default(),
         }
     }
 
+    /// С токеном подключения для источника метрик.
+    pub fn with_bearer(mut self, token: Option<SecretString>) -> Self {
+        self.bearer = token;
+        self
+    }
+
+    /// Токен подключения для источника метрик (`metrics::select`).
+    pub fn bearer(&self) -> Option<&SecretString> {
+        self.bearer.as_ref()
+    }
+
     /// Клиент `kube` подключения — только для интеграционных тестов (запросы мимо `ReadClient`:
     /// проверка прав, значения Secret, удаление пода). MCP-шлюз его не берёт (сторожок в `mcp/`).
     #[doc(hidden)]
@@ -199,6 +217,8 @@ pub async fn open(
         expires_at = cred.expires_at;
         apply_exec_credential(&mut config, &cred);
     }
+    // Токен для источника метрик — после учётных данных exec-плагина: у плагина он свой.
+    let bearer = config.auth_info.token.clone();
     let client = kube::Client::try_from(config).map_err(|e| from_kube(&e))?;
     let mut info = probe(&client, timeout).await?;
     if let Some(token) = &token {
@@ -209,7 +229,7 @@ pub async fn open(
             )
             .await;
     }
-    Ok(Connected::new(client, info, timeout, expires_at))
+    Ok(Connected::new(client, info, timeout, expires_at).with_bearer(bearer))
 }
 
 /// Подключение для тестов: клиент — мок API-сервера с ответами `handler`, платформа — Kubernetes
@@ -232,6 +252,8 @@ pub(crate) fn test_connected(
         },
         timeout: Duration::from_secs(1),
         expires_at: None,
+        bearer: None,
+        metrics: Default::default(),
     })
 }
 
@@ -327,12 +349,12 @@ mod tests {
     async fn exec_credentials_expire_a_minute_early() {
         let (client, _) = crate::k8s::testing::mock_client(|_| (200, serde_json::json!({})));
         let now = SystemTime::now();
-        let mut c = Connected {
+        let mut c = Connected::new(
             client,
-            info: connected().info.clone(),
-            timeout: Duration::from_secs(1),
-            expires_at: None,
-        };
+            connected().info.clone(),
+            Duration::from_secs(1),
+            None,
+        );
         assert!(!c.expiring(now), "токен без срока");
         c.expires_at = Some(now + Duration::from_secs(30));
         assert!(c.expiring(now));
diff --git a/src-tauri/src/k8s/read.rs b/src-tauri/src/k8s/read.rs
index 651f534..8e81655 100644
--- a/src-tauri/src/k8s/read.rs
+++ b/src-tauri/src/k8s/read.rs
@@ -183,6 +183,21 @@ fn bad_name(name: &str) -> AppError {
     )
 }
 
+/// Сервис Prometheus в кластере, к которому ходят через прокси API-сервера (спец. §7.1).
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct ServiceProxy {
+    pub namespace: String,
+    pub name: String,
+    pub port: u16,
+}
+
+impl ServiceProxy {
+    /// `monitoring/prometheus-k8s:9090` — для интерфейса.
+    pub fn label(&self) -> String {
+        format!("{}/{}:{}", self.namespace, self.name, self.port)
+    }
+}
+
 /// Клиент только для чтения поверх `kube::Client` подключения.
 #[derive(Clone)]
 pub struct ReadClient {
@@ -360,6 +375,34 @@ impl ReadClient {
         self.call(self.client.request_text(request)).await
     }
 
+    /// GET к Prometheus через прокси сервиса API-сервера
+    /// (`/api/v1/namespaces/<ns>/services/http:<сервис>:<порт>/proxy/api/v1/query…`, спец. §7.1).
+    /// Только два пути API Prometheus (`PromApi`): других запросов к сервисам кластера
+    /// `ReadClient` не делает. Нужно право `get services/proxy` в проекте сервиса.
+    pub async fn prometheus(
+        &self,
+        svc: &ServiceProxy,
+        api: crate::metrics::prometheus::PromApi,
+        params: &[(String, String)],
+    ) -> AppResult<String> {
+        check_namespace(&svc.namespace)?;
+        check_name(&svc.name)?;
+        let query = url::form_urlencoded::Serializer::new(String::new())
+            .extend_pairs(params)
+            .finish();
+        let path = format!(
+            "/api/v1/namespaces/{}/services/http:{}:{}/proxy{}?{query}",
+            svc.namespace,
+            svc.name,
+            svc.port,
+            api.path()
+        );
+        let request = http::Request::get(path)
+            .body(Vec::new())
+            .map_err(|e| crate::error::AppError::internal_with(e.to_string()))?;
+        self.call(self.client.request_text(request)).await
+    }
+
     /// Изменения объектов namespace (`kube::runtime::watcher`): список, затем watch; после
     /// разрыва watcher сам читает список заново. Таймаут к потоку не применяется. Ошибку
     /// поток вернуть не может: namespace проверен заранее (`Namespace`).
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index 4df7791..e487184 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -154,6 +154,7 @@ pub fn run() {
             commands::objects::list_pvcs,
             commands::objects::get_quotas,
             commands::objects::get_yaml,
+            commands::resources::get_metrics_source_status,
             commands::health::run_health_check,
             commands::health::get_health,
             commands::health::set_finding_ignored,
diff --git a/src-tauri/src/metrics/mod.rs b/src-tauri/src/metrics/mod.rs
index 4c99d5e..4cf7706 100644
--- a/src-tauri/src/metrics/mod.rs
+++ b/src-tauri/src/metrics/mod.rs
@@ -1,4 +1,10 @@
-//! Метрики потребления ресурсов (спец. §7.1, §7.2): PromQL-шаблоны — чистые функции.
-//! Источники метрик (Thanos, Prometheus, `metrics.k8s.io`) и их выбор — задача 4 плана O3.
+//! Метрики потребления ресурсов (спец. §7.1, §7.2): источники с историей (Thanos Querier,
+//! Prometheus через прокси API-сервера, свой URL) за трейтом `MetricsSource`, текущие значения
+//! `metrics.k8s.io`, выбор источника и PromQL-шаблоны — чистые функции.
 
+pub mod discovery;
+pub mod http;
+pub mod pods;
+pub mod prometheus;
 pub mod promql;
+pub mod select;
diff --git a/src-tauri/src/platform/mod.rs b/src-tauri/src/platform/mod.rs
index c3583b7..dbfe442 100644
--- a/src-tauri/src/platform/mod.rs
+++ b/src-tauri/src/platform/mod.rs
@@ -17,7 +17,7 @@ pub enum PlatformKind {
 }
 
 /// Справочный дистрибутив платформы `kubernetes`: виден в интерфейсе и MCP и влияет только
-/// на порядок кандидатов автопоиска метрик (этап O3).
+/// на порядок кандидатов автопоиска метрик (`metrics::discovery::candidates`).
 #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
 #[serde(rename_all = "lowercase")]
 pub enum Distribution {
@@ -31,6 +31,19 @@ pub enum Distribution {
 }
 
 impl Distribution {
+    /// Код, как в JSON: `rancher`, `eks`.
+    pub fn id(self) -> &'static str {
+        match self {
+            Distribution::Eks => "eks",
+            Distribution::Gke => "gke",
+            Distribution::Aks => "aks",
+            Distribution::Rancher => "rancher",
+            Distribution::Deckhouse => "deckhouse",
+            Distribution::K3s => "k3s",
+            Distribution::Rke2 => "rke2",
+        }
+    }
+
     /// Название в интерфейсе (не переводится).
     pub fn title(self) -> &'static str {
         match self {
diff --git a/src/api/client.ts b/src/api/client.ts
index 48d6b26..fdafcbb 100644
--- a/src/api/client.ts
+++ b/src/api/client.ts
@@ -22,6 +22,7 @@ import type {
   McpAuditEntry,
   McpClientConfigs,
   McpStatus,
+  MetricsSourceStatus,
   NamespaceAccess,
   NetworkView,
   NodeDetails,
@@ -140,6 +141,10 @@ export const api = {
   getYaml: (connectionId: string, namespace: string, kind: YamlKind, name: string) =>
     call<string>("get_yaml", { connectionId, namespace, kind, name }),
 
+  /** Источник метрик подключения: выбранный и отвергнутые; `refresh` — выбрать заново. */
+  getMetricsSourceStatus: (connectionId: string, refresh = false) =>
+    call<MetricsSourceStatus>("get_metrics_source_status", { connectionId, refresh }),
+
   /** Watch списка проекта; события приходят в `onEvent`. Возвращает id сессии для `stopSession`. */
   startWatch: (params: WatchParams, onEvent: (e: WatchEvent) => void) => {
     const channel = new Channel<WatchEvent>();
diff --git a/src/api/types.ts b/src/api/types.ts
index 14aa459..63040c2 100644
--- a/src/api/types.ts
+++ b/src/api/types.ts
@@ -12,6 +12,7 @@ export type ErrorCode =
   | "INVALID_INPUT"
   | "READ_ONLY"
   | "CONFIRMATION_REQUIRED"
+  | "METRICS_UNAVAILABLE"
   | "INTERNAL";
 
 /** Текст из Rust: ключ словаря, параметры и английский текст (запасной вариант). */
@@ -157,13 +158,15 @@ export interface TreeItemRef {
   id: string;
 }
 
-export type TestStepName = "config" | "tcp" | "exec" | "tls" | "auth" | "discovery" | "projects";
+export type TestStepName = "config" | "tcp" | "exec" | "tls" | "auth" | "discovery" | "projects" | "metrics";
 
 export interface TestStep {
   name: TestStepName;
   ok: boolean;
   message: LocalizedText;
   error?: AppError;
+  /** Пояснения под шагом: у «Метрик» — отвергнутые источники и причины. */
+  notes?: LocalizedText[];
 }
 
 export interface TestReport {
@@ -1007,3 +1010,34 @@ export interface HealthView {
   ignored: string[];
   annotations: Annotation[];
 }
+
+/** Источник метрик (спец. §7.1). */
+export type MetricsSourceKind = "thanos" | "prometheus" | "metricsServer";
+
+export interface MetricsSourceInfo {
+  kind: MetricsSourceKind;
+  /** `auto` — найден автопоиском, `profile` — свой из настроек подключения. */
+  origin: "auto" | "profile";
+  /** Маршрут или сервис кластера, у своего источника — адрес. */
+  location: string;
+  history: boolean;
+}
+
+export type MetricsRejectReason = "forbidden" | "notFound" | "unavailable" | "timeout" | "badResponse" | "tokenMissing";
+
+export interface MetricsRejected {
+  source: MetricsSourceInfo;
+  reason: MetricsRejectReason;
+  message: LocalizedText;
+  error: AppError;
+}
+
+/** Какой источник выбран и почему отвергнуты остальные. */
+export interface MetricsSourceStatus {
+  mode: MetricsMode;
+  selected: MetricsSourceInfo | null;
+  rejected: MetricsRejected[];
+  /** Группа metrics.k8s.io есть: без истории — текущие значения (только в режиме auto). */
+  podMetrics: boolean;
+  checkedAtMs: number;
+}
diff --git a/src/components/connections/ConnectionForm.tsx b/src/components/connections/ConnectionForm.tsx
index ad480c3..0d0b187 100644
--- a/src/components/connections/ConnectionForm.tsx
+++ b/src/components/connections/ConnectionForm.tsx
@@ -605,8 +605,15 @@ export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile;
         <ul className="steps">
           {report.steps.map((s) => (
             <li key={s.name}>
-              {s.ok ? "✅" : "❌"} <strong>{t(`connections.form.step.${s.name}`)}:</strong> <span>{tr(s.message)}</span>
+              {s.ok ? "✅" : s.name === "metrics" ? "⚠️" : "❌"} <strong>{t(`connections.form.step.${s.name}`)}:</strong> <span>{tr(s.message)}</span>
               {s.error?.details && <pre className="muted">{s.error.details}</pre>}
+              {s.notes && s.notes.length > 0 && (
+                <ul className="muted">
+                  {s.notes.map((n, i) => (
+                    <li key={i}>{tr(n)}</li>
+                  ))}
+                </ul>
+              )}
             </li>
           ))}
         </ul>
````

<!-- block: locales t04-locales step=4.3 -->
````json
{
  "en": {
    "errors": {
      "metrics": {
        "badResponse": "The metrics source returned an unexpected response",
        "httpStatus": "The metrics source {{url}} answered with code {{status}}",
        "tokenMissing": "The metrics source token is not saved",
        "unavailable": "No metrics source is available"
      }
    },
    "messages": {
      "connectionTest": {
        "metricsHistory": "History of resource usage: {{source}}",
        "metricsCurrent": "No history source: only current values from metrics.k8s.io"
      },
      "metrics": {
        "thanosForbidden": "{{source}}: no rights to cluster monitoring (the cluster-monitoring-view role or similar is needed)",
        "podMetricsForbidden": "{{source}}: no rights to read pod metrics",
        "proxyForbidden": "{{source}}: no rights to the source (through the API server — get services/proxy)",
        "podMetricsMissing": "{{source}}: not installed in the cluster",
        "notFound": "{{source}}: not found",
        "unavailable": "{{source}}: does not respond",
        "timeout": "{{source}}: did not respond in time",
        "badResponse": "{{source}}: answered, but not as Prometheus",
        "tokenMissing": "{{source}}: the token is not saved — enter it in the connection settings"
      }
    },
    "connections": {
      "form": {
        "step": {
          "metrics": "Metrics"
        }
      }
    }
  },
  "ru": {
    "errors": {
      "metrics": {
        "badResponse": "Источник метрик вернул неожиданный ответ",
        "httpStatus": "Источник метрик {{url}} ответил кодом {{status}}",
        "tokenMissing": "Токен источника метрик не сохранён",
        "unavailable": "Ни один источник метрик не доступен"
      }
    },
    "messages": {
      "connectionTest": {
        "metricsHistory": "История потребления: {{source}}",
        "metricsCurrent": "Источника истории нет: только текущие значения из metrics.k8s.io"
      },
      "metrics": {
        "thanosForbidden": "{{source}}: нет прав на мониторинг кластера (нужна роль cluster-monitoring-view или аналог)",
        "podMetricsForbidden": "{{source}}: нет прав на метрики подов",
        "proxyForbidden": "{{source}}: нет прав на источник (через API-сервер — get services/proxy)",
        "podMetricsMissing": "{{source}}: не установлен в кластере",
        "notFound": "{{source}}: не найден",
        "unavailable": "{{source}}: не отвечает",
        "timeout": "{{source}}: не ответил вовремя",
        "badResponse": "{{source}}: ответил, но не как Prometheus",
        "tokenMissing": "{{source}}: токен не сохранён — введите его в настройках подключения"
      }
    },
    "connections": {
      "form": {
        "step": {
          "metrics": "Метрики"
        }
      }
    }
  }
}
````

- [ ] **Step 4: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 613 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm test`  
Expected: PASS — `Test Files 63 passed`, `Tests 479 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 179`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

Стенд kind (`dev/kind/README.md`; `KUBECONFIG` — отдельный файл, не `~/.kube/config`; после изменения стенда — заново `dev/kind/setup.sh`):  
Run: `GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --test it_connect`  
Expected: PASS — `test result: ok. 4 passed; 0 failed`

- [ ] **Step 5: Commit.**

```bash
git add \
  src-tauri/src/commands/mod.rs \
  src-tauri/src/commands/resources.rs \
  src-tauri/src/error.rs \
  src-tauri/src/k8s/connectivity.rs \
  src-tauri/src/k8s/pool.rs \
  src-tauri/src/k8s/read.rs \
  src-tauri/src/lib.rs \
  src-tauri/src/metrics/discovery.rs \
  src-tauri/src/metrics/http.rs \
  src-tauri/src/metrics/mod.rs \
  src-tauri/src/metrics/pods.rs \
  src-tauri/src/metrics/prometheus.rs \
  src-tauri/src/metrics/select.rs \
  src-tauri/src/platform/mod.rs \
  src-tauri/tests/it_connect.rs \
  src/api/client.ts \
  src/api/types.ts \
  src/components/connections/ConnectionForm.test.tsx \
  src/components/connections/ConnectionForm.tsx \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "feat(metrics): metrics sources — Thanos Querier on OpenShift, Prometheus through the API server proxy, own URL, metrics.k8s.io; the choice and the rejected sources in the connection test and the source status" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 5: Статистика workload'а за окно: CPU, память, троттлинг, OOM и рестарты худшего пода, графики и покрытие; текущие requests и limits с LimitRange по умолчанию; потребление пода

**Files:**
- Create: `src-tauri/src/resources/current.rs`, `src-tauri/src/resources/mod.rs`, `src-tauri/src/resources/usage.rs`
- Modify: `src-tauri/src/commands/resources.rs`, `src-tauri/src/k8s/pods.rs`, `src-tauri/src/k8s/workloads.rs`, `src-tauri/src/lib.rs`, `src-tauri/src/metrics/promql.rs`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задачи 3–4: `promql`, `MetricsSource`, `query`, `query_range`, `Selected`, `pods_now`; O1b: `k8s::workloads`, `k8s::pods::WorkloadRef`.
- Produces: `resources::current::{Amounts { cpu, memory }, ContainerCurrent { name, init, sidecar, requests, limits, limit_range_requests, limit_range_limits, env_names }, current}`; `resources::usage::{WINDOWS = [1, 7, 14, 30], check_window, CpuStats, MemoryStats, ThrottlingStats, ContainerStats, ContainerUsage, ChartSeries, Marker, Charts, WorkloadUsage, workload_usage(r, sel, ns, kind, name, days, now), PodUsage, pod_usage}`; `k8s::workloads::{WorkloadSpec { row, selector, template, surge, resource_version, generation }, get_spec, surge_pods}`; `k8s::pods::workload_pods`; команды `get_workload_usage`, `get_pod_usage`.

Восемь запросов источника идут параллельно (`try_join!`): CPU p50/p95/p99/max, память p95/p99/max, троттлинг среднее и p95, OOM (число, последний) и рестарты, покрытие (шагов с данными и первый), ряды CPU и памяти и отметки событий. Без kube-state-metrics OOM и рестарты — `None`. Текущие значения — из шаблона подов; где их нет, а работающий под получил их от `LimitRange`, видны оба. Без истории — одна точка `metrics.k8s.io` на контейнер. `WorkloadSpec.generation` понадобится версии предпросмотра изменений (задача 9).

- [ ] **Step 1: Тесты.**

<!-- block: file t05-src-tauri-src-resources-current-rs-tests step=5.1 path=src-tauri/src/resources/current.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;

    fn spec(v: serde_json::Value) -> PodSpec {
        serde_json::from_value(v).unwrap()
    }

    fn pod(phase: &str, containers: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "p"}, "spec": {"containers": containers}, "status": {"phase": phase}
        }))
        .unwrap()
    }

    #[test]
    fn template_values_and_limit_range_defaults() {
        let t = spec(serde_json::json!({
            "initContainers": [
                {"name": "migrate", "image": "m"},
                {"name": "proxy", "image": "p", "restartPolicy": "Always",
                 "resources": {"requests": {"cpu": "50m"}}}
            ],
            "containers": [{"name": "app", "image": "a",
                "env": [{"name": "JAVA_OPTS", "value": "-Xmx1g"}],
                "resources": {"requests": {"cpu": "250m"}, "limits": {"memory": "512Mi"}}}]
        }));
        let pods = [
            pod(
                "Pending",
                serde_json::json!([{"name": "app", "image": "a",
                "resources": {"requests": {"cpu": "1", "memory": "1Gi"}}}]),
            ),
            pod(
                "Running",
                serde_json::json!([{"name": "app", "image": "a",
                "resources": {"requests": {"cpu": "250m", "memory": "256Mi"},
                              "limits": {"cpu": "500m", "memory": "512Mi"}}}]),
            ),
        ];
        let c = current(Some(&t), &pods);
        assert_eq!(
            c.iter()
                .map(|c| (c.name.as_str(), c.init, c.sidecar, c.recommended()))
                .collect::<Vec<_>>(),
            [
                ("migrate", true, false, false),
                ("proxy", true, true, true),
                ("app", false, false, true)
            ]
        );
        let app = &c[2];
        assert_eq!(
            app.requests,
            Amounts {
                cpu: Some(0.25),
                memory: None
            }
        );
        assert_eq!(
            app.limits,
            Amounts {
                cpu: None,
                memory: Some(536_870_912.0)
            }
        );
        assert_eq!(
            app.limit_range_requests,
            Amounts {
                cpu: None,
                memory: Some(268_435_456.0)
            },
            "от работающего пода, а не от Pending"
        );
        assert_eq!(
            app.limit_range_limits,
            Amounts {
                cpu: Some(0.5),
                memory: None
            }
        );
        assert_eq!(
            app.effective_limits(),
            Amounts {
                cpu: Some(0.5),
                memory: Some(536_870_912.0)
            }
        );
        assert_eq!(app.env_names, ["JAVA_OPTS"]);
        assert!(current(None, &pods).is_empty());
    }
}
````

- [ ] **Step 2: Реализация.**

<!-- block: file t05-src-tauri-src-resources-current-rs-impl step=5.2 path=src-tauri/src/resources/current.rs part=impl -->
````rust
//! Текущие requests и limits контейнеров (спец. §7.3): из шаблона подов workload'а — именно его
//! меняет форма. Если в шаблоне значения нет, а работающий под получил его от `LimitRange` по
//! умолчанию, оно тоже есть — с пометкой.

use k8s_openapi::api::core::v1::{Container, Pod, PodSpec};
use serde::Serialize;

use crate::k8s::quantity::parse_quantity;

/// CPU в ядрах и память в байтах; `None` — не задано.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Amounts {
    pub cpu: Option<f64>,
    pub memory: Option<f64>,
}

impl Amounts {
    fn of(
        map: Option<
            &std::collections::BTreeMap<
                String,
                k8s_openapi::apimachinery::pkg::api::resource::Quantity,
            >,
        >,
    ) -> Self {
        let q = |k: &str| {
            map.and_then(|m| m.get(k))
                .and_then(|v| parse_quantity(&v.0))
        };
        Self {
            cpu: q("cpu"),
            memory: q("memory"),
        }
    }

    /// Значения `pod`, которых нет здесь.
    fn missing_from(self, pod: Self) -> Self {
        Self {
            cpu: self.cpu.is_none().then_some(pod.cpu).flatten(),
            memory: self.memory.is_none().then_some(pod.memory).flatten(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerCurrent {
    pub name: String,
    /// Init-контейнер: показывается, рекомендация не считается.
    pub init: bool,
    /// Нативный sidecar (init с `restartPolicy: Always`): работает всё время, как основной.
    pub sidecar: bool,
    pub requests: Amounts,
    pub limits: Amounts,
    /// Значения работающего пода, которых нет в шаблоне: их поставил `LimitRange` по умолчанию.
    pub limit_range_requests: Amounts,
    pub limit_range_limits: Amounts,
    /// Имена переменных окружения (значения не читаются): подсказки о JVM и Go (спец. §7.3).
    pub env_names: Vec<String>,
}

impl ContainerCurrent {
    /// Рекомендация считается: основной контейнер или нативный sidecar.
    pub fn recommended(&self) -> bool {
        !self.init || self.sidecar
    }

    /// Действующее значение: из шаблона, иначе от `LimitRange`.
    pub fn effective_requests(&self) -> Amounts {
        Amounts {
            cpu: self.requests.cpu.or(self.limit_range_requests.cpu),
            memory: self.requests.memory.or(self.limit_range_requests.memory),
        }
    }

    pub fn effective_limits(&self) -> Amounts {
        Amounts {
            cpu: self.limits.cpu.or(self.limit_range_limits.cpu),
            memory: self.limits.memory.or(self.limit_range_limits.memory),
        }
    }
}

fn resources(c: &Container) -> (Amounts, Amounts) {
    let r = c.resources.as_ref();
    (
        Amounts::of(r.and_then(|r| r.requests.as_ref())),
        Amounts::of(r.and_then(|r| r.limits.as_ref())),
    )
}

/// Контейнер `name` первого пода, где он есть (работающие поды — первыми).
fn in_pods<'a>(pods: &'a [Pod], name: &str, init: bool) -> Option<&'a Container> {
    let mut sorted: Vec<&Pod> = pods.iter().collect();
    sorted.sort_by_key(|p| p.status.as_ref().and_then(|s| s.phase.as_deref()) != Some("Running"));
    sorted.into_iter().find_map(|p| {
        let spec = p.spec.as_ref()?;
        let list = if init {
            spec.init_containers.as_ref()?
        } else {
            &spec.containers
        };
        list.iter().find(|c| c.name == name)
    })
}

/// Контейнеры шаблона (сначала init) с текущими значениями.
pub fn current(template: Option<&PodSpec>, pods: &[Pod]) -> Vec<ContainerCurrent> {
    let Some(spec) = template else {
        return Vec::new();
    };
    let init = spec.init_containers.iter().flatten().map(|c| (c, true));
    let main = spec.containers.iter().map(|c| (c, false));
    init.chain(main)
        .map(|(c, init)| {
            let (requests, limits) = resources(c);
            let (pod_requests, pod_limits) = in_pods(pods, &c.name, init)
                .map(resources)
                .unwrap_or_default();
            ContainerCurrent {
                name: c.name.clone(),
                init,
                sidecar: init && c.restart_policy.as_deref() == Some("Always"),
                limit_range_requests: requests.missing_from(pod_requests),
                limit_range_limits: limits.missing_from(pod_limits),
                requests,
                limits,
                env_names: c.env.iter().flatten().map(|e| e.name.clone()).collect(),
            }
        })
        .collect()
}
````

<!-- block: file t05-src-tauri-src-resources-mod-rs step=5.2 path=src-tauri/src/resources/mod.rs -->
````rust
//! Анализ ресурсов (спец. §7): текущие requests и limits, статистика потребления workload'а и
//! пода по выбранному источнику метрик.

pub mod current;
pub mod usage;
````

<!-- block: file t05-src-tauri-src-resources-usage-rs step=5.2 path=src-tauri/src/resources/usage.rs -->
````rust
//! Статистика потребления workload'а (спец. §7.2): по каждому контейнеру за окно — CPU, память,
//! троттлинг, OOM и рестарты; ряды графиков; покрытие окна. Источник истории — PromQL-шаблоны
//! (`metrics::promql`), без него — одна точка `metrics.k8s.io`. Поды выбираются через
//! kube-state-metrics, если его метрики есть, иначе по имени.

use std::collections::BTreeMap;

use serde::Serialize;

use super::current::{ContainerCurrent, current};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::pods::{WorkloadRef, workload_pods};
use crate::k8s::read::ReadClient;
use crate::k8s::workloads::{WorkloadKind, get_spec};
use crate::metrics::pods::{PodNow, pods_now};
use crate::metrics::prometheus::{
    Failure, MetricsSource, Sample, Series, SourceInfo, query, query_range,
};
use crate::metrics::promql::{self, Pods};
use crate::metrics::select::{Selected, SourceStatus};

/// Окна анализа, дни (спец. §7.2).
pub const WINDOWS: [u32; 4] = [1, 7, 14, 30];

pub fn check_window(days: u32) -> AppResult<u32> {
    if WINDOWS.contains(&days) {
        return Ok(days);
    }
    Err(crate::app_err!(
        ErrorCode::InvalidInput,
        "errors.resources.badWindow",
        "The analysis window must be 1, 7, 14 or 30 days, not {{days}}",
        days = days
    ))
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CpuStats {
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
    pub max: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryStats {
    pub p95: Option<f64>,
    pub p99: Option<f64>,
    pub max: Option<f64>,
}

/// Доля периодов CFS с троттлингом, 0–1.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThrottlingStats {
    pub mean: Option<f64>,
    pub p95: Option<f64>,
}

/// Статистика контейнера за окно: «худший под» (спец. §7.2).
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerStats {
    pub cpu: CpuStats,
    pub memory: MemoryStats,
    pub throttling: ThrottlingStats,
    /// Эпизоды OOM за окно; `None` — нет kube-state-metrics.
    pub oom_count: Option<u32>,
    /// Последний OOM (Unix-секунды).
    pub oom_last: Option<i64>,
    /// Рестарты за окно; `None` — нет kube-state-metrics.
    pub restarts: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerUsage {
    #[serde(flatten)]
    pub current: ContainerCurrent,
    pub stats: ContainerStats,
}

/// Ряд графика по контейнеру: (Unix-секунды, значение).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChartSeries {
    pub container: String,
    pub points: Vec<(i64, f64)>,
}

/// Отметка на графике: рестарты или OOM контейнера за шаг.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Marker {
    pub container: String,
    pub time: i64,
    pub count: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Charts {
    pub start: i64,
    pub end: i64,
    pub step_seconds: u64,
    pub cpu: Vec<ChartSeries>,
    pub memory: Vec<ChartSeries>,
    pub restarts: Vec<Marker>,
    pub ooms: Vec<Marker>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadUsage {
    pub namespace: String,
    pub kind: WorkloadKind,
    pub name: String,
    pub window_days: u32,
    /// Источник данных: история или `metrics.k8s.io`.
    pub source: SourceInfo,
    /// Выбор источника и отвергнутые.
    pub source_status: SourceStatus,
    /// Доля шагов окна с данными хотя бы по одному поду (0–1); у `metrics.k8s.io` — нет.
    pub coverage: Option<f64>,
    /// Сколько секунд от первой точки с данными до конца окна.
    pub data_seconds: Option<i64>,
    /// Метрики kube-state-metrics есть: поды выбраны по владельцам, OOM и рестарты известны.
    pub owner_metrics: bool,
    /// Желаемо реплик (у DaemonSet — подов).
    pub replicas: Option<i32>,
    /// Подов сверх желаемого при выкладке.
    pub surge: i32,
    pub containers: Vec<ContainerUsage>,
    pub charts: Option<Charts>,
    /// Только `metrics.k8s.io`: текущее потребление подов workload'а.
    pub pods_now: Vec<PodNow>,
}

fn failed(f: Failure) -> AppError {
    f.error
}

/// Значения показателей с меткой `stat` по контейнеру: `stat → контейнер → значение`.
fn by_stat(samples: &[Sample]) -> BTreeMap<String, BTreeMap<String, f64>> {
    let mut out: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    for s in samples {
        out.entry(s.label("stat").to_string())
            .or_default()
            .insert(s.label("container").to_string(), s.value);
    }
    out
}

fn series(list: Vec<Series>) -> Vec<ChartSeries> {
    let mut out: Vec<ChartSeries> = list
        .into_iter()
        .map(|s| ChartSeries {
            container: s.label("container").to_string(),
            points: s.points,
        })
        .collect();
    out.sort_by(|a, b| a.container.cmp(&b.container));
    out
}

fn markers(list: &[Series], event: &str) -> Vec<Marker> {
    let mut out: Vec<Marker> = list
        .iter()
        .filter(|s| s.label("event") == event)
        .flat_map(|s| {
            s.points
                .iter()
                .filter(|(_, v)| *v > 0.0)
                .map(|(t, v)| Marker {
                    container: s.label("container").to_string(),
                    time: *t,
                    count: *v,
                })
        })
        .collect();
    out.sort_by(|a, b| a.time.cmp(&b.time).then(a.container.cmp(&b.container)));
    out
}

/// Есть ли у проекта метрики kube-state-metrics.
async fn has_owner_metrics(src: &dyn MetricsSource, ns: &str, now: i64) -> Result<bool, Failure> {
    let v = query(src, ns, &promql::owner_metrics_present(ns), now).await?;
    Ok(v.first().is_some_and(|s| s.value > 0.0))
}

/// Статистика и графики по источнику истории.
async fn from_history(
    src: &dyn MetricsSource,
    pods: &Pods,
    days: u32,
    now: i64,
    owner: bool,
    containers: Vec<ContainerCurrent>,
) -> Result<(Vec<ContainerUsage>, Charts, Option<f64>, Option<i64>), Failure> {
    let ns = pods.namespace();
    let step = promql::chart_step_seconds(days);
    let start = now - i64::from(days) * 86_400;
    let q = [
        promql::workload_cpu(pods, days),
        promql::workload_memory(pods, days),
        promql::workload_throttling(pods, days),
        promql::workload_restarts(pods, days),
        promql::workload_coverage(pods, days),
        promql::workload_cpu_series(pods),
        promql::workload_memory_series(pods),
        promql::workload_events_series(pods, step),
    ];
    let (cpu, memory, throttling, restarts, coverage, cpu_series, memory_series, events) = tokio::try_join!(
        query(src, ns, &q[0], now),
        query(src, ns, &q[1], now),
        query(src, ns, &q[2], now),
        query(src, ns, &q[3], now),
        query(src, ns, &q[4], now),
        query_range(src, ns, &q[5], start, now, step),
        query_range(src, ns, &q[6], start, now, step),
        query_range(src, ns, &q[7], start, now, step),
    )?;
    let stats: BTreeMap<String, BTreeMap<String, f64>> = [cpu, memory, throttling, restarts]
        .iter()
        .flat_map(|v| by_stat(v))
        .collect();
    let get = |stat: &str, c: &str| stats.get(stat).and_then(|m| m.get(c)).copied();
    let usage = containers
        .into_iter()
        .map(|current| {
            let c = current.name.as_str();
            let count = |stat: &str| owner.then(|| get(stat, c).unwrap_or(0.0).round() as u32);
            let stats = ContainerStats {
                cpu: CpuStats {
                    p50: get("cpu_p50", c),
                    p95: get("cpu_p95", c),
                    p99: get("cpu_p99", c),
                    max: get("cpu_max", c),
                },
                memory: MemoryStats {
                    p95: get("memory_p95", c),
                    p99: get("memory_p99", c),
                    max: get("memory_max", c),
                },
                throttling: ThrottlingStats {
                    mean: get("throttling_mean", c),
                    p95: get("throttling_p95", c),
                },
                oom_count: count("oom_count"),
                oom_last: owner
                    .then(|| get("oom_last", c).map(|t| t as i64))
                    .flatten(),
                restarts: count("restarts"),
            };
            ContainerUsage { current, stats }
        })
        .collect();
    let cov = by_stat(&coverage);
    let steps = cov.get("steps").and_then(|m| m.get("")).copied();
    let first = cov.get("first").and_then(|m| m.get("")).copied();
    let all_steps = (u64::from(days) * 86_400 / promql::STEP_SECONDS) as f64;
    let charts = Charts {
        start,
        end: now,
        step_seconds: step,
        cpu: series(cpu_series),
        memory: series(memory_series),
        restarts: markers(&events, "restart"),
        ooms: markers(&events, "oom"),
    };
    Ok((
        usage,
        charts,
        Some(steps.map_or(0.0, |s| (s / all_steps).min(1.0))),
        first.map(|f| now - f as i64),
    ))
}

/// Одна точка `metrics.k8s.io`: максимум по подам workload'а.
fn from_pods_now(containers: Vec<ContainerCurrent>, now: &[PodNow]) -> Vec<ContainerUsage> {
    containers
        .into_iter()
        .map(|current| {
            let values = now
                .iter()
                .flat_map(|p| &p.containers)
                .filter(|c| c.name == current.name);
            let (cpu, memory) =
                values.fold((None, None), |(cpu, mem): (Option<f64>, Option<f64>), c| {
                    (
                        c.cpu.map(|v| cpu.map_or(v, |x| x.max(v))).or(cpu),
                        c.memory.map(|v| mem.map_or(v, |x| x.max(v))).or(mem),
                    )
                });
            ContainerUsage {
                current,
                stats: ContainerStats {
                    cpu: CpuStats {
                        max: cpu,
                        ..Default::default()
                    },
                    memory: MemoryStats {
                        max: memory,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            }
        })
        .collect()
}

/// Статистика workload'а за окно `days` по выбранному источнику; без источника истории —
/// текущие значения `metrics.k8s.io`, без них — `METRICS_UNAVAILABLE` с причинами.
pub async fn workload_usage(
    r: &ReadClient,
    sel: &Selected,
    namespace: &str,
    kind: WorkloadKind,
    name: &str,
    days: u32,
    now: i64,
) -> AppResult<WorkloadUsage> {
    let days = check_window(days)?;
    let spec = get_spec(r, namespace, kind, name).await?;
    let w = WorkloadRef {
        kind,
        name: name.to_string(),
    };
    let pods = match workload_pods(r, namespace, &w).await {
        Ok(l) => l.items,
        // Без прав на поды нет только пометки LimitRange.
        Err(e) if e.code == ErrorCode::Forbidden => Vec::new(),
        Err(e) => return Err(e),
    };
    let containers = current(spec.template.as_ref().and_then(|t| t.spec.as_ref()), &pods);
    let mut out = WorkloadUsage {
        namespace: namespace.to_string(),
        kind,
        name: name.to_string(),
        window_days: days,
        source: crate::metrics::pods::info(),
        source_status: sel.status.clone(),
        coverage: None,
        data_seconds: None,
        owner_metrics: false,
        replicas: spec.row.desired,
        surge: spec.surge,
        containers: Vec::new(),
        charts: None,
        pods_now: Vec::new(),
    };
    if let Some(src) = sel.history.as_deref() {
        let owner = has_owner_metrics(src, namespace, now)
            .await
            .map_err(failed)?;
        let selector = if owner {
            Pods::Owner {
                namespace: namespace.into(),
                kind,
                name: name.into(),
            }
        } else {
            Pods::Name {
                namespace: namespace.into(),
                kind,
                name: name.into(),
            }
        };
        let (usage, charts, coverage, data) =
            from_history(src, &selector, days, now, owner, containers)
                .await
                .map_err(failed)?;
        out.source = src.info().clone();
        out.owner_metrics = owner;
        out.containers = usage;
        out.charts = Some(charts);
        out.coverage = coverage;
        out.data_seconds = data;
        return Ok(out);
    }
    if !sel.status.pod_metrics {
        return Err(sel.status.unavailable());
    }
    let names: std::collections::BTreeSet<&str> = pods
        .iter()
        .filter_map(|p| p.metadata.name.as_deref())
        .collect();
    let now_list: Vec<PodNow> = pods_now(
        r,
        namespace,
        spec.selector.as_deref().filter(|s| !s.is_empty()),
    )
    .await
    .map_err(failed)?
    .into_iter()
    .filter(|p| names.contains(p.pod.as_str()))
    .collect();
    out.containers = from_pods_now(containers, &now_list);
    out.pods_now = now_list;
    Ok(out)
}

/// Текущее потребление контейнеров пода (вкладка «Ресурсы» пода).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodUsage {
    pub source: SourceInfo,
    pub containers: Vec<crate::metrics::pods::ContainerNow>,
}

/// Текущее потребление пода: по источнику истории, без него — `metrics.k8s.io`.
pub async fn pod_usage(
    r: &ReadClient,
    sel: &Selected,
    namespace: &str,
    pod: &str,
    now: i64,
) -> AppResult<PodUsage> {
    crate::k8s::read::check_name(pod)?;
    if let Some(src) = sel.history.as_deref() {
        let v = query(src, namespace, &promql::pod_now(namespace, pod), now)
            .await
            .map_err(failed)?;
        let stats = by_stat(&v);
        let mut names: Vec<&String> = stats.values().flat_map(|m| m.keys()).collect();
        names.sort();
        names.dedup();
        let get = |s: &str, c: &str| stats.get(s).and_then(|m| m.get(c)).copied();
        return Ok(PodUsage {
            source: src.info().clone(),
            containers: names
                .into_iter()
                .map(|c| crate::metrics::pods::ContainerNow {
                    name: c.clone(),
                    cpu: get("cpu", c),
                    memory: get("memory", c),
                })
                .collect(),
        });
    }
    if !sel.status.pod_metrics {
        return Err(sel.status.unavailable());
    }
    let now_list = pods_now(r, namespace, None).await.map_err(failed)?;
    Ok(PodUsage {
        source: crate::metrics::pods::info(),
        containers: now_list
            .into_iter()
            .find(|p| p.pod == pod)
            .map(|p| p.containers)
            .unwrap_or_default(),
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use crate::metrics::prometheus::PromApi;
    use crate::metrics::prometheus::fake::{FakeSource, matrix, vector};
    use crate::metrics::select::Mode;

    pub const NOW: i64 = 1_700_000_000;

    /// API-сервер с Deployment `web` (2 реплики, контейнер `app` с requests и limits) и его
    /// подом.
    pub fn cluster(s: &crate::k8s::testing::Seen) -> (u16, serde_json::Value) {
        match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => (
                200,
                serde_json::json!({"apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": {"name": "web", "namespace": "team-a", "resourceVersion": "7"},
                    "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "web"}},
                        "template": {"metadata": {"labels": {"app": "web"}}, "spec": {"containers": [
                            {"name": "app", "image": "web:1",
                             "resources": {"requests": {"cpu": "500m", "memory": "512Mi"},
                                           "limits": {"cpu": "1", "memory": "1Gi"}}}]}}}}),
            ),
            "/api/v1/namespaces/team-a/pods" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "PodList", "metadata": {},
                    "items": [{"metadata": {"name": "web-7d9f8c6b5d-x2x4z", "namespace": "team-a",
                        "labels": {"app": "web", "pod-template-hash": "7d9f8c6b5d"}},
                        "spec": {"containers": [{"name": "app", "image": "web:1"}]},
                        "status": {"phase": "Running"}}]}),
            ),
            "/apis/metrics.k8s.io/v1beta1/namespaces/team-a/pods" => (
                200,
                serde_json::json!({"apiVersion": "metrics.k8s.io/v1beta1", "kind": "PodMetricsList",
                    "metadata": {}, "items": [
                        {"metadata": {"name": "web-7d9f8c6b5d-x2x4z"}, "containers": [
                            {"name": "app", "usage": {"cpu": "120m", "memory": "300Mi"}}]},
                        {"metadata": {"name": "other-1"}, "containers": [
                            {"name": "app", "usage": {"cpu": "9", "memory": "9Gi"}}]}]}),
            ),
            _ => status(404, "NotFound", "no"),
        }
    }

    pub fn reader() -> ReadClient {
        ReadClient::new(mock_client(cluster).0, Duration::from_secs(5))
    }

    pub fn source_status(pod_metrics: bool) -> SourceStatus {
        SourceStatus {
            mode: Mode::Auto,
            selected: None,
            rejected: vec![],
            pod_metrics,
            checked_at_ms: 0,
        }
    }

    /// Prometheus со статистикой `app` за 7 дней.
    pub fn prometheus(owner: bool) -> FakeSource {
        FakeSource::new(move |api, q| {
            let stat = |pairs: &[(&str, f64)]| {
                let labels: Vec<[(&str, &str); 2]> = pairs
                    .iter()
                    .map(|(s, _)| [("container", "app"), ("stat", *s)])
                    .collect();
                vector(
                    &labels
                        .iter()
                        .zip(pairs)
                        .map(|(l, (_, v))| (&l[..], *v))
                        .collect::<Vec<_>>(),
                )
            };
            Ok(match api {
                PromApi::Query if q.starts_with("count(kube_pod_owner") => {
                    vector(&if owner { vec![(&[][..], 3.0)] } else { vec![] })
                }
                PromApi::Query if q.contains("\"cpu_p50\"") => stat(&[
                    ("cpu_p50", 0.1),
                    ("cpu_p95", 0.2),
                    ("cpu_p99", 0.3),
                    ("cpu_max", 0.5),
                ]),
                PromApi::Query if q.contains("\"memory_p95\"") => stat(&[
                    ("memory_p95", 300e6),
                    ("memory_p99", 350e6),
                    ("memory_max", 400e6),
                ]),
                PromApi::Query if q.contains("\"throttling_mean\"") => {
                    stat(&[("throttling_mean", 0.02), ("throttling_p95", 0.05)])
                }
                PromApi::Query if q.contains("\"oom_count\"") => {
                    if owner {
                        stat(&[
                            ("oom_count", 2.0),
                            ("oom_last", 1_699_990_000.0),
                            ("restarts", 3.0),
                        ])
                    } else {
                        vector(&[])
                    }
                }
                PromApi::Query if q.contains("\"steps\"") => vector(&[
                    (&[("stat", "steps")][..], 1814.4),
                    (&[("stat", "first")][..], (NOW - 6 * 86_400) as f64),
                ]),
                PromApi::QueryRange if q.contains("\"event\"") => matrix(&[
                    (
                        &[("container", "app"), ("event", "restart")][..],
                        &[(NOW - 100, 1.0), (NOW - 50, 0.0)][..],
                    ),
                    (
                        &[("container", "app"), ("event", "oom")][..],
                        &[(NOW - 100, 1.0)][..],
                    ),
                ]),
                PromApi::QueryRange => matrix(&[(
                    &[("container", "app")][..],
                    &[(NOW - 3024, 0.2), (NOW, 0.25)][..],
                )]),
                PromApi::Query => vector(&[]),
            })
        })
    }

    #[tokio::test]
    async fn history_gives_statistics_charts_and_coverage() {
        let src = Arc::new(prometheus(true));
        let sel = Selected {
            history: Some(src.clone()),
            status: source_status(false),
        };
        let u = workload_usage(
            &reader(),
            &sel,
            "team-a",
            WorkloadKind::Deployment,
            "web",
            7,
            NOW,
        )
        .await
        .unwrap();
        assert!(u.owner_metrics);
        assert_eq!((u.replicas, u.surge), (Some(2), 1));
        assert_eq!(u.source.location, "monitoring/prometheus-k8s:9090");
        let app = &u.containers[0];
        assert_eq!(app.current.requests.cpu, Some(0.5));
        assert_eq!(app.stats.cpu.p95, Some(0.2));
        assert_eq!(app.stats.memory.max, Some(400e6));
        assert_eq!(app.stats.throttling.mean, Some(0.02));
        assert_eq!(
            (app.stats.oom_count, app.stats.restarts),
            (Some(2), Some(3))
        );
        assert_eq!(app.stats.oom_last, Some(1_699_990_000));
        assert!((u.coverage.unwrap() - 0.9).abs() < 1e-9, "{:?}", u.coverage);
        assert_eq!(u.data_seconds, Some(6 * 86_400));
        let charts = u.charts.unwrap();
        assert_eq!(charts.step_seconds, 3024);
        assert_eq!(charts.cpu[0].points.len(), 2);
        assert_eq!(charts.restarts.len(), 1, "нули — не отметки");
        assert_eq!(charts.ooms[0].time, NOW - 100);
        // С kube-state-metrics поды выбраны по владельцу, а не по имени.
        let queries = src.queries();
        assert!(queries.iter().any(|q| q.contains("kube_replicaset_owner")));
        assert!(!queries.iter().any(|q| q.contains("pod=~")));
    }

    #[tokio::test]
    async fn without_kube_state_metrics_pods_are_chosen_by_name_and_ooms_are_unknown() {
        let src = Arc::new(prometheus(false));
        let sel = Selected {
            history: Some(src.clone()),
            status: source_status(false),
        };
        let u = workload_usage(
            &reader(),
            &sel,
            "team-a",
            WorkloadKind::Deployment,
            "web",
            7,
            NOW,
        )
        .await
        .unwrap();
        assert!(!u.owner_metrics);
        assert_eq!(u.containers[0].stats.oom_count, None);
        assert_eq!(u.containers[0].stats.restarts, None);
        assert!(src.queries().iter().any(|q| q.contains("pod=~\"web-")));
    }

    #[tokio::test]
    async fn pod_metrics_give_one_point_of_the_workload_pods() {
        let sel = Selected {
            history: None,
            status: source_status(true),
        };
        let u = workload_usage(
            &reader(),
            &sel,
            "team-a",
            WorkloadKind::Deployment,
            "web",
            7,
            NOW,
        )
        .await
        .unwrap();
        assert_eq!(
            u.source.kind,
            crate::metrics::prometheus::SourceKind::MetricsServer
        );
        assert_eq!(u.pods_now.len(), 1, "чужой под не попал");
        let app = &u.containers[0].stats;
        assert_eq!(app.cpu.max, Some(0.12));
        assert_eq!(app.cpu.p95, None);
        assert!(u.charts.is_none() && u.coverage.is_none());
    }

    #[tokio::test]
    async fn no_source_and_a_bad_window_are_errors() {
        let sel = Selected {
            history: None,
            status: source_status(false),
        };
        let e = workload_usage(
            &reader(),
            &sel,
            "team-a",
            WorkloadKind::Deployment,
            "web",
            7,
            NOW,
        )
        .await
        .unwrap_err();
        assert_eq!(e.code, ErrorCode::MetricsUnavailable);
        let e = workload_usage(
            &reader(),
            &sel,
            "team-a",
            WorkloadKind::Deployment,
            "web",
            3,
            NOW,
        )
        .await
        .unwrap_err();
        assert_eq!(e.key, "errors.resources.badWindow");
    }

    #[tokio::test]
    async fn pod_usage_from_history_or_pod_metrics() {
        let src = Arc::new(FakeSource::new(|_, q| {
            assert!(q.contains("pod=\"web-1\""), "{q}");
            Ok(vector(&[
                (&[("container", "app"), ("stat", "cpu")][..], 0.3),
                (&[("container", "app"), ("stat", "memory")][..], 1e8),
            ]))
        }));
        let sel = Selected {
            history: Some(src),
            status: source_status(false),
        };
        let p = pod_usage(&reader(), &sel, "team-a", "web-1", NOW)
            .await
            .unwrap();
        assert_eq!(p.containers[0].cpu, Some(0.3));
        let sel = Selected {
            history: None,
            status: source_status(true),
        };
        let p = pod_usage(&reader(), &sel, "team-a", "web-7d9f8c6b5d-x2x4z", NOW)
            .await
            .unwrap();
        assert_eq!(p.containers[0].memory, Some(300.0 * 1024.0 * 1024.0));
    }
}
````

<!-- block: patch t05-src step=5.2 -->
````diff
diff --git a/src-tauri/src/commands/resources.rs b/src-tauri/src/commands/resources.rs
index 001f210..ce3059b 100644
--- a/src-tauri/src/commands/resources.rs
+++ b/src-tauri/src/commands/resources.rs
@@ -1,11 +1,20 @@
-//! Ресурсы (спец. §4.2, §7): источник метрик подключения и его статус.
+//! Ресурсы (спец. §4.2, §7): источник метрик подключения, статистика workload'а и текущее
+//! потребление пода. Чтение в проекте идёт через `read_in`: 403 сбрасывает кэш прав проекта.
 
 use tauri::State;
 
 use crate::error::AppResult;
+use crate::k8s::workloads::WorkloadKind;
 use crate::metrics::select::SourceStatus;
+use crate::resources::usage::{PodUsage, WorkloadUsage};
 use crate::state::AppState;
 
+pub(crate) fn now_s() -> i64 {
+    std::time::SystemTime::now()
+        .duration_since(std::time::UNIX_EPOCH)
+        .map_or(0, |d| d.as_secs() as i64)
+}
+
 /// Какой источник метрик выбран и почему отвергнуты остальные; `refresh` — выбрать заново.
 #[tauri::command]
 pub async fn get_metrics_source_status(
@@ -22,3 +31,50 @@ pub async fn get_metrics_source_status(
         })
         .await
 }
+
+/// Статистика workload'а за окно `window_days` (1, 7, 14 или 30 дней).
+#[tauri::command]
+pub async fn get_workload_usage(
+    state: State<'_, AppState>,
+    connection_id: String,
+    namespace: String,
+    kind: WorkloadKind,
+    name: String,
+    window_days: u32,
+) -> AppResult<WorkloadUsage> {
+    let st = state.inner().clone();
+    let (id, ns) = (connection_id.clone(), namespace.clone());
+    state
+        .read_in(&connection_id, &namespace, |c| async move {
+            let sel = crate::metrics::select::for_connection(&st, &id, &c, false).await?;
+            crate::resources::usage::workload_usage(
+                &c.reader(),
+                &sel,
+                &ns,
+                kind,
+                &name,
+                window_days,
+                now_s(),
+            )
+            .await
+        })
+        .await
+}
+
+/// Текущее потребление контейнеров пода.
+#[tauri::command]
+pub async fn get_pod_usage(
+    state: State<'_, AppState>,
+    connection_id: String,
+    namespace: String,
+    pod: String,
+) -> AppResult<PodUsage> {
+    let st = state.inner().clone();
+    let (id, ns) = (connection_id.clone(), namespace.clone());
+    state
+        .read_in(&connection_id, &namespace, |c| async move {
+            let sel = crate::metrics::select::for_connection(&st, &id, &c, false).await?;
+            crate::resources::usage::pod_usage(&c.reader(), &sel, &ns, &pod, now_s()).await
+        })
+        .await
+}
diff --git a/src-tauri/src/k8s/pods.rs b/src-tauri/src/k8s/pods.rs
index 96baffc..2b067f0 100644
--- a/src-tauri/src/k8s/pods.rs
+++ b/src-tauri/src/k8s/pods.rs
@@ -336,53 +336,56 @@ pub async fn list_pods(
     namespace: &str,
     workload: Option<&WorkloadRef>,
 ) -> AppResult<Listed<PodRow>> {
-    let ns = Some(namespace);
     match workload {
         None => Ok(r
-            .list::<Pod>(ns, &ListParams::default())
+            .list::<Pod>(Some(namespace), &ListParams::default())
             .await?
             .map(|p| pod_row(&p))),
-        Some(w) if w.kind == WorkloadKind::CronJob => {
-            let all_jobs = r.list::<Job>(ns, &ListParams::default()).await?;
-            let jobs: BTreeSet<String> = all_jobs
+        Some(w) => Ok(workload_pods(r, namespace, w).await?.map(|p| pod_row(&p))),
+    }
+}
+
+/// Поды workload'а целиком: по селектору workload'а, а у CronJob — поды его Job.
+pub async fn workload_pods(
+    r: &ReadClient,
+    namespace: &str,
+    w: &WorkloadRef,
+) -> AppResult<Listed<Pod>> {
+    let ns = Some(namespace);
+    if w.kind == WorkloadKind::CronJob {
+        let all_jobs = r.list::<Job>(ns, &ListParams::default()).await?;
+        let jobs: BTreeSet<String> = all_jobs
+            .items
+            .into_iter()
+            .filter(|j| {
+                j.metadata
+                    .owner_references
+                    .iter()
+                    .flatten()
+                    .any(|o| o.kind == "CronJob" && o.name == w.name)
+            })
+            .filter_map(|j| j.metadata.name)
+            .collect();
+        let pods = r.list::<Pod>(ns, &ListParams::default()).await?;
+        return Ok(Listed {
+            truncated: all_jobs.truncated || pods.truncated,
+            items: pods
                 .items
                 .into_iter()
-                .filter(|j| {
-                    j.metadata
-                        .owner_references
-                        .iter()
-                        .flatten()
-                        .any(|o| o.kind == "CronJob" && o.name == w.name)
-                })
-                .filter_map(|j| j.metadata.name)
-                .collect();
-            let pods = r.list::<Pod>(ns, &ListParams::default()).await?;
-            let rows: Vec<PodRow> = pods
-                .items
-                .iter()
                 .filter(|p| owner_of(p).is_some_and(|o| o.kind == "Job" && jobs.contains(&o.name)))
-                .map(pod_row)
-                .collect();
-            Ok(Listed {
-                items: rows,
-                truncated: all_jobs.truncated || pods.truncated,
-            })
-        }
-        Some(w) => {
-            let selector = pod_selector(r, namespace, w.kind, &w.name).await?;
-            // Без селектора поды workload'а определить нечем; пустой селектор подошёл бы всем
-            // подам проекта, а это чужие поды.
-            let Some(selector) = selector.filter(|s| !s.is_empty()) else {
-                return Ok(Listed {
-                    items: Vec::new(),
-                    truncated: false,
-                });
-            };
-            Ok(r.list::<Pod>(ns, &by_labels(Some(&selector)))
-                .await?
-                .map(|p| pod_row(&p)))
-        }
+                .collect(),
+        });
     }
+    let selector = pod_selector(r, namespace, w.kind, &w.name).await?;
+    // Без селектора поды workload'а определить нечем; пустой селектор подошёл бы всем
+    // подам проекта, а это чужие поды.
+    let Some(selector) = selector.filter(|s| !s.is_empty()) else {
+        return Ok(Listed {
+            items: Vec::new(),
+            truncated: false,
+        });
+    };
+    r.list::<Pod>(ns, &by_labels(Some(&selector))).await
 }
 
 /// Состояние контейнера: работает, ждёт или завершён — с причиной, кодом выхода и временем.
diff --git a/src-tauri/src/k8s/workloads.rs b/src-tauri/src/k8s/workloads.rs
index ba95f3a..998e695 100644
--- a/src-tauri/src/k8s/workloads.rs
+++ b/src-tauri/src/k8s/workloads.rs
@@ -116,6 +116,8 @@ struct Parsed {
     latest_version: Option<i64>,
     /// StatefulSet: ControllerRevision, на которой работают поды (`status.currentRevision`).
     current_revision: Option<String>,
+    /// Сколько подов сверх желаемого создаёт выкладка (`maxSurge`); без выкладки с запасом — 0.
+    surge: i32,
 }
 
 fn row(kind: WorkloadKind, meta: &ObjectMeta, template: Option<&PodTemplateSpec>) -> WorkloadRow {
@@ -163,6 +165,21 @@ fn rollout_problem(
     }
 }
 
+/// `maxSurge` подами: число как есть, проценты — от желаемых реплик с округлением вверх, как
+/// у контроллера; по умолчанию 25 %.
+fn surge_pods(max_surge: Option<&IntOrString>, replicas: i32) -> i32 {
+    match max_surge {
+        Some(IntOrString::Int(n)) => (*n).max(0),
+        Some(IntOrString::String(s)) => s
+            .strip_suffix('%')
+            .and_then(|p| p.trim().parse::<i64>().ok())
+            .map_or(0, |p| {
+                ((i64::from(replicas.max(0)) * p.max(0) + 99) / 100) as i32
+            }),
+        None => (replicas.max(0) + 3) / 4,
+    }
+}
+
 fn rolling(max_surge: Option<&IntOrString>, max_unavailable: Option<&IntOrString>) -> String {
     let v = |x: Option<&IntOrString>| x.map_or_else(|| "25%".into(), int_or_string);
     format!(
@@ -196,6 +213,14 @@ fn parse_deployment(d: &Deployment) -> Parsed {
         }
     });
     let selector = spec.map(|s| selector_string(&s.selector));
+    let surge = match spec.and_then(|s| s.strategy.as_ref()) {
+        Some(st) if st.type_.as_deref() == Some("Recreate") => 0,
+        st => surge_pods(
+            st.and_then(|s| s.rolling_update.as_ref())
+                .and_then(|r| r.max_surge.as_ref()),
+            r.desired.unwrap_or(1),
+        ),
+    };
     Parsed {
         row: r,
         meta: d.metadata.clone(),
@@ -206,6 +231,7 @@ fn parse_deployment(d: &Deployment) -> Parsed {
         active_jobs: vec![],
         latest_version: None,
         current_revision: None,
+        surge,
     }
 }
 
@@ -232,6 +258,7 @@ fn parse_stateful_set(s: &StatefulSet) -> Parsed {
         active_jobs: vec![],
         latest_version: None,
         current_revision: status.and_then(|s| s.current_revision.clone()),
+        surge: 0,
     }
 }
 
@@ -258,6 +285,7 @@ fn parse_daemon_set(d: &DaemonSet) -> Parsed {
         active_jobs: vec![],
         latest_version: None,
         current_revision: None,
+        surge: 0,
     }
 }
 
@@ -283,6 +311,7 @@ fn parse_job(j: &Job) -> Parsed {
         active_jobs: vec![],
         latest_version: None,
         current_revision: None,
+        surge: 0,
     }
 }
 
@@ -311,6 +340,7 @@ fn parse_cron_job(c: &CronJob) -> Parsed {
         active_jobs: active,
         latest_version: None,
         current_revision: None,
+        surge: 0,
     }
 }
 
@@ -335,9 +365,17 @@ struct DcSpec {
 }
 
 #[derive(Debug, Default, Deserialize)]
+#[serde(rename_all = "camelCase")]
 struct DcStrategy {
     #[serde(rename = "type")]
     kind: Option<String>,
+    rolling_params: Option<DcRollingParams>,
+}
+
+#[derive(Debug, Default, Deserialize)]
+#[serde(rename_all = "camelCase")]
+struct DcRollingParams {
+    max_surge: Option<IntOrString>,
 }
 
 #[derive(Debug, Default, Deserialize)]
@@ -367,6 +405,14 @@ fn parse_deployment_config(o: &DynamicObject) -> AppResult<Parsed> {
     r.updated = Some(dc.status.updated_replicas.unwrap_or(0));
     let conds = conditions(&dc.status.conditions);
     r.problem = rollout_problem(&conds, r.desired, r.available);
+    let surge = match dc.spec.strategy.as_ref() {
+        Some(st) if st.kind.as_deref() == Some("Recreate") => 0,
+        st => surge_pods(
+            st.and_then(|s| s.rolling_params.as_ref())
+                .and_then(|p| p.max_surge.as_ref()),
+            r.desired.unwrap_or(1),
+        ),
+    };
     Ok(Parsed {
         row: r,
         meta: o.metadata.clone(),
@@ -377,6 +423,7 @@ fn parse_deployment_config(o: &DynamicObject) -> AppResult<Parsed> {
         active_jobs: vec![],
         latest_version: dc.status.latest_version,
         current_revision: None,
+        surge,
     })
 }
 
@@ -889,6 +936,38 @@ pub async fn get_workload(
     })
 }
 
+/// Шаблон подов workload'а и то, что нужно анализу ресурсов и изменениям (спец. §7.3, §9).
+#[derive(Debug, Clone)]
+pub struct WorkloadSpec {
+    pub row: WorkloadRow,
+    /// Селектор подов строкой.
+    pub selector: Option<String>,
+    pub template: Option<PodTemplateSpec>,
+    /// Подов сверх желаемого при выкладке (`maxSurge`); у StatefulSet, DaemonSet, Job и
+    /// CronJob — 0.
+    pub surge: i32,
+    pub resource_version: Option<String>,
+    /// `metadata.generation`: растёт только при изменении spec, не статуса.
+    pub generation: Option<i64>,
+}
+
+pub async fn get_spec(
+    r: &ReadClient,
+    namespace: &str,
+    kind: WorkloadKind,
+    workload: &str,
+) -> AppResult<WorkloadSpec> {
+    let p = parsed(r, namespace, kind, workload).await?;
+    Ok(WorkloadSpec {
+        resource_version: p.meta.resource_version.clone(),
+        generation: p.meta.generation,
+        row: p.row,
+        selector: p.selector,
+        template: p.template,
+        surge: p.surge,
+    })
+}
+
 /// Строка workload'а и селектор его подов без HPA, PDB и истории — для частого чтения
 /// (наблюдение MCP за workload'ом).
 pub async fn get_row(
@@ -931,6 +1010,43 @@ pub(crate) mod fixtures {
 mod tests {
     use super::fixtures::*;
     use super::*;
+
+    #[test]
+    fn surge_counts_extra_pods_of_a_rollout() {
+        let int = |n| IntOrString::Int(n);
+        let pct = |p: &str| IntOrString::String(p.into());
+        assert_eq!(surge_pods(None, 4), 1, "25 % по умолчанию");
+        assert_eq!(surge_pods(None, 1), 1, "вверх");
+        assert_eq!(surge_pods(Some(&pct("50%")), 3), 2);
+        assert_eq!(surge_pods(Some(&pct("0%")), 3), 0);
+        assert_eq!(surge_pods(Some(&int(2)), 10), 2);
+        assert_eq!(surge_pods(Some(&pct("x")), 3), 0);
+    }
+
+    #[tokio::test]
+    async fn spec_carries_template_surge_and_resource_version() {
+        let (client, _) = mock_client(|s| match s.path() {
+            "/apis/apps/v1/namespaces/team-a/deployments/web" => (
+                200,
+                serde_json::json!({"apiVersion": "apps/v1", "kind": "Deployment",
+                    "metadata": {"name": "web", "namespace": "team-a", "resourceVersion": "42", "generation": 3},
+                    "spec": {"replicas": 4, "selector": {"matchLabels": {"app": "web"}},
+                        "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": "50%"}},
+                        "template": fixtures::template("web", "web:1")}}),
+            ),
+            _ => status(404, "NotFound", "no"),
+        });
+        let r = ReadClient::new(client, std::time::Duration::from_secs(5));
+        let s = get_spec(&r, "team-a", WorkloadKind::Deployment, "web")
+            .await
+            .unwrap();
+        assert_eq!(s.surge, 2);
+        assert_eq!(s.resource_version.as_deref(), Some("42"));
+        assert_eq!(s.generation, Some(3));
+        assert_eq!(s.selector.as_deref(), Some("app=web"));
+        assert_eq!(s.row.desired, Some(4));
+        assert!(s.template.is_some());
+    }
     use crate::k8s::testing::{mock_client, status};
     use std::time::Duration;
 
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index e487184..a818fbb 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -14,6 +14,7 @@ pub mod metrics;
 pub mod platform;
 pub mod policy;
 pub mod product;
+pub mod resources;
 pub mod secrets;
 pub mod sessions;
 pub mod settings;
@@ -155,6 +156,8 @@ pub fn run() {
             commands::objects::get_quotas,
             commands::objects::get_yaml,
             commands::resources::get_metrics_source_status,
+            commands::resources::get_workload_usage,
+            commands::resources::get_pod_usage,
             commands::health::run_health_check,
             commands::health::get_health,
             commands::health::set_finding_ignored,
diff --git a/src-tauri/src/metrics/promql.rs b/src-tauri/src/metrics/promql.rs
index 66babc7..8494460 100644
--- a/src-tauri/src/metrics/promql.rs
+++ b/src-tauri/src/metrics/promql.rs
@@ -343,6 +343,26 @@ pub fn workload_events_series(p: &Pods, step_seconds: u64) -> String {
     ])
 }
 
+/// Текущее потребление контейнеров пода (вкладка «Ресурсы» пода): CPU (`stat="cpu"`, ядра)
+/// и память (`stat="memory"`, байты).
+pub fn pod_now(namespace: &str, pod: &str) -> String {
+    let m = format!(
+        "namespace={}, pod={}, {CONTAINERS}",
+        quote(namespace),
+        quote(pod)
+    );
+    either(&[
+        stat(
+            "cpu",
+            &format!("max by (container) (rate(container_cpu_usage_seconds_total{{{m}}}[{STEP}]))"),
+        ),
+        stat(
+            "memory",
+            &format!("max by (container) (container_memory_working_set_bytes{{{m}}})"),
+        ),
+    ])
+}
+
 /// Агрегаты проекта по поду и контейнеру (вкладка «Ресурсы» проекта и правила `resources.*`):
 /// без выбора workload'а — поды сопоставляет с workload'ами Rust, по имени и владельцу.
 fn by_pod(expr: &str) -> String {
@@ -595,6 +615,15 @@ mod tests {
         );
     }
 
+    #[test]
+    fn pod_now_reads_cpu_and_memory_of_one_pod() {
+        assert_eq!(
+            pod_now("team-a", "web-1"),
+            "label_replace(max by (container) (rate(container_cpu_usage_seconds_total{namespace=\"team-a\", pod=\"web-1\", container!=\"\", container!=\"POD\"}[5m])), \"stat\", \"cpu\", \"\", \"\") \
+             or label_replace(max by (container) (container_memory_working_set_bytes{namespace=\"team-a\", pod=\"web-1\", container!=\"\", container!=\"POD\"}), \"stat\", \"memory\", \"\", \"\")"
+        );
+    }
+
     #[test]
     fn project_queries_group_by_pod_and_container() {
         assert_eq!(
````

<!-- block: locales t05-locales step=5.2 -->
````json
{
  "en": {
    "errors": {
      "resources": {
        "badWindow": "The analysis window must be 1, 7, 14 or 30 days, not {{days}}"
      }
    }
  },
  "ru": {
    "errors": {
      "resources": {
        "badWindow": "Окно анализа — 1, 7, 14 или 30 дней, а не {{days}}"
      }
    }
  }
}
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 622 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm test`  
Expected: PASS — `Test Files 63 passed`, `Tests 479 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 180`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 4: Commit.**

```bash
git add \
  src-tauri/src/commands/resources.rs \
  src-tauri/src/k8s/pods.rs \
  src-tauri/src/k8s/workloads.rs \
  src-tauri/src/lib.rs \
  src-tauri/src/metrics/promql.rs \
  src-tauri/src/resources/current.rs \
  src-tauri/src/resources/mod.rs \
  src-tauri/src/resources/usage.rs \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "feat(resources): workload usage statistics over a window — CPU, memory, throttling, OOM and restarts of the worst pod, charts and coverage; current requests and limits with LimitRange defaults; current usage of a pod" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6: Движок рекомендаций requests и limits: перцентили с запасом, лимиты после троттлинга и OOM, границы LimitRange, запас квоты с репликами и выкладкой, allocatable ноды, сдвиг цели HPA, уверенность

**Files:**
- Create: `src-tauri/src/resources/constraints.rs`, `src-tauri/src/resources/recommend.rs`
- Modify: `src-tauri/src/commands/resources.rs`, `src-tauri/src/lib.rs`, `src-tauri/src/resources/mod.rs`

**Interfaces:**
- Consumes: задача 5: `WorkloadUsage`, `ContainerCurrent`, `Amounts`; O1b: `ReadClient`, квоты и `LimitRange` проекта.
- Produces: `resources::recommend::{Confidence, Field, Bound, Note (tag code), Violation (tag code), HpaShift, LimitRangeBounds, QuotaRoom, HpaTarget, Constraints, Values, ContainerRecommendation, Totals, Recommendation, recommend(&usage, &constraints)}`; `resources::constraints::{limit_range_bounds, quota_rooms, largest_node, hpa_targets, read_constraints}`; `resources::{Analysis { usage, recommendation }, analyze(r, sel, ns, kind, name, days, now, nodes)}`; команда `recommend_resources`.

Чистая функция по таблице §7.3: CPU request — p95 × 1,15 (шаг 10m, минимум 10m), CPU limit — только если он есть или его требует `LimitRange`, троттлинг p95 > 10 % — limit × 1,5; memory request — p99 × 1,15 (шаг 16Mi, минимум 32Mi), memory limit — max × 1,3, OOM — limit × 1,5. Ограничения не скрывают рекомендацию, а идут рядом: значения подтягиваются к границам `LimitRange` (пометка), запас квоты считается на реплики плюс поды выкладки, request — не больше allocatable крупнейшей ноды (если ноды читаются; 403 нод — вложенный `observe`), HPA по utilization — эквивалентная цель. Уверенность: высокая — окно и данные от 7 дней при покрытии от 80 %, средняя — от суток при 50 %, иначе низкая. Подсказки Java и Go — по именам env. Итог — разница сумм × реплик.

- [ ] **Step 1: Тесты.**

<!-- block: file t06-src-tauri-src-resources-constraints-rs-tests step=6.1 path=src-tauri/src/resources/constraints.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::k8s::testing::{mock_client, status};

    const MI: f64 = 1024.0 * 1024.0;

    fn list(kind: &str, items: serde_json::Value) -> (u16, serde_json::Value) {
        (
            200,
            serde_json::json!({"apiVersion": "v1", "kind": kind, "metadata": {}, "items": items}),
        )
    }

    #[tokio::test]
    async fn constraints_come_from_limit_ranges_quotas_nodes_and_the_hpa() {
        let (client, _) = mock_client(|s| match s.path() {
            "/api/v1/namespaces/team-a/limitranges" => list(
                "LimitRangeList",
                serde_json::json!([
                    {"metadata": {"name": "a"}, "spec": {"limits": [
                        {"type": "Container", "min": {"cpu": "10m"}, "max": {"memory": "2Gi"},
                         "default": {"cpu": "500m"}, "maxLimitRequestRatio": {"cpu": "4"}},
                        {"type": "Pod", "max": {"cpu": "1"}}]}},
                    {"metadata": {"name": "b"}, "spec": {"limits": [
                        {"type": "Container", "min": {"cpu": "50m"}, "max": {"memory": "1Gi"},
                         "default": {"cpu": "1"}}]}}
                ]),
            ),
            "/api/v1/namespaces/team-a/resourcequotas" => list(
                "ResourceQuotaList",
                serde_json::json!([{"metadata": {"name": "compute"},
                    "spec": {"hard": {"requests.cpu": "4"}},
                    "status": {"hard": {"requests.cpu": "4", "limits.memory": "8Gi", "pods": "10"},
                               "used": {"requests.cpu": "3500m"}}}]),
            ),
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers" => list(
                "HorizontalPodAutoscalerList",
                serde_json::json!([
                    {"metadata": {"name": "web"}, "spec": {"maxReplicas": 5,
                        "scaleTargetRef": {"kind": "Deployment", "name": "web", "apiVersion": "apps/v1"},
                        "metrics": [{"type": "Resource", "resource": {"name": "cpu",
                            "target": {"type": "Utilization", "averageUtilization": 80}}},
                            {"type": "Pods", "pods": {"metric": {"name": "rps"},
                            "target": {"type": "AverageValue", "averageValue": "10"}}}]}},
                    {"metadata": {"name": "other"}, "spec": {"maxReplicas": 5,
                        "scaleTargetRef": {"kind": "Deployment", "name": "api"},
                        "metrics": [{"type": "Resource", "resource": {"name": "memory",
                            "target": {"type": "Utilization", "averageUtilization": 70}}}]}}
                ]),
            ),
            "/api/v1/nodes" => list(
                "NodeList",
                serde_json::json!([
                    {"metadata": {"name": "a"}, "status": {"allocatable": {"cpu": "4", "memory": "8Gi"}}},
                    {"metadata": {"name": "b"}, "status": {"allocatable": {"cpu": "8", "memory": "4Gi"}}}
                ]),
            ),
            _ => status(404, "NotFound", "no"),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let c = read_constraints(&r, "team-a", WorkloadKind::Deployment, "web", true).await;
        let lr = c.limit_range.unwrap();
        assert_eq!(lr.min.cpu, Some(0.05), "наибольший минимум");
        assert_eq!(lr.max.memory, Some(1024.0 * MI), "наименьший максимум");
        assert_eq!(lr.default_limit.cpu, Some(0.5), "первый заданный");
        assert_eq!(lr.max_ratio.cpu, Some(4.0));
        assert_eq!(
            c.quotas
                .iter()
                .map(|q| (q.resource.as_str(), q.hard, q.used))
                .collect::<Vec<_>>(),
            [
                ("requests.cpu", 4.0, 3.5),
                ("limits.memory", 8192.0 * MI, 0.0)
            ]
        );
        assert_eq!(
            c.largest_node,
            Some(Amounts {
                cpu: Some(8.0),
                memory: Some(8192.0 * MI)
            })
        );
        assert_eq!(c.hpa.len(), 1);
        assert_eq!((c.hpa[0].resource.as_str(), c.hpa[0].target), ("cpu", 80));
        let c = read_constraints(&r, "team-a", WorkloadKind::Deployment, "web", false).await;
        assert_eq!(c.largest_node, None, "без права на ноды — не читаются");
    }

    #[tokio::test]
    async fn forbidden_parts_give_no_constraint() {
        let (client, _) = mock_client(|_| status(403, "Forbidden", "no"));
        let r = ReadClient::new(client, Duration::from_secs(5));
        let c = read_constraints(&r, "team-a", WorkloadKind::Deployment, "web", true).await;
        assert_eq!(c, Constraints::default());
    }
}
````

<!-- block: file t06-src-tauri-src-resources-recommend-rs-tests step=6.1 path=src-tauri/src/resources/recommend.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::prometheus::SourceOrigin;
    use crate::resources::current::ContainerCurrent;
    use crate::resources::usage::{ContainerStats, CpuStats, MemoryStats, ThrottlingStats};

    const MI: f64 = 1024.0 * 1024.0;

    fn src(kind: SourceKind) -> SourceInfo {
        SourceInfo {
            kind,
            origin: SourceOrigin::Auto,
            location: "x".into(),
            history: kind != SourceKind::MetricsServer,
        }
    }

    fn app(requests: Amounts, limits: Amounts, stats: ContainerStats) -> ContainerUsage {
        ContainerUsage {
            current: ContainerCurrent {
                name: "app".into(),
                init: false,
                sidecar: false,
                requests,
                limits,
                limit_range_requests: Amounts::default(),
                limit_range_limits: Amounts::default(),
                env_names: vec![],
            },
            stats,
        }
    }

    fn stats(cpu_p95: f64, cpu_p99: f64, mem_p99: f64, mem_max: f64) -> ContainerStats {
        ContainerStats {
            cpu: CpuStats {
                p50: Some(cpu_p95 / 2.0),
                p95: Some(cpu_p95),
                p99: Some(cpu_p99),
                max: Some(cpu_p99),
            },
            memory: MemoryStats {
                p95: Some(mem_p99),
                p99: Some(mem_p99),
                max: Some(mem_max),
            },
            throttling: ThrottlingStats::default(),
            oom_count: Some(0),
            oom_last: None,
            restarts: Some(0),
        }
    }

    fn usage(containers: Vec<ContainerUsage>) -> WorkloadUsage {
        WorkloadUsage {
            namespace: "team-a".into(),
            kind: WorkloadKind::Deployment,
            name: "web".into(),
            window_days: 7,
            source: src(SourceKind::Prometheus),
            source_status: crate::resources::usage::tests::source_status(false),
            coverage: Some(0.95),
            data_seconds: Some(7 * 86_400),
            owner_metrics: true,
            replicas: Some(3),
            surge: 1,
            containers,
            charts: None,
            pods_now: vec![],
        }
    }

    fn amounts(cpu: Option<f64>, memory: Option<f64>) -> Amounts {
        Amounts { cpu, memory }
    }

    fn rec(u: &WorkloadUsage) -> ContainerRecommendation {
        recommend(u, &Constraints::default()).containers.remove(0)
    }

    #[test]
    fn requests_and_limits_follow_the_percentiles_and_round_up() {
        let u = usage(vec![app(
            amounts(Some(1.0), Some(1024.0 * MI)),
            amounts(Some(2.0), Some(2048.0 * MI)),
            stats(0.2, 0.3, 300.0 * MI, 400.0 * MI),
        )]);
        let r = rec(&u);
        let v = r.recommended.unwrap();
        // 0,2 × 1,15 = 0,23; 0,3 × 1,3 = 0,39.
        assert_eq!(v.requests.cpu, Some(0.23));
        assert_eq!(v.limits.cpu, Some(0.39));
        // 300Mi × 1,15 = 345Mi → 352Mi; 400Mi × 1,3 = 520Mi → 528Mi.
        assert_eq!(v.requests.memory, Some(352.0 * MI));
        assert_eq!(v.limits.memory, Some(528.0 * MI));
        assert!(r.notes.is_empty(), "{:?}", r.notes);
        let all = recommend(&u, &Constraints::default());
        assert_eq!(all.confidence, Confidence::High);
        // (0,23 − 1) × 3 реплики.
        assert!(
            (all.totals.requests.cpu.unwrap() + 2.31).abs() < 1e-9,
            "{:?}",
            all.totals
        );
        assert_eq!(all.totals.limits.memory, Some((528.0 - 2048.0) * MI * 3.0));
    }

    #[test]
    fn small_usage_gets_the_floors() {
        let r = rec(&usage(vec![app(
            Amounts::default(),
            Amounts::default(),
            stats(0.001, 0.002, 5.0 * MI, 6.0 * MI),
        )]));
        let v = r.recommended.unwrap();
        assert_eq!(v.requests.cpu, Some(0.01), "не меньше 10m");
        assert_eq!(v.requests.memory, Some(32.0 * MI), "не меньше 32Mi");
        assert_eq!(v.limits.memory, Some(32.0 * MI), "не меньше request");
        assert_eq!(v.limits.cpu, None);
        assert_eq!(r.notes, [Note::CpuLimitNotSet]);
    }

    #[test]
    fn throttling_and_oom_raise_clipped_limits() {
        let mut s = stats(0.4, 0.5, 400.0 * MI, 500.0 * MI);
        s.throttling.p95 = Some(0.25);
        s.oom_count = Some(2);
        let r = rec(&usage(vec![app(
            amounts(Some(0.5), Some(512.0 * MI)),
            amounts(Some(0.5), Some(512.0 * MI)),
            s,
        )]));
        let v = r.recommended.unwrap();
        // max(0,5 × 1,3 = 0,65; 0,5 × 1,5 = 0,75).
        assert_eq!(v.limits.cpu, Some(0.75));
        // max(500Mi × 1,3 = 650 → 656Mi; 512Mi × 1,5 = 768Mi).
        assert_eq!(v.limits.memory, Some(768.0 * MI));
        assert_eq!(
            r.notes,
            [
                Note::CpuThrottled { p95: 0.25 },
                Note::OomKilled { count: 2 }
            ]
        );
    }

    #[test]
    fn limit_range_bounds_and_defaults() {
        let mut u = usage(vec![app(
            Amounts::default(),
            Amounts::default(),
            stats(0.001, 0.002, 5.0 * MI, 2000.0 * MI),
        )]);
        u.containers[0].current.limit_range_limits.cpu = Some(0.5);
        let lr = LimitRangeBounds {
            min: amounts(Some(0.05), None),
            max: amounts(None, Some(1024.0 * MI)),
            max_ratio: amounts(Some(4.0), None),
            default_limit: amounts(Some(0.5), None),
            ..Default::default()
        };
        let r = recommend(
            &u,
            &Constraints {
                limit_range: Some(lr),
                ..Default::default()
            },
        )
        .containers
        .remove(0);
        let v = r.recommended.unwrap();
        assert_eq!(v.requests.cpu, Some(0.05), "к min");
        assert_eq!(
            v.limits.cpu,
            Some(0.05),
            "limit задан LimitRange — считается"
        );
        assert_eq!(v.limits.memory, Some(1024.0 * MI), "к max");
        assert!(r.notes.contains(&Note::LimitRangeDefault {
            field: Field::CpuLimit
        }));
        assert!(r.notes.contains(&Note::LimitRange {
            field: Field::CpuRequest,
            bound: Bound::Min
        }));
        assert!(r.notes.contains(&Note::LimitRange {
            field: Field::MemoryLimit,
            bound: Bound::Max
        }));
        assert!(!r.notes.contains(&Note::CpuLimitNotSet));
    }

    #[test]
    fn ratio_raises_the_request() {
        let r = recommend(
            &usage(vec![app(
                amounts(Some(0.1), None),
                amounts(Some(2.0), None),
                stats(0.1, 1.5, 100.0 * MI, 100.0 * MI),
            )]),
            &Constraints {
                limit_range: Some(LimitRangeBounds {
                    max_ratio: amounts(Some(2.0), Some(1.5)),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .containers
        .remove(0);
        let v = r.recommended.unwrap();
        // limit 1,5 × 1,3 = 1,95 / 2 → request 0,98.
        assert_eq!(v.limits.cpu, Some(1.95));
        assert_eq!(v.requests.cpu, Some(0.98));
        // limit 130Mi → 144Mi; request 115 → 128Mi; 144/128 < 1,5 — без подъёма.
        assert!(r.notes.contains(&Note::LimitRange {
            field: Field::CpuRequest,
            bound: Bound::Ratio
        }));
        assert!(!r.notes.contains(&Note::LimitRange {
            field: Field::MemoryRequest,
            bound: Bound::Ratio
        }));
    }

    #[test]
    fn quota_nodes_and_hpa_are_checked_beside_the_recommendation() {
        let u = usage(vec![app(
            amounts(Some(0.1), Some(128.0 * MI)),
            amounts(None, Some(256.0 * MI)),
            stats(0.9, 1.0, 200.0 * MI, 250.0 * MI),
        )]);
        let c = Constraints {
            quotas: vec![
                QuotaRoom {
                    quota: "compute".into(),
                    resource: "requests.cpu".into(),
                    hard: 4.0,
                    used: 3.5,
                },
                QuotaRoom {
                    quota: "compute".into(),
                    resource: "pods".into(),
                    hard: 10.0,
                    used: 10.0,
                },
            ],
            largest_node: Some(amounts(Some(0.5), Some(1024.0 * MI * 1024.0))),
            hpa: vec![HpaTarget {
                hpa: "web".into(),
                resource: "cpu".into(),
                target: 80,
            }],
            ..Default::default()
        };
        let r = recommend(&u, &c);
        let v = r.containers[0].recommended.as_ref().unwrap();
        assert_eq!(v.requests.cpu, Some(1.04));
        // (1,04 − 0,1) × 3 + 1,04 × 1 = 3,86 > 0,5.
        let Violation::Quota { needed, free, .. } = &r.violations[0] else {
            panic!("{:?}", r.violations)
        };
        assert!((needed - 3.86).abs() < 1e-9 && (free - 0.5).abs() < 1e-9);
        assert!(
            matches!(&r.violations[1], Violation::NodeAllocatable { resource, .. } if resource == "cpu")
        );
        assert_eq!(
            r.violations.len(),
            2,
            "квота подов не про ресурсы контейнеров"
        );
        assert_eq!(
            r.hpa,
            [HpaShift {
                hpa: "web".into(),
                resource: "cpu".into(),
                target: 80,
                equivalent: 8
            }]
        );
    }

    #[test]
    fn confidence_by_window_data_and_coverage() {
        let p = src(SourceKind::Prometheus);
        assert_eq!(
            confidence(&p, 7, Some(7 * 86_400), Some(0.8)),
            Confidence::High
        );
        assert_eq!(
            confidence(&p, 7, Some(7 * 86_400 - 1), Some(0.9)),
            Confidence::Medium
        );
        assert_eq!(
            confidence(&p, 1, Some(86_400), Some(0.5)),
            Confidence::Medium
        );
        assert_eq!(confidence(&p, 7, Some(86_399), Some(0.9)), Confidence::Low);
        assert_eq!(
            confidence(&p, 7, Some(7 * 86_400), Some(0.49)),
            Confidence::Low
        );
        assert_eq!(
            confidence(&src(SourceKind::MetricsServer), 7, None, None),
            Confidence::Low
        );
    }

    #[test]
    fn pod_metrics_alone_recommend_from_the_current_value() {
        let mut s = ContainerStats::default();
        s.cpu.max = Some(0.2);
        s.memory.max = Some(100.0 * MI);
        let mut u = usage(vec![app(Amounts::default(), Amounts::default(), s)]);
        u.source = src(SourceKind::MetricsServer);
        u.coverage = None;
        u.data_seconds = None;
        let r = recommend(&u, &Constraints::default());
        assert_eq!(r.confidence, Confidence::Low);
        let v = r.containers[0].recommended.as_ref().unwrap();
        assert_eq!(v.requests.cpu, Some(0.23));
        assert_eq!(v.requests.memory, Some(128.0 * MI));
    }

    #[test]
    fn no_data_init_containers_and_runtime_hints() {
        let mut java = app(
            Amounts::default(),
            Amounts::default(),
            stats(0.1, 0.1, 64.0 * MI, 64.0 * MI),
        );
        java.current.env_names = vec!["JAVA_TOOL_OPTIONS".into(), "GOMEMLIMIT".into()];
        let mut empty = app(
            Amounts::default(),
            Amounts::default(),
            ContainerStats::default(),
        );
        empty.current.name = "idle".into();
        let mut init = app(
            Amounts::default(),
            Amounts::default(),
            stats(1.0, 1.0, MI, MI),
        );
        init.current.name = "migrate".into();
        init.current.init = true;
        let r = recommend(&usage(vec![init, java, empty]), &Constraints::default());
        assert_eq!(r.containers.len(), 2, "init-контейнер без рекомендации");
        assert!(r.containers[0].notes.contains(&Note::Java));
        assert!(r.containers[0].notes.contains(&Note::Go));
        assert_eq!(r.containers[1].recommended, None);
        assert_eq!(r.containers[1].notes, [Note::NoData]);
        assert!(
            r.totals.requests.cpu.is_some(),
            "итог — по контейнерам с рекомендацией"
        );
    }
}
````

- [ ] **Step 2: Реализация.**

<!-- block: file t06-src-tauri-src-resources-constraints-rs-impl step=6.2 path=src-tauri/src/resources/constraints.rs part=impl -->
````rust
//! Ограничения рекомендации из кластера (спец. §7.3): `LimitRange` и `ResourceQuota` проекта,
//! allocatable самой крупной ноды (если ноды читаются), HPA workload'а по utilization. Что не
//! прочиталось, то не проверяется: рекомендация без него остаётся.

use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::{LimitRange, Node, ResourceQuota};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::ListParams;

use super::current::Amounts;
use super::recommend::{Constraints, HpaTarget, LimitRangeBounds, QuotaRoom};
use crate::k8s::quantity::parse_quantity;
use crate::k8s::read::ReadClient;
use crate::k8s::workloads::WorkloadKind;

type Quantities = std::collections::BTreeMap<String, Quantity>;

fn amounts(m: Option<&Quantities>) -> Amounts {
    let q = |k: &str| m.and_then(|m| m.get(k)).and_then(|v| parse_quantity(&v.0));
    Amounts {
        cpu: q("cpu"),
        memory: q("memory"),
    }
}

/// Пара значений: из всех `LimitRange` — самое строгое (`pick`) или первое заданное.
fn merge(a: Amounts, b: Amounts, pick: fn(f64, f64) -> f64) -> Amounts {
    let one = |x: Option<f64>, y: Option<f64>| match (x, y) {
        (Some(x), Some(y)) => Some(pick(x, y)),
        (x, y) => x.or(y),
    };
    Amounts {
        cpu: one(a.cpu, b.cpu),
        memory: one(a.memory, b.memory),
    }
}

/// Границы контейнера (`type: Container`) по всем `LimitRange` проекта: наибольший минимум,
/// наименьший максимум и предел отношения, значения по умолчанию — первые заданные.
pub fn limit_range_bounds(list: &[LimitRange]) -> Option<LimitRangeBounds> {
    let items: Vec<_> = list
        .iter()
        .filter_map(|l| l.spec.as_ref())
        .flat_map(|s| &s.limits)
        .filter(|i| i.type_ == "Container")
        .collect();
    if items.is_empty() {
        return None;
    }
    let mut b = LimitRangeBounds::default();
    for i in items {
        b.min = merge(b.min, amounts(i.min.as_ref()), f64::max);
        b.max = merge(b.max, amounts(i.max.as_ref()), f64::min);
        b.max_ratio = merge(
            b.max_ratio,
            amounts(i.max_limit_request_ratio.as_ref()),
            f64::min,
        );
        b.default_limit = merge(b.default_limit, amounts(i.default.as_ref()), |x, _| x);
        b.default_request = merge(
            b.default_request,
            amounts(i.default_request.as_ref()),
            |x, _| x,
        );
    }
    Some(b)
}

/// Ресурсы квот, которые меняет рекомендация, с лимитом и занятым.
pub fn quota_rooms(list: &[ResourceQuota]) -> Vec<QuotaRoom> {
    const KEYS: [&str; 6] = [
        "requests.cpu",
        "cpu",
        "requests.memory",
        "memory",
        "limits.cpu",
        "limits.memory",
    ];
    let mut out = Vec::new();
    for q in list {
        let status = q.status.as_ref();
        let hard = status
            .and_then(|s| s.hard.as_ref())
            .or_else(|| q.spec.as_ref().and_then(|s| s.hard.as_ref()));
        let used = status.and_then(|s| s.used.as_ref());
        for key in KEYS {
            let h = hard
                .and_then(|h| h.get(key))
                .and_then(|v| parse_quantity(&v.0));
            if let Some(hard) = h {
                out.push(QuotaRoom {
                    quota: q.metadata.name.clone().unwrap_or_default(),
                    resource: key.to_string(),
                    hard,
                    used: used
                        .and_then(|u| u.get(key))
                        .and_then(|v| parse_quantity(&v.0))
                        .unwrap_or(0.0),
                });
            }
        }
    }
    out
}

/// Allocatable самой крупной ноды: CPU и память — каждый по своей самой крупной.
pub fn largest_node(list: &[Node]) -> Option<Amounts> {
    list.iter()
        .map(|n| amounts(n.status.as_ref().and_then(|s| s.allocatable.as_ref())))
        .reduce(|a, b| merge(a, b, f64::max))
}

/// HPA workload'а по utilization CPU и памяти.
pub fn hpa_targets(
    list: &[HorizontalPodAutoscaler],
    kind: WorkloadKind,
    name: &str,
) -> Vec<HpaTarget> {
    let kind = format!("{kind:?}");
    list.iter()
        .filter_map(|h| Some((h.metadata.name.clone()?, h.spec.as_ref()?)))
        .filter(|(_, s)| s.scale_target_ref.kind == kind && s.scale_target_ref.name == name)
        .flat_map(|(hpa, s)| {
            s.metrics.iter().flatten().filter_map(move |m| {
                let r = m.resource.as_ref()?;
                let target = r.target.average_utilization?;
                Some(HpaTarget {
                    hpa: hpa.clone(),
                    resource: r.name.clone(),
                    target,
                })
            })
        })
        .collect()
}

/// Ограничения из кластера. Ноды читаются, только если `nodes` (право есть); их отказ, как и
/// отказы проекта, ограничение не даёт. Чтение нод — вне проекта: его 403 не сбрасывает кэш прав
/// проекта (`observe`).
pub async fn read_constraints(
    r: &ReadClient,
    namespace: &str,
    kind: WorkloadKind,
    name: &str,
    nodes: bool,
) -> Constraints {
    let ns = Some(namespace);
    let all = ListParams::default();
    let (lr, quotas, hpa, node_list) = tokio::join!(
        r.list::<LimitRange>(ns, &all),
        r.list::<ResourceQuota>(ns, &all),
        r.list::<HorizontalPodAutoscaler>(ns, &all),
        async {
            if !nodes {
                return None;
            }
            crate::k8s::read::observe(r.list_cluster::<Node>(&all))
                .await
                .0
                .ok()
        },
    );
    Constraints {
        limit_range: lr.ok().and_then(|l| limit_range_bounds(&l.items)),
        quotas: quotas.map(|l| quota_rooms(&l.items)).unwrap_or_default(),
        largest_node: node_list.and_then(|l| largest_node(&l.items)),
        hpa: hpa
            .map(|l| hpa_targets(&l.items, kind, name))
            .unwrap_or_default(),
    }
}
````

<!-- block: file t06-src-tauri-src-resources-recommend-rs-impl step=6.2 path=src-tauri/src/resources/recommend.rs part=impl -->
````rust
//! Движок рекомендаций `requests` / `limits` (спец. §7.3) — чистая функция над статистикой
//! workload'а и ограничениями проекта; коэффициенты — константы модуля, в MVP не
//! настраиваются. Нарушение ограничения рекомендацию не скрывает, а показывается рядом.

use serde::Serialize;

use super::current::Amounts;
use super::usage::{ContainerUsage, WorkloadUsage};
use crate::k8s::workloads::WorkloadKind;
use crate::metrics::prometheus::{SourceInfo, SourceKind};

/// CPU request — p95 с запасом.
pub const CPU_REQUEST_FACTOR: f64 = 1.15;
/// CPU limit — p99 с запасом.
pub const CPU_LIMIT_FACTOR: f64 = 1.3;
/// Memory request — p99 с запасом.
pub const MEMORY_REQUEST_FACTOR: f64 = 1.15;
/// Memory limit — максимум с запасом.
pub const MEMORY_LIMIT_FACTOR: f64 = 1.3;
/// Лимит, который обрезал потребление (троттлинг, OOM), растёт не меньше чем в полтора раза.
pub const CLIPPED_LIMIT_FACTOR: f64 = 1.5;
/// Троттлинг p95, выше которого потребление CPU считается ограниченным лимитом.
pub const THROTTLED_P95: f64 = 0.10;
/// Шаг CPU — 10m, не меньше 10m.
pub const CPU_STEP: f64 = 0.01;
/// Шаг памяти — 16Mi, request не меньше 32Mi.
pub const MEMORY_STEP: f64 = 16.0 * 1024.0 * 1024.0;
pub const MEMORY_MIN: f64 = 32.0 * 1024.0 * 1024.0;

/// Переменные окружения, по которым видна JVM.
pub const JAVA_ENV: [&str; 3] = ["JAVA_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS"];

/// Уверенность рекомендации (спец. §7.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Confidence {
    Low,
    Medium,
    High,
}

/// Поле ресурсов контейнера.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Field {
    CpuRequest,
    CpuLimit,
    MemoryRequest,
    MemoryLimit,
}

/// Граница `LimitRange`, к которой подтянуто значение.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Bound {
    Min,
    Max,
    /// `maxLimitRequestRatio`: request поднят, чтобы limit / request не превышал предел.
    Ratio,
}

/// Пометка к рекомендации контейнера.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "code", rename_all = "camelCase")]
pub enum Note {
    /// CPU limit не задан, и `LimitRange` его не требует: троттлинга нет, limit не добавляется.
    CpuLimitNotSet,
    /// Троттлинг p95 выше 10 %: наблюдаемое потребление CPU ограничено лимитом.
    CpuThrottled { p95: f64 },
    /// OOMKilled в окне: наблюдаемое потребление памяти обрезано лимитом.
    OomKilled { count: u32 },
    /// Данных о контейнере за окно нет: рекомендации нет.
    NoData,
    /// Значение подтянуто к границе `LimitRange`.
    LimitRange { field: Field, bound: Bound },
    /// Текущее значение пришло от `LimitRange` по умолчанию, в шаблоне его нет.
    LimitRangeDefault { field: Field },
    /// JVM: проверить `-Xmx` / `MaxRAMPercentage` вместе с лимитом памяти.
    Java,
    /// Go: проверить `GOMEMLIMIT` вместе с лимитом памяти.
    Go,
}

/// Нарушение ограничения проекта или кластера.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "code", rename_all = "camelCase")]
pub enum Violation {
    /// Запаса квоты не хватит: `needed` сверх занятого с учётом реплик и выкладки, `free` —
    /// свободно (в ядрах или байтах).
    Quota {
        quota: String,
        resource: String,
        needed: f64,
        free: f64,
    },
    /// Request больше allocatable самой крупной ноды.
    NodeAllocatable {
        container: String,
        resource: String,
        request: f64,
        allocatable: f64,
    },
}

/// HPA по utilization: смена request сдвигает порог масштабирования (спец. §7.3).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HpaShift {
    pub hpa: String,
    pub resource: String,
    /// Текущий target utilization, %.
    pub target: i32,
    /// Target при рекомендованных requests, при котором HPA масштабирует как сейчас, %.
    pub equivalent: i32,
}

/// Границы `LimitRange` для контейнера (`type: Container`), сведённые по всем объектам проекта.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitRangeBounds {
    pub min: Amounts,
    pub max: Amounts,
    pub default_limit: Amounts,
    pub default_request: Amounts,
    pub max_ratio: Amounts,
}

impl LimitRangeBounds {
    /// `LimitRange` требует CPU limit: задан максимум, limit по умолчанию или предел отношения.
    fn requires_cpu_limit(&self) -> bool {
        self.max.cpu.is_some() || self.default_limit.cpu.is_some() || self.max_ratio.cpu.is_some()
    }
}

/// Запас `ResourceQuota`: ресурс `requests.cpu`, `limits.memory`… — лимит и занято.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaRoom {
    pub quota: String,
    pub resource: String,
    pub hard: f64,
    pub used: f64,
}

/// HPA workload'а по utilization ресурса.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HpaTarget {
    pub hpa: String,
    pub resource: String,
    pub target: i32,
}

/// Ограничения проекта и кластера (спец. §7.3).
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Constraints {
    pub limit_range: Option<LimitRangeBounds>,
    pub quotas: Vec<QuotaRoom>,
    /// Allocatable самой крупной ноды (по CPU и по памяти отдельно); `None` — ноды не читаются.
    pub largest_node: Option<Amounts>,
    pub hpa: Vec<HpaTarget>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Values {
    pub requests: Amounts,
    pub limits: Amounts,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerRecommendation {
    pub name: String,
    /// Действующие сейчас значения: шаблон, а без значения в нём — `LimitRange` по умолчанию.
    pub current: Values,
    /// Рекомендация; `None` — данных нет.
    pub recommended: Option<Values>,
    pub notes: Vec<Note>,
}

/// Разница суммарных requests и limits по workload'у с учётом реплик.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Totals {
    pub requests: Amounts,
    pub limits: Amounts,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Recommendation {
    pub namespace: String,
    pub kind: WorkloadKind,
    pub name: String,
    pub window_days: u32,
    pub source: SourceInfo,
    pub confidence: Confidence,
    pub coverage: Option<f64>,
    pub data_seconds: Option<i64>,
    pub replicas: Option<i32>,
    pub containers: Vec<ContainerRecommendation>,
    pub violations: Vec<Violation>,
    pub hpa: Vec<HpaShift>,
    /// Рекомендовано минус сейчас, × реплик: «−1,2 ядра, +512 Mi».
    pub totals: Totals,
}

/// Вверх до шага.
fn round_up(v: f64, step: f64) -> f64 {
    ((v / step) - 1e-9).ceil().max(0.0) * step
}

fn cpu(v: f64) -> f64 {
    // Округление до 10m считается в миллиядрах: доли ядра в двоичной записи неточны.
    (round_up(v, CPU_STEP).max(CPU_STEP) * 1000.0).round() / 1000.0
}

fn memory(v: f64) -> f64 {
    round_up(v, MEMORY_STEP)
}

/// Уверенность по окну, данным и покрытию (спец. §7.3).
pub fn confidence(
    source: &SourceInfo,
    days: u32,
    data_seconds: Option<i64>,
    coverage: Option<f64>,
) -> Confidence {
    if source.kind == SourceKind::MetricsServer {
        return Confidence::Low;
    }
    let data = data_seconds.unwrap_or(0);
    let cov = coverage.unwrap_or(0.0);
    if days >= 7 && data >= 7 * 86_400 && cov >= 0.8 {
        Confidence::High
    } else if data >= 86_400 && cov >= 0.5 {
        Confidence::Medium
    } else {
        Confidence::Low
    }
}

/// Подтягивает значение к границам `LimitRange`, с пометкой.
fn clamp(v: f64, min: Option<f64>, max: Option<f64>, field: Field, notes: &mut Vec<Note>) -> f64 {
    if let Some(m) = min
        && v < m
    {
        notes.push(Note::LimitRange {
            field,
            bound: Bound::Min,
        });
        return m;
    }
    if let Some(m) = max
        && v > m
    {
        notes.push(Note::LimitRange {
            field,
            bound: Bound::Max,
        });
        return m;
    }
    v
}

fn container(u: &ContainerUsage, lr: Option<&LimitRangeBounds>) -> ContainerRecommendation {
    let c = &u.current;
    let s = &u.stats;
    let current = Values {
        requests: c.effective_requests(),
        limits: c.effective_limits(),
    };
    let mut notes = Vec::new();
    for (field, from_lr) in [
        (Field::CpuRequest, c.limit_range_requests.cpu),
        (Field::CpuLimit, c.limit_range_limits.cpu),
        (Field::MemoryRequest, c.limit_range_requests.memory),
        (Field::MemoryLimit, c.limit_range_limits.memory),
    ] {
        if from_lr.is_some() {
            notes.push(Note::LimitRangeDefault { field });
        }
    }
    // Без перцентилей (только metrics.k8s.io) — по текущему значению.
    let cpu_p95 = s.cpu.p95.or(s.cpu.max);
    let cpu_p99 = s.cpu.p99.or(s.cpu.max);
    let mem_p99 = s.memory.p99.or(s.memory.max);
    let mem_max = s.memory.max;
    let (Some(cpu_p95), Some(cpu_p99), Some(mem_p99), Some(mem_max)) =
        (cpu_p95, cpu_p99, mem_p99, mem_max)
    else {
        notes.push(Note::NoData);
        return ContainerRecommendation {
            name: c.name.clone(),
            current,
            recommended: None,
            notes,
        };
    };
    let min = lr.map(|l| l.min).unwrap_or_default();
    let max = lr.map(|l| l.max).unwrap_or_default();
    let mut cpu_request = clamp(
        cpu(cpu_p95 * CPU_REQUEST_FACTOR),
        min.cpu,
        max.cpu,
        Field::CpuRequest,
        &mut notes,
    );
    let requires_limit = lr.is_some_and(LimitRangeBounds::requires_cpu_limit);
    let cpu_limit = match current.limits.cpu {
        None if !requires_limit => {
            notes.push(Note::CpuLimitNotSet);
            None
        }
        limit => {
            let mut v = cpu((cpu_p99 * CPU_LIMIT_FACTOR).max(cpu_request));
            if let (Some(p95), Some(l)) = (s.throttling.p95, limit)
                && p95 > THROTTLED_P95
            {
                notes.push(Note::CpuThrottled { p95 });
                v = v.max(cpu(l * CLIPPED_LIMIT_FACTOR));
            }
            Some(clamp(v, min.cpu, max.cpu, Field::CpuLimit, &mut notes))
        }
    };
    let mut memory_request = clamp(
        memory(mem_p99 * MEMORY_REQUEST_FACTOR).max(MEMORY_MIN),
        min.memory,
        max.memory,
        Field::MemoryRequest,
        &mut notes,
    );
    let mut memory_limit = memory((mem_max * MEMORY_LIMIT_FACTOR).max(memory_request));
    if let Some(n) = s.oom_count.filter(|n| *n > 0) {
        notes.push(Note::OomKilled { count: n });
        if let Some(l) = current.limits.memory {
            memory_limit = memory_limit.max(memory(l * CLIPPED_LIMIT_FACTOR));
        }
    }
    let memory_limit = clamp(
        memory_limit,
        min.memory,
        max.memory,
        Field::MemoryLimit,
        &mut notes,
    );
    // Предел отношения limit / request: request поднимается.
    if let Some(ratio) = lr.and_then(|l| l.max_ratio.cpu)
        && let Some(l) = cpu_limit
        && l / cpu_request > ratio
    {
        cpu_request = cpu(l / ratio);
        notes.push(Note::LimitRange {
            field: Field::CpuRequest,
            bound: Bound::Ratio,
        });
    }
    if let Some(ratio) = lr.and_then(|l| l.max_ratio.memory)
        && memory_limit / memory_request > ratio
    {
        memory_request = memory(memory_limit / ratio);
        notes.push(Note::LimitRange {
            field: Field::MemoryRequest,
            bound: Bound::Ratio,
        });
    }
    if c.env_names.iter().any(|n| JAVA_ENV.contains(&n.as_str())) {
        notes.push(Note::Java);
    }
    if c.env_names.iter().any(|n| n == "GOMEMLIMIT") {
        notes.push(Note::Go);
    }
    ContainerRecommendation {
        name: c.name.clone(),
        current,
        recommended: Some(Values {
            requests: Amounts {
                cpu: Some(cpu_request),
                memory: Some(memory_request),
            },
            limits: Amounts {
                cpu: cpu_limit,
                memory: Some(memory_limit),
            },
        }),
        notes,
    }
}

fn sum(list: &[ContainerRecommendation], f: impl Fn(&Values) -> Option<f64>, new: bool) -> f64 {
    list.iter()
        .filter_map(|c| {
            let v = if new {
                c.recommended.as_ref()
            } else {
                Some(&c.current)
            }?;
            f(v)
        })
        .sum()
}

/// Значение после рекомендации: рекомендованное, а без неё — текущее.
fn after(c: &ContainerRecommendation, f: impl Fn(&Values) -> Option<f64>) -> f64 {
    c.recommended
        .as_ref()
        .and_then(&f)
        .or(f(&c.current))
        .unwrap_or(0.0)
}

/// Рекомендация по статистике workload'а и ограничениям.
pub fn recommend(u: &WorkloadUsage, c: &Constraints) -> Recommendation {
    let containers: Vec<ContainerRecommendation> = u
        .containers
        .iter()
        .filter(|x| x.current.recommended())
        .map(|x| container(x, c.limit_range.as_ref()))
        .collect();
    let replicas = f64::from(u.replicas.unwrap_or(1).max(0));
    let surge = f64::from(u.surge.max(0));
    let with: Vec<&ContainerRecommendation> = containers
        .iter()
        .filter(|x| x.recommended.is_some())
        .collect();
    let total = |f: &dyn Fn(&Values) -> Option<f64>| -> Option<f64> {
        let new: f64 = with
            .iter()
            .map(|x| x.recommended.as_ref().and_then(f).unwrap_or(0.0))
            .sum();
        let old: f64 = with.iter().map(|x| f(&x.current).unwrap_or(0.0)).sum();
        (!with.is_empty()).then_some((new - old) * replicas)
    };
    let totals = Totals {
        requests: Amounts {
            cpu: total(&|v| v.requests.cpu),
            memory: total(&|v| v.requests.memory),
        },
        limits: Amounts {
            cpu: total(&|v| v.limits.cpu),
            memory: total(&|v| v.limits.memory),
        },
    };
    let mut violations = Vec::new();
    // Квота: прирост за все реплики плюс поды выкладки с новыми значениями.
    for q in &c.quotas {
        let pick: &dyn Fn(&Values) -> Option<f64> = match q.resource.as_str() {
            "requests.cpu" | "cpu" => &|v| v.requests.cpu,
            "requests.memory" | "memory" => &|v| v.requests.memory,
            "limits.cpu" => &|v| v.limits.cpu,
            "limits.memory" => &|v| v.limits.memory,
            _ => continue,
        };
        let new: f64 = containers.iter().map(|x| after(x, pick)).sum();
        let old: f64 = containers
            .iter()
            .map(|x| pick(&x.current).unwrap_or(0.0))
            .sum();
        let needed = (new - old) * replicas + new * surge;
        let free = q.hard - q.used;
        if needed > free + 1e-9 {
            violations.push(Violation::Quota {
                quota: q.quota.clone(),
                resource: q.resource.clone(),
                needed,
                free,
            });
        }
    }
    if let Some(node) = c.largest_node {
        for x in &with {
            let r = x
                .recommended
                .as_ref()
                .map(|v| v.requests)
                .unwrap_or_default();
            for (resource, req, alloc) in
                [("cpu", r.cpu, node.cpu), ("memory", r.memory, node.memory)]
            {
                if let (Some(req), Some(alloc)) = (req, alloc)
                    && req > alloc
                {
                    violations.push(Violation::NodeAllocatable {
                        container: x.name.clone(),
                        resource: resource.into(),
                        request: req,
                        allocatable: alloc,
                    });
                }
            }
        }
    }
    let hpa = c
        .hpa
        .iter()
        .filter_map(|h| {
            let pick: &dyn Fn(&Values) -> Option<f64> = match h.resource.as_str() {
                "cpu" => &|v| v.requests.cpu,
                "memory" => &|v| v.requests.memory,
                _ => return None,
            };
            let old = sum(&containers, pick, false);
            let new: f64 = containers.iter().map(|x| after(x, pick)).sum();
            (old > 0.0 && new > 0.0 && (new - old).abs() > 1e-9).then(|| HpaShift {
                hpa: h.hpa.clone(),
                resource: h.resource.clone(),
                target: h.target,
                equivalent: (f64::from(h.target) * old / new).round() as i32,
            })
        })
        .collect();
    Recommendation {
        namespace: u.namespace.clone(),
        kind: u.kind,
        name: u.name.clone(),
        window_days: u.window_days,
        confidence: confidence(&u.source, u.window_days, u.data_seconds, u.coverage),
        source: u.source.clone(),
        coverage: u.coverage,
        data_seconds: u.data_seconds,
        replicas: u.replicas,
        containers,
        violations,
        hpa,
        totals,
    }
}
````

<!-- block: patch t06-src step=6.2 -->
````diff
diff --git a/src-tauri/src/commands/resources.rs b/src-tauri/src/commands/resources.rs
index ce3059b..bd8ab69 100644
--- a/src-tauri/src/commands/resources.rs
+++ b/src-tauri/src/commands/resources.rs
@@ -6,6 +6,7 @@ use tauri::State;
 use crate::error::AppResult;
 use crate::k8s::workloads::WorkloadKind;
 use crate::metrics::select::SourceStatus;
+use crate::resources::Analysis;
 use crate::resources::usage::{PodUsage, WorkloadUsage};
 use crate::state::AppState;
 
@@ -78,3 +79,34 @@ pub async fn get_pod_usage(
         })
         .await
 }
+
+/// Статистика workload'а и рекомендация requests и limits (вкладка «Ресурсы» workload'а).
+#[tauri::command]
+pub async fn recommend_resources(
+    state: State<'_, AppState>,
+    connection_id: String,
+    namespace: String,
+    kind: WorkloadKind,
+    name: String,
+    window_days: u32,
+) -> AppResult<Analysis> {
+    let st = state.inner().clone();
+    let (id, ns) = (connection_id.clone(), namespace.clone());
+    state
+        .read_in(&connection_id, &namespace, |c| async move {
+            let sel = crate::metrics::select::for_connection(&st, &id, &c, false).await?;
+            let nodes = c.info.capabilities.list_nodes;
+            crate::resources::analyze(
+                &c.reader(),
+                &sel,
+                &ns,
+                kind,
+                &name,
+                window_days,
+                now_s(),
+                nodes,
+            )
+            .await
+        })
+        .await
+}
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index a818fbb..377d7f6 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -158,6 +158,7 @@ pub fn run() {
             commands::resources::get_metrics_source_status,
             commands::resources::get_workload_usage,
             commands::resources::get_pod_usage,
+            commands::resources::recommend_resources,
             commands::health::run_health_check,
             commands::health::get_health,
             commands::health::set_finding_ignored,
diff --git a/src-tauri/src/resources/mod.rs b/src-tauri/src/resources/mod.rs
index 973c2b0..b455f83 100644
--- a/src-tauri/src/resources/mod.rs
+++ b/src-tauri/src/resources/mod.rs
@@ -1,5 +1,42 @@
 //! Анализ ресурсов (спец. §7): текущие requests и limits, статистика потребления workload'а и
-//! пода по выбранному источнику метрик.
+//! пода по выбранному источнику метрик, движок рекомендаций и ограничения кластера.
 
+pub mod constraints;
 pub mod current;
+pub mod recommend;
 pub mod usage;
+
+use serde::Serialize;
+
+use crate::error::AppResult;
+use crate::k8s::read::ReadClient;
+use crate::k8s::workloads::WorkloadKind;
+use crate::metrics::select::Selected;
+
+/// Статистика workload'а и рекомендация по ней (вкладка «Ресурсы» workload'а).
+#[derive(Debug, Clone, Serialize)]
+#[serde(rename_all = "camelCase")]
+pub struct Analysis {
+    pub usage: usage::WorkloadUsage,
+    pub recommendation: recommend::Recommendation,
+}
+
+/// Статистика за окно и рекомендация с ограничениями кластера; `nodes` — ноды читаются.
+#[allow(clippy::too_many_arguments)]
+pub async fn analyze(
+    r: &ReadClient,
+    sel: &Selected,
+    namespace: &str,
+    kind: WorkloadKind,
+    name: &str,
+    days: u32,
+    now: i64,
+    nodes: bool,
+) -> AppResult<Analysis> {
+    let usage = usage::workload_usage(r, sel, namespace, kind, name, days, now).await?;
+    let c = constraints::read_constraints(r, namespace, kind, name, nodes).await;
+    Ok(Analysis {
+        recommendation: recommend::recommend(&usage, &c),
+        usage,
+    })
+}
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 633 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 180`

- [ ] **Step 4: Commit.**

```bash
git add \
  src-tauri/src/commands/resources.rs \
  src-tauri/src/lib.rs \
  src-tauri/src/resources/constraints.rs \
  src-tauri/src/resources/mod.rs \
  src-tauri/src/resources/recommend.rs
git commit -s -m "feat(resources): recommendation engine for requests and limits — percentiles with headroom, clipped limits after throttling and OOM, LimitRange bounds, quota room with replicas and surge, node allocatable, HPA target shift, confidence" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 7: Ресурсы проекта: суммы работающих подов против потребления худшего пода за окно, резерв, риски и избыток по workload'ам, запас квот; фоновая сессия с дедлайном 45 с и кэшем на 5 минут

**Files:**
- Create: `src-tauri/src/resources/project.rs`
- Modify: `src-tauri/src/commands/resources.rs`, `src-tauri/src/k8s/events.rs`, `src-tauri/src/lib.rs`, `src-tauri/src/resources/mod.rs`, `src-tauri/src/state.rs`

**Interfaces:**
- Consumes: задачи 3–6: `promql::project_*`, `MetricsSource`, `Amounts`, `QuotaRoom`; O1b: `sessions`, `KindError`, `k8s::events`.
- Produces: `resources::project::{PROJECT_DEADLINE = 45 s, PROJECT_TTL = 5 min, THROTTLED_MEAN = 0.25, MEMORY_NEAR_LIMIT = 0.90, Use, pod_container_use, Risk (tag code), Excess, at_least, assess, workload_of, kind_of, owners, WorkloadResources { kind, name, pods, requests, limits, usage, reserve, risks, excess } + actionable(), ProjectUsage, summarize, project_usage, ProjectCache}`; `k8s::events::pod_of`; `AppState.resources: Arc<ProjectCache>`; `commands::resources::{ProjectUsageEvent::{Done, Failed}, get_project_usage(+_impl)}`.

Четыре агрегирующих запроса по namespace (p95 CPU, максимум памяти, средний троттлинг, OOM по поду и контейнеру) сопоставляются с workload'ами по владельцам подов. Риски — троттлинг выше 25 % у контейнера с CPU limit, OOM, память ≥ 90 % лимита; избыток — request ≥ 3 × p95 CPU и на 0,5 ядра больше, или ≥ 2 × максимума памяти и на 512Mi больше; `actionable` — фильтр «только с рекомендациями» (решение пользователя: «есть риск или резерв»). Сбор — фоновая сессия: итог одним событием `Channel`, отмена `stop_session`, кэш по подключению, проекту и окну.

- [ ] **Step 1: Тесты.**

<!-- block: file t07-src-tauri-src-resources-project-rs-tests step=7.1 path=src-tauri/src/resources/project.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration as StdDuration;

    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use crate::metrics::prometheus::fake::{FakeSource, vector};
    use crate::resources::usage::tests::source_status;

    const MI: f64 = 1024.0 * 1024.0;

    fn pod(
        name: &str,
        owner: (&str, &str),
        labels: serde_json::Value,
        phase: &str,
        req: (&str, &str),
        lim: Option<(&str, &str)>,
    ) -> serde_json::Value {
        let mut res = serde_json::json!({"requests": {"cpu": req.0, "memory": req.1}});
        if let Some((c, m)) = lim {
            res["limits"] = serde_json::json!({"cpu": c, "memory": m});
        }
        serde_json::json!({"metadata": {"name": name, "namespace": "team-a", "labels": labels,
            "ownerReferences": [{"apiVersion": "apps/v1", "kind": owner.0, "name": owner.1,
                "uid": "u", "controller": true}]},
            "spec": {"containers": [{"name": "app", "image": "a", "resources": res}]},
            "status": {"phase": phase}})
    }

    fn cluster(s: &crate::k8s::testing::Seen) -> (u16, serde_json::Value) {
        let list = |kind: &str, items: serde_json::Value| {
            (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": kind, "metadata": {}, "items": items}),
            )
        };
        let hash = serde_json::json!({"app": "web", "pod-template-hash": "7d9f8c6b5d"});
        match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments" => list(
                "DeploymentList",
                serde_json::json!([{"metadata": {"name": "web"}, "spec": {"replicas": 2,
                    "selector": {"matchLabels": {"app": "web"}},
                    "template": crate::k8s::workloads::fixtures::template("web", "web:1")}}]),
            ),
            "/apis/batch/v1/namespaces/team-a/cronjobs" => list(
                "CronJobList",
                serde_json::json!([{"metadata": {"name": "report"}, "spec": {"schedule": "* * * * *",
                    "jobTemplate": {"spec": {"template": crate::k8s::workloads::fixtures::template("report", "r:1")}}}}]),
            ),
            "/apis/batch/v1/namespaces/team-a/jobs" => list(
                "JobList",
                serde_json::json!([{"metadata": {"name": "report-28000000", "ownerReferences": [
                    {"apiVersion": "batch/v1", "kind": "CronJob", "name": "report", "uid": "c", "controller": true}]},
                    "spec": {"template": crate::k8s::workloads::fixtures::template("report", "r:1")}}]),
            ),
            "/api/v1/namespaces/team-a/pods" => list(
                "PodList",
                serde_json::json!([
                    pod(
                        "web-7d9f8c6b5d-x2x4z",
                        ("ReplicaSet", "web-7d9f8c6b5d"),
                        hash.clone(),
                        "Running",
                        ("2", "4Gi"),
                        Some(("2", "1Gi"))
                    ),
                    pod(
                        "web-7d9f8c6b5d-b2c4d",
                        ("ReplicaSet", "web-7d9f8c6b5d"),
                        hash,
                        "Running",
                        ("2", "4Gi"),
                        Some(("2", "1Gi"))
                    ),
                    pod(
                        "report-28000000-k7m9q",
                        ("Job", "report-28000000"),
                        serde_json::json!({}),
                        "Succeeded",
                        ("100m", "64Mi"),
                        None
                    ),
                ]),
            ),
            "/api/v1/namespaces/team-a/resourcequotas" => list(
                "ResourceQuotaList",
                serde_json::json!([{"metadata": {"name": "q"}, "status": {"hard": {"requests.cpu": "10"},
                    "used": {"requests.cpu": "4100m"}}}]),
            ),
            _ => status(404, "NotFound", "no"),
        }
    }

    fn prometheus() -> FakeSource {
        FakeSource::new(|_, q| {
            let row = |pod: &'static str, v: f64| (vec![("pod", pod), ("container", "app")], v);
            let rows: Vec<(Vec<(&str, &str)>, f64)> =
                if q.contains("container_cpu_usage_seconds_total") {
                    // Удалённый за окно под web — по форме имени.
                    vec![
                        row("web-7d9f8c6b5d-x2x4z", 0.2),
                        row("web-5f6d7c8b9d-q2w4x", 0.3),
                        row("report-28000000-k7m9q", 0.05),
                    ]
                } else if q.contains("container_memory_working_set_bytes") {
                    vec![row("web-7d9f8c6b5d-x2x4z", 950.0 * MI)]
                } else if q.contains("cfs_throttled") {
                    vec![row("web-7d9f8c6b5d-b2c4d", 0.4)]
                } else {
                    vec![row("web-7d9f8c6b5d-b2c4d", 1.0)]
                };
            let refs: Vec<(&[(&str, &str)], f64)> =
                rows.iter().map(|(l, v)| (&l[..], *v)).collect();
            Ok(vector(&refs))
        })
    }

    #[tokio::test]
    async fn workloads_get_totals_usage_reserve_risks_and_excess() {
        let r = ReadClient::new(mock_client(cluster).0, StdDuration::from_secs(5));
        let sel = Selected {
            history: Some(Arc::new(prometheus())),
            status: source_status(false),
        };
        let u = project_usage(&r, &sel, &ApiGroups::default(), "team-a", 7, 1_700_000_000)
            .await
            .unwrap();
        let web = u.workloads.iter().find(|w| w.name == "web").unwrap();
        assert_eq!(web.pods, 2);
        assert_eq!(
            web.requests,
            Amounts {
                cpu: Some(4.0),
                memory: Some(8192.0 * MI)
            }
        );
        // Худший под — удалённый (0,3), × 2 работающих пода.
        assert!(
            (web.usage.cpu.unwrap() - 0.6).abs() < 1e-9,
            "{:?}",
            web.usage
        );
        assert!((web.reserve.cpu.unwrap() - 3.4).abs() < 1e-9);
        assert_eq!(web.risks.len(), 3, "{:?}", web.risks);
        assert!(
            matches!(&web.risks[0], Risk::Throttled { mean, .. } if (*mean - 0.4).abs() < 1e-9)
        );
        assert!(matches!(&web.risks[1], Risk::Oom { count: 1, .. }));
        assert!(matches!(&web.risks[2], Risk::MemoryNearLimit { .. }));
        assert_eq!(
            web.excess
                .iter()
                .map(|e| e.resource.as_str())
                .collect::<Vec<_>>(),
            ["cpu", "memory"]
        );
        assert!(web.actionable());
        let report = u.workloads.iter().find(|w| w.name == "report").unwrap();
        assert_eq!(
            report.pods, 0,
            "завершённый под Job не считается работающим"
        );
        assert_eq!(report.usage.cpu, Some(0.0));
        assert!(!report.actionable());
        assert_eq!(u.quotas[0].resource, "requests.cpu");
        assert!(!u.truncated);
    }

    #[test]
    fn pods_map_to_workloads_by_owner_or_by_name() {
        let w = [
            (WorkloadKind::Deployment, "web".to_string()),
            (WorkloadKind::Deployment, "web-api".to_string()),
            (WorkloadKind::StatefulSet, "db".to_string()),
        ];
        assert_eq!(
            workload_of("web-api-7d9f8c6b5d-x2x4z", None, &w),
            Some((WorkloadKind::Deployment, "web-api".into()))
        );
        assert_eq!(
            workload_of("db-0", None, &w),
            Some((WorkloadKind::StatefulSet, "db".into()))
        );
        assert_eq!(workload_of("cache-0", None, &w), None);
        let owner = Some((WorkloadKind::DaemonSet, "agent".to_string()));
        assert_eq!(workload_of("anything", owner.clone(), &w), owner);
    }

    #[test]
    fn thresholds_of_the_rules() {
        let u = Use {
            cpu_p95: Some(0.2),
            memory_max: Some(900.0 * MI),
            throttling_mean: Some(0.25),
            oom_count: Some(0),
        };
        let one = |cpu, mem| Amounts {
            cpu: Some(cpu),
            memory: Some(mem),
        };
        let (risks, excess) = assess("app", &u, one(0.6, 1024.0 * MI), one(1.0, 1000.0 * MI));
        assert_eq!(risks.len(), 1, "троттлинг ровно 25 % — не риск: {risks:?}");
        assert!(
            matches!(risks[0], Risk::MemoryNearLimit { .. }),
            "900 из 1000 — 90 %"
        );
        assert!(excess.is_empty(), "0,6 ≥ 3 × 0,2, но разница 0,4 < 0,5");
        let (_, excess) = assess("app", &u, one(0.7, 2048.0 * MI), one(1.0, 4096.0 * MI));
        assert_eq!(excess.len(), 2);
        let (risks, _) = assess(
            "app",
            &Use {
                throttling_mean: Some(0.9),
                ..u
            },
            one(0.1, MI),
            Amounts::default(),
        );
        assert!(risks.is_empty(), "без CPU limit троттлинга нет");
    }

    #[test]
    fn cache_keeps_results_for_five_minutes() {
        let c = ProjectCache::default();
        let u = ProjectUsage {
            namespace: "team-a".into(),
            window_days: 7,
            source: crate::metrics::pods::info(),
            source_status: source_status(true),
            workloads: vec![],
            errors: vec![],
            truncated: false,
            quotas: vec![],
            collected_at_ms: 1,
        };
        c.put("c1", &u);
        assert!(c.get("c1", "team-a", 7).is_some());
        assert!(c.get("c1", "team-a", 1).is_none());
        assert!(c.get("c2", "team-a", 7).is_none());
    }
}
````

- [ ] **Step 2: Реализация.**

<!-- block: file t07-src-tauri-src-resources-project-rs-impl step=7.2 path=src-tauri/src/resources/project.rs part=impl -->
````rust
//! Ресурсы проекта (спец. §7.4): по каждому workload'у — суммарные requests и limits работающих
//! подов против фактического потребления за окно (p95 CPU и максимум памяти «худшего пода»),
//! резерв, риски (троттлинг, OOM, память у лимита) и избыток. Метрики — агрегирующие запросы по
//! проекту (по поду и контейнеру); поды с workload'ами сопоставляет Rust: работающие — по
//! владельцу, удалённые за окно — по форме имени. Те же значения за 24 ч проверяют правила
//! `resources.*` (`health`).

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{Pod, ResourceQuota};
use kube::api::ListParams;
use serde::Serialize;
use tokio::time::Instant;

use super::current::Amounts;
use super::recommend::QuotaRoom;
use crate::error::AppResult;
use crate::k8s::pods::owner_of;
use crate::k8s::quantity::parse_quantity;
use crate::k8s::read::ReadClient;
use crate::k8s::workloads::{KindError, WorkloadKind, list_workloads};
use crate::metrics::prometheus::{Failure, MetricsSource, SourceInfo, query};
use crate::metrics::promql;
use crate::metrics::select::{Selected, SourceStatus};
use crate::platform::ApiGroups;
use crate::sync::MutexExt;

/// Дедлайн сбора по проекту.
pub const PROJECT_DEADLINE: Duration = Duration::from_secs(45);
/// Сколько живёт собранное по проекту.
pub const PROJECT_TTL: Duration = Duration::from_secs(5 * 60);
/// Троттлинг контейнера с CPU limit в среднем — риск (`resources.cpuThrottled`).
pub const THROTTLED_MEAN: f64 = 0.25;
/// Максимум памяти от memory-limit — риск (`resources.memoryNearLimit`).
pub const MEMORY_NEAR_LIMIT: f64 = 0.90;
/// Избыток CPU (`resources.overProvisioned`): request ≥ 3 × p95 и разница ≥ 0,5 ядра на под.
pub const CPU_OVER_FACTOR: f64 = 3.0;
pub const CPU_OVER_MIN: f64 = 0.5;
/// Избыток памяти: request ≥ 2 × максимум и разница ≥ 512Mi на под.
pub const MEMORY_OVER_FACTOR: f64 = 2.0;
pub const MEMORY_OVER_MIN: f64 = 512.0 * 1024.0 * 1024.0;

/// Потребление контейнера одного пода за окно.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Use {
    pub cpu_p95: Option<f64>,
    pub memory_max: Option<f64>,
    pub throttling_mean: Option<f64>,
    pub oom_count: Option<u32>,
}

impl Use {
    /// Худшее из двух подов.
    fn worst(self, o: Self) -> Self {
        Self {
            cpu_p95: larger(self.cpu_p95, o.cpu_p95),
            memory_max: larger(self.memory_max, o.memory_max),
            throttling_mean: larger(self.throttling_mean, o.throttling_mean),
            oom_count: match (self.oom_count, o.oom_count) {
                (Some(a), Some(b)) => Some(a + b),
                (a, b) => a.or(b),
            },
        }
    }
}

/// Потребление по поду и контейнеру за окно: `(под, контейнер) → значения`. `oom` — ещё и
/// эпизоды OOM (kube-state-metrics).
pub async fn pod_container_use(
    src: &dyn MetricsSource,
    namespace: &str,
    days: u32,
    now: i64,
    oom: bool,
) -> Result<BTreeMap<(String, String), Use>, Failure> {
    let q = [
        promql::project_cpu_p95(namespace, days),
        promql::project_memory_max(namespace, days),
        promql::project_throttling_mean(namespace, days),
        promql::project_oom_count(namespace, days),
    ];
    let (cpu, memory, throttling, ooms) = tokio::try_join!(
        query(src, namespace, &q[0], now),
        query(src, namespace, &q[1], now),
        query(src, namespace, &q[2], now),
        async {
            if oom {
                query(src, namespace, &q[3], now).await
            } else {
                Ok(Vec::new())
            }
        },
    )?;
    let mut out: BTreeMap<(String, String), Use> = BTreeMap::new();
    let key = |s: &crate::metrics::prometheus::Sample| {
        (s.label("pod").to_string(), s.label("container").to_string())
    };
    for s in &cpu {
        out.entry(key(s)).or_default().cpu_p95 = Some(s.value);
    }
    for s in &memory {
        out.entry(key(s)).or_default().memory_max = Some(s.value);
    }
    for s in &throttling {
        out.entry(key(s)).or_default().throttling_mean = Some(s.value);
    }
    for s in &ooms {
        out.entry(key(s)).or_default().oom_count = Some(s.value.round() as u32);
    }
    Ok(out)
}

/// Риск контейнера workload'а.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "code", rename_all = "camelCase")]
pub enum Risk {
    /// Средний троттлинг контейнера с CPU limit выше 25 %.
    Throttled { container: String, mean: f64 },
    /// Эпизоды OOM за окно.
    Oom { container: String, count: u32 },
    /// Максимум памяти — 90 % memory-limit и больше.
    MemoryNearLimit {
        container: String,
        max: f64,
        limit: f64,
    },
}

/// Избыток: request намного больше потребления (на под).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Excess {
    pub container: String,
    /// `cpu` или `memory`.
    pub resource: String,
    pub request: f64,
    /// p95 CPU или максимум памяти.
    pub usage: f64,
}

/// `a ≥ b` с запасом на погрешность дробей: граница входит (`≥ 90 %`, `≥ 0,5 ядра`).
fn at_least(a: f64, b: f64) -> bool {
    a >= b - 1e-9 * b.abs().max(1.0)
}

/// Риски и избыток контейнера по его request и limit на под (спец. §8.2, правила
/// `resources.*`).
pub fn assess(
    container: &str,
    u: &Use,
    request: Amounts,
    limit: Amounts,
) -> (Vec<Risk>, Vec<Excess>) {
    let mut risks = Vec::new();
    let mut excess = Vec::new();
    if let (Some(mean), Some(_)) = (u.throttling_mean, limit.cpu)
        && mean > THROTTLED_MEAN
    {
        risks.push(Risk::Throttled {
            container: container.into(),
            mean,
        });
    }
    if let Some(n) = u.oom_count.filter(|n| *n > 0) {
        risks.push(Risk::Oom {
            container: container.into(),
            count: n,
        });
    }
    if let (Some(max), Some(l)) = (u.memory_max, limit.memory)
        && l > 0.0
        && at_least(max, MEMORY_NEAR_LIMIT * l)
    {
        risks.push(Risk::MemoryNearLimit {
            container: container.into(),
            max,
            limit: l,
        });
    }
    if let (Some(r), Some(p95)) = (request.cpu, u.cpu_p95)
        && at_least(r, CPU_OVER_FACTOR * p95)
        && at_least(r - p95, CPU_OVER_MIN)
    {
        excess.push(Excess {
            container: container.into(),
            resource: "cpu".into(),
            request: r,
            usage: p95,
        });
    }
    if let (Some(r), Some(max)) = (request.memory, u.memory_max)
        && at_least(r, MEMORY_OVER_FACTOR * max)
        && at_least(r - max, MEMORY_OVER_MIN)
    {
        excess.push(Excess {
            container: container.into(),
            resource: "memory".into(),
            request: r,
            usage: max,
        });
    }
    (risks, excess)
}

/// Workload, которому принадлежит под: работающий — по владельцу (поды Job из CronJob — к
/// CronJob), остальные — по форме имени; из подходящих — с самым длинным именем.
pub fn workload_of(
    pod: &str,
    owner: Option<(WorkloadKind, String)>,
    workloads: &[(WorkloadKind, String)],
) -> Option<(WorkloadKind, String)> {
    if owner.is_some() {
        return owner;
    }
    workloads
        .iter()
        .filter(|(k, n)| crate::k8s::events::pod_of(&format!("{k:?}"), n, pod))
        .max_by_key(|(_, n)| n.len())
        .cloned()
}

fn kind_of(s: &str) -> Option<WorkloadKind> {
    WorkloadKind::ALL
        .into_iter()
        .find(|k| format!("{k:?}") == s)
}

/// Владелец-workload работающих подов: `pod → (вид, имя)`.
pub fn owners(pods: &[Pod], jobs: &[Job]) -> HashMap<String, (WorkloadKind, String)> {
    let cron: HashMap<&str, &str> = jobs
        .iter()
        .filter_map(|j| {
            let o = j
                .metadata
                .owner_references
                .iter()
                .flatten()
                .find(|o| o.kind == "CronJob")?;
            Some((j.metadata.name.as_deref()?, o.name.as_str()))
        })
        .collect();
    pods.iter()
        .filter_map(|p| {
            let name = p.metadata.name.clone()?;
            let o = owner_of(p)?;
            let w = match (o.kind.as_str(), cron.get(o.name.as_str())) {
                ("Job", Some(c)) => (WorkloadKind::CronJob, (*c).to_string()),
                (k, _) => (kind_of(k)?, o.name),
            };
            Some((name, w))
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadResources {
    pub kind: WorkloadKind,
    pub name: String,
    /// Работающих подов сейчас.
    pub pods: u32,
    /// Суммы по работающим подам.
    pub requests: Amounts,
    pub limits: Amounts,
    /// Потребление: p95 CPU и максимум памяти «худшего пода» по контейнерам × поды.
    pub usage: Amounts,
    /// Requests минус потребление.
    pub reserve: Amounts,
    pub risks: Vec<Risk>,
    pub excess: Vec<Excess>,
}

impl WorkloadResources {
    /// Есть что поменять: риск или избыток (фильтр «только с рекомендациями»).
    pub fn actionable(&self) -> bool {
        !self.risks.is_empty() || !self.excess.is_empty()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectUsage {
    pub namespace: String,
    pub window_days: u32,
    pub source: SourceInfo,
    pub source_status: SourceStatus,
    pub workloads: Vec<WorkloadResources>,
    /// Виды workloads, которые не прочитались.
    pub errors: Vec<KindError>,
    /// Список workloads или подов неполный.
    pub truncated: bool,
    pub quotas: Vec<QuotaRoom>,
    pub collected_at_ms: i64,
}

fn running(p: &Pod) -> bool {
    !matches!(
        p.status.as_ref().and_then(|s| s.phase.as_deref()),
        Some("Succeeded" | "Failed")
    )
}

fn pod_amounts(p: &Pod, limits: bool) -> BTreeMap<String, Amounts> {
    let Some(spec) = p.spec.as_ref() else {
        return BTreeMap::new();
    };
    let init = spec
        .init_containers
        .iter()
        .flatten()
        .filter(|c| c.restart_policy.as_deref() == Some("Always"));
    spec.containers
        .iter()
        .chain(init)
        .map(|c| {
            let r = c.resources.as_ref();
            let m = if limits {
                r.and_then(|r| r.limits.as_ref())
            } else {
                r.and_then(|r| r.requests.as_ref())
            };
            let q = |k: &str| m.and_then(|m| m.get(k)).and_then(|v| parse_quantity(&v.0));
            (
                c.name.clone(),
                Amounts {
                    cpu: q("cpu"),
                    memory: q("memory"),
                },
            )
        })
        .collect()
}

fn larger(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

fn add(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a + b),
        (a, b) => a.or(b),
    }
}

/// Сводка по workload'ам из подов и потребления по поду и контейнеру.
pub fn summarize(
    workloads: &[(WorkloadKind, String)],
    pods: &[Pod],
    owner: &HashMap<String, (WorkloadKind, String)>,
    usage: &BTreeMap<(String, String), Use>,
) -> Vec<WorkloadResources> {
    // Худшее потребление по контейнерам workload'а: (вид, имя) → контейнер → значения.
    let mut worst: HashMap<(WorkloadKind, String), BTreeMap<String, Use>> = HashMap::new();
    for ((pod, container), u) in usage {
        if let Some(w) = workload_of(pod, owner.get(pod).cloned(), workloads) {
            let e = worst
                .entry(w)
                .or_default()
                .entry(container.clone())
                .or_default();
            *e = e.worst(*u);
        }
    }
    workloads
        .iter()
        .map(|(kind, name)| {
            let mine: Vec<&Pod> = pods
                .iter()
                .filter(|p| {
                    running(p)
                        && p.metadata
                            .name
                            .as_ref()
                            .and_then(|n| owner.get(n))
                            .is_some_and(|(k, n)| k == kind && n == name)
                })
                .collect();
            let mut requests = Amounts::default();
            let mut limits = Amounts::default();
            // Request и limit контейнера на под — наибольшие среди подов.
            let mut per_pod: BTreeMap<String, (Amounts, Amounts)> = BTreeMap::new();
            for p in &mine {
                let (req, lim) = (pod_amounts(p, false), pod_amounts(p, true));
                for (c, a) in &req {
                    requests.cpu = add(requests.cpu, a.cpu);
                    requests.memory = add(requests.memory, a.memory);
                    let e = per_pod.entry(c.clone()).or_default();
                    e.0.cpu = larger(e.0.cpu, a.cpu);
                    e.0.memory = larger(e.0.memory, a.memory);
                }
                for (c, a) in &lim {
                    limits.cpu = add(limits.cpu, a.cpu);
                    limits.memory = add(limits.memory, a.memory);
                    let e = per_pod.entry(c.clone()).or_default();
                    e.1.cpu = larger(e.1.cpu, a.cpu);
                    e.1.memory = larger(e.1.memory, a.memory);
                }
            }
            let n = mine.len() as f64;
            let containers = worst.get(&(*kind, name.clone()));
            let mut usage_sum = Amounts::default();
            let mut risks = Vec::new();
            let mut excess = Vec::new();
            for (c, u) in containers.into_iter().flatten() {
                usage_sum.cpu = add(usage_sum.cpu, u.cpu_p95.map(|v| v * n));
                usage_sum.memory = add(usage_sum.memory, u.memory_max.map(|v| v * n));
                let (req, lim) = per_pod.get(c).copied().unwrap_or_default();
                let (r, e) = assess(c, u, req, lim);
                risks.extend(r);
                excess.extend(e);
            }
            let minus = |a: Option<f64>, b: Option<f64>| Some(a? - b.unwrap_or(0.0));
            WorkloadResources {
                kind: *kind,
                name: name.clone(),
                pods: mine.len() as u32,
                reserve: Amounts {
                    cpu: minus(requests.cpu, usage_sum.cpu),
                    memory: minus(requests.memory, usage_sum.memory),
                },
                requests,
                limits,
                usage: usage_sum,
                risks,
                excess,
            }
        })
        .collect()
}

/// Ресурсы проекта по выбранному источнику: история за окно или текущие значения
/// `metrics.k8s.io`.
pub async fn project_usage(
    r: &ReadClient,
    sel: &Selected,
    groups: &ApiGroups,
    namespace: &str,
    days: u32,
    now: i64,
) -> AppResult<ProjectUsage> {
    let days = super::usage::check_window(days)?;
    let ns = Some(namespace);
    let all = ListParams::default();
    let (list, pods, jobs, quotas) = tokio::join!(
        list_workloads(r, namespace, groups),
        r.list::<Pod>(ns, &all),
        r.list::<Job>(ns, &all),
        r.list::<ResourceQuota>(ns, &all),
    );
    let pods = pods?;
    let jobs = jobs.map(|l| l.items).unwrap_or_default();
    let workloads: Vec<(WorkloadKind, String)> = list
        .items
        .iter()
        .map(|w| (w.kind, w.name.clone()))
        .collect();
    let owner = owners(&pods.items, &jobs);
    let (source, usage) = if let Some(src) = sel.history.as_deref() {
        let u = pod_container_use(src, namespace, days, now, true)
            .await
            .map_err(|f| f.error)?;
        (src.info().clone(), u)
    } else if sel.status.pod_metrics {
        let now_list = crate::metrics::pods::pods_now(r, namespace, None)
            .await
            .map_err(|f| f.error)?;
        let mut u = BTreeMap::new();
        for p in now_list {
            for c in p.containers {
                u.insert(
                    (p.pod.clone(), c.name),
                    Use {
                        cpu_p95: c.cpu,
                        memory_max: c.memory,
                        ..Default::default()
                    },
                );
            }
        }
        (crate::metrics::pods::info(), u)
    } else {
        return Err(sel.status.unavailable());
    };
    Ok(ProjectUsage {
        namespace: namespace.to_string(),
        window_days: days,
        source,
        source_status: sel.status.clone(),
        workloads: summarize(&workloads, &pods.items, &owner, &usage),
        errors: list.errors,
        truncated: list.truncated || pods.truncated,
        quotas: quotas
            .map(|l| super::constraints::quota_rooms(&l.items))
            .unwrap_or_default(),
        collected_at_ms: now * 1000,
    })
}

/// Ключ собранного по проекту: подключение, проект, окно.
type ProjectKey = (String, String, u32);

/// Собранное по проектам: ключ → (когда, результат), живёт `PROJECT_TTL`.
#[derive(Default)]
pub struct ProjectCache {
    inner: std::sync::Mutex<HashMap<ProjectKey, (Instant, ProjectUsage)>>,
}

impl ProjectCache {
    pub fn get(&self, connection_id: &str, namespace: &str, days: u32) -> Option<ProjectUsage> {
        let inner = self.inner.lock_safe();
        let (at, u) = inner.get(&(connection_id.into(), namespace.into(), days))?;
        (at.elapsed() < PROJECT_TTL).then(|| u.clone())
    }

    pub fn put(&self, connection_id: &str, u: &ProjectUsage) {
        let mut inner = self.inner.lock_safe();
        inner.retain(|_, (at, _)| at.elapsed() < PROJECT_TTL);
        inner.insert(
            (connection_id.into(), u.namespace.clone(), u.window_days),
            (Instant::now(), u.clone()),
        );
    }
}
````

<!-- block: patch t07-src step=7.2 -->
````diff
diff --git a/src-tauri/src/commands/resources.rs b/src-tauri/src/commands/resources.rs
index bd8ab69..1895ce4 100644
--- a/src-tauri/src/commands/resources.rs
+++ b/src-tauri/src/commands/resources.rs
@@ -1,13 +1,17 @@
 //! Ресурсы (спец. §4.2, §7): источник метрик подключения, статистика workload'а и текущее
 //! потребление пода. Чтение в проекте идёт через `read_in`: 403 сбрасывает кэш прав проекта.
 
+use serde::Serialize;
 use tauri::State;
+use tauri::ipc::Channel;
 
-use crate::error::AppResult;
+use crate::error::{AppError, AppResult};
 use crate::k8s::workloads::WorkloadKind;
 use crate::metrics::select::SourceStatus;
 use crate::resources::Analysis;
+use crate::resources::project::{PROJECT_DEADLINE, ProjectUsage};
 use crate::resources::usage::{PodUsage, WorkloadUsage};
+use crate::sessions::{SessionGuard, Sink};
 use crate::state::AppState;
 
 pub(crate) fn now_s() -> i64 {
@@ -110,3 +114,169 @@ pub async fn recommend_resources(
         })
         .await
 }
+
+/// Итог сессии сбора по проекту.
+#[derive(Debug, Clone, Serialize)]
+#[serde(tag = "kind", rename_all = "camelCase")]
+pub enum ProjectUsageEvent {
+    Done { usage: Box<ProjectUsage> },
+    Failed { error: AppError },
+}
+
+/// Ресурсы проекта (вкладка «Ресурсы» проекта): фоновая сессия с дедлайном 45 с; итог — в
+/// `on_event`, собранное живёт в кэше 5 минут (`refresh` — собрать заново). Возвращает id
+/// сессии для `stop_session`.
+#[tauri::command]
+pub async fn get_project_usage(
+    state: State<'_, AppState>,
+    connection_id: String,
+    namespace: String,
+    window_days: u32,
+    refresh: bool,
+    on_event: Channel<ProjectUsageEvent>,
+) -> AppResult<String> {
+    get_project_usage_impl(
+        state.inner(),
+        connection_id,
+        namespace,
+        window_days,
+        refresh,
+        on_event,
+    )
+    .await
+}
+
+pub async fn get_project_usage_impl(
+    st: &AppState,
+    connection_id: String,
+    namespace: String,
+    days: u32,
+    refresh: bool,
+    sink: impl Sink<ProjectUsageEvent>,
+) -> AppResult<String> {
+    crate::k8s::read::check_namespace(&namespace)?;
+    crate::resources::usage::check_window(days)?;
+    let (id, cancel) = st.sessions.register(&connection_id);
+    let guard = SessionGuard {
+        sessions: st.sessions.clone(),
+        id: id.clone(),
+    };
+    let st = st.clone();
+    tokio::spawn(async move {
+        let _guard = guard;
+        let work = async {
+            if !refresh && let Some(u) = st.resources.get(&connection_id, &namespace, days) {
+                return Ok(u);
+            }
+            let (s, cid, ns) = (st.clone(), connection_id.clone(), namespace.clone());
+            let read = st.read_in(&connection_id, &namespace, |c| async move {
+                let sel = crate::metrics::select::for_connection(&s, &cid, &c, false).await?;
+                let groups = c.info.api_groups;
+                crate::resources::project::project_usage(
+                    &c.reader(),
+                    &sel,
+                    &groups,
+                    &ns,
+                    days,
+                    now_s(),
+                )
+                .await
+            });
+            let u = tokio::time::timeout(PROJECT_DEADLINE, Box::pin(read))
+                .await
+                .map_err(|_| crate::k8s::errors::timeout())??;
+            st.resources.put(&connection_id, &u);
+            Ok::<_, AppError>(u)
+        };
+        tokio::select! {
+            _ = cancel.cancelled() => {}
+            r = work => {
+                sink.send(match r {
+                    Ok(u) => ProjectUsageEvent::Done { usage: Box::new(u) },
+                    Err(error) => ProjectUsageEvent::Failed { error },
+                });
+            }
+        }
+    });
+    Ok(id)
+}
+
+#[cfg(test)]
+mod tests {
+    use std::sync::Arc;
+    use std::sync::atomic::{AtomicUsize, Ordering};
+    use std::time::Duration;
+
+    use super::*;
+    use crate::sessions::Collect;
+
+    async fn done(sink: &Collect<ProjectUsageEvent>, n: usize) -> Vec<ProjectUsageEvent> {
+        let wait = async {
+            while sink.items().len() < n {
+                tokio::time::sleep(Duration::from_millis(10)).await;
+            }
+        };
+        tokio::time::timeout(Duration::from_secs(5), wait)
+            .await
+            .unwrap();
+        sink.items()
+    }
+
+    #[tokio::test]
+    async fn project_usage_is_collected_once_and_cached() {
+        let dir = tempfile::tempdir().unwrap();
+        let st =
+            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
+        st.connections
+            .upsert(crate::storage::profile::sample_profile("a"))
+            .unwrap();
+        let pods = Arc::new(AtomicUsize::new(0));
+        let seen = pods.clone();
+        let connected = crate::k8s::pool::test_connected(move |s| {
+            let list = |kind: &str| {
+                (
+                    200,
+                    serde_json::json!({"apiVersion": "v1", "kind": kind, "metadata": {}, "items": []}),
+                )
+            };
+            match s.path() {
+                "/apis/metrics.k8s.io/v1beta1" => (200, serde_json::json!({})),
+                "/api/v1/namespaces/team-a/pods" => {
+                    seen.fetch_add(1, Ordering::SeqCst);
+                    list("PodList")
+                }
+                p if p.ends_with("/proxy/api/v1/query") => {
+                    crate::k8s::testing::status(404, "NotFound", "no")
+                }
+                _ => list("List"),
+            }
+        });
+        assert!(
+            st.pool
+                .insert_if_current("a", st.pool.generation("a"), connected)
+        );
+        let sink: Collect<ProjectUsageEvent> = Collect::default();
+        get_project_usage_impl(&st, "a".into(), "team-a".into(), 7, false, sink.clone())
+            .await
+            .unwrap();
+        let events = done(&sink, 1).await;
+        let ProjectUsageEvent::Done { usage } = &events[0] else {
+            panic!("{events:?}")
+        };
+        assert_eq!(usage.source.location, "metrics.k8s.io");
+        get_project_usage_impl(&st, "a".into(), "team-a".into(), 7, false, sink.clone())
+            .await
+            .unwrap();
+        done(&sink, 2).await;
+        assert_eq!(pods.load(Ordering::SeqCst), 1, "второй раз — из кэша");
+        get_project_usage_impl(&st, "a".into(), "team-a".into(), 7, true, sink.clone())
+            .await
+            .unwrap();
+        done(&sink, 3).await;
+        assert_eq!(pods.load(Ordering::SeqCst), 2, "«Обновить» собирает заново");
+        let e = get_project_usage_impl(&st, "a".into(), "team-a".into(), 3, false, sink)
+            .await
+            .unwrap_err();
+        assert_eq!(e.key, "errors.resources.badWindow");
+    }
+}
diff --git a/src-tauri/src/k8s/events.rs b/src-tauri/src/k8s/events.rs
index 65aca92..2378b85 100644
--- a/src-tauri/src/k8s/events.rs
+++ b/src-tauri/src/k8s/events.rs
@@ -169,6 +169,13 @@ fn is_child(parent: &str, child: &str, rest: &str) -> bool {
     }
 }
 
+/// Под `pod` по форме имени порождён workload'ом вида `kind` (`Deployment`, `CronJob`…) с именем
+/// `workload` — так сопоставляются с workload'ами и поды, которых уже нет (метрики за окно).
+pub fn pod_of(kind: &str, workload: &str, pod: &str) -> bool {
+    pod.strip_prefix(&format!("{workload}-"))
+        .is_some_and(|rest| is_child(kind, "Pod", rest))
+}
+
 /// Относится ли событие к объекту `o`: это он сам или его ребёнок.
 pub fn concerns(row: &EventRow, o: &EventObject) -> bool {
     if row.object.kind == o.kind && row.object.name == o.name {
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index 377d7f6..d2d2ac3 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -159,6 +159,7 @@ pub fn run() {
             commands::resources::get_workload_usage,
             commands::resources::get_pod_usage,
             commands::resources::recommend_resources,
+            commands::resources::get_project_usage,
             commands::health::run_health_check,
             commands::health::get_health,
             commands::health::set_finding_ignored,
diff --git a/src-tauri/src/resources/mod.rs b/src-tauri/src/resources/mod.rs
index b455f83..dbbb0c1 100644
--- a/src-tauri/src/resources/mod.rs
+++ b/src-tauri/src/resources/mod.rs
@@ -3,6 +3,7 @@
 
 pub mod constraints;
 pub mod current;
+pub mod project;
 pub mod recommend;
 pub mod usage;
 
diff --git a/src-tauri/src/state.rs b/src-tauri/src/state.rs
index 28e36ee..99a957f 100644
--- a/src-tauri/src/state.rs
+++ b/src-tauri/src/state.rs
@@ -29,6 +29,8 @@ pub struct AppState {
     pub access: Arc<crate::k8s::access::AccessCache>,
     /// Проверка кластера, скрытые находки и замечания LLM (`health/<id>.json`).
     pub health: Arc<crate::health::service::HealthService>,
+    /// Собранное по проектам для вкладки «Ресурсы» (кэш на 5 минут).
+    pub resources: Arc<crate::resources::project::ProjectCache>,
 }
 
 impl AppState {
@@ -43,6 +45,7 @@ impl AppState {
             sessions: Arc::new(crate::sessions::Sessions::default()),
             access: Arc::new(crate::k8s::access::AccessCache::default()),
             health: Arc::new(crate::health::service::HealthService::open(data_dir)),
+            resources: Arc::new(Default::default()),
         })
     }
 
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 638 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 180`

- [ ] **Step 4: Commit.**

```bash
git add \
  src-tauri/src/commands/resources.rs \
  src-tauri/src/k8s/events.rs \
  src-tauri/src/lib.rs \
  src-tauri/src/resources/mod.rs \
  src-tauri/src/resources/project.rs \
  src-tauri/src/state.rs
git commit -s -m "feat(resources): project resources — totals of running pods against the worst-pod usage over the window, reserve, risks and excess per workload, quota room; a background session with a 45 s deadline and a five-minute cache" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 8: Правила resources.*: троттлинг CPU, память у лимита и лишние requests за последние сутки по истории метрик; без неё проверка частичная

**Files:**
- Create: `src-tauri/src/health/rules/resources.rs`
- Modify: `src-tauri/src/health/collect.rs`, `src-tauri/src/health/rules/mod.rs`, `src-tauri/src/health/service.rs`, `src-tauri/src/health/snapshot.rs`, `src-tauri/src/health/texts.rs`, `src-tauri/src/k8s/quantity.rs`, `src-tauri/src/resources/project.rs`, `src-tauri/tests/it_health.rs`, `src/components/health/findings.test.ts`, `src/components/health/findings.ts`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задачи 4, 7: `MetricsSource`, `select::for_connection`, `project::{assess, kind_of, workload_of}`; O2: `health::{snapshot, collect, rules, texts, service}`.
- Produces: `health::snapshot::{ContainerAmounts, ContainerSnap.amounts, ProjectSnap.usage}`; `health::collect::{RESOURCES_WINDOW_DAYS = 1, collect(…, metrics: Option<&dyn MetricsSource>)}`; `health::rules::resources` — `resources.cpuThrottled`, `resources.memoryNearLimit`, `resources.overProvisioned` (37 правил); `k8s::quantity::{format_cpu, format_memory}`; `findings.ts::detailsKey` для `resources.overProvisioned`.

Пороги и сопоставление подов — те же, что у вкладки «Ресурсы» проекта. Строгость по окружению — по таблице §8.2 (троттлинг и память у лимита — предупреждение на prod и stage, информация ниже; лишние requests — информация). Служба проверки берёт источник подключения; без источника истории — разрыв `resources METRICS_UNAVAILABLE`, и проверка помечается частичной (`it_health` на kind теперь ждёт этот разрыв, а не отсутствие `check.partial`).

- [ ] **Step 1: Тесты.**

<!-- block: file t08-src-tauri-src-health-rules-resources-rs-tests step=8.1 path=src-tauri/src/health/rules/resources.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::super::fixtures::{ENVS, NOW};
    use super::super::pods::fixtures::{pod, project as with_pods};
    use super::*;
    use crate::health::model::{Finding, Severity};
    use crate::health::snapshot::{PodSnap, WorkloadSnap};
    use crate::platform::PlatformKind;
    use crate::storage::profile::Environment;

    const MI: i64 = 1024 * 1024;

    fn web_pod(
        name: &str,
        cpu_req: i64,
        cpu_lim: Option<i64>,
        mem_req: i64,
        mem_lim: i64,
    ) -> PodSnap {
        let mut p = pod(name, Some(("Deployment", "web")));
        p.containers[0].amounts = ContainerAmounts {
            cpu_request_milli: Some(cpu_req),
            memory_request: Some(mem_req),
            cpu_limit_milli: cpu_lim,
            memory_limit: Some(mem_lim),
        };
        p
    }

    fn snapshot(pods: Vec<PodSnap>, usage: &[(&str, Use)]) -> ClusterSnapshot {
        let mut p = with_pods("team-a", pods);
        p.workloads = Some(vec![WorkloadSnap {
            kind: WorkloadKind::Deployment,
            name: "web".into(),
            created: None,
            desired: Some(1),
            available: Some(1),
            progressing: None,
            available_cond: None,
            containers: vec![],
        }]);
        p.usage = Some(
            usage
                .iter()
                .map(|(pod, u)| (((*pod).to_string(), "web".to_string()), *u))
                .collect(),
        );
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Kubernetes);
        s.projects = vec![p];
        s
    }

    fn run(s: &ClusterSnapshot, env: Environment) -> Vec<Finding> {
        crate::health::rules::evaluate(s, env)
            .into_iter()
            .filter(|f| f.rule_id.starts_with("resources."))
            .collect()
    }

    fn u(cpu_p95: f64, memory_max: f64, throttling: f64) -> Use {
        Use {
            cpu_p95: Some(cpu_p95),
            memory_max: Some(memory_max),
            throttling_mean: Some(throttling),
            oom_count: None,
        }
    }

    #[test]
    fn throttled_near_limit_and_over_provisioned_workloads() {
        let s = snapshot(
            vec![web_pod(
                "web-7d9f8c6b5d-x2x4z",
                2000,
                Some(1000),
                4096 * MI,
                1024 * MI,
            )],
            &[
                ("web-7d9f8c6b5d-x2x4z", u(0.3, 950.0 * MI as f64, 0.4)),
                // Под, удалённый за сутки, — по форме имени.
                ("web-5f6d7c8b9d-q2w4x", u(0.2, 100.0 * MI as f64, 0.1)),
            ],
        );
        let f = run(&s, Environment::Prod);
        let ids: Vec<_> = f.iter().map(|f| f.rule_id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "resources.cpuThrottled",
                "resources.memoryNearLimit",
                "resources.overProvisioned"
            ]
        );
        assert_eq!(
            f[0].key,
            "resources.cpuThrottled:workload:team-a:Deployment/web"
        );
        assert_eq!(f[0].params["containers"][0], "web 40%");
        assert_eq!(f[1].params["containers"][0], "web 950Mi / 1Gi");
        assert_eq!(f[2].params["cpu"][0], "web 2 / p95 300m");
        assert_eq!(f[2].params["memory"][0], "web 4Gi / max 950Mi");
    }

    #[test]
    fn severity_follows_the_environment() {
        let s = snapshot(
            vec![web_pod(
                "web-7d9f8c6b5d-x2x4z",
                2000,
                Some(1000),
                4096 * MI,
                1024 * MI,
            )],
            &[("web-7d9f8c6b5d-x2x4z", u(0.3, 950.0 * MI as f64, 0.4))],
        );
        for env in ENVS {
            let sev: Vec<_> = run(&s, env).iter().map(|f| f.severity).collect();
            let expected = match env {
                Environment::Prod | Environment::Stage => {
                    [Severity::Warning, Severity::Warning, Severity::Info]
                }
                _ => [Severity::Info, Severity::Info, Severity::Info],
            };
            assert_eq!(sev, expected, "{env:?}");
        }
    }

    #[test]
    fn nothing_without_usage_limits_or_running_pods() {
        // Без CPU limit троттлинга нет, память далеко от лимита, request по делу.
        let calm = snapshot(
            vec![web_pod(
                "web-7d9f8c6b5d-x2x4z",
                300,
                None,
                512 * MI,
                1024 * MI,
            )],
            &[("web-7d9f8c6b5d-x2x4z", u(0.25, 400.0 * MI as f64, 0.9))],
        );
        assert!(run(&calm, Environment::Prod).is_empty());
        let mut unread = calm.clone();
        unread.projects[0].usage = None;
        assert!(run(&unread, Environment::Prod).is_empty());
        // Подов нет — сравнивать не с чем.
        let gone = snapshot(
            vec![],
            &[("web-7d9f8c6b5d-x2x4z", u(0.3, 950.0 * MI as f64, 0.4))],
        );
        assert!(run(&gone, Environment::Prod).is_empty());
    }
}
````

<!-- block: patch t08-tests step=8.1 -->
````diff
diff --git a/src-tauri/tests/it_health.rs b/src-tauri/tests/it_health.rs
index 5cf24ca..61f50df 100644
--- a/src-tauri/tests/it_health.rs
+++ b/src-tauri/tests/it_health.rs
@@ -63,6 +63,7 @@ async fn until_found(
                 Some(&requested),
                 LIMITS,
                 now(),
+                None,
             )
             .await;
             let found = evaluate(&s, Environment::Prod);
@@ -109,13 +110,21 @@ async fn rules_find_the_problems_of_the_stand() {
         "container.latestTag",
         "cluster.apiNotReady",
         "node.notReady",
-        "check.partial",
     ] {
         assert!(
             !have.iter().any(|h| h.starts_with(absent)),
             "{absent}: {have:?}"
         );
     }
+    // Источника метрик здесь нет (его проверяет it_metrics): правила `resources.*` — единственный
+    // разрыв проверки у администратора.
+    let gaps: Vec<String> = s
+        .coverage
+        .gaps
+        .iter()
+        .map(|g| format!("{} {}", g.part, g.code))
+        .collect();
+    assert_eq!(gaps, ["resources METRICS_UNAVAILABLE"]);
     let by = |rule: &str, name: &str| {
         found
             .iter()
diff --git a/src/components/health/findings.test.ts b/src/components/health/findings.test.ts
index 8c6ea3f..a187275 100644
--- a/src/components/health/findings.test.ts
+++ b/src/components/health/findings.test.ts
@@ -107,10 +107,11 @@ describe("findingTexts", () => {
       "service.noEndpoints": [{}, { referencedBy: ["Ingress/a"] }],
       "check.partial": [{}, { timedOut: true }],
       "container.noRequests": [{}, { cpu: [], memory: ["x"] }, { cpu: ["x"], memory: [] }],
+      "resources.overProvisioned": [{}, { cpu: [], memory: ["x"] }, { cpu: ["x"], memory: [] }],
     };
     for (const dict of [en as Dict, ru as Dict]) {
       const rules = dict.health.rules;
-      expect(Object.keys(rules)).toHaveLength(34);
+      expect(Object.keys(rules)).toHaveLength(37);
       expect(Object.keys(dict.health.ruleNames).sort()).toEqual(Object.keys(rules).sort());
       for (const [id, texts] of Object.entries(rules)) {
         expect(Object.keys(texts)).toEqual(expect.arrayContaining(["title", "details", "fix"]));
````

- [ ] **Step 2: Реализация.**

<!-- block: file t08-src-tauri-src-health-rules-resources-rs-impl step=8.2 path=src-tauri/src/health/rules/resources.rs part=impl -->
````rust
//! Правила `resources.*` (спец. §8.2): потребление контейнеров за 24 ч из источника истории
//! метрик против requests и limits работающих подов. Пороги и сопоставление подов с
//! workload'ами — те же, что у вкладки «Ресурсы» проекта (`resources::project`).

use std::collections::{BTreeMap, HashMap};

use serde_json::json;

use super::{Findings, listed};
use crate::health::model::Target;
use crate::health::snapshot::{ClusterSnapshot, ContainerAmounts, ProjectSnap};
use crate::k8s::quantity::{format_cpu, format_memory};
use crate::k8s::workloads::WorkloadKind;
use crate::resources::current::Amounts;
use crate::resources::project::{Excess, Risk, Use, assess, kind_of, workload_of};

fn larger(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

/// Request и limit контейнера на под — наибольшие среди работающих подов workload'а.
fn per_pod(a: &mut (Amounts, Amounts), c: &ContainerAmounts) {
    let (r, l) = (c.requests(), c.limits());
    a.0.cpu = larger(a.0.cpu, r.cpu);
    a.0.memory = larger(a.0.memory, r.memory);
    a.1.cpu = larger(a.1.cpu, l.cpu);
    a.1.memory = larger(a.1.memory, l.memory);
}

fn project(p: &ProjectSnap, out: &mut Findings) {
    let (Some(usage), Some(pods), Some(workloads)) = (&p.usage, &p.pods, &p.workloads) else {
        return;
    };
    let list: Vec<(WorkloadKind, String)> =
        workloads.iter().map(|w| (w.kind, w.name.clone())).collect();
    let owner: HashMap<&str, (WorkloadKind, String)> = pods
        .iter()
        .filter_map(|pod| {
            let o = pod.owner.as_ref()?;
            Some((pod.name.as_str(), (kind_of(&o.kind)?, o.name.clone())))
        })
        .collect();
    // Худшее потребление и request / limit на под: workload → контейнер → значения.
    let mut worst: BTreeMap<(WorkloadKind, String), BTreeMap<String, Use>> = BTreeMap::new();
    for ((pod, container), u) in usage {
        if let Some(w) = workload_of(pod, owner.get(pod.as_str()).cloned(), &list) {
            let e = worst
                .entry(w)
                .or_default()
                .entry(container.clone())
                .or_default();
            *e = Use {
                cpu_p95: larger(e.cpu_p95, u.cpu_p95),
                memory_max: larger(e.memory_max, u.memory_max),
                throttling_mean: larger(e.throttling_mean, u.throttling_mean),
                oom_count: None,
            };
        }
    }
    let mut amounts: HashMap<(WorkloadKind, String), BTreeMap<String, (Amounts, Amounts)>> =
        HashMap::new();
    for pod in pods
        .iter()
        .filter(|p| p.phase == "Running" || p.phase == "Pending")
    {
        if let Some(w) = owner.get(pod.name.as_str()) {
            let m = amounts.entry(w.clone()).or_default();
            for c in pod.containers.iter().filter(|c| !c.init || c.sidecar) {
                per_pod(m.entry(c.name.clone()).or_default(), &c.amounts);
            }
        }
    }
    for ((kind, name), containers) in &worst {
        let (mut throttled, mut near, mut cpu, mut memory) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for (c, u) in containers {
            let Some((req, lim)) = amounts.get(&(*kind, name.clone())).and_then(|m| m.get(c))
            else {
                continue;
            };
            let (risks, excess) = assess(c, u, *req, *lim);
            for r in risks {
                match r {
                    Risk::Throttled { container, mean } => {
                        throttled.push(format!("{container} {:.0}%", mean * 100.0));
                    }
                    Risk::MemoryNearLimit {
                        container,
                        max,
                        limit,
                    } => {
                        near.push(format!(
                            "{container} {} / {}",
                            format_memory(max),
                            format_memory(limit)
                        ));
                    }
                    Risk::Oom { .. } => {}
                }
            }
            for Excess {
                container,
                resource,
                request,
                usage,
            } in excess
            {
                if resource == "cpu" {
                    cpu.push(format!(
                        "{container} {} / p95 {}",
                        format_cpu(request),
                        format_cpu(usage)
                    ));
                } else {
                    memory.push(format!(
                        "{container} {} / max {}",
                        format_memory(request),
                        format_memory(usage)
                    ));
                }
            }
        }
        let target = || Target::workload(&p.namespace, *kind, name);
        if !throttled.is_empty() {
            out.emit(
                "resources.cpuThrottled",
                target(),
                None,
                json!({ "containers": listed(&throttled) }),
            );
        }
        if !near.is_empty() {
            out.emit(
                "resources.memoryNearLimit",
                target(),
                None,
                json!({ "containers": listed(&near) }),
            );
        }
        if !cpu.is_empty() || !memory.is_empty() {
            out.emit(
                "resources.overProvisioned",
                target(),
                None,
                json!({ "cpu": listed(&cpu), "memory": listed(&memory) }),
            );
        }
    }
}

pub fn check(s: &ClusterSnapshot, out: &mut Findings) {
    for p in &s.projects {
        project(p, out);
    }
}
````

<!-- block: patch t08-src step=8.2 -->
````diff
diff --git a/src-tauri/src/health/collect.rs b/src-tauri/src/health/collect.rs
index 77e62a2..83425fc 100644
--- a/src-tauri/src/health/collect.rs
+++ b/src-tauri/src/health/collect.rs
@@ -37,6 +37,7 @@ use crate::k8s::read::{Listed, ReadClient};
 use crate::k8s::storage::mounts;
 use crate::k8s::view::{ConditionView, conditions, conditions_of, created, name, seconds};
 use crate::k8s::workloads::{WorkloadFacts, WorkloadKind, list_facts};
+use crate::metrics::prometheus::MetricsSource;
 use crate::platform::PlatformKind;
 use crate::storage::profile::ConnectionProfile;
 
@@ -291,6 +292,24 @@ pub fn pod_snap(p: &Pod) -> PodSnap {
                 cpu_request: has(res.and_then(|r| r.requests.as_ref()), "cpu"),
                 memory_request: has(res.and_then(|r| r.requests.as_ref()), "memory"),
                 memory_limit: has(res.and_then(|r| r.limits.as_ref()), "memory"),
+                amounts: ContainerAmounts {
+                    cpu_request_milli: milli(
+                        res.and_then(|r| r.requests.as_ref())
+                            .and_then(|m| m.get("cpu")),
+                    ),
+                    memory_request: bytes(
+                        res.and_then(|r| r.requests.as_ref())
+                            .and_then(|m| m.get("memory")),
+                    ),
+                    cpu_limit_milli: milli(
+                        res.and_then(|r| r.limits.as_ref())
+                            .and_then(|m| m.get("cpu")),
+                    ),
+                    memory_limit: bytes(
+                        res.and_then(|r| r.limits.as_ref())
+                            .and_then(|m| m.get("memory")),
+                    ),
+                },
             }
         })
         .collect();
@@ -487,7 +506,16 @@ async fn projects(
     (names, total)
 }
 
-async fn project(rd: &Reader<'_>, info: &ConnectionInfo, ns: &str) -> ProjectSnap {
+/// Окно правил `resources.*`, дни (спец. §8.2).
+pub const RESOURCES_WINDOW_DAYS: u32 = 1;
+
+async fn project(
+    rd: &Reader<'_>,
+    info: &ConnectionInfo,
+    ns: &str,
+    metrics: Option<&dyn MetricsSource>,
+    now: i64,
+) -> ProjectSnap {
     let lp = ListParams::default();
     let n = Some(ns);
     let groups = &info.api_groups;
@@ -535,6 +563,24 @@ async fn project(rd: &Reader<'_>, info: &ConnectionInfo, ns: &str) -> ProjectSna
         rd.list("resourcequotas", n, rd.r.list::<ResourceQuota>(n, &lp)),
         rd.list("secrets", n, rd.r.names::<Secret>(ns)),
     );
+    // Потребление за 24 ч — агрегирующими запросами по проекту (три запроса под одним слотом).
+    let usage = match metrics {
+        Some(src) => {
+            rd.read("resources", n, async move {
+                crate::resources::project::pod_container_use(
+                    src,
+                    ns,
+                    RESOURCES_WINDOW_DAYS,
+                    now,
+                    false,
+                )
+                .await
+                .map_err(|f| f.error)
+            })
+            .await
+        }
+        None => None,
+    };
     let (route_objs, gateway_objs, http_objs, grpc_objs) = tokio::join!(
         async {
             match groups.routes {
@@ -661,6 +707,7 @@ async fn project(rd: &Reader<'_>, info: &ConnectionInfo, ns: &str) -> ProjectSna
         }),
         secrets: secrets.map(|v| v.into_iter().collect::<BTreeSet<_>>()),
         pods: pods.map(|v| v.iter().map(pod_snap).collect()),
+        usage,
     }
 }
 
@@ -785,7 +832,9 @@ async fn cluster(rd: &Reader<'_>, info: &ConnectionInfo, s: &mut ClusterSnapshot
     s.openshift = openshift;
 }
 
-/// Снимок кластера и проектов. `requested` — проекты проверки (`None` — все доступные).
+/// Снимок кластера и проектов. `requested` — проекты проверки (`None` — все доступные);
+/// `metrics` — источник истории метрик для правил `resources.*` (без него — разрыв
+/// `resources`).
 pub async fn collect(
     r: &ReadClient,
     info: &ConnectionInfo,
@@ -793,6 +842,7 @@ pub async fn collect(
     requested: Option<&[String]>,
     limits: Limits,
     now: i64,
+    metrics: Option<&dyn MetricsSource>,
 ) -> ClusterSnapshot {
     let rd = Reader {
         r,
@@ -805,8 +855,15 @@ pub async fn collect(
     let (names, total) = projects(&rd, info, profile, requested, limits.projects).await;
     let ((), projects) = tokio::join!(
         cluster(&rd, info, &mut s),
-        futures::future::join_all(names.iter().map(|ns| Box::pin(project(&rd, info, ns))))
+        futures::future::join_all(
+            names
+                .iter()
+                .map(|ns| Box::pin(project(&rd, info, ns, metrics, now)))
+        )
     );
+    if metrics.is_none() && !names.is_empty() {
+        rd.gap("resources", None, "METRICS_UNAVAILABLE".into());
+    }
     s.projects = projects;
     // Не больше `limits.pods` подов: проекты по порядку. Проект, чей список не поместился,
     // и все после него не прочитаны (`None`): обрезанный список правила приняли бы за полный
@@ -845,6 +902,7 @@ mod tests {
     use crate::k8s::cluster::Capabilities;
     use crate::k8s::pods::fixtures::{crash_looping, running};
     use crate::k8s::testing::{Seen, mock_raw};
+    use crate::metrics::prometheus::fake::{FakeSource, vector};
     use crate::platform::ApiGroups;
     use crate::storage::profile::sample_profile;
 
@@ -947,6 +1005,7 @@ mod tests {
             None,
             LIMITS,
             NOW,
+            None,
         )
         .await;
         assert_eq!(
@@ -1046,6 +1105,7 @@ mod tests {
             Some(&requested),
             limits,
             NOW,
+            None,
         )
         .await;
         let names: Vec<&str> = s.projects.iter().map(|p| p.namespace.as_str()).collect();
@@ -1094,6 +1154,7 @@ mod tests {
             Some(&["a".to_string()]),
             limits,
             NOW,
+            None,
         )
         .await;
         assert!(s.coverage.timed_out);
@@ -1131,6 +1192,62 @@ mod tests {
         }
     }
 
+    #[tokio::test]
+    async fn usage_of_the_day_is_read_per_project_from_the_metrics_source() {
+        let (client, _server) = mock_raw(|_| list("List", vec![]));
+        let r = ReadClient::new(client, Duration::from_secs(5));
+        let metrics = FakeSource::new(|_, q| {
+            assert!(q.contains("[1d:5m]"), "окно правил — 24 ч: {q}");
+            let v = if q.contains("container_cpu_usage_seconds_total") {
+                0.4
+            } else {
+                1e8
+            };
+            Ok(vector(&[(
+                &[("pod", "web-1"), ("container", "app")][..],
+                v,
+            )]))
+        });
+        let s = collect(
+            &r,
+            &info(admin()),
+            &sample_profile("c1"),
+            Some(&["a".to_string()]),
+            LIMITS,
+            NOW,
+            Some(&metrics),
+        )
+        .await;
+        let usage = s.projects[0].usage.as_ref().unwrap();
+        let u = usage[&("web-1".to_string(), "app".to_string())];
+        assert_eq!(u.cpu_p95, Some(0.4));
+        assert_eq!(u.memory_max, Some(1e8));
+        assert_eq!(u.oom_count, None, "OOM — правило pod.oomKilled");
+        assert_eq!(
+            metrics.queries().len(),
+            3,
+            "не больше трёх запросов на проект"
+        );
+        let failing =
+            FakeSource::new(|_, _| Err(crate::metrics::prometheus::Failure::bad_response("x")));
+        let s = collect(
+            &r,
+            &info(admin()),
+            &sample_profile("c1"),
+            Some(&["a".to_string()]),
+            LIMITS,
+            NOW,
+            Some(&failing),
+        )
+        .await;
+        assert!(s.projects[0].usage.is_none());
+        assert!(
+            gap_lines(&s).contains(&"a resources CONNECTION_FAILED".to_string()),
+            "{:?}",
+            gap_lines(&s)
+        );
+    }
+
     #[tokio::test]
     async fn an_absent_kind_is_not_a_gap() {
         // Gateway API 1.0 без GRPCRoute v1 и кластер без IngressClass отвечают 404.
@@ -1141,6 +1258,7 @@ mod tests {
         let r = ReadClient::new(client, Duration::from_secs(5));
         let mut i = info(admin());
         i.api_groups.gateway_api = true;
+        let metrics = FakeSource::new(|_, _| Ok(vector(&[])));
         let s = collect(
             &r,
             &i,
@@ -1148,6 +1266,7 @@ mod tests {
             Some(&["a".to_string()]),
             LIMITS,
             NOW,
+            Some(&metrics),
         )
         .await;
         assert_eq!(gap_lines(&s), Vec::<String>::new());
@@ -1169,7 +1288,16 @@ mod tests {
             _ => list("List", vec![]),
         });
         let r = ReadClient::new(client, Duration::from_secs(5));
-        let s = collect(&r, &info(admin()), &sample_profile("c1"), None, LIMITS, NOW).await;
+        let s = collect(
+            &r,
+            &info(admin()),
+            &sample_profile("c1"),
+            None,
+            LIMITS,
+            NOW,
+            None,
+        )
+        .await;
         assert!(s.readyz.is_none());
         assert_eq!(
             gap_lines(&s),
@@ -1240,6 +1368,7 @@ mod tests {
             Some(&["a".to_string()]),
             LIMITS,
             NOW,
+            None,
         )
         .await;
         let findings =
@@ -1285,6 +1414,7 @@ mod tests {
             Some(&requested),
             LIMITS,
             NOW,
+            None,
         )
         .await;
         assert_eq!(
@@ -1292,6 +1422,7 @@ mod tests {
             [
                 "- nodes FORBIDDEN",
                 "- pods (all projects) FORBIDDEN",
+                "- resources METRICS_UNAVAILABLE",
                 "a pods FORBIDDEN",
                 "a secrets FORBIDDEN",
                 "b secrets FORBIDDEN",
@@ -1324,6 +1455,7 @@ mod tests {
             Some(&["x".to_string()]),
             limits,
             NOW,
+            None,
         )
         .await;
         assert!(s.coverage.pod_limit && s.projects[0].pods.is_none());
diff --git a/src-tauri/src/health/rules/mod.rs b/src-tauri/src/health/rules/mod.rs
index c8a0db9..7c5e91b 100644
--- a/src-tauri/src/health/rules/mod.rs
+++ b/src-tauri/src/health/rules/mod.rs
@@ -1,10 +1,11 @@
 //! Каталог правил и их строгость по окружению (спец. §8.2). Правила — чистые функции над
-//! `ClusterSnapshot`: сеть и файлы здесь не трогаются. Правила `resources.*` — этап O3.
+//! `ClusterSnapshot`: сеть и файлы здесь не трогаются.
 
 mod cluster;
 mod network;
 pub mod pods;
 mod project;
+mod resources;
 mod workloads;
 
 use serde_json::Value;
@@ -76,6 +77,9 @@ pub const RULES: &[RuleDef] = &[
     rule("gatewayRoute.notAccepted", C, W, W),
     rule("gatewayRoute.refsUnresolved", C, W, W),
     rule("gateway.notProgrammed", C, C, W),
+    rule("resources.cpuThrottled", W, W, I),
+    rule("resources.memoryNearLimit", W, W, I),
+    rule("resources.overProvisioned", I, I, I),
     rule("check.partial", I, I, I),
 ];
 
@@ -161,6 +165,7 @@ pub fn evaluate(s: &ClusterSnapshot, env: Environment) -> Vec<Finding> {
     workloads::check(s, &mut out);
     project::check(s, &mut out);
     network::check(s, &mut out);
+    resources::check(s, &mut out);
     cluster::partial(s, &mut out);
     out.into_sorted()
 }
@@ -189,7 +194,7 @@ mod tests {
         ids.sort();
         ids.dedup();
         assert_eq!(ids.len(), n);
-        assert_eq!(n, 34, "37 правил §8.2 без трёх resources.* (этап O3)");
+        assert_eq!(n, 37, "все правила §8.2");
     }
 
     #[test]
@@ -229,6 +234,9 @@ mod tests {
             ("gatewayRoute.notAccepted", "КПП"),
             ("gatewayRoute.refsUnresolved", "КПП"),
             ("gateway.notProgrammed", "ККП"),
+            ("resources.cpuThrottled", "ППИ"),
+            ("resources.memoryNearLimit", "ППИ"),
+            ("resources.overProvisioned", "ИИИ"),
             ("check.partial", "ИИИ"),
         ];
         let sym = |s: Option<Severity>| match s {
diff --git a/src-tauri/src/health/service.rs b/src-tauri/src/health/service.rs
index 09bbd3b..aff7424 100644
--- a/src-tauri/src/health/service.rs
+++ b/src-tauri/src/health/service.rs
@@ -303,8 +303,11 @@ fn collector(state: &AppState, connection_id: &str) -> Collector {
         let (st, id) = (st.clone(), id.clone());
         Box::pin(async move {
             let profile = st.connections.get(&id)?;
+            let (st2, id2) = (st.clone(), id.clone());
             st.read(&id, |c| async move {
                 let now = now_ms() / 1000;
+                // Источник истории метрик — для правил `resources.*`; без него — разрыв.
+                let sel = crate::metrics::select::for_connection(&st2, &id2, &c, false).await?;
                 Ok(collect(
                     &c.reader(),
                     &c.info,
@@ -312,6 +315,7 @@ fn collector(state: &AppState, connection_id: &str) -> Collector {
                     requested.as_deref(),
                     LIMITS,
                     now,
+                    sel.history.as_deref(),
                 )
                 .await)
             })
diff --git a/src-tauri/src/health/snapshot.rs b/src-tauri/src/health/snapshot.rs
index dbc08fe..ae8e9da 100644
--- a/src-tauri/src/health/snapshot.rs
+++ b/src-tauri/src/health/snapshot.rs
@@ -103,6 +103,34 @@ pub struct ContainerSnap {
     pub cpu_request: bool,
     pub memory_request: bool,
     pub memory_limit: bool,
+    /// Значения requests и limits (правила `resources.*`).
+    pub amounts: ContainerAmounts,
+}
+
+/// Requests и limits контейнера пода: миллиядра и байты, `None` — не задано.
+#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
+pub struct ContainerAmounts {
+    pub cpu_request_milli: Option<i64>,
+    pub memory_request: Option<i64>,
+    pub cpu_limit_milli: Option<i64>,
+    pub memory_limit: Option<i64>,
+}
+
+impl ContainerAmounts {
+    fn amounts(cpu_milli: Option<i64>, memory: Option<i64>) -> crate::resources::current::Amounts {
+        crate::resources::current::Amounts {
+            cpu: cpu_milli.map(|m| m as f64 / 1000.0),
+            memory: memory.map(|b| b as f64),
+        }
+    }
+
+    pub fn requests(&self) -> crate::resources::current::Amounts {
+        Self::amounts(self.cpu_request_milli, self.memory_request)
+    }
+
+    pub fn limits(&self) -> crate::resources::current::Amounts {
+        Self::amounts(self.cpu_limit_milli, self.memory_limit)
+    }
 }
 
 #[derive(Debug, Clone, Default, PartialEq, Eq)]
@@ -271,6 +299,9 @@ pub struct ProjectSnap {
     pub quotas: Option<Vec<QuotaSnap>>,
     /// Имена Secret проекта (значения не читаются); `None` — нет прав на `list secrets`.
     pub secrets: Option<BTreeSet<String>>,
+    /// Потребление по поду и контейнеру за 24 ч (правила `resources.*`): `None` — нет источника
+    /// истории метрик или не прочитано.
+    pub usage: Option<BTreeMap<(String, String), crate::resources::project::Use>>,
 }
 
 /// Часть, которую не удалось прочитать: что, в каком проекте и код ошибки (`FORBIDDEN`…).
diff --git a/src-tauri/src/health/texts.rs b/src-tauri/src/health/texts.rs
index cba6a51..4409231 100644
--- a/src-tauri/src/health/texts.rs
+++ b/src-tauri/src/health/texts.rs
@@ -378,6 +378,37 @@ pub fn english(f: &Finding) -> (String, String) {
                 because(f)
             ),
         ),
+        "resources.cpuThrottled" => (
+            format!("{o}: CPU is throttled"),
+            format!(
+                "Containers with a CPU limit were throttled on average over the last 24 h: {}. The limit cuts their CPU time, so responses slow down.",
+                list(f, "containers")
+            ),
+        ),
+        "resources.memoryNearLimit" => (
+            format!("{o}: memory close to the limit"),
+            format!(
+                "Containers used 90% or more of their memory limit over the last 24 h (max / limit): {}. The next spike may end in OOMKilled.",
+                list(f, "containers")
+            ),
+        ),
+        "resources.overProvisioned" => {
+            // Пустой список не называется, как у `container.noRequests`.
+            let parts: Vec<String> = [("CPU (request / p95)", "cpu"), ("memory (request / max)", "memory")]
+                .into_iter()
+                .filter_map(|(what, name)| {
+                    let l = list(f, name);
+                    (!l.is_empty()).then(|| format!("{what}: {l}"))
+                })
+                .collect();
+            (
+                format!("{o}: requests far above the usage"),
+                format!(
+                    "Requests are much higher than the usage over the last 24 h — {}. The reserved capacity is unavailable to other pods.",
+                    parts.join("; ")
+                ),
+            )
+        }
         "check.partial" => (
             "The check is incomplete".into(),
             {
diff --git a/src-tauri/src/k8s/quantity.rs b/src-tauri/src/k8s/quantity.rs
index fe62852..6688b38 100644
--- a/src-tauri/src/k8s/quantity.rs
+++ b/src-tauri/src/k8s/quantity.rs
@@ -53,6 +53,27 @@ fn number_end(b: &[u8]) -> usize {
     i
 }
 
+/// Ядра строкой quantity: `250m`, `2`.
+pub fn format_cpu(cores: f64) -> String {
+    let milli = (cores * 1000.0).round() as i64;
+    if milli % 1000 == 0 {
+        format!("{}", milli / 1000)
+    } else {
+        format!("{milli}m")
+    }
+}
+
+/// Байты строкой quantity: `2Gi`, `512Mi`, иначе — округлённые мебибайты.
+pub fn format_memory(bytes: f64) -> String {
+    const MI: f64 = 1024.0 * 1024.0;
+    let b = bytes.round();
+    if b != 0.0 && b % (1024.0 * MI) == 0.0 {
+        format!("{}Gi", (b / (1024.0 * MI)) as i64)
+    } else {
+        format!("{}Mi", (b / MI).round() as i64)
+    }
+}
+
 /// Доля `used` от `hard`; `None`, если что-то не разобрано или лимит нулевой.
 pub fn ratio(used: &str, hard: &str) -> Option<f64> {
     let hard = parse_quantity(hard).filter(|h| *h > 0.0)?;
@@ -87,6 +108,16 @@ mod tests {
         }
     }
 
+    #[test]
+    fn quantities_back_to_text() {
+        assert_eq!(format_cpu(0.25), "250m");
+        assert_eq!(format_cpu(2.0), "2");
+        assert_eq!(format_cpu(0.0101), "10m");
+        assert_eq!(format_memory(512.0 * 1024.0 * 1024.0), "512Mi");
+        assert_eq!(format_memory(2.0 * 1024.0 * 1024.0 * 1024.0), "2Gi");
+        assert_eq!(format_memory(1_000_000_000.0), "954Mi");
+    }
+
     #[test]
     fn ratios() {
         assert_eq!(ratio("500m", "1"), Some(0.5));
diff --git a/src-tauri/src/resources/project.rs b/src-tauri/src/resources/project.rs
index 25e9492..af75030 100644
--- a/src-tauri/src/resources/project.rs
+++ b/src-tauri/src/resources/project.rs
@@ -221,7 +221,8 @@ pub fn workload_of(
         .cloned()
 }
 
-fn kind_of(s: &str) -> Option<WorkloadKind> {
+/// Вид workload'а по имени вида Kubernetes (`Deployment`).
+pub fn kind_of(s: &str) -> Option<WorkloadKind> {
     WorkloadKind::ALL
         .into_iter()
         .find(|k| format!("{k:?}") == s)
diff --git a/src/components/health/findings.ts b/src/components/health/findings.ts
index df94904..05a1bc6 100644
--- a/src/components/health/findings.ts
+++ b/src/components/health/findings.ts
@@ -79,6 +79,7 @@ export function detailsKey(f: Finding): string {
       return [p.projectLimit, p.podLimit, p.timedOut].includes(true) || (Array.isArray(p.gaps) && p.gaps.length > 0) ? "details" : "detailsUnknown";
     // Пустой список не называется: «без request CPU: ;» читается как сбой.
     case "container.noRequests":
+    case "resources.overProvisioned":
       if (Array.isArray(p.cpu) && p.cpu.length === 0) return "detailsMemory";
       return Array.isArray(p.memory) && p.memory.length === 0 ? "detailsCpu" : "details";
     default:
````

<!-- block: locales t08-locales step=8.2 -->
````json
{
  "en": {
    "health": {
      "rules": {
        "resources.cpuThrottled": {
          "title": "{{object}}: CPU is throttled",
          "details": "Containers with a CPU limit were throttled on average over the last 24 hours: {{containers}}.",
          "fix": "Raise the CPU limit or remove it: the limit cuts CPU time and slows responses. The Resources tab shows the usage and a recommendation."
        },
        "resources.memoryNearLimit": {
          "title": "{{object}}: memory close to the limit",
          "details": "Containers used 90% or more of their memory limit over the last 24 hours (maximum / limit): {{containers}}.",
          "fix": "Raise the memory limit or reduce the usage: the next spike may end in OOMKilled. The Resources tab shows the usage and a recommendation."
        },
        "resources.overProvisioned": {
          "title": "{{object}}: requests far above the usage",
          "details": "Requests are much higher than the usage over the last 24 hours — CPU (request / p95): {{cpu}}; memory (request / maximum): {{memory}}.",
          "detailsCpu": "Requests are much higher than the usage over the last 24 hours — CPU (request / p95): {{cpu}}.",
          "detailsMemory": "Requests are much higher than the usage over the last 24 hours — memory (request / maximum): {{memory}}.",
          "fix": "Lower the requests closer to the usage: the reserved capacity is unavailable to other pods and counts against the quota. The Resources tab shows a recommendation."
        }
      },
      "ruleNames": {
        "resources.cpuThrottled": "CPU throttling",
        "resources.memoryNearLimit": "Memory near the limit",
        "resources.overProvisioned": "Excess requests"
      }
    }
  },
  "ru": {
    "health": {
      "rules": {
        "resources.cpuThrottled": {
          "title": "{{object}}: троттлинг CPU",
          "details": "Контейнеры с CPU limit в среднем за последние 24 часа упирались в лимит: {{containers}}.",
          "fix": "Поднимите CPU limit или уберите его: лимит урезает процессорное время и замедляет ответы. На вкладке «Ресурсы» — потребление и рекомендация."
        },
        "resources.memoryNearLimit": {
          "title": "{{object}}: память у лимита",
          "details": "Контейнеры за последние 24 часа занимали 90% memory limit и больше (максимум / лимит): {{containers}}.",
          "fix": "Поднимите memory limit или снизьте потребление: следующий всплеск может закончиться OOMKilled. На вкладке «Ресурсы» — потребление и рекомендация."
        },
        "resources.overProvisioned": {
          "title": "{{object}}: requests намного больше потребления",
          "details": "Requests намного больше потребления за последние 24 часа — CPU (request / p95): {{cpu}}; память (request / максимум): {{memory}}.",
          "detailsCpu": "Requests намного больше потребления за последние 24 часа — CPU (request / p95): {{cpu}}.",
          "detailsMemory": "Requests намного больше потребления за последние 24 часа — память (request / максимум): {{memory}}.",
          "fix": "Снизьте requests ближе к потреблению: зарезервированное недоступно другим подам и занимает квоту. На вкладке «Ресурсы» — рекомендация."
        }
      },
      "ruleNames": {
        "resources.cpuThrottled": "Троттлинг CPU",
        "resources.memoryNearLimit": "Память у лимита",
        "resources.overProvisioned": "Лишние requests"
      }
    }
  }
}
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 643 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm test`  
Expected: PASS — `Test Files 63 passed`, `Tests 479 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 180`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

Стенд kind (`dev/kind/README.md`; `KUBECONFIG` — отдельный файл, не `~/.kube/config`; после изменения стенда — заново `dev/kind/setup.sh`):  
Run: `GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --test it_health`  
Expected: PASS — `test result: ok. 2 passed; 0 failed`

- [ ] **Step 4: Commit.**

```bash
git add \
  src-tauri/src/health/collect.rs \
  src-tauri/src/health/rules/mod.rs \
  src-tauri/src/health/rules/resources.rs \
  src-tauri/src/health/service.rs \
  src-tauri/src/health/snapshot.rs \
  src-tauri/src/health/texts.rs \
  src-tauri/src/k8s/quantity.rs \
  src-tauri/src/resources/project.rs \
  src-tauri/tests/it_health.rs \
  src/components/health/findings.test.ts \
  src/components/health/findings.ts \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "feat(health): resources rules — CPU throttling, memory near the limit and excess requests over the last 24 hours from the metrics history; a check without it is partial" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 9: Модель изменения для форм, предложений и модели: правила формы, объект, применено как предложено; факты из кластера с предупреждениями и «было → станет»; страж изменений из Kafka-версии

**Files:**
- Port: `src-tauri/src/guard.rs` (с правками)
- Create: `src-tauri/src/change/facts.rs`, `src-tauri/src/change/mod.rs`
- Modify: `PORTED.md`, `src-tauri/src/lib.rs`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задачи 5–6: `WorkloadSpec { generation }`, `LimitRangeBounds`, `QuotaRoom`, `current`; Kafka-версия @ `8bbd0bd`: `src-tauri/src/guard.rs`.
- Produces: `guard::{ensure_mutation_allowed(profile, confirm), ensure_name_confirmed(expected, typed)}`; `change::{Change (tag kind: SetResources | Scale | RolloutRestart | SetHpaBounds | DeletePod), ResourceValues { cpu, memory: Option<Option<String>> }, ContainerResources, MAX_CONTAINERS, MAX_REPLICAS, Change::{kind_name, namespace, workload, target, trimmed}, validate_form, applied_as_proposed}`; `change::facts::{RESTARTED_AT, WorkloadFacts, HpaNow, Facts::{Workload, Hpa, Pod} + version() + resource_version(), read_facts, Warning (tag code), check, FieldChange, diff}`.

Спец. §9: один `Change` у форм, файла предложений и LLM. `null` в значении — убрать, отсутствие поля — не менять. Правила формы: quantity разбираются, `limits ≥ requests`, масштаб только у Deployment, StatefulSet и DeploymentConfig (0–1000), перезапуск не у Job и CronJob, ресурсы не у Job (шаблон Job неизменяем), `1 ≤ min ≤ max` у HPA. Факты читаются только чтением; предупреждения — HPA перезапишет реплики, масштаб в 0, граница `LimitRange`, запас квоты, под без контроллера. Версия объекта для формы — `metadata.generation` (у пода — `uid`): идущая выкладка меняет `resourceVersion` статусом, но не spec. Модуль только читает кластер, им пользуется и MCP.

- [ ] **Step 1: Перенос из Kafka-версии.**

Файлы копируются с коммита `8bbd0bd` репозитория `~/kafka-explorer` (`$KAFKA_REPO`) и правятся патчами — против перенесённого файла. Строки о них в `PORTED.md` добавляет шаг реализации.

<!-- block: shell t09-port step=9.1 -->
````bash
scripts/port.sh src-tauri/src/guard.rs src-tauri/src/guard.rs
````

<!-- block: patch t09-port-src-tauri-src-guard-rs step=9.1 -->
````diff
diff --git a/src-tauri/src/guard.rs b/src-tauri/src/guard.rs
--- a/src-tauri/src/guard.rs
+++ b/src-tauri/src/guard.rs
@@ -1,7 +1,7 @@
-//! Защита мутирующих операций (спец. §5): проверяется в Rust, а не только в UI.
+//! Защита изменяющих операций (спец. §9, §12): проверяется в Rust, а не только в UI.
 
 use crate::error::{AppResult, ErrorCode};
-use crate::storage::connections::{ConnectionProfile, Environment};
+use crate::storage::profile::{ConnectionProfile, Environment};
 
 /// read-only запрещает любые изменения; prod без read-only требует ввести имя подключения.
 pub fn ensure_mutation_allowed(p: &ConnectionProfile, confirm: Option<&str>) -> AppResult<()> {
@@ -24,7 +24,7 @@ pub fn ensure_mutation_allowed(p: &ConnectionProfile, confirm: Option<&str>) ->
     Ok(())
 }
 
-/// Удаление и очистка требуют ввести имя объекта (при удалении нескольких топиков — их число).
+/// Удаление пода без контроллера требует ввести имя пода.
 pub fn ensure_name_confirmed(expected: &str, typed: Option<&str>) -> AppResult<()> {
     if typed.map(str::trim) == Some(expected.trim()) {
         Ok(())
@@ -42,7 +42,7 @@ pub fn ensure_name_confirmed(expected: &str, typed: Option<&str>) -> AppResult<(
 mod tests {
     use super::*;
     use crate::error::ErrorCode;
-    use crate::storage::connections::{Environment, sample_profile};
+    use crate::storage::profile::{Environment, sample_profile};
 
     #[test]
     fn read_only_blocks_even_with_confirmation() {
@@ -59,18 +59,18 @@ mod tests {
     fn prod_requires_connection_name() {
         let mut p = sample_profile("a");
         p.environment = Environment::Prod;
-        p.name = "kafka-prod".into();
-        for confirm in [None, Some(""), Some("kafka-pro"), Some("KAFKA-PROD")] {
+        p.name = "ocp-prod".into();
+        for confirm in [None, Some(""), Some("ocp-pro"), Some("OCP-PROD")] {
             let e = ensure_mutation_allowed(&p, confirm).unwrap_err();
             assert_eq!(e.code, ErrorCode::ConfirmationRequired, "{confirm:?}");
             assert_eq!(e.key, "errors.guard.prodConfirm");
             assert_eq!(
                 e.message,
-                "To change a prod connection, type its name “kafka-prod”"
+                "To change a prod connection, type its name “ocp-prod”"
             );
         }
-        assert!(ensure_mutation_allowed(&p, Some("kafka-prod")).is_ok());
-        assert!(ensure_mutation_allowed(&p, Some(" kafka-prod ")).is_ok());
+        assert!(ensure_mutation_allowed(&p, Some("ocp-prod")).is_ok());
+        assert!(ensure_mutation_allowed(&p, Some(" ocp-prod ")).is_ok());
     }
 
     #[test]
@@ -89,16 +89,13 @@ mod tests {
 
     #[test]
     fn object_name_must_match_exactly() {
-        assert!(ensure_name_confirmed("orders", Some("orders")).is_ok());
-        assert!(ensure_name_confirmed("orders", Some(" orders ")).is_ok());
-        // Сравнение симметрично: id с пробелом на конце подтверждается (UI отправляет trim).
-        assert!(ensure_name_confirmed("grp ", Some("grp")).is_ok());
-        assert!(ensure_name_confirmed("grp ", Some("grp ")).is_ok());
-        for typed in [None, Some("order"), Some("ORDERS")] {
-            let e = ensure_name_confirmed("orders", typed).unwrap_err();
+        assert!(ensure_name_confirmed("web-1", Some("web-1")).is_ok());
+        assert!(ensure_name_confirmed("web-1", Some(" web-1 ")).is_ok());
+        for typed in [None, Some("web"), Some("WEB-1")] {
+            let e = ensure_name_confirmed("web-1", typed).unwrap_err();
             assert_eq!(e.code, ErrorCode::ConfirmationRequired, "{typed:?}");
             assert_eq!(e.key, "errors.guard.typeToConfirm");
-            assert_eq!(e.message, "Type “orders” to confirm");
+            assert_eq!(e.message, "Type “web-1” to confirm");
         }
     }
 }
````

- [ ] **Step 2: Тесты.**

<!-- block: file t09-src-tauri-src-change-facts-rs-tests step=9.2 path=src-tauri/src/change/facts.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use super::*;
    use crate::k8s::pods::WorkloadRef;
    use crate::k8s::testing::{mock_client, status};
    use crate::k8s::workloads::WorkloadKind;

    const MI: f64 = 1024.0 * 1024.0;

    fn cluster(s: &crate::k8s::testing::Seen) -> (u16, serde_json::Value) {
        let list = |kind: &str, items: serde_json::Value| {
            (
                200,
                json!({"apiVersion": "v1", "kind": kind, "metadata": {}, "items": items}),
            )
        };
        match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => (
                200,
                json!({"apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": {"name": "web", "namespace": "team-a", "resourceVersion": "11", "generation": 2},
                    "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "web"}},
                        "template": {"metadata": {"labels": {"app": "web"},
                            "annotations": {"kubectl.kubernetes.io/restartedAt": "2026-10-01T00:00:00Z"}},
                        "spec": {"initContainers": [{"name": "migrate", "image": "m"}],
                            "containers": [{"name": "app", "image": "web:1",
                            "resources": {"requests": {"cpu": "500m", "memory": "512Mi"},
                                          "limits": {"memory": "1Gi"}}}]}}}}),
            ),
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers" => list(
                "HorizontalPodAutoscalerList",
                json!([{"metadata": {"name": "web-hpa"}, "spec": {"minReplicas": 2, "maxReplicas": 6,
                    "scaleTargetRef": {"kind": "Deployment", "name": "web", "apiVersion": "apps/v1"}}}]),
            ),
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers/web-hpa" => (
                200,
                json!({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                    "metadata": {"name": "web-hpa", "resourceVersion": "5", "generation": 1},
                    "spec": {"minReplicas": 2, "maxReplicas": 6,
                        "scaleTargetRef": {"kind": "Deployment", "name": "web"}}}),
            ),
            "/api/v1/namespaces/team-a/limitranges" => list(
                "LimitRangeList",
                json!([{"metadata": {"name": "lr"}, "spec": {"limits": [
                    {"type": "Container", "max": {"memory": "2Gi"}, "min": {"cpu": "100m"}}]}}]),
            ),
            "/api/v1/namespaces/team-a/resourcequotas" => list(
                "ResourceQuotaList",
                json!([{"metadata": {"name": "compute"}, "status": {
                    "hard": {"requests.memory": "4Gi"}, "used": {"requests.memory": "3Gi"}}}]),
            ),
            "/api/v1/namespaces/team-a/pods/solo" => (
                200,
                json!({"apiVersion": "v1", "kind": "Pod",
                    "metadata": {"name": "solo", "resourceVersion": "3", "uid": "u-solo"}, "spec": {"containers": []}}),
            ),
            _ => status(404, "NotFound", "not found"),
        }
    }

    fn reader() -> ReadClient {
        ReadClient::new(mock_client(cluster).0, Duration::from_secs(5))
    }

    fn web() -> WorkloadRef {
        WorkloadRef {
            kind: WorkloadKind::Deployment,
            name: "web".into(),
        }
    }

    fn set(containers: serde_json::Value) -> Change {
        serde_json::from_value(json!({
            "kind": "setResources", "namespace": "team-a",
            "workload": {"kind": "Deployment", "name": "web"}, "containers": containers
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn resources_are_checked_against_the_template_limit_range_and_quota() {
        let r = reader();
        let c = set(
            json!([{"name": "app", "requests": {"cpu": "50m", "memory": "1Gi"}, "limits": {"memory": "3Gi"}}]),
        );
        let f = read_facts(&r, &c).await.unwrap();
        assert_eq!(
            (f.resource_version(), f.version().as_deref()),
            (Some("11"), Some("2"))
        );
        let w = check(&c, &f).unwrap();
        assert!(w.contains(&Warning::LimitRange {
            container: "app".into(),
            field: Field::CpuRequest,
            bound: Bound::Min,
            limit: 0.1
        }));
        assert!(w.contains(&Warning::LimitRange {
            container: "app".into(),
            field: Field::MemoryLimit,
            bound: Bound::Max,
            limit: 2048.0 * MI
        }));
        // (1Gi − 512Mi) × 2 реплики + 1Gi × 1 под выкладки = 2Gi > 1Gi свободно.
        assert!(
            w.contains(&Warning::Quota {
                quota: "compute".into(),
                resource: "requests.memory".into(),
                needed: 2048.0 * MI,
                free: 1024.0 * MI
            }),
            "{w:?}"
        );
        assert_eq!(
            diff(&c, &f, "now"),
            [
                FieldChange {
                    container: Some("app".into()),
                    field: "requests.cpu".into(),
                    before: Some("500m".into()),
                    after: Some("50m".into())
                },
                FieldChange {
                    container: Some("app".into()),
                    field: "requests.memory".into(),
                    before: Some("512Mi".into()),
                    after: Some("1Gi".into())
                },
                FieldChange {
                    container: Some("app".into()),
                    field: "limits.memory".into(),
                    before: Some("1Gi".into()),
                    after: Some("3Gi".into())
                },
            ]
        );
        // Request больше текущего limit — ошибка, а не предупреждение.
        let e = check(
            &set(json!([{"name": "app", "requests": {"memory": "2Gi"}}])),
            &f,
        )
        .unwrap_err();
        assert_eq!(e.key, "errors.change.limitBelowRequest");
        let e = check(
            &set(json!([{"name": "ghost", "requests": {"cpu": "1"}}])),
            &f,
        )
        .unwrap_err();
        assert_eq!(e.key, "errors.change.noContainer");
        // Init-контейнер есть в шаблоне; «то же значение» в другой записи — не изменение.
        let same = set(
            json!([{"name": "app", "requests": {"cpu": "0.5"}}, {"name": "migrate", "limits": {"cpu": "1"}}]),
        );
        assert!(check(&same, &f).unwrap().is_empty());
        assert_eq!(diff(&same, &f, "now").len(), 1);
    }

    #[tokio::test]
    async fn scale_restart_hpa_and_pod() {
        let r = reader();
        let scale = Change::Scale {
            namespace: "team-a".into(),
            workload: web(),
            replicas: 0,
        };
        let f = read_facts(&r, &scale).await.unwrap();
        assert_eq!(
            check(&scale, &f).unwrap(),
            [
                Warning::HpaManaged {
                    hpa: "web-hpa".into()
                },
                Warning::ScaleToZero
            ]
        );
        assert_eq!(diff(&scale, &f, "now")[0].before.as_deref(), Some("2"));
        let restart = Change::RolloutRestart {
            namespace: "team-a".into(),
            workload: web(),
        };
        let d = diff(
            &restart,
            &read_facts(&r, &restart).await.unwrap(),
            "2026-10-28T12:00:00Z",
        );
        assert_eq!(d[0].before.as_deref(), Some("2026-10-01T00:00:00Z"));
        assert_eq!(d[0].after.as_deref(), Some("2026-10-28T12:00:00Z"));
        let hpa = |min| Change::SetHpaBounds {
            namespace: "team-a".into(),
            hpa: "web-hpa".into(),
            min_replicas: min,
            max_replicas: 1,
        };
        let f = read_facts(&r, &hpa(None)).await.unwrap();
        assert_eq!(
            (f.resource_version(), f.version().as_deref()),
            (Some("5"), Some("1"))
        );
        assert_eq!(
            check(&hpa(None), &f).unwrap_err().key,
            "errors.change.hpaBounds",
            "текущий min 2 > max 1"
        );
        assert!(check(&hpa(Some(1)), &f).is_ok());
        assert_eq!(diff(&hpa(Some(1)), &f, "now").len(), 2);
        let pod = Change::DeletePod {
            namespace: "team-a".into(),
            pod: "solo".into(),
        };
        let f = read_facts(&r, &pod).await.unwrap();
        assert_eq!(check(&pod, &f).unwrap(), [Warning::Orphan]);
        assert_eq!(f.version().as_deref(), Some("u-solo"));
        let missing = Change::DeletePod {
            namespace: "team-a".into(),
            pod: "ghost".into(),
        };
        assert_eq!(
            read_facts(&r, &missing).await.unwrap_err().code,
            crate::error::ErrorCode::NotFound
        );
    }
}
````

<!-- block: file t09-src-tauri-src-change-mod-rs-tests step=9.2 path=src-tauri/src/change/mod.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn web() -> WorkloadRef {
        WorkloadRef {
            kind: WorkloadKind::Deployment,
            name: "web".into(),
        }
    }

    fn set(containers: serde_json::Value) -> Change {
        serde_json::from_value(json!({
            "kind": "setResources", "namespace": "team-a",
            "workload": {"kind": "Deployment", "name": "web"}, "containers": containers
        }))
        .unwrap()
    }

    #[test]
    fn json_shape_keeps_absent_null_and_values_apart() {
        let c = set(json!([{"name": "app", "requests": {"cpu": "250m", "memory": null}}]));
        let Change::SetResources { containers, .. } = &c else {
            panic!()
        };
        assert_eq!(containers[0].requests.cpu, Some(Some("250m".into())));
        assert_eq!(containers[0].requests.memory, Some(None), "null — убрать");
        assert_eq!(containers[0].limits.cpu, None, "нет поля — не менять");
        assert_eq!(
            serde_json::to_value(&c).unwrap()["containers"][0],
            json!({"name": "app", "requests": {"cpu": "250m", "memory": null}, "limits": {}})
        );
        let h: Change = serde_json::from_value(json!({
            "kind": "setHpaBounds", "namespace": "team-a", "hpa": "web", "maxReplicas": 5
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(&h).unwrap(),
            json!({
                "kind": "setHpaBounds", "namespace": "team-a", "hpa": "web", "maxReplicas": 5
            })
        );
        assert!(
            serde_json::from_value::<Change>(json!({"kind": "deleteNamespace", "namespace": "x"}))
                .is_err()
        );
    }

    #[test]
    fn targets_and_names() {
        let s = Change::Scale {
            namespace: "team-a".into(),
            workload: web(),
            replicas: 3,
        };
        assert_eq!(s.kind_name(), "scale");
        assert_eq!(
            s.target(),
            Target::workload("team-a", WorkloadKind::Deployment, "web")
        );
        let h = Change::SetHpaBounds {
            namespace: " team-a ".into(),
            hpa: " web ".into(),
            min_replicas: None,
            max_replicas: 3,
        }
        .trimmed();
        assert_eq!(h.target(), Target::object(TargetKind::Hpa, "team-a", "web"));
        let p = Change::DeletePod {
            namespace: "team-a".into(),
            pod: "web-1".into(),
        };
        assert_eq!(
            p.target(),
            Target::object(TargetKind::Pod, "team-a", "web-1")
        );
        let c = set(json!([{"name": " app ", "requests": {"cpu": " 1 "}}])).trimmed();
        let Change::SetResources { containers, .. } = c else {
            panic!()
        };
        assert_eq!(
            (
                containers[0].name.as_str(),
                containers[0].requests.cpu.clone()
            ),
            ("app", Some(Some("1".into())))
        );
    }

    #[test]
    fn form_rules() {
        let key = |c: Change| validate_form(&c).err().map(|e| e.key);
        assert_eq!(
            key(set(
                json!([{"name": "app", "requests": {"cpu": "250m"}, "limits": {"cpu": "1"}}])
            )),
            None
        );
        assert_eq!(
            key(set(json!([]))).as_deref(),
            Some("errors.change.containersCount")
        );
        assert_eq!(
            key(set(json!([{"name": "app", "requests": {"cpu": "1"}}, {"name": "app", "limits": {"cpu": "2"}}]))).as_deref(),
            Some("errors.change.duplicateContainer")
        );
        assert_eq!(
            key(set(json!([{"name": "app"}]))).as_deref(),
            Some("errors.change.nothingToChange")
        );
        assert_eq!(
            key(set(json!([{"name": "app", "requests": {"cpu": "lots"}}]))).as_deref(),
            Some("errors.change.badQuantity")
        );
        assert_eq!(
            key(set(
                json!([{"name": "app", "requests": {"memory": "-1Mi"}}])
            ))
            .as_deref(),
            Some("errors.change.badQuantity")
        );
        assert_eq!(
            key(set(json!([{"name": "app", "requests": {"memory": "1Gi"}, "limits": {"memory": "512Mi"}}]))).as_deref(),
            Some("errors.change.limitBelowRequest")
        );
        assert_eq!(
            key(set(json!([{"name": "a/b", "requests": {"cpu": "1"}}]))).as_deref(),
            Some("errors.k8s.badName")
        );
        let scale = |kind: WorkloadKind, replicas| Change::Scale {
            namespace: "team-a".into(),
            workload: WorkloadRef {
                kind,
                name: "web".into(),
            },
            replicas,
        };
        assert_eq!(key(scale(WorkloadKind::StatefulSet, 0)), None);
        assert_eq!(
            key(scale(WorkloadKind::Deployment, 1001)).as_deref(),
            Some("errors.change.replicas")
        );
        assert_eq!(
            key(scale(WorkloadKind::DaemonSet, 1)).as_deref(),
            Some("errors.change.scaleKind")
        );
        let restart = |kind| Change::RolloutRestart {
            namespace: "team-a".into(),
            workload: WorkloadRef {
                kind,
                name: "web".into(),
            },
        };
        assert_eq!(key(restart(WorkloadKind::DaemonSet)), None);
        assert_eq!(
            key(restart(WorkloadKind::CronJob)).as_deref(),
            Some("errors.change.restartKind")
        );
        let hpa = |min, max| Change::SetHpaBounds {
            namespace: "team-a".into(),
            hpa: "web".into(),
            min_replicas: min,
            max_replicas: max,
        };
        assert_eq!(key(hpa(Some(2), 5)), None);
        assert_eq!(key(hpa(None, 1)), None);
        for (min, max) in [(Some(0), 5), (Some(6), 5), (None, 0), (None, 1001)] {
            assert_eq!(
                key(hpa(min, max)).as_deref(),
                Some("errors.change.hpaBounds"),
                "{min:?} {max}"
            );
        }
        let bad_ns = Change::DeletePod {
            namespace: "Team_A".into(),
            pod: "x".into(),
        };
        assert_eq!(
            key(bad_ns).as_deref(),
            Some("errors.connections.badNamespace")
        );
    }

    #[test]
    fn applied_is_compared_by_values_not_by_spelling() {
        let a = set(
            json!([{"name": "app", "requests": {"cpu": "500m"}}, {"name": "side", "limits": {"memory": "1Gi"}}]),
        );
        let b = set(
            json!([{"name": "side", "limits": {"memory": "1024Mi"}}, {"name": "app", "requests": {"cpu": "0.5"}}]),
        );
        assert!(applied_as_proposed(&a, &b));
        let edited = set(
            json!([{"name": "app", "requests": {"cpu": "600m"}}, {"name": "side", "limits": {"memory": "1Gi"}}]),
        );
        assert!(!applied_as_proposed(&a, &edited));
        let dropped = set(json!([{"name": "app", "requests": {"cpu": "500m"}}]));
        assert!(!applied_as_proposed(&a, &dropped));
        let scale = |n| Change::Scale {
            namespace: "team-a".into(),
            workload: web(),
            replicas: n,
        };
        assert!(applied_as_proposed(&scale(3), &scale(3)));
        assert!(!applied_as_proposed(&scale(3), &scale(4)));
        assert!(!applied_as_proposed(&scale(3), &a), "другой вид");
        let restart = Change::RolloutRestart {
            namespace: "team-a".into(),
            workload: web(),
        };
        assert!(applied_as_proposed(&restart, &restart.clone()));
    }
}
````

- [ ] **Step 3: Реализация.**

<!-- block: file t09-src-tauri-src-change-facts-rs-impl step=9.3 path=src-tauri/src/change/facts.rs part=impl -->
````rust
//! Изменение против прочитанных объектов (спец. §9, §10.5): что в кластере сейчас (`Facts`,
//! только чтение), проверка (объект и контейнеры есть, `limits ≥ requests` с текущими
//! значениями, предупреждения `LimitRange`, квоты и HPA) и «было → станет» по полям.

use std::collections::BTreeMap;

use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::{Container, LimitRange, Pod, ResourceQuota};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::ListParams;
use serde::Serialize;

use super::{Change, ContainerResources, check_bounds, check_limits};
use crate::error::AppResult;
use crate::k8s::quantity::parse_quantity;
use crate::k8s::read::ReadClient;
use crate::k8s::workloads::{WorkloadSpec, get_spec};
use crate::resources::constraints::{limit_range_bounds, quota_rooms};
use crate::resources::current::Amounts;
use crate::resources::recommend::{Bound, Field, LimitRangeBounds, QuotaRoom};

/// Аннотация шаблона, которой перезапускает `kubectl rollout restart`.
pub const RESTARTED_AT: &str = "kubectl.kubernetes.io/restartedAt";

/// Значения quantity строками, как в объекте.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Texts {
    pub cpu: Option<String>,
    pub memory: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerNow {
    pub name: String,
    pub init: bool,
    pub requests: Texts,
    pub limits: Texts,
}

impl ContainerNow {
    fn amounts(t: &Texts) -> Amounts {
        let q = |v: &Option<String>| v.as_deref().and_then(parse_quantity);
        Amounts {
            cpu: q(&t.cpu),
            memory: q(&t.memory),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HpaNow {
    pub name: String,
    pub min_replicas: Option<i32>,
    pub max_replicas: i32,
    pub resource_version: Option<String>,
    pub generation: Option<i64>,
}

/// Workload изменения сейчас: шаблон, HPA и ограничения проекта.
#[derive(Debug, Clone)]
pub struct WorkloadFacts {
    pub spec: WorkloadSpec,
    pub containers: Vec<ContainerNow>,
    /// HPA, который масштабирует workload.
    pub hpa: Option<HpaNow>,
    pub restarted_at: Option<String>,
    pub limit_range: Option<LimitRangeBounds>,
    pub quotas: Vec<QuotaRoom>,
}

/// Объект изменения в кластере сейчас.
#[derive(Debug, Clone)]
pub enum Facts {
    Workload(Box<WorkloadFacts>),
    Hpa(HpaNow),
    Pod {
        name: String,
        /// Под создан контроллером (его пересоздадут после удаления).
        controlled: bool,
        resource_version: Option<String>,
        /// Тот же объект, а не пересозданный под с тем же именем.
        uid: Option<String>,
    },
}

impl Facts {
    /// Версия объекта для формы: у workload'а и HPA — `metadata.generation` (растёт при изменении
    /// spec, а не статуса: идущая выкладка не делает предпросмотр устаревшим), у пода — `uid`.
    pub fn version(&self) -> Option<String> {
        match self {
            Facts::Workload(w) => w.spec.generation.map(|g| g.to_string()),
            Facts::Hpa(h) => h.generation.map(|g| g.to_string()),
            Facts::Pod { uid, .. } => uid.clone(),
        }
    }

    /// `resourceVersion` объекта при чтении: с ним идёт запись — объект, изменённый между чтением
    /// и записью, даёт ответ 409.
    pub fn resource_version(&self) -> Option<&str> {
        match self {
            Facts::Workload(w) => w.spec.resource_version.as_deref(),
            Facts::Hpa(h) => h.resource_version.as_deref(),
            Facts::Pod {
                resource_version, ..
            } => resource_version.as_deref(),
        }
    }
}

fn texts(m: Option<&BTreeMap<String, Quantity>>) -> Texts {
    let v = |k: &str| m.and_then(|m| m.get(k)).map(|q| q.0.clone());
    Texts {
        cpu: v("cpu"),
        memory: v("memory"),
    }
}

fn container_now(c: &Container, init: bool) -> ContainerNow {
    let r = c.resources.as_ref();
    ContainerNow {
        name: c.name.clone(),
        init,
        requests: texts(r.and_then(|r| r.requests.as_ref())),
        limits: texts(r.and_then(|r| r.limits.as_ref())),
    }
}

fn hpa_now(h: &HorizontalPodAutoscaler) -> Option<HpaNow> {
    let s = h.spec.as_ref()?;
    Some(HpaNow {
        name: h.metadata.name.clone()?,
        min_replicas: s.min_replicas,
        max_replicas: s.max_replicas,
        resource_version: h.metadata.resource_version.clone(),
        generation: h.metadata.generation,
    })
}

/// Читает объект изменения и то, против чего его проверяют. Объекта нет — `NOT_FOUND`.
pub async fn read_facts(r: &ReadClient, change: &Change) -> AppResult<Facts> {
    let ns = change.namespace();
    let all = ListParams::default();
    match change {
        Change::SetResources { workload, .. }
        | Change::Scale { workload, .. }
        | Change::RolloutRestart { workload, .. } => {
            let spec = get_spec(r, ns, workload.kind, &workload.name).await?;
            let (hpas, lrs, quotas) = tokio::join!(
                r.list::<HorizontalPodAutoscaler>(Some(ns), &all),
                r.list::<LimitRange>(Some(ns), &all),
                r.list::<ResourceQuota>(Some(ns), &all),
            );
            let kind = format!("{:?}", workload.kind);
            let hpa = hpas.ok().and_then(|l| {
                l.items
                    .iter()
                    .find(|h| {
                        h.spec.as_ref().is_some_and(|s| {
                            s.scale_target_ref.kind == kind
                                && s.scale_target_ref.name == workload.name
                        })
                    })
                    .and_then(hpa_now)
            });
            let template = spec.template.as_ref();
            let pod_spec = template.and_then(|t| t.spec.as_ref());
            let containers = pod_spec
                .map(|s| {
                    let init = s
                        .init_containers
                        .iter()
                        .flatten()
                        .map(|c| container_now(c, true));
                    init.chain(s.containers.iter().map(|c| container_now(c, false)))
                        .collect()
                })
                .unwrap_or_default();
            let restarted_at = template
                .and_then(|t| t.metadata.as_ref())
                .and_then(|m| m.annotations.as_ref())
                .and_then(|a| a.get(RESTARTED_AT).cloned());
            Ok(Facts::Workload(Box::new(WorkloadFacts {
                spec,
                containers,
                hpa,
                restarted_at,
                limit_range: lrs.ok().and_then(|l| limit_range_bounds(&l.items)),
                quotas: quotas.map(|l| quota_rooms(&l.items)).unwrap_or_default(),
            })))
        }
        Change::SetHpaBounds { hpa, .. } => {
            let h: HorizontalPodAutoscaler = r.get(ns, hpa).await?;
            hpa_now(&h)
                .map(Facts::Hpa)
                .ok_or_else(|| crate::k8s::errors::malformed("HorizontalPodAutoscaler", hpa))
        }
        Change::DeletePod { pod, .. } => {
            let p: Pod = r.get(ns, pod).await?;
            Ok(Facts::Pod {
                name: pod.clone(),
                controlled: p
                    .metadata
                    .owner_references
                    .iter()
                    .flatten()
                    .any(|o| o.controller == Some(true)),
                resource_version: p.metadata.resource_version.clone(),
                uid: p.metadata.uid.clone(),
            })
        }
    }
}

/// Предупреждение к изменению: его видно до подтверждения, применить можно.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "code", rename_all = "camelCase")]
pub enum Warning {
    /// Workload масштабирует HPA: число реплик он перезапишет.
    HpaManaged { hpa: String },
    /// Масштаб в 0: подов не останется.
    ScaleToZero,
    /// Значение вне границы `LimitRange`: API-сервер его отклонит (это покажет dry-run).
    LimitRange {
        container: String,
        field: Field,
        bound: Bound,
        limit: f64,
    },
    /// Запаса квоты не хватит с учётом реплик и подов выкладки.
    Quota {
        quota: String,
        resource: String,
        needed: f64,
        free: f64,
    },
    /// Под без контроллера: после удаления его никто не создаст заново.
    Orphan,
}

fn not_in_template(container: &str) -> crate::error::AppError {
    crate::app_err!(
        crate::error::ErrorCode::InvalidInput,
        "errors.change.noContainer",
        "There is no container {{container}} in the pod template",
        container = container
    )
}

/// Значения контейнера после изменения: requests и limits.
fn after(c: &ContainerResources, now: &ContainerNow) -> (Amounts, Amounts) {
    let v = |change: &Option<Option<String>>, current: Option<f64>| match change {
        None => current,
        Some(None) => None,
        Some(Some(q)) => parse_quantity(q),
    };
    let (r, l) = (
        ContainerNow::amounts(&now.requests),
        ContainerNow::amounts(&now.limits),
    );
    (
        Amounts {
            cpu: v(&c.requests.cpu, r.cpu),
            memory: v(&c.requests.memory, r.memory),
        },
        Amounts {
            cpu: v(&c.limits.cpu, l.cpu),
            memory: v(&c.limits.memory, l.memory),
        },
    )
}

/// Проверка изменения против кластера: ошибка — изменение неверно (нет контейнера, limit
/// меньше request с текущими значениями, границы HPA), предупреждения — применить можно.
pub fn check(change: &Change, facts: &Facts) -> AppResult<Vec<Warning>> {
    let mut out = Vec::new();
    match (change, facts) {
        (Change::SetResources { containers, .. }, Facts::Workload(w)) => {
            let WorkloadFacts {
                spec,
                containers: now,
                limit_range,
                quotas,
                ..
            } = w.as_ref();
            let mut delta = (
                Amounts::default(),
                Amounts::default(),
                Amounts::default(),
                Amounts::default(),
            );
            for c in containers {
                let n = now
                    .iter()
                    .find(|x| x.name == c.name)
                    .ok_or_else(|| not_in_template(&c.name))?;
                let (req, lim) = after(c, n);
                check_limits(&c.name, req.cpu, lim.cpu, "CPU")?;
                check_limits(&c.name, req.memory, lim.memory, "memory")?;
                if let Some(lr) = limit_range {
                    for (field, v, cpu) in [
                        (Field::CpuRequest, req.cpu, true),
                        (Field::CpuLimit, lim.cpu, true),
                        (Field::MemoryRequest, req.memory, false),
                        (Field::MemoryLimit, lim.memory, false),
                    ] {
                        let pick = |a: Amounts| if cpu { a.cpu } else { a.memory };
                        let Some(v) = v else { continue };
                        if let Some(m) = pick(lr.min).filter(|m| v < *m) {
                            out.push(Warning::LimitRange {
                                container: c.name.clone(),
                                field,
                                bound: Bound::Min,
                                limit: m,
                            });
                        }
                        if let Some(m) = pick(lr.max).filter(|m| v > *m) {
                            out.push(Warning::LimitRange {
                                container: c.name.clone(),
                                field,
                                bound: Bound::Max,
                                limit: m,
                            });
                        }
                    }
                }
                if !n.init {
                    let (r0, l0) = (
                        ContainerNow::amounts(&n.requests),
                        ContainerNow::amounts(&n.limits),
                    );
                    let add = |a: &mut Amounts, new: Amounts, old: Amounts| {
                        a.cpu = Some(
                            a.cpu.unwrap_or(0.0) + new.cpu.unwrap_or(0.0) - old.cpu.unwrap_or(0.0),
                        );
                        a.memory = Some(
                            a.memory.unwrap_or(0.0) + new.memory.unwrap_or(0.0)
                                - old.memory.unwrap_or(0.0),
                        );
                    };
                    add(&mut delta.0, req, r0);
                    add(&mut delta.1, lim, l0);
                    add(&mut delta.2, req, Amounts::default());
                    add(&mut delta.3, lim, Amounts::default());
                }
            }
            // Квота: прирост за все реплики плюс поды выкладки с новыми значениями.
            let replicas = f64::from(spec.row.desired.unwrap_or(1).max(0));
            let surge = f64::from(spec.surge.max(0));
            for q in quotas {
                let (d, new) = match q.resource.as_str() {
                    "requests.cpu" | "cpu" => (delta.0.cpu, delta.2.cpu),
                    "requests.memory" | "memory" => (delta.0.memory, delta.2.memory),
                    "limits.cpu" => (delta.1.cpu, delta.3.cpu),
                    "limits.memory" => (delta.1.memory, delta.3.memory),
                    _ => continue,
                };
                let needed = d.unwrap_or(0.0) * replicas + new.unwrap_or(0.0) * surge;
                let free = q.hard - q.used;
                if needed > free + 1e-9 && needed > 0.0 {
                    out.push(Warning::Quota {
                        quota: q.quota.clone(),
                        resource: q.resource.clone(),
                        needed,
                        free,
                    });
                }
            }
        }
        (Change::Scale { replicas, .. }, Facts::Workload(w)) => {
            if let Some(h) = &w.hpa {
                out.push(Warning::HpaManaged {
                    hpa: h.name.clone(),
                });
            }
            if *replicas == 0 {
                out.push(Warning::ScaleToZero);
            }
        }
        (Change::RolloutRestart { .. }, Facts::Workload(_)) => {}
        (
            Change::SetHpaBounds {
                min_replicas,
                max_replicas,
                ..
            },
            Facts::Hpa(h),
        ) => check_bounds(min_replicas.or(h.min_replicas), *max_replicas)?,
        (Change::DeletePod { .. }, Facts::Pod { controlled, .. }) => {
            if !controlled {
                out.push(Warning::Orphan);
            }
        }
        _ => {
            return Err(crate::error::AppError::internal_with(
                "facts do not match the change",
            ));
        }
    }
    Ok(out)
}

/// Поле «было → станет».
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldChange {
    /// Контейнер у ресурсов, иначе `None`.
    pub container: Option<String>,
    /// `requests.cpu`, `limits.memory`, `replicas`, `minReplicas`, `maxReplicas`, `restartedAt`,
    /// `pod`.
    pub field: String,
    pub before: Option<String>,
    pub after: Option<String>,
}

fn field(
    container: Option<&str>,
    name: &str,
    before: Option<String>,
    after: Option<String>,
) -> FieldChange {
    FieldChange {
        container: container.map(String::from),
        field: name.into(),
        before,
        after,
    }
}

/// «Было → станет»: только поля, которые меняются; у перезапуска — время в аннотации
/// шаблона, у удаления пода — сам под.
pub fn diff(change: &Change, facts: &Facts, now_rfc3339: &str) -> Vec<FieldChange> {
    let mut out = Vec::new();
    match (change, facts) {
        (Change::SetResources { containers, .. }, Facts::Workload(w)) => {
            let now = &w.containers;
            for c in containers {
                let Some(n) = now.iter().find(|x| x.name == c.name) else {
                    continue;
                };
                for (name, change, current) in [
                    ("requests.cpu", &c.requests.cpu, &n.requests.cpu),
                    ("requests.memory", &c.requests.memory, &n.requests.memory),
                    ("limits.cpu", &c.limits.cpu, &n.limits.cpu),
                    ("limits.memory", &c.limits.memory, &n.limits.memory),
                ] {
                    let Some(new) = change else { continue };
                    let same = match (new, current) {
                        (Some(a), Some(b)) => parse_quantity(a) == parse_quantity(b),
                        (None, None) => true,
                        _ => false,
                    };
                    if !same {
                        out.push(field(Some(&c.name), name, current.clone(), new.clone()));
                    }
                }
            }
        }
        (Change::Scale { replicas, .. }, Facts::Workload(w)) => {
            let before = w.spec.row.desired;
            if before != Some(*replicas) {
                out.push(field(
                    None,
                    "replicas",
                    before.map(|v| v.to_string()),
                    Some(replicas.to_string()),
                ));
            }
        }
        (Change::RolloutRestart { .. }, Facts::Workload(w)) => {
            out.push(field(
                None,
                "restartedAt",
                w.restarted_at.clone(),
                Some(now_rfc3339.into()),
            ));
        }
        (
            Change::SetHpaBounds {
                min_replicas,
                max_replicas,
                ..
            },
            Facts::Hpa(h),
        ) => {
            if let Some(m) = min_replicas
                && h.min_replicas != Some(*m)
            {
                out.push(field(
                    None,
                    "minReplicas",
                    h.min_replicas.map(|v| v.to_string()),
                    Some(m.to_string()),
                ));
            }
            if h.max_replicas != *max_replicas {
                out.push(field(
                    None,
                    "maxReplicas",
                    Some(h.max_replicas.to_string()),
                    Some(max_replicas.to_string()),
                ));
            }
        }
        (Change::DeletePod { .. }, Facts::Pod { name, .. }) => {
            out.push(field(None, "pod", Some(name.clone()), None));
        }
        _ => {}
    }
    out
}
````

<!-- block: file t09-src-tauri-src-change-mod-rs-impl step=9.3 path=src-tauri/src/change/mod.rs part=impl -->
````rust
//! Вид изменения `Change` (спец. §9): одна и та же форма в командах интерфейса, в файле
//! предложений и у LLM. Здесь — модель, проверка формы, объект изменения и сравнение
//! применённого с предложенным (чистые функции), проверка против прочитанных объектов и
//! «было → станет» (`facts`). Запросы записи в кластер — только в `ops`: этот модуль кластер
//! только читает, им пользуется и MCP-шлюз.

pub mod facts;

use serde::{Deserialize, Deserializer, Serialize};

use crate::error::{AppError, AppResult, ErrorCode};
use crate::health::model::{Target, TargetKind};
use crate::k8s::pods::WorkloadRef;
use crate::k8s::quantity::parse_quantity;
use crate::k8s::workloads::WorkloadKind;

/// Контейнеров в одном `setResources`.
pub const MAX_CONTAINERS: usize = 50;
/// Реплик в `scale` и границ HPA.
pub const MAX_REPLICAS: i32 = 1000;

/// Нет поля — `None` (не менять), `null` — `Some(None)` (убрать), строка — `Some(Some(…))`.
fn present<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(d).map(Some)
}

/// requests или limits контейнера: quantity строкой, `null` — убрать, нет поля — не менять.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceValues {
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub cpu: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub memory: Option<Option<String>>,
}

impl ResourceValues {
    fn is_empty(&self) -> bool {
        self.cpu.is_none() && self.memory.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerResources {
    pub name: String,
    #[serde(default)]
    pub requests: ResourceValues,
    #[serde(default)]
    pub limits: ResourceValues,
}

/// Изменение кластера (спец. §9). `deletePod` предложить нельзя (§10.5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Change {
    SetResources {
        namespace: String,
        workload: WorkloadRef,
        containers: Vec<ContainerResources>,
    },
    Scale {
        namespace: String,
        workload: WorkloadRef,
        replicas: i32,
    },
    RolloutRestart {
        namespace: String,
        workload: WorkloadRef,
    },
    #[serde(rename_all = "camelCase")]
    SetHpaBounds {
        namespace: String,
        hpa: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min_replicas: Option<i32>,
        max_replicas: i32,
    },
    DeletePod {
        namespace: String,
        pod: String,
    },
}

impl Change {
    /// Имя вида — как `kind` в JSON (журнал, ключ повтора предложения).
    pub fn kind_name(&self) -> &'static str {
        match self {
            Change::SetResources { .. } => "setResources",
            Change::Scale { .. } => "scale",
            Change::RolloutRestart { .. } => "rolloutRestart",
            Change::SetHpaBounds { .. } => "setHpaBounds",
            Change::DeletePod { .. } => "deletePod",
        }
    }

    pub fn namespace(&self) -> &str {
        match self {
            Change::SetResources { namespace, .. }
            | Change::Scale { namespace, .. }
            | Change::RolloutRestart { namespace, .. }
            | Change::SetHpaBounds { namespace, .. }
            | Change::DeletePod { namespace, .. } => namespace,
        }
    }

    pub fn workload(&self) -> Option<&WorkloadRef> {
        match self {
            Change::SetResources { workload, .. }
            | Change::Scale { workload, .. }
            | Change::RolloutRestart { workload, .. } => Some(workload),
            _ => None,
        }
    }

    /// Объект изменения: workload, HPA или под.
    pub fn target(&self) -> Target {
        let ns = self.namespace();
        match self {
            Change::SetResources { workload, .. }
            | Change::Scale { workload, .. }
            | Change::RolloutRestart { workload, .. } => {
                Target::workload(ns, workload.kind, &workload.name)
            }
            Change::SetHpaBounds { hpa, .. } => Target::object(TargetKind::Hpa, ns, hpa),
            Change::DeletePod { pod, .. } => Target::object(TargetKind::Pod, ns, pod),
        }
    }

    /// Имена без пробелов по краям: так их проверяют и хранят.
    pub fn trimmed(mut self) -> Self {
        let t = |s: &mut String| *s = s.trim().to_string();
        match &mut self {
            Change::SetResources {
                namespace,
                workload,
                containers,
            } => {
                t(namespace);
                t(&mut workload.name);
                for c in containers {
                    t(&mut c.name);
                    for v in [
                        &mut c.requests.cpu,
                        &mut c.requests.memory,
                        &mut c.limits.cpu,
                        &mut c.limits.memory,
                    ] {
                        if let Some(Some(q)) = v {
                            t(q);
                        }
                    }
                }
            }
            Change::Scale {
                namespace,
                workload,
                ..
            }
            | Change::RolloutRestart {
                namespace,
                workload,
            } => {
                t(namespace);
                t(&mut workload.name);
            }
            Change::SetHpaBounds { namespace, hpa, .. } => {
                t(namespace);
                t(hpa);
            }
            Change::DeletePod { namespace, pod } => {
                t(namespace);
                t(pod);
            }
        }
        self
    }
}

/// Неверное изменение: текст для человека (ключ словаря) и, у LLM, `invalid_argument` с
/// английским текстом.
fn invalid(text: crate::i18n::LocalizedText) -> AppError {
    AppError::localized(ErrorCode::InvalidInput, text)
}

/// Quantity поля `field` разбирается и не отрицательна.
fn quantity(v: &str, container: &str, field: &str) -> AppResult<f64> {
    match parse_quantity(v) {
        Some(q) if q >= 0.0 && q.is_finite() && !v.is_empty() => Ok(q),
        _ => Err(invalid(crate::ltext!(
            "errors.change.badQuantity",
            "Container {{container}}: “{{value}}” is not a valid {{field}} quantity",
            container = container,
            value = v.escape_debug(),
            field = field
        ))),
    }
}

/// Форма изменения без кластера: имена, числа, quantity, `limits ≥ requests` внутри изменения
/// (с текущими значениями — `facts::check`).
pub fn validate_form(c: &Change) -> AppResult<()> {
    crate::k8s::read::check_namespace(c.namespace())?;
    if let Some(w) = c.workload() {
        crate::k8s::read::check_name(&w.name)?;
    }
    match c {
        Change::SetResources { containers, .. } => {
            if containers.is_empty() || containers.len() > MAX_CONTAINERS {
                return Err(invalid(crate::ltext!(
                    "errors.change.containersCount",
                    "List 1 to {{max}} containers",
                    max = MAX_CONTAINERS
                )));
            }
            let mut seen = std::collections::BTreeSet::new();
            for ct in containers {
                crate::k8s::read::check_name(&ct.name)?;
                if !seen.insert(ct.name.as_str()) {
                    return Err(invalid(crate::ltext!(
                        "errors.change.duplicateContainer",
                        "Container {{container}} is listed twice",
                        container = ct.name
                    )));
                }
                if ct.requests.is_empty() && ct.limits.is_empty() {
                    return Err(invalid(crate::ltext!(
                        "errors.change.nothingToChange",
                        "Container {{container}}: nothing to change",
                        container = ct.name
                    )));
                }
                let mut values = [None; 4];
                for (i, (v, field)) in [
                    (&ct.requests.cpu, "CPU request"),
                    (&ct.requests.memory, "memory request"),
                    (&ct.limits.cpu, "CPU limit"),
                    (&ct.limits.memory, "memory limit"),
                ]
                .into_iter()
                .enumerate()
                {
                    if let Some(Some(q)) = v {
                        values[i] = Some(quantity(q, &ct.name, field)?);
                    }
                }
                check_limits(&ct.name, values[0], values[2], "CPU")?;
                check_limits(&ct.name, values[1], values[3], "memory")?;
            }
        }
        Change::Scale {
            workload, replicas, ..
        } => {
            if !matches!(
                workload.kind,
                WorkloadKind::Deployment
                    | WorkloadKind::StatefulSet
                    | WorkloadKind::DeploymentConfig
            ) {
                return Err(invalid(crate::ltext!(
                    "errors.change.scaleKind",
                    "Only a Deployment, StatefulSet or DeploymentConfig can be scaled"
                )));
            }
            if !(0..=MAX_REPLICAS).contains(replicas) {
                return Err(invalid(crate::ltext!(
                    "errors.change.replicas",
                    "Replicas must be from 0 to {{max}}",
                    max = MAX_REPLICAS
                )));
            }
        }
        Change::RolloutRestart { workload, .. } => {
            if matches!(workload.kind, WorkloadKind::Job | WorkloadKind::CronJob) {
                return Err(invalid(crate::ltext!(
                    "errors.change.restartKind",
                    "A Job or CronJob cannot be restarted"
                )));
            }
        }
        Change::SetHpaBounds {
            hpa,
            min_replicas,
            max_replicas,
            ..
        } => {
            crate::k8s::read::check_name(hpa)?;
            check_bounds(*min_replicas, *max_replicas)?;
        }
        Change::DeletePod { pod, .. } => crate::k8s::read::check_name(pod)?,
    }
    Ok(())
}

/// `limits ≥ requests`, если заданы оба.
pub fn check_limits(
    container: &str,
    request: Option<f64>,
    limit: Option<f64>,
    what: &str,
) -> AppResult<()> {
    if let (Some(r), Some(l)) = (request, limit)
        && l < r
    {
        return Err(invalid(crate::ltext!(
            "errors.change.limitBelowRequest",
            "Container {{container}}: the {{resource}} limit is below the request",
            container = container,
            resource = what
        )));
    }
    Ok(())
}

/// `1 ≤ min ≤ max ≤ 1000`.
pub fn check_bounds(min: Option<i32>, max: i32) -> AppResult<()> {
    let ok = (1..=MAX_REPLICAS).contains(&max) && min.is_none_or(|m| (1..=max).contains(&m));
    if ok {
        return Ok(());
    }
    Err(invalid(crate::ltext!(
        "errors.change.hpaBounds",
        "HPA bounds must be 1 ≤ minReplicas ≤ maxReplicas ≤ {{max}}",
        max = MAX_REPLICAS
    )))
}

/// Одинаковые quantity: `500m` и `0.5` — одно и то же.
fn same_quantity(a: &Option<Option<String>>, b: &Option<Option<String>>) -> bool {
    match (a, b) {
        (None, None) | (Some(None), Some(None)) => true,
        (Some(Some(x)), Some(Some(y))) => match (parse_quantity(x), parse_quantity(y)) {
            (Some(x), Some(y)) => (x - y).abs() <= 1e-9 * x.abs().max(1.0),
            _ => x == y,
        },
        _ => false,
    }
}

/// Применено ровно то, что предлагалось (спец. §10.5): тот же вид и объект, ресурсы — по
/// контейнерам без учёта порядка и записи quantity, реплики и границы HPA — числами.
pub fn applied_as_proposed(proposed: &Change, applied: &Change) -> bool {
    if proposed.kind_name() != applied.kind_name() || proposed.target() != applied.target() {
        return false;
    }
    match (proposed, applied) {
        (
            Change::SetResources { containers: a, .. },
            Change::SetResources { containers: b, .. },
        ) => {
            a.len() == b.len()
                && a.iter().all(|x| {
                    b.iter().any(|y| {
                        x.name == y.name
                            && same_quantity(&x.requests.cpu, &y.requests.cpu)
                            && same_quantity(&x.requests.memory, &y.requests.memory)
                            && same_quantity(&x.limits.cpu, &y.limits.cpu)
                            && same_quantity(&x.limits.memory, &y.limits.memory)
                    })
                })
        }
        (Change::Scale { replicas: a, .. }, Change::Scale { replicas: b, .. }) => a == b,
        (
            Change::SetHpaBounds {
                min_replicas: a1,
                max_replicas: a2,
                ..
            },
            Change::SetHpaBounds {
                min_replicas: b1,
                max_replicas: b2,
                ..
            },
        ) => a1 == b1 && a2 == b2,
        _ => true,
    }
}
````

<!-- block: patch t09-src step=9.3 -->
````diff
diff --git a/PORTED.md b/PORTED.md
index 5b03219..28d951b 100644
--- a/PORTED.md
+++ b/PORTED.md
@@ -188,3 +188,4 @@
 | `src/components/common/ChunkBoundary.tsx` | `src/components/common/ChunkBoundary.tsx` | `c5446fc` | как есть |
 | `src/components/common/ExternalLink.tsx` | `src/components/common/ExternalLink.tsx` | `c5446fc` | как есть |
 | `docs/releases-repo/SUPPORT.md` | `docs/releases-repo/SUPPORT.md` | `c5446fc` | адаптирован: название продукта |
+| `src-tauri/src/guard.rs` | `src-tauri/src/guard.rs` | `8bbd0bd` | адаптирован: профиль подключения Kubernetes, ввод имени пода без контроллера |
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index d2d2ac3..86d2683 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -1,10 +1,12 @@
 #[macro_use]
 pub mod i18n;
 pub mod auth;
+pub mod change;
 pub mod commands;
 pub mod der;
 mod dock_name;
 pub mod error;
+pub mod guard;
 pub mod health;
 pub mod k8s;
 pub mod logging;
````

<!-- block: locales t09-locales step=9.3 -->
````json
{
  "en": {
    "errors": {
      "guard": {
        "readOnly": "Connection “{{name}}” is read-only",
        "prodConfirm": "To change a prod connection, type its name “{{name}}”",
        "typeToConfirm": "Type “{{expected}}” to confirm"
      },
      "change": {
        "badQuantity": "Container {{container}}: “{{value}}” is not a valid {{field}} quantity",
        "containersCount": "List 1 to {{max}} containers",
        "duplicateContainer": "Container {{container}} is listed twice",
        "nothingToChange": "Container {{container}}: nothing to change",
        "scaleKind": "Only a Deployment, StatefulSet or DeploymentConfig can be scaled",
        "replicas": "Replicas must be from 0 to {{max}}",
        "restartKind": "A Job or CronJob cannot be restarted",
        "limitBelowRequest": "Container {{container}}: the {{resource}} limit is below the request",
        "hpaBounds": "HPA bounds must be 1 ≤ minReplicas ≤ maxReplicas ≤ {{max}}",
        "noContainer": "There is no container {{container}} in the pod template"
      }
    }
  },
  "ru": {
    "errors": {
      "guard": {
        "readOnly": "Подключение «{{name}}» только для чтения",
        "prodConfirm": "Чтобы изменить prod-подключение, введите его имя «{{name}}»",
        "typeToConfirm": "Введите «{{expected}}» для подтверждения"
      },
      "change": {
        "badQuantity": "Контейнер {{container}}: «{{value}}» — не quantity для {{field}}",
        "containersCount": "Укажите от 1 до {{max}} контейнеров",
        "duplicateContainer": "Контейнер {{container}} указан дважды",
        "nothingToChange": "Контейнер {{container}}: нечего менять",
        "scaleKind": "Масштабировать можно только Deployment, StatefulSet и DeploymentConfig",
        "replicas": "Реплик — от 0 до {{max}}",
        "restartKind": "Job и CronJob перезапустить нельзя",
        "limitBelowRequest": "Контейнер {{container}}: limit {{resource}} меньше request",
        "hpaBounds": "Границы HPA: 1 ≤ minReplicas ≤ maxReplicas ≤ {{max}}",
        "noContainer": "В шаблоне подов нет контейнера {{container}}"
      }
    }
  }
}
````

- [ ] **Step 4: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 653 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm test`  
Expected: PASS — `Test Files 63 passed`, `Tests 479 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 193`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 5: Commit.**

```bash
git add \
  PORTED.md \
  src-tauri/src/change/facts.rs \
  src-tauri/src/change/mod.rs \
  src-tauri/src/guard.rs \
  src-tauri/src/lib.rs \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "feat(change): the change model of the forms, proposals and the model — form rules, the object, applied-as-proposed; facts from the cluster with warnings and a field diff; the guard of mutations from the Kafka edition" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 10: Предложения LLM в файле проверки: открыто, отклонено, снова открыто, отозвано, применено с правками или без; срок 7 дней, не больше 50 открытых; команды вкладки «Замечания»

**Files:**
- Create (по Kafka-версии): `src-tauri/src/health/proposals.rs` ← `src-tauri/src/health/proposals.rs`
- Modify: `PORTED.md`, `src-tauri/src/commands/health.rs`, `src-tauri/src/health/mod.rs`, `src-tauri/src/health/store.rs`, `src-tauri/src/lib.rs`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задача 9: `Change`, `applied_as_proposed`; O2: `health::{store, model}`, `commands::health::{HealthView, change}`; Kafka-версия @ `8bbd0bd`: `src-tauri/src/health/proposals.rs` (G3).
- Produces: `health::proposals::{PROPOSAL_TTL_MS, MAX_OPEN_PROPOSALS = 50, ProposalStatus, Proposal}`; `store::{propose, set_proposal_status, mark_applied(&Change), withdraw_own, delete_proposal, delete_all_proposals, NewProposal, Proposed, Withdrawn, MarkApplied}`; `HealthView.proposals`; `commands::health::{mark_applied, set_proposal_status, delete_proposal, delete_all_proposals}`.

Перенос модели G3 Kafka-версии на `change::Change`. Повторное предложение того же изменения тем же клиентом обновляет открытое, а не плодит новое; отозвать можно только своё (у безымянного клиента — из той же сессии); отметку «применено» ставит Rust после успешного `apply_change` (задача 11) — с `appliedChange`, если человек поправил значения. Устаревшие открытые предложения закрываются при чтении файла.

- [ ] **Step 1: Тесты.**

<!-- block: file t10-src-tauri-src-health-proposals-rs-tests step=10.1 path=src-tauri/src/health/proposals.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn json_shape_of_a_stored_proposal() {
        let p: Proposal = serde_json::from_value(json!({
            "id": "p1", "severity": "warning", "title": "t", "rationale": "r",
            "target": {"kind": "workload", "namespace": "team-a", "name": "web", "workloadKind": "Deployment"},
            "change": {"kind": "scale", "namespace": "team-a",
                "workload": {"kind": "Deployment", "name": "web"}, "replicas": 3},
            "client": "claude-ai", "createdAtMs": 1, "updatedAtMs": 2, "status": "open"
        }))
        .unwrap();
        assert_eq!(p.status, ProposalStatus::Open);
        assert_eq!(p.change.kind_name(), "scale");
        let v = serde_json::to_value(&p).unwrap();
        for absent in [
            "closedAtMs",
            "appliedAsProposed",
            "appliedChange",
            "rejectReason",
            "sessionId",
        ] {
            assert!(v.get(absent).is_none(), "{absent}: {v}");
        }
        assert_eq!(ProposalStatus::Withdrawn.as_str(), "withdrawn");
    }
}
````

- [ ] **Step 2: Реализация.**

<!-- block: file t10-src-tauri-src-health-proposals-rs-impl step=10.2 path=src-tauri/src/health/proposals.rs part=impl -->
````rust
//! Предложения LLM (спец. §10.5, как в Kafka G3): запись в `health/<connectionId>.json`, статусы
//! и сроки. Вид изменения — `change::Change`; предложение кластер не меняет: применяет его
//! человек формой изменения с обычными стражами, а «применено» ставит Rust после успеха.

use serde::{Deserialize, Serialize};

use super::model::{Severity, Target};
use crate::change::Change;

/// Срок предложения — 7 дней с создания, как у замечаний.
pub const PROPOSAL_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1000;
/// Открытых предложений на подключение.
pub const MAX_OPEN_PROPOSALS: usize = 50;
pub const MAX_REJECT_REASON_CHARS: usize = 300;
pub const MAX_RELATED_KEY_CHARS: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProposalStatus {
    Open,
    /// Человек применил формой (ставит только Rust после успеха операции).
    Applied,
    /// Человек отклонил; «Открыть снова» возвращает в `Open`.
    Rejected,
    /// LLM отозвала своё.
    Withdrawn,
}

impl ProposalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ProposalStatus::Open => "open",
            ProposalStatus::Applied => "applied",
            ProposalStatus::Rejected => "rejected",
            ProposalStatus::Withdrawn => "withdrawn",
        }
    }
}

/// Предложение в файле `health/<connectionId>.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Proposal {
    pub id: String,
    pub severity: Severity,
    pub title: String,
    pub rationale: String,
    pub target: Target,
    pub change: Change,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_rule_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_finding_key: Option<String>,
    /// Имя клиента из диалога подтверждения; `None` — безымянный.
    #[serde(default)]
    pub client: Option<String>,
    /// Сессия безымянного клиента: отозвать можно только из неё.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub status: ProposalStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<i64>,
    /// Только у `applied`: человек применил без правок (`true`) или с правками.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_as_proposed: Option<bool>,
    /// Только у `applied` с правками: что применено фактически.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_change: Option<Change>,
    /// Только у `rejected`: необязательный комментарий человека.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<String>,
}
````

<!-- block: patch t10-src step=10.2 -->
````diff
diff --git a/PORTED.md b/PORTED.md
index 28d951b..5fab08e 100644
--- a/PORTED.md
+++ b/PORTED.md
@@ -144,9 +144,9 @@
 | `crates/glafira-mcp-kubernetes/src/server/tools.rs` | `crates/glafira-mcp/src/server/tools.rs` | `8bbd0bd` | адаптирован: 13 инструментов чтения Kubernetes, маскировка в ответах |
 | `crates/glafira-mcp-kubernetes/tests/server.rs` | `crates/glafira-mcp/tests/server.rs` | `8bbd0bd` | адаптирован: фальшивый Backend Kubernetes, область LLM, маскировка |
 | `crates/glafira-mcp-kubernetes/src/server/health_tools.rs` | `crates/glafira-mcp/src/server/health_tools.rs` | `8bbd0bd` | адаптирован: проекты проверки, фильтр области LLM, объекты Kubernetes |
-| `src-tauri/src/health/store.rs` | `src-tauri/src/health/store.rs` | `8bbd0bd` | адаптирован: без предложений LLM (этап O3) и лага групп, проекты проверки, объекты Kubernetes |
+| `src-tauri/src/health/store.rs` | `src-tauri/src/health/store.rs` | `8bbd0bd` | адаптирован: без лага групп, проекты проверки, объекты Kubernetes; предложения LLM — с изменениями Kubernetes (`change::Change`) |
 | `src-tauri/src/health/service.rs` | `src-tauri/src/health/service.rs` | `8bbd0bd` | адаптирован: сборщик снимка Kubernetes, проекты проверки, без лага групп |
-| `src-tauri/src/commands/health.rs` | `src-tauri/src/commands/health.rs` | `8bbd0bd` | адаптирован: без предложений LLM (этап O3) |
+| `src-tauri/src/commands/health.rs` | `src-tauri/src/commands/health.rs` | `8bbd0bd` | адаптирован: отметка «применено» — по изменению Kubernetes (`change::Change`) |
 | `src-tauri/src/mcp/files.rs` | `src-tauri/src/mcp/files.rs` | `8bbd0bd` | адаптирован: ссылка на спеку |
 | `src-tauri/src/mcp/approvals.rs` | `src-tauri/src/mcp/approvals.rs` | `8bbd0bd` | адаптирован: крейт glafira-mcp-core, ссылка на спеку |
 | `src-tauri/src/mcp/backend.rs` | `src-tauri/src/mcp/backend.rs` | `8bbd0bd` | адаптирован: чтения Kubernetes через ReadClient, область llmNamespaces, проверка по проектам, объекты Kubernetes |
@@ -189,3 +189,4 @@
 | `src/components/common/ExternalLink.tsx` | `src/components/common/ExternalLink.tsx` | `c5446fc` | как есть |
 | `docs/releases-repo/SUPPORT.md` | `docs/releases-repo/SUPPORT.md` | `c5446fc` | адаптирован: название продукта |
 | `src-tauri/src/guard.rs` | `src-tauri/src/guard.rs` | `8bbd0bd` | адаптирован: профиль подключения Kubernetes, ввод имени пода без контроллера |
+| `src-tauri/src/health/proposals.rs` | `src-tauri/src/health/proposals.rs` | `8bbd0bd` | адаптирован: модель, статусы и сроки предложения; вид изменения — `change::Change` (§9) |
diff --git a/src-tauri/src/commands/health.rs b/src-tauri/src/commands/health.rs
index 0fd50b5..9897431 100644
--- a/src-tauri/src/commands/health.rs
+++ b/src-tauri/src/commands/health.rs
@@ -5,18 +5,24 @@ use serde::Serialize;
 use tauri::State;
 
 use super::blocking;
+use crate::change::Change;
 use crate::error::AppResult;
+use crate::health::proposals::{Proposal, ProposalStatus};
 use crate::health::service::now_ms;
-use crate::health::store::{Annotation, AnnotationStatus, CheckRun, HealthStore, Trigger};
+use crate::health::store::{
+    Annotation, AnnotationStatus, CheckRun, HealthStore, MarkApplied, Trigger,
+};
 use crate::state::AppState;
 
-/// Состояние подключения для интерфейса: последняя проверка, скрытые находки и замечания LLM.
+/// Состояние подключения для интерфейса: последняя проверка, скрытые находки, замечания и
+/// предложения LLM.
 #[derive(Debug, Clone, Serialize)]
 #[serde(rename_all = "camelCase")]
 pub struct HealthView {
     pub last_check: Option<CheckRun>,
     pub ignored: Vec<String>,
     pub annotations: Vec<Annotation>,
+    pub proposals: Vec<Proposal>,
 }
 
 fn view(st: &AppState, connection_id: &str) -> AppResult<HealthView> {
@@ -26,9 +32,50 @@ fn view(st: &AppState, connection_id: &str) -> AppResult<HealthView> {
         last_check: f.last_check,
         ignored: f.ignored.into_iter().map(|i| i.key).collect(),
         annotations: f.annotations,
+        proposals: f.proposals,
     })
 }
 
+/// Отметка «применено» после успеха изменения в кластере (спец. §9, шаг 5). Изменение уже
+/// прошло, поэтому ничто здесь его не отменяет: предложения нет, оно закрыто или про другое
+/// изменение — отметка пропускается без события; ошибка файла — только в лог. Без
+/// `proposal_id` ничего не делает.
+pub async fn mark_applied(
+    st: &AppState,
+    connection_id: &str,
+    proposal_id: Option<String>,
+    applied: Change,
+) {
+    let Some(pid) = proposal_id else {
+        return;
+    };
+    let (st, id, pid2) = (st.clone(), connection_id.to_string(), pid.clone());
+    let r = blocking(move || {
+        let generation = st.health.generation(&id);
+        st.connections.get(&id)?;
+        let r = st.health.write(&id, generation, |s| {
+            s.mark_applied(&id, &pid2, &applied, now_ms())
+        })?;
+        if matches!(r, MarkApplied::Applied { .. }) {
+            st.health.changed(&id);
+        }
+        Ok(r)
+    })
+    .await;
+    match r {
+        Ok(MarkApplied::Applied { .. }) => {}
+        Ok(MarkApplied::Skipped(why)) => {
+            tracing::warn!(
+                "proposal {pid} of connection {connection_id} not marked applied: {why}"
+            );
+        }
+        Err(e) => tracing::warn!(
+            "proposal {pid} of connection {connection_id} not marked applied: {}",
+            e.message
+        ),
+    }
+}
+
 /// Изменение файла подключения на blocking-потоке, затем событие и свежее состояние. Запись идёт
 /// через `HealthService::write`: поколение читается до проверки подключения, и подключение,
 /// удалённое в промежутке, файла не получит.
@@ -121,6 +168,44 @@ pub async fn delete_all_annotations(
     .await
 }
 
+/// «Отклонить» (`rejected`, причина необязательна) или «Открыть снова» (`open`).
+#[tauri::command]
+pub async fn set_proposal_status(
+    state: State<'_, AppState>,
+    connection_id: String,
+    proposal_id: String,
+    status: ProposalStatus,
+    reason: Option<String>,
+) -> AppResult<HealthView> {
+    change(&state, connection_id, move |s, id| {
+        s.set_proposal_status(id, &proposal_id, status, reason, now_ms())
+    })
+    .await
+}
+
+#[tauri::command]
+pub async fn delete_proposal(
+    state: State<'_, AppState>,
+    connection_id: String,
+    proposal_id: String,
+) -> AppResult<HealthView> {
+    change(&state, connection_id, move |s, id| {
+        s.delete_proposal(id, &proposal_id, now_ms())
+    })
+    .await
+}
+
+#[tauri::command]
+pub async fn delete_all_proposals(
+    state: State<'_, AppState>,
+    connection_id: String,
+) -> AppResult<HealthView> {
+    change(&state, connection_id, |s, id| {
+        s.delete_all_proposals(id, now_ms()).map(|_| ())
+    })
+    .await
+}
+
 #[cfg(test)]
 mod tests {
     use std::sync::Arc;
@@ -128,7 +213,7 @@ mod tests {
 
     use super::*;
     use crate::health::model::{Severity, Target};
-    use crate::health::store::{Annotated, Author, NewAnnotation};
+    use crate::health::store::{Annotated, Author, NewAnnotation, NewProposal, Proposed};
     use crate::secrets::MemoryStore;
     use crate::storage::profile::sample_profile;
 
@@ -203,4 +288,111 @@ mod tests {
         assert_eq!(e.key, "errors.connections.notFound");
         assert!(!dir.path().join("health").join("nope.json").exists());
     }
+
+    fn scale(replicas: i32) -> Change {
+        Change::Scale {
+            namespace: "team-a".into(),
+            workload: crate::k8s::pods::WorkloadRef {
+                kind: crate::k8s::workloads::WorkloadKind::Deployment,
+                name: "web".into(),
+            },
+            replicas,
+        }
+    }
+
+    fn proposal(st: &AppState) -> String {
+        match st
+            .health
+            .store
+            .propose(
+                "c1",
+                NewProposal {
+                    severity: Severity::Warning,
+                    title: "Scale out".into(),
+                    rationale: "r".into(),
+                    change: scale(3),
+                    related_rule_id: None,
+                    related_finding_key: None,
+                    author: Author {
+                        client: Some("claude-ai".into()),
+                        session_id: "s".into(),
+                    },
+                },
+                now_ms(),
+            )
+            .unwrap()
+        {
+            Proposed::Created(id) => id,
+            other => panic!("{other:?}"),
+        }
+    }
+
+    #[tokio::test]
+    async fn proposal_commands_change_status_and_delete() {
+        let dir = tempfile::tempdir().unwrap();
+        let st = state(dir.path());
+        let n = Arc::new(AtomicUsize::new(0));
+        let n2 = n.clone();
+        st.health.set_notify(Arc::new(move |_| {
+            n2.fetch_add(1, Ordering::SeqCst);
+        }));
+        let id = proposal(&st);
+        let v = change(&st, "c1".into(), {
+            let id = id.clone();
+            move |s, c| {
+                s.set_proposal_status(
+                    c,
+                    &id,
+                    ProposalStatus::Rejected,
+                    Some("later".into()),
+                    now_ms(),
+                )
+            }
+        })
+        .await
+        .unwrap();
+        assert_eq!(v.proposals[0].status, ProposalStatus::Rejected);
+        assert_eq!(v.proposals[0].reject_reason.as_deref(), Some("later"));
+        let v = change(&st, "c1".into(), move |s, c| {
+            s.delete_proposal(c, &id, now_ms())
+        })
+        .await
+        .unwrap();
+        assert!(v.proposals.is_empty());
+        proposal(&st);
+        let v = change(&st, "c1".into(), |s, c| {
+            s.delete_all_proposals(c, now_ms()).map(|_| ())
+        })
+        .await
+        .unwrap();
+        assert!(v.proposals.is_empty());
+        assert_eq!(n.load(Ordering::SeqCst), 3);
+    }
+
+    #[tokio::test]
+    async fn mark_applied_closes_the_proposal_and_notifies() {
+        let dir = tempfile::tempdir().unwrap();
+        let st = state(dir.path());
+        let n = Arc::new(AtomicUsize::new(0));
+        let n2 = n.clone();
+        st.health.set_notify(Arc::new(move |_| {
+            n2.fetch_add(1, Ordering::SeqCst);
+        }));
+        let id = proposal(&st);
+        mark_applied(&st, "c1", None, scale(3)).await;
+        assert_eq!(n.load(Ordering::SeqCst), 0, "без id предложения — ничего");
+        mark_applied(&st, "c1", Some(id.clone()), scale(4)).await;
+        let p = &view(&st, "c1").unwrap().proposals[0];
+        assert_eq!(
+            (p.status, p.applied_as_proposed, p.applied_change.clone()),
+            (ProposalStatus::Applied, Some(false), Some(scale(4)))
+        );
+        assert_eq!(n.load(Ordering::SeqCst), 1);
+        // Неизвестное предложение и удалённое подключение — не ошибка: изменение уже прошло.
+        mark_applied(&st, "c1", Some("nope".into()), scale(4)).await;
+        assert_eq!(n.load(Ordering::SeqCst), 1);
+        st.delete_connection_full("c1").unwrap();
+        mark_applied(&st, "c1", Some(id), scale(4)).await;
+        assert!(!dir.path().join("health").join("c1.json").exists());
+    }
 }
diff --git a/src-tauri/src/health/mod.rs b/src-tauri/src/health/mod.rs
index d5954bb..0f875ec 100644
--- a/src-tauri/src/health/mod.rs
+++ b/src-tauri/src/health/mod.rs
@@ -6,6 +6,7 @@ pub const CHANGED_EVENT: &str = "health://changed";
 
 pub mod collect;
 pub mod model;
+pub mod proposals;
 pub mod rules;
 pub mod service;
 pub mod snapshot;
diff --git a/src-tauri/src/health/store.rs b/src-tauri/src/health/store.rs
index bebe4c1..80932dd 100644
--- a/src-tauri/src/health/store.rs
+++ b/src-tauri/src/health/store.rs
@@ -1,6 +1,6 @@
-//! Файл `health/<connectionId>.json` (спец. §8.1, §13): последняя проверка, скрытые находки и
-//! замечания LLM. Срок замечания — 7 дней с создания; открытых — не больше 200 на подключение.
-//! Предложения LLM (этап O3) добавятся в этот же файл.
+//! Файл `health/<connectionId>.json` (спец. §8.1, §10.5, §13): последняя проверка, скрытые
+//! находки, замечания и предложения LLM. Срок замечания и предложения — 7 дней с создания;
+//! открытых замечаний — не больше 200, предложений — 50 на подключение.
 
 use std::path::{Path, PathBuf};
 use std::sync::Mutex;
@@ -8,6 +8,10 @@ use std::sync::Mutex;
 use serde::{Deserialize, Serialize};
 
 use super::model::{Finding, Severity, Target};
+use super::proposals::{
+    MAX_OPEN_PROPOSALS, MAX_REJECT_REASON_CHARS, PROPOSAL_TTL_MS, Proposal, ProposalStatus,
+};
+use crate::change::{Change, applied_as_proposed};
 use crate::error::{AppError, AppResult, ErrorCode};
 use crate::storage::fs::{read_json_or_quarantine, write_json};
 use crate::storage::profile::Environment;
@@ -110,6 +114,9 @@ pub struct HealthFile {
     pub ignored: Vec<Ignored>,
     #[serde(default)]
     pub annotations: Vec<Annotation>,
+    /// Предложения LLM: любой статус до истечения срока.
+    #[serde(default)]
+    pub proposals: Vec<Proposal>,
 }
 
 impl HealthFile {
@@ -120,6 +127,8 @@ impl HealthFile {
     fn purge(&mut self, now_ms: i64) {
         self.annotations
             .retain(|a| now_ms - a.created_at_ms < ANNOTATION_TTL_MS);
+        self.proposals
+            .retain(|p| now_ms - p.created_at_ms < PROPOSAL_TTL_MS);
     }
 }
 
@@ -135,6 +144,10 @@ impl Author {
         self.owns_record(&a.client, a.session_id.as_deref())
     }
 
+    fn owns_proposal(&self, p: &Proposal) -> bool {
+        self.owns_record(&p.client, p.session_id.as_deref())
+    }
+
     /// Именованный клиент владеет записями с тем же именем; безымянный — только своей сессии.
     fn owns_record(&self, client: &Option<String>, session_id: Option<&str>) -> bool {
         match (&self.client, client) {
@@ -170,6 +183,45 @@ pub enum Removed {
     NotOwner,
 }
 
+/// Новое предложение LLM (спец. §10.5).
+#[derive(Debug, Clone)]
+pub struct NewProposal {
+    pub severity: Severity,
+    pub title: String,
+    pub rationale: String,
+    pub change: Change,
+    pub related_rule_id: Option<String>,
+    pub related_finding_key: Option<String>,
+    pub author: Author,
+}
+
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub enum Proposed {
+    Created(String),
+    /// Открытое предложение того же автора, вида и объекта обновлено.
+    Updated(String),
+    LimitReached,
+}
+
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum Withdrawn {
+    Withdrawn,
+    NotFound,
+    NotOwner,
+    /// Уже не открытое: применено, отклонено или отозвано раньше.
+    Closed(ProposalStatus),
+}
+
+/// Итог отметки «применено» в команде изменения (спец. §9, шаг 5).
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub enum MarkApplied {
+    Applied {
+        as_proposed: bool,
+    },
+    /// Предложения нет, оно закрыто или относится к другому изменению: операция прошла, отметки нет.
+    Skipped(&'static str),
+}
+
 /// Текст замечания: без управляющих символов (кроме перевода строки), не длиннее `max` символов.
 pub fn sanitize(text: &str, max: usize) -> String {
     let clean: String = text
@@ -452,6 +504,203 @@ impl HealthStore {
         })
     }
 
+    // ---- Предложения LLM ----
+
+    fn proposal_not_found() -> crate::error::AppError {
+        app_err!(
+            ErrorCode::NotFound,
+            "errors.health.proposalNotFound",
+            "The proposal was deleted or has expired"
+        )
+    }
+
+    /// `propose_change` из MCP: повтор того же автора по тому же виду и объекту среди открытых
+    /// обновляет запись; открытых — не больше `MAX_OPEN_PROPOSALS`.
+    pub fn propose(
+        &self,
+        connection_id: &str,
+        new: NewProposal,
+        now_ms: i64,
+    ) -> AppResult<Proposed> {
+        let title = sanitize(&new.title, MAX_TITLE_CHARS);
+        let rationale = sanitize(&new.rationale, MAX_BODY_CHARS);
+        let change = new.change.trimmed();
+        let target = change.target();
+        self.update(connection_id, now_ms, |f| {
+            if let Some(p) = f.proposals.iter_mut().find(|p| {
+                p.status == ProposalStatus::Open
+                    && new.author.owns_proposal(p)
+                    && p.change.kind_name() == change.kind_name()
+                    && p.target == target
+            }) {
+                p.severity = new.severity;
+                p.title = title;
+                p.rationale = rationale;
+                p.change = change;
+                p.related_rule_id = new.related_rule_id;
+                p.related_finding_key = new.related_finding_key;
+                p.updated_at_ms = now_ms;
+                return Proposed::Updated(p.id.clone());
+            }
+            let open = f
+                .proposals
+                .iter()
+                .filter(|p| p.status == ProposalStatus::Open)
+                .count();
+            if open >= MAX_OPEN_PROPOSALS {
+                return Proposed::LimitReached;
+            }
+            let id = uuid::Uuid::new_v4().to_string();
+            let session_id = new
+                .author
+                .client
+                .is_none()
+                .then(|| new.author.session_id.clone());
+            f.proposals.push(Proposal {
+                id: id.clone(),
+                severity: new.severity,
+                title,
+                rationale,
+                target,
+                change,
+                related_rule_id: new.related_rule_id,
+                related_finding_key: new.related_finding_key,
+                client: new.author.client,
+                session_id,
+                created_at_ms: now_ms,
+                updated_at_ms: now_ms,
+                status: ProposalStatus::Open,
+                closed_at_ms: None,
+                applied_as_proposed: None,
+                applied_change: None,
+                reject_reason: None,
+            });
+            Proposed::Created(id)
+        })
+    }
+
+    /// Человек: «Отклонить» (из открытого, с необязательной причиной) или «Открыть снова»
+    /// (из отклонённого). Применённое и отозванное не меняются.
+    pub fn set_proposal_status(
+        &self,
+        connection_id: &str,
+        proposal_id: &str,
+        status: ProposalStatus,
+        reason: Option<String>,
+        now_ms: i64,
+    ) -> AppResult<()> {
+        let reason = reason
+            .map(|r| sanitize(&r, MAX_REJECT_REASON_CHARS))
+            .filter(|r| !r.is_empty());
+        self.update(connection_id, now_ms, |f| {
+            let Some(p) = f.proposals.iter_mut().find(|p| p.id == proposal_id) else {
+                return Err(Self::proposal_not_found());
+            };
+            match (p.status, status) {
+                (ProposalStatus::Open, ProposalStatus::Rejected) => {
+                    p.status = ProposalStatus::Rejected;
+                    p.closed_at_ms = Some(now_ms);
+                    p.reject_reason = reason;
+                }
+                (ProposalStatus::Rejected, ProposalStatus::Open) => {
+                    p.status = ProposalStatus::Open;
+                    p.closed_at_ms = None;
+                    p.reject_reason = None;
+                    p.updated_at_ms = now_ms;
+                }
+                (from, to) => {
+                    return Err(app_err!(
+                        ErrorCode::InvalidInput,
+                        "errors.health.proposalClosed",
+                        "The proposal is {{status}} and cannot be changed to {{to}}",
+                        status = from.as_str(),
+                        to = to.as_str()
+                    ));
+                }
+            }
+            Ok(())
+        })?
+    }
+
+    /// Команда изменения после успеха в кластере: открытое предложение того же вида и объекта
+    /// становится `applied`; иначе отметка пропускается (операция уже прошла) и файл не пишется.
+    pub fn mark_applied(
+        &self,
+        connection_id: &str,
+        proposal_id: &str,
+        applied: &Change,
+        now_ms: i64,
+    ) -> AppResult<MarkApplied> {
+        self.update_if(connection_id, now_ms, |f| {
+            let Some(p) = f.proposals.iter_mut().find(|p| p.id == proposal_id) else {
+                return (MarkApplied::Skipped("proposal not found"), false);
+            };
+            if p.status != ProposalStatus::Open {
+                return (MarkApplied::Skipped("proposal is not open"), false);
+            }
+            if p.change.kind_name() != applied.kind_name() || p.target != applied.target() {
+                return (
+                    MarkApplied::Skipped("proposal is about another change"),
+                    false,
+                );
+            }
+            let as_proposed = applied_as_proposed(&p.change, applied);
+            p.status = ProposalStatus::Applied;
+            p.closed_at_ms = Some(now_ms);
+            p.applied_as_proposed = Some(as_proposed);
+            // С правками — что применено фактически; как предложено — оно совпадает с `change`.
+            p.applied_change = (!as_proposed).then(|| applied.clone());
+            (MarkApplied::Applied { as_proposed }, true)
+        })
+    }
+
+    /// `withdraw_proposal` из MCP: только своё и только открытое.
+    pub fn withdraw_own(
+        &self,
+        connection_id: &str,
+        proposal_id: &str,
+        author: &Author,
+        now_ms: i64,
+    ) -> AppResult<Withdrawn> {
+        self.update(connection_id, now_ms, |f| {
+            let Some(p) = f.proposals.iter_mut().find(|p| p.id == proposal_id) else {
+                return Withdrawn::NotFound;
+            };
+            if !author.owns_proposal(p) {
+                return Withdrawn::NotOwner;
+            }
+            if p.status != ProposalStatus::Open {
+                return Withdrawn::Closed(p.status);
+            }
+            p.status = ProposalStatus::Withdrawn;
+            p.closed_at_ms = Some(now_ms);
+            Withdrawn::Withdrawn
+        })
+    }
+
+    pub fn delete_proposal(
+        &self,
+        connection_id: &str,
+        proposal_id: &str,
+        now_ms: i64,
+    ) -> AppResult<()> {
+        let found = self.update(connection_id, now_ms, |f| {
+            let n = f.proposals.len();
+            f.proposals.retain(|p| p.id != proposal_id);
+            f.proposals.len() < n
+        })?;
+        found.then_some(()).ok_or_else(Self::proposal_not_found)
+    }
+
+    /// «Удалить все предложения LLM» у подключения.
+    pub fn delete_all_proposals(&self, connection_id: &str, now_ms: i64) -> AppResult<usize> {
+        self.update(connection_id, now_ms, |f| {
+            let n = f.proposals.len();
+            f.proposals.clear();
+            n
+        })
+    }
+
     /// Удаление вместе с подключением.
     pub fn delete_connection(&self, connection_id: &str) -> AppResult<()> {
         let _g = self.lock.lock_safe();
@@ -502,6 +751,345 @@ mod tests {
         }
     }
 
+    fn scale(workload: &str, replicas: i32) -> Change {
+        Change::Scale {
+            namespace: "team-a".into(),
+            workload: crate::k8s::pods::WorkloadRef {
+                kind: crate::k8s::workloads::WorkloadKind::Deployment,
+                name: workload.into(),
+            },
+            replicas,
+        }
+    }
+
+    fn proposal(change: Change, who: Author) -> NewProposal {
+        NewProposal {
+            severity: Severity::Warning,
+            title: "Scale web out".into(),
+            rationale: "single replica\nis risky".into(),
+            change,
+            related_rule_id: Some("workload.singleReplica".into()),
+            related_finding_key: None,
+            author: who,
+        }
+    }
+
+    fn proposed_id(p: Proposed) -> String {
+        match p {
+            Proposed::Created(id) => id,
+            other => panic!("{other:?}"),
+        }
+    }
+
+    #[test]
+    fn proposal_of_same_author_kind_and_object_is_updated() {
+        let (d, s) = store();
+        let claude = || author(Some("claude-ai"), "s1");
+        let mut first = scale("web", 2);
+        if let Change::Scale { workload, .. } = &mut first {
+            workload.name = " web ".into();
+        }
+        let id = proposed_id(s.propose(ID, proposal(first, claude()), T0).unwrap());
+        let mut again = proposal(scale("web", 3), author(Some("claude-ai"), "s9"));
+        again.title = "  New title ".into();
+        again.severity = Severity::Critical;
+        assert_eq!(
+            s.propose(ID, again, T0 + 5).unwrap(),
+            Proposed::Updated(id.clone())
+        );
+        // Другой вид по тому же workload'у и другой автор — новые записи.
+        let restart = Change::RolloutRestart {
+            namespace: "team-a".into(),
+            workload: crate::k8s::pods::WorkloadRef {
+                kind: crate::k8s::workloads::WorkloadKind::Deployment,
+                name: "web".into(),
+            },
+        };
+        proposed_id(s.propose(ID, proposal(restart, claude()), T0).unwrap());
+        proposed_id(
+            s.propose(
+                ID,
+                proposal(scale("web", 2), author(Some("cursor"), "s2")),
+                T0,
+            )
+            .unwrap(),
+        );
+        let f = s.load(ID, T0 + 5).unwrap();
+        assert_eq!(f.proposals.len(), 3);
+        let p = f.proposals.iter().find(|p| p.id == id).unwrap();
+        assert_eq!(p.title, "New title");
+        assert_eq!(p.severity, Severity::Critical);
+        assert_eq!(p.updated_at_ms, T0 + 5);
+        assert_eq!(
+            p.target,
+            Target::workload(
+                "team-a",
+                crate::k8s::workloads::WorkloadKind::Deployment,
+                "web"
+            )
+        );
+        assert_eq!(p.change, scale("web", 3));
+        assert_eq!(p.status, ProposalStatus::Open);
+        assert_eq!(p.client.as_deref(), Some("claude-ai"));
+        assert_eq!(p.session_id, None);
+        let raw: serde_json::Value =
+            serde_json::from_slice(&std::fs::read(d.path().join("health/c1.json")).unwrap())
+                .unwrap();
+        assert_eq!(raw["proposals"][0]["change"]["kind"], "scale");
+        assert_eq!(raw["proposals"][0]["status"], "open");
+        assert!(raw["proposals"][0].get("appliedAsProposed").is_none());
+    }
+
+    #[test]
+    fn open_proposals_are_limited_to_fifty() {
+        let (d, s) = store();
+        let id = proposed_id(
+            s.propose(ID, proposal(scale("w0", 2), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        let mut f = s.load(ID, T0).unwrap();
+        let first = f.proposals[0].clone();
+        f.proposals = (0..MAX_OPEN_PROPOSALS)
+            .map(|i| {
+                let change = scale(&format!("w{i}"), 2);
+                Proposal {
+                    id: if i == 0 { id.clone() } else { format!("p{i}") },
+                    target: change.target(),
+                    change,
+                    ..first.clone()
+                }
+            })
+            .collect();
+        write_json(&d.path().join("health/c1.json"), &f).unwrap();
+        assert_eq!(
+            s.propose(
+                ID,
+                proposal(scale("one-more", 2), author(Some("c"), "s")),
+                T0
+            )
+            .unwrap(),
+            Proposed::LimitReached
+        );
+        // Обновление существующего лимит не трогает.
+        assert_eq!(
+            s.propose(ID, proposal(scale("w5", 3), author(Some("c"), "s")), T0)
+                .unwrap(),
+            Proposed::Updated("p5".into())
+        );
+        // Закрытые не считаются.
+        s.set_proposal_status(ID, &id, ProposalStatus::Rejected, None, T0)
+            .unwrap();
+        proposed_id(
+            s.propose(
+                ID,
+                proposal(scale("one-more", 2), author(Some("c"), "s")),
+                T0,
+            )
+            .unwrap(),
+        );
+    }
+
+    #[test]
+    fn proposals_expire_after_seven_days_whatever_the_status() {
+        let (_d, s) = store();
+        let id = proposed_id(
+            s.propose(ID, proposal(scale("web", 2), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        s.mark_applied(ID, &id, &scale("web", 2), T0 + 1).unwrap();
+        assert_eq!(
+            s.load(ID, T0 + PROPOSAL_TTL_MS - 1)
+                .unwrap()
+                .proposals
+                .len(),
+            1
+        );
+        assert!(
+            s.load(ID, T0 + PROPOSAL_TTL_MS)
+                .unwrap()
+                .proposals
+                .is_empty()
+        );
+    }
+
+    #[test]
+    fn status_transitions_reject_reopen_and_refuse_closed() {
+        let (_d, s) = store();
+        let id = proposed_id(
+            s.propose(ID, proposal(scale("web", 2), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        s.set_proposal_status(
+            ID,
+            &id,
+            ProposalStatus::Rejected,
+            Some("  too risky\u{7} ".into()),
+            T0 + 1,
+        )
+        .unwrap();
+        let p = &s.load(ID, T0).unwrap().proposals[0];
+        assert_eq!(
+            (p.status, p.closed_at_ms),
+            (ProposalStatus::Rejected, Some(T0 + 1))
+        );
+        assert_eq!(p.reject_reason.as_deref(), Some("too risky"));
+        let e = s
+            .set_proposal_status(ID, &id, ProposalStatus::Rejected, None, T0 + 2)
+            .unwrap_err();
+        assert_eq!(e.key, "errors.health.proposalClosed");
+        s.set_proposal_status(ID, &id, ProposalStatus::Open, None, T0 + 3)
+            .unwrap();
+        let p = &s.load(ID, T0).unwrap().proposals[0];
+        assert_eq!(
+            (p.status, p.closed_at_ms, p.reject_reason.clone()),
+            (ProposalStatus::Open, None, None)
+        );
+        assert_eq!(p.updated_at_ms, T0 + 3);
+        // Применённое снова не открыть и не отклонить.
+        let id2 = proposed_id(
+            s.propose(ID, proposal(scale("api", 2), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        s.mark_applied(ID, &id2, &scale("api", 2), T0).unwrap();
+        for to in [ProposalStatus::Open, ProposalStatus::Rejected] {
+            let e = s.set_proposal_status(ID, &id2, to, None, T0).unwrap_err();
+            assert_eq!(
+                (e.code, e.key.as_str()),
+                (ErrorCode::InvalidInput, "errors.health.proposalClosed")
+            );
+            assert_eq!(e.params["status"], "applied");
+        }
+        let e = s
+            .set_proposal_status(ID, "nope", ProposalStatus::Rejected, None, T0)
+            .unwrap_err();
+        assert_eq!(e.key, "errors.health.proposalNotFound");
+    }
+
+    #[test]
+    fn mark_applied_records_edits_and_skips_foreign_or_closed() {
+        let (d, s) = store();
+        assert_eq!(
+            s.mark_applied(ID, "nope", &scale("web", 2), T0).unwrap(),
+            MarkApplied::Skipped("proposal not found")
+        );
+        assert!(
+            !d.path().join("health").join("c1.json").exists(),
+            "пропуск файл не пишет"
+        );
+        let id = proposed_id(
+            s.propose(ID, proposal(scale("web", 2), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        assert_eq!(
+            s.mark_applied(ID, &id, &scale("other", 2), T0).unwrap(),
+            MarkApplied::Skipped("proposal is about another change")
+        );
+        assert_eq!(
+            s.mark_applied(ID, &id, &scale("web", 4), T0 + 1).unwrap(),
+            MarkApplied::Applied { as_proposed: false }
+        );
+        let p = &s.load(ID, T0).unwrap().proposals[0];
+        assert_eq!(
+            (p.status, p.closed_at_ms, p.applied_as_proposed),
+            (ProposalStatus::Applied, Some(T0 + 1), Some(false))
+        );
+        assert_eq!(p.applied_change, Some(scale("web", 4)));
+        assert_eq!(p.change, scale("web", 2));
+        assert_eq!(
+            s.mark_applied(ID, &id, &scale("web", 2), T0 + 2).unwrap(),
+            MarkApplied::Skipped("proposal is not open")
+        );
+        let id2 = proposed_id(
+            s.propose(ID, proposal(scale("api", 2), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        assert_eq!(
+            s.mark_applied(ID, &id2, &scale("api", 2), T0).unwrap(),
+            MarkApplied::Applied { as_proposed: true }
+        );
+        let f = s.load(ID, T0).unwrap();
+        assert_eq!(
+            f.proposals
+                .iter()
+                .find(|p| p.id == id2)
+                .unwrap()
+                .applied_change,
+            None
+        );
+    }
+
+    #[test]
+    fn llm_withdraws_only_its_own_open_proposals() {
+        let (_d, s) = store();
+        let named = proposed_id(
+            s.propose(
+                ID,
+                proposal(scale("a", 2), author(Some("claude-ai"), "s1")),
+                T0,
+            )
+            .unwrap(),
+        );
+        let unnamed = proposed_id(
+            s.propose(ID, proposal(scale("b", 2), author(None, "s2")), T0)
+                .unwrap(),
+        );
+        assert_eq!(
+            s.withdraw_own(ID, &named, &author(Some("cursor"), "s1"), T0)
+                .unwrap(),
+            Withdrawn::NotOwner
+        );
+        assert_eq!(
+            s.withdraw_own(ID, &unnamed, &author(None, "s3"), T0)
+                .unwrap(),
+            Withdrawn::NotOwner
+        );
+        assert_eq!(
+            s.withdraw_own(ID, &unnamed, &author(None, "s2"), T0)
+                .unwrap(),
+            Withdrawn::Withdrawn
+        );
+        assert_eq!(
+            s.withdraw_own(ID, &unnamed, &author(None, "s2"), T0)
+                .unwrap(),
+            Withdrawn::Closed(ProposalStatus::Withdrawn)
+        );
+        assert_eq!(
+            s.withdraw_own(ID, "nope", &author(Some("claude-ai"), "s1"), T0)
+                .unwrap(),
+            Withdrawn::NotFound
+        );
+        // Отозванное LLM человек снова не откроет.
+        let e = s
+            .set_proposal_status(ID, &unnamed, ProposalStatus::Open, None, T0)
+            .unwrap_err();
+        assert_eq!(e.params["status"], "withdrawn");
+    }
+
+    #[test]
+    fn proposals_are_deleted_one_by_one_and_all_at_once() {
+        let (_d, s) = store();
+        let id = proposed_id(
+            s.propose(ID, proposal(scale("a", 2), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        proposed_id(
+            s.propose(ID, proposal(scale("b", 2), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        s.delete_proposal(ID, &id, T0).unwrap();
+        let e = s.delete_proposal(ID, &id, T0).unwrap_err();
+        assert_eq!(e.key, "errors.health.proposalNotFound");
+        assert_eq!(s.delete_all_proposals(ID, T0).unwrap(), 1);
+        assert!(s.load(ID, T0).unwrap().proposals.is_empty());
+        // Замечания при этом не трогаются.
+        created(
+            s.annotate(ID, note("n", author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        assert_eq!(s.delete_all_proposals(ID, T0).unwrap(), 0);
+        assert_eq!(s.load(ID, T0).unwrap().annotations.len(), 1);
+    }
+
     #[test]
     fn empty_state_without_file() {
         let (_d, s) = store();
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index 86d2683..d4cf38d 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -168,6 +168,9 @@ pub fn run() {
             commands::health::set_annotation_status,
             commands::health::delete_annotation,
             commands::health::delete_all_annotations,
+            commands::health::set_proposal_status,
+            commands::health::delete_proposal,
+            commands::health::delete_all_proposals,
             commands::mcp::get_mcp_status,
             commands::mcp::set_mcp_enabled,
             commands::mcp::answer_mcp_approval,
````

<!-- block: locales t10-locales step=10.2 -->
````json
{
  "en": {
    "errors": {
      "health": {
        "proposalNotFound": "The proposal was deleted or has expired",
        "proposalClosed": "The proposal is {{status}} and cannot be changed to {{to}}"
      }
    }
  },
  "ru": {
    "errors": {
      "health": {
        "proposalNotFound": "Предложение удалено или истёк его срок",
        "proposalClosed": "Предложение в статусе {{status}}, перевести его в {{to}} нельзя"
      }
    }
  }
}
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 663 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm test`  
Expected: PASS — `Test Files 63 passed`, `Tests 479 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 195`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 4: Commit.**

```bash
git add \
  PORTED.md \
  src-tauri/src/commands/health.rs \
  src-tauri/src/health/mod.rs \
  src-tauri/src/health/proposals.rs \
  src-tauri/src/health/store.rs \
  src-tauri/src/lib.rs \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "feat(health): LLM proposals in the health file — open, rejected, reopened, withdrawn and applied with or without edits, seven-day expiry, fifty open per connection; commands of the Findings tab" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 11: Изменяющие операции форм: предпросмотр с «было → станет» и server-side dry-run, применение после стражей с версией предпросмотра и повтором при гонке со статусом, CONFLICT и ADMISSION_DENIED, журнал и отметка предложения; права patch и delete

**Files:**
- Create: `src-tauri/src/commands/changes.rs`, `src-tauri/src/ops/mod.rs`
- Modify: `src-tauri/src/change/mod.rs`, `src-tauri/src/commands/mod.rs`, `src-tauri/src/error.rs`, `src-tauri/src/k8s/access.rs`, `src-tauri/src/k8s/pool.rs`, `src-tauri/src/lib.rs`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задачи 9–10: `Change`, `read_facts`, `check`, `diff`, `Facts::{version, resource_version}`, `guard`, `mark_applied`; O1b: `k8s::access`, `k8s::pool::Connected`, `k8s::errors`.
- Produces: `ops::{FIELD_MANAGER = "glafira-kubernetes", Verb, Write { verb, path, body, dry_run }, ClusterWriter, KubeWriter, admission, workload_path, writes(change, facts, now), DryRun (tag status: passed | rejected { error } | skipped), Preview { change, version, diff, warnings, dry_run, confirm_pod_name }, preview, apply(r, w, change, expected, confirm_pod, now), now_rfc3339}`; `commands::changes::{preview_change(+_impl), apply_change(+_impl)(connection_id, change, expected_version, confirm, confirm_pod, proposal_id)}`; `ErrorCode::AdmissionDenied`; `Connected::writer()`; `k8s::access::Verbs { get, list, watch, patch, delete, create }`, подресурсы `deployments/scale.apps`, `deploymentconfigs/instantiate.apps.openshift.io`…

Запросы записи в кластер есть только в `ops` (сторожок задачи 1 не пускает туда `mcp/`). Ресурсы — strategic merge patch по имени контейнера (у CronJob — шаблон Job), у DeploymentConfig — JSON patch по индексу с `resourceVersion` прочитанного объекта; реплики — подресурс `scale`; перезапуск — аннотация `kubectl.kubernetes.io/restartedAt`, у DeploymentConfig — `instantiate` (без dry-run); границы HPA — merge patch; удаление пода — с предусловием `uid`. Значения абсолютные, как у kubectl. Применение перечитывает объект, сверяет версию предпросмотра (другая — `errors.ops.changed`, `CONFLICT`) и при ответе 409 (контроллер обновил статус между чтением и записью) повторяет до 3 раз — так же dry-run предпросмотра. У DELETE dry-run передаётся в теле `DeleteOptions`: строку запроса API-сервер у DELETE с телом не читает, и `?dryRun=All` удалил бы под по-настоящему (дефект нашёл стенд kind, задача 13). Отказ admission (403, не разобранный как отказ RBAC) — `ADMISSION_DENIED` с текстом сервера. Профиль «только чтение» и предпросмотр не готовит; журнал — без значений.

- [ ] **Step 1: Тесты.**

<!-- block: file t11-src-tauri-src-commands-changes-rs-tests step=11.1 path=src-tauri/src/commands/changes.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::error::ErrorCode;
    use crate::storage::profile::{Environment, sample_profile};

    fn state(dir: &std::path::Path, env: Environment, read_only: bool) -> AppState {
        let st = AppState::new(dir, Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        let mut p = sample_profile("c1");
        p.name = "prod-1".into();
        p.environment = env;
        p.read_only = read_only;
        st.connections.upsert(p).unwrap();
        let connected = crate::k8s::pool::test_connected(|s| match s.path() {
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers/web" => (
                200,
                json!({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                    "metadata": {"name": "web", "resourceVersion": "5", "generation": 4},
                    "spec": {"minReplicas": 2, "maxReplicas": 6,
                        "scaleTargetRef": {"kind": "Deployment", "name": "web"}}}),
            ),
            _ => crate::k8s::testing::status(404, "NotFound", "no"),
        });
        assert!(
            st.pool
                .insert_if_current("c1", st.pool.generation("c1"), connected)
        );
        st
    }

    fn hpa() -> Change {
        Change::SetHpaBounds {
            namespace: "team-a".into(),
            hpa: "web".into(),
            min_replicas: Some(1),
            max_replicas: 4,
        }
    }

    #[tokio::test]
    async fn guards_come_before_the_cluster() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path(), Environment::Prod, true);
        let e = apply_change_impl(&st, "c1", hpa(), None, Some("prod-1".into()), None, None)
            .await
            .unwrap_err();
        assert_eq!(
            e.code,
            ErrorCode::ReadOnly,
            "read-only — никакое подтверждение не помогает"
        );
        let e = preview_change_impl(&st, "c1", hpa()).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::ReadOnly);
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path(), Environment::Prod, false);
        let e = apply_change_impl(&st, "c1", hpa(), None, Some("prod".into()), None, None)
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::ConfirmationRequired);
        // Предпросмотр prod не требует имени: оно — для применения.
        let p = preview_change_impl(&st, "c1", hpa()).await.unwrap();
        assert_eq!(p.version.as_deref(), Some("4"));
        assert_eq!(p.diff.len(), 2);
    }
}
````

- [ ] **Step 2: Реализация.**

<!-- block: file t11-src-tauri-src-commands-changes-rs-impl step=11.2 path=src-tauri/src/commands/changes.rs part=impl -->
````rust
//! Изменения кластера из форм (спец. §9, §4.2): предпросмотр с dry-run и применение после
//! стражей. MCP-шлюз этих команд не вызывает (сторожок `mcp::guard`).

use tauri::State;

use crate::change::Change;
use crate::error::AppResult;
use crate::ops::Preview;
use crate::state::AppState;

/// «Было → станет», предупреждения и server-side dry-run. Профиль «только чтение» изменений
/// не готовит: формы на нём не открываются (`READ_ONLY`).
#[tauri::command]
pub async fn preview_change(
    state: State<'_, AppState>,
    connection_id: String,
    change: Change,
) -> AppResult<Preview> {
    preview_change_impl(state.inner(), &connection_id, change).await
}

pub async fn preview_change_impl(
    st: &AppState,
    connection_id: &str,
    change: Change,
) -> AppResult<Preview> {
    let profile = st.connections.get(connection_id)?;
    if profile.read_only {
        crate::guard::ensure_mutation_allowed(&profile, None)?;
    }
    let ns = change.namespace().to_string();
    // Future предпросмотра велика для стека потока в debug-сборке — в куче.
    Box::pin(st.read_in(connection_id, &ns, |c| async move {
        Box::pin(crate::ops::preview(
            &c.reader(),
            &c.writer(),
            change,
            &crate::ops::now_rfc3339(),
        ))
        .await
    }))
    .await
}

/// Применение: стражи профиля (только чтение, имя подключения для prod), у пода без
/// контроллера — его имя (`confirm_pod`), сверка с версией предпросмотра, журнал
/// приложения (подключение, вид, объект, исход — без значений), отметка предложения LLM.
#[tauri::command]
#[allow(clippy::too_many_arguments)]
pub async fn apply_change(
    state: State<'_, AppState>,
    connection_id: String,
    change: Change,
    expected_version: Option<String>,
    confirm: Option<String>,
    confirm_pod: Option<String>,
    proposal_id: Option<String>,
) -> AppResult<Change> {
    apply_change_impl(
        state.inner(),
        &connection_id,
        change,
        expected_version,
        confirm,
        confirm_pod,
        proposal_id,
    )
    .await
}

pub async fn apply_change_impl(
    st: &AppState,
    connection_id: &str,
    change: Change,
    expected_version: Option<String>,
    confirm: Option<String>,
    confirm_pod: Option<String>,
    proposal_id: Option<String>,
) -> AppResult<Change> {
    let profile = st.connections.get(connection_id)?;
    crate::guard::ensure_mutation_allowed(&profile, confirm.as_deref())?;
    let (kind, target) = (change.kind_name(), change.target());
    let ns = change.namespace().to_string();
    let result = Box::pin(st.read_in(connection_id, &ns, |c| async move {
        Box::pin(crate::ops::apply(
            &c.reader(),
            &c.writer(),
            change,
            expected_version.as_deref(),
            confirm_pod.as_deref(),
            &crate::ops::now_rfc3339(),
        ))
        .await
    }))
    .await;
    let object = format!(
        "{}/{}",
        target.namespace.as_deref().unwrap_or_default(),
        target.name.as_deref().unwrap_or_default()
    );
    match &result {
        Ok(_) => {
            tracing::info!("change applied: connection={connection_id} kind={kind} object={object}")
        }
        Err(e) => tracing::warn!(
            "change failed: connection={connection_id} kind={kind} object={object} code={:?}",
            e.code
        ),
    }
    if let Ok(applied) = &result {
        super::health::mark_applied(st, connection_id, proposal_id, applied.clone()).await;
    }
    result
}
````

<!-- block: file t11-src-tauri-src-ops-mod-rs step=11.2 path=src-tauri/src/ops/mod.rs -->
````rust
//! Изменяющие операции (спец. §9): запросы записи в кластер есть только здесь. Порядок — как в
//! спеке: предпросмотр читает объект, строит «было → станет» и выполняет server-side dry-run;
//! применение после стражей (`guard`) перечитывает объект, сверяет версию предпросмотра
//! (`Facts::version`: изменившийся spec даёт `CONFLICT`) и пишет с
//! `fieldManager=glafira-kubernetes`. MCP-шлюз сюда не
//! доходит (сторожок `mcp::guard`): модель только предлагает изменения (`health::proposals`).

use std::time::Duration;

use futures::future::BoxFuture;
use k8s_openapi::api::core::v1::ResourceRequirements;
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::change::facts::{Facts, WorkloadFacts};
use crate::change::facts::{FieldChange, RESTARTED_AT, Warning, check, diff, read_facts};
use crate::change::{Change, ContainerResources, validate_form};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::read::ReadClient;
use crate::k8s::workloads::WorkloadKind;

/// Кем изменено поле (`managedFields`).
pub const FIELD_MANAGER: &str = "glafira-kubernetes";

/// Вид запроса записи.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verb {
    /// Strategic merge patch (встроенные виды: контейнеры сливаются по имени).
    Strategic,
    /// JSON merge patch (подресурс `scale`, HPA).
    Merge,
    /// JSON patch (DeploymentConfig: контейнер — по индексу).
    Json,
    /// `instantiate` DeploymentConfig.
    Post,
    Delete,
}

impl Verb {
    fn method(self) -> http::Method {
        match self {
            Verb::Strategic | Verb::Merge | Verb::Json => http::Method::PATCH,
            Verb::Post => http::Method::POST,
            Verb::Delete => http::Method::DELETE,
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Verb::Strategic => "application/strategic-merge-patch+json",
            Verb::Merge => "application/merge-patch+json",
            Verb::Json => "application/json-patch+json",
            Verb::Post | Verb::Delete => "application/json",
        }
    }
}

/// Запрос записи: что уйдёт в кластер.
#[derive(Debug, Clone, PartialEq)]
pub struct Write {
    pub verb: Verb,
    /// Путь API без строки запроса.
    pub path: String,
    pub body: Value,
    /// API принимает `dryRun=All` (у `instantiate` DeploymentConfig — нет).
    pub dry_run: bool,
}

/// Запись в кластер: в тестах подменяется.
pub trait ClusterWriter: Send + Sync {
    /// `dry_run` — server-side dry-run: проверки и admission без изменения кластера.
    fn send<'a>(&'a self, w: &'a Write, dry_run: bool) -> BoxFuture<'a, AppResult<()>>;
}

/// Запись через клиент подключения (`Connected::writer`): запросы — только `Write`.
pub struct KubeWriter {
    client: kube::Client,
    timeout: Duration,
}

impl KubeWriter {
    pub fn new(client: kube::Client, timeout: Duration) -> Self {
        Self { client, timeout }
    }
}

/// Отказ admission (webhook, квота, `LimitRange`) — `ADMISSION_DENIED`, а не «нет прав»: 403 с
/// текстом, который не разобрался как отказ RBAC (`k8s::errors::forbidden`).
fn admission(e: AppError) -> AppError {
    if e.key != "errors.k8s.forbidden" {
        return e;
    }
    let message = e.params.get("message").cloned().unwrap_or_default();
    crate::app_err!(
        ErrorCode::AdmissionDenied,
        "errors.ops.admissionDenied",
        "The cluster rejected the change: {{message}}",
        message = message
    )
}

impl ClusterWriter for KubeWriter {
    fn send<'a>(&'a self, w: &'a Write, dry_run: bool) -> BoxFuture<'a, AppResult<()>> {
        Box::pin(async move {
            // У DELETE с телом API-сервер берёт параметры только из `DeleteOptions`: строку
            // запроса он не читает, и `dryRun` в ней удалил бы объект по-настоящему.
            let mut body = w.body.clone();
            // Сериализатор не `Send`: строка запроса собирается до первого `await`.
            let query = {
                let mut q = url::form_urlencoded::Serializer::new(String::new());
                if w.verb == Verb::Delete {
                    if dry_run {
                        body["dryRun"] = json!(["All"]);
                    }
                } else {
                    q.append_pair("fieldManager", FIELD_MANAGER);
                    if dry_run {
                        q.append_pair("dryRun", "All");
                    }
                }
                q.finish()
            };
            let uri = match query.is_empty() {
                true => w.path.clone(),
                false => format!("{}?{query}", w.path),
            };
            let request = http::Request::builder()
                .method(w.verb.method())
                .uri(uri)
                .header(http::header::CONTENT_TYPE, w.verb.content_type())
                .body(
                    serde_json::to_vec(&body)
                        .map_err(|e| AppError::internal_with(e.to_string()))?,
                )
                .map_err(|e| AppError::internal_with(e.to_string()))?;
            crate::k8s::cluster::timed(self.timeout, self.client.request_text(request))
                .await
                .map(|_| ())
                .map_err(admission)
        })
    }
}

/// Путь workload'а в API.
fn workload_path(namespace: &str, kind: WorkloadKind, name: &str) -> String {
    let (prefix, plural) = match kind {
        WorkloadKind::Deployment => ("/apis/apps/v1", "deployments"),
        WorkloadKind::StatefulSet => ("/apis/apps/v1", "statefulsets"),
        WorkloadKind::DaemonSet => ("/apis/apps/v1", "daemonsets"),
        WorkloadKind::Job => ("/apis/batch/v1", "jobs"),
        WorkloadKind::CronJob => ("/apis/batch/v1", "cronjobs"),
        WorkloadKind::DeploymentConfig => ("/apis/apps.openshift.io/v1", "deploymentconfigs"),
    };
    format!("{prefix}/namespaces/{namespace}/{plural}/{name}")
}

/// `requests` или `limits` изменения для strategic merge patch: значение или `null` (убрать).
fn smp_values(v: &crate::change::ResourceValues) -> Value {
    let mut m = Map::new();
    for (k, x) in [("cpu", &v.cpu), ("memory", &v.memory)] {
        if let Some(x) = x {
            m.insert(k.into(), x.clone().map_or(Value::Null, Value::String));
        }
    }
    Value::Object(m)
}

/// Ресурсы контейнера после изменения целиком: прочие ресурсы (`ephemeral-storage`, GPU)
/// остаются — для JSON patch DeploymentConfig.
fn merged(current: Option<&ResourceRequirements>, c: &ContainerResources) -> Value {
    let mut out = serde_json::to_value(current.cloned().unwrap_or_default()).unwrap_or(json!({}));
    for (part, values) in [("requests", &c.requests), ("limits", &c.limits)] {
        let map = out
            .as_object_mut()
            .map(|o| o.entry(part).or_insert_with(|| json!({})));
        let Some(Value::Object(map)) = map else {
            continue;
        };
        for (k, x) in [("cpu", &values.cpu), ("memory", &values.memory)] {
            match x {
                None => {}
                Some(None) => {
                    map.remove(k);
                }
                Some(Some(q)) => {
                    map.insert(k.into(), Value::String(q.clone()));
                }
            }
        }
    }
    if let Value::Object(o) = &mut out {
        o.retain(|_, v| !v.as_object().is_some_and(Map::is_empty) && !v.is_null());
    }
    out
}

/// Ресурсы контейнеров: strategic merge patch по имени контейнера (у CronJob — шаблон Job),
/// у DeploymentConfig — JSON patch по индексу с `resourceVersion` в объекте.
fn set_resources(
    w: &WorkloadFacts,
    namespace: &str,
    kind: WorkloadKind,
    name: &str,
    containers: &[ContainerResources],
    rv: Option<&str>,
) -> AppResult<Write> {
    let path = workload_path(namespace, kind, name);
    let pod = w.spec.template.as_ref().and_then(|t| t.spec.as_ref());
    if kind == WorkloadKind::DeploymentConfig {
        let mut ops =
            vec![json!({"op": "replace", "path": "/metadata/resourceVersion", "value": rv})];
        for c in containers {
            let (list, idx, current) = pod
                .and_then(|p| {
                    let find = |list: &[k8s_openapi::api::core::v1::Container],
                                field: &'static str| {
                        list.iter()
                            .position(|x| x.name == c.name)
                            .map(|i| (field, i, list[i].resources.clone()))
                    };
                    find(&p.containers, "containers").or_else(|| {
                        p.init_containers
                            .as_deref()
                            .and_then(|l| find(l, "initContainers"))
                    })
                })
                .ok_or_else(|| AppError::internal_with("container is not in the template"))?;
            ops.push(json!({
                "op": "add",
                "path": format!("/spec/template/spec/{list}/{idx}/resources"),
                "value": merged(current.as_ref(), c),
            }));
        }
        return Ok(Write {
            verb: Verb::Json,
            path,
            body: Value::Array(ops),
            dry_run: true,
        });
    }
    if kind == WorkloadKind::Job {
        // `validate_form` такое изменение не пропускает: шаблон Job неизменяем.
        return Err(AppError::internal_with(
            "the pod template of a Job is immutable",
        ));
    }
    let init: std::collections::BTreeSet<&str> = pod
        .and_then(|p| p.init_containers.as_ref())
        .into_iter()
        .flatten()
        .map(|c| c.name.as_str())
        .collect();
    let item = |c: &ContainerResources| {
        let mut res = Map::new();
        for (part, v) in [("requests", &c.requests), ("limits", &c.limits)] {
            let values = smp_values(v);
            if values.as_object().is_some_and(|o| !o.is_empty()) {
                res.insert(part.into(), values);
            }
        }
        json!({"name": c.name, "resources": res})
    };
    let main: Vec<Value> = containers
        .iter()
        .filter(|c| !init.contains(c.name.as_str()))
        .map(item)
        .collect();
    let inits: Vec<Value> = containers
        .iter()
        .filter(|c| init.contains(c.name.as_str()))
        .map(item)
        .collect();
    let mut spec = Map::new();
    if !main.is_empty() {
        spec.insert("containers".into(), Value::Array(main));
    }
    if !inits.is_empty() {
        spec.insert("initContainers".into(), Value::Array(inits));
    }
    let template = json!({"spec": spec});
    let body = match kind {
        WorkloadKind::CronJob => json!({"spec": {"jobTemplate": {"spec": {"template": template}}}}),
        _ => json!({"spec": {"template": template}}),
    };
    Ok(Write {
        verb: Verb::Strategic,
        path,
        body,
        dry_run: true,
    })
}

/// Запросы записи изменения. Значения абсолютные, как у kubectl: `resourceVersion` прочитанного
/// объекта требует только JSON patch DeploymentConfig, адресующий контейнер по индексу (иначе 409);
/// под удаляется, только если это тот же объект (`uid`).
pub fn writes(change: &Change, facts: &Facts, now: &str) -> AppResult<Vec<Write>> {
    let ns = change.namespace();
    let rv = facts.resource_version();
    Ok(match (change, facts) {
        (
            Change::SetResources {
                workload,
                containers,
                ..
            },
            Facts::Workload(w),
        ) => vec![set_resources(
            w,
            ns,
            workload.kind,
            &workload.name,
            containers,
            rv,
        )?],
        (
            Change::Scale {
                workload, replicas, ..
            },
            Facts::Workload(_),
        ) => vec![Write {
            verb: Verb::Merge,
            path: format!("{}/scale", workload_path(ns, workload.kind, &workload.name)),
            body: json!({"spec": {"replicas": replicas}}),
            dry_run: true,
        }],
        (Change::RolloutRestart { workload, .. }, Facts::Workload(_)) => {
            let path = workload_path(ns, workload.kind, &workload.name);
            if workload.kind == WorkloadKind::DeploymentConfig {
                // `oc rollout latest`: новая выкладка; dry-run этот вызов не принимает.
                vec![Write {
                    verb: Verb::Post,
                    path: format!("{path}/instantiate"),
                    body: json!({"kind": "DeploymentRequest", "apiVersion": "apps.openshift.io/v1",
                        "name": workload.name, "latest": true, "force": true}),
                    dry_run: false,
                }]
            } else {
                vec![Write {
                    verb: Verb::Strategic,
                    path,
                    body: json!({"spec": {"template": {"metadata": {"annotations": {RESTARTED_AT: now}}}}}),
                    dry_run: true,
                }]
            }
        }
        (
            Change::SetHpaBounds {
                hpa,
                min_replicas,
                max_replicas,
                ..
            },
            Facts::Hpa(_),
        ) => {
            let mut spec = json!({"maxReplicas": max_replicas});
            if let Some(m) = min_replicas {
                spec["minReplicas"] = json!(m);
            }
            vec![Write {
                verb: Verb::Merge,
                path: format!(
                    "/apis/autoscaling/v2/namespaces/{ns}/horizontalpodautoscalers/{hpa}"
                ),
                body: json!({"spec": spec}),
                dry_run: true,
            }]
        }
        (Change::DeletePod { pod, .. }, Facts::Pod { uid, .. }) => vec![Write {
            verb: Verb::Delete,
            path: format!("/api/v1/namespaces/{ns}/pods/{pod}"),
            body: json!({"apiVersion": "v1", "kind": "DeleteOptions",
                "preconditions": {"uid": uid}}),
            dry_run: true,
        }],
        _ => return Err(AppError::internal_with("facts do not match the change")),
    })
}

/// Итог server-side dry-run.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum DryRun {
    Passed,
    /// API-сервер отклонил изменение: ошибка до подтверждения.
    Rejected {
        error: AppError,
    },
    /// Вызов dry-run не принимает (`instantiate` DeploymentConfig).
    Skipped,
}

/// Предпросмотр изменения для формы.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Preview {
    pub change: Change,
    /// Версия объекта (`Facts::version`): с ней идёт применение, объект изменился — `CONFLICT`.
    pub version: Option<String>,
    pub diff: Vec<FieldChange>,
    pub warnings: Vec<Warning>,
    pub dry_run: DryRun,
    /// Под без контроллера: при применении нужно ввести его имя.
    pub confirm_pod_name: bool,
}

/// Попыток записи: контроллер обновляет статус объекта и его `resourceVersion` между чтением и
/// записью (идущая выкладка), ответ 409 — повод перечитать объект.
const WRITE_ATTEMPTS: usize = 3;

/// Ответ 409: объект изменился между чтением и записью.
fn raced(e: &AppError) -> bool {
    e.key == "errors.k8s.conflict"
}

/// Предпросмотр: форма, объект, проверка, «было → станет», dry-run.
pub async fn preview(
    r: &ReadClient,
    w: &dyn ClusterWriter,
    change: Change,
    now: &str,
) -> AppResult<Preview> {
    let change = change.trimmed();
    validate_form(&change)?;
    let mut attempt = 1;
    loop {
        // Чтение объекта и ограничений — большая future: в debug-сборке в куче.
        let facts = Box::pin(read_facts(r, &change)).await?;
        let warnings = check(&change, &facts)?;
        let mut dry_run = DryRun::Passed;
        for write in writes(&change, &facts, now)? {
            if !write.dry_run {
                dry_run = DryRun::Skipped;
                continue;
            }
            if let Err(error) = w.send(&write, true).await {
                dry_run = DryRun::Rejected { error };
                break;
            }
        }
        if let DryRun::Rejected { error } = &dry_run
            && raced(error)
            && attempt < WRITE_ATTEMPTS
        {
            attempt += 1;
            continue;
        }
        return Ok(Preview {
            diff: diff(&change, &facts, now),
            confirm_pod_name: warnings.contains(&Warning::Orphan),
            version: facts.version(),
            warnings,
            dry_run,
            change,
        });
    }
}

/// Применение после стражей подключения: объект перечитывается и сверяется с версией
/// предпросмотра `expected` (spec изменился — `CONFLICT`), под без контроллера требует ввода
/// имени (`confirm_pod`). Возвращает применённое изменение.
pub async fn apply(
    r: &ReadClient,
    w: &dyn ClusterWriter,
    change: Change,
    expected: Option<&str>,
    confirm_pod: Option<&str>,
    now: &str,
) -> AppResult<Change> {
    let change = change.trimmed();
    validate_form(&change)?;
    let mut attempt = 1;
    loop {
        // Чтение объекта и ограничений — большая future: в debug-сборке в куче.
        let facts = Box::pin(read_facts(r, &change)).await?;
        let warnings = check(&change, &facts)?;
        if let (Change::DeletePod { pod, .. }, true) =
            (&change, warnings.contains(&Warning::Orphan))
        {
            crate::guard::ensure_name_confirmed(pod, confirm_pod)?;
        }
        if let (Some(e), Some(v)) = (expected, facts.version())
            && e != v
        {
            return Err(changed());
        }
        let mut sent = Ok(());
        for write in writes(&change, &facts, now)? {
            sent = w.send(&write, false).await;
            if sent.is_err() {
                break;
            }
        }
        match sent {
            Err(e) if raced(&e) && attempt < WRITE_ATTEMPTS => attempt += 1,
            sent => return sent.map(|()| change),
        }
    }
}

/// Объект изменился после предпросмотра.
fn changed() -> AppError {
    crate::app_err!(
        ErrorCode::Conflict,
        "errors.ops.changed",
        "The object has changed since the preview: refresh and retry"
    )
}

/// Сейчас в RFC 3339 (аннотация перезапуска).
pub fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Mutex;
    use std::time::Duration as StdDuration;

    use super::*;
    use crate::k8s::pods::WorkloadRef;
    use crate::k8s::testing::{mock_client, status};

    /// Ответ записи для тестов.
    type Answer = Box<dyn Fn(&Write, bool) -> AppResult<()> + Send + Sync>;

    /// Запись для тестов: запоминает запросы, отвечает `answer`.
    pub struct FakeWriter {
        pub sent: Mutex<Vec<(Write, bool)>>,
        answer: Answer,
    }

    impl FakeWriter {
        pub fn new(answer: impl Fn(&Write, bool) -> AppResult<()> + Send + Sync + 'static) -> Self {
            Self {
                sent: Mutex::new(Vec::new()),
                answer: Box::new(answer),
            }
        }
    }

    impl ClusterWriter for FakeWriter {
        fn send<'a>(&'a self, w: &'a Write, dry_run: bool) -> BoxFuture<'a, AppResult<()>> {
            self.sent.lock().unwrap().push((w.clone(), dry_run));
            let r = (self.answer)(w, dry_run);
            Box::pin(async move { r })
        }
    }

    fn deployment() -> serde_json::Value {
        json!({"apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "web", "namespace": "team-a", "resourceVersion": "11", "generation": 2},
            "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "web"}},
                "template": {"metadata": {"labels": {"app": "web"}}, "spec": {
                    "initContainers": [{"name": "proxy", "image": "p", "restartPolicy": "Always"}],
                    "containers": [{"name": "app", "image": "web:1", "resources": {
                        "requests": {"cpu": "500m", "ephemeral-storage": "1Gi"}, "limits": {"memory": "1Gi"}}}]}}}})
    }

    fn cluster(s: &crate::k8s::testing::Seen) -> (u16, serde_json::Value) {
        let list = |kind: &str| {
            (
                200,
                json!({"apiVersion": "v1", "kind": kind, "metadata": {}, "items": []}),
            )
        };
        match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => (200, deployment()),
            "/apis/apps.openshift.io/v1/namespaces/team-a/deploymentconfigs/web" => (
                200,
                json!({"apiVersion": "apps.openshift.io/v1", "kind": "DeploymentConfig",
                    "metadata": {"name": "web", "namespace": "team-a", "resourceVersion": "21", "generation": 7},
                    "spec": {"replicas": 1, "selector": {"app": "web"},
                        "template": {"metadata": {"labels": {"app": "web"}}, "spec": {"containers": [
                            {"name": "side", "image": "s"},
                            {"name": "app", "image": "web:1", "resources": {
                                "requests": {"cpu": "500m", "ephemeral-storage": "1Gi"}}}]}}},
                    "status": {}}),
            ),
            "/apis/batch/v1/namespaces/team-a/cronjobs/web" => (
                200,
                json!({"apiVersion": "batch/v1", "kind": "CronJob",
                    "metadata": {"name": "web", "namespace": "team-a", "resourceVersion": "31"},
                    "spec": {"schedule": "* * * * *", "jobTemplate": {"spec": {"template": {
                        "spec": {"containers": [{"name": "app", "image": "r"}]}}}}}}),
            ),
            "/api/v1/namespaces/team-a/pods/solo" => (
                200,
                json!({"apiVersion": "v1", "kind": "Pod",
                    "metadata": {"name": "solo", "resourceVersion": "3", "uid": "u-solo"}, "spec": {"containers": []}}),
            ),
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers/web" => (
                200,
                json!({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                    "metadata": {"name": "web", "resourceVersion": "5", "generation": 1},
                    "spec": {"minReplicas": 2, "maxReplicas": 6, "scaleTargetRef": {"kind": "Deployment", "name": "web"}}}),
            ),
            p if p.ends_with("/horizontalpodautoscalers") => list("HorizontalPodAutoscalerList"),
            p if p.ends_with("/limitranges") => list("LimitRangeList"),
            p if p.ends_with("/resourcequotas") => list("ResourceQuotaList"),
            _ => status(404, "NotFound", "not found"),
        }
    }

    pub fn reader() -> ReadClient {
        ReadClient::new(mock_client(cluster).0, StdDuration::from_secs(5))
    }

    fn workload(kind: WorkloadKind) -> WorkloadRef {
        WorkloadRef {
            kind,
            name: "web".into(),
        }
    }

    fn set(kind: &str, containers: Value) -> Change {
        serde_json::from_value(json!({"kind": "setResources", "namespace": "team-a",
            "workload": {"kind": kind, "name": "web"}, "containers": containers}))
        .unwrap()
    }

    #[tokio::test]
    async fn resources_are_a_strategic_merge_patch_by_container_name() {
        let w = FakeWriter::new(|_, _| Ok(()));
        let c = set(
            "Deployment",
            json!([
                {"name": "app", "requests": {"cpu": "250m", "memory": null}, "limits": {"memory": "2Gi"}},
                {"name": "proxy", "requests": {"cpu": "50m"}}
            ]),
        );
        let p = preview(&reader(), &w, c, "now").await.unwrap();
        assert!(matches!(p.dry_run, DryRun::Passed));
        assert_eq!(
            p.version.as_deref(),
            Some("2"),
            "generation, не resourceVersion"
        );
        let sent = w.sent.lock().unwrap();
        let (write, dry) = &sent[0];
        assert!(*dry);
        assert_eq!(
            (write.verb, write.path.as_str()),
            (
                Verb::Strategic,
                "/apis/apps/v1/namespaces/team-a/deployments/web"
            )
        );
        assert_eq!(
            write.body,
            json!({"spec": {"template": {"spec": {
            "containers": [{"name": "app", "resources": {"requests": {"cpu": "250m", "memory": null}, "limits": {"memory": "2Gi"}}}],
            "initContainers": [{"name": "proxy", "resources": {"requests": {"cpu": "50m"}}}]}}}})
        );
    }

    #[tokio::test]
    async fn deployment_config_resources_are_a_json_patch_that_keeps_other_resources() {
        let w = FakeWriter::new(|_, _| Ok(()));
        let c = set(
            "DeploymentConfig",
            json!([{"name": "app", "requests": {"cpu": null, "memory": "256Mi"}}]),
        );
        apply(&reader(), &w, c, Some("7"), None, "now")
            .await
            .unwrap();
        let (write, dry) = w.sent.lock().unwrap()[0].clone();
        assert!(!dry);
        assert_eq!(write.verb, Verb::Json);
        assert_eq!(
            write.body,
            json!([
                {"op": "replace", "path": "/metadata/resourceVersion", "value": "21"},
                {"op": "add", "path": "/spec/template/spec/containers/1/resources",
                 "value": {"requests": {"ephemeral-storage": "1Gi", "memory": "256Mi"}}}
            ])
        );
    }

    #[tokio::test]
    async fn cron_job_template_and_job_refusal() {
        let w = FakeWriter::new(|_, _| Ok(()));
        let c = set("CronJob", json!([{"name": "app", "limits": {"cpu": "1"}}]));
        preview(&reader(), &w, c, "now").await.unwrap();
        let body = w.sent.lock().unwrap()[0].0.body.clone();
        assert_eq!(
            body["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]
                ["cpu"],
            "1"
        );
        assert_eq!(
            body.get("metadata"),
            None,
            "без resourceVersion: значения абсолютные"
        );
    }

    #[tokio::test]
    async fn scale_restart_hpa_and_delete() {
        let w = FakeWriter::new(|_, _| Ok(()));
        let r = reader();
        let scale = Change::Scale {
            namespace: "team-a".into(),
            workload: workload(WorkloadKind::Deployment),
            replicas: 4,
        };
        apply(&r, &w, scale, Some("2"), None, "now").await.unwrap();
        let restart = Change::RolloutRestart {
            namespace: "team-a".into(),
            workload: workload(WorkloadKind::Deployment),
        };
        apply(&r, &w, restart, None, None, "2026-10-28T12:00:00Z")
            .await
            .unwrap();
        let dc = Change::RolloutRestart {
            namespace: "team-a".into(),
            workload: workload(WorkloadKind::DeploymentConfig),
        };
        let p = preview(&r, &w, dc.clone(), "now").await.unwrap();
        assert!(
            matches!(p.dry_run, DryRun::Skipped),
            "instantiate без dry-run"
        );
        apply(&r, &w, dc, None, None, "now").await.unwrap();
        let hpa = Change::SetHpaBounds {
            namespace: "team-a".into(),
            hpa: "web".into(),
            min_replicas: Some(3),
            max_replicas: 8,
        };
        apply(&r, &w, hpa, Some("1"), None, "now").await.unwrap();
        let sent: Vec<(Verb, String, Value)> = w
            .sent
            .lock()
            .unwrap()
            .iter()
            .map(|(x, _)| (x.verb, x.path.clone(), x.body.clone()))
            .collect();
        assert_eq!(
            sent[0],
            (
                Verb::Merge,
                "/apis/apps/v1/namespaces/team-a/deployments/web/scale".into(),
                json!({"spec": {"replicas": 4}})
            )
        );
        assert_eq!(
            sent[1].2["spec"]["template"]["metadata"]["annotations"][RESTARTED_AT],
            "2026-10-28T12:00:00Z"
        );
        assert_eq!(
            sent[2].1,
            "/apis/apps.openshift.io/v1/namespaces/team-a/deploymentconfigs/web/instantiate"
        );
        assert_eq!(sent[2].2["latest"], true);
        assert_eq!(
            sent[3],
            (
                Verb::Merge,
                "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers/web".into(),
                json!({"spec": {"minReplicas": 3, "maxReplicas": 8}})
            )
        );
        // Под без контроллера удаляется только с его именем.
        let pod = Change::DeletePod {
            namespace: "team-a".into(),
            pod: "solo".into(),
        };
        let p = preview(&r, &w, pod.clone(), "now").await.unwrap();
        assert!(p.confirm_pod_name);
        let e = apply(&r, &w, pod.clone(), Some("u-solo"), None, "now")
            .await
            .unwrap_err();
        assert_eq!(e.key, "errors.guard.typeToConfirm");
        apply(&r, &w, pod, Some("u-solo"), Some("solo"), "now")
            .await
            .unwrap();
        let last = w.sent.lock().unwrap().last().unwrap().0.clone();
        assert_eq!(
            (last.verb, last.body["preconditions"].clone()),
            (Verb::Delete, json!({"uid": "u-solo"}))
        );
    }

    #[tokio::test]
    async fn a_changed_object_conflicts_and_a_rejected_dry_run_is_shown() {
        let w = FakeWriter::new(|_, dry| {
            if dry {
                Err(admission(crate::k8s::errors::forbidden(
                    "admission webhook \"policy.example.com\" denied the request: no",
                )))
            } else {
                Ok(())
            }
        });
        let c = set(
            "Deployment",
            json!([{"name": "app", "requests": {"cpu": "1"}}]),
        );
        let p = preview(&reader(), &w, c.clone(), "now").await.unwrap();
        let DryRun::Rejected { error } = p.dry_run else {
            panic!("{:?}", p.dry_run)
        };
        assert_eq!(error.code, ErrorCode::AdmissionDenied);
        assert!(
            error.message.contains("policy.example.com"),
            "{}",
            error.message
        );
        let e = apply(&reader(), &w, c.clone(), Some("10"), None, "now")
            .await
            .unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (ErrorCode::Conflict, "errors.ops.changed")
        );
        // Гонка со статусом (409) — объект перечитывается, dry-run и запись повторяются; не больше
        // трёх раз.
        let racing = || {
            let tries = std::sync::atomic::AtomicUsize::new(0);
            FakeWriter::new(move |_, _| {
                match tries.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                    0 => Err(crate::k8s::errors::from_status(
                        &kube::core::Status::failure("the object has been modified", "Conflict")
                            .with_code(409),
                    )),
                    _ => Ok(()),
                }
            })
        };
        let w = racing();
        let p = preview(&reader(), &w, c.clone(), "now").await.unwrap();
        assert!(matches!(p.dry_run, DryRun::Passed), "{:?}", p.dry_run);
        let w = racing();
        apply(&reader(), &w, c.clone(), Some("2"), None, "now")
            .await
            .unwrap();
        assert_eq!(w.sent.lock().unwrap().len(), 2);
        let busy = FakeWriter::new(|_, _| {
            Err(crate::k8s::errors::from_status(
                &kube::core::Status::failure("the object has been modified", "Conflict")
                    .with_code(409),
            ))
        });
        let e = apply(&reader(), &busy, c, None, None, "now")
            .await
            .unwrap_err();
        assert_eq!(
            (e.key.as_str(), busy.sent.lock().unwrap().len()),
            ("errors.k8s.conflict", WRITE_ATTEMPTS)
        );
        // Отказ RBAC остаётся «нет прав».
        let rbac = admission(crate::k8s::errors::forbidden(
            "deployments.apps \"web\" is forbidden: User \"u\" cannot patch resource \"deployments\" in API group \"apps\" in the namespace \"team-a\"",
        ));
        assert_eq!(rbac.code, ErrorCode::Forbidden);
    }

    #[tokio::test]
    async fn kube_writer_sends_the_field_manager_and_dry_run() {
        let (client, server) = mock_client(|_| (200, json!({})));
        let w = KubeWriter::new(client, StdDuration::from_secs(5));
        let write = Write {
            verb: Verb::Merge,
            path: "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers/web".into(),
            body: json!({"spec": {"maxReplicas": 3}}),
            dry_run: true,
        };
        w.send(&write, true).await.unwrap();
        let delete = Write {
            verb: Verb::Delete,
            path: "/api/v1/namespaces/team-a/pods/web-1".into(),
            body: json!({"preconditions": {"uid": "u1"}}),
            dry_run: true,
        };
        w.send(&delete, true).await.unwrap();
        w.send(&delete, false).await.unwrap();
        drop(w);
        let seen = server.await.unwrap();
        assert_eq!(seen[0].method, http::Method::PATCH);
        assert!(
            seen[0]
                .uri
                .ends_with("?fieldManager=glafira-kubernetes&dryRun=All"),
            "{}",
            seen[0].uri
        );
        assert_eq!(seen[0].body, json!({"spec": {"maxReplicas": 3}}));
        // DELETE: dry-run — в теле `DeleteOptions`, строку запроса API-сервер не читает.
        assert_eq!(seen[1].method, http::Method::DELETE);
        assert_eq!(seen[1].uri, "/api/v1/namespaces/team-a/pods/web-1");
        assert_eq!(
            seen[1].body,
            json!({"preconditions": {"uid": "u1"}, "dryRun": ["All"]})
        );
        assert_eq!(seen[2].body, json!({"preconditions": {"uid": "u1"}}));
    }
}
````

<!-- block: patch t11-src step=11.2 -->
````diff
diff --git a/src-tauri/src/change/mod.rs b/src-tauri/src/change/mod.rs
index 851a1f1..82f5749 100644
--- a/src-tauri/src/change/mod.rs
+++ b/src-tauri/src/change/mod.rs
@@ -213,7 +213,17 @@ pub fn validate_form(c: &Change) -> AppResult<()> {
         crate::k8s::read::check_name(&w.name)?;
     }
     match c {
-        Change::SetResources { containers, .. } => {
+        Change::SetResources {
+            workload,
+            containers,
+            ..
+        } => {
+            if workload.kind == WorkloadKind::Job {
+                return Err(invalid(crate::ltext!(
+                    "errors.change.resourcesKind",
+                    "The resources of a Job cannot be changed: its pod template is immutable"
+                )));
+            }
             if containers.is_empty() || containers.len() > MAX_CONTAINERS {
                 return Err(invalid(crate::ltext!(
                     "errors.change.containersCount",
@@ -515,6 +525,10 @@ mod tests {
             key(set(json!([{"name": "a/b", "requests": {"cpu": "1"}}]))).as_deref(),
             Some("errors.k8s.badName")
         );
+        let job: Change = serde_json::from_value(json!({"kind": "setResources", "namespace": "team-a",
+            "workload": {"kind": "Job", "name": "once"}, "containers": [{"name": "app", "requests": {"cpu": "1"}}]}))
+        .unwrap();
+        assert_eq!(key(job).as_deref(), Some("errors.change.resourcesKind"));
         let scale = |kind: WorkloadKind, replicas| Change::Scale {
             namespace: "team-a".into(),
             workload: WorkloadRef {
diff --git a/src-tauri/src/commands/mod.rs b/src-tauri/src/commands/mod.rs
index 2fad953..cc09eb4 100644
--- a/src-tauri/src/commands/mod.rs
+++ b/src-tauri/src/commands/mod.rs
@@ -1,5 +1,6 @@
 pub mod app;
 pub mod auth;
+pub mod changes;
 pub mod cluster;
 pub mod connections;
 pub mod folders;
diff --git a/src-tauri/src/error.rs b/src-tauri/src/error.rs
index 2125de6..e5696dc 100644
--- a/src-tauri/src/error.rs
+++ b/src-tauri/src/error.rs
@@ -19,6 +19,8 @@ pub enum ErrorCode {
     InvalidInput,
     ReadOnly,
     ConfirmationRequired,
+    /// Отказ admission: webhook, квота, `LimitRange` (спец. §9).
+    AdmissionDenied,
     /// Ни один источник метрик не подошёл (спец. §7.1).
     MetricsUnavailable,
     Internal,
diff --git a/src-tauri/src/k8s/access.rs b/src-tauri/src/k8s/access.rs
index abdf887..c3b8f46 100644
--- a/src-tauri/src/k8s/access.rs
+++ b/src-tauri/src/k8s/access.rs
@@ -1,6 +1,7 @@
 //! Права пользователя в проекте (спец. §5.4): `SelfSubjectRulesReview` при открытии проекта,
 //! кэш на 5 минут, сброс при ответе 403. По ним интерфейс не показывает вкладки и разделы,
-//! которые не читаются; окончательно решает API-сервер.
+//! которые не читаются, и делает неактивными кнопки изменений (§9); окончательно решает
+//! API-сервер.
 
 use std::collections::{BTreeMap, HashMap};
 use std::sync::Mutex;
@@ -42,6 +43,11 @@ pub const RESOURCES: &[(&str, &str)] = &[
     ("batch", "cronjobs"),
     ("apps.openshift.io", "deploymentconfigs"),
     ("", "replicationcontrollers"),
+    // Подресурсы изменений (§9): масштаб и выкладка DeploymentConfig.
+    ("apps", "deployments/scale"),
+    ("apps", "statefulsets/scale"),
+    ("apps.openshift.io", "deploymentconfigs/scale"),
+    ("apps.openshift.io", "deploymentconfigs/instantiate"),
 ];
 
 /// Сколько живут права проекта в кэше.
@@ -53,6 +59,12 @@ pub struct Verbs {
     pub get: bool,
     pub list: bool,
     pub watch: bool,
+    /// Изменения (§9): ресурсы, перезапуск, масштаб, границы HPA.
+    pub patch: bool,
+    /// Удаление пода.
+    pub delete: bool,
+    /// `instantiate` DeploymentConfig.
+    pub create: bool,
 }
 
 #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
@@ -113,6 +125,9 @@ pub fn access_from_rules(rules: &[ResourceRule], incomplete: bool) -> NamespaceA
                         get: can("get"),
                         list: can("list"),
                         watch: can("watch"),
+                        patch: can("patch"),
+                        delete: can("delete"),
+                        create: can("create"),
                     },
                 )
             })
@@ -251,9 +266,17 @@ mod tests {
             Verbs {
                 get: false,
                 list: true,
-                watch: false
+                watch: false,
+                patch: false,
+                delete: false,
+                create: false,
             }
         );
+        assert!(a.resources["jobs.batch"].patch && a.resources["jobs.batch"].delete);
+        assert!(
+            !a.resources["deployments/scale.apps"].patch,
+            "list подресурсу не даёт patch"
+        );
         assert!(a.resources["pods/log"].get);
         assert!(!a.resources["secrets"].list);
     }
diff --git a/src-tauri/src/k8s/pool.rs b/src-tauri/src/k8s/pool.rs
index 6d99334..788fb94 100644
--- a/src-tauri/src/k8s/pool.rs
+++ b/src-tauri/src/k8s/pool.rs
@@ -72,6 +72,12 @@ impl Connected {
         ReadClient::new(self.client.clone(), self.timeout)
     }
 
+    /// Запись в кластер — только для `ops` (изменяющие операции после стражей). MCP-шлюз её не
+    /// берёт (сторожок `mcp::guard`).
+    pub fn writer(&self) -> crate::ops::KubeWriter {
+        crate::ops::KubeWriter::new(self.client.clone(), self.timeout)
+    }
+
     /// Учётные данные истекают в ближайшую минуту: пора запустить плагин снова.
     pub fn expiring(&self, now: SystemTime) -> bool {
         self.expires_at
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index d4cf38d..32a009d 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -13,6 +13,7 @@ pub mod logging;
 pub mod logs;
 pub mod mcp;
 pub mod metrics;
+pub mod ops;
 pub mod platform;
 pub mod policy;
 pub mod product;
@@ -162,6 +163,8 @@ pub fn run() {
             commands::resources::get_pod_usage,
             commands::resources::recommend_resources,
             commands::resources::get_project_usage,
+            commands::changes::preview_change,
+            commands::changes::apply_change,
             commands::health::run_health_check,
             commands::health::get_health,
             commands::health::set_finding_ignored,
````

<!-- block: locales t11-locales step=11.2 -->
````json
{
  "en": {
    "errors": {
      "change": {
        "resourcesKind": "The resources of a Job cannot be changed: its pod template is immutable"
      },
      "ops": {
        "admissionDenied": "The cluster rejected the change: {{message}}",
        "changed": "The object has changed since the preview: refresh and retry"
      }
    }
  },
  "ru": {
    "errors": {
      "change": {
        "resourcesKind": "Ресурсы Job изменить нельзя: шаблон его подов неизменяем"
      },
      "ops": {
        "admissionDenied": "Кластер отклонил изменение: {{message}}",
        "changed": "Объект изменился после предпросмотра: обновите и повторите"
      }
    }
  }
}
````

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 670 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm test`  
Expected: PASS — `Test Files 63 passed`, `Tests 479 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 198`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 4: Commit.**

```bash
git add \
  src-tauri/src/change/mod.rs \
  src-tauri/src/commands/changes.rs \
  src-tauri/src/commands/mod.rs \
  src-tauri/src/error.rs \
  src-tauri/src/k8s/access.rs \
  src-tauri/src/k8s/pool.rs \
  src-tauri/src/lib.rs \
  src-tauri/src/ops/mod.rs \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "feat(ops): mutating operations of the forms — preview with a field diff and a server-side dry-run, apply after the guards with the field manager and the previewed generation, a retry on a status race, CONFLICT and ADMISSION_DENIED, the app log and the proposal mark; patch and delete rights" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 12: MCP: get_resource_usage, recommend_resources, propose_change, list_proposals и withdraw_proposal — 22 инструмента; предложения проверяются чтением кластера и хранятся в Glafira; адрес своего источника метрик модель не получает

**Files:**
- Port: `crates/glafira-mcp-kubernetes/src/server/proposal_tools.rs` (с правками)
- Create (по Kafka-версии): `crates/glafira-mcp-kubernetes/src/proposals.rs` ← `crates/glafira-mcp/src/proposals.rs`
- Create: `crates/glafira-mcp-kubernetes/src/resources.rs`, `crates/glafira-mcp-kubernetes/src/server/resource_tools.rs`, `src-tauri/src/mcp/proposals.rs`, `src-tauri/src/mcp/resources.rs`
- Modify: `PORTED.md`, `crates/glafira-mcp-kubernetes/src/lib.rs`, `crates/glafira-mcp-kubernetes/src/model.rs`, `crates/glafira-mcp-kubernetes/src/server/mod.rs`, `crates/glafira-mcp-kubernetes/tests/server.rs`, `src-tauri/src/mcp/backend.rs`, `src-tauri/src/mcp/mod.rs`, `src-tauri/src/metrics/select.rs`, `src-tauri/src/resources/mod.rs`, `src-tauri/src/resources/usage.rs`

**Interfaces:**
- Consumes: задачи 4–6, 9–10: `for_connection`, `workload_usage`, `analyze`, `read_facts`, `check`, `store::{propose, withdraw_own}`; O2: крейт `glafira-mcp-kubernetes` (`model`, `server`), `AppBackend`; Kafka-версия @ `8bbd0bd`: `crates/glafira-mcp/src/{proposals.rs, server/proposal_tools.rs}`.
- Produces: крейт: `resources::{WINDOWS, WINDOW_DEFAULT = 7, Source, RejectedSource, UsageStats, ContainerUsage, ContainerSeries, Marker, UsageSeries, ResourceUsage, ContainerRecommendation, Totals, Recommendation}`, `proposals::{ResourceValuesSpec, ContainerResourcesSpec, ChangeSpec (4 вида), NewProposal, ProposalOp, ProposalView, ProposalOutput}`, `ReadOp::{ResourceUsage, RecommendResources}`, `ReadOutput::{ResourceUsage, Recommendation}`, `Backend::proposals`, `server::{resource_tools, proposal_tools}`, новые инструкции сервера; приложение: `mcp::resources::{usage, recommendation, metrics_unavailable}`, `mcp::proposals::proposals_impl`.

Спец. §10.4–10.5. Окно — 1, 7, 14 или 30 дней (иначе `invalid_argument`); ряды, не поместившиеся в 100 КБ, опускаются с подсказкой. Ответ называет вид источника и причины отказа, но не адрес своего источника из профиля. `propose_change` проверяет предложение только чтением (объект, контейнеры, quantity, `limits ≥ requests`); `scale` под управлением HPA — `invalid_argument` с советом `setHpaBounds`; нарушения `LimitRange` и квоты — `warnings`, предложение создаётся; у read-only подключения — `readOnly: true`. Удаление пода предложить нельзя. Server-side dry-run при создании не выполняется: у бэкенда MCP нет запросов на запись даже с `dryRun`.

- [ ] **Step 1: Перенос из Kafka-версии.**

Файлы копируются с коммита `8bbd0bd` репозитория `~/kafka-explorer` (`$KAFKA_REPO`) и правятся патчами — против перенесённого файла. Строки о них в `PORTED.md` добавляет шаг реализации.

<!-- block: shell t12-port step=12.1 -->
````bash
scripts/port.sh crates/glafira-mcp/src/server/proposal_tools.rs crates/glafira-mcp-kubernetes/src/server/proposal_tools.rs
````

<!-- block: patch t12-port-crates-glafira-mcp-kubernetes-src-server-proposal-tools-rs step=12.1 -->
````diff
diff --git a/crates/glafira-mcp-kubernetes/src/server/proposal_tools.rs b/crates/glafira-mcp-kubernetes/src/server/proposal_tools.rs
--- a/crates/glafira-mcp-kubernetes/src/server/proposal_tools.rs
+++ b/crates/glafira-mcp-kubernetes/src/server/proposal_tools.rs
@@ -1,6 +1,9 @@
-//! Инструменты предложений (спец. G3 §4). Кластер они не меняют: предложение пишется в Glafira
-//! и показывается человеку рядом с объектом; применяет его человек формой.
+//! Инструменты предложений (спец. §10.4, §10.5, как в Kafka G3). Кластер они не меняют:
+//! предложение пишется в Glafira и показывается человеку рядом с объектом; применяет его человек
+//! формой. Предложения вне области LLM (проекты не из `llmNamespaces`) в ответ не попадают.
 
+use glafira_mcp_core::fit;
+use glafira_mcp_core::{BackendError, to_value};
 use rmcp::handler::server::wrapper::Parameters;
 use rmcp::model::CallToolResult;
 use rmcp::service::RequestContext;
@@ -9,11 +12,9 @@ use schemars::JsonSchema;
 use serde::{Deserialize, Serialize};
 use serde_json::{Value, json};
 
-use super::{GatewayServer, Recency, newest_first, to_value};
-use crate::health::{Severity, StatusFilter};
-use crate::model::BackendError;
+use super::KubeServer;
+use crate::health::{Severity, StatusFilter, in_scope};
 use crate::proposals::*;
-use crate::rules;
 
 /// У `list_proposals` нет страниц: при обрезке остаются самые свежие, остальное — сужением по status.
 const PROPOSALS_TRUNCATED_HINT: &str = "The response was truncated to 100 KB: only the most recently updated proposals are shown; narrow with status";
@@ -70,22 +71,37 @@ fn proposals_of(b: &mut ProposalsBody) -> &mut Vec<ProposalView> {
     &mut b.items
 }
 
-impl Recency for ProposalView {
-    fn updated_at_ms(&self) -> i64 {
-        self.updated_at_ms
-    }
-    fn created_at_ms(&self) -> i64 {
-        self.created_at_ms
-    }
-    fn id(&self) -> &str {
-        &self.id
+/// Сначала недавно обновлённые, при равенстве — созданные позже, затем по id: обрезка до 100 КБ
+/// оставляет голову.
+fn newest_first(items: &mut [ProposalView]) {
+    items.sort_by(|a, b| {
+        b.updated_at_ms
+            .cmp(&a.updated_at_ms)
+            .then(b.created_at_ms.cmp(&a.created_at_ms))
+            .then_with(|| a.id.cmp(&b.id))
+    });
+}
+
+impl KubeServer {
+    /// Операция предложений. `cluster` — она читает кластер (проверка предложения): тогда
+    /// занимает слоты параллельности, как чтение.
+    async fn proposals(
+        &self,
+        op: ProposalOp,
+        cluster: bool,
+    ) -> Result<ProposalOutput, BackendError> {
+        let _slots = match cluster {
+            true => Some(self.session.slots().await?),
+            false => None,
+        };
+        self.backend.proposals(op).await
     }
 }
 
 #[tool_router(router = proposal_router, vis = "pub(crate)")]
-impl GatewayServer {
+impl KubeServer {
     #[tool(
-        description = "Propose a change for the user to apply in Glafira. Does not change the cluster: the proposal appears next to the object with your title and rationale, and the user opens it as a pre-filled form, may edit it and confirms (or rejects it). Kinds: alterTopicConfigs (set or reset topic configs), createTopic, addPartitions, resetOffsets (the group must be empty when applied). Deleting or purging cannot be proposed — describe it in the chat instead. One problem — one proposal; a proposal with the same kind and object as your open one replaces it. The proposal is checked against the cluster (unknown topic or group, unknown config name, partitions not increasing). Then check the user's decision with list_proposals. readOnly: true in the response means the connection is read-only in Glafira and the user must lift that first. A proposal expires after 7 days; at most 50 open per connection.",
+        description = "Propose a change for the user to apply in Glafira. Does not change the cluster: the proposal appears next to the object with your title and rationale, and the user opens it as a pre-filled form, may edit it, reviews a server-side dry-run and confirms (or rejects it). Kinds: setResources (requests and limits of containers), scale (replicas), rolloutRestart, setHpaBounds. Deleting objects or other edits cannot be proposed — describe them in the chat instead. One problem — one proposal; a proposal with the same kind and object as your open one replaces it. The proposal is checked against the cluster (unknown workload, container or HPA, limits below requests, scaling a workload managed by an HPA); LimitRange, quota and other warnings come back in warnings and do not block it. Then check the user's decision with list_proposals. readOnly: true in the response means the connection is read-only in Glafira and the user must lift that first. A proposal expires after 7 days; at most 50 open per connection.",
         annotations(
             read_only_hint = false,
             destructive_hint = false,
@@ -102,11 +118,13 @@ impl GatewayServer {
         let args = json!({
             "connectionId": p.connection_id,
             "kind": p.change.kind_name(),
+            "namespace": p.change.namespace(),
             "target": p.change.target_name(),
             "severity": p.severity,
             "relatedRuleId": p.related_rule_id,
         });
         Ok(self
+            .session
             .call(
                 &ctx,
                 "propose_change",
@@ -129,9 +147,10 @@ impl GatewayServer {
                             "relatedFindingKey is longer than {MAX_RELATED_KEY_CHARS} characters"
                         )));
                     }
+                    self.scope(&p.connection_id, Some(p.change.namespace().trim()))?;
                     let op = ProposalOp::Propose {
                         connection_id: p.connection_id.clone(),
-                        session: self.identify(None),
+                        session: self.session.identify(None),
                         proposal: NewProposal {
                             severity: p.severity,
                             title: p.title.clone(),
@@ -146,10 +165,12 @@ impl GatewayServer {
                             proposal_id,
                             created,
                             read_only,
+                            warnings,
                         } => Ok(json!({
                             "proposalId": proposal_id,
                             "created": created,
                             "readOnly": read_only,
+                            "warnings": warnings,
                         })),
                         _ => Err(BackendError::unexpected()),
                     }
@@ -159,7 +180,7 @@ impl GatewayServer {
     }
 
     #[tool(
-        description = "List the change proposals left in Glafira for a connection by any client, with the user's decision: open, applied (appliedAsProposed tells whether the user applied it unchanged), rejected (with rejectReason when the user gave one) or withdrawn. When the user applied it with edits, appliedChange holds what was actually applied (when known), in the same shape as change. Check it after proposing instead of asking the user in the chat.",
+        description = "List the change proposals left in Glafira for a connection by any client, with the user's decision: open, applied (appliedAsProposed tells whether the user applied it unchanged), rejected (with rejectReason when the user gave one) or withdrawn. When the user applied it with edits, appliedChange holds what was actually applied, in the same shape as change. Check it after proposing instead of asking the user in the chat.",
         annotations(
             read_only_hint = true,
             destructive_hint = false,
@@ -171,27 +192,30 @@ impl GatewayServer {
         Parameters(p): Parameters<ListProposalsParams>,
         ctx: RequestContext<RoleServer>,
     ) -> Result<CallToolResult, ErrorData> {
-        let args = to_value(&p).unwrap_or(Value::Null);
+        let args = serde_json::to_value(&p).unwrap_or(Value::Null);
         Ok(self
+            .session
             .call(
                 &ctx,
                 "list_proposals",
                 args,
                 Some(&p.connection_id),
                 async {
+                    let scope = self.scope(&p.connection_id, None)?;
                     let op = ProposalOp::List {
                         connection_id: p.connection_id.clone(),
                         status: p.status.unwrap_or_default(),
                     };
                     match self.proposals(op, false).await? {
                         ProposalOutput::Proposals(mut items) => {
+                            items.retain(|x| in_scope(&x.target, &scope));
                             newest_first(&mut items);
                             let mut body = ProposalsBody {
                                 items,
                                 truncated: false,
                                 hint: None,
                             };
-                            if rules::fit(&mut body, proposals_of) {
+                            if fit::fit(&mut body, proposals_of) {
                                 body.truncated = true;
                                 body.hint = Some(PROPOSALS_TRUNCATED_HINT);
                             }
@@ -218,17 +242,19 @@ impl GatewayServer {
         Parameters(p): Parameters<WithdrawProposalParams>,
         ctx: RequestContext<RoleServer>,
     ) -> Result<CallToolResult, ErrorData> {
-        let args = to_value(&p).unwrap_or(Value::Null);
+        let args = serde_json::to_value(&p).unwrap_or(Value::Null);
         Ok(self
+            .session
             .call(
                 &ctx,
                 "withdraw_proposal",
                 args,
                 Some(&p.connection_id),
                 async {
+                    self.scope(&p.connection_id, None)?;
                     let op = ProposalOp::Withdraw {
                         connection_id: p.connection_id.clone(),
-                        session: self.identify(None),
+                        session: self.session.identify(None),
                         proposal_id: p.proposal_id.clone(),
                     };
                     match self.proposals(op, false).await? {
````

- [ ] **Step 2: Тесты.**

<!-- block: file t12-crates-glafira-mcp-kubernetes-src-proposals-rs-tests step=12.2 path=crates/glafira-mcp-kubernetes/src/proposals.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn change_json_matches_the_stored_form() {
        let c: ChangeSpec = serde_json::from_value(json!({
            "kind": "setResources", "namespace": "team-a",
            "workload": {"kind": "Deployment", "name": "web"},
            "containers": [{"name": "app", "requests": {"cpu": "250m", "memory": null}}]
        }))
        .unwrap();
        assert_eq!(
            (c.kind_name(), c.target_name(), c.namespace()),
            ("setResources", "web", "team-a")
        );
        assert_eq!(
            serde_json::to_value(&c).unwrap()["containers"][0],
            json!({"name": "app", "requests": {"cpu": "250m", "memory": null}, "limits": {}})
        );
        let h: ChangeSpec = serde_json::from_value(json!({
            "kind": "setHpaBounds", "namespace": "team-a", "hpa": "web", "maxReplicas": 5
        }))
        .unwrap();
        assert_eq!(h.target_name(), "web");
        assert!(
            serde_json::from_value::<ChangeSpec>(
                json!({"kind": "deletePod", "namespace": "a", "pod": "x"})
            )
            .is_err(),
            "удаление пода предложить нельзя"
        );
    }

    #[test]
    fn schema_uses_camel_case_field_names() {
        let schema = serde_json::to_value(schemars::schema_for!(ChangeSpec)).unwrap();
        let text = schema.to_string();
        for name in [
            "minReplicas",
            "maxReplicas",
            "setHpaBounds",
            "rolloutRestart",
        ] {
            assert!(text.contains(name), "{name}: {text}");
        }
        assert!(!text.contains("max_replicas"), "{text}");
    }
}
````

<!-- block: file t12-src-tauri-src-mcp-resources-rs-tests step=12.2 path=src-tauri/src/mcp/resources.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::prometheus::{Reason, SourceKind};
    use crate::metrics::select::{Mode, Rejected};

    fn status() -> SourceStatus {
        let own = SourceInfo {
            kind: SourceKind::Prometheus,
            origin: SourceOrigin::Profile,
            location: "https://secret-prometheus.corp.example".into(),
            history: true,
        };
        let auto = SourceInfo {
            kind: SourceKind::MetricsServer,
            origin: SourceOrigin::Auto,
            location: "metrics.k8s.io".into(),
            history: false,
        };
        let rejected = |source: SourceInfo, reason| Rejected {
            message: crate::metrics::prometheus::reason_text(reason, &source),
            error: crate::error::AppError::internal_with(source.location.clone()),
            source,
            reason,
        };
        SourceStatus {
            mode: Mode::Prometheus,
            selected: None,
            rejected: vec![
                rejected(own, Reason::Forbidden),
                rejected(auto, Reason::NotFound),
            ],
            pod_metrics: false,
            checked_at_ms: 0,
        }
    }

    #[test]
    fn the_model_never_gets_the_own_source_address() {
        let e = metrics_unavailable(&status());
        assert_eq!(e.code, "metrics_unavailable");
        assert!(!e.message.contains("secret-prometheus"), "{}", e.message);
        assert!(
            e.message
                .contains("prometheus (own source from the connection settings): forbidden"),
            "{}",
            e.message
        );
        assert!(
            e.message.contains("metricsServer metrics.k8s.io: notFound"),
            "{}",
            e.message
        );
        let r = rejected(&status());
        assert_eq!(r[0].location, None);
    }

    #[test]
    fn notes_and_warnings_read_as_english_sentences() {
        assert!(
            note(&Note::CpuThrottled { p95: 0.25 }).starts_with("Throttled in 25% of CPU periods")
        );
        assert_eq!(
            note(&Note::LimitRange {
                field: Field::MemoryLimit,
                bound: Bound::Max
            }),
            "The memory limit was pulled to the LimitRange maximum"
        );
        assert_eq!(
            violation(&Violation::Quota {
                quota: "compute".into(),
                resource: "requests.memory".into(),
                needed: 2048.0 * MIB,
                free: 1024.0 * MIB,
            }),
            "ResourceQuota compute: requests.memory needs 2Gi more with all replicas and the rollout surge, only 1Gi is free"
        );
        assert_eq!(
            warning(&Warning::LimitRange {
                container: "app".into(),
                field: Field::CpuRequest,
                bound: Bound::Min,
                limit: 0.1,
            }),
            "Container app: the CPU request is below the LimitRange minimum 100m: new pods will be rejected"
        );
    }
}
````

<!-- block: patch t12-tests step=12.2 -->
````diff
diff --git a/crates/glafira-mcp-kubernetes/tests/server.rs b/crates/glafira-mcp-kubernetes/tests/server.rs
index cb86f78..926a0bd 100644
--- a/crates/glafira-mcp-kubernetes/tests/server.rs
+++ b/crates/glafira-mcp-kubernetes/tests/server.rs
@@ -12,6 +12,8 @@ use glafira_mcp_core::{
 };
 use glafira_mcp_kubernetes::health::*;
 use glafira_mcp_kubernetes::model::*;
+use glafira_mcp_kubernetes::proposals::*;
+use glafira_mcp_kubernetes::resources::*;
 use glafira_mcp_kubernetes::{GatewayOptions, INSTRUCTIONS};
 use rmcp::model::{
     CallToolRequestParams, CallToolResult, ClientCapabilities, ClientConfig, Implementation,
@@ -49,6 +51,9 @@ struct Fake {
     scope_after_check: Mutex<Option<(String, Option<BTreeSet<String>>)>>,
     /// Клиенты, о праве которых шлюз спрашивал приложение, по порядку (`None` — безымянный).
     asked: Mutex<Vec<Option<String>>>,
+    proposal_ops: Mutex<Vec<ProposalOp>>,
+    /// Ряды графиков такой длины, что ответ не помещается в 100 КБ.
+    big_series: AtomicBool,
 }
 
 impl Fake {
@@ -67,6 +72,8 @@ impl Fake {
             scope_overrides: Mutex::new(BTreeMap::new()),
             scope_after_check: Mutex::new(None),
             asked: Mutex::new(Vec::new()),
+            proposal_ops: Mutex::new(Vec::new()),
+            big_series: AtomicBool::new(false),
         })
     }
 
@@ -283,6 +290,113 @@ fn annotations() -> BTreeMap<String, String> {
     ])
 }
 
+fn quantities(cpu: &str, memory: &str) -> Resources {
+    Resources {
+        cpu: Some(cpu.into()),
+        memory: Some(memory.into()),
+    }
+}
+
+fn source() -> Source {
+    Source {
+        kind: "prometheus".into(),
+        origin: "auto".into(),
+        location: Some("monitoring/prometheus-k8s:9090".into()),
+        history: true,
+    }
+}
+
+fn sample_usage(window_days: u32, points: i64) -> ResourceUsage {
+    ResourceUsage {
+        window_days,
+        source: source(),
+        rejected: vec![],
+        coverage_percent: Some(95.0),
+        data_days: Some(7.0),
+        kube_state_metrics: true,
+        replicas: Some(2),
+        containers: vec![ContainerUsage {
+            name: "app".into(),
+            init: false,
+            requests: quantities("500m", "512Mi"),
+            limits: quantities("1", "1Gi"),
+            limit_range_defaults: vec![],
+            stats: UsageStats {
+                cpu_p95: Some("200m".into()),
+                memory_max: Some("400Mi".into()),
+                oom_count: Some(0),
+                ..Default::default()
+            },
+        }],
+        series: Some(UsageSeries {
+            step_seconds: 3024,
+            cpu_cores: vec![ContainerSeries {
+                container: "app".into(),
+                points: (0..points).map(|i| (i * 3024, 0.123_456_789)).collect(),
+            }],
+            memory_mib: vec![],
+            restarts: vec![],
+            ooms: vec![],
+        }),
+    }
+}
+
+fn sample_recommendation(window_days: u32) -> Recommendation {
+    Recommendation {
+        window_days,
+        source: source(),
+        rejected: vec![],
+        confidence: "high".into(),
+        coverage_percent: Some(95.0),
+        data_days: Some(7.0),
+        replicas: Some(2),
+        containers: vec![ContainerRecommendation {
+            name: "app".into(),
+            current_requests: quantities("500m", "512Mi"),
+            current_limits: quantities("1", "1Gi"),
+            recommended_requests: Some(quantities("230m", "352Mi")),
+            recommended_limits: Some(quantities("390m", "528Mi")),
+            notes: vec![],
+        }],
+        violations: vec![],
+        hpa: vec![],
+        totals: Totals::default(),
+    }
+}
+
+fn proposal_view(id: &str, ns: &str, updated: i64) -> ProposalView {
+    ProposalView {
+        id: id.into(),
+        severity: Severity::Warning,
+        title: "Scale out".into(),
+        rationale: "r".into(),
+        target: Target {
+            kind: TargetKind::Workload,
+            namespace: Some(ns.into()),
+            name: Some("web".into()),
+            workload_kind: Some(WorkloadKind::Deployment),
+        },
+        change: ChangeSpec::Scale {
+            namespace: ns.into(),
+            workload: WorkloadRef {
+                kind: WorkloadKind::Deployment,
+                name: "web".into(),
+            },
+            replicas: 3,
+        },
+        related_rule_id: None,
+        related_finding_key: None,
+        client: Some("claude-ai".into()),
+        status: "open".into(),
+        created_at_ms: 1,
+        updated_at_ms: updated,
+        closed_at_ms: None,
+        applied_as_proposed: None,
+        applied_change: None,
+        reject_reason: None,
+    }
+}
+
 impl Hooks for Fake {
     fn access(&self, session: &SessionView) -> SessionAccess {
         self.asked
@@ -474,11 +588,53 @@ impl Backend for Fake {
                     };
                     ReadOutput::WorkloadState(s)
                 }
+                ReadOp::ResourceUsage { window_days, .. } => {
+                    let points = if self.big_series.load(Ordering::SeqCst) {
+                        6000
+                    } else {
+                        2
+                    };
+                    ReadOutput::ResourceUsage(sample_usage(window_days, points))
+                }
+                ReadOp::RecommendResources { window_days, .. } => {
+                    ReadOutput::Recommendation(sample_recommendation(window_days))
+                }
                 _ => return Err(BackendError::unexpected()),
             })
         })
     }
 
+    fn proposals(&self, op: ProposalOp) -> BoxFuture<'_, Result<ProposalOutput, BackendError>> {
+        Box::pin(async move {
+            self.proposal_ops.lock().unwrap().push(op.clone());
+            match op {
+                ProposalOp::Propose { proposal, .. } => {
+                    if proposal.change.target_name() == "ghost" {
+                        return Err(BackendError::not_found("There is no workload team-a/ghost"));
+                    }
+                    Ok(ProposalOutput::Proposed {
+                        proposal_id: "p1".into(),
+                        created: true,
+                        read_only: false,
+                        warnings: vec!["The quota compute has no room for requests.memory".into()],
+                    })
+                }
+                ProposalOp::List { .. } => Ok(ProposalOutput::Proposals(vec![
+                    proposal_view("p0", "team-a", 10),
+                    proposal_view("p1", "team-b", 30),
+                    proposal_view("p2", "team-a", 20),
+                ])),
+                ProposalOp::Withdraw { proposal_id, .. } => match proposal_id.as_str() {
+                    "foreign" => Err(BackendError::new(
+                        "not_allowed",
+                        "This proposal belongs to another client",
+                    )),
+                    _ => Ok(ProposalOutput::Withdrawn),
+                },
+            }
+        })
+    }
+
     fn health(&self, op: HealthOp) -> BoxFuture<'_, Result<HealthOutput, BackendError>> {
         Box::pin(async move {
             self.health_ops.lock().unwrap().push(op.clone());
@@ -624,6 +780,7 @@ async fn exposes_exactly_the_tools() {
             "get_cluster_overview",
             "get_network",
             "get_quotas",
+            "get_resource_usage",
             "health_check",
             "list_annotations",
             "list_connections",
@@ -631,15 +788,22 @@ async fn exposes_exactly_the_tools() {
             "list_nodes",
             "list_pods",
             "list_projects",
+            "list_proposals",
             "list_workloads",
             "observe_workload",
+            "propose_change",
+            "recommend_resources",
             "remove_annotation",
+            "withdraw_proposal",
         ]
     );
     for t in &tools {
         let a = t.annotations.as_ref().unwrap();
-        // Замечания пишутся в Glafira (не в кластер): только они не read-only.
-        let writes = matches!(t.name.as_ref(), "annotate" | "remove_annotation");
+        // Замечания и предложения пишутся в Glafira (не в кластер): только они не read-only.
+        let writes = matches!(
+            t.name.as_ref(),
+            "annotate" | "remove_annotation" | "propose_change" | "withdraw_proposal"
+        );
         assert_eq!(
             (a.read_only_hint, a.destructive_hint, a.open_world_hint),
             (Some(!writes), Some(false), Some(false)),
@@ -1315,6 +1479,10 @@ impl Backend for Odd {
         })
     }
 
+    fn proposals(&self, op: ProposalOp) -> BoxFuture<'_, Result<ProposalOutput, BackendError>> {
+        self.fake.proposals(op)
+    }
+
     fn health(&self, op: HealthOp) -> BoxFuture<'_, Result<HealthOutput, BackendError>> {
         match (&self.mode, &op) {
             (Mode::Run(run), HealthOp::Check { .. }) => {
@@ -2171,3 +2339,171 @@ async fn long_annotation_list_is_cut_to_the_newest() {
         .collect();
     assert_eq!(ids, expected);
 }
+
+#[tokio::test]
+async fn resource_tools_check_the_window_and_the_scope() {
+    let fake = Fake::new();
+    let e = env(fake.clone()).await;
+    let c = client(&e.ep).await;
+    let w = json!({"kind": "Deployment", "name": "web"});
+    let v = ok(call(
+        &c,
+        "get_resource_usage",
+        json!({"connectionId": "c1", "namespace": "team-a", "workload": w}),
+    )
+    .await);
+    assert_eq!(v["windowDays"], 7, "окно по умолчанию — 7 дней");
+    assert_eq!(v["containers"][0]["stats"]["cpuP95"], "200m");
+    assert_eq!(
+        v["series"]["cpuCores"][0]["points"][1],
+        json!([3024, 0.123_456_789])
+    );
+    let v = ok(call(
+        &c,
+        "recommend_resources",
+        json!({"connectionId": "c1", "namespace": "team-a", "workload": w, "windowDays": 30}),
+    )
+    .await);
+    assert_eq!(v["windowDays"], 30);
+    assert_eq!(v["confidence"], "high");
+    assert_eq!(
+        v["containers"][0]["recommendedRequests"],
+        json!({"cpu": "230m", "memory": "352Mi"})
+    );
+    let msg = err(&call(
+        &c,
+        "recommend_resources",
+        json!({"connectionId": "c1", "namespace": "team-a", "workload": w, "windowDays": 3}),
+    )
+    .await);
+    assert!(
+        msg.starts_with("invalid_argument") && msg.contains("1, 7, 14 or 30"),
+        "{msg}"
+    );
+    let msg = err(&call(
+        &c,
+        "get_resource_usage",
+        json!({"connectionId": "c1", "namespace": "team-b", "workload": w}),
+    )
+    .await);
+    assert!(msg.starts_with("not_allowed"), "{msg}");
+    assert_eq!(
+        fake.ops
+            .lock()
+            .unwrap()
+            .iter()
+            .filter(|o| matches!(
+                o,
+                ReadOp::ResourceUsage { .. } | ReadOp::RecommendResources { .. }
+            ))
+            .count(),
+        2,
+        "окно и область проверены до бэкенда"
+    );
+}
+
+#[tokio::test]
+async fn usage_without_series_when_they_do_not_fit() {
+    let fake = Fake::new();
+    fake.big_series.store(true, Ordering::SeqCst);
+    let e = env(fake).await;
+    let c = client(&e.ep).await;
+    let v = ok(call(
+        &c,
+        "get_resource_usage",
+        json!({"connectionId": "c1", "namespace": "team-a", "workload": {"kind": "Deployment", "name": "web"}}),
+    )
+    .await);
+    assert!(v.get("series").is_none());
+    assert!(v["hint"].as_str().unwrap().contains("100 KB"));
+    assert_eq!(v["containers"][0]["name"], "app");
+}
+
+#[tokio::test]
+async fn proposals_are_checked_scoped_and_listed() {
+    let fake = Fake::new();
+    let e = env(fake.clone()).await;
+    let c = client(&e.ep).await;
+    let change = |ns: &str, name: &str| json!({"kind": "scale", "namespace": ns, "workload": {"kind": "Deployment", "name": name}, "replicas": 3});
+    let propose = |change: Value, rationale: &str| {
+        json!({"connectionId": "c1", "severity": "warning", "title": "Scale out",
+            "rationale": rationale, "change": change, "relatedRuleId": "workload.singleReplica"})
+    };
+    let v = ok(call(
+        &c,
+        "propose_change",
+        propose(change("team-a", "web"), "One replica is a risk"),
+    )
+    .await);
+    assert_eq!(
+        v,
+        json!({"proposalId": "p1", "created": true, "readOnly": false,
+        "warnings": ["The quota compute has no room for requests.memory"]})
+    );
+    let msg = err(&call(&c, "propose_change", propose(change("team-a", "web"), "  ")).await);
+    assert!(
+        msg.starts_with("invalid_argument") && msg.contains("rationale"),
+        "{msg}"
+    );
+    let msg = err(&call(&c, "propose_change", propose(change("team-b", "web"), "r")).await);
+    assert!(msg.starts_with("not_allowed"), "{msg}");
+    let msg = err(&call(
+        &c,
+        "propose_change",
+        propose(change("team-a", "ghost"), "r"),
+    )
+    .await);
+    assert!(msg.starts_with("not_found"), "{msg}");
+    let delete = json!({"kind": "deletePod", "namespace": "team-a", "pod": "web-1"});
+    let r = call(&c, "propose_change", propose(delete, "r")).await;
+    assert_eq!(r.is_error, Some(true), "удаление пода предложить нельзя");
+    assert_eq!(
+        fake.proposal_ops.lock().unwrap().len(),
+        2,
+        "до бэкенда дошли только верные"
+    );
+    // В журнал — без заголовка и обоснования.
+    let recent = e.audit.recent(20);
+    let entry = recent
+        .iter()
+        .find(|x| x["tool"] == "propose_change" && x["outcome"] == "ok")
+        .unwrap();
+    let logged = entry.to_string();
+    assert!(
+        !logged.contains("Scale out") && !logged.contains("One replica"),
+        "{logged}"
+    );
+    assert!(
+        logged.contains("scale") && logged.contains("web"),
+        "{logged}"
+    );
+
+    let v = ok(call(
+        &c,
+        "list_proposals",
+        json!({"connectionId": "c1", "status": "all"}),
+    )
+    .await);
+    let ids: Vec<&str> = v["items"]
+        .as_array()
+        .unwrap()
+        .iter()
+        .map(|i| i["id"].as_str().unwrap())
+        .collect();
+    assert_eq!(ids, ["p2", "p0"], "новые первыми, team-b не видно");
+    assert_eq!(v["items"][0]["change"]["kind"], "scale");
+    let v = ok(call(
+        &c,
+        "withdraw_proposal",
+        json!({"connectionId": "c1", "proposalId": "p2"}),
+    )
+    .await);
+    assert_eq!(v, json!({"withdrawn": true}));
+    let msg = err(&call(
+        &c,
+        "withdraw_proposal",
+        json!({"connectionId": "c1", "proposalId": "foreign"}),
+    )
+    .await);
+    assert!(msg.starts_with("not_allowed"), "{msg}");
+}
````

- [ ] **Step 3: Реализация.**

<!-- block: file t12-crates-glafira-mcp-kubernetes-src-proposals-rs-impl step=12.3 path=crates/glafira-mcp-kubernetes/src/proposals.rs part=impl -->
````rust
//! Предложения LLM (спец. §10.5, как в Kafka G3): виды изменения (их схема уходит LLM),
//! операции [`ProposalOp`] и ответы. Ни одна операция не меняет кластер: предложение пишется в
//! Glafira, применяет его человек заполненной формой приложения.

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

use glafira_mcp_core::SessionView;

use crate::health::{Severity, StatusFilter, Target};
use crate::model::WorkloadRef;

/// Длина `relatedFindingKey`.
pub const MAX_RELATED_KEY_CHARS: usize = 200;

/// Нет поля — `None`, `null` — `Some(None)`, строка — `Some(Some(…))`.
fn present<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(d).map(Some)
}

/// CPU and memory of requests or limits: quantity strings like "250m" or "512Mi"; null removes the value; omit a field to keep the current value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceValuesSpec {
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub cpu: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub memory: Option<Option<String>>,
}

/// New requests and limits of one container of the pod template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ContainerResourcesSpec {
    /// Container name from describe_workload.
    pub name: String,
    #[serde(default)]
    pub requests: ResourceValuesSpec,
    #[serde(default)]
    pub limits: ResourceValuesSpec,
}

/// The change an LLM proposes (`kind`): the same JSON shape Glafira stores and shows in its forms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ChangeSpec {
    /// Set requests and limits of containers in the pod template (not of a Job); limits must not be below requests. The user reviews the change and a server-side dry-run before applying.
    SetResources {
        namespace: String,
        workload: WorkloadRef,
        /// 1 to 50 containers.
        containers: Vec<ContainerResourcesSpec>,
    },
    /// Set the number of replicas of a Deployment, StatefulSet or DeploymentConfig, 0..=1000. Not for a workload scaled by an HPA: propose setHpaBounds instead.
    Scale {
        namespace: String,
        workload: WorkloadRef,
        replicas: i32,
    },
    /// Restart the pods of a workload with a rolling update (not a Job or CronJob).
    RolloutRestart {
        namespace: String,
        workload: WorkloadRef,
    },
    /// Set minReplicas (optional) and maxReplicas of a HorizontalPodAutoscaler: 1 <= min <= max <= 1000.
    #[serde(rename_all = "camelCase")]
    SetHpaBounds {
        namespace: String,
        hpa: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min_replicas: Option<i32>,
        max_replicas: i32,
    },
}

impl ChangeSpec {
    /// Имя вида — как `kind` в JSON (журнал).
    pub fn kind_name(&self) -> &'static str {
        match self {
            ChangeSpec::SetResources { .. } => "setResources",
            ChangeSpec::Scale { .. } => "scale",
            ChangeSpec::RolloutRestart { .. } => "rolloutRestart",
            ChangeSpec::SetHpaBounds { .. } => "setHpaBounds",
        }
    }

    pub fn namespace(&self) -> &str {
        match self {
            ChangeSpec::SetResources { namespace, .. }
            | ChangeSpec::Scale { namespace, .. }
            | ChangeSpec::RolloutRestart { namespace, .. }
            | ChangeSpec::SetHpaBounds { namespace, .. } => namespace,
        }
    }

    /// Имя объекта: workload или HPA (журнал).
    pub fn target_name(&self) -> &str {
        match self {
            ChangeSpec::SetResources { workload, .. }
            | ChangeSpec::Scale { workload, .. }
            | ChangeSpec::RolloutRestart { workload, .. } => &workload.name,
            ChangeSpec::SetHpaBounds { hpa, .. } => hpa,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewProposal {
    pub severity: Severity,
    pub title: String,
    pub rationale: String,
    pub change: ChangeSpec,
    pub related_rule_id: Option<String>,
    pub related_finding_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalOp {
    Propose {
        connection_id: String,
        session: SessionView,
        proposal: NewProposal,
    },
    List {
        connection_id: String,
        status: StatusFilter,
    },
    Withdraw {
        connection_id: String,
        session: SessionView,
        proposal_id: String,
    },
}

impl ProposalOp {
    pub fn connection_id(&self) -> &str {
        match self {
            ProposalOp::Propose { connection_id, .. }
            | ProposalOp::List { connection_id, .. }
            | ProposalOp::Withdraw { connection_id, .. } => connection_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProposalView {
    pub id: String,
    pub severity: Severity,
    pub title: String,
    pub rationale: String,
    pub target: Target,
    pub change: ChangeSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_rule_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_finding_key: Option<String>,
    /// Клиент, оставивший предложение; `None` — безымянный.
    pub client: Option<String>,
    /// open, applied (человек применил), rejected (человек отклонил), withdrawn (LLM отозвала).
    pub status: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<i64>,
    /// Только у applied: применено без правок (`true`) или с правками.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_as_proposed: Option<bool>,
    /// Что применено фактически, если оно отличается от предложенного.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_change: Option<ChangeSpec>,
    /// Только у rejected: комментарий человека, если он его оставил.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProposalOutput {
    Proposed {
        proposal_id: String,
        created: bool,
        /// Подключение только для чтения: применить сможет человек, сняв флажок.
        read_only: bool,
        /// Нарушения `LimitRange`, квоты, HPA и масштаб в 0 словами: предложение создано.
        warnings: Vec<String>,
    },
    Proposals(Vec<ProposalView>),
    Withdrawn,
}
````

<!-- block: file t12-crates-glafira-mcp-kubernetes-src-resources-rs step=12.3 path=crates/glafira-mcp-kubernetes/src/resources.rs -->
````rust
//! Анализ ресурсов для LLM (спец. §7, §10.4): статистика потребления workload'а за окно и
//! рекомендация `requests` / `limits`. Значения CPU и памяти — строками quantity (`250m`,
//! `512Mi`), их можно сразу предложить в `propose_change`. Адрес своего источника метрик из
//! профиля модель не получает.

use serde::Serialize;

use crate::model::Resources;

/// Окна анализа, дни (спец. §7.2).
pub const WINDOWS: [u32; 4] = [1, 7, 14, 30];
pub const WINDOW_DEFAULT: u32 = 7;

/// Источник метрик.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    /// thanos, prometheus или metricsServer.
    pub kind: String,
    /// auto (найден автопоиском) или profile (свой из настроек подключения).
    pub origin: String,
    /// Сервис или маршрут кластера; у своего источника — нет.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// Есть история; у metricsServer — только текущее значение.
    pub history: bool,
}

/// Источник, который не подошёл, и почему (forbidden, notFound, unavailable, timeout,
/// badResponse, tokenMissing).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RejectedSource {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    pub reason: String,
}

/// Статистика контейнера за окно: «худший под».
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageStats {
    pub cpu_p50: Option<String>,
    pub cpu_p95: Option<String>,
    pub cpu_p99: Option<String>,
    pub cpu_max: Option<String>,
    pub memory_p95: Option<String>,
    pub memory_p99: Option<String>,
    pub memory_max: Option<String>,
    pub throttling_mean_percent: Option<f64>,
    pub throttling_p95_percent: Option<f64>,
    /// `None` — нет kube-state-metrics.
    pub oom_count: Option<u32>,
    pub last_oom_seconds_ago: Option<i64>,
    pub restarts: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerUsage {
    pub name: String,
    pub init: bool,
    pub requests: Resources,
    pub limits: Resources,
    /// Значения работающего пода от `LimitRange` по умолчанию (в шаблоне их нет):
    /// cpuRequest, cpuLimit, memoryRequest, memoryLimit.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub limit_range_defaults: Vec<String>,
    pub stats: UsageStats,
}

/// Ряд по контейнеру: `[секунд назад, значение]`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerSeries {
    pub container: String,
    pub points: Vec<(i64, f64)>,
}

/// Рестарты или OOM контейнера за шаг.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Marker {
    pub container: String,
    pub seconds_ago: i64,
    pub count: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageSeries {
    pub step_seconds: u64,
    pub cpu_cores: Vec<ContainerSeries>,
    pub memory_mib: Vec<ContainerSeries>,
    pub restarts: Vec<Marker>,
    pub ooms: Vec<Marker>,
}

/// Ответ `get_resource_usage`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceUsage {
    pub window_days: u32,
    pub source: Source,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rejected: Vec<RejectedSource>,
    pub coverage_percent: Option<f64>,
    pub data_days: Option<f64>,
    pub kube_state_metrics: bool,
    pub replicas: Option<i32>,
    pub containers: Vec<ContainerUsage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series: Option<UsageSeries>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerRecommendation {
    pub name: String,
    pub current_requests: Resources,
    pub current_limits: Resources,
    /// `None` — данных о контейнере за окно нет.
    pub recommended_requests: Option<Resources>,
    pub recommended_limits: Option<Resources>,
    /// Пометки словами: троттлинг, OOM, CPU limit не задан, границы LimitRange, JVM и Go.
    pub notes: Vec<String>,
}

/// Разница суммарных requests и limits с учётом реплик (рекомендовано минус сейчас).
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Totals {
    pub requests_cpu_cores: Option<f64>,
    pub requests_memory_mib: Option<f64>,
    pub limits_cpu_cores: Option<f64>,
    pub limits_memory_mib: Option<f64>,
}

/// Ответ `recommend_resources`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Recommendation {
    pub window_days: u32,
    pub source: Source,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rejected: Vec<RejectedSource>,
    /// high, medium или low.
    pub confidence: String,
    pub coverage_percent: Option<f64>,
    pub data_days: Option<f64>,
    pub replicas: Option<i32>,
    pub containers: Vec<ContainerRecommendation>,
    /// Ограничения проекта и кластера словами: запас квоты, allocatable ноды.
    pub violations: Vec<String>,
    /// HPA по utilization: смена request сдвигает порог масштабирования.
    pub hpa: Vec<String>,
    pub totals: Totals,
}
````

<!-- block: file t12-crates-glafira-mcp-kubernetes-src-server-resource-tools-rs step=12.3 path=crates/glafira-mcp-kubernetes/src/server/resource_tools.rs -->
````rust
//! Инструменты анализа ресурсов (спец. §7, §10.4): статистика потребления workload'а и
//! рекомендация `requests` / `limits`. Кластер только читается: метрики — у источника, который
//! выбрала Glafira; без источника истории — текущие значения `metrics.k8s.io` с
//! `confidence: "low"`, без них — `metrics_unavailable` с причинами.

use glafira_mcp_core::fit::MAX_RESPONSE_BYTES;
use glafira_mcp_core::{BackendError, to_value};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::KubeServer;
use crate::model::{ReadOp, ReadOutput, WorkloadRef};
use crate::resources::{WINDOW_DEFAULT, WINDOWS};

/// Ряды не поместились в предел ответа.
const SERIES_OMITTED_HINT: &str =
    "The series were omitted to fit the 100 KB response limit; ask for a shorter window";

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Project (namespace) from list_projects.
    pub namespace: String,
    pub workload: WorkloadRef,
    /// Analysis window in days: 1, 7, 14 or 30. Default: 7.
    #[serde(default)]
    pub window_days: Option<u32>,
}

fn window(p: &ResourceParams) -> Result<u32, BackendError> {
    let days = p.window_days.unwrap_or(WINDOW_DEFAULT);
    if WINDOWS.contains(&days) {
        return Ok(days);
    }
    Err(BackendError::invalid_argument(format!(
        "windowDays must be 1, 7, 14 or 30, got {days}"
    )))
}

impl KubeServer {
    async fn usage(&self, p: &ResourceParams) -> Result<Value, BackendError> {
        let op = ReadOp::ResourceUsage {
            connection_id: p.connection_id.clone(),
            namespace: p.namespace.clone(),
            workload: p.workload.clone(),
            window_days: window(p)?,
        };
        let ReadOutput::ResourceUsage(mut u) = self.read(op).await? else {
            return Err(BackendError::unexpected());
        };
        // Ряды — самая большая часть ответа: не поместились — без них.
        let size = serde_json::to_vec(&u).map_or(0, |v| v.len());
        if size > MAX_RESPONSE_BYTES {
            u.series = None;
            let mut v = to_value(&u)?;
            v["hint"] = json!(SERIES_OMITTED_HINT);
            return Ok(v);
        }
        to_value(&u)
    }

    async fn recommendation(&self, p: &ResourceParams) -> Result<Value, BackendError> {
        let op = ReadOp::RecommendResources {
            connection_id: p.connection_id.clone(),
            namespace: p.namespace.clone(),
            workload: p.workload.clone(),
            window_days: window(p)?,
        };
        match self.read(op).await? {
            ReadOutput::Recommendation(r) => to_value(&r),
            _ => Err(BackendError::unexpected()),
        }
    }
}

#[tool_router(router = resource_router, vis = "pub(crate)")]
impl KubeServer {
    #[tool(
        description = "Resource usage of a workload over a window (1, 7, 14 or 30 days) for each container: current requests and limits (with values set by LimitRange defaults), CPU p50/p95/p99/max and memory p95/p99/max of the worst pod as quantities, CPU throttling, OOM kills and restarts (when kube-state-metrics is available), series for charts (cpuCores, memoryMib as [secondsAgo, value]), the metrics source, the coverage of the window and the sources that were rejected. Without a history source only the current values of metrics.k8s.io are returned.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn get_resource_usage(
        &self,
        Parameters(p): Parameters<ResourceParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = serde_json::to_value(&p).unwrap_or(Value::Null);
        Ok(self
            .session
            .call(
                &ctx,
                "get_resource_usage",
                args,
                Some(&p.connection_id),
                self.usage(&p),
            )
            .await)
    }

    #[tool(
        description = "Recommended requests and limits for each container of a workload from its actual usage over a window (1, 7, 14 or 30 days), with notes (CPU throttled by the limit, OOM kills, no CPU limit, LimitRange bounds, JVM or Go memory settings), project and cluster constraints (quota room with replicas and rollout surge, the largest node), the HPA target shift and the change of total requests and limits. confidence is high, medium or low: say so to the user when it is low. Start a resources review with this tool; propose the values with propose_change (kind setResources).",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn recommend_resources(
        &self,
        Parameters(p): Parameters<ResourceParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = serde_json::to_value(&p).unwrap_or(Value::Null);
        Ok(self
            .session
            .call(
                &ctx,
                "recommend_resources",
                args,
                Some(&p.connection_id),
                self.recommendation(&p),
            )
            .await)
    }
}
````

<!-- block: file t12-src-tauri-src-mcp-proposals-rs step=12.3 path=src-tauri/src/mcp/proposals.rs -->
````rust
//! Предложения LLM в приложении (спец. §10.5): проверка при создании против кластера — только
//! чтением (`change::facts`: workload, HPA и контейнеры есть, quantity разбираются, `limits ≥
//! requests`; `scale` workload'а под HPA — `invalid_argument` с советом `setHpaBounds`;
//! нарушения `LimitRange` и квоты — в `warnings`), запись в файл Glafira. Server-side dry-run
//! при создании не выполняется: у шлюза нет запросов записи даже с `dryRun`.

use glafira_mcp_core::BackendError;
use glafira_mcp_kubernetes::health as mh;
use glafira_mcp_kubernetes::proposals as mp;

use super::backend::{AppBackend, health_to_backend, to_backend};
use super::map::reshape;
use crate::change::facts::{Warning, check, read_facts};
use crate::change::{Change, validate_form};
use crate::commands::blocking;
use crate::health::proposals::{Proposal, ProposalStatus};
use crate::health::service::now_ms;
use crate::health::store::{NewProposal, Proposed, Withdrawn};

/// Ошибка проверки изменения — `invalid_argument` с английским текстом (ключи `errors.change.*`
/// и `errors.k8s.badName` несут только то, что прислала модель); объекта нет — `not_found`.
fn invalid(e: crate::error::AppError) -> BackendError {
    match e.code {
        crate::error::ErrorCode::InvalidInput => BackendError::invalid_argument(e.message),
        _ => to_backend(e),
    }
}

fn view(p: Proposal) -> Option<mp::ProposalView> {
    Some(mp::ProposalView {
        id: p.id,
        severity: reshape(&p.severity)?,
        title: p.title,
        rationale: p.rationale,
        target: reshape(&p.target)?,
        // Удаление пода модель предложить не может: такой записи в её ответе нет.
        change: reshape(&p.change)?,
        related_rule_id: p.related_rule_id,
        related_finding_key: p.related_finding_key,
        client: p.client,
        status: p.status.as_str().to_string(),
        created_at_ms: p.created_at_ms,
        updated_at_ms: p.updated_at_ms,
        closed_at_ms: p.closed_at_ms,
        applied_as_proposed: p.applied_as_proposed,
        applied_change: p.applied_change.as_ref().and_then(reshape),
        reject_reason: p.reject_reason,
    })
}

impl AppBackend {
    pub(super) async fn proposals_impl(
        &self,
        op: mp::ProposalOp,
    ) -> Result<mp::ProposalOutput, BackendError> {
        // Поколение — до проверки доступа: запись в подключение, удалённое за это время, отклоняется.
        let generation = self.state.health.generation(op.connection_id());
        let profile = self.allowed(op.connection_id())?;
        let st = self.state.clone();
        match op {
            mp::ProposalOp::Propose {
                connection_id,
                session,
                proposal,
            } => {
                super::backend::check_note_texts(
                    proposal.related_rule_id.as_deref(),
                    &proposal.title,
                )?;
                let change: Change = reshape(&proposal.change)
                    .ok_or_else(|| BackendError::invalid_argument("change cannot be read"))?;
                let change = change.trimmed();
                Self::in_scope(&profile, change.namespace())?;
                validate_form(&change).map_err(invalid)?;
                let ns = change.namespace().to_string();
                let c2 = change.clone();
                let facts = Box::pin(st.read_in(&connection_id, &ns, |c| async move {
                    read_facts(&c.reader(), &c2).await
                }))
                .await
                .map_err(to_backend)?;
                let warnings = check(&change, &facts).map_err(invalid)?;
                if let (Change::Scale { .. }, Some(Warning::HpaManaged { hpa })) = (
                    &change,
                    warnings
                        .iter()
                        .find(|w| matches!(w, Warning::HpaManaged { .. })),
                ) {
                    return Err(BackendError::invalid_argument(format!(
                        "HPA {hpa} scales this workload and would overwrite the replicas: propose setHpaBounds instead"
                    )));
                }
                let new = NewProposal {
                    severity: reshape(&proposal.severity).ok_or_else(BackendError::unexpected)?,
                    title: proposal.title,
                    rationale: proposal.rationale,
                    change,
                    related_rule_id: proposal.related_rule_id,
                    related_finding_key: proposal.related_finding_key,
                    author: self.author(&session),
                };
                let id = connection_id.clone();
                let st2 = st.clone();
                let r = blocking(move || {
                    st2.health
                        .write(&id, generation, |s| s.propose(&id, new, now_ms()))
                })
                .await
                .map_err(health_to_backend)?;
                let (proposal_id, created) = match r {
                    Proposed::Created(id) => (id, true),
                    Proposed::Updated(id) => (id, false),
                    Proposed::LimitReached => {
                        return Err(BackendError::new(
                            "limit_reached",
                            "This connection already has 50 open proposals; ask the user to apply or reject some in Glafira",
                        ));
                    }
                };
                st.health.changed(&connection_id);
                Ok(mp::ProposalOutput::Proposed {
                    proposal_id,
                    created,
                    read_only: profile.read_only,
                    warnings: warnings.iter().map(super::resources::warning).collect(),
                })
            }
            mp::ProposalOp::List {
                connection_id,
                status,
            } => {
                let f = blocking(move || st.health.store.load(&connection_id, now_ms()))
                    .await
                    .map_err(health_to_backend)?;
                Ok(mp::ProposalOutput::Proposals(
                    f.proposals
                        .into_iter()
                        .filter(|p| match status {
                            mh::StatusFilter::Open => p.status == ProposalStatus::Open,
                            mh::StatusFilter::Closed => p.status != ProposalStatus::Open,
                            mh::StatusFilter::All => true,
                        })
                        .filter_map(view)
                        .collect(),
                ))
            }
            mp::ProposalOp::Withdraw {
                connection_id,
                session,
                proposal_id,
            } => {
                let author = self.author(&session);
                let id = connection_id.clone();
                let st2 = st.clone();
                let r = blocking(move || {
                    st2.health.write(&id, generation, |s| {
                        s.withdraw_own(&id, &proposal_id, &author, now_ms())
                    })
                })
                .await
                .map_err(health_to_backend)?;
                match r {
                    Withdrawn::Withdrawn => {
                        st.health.changed(&connection_id);
                        Ok(mp::ProposalOutput::Withdrawn)
                    }
                    Withdrawn::NotFound => Err(BackendError::not_found(
                        "There is no such proposal: it was deleted or has expired",
                    )),
                    Withdrawn::NotOwner => Err(BackendError::new(
                        "not_allowed",
                        "This proposal belongs to another client",
                    )),
                    Withdrawn::Closed(s) => Err(BackendError::invalid_argument(format!(
                        "The proposal is already {}",
                        s.as_str()
                    ))),
                }
            }
        }
    }
}
````

<!-- block: file t12-src-tauri-src-mcp-resources-rs-impl step=12.3 path=src-tauri/src/mcp/resources.rs part=impl -->
````rust
//! Анализ ресурсов для LLM (спец. §7, §10.4): статистика и рекомендация приложения → ответы
//! крейта. Значения — строками quantity, пометки и ограничения — английскими фразами. Адрес
//! своего источника метрик из профиля модель не получает: у источника из профиля нет `location`,
//! у ошибки `metrics_unavailable` — только виды источников и коды причин.

use glafira_mcp_core::BackendError;
use glafira_mcp_kubernetes::model::Resources;
use glafira_mcp_kubernetes::resources as mr;

use crate::change::facts::Warning;
use crate::k8s::quantity::{format_cpu, format_memory};
use crate::metrics::prometheus::{SourceInfo, SourceOrigin};
use crate::metrics::select::SourceStatus;
use crate::resources::current::Amounts;
use crate::resources::recommend::{Bound, Field, Note, Recommendation, Violation};
use crate::resources::usage::WorkloadUsage;

const MIB: f64 = 1024.0 * 1024.0;

fn code<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default()
}

fn round(v: f64, digits: i32) -> f64 {
    let k = 10f64.powi(digits);
    (v * k).round() / k
}

fn quantities(a: Amounts) -> Resources {
    Resources {
        cpu: a.cpu.map(format_cpu),
        memory: a.memory.map(format_memory),
    }
}

fn source(s: &SourceInfo) -> mr::Source {
    mr::Source {
        kind: code(&s.kind),
        origin: code(&s.origin),
        location: (s.origin == SourceOrigin::Auto).then(|| s.location.clone()),
        history: s.history,
    }
}

fn rejected(status: &SourceStatus) -> Vec<mr::RejectedSource> {
    status
        .rejected
        .iter()
        .map(|r| mr::RejectedSource {
            kind: code(&r.source.kind),
            location: (r.source.origin == SourceOrigin::Auto).then(|| r.source.location.clone()),
            reason: code(&r.reason),
        })
        .collect()
}

/// Ни одного источника: виды источников и коды причин, без адресов из профиля.
pub fn metrics_unavailable(status: &SourceStatus) -> BackendError {
    let reasons: Vec<String> = rejected(status)
        .into_iter()
        .map(|r| match r.location {
            Some(l) => format!("{} {l}: {}", r.kind, r.reason),
            None => format!(
                "{} (own source from the connection settings): {}",
                r.kind, r.reason
            ),
        })
        .collect();
    let tail = match reasons.is_empty() {
        true => String::new(),
        false => format!(" — {}", reasons.join("; ")),
    };
    BackendError::new(
        "metrics_unavailable",
        format!(
            "No metrics source is available{tail}. Ask the user to set a metrics source for the connection in Glafira."
        ),
    )
}

/// `get_resource_usage`: время рядов — секунды до момента ответа.
pub fn usage(u: &WorkloadUsage, now: i64) -> mr::ResourceUsage {
    let fields = |c: &crate::resources::current::ContainerCurrent| {
        let mut out = Vec::new();
        for (name, v) in [
            ("cpuRequest", c.limit_range_requests.cpu),
            ("cpuLimit", c.limit_range_limits.cpu),
            ("memoryRequest", c.limit_range_requests.memory),
            ("memoryLimit", c.limit_range_limits.memory),
        ] {
            if v.is_some() {
                out.push(name.to_string());
            }
        }
        out
    };
    let percent = |v: Option<f64>| v.map(|x| round(x * 100.0, 1));
    mr::ResourceUsage {
        window_days: u.window_days,
        source: source(&u.source),
        rejected: rejected(&u.source_status),
        coverage_percent: percent(u.coverage),
        data_days: u.data_seconds.map(|s| round(s as f64 / 86_400.0, 1)),
        kube_state_metrics: u.owner_metrics,
        replicas: u.replicas,
        containers: u
            .containers
            .iter()
            .map(|c| {
                let s = &c.stats;
                mr::ContainerUsage {
                    name: c.current.name.clone(),
                    init: c.current.init,
                    requests: quantities(c.current.requests),
                    limits: quantities(c.current.limits),
                    limit_range_defaults: fields(&c.current),
                    stats: mr::UsageStats {
                        cpu_p50: s.cpu.p50.map(format_cpu),
                        cpu_p95: s.cpu.p95.map(format_cpu),
                        cpu_p99: s.cpu.p99.map(format_cpu),
                        cpu_max: s.cpu.max.map(format_cpu),
                        memory_p95: s.memory.p95.map(format_memory),
                        memory_p99: s.memory.p99.map(format_memory),
                        memory_max: s.memory.max.map(format_memory),
                        throttling_mean_percent: percent(s.throttling.mean),
                        throttling_p95_percent: percent(s.throttling.p95),
                        oom_count: s.oom_count,
                        last_oom_seconds_ago: s.oom_last.map(|t| now - t),
                        restarts: s.restarts,
                    },
                }
            })
            .collect(),
        series: u.charts.as_ref().map(|ch| {
            let series =
                |list: &[crate::resources::usage::ChartSeries], scale: f64, digits: i32| {
                    list.iter()
                        .map(|s| mr::ContainerSeries {
                            container: s.container.clone(),
                            points: s
                                .points
                                .iter()
                                .map(|(t, v)| (now - t, round(v / scale, digits)))
                                .collect(),
                        })
                        .collect()
                };
            let markers = |list: &[crate::resources::usage::Marker]| {
                list.iter()
                    .map(|m| mr::Marker {
                        container: m.container.clone(),
                        seconds_ago: now - m.time,
                        count: m.count,
                    })
                    .collect()
            };
            mr::UsageSeries {
                step_seconds: ch.step_seconds,
                cpu_cores: series(&ch.cpu, 1.0, 3),
                memory_mib: series(&ch.memory, MIB, 1),
                restarts: markers(&ch.restarts),
                ooms: markers(&ch.ooms),
            }
        }),
    }
}

fn field(f: Field) -> &'static str {
    match f {
        Field::CpuRequest => "CPU request",
        Field::CpuLimit => "CPU limit",
        Field::MemoryRequest => "memory request",
        Field::MemoryLimit => "memory limit",
    }
}

fn value(f: Field, v: f64) -> String {
    match f {
        Field::CpuRequest | Field::CpuLimit => format_cpu(v),
        Field::MemoryRequest | Field::MemoryLimit => format_memory(v),
    }
}

/// Количество ресурса словами: ядра или память.
fn amount(resource: &str, v: f64) -> String {
    if resource.ends_with("cpu") {
        format_cpu(v)
    } else {
        format_memory(v)
    }
}

pub fn note(n: &Note) -> String {
    match n {
        Note::CpuLimitNotSet => {
            "No CPU limit is set and LimitRange does not require one: there is no throttling, so no limit is added".into()
        }
        Note::CpuThrottled { p95 } => format!(
            "Throttled in {:.0}% of CPU periods (p95): the observed CPU is capped by the limit, so the limit is raised at least 1.5 times",
            p95 * 100.0
        ),
        Note::OomKilled { count } => format!(
            "OOMKilled {count} times in the window: the observed memory is cut by the limit, so the limit is raised at least 1.5 times"
        ),
        Note::NoData => "No usage data for this container in the window: no recommendation".into(),
        Note::LimitRange { field: f, bound } => format!(
            "The {} was pulled to the LimitRange {}",
            field(*f),
            match bound {
                Bound::Min => "minimum",
                Bound::Max => "maximum",
                Bound::Ratio => "maxLimitRequestRatio",
            }
        ),
        Note::LimitRangeDefault { field: f } => format!(
            "The current {} comes from a LimitRange default, not from the pod template",
            field(*f)
        ),
        Note::Java => {
            "JVM settings in the environment (JAVA_OPTS, JAVA_TOOL_OPTIONS or JDK_JAVA_OPTIONS): check -Xmx or MaxRAMPercentage together with the memory limit".into()
        }
        Note::Go => "GOMEMLIMIT in the environment: check it together with the memory limit".into(),
    }
}

pub fn violation(v: &Violation) -> String {
    match v {
        Violation::Quota {
            quota,
            resource,
            needed,
            free,
        } => format!(
            "ResourceQuota {quota}: {resource} needs {} more with all replicas and the rollout surge, only {} is free",
            amount(resource, *needed),
            amount(resource, free.max(0.0))
        ),
        Violation::NodeAllocatable {
            container,
            resource,
            request,
            allocatable,
        } => format!(
            "Container {container}: the {resource} request {} is above the allocatable {} of the largest node",
            amount(resource, *request),
            amount(resource, *allocatable)
        ),
    }
}

/// Предупреждение к предложению.
pub fn warning(w: &Warning) -> String {
    match w {
        Warning::HpaManaged { hpa } => {
            format!("HPA {hpa} scales this workload and will overwrite the number of replicas")
        }
        Warning::ScaleToZero => "Scaling to 0 removes all pods of the workload".into(),
        Warning::LimitRange {
            container,
            field: f,
            bound,
            limit,
        } => format!(
            "Container {container}: the {} is {} the LimitRange {} {}: new pods will be rejected",
            field(*f),
            match bound {
                Bound::Max => "above",
                _ => "below",
            },
            match bound {
                Bound::Max => "maximum",
                _ => "minimum",
            },
            value(*f, *limit)
        ),
        Warning::Quota {
            quota,
            resource,
            needed,
            free,
        } => format!(
            "ResourceQuota {quota}: {resource} needs {} more with all replicas and the rollout surge, only {} is free",
            amount(resource, *needed),
            amount(resource, free.max(0.0))
        ),
        Warning::Orphan => "The pod has no controller: nothing will recreate it".into(),
    }
}

/// `recommend_resources`.
pub fn recommendation(r: &Recommendation, status: &SourceStatus) -> mr::Recommendation {
    mr::Recommendation {
        window_days: r.window_days,
        source: source(&r.source),
        rejected: rejected(status),
        confidence: code(&r.confidence),
        coverage_percent: r.coverage.map(|c| round(c * 100.0, 1)),
        data_days: r.data_seconds.map(|s| round(s as f64 / 86_400.0, 1)),
        replicas: r.replicas,
        containers: r
            .containers
            .iter()
            .map(|c| mr::ContainerRecommendation {
                name: c.name.clone(),
                current_requests: quantities(c.current.requests),
                current_limits: quantities(c.current.limits),
                recommended_requests: c.recommended.as_ref().map(|v| quantities(v.requests)),
                recommended_limits: c.recommended.as_ref().map(|v| quantities(v.limits)),
                notes: c.notes.iter().map(note).collect(),
            })
            .collect(),
        violations: r.violations.iter().map(violation).collect(),
        hpa: r
            .hpa
            .iter()
            .map(|h| {
                format!(
                    "HPA {} scales on {} utilization of {}%; with the recommended requests the same scaling needs a target of {}%",
                    h.hpa, h.resource, h.target, h.equivalent
                )
            })
            .collect(),
        totals: mr::Totals {
            requests_cpu_cores: r.totals.requests.cpu.map(|v| round(v, 3)),
            requests_memory_mib: r.totals.requests.memory.map(|v| round(v / MIB, 1)),
            limits_cpu_cores: r.totals.limits.cpu.map(|v| round(v, 3)),
            limits_memory_mib: r.totals.limits.memory.map(|v| round(v / MIB, 1)),
        },
    }
}
````

<!-- block: patch t12-src step=12.3 -->
````diff
diff --git a/PORTED.md b/PORTED.md
index 5fab08e..ed5d801 100644
--- a/PORTED.md
+++ b/PORTED.md
@@ -190,3 +190,5 @@
 | `docs/releases-repo/SUPPORT.md` | `docs/releases-repo/SUPPORT.md` | `c5446fc` | адаптирован: название продукта |
 | `src-tauri/src/guard.rs` | `src-tauri/src/guard.rs` | `8bbd0bd` | адаптирован: профиль подключения Kubernetes, ввод имени пода без контроллера |
 | `src-tauri/src/health/proposals.rs` | `src-tauri/src/health/proposals.rs` | `8bbd0bd` | адаптирован: модель, статусы и сроки предложения; вид изменения — `change::Change` (§9) |
+| `crates/glafira-mcp-kubernetes/src/proposals.rs` | `crates/glafira-mcp/src/proposals.rs` | `8bbd0bd` | адаптирован: виды изменения Kubernetes (§9), предупреждения проверки |
+| `crates/glafira-mcp-kubernetes/src/server/proposal_tools.rs` | `crates/glafira-mcp/src/server/proposal_tools.rs` | `8bbd0bd` | адаптирован: виды изменения Kubernetes, область LLM, предупреждения |
diff --git a/crates/glafira-mcp-kubernetes/src/lib.rs b/crates/glafira-mcp-kubernetes/src/lib.rs
index b3ebd93..169af3b 100644
--- a/crates/glafira-mcp-kubernetes/src/lib.rs
+++ b/crates/glafira-mcp-kubernetes/src/lib.rs
@@ -1,14 +1,16 @@
-//! MCP-шлюз Glafira for Kubernetes (спец. §10): инструменты только для чтения, типы ответов для
-//! LLM, маскировка и трейт [`Backend`]. Транспорт, сессии, лимиты и журнал — в `glafira-mcp-core`.
+//! MCP-шлюз Glafira for Kubernetes (спец. §10): инструменты, типы ответов для LLM, маскировка и
+//! трейт [`Backend`]. Транспорт, сессии, лимиты и журнал — в `glafira-mcp-core`.
 //!
 //! Крейт не зависит от `kube`, `k8s-openapi`, `reqwest` и крейта приложения: данные он получает
-//! через [`Backend`], изменяющих операций в [`ReadOp`] нет. Замечания LLM ([`health`]) пишутся
-//! в Glafira, а не в кластер.
+//! через [`Backend`], изменяющих операций в [`ReadOp`] нет. Замечания ([`health`]) и предложения
+//! ([`proposals`]) LLM пишутся в Glafira, а не в кластер: изменение применяет человек.
 
 pub mod health;
 pub mod mask;
 pub mod model;
 pub mod observe;
+pub mod proposals;
+pub mod resources;
 pub mod server;
 
 pub use model::{Backend, ReadOp, ReadOutput, Scope};
diff --git a/crates/glafira-mcp-kubernetes/src/model.rs b/crates/glafira-mcp-kubernetes/src/model.rs
index 625c858..c0ed867 100644
--- a/crates/glafira-mcp-kubernetes/src/model.rs
+++ b/crates/glafira-mcp-kubernetes/src/model.rs
@@ -120,6 +120,20 @@ pub enum ReadOp {
         namespace: String,
         workload: WorkloadRef,
     },
+    /// Статистика потребления workload'а за окно (`get_resource_usage`).
+    ResourceUsage {
+        connection_id: String,
+        namespace: String,
+        workload: WorkloadRef,
+        window_days: u32,
+    },
+    /// Рекомендация requests и limits (`recommend_resources`).
+    RecommendResources {
+        connection_id: String,
+        namespace: String,
+        workload: WorkloadRef,
+        window_days: u32,
+    },
 }
 
 impl ReadOp {
@@ -137,7 +151,9 @@ impl ReadOp {
             | ReadOp::ListEvents { connection_id, .. }
             | ReadOp::Quotas { connection_id, .. }
             | ReadOp::Network { connection_id, .. }
-            | ReadOp::WorkloadState { connection_id, .. } => Some(connection_id),
+            | ReadOp::WorkloadState { connection_id, .. }
+            | ReadOp::ResourceUsage { connection_id, .. }
+            | ReadOp::RecommendResources { connection_id, .. } => Some(connection_id),
         }
     }
 
@@ -151,7 +167,9 @@ impl ReadOp {
             | ReadOp::ListEvents { namespace, .. }
             | ReadOp::Quotas { namespace, .. }
             | ReadOp::Network { namespace, .. }
-            | ReadOp::WorkloadState { namespace, .. } => Some(namespace),
+            | ReadOp::WorkloadState { namespace, .. }
+            | ReadOp::ResourceUsage { namespace, .. }
+            | ReadOp::RecommendResources { namespace, .. } => Some(namespace),
             _ => None,
         }
     }
@@ -172,6 +190,8 @@ pub enum ReadOutput {
     Quotas(Quotas),
     Network(Network),
     WorkloadState(WorkloadState),
+    ResourceUsage(crate::resources::ResourceUsage),
+    Recommendation(crate::resources::Recommendation),
 }
 
 /// Источник данных шлюза. Реализация приложения проверяет `llmAccess` подключения и в `read`,
@@ -182,6 +202,12 @@ pub trait Backend: Hooks {
     fn read(&self, op: ReadOp) -> BoxFuture<'_, Result<ReadOutput, BackendError>>;
     /// Проверка кластера и замечания: кластер только читается, замечания пишутся в Glafira.
     fn health(&self, op: HealthOp) -> BoxFuture<'_, Result<HealthOutput, BackendError>>;
+    /// Предложения изменений: кластер только читается (проверка предложения), предложение
+    /// пишется в Glafira, применяет его человек.
+    fn proposals(
+        &self,
+        op: crate::proposals::ProposalOp,
+    ) -> BoxFuture<'_, Result<crate::proposals::ProposalOutput, BackendError>>;
 }
 
 /// Часть ответа: данные или причина, по которой их нет (права, ошибка чтения).
diff --git a/crates/glafira-mcp-kubernetes/src/server/mod.rs b/crates/glafira-mcp-kubernetes/src/server/mod.rs
index 20a841b..4864654 100644
--- a/crates/glafira-mcp-kubernetes/src/server/mod.rs
+++ b/crates/glafira-mcp-kubernetes/src/server/mod.rs
@@ -1,6 +1,6 @@
-//! Обработчик MCP одной сессии (спец. §10): инструменты, инструкции сервера, проверка области
-//! доступа LLM перед каждой операцией. Транспорт, сессии, право, лимиты и журнал —
-//! `glafira_mcp_core::server`.
+//! Обработчик MCP одной сессии (спец. §10): инструменты чтения, проверки, ресурсов и
+//! предложений, инструкции сервера, проверка области доступа LLM перед каждой операцией.
+//! Транспорт, сессии, право, лимиты и журнал — `glafira_mcp_core::server`.
 
 use std::sync::Arc;
 use std::time::Duration;
@@ -18,18 +18,26 @@ use crate::mask::Masker;
 use crate::model::{Backend, ReadOp, ReadOutput, Scope, not_allowed};
 
 mod health_tools;
+mod proposal_tools;
+mod resource_tools;
 mod tools;
 
 pub const INSTRUCTIONS: &str = "\
 Glafira is a gateway to Kubernetes clusters (including OpenShift and Platform V DropApp) that the \
 user explicitly allowed. It reads metadata: the cluster overview, nodes, projects (namespaces), \
-workloads, pods, events, quotas and network objects. Nothing in the clusters can be changed through \
-it: when a change is needed, describe it in the chat — the user applies changes in Glafira \
-themselves. Call list_connections first to get connection ids, then list_projects: you see only \
-the projects the user allowed for LLM access.
+workloads, pods, events, quotas, network objects and the resource usage from the cluster metrics. \
+Nothing in the clusters can be changed through it: propose a change with propose_change — the user \
+sees it next to the object in Glafira, opens it as a pre-filled form and applies or rejects it \
+themselves. Do not wait for the result in the same call; check the user's decision later with \
+list_proposals. If a proposal comes back with readOnly: true, ask the user to lift read-only for the \
+connection in Glafira instead of looking for a workaround. Deleting objects and changes other than \
+the four kinds of propose_change can only be described in the chat. Call list_connections first to \
+get connection ids, then list_projects: you see only the projects the user allowed for LLM access.
 
 To review a cluster or a project, start with health_check: it runs Glafira's deterministic rules. \
-Take the facts from it, then explain them, set priorities and find connections between them. \
+For requests and limits, start with recommend_resources: it computes them from the actual usage. \
+Take the facts from these tools, then explain them, set priorities and find connections between \
+them. When a recommendation has confidence: \"low\", tell the user and do not present it as precise. \
 Findings with ignored: true were dismissed by the user. You can leave notes with annotate: the \
 user sees them next to the objects in Glafira (the clusters are not changed). Do not repeat \
 findings as notes; add your conclusions and link them with relatedRuleId.
@@ -109,7 +117,10 @@ impl KubeServer {
             session,
             backend,
             options,
-            tool_router: Self::tool_router() + Self::health_router(),
+            tool_router: Self::tool_router()
+                + Self::health_router()
+                + Self::resource_router()
+                + Self::proposal_router(),
         }
     }
 
diff --git a/src-tauri/src/mcp/backend.rs b/src-tauri/src/mcp/backend.rs
index ddf5a98..af678f3 100644
--- a/src-tauri/src/mcp/backend.rs
+++ b/src-tauri/src/mcp/backend.rs
@@ -44,7 +44,7 @@ pub type OnChange = Arc<dyn Fn(bool) + Send + Sync>;
 const POD_EVENTS: usize = 20;
 
 pub struct AppBackend {
-    state: AppState,
+    pub(super) state: AppState,
     approvals: Arc<Approvals>,
     on_change: OnChange,
 }
@@ -52,7 +52,7 @@ pub struct AppBackend {
 /// Код ошибки — как в приложении (`FORBIDDEN`, `NOT_FOUND`, `SESSION_EXPIRED`…), текст —
 /// английский. Подробности (`details`) LLM не получает, а тексты с данными профиля — адресом,
 /// командой плагина, именем пользователя — заменены фиксированной фразой (`map::llm_message`).
-fn to_backend(e: AppError) -> BackendError {
+pub(super) fn to_backend(e: AppError) -> BackendError {
     BackendError::new(code(&e), map::llm_message(&e))
 }
 
@@ -63,7 +63,7 @@ const BACKGROUND_FAILED: &str = "A background task in Glafira failed; retry the
 
 /// Ошибки проверки и замечаний: сбой файла Glafira и паника фоновой задачи — `internal` с
 /// общим текстом; остальные (кластер, подключение) — как у чтений.
-fn health_to_backend(e: AppError) -> BackendError {
+pub(super) fn health_to_backend(e: AppError) -> BackendError {
     if ["errors.io", "errors.json"].contains(&e.key.as_str())
         || e.key.starts_with("errors.storage.")
     {
@@ -128,7 +128,7 @@ impl AppBackend {
     }
 
     /// Профиль подключения, открытого для LLM; нет подключения и доступ выключен — одна ошибка.
-    fn allowed(&self, id: &str) -> Result<ConnectionProfile, BackendError> {
+    pub(super) fn allowed(&self, id: &str) -> Result<ConnectionProfile, BackendError> {
         match self.state.connections.get(id) {
             Ok(p) if !p.llm_access.is_off() => Ok(p),
             _ => Err(not_allowed()),
@@ -136,7 +136,7 @@ impl AppBackend {
     }
 
     /// Проект в области LLM подключения (крейт проверяет это до вызова; здесь — ещё раз).
-    fn in_scope(p: &ConnectionProfile, namespace: &str) -> Result<(), BackendError> {
+    pub(super) fn in_scope(p: &ConnectionProfile, namespace: &str) -> Result<(), BackendError> {
         match &p.llm_namespaces {
             Some(list) if !list.iter().any(|n| n == namespace) => Err(not_allowed()),
             _ => Ok(()),
@@ -411,6 +411,71 @@ impl AppBackend {
                 .map_err(to_backend)?;
                 ReadOutput::WorkloadState(s)
             }
+            ReadOp::ResourceUsage {
+                connection_id,
+                namespace,
+                workload,
+                window_days,
+            } => {
+                let (st2, id, ns) = (st.clone(), connection_id.clone(), namespace.clone());
+                let r = Box::pin(st.read_in(&connection_id, &namespace, |c| async move {
+                    let sel = crate::metrics::select::for_connection(&st2, &id, &c, false).await?;
+                    if sel.history.is_none() && !sel.status.pod_metrics {
+                        return Ok(Err(sel.status.clone()));
+                    }
+                    let kind = workload_kind(workload.kind);
+                    crate::resources::usage::workload_usage(
+                        &c.reader(),
+                        &sel,
+                        &ns,
+                        kind,
+                        &workload.name,
+                        window_days,
+                        now,
+                    )
+                    .await
+                    .map(Ok)
+                }))
+                .await
+                .map_err(to_backend)?;
+                let u = r.map_err(|s| super::resources::metrics_unavailable(&s))?;
+                ReadOutput::ResourceUsage(super::resources::usage(&u, now))
+            }
+            ReadOp::RecommendResources {
+                connection_id,
+                namespace,
+                workload,
+                window_days,
+            } => {
+                let (st2, id, ns) = (st.clone(), connection_id.clone(), namespace.clone());
+                let r = Box::pin(st.read_in(&connection_id, &namespace, |c| async move {
+                    let sel = crate::metrics::select::for_connection(&st2, &id, &c, false).await?;
+                    if sel.history.is_none() && !sel.status.pod_metrics {
+                        return Ok(Err(sel.status.clone()));
+                    }
+                    let kind = workload_kind(workload.kind);
+                    let nodes = c.info.capabilities.list_nodes;
+                    crate::resources::analyze(
+                        &c.reader(),
+                        &sel,
+                        &ns,
+                        kind,
+                        &workload.name,
+                        window_days,
+                        now,
+                        nodes,
+                    )
+                    .await
+                    .map(|a| Ok((a, sel.status.clone())))
+                }))
+                .await
+                .map_err(to_backend)?;
+                let (a, status) = r.map_err(|s| super::resources::metrics_unavailable(&s))?;
+                ReadOutput::Recommendation(super::resources::recommendation(
+                    &a.recommendation,
+                    &status,
+                ))
+            }
         };
         Ok(out)
     }
@@ -418,7 +483,7 @@ impl AppBackend {
 
 impl AppBackend {
     /// Автор замечаний и проверок: имя из диалога подтверждения.
-    fn author(&self, session: &SessionView) -> Author {
+    pub(super) fn author(&self, session: &SessionView) -> Author {
         Author {
             client: self.approvals.shown_name(&session.id),
             session_id: session.id.clone(),
@@ -695,7 +760,10 @@ async fn exists(r: &ReadClient, t: &Target, ns: &str, name: &str) -> AppResult<b
 }
 
 /// Общие проверки текстов замечания: правило из каталога, непустой заголовок.
-fn check_note_texts(related_rule_id: Option<&str>, title: &str) -> Result<(), BackendError> {
+pub(super) fn check_note_texts(
+    related_rule_id: Option<&str>,
+    title: &str,
+) -> Result<(), BackendError> {
     if let Some(r) = related_rule_id
         && !rules::rule_ids().any(|id| id == r)
     {
@@ -794,6 +862,14 @@ impl Backend for AppBackend {
     fn health(&self, op: mh::HealthOp) -> BoxFuture<'_, Result<mh::HealthOutput, BackendError>> {
         Box::pin(self.health_impl(op))
     }
+
+    fn proposals(
+        &self,
+        op: glafira_mcp_kubernetes::proposals::ProposalOp,
+    ) -> BoxFuture<'_, Result<glafira_mcp_kubernetes::proposals::ProposalOutput, BackendError>>
+    {
+        Box::pin(self.proposals_impl(op))
+    }
 }
 
 #[cfg(test)]
@@ -1271,4 +1347,197 @@ mod tests {
         let e = crate::k8s::errors::from_transport_text("tls handshake eof");
         assert_eq!(to_backend(e.clone()).message, e.message);
     }
+
+    fn workload_ref() -> m::WorkloadRef {
+        m::WorkloadRef {
+            kind: m::WorkloadKind::Deployment,
+            name: "web".into(),
+        }
+    }
+
+    #[tokio::test]
+    async fn resources_come_from_the_metrics_source_without_the_own_address() {
+        let (_d, b) = backend();
+        pooled(&b, "open", crate::resources::usage::tests::cluster);
+        let usage = || ReadOp::ResourceUsage {
+            connection_id: "open".into(),
+            namespace: "team-a".into(),
+            workload: workload_ref(),
+            window_days: 7,
+        };
+        // Prometheus через прокси нет (404), metrics.k8s.io есть — одна точка без истории.
+        let ReadOutput::ResourceUsage(u) = b.read(usage()).await.unwrap() else {
+            panic!("usage")
+        };
+        assert_eq!(u.source.kind, "metricsServer");
+        assert_eq!(u.containers[0].stats.cpu_max.as_deref(), Some("120m"));
+        assert_eq!(u.containers[0].requests.cpu.as_deref(), Some("500m"));
+        assert!(u.series.is_none());
+        assert_eq!(u.rejected.len(), 4, "кандидаты Prometheus с причинами");
+        let ReadOutput::Recommendation(r) = b
+            .read(ReadOp::RecommendResources {
+                connection_id: "open".into(),
+                namespace: "team-a".into(),
+                workload: workload_ref(),
+                window_days: 7,
+            })
+            .await
+            .unwrap()
+        else {
+            panic!("recommendation")
+        };
+        assert_eq!(r.confidence, "low");
+        assert_eq!(
+            r.containers[0]
+                .recommended_requests
+                .as_ref()
+                .unwrap()
+                .cpu
+                .as_deref(),
+            Some("140m")
+        );
+        // Свой источник без токена: модель не видит его адреса.
+        let mut p = b.state.connections.get("open").unwrap();
+        p.metrics = crate::storage::profile::MetricsConfig::Prometheus(
+            crate::storage::profile::MetricsEndpoint {
+                url: "https://secret-prometheus.corp.example".into(),
+                auth: crate::storage::profile::MetricsAuth::Bearer,
+                ca: crate::storage::profile::MetricsCa::Connection,
+            },
+        );
+        b.state.connections.upsert(p).unwrap();
+        pooled(&b, "open", crate::resources::usage::tests::cluster);
+        let e = b.read(usage()).await.unwrap_err();
+        assert_eq!(e.code, "metrics_unavailable");
+        assert!(!e.message.contains("secret-prometheus"), "{}", e.message);
+        assert!(e.message.contains("tokenMissing"), "{}", e.message);
+    }
+
+    fn proposal(change: serde_json::Value) -> glafira_mcp_kubernetes::proposals::NewProposal {
+        glafira_mcp_kubernetes::proposals::NewProposal {
+            severity: mh::Severity::Warning,
+            title: "Fix".into(),
+            rationale: "r".into(),
+            change: serde_json::from_value(change).unwrap(),
+            related_rule_id: None,
+            related_finding_key: None,
+        }
+    }
+
+    #[tokio::test]
+    async fn proposals_are_checked_against_the_cluster_and_kept_in_glafira() {
+        use glafira_mcp_kubernetes::proposals::{ProposalOp, ProposalOutput};
+        let (_d, b) = backend();
+        pooled(&b, "open", |s| match s.path() {
+            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers" => list(vec![
+                serde_json::json!({"metadata": {"name": "web-hpa"}, "spec": {"maxReplicas": 5,
+                    "scaleTargetRef": {"kind": "Deployment", "name": "web"}}}),
+            ]),
+            "/api/v1/namespaces/team-a/limitranges" => list(vec![serde_json::json!({
+                "metadata": {"name": "lr"},
+                "spec": {"limits": [{"type": "Container", "min": {"cpu": "100m"}}]}})]),
+            p if p.ends_with("/resourcequotas") => list(vec![]),
+            other => crate::resources::usage::tests::cluster(&Seen {
+                method: s.method.clone(),
+                uri: other.to_string(),
+                body: s.body.clone(),
+            }),
+        });
+        let session = view("s1", Some("claude-ai"));
+        b.access(&session);
+        let propose = |change: serde_json::Value| {
+            b.proposals(ProposalOp::Propose {
+                connection_id: "open".into(),
+                session: session.clone(),
+                proposal: proposal(change),
+            })
+        };
+        let w = serde_json::json!({"kind": "Deployment", "name": "web"});
+        let e = propose(serde_json::json!({"kind": "scale", "namespace": "team-a", "workload": w, "replicas": 3}))
+            .await
+            .unwrap_err();
+        assert_eq!(e.code, "invalid_argument");
+        assert!(e.message.contains("setHpaBounds"), "{}", e.message);
+        let e = propose(
+            serde_json::json!({"kind": "setResources", "namespace": "team-a", "workload": w,
+            "containers": [{"name": "ghost", "requests": {"cpu": "1"}}]}),
+        )
+        .await
+        .unwrap_err();
+        assert_eq!(e.code, "invalid_argument");
+        assert!(e.message.contains("ghost"), "{}", e.message);
+        let e = propose(
+            serde_json::json!({"kind": "rolloutRestart", "namespace": "team-b", "workload": w}),
+        )
+        .await
+        .unwrap_err();
+        assert_eq!(e, not_allowed());
+        let out = propose(
+            serde_json::json!({"kind": "setResources", "namespace": "team-a", "workload": w,
+            "containers": [{"name": "app", "requests": {"cpu": "50m"}}]}),
+        )
+        .await
+        .unwrap();
+        let ProposalOutput::Proposed {
+            proposal_id,
+            created,
+            read_only,
+            warnings,
+        } = out
+        else {
+            panic!("proposed")
+        };
+        assert!(created && !read_only);
+        assert_eq!(
+            warnings,
+            [
+                "Container app: the CPU request is below the LimitRange minimum 100m: new pods will be rejected"
+            ]
+        );
+        let ProposalOutput::Proposals(items) = b
+            .proposals(ProposalOp::List {
+                connection_id: "open".into(),
+                status: mh::StatusFilter::Open,
+            })
+            .await
+            .unwrap()
+        else {
+            panic!("list")
+        };
+        assert_eq!(items.len(), 1);
+        assert_eq!(items[0].change.kind_name(), "setResources");
+        assert_eq!(
+            items[0].target.workload_kind,
+            Some(m::WorkloadKind::Deployment)
+        );
+        let other = view("s2", Some("cursor"));
+        b.access(&other);
+        let e = b
+            .proposals(ProposalOp::Withdraw {
+                connection_id: "open".into(),
+                session: other,
+                proposal_id: proposal_id.clone(),
+            })
+            .await
+            .unwrap_err();
+        assert_eq!(e.code, "not_allowed");
+        assert!(matches!(
+            b.proposals(ProposalOp::Withdraw {
+                connection_id: "open".into(),
+                session,
+                proposal_id,
+            })
+            .await
+            .unwrap(),
+            ProposalOutput::Withdrawn
+        ));
+        let e = b
+            .proposals(ProposalOp::List {
+                connection_id: "closed".into(),
+                status: mh::StatusFilter::All,
+            })
+            .await
+            .unwrap_err();
+        assert_eq!(e, not_allowed());
+    }
 }
diff --git a/src-tauri/src/mcp/mod.rs b/src-tauri/src/mcp/mod.rs
index 79864fd..504b90c 100644
--- a/src-tauri/src/mcp/mod.rs
+++ b/src-tauri/src/mcp/mod.rs
@@ -9,6 +9,8 @@ pub mod files;
 #[cfg(test)]
 mod guard;
 pub mod map;
+pub mod proposals;
+pub mod resources;
 pub mod shim;
 
 use std::ffi::OsString;
diff --git a/src-tauri/src/metrics/select.rs b/src-tauri/src/metrics/select.rs
index 52b4bc9..0a30105 100644
--- a/src-tauri/src/metrics/select.rs
+++ b/src-tauri/src/metrics/select.rs
@@ -297,7 +297,7 @@ pub async fn for_connection(
     };
     let (s, _) = crate::k8s::read::observe(
         c.metrics
-            .get_or_select(refresh, || async { select(&inputs, now_s()).await }),
+            .get_or_select(refresh, || Box::pin(select(&inputs, now_s()))),
     )
     .await;
     Ok(s)
diff --git a/src-tauri/src/resources/mod.rs b/src-tauri/src/resources/mod.rs
index dbbb0c1..d9893e7 100644
--- a/src-tauri/src/resources/mod.rs
+++ b/src-tauri/src/resources/mod.rs
@@ -34,8 +34,15 @@ pub async fn analyze(
     now: i64,
     nodes: bool,
 ) -> AppResult<Analysis> {
-    let usage = usage::workload_usage(r, sel, namespace, kind, name, days, now).await?;
-    let c = constraints::read_constraints(r, namespace, kind, name, nodes).await;
+    // Future сбора велики для стека потока в debug-сборке — в куче.
+    let usage = Box::pin(usage::workload_usage(
+        r, sel, namespace, kind, name, days, now,
+    ))
+    .await?;
+    let c = Box::pin(constraints::read_constraints(
+        r, namespace, kind, name, nodes,
+    ))
+    .await;
     Ok(Analysis {
         recommendation: recommend::recommend(&usage, &c),
         usage,
diff --git a/src-tauri/src/resources/usage.rs b/src-tauri/src/resources/usage.rs
index dedcce2..d1eeea9 100644
--- a/src-tauri/src/resources/usage.rs
+++ b/src-tauri/src/resources/usage.rs
@@ -369,8 +369,9 @@ pub async fn workload_usage(
                 name: name.into(),
             }
         };
+        // Восемь запросов одной future: в debug-сборке она велика для стека потока — в куче.
         let (usage, charts, coverage, data) =
-            from_history(src, &selector, days, now, owner, containers)
+            Box::pin(from_history(src, &selector, days, now, owner, containers))
                 .await
                 .map_err(failed)?;
         out.source = src.info().clone();
@@ -490,6 +491,7 @@ pub(crate) mod tests {
                         "spec": {"containers": [{"name": "app", "image": "web:1"}]},
                         "status": {"phase": "Running"}}]}),
             ),
+            "/apis/metrics.k8s.io/v1beta1" => (200, serde_json::json!({"kind": "APIResourceList"})),
             "/apis/metrics.k8s.io/v1beta1/namespaces/team-a/pods" => (
                 200,
                 serde_json::json!({"apiVersion": "metrics.k8s.io/v1beta1", "kind": "PodMetricsList",
````

- [ ] **Step 4: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 674 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `cargo test --manifest-path crates/glafira-mcp-kubernetes/Cargo.toml`  
Expected: PASS — модульные `test result: ok. 24 passed`, `manifest` — `test result: ok. 1 passed`, `server` — `test result: ok. 35 passed`

Run: `cargo clippy --manifest-path crates/glafira-mcp-kubernetes/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path crates/glafira-mcp-kubernetes/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 198`

- [ ] **Step 5: Commit.**

```bash
git add \
  PORTED.md \
  crates/glafira-mcp-kubernetes/src/lib.rs \
  crates/glafira-mcp-kubernetes/src/model.rs \
  crates/glafira-mcp-kubernetes/src/proposals.rs \
  crates/glafira-mcp-kubernetes/src/resources.rs \
  crates/glafira-mcp-kubernetes/src/server/mod.rs \
  crates/glafira-mcp-kubernetes/src/server/proposal_tools.rs \
  crates/glafira-mcp-kubernetes/src/server/resource_tools.rs \
  crates/glafira-mcp-kubernetes/tests/server.rs \
  src-tauri/src/mcp/backend.rs \
  src-tauri/src/mcp/mod.rs \
  src-tauri/src/mcp/proposals.rs \
  src-tauri/src/mcp/resources.rs \
  src-tauri/src/metrics/select.rs \
  src-tauri/src/resources/mod.rs \
  src-tauri/src/resources/usage.rs
git commit -s -m "feat(mcp): get_resource_usage, recommend_resources, propose_change, list_proposals and withdraw_proposal — 22 tools; proposals are checked against the cluster by reading and kept in Glafira; the model never gets the address of an own metrics source" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 13: Стенд kind для изменений и метрик: каждый вид изменения — предпросмотр, применение и CONFLICT на настоящем API-сервере; поддельный Prometheus за прокси API-сервера — статистика, рекомендация и правила resources.*; ресурсы и предложения через MCP

**Files:**
- Create: `dev/kind/metrics.yaml`, `dev/kind/ops.yaml`, `src-tauri/tests/common/prometheus.rs`, `src-tauri/tests/it_metrics.rs`, `src-tauri/tests/it_ops.rs`
- Modify: `dev/kind/README.md`, `dev/kind/setup.sh`, `src-tauri/tests/common/mod.rs`, `src-tauri/tests/it_mcp.rs`

**Interfaces:**
- Consumes: задачи 4–12; O2: стенд `dev/kind` (`setup.sh`, `fixtures.yaml`), `tests/common/mod.rs`, `it_mcp.rs`.
- Produces: `dev/kind/ops.yaml` (Deployment `tuned`, HPA `tuned-hpa`), `dev/kind/metrics.yaml` (namespace `monitoring`, Service `prometheus-k8s:9090` с селектором под-заглушки, под `prometheus-stub` с hostNetwork на control-plane, Deployment `busy` в `team-a` с явными limits), `setup.sh` — `out/host-address` и DNAT порта 19090 адреса заглушки на рабочую машину; `tests/common/prometheus.rs::{PORT = 19090, CPU_P95, MEMORY_MAX, THROTTLING, serve(kubeconfig, pods)}`; `tests/it_ops.rs`, `tests/it_metrics.rs`; `it_mcp` — ресурсы и предложения.

Решение пользователя: поддельный Prometheus, а не kube-prometheus на стенде. Образов с HTTP-сервером на ноде kind нет, поэтому сервер — в процессе теста на рабочей машине, а прокси API-сервера ведёт только на адреса подов (Endpoints на адрес хоста он отвергает): Service выбирает под-заглушку с hostNetwork (её адрес — адрес ноды), `setup.sh` добавляет в ноде правило DNAT с порта 19090 на `out/host-address` (macOS — `host.docker.internal`, Linux — шлюз сети kind). `it_ops` возвращает объекты `ops.yaml` в исходное состояние и создаёт под без контроллера с `generateName` — тест можно повторять. `it_metrics` проверяет автопоиск, статистику и рекомендацию `busy` по записанным ответам, правила `resources.cpuThrottled` и `resources.memoryNearLimit`, отказ `forbidden` у `dev-token`. Порт 19090 рабочей машины на время `it_metrics` и `it_mcp` должен быть свободен.

- [ ] **Step 1: Тесты.**

<!-- block: file t13-src-tauri-tests-common-prometheus-rs step=13.1 path=src-tauri/tests/common/prometheus.rs -->
````rust
//! Поддельный Prometheus стенда kind (`dev/kind/metrics.yaml`): HTTP-сервер теста на рабочей машине,
//! к которому прокси API-сервера ходит через под-заглушку и перенаправление `setup.sh`. Отвечает
//! записанными ответами Prometheus HTTP API по тексту запроса.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Порт поддельного Prometheus на рабочей машине: на него ведёт перенаправление стенда.
pub const PORT: u16 = 19090;
pub const CPU_P95: f64 = 0.012;
pub const MEMORY_MAX: f64 = 30.0 * 1024.0 * 1024.0;
pub const THROTTLING: f64 = 0.4;

/// Ответ Prometheus HTTP API по тексту запроса: контейнер app — p95 CPU 12m, максимум памяти
/// 30 Mi при лимите 32 Mi, троттлинг 40 %, данные за всё окно; запросы проекта — по подам `pods`.
fn answer(path: &str, p: &HashMap<String, String>, pods: &[String]) -> Value {
    let q = p.get("query").map_or("", String::as_str);
    let num = |k: &str| p.get(k).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    if path.ends_with("/query_range") {
        let (start, end) = (num("start"), num("end"));
        let series = |v: f64| {
            json!([{"metric": {"container": "app"},
                "values": [[start, v.to_string()], [end, v.to_string()]]}])
        };
        let result = if q.contains("\"event\"") {
            json!([])
        } else if q.contains("container_memory_working_set_bytes") {
            series(MEMORY_MAX)
        } else {
            series(CPU_P95)
        };
        return json!({"status": "success", "data": {"resultType": "matrix", "result": result}});
    }
    let t = num("time");
    let sample = |metric: Value, v: f64| json!({"metric": metric, "value": [t, v.to_string()]});
    let stats = |list: &[(&str, f64)]| -> Vec<Value> {
        list.iter()
            .map(|(s, v)| sample(json!({"container": "app", "stat": s}), *v))
            .collect()
    };
    let per_pod = |v: f64| -> Vec<Value> {
        pods.iter()
            .map(|pod| sample(json!({"pod": pod, "container": "app"}), v))
            .collect()
    };
    let mi = 1024.0 * 1024.0;
    let result = if q == "vector(1)" {
        vec![sample(json!({}), 1.0)]
    } else if q.starts_with("count(kube_pod_owner") {
        vec![sample(json!({}), 3.0)]
    } else if q.contains("\"cpu_p50\"") {
        stats(&[
            ("cpu_p50", 0.008),
            ("cpu_p95", CPU_P95),
            ("cpu_p99", 0.015),
            ("cpu_max", 0.02),
        ])
    } else if q.contains("\"memory_p95\"") {
        stats(&[
            ("memory_p95", 20.0 * mi),
            ("memory_p99", 22.0 * mi),
            ("memory_max", MEMORY_MAX),
        ])
    } else if q.contains("\"throttling_mean\"") {
        stats(&[("throttling_mean", THROTTLING), ("throttling_p95", 0.6)])
    } else if q.contains("\"restarts\"") {
        stats(&[("oom_count", 0.0), ("restarts", 0.0)])
    } else if q.contains("\"steps\"") {
        vec![
            sample(json!({"stat": "steps"}), 1e6),
            sample(json!({"stat": "first"}), t - 31.0 * 86_400.0),
        ]
    } else if q.contains("container_cpu_cfs_throttled_periods_total") {
        per_pod(THROTTLING)
    } else if q.contains("quantile_over_time(0.95") {
        per_pod(CPU_P95)
    } else if q.contains("container_memory_working_set_bytes") {
        per_pod(MEMORY_MAX)
    } else {
        Vec::new()
    };
    json!({"status": "success", "data": {"resultType": "vector", "result": result}})
}

/// Поддельный Prometheus рядом с kubeconfig стенда: по запросу на соединение. На Linux адрес из
/// `host-address` — шлюз сети kind, то есть адрес этой машины; Docker Desktop доставляет
/// host.docker.internal на её loopback.
pub async fn serve(kubeconfig: &Path, pods: Arc<Mutex<Vec<String>>>) {
    let host = std::fs::read_to_string(kubeconfig.with_file_name("host-address")).unwrap();
    let listener = match TcpListener::bind((host.trim(), PORT)).await {
        Ok(l) => l,
        Err(_) => TcpListener::bind(("127.0.0.1", PORT)).await.unwrap(),
    };
    tokio::spawn(async move {
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            let pods = pods.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    match s.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                let head = String::from_utf8_lossy(&buf);
                let target = head.split_whitespace().nth(1).unwrap_or("/");
                let (path, query) = target.split_once('?').unwrap_or((target, ""));
                let params: HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
                    .into_owned()
                    .collect();
                let body = answer(path, &params, &pods.lock().unwrap()).to_string();
                let reply = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(reply.as_bytes()).await;
            });
        }
    });
}
````

<!-- block: file t13-src-tauri-tests-it-metrics-rs step=13.1 path=src-tauri/tests/it_metrics.rs -->
````rust
//! Источник метрик на настоящем API-сервере (kind, `dev/kind/setup.sh`, `dev/kind/metrics.yaml`):
//! автопоиск находит Service monitoring/prometheus-k8s и ходит в него через прокси API-сервера, а
//! отвечает поддельный Prometheus — HTTP-сервер этого теста на рабочей машине (порт 19090, на него
//! ведёт перенаправление стенда). По записанным ответам — статистика и рекомендация Deployment busy
//! и правила `resources.*` проверки; у dev-token прав на прокси нет — источник отвергнут.
//! Без `GO_IT_KUBECONFIG` тест пропускается.

mod common;

use std::sync::{Arc, Mutex};

use common::prometheus::{CPU_P95, MEMORY_MAX, THROTTLING, serve};
use common::{import, kubeconfig_path};
use glafira_kubernetes_lib::error::ErrorCode;
use glafira_kubernetes_lib::health::collect::{LIMITS, collect};
use glafira_kubernetes_lib::health::rules::evaluate;
use glafira_kubernetes_lib::k8s::workloads::WorkloadKind;
use glafira_kubernetes_lib::metrics::prometheus::Reason;
use glafira_kubernetes_lib::metrics::select::for_connection;
use glafira_kubernetes_lib::resources::analyze;
use glafira_kubernetes_lib::resources::recommend::{Confidence, Note};
use glafira_kubernetes_lib::resources::usage::workload_usage;
use glafira_kubernetes_lib::state::AppState;
use glafira_kubernetes_lib::storage::profile::Environment;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};

const NS: &str = "team-a";

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[tokio::test]
async fn prometheus_behind_the_api_server_proxy() {
    let Some(path) = kubeconfig_path() else {
        return;
    };
    let pods = Arc::new(Mutex::new(Vec::new()));
    serve(&path, pods.clone()).await;

    // Администратор: автопоиск выбирает Prometheus за прокси API-сервера.
    let (profile, secrets) = import(&path, "admin");
    let id = profile.id.clone();
    let dir = tempfile::tempdir().unwrap();
    let st = AppState::new(dir.path(), secrets).unwrap();
    st.connections.upsert(profile.clone()).unwrap();
    let c = st.connected(&id, false).await.unwrap();
    let sel = for_connection(&st, &id, &c, true).await.unwrap();
    let info = sel.status.selected.clone().expect("источник выбран");
    assert_eq!(info.label(), "Prometheus monitoring/prometheus-k8s:9090");

    // Статистика и рекомендация Deployment busy за 7 дней.
    let a = analyze(
        &c.reader(),
        &sel,
        NS,
        WorkloadKind::Deployment,
        "busy",
        7,
        now(),
        true,
    )
    .await
    .unwrap();
    let app = &a.usage.containers[0];
    assert_eq!(
        (
            app.current.name.as_str(),
            app.stats.cpu.p95,
            app.stats.memory.max,
            app.stats.throttling.mean
        ),
        ("app", Some(CPU_P95), Some(MEMORY_MAX), Some(THROTTLING))
    );
    assert_eq!((a.usage.coverage, a.usage.owner_metrics), (Some(1.0), true));
    assert_eq!(a.usage.charts.as_ref().unwrap().cpu[0].points.len(), 2);
    // Запас к p95 CPU и максимуму памяти; троттлинг поднимает CPU limit.
    let r = &a.recommendation;
    let rec = r.containers[0].recommended.as_ref().unwrap();
    let mi = 1024.0 * 1024.0;
    assert_eq!(
        (
            rec.requests.cpu,
            rec.requests.memory,
            rec.limits.cpu,
            rec.limits.memory
        ),
        (Some(0.02), Some(32.0 * mi), Some(0.08), Some(48.0 * mi))
    );
    assert_eq!(r.confidence, Confidence::High);
    assert_eq!(r.containers[0].notes, [Note::CpuThrottled { p95: 0.6 }]);

    // Проверка проекта: троттлинг и память у лимита подов busy.
    let list = Api::<Pod>::namespaced(c.kube_client().clone(), NS)
        .list(&ListParams::default().labels("app=busy"))
        .await
        .unwrap();
    *pods.lock().unwrap() = list
        .items
        .into_iter()
        .filter_map(|p| p.metadata.name)
        .collect();
    let s = collect(
        &c.reader(),
        &c.info,
        &profile,
        Some(&[NS.to_string()]),
        LIMITS,
        now(),
        sel.history.as_deref(),
    )
    .await;
    let found: Vec<String> = evaluate(&s, Environment::Prod)
        .iter()
        .filter(|f| f.rule_id.starts_with("resources."))
        .map(|f| format!("{} {}", f.rule_id, f.target.name.as_deref().unwrap_or("-")))
        .collect();
    assert_eq!(
        found,
        [
            "resources.cpuThrottled busy",
            "resources.memoryNearLimit busy"
        ]
    );

    // dev-token: прокси к сервису в чужом проекте запрещён, metrics.k8s.io на kind нет.
    let (profile, secrets) = import(&path, "dev-token");
    let id = profile.id.clone();
    let dir = tempfile::tempdir().unwrap();
    let st = AppState::new(dir.path(), secrets).unwrap();
    st.connections.upsert(profile).unwrap();
    let c = st.connected(&id, false).await.unwrap();
    let sel = for_connection(&st, &id, &c, true).await.unwrap();
    assert!(sel.status.selected.is_none());
    let rejected = sel
        .status
        .rejected
        .iter()
        .find(|r| r.source.location == "monitoring/prometheus-k8s:9090")
        .expect("кандидат отвергнут");
    assert_eq!(rejected.reason, Reason::Forbidden);
    let e = workload_usage(
        &c.reader(),
        &sel,
        NS,
        WorkloadKind::Deployment,
        "busy",
        7,
        now(),
    )
    .await
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::MetricsUnavailable);
}
````

<!-- block: file t13-src-tauri-tests-it-ops-rs step=13.1 path=src-tauri/tests/it_ops.rs -->
````rust
//! Изменения §9 на настоящем API-сервере (kind, `dev/kind/setup.sh`, объекты `dev/kind/ops.yaml`):
//! предпросмотр с server-side dry-run, применение каждого вида изменения с версией предпросмотра
//! (идущая выкладка ей не мешает), `CONFLICT` при изменённом spec, стражи профиля (только чтение, имя prod-подключения,
//! имя пода без контроллера). Подключение — `dev-token`: роли `edit` в team-a хватает.
//! Без `GO_IT_KUBECONFIG` тест пропускается.

mod common;

use common::{import, kubeconfig_path};
use glafira_kubernetes_lib::change::Change;
use glafira_kubernetes_lib::commands::changes::{apply_change_impl, preview_change_impl};
use glafira_kubernetes_lib::error::ErrorCode;
use glafira_kubernetes_lib::ops::DryRun;
use glafira_kubernetes_lib::state::AppState;
use glafira_kubernetes_lib::storage::profile::Environment;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, Patch, PatchParams, PostParams};
use serde_json::json;

const NS: &str = "team-a";

fn change(v: serde_json::Value) -> Change {
    serde_json::from_value(v).unwrap()
}

#[tokio::test]
async fn every_change_kind_previews_applies_and_conflicts() {
    let Some(path) = kubeconfig_path() else {
        return;
    };
    let (profile, secrets) = import(&path, "dev-token");
    let id = profile.id.clone();
    let dir = tempfile::tempdir().unwrap();
    let st = AppState::new(dir.path(), secrets).unwrap();
    st.connections.upsert(profile.clone()).unwrap();
    let c = st.connected(&id, false).await.unwrap();
    let deployments: Api<Deployment> = Api::namespaced(c.kube_client().clone(), NS);
    let hpas: Api<HorizontalPodAutoscaler> = Api::namespaced(c.kube_client().clone(), NS);
    let workload = json!({"kind": "Deployment", "name": "tuned"});

    // Исходное состояние `ops.yaml`: тест можно повторять.
    let reset = json!({"spec": {"replicas": 1, "template": {"spec": {"containers": [{"name": "app",
        "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}, "limits": {"memory": "64Mi"}}}]}}}});
    deployments
        .patch("tuned", &PatchParams::default(), &Patch::Strategic(reset))
        .await
        .unwrap();
    let reset = json!({"spec": {"minReplicas": 1, "maxReplicas": 2}});
    hpas.patch("tuned-hpa", &PatchParams::default(), &Patch::Merge(reset))
        .await
        .unwrap();

    // Ресурсы: предпросмотр с dry-run, применение, устаревшая версия — CONFLICT.
    let resources = change(
        json!({"kind": "setResources", "namespace": NS, "workload": workload,
        "containers": [{"name": "app", "requests": {"cpu": "20m"}, "limits": {"memory": "96Mi"}}]}),
    );
    let p = preview_change_impl(&st, &id, resources.clone())
        .await
        .unwrap();
    assert!(matches!(p.dry_run, DryRun::Passed), "{:?}", p.dry_run);
    assert_eq!(p.diff.len(), 2, "{:?}", p.diff);
    let rv = p.version.clone();
    apply_change_impl(&st, &id, resources.clone(), rv.clone(), None, None, None)
        .await
        .unwrap();
    let d = deployments.get("tuned").await.unwrap();
    let r = d.spec.unwrap().template.spec.unwrap().containers[0]
        .resources
        .clone()
        .unwrap();
    assert_eq!(r.requests.unwrap()["cpu"].0, "20m");
    assert_eq!(r.limits.unwrap()["memory"].0, "96Mi");
    let managed = d.metadata.managed_fields.unwrap_or_default();
    assert!(
        managed
            .iter()
            .any(|m| m.manager.as_deref() == Some("glafira-kubernetes")),
        "fieldManager"
    );
    let e = apply_change_impl(&st, &id, resources, rv, None, None, None)
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict, "{e:?}");

    // Реплики через подресурс scale, перезапуск — аннотацией шаблона.
    let scale =
        change(json!({"kind": "scale", "namespace": NS, "workload": workload, "replicas": 2}));
    let p = preview_change_impl(&st, &id, scale.clone()).await.unwrap();
    apply_change_impl(&st, &id, scale, p.version, None, None, None)
        .await
        .unwrap();
    assert_eq!(
        deployments
            .get("tuned")
            .await
            .unwrap()
            .spec
            .unwrap()
            .replicas,
        Some(2)
    );
    let restart = change(json!({"kind": "rolloutRestart", "namespace": NS, "workload": workload}));
    apply_change_impl(&st, &id, restart, None, None, None, None)
        .await
        .unwrap();
    let annotations = deployments
        .get("tuned")
        .await
        .unwrap()
        .spec
        .unwrap()
        .template
        .metadata
        .unwrap()
        .annotations
        .unwrap();
    assert!(annotations.contains_key("kubectl.kubernetes.io/restartedAt"));
    let back =
        change(json!({"kind": "scale", "namespace": NS, "workload": workload, "replicas": 1}));
    apply_change_impl(&st, &id, back, None, None, None, None)
        .await
        .unwrap();

    // Границы HPA.
    let hpa = change(
        json!({"kind": "setHpaBounds", "namespace": NS, "hpa": "tuned-hpa",
        "minReplicas": 1, "maxReplicas": 3}),
    );
    let p = preview_change_impl(&st, &id, hpa.clone()).await.unwrap();
    assert!(matches!(p.dry_run, DryRun::Passed), "{:?}", p.dry_run);
    apply_change_impl(&st, &id, hpa, p.version, None, None, None)
        .await
        .unwrap();
    assert_eq!(
        hpas.get("tuned-hpa")
            .await
            .unwrap()
            .spec
            .unwrap()
            .max_replicas,
        3
    );

    // Под без контроллера удаляется только с его именем; имя у каждого прогона своё.
    let pods: Api<Pod> = Api::namespaced(c.kube_client().clone(), NS);
    let image = deployments
        .get("tuned")
        .await
        .unwrap()
        .spec
        .unwrap()
        .template
        .spec
        .unwrap()
        .containers[0]
        .image
        .clone();
    let solo: Pod = serde_json::from_value(json!({"metadata": {"generateName": "solo-"},
        "spec": {"terminationGracePeriodSeconds": 1, "containers": [{"name": "app", "image": image,
            "imagePullPolicy": "IfNotPresent", "command": ["bash", "-c", "exec 3<> <(:); read -u 3"]}]}}))
    .unwrap();
    let solo = pods.create(&PostParams::default(), &solo).await.unwrap();
    let name = solo.metadata.name.unwrap();
    let delete = change(json!({"kind": "deletePod", "namespace": NS, "pod": name}));
    let p = preview_change_impl(&st, &id, delete.clone()).await.unwrap();
    assert!(p.confirm_pod_name);
    assert!(matches!(p.dry_run, DryRun::Passed), "{:?}", p.dry_run);
    let after = pods.get(&name).await.unwrap();
    assert!(
        after.metadata.deletion_timestamp.is_none(),
        "dry-run не удаляет: {:?}",
        after.metadata
    );
    let e = apply_change_impl(
        &st,
        &id,
        delete.clone(),
        p.version.clone(),
        None,
        None,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::ConfirmationRequired, "{e:?}");
    apply_change_impl(&st, &id, delete, p.version, None, Some(name.clone()), None)
        .await
        .unwrap();
    let pod = pods.get_opt(&name).await.unwrap();
    assert!(
        pod.is_none_or(|p| p.metadata.deletion_timestamp.is_some()),
        "под удаляется"
    );

    // Стражи профиля — до кластера.
    let mut prod = profile.clone();
    prod.environment = Environment::Prod;
    st.connections.upsert(prod.clone()).unwrap();
    let scale =
        change(json!({"kind": "scale", "namespace": NS, "workload": workload, "replicas": 1}));
    let e = apply_change_impl(&st, &id, scale.clone(), None, None, None, None)
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::ConfirmationRequired);
    apply_change_impl(
        &st,
        &id,
        scale.clone(),
        None,
        Some(prod.name.clone()),
        None,
        None,
    )
    .await
    .unwrap();
    prod.read_only = true;
    st.connections.upsert(prod.clone()).unwrap();
    let e = apply_change_impl(&st, &id, scale, None, Some(prod.name.clone()), None, None)
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::ReadOnly);
}
````

<!-- block: patch t13-tests step=13.1 -->
````diff
diff --git a/src-tauri/tests/common/mod.rs b/src-tauri/tests/common/mod.rs
index dc0e78d..bd59034 100644
--- a/src-tauri/tests/common/mod.rs
+++ b/src-tauri/tests/common/mod.rs
@@ -1,6 +1,8 @@
 //! Общее для интеграционных тестов на kind: kubeconfig стенда и подключение из его контекста.
 #![allow(dead_code)]
 
+pub mod prometheus;
+
 use std::path::{Path, PathBuf};
 use std::sync::Arc;
 
diff --git a/src-tauri/tests/it_mcp.rs b/src-tauri/tests/it_mcp.rs
index 2250fd8..9eb4d60 100644
--- a/src-tauri/tests/it_mcp.rs
+++ b/src-tauri/tests/it_mcp.rs
@@ -1,13 +1,16 @@
 //! MCP-шлюз на настоящем кластере (kind, `dev/kind/setup.sh`): контроллер приложения поднимает
 //! сервер, клиент rmcp подключается через `mcp.json` и рукопожатие и вызывает инструменты. LLM видит
-//! только проект team-a (`llmNamespaces`), значения Secret в ответы не попадают.
+//! только проект team-a (`llmNamespaces`), значения Secret в ответы не попадают. Метрики — от
+//! поддельного Prometheus стенда (`common::prometheus`), изменения — только предложения.
 //! Без `GO_IT_KUBECONFIG` тест пропускается.
 
 mod common;
 
+use std::sync::{Arc, Mutex};
 use std::time::Duration;
 
 use base64::Engine;
+use common::prometheus::serve;
 use common::{import, kubeconfig_path};
 use glafira_kubernetes_lib::mcp::McpController;
 use glafira_kubernetes_lib::mcp::controller::ServerState;
@@ -60,6 +63,8 @@ async fn the_gateway_reads_the_stand_within_the_llm_scope() {
     let Some(path) = kubeconfig_path() else {
         return;
     };
+    // До первого обращения к метрикам: выбор источника живёт в подключении 5 минут.
+    serve(&path, Arc::new(Mutex::new(Vec::new()))).await;
     let (mut profile, secrets) = import(&path, "admin");
     profile.llm_access = LlmAccess::Metadata;
     profile.llm_namespaces = Some(vec![NS.into()]);
@@ -205,6 +210,67 @@ async fn the_gateway_reads_the_stand_within_the_llm_scope() {
     assert_eq!(v["samples"], 6, "{v}");
     assert_eq!(v["initial"]["pods"].as_array().unwrap().len(), 1);
 
+    // Ресурсы: Prometheus за прокси API-сервера.
+    let busy = json!({"kind": "Deployment", "name": "busy"});
+    let v = ok(call(
+        &c,
+        "get_resource_usage",
+        json!({"connectionId": id, "namespace": NS, "workload": busy}),
+    )
+    .await);
+    assert_eq!(v["source"]["kind"], "prometheus", "{v}");
+    assert_eq!(v["containers"][0]["stats"]["cpuP95"], "12m", "{v}");
+    assert_eq!(v["containers"][0]["stats"]["throttlingMeanPercent"], 40.0);
+    let msg = err(&call(
+        &c,
+        "get_resource_usage",
+        json!({"connectionId": id, "namespace": NS, "workload": busy, "windowDays": 3}),
+    )
+    .await);
+    assert!(msg.starts_with("invalid_argument"), "{msg}");
+    let v = ok(call(
+        &c,
+        "recommend_resources",
+        json!({"connectionId": id, "namespace": NS, "workload": busy, "windowDays": 14}),
+    )
+    .await);
+    assert_eq!(v["confidence"], "high", "{v}");
+    assert_eq!(
+        v["containers"][0]["recommendedRequests"],
+        json!({"cpu": "20m", "memory": "32Mi"})
+    );
+
+    // Изменение — только предложение: Glafira проверяет его чтением кластера и хранит у себя.
+    let propose = json!({"connectionId": id, "severity": "warning", "title": "Raise the CPU limit",
+        "rationale": "40% of CPU periods are throttled", "relatedRuleId": "resources.cpuThrottled",
+        "change": {"kind": "setResources", "namespace": NS, "workload": busy,
+            "containers": [{"name": "app", "limits": {"cpu": "80m"}}]}});
+    let v = ok(call(&c, "propose_change", propose.clone()).await);
+    assert_eq!(
+        (&v["created"], &v["readOnly"]),
+        (&json!(true), &json!(false)),
+        "{v}"
+    );
+    let pid = v["proposalId"].clone();
+    let mut ghost = propose;
+    ghost["change"]["workload"]["name"] = "ghost".into();
+    // Ошибка чтения кластера — с кодом приложения, как у инструментов чтения.
+    let msg = err(&call(&c, "propose_change", ghost).await);
+    assert!(msg.starts_with("NOT_FOUND:"), "{msg}");
+    let v = ok(call(&c, "list_proposals", json!({"connectionId": id})).await);
+    assert_eq!(
+        (&v["items"][0]["id"], &v["items"][0]["status"]),
+        (&pid, &json!("open")),
+        "{v}"
+    );
+    let v = ok(call(
+        &c,
+        "withdraw_proposal",
+        json!({"connectionId": id, "proposalId": pid}),
+    )
+    .await);
+    assert_eq!(v["withdrawn"], true);
+
     let calls: Vec<Value> = ctl.audit_recent(50);
     assert!(
         calls
````

- [ ] **Step 2: Реализация.**

<!-- block: file t13-dev-kind-metrics-yaml step=13.2 path=dev/kind/metrics.yaml -->
````yaml
# Поддельный Prometheus для автопоиска через прокси API-сервера (tests/it_metrics.rs). Service
# monitoring/prometheus-k8s:9090 — первый кандидат списка (metrics/discovery.rs). Прокси API-сервера ведёт
# только на адреса подов, поэтому за Service стоит под-заглушка с hostNetwork на ноде control-plane: её адрес —
# адрес ноды. setup.sh перенаправляет порт 19090 этого адреса на рабочую машину (DNAT в ноде), где тест
# отвечает записанными ответами Prometheus HTTP API: образов с HTTP-сервером на ноде нет. Deployment busy
# в team-a — нагрузка этих ответов: явные requests и limits (правила resources.* сравнивают с ними).
# Образ __HELPER_IMAGE__ — помощник local-path-provisioner (fixtures.yaml).
apiVersion: v1
kind: Namespace
metadata:
  name: monitoring
---
apiVersion: v1
kind: Service
metadata:
  name: prometheus-k8s
  namespace: monitoring
spec:
  selector: { app: prometheus-stub }
  ports:
    - { name: web, port: 9090, targetPort: 19090 }
---
apiVersion: v1
kind: Pod
metadata:
  name: prometheus-stub
  namespace: monitoring
  labels: { app: prometheus-stub }
spec:
  hostNetwork: true
  nodeSelector: { node-role.kubernetes.io/control-plane: "" }
  tolerations: [{ operator: Exists }]
  terminationGracePeriodSeconds: 1
  containers:
    - name: stub
      image: __HELPER_IMAGE__
      imagePullPolicy: IfNotPresent
      command: ["bash", "-c", "exec 3<> <(:); read -u 3"]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: busy
  namespace: team-a
spec:
  replicas: 2
  selector:
    matchLabels: { app: busy }
  template:
    metadata:
      labels: { app: busy }
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: app
          image: __HELPER_IMAGE__
          imagePullPolicy: IfNotPresent
          command: ["bash", "-c", "exec 3<> <(:); read -u 3"]
          resources:
            requests: { cpu: 10m, memory: 16Mi }
            limits: { cpu: 50m, memory: 32Mi }
````

<!-- block: file t13-dev-kind-ops-yaml step=13.2 path=dev/kind/ops.yaml -->
````yaml
# Объекты проекта team-a для изменений (tests/it_ops.rs): Deployment tuned меняют формы §9 — ресурсы,
# реплики, перезапуск; HPA tuned-hpa — границы. HPA нацелен на Deployment, которого нет, — контроллер не
# трогает реплики tuned. Образ __HELPER_IMAGE__ — помощник local-path-provisioner (fixtures.yaml).
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tuned
  namespace: team-a
spec:
  replicas: 1
  selector:
    matchLabels: { app: tuned }
  template:
    metadata:
      labels: { app: tuned }
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: app
          image: __HELPER_IMAGE__
          imagePullPolicy: IfNotPresent
          command: ["bash", "-c", "exec 3<> <(:); read -u 3"]
          resources:
            requests: { cpu: 10m, memory: 16Mi }
            limits: { memory: 64Mi }
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: tuned-hpa
  namespace: team-a
spec:
  scaleTargetRef: { apiVersion: apps/v1, kind: Deployment, name: idle }
  minReplicas: 1
  maxReplicas: 2
  metrics:
    - type: Resource
      resource: { name: cpu, target: { type: Utilization, averageUtilization: 80 } }
````

<!-- block: patch t13-src step=13.2 -->
````diff
diff --git a/dev/kind/README.md b/dev/kind/README.md
index 029f60a..9008acd 100644
--- a/dev/kind/README.md
+++ b/dev/kind/README.md
@@ -4,13 +4,22 @@
 (сертификат, токен и exec-плагин — скрипт, который выдаёт токен сервисной учётной записи), платформу `kubernetes`,
 права и импорт kubeconfig (`tests/it_connect.rs`), чтение кластера (`tests/it_read.rs`): проблемные поды
 (CrashLoopBackOff, ImagePullBackOff, Pending), workloads, события, сеть, конфигурацию, PVC, квоты, YAML, логи и
-watch, — проверку кластера (`tests/it_health.rs`): правила §8.2 на настоящем API-сервере, — и MCP-шлюз
-(`tests/it_mcp.rs`): клиент rmcp через `mcp.json` и рукопожатие вызывает инструменты в области LLM. Объекты проекта team-a —
-в `fixtures.yaml` и `health.yaml`; образы не скачиваются: контейнеры запускает образ-помощник local-path-provisioner
+watch, — проверку кластера (`tests/it_health.rs`): правила §8.2 на настоящем API-сервере, — изменения §9
+(`tests/it_ops.rs`): предпросмотр с dry-run и применение каждого вида изменения, `CONFLICT`, стражи, — источник
+метрик (`tests/it_metrics.rs`): автопоиск Prometheus через прокси API-сервера, статистика, рекомендация и правила
+`resources.*`, — и MCP-шлюз (`tests/it_mcp.rs`): клиент rmcp через `mcp.json` и рукопожатие вызывает инструменты в
+области LLM, включая ресурсы и предложения изменений. Объекты проекта team-a — в `fixtures.yaml`, `health.yaml`,
+`ops.yaml` и `metrics.yaml`; образы не скачиваются: контейнеры запускает образ-помощник local-path-provisioner
 (bash), который уже есть на ноде. `crds.yaml` — минимальные CRD Route и Gateway API без контроллеров: группы API есть,
 статус маршрута Gateway API пишет `setup.sh`, у Route статуса нет (роутера нет). Группы `config.openshift.io` нет —
-платформа стенда `kubernetes`. Остальное OpenShift (проекты, OAuth, операторы) проверяет смоук на OpenShift Local или
-dev-кластере (`docs/smoke/o1a.md`, `docs/smoke/o1b.md`, `docs/smoke/o2.md`).
+платформа стенда `kubernetes`. Остальное OpenShift (проекты, OAuth, операторы, Thanos Querier и настоящие метрики)
+проверяет смоук на OpenShift Local или dev-кластере (`docs/smoke/o1a.md`, `docs/smoke/o1b.md`, `docs/smoke/o2.md`,
+`docs/smoke/o3.md`).
+
+Prometheus на стенде поддельный: прокси API-сервера ведёт только на адреса подов, поэтому за Service
+`monitoring/prometheus-k8s` стоит под-заглушка с hostNetwork на ноде control-plane, а `setup.sh` перенаправляет порт
+19090 её адреса (DNAT в ноде) на рабочую машину — там записанными ответами Prometheus HTTP API отвечает сам тест
+(`tests/common/prometheus.rs`). Порт 19090 рабочей машины на время `it_metrics` и `it_mcp` должен быть свободен.
 
 ```bash
 brew install kind            # или https://kind.sigs.k8s.io/docs/user/quick-start/
diff --git a/dev/kind/setup.sh b/dev/kind/setup.sh
index 46757ef..4577ab6 100755
--- a/dev/kind/setup.sh
+++ b/dev/kind/setup.sh
@@ -1,8 +1,9 @@
 #!/usr/bin/env bash
 # Стенд интеграционных тестов на kind: проект team-a, сервисная учётная запись glafira-dev с ролью edit
-# только в нём, объекты проекта для тестов чтения (fixtures.yaml) и проверки (crds.yaml, health.yaml) и
-# kubeconfig dev/kind/out/kubeconfig с контекстами admin (сертификат kind) и dev-token (токен glafira-dev,
-# проект team-a).
+# только в нём, объекты проекта для тестов чтения (fixtures.yaml), проверки (crds.yaml, health.yaml) и
+# изменений (ops.yaml), поддельный Prometheus (metrics.yaml) с перенаправлением на рабочую машину (адрес —
+# в out/host-address) и kubeconfig dev/kind/out/kubeconfig с контекстами admin (сертификат kind) и
+# dev-token (токен glafira-dev, проект team-a).
 # Перед запуском: kind create cluster --name glafira. KUBECONFIG — отдельный файл, не ~/.kube/config:
 # export KUBECONFIG="$PWD/dev/kind/out/kind-admin.kubeconfig" (до kind create cluster; не out/kubeconfig,
 # его пишет этот скрипт).
@@ -33,6 +34,27 @@ sed "s|__HELPER_IMAGE__|$helper|g" "$here/fixtures.yaml" | k apply -f -
 k -n team-a rollout status deployment/web --timeout=180s
 k -n team-a wait --for=condition=complete job/migrate --timeout=180s
 k -n team-a rollout status deployment/logger --timeout=180s
+sed "s|__HELPER_IMAGE__|$helper|g" "$here/ops.yaml" | k apply -f -
+k -n team-a rollout status deployment/tuned --timeout=180s
+sed "s|__HELPER_IMAGE__|$helper|g" "$here/metrics.yaml" | k apply -f -
+k -n monitoring wait --for=condition=ready pod/prometheus-stub --timeout=180s
+k -n team-a rollout status deployment/busy --timeout=180s
+# Адрес рабочей машины, видимый из ноды: на macOS — host.docker.internal, на Linux — шлюз сети kind.
+node="$cluster-control-plane"
+host="$(docker exec "$node" getent ahostsv4 host.docker.internal 2>/dev/null | awk 'NR == 1 { print $1 }' || true)"
+if [ -z "$host" ]; then
+  host="$(docker network inspect kind -f '{{range .IPAM.Config}}{{.Gateway}} {{end}}' | tr ' ' '\n' | grep -m1 '\.' || true)"
+fi
+if [ -z "$host" ]; then
+  echo "не найден адрес рабочей машины для поддельного Prometheus" >&2
+  exit 1
+fi
+echo "$host" > "$out/host-address"
+# Прокси API-сервера (на той же ноде) идёт на адрес заглушки: порт 19090 — на рабочую машину.
+stub="$(k -n monitoring get pod prometheus-stub -o jsonpath='{.status.podIP}')"
+rule=(OUTPUT -d "$stub/32" -p tcp --dport 19090 -m comment --comment glafira-prometheus-stub -j DNAT
+  --to-destination "$host:19090")
+docker exec "$node" iptables -t nat -C "${rule[@]}" 2>/dev/null || docker exec "$node" iptables -t nat -I "${rule[@]}"
 k apply -f "$here/health.yaml"
 # Контроллера Gateway API на kind нет: статус маршрута пишет стенд — шлюз маршрут принял, а бэкенда нет.
 k -n team-a patch httproute api --subresource=status --type=merge -p '{"status":{"parents":[{
````

- [ ] **Step 3: Проверки.**

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 198`

Стенд kind (`dev/kind/README.md`; `KUBECONFIG` — отдельный файл, не `~/.kube/config`; после изменения стенда — заново `dev/kind/setup.sh`):  
Run: `GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --test it_connect`  
Expected: PASS — `test result: ok. 4 passed; 0 failed`

Стенд kind (`dev/kind/README.md`; `KUBECONFIG` — отдельный файл, не `~/.kube/config`; после изменения стенда — заново `dev/kind/setup.sh`):  
Run: `GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --test it_read`  
Expected: PASS — `test result: ok. 6 passed; 0 failed`

Стенд kind (`dev/kind/README.md`; `KUBECONFIG` — отдельный файл, не `~/.kube/config`; после изменения стенда — заново `dev/kind/setup.sh`):  
Run: `GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --test it_health`  
Expected: PASS — `test result: ok. 2 passed; 0 failed`

Стенд kind (`dev/kind/README.md`; `KUBECONFIG` — отдельный файл, не `~/.kube/config`; после изменения стенда — заново `dev/kind/setup.sh`):  
Run: `GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --test it_ops`  
Expected: PASS — `test result: ok. 1 passed; 0 failed`

Стенд kind (`dev/kind/README.md`; `KUBECONFIG` — отдельный файл, не `~/.kube/config`; после изменения стенда — заново `dev/kind/setup.sh`):  
Run: `GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --test it_metrics`  
Expected: PASS — `test result: ok. 1 passed; 0 failed`

Стенд kind (`dev/kind/README.md`; `KUBECONFIG` — отдельный файл, не `~/.kube/config`; после изменения стенда — заново `dev/kind/setup.sh`):  
Run: `GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --test it_mcp`  
Expected: PASS — `test result: ok. 1 passed; 0 failed`

- [ ] **Step 4: Commit.**

```bash
git add \
  dev/kind/README.md \
  dev/kind/metrics.yaml \
  dev/kind/ops.yaml \
  dev/kind/setup.sh \
  src-tauri/tests/common/mod.rs \
  src-tauri/tests/common/prometheus.rs \
  src-tauri/tests/it_mcp.rs \
  src-tauri/tests/it_metrics.rs \
  src-tauri/tests/it_ops.rs
git commit -s -m "test(kind): stand for changes and metrics — every change kind previewed, applied and conflicting on a real API server, a fake Prometheus behind the API server proxy for usage, recommendations and resources rules, MCP resources and proposals" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 14: Интерфейс: типы и API ресурсов, изменений и предложений; графики uPlot — худший под по контейнерам с линиями request и limit и отметками рестартов и OOM; количества Kubernetes

**Files:**
- Create: `src/components/resources/UsageChart.test.tsx`, `src/components/resources/UsageChart.tsx`, `src/components/resources/chart.test.ts`, `src/components/resources/chart.ts`, `src/lib/quantity.test.ts`, `src/lib/quantity.ts`
- Modify: `package.json`, `src/api/client.test.ts`, `src/api/client.ts`, `src/api/types.ts`, `src/components/Sidebar.test.tsx`, `src/components/cluster/ClusterOverview.test.tsx`, `src/components/cluster/ConnectionPanel.test.tsx`, `src/components/connections/tree/ConnectionNode.test.tsx`, `src/components/connections/tree/ConnectionTree.test.tsx`, `src/components/health/CheckBar.test.tsx`, `src/components/health/FindingsPanel.test.tsx`, `src/components/objects/PodList.test.tsx`, `src/components/objects/PodPanel.test.tsx`, `src/components/objects/WorkloadPanel.test.tsx`, `src/components/project/ProjectPanel.test.tsx`, `src/components/project/ProjectResources.test.tsx`, `src/components/project/access.test.ts`, `src/styles.css`, `src/test/health.ts`
- Generated: `pnpm-lock.yaml` (cargo, pnpm)

**Interfaces:**
- Consumes: задачи 4–12: команды `get_workload_usage`, `get_pod_usage`, `recommend_resources`, `get_project_usage`, `preview_change`, `apply_change`, `set_proposal_status`, `delete_proposal`, `delete_all_proposals`.
- Produces: `uplot` 1.6.32 (точная версия); TS: `Amounts`, `ContainerCurrent`, `ContainerStats`, `ContainerUsage`, `Charts`, `WorkloadUsage`, `PodUsage`, `Recommendation`, `RecommendationNote`, `Violation`, `Analysis`, `Risk`, `Excess`, `WorkloadResources`, `ProjectUsage`, `ProjectUsageEvent`, `ResourceValues`, `ContainerResources`, `Change`, `FieldChange`, `ChangeWarning`, `DryRun`, `ChangePreview`, `Proposal`, `ErrorCode` + `ADMISSION_DENIED`, `Verbs` + `patch`, `delete`, `create`, `HealthView.proposals`; `api.{getWorkloadUsage, getPodUsage, recommendResources, startProjectUsage, previewChange, applyChange, setProposalStatus, deleteProposal, deleteAllProposals}`; `lib/quantity.ts::{cpuQuantity, memoryQuantity, cpuText, memoryText, signedText, percentText}`; `components/resources/chart.ts::{ChartLine, chartData, themeColor}`, `UsageChart`.

Количества — как `format_cpu` и `format_memory` в Rust: «250m», «2», «512Mi», «2Gi»; их же форма отправит в Rust. Данные графика — общая ось времени из точек всех рядов, пропуски — `null`, линии request и limit, отметки — точкой на высоте худшего значения. Цвета — CSS-переменные темы (`--chart-1…6`, `--muted`, `--danger`, `--warn`); ширина следует за контейнером (`ResizeObserver`). Тесты мокают `uplot`: в jsdom нет canvas. Тесты, собирающие `HealthView` и `Verbs`, получают новые поля.

- [ ] **Step 1: Тесты.**

<!-- block: file t14-src-components-resources-usagechart-test-tsx step=14.1 path=src/components/resources/UsageChart.test.tsx -->
````tsx
import { render } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

const plots: { opts: { series: { label?: string }[] }; destroy: () => void }[] = [];
vi.mock("uplot", () => ({
  default: vi.fn().mockImplementation(function (this: object, opts: { series: { label?: string }[] }) {
    const p = { opts, destroy: vi.fn(), setSize: vi.fn() };
    plots.push(p);
    return p;
  }),
}));
globalThis.ResizeObserver ??= class {
  observe() {}
  disconnect() {}
  unobserve() {}
} as unknown as typeof ResizeObserver;

import { UsageChart } from "./UsageChart";

describe("UsageChart", () => {
  it("ряды контейнеров, линии request и limit, отметки; при размонтировании график уничтожается", () => {
    const { unmount } = render(
      <UsageChart
        title="CPU"
        series={[{ container: "app", points: [[1, 0.1]] }]}
        lines={[
          { label: "app request", value: 0.2, dash: "request" },
          { label: "app limit", value: 0.5, dash: "limit" },
        ]}
        markers={[]}
        markerLabel="Рестарты и OOM"
        format={String}
      />,
    );
    expect(plots[0].opts.series.map((s) => s.label)).toEqual([undefined, "app", "app request", "app limit", "Рестарты и OOM"]);
    unmount();
    expect(plots[0].destroy).toHaveBeenCalled();
  });
});
````

<!-- block: file t14-src-components-resources-chart-test-ts step=14.1 path=src/components/resources/chart.test.ts -->
````ts
import { describe, expect, it } from "vitest";
import { chartData } from "./chart";

describe("chartData", () => {
  it("общая ось времени, пропуски — null, линии и отметки", () => {
    const data = chartData(
      [
        { container: "app", points: [[100, 0.2], [400, 0.3]] },
        { container: "proxy", points: [[100, 0.05], [700, 0.04]] },
      ],
      [{ label: "app request", value: 0.25, dash: "request" }],
      [{ container: "app", time: 400, count: 1 }],
    );
    expect(data).toEqual([
      [100, 400, 700],
      [0.2, 0.3, null],
      [0.05, null, 0.04],
      [0.25, 0.25, 0.25],
      [null, 0.3, null],
    ]);
  });
});
````

<!-- block: file t14-src-lib-quantity-test-ts step=14.1 path=src/lib/quantity.test.ts -->
````ts
import { describe, expect, it } from "vitest";
import { cpuQuantity, cpuText, memoryQuantity, memoryText, percentText, signedText } from "./quantity";

const MI = 1024 * 1024;

describe("quantity", () => {
  it("CPU и память — как quantity из Rust", () => {
    expect([cpuQuantity(0.25), cpuQuantity(2), cpuQuantity(0.0123), cpuQuantity(1.5)]).toEqual(["250m", "2", "12m", "1500m"]);
    expect([memoryQuantity(512 * MI), memoryQuantity(2048 * MI), memoryQuantity(30.4 * MI)]).toEqual(["512Mi", "2Gi", "30Mi"]);
    expect([cpuText(null), memoryText(undefined)]).toEqual(["—", "—"]);
  });

  it("разница со знаком и доли", () => {
    expect([signedText(-1.2, cpuQuantity), signedText(512 * MI, memoryQuantity), signedText(0.0001, cpuQuantity)]).toEqual(["−1200m", "+512Mi", "0"]);
    expect([percentText(0.404), percentText(null)]).toEqual(["40 %", "—"]);
  });
});
````

<!-- block: patch t14-tests step=14.1 -->
````diff
diff --git a/src/api/client.test.ts b/src/api/client.test.ts
index a0d1951..95ae2f7 100644
--- a/src/api/client.test.ts
+++ b/src/api/client.test.ts
@@ -6,7 +6,7 @@ vi.mock("@tauri-apps/api/core", () => ({
 }));
 
 import { api, toAppError } from "./client";
-import type { AppError, ConnectionProfile } from "./types";
+import type { AppError, Change, ConnectionProfile } from "./types";
 
 beforeEach(() => {
   invoke.mockReset();
@@ -92,6 +92,22 @@ describe("api client", () => {
     await expect(api.listConnections()).rejects.toEqual(err);
   });
 
+  it("applyChange передаёт версию предпросмотра, подтверждения и предложение", async () => {
+    invoke.mockResolvedValue({});
+    const change: Change = { kind: "scale", namespace: "team-a", workload: { kind: "Deployment", name: "web" }, replicas: 3 };
+    await api.applyChange("c1", change, "4", "prod", null, "p1");
+    expect(invoke).toHaveBeenCalledWith("apply_change", {
+      connectionId: "c1",
+      change,
+      expectedVersion: "4",
+      confirm: "prod",
+      confirmPod: null,
+      proposalId: "p1",
+    });
+    await api.setProposalStatus("c1", "p1", "rejected");
+    expect(invoke).toHaveBeenLastCalledWith("set_proposal_status", { connectionId: "c1", proposalId: "p1", status: "rejected", reason: null });
+  });
+
   it("строковые ошибки превращает в INTERNAL", async () => {
     invoke.mockRejectedValue("boom");
     await expect(api.listConnections()).rejects.toEqual({ code: "INTERNAL", message: "boom", retryable: false });
diff --git a/src/components/Sidebar.test.tsx b/src/components/Sidebar.test.tsx
index 384a0c7..6a4b40d 100644
--- a/src/components/Sidebar.test.tsx
+++ b/src/components/Sidebar.test.tsx
@@ -11,7 +11,7 @@ vi.mock("../api/client", async (importOriginal) => {
   return {
     ...orig,
     api: {
-      getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] }),
+      getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }),
       listConnections: vi.fn().mockResolvedValue([{ ...newProfile(), id: "c", name: "local" }]),
       listFolders: vi.fn().mockResolvedValue([]),
       getMcpStatus: vi.fn().mockResolvedValue(mcpStatus()),
diff --git a/src/components/cluster/ClusterOverview.test.tsx b/src/components/cluster/ClusterOverview.test.tsx
index c7da45f..d26bfda 100644
--- a/src/components/cluster/ClusterOverview.test.tsx
+++ b/src/components/cluster/ClusterOverview.test.tsx
@@ -7,7 +7,7 @@ vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));
 
 vi.mock("../../api/client", async (importOriginal) => {
   const orig = await importOriginal<typeof import("../../api/client")>();
-  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] }), listConnections: vi.fn().mockResolvedValue([]), getClusterOverview: vi.fn(), listClusterOperators: vi.fn(), connect: vi.fn() } };
+  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }), listConnections: vi.fn().mockResolvedValue([]), getClusterOverview: vi.fn(), listClusterOperators: vi.fn(), connect: vi.fn() } };
 });
 
 import { openUrl } from "@tauri-apps/plugin-opener";
diff --git a/src/components/cluster/ConnectionPanel.test.tsx b/src/components/cluster/ConnectionPanel.test.tsx
index 1acb003..ab1133c 100644
--- a/src/components/cluster/ConnectionPanel.test.tsx
+++ b/src/components/cluster/ConnectionPanel.test.tsx
@@ -11,7 +11,7 @@ vi.mock("../../api/client", async (importOriginal) => {
   return {
     ...orig,
     api: {
-      getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] }),
+      getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }),
       listConnections: vi.fn(),
       connect: vi.fn(),
       startOauthLogin: vi.fn(),
diff --git a/src/components/connections/tree/ConnectionNode.test.tsx b/src/components/connections/tree/ConnectionNode.test.tsx
index f7c0c55..3f4b696 100644
--- a/src/components/connections/tree/ConnectionNode.test.tsx
+++ b/src/components/connections/tree/ConnectionNode.test.tsx
@@ -6,7 +6,7 @@ import { renderWithClient } from "../../../test/render";
 
 vi.mock("../../../api/client", async (importOriginal) => {
   const orig = await importOriginal<typeof import("../../../api/client")>();
-  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] }), listProjects: vi.fn(), connect: vi.fn() } };
+  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }), listProjects: vi.fn(), connect: vi.fn() } };
 });
 
 import { api } from "../../../api/client";
diff --git a/src/components/connections/tree/ConnectionTree.test.tsx b/src/components/connections/tree/ConnectionTree.test.tsx
index 5743c22..6e38b47 100644
--- a/src/components/connections/tree/ConnectionTree.test.tsx
+++ b/src/components/connections/tree/ConnectionTree.test.tsx
@@ -11,7 +11,7 @@ vi.mock("../../../api/client", async (importOriginal) => {
   return {
     ...orig,
     api: {
-      getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] }),
+      getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }),
       listFolders: vi.fn(),
       listConnections: vi.fn(),
       createFolder: vi.fn(),
diff --git a/src/components/health/CheckBar.test.tsx b/src/components/health/CheckBar.test.tsx
index 3eef3e4..41afc39 100644
--- a/src/components/health/CheckBar.test.tsx
+++ b/src/components/health/CheckBar.test.tsx
@@ -90,7 +90,7 @@ describe("CheckBar: ссылка «Все замечания»", () => {
   });
 
   it("пока проверок не было, ссылки нет", async () => {
-    vi.mocked(api.getHealth).mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] });
+    vi.mocked(api.getHealth).mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] });
     renderWithClient(<CheckBar connectionId="c1" allLink />);
     await screen.findByText("Кластер ещё не проверялся.");
     expect(screen.queryByRole("button", { name: "Все замечания" })).not.toBeInTheDocument();
diff --git a/src/components/health/FindingsPanel.test.tsx b/src/components/health/FindingsPanel.test.tsx
index a0128d5..a4fee5c 100644
--- a/src/components/health/FindingsPanel.test.tsx
+++ b/src/components/health/FindingsPanel.test.tsx
@@ -58,6 +58,7 @@ function view(p: Partial<HealthView> = {}): HealthView {
     },
     ignored: [],
     annotations: [],
+    proposals: [],
     ...p,
   };
 }
@@ -70,7 +71,7 @@ beforeEach(() => {
 
 describe("FindingsPanel", () => {
   it("запускает проверку и группирует находки по правилу", async () => {
-    vi.mocked(api.getHealth).mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] });
+    vi.mocked(api.getHealth).mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] });
     vi.mocked(api.runHealthCheck).mockResolvedValue(view());
     const user = userEvent.setup();
     renderWithClient(<FindingsPanel connectionId="c1" />);
diff --git a/src/components/objects/PodList.test.tsx b/src/components/objects/PodList.test.tsx
index 69a8da5..6933841 100644
--- a/src/components/objects/PodList.test.tsx
+++ b/src/components/objects/PodList.test.tsx
@@ -14,7 +14,7 @@ vi.mock("@tanstack/react-virtual", () => ({
 
 vi.mock("../../api/client", async (importOriginal) => {
   const orig = await importOriginal<typeof import("../../api/client")>();
-  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] }), startWatch: vi.fn(), stopSession: vi.fn(), listPods: vi.fn() } };
+  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }), startWatch: vi.fn(), stopSession: vi.fn(), listPods: vi.fn() } };
 });
 
 import { api } from "../../api/client";
diff --git a/src/components/objects/PodPanel.test.tsx b/src/components/objects/PodPanel.test.tsx
index 775b43e..9180a2f 100644
--- a/src/components/objects/PodPanel.test.tsx
+++ b/src/components/objects/PodPanel.test.tsx
@@ -12,7 +12,7 @@ vi.mock("@tanstack/react-virtual", () => ({
 
 vi.mock("../../api/client", async (importOriginal) => {
   const orig = await importOriginal<typeof import("../../api/client")>();
-  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] }), getPod: vi.fn(), listEvents: vi.fn(), getYaml: vi.fn() } };
+  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }), getPod: vi.fn(), listEvents: vi.fn(), getYaml: vi.fn() } };
 });
 
 import { api } from "../../api/client";
diff --git a/src/components/objects/WorkloadPanel.test.tsx b/src/components/objects/WorkloadPanel.test.tsx
index 48aaee1..545b99e 100644
--- a/src/components/objects/WorkloadPanel.test.tsx
+++ b/src/components/objects/WorkloadPanel.test.tsx
@@ -12,7 +12,7 @@ vi.mock("@tanstack/react-virtual", () => ({
 
 vi.mock("../../api/client", async (importOriginal) => {
   const orig = await importOriginal<typeof import("../../api/client")>();
-  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] }), getWorkload: vi.fn(), startWatch: vi.fn(), stopSession: vi.fn(), listEvents: vi.fn(), getYaml: vi.fn() } };
+  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }), getWorkload: vi.fn(), startWatch: vi.fn(), stopSession: vi.fn(), listEvents: vi.fn(), getYaml: vi.fn() } };
 });
 
 import { api } from "../../api/client";
diff --git a/src/components/project/ProjectPanel.test.tsx b/src/components/project/ProjectPanel.test.tsx
index d7ebe99..527d851 100644
--- a/src/components/project/ProjectPanel.test.tsx
+++ b/src/components/project/ProjectPanel.test.tsx
@@ -17,7 +17,7 @@ vi.mock("../../api/client", async (importOriginal) => {
   return {
     ...orig,
     api: {
-      getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] }),
+      getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }),
       getNamespaceAccess: vi.fn(),
       listPods: vi.fn(),
       listWorkloads: vi.fn(),
@@ -104,7 +104,7 @@ describe("ProjectPanel", () => {
 
   it("подвкладки без прав на чтение не показываются", async () => {
     vi.mocked(api.getNamespaceAccess).mockResolvedValue({
-      resources: { pods: { get: true, list: true, watch: false }, events: { get: false, list: false, watch: false } },
+      resources: { pods: { get: true, list: true, watch: false, patch: false, delete: false, create: false }, events: { get: false, list: false, watch: false, patch: false, delete: false, create: false } },
       incomplete: false,
     });
     renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
@@ -162,11 +162,11 @@ describe("права проекта: перечитывание", () => {
   afterEach(() => vi.useRealTimers());
 
   const READ_EVENTS_DENIED: NamespaceAccess = {
-    resources: { pods: { get: true, list: true, watch: true }, events: { get: false, list: false, watch: false } },
+    resources: { pods: { get: true, list: true, watch: true, patch: false, delete: false, create: false }, events: { get: false, list: false, watch: false, patch: false, delete: false, create: false } },
     incomplete: false,
   };
   const READ_EVENTS_GRANTED: NamespaceAccess = {
-    resources: { pods: { get: true, list: true, watch: true }, events: { get: true, list: true, watch: true } },
+    resources: { pods: { get: true, list: true, watch: true, patch: false, delete: false, create: false }, events: { get: true, list: true, watch: true, patch: false, delete: false, create: false } },
     incomplete: false,
   };
 
diff --git a/src/components/project/ProjectResources.test.tsx b/src/components/project/ProjectResources.test.tsx
index 0b123cb..5bc740b 100644
--- a/src/components/project/ProjectResources.test.tsx
+++ b/src/components/project/ProjectResources.test.tsx
@@ -5,7 +5,7 @@ import { renderWithClient } from "../../test/render";
 
 vi.mock("../../api/client", async (importOriginal) => {
   const orig = await importOriginal<typeof import("../../api/client")>();
-  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] }), getNetwork: vi.fn(), getConfig: vi.fn(), getConfigMapValues: vi.fn(), listPvcs: vi.fn(), getQuotas: vi.fn() } };
+  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }), getNetwork: vi.fn(), getConfig: vi.fn(), getConfigMapValues: vi.fn(), listPvcs: vi.fn(), getQuotas: vi.fn() } };
 });
 
 import { api } from "../../api/client";
diff --git a/src/components/project/access.test.ts b/src/components/project/access.test.ts
index 57cf51f..16b800f 100644
--- a/src/components/project/access.test.ts
+++ b/src/components/project/access.test.ts
@@ -2,7 +2,7 @@ import { describe, expect, it } from "vitest";
 import type { NamespaceAccess, Verbs } from "../../api/types";
 import { WORKLOAD_RESOURCES, canList } from "./access";
 
-const verbs = (list: boolean): Verbs => ({ get: list, list, watch: list });
+const verbs = (list: boolean): Verbs => ({ get: list, list, watch: list, patch: false, delete: false, create: false });
 const access = (resources: Record<string, Verbs>, incomplete = false): NamespaceAccess => ({ resources, incomplete });
 
 describe("canList", () => {
diff --git a/src/test/health.ts b/src/test/health.ts
index 8ae5fd7..2ae5840 100644
--- a/src/test/health.ts
+++ b/src/test/health.ts
@@ -1,4 +1,4 @@
-import type { Annotation, Finding, HealthTarget, HealthView } from "../api/types";
+import type { Annotation, Finding, HealthTarget, HealthView, Proposal } from "../api/types";
 
 /** Находка для тестов: ключ — из правила и объекта. */
 export function finding(ruleId: string, severity: Finding["severity"], target: HealthTarget, params: Record<string, unknown> = {}): Finding {
@@ -22,7 +22,7 @@ export function note(severity: Annotation["severity"], name: string, status: Ann
 }
 
 /** Состояние подключения после проверки `findings` (prod, интерфейс, 2 проекта). */
-export function checkedView(findings: Finding[], annotations: Annotation[] = [], ignored: string[] = []): HealthView {
+export function checkedView(findings: Finding[], annotations: Annotation[] = [], ignored: string[] = [], proposals: Proposal[] = []): HealthView {
   return {
     lastCheck: {
       runId: "r1",
@@ -36,5 +36,6 @@ export function checkedView(findings: Finding[], annotations: Annotation[] = [],
     },
     ignored,
     annotations,
+    proposals,
   };
 }
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/api/client.test.ts src/components/Sidebar.test.tsx src/components/cluster/ClusterOverview.test.tsx src/components/cluster/ConnectionPanel.test.tsx src/components/connections/tree/ConnectionNode.test.tsx src/components/connections/tree/ConnectionTree.test.tsx src/components/health/CheckBar.test.tsx src/components/health/FindingsPanel.test.tsx src/components/objects/PodList.test.tsx src/components/objects/PodPanel.test.tsx src/components/objects/WorkloadPanel.test.tsx src/components/project/ProjectPanel.test.tsx src/components/project/ProjectResources.test.tsx src/components/project/access.test.ts src/components/resources/UsageChart.test.tsx src/components/resources/chart.test.ts src/lib/quantity.test.ts`  
Expected: FAIL — нет `lib/quantity.ts`, `chart.ts` и `UsageChart`.

- [ ] **Step 3: Реализация.**

<!-- block: file t14-src-components-resources-usagechart-tsx step=14.3 path=src/components/resources/UsageChart.tsx -->
````tsx
import { useEffect, useRef } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import type { ChartMarker, ChartSeries } from "../../api/types";
import { type ChartLine, chartData, themeColor } from "./chart";

const PALETTE: [string, string][] = [
  ["--chart-1", "#3b82f6"],
  ["--chart-2", "#10b981"],
  ["--chart-3", "#8b5cf6"],
  ["--chart-4", "#f97316"],
  ["--chart-5", "#ec4899"],
  ["--chart-6", "#14b8a6"],
];

const HEIGHT = 220;

interface Props {
  title: string;
  series: ChartSeries[];
  lines: ChartLine[];
  markers: ChartMarker[];
  /** Подпись отметок: «Рестарты и OOM». */
  markerLabel: string;
  format: (v: number) => string;
}

/**
 * График потребления (спец. §7.4): ряд худшего пода по каждому контейнеру, линии текущих request и limit, отметки
 * рестартов и OOM. Цвета — из CSS-переменных темы; ширина следует за контейнером.
 */
export function UsageChart({ title, series, lines, markers, markerLabel, format }: Props) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const color = (i: number) => themeColor(...PALETTE[i % PALETTE.length]);
    const muted = themeColor("--muted", "#6b7280");
    const axis = { stroke: muted, grid: { stroke: themeColor("--border", "#e3e5e8") }, ticks: { stroke: muted } };
    const plot = new uPlot(
      {
        width: el.clientWidth || 600,
        height: HEIGHT,
        title,
        axes: [axis, { ...axis, size: 60, values: (_u: uPlot, vals: number[]) => vals.map(format) }],
        series: [
          {},
          ...series.map((s, i) => ({ label: s.container, stroke: color(i), width: 1.5, value: (_u: uPlot, v: number | null) => (v == null ? "—" : format(v)) })),
          ...lines.map((l) => ({
            label: l.label,
            stroke: l.dash === "limit" ? themeColor("--danger", "#dc2626") : muted,
            dash: l.dash === "limit" ? [6, 4] : [2, 4],
            width: 1,
            value: (_u: uPlot, v: number | null) => (v == null ? "—" : format(v)),
          })),
          { label: markerLabel, stroke: themeColor("--warn", "#f59e0b"), paths: () => null, points: { show: true, size: 8 } },
        ],
      },
      chartData(series, lines, markers) as uPlot.AlignedData,
      el,
    );
    const resize = new ResizeObserver(() => plot.setSize({ width: el.clientWidth || 600, height: HEIGHT }));
    resize.observe(el);
    return () => {
      resize.disconnect();
      plot.destroy();
    };
  }, [title, series, lines, markers, markerLabel, format]);
  return <div className="usage-chart" ref={ref} />;
}
````

<!-- block: file t14-src-components-resources-chart-ts step=14.3 path=src/components/resources/chart.ts -->
````ts
import type { ChartMarker, ChartSeries } from "../../api/types";

/** Горизонтальная линия графика: текущий request или limit контейнера. */
export interface ChartLine {
  label: string;
  value: number;
  dash: "request" | "limit";
}

/**
 * Данные uPlot: общая ось времени (объединение точек всех рядов), ряды контейнеров с `null` там, где точки нет,
 * линии request и limit по всей ширине и отметки (рестарты, OOM) — точкой на высоте худшего значения в этот момент.
 */
export function chartData(series: ChartSeries[], lines: ChartLine[], markers: ChartMarker[]): (number | null)[][] {
  const xs = [...new Set([...series.flatMap((s) => s.points.map(([t]) => t)), ...markers.map((m) => m.time)])].sort((a, b) => a - b);
  const rows = series.map((s) => {
    const at = new Map(s.points);
    return xs.map((x) => at.get(x) ?? null);
  });
  const top = xs.map((_, i) => Math.max(0, ...rows.map((r) => r[i] ?? 0)));
  const marked = new Set(markers.map((m) => m.time));
  return [xs, ...rows, ...lines.map((l) => xs.map(() => l.value)), xs.map((x, i) => (marked.has(x) ? top[i] : null))];
}

/** Цвет из CSS-переменной темы (`--chart-1`…); вне браузера — запасной. */
export function themeColor(name: string, fallback: string): string {
  const v = typeof document === "undefined" ? "" : getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}
````

<!-- block: file t14-src-lib-quantity-ts step=14.3 path=src/lib/quantity.ts -->
````ts
/** Количества Kubernetes в интерфейсе (спец. §7): CPU приходит в ядрах, память — в байтах. */

const MI = 1024 * 1024;
const GI = 1024 * MI;

/** CPU строкой quantity, как `format_cpu` в Rust: целые ядра — «2», иначе милли-ядра — «250m». */
export function cpuQuantity(cores: number): string {
  const milli = Math.round(cores * 1000);
  return milli % 1000 === 0 ? String(milli / 1000) : `${milli}m`;
}

/** Память строкой quantity, как `format_memory` в Rust: целые гибибайты — «2Gi», иначе мебибайты — «512Mi». */
export function memoryQuantity(bytes: number): string {
  const b = Math.round(bytes);
  return b !== 0 && b % GI === 0 ? `${b / GI}Gi` : `${Math.round(b / MI)}Mi`;
}

/** CPU для таблиц; нет значения — «—». */
export const cpuText = (cores: number | null | undefined) => (cores == null ? "—" : cpuQuantity(cores));

/** Память для таблиц; нет значения — «—». */
export const memoryText = (bytes: number | null | undefined) => (bytes == null ? "—" : memoryQuantity(bytes));

/** Разница со знаком: «+250m», «−512Mi», «0». */
export function signedText(value: number | null | undefined, text: (v: number) => string): string {
  if (value == null) return "—";
  const t = text(Math.abs(value));
  if (t === "0" || t === "0Mi" || t === "0m") return "0";
  return `${value < 0 ? "−" : "+"}${t}`;
}

/** Доля 0–1 процентами: «40 %». */
export const percentText = (share: number | null | undefined) => (share == null ? "—" : `${Math.round(share * 100)} %`);
````

<!-- block: patch t14-src step=14.3 -->
````diff
diff --git a/package.json b/package.json
index bc8b69a..ff55c40 100644
--- a/package.json
+++ b/package.json
@@ -27,6 +27,7 @@
     "react-dom": "^19.1.0",
     "react-i18next": "^15.7.4",
     "react-markdown": "9.1.0",
+    "uplot": "1.6.32",
     "zustand": "^5.0.15"
   },
   "devDependencies": {
diff --git a/src/api/client.ts b/src/api/client.ts
index fdafcbb..3696de3 100644
--- a/src/api/client.ts
+++ b/src/api/client.ts
@@ -1,7 +1,10 @@
 import { Channel, invoke } from "@tauri-apps/api/core";
 import type {
+  Analysis,
   AnnotationStatus,
   AppError,
+  Change,
+  ChangePreview,
   AppInfo,
   ClusterOverview,
   ConfigValue,
@@ -30,8 +33,11 @@ import type {
   OperatorRow,
   PodDetails,
   PodRow,
+  PodUsage,
   PolicyStatus,
   ProjectList,
+  ProjectUsageEvent,
+  ProposalStatus,
   PvcRow,
   QuotasView,
   SecretsInput,
@@ -44,6 +50,7 @@ import type {
   WorkloadKind,
   WorkloadList,
   WorkloadRef,
+  WorkloadUsage,
   YamlKind,
 } from "./types";
 
@@ -144,6 +151,34 @@ export const api = {
   /** Источник метрик подключения: выбранный и отвергнутые; `refresh` — выбрать заново. */
   getMetricsSourceStatus: (connectionId: string, refresh = false) =>
     call<MetricsSourceStatus>("get_metrics_source_status", { connectionId, refresh }),
+  /** Статистика workload'а за окно 1, 7, 14 или 30 дней. */
+  getWorkloadUsage: (connectionId: string, namespace: string, kind: WorkloadKind, name: string, windowDays: number) =>
+    call<WorkloadUsage>("get_workload_usage", { connectionId, namespace, kind, name, windowDays }),
+  getPodUsage: (connectionId: string, namespace: string, pod: string) => call<PodUsage>("get_pod_usage", { connectionId, namespace, pod }),
+  /** Статистика и рекомендация requests и limits (вкладка «Ресурсы» workload'а). */
+  recommendResources: (connectionId: string, namespace: string, kind: WorkloadKind, name: string, windowDays: number) =>
+    call<Analysis>("recommend_resources", { connectionId, namespace, kind, name, windowDays }),
+  /** Ресурсы проекта — фоновая сессия; итог приходит в `onEvent`. Возвращает id сессии для `stopSession`. */
+  startProjectUsage: (connectionId: string, namespace: string, windowDays: number, refresh: boolean, onEvent: (e: ProjectUsageEvent) => void) => {
+    const channel = new Channel<ProjectUsageEvent>();
+    channel.onmessage = onEvent;
+    return call<string>("get_project_usage", { connectionId, namespace, windowDays, refresh, onEvent: channel });
+  },
+
+  /** «Было → станет», предупреждения и server-side dry-run изменения (спец. §9). */
+  previewChange: (connectionId: string, change: Change) => call<ChangePreview>("preview_change", { connectionId, change }),
+  /**
+   * Применение после стражей: `confirm` — имя prod-подключения, `confirmPod` — имя пода без контроллера,
+   * `proposalId` — предложение LLM, которое применяется. Возвращает применённое изменение.
+   */
+  applyChange: (
+    connectionId: string,
+    change: Change,
+    expectedVersion: string | null,
+    confirm: string | null,
+    confirmPod: string | null,
+    proposalId: string | null,
+  ) => call<Change>("apply_change", { connectionId, change, expectedVersion, confirm, confirmPod, proposalId }),
 
   /** Watch списка проекта; события приходят в `onEvent`. Возвращает id сессии для `stopSession`. */
   startWatch: (params: WatchParams, onEvent: (e: WatchEvent) => void) => {
@@ -181,4 +216,9 @@ export const api = {
   deleteAnnotation: (connectionId: string, annotationId: string) =>
     call<HealthView>("delete_annotation", { connectionId, annotationId }),
   deleteAllAnnotations: (connectionId: string) => call<HealthView>("delete_all_annotations", { connectionId }),
+  /** «Отклонить» (с необязательной причиной) и «Открыть снова» предложения LLM. */
+  setProposalStatus: (connectionId: string, proposalId: string, status: ProposalStatus, reason: string | null = null) =>
+    call<HealthView>("set_proposal_status", { connectionId, proposalId, status, reason }),
+  deleteProposal: (connectionId: string, proposalId: string) => call<HealthView>("delete_proposal", { connectionId, proposalId }),
+  deleteAllProposals: (connectionId: string) => call<HealthView>("delete_all_proposals", { connectionId }),
 };
diff --git a/src/api/types.ts b/src/api/types.ts
index 63040c2..b4b04ed 100644
--- a/src/api/types.ts
+++ b/src/api/types.ts
@@ -13,6 +13,7 @@ export type ErrorCode =
   | "READ_ONLY"
   | "CONFIRMATION_REQUIRED"
   | "METRICS_UNAVAILABLE"
+  | "ADMISSION_DENIED"
   | "INTERNAL";
 
 /** Текст из Rust: ключ словаря, параметры и английский текст (запасной вариант). */
@@ -505,6 +506,12 @@ export interface Verbs {
   get: boolean;
   list: boolean;
   watch: boolean;
+  /** Изменения (§9): ресурсы, перезапуск, масштаб, границы HPA. */
+  patch: boolean;
+  /** Удаление пода. */
+  delete: boolean;
+  /** `instantiate` DeploymentConfig. */
+  create: boolean;
 }
 
 /** Права в проекте: `ресурс.группа` (`pods`, `deployments.apps`, `pods/log`) → права. */
@@ -1004,11 +1011,12 @@ export interface Annotation {
   closedAtMs?: number;
 }
 
-/** Последняя проверка, ключи скрытых находок и замечания LLM подключения. */
+/** Последняя проверка, ключи скрытых находок, замечания и предложения LLM подключения. */
 export interface HealthView {
   lastCheck: CheckRun | null;
   ignored: string[];
   annotations: Annotation[];
+  proposals: Proposal[];
 }
 
 /** Источник метрик (спец. §7.1). */
@@ -1041,3 +1049,274 @@ export interface MetricsSourceStatus {
   podMetrics: boolean;
   checkedAtMs: number;
 }
+
+// --- Ресурсы, изменения и предложения (этап O3). CPU — в ядрах, память — в байтах, время — секунды Unix.
+
+/** CPU и память; `null` — не задано или нет данных. */
+export interface Amounts {
+  cpu: number | null;
+  memory: number | null;
+}
+
+/** Действующие requests и limits контейнера из шаблона и значения `LimitRange` по умолчанию у работающих подов. */
+export interface ContainerCurrent {
+  name: string;
+  init: boolean;
+  sidecar: boolean;
+  requests: Amounts;
+  limits: Amounts;
+  limitRangeRequests: Amounts;
+  limitRangeLimits: Amounts;
+  envNames: string[];
+}
+
+export interface ContainerStats {
+  cpu: { p50: number | null; p95: number | null; p99: number | null; max: number | null };
+  memory: { p95: number | null; p99: number | null; max: number | null };
+  /** Доля периодов CFS с троттлингом, 0–1. */
+  throttling: { mean: number | null; p95: number | null };
+  /** `null` — нет kube-state-metrics. */
+  oomCount: number | null;
+  oomLast: number | null;
+  restarts: number | null;
+}
+
+export type ContainerUsage = ContainerCurrent & { stats: ContainerStats };
+
+export interface ChartSeries {
+  container: string;
+  /** [секунды Unix, значение]. */
+  points: [number, number][];
+}
+
+export interface ChartMarker {
+  container: string;
+  time: number;
+  count: number;
+}
+
+export interface Charts {
+  start: number;
+  end: number;
+  stepSeconds: number;
+  cpu: ChartSeries[];
+  memory: ChartSeries[];
+  restarts: ChartMarker[];
+  ooms: ChartMarker[];
+}
+
+export interface ContainerNow {
+  name: string;
+  cpu: number | null;
+  memory: number | null;
+}
+
+export interface PodNow {
+  pod: string;
+  containers: ContainerNow[];
+}
+
+/** Статистика workload'а за окно (спец. §7.2). */
+export interface WorkloadUsage {
+  namespace: string;
+  kind: WorkloadKind;
+  name: string;
+  windowDays: number;
+  source: MetricsSourceInfo;
+  sourceStatus: MetricsSourceStatus;
+  /** Доля шагов окна с данными, 0–1; у metrics.k8s.io — `null`. */
+  coverage: number | null;
+  dataSeconds: number | null;
+  ownerMetrics: boolean;
+  replicas: number | null;
+  surge: number;
+  containers: ContainerUsage[];
+  charts: Charts | null;
+  /** Только metrics.k8s.io: текущее потребление подов. */
+  podsNow: PodNow[];
+}
+
+export interface PodUsage {
+  source: MetricsSourceInfo;
+  containers: ContainerNow[];
+}
+
+export type Confidence = "low" | "medium" | "high";
+export type ResourceField = "cpuRequest" | "cpuLimit" | "memoryRequest" | "memoryLimit";
+export type LimitBound = "min" | "max" | "ratio";
+
+export type RecommendationNote =
+  | { code: "cpuLimitNotSet" }
+  | { code: "cpuThrottled"; p95: number }
+  | { code: "oomKilled"; count: number }
+  | { code: "noData" }
+  | { code: "limitRange"; field: ResourceField; bound: LimitBound }
+  | { code: "limitRangeDefault"; field: ResourceField }
+  | { code: "java" }
+  | { code: "go" };
+
+export type Violation =
+  | { code: "quota"; quota: string; resource: string; needed: number; free: number }
+  | { code: "nodeAllocatable"; container: string; resource: string; request: number; allocatable: number };
+
+export interface HpaShift {
+  hpa: string;
+  resource: string;
+  target: number;
+  equivalent: number;
+}
+
+export interface ResourceSet {
+  requests: Amounts;
+  limits: Amounts;
+}
+
+export interface ContainerRecommendation {
+  name: string;
+  current: ResourceSet;
+  /** `null` — данных нет. */
+  recommended: ResourceSet | null;
+  notes: RecommendationNote[];
+}
+
+/** Рекомендация requests и limits (спец. §7.3). */
+export interface Recommendation {
+  namespace: string;
+  kind: WorkloadKind;
+  name: string;
+  windowDays: number;
+  source: MetricsSourceInfo;
+  confidence: Confidence;
+  coverage: number | null;
+  dataSeconds: number | null;
+  replicas: number | null;
+  containers: ContainerRecommendation[];
+  violations: Violation[];
+  hpa: HpaShift[];
+  /** Рекомендовано минус сейчас, × реплик. */
+  totals: ResourceSet;
+}
+
+export interface Analysis {
+  usage: WorkloadUsage;
+  recommendation: Recommendation;
+}
+
+export type Risk =
+  | { code: "throttled"; container: string; mean: number }
+  | { code: "oom"; container: string; count: number }
+  | { code: "memoryNearLimit"; container: string; max: number; limit: number };
+
+export interface Excess {
+  container: string;
+  resource: "cpu" | "memory";
+  request: number;
+  usage: number;
+}
+
+/** Workload во вкладке «Ресурсы» проекта: суммы по работающим подам. */
+export interface WorkloadResources {
+  kind: WorkloadKind;
+  name: string;
+  pods: number;
+  requests: Amounts;
+  limits: Amounts;
+  usage: Amounts;
+  reserve: Amounts;
+  risks: Risk[];
+  excess: Excess[];
+}
+
+export interface QuotaRoom {
+  quota: string;
+  resource: string;
+  hard: number;
+  used: number;
+}
+
+export interface ProjectUsage {
+  namespace: string;
+  windowDays: number;
+  source: MetricsSourceInfo;
+  sourceStatus: MetricsSourceStatus;
+  workloads: WorkloadResources[];
+  errors: KindError[];
+  truncated: boolean;
+  quotas: QuotaRoom[];
+  collectedAtMs: number;
+}
+
+export type ProjectUsageEvent = { kind: "done"; usage: ProjectUsage } | { kind: "failed"; error: AppError };
+
+/** Значение quantity строкой; `null` — убрать; нет поля — не менять. */
+export interface ResourceValues {
+  cpu?: string | null;
+  memory?: string | null;
+}
+
+export interface ContainerResources {
+  name: string;
+  requests: ResourceValues;
+  limits: ResourceValues;
+}
+
+/** Изменение кластера (спец. §9): одна форма у интерфейса, предложений и LLM. */
+export type Change =
+  | { kind: "setResources"; namespace: string; workload: WorkloadRef; containers: ContainerResources[] }
+  | { kind: "scale"; namespace: string; workload: WorkloadRef; replicas: number }
+  | { kind: "rolloutRestart"; namespace: string; workload: WorkloadRef }
+  | { kind: "setHpaBounds"; namespace: string; hpa: string; minReplicas?: number; maxReplicas: number }
+  | { kind: "deletePod"; namespace: string; pod: string };
+
+export type ChangeKind = Change["kind"];
+
+/** Поле «было → станет»: `requests.cpu`, `replicas`, `restartedAt`, `pod`… */
+export interface FieldChange {
+  container: string | null;
+  field: string;
+  before: string | null;
+  after: string | null;
+}
+
+export type ChangeWarning =
+  | { code: "hpaManaged"; hpa: string }
+  | { code: "scaleToZero" }
+  | { code: "limitRange"; container: string; field: ResourceField; bound: LimitBound; limit: number }
+  | { code: "quota"; quota: string; resource: string; needed: number; free: number }
+  | { code: "orphan" };
+
+export type DryRun = { status: "passed" } | { status: "rejected"; error: AppError } | { status: "skipped" };
+
+export interface ChangePreview {
+  change: Change;
+  /** Версия объекта (generation, у пода uid): с ней идёт применение. */
+  version: string | null;
+  diff: FieldChange[];
+  warnings: ChangeWarning[];
+  dryRun: DryRun;
+  /** Под без контроллера: при применении нужно ввести его имя. */
+  confirmPodName: boolean;
+}
+
+export type ProposalStatus = "open" | "applied" | "rejected" | "withdrawn";
+
+/** Предложение LLM (спец. §10.5): изменение, которое человек применяет формой. */
+export interface Proposal {
+  id: string;
+  severity: Severity;
+  title: string;
+  rationale: string;
+  target: HealthTarget;
+  change: Change;
+  relatedRuleId?: string;
+  relatedFindingKey?: string;
+  client: string | null;
+  sessionId?: string;
+  createdAtMs: number;
+  updatedAtMs: number;
+  status: ProposalStatus;
+  closedAtMs?: number;
+  appliedAsProposed?: boolean;
+  appliedChange?: Change;
+  rejectReason?: string;
+}
diff --git a/src/styles.css b/src/styles.css
index 7df3b75..6702bf0 100644
--- a/src/styles.css
+++ b/src/styles.css
@@ -7,6 +7,12 @@
   --accent: #3b82f6;
   --danger: #dc2626;
   --warn: #f59e0b;
+  --chart-1: #3b82f6;
+  --chart-2: #10b981;
+  --chart-3: #8b5cf6;
+  --chart-4: #f97316;
+  --chart-5: #ec4899;
+  --chart-6: #14b8a6;
   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
   font-size: 13px;
   color: var(--fg);
@@ -191,3 +197,13 @@ label.inline { display: inline-flex; gap: 4px; align-items: center; white-space:
 .health-badge { margin-left: 4px; }
 .health-block { border: 1px solid var(--border); border-radius: 6px; padding: 8px; }
 .health-block h3 { margin: 0 0 4px; font-size: 14px; }
+
+/* Ресурсы (этап O3): графики uPlot и таблицы «сейчас → рекомендация». */
+.usage-chart { width: 100%; min-height: 220px; }
+.usage-chart .u-title { font-size: 13px; font-weight: 500; }
+.usage-chart .u-legend { font-size: 12px; color: var(--fg); }
+.resource-table td.num, .resource-table th.num { text-align: right; white-space: nowrap; }
+.resource-table td.changed { font-weight: 600; }
+.confidence-low { color: var(--warn); }
+.risk { color: var(--danger); }
+.diff-table td.after { font-weight: 600; }
````

<!-- block: shell t14-install step=14.3 -->
````bash
pnpm install
````

- [ ] **Step 4: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 66 passed`, `Tests 484 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 198`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 5: Commit.**

```bash
git add \
  package.json \
  pnpm-lock.yaml \
  src/api/client.test.ts \
  src/api/client.ts \
  src/api/types.ts \
  src/components/Sidebar.test.tsx \
  src/components/cluster/ClusterOverview.test.tsx \
  src/components/cluster/ConnectionPanel.test.tsx \
  src/components/connections/tree/ConnectionNode.test.tsx \
  src/components/connections/tree/ConnectionTree.test.tsx \
  src/components/health/CheckBar.test.tsx \
  src/components/health/FindingsPanel.test.tsx \
  src/components/objects/PodList.test.tsx \
  src/components/objects/PodPanel.test.tsx \
  src/components/objects/WorkloadPanel.test.tsx \
  src/components/project/ProjectPanel.test.tsx \
  src/components/project/ProjectResources.test.tsx \
  src/components/project/access.test.ts \
  src/components/resources/UsageChart.test.tsx \
  src/components/resources/UsageChart.tsx \
  src/components/resources/chart.test.ts \
  src/components/resources/chart.ts \
  src/lib/quantity.test.ts \
  src/lib/quantity.ts \
  src/styles.css \
  src/test/health.ts
git commit -s -m "feat(ui): IPC types and API of resources, changes and proposals; uPlot charts of the worst pod per container with request and limit lines and restart and OOM marks; Kubernetes quantities in the interface" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 15: Формы изменений поверх экрана: ресурсы контейнеров, реплики, перезапуск, границы HPA и удаление пода с «было → станет», предупреждениями и dry-run, подтверждения prod и имени пода, «Обновить и повторить» при CONFLICT; кнопки без прав неактивны

**Files:**
- Port: `src/components/common/ConfirmDialog.test.tsx` (с правками), `src/components/common/ConfirmDialog.tsx` (с правками), `src/lib/useConnection.ts`
- Create: `src/components/change/ChangeActions.tsx`, `src/components/change/ChangeDialog.test.tsx`, `src/components/change/ChangeDialog.tsx`, `src/components/change/changes.test.ts`, `src/components/change/changes.ts`
- Modify: `PORTED.md`, `docs/i18n/same-as-en.json`, `src/api/types.ts`, `src/components/objects/PodPanel.tsx`, `src/components/objects/WorkloadPanel.tsx`, `src/locales/en.json`, `src/locales/ru.json`, `src/styles.css`

**Interfaces:**
- Consumes: задача 14: `Change`, `ChangePreview`, `api.{previewChange, applyChange}`, `NamespaceAccess`; O1b: `useNamespaceAccess`, `focusTrap`, `.modal`; Kafka-версия @ `8bbd0bd`: `ConfirmDialog`, `useConnection`.
- Produces: `components/common/ConfirmDialog.tsx::{ConfirmDialog, needsConfirmation}`, `Confirmation { confirm, typedName }`; `lib/useConnection.ts::useConnection`; `components/change/changes.ts::{WORKLOAD_RESOURCE, canSetResources, canScale, canRestart, changeRight, allowed, withValues, hasValues, fieldAmount, quotaAmount, warningText, changeObject}`; `ChangeDialog({ connectionId, initial, hints?, notice?, proposal?, onClose })`, `ResourceHints`; `ChangeActions.tsx::{hintsOf, emptyResources, ChangeButton, WorkloadActions, PodActions}`.

Решение пользователя: форма — диалог поверх экрана. Поля ресурсов: пустое — «не менять» (в подсказке — текущее значение шаблона), «✕» — убрать (`null`); строки без значений не отправляются. Любая правка сбрасывает предпросмотр. «Проверить» показывает diff, предупреждения и итог dry-run (отказ — без «Применить»); подтверждение — `ConfirmDialog` Kafka-версии (имя prod-подключения, имя пода без контроллера). `CONFLICT` даёт «Обновить и повторить». Кнопки на обзоре workload'а (решение пользователя: форма ресурсов открывается и без рекомендации) и пода неактивны без права (`patch` подресурса `scale`, `create` у `instantiate` DeploymentConfig, `delete` подов) и на подключении только для чтения. Термины Kubernetes в ru (`CPU request` и др.) — в `docs/i18n/same-as-en.json`.

- [ ] **Step 1: Перенос из Kafka-версии.**

Файлы копируются с коммита `8bbd0bd` репозитория `~/kafka-explorer` (`$KAFKA_REPO`) и правятся патчами — против перенесённого файла. Строки о них в `PORTED.md` добавляет шаг реализации.

<!-- block: shell t15-port step=15.1 -->
````bash
scripts/port.sh src/components/common/ConfirmDialog.tsx src/components/common/ConfirmDialog.tsx
scripts/port.sh src/components/common/ConfirmDialog.test.tsx src/components/common/ConfirmDialog.test.tsx
scripts/port.sh src/lib/useConnection.ts src/lib/useConnection.ts
````

<!-- block: patch t15-port-src-components-common-confirmdialog-tsx step=15.1 -->
````diff
diff --git a/src/components/common/ConfirmDialog.tsx b/src/components/common/ConfirmDialog.tsx
--- a/src/components/common/ConfirmDialog.tsx
+++ b/src/components/common/ConfirmDialog.tsx
@@ -2,7 +2,7 @@ import { type KeyboardEvent, type ReactNode, useEffect, useRef, useState } from
 import { Trans, useTranslation } from "react-i18next";
 import type { Confirmation, ConnectionProfile } from "../../api/types";
 
-/** Нужен ли диалог: prod-подключение или операция с вводом имени объекта (удаление, purge). */
+/** Нужен ли диалог: prod-подключение или операция с вводом имени объекта (удаление пода без контроллера). */
 export function needsConfirmation(connection: ConnectionProfile, objectName?: string): boolean {
   return connection.environment === "prod" || objectName !== undefined;
 }
@@ -13,7 +13,7 @@ interface Props {
   confirmLabel: string;
   /** Имя объекта, которое нужно ввести для подтверждения. */
   objectName?: string;
-  /** Что именно вводится: «имя топика», «число топиков», «id группы». */
+  /** Что именно вводится: «имя пода». */
   objectLabel?: string;
   danger?: boolean;
   busy?: boolean;
````

<!-- block: patch t15-port-src-components-common-confirmdialog-test-tsx step=15.1 -->
````diff
diff --git a/src/components/common/ConfirmDialog.test.tsx b/src/components/common/ConfirmDialog.test.tsx
--- a/src/components/common/ConfirmDialog.test.tsx
+++ b/src/components/common/ConfirmDialog.test.tsx
@@ -13,34 +13,34 @@ describe("ConfirmDialog", () => {
     const user = userEvent.setup();
     const onConfirm = vi.fn();
     render(
-      <ConfirmDialog connection={dev} title="Удалить топик?" objectName="orders" objectLabel="имя топика" confirmLabel="Удалить" danger onConfirm={onConfirm} onCancel={() => {}} />,
+      <ConfirmDialog connection={dev} title="Удалить под?" objectName="web-1" objectLabel="имя пода" confirmLabel="Удалить" danger onConfirm={onConfirm} onCancel={() => {}} />,
     );
     const button = screen.getByRole("button", { name: "Удалить" });
     expect(button).toBeDisabled();
     expect(screen.queryByLabelText("Имя подключения")).not.toBeInTheDocument();
-    await user.type(screen.getByLabelText("Подтверждение: имя топика"), "order");
+    await user.type(screen.getByLabelText("Подтверждение: имя пода"), "web-");
     expect(button).toBeDisabled();
-    await user.type(screen.getByLabelText("Подтверждение: имя топика"), "s");
+    await user.type(screen.getByLabelText("Подтверждение: имя пода"), "1");
     await user.click(button);
-    expect(onConfirm).toHaveBeenCalledWith({ confirm: null, typedName: "orders" });
+    expect(onConfirm).toHaveBeenCalledWith({ confirm: null, typedName: "web-1" });
   });
 
   it("имя объекта с пробелом на конце подтверждается (trim с обеих сторон)", async () => {
     const user = userEvent.setup();
     const onConfirm = vi.fn();
     render(
-      <ConfirmDialog connection={dev} title="Удалить группу?" objectName="grp " objectLabel="id группы" confirmLabel="Удалить" danger onConfirm={onConfirm} onCancel={() => {}} />,
+      <ConfirmDialog connection={dev} title="Удалить под?" objectName="web-1 " objectLabel="имя пода" confirmLabel="Удалить" danger onConfirm={onConfirm} onCancel={() => {}} />,
     );
-    await user.type(screen.getByLabelText("Подтверждение: id группы"), "grp ");
+    await user.type(screen.getByLabelText("Подтверждение: имя пода"), "web-1 ");
     await user.click(screen.getByRole("button", { name: "Удалить" }));
-    expect(onConfirm).toHaveBeenCalledWith({ confirm: null, typedName: "grp" });
+    expect(onConfirm).toHaveBeenCalledWith({ confirm: null, typedName: "web-1" });
   });
 
   it("для prod требует имя подключения", async () => {
     const user = userEvent.setup();
     const onConfirm = vi.fn();
-    render(<ConfirmDialog connection={prod} title="Создать топик?" confirmLabel="Создать" onConfirm={onConfirm} onCancel={() => {}} />);
-    const button = screen.getByRole("button", { name: "Создать" });
+    render(<ConfirmDialog connection={prod} title="Масштабировать?" confirmLabel="Применить" onConfirm={onConfirm} onCancel={() => {}} />);
+    const button = screen.getByRole("button", { name: "Применить" });
     expect(button).toBeDisabled();
     await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
     await user.click(button);
@@ -51,20 +51,20 @@ describe("ConfirmDialog", () => {
     const user = userEvent.setup();
     const onConfirm = vi.fn();
     render(
-      <ConfirmDialog connection={prod} title="Удалить топик?" objectName="orders" objectLabel="имя топика" confirmLabel="Удалить" danger onConfirm={onConfirm} onCancel={() => {}} />,
+      <ConfirmDialog connection={prod} title="Удалить под?" objectName="web-1" objectLabel="имя пода" confirmLabel="Удалить" danger onConfirm={onConfirm} onCancel={() => {}} />,
     );
     const button = screen.getByRole("button", { name: "Удалить" });
-    await user.type(screen.getByLabelText("Подтверждение: имя топика"), "orders");
+    await user.type(screen.getByLabelText("Подтверждение: имя пода"), "web-1");
     expect(button).toBeDisabled();
     await user.type(screen.getByLabelText("Имя подключения"), "prod-");
     expect(button).toBeDisabled();
     await user.type(screen.getByLabelText("Имя подключения"), "1");
     expect(button).toBeEnabled();
-    await user.clear(screen.getByLabelText("Подтверждение: имя топика"));
+    await user.clear(screen.getByLabelText("Подтверждение: имя пода"));
     expect(button).toBeDisabled();
-    await user.type(screen.getByLabelText("Подтверждение: имя топика"), "orders");
+    await user.type(screen.getByLabelText("Подтверждение: имя пода"), "web-1");
     await user.click(button);
-    expect(onConfirm).toHaveBeenCalledWith({ confirm: "prod-1", typedName: "orders" });
+    expect(onConfirm).toHaveBeenCalledWith({ confirm: "prod-1", typedName: "web-1" });
   });
 
   it("отмена", async () => {
@@ -107,7 +107,7 @@ describe("ConfirmDialog", () => {
 
   it("needsConfirmation", () => {
     expect(needsConfirmation(dev)).toBe(false);
-    expect(needsConfirmation(dev, "orders")).toBe(true);
+    expect(needsConfirmation(dev, "web-1")).toBe(true);
     expect(needsConfirmation(prod)).toBe(true);
   });
 });
````

- [ ] **Step 2: Тесты.**

<!-- block: file t15-src-components-change-changedialog-test-tsx step=15.2 path=src/components/change/ChangeDialog.test.tsx -->
````tsx
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { listConnections: vi.fn(), previewChange: vi.fn(), applyChange: vi.fn(), getNamespaceAccess: vi.fn() } };
});

import { api } from "../../api/client";
import type { AppError, Change, ChangePreview, ConnectionProfile, WorkloadDetails } from "../../api/types";
import { workloadRow } from "../../test/objects";
import { newProfile } from "../connections/profile";
import { WorkloadActions } from "./ChangeActions";
import { ChangeDialog } from "./ChangeDialog";

const dev: ConnectionProfile = { ...newProfile(), id: "c1", name: "dev", environment: "dev" };
const web = { kind: "Deployment", name: "web" } as const;
const resources: Change = {
  kind: "setResources",
  namespace: "team-a",
  workload: web,
  containers: [
    { name: "app", requests: {}, limits: {} },
    { name: "proxy", requests: {}, limits: {} },
  ],
};
const preview = (change: Change, p: Partial<ChangePreview> = {}): ChangePreview => ({
  change,
  version: "4",
  diff: [{ container: "app", field: "requests.cpu", before: "500m", after: "250m" }],
  warnings: [],
  dryRun: { status: "passed" },
  confirmPodName: false,
  ...p,
});

beforeEach(() => {
  vi.mocked(api.listConnections).mockResolvedValue([dev]);
  vi.mocked(api.previewChange).mockReset();
  vi.mocked(api.applyChange).mockReset();
});

describe("ChangeDialog", () => {
  it("ресурсы: отправляются только заполненные поля, «было → станет», применение с версией предпросмотра", async () => {
    vi.mocked(api.previewChange).mockImplementation(async (_id, c) => preview(c));
    vi.mocked(api.applyChange).mockImplementation(async (_id, c) => c);
    const onClose = vi.fn();
    const user = userEvent.setup();
    renderWithClient(<ChangeDialog connectionId="c1" initial={resources} hints={{ app: { requests: { cpu: "500m" }, limits: {} } }} onClose={onClose} />);
    expect(screen.getByRole("dialog", { name: "Ресурсы контейнеров: Deployment web" })).toBeInTheDocument();
    expect(screen.getByLabelText("app CPU request")).toHaveAttribute("placeholder", "сейчас 500m");
    await user.type(screen.getByLabelText("app CPU request"), "250m");
    await user.click(screen.getByRole("button", { name: "Убрать значение: app CPU limit" }));
    await user.click(screen.getByRole("button", { name: "Проверить" }));
    const sent: Change = {
      kind: "setResources",
      namespace: "team-a",
      workload: web,
      containers: [{ name: "app", requests: { cpu: "250m" }, limits: { cpu: null } }],
    };
    expect(api.previewChange).toHaveBeenCalledWith("c1", sent);
    expect(await screen.findByText("500m")).toBeInTheDocument();
    expect(screen.getByText("API-сервер принял изменение (dry-run)", { exact: false })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Применить" }));
    await waitFor(() => expect(onClose).toHaveBeenCalled());
    expect(api.applyChange).toHaveBeenCalledWith("c1", sent, "4", null, null, null);
  });

  it("правка формы сбрасывает предпросмотр; CONFLICT — «Обновить и повторить»", async () => {
    const conflict: AppError = { code: "CONFLICT", key: "errors.ops.changed", message: "The object has changed", retryable: false };
    vi.mocked(api.previewChange).mockImplementation(async (_id, c) => preview(c));
    vi.mocked(api.applyChange).mockRejectedValue(conflict);
    const user = userEvent.setup();
    const scale: Change = { kind: "scale", namespace: "team-a", workload: web, replicas: 2 };
    renderWithClient(<ChangeDialog connectionId="c1" initial={scale} onClose={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Проверить" }));
    await user.click(await screen.findByRole("button", { name: "Применить" }));
    await user.click(await screen.findByRole("button", { name: "Обновить и повторить" }));
    expect(screen.queryByRole("button", { name: "Применить" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Проверить" }));
    expect(api.previewChange).toHaveBeenCalledTimes(2);
    await user.clear(screen.getByLabelText("Реплики"));
    await user.type(screen.getByLabelText("Реплики"), "5");
    expect(screen.getByRole("button", { name: "Проверить" })).toBeInTheDocument();
  });

  it("под без контроллера удаляется только с его именем; prod — с именем подключения", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([{ ...dev, name: "prod-1", environment: "prod" }]);
    const del: Change = { kind: "deletePod", namespace: "team-a", pod: "solo" };
    vi.mocked(api.previewChange).mockResolvedValue(
      preview(del, { version: "u1", diff: [{ container: null, field: "pod", before: "solo", after: null }], warnings: [{ code: "orphan" }], confirmPodName: true }),
    );
    vi.mocked(api.applyChange).mockResolvedValue(del);
    const user = userEvent.setup();
    renderWithClient(<ChangeDialog connectionId="c1" initial={del} onClose={() => {}} />);
    await user.click(await screen.findByRole("button", { name: "Проверить" }));
    expect(await screen.findByText("У пода нет контроллера: заново его никто не создаст")).toBeInTheDocument();
    const button = screen.getByRole("button", { name: "Удалить" });
    await user.type(screen.getByLabelText("Подтверждение: имя пода"), "solo");
    expect(button).toBeDisabled();
    await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
    await user.click(button);
    await waitFor(() => expect(api.applyChange).toHaveBeenCalledWith("c1", del, "u1", "prod-1", "solo", null));
  });

  it("отказ dry-run виден до подтверждения, применить нельзя", async () => {
    const denied: AppError = { code: "ADMISSION_DENIED", key: "errors.ops.admissionDenied", params: { message: "quota exceeded" }, message: "rejected", retryable: false };
    const restart: Change = { kind: "rolloutRestart", namespace: "team-a", workload: web };
    vi.mocked(api.previewChange).mockResolvedValue(preview(restart, { dryRun: { status: "rejected", error: denied } }));
    const user = userEvent.setup();
    renderWithClient(<ChangeDialog connectionId="c1" initial={restart} onClose={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Проверить" }));
    expect(await screen.findByText("API-сервер отклонил изменение")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Применить" })).not.toBeInTheDocument();
  });
});

describe("WorkloadActions", () => {
  const details: WorkloadDetails = {
    workload: workloadRow("web", { desired: 2 }),
    conditions: [],
    selector: null,
    strategy: null,
    containers: [],
    labels: {},
    annotations: {},
    activeJobs: [],
    hpa: { data: { name: "web-hpa", minReplicas: 2, maxReplicas: 6, currentReplicas: 2, desiredReplicas: 2, metrics: [], conditions: [] } },
    pdb: { data: null },
    history: null,
  };
  const verbs = (patch: boolean) => ({ get: true, list: true, watch: true, patch, delete: false, create: false });

  it("без прав кнопка неактивна с подсказкой; read-only — все неактивны", async () => {
    vi.mocked(api.getNamespaceAccess).mockResolvedValue({
      resources: { "deployments.apps": verbs(true), "deployments/scale.apps": verbs(false), "horizontalpodautoscalers.autoscaling": verbs(true) },
      incomplete: false,
    });
    const { unmount } = renderWithClient(<WorkloadActions connectionId="c1" namespace="team-a" d={details} />);
    await waitFor(() => expect(screen.getByRole("button", { name: "Масштабировать" })).toBeDisabled());
    expect(screen.getByRole("button", { name: "Масштабировать" })).toHaveAttribute("title", "Нет прав: patch deployments/scale.apps");
    expect(screen.getByRole("button", { name: "Границы HPA" })).toBeEnabled();
    unmount();
    vi.mocked(api.listConnections).mockResolvedValue([{ ...dev, readOnly: true }]);
    renderWithClient(<WorkloadActions connectionId="c1" namespace="team-a" d={details} />);
    await waitFor(() => expect(screen.getByRole("button", { name: "Перезапустить" })).toBeDisabled());
  });
});
````

<!-- block: file t15-src-components-change-changes-test-ts step=15.2 path=src/components/change/changes.test.ts -->
````ts
import { describe, expect, it } from "vitest";
import type { Change, NamespaceAccess, Verbs } from "../../api/types";
import { i18n } from "../../i18n";
import { allowed, changeRight, hasValues, warningText, withValues } from "./changes";

const t = i18n.t.bind(i18n);
const web = { kind: "Deployment", name: "web" } as const;
const verbs = (patch: boolean): Verbs => ({ get: true, list: true, watch: true, patch, delete: false, create: false });

describe("изменения", () => {
  it("права: масштаб — подресурс scale, перезапуск DeploymentConfig — instantiate", () => {
    expect(changeRight({ kind: "scale", namespace: "a", workload: web, replicas: 2 })).toEqual({ resource: "deployments/scale.apps", verb: "patch" });
    expect(changeRight({ kind: "rolloutRestart", namespace: "a", workload: { kind: "DeploymentConfig", name: "x" } })).toEqual({
      resource: "deploymentconfigs/instantiate.apps.openshift.io",
      verb: "create",
    });
    const access: NamespaceAccess = { resources: { "deployments.apps": verbs(false) }, incomplete: false };
    const set: Change = { kind: "setResources", namespace: "a", workload: web, containers: [] };
    expect(allowed(access, set)).toBe(false);
    expect(allowed({ ...access, incomplete: true }, set)).toBe(true);
    expect(allowed(access, { kind: "deletePod", namespace: "a", pod: "p" })).toBe(true);
  });

  it("пустые строки формы ресурсов не отправляются", () => {
    const c: Change = {
      kind: "setResources",
      namespace: "a",
      workload: web,
      containers: [
        { name: "app", requests: { cpu: "250m" }, limits: {} },
        { name: "proxy", requests: {}, limits: {} },
        { name: "init", requests: {}, limits: { memory: null } },
      ],
    };
    expect(withValues(c)).toMatchObject({ containers: [{ name: "app" }, { name: "init" }] });
    expect(hasValues({ ...c, containers: [{ name: "proxy", requests: {}, limits: {} }] })).toBe(false);
  });

  it("предупреждения словами", () => {
    expect(warningText({ code: "quota", quota: "compute", resource: "requests.cpu", needed: 1.5, free: 0.5 }, t)).toBe(
      "Квоте compute не хватит requests.cpu: нужно 1500m, свободно 500m",
    );
    expect(warningText({ code: "limitRange", container: "app", field: "memoryLimit", bound: "max", limit: 1024 ** 3 }, t)).toBe(
      "app: memory limit больше максимума LimitRange 1Gi — сервер отклонит изменение",
    );
  });
});
````

- [ ] **Step 3: Увидеть падение.**

Run: `pnpm vitest run src/components/change/ChangeDialog.test.tsx src/components/change/changes.test.ts`  
Expected: FAIL — нет `ConfirmDialog`, `ChangeDialog` и `changes.ts`.

- [ ] **Step 4: Реализация.**

<!-- block: file t15-src-components-change-changeactions-tsx step=15.4 path=src/components/change/ChangeActions.tsx -->
````tsx
import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { Change, NamespaceAccess, WorkloadDetails } from "../../api/types";
import { useConnection } from "../../lib/useConnection";
import { useNamespaceAccess } from "../project/useNamespaceAccess";
import { allowed, canRestart, canScale, canSetResources, changeRight } from "./changes";
import { ChangeDialog, type ResourceHints } from "./ChangeDialog";

/** Текущие requests и limits контейнеров шаблона — подсказки формы ресурсов. */
export function hintsOf(d: WorkloadDetails): ResourceHints {
  return Object.fromEntries(d.containers.map((c) => [c.name, { requests: c.requests, limits: c.limits }]));
}

/** Пустая форма ресурсов: все контейнеры шаблона, ни одно поле не меняется. */
export function emptyResources(namespace: string, d: WorkloadDetails): Change {
  return {
    kind: "setResources",
    namespace,
    workload: { kind: d.workload.kind, name: d.workload.name },
    containers: d.containers.map((c) => ({ name: c.name, requests: {}, limits: {} })),
  };
}

/** Кнопка изменения: без прав или на подключении только для чтения — неактивна с подсказкой (спец. §5.4). */
export function ChangeButton({ change, access, readOnly, label, onOpen }: { change: Change; access?: NamespaceAccess; readOnly: boolean; label: string; onOpen: (c: Change) => void }) {
  const { t } = useTranslation();
  const right = changeRight(change);
  const can = allowed(access, change);
  const title = readOnly ? t("change.readOnly") : can ? undefined : t("change.noRights", right);
  return (
    <button type="button" disabled={readOnly || !can} title={title} onClick={() => onOpen(change)}>
      {label}
    </button>
  );
}

/** Изменения на обзоре workload'а (спец. §9): ресурсы контейнеров, реплики, перезапуск, границы HPA. */
export function WorkloadActions({ connectionId, namespace, d }: { connectionId: string; namespace: string; d: WorkloadDetails }) {
  const { t } = useTranslation();
  const access = useNamespaceAccess(connectionId, namespace).data;
  const readOnly = useConnection(connectionId)?.readOnly === true;
  const [open, setOpen] = useState<Change | null>(null);
  const w = { kind: d.workload.kind, name: d.workload.name };
  const hpa = d.hpa.data;
  const buttons: { change: Change; label: string }[] = [];
  if (canSetResources(w.kind)) buttons.push({ change: emptyResources(namespace, d), label: t("change.action.setResources") });
  if (canScale(w.kind)) buttons.push({ change: { kind: "scale", namespace, workload: w, replicas: d.workload.desired ?? 1 }, label: t("change.action.scale") });
  if (canRestart(w.kind)) buttons.push({ change: { kind: "rolloutRestart", namespace, workload: w }, label: t("change.action.rolloutRestart") });
  if (hpa) {
    buttons.push({
      change: { kind: "setHpaBounds", namespace, hpa: hpa.name, minReplicas: hpa.minReplicas ?? undefined, maxReplicas: hpa.maxReplicas },
      label: t("change.action.setHpaBounds"),
    });
  }
  if (buttons.length === 0) return null;
  return (
    <div className="change-actions">
      {buttons.map((b) => (
        <ChangeButton key={b.change.kind} change={b.change} access={access} readOnly={readOnly} label={b.label} onOpen={setOpen} />
      ))}
      {open && <ChangeDialog connectionId={connectionId} initial={open} hints={hintsOf(d)} onClose={() => setOpen(null)} />}
    </div>
  );
}

/** «Удалить под» на обзоре пода. */
export function PodActions({ connectionId, namespace, pod }: { connectionId: string; namespace: string; pod: string }) {
  const { t } = useTranslation();
  const access = useNamespaceAccess(connectionId, namespace).data;
  const readOnly = useConnection(connectionId)?.readOnly === true;
  const [open, setOpen] = useState<Change | null>(null);
  return (
    <div className="change-actions">
      <ChangeButton change={{ kind: "deletePod", namespace, pod }} access={access} readOnly={readOnly} label={t("change.action.deletePod")} onOpen={setOpen} />
      {open && <ChangeDialog connectionId={connectionId} initial={open} onClose={() => setOpen(null)} />}
    </div>
  );
}
````

<!-- block: file t15-src-components-change-changedialog-tsx step=15.4 path=src/components/change/ChangeDialog.tsx -->
````tsx
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { type KeyboardEvent, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { AppError, Change, ChangePreview, Confirmation, ContainerResources, Proposal, ResourceValues } from "../../api/types";
import { trapTab } from "../../lib/focusTrap";
import { healthKey } from "../../lib/useHealth";
import { useConnection } from "../../lib/useConnection";
import { ConfirmDialog } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { changeObject, hasValues, warningText, withValues } from "./changes";

/** Текущие requests и limits контейнера из шаблона — подсказки в полях формы ресурсов. */
export type ResourceHints = Record<string, { requests: Record<string, string>; limits: Record<string, string> }>;

interface Props {
  connectionId: string;
  /** Изменение, с которого форма начинается: пустая форма, рекомендация или предложение LLM. */
  initial: Change;
  hints?: ResourceHints;
  /** Предупреждение над формой: например, низкая уверенность рекомендации. */
  notice?: string;
  /** Применяется предложение LLM: строка над формой и `proposalId` при применении. */
  proposal?: Proposal;
  onClose: () => void;
}

/**
 * Форма изменения (спец. §9) поверх экрана: поля вида изменения, «Проверить» — «было → станет», предупреждения и
 * server-side dry-run, затем подтверждение (имя prod-подключения, имя пода без контроллера) и применение с версией
 * предпросмотра. Любая правка формы сбрасывает предпросмотр. Изменившийся объект (`CONFLICT`) — «Обновить и повторить».
 */
export function ChangeDialog({ connectionId, initial, hints, notice, proposal, onClose }: Props) {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const connection = useConnection(connectionId);
  const [change, setChange] = useState<Change>(initial);
  const [preview, setPreview] = useState<ChangePreview | null>(null);
  const rootRef = useRef<HTMLDivElement>(null);
  const check = useMutation({ mutationFn: (c: Change) => api.previewChange(connectionId, withValues(c)), onSuccess: setPreview });
  const apply = useMutation({
    mutationFn: ({ p, c }: { p: ChangePreview; c: Confirmation }) =>
      api.applyChange(connectionId, p.change, p.version, c.confirm, p.confirmPodName ? c.typedName : null, proposal?.id ?? null),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["workload", connectionId] });
      void qc.invalidateQueries({ queryKey: ["pod", connectionId] });
      void qc.invalidateQueries({ queryKey: ["resources", connectionId] });
      void qc.invalidateQueries({ queryKey: healthKey(connectionId) });
      onClose();
    },
  });
  const busy = check.isPending || apply.isPending;
  const readOnly = connection?.readOnly === true;

  useEffect(() => {
    const returnTo = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    rootRef.current?.focus();
    return () => {
      if (returnTo?.isConnected) returnTo.focus();
    };
  }, []);

  const edit = (c: Change) => {
    setChange(c);
    setPreview(null);
    check.reset();
    apply.reset();
  };
  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === "Tab") trapTab(rootRef.current, e);
    if (e.key === "Escape" && !busy) {
      e.stopPropagation();
      onClose();
    }
  };
  const conflict = (apply.error as AppError | null)?.code === "CONFLICT";
  const title = `${t(`change.title.${change.kind}`)}: ${changeObject(change)}`;

  return (
    <div className="modal-backdrop">
      <div className="modal" role="dialog" aria-modal="true" aria-labelledby="change-title" tabIndex={-1} ref={rootRef} onKeyDown={onKeyDown}>
        <h3 id="change-title">{title}</h3>
        {proposal && <p className="muted">{t("health.proposals.applying", { title: proposal.title })}</p>}
        {notice && <p className="problem">{notice}</p>}
        {readOnly && <p className="problem">{t("change.readOnly")}</p>}
        <ChangeFields change={change} hints={hints} disabled={busy} onChange={edit} />
        {!preview && (
          <div className="toolbar">
            <button type="button" className="primary" disabled={busy || readOnly || !hasValues(change)} onClick={() => check.mutate(change)}>
              {t("change.preview")}
            </button>
            <button type="button" disabled={busy} onClick={onClose}>
              {t("common.cancel")}
            </button>
          </div>
        )}
        {check.error && <ErrorBanner error={check.error} />}
        {preview && <PreviewView preview={preview} />}
        {preview?.dryRun.status === "rejected" && (
          <div className="toolbar">
            <button type="button" onClick={onClose}>
              {t("common.close")}
            </button>
          </div>
        )}
        {preview && preview.dryRun.status !== "rejected" && connection && (
          <ConfirmDialog
            connection={connection}
            title={t("change.confirmTitle")}
            confirmLabel={change.kind === "deletePod" ? t("change.delete") : t("change.apply")}
            danger={change.kind === "deletePod"}
            objectName={preview.confirmPodName && change.kind === "deletePod" ? change.pod : undefined}
            objectLabel={t("change.podName")}
            busy={busy}
            onConfirm={(c) => apply.mutate({ p: preview, c })}
            onCancel={onClose}
          />
        )}
        {apply.error && <ErrorBanner error={apply.error} />}
        {conflict && (
          <button type="button" disabled={busy} onClick={() => edit(change)}>
            {t("change.refreshRetry")}
          </button>
        )}
      </div>
    </div>
  );
}

/** «Было → станет», предупреждения и итог dry-run. */
function PreviewView({ preview }: { preview: ChangePreview }) {
  const { t } = useTranslation();
  const value = (v: string | null) => v ?? t("change.notSet");
  return (
    <>
      <h4>{t("change.diffTitle")}</h4>
      {preview.diff.length === 0 ? (
        <p className="muted">{t("change.noDiff")}</p>
      ) : (
        <table className="diff-table">
          <thead>
            <tr>
              <th>{t("change.col.container")}</th>
              <th>{t("change.col.field")}</th>
              <th>{t("change.col.before")}</th>
              <th>{t("change.col.after")}</th>
            </tr>
          </thead>
          <tbody>
            {preview.diff.map((d) => (
              <tr key={`${d.container ?? ""}:${d.field}`}>
                <td>{d.container ?? "—"}</td>
                <td>{t(`change.diff.${d.field}`)}</td>
                <td>{value(d.before)}</td>
                <td className="after">{value(d.after)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {preview.warnings.map((w) => (
        <p key={JSON.stringify(w)} className="problem">
          {warningText(w, t)}
        </p>
      ))}
      {preview.dryRun.status === "passed" && <p className="muted">✅ {t("change.dryRun.passed")}</p>}
      {preview.dryRun.status === "skipped" && <p className="muted">{t("change.dryRun.skipped")}</p>}
      {preview.dryRun.status === "rejected" && (
        <>
          <p className="problem">{t("change.dryRun.rejected")}</p>
          <ErrorBanner error={preview.dryRun.error} />
        </>
      )}
    </>
  );
}

interface FieldsProps {
  change: Change;
  hints?: ResourceHints;
  disabled: boolean;
  onChange: (c: Change) => void;
}

/** Поля вида изменения. */
function ChangeFields({ change, hints, disabled, onChange }: FieldsProps) {
  const { t } = useTranslation();
  const int = (s: string) => (s.trim() === "" ? NaN : Number(s));
  switch (change.kind) {
    case "setResources":
      return <ResourceFields change={change} hints={hints} disabled={disabled} onChange={onChange} />;
    case "scale":
      return (
        <label className="form">
          {t("change.replicas")}
          <input type="number" min={0} value={Number.isNaN(change.replicas) ? "" : change.replicas} disabled={disabled} onChange={(e) => onChange({ ...change, replicas: int(e.target.value) })} />
        </label>
      );
    case "setHpaBounds":
      return (
        <div className="form">
          <label htmlFor="hpa-min">{t("change.minReplicas")}</label>
          <input
            id="hpa-min"
            type="number"
            min={1}
            placeholder={t("change.keep")}
            value={change.minReplicas ?? ""}
            disabled={disabled}
            onChange={(e) => onChange({ ...change, minReplicas: e.target.value.trim() === "" ? undefined : int(e.target.value) })}
          />
          <label htmlFor="hpa-max">{t("change.maxReplicas")}</label>
          <input id="hpa-max" type="number" min={1} value={Number.isNaN(change.maxReplicas) ? "" : change.maxReplicas} disabled={disabled} onChange={(e) => onChange({ ...change, maxReplicas: int(e.target.value) })} />
        </div>
      );
    case "rolloutRestart":
      return <p className="muted">{change.workload.kind === "DeploymentConfig" ? t("change.restartDc") : t("change.restartNote")}</p>;
    case "deletePod":
      return <p className="muted">{t("change.deleteNote")}</p>;
  }
}

type Part = "requests" | "limits";
type Res = "cpu" | "memory";
const CELLS: [Part, Res][] = [
  ["requests", "cpu"],
  ["requests", "memory"],
  ["limits", "cpu"],
  ["limits", "memory"],
];

/**
 * Таблица контейнеров: пустое поле — «не менять» (в подсказке — текущее значение), «✕» — убрать значение
 * (`null`), повторный щелчок возвращает «не менять».
 */
function ResourceFields({ change, hints, disabled, onChange }: FieldsProps & { change: Extract<Change, { kind: "setResources" }> }) {
  const { t } = useTranslation();
  const set = (i: number, part: Part, res: Res, v: string | null | undefined) => {
    const containers = change.containers.map((c, j): ContainerResources => {
      if (j !== i) return c;
      const values: ResourceValues = { ...c[part] };
      if (v === undefined) delete values[res];
      else values[res] = v;
      return { ...c, [part]: values };
    });
    onChange({ ...change, containers });
  };
  return (
    <table className="resource-form">
      <thead>
        <tr>
          <th>{t("change.col.container")}</th>
          {CELLS.map(([p, r]) => (
            <th key={`${p}.${r}`}>{t(`change.diff.${p}.${r}`)}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {change.containers.map((c, i) => (
          <tr key={c.name}>
            <td>{c.name}</td>
            {CELLS.map(([p, r]) => {
              const v = c[p][r];
              const now = hints?.[c.name]?.[p]?.[r];
              const label = `${c.name} ${t(`change.diff.${p}.${r}`)}`;
              return (
                <td key={`${p}.${r}`}>
                  <input
                    aria-label={label}
                    className={v === null ? "removed" : undefined}
                    value={v ?? ""}
                    placeholder={v === null ? t("change.removed") : now ? t("change.current", { value: now }) : t("change.keep")}
                    disabled={disabled || v === null}
                    onChange={(e) => set(i, p, r, e.target.value.trim() === "" ? undefined : e.target.value)}
                  />
                  <button
                    type="button"
                    className="icon"
                    aria-label={`${t("change.remove")}: ${label}`}
                    aria-pressed={v === null}
                    title={t("change.remove")}
                    disabled={disabled}
                    onClick={() => set(i, p, r, v === null ? undefined : null)}
                  >
                    ✕
                  </button>
                </td>
              );
            })}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
````

<!-- block: file t15-src-components-change-changes-ts step=15.4 path=src/components/change/changes.ts -->
````ts
import type { TFunction } from "i18next";
import type { Change, ChangeWarning, NamespaceAccess, ResourceField, WorkloadKind } from "../../api/types";
import { cpuQuantity, memoryQuantity } from "../../lib/quantity";

/** Ресурс прав (`ресурс.группа`) вида workload'а. */
export const WORKLOAD_RESOURCE: Record<WorkloadKind, string> = {
  Deployment: "deployments.apps",
  StatefulSet: "statefulsets.apps",
  DaemonSet: "daemonsets.apps",
  DeploymentConfig: "deploymentconfigs.apps.openshift.io",
  Job: "jobs.batch",
  CronJob: "cronjobs.batch",
};

const SCALE_RESOURCE: Partial<Record<WorkloadKind, string>> = {
  Deployment: "deployments/scale.apps",
  StatefulSet: "statefulsets/scale.apps",
  DeploymentConfig: "deploymentconfigs/scale.apps.openshift.io",
};

/** Что можно сделать с видом workload'а (спец. §9): шаблон Job неизменяем, Job и CronJob не перезапускаются. */
export const canSetResources = (k: WorkloadKind) => k !== "Job";
export const canScale = (k: WorkloadKind) => k in SCALE_RESOURCE;
export const canRestart = (k: WorkloadKind) => k !== "Job" && k !== "CronJob";

type Verb = "patch" | "delete" | "create";

/** Право, которое нужно изменению: ресурс и глагол. */
export function changeRight(c: Change): { resource: string; verb: Verb } {
  switch (c.kind) {
    case "setResources":
      return { resource: WORKLOAD_RESOURCE[c.workload.kind], verb: "patch" };
    case "scale":
      return { resource: SCALE_RESOURCE[c.workload.kind] ?? WORKLOAD_RESOURCE[c.workload.kind], verb: "patch" };
    case "rolloutRestart":
      return c.workload.kind === "DeploymentConfig"
        ? { resource: "deploymentconfigs/instantiate.apps.openshift.io", verb: "create" }
        : { resource: WORKLOAD_RESOURCE[c.workload.kind], verb: "patch" };
    case "setHpaBounds":
      return { resource: "horizontalpodautoscalers.autoscaling", verb: "patch" };
    case "deletePod":
      return { resource: "pods", verb: "delete" };
  }
}

/**
 * Разрешено ли изменение (спец. §5.4): права неизвестны, перечислены не целиком или ресурс не из списка — «да»,
 * окончательно решает API-сервер.
 */
export function allowed(access: NamespaceAccess | undefined, c: Change): boolean {
  if (!access || access.incomplete) return true;
  const { resource, verb } = changeRight(c);
  return access.resources[resource]?.[verb] ?? true;
}

/** Только контейнеры, где что-то меняется: пустые строки формы не отправляются. */
export function withValues(c: Change): Change {
  if (c.kind !== "setResources") return c;
  const set = (v: { cpu?: string | null; memory?: string | null }) => v.cpu !== undefined || v.memory !== undefined;
  return { ...c, containers: c.containers.filter((x) => set(x.requests) || set(x.limits)) };
}

/** Есть что отправить: у ресурсов — хотя бы одно поле. */
export function hasValues(c: Change): boolean {
  const w = withValues(c);
  return w.kind !== "setResources" || w.containers.length > 0;
}

const isCpu = (s: string) => s.toLowerCase().includes("cpu");

/** Значение поля ресурсов: у CPU — ядра, у памяти — байты, у `maxLimitRequestRatio` — само отношение. */
export function fieldAmount(field: ResourceField, v: number, ratio = false): string {
  if (ratio) return String(v);
  return isCpu(field) ? cpuQuantity(v) : memoryQuantity(v);
}

/** Значение ресурса квоты: `requests.cpu` — ядра, `limits.memory` — байты. */
export const quotaAmount = (resource: string, v: number) => (isCpu(resource) ? cpuQuantity(v) : memoryQuantity(v));

/** Предупреждение к изменению словами. */
export function warningText(w: ChangeWarning, t: TFunction): string {
  switch (w.code) {
    case "hpaManaged":
      return t("change.warning.hpaManaged", { hpa: w.hpa });
    case "scaleToZero":
      return t("change.warning.scaleToZero");
    case "limitRange":
      return t("change.warning.limitRange", {
        container: w.container,
        field: t(`change.fieldName.${w.field}`),
        bound: t(`change.bound.${w.bound}`),
        limit: fieldAmount(w.field, w.limit, w.bound === "ratio"),
      });
    case "quota":
      return t("change.warning.quota", { quota: w.quota, resource: w.resource, needed: quotaAmount(w.resource, w.needed), free: quotaAmount(w.resource, w.free) });
    case "orphan":
      return t("change.warning.orphan");
  }
}

/** Объект изменения словами: «Deployment web», «HPA web-hpa», «под web-1». */
export function changeObject(c: Change): string {
  switch (c.kind) {
    case "setHpaBounds":
      return `HPA ${c.hpa}`;
    case "deletePod":
      return `Pod ${c.pod}`;
    default:
      return `${c.workload.kind} ${c.workload.name}`;
  }
}
````

<!-- block: patch t15-src step=15.4 -->
````diff
diff --git a/PORTED.md b/PORTED.md
index ed5d801..2eea260 100644
--- a/PORTED.md
+++ b/PORTED.md
@@ -192,3 +192,6 @@
 | `src-tauri/src/health/proposals.rs` | `src-tauri/src/health/proposals.rs` | `8bbd0bd` | адаптирован: модель, статусы и сроки предложения; вид изменения — `change::Change` (§9) |
 | `crates/glafira-mcp-kubernetes/src/proposals.rs` | `crates/glafira-mcp/src/proposals.rs` | `8bbd0bd` | адаптирован: виды изменения Kubernetes (§9), предупреждения проверки |
 | `crates/glafira-mcp-kubernetes/src/server/proposal_tools.rs` | `crates/glafira-mcp/src/server/proposal_tools.rs` | `8bbd0bd` | адаптирован: виды изменения Kubernetes, область LLM, предупреждения |
+| `src/components/common/ConfirmDialog.tsx` | `src/components/common/ConfirmDialog.tsx` | `8bbd0bd` | адаптирован: подписи — имя пода |
+| `src/components/common/ConfirmDialog.test.tsx` | `src/components/common/ConfirmDialog.test.tsx` | `8bbd0bd` | адаптирован: объекты Kubernetes в примерах |
+| `src/lib/useConnection.ts` | `src/lib/useConnection.ts` | `8bbd0bd` | как есть |
diff --git a/docs/i18n/same-as-en.json b/docs/i18n/same-as-en.json
index 8b802e1..350f352 100644
--- a/docs/i18n/same-as-en.json
+++ b/docs/i18n/same-as-en.json
@@ -27,6 +27,14 @@
     "health.object.gateway",
     "health.object.pvc",
     "health.object.hpa",
-    "health.object.quota"
+    "health.object.quota",
+    "change.diff.requests.cpu",
+    "change.diff.requests.memory",
+    "change.diff.limits.cpu",
+    "change.diff.limits.memory",
+    "change.fieldName.cpuRequest",
+    "change.fieldName.cpuLimit",
+    "change.fieldName.memoryRequest",
+    "change.fieldName.memoryLimit"
   ]
 }
diff --git a/src/api/types.ts b/src/api/types.ts
index b4b04ed..d094cc2 100644
--- a/src/api/types.ts
+++ b/src/api/types.ts
@@ -1270,6 +1270,12 @@ export type Change =
 
 export type ChangeKind = Change["kind"];
 
+/** Подтверждение изменения (`ConfirmDialog`): имя prod-подключения и введённое имя объекта (пода). */
+export interface Confirmation {
+  confirm: string | null;
+  typedName: string | null;
+}
+
 /** Поле «было → станет»: `requests.cpu`, `replicas`, `restartedAt`, `pod`… */
 export interface FieldChange {
   container: string | null;
diff --git a/src/components/objects/PodPanel.tsx b/src/components/objects/PodPanel.tsx
index d2f880f..e3f645d 100644
--- a/src/components/objects/PodPanel.tsx
+++ b/src/components/objects/PodPanel.tsx
@@ -12,6 +12,7 @@ import { ReadError } from "../common/ReadError";
 import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
 import { SubTabs } from "../common/SubTabs";
 import { YamlView } from "../common/YamlView";
+import { PodActions } from "../change/ChangeActions";
 import { matchPod } from "../health/findings";
 import { HealthBlock } from "../health/HealthBlock";
 import { LogsView } from "../logs/LogsView";
@@ -66,6 +67,7 @@ function PodOverview({ connectionId, namespace, name }: { connectionId: string;
       <HealthBlock connectionId={connectionId} match={matchPod(namespace, name)} showTarget />
       {q.isPending && <p className="muted">{t("common.loading")}</p>}
       {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
+      {q.data && <PodActions connectionId={connectionId} namespace={namespace} pod={name} />}
       {q.data && <PodSummary connectionId={connectionId} namespace={namespace} d={q.data} />}
     </div>
   );
diff --git a/src/components/objects/WorkloadPanel.tsx b/src/components/objects/WorkloadPanel.tsx
index 7edb9a5..693e645 100644
--- a/src/components/objects/WorkloadPanel.tsx
+++ b/src/components/objects/WorkloadPanel.tsx
@@ -12,6 +12,7 @@ import { ReadError } from "../common/ReadError";
 import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
 import { SubTabs } from "../common/SubTabs";
 import { YamlView } from "../common/YamlView";
+import { WorkloadActions } from "../change/ChangeActions";
 import { matchWorkload } from "../health/findings";
 import { HealthBlock } from "../health/HealthBlock";
 import { ContainerCard } from "./ContainerCard";
@@ -62,6 +63,7 @@ function WorkloadOverview({ connectionId, namespace, kind, name }: { connectionI
       <HealthBlock connectionId={connectionId} match={matchWorkload(namespace, kind, name)} />
       {q.isPending && <p className="muted">{t("common.loading")}</p>}
       {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
+      {q.data && <WorkloadActions connectionId={connectionId} namespace={namespace} d={q.data} />}
       {q.data && <WorkloadSummary d={q.data} />}
     </div>
   );
diff --git a/src/styles.css b/src/styles.css
index 6702bf0..1f8f354 100644
--- a/src/styles.css
+++ b/src/styles.css
@@ -207,3 +207,9 @@ label.inline { display: inline-flex; gap: 4px; align-items: center; white-space:
 .confidence-low { color: var(--warn); }
 .risk { color: var(--danger); }
 .diff-table td.after { font-weight: 600; }
+.confirm { border: 1px solid var(--warn); border-radius: 6px; padding: 8px; display: flex; flex-direction: column; gap: 6px; background: color-mix(in srgb, var(--warn) 8%, transparent); }
+.confirm label { display: flex; flex-direction: column; gap: 2px; }
+.resource-form td { padding: 2px 4px; }
+.resource-form input { width: 7em; }
+.resource-form .removed { color: var(--muted); font-style: italic; }
+.change-actions { display: flex; flex-wrap: wrap; gap: 6px; margin: 8px 0; }
````

<!-- block: locales t15-locales step=15.4 -->
````json
{
  "en": {
    "common": {
      "confirm": {
        "typeObject": "To confirm, type the {{label}} <code>{{name}}</code>",
        "prodNotice": "This is a prod connection. Type its name <code>{{name}}</code>",
        "defaultLabel": "name",
        "objectAria": "Confirmation: {{label}}",
        "connectionAria": "Connection name"
      }
    },
    "change": {
      "title": {
        "setResources": "Container resources",
        "scale": "Replicas",
        "rolloutRestart": "Restart",
        "setHpaBounds": "HPA bounds",
        "deletePod": "Delete pod"
      },
      "action": {
        "setResources": "Change resources",
        "scale": "Scale",
        "rolloutRestart": "Restart",
        "setHpaBounds": "HPA bounds",
        "deletePod": "Delete pod"
      },
      "preview": "Check",
      "apply": "Apply",
      "delete": "Delete",
      "confirmTitle": "Apply the change?",
      "podName": "pod name",
      "refreshRetry": "Refresh and retry",
      "readOnly": "The connection is read-only: changes are not available",
      "noRights": "No rights: {{verb}} {{resource}}",
      "diffTitle": "Before → after",
      "noDiff": "Nothing changes",
      "notSet": "not set",
      "col": {
        "container": "Container",
        "field": "Field",
        "before": "Before",
        "after": "After"
      },
      "diff": {
        "requests": { "cpu": "CPU request", "memory": "Memory request" },
        "limits": { "cpu": "CPU limit", "memory": "Memory limit" },
        "replicas": "Replicas",
        "minReplicas": "Min replicas",
        "maxReplicas": "Max replicas",
        "restartedAt": "Restarted at",
        "pod": "Pod"
      },
      "dryRun": {
        "passed": "The API server accepted the change (dry-run)",
        "skipped": "This call has no dry-run: the API server checks it when it is applied",
        "rejected": "The API server rejected the change"
      },
      "replicas": "Replicas",
      "minReplicas": "Min replicas",
      "maxReplicas": "Max replicas",
      "keep": "keep",
      "removed": "will be removed",
      "current": "now {{value}}",
      "remove": "Remove the value",
      "restartNote": "Pods are recreated one by one according to the rollout strategy.",
      "restartDc": "A new DeploymentConfig rollout starts (oc rollout latest).",
      "deleteNote": "The pod is deleted gracefully; its controller, if any, creates a new one.",
      "warning": {
        "hpaManaged": "HPA {{hpa}} scales this workload and will overwrite the replicas",
        "scaleToZero": "No pods will be left",
        "limitRange": "{{container}}: {{field}} {{bound}} of the LimitRange {{limit}} — the API server will reject the change",
        "quota": "The quota {{quota}} lacks {{resource}}: {{needed}} needed, {{free}} free",
        "orphan": "The pod has no controller: nobody will create it again"
      },
      "fieldName": {
        "cpuRequest": "CPU request",
        "cpuLimit": "CPU limit",
        "memoryRequest": "memory request",
        "memoryLimit": "memory limit"
      },
      "bound": {
        "min": "is below the minimum",
        "max": "is above the maximum",
        "ratio": "exceeds the limit/request ratio"
      }
    },
    "health": {
      "proposals": {
        "applying": "Applying an LLM proposal: {{title}}"
      }
    }
  },
  "ru": {
    "common": {
      "confirm": {
        "typeObject": "Для подтверждения введите {{label}} <code>{{name}}</code>",
        "prodNotice": "Это prod-подключение. Введите его имя <code>{{name}}</code>",
        "defaultLabel": "имя",
        "objectAria": "Подтверждение: {{label}}",
        "connectionAria": "Имя подключения"
      }
    },
    "change": {
      "title": {
        "setResources": "Ресурсы контейнеров",
        "scale": "Реплики",
        "rolloutRestart": "Перезапуск",
        "setHpaBounds": "Границы HPA",
        "deletePod": "Удаление пода"
      },
      "action": {
        "setResources": "Изменить ресурсы",
        "scale": "Масштабировать",
        "rolloutRestart": "Перезапустить",
        "setHpaBounds": "Границы HPA",
        "deletePod": "Удалить под"
      },
      "preview": "Проверить",
      "apply": "Применить",
      "delete": "Удалить",
      "confirmTitle": "Применить изменение?",
      "podName": "имя пода",
      "refreshRetry": "Обновить и повторить",
      "readOnly": "Подключение только для чтения: изменения недоступны",
      "noRights": "Нет прав: {{verb}} {{resource}}",
      "diffTitle": "Было → станет",
      "noDiff": "Ничего не меняется",
      "notSet": "не задано",
      "col": {
        "container": "Контейнер",
        "field": "Поле",
        "before": "Было",
        "after": "Станет"
      },
      "diff": {
        "requests": { "cpu": "CPU request", "memory": "Memory request" },
        "limits": { "cpu": "CPU limit", "memory": "Memory limit" },
        "replicas": "Реплики",
        "minReplicas": "Минимум реплик",
        "maxReplicas": "Максимум реплик",
        "restartedAt": "Перезапуск",
        "pod": "Под"
      },
      "dryRun": {
        "passed": "API-сервер принял изменение (dry-run)",
        "skipped": "У этого вызова нет dry-run: API-сервер проверит его при применении",
        "rejected": "API-сервер отклонил изменение"
      },
      "replicas": "Реплики",
      "minReplicas": "Минимум реплик",
      "maxReplicas": "Максимум реплик",
      "keep": "не менять",
      "removed": "будет убрано",
      "current": "сейчас {{value}}",
      "remove": "Убрать значение",
      "restartNote": "Поды пересоздаются по очереди по стратегии выкладки.",
      "restartDc": "Запустится новая выкладка DeploymentConfig (oc rollout latest).",
      "deleteNote": "Под удаляется штатно; его контроллер, если он есть, создаст новый.",
      "warning": {
        "hpaManaged": "HPA {{hpa}} масштабирует этот workload и перезапишет число реплик",
        "scaleToZero": "Подов не останется",
        "limitRange": "{{container}}: {{field}} {{bound}} LimitRange {{limit}} — сервер отклонит изменение",
        "quota": "Квоте {{quota}} не хватит {{resource}}: нужно {{needed}}, свободно {{free}}",
        "orphan": "У пода нет контроллера: заново его никто не создаст"
      },
      "fieldName": {
        "cpuRequest": "CPU request",
        "cpuLimit": "CPU limit",
        "memoryRequest": "memory request",
        "memoryLimit": "memory limit"
      },
      "bound": {
        "min": "меньше минимума",
        "max": "больше максимума",
        "ratio": "превышает отношение limit/request"
      }
    },
    "health": {
      "proposals": {
        "applying": "Применяется предложение LLM: {{title}}"
      }
    }
  }
}
````

- [ ] **Step 5: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 674 passed; 0 failed`

Run: `pnpm test`  
Expected: PASS — `Test Files 69 passed`, `Tests 500 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 198`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 6: Commit.**

```bash
git add \
  PORTED.md \
  docs/i18n/same-as-en.json \
  src/api/types.ts \
  src/components/change/ChangeActions.tsx \
  src/components/change/ChangeDialog.test.tsx \
  src/components/change/ChangeDialog.tsx \
  src/components/change/changes.test.ts \
  src/components/change/changes.ts \
  src/components/common/ConfirmDialog.test.tsx \
  src/components/common/ConfirmDialog.tsx \
  src/components/objects/PodPanel.tsx \
  src/components/objects/WorkloadPanel.tsx \
  src/lib/useConnection.ts \
  src/locales/en.json \
  src/locales/ru.json \
  src/styles.css
git commit -s -m "feat(ui): change forms over the screen — container resources, replicas, restart, HPA bounds and pod deletion with a before-after diff, warnings and a server-side dry-run, the prod and pod-name confirmations, refresh and retry on CONFLICT; buttons disabled without rights; ConfirmDialog from the Kafka edition" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 16: Вкладки «Ресурсы» workload'а и пода: окно, источник, покрытие и уверенность, «сейчас → рекомендация» по контейнерам со статистикой худшего пода, итог, ограничения и сдвиг HPA, графики отдельным чанком; «Применить рекомендацию» открывает форму ресурсов

**Files:**
- Create: `src/components/resources/WorkloadResources.test.tsx`, `src/components/resources/WorkloadResources.tsx`, `src/components/resources/recommend.test.ts`, `src/components/resources/recommend.ts`
- Modify: `docs/i18n/same-as-en.json`, `src/components/objects/PodPanel.tsx`, `src/components/objects/WorkloadPanel.tsx`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задачи 14–15: `api.{recommendResources, getPodUsage}`, `UsageChart`, `ChangeDialog`, `ChangeButton`, `quantity`.
- Produces: `components/resources/recommend.ts::{WINDOWS, DEFAULT_WINDOW = 7, sourceLabel, recommendationChange, effective, noteText, violationText}`; `WorkloadResources.tsx::{WindowSelect, WorkloadResources, PodsNow, PodResources}`; подвкладка `resources` у `WorkloadPanel` и `PodPanel`.

`UsageChart` грузится лениво (`lazy` + `ChunkBoundary`): uPlot при загрузке модуля зовёт `matchMedia`, которого нет в jsdom, а в сборке он уходит в свой чанк. Форма из рекомендации получает только рекомендованные значения; при низкой уверенности — предупреждение над формой. Окно по умолчанию — 7 дней; своё окно по умолчанию в `settings.json` (§13) отложено.

- [ ] **Step 1: Тесты.**

<!-- block: file t16-src-components-resources-workloadresources-test-tsx step=16.1 path=src/components/resources/WorkloadResources.test.tsx -->
````tsx
import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: { listConnections: vi.fn().mockResolvedValue([]), getNamespaceAccess: vi.fn(), recommendResources: vi.fn(), getPodUsage: vi.fn(), previewChange: vi.fn() },
  };
});
vi.mock("./UsageChart", () => ({ UsageChart: ({ title }: { title: string }) => <p>график: {title}</p> }));

import { api } from "../../api/client";
import type { Analysis, MetricsSourceInfo } from "../../api/types";
import { PodResources, WorkloadResources } from "./WorkloadResources";

const MI = 1024 * 1024;
const source: MetricsSourceInfo = { kind: "prometheus", origin: "auto", location: "monitoring/prometheus-k8s:9090", history: true };
const none = { cpu: null, memory: null };

function analysis(confidence: Analysis["recommendation"]["confidence"]): Analysis {
  return {
    usage: {
      namespace: "team-a",
      kind: "Deployment",
      name: "web",
      windowDays: 7,
      source,
      sourceStatus: { mode: "auto", selected: source, rejected: [], podMetrics: false, checkedAtMs: 1 },
      coverage: 1,
      dataSeconds: 31 * 86_400,
      ownerMetrics: true,
      replicas: 2,
      surge: 1,
      containers: [
        {
          name: "app",
          init: false,
          sidecar: false,
          requests: { cpu: 0.01, memory: 16 * MI },
          limits: { cpu: 0.05, memory: 32 * MI },
          limitRangeRequests: none,
          limitRangeLimits: none,
          envNames: [],
          stats: {
            cpu: { p50: 0.008, p95: 0.012, p99: 0.015, max: 0.02 },
            memory: { p95: 20 * MI, p99: 22 * MI, max: 30 * MI },
            throttling: { mean: 0.4, p95: 0.6 },
            oomCount: 0,
            oomLast: null,
            restarts: 0,
          },
        },
      ],
      charts: { start: 0, end: 1, stepSeconds: 300, cpu: [], memory: [], restarts: [], ooms: [] },
      podsNow: [],
    },
    recommendation: {
      namespace: "team-a",
      kind: "Deployment",
      name: "web",
      windowDays: 7,
      source,
      confidence,
      coverage: 1,
      dataSeconds: 31 * 86_400,
      replicas: 2,
      containers: [
        {
          name: "app",
          current: { requests: { cpu: 0.01, memory: 16 * MI }, limits: { cpu: 0.05, memory: 32 * MI } },
          recommended: { requests: { cpu: 0.02, memory: 32 * MI }, limits: { cpu: 0.08, memory: 48 * MI } },
          notes: [{ code: "cpuThrottled", p95: 0.6 }],
        },
      ],
      violations: [],
      hpa: [],
      totals: { requests: { cpu: 0.02, memory: 32 * MI }, limits: { cpu: 0.06, memory: 32 * MI } },
    },
  };
}

beforeEach(() => {
  vi.mocked(api.getNamespaceAccess).mockResolvedValue({ resources: {}, incomplete: true });
});

describe("WorkloadResources", () => {
  it("источник, уверенность, «сейчас → рекомендация», статистика, итог и графики", async () => {
    vi.mocked(api.recommendResources).mockResolvedValue(analysis("high"));
    renderWithClient(<WorkloadResources connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    expect(await screen.findByText("10m → 20m")).toBeInTheDocument();
    expect(api.recommendResources).toHaveBeenCalledWith("c1", "team-a", "Deployment", "web", 7);
    expect(screen.getByText(/Prometheus monitoring\/prometheus-k8s:9090/)).toHaveTextContent("покрытие 100 %");
    expect(screen.getByText("уверенность: высокая")).toBeInTheDocument();
    const row = screen.getByRole("row", { name: /app/ });
    expect(within(row).getByText("16Mi → 32Mi")).toBeInTheDocument();
    expect(within(row).getByText("12m / 20m")).toBeInTheDocument();
    expect(within(row).getByText("40 %")).toBeInTheDocument();
    expect(screen.getByText(/app: Троттлинг p95 — 60 %/)).toBeInTheDocument();
    expect(screen.getByText(/На весь workload \(2 реплик\): requests CPU \+20m, память \+32Mi/)).toBeInTheDocument();
    expect(await screen.findByText("график: CPU, ядра")).toBeInTheDocument();
  });

  it("«Применить рекомендацию» открывает форму с рекомендованными значениями; низкая уверенность — предупреждение", async () => {
    vi.mocked(api.recommendResources).mockResolvedValue(analysis("low"));
    const user = userEvent.setup();
    renderWithClient(<WorkloadResources connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    await user.click(await screen.findByRole("button", { name: "Применить рекомендацию" }));
    const dialog = screen.getByRole("dialog", { name: "Ресурсы контейнеров: Deployment web" });
    expect(within(dialog).getByText(/Уверенность рекомендации низкая/)).toBeInTheDocument();
    expect(within(dialog).getByLabelText("app CPU request")).toHaveValue("20m");
    expect(within(dialog).getByLabelText("app Memory limit")).toHaveValue("48Mi");
  });

  it("окно меняет запрос; без источника метрик — ошибка с причиной", async () => {
    vi.mocked(api.recommendResources).mockRejectedValue({ code: "METRICS_UNAVAILABLE", key: "errors.metrics.unavailable", message: "No metrics source is available", retryable: false });
    const user = userEvent.setup();
    renderWithClient(<WorkloadResources connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    expect(await screen.findByText("Ни один источник метрик не доступен", { exact: false })).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Окно"), "30");
    expect(api.recommendResources).toHaveBeenLastCalledWith("c1", "team-a", "Deployment", "web", 30);
  });
});

describe("PodResources", () => {
  it("текущее потребление контейнеров пода", async () => {
    vi.mocked(api.getPodUsage).mockResolvedValue({ source: { kind: "metricsServer", origin: "auto", location: "metrics.k8s.io", history: false }, containers: [{ name: "app", cpu: 0.25, memory: 512 * MI }] });
    renderWithClient(<PodResources connectionId="c1" namespace="team-a" pod="web-1" />);
    expect(await screen.findByText("512Mi")).toBeInTheDocument();
    expect(screen.getByText("250m")).toBeInTheDocument();
    expect(screen.getByText("Источник: metrics.k8s.io")).toBeInTheDocument();
  });
});
````

<!-- block: file t16-src-components-resources-recommend-test-ts step=16.1 path=src/components/resources/recommend.test.ts -->
````ts
import { describe, expect, it } from "vitest";
import type { Recommendation } from "../../api/types";
import { i18n } from "../../i18n";
import { noteText, recommendationChange, sourceLabel, violationText } from "./recommend";

const t = i18n.t.bind(i18n);
const MI = 1024 * 1024;

describe("рекомендация", () => {
  it("в форму идут рекомендованные значения quantity-строками; контейнер без данных не меняется", () => {
    const r = {
      namespace: "team-a",
      kind: "Deployment",
      name: "web",
      containers: [
        { name: "app", current: {}, recommended: { requests: { cpu: 0.02, memory: 32 * MI }, limits: { cpu: null, memory: 48 * MI } }, notes: [] },
        { name: "proxy", current: {}, recommended: null, notes: [{ code: "noData" }] },
      ],
    } as unknown as Recommendation;
    expect(recommendationChange(r)).toEqual({
      kind: "setResources",
      namespace: "team-a",
      workload: { kind: "Deployment", name: "web" },
      containers: [{ name: "app", requests: { cpu: "20m", memory: "32Mi" }, limits: { memory: "48Mi" } }],
    });
  });

  it("источник, пометки и нарушения словами", () => {
    expect(sourceLabel({ kind: "prometheus", origin: "auto", location: "monitoring/prometheus-k8s:9090", history: true })).toBe("Prometheus monitoring/prometheus-k8s:9090");
    expect(noteText({ code: "cpuThrottled", p95: 0.6 }, t)).toBe("Троттлинг p95 — 60 %: наблюдаемое потребление CPU ограничено лимитом, limit не меньше текущего × 1,5");
    expect(violationText({ code: "nodeAllocatable", container: "app", resource: "memory", request: 64 * 1024 * MI, allocatable: 32 * 1024 * MI }, t)).toBe(
      "app: request memory 64Gi больше allocatable самой крупной ноды 32Gi",
    );
  });
});
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/resources/WorkloadResources.test.tsx src/components/resources/recommend.test.ts`  
Expected: FAIL — нет `recommend.ts` и `WorkloadResources`.

- [ ] **Step 3: Реализация.**

<!-- block: file t16-src-components-resources-workloadresources-tsx step=16.3 path=src/components/resources/WorkloadResources.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { Suspense, lazy, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { Analysis, Change, ContainerRecommendation, ContainerUsage, PodNow, WorkloadKind } from "../../api/types";
import { cpuText, memoryText, percentText, signedText, cpuQuantity, memoryQuantity } from "../../lib/quantity";
import { useConnection } from "../../lib/useConnection";
import { ChangeButton } from "../change/ChangeActions";
import { canSetResources } from "../change/changes";
import { ChangeDialog } from "../change/ChangeDialog";
import { Hint } from "../common/Hint";
import { ChunkBoundary } from "../common/ChunkBoundary";
import { ReadError } from "../common/ReadError";
import { useNamespaceAccess } from "../project/useNamespaceAccess";
import type { ChartLine } from "./chart";
import { DEFAULT_WINDOW, WINDOWS, effective, noteText, recommendationChange, sourceLabel, violationText } from "./recommend";

// uPlot — отдельный чанк: грузится, когда есть что рисовать.
const UsageChart = lazy(() => import("./UsageChart").then((m) => ({ default: m.UsageChart })));

interface Props {
  connectionId: string;
  namespace: string;
  kind: WorkloadKind;
  name: string;
}

/** Выбор окна анализа: 1, 7, 14 или 30 дней. */
export function WindowSelect({ days, onChange }: { days: number; onChange: (d: number) => void }) {
  const { t } = useTranslation();
  return (
    <span className="field-label">
      <label>
        {t("resources.window")}{" "}
        <select value={days} onChange={(e) => onChange(Number(e.target.value))}>
          {WINDOWS.map((d) => (
            <option key={d} value={d}>
              {t("resources.days", { count: d })}
            </option>
          ))}
        </select>
      </label>
      <Hint k="hints.resources.window" label={t("resources.window")} />
    </span>
  );
}

/**
 * «Ресурсы» workload'а (спец. §7.4): окно, источник, покрытие и уверенность; таблица «сейчас → рекомендация» по
 * контейнерам со статистикой худшего пода; итог на весь workload; графики; «Применить рекомендацию» открывает форму
 * «Ресурсы контейнеров» с рекомендованными значениями.
 */
export function WorkloadResources({ connectionId, namespace, kind, name }: Props) {
  const { t } = useTranslation();
  const [days, setDays] = useState(DEFAULT_WINDOW);
  const [form, setForm] = useState<Change | null>(null);
  const access = useNamespaceAccess(connectionId, namespace).data;
  const readOnly = useConnection(connectionId)?.readOnly === true;
  const q = useQuery({
    queryKey: ["resources", connectionId, namespace, kind, name, days],
    queryFn: () => api.recommendResources(connectionId, namespace, kind, name, days),
  });
  const a = q.data;
  const change = a ? recommendationChange(a.recommendation) : null;
  const low = a?.recommendation.confidence === "low";
  return (
    <div className="section">
      <div className="toolbar">
        <WindowSelect days={days} onChange={setDays} />
        <button type="button" disabled={q.isFetching} onClick={() => void q.refetch()}>
          {t("common.refresh")}
        </button>
        {change && change.kind === "setResources" && change.containers.length > 0 && canSetResources(kind) && (
          <ChangeButton change={change} access={access} readOnly={readOnly} label={t("resources.applyRecommendation")} onOpen={setForm} />
        )}
      </div>
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {a && <AnalysisView a={a} />}
      {form && (
        <ChangeDialog
          connectionId={connectionId}
          initial={form}
          notice={low ? t("resources.lowConfidence") : undefined}
          onClose={() => setForm(null)}
        />
      )}
    </div>
  );
}

function AnalysisView({ a }: { a: Analysis }) {
  const { t } = useTranslation();
  const { usage: u, recommendation: r } = a;
  const recs = new Map(r.containers.map((c) => [c.name, c]));
  return (
    <>
      <p className="muted">
        {t("resources.source", { source: sourceLabel(u.source) })} · {t("resources.days", { count: u.windowDays })}
        {u.coverage != null && <> · {t("resources.coverage", { value: percentText(u.coverage) })}</>}
        {u.dataSeconds != null && <> · {t("resources.dataDays", { count: Math.round((u.dataSeconds / 86_400) * 10) / 10 })}</>}
        {" · "}
        <span className={`confidence-${r.confidence}`}>{t("resources.confidence", { value: t(`resources.confidenceLevel.${r.confidence}`) })}</span>{" "}
        <Hint k="hints.resources.confidence" label={t("resources.confidenceLabel")} />
      </p>
      {!u.ownerMetrics && u.source.history && <p className="muted">{t("resources.noKubeStateMetrics")}</p>}
      <table className="resource-table">
        <thead>
          <tr>
            <th>{t("change.col.container")}</th>
            <th>{t("change.diff.requests.cpu")}</th>
            <th>{t("change.diff.limits.cpu")}</th>
            <th>{t("change.diff.requests.memory")}</th>
            <th>{t("change.diff.limits.memory")}</th>
            <th className="num">{t("resources.col.cpu")}</th>
            <th className="num">{t("resources.col.memory")}</th>
            <th className="num">{t("resources.col.throttling")}</th>
            <th className="num">{t("resources.col.oom")}</th>
            <th className="num">{t("resources.col.restarts")}</th>
          </tr>
        </thead>
        <tbody>
          {u.containers.map((c) => (
            <ContainerRow key={`${c.init}:${c.name}`} c={c} rec={recs.get(c.name)} />
          ))}
        </tbody>
      </table>
      {r.containers.flatMap((c) => c.notes.map((n) => <p key={`${c.name}:${JSON.stringify(n)}`} className="muted">{`${c.name}: ${noteText(n, t)}`}</p>))}
      <p>
        {t("resources.totals", {
          requestsCpu: signedText(r.totals.requests.cpu, cpuQuantity),
          requestsMemory: signedText(r.totals.requests.memory, memoryQuantity),
          limitsCpu: signedText(r.totals.limits.cpu, cpuQuantity),
          limitsMemory: signedText(r.totals.limits.memory, memoryQuantity),
          replicas: r.replicas ?? "—",
        })}
      </p>
      {r.violations.map((v) => (
        <p key={JSON.stringify(v)} className="problem">
          {violationText(v, t)}
        </p>
      ))}
      {r.hpa.map((h) => (
        <p key={h.hpa + h.resource} className="problem">
          {t("resources.hpaShift", { ...h })}
        </p>
      ))}
      {u.charts && <UsageCharts a={a} />}
      {u.podsNow.length > 0 && <PodsNow pods={u.podsNow} />}
    </>
  );
}

function pair(now: number | null, rec: number | null | undefined, text: (v: number | null) => string) {
  if (rec === undefined || rec === now) return <td>{text(now)}</td>;
  return (
    <td className="changed">
      {text(now)} → {text(rec)}
    </td>
  );
}

function ContainerRow({ c, rec }: { c: ContainerUsage; rec?: ContainerRecommendation }) {
  const { t } = useTranslation();
  const now = effective(c);
  const r = rec?.recommended ?? undefined;
  const s = c.stats;
  return (
    <tr>
      <td>
        {c.name}
        {c.init && !c.sidecar && <span className="muted"> ({t("resources.init")})</span>}
      </td>
      {pair(now.requests.cpu, r?.requests.cpu, cpuText)}
      {pair(now.limits.cpu, r?.limits.cpu, cpuText)}
      {pair(now.requests.memory, r?.requests.memory, memoryText)}
      {pair(now.limits.memory, r?.limits.memory, memoryText)}
      <td className="num">
        {cpuText(s.cpu.p95)} / {cpuText(s.cpu.max)}
      </td>
      <td className="num">{memoryText(s.memory.max)}</td>
      <td className="num">{percentText(s.throttling.mean)}</td>
      <td className="num">{s.oomCount ?? "—"}</td>
      <td className="num">{s.restarts ?? "—"}</td>
    </tr>
  );
}

/** Графики CPU и памяти: худший под по контейнерам, линии текущих request и limit, отметки рестартов и OOM. */
function UsageCharts({ a }: { a: Analysis }) {
  const { t } = useTranslation();
  const charts = a.usage.charts!;
  const lines = useMemo(() => {
    const out: { cpu: ChartLine[]; memory: ChartLine[] } = { cpu: [], memory: [] };
    for (const c of a.usage.containers.filter((x) => !x.init || x.sidecar)) {
      const e = effective(c);
      for (const res of ["cpu", "memory"] as const) {
        if (e.requests[res] != null) out[res].push({ label: `${c.name} request`, value: e.requests[res]!, dash: "request" });
        if (e.limits[res] != null) out[res].push({ label: `${c.name} limit`, value: e.limits[res]!, dash: "limit" });
      }
    }
    return out;
  }, [a]);
  const markers = useMemo(() => [...charts.restarts, ...charts.ooms], [charts]);
  return (
    <ChunkBoundary fallback={<p className="muted">{t("resources.chartFailed")}</p>}>
      <Suspense fallback={<p className="muted">{t("common.loading")}</p>}>
        <UsageChart title={t("resources.chartCpu")} series={charts.cpu} lines={lines.cpu} markers={markers} markerLabel={t("resources.marks")} format={cpuQuantity} />
        <UsageChart title={t("resources.chartMemory")} series={charts.memory} lines={lines.memory} markers={markers} markerLabel={t("resources.marks")} format={memoryQuantity} />
      </Suspense>
    </ChunkBoundary>
  );
}

/** Только metrics.k8s.io: текущее потребление подов. */
export function PodsNow({ pods }: { pods: PodNow[] }) {
  const { t } = useTranslation();
  return (
    <>
      <h3>{t("resources.now")}</h3>
      <table className="resource-table">
        <thead>
          <tr>
            <th>{t("resources.col.pod")}</th>
            <th>{t("change.col.container")}</th>
            <th className="num">CPU</th>
            <th className="num">{t("resources.col.memoryNow")}</th>
          </tr>
        </thead>
        <tbody>
          {pods.flatMap((p) =>
            p.containers.map((c) => (
              <tr key={`${p.pod}:${c.name}`}>
                <td>{p.pod}</td>
                <td>{c.name}</td>
                <td className="num">{cpuText(c.cpu)}</td>
                <td className="num">{memoryText(c.memory)}</td>
              </tr>
            )),
          )}
        </tbody>
      </table>
    </>
  );
}

/** «Ресурсы» пода (спец. §6.5): текущее потребление по контейнерам. */
export function PodResources({ connectionId, namespace, pod }: { connectionId: string; namespace: string; pod: string }) {
  const { t } = useTranslation();
  const q = useQuery({ queryKey: ["resources", connectionId, namespace, "pod", pod], queryFn: () => api.getPodUsage(connectionId, namespace, pod) });
  return (
    <div className="section">
      <div className="toolbar">
        <button type="button" disabled={q.isFetching} onClick={() => void q.refetch()}>
          {t("common.refresh")}
        </button>
      </div>
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && (
        <>
          <p className="muted">{t("resources.source", { source: sourceLabel(q.data.source) })}</p>
          <PodsNow pods={[{ pod, containers: q.data.containers }]} />
        </>
      )}
    </div>
  );
}
````

<!-- block: file t16-src-components-resources-recommend-ts step=16.3 path=src/components/resources/recommend.ts -->
````ts
import type { TFunction } from "i18next";
import type { Amounts, Change, ContainerCurrent, MetricsSourceInfo, Recommendation, RecommendationNote, ResourceValues, Violation } from "../../api/types";
import { cpuQuantity, memoryQuantity } from "../../lib/quantity";
import { quotaAmount } from "../change/changes";

/** Окна анализа (спец. §7.2); по умолчанию — 7 дней. */
export const WINDOWS = [1, 7, 14, 30] as const;
export const DEFAULT_WINDOW = 7;

/** Источник словами, как `SourceInfo::label` в Rust: «Prometheus monitoring/prometheus-k8s:9090», «metrics.k8s.io». */
export function sourceLabel(s: MetricsSourceInfo): string {
  if (s.kind === "thanos") return `Thanos Querier ${s.location}`;
  if (s.kind === "prometheus") return `Prometheus ${s.location}`;
  return s.location;
}

const values = (a: Amounts): ResourceValues => ({
  ...(a.cpu != null && { cpu: cpuQuantity(a.cpu) }),
  ...(a.memory != null && { memory: memoryQuantity(a.memory) }),
});

/** Форма «Ресурсы контейнеров» из рекомендации: рекомендованные значения; чего нет — не меняется. */
export function recommendationChange(r: Recommendation): Change {
  return {
    kind: "setResources",
    namespace: r.namespace,
    workload: { kind: r.kind, name: r.name },
    containers: r.containers.flatMap((c) => (c.recommended ? [{ name: c.name, requests: values(c.recommended.requests), limits: values(c.recommended.limits) }] : [])),
  };
}

/** Действующие requests и limits: из шаблона, без значения в нём — `LimitRange` по умолчанию. */
export function effective(c: ContainerCurrent): { requests: Amounts; limits: Amounts } {
  const pick = (a: Amounts, d: Amounts): Amounts => ({ cpu: a.cpu ?? d.cpu, memory: a.memory ?? d.memory });
  return { requests: pick(c.requests, c.limitRangeRequests), limits: pick(c.limits, c.limitRangeLimits) };
}

/** Пометка к рекомендации контейнера словами. */
export function noteText(n: RecommendationNote, t: TFunction): string {
  switch (n.code) {
    case "cpuThrottled":
      return t("resources.note.cpuThrottled", { p95: Math.round(n.p95 * 100) });
    case "oomKilled":
      return t("resources.note.oomKilled", { count: n.count });
    case "limitRange":
      return t("resources.note.limitRange", { field: t(`change.fieldName.${n.field}`), bound: t(`resources.bound.${n.bound}`) });
    case "limitRangeDefault":
      return t("resources.note.limitRangeDefault", { field: t(`change.fieldName.${n.field}`) });
    default:
      return t(`resources.note.${n.code}`);
  }
}

/** Нарушение ограничения кластера словами. */
export function violationText(v: Violation, t: TFunction): string {
  if (v.code === "quota") {
    return t("change.warning.quota", { quota: v.quota, resource: v.resource, needed: quotaAmount(v.resource, v.needed), free: quotaAmount(v.resource, v.free) });
  }
  return t("resources.violation.nodeAllocatable", {
    container: v.container,
    resource: v.resource,
    request: quotaAmount(v.resource, v.request),
    allocatable: quotaAmount(v.resource, v.allocatable),
  });
}
````

<!-- block: patch t16-src step=16.3 -->
````diff
diff --git a/docs/i18n/same-as-en.json b/docs/i18n/same-as-en.json
index 350f352..b0d278e 100644
--- a/docs/i18n/same-as-en.json
+++ b/docs/i18n/same-as-en.json
@@ -35,6 +35,9 @@
     "change.fieldName.cpuRequest",
     "change.fieldName.cpuLimit",
     "change.fieldName.memoryRequest",
-    "change.fieldName.memoryLimit"
+    "change.fieldName.memoryLimit",
+    "resources.init",
+    "resources.col.cpu",
+    "resources.col.oom"
   ]
 }
diff --git a/src/components/objects/PodPanel.tsx b/src/components/objects/PodPanel.tsx
index e3f645d..5ee67b9 100644
--- a/src/components/objects/PodPanel.tsx
+++ b/src/components/objects/PodPanel.tsx
@@ -13,17 +13,18 @@ import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
 import { SubTabs } from "../common/SubTabs";
 import { YamlView } from "../common/YamlView";
 import { PodActions } from "../change/ChangeActions";
+import { PodResources } from "../resources/WorkloadResources";
 import { matchPod } from "../health/findings";
 import { HealthBlock } from "../health/HealthBlock";
 import { LogsView } from "../logs/LogsView";
 import { ContainerCard } from "./ContainerCard";
 import { ObjectEvents } from "./EventList";
 
-type Sub = "overview" | "logs" | "events" | "yaml";
+type Sub = "overview" | "logs" | "events" | "yaml" | "resources";
 
 const WORKLOAD_KINDS: string[] = ["Deployment", "StatefulSet", "DaemonSet", "DeploymentConfig", "Job", "CronJob"] satisfies WorkloadKind[];
 
-/** Вкладка пода (спец. §6.5–6.6): обзор с состоянием каждого контейнера, логи, события, YAML. */
+/** Вкладка пода (спец. §6.5–6.6): обзор с состоянием каждого контейнера, логи, события, YAML, ресурсы. */
 export function PodPanel({ connectionId, namespace, name }: { connectionId: string; namespace: string; name: string }) {
   const { t } = useTranslation();
   const [sub, setSub] = useState<Sub>("overview");
@@ -34,6 +35,7 @@ export function PodPanel({ connectionId, namespace, name }: { connectionId: stri
     { id: "logs", label: t("logs.title") },
     { id: "events", label: t("objects.tabs.events") },
     { id: "yaml", label: "YAML" },
+    { id: "resources", label: t("objects.tabs.resources") },
   ];
   return (
     <div className="page">
@@ -48,6 +50,7 @@ export function PodPanel({ connectionId, namespace, name }: { connectionId: stri
       {sub === "logs" && details.error && <ReadError error={details.error} connectionId={connectionId} onRetry={() => void details.refetch()} />}
       {sub === "events" && <ObjectEvents connectionId={connectionId} namespace={namespace} object={{ kind: "Pod", name }} />}
       {sub === "yaml" && <YamlView connectionId={connectionId} namespace={namespace} kind="Pod" name={name} />}
+      {sub === "resources" && <PodResources connectionId={connectionId} namespace={namespace} pod={name} />}
     </div>
   );
 }
diff --git a/src/components/objects/WorkloadPanel.tsx b/src/components/objects/WorkloadPanel.tsx
index 693e645..8e20235 100644
--- a/src/components/objects/WorkloadPanel.tsx
+++ b/src/components/objects/WorkloadPanel.tsx
@@ -13,21 +13,23 @@ import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
 import { SubTabs } from "../common/SubTabs";
 import { YamlView } from "../common/YamlView";
 import { WorkloadActions } from "../change/ChangeActions";
+import { WorkloadResources } from "../resources/WorkloadResources";
 import { matchWorkload } from "../health/findings";
 import { HealthBlock } from "../health/HealthBlock";
 import { ContainerCard } from "./ContainerCard";
 import { ObjectEvents } from "./EventList";
 import { PodList } from "./PodList";
 
-type Sub = "overview" | "pods" | "events" | "yaml";
+type Sub = "overview" | "pods" | "resources" | "events" | "yaml";
 
-/** Вкладка workload'а (спец. §6.4): обзор, его поды, события вместе с подами, YAML. */
+/** Вкладка workload'а (спец. §6.4): обзор, его поды, ресурсы, события вместе с подами, YAML. */
 export function WorkloadPanel({ connectionId, namespace, kind, name }: { connectionId: string; namespace: string; kind: WorkloadKind; name: string }) {
   const { t } = useTranslation();
   const [sub, setSub] = useState<Sub>("overview");
   const tabs: { id: Sub; label: string }[] = [
     { id: "overview", label: t("objects.tabs.overview") },
     { id: "pods", label: t("objects.tabs.pods") },
+    { id: "resources", label: t("objects.tabs.resources") },
     { id: "events", label: t("objects.tabs.events") },
     { id: "yaml", label: "YAML" },
   ];
@@ -42,6 +44,7 @@ export function WorkloadPanel({ connectionId, namespace, kind, name }: { connect
       <SubTabs tabs={tabs} active={sub} onChange={setSub} />
       {sub === "overview" && <WorkloadOverview connectionId={connectionId} namespace={namespace} kind={kind} name={name} />}
       {sub === "pods" && <PodList connectionId={connectionId} namespace={namespace} workload={{ kind, name }} />}
+      {sub === "resources" && <WorkloadResources connectionId={connectionId} namespace={namespace} kind={kind} name={name} />}
       {sub === "events" && <ObjectEvents connectionId={connectionId} namespace={namespace} object={{ kind, name }} />}
       {sub === "yaml" && <YamlView connectionId={connectionId} namespace={namespace} kind={kind} name={name} />}
     </div>
````

<!-- block: locales t16-locales step=16.3 -->
````json
{
  "en": {
    "objects": {
      "tabs": {
        "resources": "Resources"
      }
    },
    "resources": {
      "window": "Window",
      "days": "{{count}} d",
      "source": "Source: {{source}}",
      "coverage": "coverage {{value}}",
      "dataDays": "data for {{count}} d",
      "confidence": "confidence: {{value}}",
      "confidenceLabel": "Confidence",
      "confidenceLevel": {
        "low": "low",
        "medium": "medium",
        "high": "high"
      },
      "noKubeStateMetrics": "No kube-state-metrics: pods are matched by name, OOM and restarts are unknown.",
      "applyRecommendation": "Apply the recommendation",
      "lowConfidence": "The confidence of the recommendation is low: little data for the window. Check the values before applying.",
      "init": "init",
      "col": {
        "cpu": "CPU p95 / max",
        "memory": "Memory max",
        "throttling": "Throttling",
        "oom": "OOM",
        "restarts": "Restarts",
        "pod": "Pod",
        "memoryNow": "Memory"
      },
      "totals": "For the whole workload ({{replicas}} replicas): requests CPU {{requestsCpu}}, memory {{requestsMemory}}; limits CPU {{limitsCpu}}, memory {{limitsMemory}}",
      "hpaShift": "HPA {{hpa}}: the {{resource}} target {{target}} % is relative to the request; for the same scaling with the new request set it to {{equivalent}} %",
      "chartCpu": "CPU, cores",
      "chartMemory": "Memory",
      "marks": "Restarts and OOM",
      "now": "Current usage",
      "note": {
        "cpuLimitNotSet": "The CPU limit is not set — there is no throttling, no limit is added",
        "cpuThrottled": "Throttling p95 is {{p95}} %: the observed CPU usage is capped by the limit, the limit is at least the current one × 1.5",
        "oomKilled": "OOMKilled {{count}} times in the window: the observed memory usage is cut by the limit, the limit is at least the current one × 1.5",
        "noData": "No data for the window: no recommendation",
        "limitRange": "{{field}} is pulled to the LimitRange {{bound}}",
        "limitRangeDefault": "The current {{field}} comes from the LimitRange default, the template has none",
        "java": "Java options are set: check -Xmx or MaxRAMPercentage together with the memory limit",
        "go": "GOMEMLIMIT is set: check it together with the memory limit"
      },
      "bound": {
        "min": "minimum",
        "max": "maximum",
        "ratio": "limit/request ratio"
      },
      "violation": {
        "nodeAllocatable": "{{container}}: request {{resource}} {{request}} is above the allocatable of the largest node {{allocatable}}"
      },
      "chartFailed": "The chart could not be loaded"
    },
    "hints": {
      "resources": {
        "window": "Statistics over the last 1, 7, 14 or 30 days, but not longer than the metrics source keeps. Percentiles are taken per pod, then the worst pod.",
        "confidence": "High — window and data of 7 days or more with coverage of 80 % or more; medium — data of 24 hours or more with coverage of 50 % or more; low — less, or only metrics.k8s.io."
      }
    }
  },
  "ru": {
    "objects": {
      "tabs": {
        "resources": "Ресурсы"
      }
    },
    "resources": {
      "window": "Окно",
      "days": "{{count}} дн.",
      "source": "Источник: {{source}}",
      "coverage": "покрытие {{value}}",
      "dataDays": "данные за {{count}} дн.",
      "confidence": "уверенность: {{value}}",
      "confidenceLabel": "Уверенность",
      "confidenceLevel": {
        "low": "низкая",
        "medium": "средняя",
        "high": "высокая"
      },
      "noKubeStateMetrics": "Нет kube-state-metrics: поды выбраны по имени, OOM и рестарты неизвестны.",
      "applyRecommendation": "Применить рекомендацию",
      "lowConfidence": "Уверенность рекомендации низкая: данных за окно мало. Проверьте значения перед применением.",
      "init": "init",
      "col": {
        "cpu": "CPU p95 / max",
        "memory": "Память max",
        "throttling": "Троттлинг",
        "oom": "OOM",
        "restarts": "Рестарты",
        "pod": "Под",
        "memoryNow": "Память"
      },
      "totals": "На весь workload ({{replicas}} реплик): requests CPU {{requestsCpu}}, память {{requestsMemory}}; limits CPU {{limitsCpu}}, память {{limitsMemory}}",
      "hpaShift": "HPA {{hpa}}: цель {{resource}} {{target}} % считается от request; чтобы масштабирование осталось прежним при новом request, поставьте {{equivalent}} %",
      "chartCpu": "CPU, ядра",
      "chartMemory": "Память",
      "marks": "Рестарты и OOM",
      "now": "Текущее потребление",
      "note": {
        "cpuLimitNotSet": "CPU limit не задан — троттлинга нет, limit не добавляется",
        "cpuThrottled": "Троттлинг p95 — {{p95}} %: наблюдаемое потребление CPU ограничено лимитом, limit не меньше текущего × 1,5",
        "oomKilled": "OOMKilled {{count}} раз за окно: наблюдаемое потребление памяти обрезано лимитом, limit не меньше текущего × 1,5",
        "noData": "Данных за окно нет: рекомендации нет",
        "limitRange": "{{field}} подтянут к границе LimitRange: {{bound}}",
        "limitRangeDefault": "Текущий {{field}} — по умолчанию LimitRange, в шаблоне его нет",
        "java": "Заданы опции Java: проверьте -Xmx или MaxRAMPercentage вместе с лимитом памяти",
        "go": "Задан GOMEMLIMIT: проверьте его вместе с лимитом памяти"
      },
      "bound": {
        "min": "минимум",
        "max": "максимум",
        "ratio": "отношение limit/request"
      },
      "violation": {
        "nodeAllocatable": "{{container}}: request {{resource}} {{request}} больше allocatable самой крупной ноды {{allocatable}}"
      },
      "chartFailed": "График не загрузился"
    },
    "hints": {
      "resources": {
        "window": "Статистика за последние 1, 7, 14 или 30 дней, но не дольше срока хранения источника метрик. Перцентили считаются по каждому поду, затем берётся худший под.",
        "confidence": "Высокая — окно и данные от 7 дней при покрытии от 80 %; средняя — данные от 24 часов при покрытии от 50 %; низкая — меньше либо только metrics.k8s.io."
      }
    }
  }
}
````

- [ ] **Step 4: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 674 passed; 0 failed`

Run: `pnpm test`  
Expected: PASS — `Test Files 71 passed`, `Tests 506 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 198`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 5: Commit.**

```bash
git add \
  docs/i18n/same-as-en.json \
  src/components/objects/PodPanel.tsx \
  src/components/objects/WorkloadPanel.tsx \
  src/components/resources/WorkloadResources.test.tsx \
  src/components/resources/WorkloadResources.tsx \
  src/components/resources/recommend.test.ts \
  src/components/resources/recommend.ts \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "feat(ui): Resources tabs of a workload and a pod — window, source, coverage and confidence, now and recommended requests and limits per container with the worst-pod statistics, the workload total, constraints and HPA shift, charts as a lazy chunk; Apply the recommendation opens the resources form" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 17: Вкладка «Ресурсы» проекта: фоновая сессия сбора с источником и квотами, requests и limits работающих подов против потребления за окно, резерв, риски и избыток, сортировка по риску или резерву, «только с рекомендациями», не больше 100 строк

**Files:**
- Create: `src/components/resources/ProjectUsageView.test.tsx`, `src/components/resources/ProjectUsageView.tsx`, `src/components/resources/projectRows.ts`
- Modify: `docs/i18n/same-as-en.json`, `src/components/project/ProjectPanel.tsx`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задачи 7, 14, 16: `api.startProjectUsage`, `ProjectUsageEvent`, `WindowSelect`, `sourceLabel`, `quotaAmount`; O1b: `KindErrors`, `api.stopSession`.
- Produces: `components/resources/projectRows.ts::{MAX_ROWS = 100, Sort, riskText, excessText, actionable, projectRows}`; `ProjectUsageView({ connectionId, namespace })`; подвкладка `resources` у `ProjectPanel` (видна при праве читать поды).

Сессия начинается при открытии и смене окна, «Обновить» собирает мимо кэша (`refresh`), закрытие вкладки и новый запуск останавливают прежнюю (`stopSession`, в том числе если id пришёл после размонтирования). Больше 100 строк — пометка и фильтр по имени.

- [ ] **Step 1: Тесты.**

<!-- block: file t17-src-components-resources-projectusageview-test-tsx step=17.1 path=src/components/resources/ProjectUsageView.test.tsx -->
````tsx
import { act, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { startProjectUsage: vi.fn(), stopSession: vi.fn().mockResolvedValue(true) } };
});

import { api } from "../../api/client";
import type { ProjectUsage, ProjectUsageEvent, WorkloadResources } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { ProjectUsageView } from "./ProjectUsageView";
import { projectRows } from "./projectRows";

const MI = 1024 * 1024;
const workload = (name: string, patch: Partial<WorkloadResources> = {}): WorkloadResources => ({
  kind: "Deployment",
  name,
  pods: 2,
  requests: { cpu: 1, memory: 512 * MI },
  limits: { cpu: 2, memory: 1024 * MI },
  usage: { cpu: 0.2, memory: 256 * MI },
  reserve: { cpu: 0.8, memory: 256 * MI },
  risks: [],
  excess: [],
  ...patch,
});
const usage = (workloads: WorkloadResources[]): ProjectUsage => ({
  namespace: "team-a",
  windowDays: 7,
  source: { kind: "prometheus", origin: "auto", location: "monitoring/prometheus-k8s:9090", history: true },
  sourceStatus: { mode: "auto", selected: null, rejected: [], podMetrics: false, checkedAtMs: 1 },
  workloads,
  errors: [],
  truncated: false,
  quotas: [{ quota: "compute", resource: "requests.cpu", hard: 4, used: 3 }],
  collectedAtMs: Date.now(),
});

let send: (e: ProjectUsageEvent) => void = () => {};
beforeEach(() => {
  vi.mocked(api.startProjectUsage).mockReset();
  vi.mocked(api.startProjectUsage).mockImplementation(async (_c, _n, _d, _r, onEvent) => {
    send = onEvent;
    return "s1";
  });
  vi.mocked(api.stopSession).mockClear();
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("ProjectUsageView", () => {
  it("сессия сбора, квоты, риски и избыток; «Обновить» — мимо кэша; закрытие останавливает сессию", async () => {
    const user = userEvent.setup();
    const { unmount } = renderWithClient(<ProjectUsageView connectionId="c1" namespace="team-a" />);
    expect(screen.getByText(/Собираю потребление проекта/)).toBeInTheDocument();
    expect(api.startProjectUsage).toHaveBeenCalledWith("c1", "team-a", 7, false, expect.any(Function));
    act(() =>
      send({
        kind: "done",
        usage: usage([
          workload("idle", { excess: [{ container: "app", resource: "cpu", request: 2, usage: 0.3 }] }),
          workload("hot", { risks: [{ code: "throttled", container: "app", mean: 0.4 }] }),
        ]),
      }),
    );
    expect(await screen.findByText("app: троттлинг 40 %")).toBeInTheDocument();
    expect(screen.getByText("app: CPU request 2 при p95 300m")).toBeInTheDocument();
    expect(screen.getByText(/compute requests.cpu: 3 \/ 4 \(75 %\)/)).toBeInTheDocument();
    const rows = screen.getAllByRole("row").slice(1);
    expect(within(rows[0]).getByRole("button", { name: "hot" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "hot" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "workload", workloadKind: "Deployment", name: "hot" });
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    expect(api.startProjectUsage).toHaveBeenLastCalledWith("c1", "team-a", 7, true, expect.any(Function));
    unmount();
    expect(api.stopSession).toHaveBeenCalledWith("s1");
  });

  it("сортировка по резерву, «только с рекомендациями», не больше 100 строк", () => {
    const list = [workload("a", { reserve: { cpu: 0.1, memory: 0 } }), workload("b", { reserve: { cpu: 0.5, memory: 0 }, risks: [{ code: "oom", container: "app", count: 1 }] })];
    expect(projectRows(list, "reserve", false, "").map((w) => w.name)).toEqual(["b", "a"]);
    expect(projectRows(list, "risk", true, "").map((w) => w.name)).toEqual(["b"]);
    expect(projectRows(list, "risk", false, "A").map((w) => w.name)).toEqual(["a"]);
  });

  it("больше 100 workloads — пометка", async () => {
    renderWithClient(<ProjectUsageView connectionId="c1" namespace="team-a" />);
    await vi.waitFor(() => expect(api.startProjectUsage).toHaveBeenCalled());
    act(() => send({ kind: "done", usage: usage(Array.from({ length: 120 }, (_, i) => workload(`w${i}`))) }));
    expect(await screen.findByText("Показаны 100 из 120: сузьте список по имени")).toBeInTheDocument();
    expect(screen.getAllByRole("row")).toHaveLength(101);
  });
});
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/resources/ProjectUsageView.test.tsx`  
Expected: FAIL — нет `ProjectUsageView` и `projectRows.ts`.

- [ ] **Step 3: Реализация.**

<!-- block: file t17-src-components-resources-projectusageview-tsx step=17.3 path=src/components/resources/ProjectUsageView.tsx -->
````tsx
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { AppError, ProjectUsage } from "../../api/types";
import { currentLocale } from "../../i18n";
import { formatAgo } from "../../lib/formatAgo";
import { cpuText, memoryText } from "../../lib/quantity";
import { useWorkspace } from "../../state/workspace";
import { ReadError } from "../common/ReadError";
import { KindErrors } from "../project/KindErrors";
import { quotaAmount } from "../change/changes";
import { MAX_ROWS, type Sort, excessText, projectRows, riskText } from "./projectRows";
import { DEFAULT_WINDOW, sourceLabel } from "./recommend";
import { WindowSelect } from "./WorkloadResources";

type State = { status: "loading" } | { status: "done"; usage: ProjectUsage } | { status: "failed"; error: AppError };

/**
 * Сбор по проекту — фоновая сессия `get_project_usage` (дедлайн 45 с, кэш 5 минут в Rust); закрытие вкладки или
 * новый запуск останавливает прежнюю. `nonce` — «Обновить»: собрать заново, мимо кэша.
 */
function useProjectUsage(connectionId: string, namespace: string, days: number, nonce: number): State {
  const [state, setState] = useState<State>({ status: "loading" });
  useEffect(() => {
    let stopped = false;
    let sessionId: string | null = null;
    setState({ status: "loading" });
    api
      .startProjectUsage(connectionId, namespace, days, nonce > 0, (e) => {
        if (stopped) return;
        setState(e.kind === "done" ? { status: "done", usage: e.usage } : { status: "failed", error: e.error });
      })
      .then((id) => {
        if (stopped) api.stopSession(id).catch(() => undefined);
        else sessionId = id;
      })
      .catch((error: AppError) => !stopped && setState({ status: "failed", error }));
    return () => {
      stopped = true;
      if (sessionId) api.stopSession(sessionId).catch(() => undefined);
    };
  }, [connectionId, namespace, days, nonce]);
  return state;
}

/**
 * «Ресурсы» проекта (спец. §7.4): суммарные requests и limits работающих подов против потребления за окно, резерв,
 * риски и избыток по workload'ам, использование квот; сортировка по риску или резерву, фильтр «только с
 * рекомендациями» (риск или избыток), не больше 100 строк.
 */
export function ProjectUsageView({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const [days, setDays] = useState(DEFAULT_WINDOW);
  const [nonce, setNonce] = useState(0);
  const [sort, setSort] = useState<Sort>("risk");
  const [onlyActionable, setOnlyActionable] = useState(false);
  const [query, setQuery] = useState("");
  const state = useProjectUsage(connectionId, namespace, days, nonce);
  const u = state.status === "done" ? state.usage : null;
  const rows = u ? projectRows(u.workloads, sort, onlyActionable, query) : [];
  return (
    <div className="section">
      <div className="toolbar">
        <WindowSelect days={days} onChange={setDays} />
        <select aria-label={t("resources.sort")} value={sort} onChange={(e) => setSort(e.target.value as Sort)}>
          <option value="risk">{t("resources.sortRisk")}</option>
          <option value="reserve">{t("resources.sortReserve")}</option>
        </select>
        <label>
          <input type="checkbox" checked={onlyActionable} onChange={(e) => setOnlyActionable(e.target.checked)} />
          {t("resources.onlyActionable")}
        </label>
        <input aria-label={t("resources.filter")} placeholder={t("resources.filter")} value={query} onChange={(e) => setQuery(e.target.value)} />
        <button type="button" disabled={state.status === "loading"} onClick={() => setNonce((n) => n + 1)}>
          {t("common.refresh")}
        </button>
      </div>
      {state.status === "loading" && <p className="muted">{t("resources.collecting")}</p>}
      {state.status === "failed" && <ReadError error={state.error} connectionId={connectionId} onRetry={() => setNonce((n) => n + 1)} />}
      {u && (
        <>
          <p className="muted">
            {t("resources.source", { source: sourceLabel(u.source) })} · {t("resources.days", { count: u.windowDays })} ·{" "}
            {t("resources.collected", { when: formatAgo(u.collectedAtMs, Date.now(), currentLocale()) })}
          </p>
          {u.truncated && <p className="muted">{t("resources.truncated")}</p>}
          <KindErrors errors={u.errors} />
          {u.quotas.length > 0 && (
            <p>
              {t("resources.quotas")}{" "}
              {u.quotas
                .map((q) => `${q.quota} ${q.resource}: ${quotaAmount(q.resource, q.used)} / ${quotaAmount(q.resource, q.hard)} (${q.hard > 0 ? Math.round((q.used / q.hard) * 100) : 0} %)`)
                .join("; ")}
            </p>
          )}
          {rows.length === 0 ? (
            <p className="empty">{t("resources.noWorkloads")}</p>
          ) : (
            <table className="resource-table">
              <thead>
                <tr>
                  <th>{t("resources.col.workload")}</th>
                  <th className="num">{t("resources.col.pods")}</th>
                  <th className="num">{t("resources.col.requests")}</th>
                  <th className="num">{t("resources.col.limits")}</th>
                  <th className="num">{t("resources.col.usage")}</th>
                  <th className="num">{t("resources.col.reserve")}</th>
                  <th>{t("resources.col.risks")}</th>
                </tr>
              </thead>
              <tbody>
                {rows.slice(0, MAX_ROWS).map((w) => (
                  <tr key={`${w.kind}/${w.name}`}>
                    <td>
                      <span className="muted">{w.kind}</span>{" "}
                      <button type="button" className="link" onClick={() => open({ kind: "workload", connectionId, namespace, workloadKind: w.kind, name: w.name })}>
                        {w.name}
                      </button>
                    </td>
                    <td className="num">{w.pods}</td>
                    <td className="num">
                      {cpuText(w.requests.cpu)} / {memoryText(w.requests.memory)}
                    </td>
                    <td className="num">
                      {cpuText(w.limits.cpu)} / {memoryText(w.limits.memory)}
                    </td>
                    <td className="num">
                      {cpuText(w.usage.cpu)} / {memoryText(w.usage.memory)}
                    </td>
                    <td className="num">
                      {cpuText(w.reserve.cpu)} / {memoryText(w.reserve.memory)}
                    </td>
                    <td>
                      {w.risks.map((r) => (
                        <div key={JSON.stringify(r)} className="risk">
                          {riskText(r, t)}
                        </div>
                      ))}
                      {w.excess.map((e) => (
                        <div key={JSON.stringify(e)}>{excessText(e, t)}</div>
                      ))}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {rows.length > MAX_ROWS && <p className="muted">{t("resources.moreRows", { shown: MAX_ROWS, total: rows.length })}</p>}
        </>
      )}
    </div>
  );
}
````

<!-- block: file t17-src-components-resources-projectrows-ts step=17.3 path=src/components/resources/projectRows.ts -->
````ts
import type { TFunction } from "i18next";
import type { Excess, Risk, WorkloadResources } from "../../api/types";
import { cpuQuantity, memoryQuantity } from "../../lib/quantity";

/** Строк в таблице проекта (спец. §7.4): дальше — пометка и фильтр по имени. */
export const MAX_ROWS = 100;

export type Sort = "risk" | "reserve";

/** Риск контейнера словами: «app: троттлинг 40 %», «app: OOM 2», «app: память 950Mi из 1Gi». */
export function riskText(r: Risk, t: TFunction): string {
  switch (r.code) {
    case "throttled":
      return t("resources.risk.throttled", { container: r.container, value: Math.round(r.mean * 100) });
    case "oom":
      return t("resources.risk.oom", { container: r.container, count: r.count });
    case "memoryNearLimit":
      return t("resources.risk.memoryNearLimit", { container: r.container, max: memoryQuantity(r.max), limit: memoryQuantity(r.limit) });
  }
}

/** Избыток словами: «app: CPU request 2 при p95 300m». */
export function excessText(e: Excess, t: TFunction): string {
  const f = e.resource === "cpu" ? cpuQuantity : memoryQuantity;
  return t(`resources.excess.${e.resource}`, { container: e.container, request: f(e.request), usage: f(e.usage) });
}

/** Есть что поменять: риск или избыток — как `WorkloadResources::actionable` в Rust. */
export const actionable = (w: WorkloadResources) => w.risks.length > 0 || w.excess.length > 0;

/**
 * Строки таблицы: фильтр «только с рекомендациями» и по имени, сортировка «по риску» (больше рисков — выше, затем
 * избыток) или «по резерву» (больший резерв CPU, затем памяти — выше); имя — при равенстве.
 */
export function projectRows(list: WorkloadResources[], sort: Sort, onlyActionable: boolean, query: string): WorkloadResources[] {
  const q = query.trim().toLowerCase();
  const rows = list.filter((w) => (!onlyActionable || actionable(w)) && (q === "" || w.name.toLowerCase().includes(q)));
  const byName = (a: WorkloadResources, b: WorkloadResources) => a.name.localeCompare(b.name);
  if (sort === "risk") return rows.sort((a, b) => b.risks.length - a.risks.length || b.excess.length - a.excess.length || byName(a, b));
  return rows.sort((a, b) => (b.reserve.cpu ?? 0) - (a.reserve.cpu ?? 0) || (b.reserve.memory ?? 0) - (a.reserve.memory ?? 0) || byName(a, b));
}
````

<!-- block: patch t17-src step=17.3 -->
````diff
diff --git a/docs/i18n/same-as-en.json b/docs/i18n/same-as-en.json
index b0d278e..6599250 100644
--- a/docs/i18n/same-as-en.json
+++ b/docs/i18n/same-as-en.json
@@ -38,6 +38,8 @@
     "change.fieldName.memoryLimit",
     "resources.init",
     "resources.col.cpu",
-    "resources.col.oom"
+    "resources.col.oom",
+    "resources.col.workload",
+    "resources.risk.oom"
   ]
 }
diff --git a/src/components/project/ProjectPanel.tsx b/src/components/project/ProjectPanel.tsx
index 66b2264..9a6f4d2 100644
--- a/src/components/project/ProjectPanel.tsx
+++ b/src/components/project/ProjectPanel.tsx
@@ -17,8 +17,9 @@ import { ProjectOverview } from "./ProjectOverview";
 import { QuotasView } from "./QuotasView";
 import { StorageView } from "./StorageView";
 import { WorkloadsTable } from "./WorkloadsTable";
+import { ProjectUsageView } from "../resources/ProjectUsageView";
 
-type Sub = "overview" | "workloads" | "pods" | "network" | "config" | "storage" | "events" | "quotas";
+type Sub = "overview" | "workloads" | "pods" | "network" | "config" | "storage" | "events" | "resources" | "quotas";
 
 /** Ресурсы «Сети»: подвкладка видна, если читается хоть один. */
 const NETWORK_RESOURCES = [
@@ -45,6 +46,7 @@ export function ProjectPanel({ connectionId, namespace }: { connectionId: string
     { id: "config", label: t("project.tabs.config"), visible: canList(a, "configmaps", "secrets") },
     { id: "storage", label: t("project.tabs.storage"), visible: canList(a, "persistentvolumeclaims") },
     { id: "events", label: t("objects.tabs.events"), visible: canList(a, "events") },
+    { id: "resources", label: t("objects.tabs.resources"), visible: canList(a, "pods") },
     { id: "quotas", label: t("project.tabs.quotas"), visible: canList(a, "resourcequotas", "limitranges") },
   ];
   const visible = tabs.filter((x) => x.visible);
@@ -63,6 +65,7 @@ export function ProjectPanel({ connectionId, namespace }: { connectionId: string
       {active === "config" && <ConfigView connectionId={connectionId} namespace={namespace} />}
       {active === "storage" && <StorageView connectionId={connectionId} namespace={namespace} />}
       {active === "events" && <ProjectEvents connectionId={connectionId} namespace={namespace} />}
+      {active === "resources" && <ProjectUsageView connectionId={connectionId} namespace={namespace} />}
       {active === "quotas" && <QuotasView connectionId={connectionId} namespace={namespace} />}
     </div>
   );
````

<!-- block: locales t17-locales step=17.3 -->
````json
{
  "en": {
    "resources": {
      "sort": "Sort",
      "sortRisk": "By risk",
      "sortReserve": "By reserve",
      "onlyActionable": "Only with recommendations",
      "filter": "Workload name",
      "collecting": "Collecting the project usage (up to 45 s)…",
      "collected": "collected {{when}}",
      "truncated": "The list of workloads or pods is incomplete: the totals cover only what was read.",
      "quotas": "Quotas:",
      "noWorkloads": "No workloads",
      "moreRows": "Showing {{shown}} of {{total}}: narrow the list by name",
      "col": {
        "workload": "Workload",
        "pods": "Pods",
        "requests": "Requests CPU / memory",
        "limits": "Limits CPU / memory",
        "usage": "Usage p95 CPU / max memory",
        "reserve": "Reserve",
        "risks": "Risks and excess"
      },
      "risk": {
        "throttled": "{{container}}: throttling {{value}} %",
        "oom": "{{container}}: OOM {{count}}",
        "memoryNearLimit": "{{container}}: memory {{max}} of {{limit}}"
      },
      "excess": {
        "cpu": "{{container}}: CPU request {{request}} with p95 {{usage}}",
        "memory": "{{container}}: memory request {{request}} with max {{usage}}"
      }
    }
  },
  "ru": {
    "resources": {
      "sort": "Сортировка",
      "sortRisk": "По риску",
      "sortReserve": "По резерву",
      "onlyActionable": "Только с рекомендациями",
      "filter": "Имя workload'а",
      "collecting": "Собираю потребление проекта (до 45 с)…",
      "collected": "собрано {{when}}",
      "truncated": "Список workloads или подов неполный: итоги — только по прочитанному.",
      "quotas": "Квоты:",
      "noWorkloads": "Workloads нет",
      "moreRows": "Показаны {{shown}} из {{total}}: сузьте список по имени",
      "col": {
        "workload": "Workload",
        "pods": "Поды",
        "requests": "Requests CPU / память",
        "limits": "Limits CPU / память",
        "usage": "Потребление p95 CPU / max памяти",
        "reserve": "Резерв",
        "risks": "Риски и избыток"
      },
      "risk": {
        "throttled": "{{container}}: троттлинг {{value}} %",
        "oom": "{{container}}: OOM {{count}}",
        "memoryNearLimit": "{{container}}: память {{max}} из {{limit}}"
      },
      "excess": {
        "cpu": "{{container}}: CPU request {{request}} при p95 {{usage}}",
        "memory": "{{container}}: memory request {{request}} при max {{usage}}"
      }
    }
  }
}
````

- [ ] **Step 4: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 674 passed; 0 failed`

Run: `pnpm test`  
Expected: PASS — `Test Files 72 passed`, `Tests 509 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 198`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 5: Commit.**

```bash
git add \
  docs/i18n/same-as-en.json \
  src/components/project/ProjectPanel.tsx \
  src/components/resources/ProjectUsageView.test.tsx \
  src/components/resources/ProjectUsageView.tsx \
  src/components/resources/projectRows.ts \
  src/locales/en.json \
  src/locales/ru.json
git commit -s -m "feat(ui): Resources tab of a project — a background collection session with the source and quotas, requests and limits of running pods against usage over the window, reserve, risks and excess per workload, sorting by risk or reserve, only with recommendations, at most 100 rows" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 18: Предложения LLM во вкладке «Замечания», блоке объекта и значках: отклонить с причиной, открыть снова, удалить, применено с правками или без; «Открыть в форме» — форма изменения поверх вкладки workload'а или проекта с id предложения

**Files:**
- Port: `src/components/health/ProposalItem.test.tsx` (с правками), `src/components/health/ProposalItem.tsx` (с правками), `src/components/health/useActivePrefill.ts`
- Create (по Kafka-версии): `src/components/health/proposals.ts` ← `src/components/health/proposals.ts`
- Create: `src/components/health/proposals.test.ts`
- Modify: `PORTED.md`, `src/components/TabContent.tsx`, `src/components/health/FindingsPanel.test.tsx`, `src/components/health/FindingsPanel.tsx`, `src/components/health/HealthBlock.tsx`, `src/components/health/findings.test.ts`, `src/components/health/findings.ts`, `src/components/objects/WorkloadPanel.test.tsx`, `src/components/objects/WorkloadPanel.tsx`, `src/components/project/ProjectPanel.tsx`, `src/lib/useHealth.ts`, `src/locales/en.json`, `src/locales/ru.json`, `src/state/workspace.ts`

**Interfaces:**
- Consumes: задачи 10, 14–15: `Proposal`, `api.{setProposalStatus, deleteProposal, deleteAllProposals}`, `ChangeDialog`; O2: `FindingsPanel`, `HealthBlock`, `findings.ts`, `useHealthActions`; Kafka-версия @ `8bbd0bd`: `ProposalItem`, `useActivePrefill`, `proposals.ts`.
- Produces: `state/workspace.ts::ProposalPrefill`, `proposal?` у вкладок `workload` и `project`; `health/proposals.ts::{proposalTab, changeSummary, proposalStatusText}`; `ProposalItem`; `useActivePrefill`; `findings.ts::{openProposals, itemsFor → proposals, BadgeItem kind proposal}`; `useHealthActions().{setProposalStatus, removeProposal, removeAllProposals}`.

Перенос G3 Kafka-версии. «Открыть в форме» у изменения workload'а открывает его вкладку, у границ HPA — вкладку проекта; `useActivePrefill` по новому `nonce` открывает форму заново, закрытие снимает предзаполнение. Форма передаёт `proposalId`, и Rust ставит «применено» или «применено с правками». Значок считает и открытые предложения «критично» и «предупреждение».

- [ ] **Step 1: Перенос из Kafka-версии.**

Файлы копируются с коммита `8bbd0bd` репозитория `~/kafka-explorer` (`$KAFKA_REPO`) и правятся патчами — против перенесённого файла. Строки о них в `PORTED.md` добавляет шаг реализации.

<!-- block: shell t18-port step=18.1 -->
````bash
scripts/port.sh src/components/health/ProposalItem.tsx src/components/health/ProposalItem.tsx
scripts/port.sh src/components/health/ProposalItem.test.tsx src/components/health/ProposalItem.test.tsx
scripts/port.sh src/components/health/useActivePrefill.ts src/components/health/useActivePrefill.ts
````

<!-- block: patch t18-port-src-components-health-proposalitem-tsx step=18.1 -->
````diff
diff --git a/src/components/health/ProposalItem.tsx b/src/components/health/ProposalItem.tsx
--- a/src/components/health/ProposalItem.tsx
+++ b/src/components/health/ProposalItem.tsx
@@ -21,7 +21,7 @@ interface Props {
 }
 
 /**
- * Предложение LLM (спец. G3 §7): заголовок, обоснование как есть, описание изменения (у применённого
+ * Предложение LLM (спец. §10.5): заголовок, обоснование как есть, описание изменения (у применённого
  * с правками — и фактически применённого), подпись клиента и статус. «Открыть в форме» открывает
  * вкладку объекта с заполненной формой; для read-only подключения кнопка неактивна. «Отклонить»
  * спрашивает необязательную причину.
@@ -45,21 +45,13 @@ export function ProposalItem({ connectionId, proposal: p, busy, showTarget = tru
       <div className="finding-head">
         <SeverityMark severity={p.severity} />
         <strong>{p.title}</strong>
-        {showTarget &&
-          (p.change.kind === "createTopic" ? (
-            // Топика ещё нет: имя без ссылки.
-            <span className="health-target">
-              <span className="muted">{t("health.kind.topic")}</span> {p.target.name}
-            </span>
-          ) : (
-            <TargetLink connectionId={connectionId} target={p.target} />
-          ))}
+        {showTarget && <TargetLink connectionId={connectionId} target={p.target} />}
       </div>
       <p className="note-body">{p.rationale}</p>
-      <p className="proposal-change">{changeSummary(p.change, t, lang)}</p>
+      <p className="proposal-change">{changeSummary(p.change, t)}</p>
       {p.appliedChange && (
         <p className="proposal-change">
-          {t("health.proposals.appliedAs", { change: changeSummary(p.appliedChange, t, lang) })}
+          {t("health.proposals.appliedAs", { change: changeSummary(p.appliedChange, t) })}
         </p>
       )}
       <p className="muted">
````

<!-- block: patch t18-port-src-components-health-proposalitem-test-tsx step=18.1 -->
````diff
diff --git a/src/components/health/ProposalItem.test.tsx b/src/components/health/ProposalItem.test.tsx
--- a/src/components/health/ProposalItem.test.tsx
+++ b/src/components/health/ProposalItem.test.tsx
@@ -14,10 +14,10 @@ import { ProposalItem } from "./ProposalItem";
 const applied = (p: Partial<Proposal> = {}): Proposal => ({
   id: "p1",
   severity: "warning",
-  title: "Больше партиций",
-  rationale: "консьюмеров больше, чем партиций",
-  target: { kind: "topic", name: "smoke-g2" },
-  change: { kind: "addPartitions", topic: "smoke-g2", newTotal: 3 },
+  title: "Две реплики",
+  rationale: "одна реплика — простой при выкладке",
+  target: { kind: "workload", namespace: "team-a", name: "web", workloadKind: "Deployment" },
+  change: { kind: "scale", namespace: "team-a", workload: { kind: "Deployment", name: "web" }, replicas: 2 },
   client: "claude-code",
   createdAtMs: 1,
   updatedAtMs: 1,
@@ -41,16 +41,16 @@ beforeEach(() => {
 
 describe("ProposalItem", () => {
   it("у применённого с правками показывает, что применено фактически", () => {
-    renderItem(applied({ appliedChange: { kind: "addPartitions", topic: "smoke-g2", newTotal: 4 } }));
-    expect(screen.getByText("Партиций: до 3")).toBeInTheDocument();
-    const line = screen.getByText("Применено: Партиций: до 4");
+    renderItem(applied({ appliedChange: { kind: "scale", namespace: "team-a", workload: { kind: "Deployment", name: "web" }, replicas: 3 } }));
+    expect(screen.getByText("Реплик: 2")).toBeInTheDocument();
+    const line = screen.getByText("Применено: Реплик: 3");
     expect(line).toHaveClass("proposal-change");
     expect(screen.getByText(/применено с правками/)).toBeInTheDocument();
   });
 
   it("без фактически применённого строки нет", () => {
     renderItem(applied());
-    expect(screen.getByText("Партиций: до 3")).toBeInTheDocument();
+    expect(screen.getByText("Реплик: 2")).toBeInTheDocument();
     expect(screen.queryByText(/^Применено:/)).not.toBeInTheDocument();
   });
 });
````

- [ ] **Step 2: Тесты.**

<!-- block: file t18-src-components-health-proposals-test-ts step=18.2 path=src/components/health/proposals.test.ts -->
````ts
import { describe, expect, it } from "vitest";
import type { Change, Proposal } from "../../api/types";
import { i18n } from "../../i18n";
import { changeSummary, proposalStatusText, proposalTab } from "./proposals";

const t = i18n.getFixedT("ru");
const web = { kind: "Deployment", name: "web" } as const;

function proposal(p: Partial<Proposal> = {}): Proposal {
  return {
    id: "p1",
    severity: "warning",
    title: "Two replicas",
    rationale: "r",
    target: { kind: "workload", namespace: "team-a", name: "web", workloadKind: "Deployment" },
    change: { kind: "scale", namespace: "team-a", workload: web, replicas: 2 },
    client: "claude-ai",
    createdAtMs: 1,
    updatedAtMs: 1,
    status: "open",
    ...p,
  };
}

describe("changeSummary", () => {
  it("ресурсы по контейнерам: значение или «убрать»", () => {
    const c: Change = {
      kind: "setResources",
      namespace: "team-a",
      workload: web,
      containers: [{ name: "app", requests: { cpu: "250m" }, limits: { cpu: null, memory: "1Gi" } }],
    };
    expect(changeSummary(c, t)).toBe("Ресурсы: app — CPU request 250m, CPU limit убрать, Memory limit 1Gi");
  });

  it("реплики, перезапуск и границы HPA", () => {
    expect(changeSummary({ kind: "scale", namespace: "a", workload: web, replicas: 3 }, t)).toBe("Реплик: 3");
    expect(changeSummary({ kind: "rolloutRestart", namespace: "a", workload: web }, t)).toBe("Перезапуск");
    expect(changeSummary({ kind: "setHpaBounds", namespace: "a", hpa: "web", maxReplicas: 8 }, t)).toBe("Границы HPA web: не менять–8");
  });
});

describe("proposalTab", () => {
  it("изменение workload'а — его вкладка, границы HPA — вкладка проекта", () => {
    const p = proposal();
    expect(proposalTab("c1", p, 5)).toEqual({ kind: "workload", connectionId: "c1", namespace: "team-a", workloadKind: "Deployment", name: "web", proposal: { proposal: p, nonce: 5 } });
    const hpa = proposal({ change: { kind: "setHpaBounds", namespace: "team-a", hpa: "web", minReplicas: 2, maxReplicas: 6 } });
    expect(proposalTab("c1", hpa, 5)).toMatchObject({ kind: "project", namespace: "team-a" });
  });
});

describe("proposalStatusText", () => {
  it("статусы закрытого предложения", () => {
    expect(proposalStatusText(proposal(), t, 1, "ru")).toBeNull();
    expect(proposalStatusText(proposal({ status: "rejected", rejectReason: "позже" }), t, 1, "ru")).toBe("отклонено: позже");
    expect(proposalStatusText(proposal({ status: "withdrawn" }), t, 1, "ru")).toBe("отозвано");
  });
});
````

<!-- block: patch t18-tests step=18.2 -->
````diff
diff --git a/src/components/health/FindingsPanel.test.tsx b/src/components/health/FindingsPanel.test.tsx
index a4fee5c..461212e 100644
--- a/src/components/health/FindingsPanel.test.tsx
+++ b/src/components/health/FindingsPanel.test.tsx
@@ -1,7 +1,7 @@
 import { screen, waitFor, within } from "@testing-library/react";
 import userEvent from "@testing-library/user-event";
 import { beforeEach, describe, expect, it, vi } from "vitest";
-import type { Annotation, HealthView } from "../../api/types";
+import type { Annotation, HealthView, Proposal } from "../../api/types";
 import { useWorkspace } from "../../state/workspace";
 import { finding } from "../../test/health";
 import { renderWithClient } from "../../test/render";
@@ -19,6 +19,9 @@ vi.mock("../../api/client", async (importOriginal) => {
       setAnnotationStatus: vi.fn(),
       deleteAnnotation: vi.fn(),
       deleteAllAnnotations: vi.fn(),
+      setProposalStatus: vi.fn(),
+      deleteProposal: vi.fn(),
+      deleteAllProposals: vi.fn(),
     },
   };
 });
@@ -138,7 +141,8 @@ describe("FindingsPanel", () => {
     await user.type(screen.getByRole("textbox", { name: "Поиск по имени или проекту" }), "team-b");
     expect(screen.getByText("Deployment team-b/legacy: образы с тегом latest")).toBeInTheDocument();
     expect(screen.queryByText("Deployment team-a/web: образы с тегом latest")).not.toBeInTheDocument();
-    expect(screen.getByText("Под фильтры ничего не подходит.")).toBeInTheDocument();
+    // Пусто и у предложений, и у замечаний.
+    expect(screen.getAllByText("Под фильтры ничего не подходит.")).toHaveLength(2);
   });
 
   it("закрывает и удаляет замечания LLM", async () => {
@@ -160,4 +164,33 @@ describe("FindingsPanel", () => {
     expect(api.deleteAllAnnotations).toHaveBeenCalledWith("c1");
     await waitFor(() => expect(screen.getByText("Замечаний LLM нет.")).toBeInTheDocument());
   });
+
+  it("предложения LLM: отклонение с причиной и «Открыть в форме» — вкладка workload'а с формой", async () => {
+    const proposal: Proposal = {
+      id: "p1",
+      severity: "warning",
+      title: "Two replicas",
+      rationale: "one replica is a risk",
+      target: { kind: "workload", namespace: "team-a", name: "web", workloadKind: "Deployment" },
+      change: { kind: "scale", namespace: "team-a", workload: { kind: "Deployment", name: "web" }, replicas: 2 },
+      client: "claude-ai",
+      createdAtMs: 1,
+      updatedAtMs: 1,
+      status: "open",
+    };
+    vi.mocked(api.getHealth).mockResolvedValue(view({ proposals: [proposal] }));
+    vi.mocked(api.setProposalStatus).mockResolvedValue(view({ proposals: [{ ...proposal, status: "rejected", rejectReason: "later" }] }));
+    useWorkspace.setState({ tabs: [], activeId: null });
+    const user = userEvent.setup();
+    renderWithClient(<FindingsPanel connectionId="c1" />);
+    const item = (await screen.findByText("Two replicas")).closest("li")!;
+    expect(within(item).getByText("Реплик: 2")).toBeInTheDocument();
+    await user.click(within(item).getByRole("button", { name: "Открыть в форме" }));
+    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "workload", name: "web", proposal: { proposal } });
+    await user.click(within(item).getByRole("button", { name: "Отклонить" }));
+    await user.type(within(item).getByLabelText("Причина (необязательно)"), "later");
+    await user.click(within(item).getByRole("button", { name: "Отклонить предложение" }));
+    expect(api.setProposalStatus).toHaveBeenCalledWith("c1", "p1", "rejected", "later");
+    await waitFor(() => expect(screen.getByText(/отклонено: later/)).toBeInTheDocument());
+  });
 });
diff --git a/src/components/health/findings.test.ts b/src/components/health/findings.test.ts
index a187275..27fc273 100644
--- a/src/components/health/findings.test.ts
+++ b/src/components/health/findings.test.ts
@@ -167,7 +167,7 @@ describe("badge tooltip", () => {
     const b = badgeFor(v, isWorkload);
     expect(b).toMatchObject({ count: 4, severity: "critical" });
     // Внутри строгости: сначала находки, затем замечания; порядок как в данных.
-    expect(b?.items.map((i) => (i.kind === "finding" ? i.finding.target.name : i.note.title))).toEqual(["c1", "Critical note", "w1", "Warning note"]);
+    expect(b?.items.map((i) => (i.kind === "finding" ? i.finding.target.name : i.kind === "note" ? i.note.title : i.proposal.title))).toEqual(["c1", "Critical note", "w1", "Warning note"]);
     expect(badgeTooltip(b!, t)).toBe(
       [
         "Требуют внимания: 4",
diff --git a/src/components/objects/WorkloadPanel.test.tsx b/src/components/objects/WorkloadPanel.test.tsx
index 545b99e..2b917db 100644
--- a/src/components/objects/WorkloadPanel.test.tsx
+++ b/src/components/objects/WorkloadPanel.test.tsx
@@ -12,11 +12,11 @@ vi.mock("@tanstack/react-virtual", () => ({
 
 vi.mock("../../api/client", async (importOriginal) => {
   const orig = await importOriginal<typeof import("../../api/client")>();
-  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }), getWorkload: vi.fn(), startWatch: vi.fn(), stopSession: vi.fn(), listEvents: vi.fn(), getYaml: vi.fn() } };
+  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }), getWorkload: vi.fn(), listConnections: vi.fn().mockResolvedValue([]), startWatch: vi.fn(), stopSession: vi.fn(), listEvents: vi.fn(), getYaml: vi.fn() } };
 });
 
 import { api } from "../../api/client";
-import type { WorkloadDetails } from "../../api/types";
+import type { Proposal, WorkloadDetails } from "../../api/types";
 import { container, workloadRow } from "../../test/objects";
 import { WorkloadPanel } from "./WorkloadPanel";
 
@@ -88,4 +88,27 @@ describe("WorkloadPanel", () => {
     expect(screen.getByText(/приостановлен/)).toBeInTheDocument();
     expect(screen.queryByText("История выкладки")).not.toBeInTheDocument();
   });
+
+  it("«Открыть в форме» предложения LLM: форма поверх вкладки, «Отмена» её закрывает", async () => {
+    vi.mocked(api.getWorkload).mockResolvedValue(DETAILS);
+    const user = userEvent.setup();
+    const proposal: Proposal = {
+      id: "p1",
+      severity: "warning",
+      title: "Two replicas",
+      rationale: "r",
+      target: { kind: "workload", namespace: "team-a", name: "web", workloadKind: "Deployment" },
+      change: { kind: "scale", namespace: "team-a", workload: { kind: "Deployment", name: "web" }, replicas: 2 },
+      client: null,
+      createdAtMs: 1,
+      updatedAtMs: 1,
+      status: "open",
+    };
+    renderWithClient(<WorkloadPanel connectionId="c1" namespace="team-a" kind="Deployment" name="web" proposal={{ proposal, nonce: 1 }} />);
+    const dialog = screen.getByRole("dialog", { name: "Реплики: Deployment web" });
+    expect(dialog).toHaveTextContent("Применяется предложение LLM: Two replicas");
+    expect(screen.getByLabelText("Реплики")).toHaveValue(2);
+    await user.click(screen.getByRole("button", { name: "Отмена" }));
+    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
+  });
 });
````

- [ ] **Step 3: Увидеть падение.**

Run: `pnpm vitest run src/components/health/FindingsPanel.test.tsx src/components/health/findings.test.ts src/components/health/proposals.test.ts src/components/objects/WorkloadPanel.test.tsx`  
Expected: FAIL — нет `ProposalItem` и `proposals.ts`; «Открыть в форме» не открывает форму.

- [ ] **Step 4: Реализация.**

<!-- block: file t18-src-components-health-proposals-ts step=18.4 path=src/components/health/proposals.ts -->
````ts
import type { TFunction } from "i18next";
import type { Change, Proposal, ResourceValues } from "../../api/types";
import { formatAgo } from "../../lib/formatAgo";
import type { TabSpec } from "../../state/workspace";

/**
 * Вкладка, в которой предложение открывается заполненной формой (спец. §10.5): у изменений workload'а — его вкладка,
 * у границ HPA — вкладка проекта.
 */
export function proposalTab(connectionId: string, proposal: Proposal, nonce = Date.now()): TabSpec {
  const prefill = { proposal, nonce };
  const c = proposal.change;
  if (c.kind === "setHpaBounds" || c.kind === "deletePod") return { kind: "project", connectionId, namespace: c.namespace, proposal: prefill };
  return { kind: "workload", connectionId, namespace: c.namespace, workloadKind: c.workload.kind, name: c.workload.name, proposal: prefill };
}

/** Значения ресурсов словами: «CPU request 250m, memory limit — убрать». */
function valuesText(part: "requests" | "limits", v: ResourceValues, t: TFunction): string[] {
  return (["cpu", "memory"] as const).flatMap((r) =>
    v[r] === undefined ? [] : [`${t(`change.diff.${part}.${r}`)} ${v[r] === null ? t("health.proposals.change.remove") : v[r]}`],
  );
}

/** Описание изменения для карточки предложения: что именно меняется, на языке интерфейса. */
export function changeSummary(change: Change, t: TFunction): string {
  switch (change.kind) {
    case "setResources":
      return `${t("health.proposals.change.setResources")} ${change.containers
        .map((c) => `${c.name} — ${[...valuesText("requests", c.requests, t), ...valuesText("limits", c.limits, t)].join(", ")}`)
        .join("; ")}`;
    case "scale":
      return t("health.proposals.change.scale", { replicas: change.replicas });
    case "rolloutRestart":
      return t("health.proposals.change.rolloutRestart");
    case "setHpaBounds":
      return t("health.proposals.change.setHpaBounds", { hpa: change.hpa, min: change.minReplicas ?? t("health.proposals.change.keep"), max: change.maxReplicas });
    case "deletePod":
      return t("change.title.deletePod");
  }
}

/** Статус закрытого предложения: «применено 5 минут назад», «применено с правками …», «отклонено: …», «отозвано». */
export function proposalStatusText(p: Proposal, t: TFunction, nowMs: number, lang: string): string | null {
  switch (p.status) {
    case "open":
      return null;
    case "applied": {
      const when = formatAgo(p.closedAtMs ?? p.updatedAtMs, nowMs, lang);
      return p.appliedAsProposed === false ? t("health.proposals.status.appliedEdited", { when }) : t("health.proposals.status.applied", { when });
    }
    case "rejected":
      return p.rejectReason ? t("health.proposals.status.rejectedWithReason", { reason: p.rejectReason }) : t("health.proposals.status.rejected");
    case "withdrawn":
      return t("health.proposals.status.withdrawn");
  }
}
````

<!-- block: patch t18-src step=18.4 -->
````diff
diff --git a/PORTED.md b/PORTED.md
index 2eea260..96b5caf 100644
--- a/PORTED.md
+++ b/PORTED.md
@@ -195,3 +195,7 @@
 | `src/components/common/ConfirmDialog.tsx` | `src/components/common/ConfirmDialog.tsx` | `8bbd0bd` | адаптирован: подписи — имя пода |
 | `src/components/common/ConfirmDialog.test.tsx` | `src/components/common/ConfirmDialog.test.tsx` | `8bbd0bd` | адаптирован: объекты Kubernetes в примерах |
 | `src/lib/useConnection.ts` | `src/lib/useConnection.ts` | `8bbd0bd` | как есть |
+| `src/components/health/ProposalItem.tsx` | `src/components/health/ProposalItem.tsx` | `8bbd0bd` | адаптирован: объект — всегда ссылка, описание изменений Kubernetes |
+| `src/components/health/ProposalItem.test.tsx` | `src/components/health/ProposalItem.test.tsx` | `8bbd0bd` | адаптирован: предложение о репликах |
+| `src/components/health/useActivePrefill.ts` | `src/components/health/useActivePrefill.ts` | `8bbd0bd` | как есть |
+| `src/components/health/proposals.ts` | `src/components/health/proposals.ts` | `8bbd0bd` | адаптирован: вкладка формы и описание изменений Kubernetes (§9) |
diff --git a/src/components/TabContent.tsx b/src/components/TabContent.tsx
index a0f978a..07552aa 100644
--- a/src/components/TabContent.tsx
+++ b/src/components/TabContent.tsx
@@ -53,9 +53,9 @@ export function TabContent({ tab }: { tab: Tab }) {
     case "findings":
       return <FindingsPanel connectionId={tab.connectionId} />;
     case "project":
-      return <ProjectPanel connectionId={tab.connectionId} namespace={tab.namespace} />;
+      return <ProjectPanel connectionId={tab.connectionId} namespace={tab.namespace} proposal={tab.proposal} />;
     case "workload":
-      return <WorkloadPanel connectionId={tab.connectionId} namespace={tab.namespace} kind={tab.workloadKind} name={tab.name} />;
+      return <WorkloadPanel connectionId={tab.connectionId} namespace={tab.namespace} kind={tab.workloadKind} name={tab.name} proposal={tab.proposal} />;
     case "pod":
       return <PodPanel connectionId={tab.connectionId} namespace={tab.namespace} name={tab.name} />;
   }
diff --git a/src/components/health/FindingsPanel.tsx b/src/components/health/FindingsPanel.tsx
index 660cbec..438288b 100644
--- a/src/components/health/FindingsPanel.tsx
+++ b/src/components/health/FindingsPanel.tsx
@@ -7,6 +7,7 @@ import { Hint } from "../common/Hint";
 import { CheckBar } from "./CheckBar";
 import { findingsOf, SEVERITIES, newestClosedFirst } from "./findings";
 import { FindingItem, NoteItem, SeverityMark } from "./parts";
+import { ProposalItem } from "./ProposalItem";
 
 const KINDS: TargetKind[] = [
   "cluster",
@@ -27,7 +28,7 @@ const KINDS: TargetKind[] = [
 
 type Source = "all" | "rules" | "llm";
 
-/** Вкладка «Замечания»: находки последней проверки по правилам и замечания LLM (спец. §8.1). */
+/** Вкладка «Замечания»: находки последней проверки по правилам, замечания и предложения LLM (спец. §8.1, §10.5). */
 export function FindingsPanel({ connectionId }: { connectionId: string }) {
   const { t } = useTranslation();
   const health = useHealth(connectionId);
@@ -38,6 +39,7 @@ export function FindingsPanel({ connectionId }: { connectionId: string }) {
   const [query, setQuery] = useState("");
   const [showHidden, setShowHidden] = useState(false);
   const [confirmAll, setConfirmAll] = useState(false);
+  const [confirmAllProposals, setConfirmAllProposals] = useState(false);
 
   const view = health.data;
   const hidden = new Set(view?.ignored ?? []);
@@ -54,8 +56,30 @@ export function FindingsPanel({ connectionId }: { connectionId: string }) {
   const notes = source === "rules" ? [] : (view?.annotations ?? []).filter(matches);
   const open = notes.filter((a) => a.status === "open");
   const closed = newestClosedFirst(notes.filter((a) => a.status !== "open"));
-  const busy = actions.ignore.isPending || actions.setStatus.isPending || actions.remove.isPending || actions.removeAll.isPending;
-  const error = actions.ignore.error ?? actions.setStatus.error ?? actions.remove.error ?? actions.removeAll.error;
+  const proposals = source === "rules" ? [] : (view?.proposals ?? []).filter(matches);
+  const openProposals = proposals.filter((p) => p.status === "open");
+  const closedProposals = newestClosedFirst(proposals.filter((p) => p.status !== "open"));
+  const busy =
+    actions.ignore.isPending ||
+    actions.setStatus.isPending ||
+    actions.remove.isPending ||
+    actions.removeAll.isPending ||
+    actions.setProposalStatus.isPending ||
+    actions.removeProposal.isPending ||
+    actions.removeAllProposals.isPending;
+  const error =
+    actions.ignore.error ??
+    actions.setStatus.error ??
+    actions.remove.error ??
+    actions.removeAll.error ??
+    actions.setProposalStatus.error ??
+    actions.removeProposal.error ??
+    actions.removeAllProposals.error;
+  const proposalStatus = (id: string) => (status: "rejected" | "open", reason?: string) =>
+    actions.setProposalStatus.mutateAsync({ id, status, reason }).then(
+      () => true,
+      () => false,
+    );
   const filtered = severity !== "all" || kind !== "all" || q !== "";
 
   return (
@@ -127,6 +151,52 @@ export function FindingsPanel({ connectionId }: { connectionId: string }) {
         </section>
       )}
 
+      {source !== "rules" && (
+        <section aria-labelledby="health-proposals">
+          <h3 id="health-proposals">
+            {t("health.proposals.title")} <Hint k="hints.health.proposals" label={t("health.proposals.title")} />
+          </h3>
+          <p className="hint">{t("health.proposals.intro")}</p>
+          {openProposals.length === 0 && <p className="empty">{filtered ? t("health.nothingMatches") : t("health.proposals.none")}</p>}
+          <ul className="finding-list">
+            {openProposals.map((p) => (
+              <ProposalItem key={p.id} connectionId={connectionId} proposal={p} busy={busy} onStatus={proposalStatus(p.id)} onDelete={() => actions.removeProposal.mutate(p.id)} />
+            ))}
+          </ul>
+          {closedProposals.length > 0 && (
+            <details className="finding-group">
+              <summary>{t("health.closedNotes", { count: closedProposals.length })}</summary>
+              <ul className="finding-list">
+                {closedProposals.map((p) => (
+                  <ProposalItem key={p.id} connectionId={connectionId} proposal={p} busy={busy} onStatus={proposalStatus(p.id)} onDelete={() => actions.removeProposal.mutate(p.id)} />
+                ))}
+              </ul>
+            </details>
+          )}
+          {(view?.proposals.length ?? 0) > 0 &&
+            (confirmAllProposals ? (
+              <p role="alert">
+                {t("health.proposals.deleteAllConfirm")}{" "}
+                <button
+                  type="button"
+                  className="danger"
+                  disabled={busy}
+                  onClick={() => actions.removeAllProposals.mutate(undefined, { onSettled: () => setConfirmAllProposals(false) })}
+                >
+                  {t("health.proposals.deleteAll")}
+                </button>{" "}
+                <button type="button" onClick={() => setConfirmAllProposals(false)}>
+                  {t("common.cancel")}
+                </button>
+              </p>
+            ) : (
+              <button type="button" className="link danger" disabled={busy} onClick={() => setConfirmAllProposals(true)}>
+                {t("health.proposals.deleteAll")}
+              </button>
+            ))}
+        </section>
+      )}
+
       {source !== "rules" && (
         <section aria-labelledby="health-notes">
           <h3 id="health-notes">{t("health.notes")}</h3>
diff --git a/src/components/health/HealthBlock.tsx b/src/components/health/HealthBlock.tsx
index 7ea42ae..fc8b3fe 100644
--- a/src/components/health/HealthBlock.tsx
+++ b/src/components/health/HealthBlock.tsx
@@ -4,6 +4,7 @@ import { useWorkspace } from "../../state/workspace";
 import { ErrorBanner } from "../common/ErrorBanner";
 import { badgeFor, badgeTooltip, itemsFor, type Match } from "./findings";
 import { FindingItem, NoteItem, SeverityMark } from "./parts";
+import { ProposalItem } from "./ProposalItem";
 
 /**
  * Значок объекта: число находок и замечаний уровня «критично» и «предупреждение»; в подсказке — их заголовки.
@@ -21,16 +22,21 @@ export function HealthBadge({ connectionId, match }: { connectionId: string; mat
   );
 }
 
-/** Блок «Замечания» на экране объекта: его находки и открытые замечания LLM с действиями. */
+/** Блок «Замечания» на экране объекта: его находки, открытые замечания и предложения LLM с действиями. */
 export function HealthBlock({ connectionId, match, showTarget = false }: { connectionId: string; match: Match; showTarget?: boolean }) {
   const { t } = useTranslation();
   const health = useHealth(connectionId);
   const actions = useHealthActions(connectionId);
   const open = useWorkspace((s) => s.open);
-  const { findings, notes } = itemsFor(health.data, match);
-  if (findings.length === 0 && notes.length === 0) return null;
-  const busy = actions.ignore.isPending || actions.setStatus.isPending || actions.remove.isPending;
-  const error = actions.ignore.error ?? actions.setStatus.error ?? actions.remove.error;
+  const { findings, notes, proposals } = itemsFor(health.data, match);
+  if (findings.length === 0 && notes.length === 0 && proposals.length === 0) return null;
+  const busy =
+    actions.ignore.isPending ||
+    actions.setStatus.isPending ||
+    actions.remove.isPending ||
+    actions.setProposalStatus.isPending ||
+    actions.removeProposal.isPending;
+  const error = actions.ignore.error ?? actions.setStatus.error ?? actions.remove.error ?? actions.setProposalStatus.error ?? actions.removeProposal.error;
   return (
     <section className="health-block" aria-label={t("health.title")}>
       <h3>
@@ -62,6 +68,17 @@ export function HealthBlock({ connectionId, match, showTarget = false }: { conne
             onDelete={() => actions.remove.mutate(a.id)}
           />
         ))}
+        {proposals.map((p) => (
+          <ProposalItem
+            key={p.id}
+            connectionId={connectionId}
+            proposal={p}
+            busy={busy}
+            showTarget={showTarget}
+            onStatus={(status, reason) => actions.setProposalStatus.mutateAsync({ id: p.id, status, reason }).then(() => true, () => false)}
+            onDelete={() => actions.removeProposal.mutate(p.id)}
+          />
+        ))}
       </ul>
       {error && <ErrorBanner error={error} />}
     </section>
diff --git a/src/components/health/findings.ts b/src/components/health/findings.ts
index 05a1bc6..7ed6cd0 100644
--- a/src/components/health/findings.ts
+++ b/src/components/health/findings.ts
@@ -1,5 +1,5 @@
 import type { TFunction } from "i18next";
-import type { Annotation, Finding, HealthTarget, HealthView, Severity, WorkloadKind } from "../../api/types";
+import type { Annotation, Finding, HealthTarget, HealthView, Proposal, Severity, WorkloadKind } from "../../api/types";
 
 export const SEVERITY_RANK: Record<Severity, number> = { info: 0, warning: 1, critical: 2 };
 export const SEVERITIES: Severity[] = ["critical", "warning", "info"];
@@ -112,7 +112,11 @@ export function openNotes(view: HealthView | undefined): Annotation[] {
   return (view?.annotations ?? []).filter((a) => a.status === "open");
 }
 
-/** Закрытые замечания — недавно закрытые сверху (по времени закрытия, иначе обновления). */
+export function openProposals(view: HealthView | undefined): Proposal[] {
+  return (view?.proposals ?? []).filter((p) => p.status === "open");
+}
+
+/** Закрытые замечания и предложения — недавно закрытые сверху (по времени закрытия, иначе обновления). */
 export function newestClosedFirst<T extends { closedAtMs?: number; updatedAtMs: number }>(items: T[]): T[] {
   return [...items].sort((a, b) => (b.closedAtMs ?? b.updatedAtMs) - (a.closedAtMs ?? a.updatedAtMs));
 }
@@ -144,34 +148,39 @@ export const matchPod =
     t.namespace === namespace &&
     ((t.kind === "pod" && t.name === name) || (t.kind === "workload" && Array.isArray(params?.pods) && params.pods.includes(name)));
 
-/** Что относится к объекту: видимые находки и открытые замечания. Без `match` — всё подключение. */
+/** Что относится к объекту: видимые находки, открытые замечания и открытые предложения. Без `match` — всё подключение. */
 export function itemsFor(view: HealthView | undefined, match?: Match) {
   const m = match ?? (() => true);
   return {
     findings: findingsOf(view, false).filter((f) => m(f.target, f.params)),
     notes: openNotes(view).filter((a) => m(a.target)),
+    proposals: openProposals(view).filter((p) => m(p.target)),
   };
 }
 
-/** Что посчитал значок: находка или открытое замечание уровня «критично» или «предупреждение». */
-export type BadgeItem = { kind: "finding"; severity: Severity; finding: Finding } | { kind: "note"; severity: Severity; note: Annotation };
+/** Что посчитал значок: находка, открытое замечание или открытое предложение уровня «критично» или «предупреждение». */
+export type BadgeItem =
+  | { kind: "finding"; severity: Severity; finding: Finding }
+  | { kind: "note"; severity: Severity; note: Annotation }
+  | { kind: "proposal"; severity: Severity; proposal: Proposal };
 
 export interface Badge {
   count: number;
   severity: Severity;
-  /** Самые строгие первыми; внутри строгости — сначала находки, затем замечания; порядок данных сохраняется. */
+  /** Самые строгие первыми; внутри строгости — сначала находки, затем замечания, затем предложения; порядок данных сохраняется. */
   items: BadgeItem[];
 }
 
 /** Сколько заголовков показывает подсказка значка; остальное сворачивается в «…». */
 export const BADGE_TITLES_MAX = 10;
 
-/** Значок: число видимых находок и открытых замечаний уровня «критично» и «предупреждение»; без `match` — всего подключения. */
+/** Значок: число видимых находок, открытых замечаний и предложений уровня «критично» и «предупреждение»; без `match` — всего подключения. */
 export function badgeFor(view: HealthView | undefined, match?: Match): Badge | null {
-  const { findings, notes } = itemsFor(view, match);
+  const { findings, notes, proposals } = itemsFor(view, match);
   const items: BadgeItem[] = [
     ...findings.map((finding): BadgeItem => ({ kind: "finding", severity: finding.severity, finding })),
     ...notes.map((note): BadgeItem => ({ kind: "note", severity: note.severity, note })),
+    ...proposals.map((proposal): BadgeItem => ({ kind: "proposal", severity: proposal.severity, proposal })),
   ]
     .filter((i) => i.severity !== "info")
     // Сортировка стабильна: порядок внутри одной строгости остаётся прежним.
@@ -185,12 +194,12 @@ const oneLine = (s: string) => s.replace(/\s+/g, " ").trim();
 /**
  * Подсказка значка: первая строка — «Требуют внимания: N», далее заголовки учтённого (не больше 10, затем «…»).
  * Строки через перевод строки: нативная подсказка `title` его показывает. Пробелы и переводы строк
- * в заголовке замечания LLM схлопываются: одна строка подсказки — один заголовок.
+ * в заголовке замечания или предложения LLM схлопываются: одна строка подсказки — один заголовок.
  */
 export function badgeTooltip(badge: Badge, t: TFunction): string {
   const titles = badge.items
     .slice(0, BADGE_TITLES_MAX)
-    .map((i) => (i.kind === "finding" ? findingTexts(i.finding, t).title : oneLine(i.note.title)));
+    .map((i) => (i.kind === "finding" ? findingTexts(i.finding, t).title : i.kind === "note" ? oneLine(i.note.title) : oneLine(i.proposal.title)));
   if (badge.items.length > BADGE_TITLES_MAX) titles.push("…");
   return [t("health.badgeTitle", { count: badge.count }), ...titles].join("\n");
 }
diff --git a/src/components/objects/WorkloadPanel.tsx b/src/components/objects/WorkloadPanel.tsx
index 8e20235..77befa5 100644
--- a/src/components/objects/WorkloadPanel.tsx
+++ b/src/components/objects/WorkloadPanel.tsx
@@ -12,7 +12,10 @@ import { ReadError } from "../common/ReadError";
 import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
 import { SubTabs } from "../common/SubTabs";
 import { YamlView } from "../common/YamlView";
-import { WorkloadActions } from "../change/ChangeActions";
+import { WorkloadActions, hintsOf } from "../change/ChangeActions";
+import { ChangeDialog } from "../change/ChangeDialog";
+import { useActivePrefill } from "../health/useActivePrefill";
+import type { ProposalPrefill } from "../../state/workspace";
 import { WorkloadResources } from "../resources/WorkloadResources";
 import { matchWorkload } from "../health/findings";
 import { HealthBlock } from "../health/HealthBlock";
@@ -23,9 +26,25 @@ import { PodList } from "./PodList";
 type Sub = "overview" | "pods" | "resources" | "events" | "yaml";
 
 /** Вкладка workload'а (спец. §6.4): обзор, его поды, ресурсы, события вместе с подами, YAML. */
-export function WorkloadPanel({ connectionId, namespace, kind, name }: { connectionId: string; namespace: string; kind: WorkloadKind; name: string }) {
+export function WorkloadPanel({
+  connectionId,
+  namespace,
+  kind,
+  name,
+  proposal,
+}: {
+  connectionId: string;
+  namespace: string;
+  kind: WorkloadKind;
+  name: string;
+  /** «Открыть в форме» предложения LLM: форма изменения поверх вкладки. */
+  proposal?: ProposalPrefill;
+}) {
   const { t } = useTranslation();
   const [sub, setSub] = useState<Sub>("overview");
+  const [prefill, clearPrefill] = useActivePrefill(proposal);
+  // Подсказки формы ресурсов — из того же запроса, что и обзор.
+  const details = useQuery({ queryKey: ["workload", connectionId, namespace, kind, name], queryFn: () => api.getWorkload(connectionId, namespace, kind, name), enabled: prefill !== undefined });
   const tabs: { id: Sub; label: string }[] = [
     { id: "overview", label: t("objects.tabs.overview") },
     { id: "pods", label: t("objects.tabs.pods") },
@@ -47,6 +66,16 @@ export function WorkloadPanel({ connectionId, namespace, kind, name }: { connect
       {sub === "resources" && <WorkloadResources connectionId={connectionId} namespace={namespace} kind={kind} name={name} />}
       {sub === "events" && <ObjectEvents connectionId={connectionId} namespace={namespace} object={{ kind, name }} />}
       {sub === "yaml" && <YamlView connectionId={connectionId} namespace={namespace} kind={kind} name={name} />}
+      {prefill && (
+        <ChangeDialog
+          key={prefill.nonce}
+          connectionId={connectionId}
+          initial={prefill.proposal.change}
+          proposal={prefill.proposal}
+          hints={details.data ? hintsOf(details.data) : undefined}
+          onClose={clearPrefill}
+        />
+      )}
     </div>
   );
 }
diff --git a/src/components/project/ProjectPanel.tsx b/src/components/project/ProjectPanel.tsx
index 9a6f4d2..4e58c50 100644
--- a/src/components/project/ProjectPanel.tsx
+++ b/src/components/project/ProjectPanel.tsx
@@ -18,6 +18,9 @@ import { QuotasView } from "./QuotasView";
 import { StorageView } from "./StorageView";
 import { WorkloadsTable } from "./WorkloadsTable";
 import { ProjectUsageView } from "../resources/ProjectUsageView";
+import { ChangeDialog } from "../change/ChangeDialog";
+import { useActivePrefill } from "../health/useActivePrefill";
+import type { ProposalPrefill } from "../../state/workspace";
 
 type Sub = "overview" | "workloads" | "pods" | "network" | "config" | "storage" | "events" | "resources" | "quotas";
 
@@ -32,9 +35,11 @@ const NETWORK_RESOURCES = [
 ];
 
 /** Вкладка проекта (спец. §6.3). Подвкладки без прав на чтение не показываются (§5.4); показывается одна, её watch идёт, пока она открыта. */
-export function ProjectPanel({ connectionId, namespace }: { connectionId: string; namespace: string }) {
+export function ProjectPanel({ connectionId, namespace, proposal }: { connectionId: string; namespace: string; proposal?: ProposalPrefill }) {
   const { t } = useTranslation();
   const [sub, setSub] = useState<Sub>("overview");
+  // «Открыть в форме» предложения о границах HPA: форма изменения поверх вкладки проекта.
+  const [prefill, clearPrefill] = useActivePrefill(proposal);
   // Права перечитываются по таймеру и по «Обновить» в обзоре, иначе подвкладка, скрытая по устаревшим правам, не вернулась бы.
   const access = useNamespaceAccess(connectionId, namespace);
   const a = access.data;
@@ -67,6 +72,7 @@ export function ProjectPanel({ connectionId, namespace }: { connectionId: string
       {active === "events" && <ProjectEvents connectionId={connectionId} namespace={namespace} />}
       {active === "resources" && <ProjectUsageView connectionId={connectionId} namespace={namespace} />}
       {active === "quotas" && <QuotasView connectionId={connectionId} namespace={namespace} />}
+      {prefill && <ChangeDialog key={prefill.nonce} connectionId={connectionId} initial={prefill.proposal.change} proposal={prefill.proposal} onClose={clearPrefill} />}
     </div>
   );
 }
diff --git a/src/lib/useHealth.ts b/src/lib/useHealth.ts
index 1566a20..bfc09d8 100644
--- a/src/lib/useHealth.ts
+++ b/src/lib/useHealth.ts
@@ -4,6 +4,9 @@ import { useEffect } from "react";
 import { api } from "../api/client";
 import type { AnnotationStatus, HealthView } from "../api/types";
 
+/** Статус, который человек ставит предложению: «Отклонить» или «Открыть снова». */
+type ProposalAction = "rejected" | "open";
+
 /** Событие из Rust (`crate::health::CHANGED_EVENT`): изменилась проверка или замечания подключения. */
 export const HEALTH_EVENT = "health://changed";
 export const healthKey = (connectionId: string) => ["health", connectionId] as const;
@@ -40,7 +43,7 @@ export function useHealthEvents(): void {
   }, [qc]);
 }
 
-/** Действия вкладки «Замечания»: каждое возвращает свежее состояние подключения. */
+/** Действия вкладки «Замечания» (находки, замечания и предложения LLM): каждое возвращает свежее состояние подключения. */
 export function useHealthActions(connectionId: string) {
   const qc = useQueryClient();
   const onSuccess = (v: HealthView) => qc.setQueryData(healthKey(connectionId), v);
@@ -56,5 +59,11 @@ export function useHealthActions(connectionId: string) {
     }),
     remove: useMutation({ mutationFn: (id: string) => api.deleteAnnotation(connectionId, id), onSuccess }),
     removeAll: useMutation({ mutationFn: () => api.deleteAllAnnotations(connectionId), onSuccess }),
+    setProposalStatus: useMutation({
+      mutationFn: (p: { id: string; status: ProposalAction; reason?: string }) => api.setProposalStatus(connectionId, p.id, p.status, p.reason ?? null),
+      onSuccess,
+    }),
+    removeProposal: useMutation({ mutationFn: (id: string) => api.deleteProposal(connectionId, id), onSuccess }),
+    removeAllProposals: useMutation({ mutationFn: () => api.deleteAllProposals(connectionId), onSuccess }),
   };
 }
diff --git a/src/state/workspace.ts b/src/state/workspace.ts
index 224fc3f..37dadaf 100644
--- a/src/state/workspace.ts
+++ b/src/state/workspace.ts
@@ -1,5 +1,8 @@
 import { create } from "zustand";
-import type { WorkloadKind } from "../api/types";
+import type { Proposal, WorkloadKind } from "../api/types";
+
+/** Предложение LLM, которое открывается заполненной формой; новый `nonce` — новое «Открыть в форме». */
+export type ProposalPrefill = { proposal: Proposal; nonce: number };
 
 export type TabSpec =
   /** Обзор подключения и кластера: кто вошёл, права, API-сервер, ноды, у OpenShift — обновления и операторы. */
@@ -7,8 +10,8 @@ export type TabSpec =
   | { kind: "nodes"; connectionId: string }
   /** Находки проверки кластера и замечания LLM подключения. */
   | { kind: "findings"; connectionId: string }
-  | { kind: "project"; connectionId: string; namespace: string }
-  | { kind: "workload"; connectionId: string; namespace: string; workloadKind: WorkloadKind; name: string }
+  | { kind: "project"; connectionId: string; namespace: string; proposal?: ProposalPrefill }
+  | { kind: "workload"; connectionId: string; namespace: string; workloadKind: WorkloadKind; name: string; proposal?: ProposalPrefill }
   | { kind: "pod"; connectionId: string; namespace: string; name: string }
   /** folderId — папка нового подключения («Новое подключение здесь»). */
   | { kind: "connection-form"; connectionId: string | null; folderId?: string | null }
````

<!-- block: locales t18-locales step=18.4 -->
````json
{
  "en": {
    "health": {
      "proposals": {
        "title": "LLM proposals",
        "intro": "LLM assistants connected over MCP can propose changes here. Nothing is applied until you open a proposal in a form and confirm it.",
        "none": "No LLM proposals.",
        "by": "LLM proposal · {{client}}",
        "openForm": "Open in form",
        "readOnly": "The connection is read-only",
        "reject": "Reject",
        "reasonPlaceholder": "Reason (optional)",
        "rejectConfirm": "Reject proposal",
        "deleteAll": "Delete all LLM proposals",
        "deleteAllConfirm": "Delete all LLM proposals of this connection?",
        "status": {
          "applied": "applied {{when}}",
          "appliedEdited": "applied with edits {{when}}",
          "rejected": "rejected",
          "rejectedWithReason": "rejected: {{reason}}",
          "withdrawn": "withdrawn"
        },
        "appliedAs": "Applied: {{change}}",
        "change": {
          "setResources": "Resources:",
          "remove": "remove",
          "keep": "keep",
          "scale": "Replicas: {{replicas}}",
          "rolloutRestart": "Restart",
          "setHpaBounds": "HPA {{hpa}} bounds: {{min}}–{{max}}"
        }
      }
    },
    "hints": {
      "health": {
        "proposals": "A change an LLM assistant suggests. Nothing is applied until you open it in a form and confirm; you can edit the values first."
      }
    }
  },
  "ru": {
    "health": {
      "proposals": {
        "title": "Предложения LLM",
        "intro": "LLM-ассистенты, подключённые по MCP, могут предлагать здесь изменения. Ничего не применяется, пока вы не откроете предложение в форме и не подтвердите его.",
        "none": "Предложений LLM нет.",
        "by": "Предложение LLM · {{client}}",
        "openForm": "Открыть в форме",
        "readOnly": "Подключение только для чтения",
        "reject": "Отклонить",
        "reasonPlaceholder": "Причина (необязательно)",
        "rejectConfirm": "Отклонить предложение",
        "deleteAll": "Удалить все предложения LLM",
        "deleteAllConfirm": "Удалить все предложения LLM у этого подключения?",
        "status": {
          "applied": "применено {{when}}",
          "appliedEdited": "применено с правками {{when}}",
          "rejected": "отклонено",
          "rejectedWithReason": "отклонено: {{reason}}",
          "withdrawn": "отозвано"
        },
        "appliedAs": "Применено: {{change}}",
        "change": {
          "setResources": "Ресурсы:",
          "remove": "убрать",
          "keep": "не менять",
          "scale": "Реплик: {{replicas}}",
          "rolloutRestart": "Перезапуск",
          "setHpaBounds": "Границы HPA {{hpa}}: {{min}}–{{max}}"
        }
      }
    },
    "hints": {
      "health": {
        "proposals": "Изменение, которое предлагает LLM-ассистент. Ничего не применяется, пока вы не откроете его в форме и не подтвердите; значения можно сначала поправить."
      }
    }
  }
}
````

- [ ] **Step 5: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 674 passed; 0 failed`

Run: `pnpm test`  
Expected: PASS — `Test Files 74 passed`, `Tests 517 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 198`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 6: Commit.**

```bash
git add \
  PORTED.md \
  src/components/TabContent.tsx \
  src/components/health/FindingsPanel.test.tsx \
  src/components/health/FindingsPanel.tsx \
  src/components/health/HealthBlock.tsx \
  src/components/health/ProposalItem.test.tsx \
  src/components/health/ProposalItem.tsx \
  src/components/health/findings.test.ts \
  src/components/health/findings.ts \
  src/components/health/proposals.test.ts \
  src/components/health/proposals.ts \
  src/components/health/useActivePrefill.ts \
  src/components/objects/WorkloadPanel.test.tsx \
  src/components/objects/WorkloadPanel.tsx \
  src/components/project/ProjectPanel.tsx \
  src/lib/useHealth.ts \
  src/locales/en.json \
  src/locales/ru.json \
  src/state/workspace.ts
git commit -s -m "feat(ui): LLM proposals in the Findings tab, the object block and badges — reject with a reason, reopen, delete, applied with or without edits; Open in form opens the change form over the workload or project tab with the proposal id; ProposalItem from the Kafka edition" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 19: Документация: глава «Ресурсы и изменения», метрики, правила resources.*, предложения LLM и инструменты ресурсов в справке; смоук O3; README; уточнения спеки этапа

**Files:**
- Create: `docs/guide/en/resources-changes.md`, `docs/guide/ru/resources-changes.md`, `docs/smoke/o3.md`
- Modify: `README.md`, `docs/guide/en/connections.md`, `docs/guide/en/health-check.md`, `docs/guide/en/llm-mcp.md`, `docs/guide/en/pods-logs.md`, `docs/guide/en/projects-workloads.md`, `docs/guide/ru/connections.md`, `docs/guide/ru/health-check.md`, `docs/guide/ru/llm-mcp.md`, `docs/guide/ru/pods-logs.md`, `docs/guide/ru/projects-workloads.md`, `docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md`, `scripts/i18n-check.mjs`, `src/guide/chapters.ts`

**Interfaces:**
- Consumes: задачи 1–18.
- Produces: глава `docs/guide/{en,ru}/resources-changes.md` (`GUIDE_CHAPTERS` из восьми глав); разделы `connections.md#metrics`, `health-check.md#proposals`; `docs/smoke/o3.md`; README «Ресурсы и изменения»; правки спеки §4.1, §5.3, §7.1, §9, §10.4, §17.

Справка — с якорями, одинаковыми на двух языках, и метками `[[ui:…]]` (проверяет `i18n:check`). Смоук — 14 пунктов на OpenShift Local с мониторингом и kind, с Claude Code. Спека получает уточнения прототипа: версия предпросмотра (`generation`/`uid`), абсолютные значения и повтор при 409, dry-run DELETE в теле `DeleteOptions`, ресурсы не у Job, шаг «Метрики» — предупреждение, настройка метрик локальная и `http://` без авторизации, окна MCP 1/7/14/30, сторожок разбором путей, тексты ошибок модели по списку ключей, стенд метрик kind.

- [ ] **Step 1: Реализация.**

<!-- block: file t19-docs-guide-en-resources-changes-md step=19.1 path=docs/guide/en/resources-changes.md -->
````markdown
# Resources and changes

Glafira shows how much CPU and memory containers actually use, compares it with their `requests` and `limits` and
recommends new values. Changes — container resources, replicas, restart, HPA bounds and pod deletion — are made with
forms that the API server checks before anything is applied.

## Metrics source {#source}

The usage history comes from Prometheus. The [[ui:connections.form.metricsMode.auto]] mode finds Thanos Querier in
`openshift-monitoring` on OpenShift and Prometheus through the API server proxy on other clusters (kube-prometheus,
Prometheus Operator, Rancher Monitoring). If none fits, Glafira shows the current values from `metrics.k8s.io` — no
history and low confidence. Your own Thanos or Prometheus is set in the connection form (see
[Connections](connections.md#metrics)). Which source is chosen and why the others did not fit is shown in the
connection test and above the resource tables.

The rights are yours: Thanos on OpenShift needs the `cluster-monitoring-view` role or similar, the Prometheus proxy
needs `get services/proxy` in its project. Usually only cluster administrators have it.

## Workload resources {#workload}

The [[ui:objects.tabs.resources]] sub-tab of a workload counts statistics over the [[ui:resources.window]]: 1, 7, 14
or 30 days. Percentiles are taken per pod, then the worst pod: replicas can have different load. The table per
container shows "now → recommendation" for CPU and memory, CPU p95 and maximum, memory maximum, throttling, OOM and
restarts. Values a pod got from a `LimitRange` default count as current too. Below the table are the total for the
whole workload with replicas, constraint violations (quota, the largest node) and CPU and memory charts with lines of
the current request and limit and marks of restarts and OOM.

[[ui:resources.applyRecommendation]] opens the container resources form with the recommended values: you can edit them
before the check.

## How the recommendation is made {#recommendation}

- CPU request — p95 × 1.15, at least 10m; a CPU limit is added only if there already is one or a `LimitRange` requires
  it. Throttling p95 above 10 % means the usage hit the limit: the limit is at least the current one × 1.5.
- Memory request — p99 × 1.15, at least 32Mi; memory limit — at least the maximum × 1.3. OOM in the window — the limit is
  at least the current one × 1.5.
- Values are pulled to the `LimitRange` bounds; the quota room counts replicas and rollout pods. If an HPA scales the
  workload by CPU or memory, Glafira says which target keeps the old behaviour.
- Confidence is [[ui:resources.confidenceLevel.high]] with a window and data of 7 days or more and coverage of 80 % or
  more, [[ui:resources.confidenceLevel.medium]] with data of a day or more and coverage of 50 % or more, otherwise
  [[ui:resources.confidenceLevel.low]]. With low confidence the form warns, but you can apply.
- For containers with `JAVA_OPTS`, `JAVA_TOOL_OPTIONS`, `JDK_JAVA_OPTIONS` or `GOMEMLIMIT` Glafira reminds you to check
  `-Xmx`, `MaxRAMPercentage` or `GOMEMLIMIT` together with the memory limit.

## Project resources {#project}

The [[ui:objects.tabs.resources]] sub-tab of a project collects all workloads of the project: totals of requests and
limits of running pods, usage (CPU p95 and memory maximum), reserve, risks — throttling, OOM, memory near the limit —
and excess requests, and above the table the quota usage. The collection runs in the background for up to 45 seconds
and is kept for 5 minutes; [[ui:common.refresh]] collects again. Sorting is [[ui:resources.sortRisk]] or
[[ui:resources.sortReserve]]; [[ui:resources.onlyActionable]] keeps workloads with a risk or excess. The table shows up
to 100 rows — narrow the list by name for more.

## Pod resources {#pod}

The [[ui:objects.tabs.resources]] sub-tab of a pod shows the current CPU and memory usage per container.

## Changes {#changes}

The workload overview has [[ui:change.action.setResources]], [[ui:change.action.scale]] (Deployment, StatefulSet,
DeploymentConfig), [[ui:change.action.rolloutRestart]] and, if the workload has an HPA,
[[ui:change.action.setHpaBounds]]; the pod overview has [[ui:change.action.deletePod]]. Without rights in the project
a button is disabled and its hint names the right; on a read-only connection all are disabled.

1. The form opens over the screen. In the resources form an empty field means "keep" (its hint shows the current
   value), "✕" removes the value.
2. [[ui:change.preview]] shows "before → after", warnings (an HPA will overwrite the replicas, a `LimitRange` bound,
   the quota room, a pod without a controller) and the result of a check on the API server that does not change the
   cluster (server-side dry-run): a quota, `LimitRange` or webhook refusal is visible before you confirm.
3. [[ui:change.apply]]. On a prod connection type its name, for a pod without a controller — the pod name.
4. If the object changed while the form was open, Glafira changes nothing and offers [[ui:change.refreshRetry]].

Glafira writes changes with the `glafira-kubernetes` field manager (visible in `managedFields`), and the app log gets
the connection, the kind of change, the object and the result, without values.

## LLM proposals {#proposals}

An LLM assistant does not change the cluster, but it can propose a change: resources, replicas, restart or HPA bounds.
The proposal appears in the [[ui:sidebar.findings]] tab and on the object screen. [[ui:health.proposals.openForm]]
opens the form with the fields filled in and a line about the proposal; then it is an ordinary change, and the model
learns whether it was applied and whether with edits. More in [Cluster check and notes](health-check.md#proposals).
````

<!-- block: file t19-docs-guide-ru-resources-changes-md step=19.1 path=docs/guide/ru/resources-changes.md -->
````markdown
# Ресурсы и изменения

Glafira показывает, сколько CPU и памяти контейнеры на самом деле потребляют, сравнивает это с их `requests` и
`limits` и рекомендует новые значения. Изменения — ресурсы контейнеров, число реплик, перезапуск, границы HPA и
удаление пода — делаются формами с проверкой на API-сервере до применения.

## Источник метрик {#source}

История потребления берётся из Prometheus. Режим [[ui:connections.form.metricsMode.auto]] у OpenShift находит Thanos
Querier в `openshift-monitoring`, у остальных кластеров — Prometheus через прокси API-сервера (kube-prometheus,
Prometheus Operator, Rancher Monitoring). Если ни один не подошёл, Glafira показывает текущие значения из
`metrics.k8s.io` — без истории и с низкой уверенностью. Свой Thanos или Prometheus задаётся в форме подключения (см.
[Подключения](connections.md#metrics)). Какой источник выбран и почему не подошли остальные, видно в проверке
подключения и над таблицами ресурсов.

Права нужны свои: Thanos у OpenShift требует роль `cluster-monitoring-view` или похожую, прокси к Prometheus — право
`get services/proxy` в его проекте. Обычно оно есть только у администраторов кластера.

## Ресурсы workload'а {#workload}

Подвкладка [[ui:objects.tabs.resources]] у workload'а считает статистику за [[ui:resources.window]]: 1, 7, 14 или 30
дней. Перцентили считаются по каждому поду, затем берётся худший под: у реплик бывает разная нагрузка. Таблица по
контейнерам показывает «сейчас → рекомендация» для CPU и памяти, p95 и максимум CPU, максимум памяти, троттлинг, OOM и
рестарты. Значения, которые под получил от `LimitRange` по умолчанию, тоже считаются текущими. Под таблицей — итог на
весь workload с учётом реплик, нарушения ограничений (квота, крупнейшая нода) и графики CPU и памяти с линиями
текущих request и limit и отметками рестартов и OOM.

[[ui:resources.applyRecommendation]] открывает форму ресурсов контейнеров с рекомендованными значениями: их можно
поправить перед проверкой.

## Как считается рекомендация {#recommendation}

- CPU request — p95 × 1,15, не меньше 10m; CPU limit добавляется, только если он уже есть или его требует `LimitRange`.
  Троттлинг p95 выше 10 % значит, что потребление упиралось в лимит: limit — не меньше текущего × 1,5.
- Memory request — p99 × 1,15, не меньше 32Mi; memory limit — не меньше максимума × 1,3. OOM в окне — limit не меньше
  текущего × 1,5.
- Значения подтягиваются к границам `LimitRange`; запас квоты считается с учётом реплик и подов выкладки. Если workload
  масштабирует HPA по CPU или памяти, Glafira пишет, какой target сохранит прежнее поведение.
- Уверенность [[ui:resources.confidenceLevel.high]] — окно и данные от 7 дней при покрытии от 80 %,
  [[ui:resources.confidenceLevel.medium]] — данные от суток при покрытии от 50 %, иначе
  [[ui:resources.confidenceLevel.low]]. С низкой уверенностью форма предупреждает, но применить можно.
- У контейнеров с `JAVA_OPTS`, `JAVA_TOOL_OPTIONS`, `JDK_JAVA_OPTIONS` или `GOMEMLIMIT` Glafira напоминает проверить
  `-Xmx`, `MaxRAMPercentage` или `GOMEMLIMIT` вместе с лимитом памяти.

## Ресурсы проекта {#project}

Подвкладка [[ui:objects.tabs.resources]] у проекта собирает все workloads проекта: суммы requests и limits
работающих подов, потребление (p95 CPU и максимум памяти), резерв, риски — троттлинг, OOM, память у лимита — и
избыток requests, а над таблицей — использование квот. Сбор идёт в фоне до 45 секунд, результат хранится 5 минут;
[[ui:common.refresh]] собирает заново. Сортировка — [[ui:resources.sortRisk]] или [[ui:resources.sortReserve]];
[[ui:resources.onlyActionable]] оставляет workloads с риском или избытком. Таблица показывает до 100 строк — дальше
сузьте список по имени.

## Ресурсы пода {#pod}

Подвкладка [[ui:objects.tabs.resources]] у пода показывает текущее потребление CPU и памяти по контейнерам.

## Изменения {#changes}

На обзоре workload'а есть [[ui:change.action.setResources]], [[ui:change.action.scale]] (Deployment, StatefulSet,
DeploymentConfig), [[ui:change.action.rolloutRestart]] и, если у workload'а есть HPA, [[ui:change.action.setHpaBounds]];
на обзоре пода — [[ui:change.action.deletePod]]. Без прав в проекте кнопка неактивна, подсказка называет нужное право;
у подключения только для чтения неактивны все.

1. Форма открывается поверх экрана. В форме ресурсов пустое поле — «не менять» (в подсказке — текущее значение),
   «✕» убирает значение.
2. [[ui:change.preview]] показывает «было → станет», предупреждения (HPA перезапишет реплики, граница `LimitRange`,
   запас квоты, под без контроллера) и итог проверки на API-сервере без изменения кластера (server-side dry-run):
   отказ квоты, `LimitRange` или webhook видно до подтверждения.
3. [[ui:change.apply]]. У prod-подключения нужно ввести его имя, у пода без контроллера — имя пода.
4. Если объект изменили, пока форма была открыта, Glafira ничего не меняет и предлагает
   [[ui:change.refreshRetry]].

Glafira пишет изменения с менеджером полей `glafira-kubernetes` (его видно в `managedFields`), а в журнал приложения —
подключение, вид изменения, объект и итог, без значений.

## Предложения LLM {#proposals}

LLM-ассистент не меняет кластер, но может предложить изменение: ресурсы, реплики, перезапуск или границы HPA.
Предложение появляется на вкладке [[ui:sidebar.findings]] и на экране объекта. [[ui:health.proposals.openForm]]
открывает форму с заполненными полями и строкой о предложении; дальше всё как при обычном изменении, и модель узнает,
применено ли оно и с правками ли. Подробнее — [Проверка кластера и замечания](health-check.md#proposals).
````

<!-- block: file t19-docs-smoke-o3-md step=19.1 path=docs/smoke/o3.md -->
````markdown
# Смоук O3 (ресурсы, изменения, предложения)

Сборка: `pnpm tauri build --debug`; пункты 1–12 идут и в `pnpm tauri dev` (если порт 1420 занят — `pnpm tauri dev
--config '{"build":{"devUrl":"http://localhost:1430","beforeDevCommand":"pnpm dev --port 1430"}}'`). Кластеры: стенд
kind (`dev/kind/README.md`; объекты `team-a` из `fixtures.yaml`, `ops.yaml`, `metrics.yaml`) и OpenShift Local
(`crc config set enable-cluster-monitoring true`, затем `crc start`) или dev-кластер OpenShift с мониторингом.
Подключения из прошлых смоуков: `admin` и `dev-token` (импорт `dev/kind/out/kubeconfig`), `crc` (`kubeadmin`).
Настоящие метрики есть только у OpenShift: поддельный Prometheus стенда kind отвечает лишь во время `it_metrics` и
`it_mcp`. MCP-клиент — Claude Code (`claude`).

1. Метрики в форме подключения. У `crc`: «Метрики» — «Автоматически»; «Свой Prometheus» показывает адрес, авторизацию и
   CA; «Свой токен» — поле «Токен источника»; адрес `http://` с авторизацией не сохраняется (ошибка у поля). Вернуть
   «Автоматически». «Экспортировать всё» — в файле нет поля `metrics`.
2. Проверка подключения. `crc`: шаг «Метрики» ✅ «История потребления: Thanos Querier openshift-monitoring/thanos-querier».
   `admin` на kind: шаг ⚠️ с причинами под ним («Prometheus monitoring/prometheus-k8s:9090: не отвечает», «… не
   найден», «metrics.k8s.io: не установлен в кластере»), итог проверки — успех. `dev-token`: «нет прав на источник».
3. Ресурсы workload'а (`crc`, Deployment в своём проекте, например `oc new-app` из примера). Подвкладка «Ресурсы»:
   источник, окно 7 дн., покрытие, уверенность; таблица «сейчас → рекомендация», p95 / max CPU, память, троттлинг;
   итог на весь workload; графики CPU и памяти с линиями request и limit — в светлой и тёмной теме читаются; окно
   1 / 14 / 30 дн. меняет данные. Под этого workload'а — подвкладка «Ресурсы» с текущим потреблением.
4. Применение рекомендации. «Применить рекомендацию» — форма поверх экрана с рекомендованными значениями; при
   уверенности «низкая» (окно 1 дн. у нового workload'а) — предупреждение. «Проверить» — «было → станет» и «API-сервер
   принял изменение (dry-run)»; «Применить» — поды пересоздаются, в `oc get deploy -o yaml` в `managedFields` есть
   `glafira-kubernetes`. На prod-подключении (снять «Только чтение») кнопка ждёт имя подключения.
5. Изменившийся объект. Открыть форму «Изменить ресурсы», «Проверить», затем в терминале `oc set resources deploy/<имя>
   --requests=cpu=15m`, затем «Применить»: ошибка «объект изменился» и кнопка «Обновить и повторить»; повторная
   проверка показывает новое «было». Идущая выкладка (сразу после пункта 4) применению не мешает.
6. Отказ admission. Создать `LimitRange` с `max: {memory: 256Mi}` в проекте; форма ресурсов с memory limit `1Gi` →
   «Проверить»: предупреждение о границе `LimitRange` и «API-сервер отклонил изменение» с текстом сервера; «Применить» нет.
7. Остальные изменения на kind (`admin`, `team-a`): «Масштабировать» `tuned` до 2 и обратно; «Перезапустить» — новая
   аннотация `kubectl.kubernetes.io/restartedAt` в шаблоне; «Границы HPA» у Deployment `idle` нет (HPA `tuned-hpa`
   ссылается на него, а самого Deployment нет), поэтому границы — через предложение (пункт 11) или на `crc`. Под без
   контроллера (`kubectl -n team-a run solo --image=<образ helper> -- bash -c 'exec 3<> <(:); read -u 3'`): «Удалить
   под» — предупреждение «У пода нет контроллера», кнопка ждёт имя пода; после «Проверить» под не удалён.
8. DeploymentConfig (`crc`, если в кластере есть DeploymentConfig): «Изменить ресурсы» применяется; «Перезапустить» —
   «У этого вызова нет dry-run», новая выкладка (`oc rollout history dc/<имя>`).
9. Права и только чтение. `dev-token`: в `team-a` кнопки активны. Профиль `dev-token` с «Только чтение» — все кнопки
   неактивны с подсказкой «Подключение только для чтения».
10. Ресурсы проекта (`crc`, проект с несколькими workloads). Подвкладка «Ресурсы»: «Собираю потребление проекта», затем
    таблица, квоты над ней; «По резерву» и «По риску» меняют порядок; «Только с рекомендациями» оставляет workloads с
    риском или избытком; «Обновить» собирает заново, повторное открытие вкладки в течение 5 минут — сразу из кэша.
11. Правила `resources.*` (`crc`): «Проверить кластер» — при перегруженном контейнере с CPU limit находка «Троттлинг
    CPU», у workload'а с большим запасом requests — «Лишние requests» (информация). На kind без источника — находка
    «Неполная проверка» называет `resources`.
12. Справка. Глава «Ресурсы и изменения» на обоих языках, раздел «Метрики» в главе «Подключения», «Предложения LLM» в
    главе «Проверка кластера и замечания».
13. Предложения через MCP (собранное приложение, `crc` с доступом LLM «Только метаданные»). «Посмотри ресурсы <workload>
    за неделю и предложи requests и limits»: ассистент вызывает `get_resource_usage` и `recommend_resources`, затем
    `propose_change`. На вкладке «Замечания» — раздел «Предложения LLM» с описанием изменения, блок у workload'а, значок.
    «Открыть в форме» — вкладка workload'а с формой и строкой «Применяется предложение LLM: …»; поправить одно значение и
    применить: предложение «применено с правками», «Применено: …». «Какие у тебя предложения?» — `list_proposals`
    показывает `applied` и `appliedAsProposed: false`. Новое предложение → «Отклонить» с причиной: ассистент видит
    причину. «Отзови своё предложение» — `withdraw_proposal`, статус «отозвано».
14. Граница MCP. Ассистент не может применить изменение: инструментов записи нет, «Примени это» — ассистент отвечает, что
    применяете вы. Адрес своего источника метрик (пункт 1, «Свой Prometheus») в ответах ассистента не появляется.
````

<!-- block: patch t19-src step=19.1 -->
````diff
diff --git a/README.md b/README.md
index 4f77e69..c9b1f33 100644
--- a/README.md
+++ b/README.md
@@ -133,10 +133,10 @@ GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-t
   вида в кластере нет, и разрывом не считается; у `/readyz` и списка проектов 404 — разрыв. Проект, чей список подов не
   поместился в 5000, и следующие за ним проекты с подами остаются без списка подов: в `check.partial` — только
   `podLimit`, а правила, которым нужны поды, такие проекты пропускают. Правила (`rules/`) — чистые функции над снимком,
-  34 правила со строгостью по окружению из таблицы §8.2 спеки (`RULES`, тест `severity_matrix_matches_spec`). Одна
+  37 правил со строгостью по окружению из таблицы §8.2 спеки (`RULES`, тест `severity_matrix_matches_spec`). Одна
   проверка на подключение: запрос с тем же списком проектов и в том же окружении, пришедший во время проверки, ждёт её,
   иной дожидается её окончания и запускает свою; свежую проверку повторно используют, только если она сделана в текущем
-  окружении и охватывает запрошенные проекты (`service.rs`). Результат, скрытые находки и замечания LLM —
+  окружении и охватывает запрошенные проекты (`service.rs`). Результат, скрытые находки, замечания и предложения LLM —
   `health/<id подключения>.json`; изменение шлёт событие `health://changed`. Тексты находок для интерфейса —
   `health.rules.<ruleId>.*` в словарях, для LLM — `health/texts.rs` (по-английски).
 - MCP (`crates/glafira-mcp-core`, `crates/glafira-mcp-kubernetes`, `src-tauri/src/mcp/`): шим `glafira-kubernetes mcp`
@@ -145,8 +145,11 @@ GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-t
   `manifest` в каждом крейте): данные дают `Backend` приложения (`mcp/backend.rs`), чтения идут через `ReadClient` и
   общий пул клиентов, подключение открывается по запросу. Область LLM (`llmAccess`, `llmNamespaces` профиля) проверяется
   в крейте до каждого чтения и ещё раз в бэкенде; чужой, выключенный и несуществующий объект дают одинаковый
-  `not_allowed`. Сторожок `mcp_never_reaches_writes_or_the_raw_client` (`mcp/mod.rs`) ищет по тексту файлов `mcp/` и не
-  даёт коду дойти до записи в кластер и до `kube::Client`.
+  `not_allowed`. Сторожок `mcp_never_reaches_writes_or_the_raw_client` (`mcp/guard.rs`) разбирает `use`-деревья и пути
+  всех файлов `mcp/` (псевдонимы, `self`, вложенные группы, без комментариев и строк) и не даёт коду дойти до записи в
+  кластер (`ops`, `commands::changes`, `ClusterWriter`), до `kube::Client` и `kube::Api` и до HTTP-клиентов; из `kube`
+  разрешены только `ListParams` и `Status`. Тексты ошибок доходят до модели только из списка ключей `LLM_TEXT_KEYS`
+  (`mcp/map.rs`), остальные — общей фразой.
 - Маскировка (`glafira-mcp-kubernetes/src/mask.rs`): `command`/`args` и аннотации — по именам полей и встроенным
   шаблонам (§10.3 спеки). К тексту из кластера применяются только встроенные шаблоны, без пар `имя=значение`: в прозе
   они портят диагностику. Это сообщения событий и условий, сообщения состояний контейнеров, образы и строки проб,
@@ -159,8 +162,28 @@ GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-t
   `errors.connections.loginRequired`) остаются как есть. Сбой файла проверки и замечаний и паника фоновой задачи —
   `internal` с общей фразой.
 - Интеграционные тесты на kind: `tests/it_health.rs` (правила на настоящем API-сервере, частичная проверка без прав) и
-  `tests/it_mcp.rs` (клиент rmcp → `mcp.json` → рукопожатие → инструменты в области LLM; значения Secret в ответы не
-  попадают).
+  `tests/it_mcp.rs` (клиент rmcp → `mcp.json` → рукопожатие → инструменты в области LLM, ресурсы и предложения;
+  значения Secret в ответы не попадают).
+
+## Ресурсы и изменения
+- Источник метрик (`src-tauri/src/metrics/`): трейт `MetricsSource` (только GET `/api/v1/query` и `/api/v1/query_range`);
+  автопоиск — Thanos Querier у OpenShift, Prometheus через прокси API-сервера (`ReadClient::prometheus`, список
+  сервисов — `discovery.rs`), свой URL (`http.rs`), без истории — `metrics.k8s.io`. Выбор живёт в подключении 5 минут;
+  403 проб не сбрасывают кэш прав проекта. PromQL — чистые функции `promql.rs` с экранированием меток.
+- Статистика и рекомендация (`src-tauri/src/resources/`): худший под за окно 1, 7, 14 или 30 дней, движок
+  `recommend.rs` с ограничениями `LimitRange`, квоты (реплики и `maxSurge`), крупнейшей ноды и сдвигом цели HPA,
+  уверенность; ресурсы проекта — фоновая сессия с дедлайном 45 с и кэшем 5 минут (`project.rs`).
+- Изменения (`src-tauri/src/change/`, `src-tauri/src/ops/`): запросы записи в кластер есть только в `ops`. Предпросмотр
+  читает объект, строит «было → станет» и выполняет server-side dry-run (у DELETE `dryRun` — в теле `DeleteOptions`:
+  строку запроса API-сервер у DELETE с телом не читает). Версия предпросмотра — `metadata.generation` (у пода — `uid`):
+  идущая выкладка её не меняет. Применение после стражей (`guard.rs`) перечитывает объект, сверяет версию
+  (`CONFLICT`), пишет с `fieldManager=glafira-kubernetes` и повторяет до 3 раз при ответе 409 из-за статуса;
+  `resourceVersion` требует только JSON patch DeploymentConfig. Отказ admission — `ADMISSION_DENIED`.
+- Предложения LLM (`health/proposals.rs`, инструменты `propose_change`, `list_proposals`, `withdraw_proposal`) хранятся в
+  `health/<id>.json`; при создании проверяются только чтением кластера, применяет их человек формой, отметку
+  «применено» ставит `apply_change`.
+- Интеграционные тесты на kind: `tests/it_ops.rs` (каждый вид изменения, `CONFLICT`, стражи) и `tests/it_metrics.rs`
+  (поддельный Prometheus за прокси API-сервера: автопоиск, статистика, рекомендация, правила `resources.*`).
 
 ## Где хранятся данные
 - Профили и папки: `connections.json` в каталоге данных (macOS: `~/Library/Application Support/io.github.vvnovg.glafira.kubernetes/`).
@@ -172,12 +195,13 @@ GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-t
   выводе — `***`.
 - Настройки: `settings.json` (в том числе включённый MCP и подтверждённые MCP-клиенты). Файлы — с правами только для
   владельца (0600 на macOS и Linux).
-- Проверка кластера и замечания LLM: `health/<id>.json` (id из строчных латинских букв, цифр, `-` и `_` — как есть; любой другой,
+- Проверка кластера, замечания и предложения LLM: `health/<id>.json` (id из строчных латинских букв, цифр, `-` и `_` — как есть; любой другой,
   в том числе с заглавной буквой (файловые системы macOS и Windows по умолчанию не различают регистр, и `Prod-1` с
   `prod-1` делили бы файл) и начинающийся с `h-`, — `h-<hex>`; у id длиннее 100 байт файла нет); удаляется вместе с подключением. Журнал
   MCP — `mcp-audit.jsonl` (ротация при 5 МБ в `mcp-audit.1.jsonl`): вызовы без содержимого ответов и текстов замечаний.
   `mcp.json` — только пока работает MCP-сервер.
-- Доступ LLM (`llmAccess`, `llmNamespaces`) — локальные поля профиля: экспорт их не пишет, импорт не переносит.
+- Доступ LLM (`llmAccess`, `llmNamespaces`) и источник метрик (`metrics`) — локальные поля профиля: экспорт их не
+  пишет, импорт не переносит. Свой токен источника метрик — в хранилище секретов, ключ `<id>/metricsToken`.
 - Экспорт подключений — файл `*.glafira-kubernetes-connections.json` без токенов и ключей; секретные значения
   exec-плагина в нём пустые (`--oidc-client-secret=`), строка запуска приходит неподтверждённой. Повторный импорт поверх
   существующего подключения сохраняет локальные значения. Если импорт с «Обновлять существующие подключения» меняет
diff --git a/docs/guide/en/connections.md b/docs/guide/en/connections.md
index d6236c8..d12e676 100644
--- a/docs/guide/en/connections.md
+++ b/docs/guide/en/connections.md
@@ -93,6 +93,20 @@ groups, not on the platform: Route, projects and Gateway API appear wherever the
 in kubeconfig). The mode without certificate verification is insecure; your organization may forbid it.
 [[ui:connections.form.proxy]]: the system proxy (`HTTPS_PROXY` and `NO_PROXY`), no proxy, or an explicit address.
 
+## Metrics {#metrics}
+
+[[ui:connections.form.metrics]] — where Glafira takes the usage history for the Resources tabs and the check rules
+from (see [Resources and changes](resources-changes.md#source)). [[ui:connections.form.metricsMode.auto]] looks for
+Thanos Querier on OpenShift and for Prometheus through the API server proxy on other clusters, and without them takes
+the current values from `metrics.k8s.io`. [[ui:connections.form.metricsMode.thanos]] and
+[[ui:connections.form.metricsMode.prometheus]] are your own address (for example, a tenancy endpoint your
+administrators opened): [[ui:connections.form.metricsUrl]], [[ui:connections.form.metricsAuth]] (the connection token,
+your own token or no authorization) and [[ui:connections.form.metricsCa]]. Your own source is not replaced by another:
+if it does not respond, the Resources tabs say why. An `http://` address is allowed only without authorization. Your
+own token is kept in the system keychain. The metrics setting stays on this computer: export does not write it. The
+connection test shows the [[ui:connections.form.step.metrics]] step: which source is chosen and why the others did not
+fit; without a source the step is a warning, not an error.
+
 ## Rights {#rights}
 
 Glafira works with your rights only. On connection it checks what you may read at the cluster level (projects, nodes,
@@ -123,7 +137,8 @@ conditions, taints, capacity and allocatable resources, addresses and labels.
 ## Environments and read-only {#environments}
 
 [[ui:connections.form.environment]] sets the color of the connection. A new `prod` connection is
-[[ui:common.readOnly]]: Glafira will not change anything in the cluster through it.
+[[ui:common.readOnly]]: Glafira will not change anything in the cluster through it. If you clear the box, every change
+on a prod connection requires typing its name (see [Resources and changes](resources-changes.md#changes)).
 
 ## LLM access {#llm}
 
diff --git a/docs/guide/en/health-check.md b/docs/guide/en/health-check.md
index 3f6b5ab..5f18299 100644
--- a/docs/guide/en/health-check.md
+++ b/docs/guide/en/health-check.md
@@ -31,6 +31,9 @@ it. The result is kept until the next check.
   not programmed.
 - Quotas, storage and scaling: a quota used at 90% or more, a PVC lost or waiting for a volume for more than 5
   minutes, an HPA at its maximum or unable to scale.
+- Resources over the last day from the metrics history (see [Resources and changes](resources-changes.md#source)):
+  CPU throttling above 25 %, memory near the limit (90 % or more), requests far above the usage. Without a history
+  source these rules do not run and the check is marked partial.
 
 ## Severity and environment {#severity}
 
@@ -47,7 +50,7 @@ On the [[ui:sidebar.findings]] tab, findings are grouped by rule; each has an ex
 project. The object link opens its tab: the workload, the pod, the nodes or the project.
 
 A badge with a number — on the [[ui:sidebar.findings]] item and on projects in the connection tree, in the workload
-and pod tables — shows how many findings and open LLM notes with severity [[ui:health.severity.critical]] or
+and pod tables — shows how many findings, open LLM notes and proposals with severity [[ui:health.severity.critical]] or
 [[ui:health.severity.warning]] concern the object. Point at the badge to see their titles. A pod screen also shows
 the findings of its workload that mention this pod.
 
@@ -68,6 +71,20 @@ formatting or links.
 - [[ui:common.delete]] deletes a note, [[ui:health.deleteAll]] deletes all notes of the connection.
 - A note is deleted automatically 7 days after it was created. A connection can have at most 200 open notes.
 
+## LLM proposals {#proposals}
+
+The assistant can propose a change: container resources, replicas, restart or HPA bounds (not pod deletion). Glafira
+checks a proposal by reading the cluster — the object and the containers exist, the values parse, `limits` are not
+below `requests`; the assistant gets `LimitRange` and quota violations as warnings. Proposals are shown in
+[[ui:health.proposals.title]] and on the object screens.
+
+- [[ui:health.proposals.openForm]] opens the change form with the fields filled in (see
+  [Resources and changes](resources-changes.md#proposals)); after it is applied the proposal is marked "applied" or
+  "applied with edits", and the assistant sees it. On a read-only connection the button is disabled.
+- [[ui:health.proposals.reject]] — with an optional reason the assistant sees; [[ui:health.reopen]] brings the
+  proposal back. The assistant can withdraw its own proposal.
+- An open proposal expires after 7 days; a connection has at most 50 open ones.
+
 ## With an LLM assistant {#with-llm}
 
 The assistant runs the same check with the `health_check` tool — over the projects opened to it in the connection
diff --git a/docs/guide/en/llm-mcp.md b/docs/guide/en/llm-mcp.md
index 801af4e..1317a00 100644
--- a/docs/guide/en/llm-mcp.md
+++ b/docs/guide/en/llm-mcp.md
@@ -3,7 +3,7 @@
 Glafira can act as a gateway for an LLM assistant: Claude Desktop, Claude Code, Cursor or another client that
 supports MCP (Model Context Protocol). The assistant reads the metadata of the connections and projects you open for
 it, runs the cluster check and helps you find the causes of problems: restart loops, unavailable workloads, network,
-quotas. It cannot change anything in a cluster through Glafira: you make changes yourself.
+quotas, resources. It cannot change anything in a cluster through Glafira: it proposes changes, and you apply them.
 
 ## How it works {#how-it-works}
 
@@ -66,7 +66,9 @@ the cluster and the nodes — if you can read them:
   (a value in the spec, a ConfigMap, a Secret, a field), without values; `command` and `args` with secrets hidden;
 - annotations — without the copy of the whole object (`last-applied-configuration`), with the values of secret keys
   hidden;
-- cluster check findings and its own notes.
+- cluster check findings, its own notes and proposals;
+- the resource usage of a workload over a window and the recommendation of requests and limits — from the same metrics
+  source as the Resources tabs; the assistant does not get the address of your own metrics source.
 
 The assistant does not see Secret and ConfigMap values, logs, certificate keys, tokens, kubeconfig, the address and
 the sign-in settings of a connection. By a secret name (`password`, `token`, `secret`, `apikey`, `credentials`,
@@ -105,6 +107,12 @@ The assistant can run Glafira's built-in cluster check (the `health_check` tool)
 also leave notes about objects: they appear on the [[ui:sidebar.findings]] tab and on the object screens, and the
 cluster is not changed. See [Cluster check and notes](health-check.md#with-llm).
 
+The assistant looks at resources with the `get_resource_usage` and `recommend_resources` tools (a window of 1, 7, 14
+or 30 days) and proposes a change with `propose_change`; `list_proposals` shows what happened to its proposals,
+`withdraw_proposal` withdraws its own. You apply a proposal with a Glafira form — see
+[Cluster check and notes](health-check.md#proposals). Ask, for example: "Look at the resources of web over a week and
+propose requests and limits."
+
 ## If it does not work {#troubleshooting}
 
 The client shows a message from Glafira:
@@ -115,4 +123,6 @@ The client shows a message from Glafira:
   [[ui:settings.mcp.section]];
 - `Invalid MCP token` — restart the MCP server in the client: Glafira issues a new token every time it starts;
 - `not_allowed` — the connection or project does not exist, LLM access to it is off, or the project is not in
-  [[ui:connections.form.llmScope]].
+  [[ui:connections.form.llmScope]];
+- `metrics_unavailable` — there is no metrics source: check the [[ui:connections.form.step.metrics]] step of the
+  connection test (see [Connections](connections.md#metrics)).
diff --git a/docs/guide/en/pods-logs.md b/docs/guide/en/pods-logs.md
index dc05eba..e4b6956 100644
--- a/docs/guide/en/pods-logs.md
+++ b/docs/guide/en/pods-logs.md
@@ -20,6 +20,8 @@ requests and limits and the environment variables — names and sources only (a
 Secret key, a field), never the values. [[ui:objects.tabs.events]] and YAML are there too. YAML shows the object
 without `managedFields` and without the annotations that copy the whole object; the values of environment variables
 with secret-looking names (PASSWORD, TOKEN, KEY…) are replaced by `***`, other values are shown.
+[[ui:objects.tabs.resources]] shows the current CPU and memory usage per container; [[ui:change.action.deletePod]]
+on the overview deletes the pod (see [Resources and changes](resources-changes.md#changes)).
 
 ## Logs {#logs}
 
diff --git a/docs/guide/en/projects-workloads.md b/docs/guide/en/projects-workloads.md
index 1d4fca5..7fdb2f8 100644
--- a/docs/guide/en/projects-workloads.md
+++ b/docs/guide/en/projects-workloads.md
@@ -14,7 +14,7 @@ and [[ui:connections.form.manualNamespaces]].
 
 A project opens in its own tab: [[ui:objects.tabs.overview]], [[ui:project.tabs.workloads]],
 [[ui:objects.tabs.pods]], [[ui:project.tabs.network]], [[ui:project.tabs.config]], [[ui:project.tabs.storage]],
-[[ui:objects.tabs.events]] and [[ui:project.tabs.quotas]]. Glafira asks the cluster what you may read in the project
+[[ui:objects.tabs.events]], [[ui:objects.tabs.resources]] and [[ui:project.tabs.quotas]]. Glafira asks the cluster what you may read in the project
 and hides the subtabs you have no rights for. The answer is kept for 5 minutes; the tab asks again every 5 minutes and
 when you press [[ui:common.refresh]] on the overview, so a subtab hidden by rights that changed comes back. The
 overview shows the pods by phase and the problem pods, the workloads that are not fine (and the kinds of workloads you
@@ -50,7 +50,9 @@ the pods of an indexed Job, Jobs created from a CronJob by hand and the children
 about 46 characters are not included. YAML shows the object as `kubectl get -o yaml` does, but without `managedFields`,
 without the annotations that copy the whole object (`kubectl.kubernetes.io/last-applied-configuration` and the like) and
 without the private key of a Route; the values of environment variables with secret-looking names (PASSWORD, TOKEN,
-KEY…) are replaced by `***`.
+KEY…) are replaced by `***`. [[ui:objects.tabs.resources]] shows the usage and the recommendation of requests and
+limits, and the buttons on the overview change resources and replicas, restart the workload and set the HPA bounds
+(see [Resources and changes](resources-changes.md#workload)).
 
 ## Network {#network}
 
diff --git a/docs/guide/ru/connections.md b/docs/guide/ru/connections.md
index 2f1d827..21472aa 100644
--- a/docs/guide/ru/connections.md
+++ b/docs/guide/ru/connections.md
@@ -91,6 +91,20 @@ Deckhouse, k3s, RKE2). Platform V DropApp пока работает как Kuber
 `certificate-authority` в kubeconfig). Режим без проверки сертификата небезопасен; организация может его запретить.
 [[ui:connections.form.proxy]]: системный прокси (`HTTPS_PROXY` и `NO_PROXY`), без прокси или явный адрес.
 
+## Метрики {#metrics}
+
+[[ui:connections.form.metrics]] — откуда Glafira берёт историю потребления для вкладок «Ресурсы» и правил проверки
+(см. [Ресурсы и изменения](resources-changes.md#source)). [[ui:connections.form.metricsMode.auto]] ищет Thanos Querier
+у OpenShift и Prometheus через прокси API-сервера у остальных кластеров, а без них берёт текущие значения из
+`metrics.k8s.io`. [[ui:connections.form.metricsMode.thanos]] и [[ui:connections.form.metricsMode.prometheus]] — свой
+адрес (например, tenancy-endpoint, который открыли администраторы): [[ui:connections.form.metricsUrl]],
+[[ui:connections.form.metricsAuth]] (токен подключения, свой токен или без авторизации) и
+[[ui:connections.form.metricsCa]]. Свой источник другим не заменяется: если он не отвечает, вкладки «Ресурсы» говорят
+почему. Адрес `http://` допускается только без авторизации. Свой токен хранится в связке ключей системы. Настройка
+метрик остаётся на этом компьютере: экспорт её не пишет. Проверка подключения показывает шаг
+[[ui:connections.form.step.metrics]]: какой источник выбран и почему не подошли остальные; без источника шаг —
+предупреждение, а не ошибка.
+
 ## Права {#rights}
 
 Glafira работает только с вашими правами. При подключении она проверяет, что вам доступно на уровне кластера
@@ -119,7 +133,8 @@ Glafira работает только с вашими правами. При п
 ## Окружения и только чтение {#environments}
 
 [[ui:connections.form.environment]] задаёт цвет подключения. Новое подключение `prod` — [[ui:common.readOnly]]:
-Glafira ничего не изменит в кластере через него.
+Glafira ничего не изменит в кластере через него. Если снять флажок, каждое изменение на prod-подключении требует ввести
+его имя (см. [Ресурсы и изменения](resources-changes.md#changes)).
 
 ## Доступ LLM {#llm}
 
diff --git a/docs/guide/ru/health-check.md b/docs/guide/ru/health-check.md
index 679a262..b2b9d07 100644
--- a/docs/guide/ru/health-check.md
+++ b/docs/guide/ru/health-check.md
@@ -29,6 +29,9 @@ LLM-ассистент. Результат хранится до следующ
   backend или Secret для TLS; маршрут Gateway API не принят или его ссылки не разрешены; Gateway не настроен.
 - Квоты, хранилище и масштабирование: квота использована на 90% и больше, PVC потерян или ждёт тома дольше 5 минут,
   HPA на максимуме или не может масштабировать.
+- Ресурсы за последние сутки по истории метрик (см. [Ресурсы и изменения](resources-changes.md#source)): троттлинг
+  CPU выше 25 %, память у лимита (90 % и больше), requests намного больше потребления. Без источника истории эти
+  правила не работают, и проверка помечается частичной.
 
 ## Строгость и окружение {#severity}
 
@@ -45,7 +48,7 @@ LLM-ассистент. Результат хранится до следующ
 проекту. Ссылка на объект открывает его вкладку: workload, под, ноды или проект.
 
 Значок с числом — у пункта [[ui:sidebar.findings]] и у проектов в дереве подключений, в таблицах workloads и подов —
-показывает, сколько находок и открытых замечаний LLM со строгостью [[ui:health.severity.critical]] или
+показывает, сколько находок, открытых замечаний и предложений LLM со строгостью [[ui:health.severity.critical]] или
 [[ui:health.severity.warning]] относится к объекту. Наведите на значок указатель, чтобы увидеть их заголовки. Экран
 пода показывает и находки его workload'а, в которых этот под упомянут.
 
@@ -66,6 +69,20 @@ LLM-ассистент, подключённый по MCP (см. [LLM-ассис
 - [[ui:common.delete]] удаляет замечание, [[ui:health.deleteAll]] — все замечания подключения.
 - Замечание удаляется само через 7 дней после создания. У подключения может быть не больше 200 открытых замечаний.
 
+## Предложения LLM {#proposals}
+
+Ассистент может предложить изменение: ресурсы контейнеров, реплики, перезапуск или границы HPA (удаление пода —
+нет). Glafira проверяет предложение чтением кластера — объект и контейнеры есть, значения разбираются, `limits` не
+меньше `requests`; нарушения `LimitRange` и квоты ассистент получает предупреждениями. Предложения показываются в
+разделе [[ui:health.proposals.title]] и на экранах объектов.
+
+- [[ui:health.proposals.openForm]] открывает форму изменения с заполненными полями (см.
+  [Ресурсы и изменения](resources-changes.md#proposals)); после применения предложение помечается
+  «применено» или «применено с правками», и ассистент это видит. У подключения только для чтения кнопка неактивна.
+- [[ui:health.proposals.reject]] — с необязательной причиной, которую видит ассистент; [[ui:health.reopen]] возвращает
+  предложение. Ассистент может отозвать своё предложение.
+- Открытое предложение устаревает через 7 дней; у подключения — не больше 50 открытых.
+
 ## Вместе с LLM-ассистентом {#with-llm}
 
 Ассистент запускает ту же проверку инструментом `health_check` — по проектам, открытым ему в форме подключения
diff --git a/docs/guide/ru/llm-mcp.md b/docs/guide/ru/llm-mcp.md
index 6941896..a1df51c 100644
--- a/docs/guide/ru/llm-mcp.md
+++ b/docs/guide/ru/llm-mcp.md
@@ -3,7 +3,7 @@
 Glafira может работать шлюзом для LLM-ассистента: Claude Desktop, Claude Code, Cursor или другого клиента с
 поддержкой MCP (Model Context Protocol). Ассистент читает метаданные подключений и проектов, которые вы ему открыли,
 запускает проверку кластера и помогает найти причины проблем: перезапуски по кругу, недоступные workloads, сеть,
-квоты. Изменить что-либо в кластере через Glafira он не может: изменения вы делаете сами.
+квоты, ресурсы. Изменить что-либо в кластере через Glafira он не может: он предлагает изменения, а применяете их вы.
 
 ## Как это устроено {#how-it-works}
 
@@ -65,7 +65,9 @@ kubeconfig остаются в Glafira, ассистент их не получ
   источник (значение в спецификации, ConfigMap, Secret, поле), без значений; `command` и `args` — со скрытыми
   секретами;
 - аннотации — без копии всего объекта (`last-applied-configuration`), значения секретных ключей скрыты;
-- находки проверки кластера и свои замечания.
+- находки проверки кластера, свои замечания и предложения;
+- потребление ресурсов workload'а за окно и рекомендацию requests и limits — из того же источника метрик, что и
+  вкладки «Ресурсы»; адрес своего источника метрик ассистент не получает.
 
 Ассистент не видит значений Secret и ConfigMap, логов, ключей сертификатов, токенов, kubeconfig, адреса и настроек
 входа подключения. По секретному имени (`password`, `token`, `secret`, `apikey`, `credentials`, `private`,
@@ -103,6 +105,12 @@ kubeconfig остаются в Glafira, ассистент их не получ
 Он может также оставлять замечания об объектах: они появляются на вкладке [[ui:sidebar.findings]] и на экранах
 объектов, а кластер при этом не меняется. См. [Проверка кластера и замечания](health-check.md#with-llm).
 
+Ресурсы ассистент смотрит инструментами `get_resource_usage` и `recommend_resources` (окно 1, 7, 14 или 30 дней), а
+изменение предлагает инструментом `propose_change`; `list_proposals` показывает, что стало с его предложениями,
+`withdraw_proposal` отзывает своё. Предложение применяете вы формой Glafira — см.
+[Проверка кластера и замечания](health-check.md#proposals). Попросите, например: «Посмотри ресурсы web за неделю и
+предложи requests и limits».
+
 ## Если не работает {#troubleshooting}
 
 Клиент показывает сообщение из Glafira:
@@ -113,4 +121,6 @@ kubeconfig остаются в Glafira, ассистент их не получ
   [[ui:settings.mcp.section]];
 - `Invalid MCP token` — перезапустите MCP-сервер в клиенте: Glafira выдаёт новый токен при каждом запуске;
 - `not_allowed` — подключения или проекта нет, доступ LLM к нему выключен или проект не входит в
-  [[ui:connections.form.llmScope]].
+  [[ui:connections.form.llmScope]];
+- `metrics_unavailable` — нет источника метрик: проверьте шаг [[ui:connections.form.step.metrics]] в проверке
+  подключения (см. [Подключения](connections.md#metrics)).
diff --git a/docs/guide/ru/pods-logs.md b/docs/guide/ru/pods-logs.md
index ec08b10..f3528bc 100644
--- a/docs/guide/ru/pods-logs.md
+++ b/docs/guide/ru/pods-logs.md
@@ -19,6 +19,8 @@
 и источники (значение в спецификации, ключ ConfigMap или Secret, поле), значения не показываются. Есть и
 [[ui:objects.tabs.events]], и YAML. YAML — объект без `managedFields` и без аннотаций с копией всего объекта; значения
 переменных окружения с секретными именами (PASSWORD, TOKEN, KEY…) заменены на `***`, остальные значения видны.
+[[ui:objects.tabs.resources]] — текущее потребление CPU и памяти по контейнерам; [[ui:change.action.deletePod]] на
+обзоре удаляет под (см. [Ресурсы и изменения](resources-changes.md#changes)).
 
 ## Логи {#logs}
 
diff --git a/docs/guide/ru/projects-workloads.md b/docs/guide/ru/projects-workloads.md
index 37ee80e..f2b7f7f 100644
--- a/docs/guide/ru/projects-workloads.md
+++ b/docs/guide/ru/projects-workloads.md
@@ -14,7 +14,7 @@ Glafira читает кластер с вашими правами. Часть 
 
 Проект открывается в своей вкладке: [[ui:objects.tabs.overview]], [[ui:project.tabs.workloads]],
 [[ui:objects.tabs.pods]], [[ui:project.tabs.network]], [[ui:project.tabs.config]], [[ui:project.tabs.storage]],
-[[ui:objects.tabs.events]] и [[ui:project.tabs.quotas]]. Glafira спрашивает у кластера, что вам можно читать в проекте,
+[[ui:objects.tabs.events]], [[ui:objects.tabs.resources]] и [[ui:project.tabs.quotas]]. Glafira спрашивает у кластера, что вам можно читать в проекте,
 и прячет подвкладки, на которые прав нет. Ответ хранится 5 минут; вкладка спрашивает снова каждые 5 минут и по
 [[ui:common.refresh]] в обзоре, так что подвкладка, скрытая по изменившимся правам, возвращается. Обзор показывает поды
 по фазам и проблемные поды, workloads не в норме (и виды workloads, которые вам нельзя читать, с причиной), квоты,
@@ -50,6 +50,8 @@ workload'а показывает реплики (желаемо, готово, 
 длиннее примерно 46 символов сюда не попадают. YAML — объект, как в `kubectl get -o yaml`, но без `managedFields`, без
 аннотаций с копией всего объекта (`kubectl.kubernetes.io/last-applied-configuration` и подобных) и без закрытого ключа
 Route; значения переменных окружения с секретными именами (PASSWORD, TOKEN, KEY…) заменены на `***`.
+[[ui:objects.tabs.resources]] — потребление и рекомендация requests и limits, а кнопки на обзоре меняют ресурсы,
+реплики, перезапускают workload и задают границы HPA (см. [Ресурсы и изменения](resources-changes.md#workload)).
 
 ## Сеть {#network}
 
diff --git a/docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md b/docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md
index 95bcb80..062bf27 100644
--- a/docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md
+++ b/docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md
@@ -147,9 +147,12 @@ crates/glafira-mcp-kubernetes — инструменты, типы ответо
 - **Гарантия MCP «только чтение».** Крейты `glafira-mcp-*` не зависят от `kube`, `k8s-openapi`, `reqwest`, `hyper`,
   `tauri` и крейта приложения — `tests/manifest.rs` каждого крейта читает его `Cargo.toml`. Реализация `Backend`
   в `src-tauri/src/mcp/` получает только `ReadClient` (newtype без доступа к внутреннему `kube::Client`: get, list,
-  watch, logs и GET-запросы к Prometheus); тест-«сторожок» ищет по тексту файлов `src-tauri/src/mcp/` и не допускает
-  в них `ops::`, `ClusterWriter`, `kube_client(` (`Connected::kube_client`), `kube::Client`, `kube::Api`,
-  `kube::api::Api<`, `PostParams`, `PatchParams` и `DeleteParams`. Предложения и замечания пишутся только в файл Glafira.
+  watch, logs и GET-запросы к Prometheus); тест-«сторожок» разбирает `use`-деревья и пути файлов `src-tauri/src/mcp/`
+  (псевдонимы, `self`, вложенные группы; комментарии и строки не считаются) и не допускает в них `crate::ops`,
+  `crate::commands::changes`, `ClusterWriter`, `KubeWriter`, `kube_client`, `Api` и `Client` из `kube`, `Patch`,
+  `PostParams`, `PatchParams`, `DeleteParams`, `apply_change`, `preview_change`, `reqwest`, `hyper` и `http`; из `kube`
+  разрешены только `ListParams` и `Status` (поиск подстрок обходился `use kube::{Api, Client}`, этап O3). Предложения и
+  замечания пишутся только в файл Glafira.
 - **Длительные операции** (watch, логи, сбор метрик проекта, проверка) — фоновые tokio-задачи с `SessionId`; данные
   уходят во фронтенд через `tauri::ipc::Channel` батчами (до 200 элементов или раз в 100 мс; начальный снимок `Reset` уходит одним сообщением и бывает больше); отмена —
   `stop_session(id)` через `CancellationToken`.
@@ -158,7 +161,9 @@ crates/glafira-mcp-kubernetes — инструменты, типы ответо
   открыто, открывает его сам (`AppState::read`, как вкладка, с теми же проверками политики); если для этого нужен вход,
   вызов получает ошибку, а вход человек выполняет в окне. Тексты ошибок с данными профиля (адрес API-сервера и прокси,
   команда exec-плагина, ответ OAuth-сервера, имя пользователя из 403) модель не получает: код остаётся, а текст
-  заменяется фиксированной английской фразой, подробности видит только человек (§10.7).
+  заменяется фиксированной английской фразой, подробности видит только человек (§10.7). С этапа O3 — по списку
+  разрешённых ключей (`LLM_TEXT_KEYS` в `mcp/map.rs`, не длиннее 500 символов): текст ошибки с другим ключом модель
+  получает общей фразой «The operation failed; the user can see the details in Glafira».
 - **Таймауты:** запрос — 30 с, подключение — 10 с (настраиваются в профиле); у каждой сетевой операции есть таймаут
   и отмена, UI не блокируется. Долгие потоки (watch, follow логов) защищает таймаут чтения клиента — 295 с без единого
   байта: соединение, умершее после сна, смены Wi-Fi или VPN, обрывается, а не висит молча (TCP keepalive у `kube`
@@ -379,7 +384,8 @@ oc. В тексте отказа — только имя ключа: значе
 4. discovery: платформа и дистрибутив (§4.3), версия (`ClusterVersion`, если читается, иначе `/version`), найдены ли
    группы OpenShift и Gateway API;
 5. проекты: число доступных;
-6. метрики: какой источник выбран бы в режиме `auto` и почему отвергнуты остальные (§7.1).
+6. метрики: какой источник выбран бы в режиме `auto` и почему отвергнуты остальные (§7.1). Шаг — предупреждение:
+   без источника метрик подключение работает, итог проверки — успех.
 
 ### 5.4 Права (RBAC)
 
@@ -577,6 +583,11 @@ workloads, событий, PVC, нод, обзора проекта и дере
 
 Явно выбранный источник не заменяется другим: при ошибке — `METRICS_UNAVAILABLE` с причиной.
 
+Настройка источника — локальное поле профиля (`metrics`): экспорт её не пишет, импорт нового подключения даёт `auto`,
+обновление существующего сохраняет её. Свой адрес `http://` допускается только без авторизации; свой токен — в
+keychain, ключ `<id>/metricsToken`. Выбор живёт в подключении 5 минут; ответы 403 при пробах не сбрасывают кэш прав
+проекта.
+
 ### 7.2 Сбор статистики
 
 - **Объект:** workload (все его поды за окно, включая уже удалённые) — по каждому контейнеру; init-контейнеры
@@ -765,7 +776,7 @@ request и limit, пометки и нарушения ограничений; 
 
 | `kind` | Поля | Как применяется | Особенности |
 |---|---|---|---|
-| `setResources` | `namespace`, `workload { kind, name }`, `containers [{ name, requests { cpu?, memory? }, limits { cpu?, memory? } }]` — значение quantity-строкой или `null` (убрать), отсутствие поля — не менять | strategic merge patch по имени контейнера; для DeploymentConfig — JSON patch по индексу контейнера с проверкой `resourceVersion` | `limits ≥ requests`; ошибки `LimitRange` и квоты видны на dry-run |
+| `setResources` | `namespace`, `workload { kind, name }`, `containers [{ name, requests { cpu?, memory? }, limits { cpu?, memory? } }]` — значение quantity-строкой или `null` (убрать), отсутствие поля — не менять | strategic merge patch по имени контейнера; для DeploymentConfig — JSON patch по индексу контейнера с проверкой `resourceVersion` | `limits ≥ requests`; ошибки `LimitRange` и квоты видны на dry-run; недоступно для Job (шаблон неизменяем) |
 | `scale` | `namespace`, `workload`, `replicas` (0–1000) | subresource `scale` | Deployment, StatefulSet, DeploymentConfig; у workload'а с HPA — предупреждение «HPA перезапишет число реплик» |
 | `rolloutRestart` | `namespace`, `workload` | аннотация `kubectl.kubernetes.io/restartedAt` в шаблоне; для DeploymentConfig — `instantiate` | недоступно для Job и CronJob |
 | `setHpaBounds` | `namespace`, `hpa`, `minReplicas?`, `maxReplicas` | merge patch | `1 ≤ min ≤ max` |
@@ -778,8 +789,12 @@ request и limit, пометки и нарушения ограничений; 
    - профиль «только чтение» → `READ_ONLY` (никакое подтверждение не помогает);
    - окружение `prod` и `confirm` ≠ имя подключения → `CONFIRMATION_REQUIRED`;
    - `deletePod` пода без контроллера — дополнительно ввод имени пода.
-3. Запрос с `fieldManager=glafira-kubernetes` и `expectedResourceVersion` из предпросмотра; объект изменился —
-   `CONFLICT` с кнопкой «Обновить и повторить».
+3. Запрос с `fieldManager=glafira-kubernetes` и версией из предпросмотра: `metadata.generation` (у пода — `uid`), а не
+   `resourceVersion` — идущая выкладка меняет статус, но не spec. Применение перечитывает объект; другая версия —
+   `CONFLICT` с кнопкой «Обновить и повторить». Значения пишутся абсолютными, как у `kubectl`; `resourceVersion`
+   прочитанного объекта требует только JSON patch DeploymentConfig (адресует контейнер по индексу). Ответ 409 из-за
+   обновления статуса между чтением и записью — повтор до 3 раз (и у dry-run предпросмотра). У DELETE dry-run и
+   предусловие `uid` передаются в теле `DeleteOptions`: строку запроса API-сервер у DELETE с телом не читает.
 4. Запись в журнал приложения (подключение, вид, объект, исход; без значений env и конфигурации).
 5. Если передан `proposalId` — отметка «применено» (§10.5).
 
@@ -860,8 +875,8 @@ request и limit, пометки и нарушения ограничений; 
 | `list_events` | metadata | `connectionId`, `namespace`, `object? { kind, name }`, `type?` (`Warning`), `sinceSeconds = 3600` (≤ 86400), `limit` | `{ items [{ lastSeen, type, reason, object, message, count }], truncated }` |
 | `get_quotas` | metadata | `connectionId`, `namespace` | ResourceQuota (использовано / лимит), LimitRange |
 | `get_network` | metadata | `connectionId`, `namespace`, `filter?` | Services (тип, порты, готовые endpoints), Routes (хост, путь, сервис, TLS-терминация, `admitted`, срок встроенного сертификата), Ingress (класс, хосты, пути → сервис и порт, TLS-хосты и имя Secret, адрес), HTTPRoute и GRPCRoute (хосты, родители с условиями, бэкенды), Gateway проекта (класс, listeners, `programmed`); виды без группы API в кластере не возвращаются |
-| `get_resource_usage` | metadata | `connectionId`, `namespace`, `workload { kind, name }`, `windowDays = 7` (1–30) | статистика §7.2 по контейнерам, ряды (≤ 200 точек), источник, окно, покрытие |
-| `recommend_resources` | metadata | `connectionId`, `namespace`, `workload`, `windowDays = 7` | результат движка §7.3 |
+| `get_resource_usage` | metadata | `connectionId`, `namespace`, `workload { kind, name }`, `windowDays = 7` (1, 7, 14 или 30) | статистика §7.2 по контейнерам, ряды (≤ 200 точек), источник, окно, покрытие |
+| `recommend_resources` | metadata | `connectionId`, `namespace`, `workload`, `windowDays = 7` (1, 7, 14 или 30) | результат движка §7.3 |
 | `observe_workload` | metadata | `connectionId`, `namespace`, `workload`, `durationSeconds = 30` (10–50) | хронология: `replicasChanged`, `podAdded`, `podRemoved`, `podReady`, `podNotReady`, `containerRestarted { reason, exitCode }`, `rolloutProgress`, `warningEvent { reason, message }`; не больше 500 событий; `summary` |
 | `get_pod_logs` | logs | `connectionId`, `namespace`, `pod`, `container?`, `previous = false`, `sinceSeconds?` (≤ 7 дней) или `sinceTime?`, `tailLines = 200` (≤ 2000), `filter? { contains? \| regex?, caseSensitive = false }` | `{ lines [{ ts, text }], scanned, returned, masked, truncated }` |
 | `search_workload_logs` | logs | `connectionId`, `namespace`, `workload`, `filter`, `sinceSeconds = 3600` (≤ 86400), `maxMatches = 200`, `contextLines = 0` (≤ 5) | совпадения по подам и контейнерам, `scanned`, `masked`, `truncated` |
@@ -1093,13 +1108,17 @@ keychain: сервис glafira-kubernetes, ключи <connection-id>/<field>
   ноде, поэтому образы не скачиваются. В O2 — ещё `it_health` и `it_mcp` на том же kind: минимальные CRD Route и Gateway
   API (`dev/kind/crds.yaml`, схема без проверки полей, без контроллеров и без групп `config.openshift.io` — платформа
   остаётся `kubernetes`; статус HTTPRoute пишет `setup.sh`) и объекты для правил (`dev/kind/health.yaml`); правила
-  ClusterOperator, ClusterVersion и MachineConfigPool проверяются unit-тестами на снимках и смоуком на OpenShift.
+  ClusterOperator, ClusterVersion и MachineConfigPool проверяются unit-тестами на снимках и смоуком на OpenShift. В O3
+  — ещё `it_ops` (каждый вид `Change` с dry-run, `CONFLICT`, стражи) и `it_metrics` на том же kind
+  (`dev/kind/ops.yaml`, `dev/kind/metrics.yaml`): образов с HTTP-сервером на ноде нет, поэтому поддельный Prometheus —
+  HTTP-сервер самого теста на рабочей машине; прокси API-сервера ведёт только на адреса подов, и за Service
+  `monitoring/prometheus-k8s` стоит под-заглушка с hostNetwork, чей порт `setup.sh` перенаправляет (DNAT в ноде) на
+  рабочую машину. kube-state-metrics, ingress-nginx и kube-prometheus на стенде не ставятся: настоящие метрики и
+  DeploymentConfig проверяет смоук на OpenShift.
 - **MCP-крейты:** фейковый `Backend`, MCP-клиент rmcp — список инструментов и аннотации, сторожок имён, уровни
   доступа (`not_allowed`, `logs_disabled`), `llmNamespaces`, маскировка в ответах, усечение до 100 КБ, лимиты,
   рукопожатие, журнал без содержимого; `tests/manifest.rs` каждого крейта — нет зависимости от `kube`, `k8s-openapi`,
-  `reqwest`, `hyper`, `tauri` и приложения; сторожок «`src-tauri/src/mcp/`» — поиск по тексту файлов: нет `ops::`,
-  `ClusterWriter`, `kube_client(`, `kube::Client`, `kube::Api`, `kube::api::Api<`, `PostParams`, `PatchParams`,
-  `DeleteParams`.
+  `reqwest`, `hyper`, `tauri` и приложения; сторожок «`src-tauri/src/mcp/`» — разбор `use`-деревьев и путей (§4.1).
 - **Фронтенд:** Vitest + Testing Library — формы подключения и импорта kubeconfig, дерево, вкладки проекта,
   workload'а и пода, логи, «Ресурсы» (таблица, итог, уверенность, переход к форме), формы изменений с diff и
   подтверждением, «Замечания», MCP-настройки; IPC мокается.
diff --git a/scripts/i18n-check.mjs b/scripts/i18n-check.mjs
index 221cca1..216968e 100644
--- a/scripts/i18n-check.mjs
+++ b/scripts/i18n-check.mjs
@@ -562,7 +562,7 @@ export function findRustCyrillic(files) {
 }
 
 /** Главы руководства: тот же список, что `GUIDE_CHAPTERS` в `src/guide/chapters.ts`. */
-export const GUIDE_CHAPTERS = ["getting-started", "connections", "projects-workloads", "pods-logs", "health-check", "llm-mcp", "updates"];
+export const GUIDE_CHAPTERS = ["getting-started", "connections", "projects-workloads", "pods-logs", "health-check", "resources-changes", "llm-mcp", "updates"];
 
 const KEBAB = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
 
diff --git a/src/guide/chapters.ts b/src/guide/chapters.ts
index fa92805..e4d2dbc 100644
--- a/src/guide/chapters.ts
+++ b/src/guide/chapters.ts
@@ -1,5 +1,5 @@
 /** Главы руководства (спец. §16); каждый этап добавляет свои. Тот же список — в `scripts/i18n-check.mjs`. */
-export const GUIDE_CHAPTERS = ["getting-started", "connections", "projects-workloads", "pods-logs", "health-check", "llm-mcp", "updates"] as const;
+export const GUIDE_CHAPTERS = ["getting-started", "connections", "projects-workloads", "pods-logs", "health-check", "resources-changes", "llm-mcp", "updates"] as const;
 
 export type ChapterId = (typeof GUIDE_CHAPTERS)[number];
 
````

- [ ] **Step 2: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 74 passed`, `Tests 517 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 198`

Run: `pnpm test:scripts`  
Expected: PASS — `# pass 86`, `# fail 0`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 3: Commit.**

```bash
git add \
  README.md \
  docs/guide/en/connections.md \
  docs/guide/en/health-check.md \
  docs/guide/en/llm-mcp.md \
  docs/guide/en/pods-logs.md \
  docs/guide/en/projects-workloads.md \
  docs/guide/en/resources-changes.md \
  docs/guide/ru/connections.md \
  docs/guide/ru/health-check.md \
  docs/guide/ru/llm-mcp.md \
  docs/guide/ru/pods-logs.md \
  docs/guide/ru/projects-workloads.md \
  docs/guide/ru/resources-changes.md \
  docs/smoke/o3.md \
  docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md \
  scripts/i18n-check.mjs \
  src/guide/chapters.ts
git commit -s -m "docs: Resources and changes chapter of the guide, metrics source, resources rules, LLM proposals and the resource tools in the guide; the O3 smoke; README; spec refinements of the stage — preview version, DELETE dry-run, metrics step, MCP windows, the parsing guard, the metrics stand" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

## Покрытие спеки

| Требование | Задачи |
|---|---|
| §7.1 источник метрик: автопоиск Thanos и Prometheus через прокси API-сервера, свой адрес, `metrics.k8s.io`, причины отказа; §5.3 шаг «Метрики» | 2, 4 |
| §7.2 сбор статистики: худший под, поды по владельцам или имени, окно, покрытие, графики | 3, 5 |
| §7.3 движок рекомендаций, ограничения, уверенность, подсказки Java и Go | 6 |
| §7.4 интерфейс: «Ресурсы» workload'а, проекта и пода, графики, «Применить рекомендацию» | 7, 14, 16, 17 |
| §8.2 правила `resources.*` | 8 |
| §9 изменяющие операции: модель, предпросмотр с dry-run, стражи, `CONFLICT`, `fieldManager`, журнал; §5.4 кнопки без прав | 9, 11, 15 |
| §10.4–10.6 инструменты `get_resource_usage`, `recommend_resources`, `propose_change`, `list_proposals`, `withdraw_proposal` (22), предложения LLM, инструкции сервера | 10, 12, 18 |
| §4.1 граница записи MCP (сторожок), §10.7 тексты ошибок для модели | 1, 11 |
| §13 хранение: токен источника, предложения в `health/<id>.json`, локальная настройка метрик | 2, 10 |
| §15 `METRICS_UNAVAILABLE`, `ADMISSION_DENIED`, `CONFLICT` | 4, 11, 15 |
| §17 тесты: движок, PromQL, источники, операции, MCP, фронтенд, kind | 3–18, 13 |
| §16 справка; смоук; README; уточнения спеки | 19 |
| Отложенное O2 о границе записи и ошибках модели | 1 |
