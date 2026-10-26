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
  прав; списки — страницами по 500, не больше 10 000 объектов. Каждая команда работает с правами пользователя; часть
  экрана без прав приходит причиной (`Part` в `src-tauri/src/k8s/view.rs`), остальное показывается.
- Обзор кластера (`k8s/overview.rs`): `/readyz?verbose`, ноды по ролям, классы Ingress и Gateway, число проектов; у
  OpenShift — адаптер платформы (`platform/adapters.rs`): ClusterVersion, Infrastructure, консоль, ClusterOperators,
  MachineConfigPools и срок токена (`useroauthaccesstokens`).
- Проекты, права в проекте (`SelfSubjectRulesReview`, кэш на 5 минут, сброс при 403), workloads, поды, события, сеть,
  конфигурация (значения Secret не читаются интерфейсом), PVC, квоты, YAML без `managedFields` — модули `src-tauri/src/k8s/`.
- Списки подов, workloads и событий проекта обновляются через watch (`src-tauri/src/watch/`), без прав на watch — опрос
  раз в 10 с; логи (`src-tauri/src/logs/`) — фильтр в Rust, обрезка строк длиннее 64 КБ, переподключение в follow.
  Сессии останавливаются `stop_session`, при отключении и удалении подключения.
- Одновременные чтения одного подключения подключаются один раз: кто ждал очереди, пока чужая попытка не удалась,
  получает ту же ошибку (`AppState::connected`), — exec-плагин не запускается по разу на каждую подвкладку.

## Где хранятся данные
- Профили и папки: `connections.json` в каталоге данных (macOS: `~/Library/Application Support/io.github.vvnovg.glafira.kubernetes/`).
- Токены и ключи клиентских сертификатов — в системном хранилище секретов, сервис `glafira-kubernetes`, ключи
  `<id>/token` и `<id>/clientKey`. Исключение — секретные значения переменных окружения и аргументов exec-плагина: они
  лежат в `connections.json` (права 0600, как у kubeconfig). Секретно имя, где есть TOKEN, SECRET, PASS, KEY,
  CREDENTIAL, BEARER или COOKIE: `--client-secret x`, `--oidc-client-secret=x`, `AWS_SECRET_ACCESS_KEY`
  (`is_secret_name` в `src-tauri/src/auth/exec.rs`). В строке запуска такие значения скрыты (`••••`), в отладочном
  выводе — `***`.
- Настройки: `settings.json`. Файлы — с правами только для владельца (0600 на macOS и Linux).
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
