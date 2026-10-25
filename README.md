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
