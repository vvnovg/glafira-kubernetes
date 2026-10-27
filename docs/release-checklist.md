# Чек-лист релиза

Процесс перенесён из Glafira for Apache Kafka (`docs/release-checklist.md` там же); отличия — языки (en и ru до этапа O6),
репозиторий релизов и проверки установщиков.

## Один раз (владелец)
1. Ключ updater — свой, не Kafka-версии: `pnpm tauri signer generate -w ~/.tauri/glafira-kubernetes-updater.key`.
   Открытый ключ (`.key.pub`) — в `src-tauri/tauri.conf.json` → `plugins.updater.pubkey` вместо `REPLACE_WITH_UPDATER_PUBKEY`.
2. Публичный репозиторий `vvnovg/glafira-kubernetes-releases` с README из `docs/releases-repo/README.md` и `SUPPORT.md`
   из `docs/releases-repo/SUPPORT.md` (дальше их обновляет каждый релиз).
3. Секреты репозитория `vvnovg/glafira-kubernetes` (Settings → Secrets and variables → Actions):
   `TAURI_SIGNING_PRIVATE_KEY` (содержимое файла ключа), `TAURI_SIGNING_PRIVATE_KEY_PASSWORD`,
   `RELEASES_TOKEN` (fine-grained PAT: только `vvnovg/glafira-kubernetes-releases`, Contents: Read and write).
   Срок токена — 1 год; напоминание в календаре за 2 недели. Истёкший токен ломает только шаг publish:
   создать новый с теми же правами и `gh secret set RELEASES_TOKEN --repo vvnovg/glafira-kubernetes`, затем перезапустить job publish.
   Подпись и нотаризация Apple — те же значения, что у glafira-kafka (секреты между репозиториями не копируются):
   `APPLE_CERTIFICATE`, `APPLE_CERTIFICATE_PASSWORD`, `APPLE_SIGNING_IDENTITY`, `APPLE_ID`, `APPLE_PASSWORD`,
   `APPLE_TEAM_ID`; при наличии — `APPLE_INSTALLER_IDENTITY` (подпись `.pkg`). Заданы не все — сборка падает;
   не задан ни один — macOS-сборка без подписи (предупреждение в логе).
4. Резервная копия закрытого ключа updater вне компьютера разработки.

## Пробный прогон
Нужен `release.yml` в ветке `main`. `gh workflow run release --repo vvnovg/glafira-kubernetes -f dry_run=true` собирает
все платформы и `latest.json` без публикации (артефакт `release-vX.Y.Z`). `node scripts/check-release.mjs --dry-run vX.Y.Z`
превращает проблему с ключом updater в предупреждение; сборка по тегу всегда строгая.

## Каждый релиз
1. Версия в `package.json`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`. Заметки к выпуску — раздел `## X.Y.Z`
   в `CHANGELOG.md` (английский: тело GitHub-релиза и `notes` в `latest.json`) и в `docs/changelog/ru.md` — по смыслу.
   Оба языка идут в `notes_i18n`, и «О программе» показывает заметки на языке интерфейса как Markdown.
   `pnpm i18n:check` — файлы списков изменений.
2. `cargo update -w --offline --manifest-path src-tauri/Cargo.toml` — новая версия попадает в `Cargo.lock`
   (без этого `cargo metadata --locked` в скрипте ниже падает), затем `node scripts/third-party.mjs` →
   обновлённый `THIRD_PARTY.md` (лицензии зависимостей); закоммитить.
3. `node scripts/check-release.mjs vX.Y.Z` → «можно выпускать».
4. `git tag vX.Y.Z && git push origin vX.Y.Z` → workflow `release`. Релиз создаётся черновиком со всеми ассетами
   в `vvnovg/glafira-kubernetes-releases` и публикуется атомарно (`--draft=false --latest`); updater читает
   `releases/latest/download/latest.json` (артефакты updater — формат v2). Тот же тег запускает в `ci.yml` job `platforms`.
5. Смоук на трёх ОС (ниже) по установщикам из релиза.

## Никогда не менять
- `bundle.windows.wix.upgradeCode` в `src-tauri/tauri.conf.json` (`c3a3f458-3ec5-4961-909a-d0ee6b19db8a`):
  по нему MSI новой версии обновляет установленную, а не ставится рядом.
- `mainBinaryName` (`glafira-kubernetes`) и `identifier` (`io.github.vvnovg.glafira.kubernetes`): от них зависят путь
  установки, имя процесса, данные, keychain, политика и путь в конфигурациях MCP-клиентов.
- Ключ updater (`plugins.updater.pubkey`): со сменой ключа все установленные версии перестанут принимать обновления.

## Известные ограничения
- `.pkg` пока не нотаризуется отдельно (как и в Kafka-версии); Windows-сборки без подписи (SmartScreen).
- Архива диагностики пока нет (этап O6).

## Смоук установщиков
- [ ] macOS (`.dmg` и `.pkg`): установка, запуск без предупреждения Gatekeeper, подключение к kind (`dev/kind/`), «О программе» — версия.
- [ ] Windows (`.msi`, `msiexec /qn` из администраторской консоли): то же; в «Программах и компонентах» одна запись.
- [ ] Linux (`.deb`, `.rpm`, `.AppImage`): то же; бинарь `/usr/bin/glafira-kubernetes`.
- [ ] MCP: фрагмент для Claude Code из «Настройки» → MCP указывает на установленное приложение; клиент подключается.
- [ ] Обновление: установить предыдущую версию → «Проверить обновления» → «Установить и перезапустить» → новая версия, данные на месте
      (для `.deb` и `.rpm` — отдельно: ставится пакет того же типа, запрашивается пароль администратора).
- [ ] Политика `updatesEnabled = false` (`docs/admin/policy.json`): «Обновления управляются вашей организацией», в логе нет запросов к серверу обновлений.
- [ ] Политика с `updateUrl: "http://…"`: в «О программе» ошибка политики, обновления выключены.

## Сквозная проверка обновления (обязательна для первого релиза с updater, затем — при изменениях updater)
Порядок — как в Kafka-версии: пробный прогон версии A, временная ветка с версией B (без слияния), тестовое https-зеркало
с `latest.json`, пересобранным `node scripts/make-latest-json.mjs --version <B> --base-url <зеркало> --dir <ассеты B> --require-all`,
политика `updateUrl` на зеркало на тестовых машинах. На каждой ОС и каждом типе установки:
- [ ] установить A, создать подключение, подключить Claude Code по MCP;
- [ ] «Проверить обновления» → «Доступна версия B»; заметки — Markdown на языке интерфейса;
- [ ] «Установить и перезапустить» → подтверждение про подключённых ассистентов → «Отключить ассистентов и обновить»;
- [ ] после перезапуска «О программе» показывает B; подключения, секреты, настройки и замечания на месте;
      `mcp.json` в папке данных — новый (на Windows это проверка хука выхода перед запуском MSI);
- [ ] Windows: одна запись версии B в «Программах и компонентах», установка требует UAC;
- [ ] отрицательные проверки: `latest.json` с испорченной подписью → ошибка установки, A работает.
Удалить временную ветку и тестовое зеркало. Только после этого ставить тег.
