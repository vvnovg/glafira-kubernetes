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
| `src/vite-env.d.ts` | `src/vite-env.d.ts` | `8bbd0bd` | как есть |
| `src/polyfills.ts` | `src/polyfills.ts` | `8bbd0bd` | как есть |
| `src/polyfills.test.ts` | `src/polyfills.test.ts` | `8bbd0bd` | как есть |
| `src/styles.css` | `src/styles.css` | `8bbd0bd` | как есть |
| `scripts/check-dist-compat.mjs` | `scripts/check-dist-compat.mjs` | `8bbd0bd` | адаптирован: исправлено ложное срабатывание `...at(` (перенести в Kafka-версию) |
| `scripts/check-dist-compat.test.mjs` | `scripts/check-dist-compat.test.mjs` | `8bbd0bd` | адаптирован: тест `...at(` |
| `src-tauri/build.rs` | `src-tauri/build.rs` | `8bbd0bd` | как есть |
| `src-tauri/capabilities/default.json` | `src-tauri/capabilities/default.json` | `8bbd0bd` | как есть |
| `index.html` | `index.html` | `8bbd0bd` | адаптирован: заголовок |
| `.gitignore` | `.gitignore` | `8bbd0bd` | адаптирован: без путей Kafka-версии; `/logs` — только каталог в корне, не модули `logs/` |
| `NOTICE` | `NOTICE` | `8bbd0bd` | адаптирован: имя продукта и товарные знаки |
| `TRADEMARK.md` | `TRADEMARK.md` | `8bbd0bd` | адаптирован: имя продукта и товарные знаки |
| `SECURITY.md` | `SECURITY.md` | `8bbd0bd` | адаптирован: имя продукта и товарные знаки |
| `CONTRIBUTING.md` | `CONTRIBUTING.md` | `8bbd0bd` | адаптирован: имя продукта и товарные знаки |
| `src-tauri/.gitignore` | `src-tauri/.gitignore` | `8bbd0bd` | как есть |
| `src-tauri/src/i18n.rs` | `src-tauri/src/i18n.rs` | `8bbd0bd` | как есть |
| `src-tauri/src/sync.rs` | `src-tauri/src/sync.rs` | `8bbd0bd` | как есть |
| `src-tauri/src/storage/fs.rs` | `src-tauri/src/storage/fs.rs` | `8bbd0bd` | как есть |
| `src-tauri/src/logging.rs` | `src-tauri/src/logging.rs` | `8bbd0bd` | адаптирован: комментарий |
| `src-tauri/src/dock_name.rs` | `src-tauri/src/dock_name.rs` | `8bbd0bd` | адаптирован: комментарий |
| `src-tauri/src/secrets.rs` | `src-tauri/src/secrets.rs` | `8bbd0bd` | адаптирован: поля секретов подключения |
| `src-tauri/src/error.rs` | `src-tauri/src/error.rs` | `8bbd0bd` | адаптирован: коды Kubernetes, без librdkafka |
| `src-tauri/src/policy/mod.rs` | `src-tauri/src/policy/mod.rs` | `8bbd0bd` | адаптирован: ключи allowInsecureTls и allowExecAuth, языки тестов |
| `src-tauri/src/policy/sources.rs` | `src-tauri/src/policy/sources.rs` | `8bbd0bd` | адаптирован: тест примеров с allowInsecureTls и allowExecAuth |
| `src-tauri/src/settings.rs` | `src-tauri/src/settings.rs` | `8bbd0bd` | адаптирован: языки en и ru |
| `src-tauri/src/commands/settings.rs` | `src-tauri/src/commands/settings.rs` | `8bbd0bd` | адаптирован: языки тестов |
| `src-tauri/src/state.rs` | `src-tauri/src/state.rs` | `8bbd0bd` | адаптирован: только хранилища, без Kafka |
| `src-tauri/src/commands/mod.rs` | `src-tauri/src/commands/mod.rs` | `8bbd0bd` | адаптирован: только blocking |
| `docs/admin/policy.json` | `docs/admin/policy.json` | `8bbd0bd` | адаптирован: идентификаторы, allowInsecureTls, allowExecAuth |
| `docs/admin/policy.mobileconfig` | `docs/admin/policy.mobileconfig` | `8bbd0bd` | адаптирован: идентификаторы, allowInsecureTls, allowExecAuth |
| `docs/admin/policy.reg` | `docs/admin/policy.reg` | `8bbd0bd` | адаптирован: идентификаторы, allowInsecureTls, allowExecAuth |
| `src-tauri/src/storage/connections.rs` | `src-tauri/src/storage/connections.rs` | `8bbd0bd` | адаптирован: профиль вынесен в profile.rs, тесты под Kubernetes |
| `src-tauri/src/storage/tree.rs` | `src-tauri/src/storage/tree.rs` | `8bbd0bd` | как есть |
| `src-tauri/src/storage/exchange.rs` | `src-tauri/src/storage/exchange.rs` | `8bbd0bd` | адаптирован: формат glafira-kubernetes-connections, флаги секретов, запрет insecure политикой |
| `src-tauri/src/commands/folders.rs` | `src-tauri/src/commands/folders.rs` | `8bbd0bd` | адаптирован: политика в импорте, без сессий Kafka |
| `src/i18n/index.ts` | `src/i18n/index.ts` | `8bbd0bd` | как есть |
| `src/i18n/text.ts` | `src/i18n/text.ts` | `8bbd0bd` | как есть |
| `src/i18n/text.test.ts` | `src/i18n/text.test.ts` | `8bbd0bd` | как есть |
| `src/i18n/locales.test.ts` | `src/i18n/locales.test.ts` | `8bbd0bd` | как есть |
| `src/i18n/documentLang.test.ts` | `src/i18n/documentLang.test.ts` | `8bbd0bd` | как есть |
| `src/lib/format.ts` | `src/lib/format.ts` | `8bbd0bd` | как есть |
| `src/lib/format.test.ts` | `src/lib/format.test.ts` | `8bbd0bd` | как есть |
| `src/lib/formatAgo.ts` | `src/lib/formatAgo.ts` | `8bbd0bd` | как есть |
| `src/lib/focusTrap.ts` | `src/lib/focusTrap.ts` | `8bbd0bd` | как есть |
| `src/lib/activate.ts` | `src/lib/activate.ts` | `8bbd0bd` | как есть |
| `src/lib/activate.test.tsx` | `src/lib/activate.test.tsx` | `8bbd0bd` | как есть |
| `src/lib/plainInputs.ts` | `src/lib/plainInputs.ts` | `8bbd0bd` | как есть |
| `src/lib/plainInputs.test.ts` | `src/lib/plainInputs.test.ts` | `8bbd0bd` | как есть |
| `src/lib/useDebouncedValue.ts` | `src/lib/useDebouncedValue.ts` | `8bbd0bd` | как есть |
| `src/lib/useDebouncedValue.test.ts` | `src/lib/useDebouncedValue.test.ts` | `8bbd0bd` | как есть |
| `src/lib/env.ts` | `src/lib/env.ts` | `8bbd0bd` | как есть |
| `src/lib/useLanguageSync.ts` | `src/lib/useLanguageSync.ts` | `8bbd0bd` | как есть |
| `src/test/render.tsx` | `src/test/render.tsx` | `8bbd0bd` | как есть |
| `src/components/common/ErrorBanner.tsx` | `src/components/common/ErrorBanner.tsx` | `8bbd0bd` | как есть |
| `src/components/common/ErrorBanner.test.tsx` | `src/components/common/ErrorBanner.test.tsx` | `8bbd0bd` | адаптирован: ключи и коды Kubernetes |
| `src/components/common/Hint.tsx` | `src/components/common/Hint.tsx` | `8bbd0bd` | как есть |
| `src/components/common/Hint.test.tsx` | `src/components/common/Hint.test.tsx` | `8bbd0bd` | адаптирован: act вокруг focus() |
| `src/components/common/EnvBadge.tsx` | `src/components/common/EnvBadge.tsx` | `8bbd0bd` | как есть |
| `src/i18n/languages.ts` | `src/i18n/languages.ts` | `8bbd0bd` | адаптирован: языки en и ru |
| `src/lib/links.ts` | `src/lib/links.ts` | `8bbd0bd` | адаптирован: адреса репозиториев |
| `src/test/setup.ts` | `src/test/setup.ts` | `8bbd0bd` | как есть |
| `src/api/client.ts` | `src/api/client.ts` | `8bbd0bd` | адаптирован: команды Kubernetes |
| `src/api/client.test.ts` | `src/api/client.test.ts` | `8bbd0bd` | адаптирован: команды Kubernetes |
| `src/api/types.ts` | `src/api/types.ts` | `8bbd0bd` | адаптирован: типы Kubernetes |
| `scripts/i18n-check.mjs` | `scripts/i18n-check.mjs` | `8bbd0bd` | адаптирован: главы руководства O1 |
| `scripts/i18n-check.test.mjs` | `scripts/i18n-check.test.mjs` | `8bbd0bd` | как есть |
| `src/components/connections/tree/folderTree.ts` | `src/components/connections/tree/folderTree.ts` | `8bbd0bd` | как есть |
| `src/components/connections/tree/folderTree.test.ts` | `src/components/connections/tree/folderTree.test.ts` | `8bbd0bd` | как есть |
| `src/components/connections/ConnectionFormTab.tsx` | `src/components/connections/ConnectionFormTab.tsx` | `8bbd0bd` | как есть |
| `src/state/workspace.ts` | `src/state/workspace.ts` | `8bbd0bd` | адаптирован: вкладки O1 |
| `src/components/connections/profile.ts` | `src/components/connections/profile.ts` | `8bbd0bd` | адаптирован: профиль Kubernetes |
| `src/components/connections/ConnectionForm.tsx` | `src/components/connections/ConnectionForm.tsx` | `8bbd0bd` | адаптирован: поля Kubernetes, секреты, проверка по шагам |
| `src/guide/parse.ts` | `src/guide/parse.ts` | `8bbd0bd` | как есть |
| `src/guide/chapters.test.ts` | `src/guide/chapters.test.ts` | `8bbd0bd` | как есть |
| `src/components/help/HelpPanel.tsx` | `src/components/help/HelpPanel.tsx` | `8bbd0bd` | как есть |
| `src/components/help/HelpPanel.test.tsx` | `src/components/help/HelpPanel.test.tsx` | `8bbd0bd` | как есть |
| `src/state/helpContext.ts` | `src/state/helpContext.ts` | `8bbd0bd` | как есть |
| `src/guide/parse.test.ts` | `src/guide/parse.test.ts` | `8bbd0bd` | адаптирован: главы O1 |
| `src/guide/chapters.ts` | `src/guide/chapters.ts` | `8bbd0bd` | адаптирован: главы O1 |
| `src/components/help/helpChapterFor.ts` | `src/components/help/helpChapterFor.ts` | `8bbd0bd` | адаптирован: вкладки O1 |
| `src/components/help/helpChapterFor.test.ts` | `src/components/help/helpChapterFor.test.ts` | `8bbd0bd` | адаптирован: вкладки O1 |
| `src/components/settings/SettingsPanel.tsx` | `src/components/settings/SettingsPanel.tsx` | `8bbd0bd` | адаптирован: без раздела MCP |
| `src/components/settings/SettingsPanel.test.tsx` | `src/components/settings/SettingsPanel.test.tsx` | `8bbd0bd` | адаптирован: без MCP |
| `src/components/about/AboutPanel.tsx` | `src/components/about/AboutPanel.tsx` | `8bbd0bd` | адаптирован: без обновлений и диагностики (O6), товарные знаки |
| `src/components/about/AboutPanel.test.tsx` | `src/components/about/AboutPanel.test.tsx` | `8bbd0bd` | адаптирован: без обновлений и диагностики |
| `src-tauri/src/commands/app.rs` | `src-tauri/src/commands/app.rs` | `8bbd0bd` | адаптирован: только get_app_info |
| `scripts/guide-publish.mjs` | `scripts/guide-publish.mjs` | `8bbd0bd` | адаптирован: заголовок |
| `scripts/guide-publish.test.mjs` | `scripts/guide-publish.test.mjs` | `8bbd0bd` | адаптирован: языки en и ru |
| `scripts/guide-publish.d.mts` | `scripts/guide-publish.d.mts` | `8bbd0bd` | как есть |
| `src/components/connections/tree/FolderMenu.tsx` | `src/components/connections/tree/FolderMenu.tsx` | `8bbd0bd` | как есть |
| `src/components/connections/tree/ConnectionTree.tsx` | `src/components/connections/tree/ConnectionTree.tsx` | `8bbd0bd` | адаптирован: файл обмена Kubernetes, секреты, импорт kubeconfig |
| `src/components/connections/tree/ConnectionTree.test.tsx` | `src/components/connections/tree/ConnectionTree.test.tsx` | `8bbd0bd` | адаптирован: пункты подключения, секреты, kubeconfig, act вокруг ожидания |
| `src/components/connections/tree/ImportDialog.tsx` | `src/components/connections/tree/ImportDialog.tsx` | `8bbd0bd` | адаптирован: флаги секретов |
| `src/components/connections/tree/ImportDialog.test.tsx` | `src/components/connections/tree/ImportDialog.test.tsx` | `8bbd0bd` | адаптирован: флаги секретов |
| `src/components/connections/tree/ConnectionNode.tsx` | `src/components/connections/tree/ConnectionNode.tsx` | `8bbd0bd` | адаптирован: пункты Kubernetes O1 |
| `src/lib/useHelpShortcut.ts` | `src/lib/useHelpShortcut.ts` | `8bbd0bd` | адаптирован: комментарий про главу подключений |
| `src/guide/noLookbehind.test.ts` | `src/guide/noLookbehind.test.ts` | `8bbd0bd` | адаптирован: порог числа файлов |
| `src/components/TabContent.help.test.tsx` | `src/components/TabContent.help.test.tsx` | `8bbd0bd` | адаптирован: spy на console.error без возврата из beforeEach |
| `src/lib/useAppMenu.ts` | `src/lib/useAppMenu.ts` | `8bbd0bd` | адаптирован: без пунктов решения проблем и диагностики (O6) |
| `src/lib/useAppMenu.test.ts` | `src/lib/useAppMenu.test.ts` | `8bbd0bd` | адаптирован: пункты меню O1 |
| `src/components/Layout.tsx` | `src/components/Layout.tsx` | `8bbd0bd` | адаптирован: вкладки O1, без MCP и обновлений, комментарий про подключения |
| `src/components/Layout.test.tsx` | `src/components/Layout.test.tsx` | `8bbd0bd` | адаптирован: профиль Kubernetes, вкладки O1 |
| `src/components/Sidebar.tsx` | `src/components/Sidebar.tsx` | `8bbd0bd` | адаптирован: без индикатора MCP |
| `src/components/Sidebar.test.tsx` | `src/components/Sidebar.test.tsx` | `8bbd0bd` | адаптирован: пункты подключения O1 |
| `src/components/TabContent.tsx` | `src/components/TabContent.tsx` | `8bbd0bd` | адаптирован: вкладки O1 |
| `deny.toml` | `deny.toml` | `8bbd0bd` | адаптирован: без C-библиотек librdkafka и OpenSSL |
| `.github/FUNDING.yml` | `.github/FUNDING.yml` | `8bbd0bd` | адаптирован: адрес поддержки |
| `.github/workflows/ci.yml` | `.github/workflows/ci.yml` | `8bbd0bd` | адаптирован: без Kafka, стенд kind, крейты MCP-шлюза |
| `docs/i18n/README.md` | `docs/i18n/README.md` | `8bbd0bd` | адаптирован: языки en и ru, без Kafka |
| `docs/i18n/glossary.md` | `docs/i18n/glossary.md` | `8bbd0bd` | адаптирован: термины Kubernetes и OpenShift, en и ru |
| `src-tauri/src/der.rs` | `src-tauri/src/tls/der.rs` | `8bbd0bd` | адаптирован: комментарий модуля |
| `crates/glafira-mcp-core/Cargo.toml` | `crates/glafira-mcp/Cargo.toml` | `8bbd0bd` | адаптирован: только транспорт и сессии, без schemars |
| `crates/glafira-mcp-core/src/endpoint.rs` | `crates/glafira-mcp/src/endpoint.rs` | `8bbd0bd` | адаптирован: имя продукта — параметр `choose` |
| `crates/glafira-mcp-core/src/handshake.rs` | `crates/glafira-mcp/src/handshake.rs` | `8bbd0bd` | адаптирован: ссылка на спеку |
| `crates/glafira-mcp-core/src/audit.rs` | `crates/glafira-mcp/src/audit.rs` | `8bbd0bd` | адаптирован: ссылка на спеку |
| `crates/glafira-mcp-core/src/limits.rs` | `crates/glafira-mcp/src/limits.rs` | `8bbd0bd` | адаптирован: комментарии |
| `crates/glafira-mcp-core/src/fit.rs` | `crates/glafira-mcp/src/rules.rs` | `8bbd0bd` | адаптирован: без конфигов Kafka — фильтр, страницы и предел ответа |
| `crates/glafira-mcp-core/src/session.rs` | `crates/glafira-mcp/src/model.rs` | `8bbd0bd` | адаптирован: только сессия, право, ошибка и `Hooks` |
| `crates/glafira-mcp-core/src/server.rs` | `crates/glafira-mcp/src/server/mod.rs` | `8bbd0bd` | адаптирован: обвязка вызова без доменных типов, обработчик продукта создаёт `make` |
| `crates/glafira-mcp-core/src/bin/gui_stdio_probe.rs` | `crates/glafira-mcp/src/bin/gui_stdio_probe.rs` | `8bbd0bd` | как есть |
| `crates/glafira-mcp-core/tests/gui_stdio.rs` | `crates/glafira-mcp/tests/gui_stdio.rs` | `8bbd0bd` | как есть |
| `crates/glafira-mcp-core/tests/manifest.rs` | `crates/glafira-mcp/tests/manifest.rs` | `8bbd0bd` | адаптирован: запрещены зависимости Kubernetes и приложения |
| `crates/glafira-mcp-core/tests/server.rs` | `crates/glafira-mcp/tests/server.rs` | `8bbd0bd` | адаптирован: игрушечный обработчик вместо инструментов Kafka |
| `crates/glafira-mcp-kubernetes/Cargo.toml` | `crates/glafira-mcp/Cargo.toml` | `8bbd0bd` | адаптирован: поверх glafira-mcp-core, regex для маскировки |
| `crates/glafira-mcp-kubernetes/src/model.rs` | `crates/glafira-mcp/src/model.rs` | `8bbd0bd` | адаптирован: операции и ответы Kubernetes, область LLM (`Scope`) |
| `crates/glafira-mcp-kubernetes/src/health.rs` | `crates/glafira-mcp/src/health.rs` | `8bbd0bd` | адаптирован: объекты Kubernetes с проектом, проекты проверки, фильтр области |
| `crates/glafira-mcp-kubernetes/src/observe.rs` | `crates/glafira-mcp/src/observe.rs` | `8bbd0bd` | адаптирован: хронология workload'а вместо группы |
| `crates/glafira-mcp-kubernetes/tests/manifest.rs` | `crates/glafira-mcp/tests/manifest.rs` | `8bbd0bd` | адаптирован: запрещены kube, k8s-openapi, reqwest и приложение |
| `crates/glafira-mcp-kubernetes/src/server/mod.rs` | `crates/glafira-mcp/src/server/mod.rs` | `8bbd0bd` | адаптирован: обработчик поверх `glafira_mcp_core::Session`, инструкции Kubernetes, проверка области LLM |
| `crates/glafira-mcp-kubernetes/src/server/tools.rs` | `crates/glafira-mcp/src/server/tools.rs` | `8bbd0bd` | адаптирован: 13 инструментов чтения Kubernetes, маскировка в ответах |
| `crates/glafira-mcp-kubernetes/tests/server.rs` | `crates/glafira-mcp/tests/server.rs` | `8bbd0bd` | адаптирован: фальшивый Backend Kubernetes, область LLM, маскировка |
| `crates/glafira-mcp-kubernetes/src/server/health_tools.rs` | `crates/glafira-mcp/src/server/health_tools.rs` | `8bbd0bd` | адаптирован: проекты проверки, фильтр области LLM, объекты Kubernetes |
| `src-tauri/src/health/store.rs` | `src-tauri/src/health/store.rs` | `8bbd0bd` | адаптирован: без предложений LLM (этап O3) и лага групп, проекты проверки, объекты Kubernetes |
| `src-tauri/src/health/service.rs` | `src-tauri/src/health/service.rs` | `8bbd0bd` | адаптирован: сборщик снимка Kubernetes, проекты проверки, без лага групп |
| `src-tauri/src/commands/health.rs` | `src-tauri/src/commands/health.rs` | `8bbd0bd` | адаптирован: без предложений LLM (этап O3) |
