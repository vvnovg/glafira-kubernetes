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
| `src-tauri/src/i18n.rs` | `src-tauri/src/i18n.rs` | `8bbd0bd` | как есть |
| `src-tauri/src/sync.rs` | `src-tauri/src/sync.rs` | `8bbd0bd` | как есть |
| `src-tauri/src/storage/fs.rs` | `src-tauri/src/storage/fs.rs` | `8bbd0bd` | как есть |
| `src-tauri/src/logging.rs` | `src-tauri/src/logging.rs` | `8bbd0bd` | адаптирован: комментарий |
| `src-tauri/src/dock_name.rs` | `src-tauri/src/dock_name.rs` | `8bbd0bd` | адаптирован: комментарий |
| `src-tauri/src/secrets.rs` | `src-tauri/src/secrets.rs` | `8bbd0bd` | адаптирован: поля секретов подключения |
| `src-tauri/src/error.rs` | `src-tauri/src/error.rs` | `8bbd0bd` | адаптирован: коды Kubernetes, без librdkafka |
| `src-tauri/src/policy/mod.rs` | `src-tauri/src/policy/mod.rs` | `8bbd0bd` | адаптирован: ключ allowInsecureTls, языки тестов |
| `src-tauri/src/policy/sources.rs` | `src-tauri/src/policy/sources.rs` | `8bbd0bd` | адаптирован: тест примеров с allowInsecureTls |
| `src-tauri/src/settings.rs` | `src-tauri/src/settings.rs` | `8bbd0bd` | адаптирован: языки en и ru |
| `src-tauri/src/commands/settings.rs` | `src-tauri/src/commands/settings.rs` | `8bbd0bd` | адаптирован: языки тестов |
| `src-tauri/src/state.rs` | `src-tauri/src/state.rs` | `8bbd0bd` | адаптирован: только хранилища, без Kafka |
| `src-tauri/src/commands/mod.rs` | `src-tauri/src/commands/mod.rs` | `8bbd0bd` | адаптирован: только blocking |
| `docs/admin/policy.json` | `docs/admin/policy.json` | `8bbd0bd` | адаптирован: идентификаторы, allowInsecureTls |
| `docs/admin/policy.mobileconfig` | `docs/admin/policy.mobileconfig` | `8bbd0bd` | адаптирован: идентификаторы, allowInsecureTls |
| `docs/admin/policy.reg` | `docs/admin/policy.reg` | `8bbd0bd` | адаптирован: идентификаторы, allowInsecureTls |
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
| `src/components/common/Hint.test.tsx` | `src/components/common/Hint.test.tsx` | `8bbd0bd` | как есть |
| `src/components/common/EnvBadge.tsx` | `src/components/common/EnvBadge.tsx` | `8bbd0bd` | как есть |
| `src/i18n/languages.ts` | `src/i18n/languages.ts` | `8bbd0bd` | адаптирован: языки en и ru |
| `src/lib/links.ts` | `src/lib/links.ts` | `8bbd0bd` | адаптирован: адреса репозиториев |
| `src/test/setup.ts` | `src/test/setup.ts` | `8bbd0bd` | как есть |
| `src/api/client.ts` | `src/api/client.ts` | `8bbd0bd` | адаптирован: команды Kubernetes |
| `src/api/types.ts` | `src/api/types.ts` | `8bbd0bd` | адаптирован: типы Kubernetes |
| `scripts/i18n-check.mjs` | `scripts/i18n-check.mjs` | `8bbd0bd` | адаптирован: главы руководства O1 |
| `scripts/i18n-check.test.mjs` | `scripts/i18n-check.test.mjs` | `8bbd0bd` | как есть |
