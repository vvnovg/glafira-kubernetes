# Glafira for Kubernetes — этап U1 «Ревизия интерфейса под macOS» (перенос из Kafka-версии)

**Дата:** 2026-11-06
**Источник:** Glafira for Apache Kafka, коммит `6a3fcfe` в `main` (выпуск 0.6.0), спецификация
`docs/superpowers/specs/2026-11-06-glafira-ui-revision-design.md` там же. Здесь описаны только решения переноса:
что берётся как есть, что адаптируется под Kubernetes и почему. Токены, типографика, элементы управления,
окно, меню и тесты — по спецификации Kafka-версии (§2–§9), она здесь не повторяется.
**Ветка:** `feat/ui-revision-u1` от `main` (0.2.0). Версию не поднимать: выпуск — отдельное решение владельца.
**Порядок в семействе:** Kafka → **Kubernetes** → PostgreSQL → LLM Inference.

Новых функций нет. Новые строки словаря — пункты меню `menu.*` и подписи значков состояния `common.state.*` (en, ru).

## 1. Решения

| Вопрос | Решение |
|---|---|
| Общие файлы | Переносятся `scripts/port.sh` с коммита `6a3fcfe` (скрипт получает номер коммита третьим аргументом; по умолчанию прежний `8bbd0bd`); каждая строка `PORTED.md` обновляется на новый коммит |
| `styles.css` | Как есть из Kafka-версии. Правила, которых нет в Kafka-версии (токены графиков `--chart-1…6`, экраны ресурсов этапа O3), переезжают в `src/app.css`: тогда `styles.css` снова «как есть» и следующие переносы — одной командой |
| `app.css` | Согласован с токенами: скругления `var(--radius)` (6 px) и 8 px у карточек, фокус списка логов — кольцо `inset var(--ring)`, подсказка выбора в списке подсказок (`.combo-list`) — акцент с белым текстом, блок `.release-notes` убран (он теперь в `styles.css`) |
| Дерево | `TreeRow` как есть; узлы Kubernetes — ниже (§2). Вложенные `ul` дают отступ 14 px и направляющие; у `ProjectsNode` — тот же `ul` |
| Подсветка активной вкладки | `aria-current="page"` и класс `current` у листа: «Обзор» — вкладка `cluster`, «Ноды» — `nodes`, «Замечания» — `findings`, строка проекта — вкладки `project`, `workload` и `pod` этого namespace (у workload'а и пода своих узлов в дереве нет — спека O1 §6.1). Пока «Проекты» свёрнуты, подсвечивается их строка, чтобы активная вкладка проекта не оставалась без следа в дереве |
| Значки подвкладок | Значки типов объектов, которых нет в дереве (workloads, поды, логи, события, сеть, конфигурация, хранилище, квоты, ресурсы, YAML), стоят в сегментных подвкладках `SubTabs` экранов проекта, workload'а и пода: `SubTabs` получает необязательный `icon` у вкладки |
| Избранный проект | Значок `star` вместо символа `★` перед именем; имя кнопки — имя проекта |
| Символы и emoji | `✎ ⋯ ▸ ▾ 🔒 × ✕ ● ★ ✅ ⚠ ⚠️ ❌ ⏳` заменены на `Icon`; `styles.test.ts` проверяет расширенный набор (`★✅⚠❌⏳` — адаптация, в Kafka-версии их нет) |
| «Обновить» | `data-action="refresh"`, значок `refresh`, подсказка `⌘R`: `RefreshControls` (обзор кластера, ноды, обзор проекта, workload, под, события, сеть, конфигурация, хранилище, квоты), «Обновить» в `YamlView`, `WorkloadResources`, `PodResources`, `ProjectUsageView`. На каждом экране в один момент видна одна подвкладка, поэтому первая кнопка видимой вкладки — всегда та, что на экране; у «Обновить журнал» в настройках MCP — `data-action` и значок без подсказки |
| Фильтры | `data-action="filter"`: поиск находок, фильтры подов, workloads, событий, ресурсов проекта, логов. ⌘F ставит фокус в первый видимый; без него — в фильтр дерева |
| Enter в формах | Не применяется: у логов и watch нет кнопки запуска (сессия перезапускается по изменению полей), у формы подключения «Проверить» и «Сохранить» — вне объёма U1 |
| Меню macOS | Как в Kafka-версии (`Menu.new`, не `Menu.default()` — ⌘W закрывает вкладку); Help со своими пунктами Kubernetes-версии: «Руководство Glafira» (⇧⌘/), разделитель, «Поддержать проект», «О программе» — без «Решения проблем» и «Собрать диагностику…» (этап O6) |
| Окно | `titleBarStyle: Overlay`, `hiddenTitle`, `tauri-plugin-window-state`, разрешения `core:window:allow-start-dragging`, `core:window:allow-close`, `window-state:default` — как в Kafka-версии |
| Цвета окружений | `ENV_COLORS` на токены: `local` `--muted`, `dev` `--accent`, `test` `--ok`, `stage` `--warn`, `prod` `--badge-danger` |
| Скриншоты | Снимок экрана из сессии недоступен: dev-сервер Vite + Chromium (Playwright из каталога сессии, не из репозитория) с заглушкой `window.__TAURI_INTERNALS__`, отвечающей на команды Kubernetes-версии фикстурами; обе темы через `colorScheme` |

## 2. Дерево подключений: узлы и значки

```
Подключение      точка цвета (dot) · бейдж окружения · замок read-only · LLM · «Изменить» (pencil, при наведении и фокусе)
 ├ Обзор         grid          → вкладка cluster
 ├ Проекты ▸     namespace     кнопка-шеврон (aria-expanded), дети читаются по щелчку
 │   └ <проект>  namespace / star (избранный)   → вкладка project; справа значок замечаний проекта
 ├ Ноды          server        → вкладка nodes (если есть права)
 └ Замечания     flag          → вкладка findings; справа значок высшей строгости и число
```

Фильтр проектов (от 8 проектов) — `.search-field` со значком `search`, как фильтр подключений. Сообщения
«Загрузка…», ошибка чтения и «Войти снова» / «Повторить» — как прежде, внутри `li` под строкой «Проекты».

Нижняя панель: «Настройки» (`settings`), «Поддержать проект» (`heart`), «Справка» (`book`), «О программе» (`info`) —
`TreeRow` без колонки шеврона; индикатор MCP (`dot`) над ними.

## 3. Значки (`Icon.tsx`)

Набор Kafka-версии как есть плюс значки Kubernetes (обводка 1.5, viewBox 16; в Kafka-версию не переносятся —
расхождение отмечено в `PORTED.md`):

| Имя | Рисунок | Где |
|---|---|---|
| `server` | две полки стойки с индикаторами | «Ноды» |
| `namespace` | квадрат пунктиром | «Проекты», проект |
| `star` | звезда с заливкой | избранный проект |
| `cube` | изометрический куб | подвкладка «Workloads» |
| `pod` | шестиугольник с точкой | подвкладка «Поды» |
| `logs` | строки текста со знаком `>` | подвкладка «Логи» |
| `clock` | циферблат | подвкладка «События» |
| `network` | три узла, соединённые линиями | подвкладка «Сеть» |
| `config` | два ползунка | подвкладка «Конфигурация» |
| `storage` | цилиндр | подвкладка «Хранилище» |
| `quota` | круговая шкала | подвкладка «Квоты» |
| `chart` | столбики | подвкладка «Ресурсы» |
| `code` | угловые скобки | подвкладка «YAML» |
| `check` | галочка | «готов», `/readyz`, шаги проверки подключения |
| `error` | круг с крестом | неудачный шаг проверки подключения |
| `hourglass` | песочные часы | оператор в состоянии Progressing |

Значки состояния (`check`, `warning`, `error`, `hourglass`) рисует компонент `StateMark` (`src/components/common/StateMark.tsx`):
`<span role="img" aria-label>` с подписью из `common.state.*` или своей, цвет по состоянию (`--ok`, `--warn`, `--danger`, `--muted`).

## 4. Переносимые файлы

Через `scripts/port.sh … 6a3fcfe` как есть: `src/styles.css`, `src/styles.test.ts`, `src/components/common/Icon.tsx`
(затем дополнен значками §3), `Icon.test.tsx`, `Kbd.tsx`, `Kbd.test.tsx`, `src/components/connections/tree/TreeRow.tsx`,
`TreeRow.test.tsx`, `src/lib/platform.ts`, `src/lib/useResizable.ts`, `src/lib/shortcuts.ts`, `shortcuts.test.tsx`,
`src/components/Sidebar.tsx`, `src/components/common/EnvBadge.tsx`, `Hint.tsx`, `Hint.test.tsx`, `src/lib/env.ts`,
`src/components/mcp/McpIndicator.tsx`, `src/components/about/UpdateNotice.tsx`.

Адаптированы (правки Kafka-версии наложены на версию Kubernetes): `src/components/Layout.tsx` (контекст активной
вкладки остаётся), `src/components/connections/tree/ConnectionTree.tsx` (импорт kubeconfig, формат файла обмена),
`ConnectionNode.tsx` (узлы §2), `src/lib/useAppMenu.ts` и `useAppMenu.test.ts` (Help Kubernetes-версии),
`src/components/health/parts.tsx` (`SeverityMark` — `dot`), `src/components/common/SubTabs.tsx` (значки),
`RefreshControls.tsx`, `YamlView.tsx`, экраны ресурсов, `ClusterOverview.tsx`, `NetworkView.tsx`, `ConnectionForm.tsx`,
`ChangeDialog.tsx`, `src/app.css`, `src-tauri/tauri.conf.json`, `src-tauri/capabilities/default.json`,
`src-tauri/Cargo.toml`, `src-tauri/src/lib.rs`, словари `en.json` и `ru.json` (ключи `menu.*`).

## 5. Тесты

Тесты Kafka-версии как есть: `Icon`, `Kbd`, `TreeRow`, `shortcuts`, `styles` (с расширенным набором глифов).
Адаптированы: `useAppMenu.test.ts` (пункты Help), `ConnectionNode.test.tsx` (избранный проект — значок `star`,
подсветка узла активной вкладки, в том числе для вкладок workload'а и пода), `Sidebar.test.tsx` и
`ConnectionTree.test.tsx` (имена кнопок через `aria-label`), `SubTabs` (значок у подвкладки).
Проверка: `pnpm test`, `pnpm test:scripts`, `pnpm i18n:check`, `pnpm build`, `cargo fmt --check`,
`cargo clippy --all-targets -- -D warnings`, `cargo test --lib`.

## 6. Вне объёма

Как в Kafka-версии: палитра команд, изменяемая ширина панели деталей, пересмотр форм. Дополнительно: узлы
workloads и подов в дереве (спека O1 §6.1 их не предусматривает), Enter в форме логов.
