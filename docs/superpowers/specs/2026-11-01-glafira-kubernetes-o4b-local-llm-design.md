# Glafira for Kubernetes — этап O4b «Локальные модели»: спека

Уточнения поверх основной спеки (`2026-10-24-glafira-kubernetes-design.md`); где эта спека молчит, действует основная.

## 1. Цель

MCP-шлюз должен надёжно работать с локальными моделями (Qwen3.6-35B-A3B и подобными на Ollama или vLLM) в закрытом
контуре, где внешние LLM запрещены (глава руководства «Локальная модель»). Эксперимент 2026-11-01 показал четыре
места, где слабая модель с контекстом 64 тыс. токенов спотыкается о шлюз:

- (b) ошибка разбора аргументов (`failed to deserialize parameters: …`) не говорит, как вызвать правильно, и модель
  повторяет тот же вызов до исчерпания шагов (Qwen3.6 — 14 раз подряд);
- (c) ответ до 100 КБ (около 25–30 тыс. токенов) переполняет контекст 64 тыс. (gpt-oss-20b на поиске по логам);
- (d) схемы 24 инструментов занимают около 8,7 тыс. токенов, из них 2,3 тыс. (26 %) — четыре инструмента записи,
  которые нужны не каждому клиенту;
- (e) модели присылают вложенный объект строкой с JSON (парсер `qwen3_coder`), `null` и пустой ключ `""`.

Решения повторяют Glafira for Kafka 0.5.3 (glafira-kafka #11, #12) один в один — названия, значения, места в
интерфейсе, тексты для модели — с двумя добавлениями этой версии: предел действует и на инструменты логов (§3.3), а
клиент с компактным набором получает инструкции без инструментов записи (§3.4). Второе Kafka-версия повторяет у себя.

## 2. Вне объёма

Ключ политики организации для предела и набора; предел в токенах; автоматический подбор предела по модели (шлюз не
знает, какая модель за клиентом); другие наборы инструментов, кроме полного и компактного.

## 3. Шлюз

### 3.1 Подсказка формы при ошибке разбора

Если rmcp не разобрал аргументы (текст результата начинается с `failed to deserialize parameters`), к тексту
добавляется:

```
 Expected arguments ("?" marks optional, pass objects as JSON objects, not strings): <набросок>
```

Набросок строится по схеме инструмента без `$ref` (`inline_refs`): объект — `{"ключ":значение,…}` с ключами в
порядке схемы; обязательный ключ — как есть, необязательный — с `?` в конце имени; `"string"`, `"integer"`,
`"number"`, `"boolean"` — по `type` (у `type: ["integer","null"]` — без `null`); перечисление — значения через `|`
(`"Deployment|StatefulSet|DaemonSet|DeploymentConfig|Job|CronJob"`); `anyOf`/`oneOf` из скаляров — варианты через
` | `, из объектов — первый вариант; массив — `[<набросок элемента>]`. Пример для `search_workload_logs`:

```
{"connectionId":"string","namespace":"string","workload":{"kind":"Deployment|StatefulSet|DaemonSet|DeploymentConfig|Job|CronJob","name":"string"},"filter":{"caseSensitive?":"boolean","contains?":"string","regex?":"string"},"sinceSeconds?":"integer","maxMatches?":"integer","contextLines?":"integer"}
```

Код ошибки и остальной текст не меняются; журнал MCP пишет вызов как прежде.

### 3.2 Приведение аргументов к схеме

До разбора аргументы проходят `normalize_args(args, schema)` по той же схеме без `$ref`, рекурсивно:

- строка, которая разбирается как JSON-объект или массив, заменяется разобранным значением, если схема на этом месте
  строку не допускает (`type` без `"string"`; у `anyOf`/`oneOf` — ни один вариант не строка) — в том числе внутри
  вложенных объектов, элементов массивов и вариантов `anyOf`/`oneOf`;
- ключи со значением `null` и ключ `""` удаляются (`null` в обязательном поле даёт обычное `missing field` с
  подсказкой §3.1);
- остальные лишние ключи не трогаются: serde их пропускает, а у форм с `deny_unknown_fields` (`ResourceValuesSpec`,
  `ContainerResourcesSpec`) лишнее поле остаётся ошибкой аргументов (§10.5 основной спеки).

### 3.3 Предел размера ответа

- Значения — 100, 48 и 24 КБ (`RESPONSE_LIMITS_KB`), по умолчанию 100 (`MAX_RESPONSE_BYTES`). Предел — свойство
  клиента: `ClientOptions.response_limit_bytes` (§4.1) — и действует на весь вызов: обвязка `Session::call` запускает
  тело инструмента в `with_response_limit(limit, …)` (tokio `task_local`), функции усечения читают `response_limit()`
  (вне обвязки — 100 КБ).
- Усечение — как сейчас (`fit`: главный список, `truncated: true`, `hint`); в подсказках «100 KB» заменяется на «the
  size limit», например: `The response was truncated to the size limit: narrow the filter or page with offset`.
- Страховка: ответ больше `предел + HARD_LIMIT_MARGIN` (1 КБ) не отправляется — `response_too_large` с текстом
  `The response is larger than the size limit even after truncation; narrow the request: a filter, fewer items or a smaller project`.
- Логи: `get_pod_logs` отбрасывает самые старые строки, пока ответ не уложится в `response_limit()` (вместо
  постоянных 100 КБ), с `truncated: true` и прежней подсказкой; `search_workload_logs` укладывает в предел ответ с
  совпадениями: отбрасываются последние по порядку ответа совпадения с их контекстом, `truncated: true` и подсказка
  `Matches were truncated to the size limit: narrow the filter or the time range`. Длина одного значения (4 КБ у
  строки лога и значения переменной) и бюджет просмотра (20 подов, 50 МБ, 40 с) не меняются.

### 3.4 Компактный набор инструментов

- Клиенту с `ClientOptions.compact_tools = true` скрываются `annotate`, `remove_annotation`, `propose_change`,
  `withdraw_proposal` (`COMPACT_HIDDEN_TOOLS`); `list_annotations` и `list_proposals` остаются (замечания и
  предложения, сделанные раньше, видны).
- `tools/list` такому клиенту их не отдаёт; `tools/call` скрытого инструмента — ошибка протокола `invalid_params`
  `tool not found`, как у несуществующего. `cache_scope` списка инструментов — `Private` (список зависит от клиента);
  новый список клиент получает после переподключения, вызов скрытого инструмента отклоняется сразу.
- Инструкции сервера (§10.6 основной спеки) собираются из частей; компактный клиент получает их без инструментов
  записи — и в `initialize`, и в `server/discover` (клиент без `initialize` известен из `_meta`; до того как он себя
  назвал, — полные инструкции):
  - в первом абзаце фрагмент от `Nothing in the clusters can be changed through it: propose a change with
    propose_change` до `can only be described in the chat.` заменяется на: `Nothing in the clusters can be changed
    through it, and this client has no tools for notes or change proposals: describe a needed change in the chat,
    and the user applies it in Glafira.`;
  - во втором абзаце удаляются два последних предложения: от `You can leave notes with annotate` до `link them with
    relatedRuleId.`;
  - остальные абзацы — без изменений.

## 4. Приложение

### 4.1 Настройки и `Hooks`

- `glafira-mcp-core`: `pub struct ClientOptions { pub response_limit_bytes: usize, pub compact_tools: bool }`,
  `Default` — `MAX_RESPONSE_BYTES`, `false`; в трейте `Hooks` — `fn client_options(&self, session: &SessionView) ->
  ClientOptions` со значением по умолчанию `ClientOptions::default()`.
- `settings.json` → `mcp.responseLimitKb: number | null` (`null` и отсутствие — 100) и
  `mcp.approvedClients[].compactTools: bool` (по умолчанию `false`; отзыв клиента удаляет запись — при новом
  подтверждении флажок снят). `AppBackend::client_options`: предел — из `responseLimitKb` (КБ × 1024), набор — по
  имени клиента сессии среди подтверждённых; клиент без имени — полный набор.
- Команды Tauri: `set_mcp_response_limit(kb: u32) -> McpStatus` (значение не из `RESPONSE_LIMITS_KB` —
  `errors.settings.unknownResponseLimit`) и `set_mcp_client_compact(name: String, compact: bool) -> McpStatus`
  (неизвестный клиент — без изменений, статус как есть). `McpStatus` получает `responseLimitKb` и `compactTools` у
  подтверждённых клиентов. Изменение действует со следующего вызова инструмента.

### 4.2 Интерфейс

«Настройки» → «LLM-ассистенты (MCP)»:
- выпадающий список «Предел размера ответа» (`settings.mcp.responseLimit`; варианты — `settings.mcp.responseLimitKb`
  «{{kb}} КБ»: 100, 48, 24) с подсказкой (`hints.settings.mcpResponseLimit`): меньший предел — для локальной модели с
  контекстом 64 тыс. токенов; ответ 100 КБ — около 25–30 тыс. токенов; списки и логи усекаются, модель видит
  `truncated`;
- в таблице подтверждённых клиентов — столбец «Компактный набор» (`settings.mcp.colCompact`) с флажком у каждого
  клиента (`aria-label` — `settings.mcp.compactFor` «Компактный набор инструментов для {{name}}») и подсказкой
  (`hints.settings.mcpCompact`): без заметок и предложений изменений, около 2,3 тыс. токенов меньше; новый список
  клиент увидит после переподключения.

Ключи — как в Kafka-версии; словари en и ru.

### 4.3 Документация

- Глава «LLM-ассистенты (MCP)»: новый раздел «Предел ответа и компактный набор» `{#response-limit}` после
  «Подтверждение клиента» — оба параметра, где их задать и когда действуют; в нём же фраза о подсказке формы при
  ошибке аргументов.
- Глава «Локальная модель»: новый раздел «Настройки Glafira» `{#glafira-settings}` после «Модель в Ollama» — для
  контекста 64k предел 48 или 24 КБ и компактный набор, если предложения не нужны, со ссылкой на
  `llm-mcp.md#response-limit`; пункт `invalid type: string, expected struct` в «Если не работает» —
  Glafira разбирает такие строки сама (с этой версии).
- Основная спека: §10.4 — компактный набор и инструкции; §10.6 — части инструкций; §10.7 — предел (значения, `hint`,
  `response_too_large`), подсказка формы, приведение аргументов; §13 — `mcp.responseLimitKb`,
  `approvedClients[].compactTools`; §19 — строка этапа O4b.
- CHANGELOG (en, ru).

## 5. Тесты

- `glafira-mcp-core`, модульные: `normalize_args` — строка с объектом и с массивом на месте объекта (верхний уровень,
  вложенный объект, элемент массива, вариант `anyOf`), строка на месте строки не меняется, невалидный JSON остаётся
  строкой, `null` и `""` удаляются; `shape` — обязательные и необязательные ключи, `type` с `null`, перечисление,
  `anyOf` из скаляров, массив, вложенный объект; обвязка — предел из `ClientOptions` виден телу через
  `response_limit()`, `response_too_large` сверх `предел + 1 КБ`.
- `glafira-mcp-kubernetes`, тесты шлюза: `search_workload_logs` с `workload` строкой проходит; вызов без
  `connectionId` — ошибка с наброском формы; компактный клиент — `tools/list` без четырёх инструментов, вызов
  `propose_change` — `tool not found`, инструкции без `propose_change` и `annotate` и с заменяющей фразой (и через
  `initialize`, и через `server/discover`), полный клиент — прежние инструкции; при пределе 24 КБ `list_pods` с
  большим списком, `get_pod_logs` и `search_workload_logs` укладываются в 24 КБ + 1 КБ с `truncated: true`.
- Приложение: Rust — сохранение `responseLimitKb`, отказ на 50, `compactTools` у клиента, сброс при отзыве,
  `client_options` по имени сессии; Vitest — выпадающий список и флажок в `McpSection` вызывают команды и показывают
  ответ.
- Модель: `dev/llm-eval` на Qwen3.6-35B-A3B (Ollama, `qwen3.6-64k:35b-a3b`) с пределом 24 КБ и компактным набором —
  5 сценариев × 3, без потери верных ответов относительно прогона 2026-11-01; на живом стенде вызов
  `search_workload_logs` по `logger` с фильтром `line` и `sinceSeconds` 86400 при пределе 24 КБ отдаёт ответ не
  больше 25 КБ с `truncated: true` (точный поиск s4 возвращает одно совпадение и предел не задевает).
