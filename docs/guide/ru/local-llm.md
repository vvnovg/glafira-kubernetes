# Локальная модель

Если организация запрещает отправлять данные внешним LLM-сервисам, ассистент может работать с моделью внутри контура:
на сервере организации или прямо на компьютере инженера. Glafira для этого не меняется: она остаётся MCP-сервером (см.
[LLM-ассистенты (MCP)](llm-mcp.md#how-it-works)), а меняется только то, к какой модели обращается клиент. В главе —
как выбрать, где будет работать модель, какие модели и клиенты проверены с Glafira, готовые настройки и проверка
связки без сети.

## Где работает модель {#where}

- **Внешний сервис** (Claude, GPT и другие) — проще всего и сильнее всего, если политика организации это разрешает.
  Настройка клиента — в главе [LLM-ассистенты (MCP)](llm-mcp.md#connect-client).
- **Сервер моделей организации во внутреннем контуре** — общий для команды GPU-сервер с OpenAI- или
  Anthropic-совместимым API. Клиент настраивается так же, как для локальной модели ниже, только с адресом и ключом
  этого сервера. Аудит и настройку самого сервера моделей делает отдельный инструмент семейства Glafira.
- **Модель на компьютере инженера** — когда внешний сервис запрещён, а сервера в контуре ещё нет или его как раз
  создают и настраивают. Данные кластера не покидают компьютер: модель, клиент и Glafira работают на нём.

Дальше речь в основном о третьем варианте; настройки клиентов подходят и для второго.

## Требования к компьютеру {#hardware}

Модели нужна быстрая память: вся модель и её контекст должны поместиться в видеопамять или, на Apple Silicon, в общую
память. Ассистент на каждом шаге отправляет модели схемы инструментов Glafira (около 9 тыс. токенов) и всю переписку,
поэтому нужен контекст не меньше 32 тыс. токенов, лучше 64 тыс.

На слабом железе выигрывают модели MoE с небольшим числом активных параметров (например, Qwen3.6-35B-A3B: 35 млрд
параметров, из них на каждый токен работают 3 млрд): скорость определяется активными параметрами, а не размером.
Плотная модель того же класса (Qwen3.8-27B) отвечает точнее, но в 3–4 раза медленнее.

- **macOS на Apple Silicon** — сервер Ollama. 48 ГБ и больше: Qwen3.6-35B-A3B; 32 ГБ: gpt-oss-20b, а 35B-A3B — на
  пределе; 16–24 ГБ мало для задач анализа кластера. Проверено на M4 Pro с 48 ГБ.
- **Linux с видеокартой NVIDIA** — vLLM или Ollama. 24 ГБ видеопамяти хватит для Qwen3.6-35B-A3B или Qwen3.8-27B в
  4 битах с ограниченным контекстом, 16 ГБ — для gpt-oss-20b. vLLM — тот же сервер, что обычно стоит в контуре
  организации, поэтому это хорошая репетиция второго варианта. Не проверялось.
- **Windows с видеокартой NVIDIA** — Ollama; vLLM работает только через WSL2. Не проверялось.
- **Компьютер без дискретной видеокарты** — только модели MoE на процессоре (Ollama) при 32–64 ГБ памяти DDR5; ответа
  придётся ждать заметно дольше. Не проверялось.
- **Mac на процессоре Intel** не подходит.

## Проверенные модели {#models}

Пять задач на тестовом кластере: найти проблемные поды проекта, объяснить падение пода по логу (не раскрыв пароль из
лога), объяснить Pending, найти строку в логах полуторачасовой давности, посоветовать requests и limits. Mac M4 Pro,
48 ГБ, модели в 4 битах:

- **Qwen3.6-35B-A3B** — рекомендуем. Все задачи верно, 10–65 секунд на задачу в Ollama.
- **Qwen3.8-27B** — все задачи верно, ответы самые подробные (сверяет ресурсы узла, taints, LimitRange), но 2–11 минут
  на задачу.
- **gpt-oss-20b** — быстрая и экономная по памяти, справилась с четырьмя задачами из пяти; на поиске в логах переполнила
  контекст 64 тыс. токенов большим ответом.
- **Qwen3-30B-A3B-Instruct** — справилась с четырьмя задачами из пяти: в поиске строки в логах искала подстроку вместо
  точного совпадения.

Ни одна модель не раскрыла замаскированные значения. Все модели иногда вызывают инструмент с неверными аргументами и
исправляются сами по ответу Glafira.

## Модель в Ollama {#ollama}

Ollama (https://ollama.com) — открытый сервер моделей для macOS, Windows и Linux; на Apple Silicon он работает на MLX
и в наших замерах был в 1,3–3 раза быстрее vLLM с vllm-metal. Один адрес отвечает и по OpenAI API (для Qwen Code), и по
Anthropic API (для Claude Code).

Контекст по умолчанию у Ollama меньше, чем нужно: без настройки длинный запрос молча обрезается. Надёжнее всего —
отдельная модель с нужным контекстом, она одинаково работает для всех клиентов и почти не занимает места на диске:

```bash
ollama pull qwen3.6:35b-a3b
printf 'FROM qwen3.6:35b-a3b\nPARAMETER num_ctx 65536\n' > Modelfile
ollama create qwen3.6-64k:35b-a3b -f Modelfile
```

Модель весит около 23 ГБ. Через 5 минут без запросов Ollama выгружает её из памяти, и следующий ответ начинается
примерно на 10 секунд позже; время хранения задаёт переменная `OLLAMA_KEEP_ALIVE`. В закрытом контуре модель скачивают
на компьютере с доступом в интернет и переносят каталог моделей Ollama (`~/.ollama/models`) либо разворачивают из
внутреннего зеркала.

## Настройки Glafira {#glafira-settings}

С контекстом 64 тыс. токенов выберите в разделе [[ui:settings.mcp.section]] [[ui:settings.mcp.responseLimit]] 48 или
24 КБ: ответ на 100 КБ занимает около 25–30 тыс. токенов и вместе со схемами инструментов и перепиской может
переполнить контекст. Если заметки и предложения изменений ассистенту не нужны, отметьте для клиента
[[ui:settings.mcp.colCompact]]: запрос на каждом шаге станет короче примерно на 2,3 тыс. токенов. Подробнее — в
разделе [Предел ответа и компактный набор](llm-mcp.md#response-limit).

## Qwen Code {#qwen-code}

Qwen Code (https://github.com/QwenLM/qwen-code) — открытый консольный ассистент (Apache 2.0), рассчитанный на модели
Qwen. Ставится из npm (в закрытом контуре — из внутреннего зеркала); проверена версия 0.26.5:

```bash
npm install -g @qwen-code/qwen-code@0.26.5
```

**Положите настройки до первого запуска.** Без них Qwen Code при запуске обновляется сам (это исходящий трафик и
непроверенная версия) и включает режим подтверждения `auto`, в котором каждый вызов инструмента сначала оценивает
модель, — лишний запрос на каждый шаг. Настройки пользователя лежат в `~/.qwen/settings.json` (на Windows —
`%USERPROFILE%\.qwen\settings.json`), администратор может разложить их системными настройками:

```json
{
  "env": { "LOCAL_LLM_API_KEY": "ollama" },
  "modelProviders": {
    "openai": [
      {
        "id": "qwen3.6-64k:35b-a3b",
        "name": "Qwen3.6-35B-A3B, 64k context (local Ollama)",
        "envKey": "LOCAL_LLM_API_KEY",
        "baseUrl": "http://127.0.0.1:11434/v1",
        "generationConfig": {
          "timeout": 600000,
          "streamIdleTimeoutMs": 600000,
          "maxRetries": 1,
          "contextWindowSize": 65536,
          "samplingParams": { "temperature": 0.2, "max_tokens": 8192 }
        }
      }
    ]
  },
  "security": { "auth": { "selectedType": "openai" } },
  "model": { "name": "qwen3.6-64k:35b-a3b" },
  "privacy": { "usageStatisticsEnabled": false },
  "general": { "enableAutoUpdate": false },
  "telemetry": { "enabled": false },
  "tools": {
    "approvalMode": "default",
    "disabled": [
      "run_shell_command", "exec", "edit", "write_file", "notebook_edit",
      "web_fetch", "web_search", "image_gen",
      "agent", "create_sub_session", "list_agents", "task_stop", "send_message",
      "task_create", "task_update", "task_list",
      "team_create", "team_delete", "team_plan_approval", "request_shutdown",
      "cron_create", "cron_list", "cron_delete", "loop_wakeup", "monitor",
      "autofix_pr", "enter_worktree", "exit_worktree", "workflow",
      "artifact", "record_artifact", "record_source", "report_findings",
      "skill", "save_memory", "manage_memory", "search_memory",
      "lsp", "advisor", "get_goal", "update_goal", "zoom_image",
      "enter_plan_mode", "exit_plan_mode", "list_directory", "glob"
    ]
  },
  "mcpServers": {
    "glafira-kubernetes": {
      "command": "/Applications/Glafira for Kubernetes.app/Contents/MacOS/glafira-kubernetes",
      "args": ["mcp"],
      "timeout": 120000,
      "trust": true,
      "alwaysLoadTools": true
    }
  }
}
```

Что здесь важно:

- `envKey` — имя переменной с ключом, а не сам ключ; Ollama ключ не проверяет. Для сервера моделей организации укажите
  его адрес в `baseUrl` и ключ в переменной.
- `contextWindowSize` задаётся внутри записи модели: общие настройки генерации для моделей из `modelProviders` не
  действуют.
- `tools.disabled` убирает встроенные инструменты, которые для анализа кластера не нужны: прежде всего командную
  строку (`run_shell_command`), запись файлов и веб. С командной строкой модель могла бы обойти Glafira и выполнить
  `kubectl` с вашим kubeconfig — без маскировки и без журнала MCP. Меньше инструментов — короче запрос на каждом шаге
  и быстрее ответ. Оставлены чтение и поиск по файлам (большой ответ инструмента Qwen Code сохраняет в файл, и модель
  читает его оттуда), вопросы пользователю и список задач.
- `alwaysLoadTools` загружает инструменты Glafira сразу; без него модель перед первым вызовом каждого инструмента
  тратит шаг на его поиск. `trust` разрешает инструменты Glafira без подтверждения в Qwen Code: изменить кластер
  через них нельзя (см. [LLM-ассистенты (MCP)](llm-mcp.md#what-llm-sees)).
- Путь к Glafira — как во фрагменте [[ui:settings.mcp.otherClient]] в разделе [[ui:settings.mcp.section]].

Glafira видит Qwen Code как клиента `qwen-cli-mcp-client-glafira-kubernetes` и при первом подключении попросит его
подтвердить (см. [Подтверждение клиента](llm-mcp.md#approve)): запустите `qwen` интерактивно и подтвердите клиента в
окне Glafira. Если MCP-сервер записан не в настройках пользователя, а в `.qwen/settings.json` папки проекта, Qwen Code
запустит его только после `qwen mcp approve glafira-kubernetes`, а любое изменение записи сервера сбрасывает это
одобрение.

## Claude Code {#claude-code}

Claude Code работает с локальной моделью через Anthropic-совместимый адрес Ollama (или vLLM); для этого ему задают
переменные окружения. Не записывайте их в профиль оболочки или в `env` файла `settings.json`: тогда на локальную
модель переключатся все запуски Claude Code, в том числе работа с моделями Anthropic. Удобнее отдельная команда,
например `~/bin/claude-local` (сделайте файл исполняемым), — обычный `claude` она не затрагивает:

```bash
#!/bin/sh
# Claude Code на локальной модели; обычный claude не затронут.
# Своя конфигурация и история, чтобы выбор модели не попадал в основную.
export CLAUDE_CONFIG_DIR="$HOME/.claude-local"
export ANTHROPIC_BASE_URL=http://127.0.0.1:11434
export ANTHROPIC_API_KEY=ollama
export ANTHROPIC_AUTH_TOKEN=ollama
export ANTHROPIC_MODEL=qwen3.6-64k:35b-a3b
export ANTHROPIC_DEFAULT_OPUS_MODEL=qwen3.6-64k:35b-a3b
export ANTHROPIC_DEFAULT_SONNET_MODEL=qwen3.6-64k:35b-a3b
export ANTHROPIC_DEFAULT_HAIKU_MODEL=qwen3.6-64k:35b-a3b
export CLAUDE_CODE_SUBAGENT_MODEL=qwen3.6-64k:35b-a3b
export CLAUDE_CODE_MAX_CONTEXT_TOKENS=65536
export CLAUDE_CODE_MAX_OUTPUT_TOKENS=8192
export ENABLE_TOOL_SEARCH=false
export CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1
export DISABLE_TELEMETRY=1
export DISABLE_ERROR_REPORTING=1
exec claude "$@"
```

- Адрес — без `/v1`. Все уровни моделей и субагенты указывают на локальную модель, иначе Claude Code обратится к
  моделям, которых на сервере нет.
- `CLAUDE_CODE_MAX_CONTEXT_TOKENS` сообщает настоящий размер контекста: по имени модели Claude Code его не знает и
  пишет в лог `unrecognized_model` — это не ошибка.
- `ENABLE_TOOL_SEARCH=false` загружает инструменты Glafira сразу, как `alwaysLoadTools` у Qwen Code.
- `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC` отключает обновления, телеметрию, отчёты об ошибках и прочие обращения
  наружу. С ключом в `ANTHROPIC_API_KEY` вход в аккаунт не нужен.
- Сервер Glafira подключается как обычно (см. [Подключение клиента](llm-mcp.md#connect-client)), только командой
  `claude-local mcp add …`: у отдельной конфигурации свой список MCP-серверов. Встроенные
  инструменты стоит сузить до чтения файлов: `claude-local --tools "Read,Grep"`; инструменты Glafira можно разрешить без
  вопросов: `--allowedTools "mcp__glafira-kubernetes"`.
- В неинтерактивном режиме (`claude-local -p`) направьте ввод из `/dev/null`, иначе Claude Code ждёт ввода.

Claude Code подключается к Glafira как клиент `claude-code`.

## Сервер в контуре организации {#internal-server}

Если в организации есть свой сервер моделей, клиенту меняют только адрес, ключ и имя модели: в Qwen Code — `baseUrl`,
переменную ключа и `id`, в Claude Code — `ANTHROPIC_BASE_URL`, `ANTHROPIC_API_KEY` и имена моделей (имя модели не
должно содержать `/`). Остальные настройки из разделов выше остаются.

Если сервер — vLLM, у него должен быть включён разбор вызовов инструментов (`--enable-auto-tool-choice` и подходящий
модели `--tool-call-parser`, для Qwen3.6 — `qwen3_coder`) и контекст не меньше 64 тыс. токенов (`--max-model-len`).
Если в модели включены рассуждения, они расходуют тот же лимит токенов ответа, что и сам ответ: лимит в 8 тыс. токенов
надёжнее 4 тыс.

## Проверка {#smoke}

Проверьте связку один раз после настройки и после смены модели, клиента или версии Glafira:

1. Запустите Glafira, включите MCP и откройте нужному подключению доступ LLM (см.
   [LLM-ассистенты (MCP)](llm-mcp.md#enable)).
2. Запустите клиент и попросите: «Покажи подключения Glafira». Подтвердите клиента в окне Glafira; ассистент должен
   перечислить подключения с доступом LLM.
3. Задайте типовые вопросы о проекте: «Что не так в проекте …? Найди проблемные поды и объясни причины», «Почему падает
   под …? Посмотри лог», «Почему под … в Pending?». Ответы должны опираться на объекты и сообщения кластера.
4. Если лог содержит пароль или токен, убедитесь, что ассистент видит и показывает `***`.
5. В разделе [[ui:settings.mcp.section]] откройте журнал MCP: там видны вызовы ассистента (см.
   [Сессии, отзыв и журнал](llm-mcp.md#sessions-log)).

Без сети на macOS проверку повторяют в песочнице, которая запрещает все исходящие соединения, кроме компьютера
(localhost) и локальных сокетов — через них работают Ollama и Glafira. Профиль, например `offline.sb`:

```
(version 1)
(allow default)
(deny network-outbound)
(allow network-outbound (remote ip "localhost:*"))
(allow network-outbound (remote unix-socket))
```

```bash
sandbox-exec -f offline.sb curl -sS -m 5 https://ollama.com          # должно не соединиться
sandbox-exec -f offline.sb qwen "Что не так в проекте team-a? Найди проблемные поды"
sandbox-exec -f offline.sb claude-local -p "Что не так в проекте team-a? Найди проблемные поды" < /dev/null
```

Если клиент отвечает так же, как с сетью, ему для работы ничего, кроме локальной модели и Glafira, не нужно. Разработчики
Glafira проверяют модели скриптом из `dev/llm-eval` репозитория: пять задач, протоколы и время.

## Если не работает {#troubleshooting}

- Ответ обрывается или модель «забывает» начало разговора — у модели маленький контекст: проверьте `num_ctx` в Ollama
  (раздел [Модель в Ollama](local-llm.md#ollama)) и `contextWindowSize` или `CLAUDE_CODE_MAX_CONTEXT_TOKENS` в клиенте.
- Ответ пустой или оборван на середине мысли — рассуждения модели съели лимит токенов ответа: поднимите его до 8 тыс.
- Ассистент получает ошибку `failed to deserialize parameters` — модель вызвала инструмент с неверными аргументами.
  Glafira добавляет к ошибке набросок ожидаемых аргументов и сама разбирает вложенный объект, переданный строкой с
  JSON, поэтому модель обычно исправляется со следующей попытки.
- Модель перебирает похожие вызовы с ошибкой — так бывает у небольших моделей; возьмите модель покрупнее или Qwen3.6-35B-A3B.
- Первый ответ после перерыва идёт дольше обычного — Ollama выгрузила модель из памяти; см. `OLLAMA_KEEP_ALIVE`.
- Qwen Code при запуске предлагает выбрать провайдера — не найден `~/.qwen/settings.json` с `modelProviders` и
  `security.auth.selectedType`.
- Инструменты Glafira не видны в Qwen Code — `qwen mcp list` показывает `Pending approval`: выполните
  `qwen mcp approve glafira-kubernetes`.
- `client_not_approved` — подтвердите клиента в окне Glafira, пока он подключён (см.
  [Подтверждение клиента](llm-mcp.md#approve)).
