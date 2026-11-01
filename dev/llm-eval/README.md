# Проверка с локальной моделью

Скриптовая проверка MCP-шлюза Glafira с моделью на локальном сервере (Ollama, vLLM): пять сценариев на стенде
`dev/kind` (проект team-a) прогоняет минимальный MCP-клиент `agent.py`. У него нет своих инструментов и длинного
системного промпта, поэтому время прогона — нижняя граница для настоящих клиентов, а протоколы удобно сравнивать между
моделями, серверами и версиями Glafira. Ручная проверка с клиентами (Qwen Code, Claude Code) и без сети — в главе
руководства «Локальная модель» (`docs/guide/ru/local-llm.md`).

| Сценарий | Что проверяет | Верный ответ |
|---|---|---|
| `s1-health` | проблемные поды проекта | crash — CrashLoopBackOff, exit 1; pull — образ из несуществующего реестра; pending — nodeSelector; у остальных проблем нет |
| `s2-crash` | лог и маскировка | лог `starting` / `connect db password=*** user=app` / `boom`, пароль не назван |
| `s3-pending` | планирование | FailedScheduling из-за nodeSelector `glafira/none`, не ресурсы и не квота |
| `s4-logs` | поиск в логах | строка `line N` полуторачасовой давности с одной строкой до и после (`sinceSeconds` больше 3600) |
| `s5-resources` | рекомендация ресурсов | на стенде нет метрик: честный ответ без выдуманного потребления |

Эталонные ответы — поле `truth` в `scenarios.json`; протокол сверяется с ними вручную (`summarize.py --answers`).

```bash
dev/kind/setup.sh                                   # стенд, см. dev/kind/README.md
ollama pull qwen3.6:35b-a3b
ollama create qwen3.6-64k:35b-a3b -f dev/llm-eval/clients/Modelfile
GLAFIRA_BIN="$PWD/src-tauri/target/debug/bundle/macos/Glafira for Kubernetes.app/Contents/MacOS/glafira-kubernetes" \
  MODEL=qwen3.6-64k:35b-a3b dev/llm-eval/batch.sh ollama36 3
uv run --no-project python dev/llm-eval/summarize.py ollama36 --answers
```

Перед серией запустите Glafira с подключением `admin` из `dev/kind/out/kubeconfig`, включите MCP, откройте подключению
доступ LLM уровня «Метаданные и логи» и подтвердите клиента `glafira-llm-eval`, когда Glafira спросит. Диалог
подтверждения виден, пока клиент подключён: если первый прогон закончился отказом, подтвердите клиента во время
следующего.

- `bench.py` — время до первого токена и скорость генерации сервера, с кэшем префикса и без.
- `clients/` — проверенные настройки клиентов для главы руководства: `qwen-settings.json` (Qwen Code),
  `claude-code.env` и `claude-mcp.json` (Claude Code), `Modelfile` (модель с контекстом 64k для Ollama), `offline.sb`
  (профиль `sandbox-exec` без исходящих соединений, кроме localhost).
- Протоколы пишутся в `runs/` (каталог не в git).

Результаты 2026-11-01, Mac M4 Pro 48 ГБ, Qwen3.6-35B-A3B в 4 битах, по 3 прогона каждого сценария:

| Сервер или клиент | Верно | s1 | s2 | s3 | s4 | s5 |
|---|---|---|---|---|---|---|
| `agent.py` + Ollama 0.40.6 (MLX) | 15/15 | 57–64 с | 18–21 с | 20–26 с | 19–20 с | 8–16 с |
| `agent.py` + vLLM 0.32 + vllm-metal | 15/15 | 75–87 с | 34–38 с | 54–57 с | 50–88 с | 23–24 с |
| Claude Code 2.1.316 + Ollama | 15/15 | 44–67 с | 27–33 с | 27–36 с | 21–26 с | 23–30 с |
| Qwen Code 0.26.5 + Ollama | 15/15 | 74–96 с | 35–63 с | 38–45 с | 30–54 с | 15–35 с |
