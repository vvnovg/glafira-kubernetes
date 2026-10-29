# Стенд kind

Обычный Kubernetes в Docker для интеграционных тестов Rust: Glafira проверяет на нём подключение, вход
(сертификат, токен и exec-плагин — скрипт, который выдаёт токен сервисной учётной записи), платформу `kubernetes`,
права и импорт kubeconfig (`tests/it_connect.rs`), чтение кластера (`tests/it_read.rs`): проблемные поды
(CrashLoopBackOff, ImagePullBackOff, Pending), workloads, события, сеть, конфигурацию, PVC, квоты, YAML, логи и
watch, — проверку кластера (`tests/it_health.rs`): правила §8.2 на настоящем API-сервере, — изменения §9
(`tests/it_ops.rs`): предпросмотр с dry-run и применение каждого вида изменения, `CONFLICT`, стражи, — источник
метрик (`tests/it_metrics.rs`): автопоиск Prometheus через прокси API-сервера, статистика, рекомендация и правила
`resources.*`, — и MCP-шлюз (`tests/it_mcp.rs`): клиент rmcp через `mcp.json` и рукопожатие вызывает инструменты в
области LLM, включая ресурсы и предложения изменений. Объекты проекта team-a — в `fixtures.yaml`, `health.yaml`,
`ops.yaml` и `metrics.yaml`; образы не скачиваются: контейнеры запускает образ-помощник local-path-provisioner
(bash), который уже есть на ноде. `crds.yaml` — минимальные CRD Route и Gateway API без контроллеров: группы API есть,
статус маршрута Gateway API пишет `setup.sh`, у Route статуса нет (роутера нет). Группы `config.openshift.io` нет —
платформа стенда `kubernetes`. Остальное OpenShift (проекты, OAuth, операторы, Thanos Querier и настоящие метрики)
проверяет смоук на OpenShift Local или dev-кластере (`docs/smoke/o1a.md`, `docs/smoke/o1b.md`, `docs/smoke/o2.md`,
`docs/smoke/o3.md`).

Prometheus на стенде поддельный: прокси API-сервера ведёт только на адреса подов, поэтому за Service
`monitoring/prometheus-k8s` стоит под-заглушка с hostNetwork на ноде control-plane, а `setup.sh` перенаправляет порт
19090 её адреса (DNAT в ноде) на рабочую машину — там записанными ответами Prometheus HTTP API отвечает сам тест
(`tests/common/prometheus.rs`). Порт 19090 рабочей машины на время `it_metrics` и `it_mcp` должен быть свободен.

```bash
brew install kind            # или https://kind.sigs.k8s.io/docs/user/quick-start/
mkdir -p dev/kind/out && export KUBECONFIG="$PWD/dev/kind/out/kind-admin.kubeconfig"
kind create cluster --name glafira
dev/kind/setup.sh
GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --tests
kind delete cluster --name glafira
```

`KUBECONFIG` — отдельный файл, `~/.kube/config` не меняется: `kind create cluster` и `kind delete cluster` пишут в него
(команды идут в одной оболочке, где задан `KUBECONFIG`), а `setup.sh` берёт из него контекст `kind-glafira`. Имя
`out/kubeconfig` занято: этот файл со своими контекстами `admin` и `dev-token` пишет `setup.sh`.

На свежем кластере `it_read` иногда идёт 4–5 минут: kubelet не всегда сразу показывает CrashLoopBackOff пода `crash`.

`dev/kind/out/` в git не попадает: в kubeconfig — ключ администратора кластера и токен.
