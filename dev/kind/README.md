# Стенд kind

Обычный Kubernetes в Docker для интеграционных тестов Rust: Glafira проверяет на нём подключение, вход
(сертификат, токен и exec-плагин — скрипт, который выдаёт токен сервисной учётной записи), платформу `kubernetes`,
права и импорт kubeconfig (`tests/it_connect.rs`), чтение кластера (`tests/it_read.rs`): проблемные поды
(CrashLoopBackOff, ImagePullBackOff, Pending), workloads, события, сеть, конфигурацию, PVC, квоты, YAML, логи и
watch, — и проверку кластера (`tests/it_health.rs`): правила §8.2 на настоящем API-сервере. Объекты проекта team-a —
в `fixtures.yaml` и `health.yaml`; образы не скачиваются: контейнеры запускает образ-помощник local-path-provisioner
(bash), который уже есть на ноде. `crds.yaml` — минимальные CRD Route и Gateway API без контроллеров: группы API есть,
статус маршрута Gateway API пишет `setup.sh`, у Route статуса нет (роутера нет). Группы `config.openshift.io` нет —
платформа стенда `kubernetes`. Остальное OpenShift (проекты, OAuth, операторы) проверяет смоук на OpenShift Local или
dev-кластере (`docs/smoke/o1a.md`, `docs/smoke/o1b.md`, `docs/smoke/o2.md`).

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
