# Стенд kind

Обычный Kubernetes в Docker для интеграционных тестов Rust: Glafira проверяет на нём подключение, вход
(сертификат, токен и exec-плагин — скрипт, который выдаёт токен сервисной учётной записи), платформу `kubernetes`,
права и импорт kubeconfig. Особенностей OpenShift (проекты, OAuth, маршруты) на kind нет — их проверяет смоук
на OpenShift Local или dev-кластере (`docs/smoke/o1a.md`).

```bash
brew install kind            # или https://kind.sigs.k8s.io/docs/user/quick-start/
kind create cluster --name glafira
dev/kind/setup.sh
GO_IT_KUBECONFIG="$PWD/dev/kind/out/kubeconfig" cargo test --manifest-path src-tauri/Cargo.toml --tests
kind delete cluster --name glafira
```

`dev/kind/out/` в git не попадает: в kubeconfig — ключ администратора кластера и токен.
