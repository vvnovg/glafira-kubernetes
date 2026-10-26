#!/usr/bin/env bash
# Стенд интеграционных тестов на kind: проект team-a, сервисная учётная запись glafira-dev с ролью edit
# только в нём, объекты проекта для тестов чтения (fixtures.yaml) и kubeconfig dev/kind/out/kubeconfig
# с контекстами admin (сертификат kind) и dev-token (токен glafira-dev, проект team-a).
# Перед запуском: kind create cluster --name glafira. KUBECONFIG — отдельный файл, не ~/.kube/config:
# export KUBECONFIG="$PWD/dev/kind/out/kind-admin.kubeconfig" (до kind create cluster; не out/kubeconfig,
# его пишет этот скрипт).
set -euo pipefail
cluster="${KIND_CLUSTER:-glafira}"
here="$(cd "$(dirname "$0")" && pwd)"
out="$here/out"
ctx="kind-$cluster"
k() { kubectl --context "$ctx" "$@"; }
mkdir -p "$out"
k create namespace team-a --dry-run=client -o yaml | k apply -f -
k -n team-a create serviceaccount glafira-dev --dry-run=client -o yaml | k apply -f -
k -n team-a create rolebinding glafira-dev-edit --clusterrole=edit --serviceaccount=team-a:glafira-dev \
  --dry-run=client -o yaml | k apply -f -
# Образ с bash, который уже есть на ноде: помощник local-path-provisioner.
helper="$(k -n local-path-storage get configmap local-path-config -o jsonpath='{.data.helperPod\.yaml}' |
  awk '/image:/ { print $2; exit }')"
if [ -z "$helper" ]; then
  echo "не найден образ-помощник: в configmap local-path-config (local-path-storage) нет helperPod.yaml с image" >&2
  exit 1
fi
# Шаблон Job не меняется после создания: повторный запуск стенда создаёт Job заново.
k -n team-a delete job migrate --ignore-not-found
sed "s|__HELPER_IMAGE__|$helper|g" "$here/fixtures.yaml" | k apply -f -
k -n team-a rollout status deployment/web --timeout=180s
k -n team-a wait --for=condition=complete job/migrate --timeout=180s
k -n team-a rollout status deployment/logger --timeout=180s
token="$(k -n team-a create token glafira-dev --duration=24h)"
kind get kubeconfig --name "$cluster" > "$out/kubeconfig"
chmod 600 "$out/kubeconfig"
export KUBECONFIG="$out/kubeconfig"
kubectl config rename-context "$ctx" admin
kubectl config set-credentials glafira-dev --token="$token"
kubectl config set-context dev-token --cluster="$ctx" --user=glafira-dev --namespace=team-a
echo "kubeconfig: $out/kubeconfig (контексты admin и dev-token)"
