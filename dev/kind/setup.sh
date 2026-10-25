#!/usr/bin/env bash
# Стенд интеграционных тестов на kind: проект team-a, сервисная учётная запись glafira-dev с ролью edit
# только в нём и kubeconfig dev/kind/out/kubeconfig с контекстами admin (сертификат kind) и dev-token
# (токен glafira-dev, проект team-a). Перед запуском: kind create cluster --name glafira
set -euo pipefail
cluster="${KIND_CLUSTER:-glafira}"
out="$(cd "$(dirname "$0")" && pwd)/out"
ctx="kind-$cluster"
k() { kubectl --context "$ctx" "$@"; }
mkdir -p "$out"
k create namespace team-a --dry-run=client -o yaml | k apply -f -
k -n team-a create serviceaccount glafira-dev --dry-run=client -o yaml | k apply -f -
k -n team-a create rolebinding glafira-dev-edit --clusterrole=edit --serviceaccount=team-a:glafira-dev \
  --dry-run=client -o yaml | k apply -f -
token="$(k -n team-a create token glafira-dev --duration=24h)"
kind get kubeconfig --name "$cluster" > "$out/kubeconfig"
chmod 600 "$out/kubeconfig"
export KUBECONFIG="$out/kubeconfig"
kubectl config rename-context "$ctx" admin
kubectl config set-credentials glafira-dev --token="$token"
kubectl config set-context dev-token --cluster="$ctx" --user=glafira-dev --namespace=team-a
echo "kubeconfig: $out/kubeconfig (контексты admin и dev-token)"
