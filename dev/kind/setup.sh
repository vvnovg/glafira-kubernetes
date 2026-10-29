#!/usr/bin/env bash
# Стенд интеграционных тестов на kind: проект team-a, сервисная учётная запись glafira-dev с ролью edit
# только в нём, объекты проекта для тестов чтения (fixtures.yaml), проверки (crds.yaml, health.yaml) и
# изменений (ops.yaml), поддельный Prometheus (metrics.yaml) с перенаправлением на рабочую машину (адрес —
# в out/host-address) и kubeconfig dev/kind/out/kubeconfig с контекстами admin (сертификат kind) и
# dev-token (токен glafira-dev, проект team-a).
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
# CRD Route и Gateway API — до объектов проекта: их виды должны быть уже зарегистрированы.
k apply -f "$here/crds.yaml"
k wait --for condition=established --timeout=60s -f "$here/crds.yaml"
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
sed "s|__HELPER_IMAGE__|$helper|g" "$here/ops.yaml" | k apply -f -
k -n team-a rollout status deployment/tuned --timeout=180s
sed "s|__HELPER_IMAGE__|$helper|g" "$here/metrics.yaml" | k apply -f -
k -n monitoring wait --for=condition=ready pod/prometheus-stub --timeout=180s
k -n team-a rollout status deployment/busy --timeout=180s
# Адрес рабочей машины, видимый из ноды: на macOS — host.docker.internal, на Linux — шлюз сети kind.
node="$cluster-control-plane"
host="$(docker exec "$node" getent ahostsv4 host.docker.internal 2>/dev/null | awk 'NR == 1 { print $1 }' || true)"
if [ -z "$host" ]; then
  host="$(docker network inspect kind -f '{{range .IPAM.Config}}{{.Gateway}} {{end}}' | tr ' ' '\n' | grep -m1 '\.' || true)"
fi
if [ -z "$host" ]; then
  echo "не найден адрес рабочей машины для поддельного Prometheus" >&2
  exit 1
fi
echo "$host" > "$out/host-address"
# Прокси API-сервера (на той же ноде) идёт на адрес заглушки: порт 19090 — на рабочую машину.
stub="$(k -n monitoring get pod prometheus-stub -o jsonpath='{.status.podIP}')"
rule=(OUTPUT -d "$stub/32" -p tcp --dport 19090 -m comment --comment glafira-prometheus-stub -j DNAT
  --to-destination "$host:19090")
docker exec "$node" iptables -t nat -C "${rule[@]}" 2>/dev/null || docker exec "$node" iptables -t nat -I "${rule[@]}"
k apply -f "$here/health.yaml"
# Контроллера Gateway API на kind нет: статус маршрута пишет стенд — шлюз маршрут принял, а бэкенда нет.
k -n team-a patch httproute api --subresource=status --type=merge -p '{"status":{"parents":[{
  "parentRef":{"name":"public"},"controllerName":"example.com/glafira-stand","conditions":[
  {"type":"Accepted","status":"True","reason":"Accepted","message":"accepted","observedGeneration":1,
   "lastTransitionTime":"2026-10-26T00:00:00Z"},
  {"type":"ResolvedRefs","status":"False","reason":"BackendNotFound","message":"Service missing-svc not found",
   "observedGeneration":1,"lastTransitionTime":"2026-10-26T00:00:00Z"}]}]}}'
token="$(k -n team-a create token glafira-dev --duration=24h)"
kind get kubeconfig --name "$cluster" > "$out/kubeconfig"
chmod 600 "$out/kubeconfig"
export KUBECONFIG="$out/kubeconfig"
kubectl config rename-context "$ctx" admin
kubectl config set-credentials glafira-dev --token="$token"
kubectl config set-context dev-token --cluster="$ctx" --user=glafira-dev --namespace=team-a
echo "kubeconfig: $out/kubeconfig (контексты admin и dev-token)"
