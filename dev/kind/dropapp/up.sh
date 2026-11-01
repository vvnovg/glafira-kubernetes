#!/usr/bin/env bash
# Имитация Platform V DropApp на kind (tests/it_dropapp.rs и смоук входа через OIDC): кластер glafira-dropapp с
# API-сервером, принимающим токены Dex, Dex в dapp-auth (auth.yaml), CRD и объекты DropApp (crds.yaml, objects.yaml)
# и VictoriaMetrics в dapp-monitoring (metrics.yaml). Пишет dev/kind/dropapp/out/kubeconfig с контекстами admin
# (сертификат kind), oidc-admin и oidc-dev (kubectl oidc-login, как kubeconfig от Loginapp). Повторный запуск
# досоздаёт недостающее. KUBECONFIG — out/kind-admin.kubeconfig, ~/.kube/config не меняется.
set -euo pipefail
cluster=glafira-dropapp
here="$(cd "$(dirname "$0")" && pwd)"
out="$here/out"
issuer=https://dex.glafira-dropapp.localhost:5556
secret=glafira-dropapp-secret
ctx="kind-$cluster"
export KUBECONFIG="$out/kind-admin.kubeconfig"
k() { kubectl --context "$ctx" "$@"; }
mkdir -p "$out"

exists() { kind get clusters 2>/dev/null | grep -qx "$cluster"; }
# CA Dex смонтирован в ноду при создании кластера: новый CA без нового кластера API-сервер не увидит.
if exists && [ ! -f "$out/dex-ca.crt" ]; then
  echo "кластер $cluster есть, а $out/dex-ca.crt нет: пересоздайте стенд (down.sh, затем up.sh)" >&2
  exit 1
fi
if [ ! -f "$out/dex-ca.crt" ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=glafira-dropapp-dex-ca" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -keyout "$out/dex-ca.key" -out "$out/dex-ca.crt" 2>/dev/null
  openssl req -newkey rsa:2048 -nodes -subj "/CN=dex.glafira-dropapp.localhost" \
    -keyout "$out/dex.key" -out "$out/dex.csr" 2>/dev/null
  printf '%s\n' "subjectAltName=DNS:dex.glafira-dropapp.localhost,DNS:dex.dapp-auth.svc,IP:127.0.0.1" \
    "extendedKeyUsage=serverAuth" "keyUsage=critical,digitalSignature,keyEncipherment" > "$out/dex.ext"
  openssl x509 -req -in "$out/dex.csr" -CA "$out/dex-ca.crt" -CAkey "$out/dex-ca.key" -CAcreateserial \
    -days 825 -extfile "$out/dex.ext" -out "$out/dex.crt" 2>/dev/null
fi

if ! exists; then
  sed -e "s|__OUT__|$out|g" -e "s|__HERE__|$here|g" "$here/kind.yaml" > "$out/kind.yaml"
  kind create cluster --name "$cluster" --config "$out/kind.yaml"
fi
# Образы уже скачаны на рабочую машину: в ноду их кладёт kind (уже загруженные пропускаются). Хранилище образов
# containerd в Docker Desktop держит только свою платформу многоплатформенного образа, а kind импортирует все
# (--all-platforms) и падает на отсутствующих слоях — тогда образ импортируется в ноду только своей платформы.
for image in ghcr.io/dexidp/dex:v2.45.1 victoriametrics/vmstorage:v1.148.0-cluster \
  victoriametrics/vminsert:v1.148.0-cluster victoriametrics/vmselect:v1.148.0-cluster \
  victoriametrics/vmagent:v1.148.0; do
  if ! kind load docker-image --name "$cluster" "$image" 2>/dev/null; then
    docker save --platform "$(docker image inspect -f '{{.Os}}/{{.Architecture}}' "$image")" "$image" |
      docker exec -i "$cluster-control-plane" ctr --namespace=k8s.io images import --digests \
        --snapshotter=overlayfs - >/dev/null
  fi
done

k apply -f "$here/crds.yaml"
k wait --for condition=established --timeout=60s -f "$here/crds.yaml"
# Образ с bash, который уже есть на ноде: помощник local-path-provisioner.
helper="$(k -n local-path-storage get configmap local-path-config -o jsonpath='{.data.helperPod\.yaml}' |
  awk '/image:/ { print $2; exit }')"
if [ -z "$helper" ]; then
  echo "не найден образ-помощник: в configmap local-path-config (local-path-storage) нет helperPod.yaml с image" >&2
  exit 1
fi
sed "s|__HELPER_IMAGE__|$helper|g" "$here/objects.yaml" | k apply -f -
# Статусы объектов DropApp: поля, которые разбирает приложение (k8s/dropapp.rs) — phase и retry у Component и
# Tool, status у DropAppNode, условие Ready у IngressTLS.
status() { k patch "$1" "$2" ${3:+-n "$3"} --subresource=status --type=merge -p "{\"status\":$4}"; }
for c in auth cni monitoring; do
  status components.config.dropapp.ru "$c" "" '{"phase":"success","retry":0}'
done
status components.config.dropapp.ru ingress-controller "" '{"phase":"upgrading","retry":0}'
for t in dex grafana; do
  status tools.config.dropapp.ru "$t" "" '{"phase":"success","retry":0}'
done
status tools.config.dropapp.ru console "" '{"phase":"failed","retry":3}'
status dropappnodes.nodes.dropapp.ru glafira-dropapp-control-plane "" '{"status":"ready"}'
status dropappnodes.nodes.dropapp.ru worker-2 "" '{"status":"provisioning"}'
status ingresstlses.tls.dropapp.ru default-cert dapp-ingress '{"conditions":[{"type":"Ready","status":"True",
  "reason":"Ready","message":"Certificate is up to date and has not expired"}]}'
status ingresstlses.tls.dropapp.ru apps-cert dapp-ingress '{"conditions":[{"type":"Ready","status":"False",
  "reason":"IssuerNotReady","message":"Issuer dapp-ca-issuer is not ready"}]}'
# Причина сбоя Tool — в событиях (kubectl describe tool): событие по uid объекта, в default — у Tool нет namespace.
uid="$(k get tools.config.dropapp.ru console -o jsonpath='{.metadata.uid}')"
now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
k apply -f - <<EOF
apiVersion: v1
kind: Event
metadata:
  name: console.install-failed
  namespace: default
involvedObject:
  apiVersion: config.dropapp.ru/v1alpha1
  kind: Tool
  name: console
  uid: $uid
type: Warning
reason: InstallFailed
message: "helm upgrade failed: timed out waiting for the condition"
source: { component: dapp-operator }
count: 3
firstTimestamp: "$now"
lastTimestamp: "$now"
EOF

k create namespace dapp-auth --dry-run=client -o yaml | k apply -f -
k -n dapp-auth create secret tls dex-tls --cert="$out/dex.crt" --key="$out/dex.key" --dry-run=client -o yaml |
  k apply -f -
k apply -f "$here/auth.yaml"
k apply -f "$here/metrics.yaml"
k -n team-a rollout status deployment/web --timeout=180s
k -n dapp-auth rollout status deployment/dex --timeout=180s
for d in vmstorage vminsert vmselect vmagent; do
  k -n dapp-monitoring rollout status "deployment/$d" --timeout=180s
done
# Метрики видны через прокси API-сервера, как их читает Glafira: первые замеры vmagent доходят до vmselect не сразу.
query=/api/v1/namespaces/dapp-monitoring/services/http:vmselect-victoria-metrics-k8s-stack:8481/proxy
query="$query/select/0/prometheus/api/v1/query?query=up"
deadline=$((SECONDS + 180))
until k get --raw "$query" 2>/dev/null | grep -q '"result":\[{'; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "vmselect не отдал метрики за 3 минуты: $query" >&2
    exit 1
  fi
  sleep 5
done

kind get kubeconfig --name "$cluster" > "$out/kubeconfig"
chmod 600 "$out/kubeconfig"
(
  export KUBECONFIG="$out/kubeconfig"
  kubectl config rename-context "$ctx" admin >/dev/null
  for who in admin dev; do
    kubectl config set-credentials "oidc-$who" --exec-api-version=client.authentication.k8s.io/v1 \
      --exec-interactive-mode=IfAvailable --exec-command=kubectl --exec-arg=oidc-login --exec-arg=get-token \
      --exec-arg="--oidc-issuer-url=$issuer" --exec-arg=--oidc-client-id=dapp \
      --exec-arg="--oidc-client-secret=$secret" --exec-arg=--oidc-extra-scope=email \
      --exec-arg=--oidc-extra-scope=groups --exec-arg="--certificate-authority=$out/dex-ca.crt" \
      --exec-arg="--token-cache-dir=$out/oidc-cache/$who" >/dev/null
  done
  kubectl config set-context oidc-admin --cluster="$ctx" --user=oidc-admin >/dev/null
  kubectl config set-context oidc-dev --cluster="$ctx" --user=oidc-dev --namespace=team-a >/dev/null
)
echo "kubeconfig: $out/kubeconfig (контексты admin, oidc-admin, oidc-dev)"
echo "издатель Dex: $issuer (CA: $out/dex-ca.crt), клиент dapp, секрет $secret"
echo "пользователи: admin@dropapp.test / dropapp-admin (cluster-admin), dev@dropapp.test / dropapp-dev (admin в team-a)"
