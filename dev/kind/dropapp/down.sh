#!/usr/bin/env bash
# Удаляет кластер имитации DropApp и каталог вывода (kubeconfig, CA и ключи Dex, кэш токенов oidc-login).
set -euo pipefail
cluster=glafira-dropapp
here="$(cd "$(dirname "$0")" && pwd)"
export KUBECONFIG="$here/out/kind-admin.kubeconfig"
kind delete cluster --name "$cluster"
rm -rf "$here/out"
