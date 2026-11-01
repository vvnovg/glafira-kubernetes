#!/bin/bash
# Серия: сценарии × N повторов агентом на текущей модели. Аргументы: <метка> [повторов=3].
# Переменные: BASE_URL, MODEL, SCENARIOS (по умолчанию все пять), AGENT_ARGS, GLAFIRA_BIN.
# s4 ищет строку, записанную полтора часа назад: номер берётся из лога logger на стенде dev/kind.
set -u
cd "$(dirname "$0")"
TAG=$1; N=${2:-3}
KUBECONFIG_KIND=${KUBECONFIG_KIND:-../kind/out/kubeconfig}
PY="uv run --no-project --with openai==3.41.0 --with mcp==2.3.0 python"
for r in $(seq 1 "$N"); do
  for s in ${SCENARIOS:-s1-health s2-crash s3-pending s4-logs s5-resources}; do
    VAR=()
    if [ "$s" = s4-logs ]; then
      # --tail=5 и только целые строки: последняя может быть недописанной.
      LAST=$(KUBECONFIG=$KUBECONFIG_KIND kubectl -n team-a logs deploy/logger --tail=5 | awk '/^line [0-9]+$/{n=$2} END{print n}')
      VAR=(--var "LINE=$((LAST - 5400))")
    fi
    echo "##### $TAG r$r $s $(date +%T) ${VAR[*]:-}"
    $PY agent.py --scenario "$s" --tag "$TAG-r$r" --base-url "${BASE_URL:-http://127.0.0.1:11434/v1}" \
      ${MODEL:+--model "$MODEL"} ${AGENT_ARGS:-} ${VAR[@]+"${VAR[@]}"} 2>&1 | grep -E "^=== |Error|stopped" | tail -3
  done
done
echo "##### done $(date +%T)"
