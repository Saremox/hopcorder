#!/usr/bin/env bash
# Installs the server chart with its own ClickHouse pod (clickhouse.deploy)
# and the agent chart in both modes into the current Kubernetes context (a
# throwaway kind cluster), checks that every agent's records arrive, and that
# ClickHouse's read-only user can read them but not write. Images:
# hopcorder-server:ci and hopcorder-agent:ci, already loaded into the cluster.
set -euo pipefail
cd "$(dirname "$0")"

helm install hopcorder-server ../hopcorder-server -f server-values.yaml --wait --timeout 5m
helm install hopcorder-agent ../hopcorder-agent -f agent-values.yaml --wait --timeout 3m
helm install hopcorder-agent-ds ../hopcorder-agent -f agent-ds-values.yaml --wait --timeout 3m

kubectl port-forward svc/hopcorder-server 18080:8080 >/dev/null 2>&1 &
pf=$!
trap 'kill $pf 2>/dev/null || true' EXIT

nodes=$(kubectl get nodes -o name | wc -l)
want=$((2 + nodes)) # two StatefulSet agents and one DaemonSet agent per node
for i in $(seq 90); do
  if streams=$(curl -sf "http://127.0.0.1:18080/api/v1/query/targets?from=$(( ($(date +%s) - 3600) * 1000 ))"); then
    agents=$(jq '[.rows[] | select(.cycles > 0) | .agent_id] | unique | length' <<<"$streams")
    reached=$(jq '[.rows[] | select(.protocol == "tcp" and .last_reached)] | length' <<<"$streams")
    echo "agents with cycles: $agents/$want, reached tcp streams: $reached"
    if [ "$agents" -ge "$want" ] && [ "$reached" -ge 1 ]; then
      jq -c '.rows[] | {agent_id, target, protocol, cycles, last_reached, last_path_len}' <<<"$streams"
      curl -sf http://127.0.0.1:18080/ui/ | grep -q '<div id="app">'
      ro=$(kubectl get secret hopcorder-server-clickhouse-readonly -o jsonpath='{.data.password}' | base64 -d)
      chq() { kubectl exec statefulset/hopcorder-server-clickhouse -- clickhouse-client --user grafana --password "$ro" --query "$1"; }
      if [ "$(chq 'SELECT count() > 0 FROM hopcorder.cycles')" != 1 ]; then
        echo "ClickHouse's read-only user cannot read the cycles"
        exit 1
      fi
      if out=$(chq "INSERT INTO hopcorder.agent_seq (agent_id, max_seq) VALUES ('smoke', 1)" 2>&1) ||
        ! grep -q 'Not enough privileges' <<<"$out"; then
        echo "ClickHouse's read-only user can write (or the check failed otherwise): $out"
        exit 1
      fi
      # The image's passwordless default user is gone.
      if out=$(kubectl exec statefulset/hopcorder-server-clickhouse -- clickhouse-client --user default --query 'SELECT 1' 2>&1) ||
        ! grep -q 'Authentication failed' <<<"$out"; then
        echo "ClickHouse's default user still logs in (or the check failed otherwise): $out"
        exit 1
      fi
      echo "smoke test passed"
      exit 0
    fi
  fi
  sleep 2
done
echo "records did not arrive"
kubectl get pods -o wide
exit 1
