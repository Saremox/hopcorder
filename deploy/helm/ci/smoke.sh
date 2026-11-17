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
      # DNS rebinding: through the port-forward, a page whose own name
      # resolves to 127.0.0.1 gets 421; probes and scrapers, which use the
      # pod's address, are served.
      code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Host: rebind.attacker.example:18080' \
        http://127.0.0.1:18080/api/v1/query/targets)
      if [ "$code" != 421 ]; then
        echo "a request for a foreign Host got $code, want 421"
        exit 1
      fi
      curl -sf -H 'Host: 10.244.0.9:8080' http://127.0.0.1:18080/readyz >/dev/null
      metrics=$(curl -sf -H 'Host: 10.244.0.9:8080' http://127.0.0.1:18080/metrics)
      if ! grep -qx 'hopcorder_server_host_refused_total 1' <<<"$metrics"; then
        echo "other requests were refused too (an agent or a probe?):"
        grep '^hopcorder_server_host_refused_total' <<<"$metrics" || true
        exit 1
      fi
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
      # readonly = 1 with the memory limits of server-values.yaml. Its
      # queries may set max_execution_time (1 to 300 s), which the Grafana
      # plugin sends, and nothing else: no setting that spills temporary
      # data to the volume, turns off memory tracking (one query OOM-killed
      # ClickHouse), pins memory in the query cache, makes ClickHouse write
      # MB/s of its own logs or keeps a query out of query_log.
      if [ "$(chq "SELECT getSetting('readonly'), getSetting('max_memory_usage'), getSetting('max_memory_usage_for_user')")" != \
        "$(printf '1\t200000000\t300000000')" ]; then
        echo "ClickHouse's read-only user does not have its profile"
        exit 1
      fi
      chq "SELECT 1 SETTINGS max_execution_time = 65" >/dev/null
      for s in "max_execution_time = 0" "max_execution_time = 301" "readonly = 2" "max_memory_usage = 400000000" \
        "max_untracked_memory = 100000000000" "memory_profiler_step = 0" "max_threads = 1000" \
        "use_concurrency_control = 0" "use_query_cache = 1" "query_cache_ttl = 100000000" \
        "memory_profiler_sample_probability = 1" "trace_profile_events = 1" \
        "query_profiler_real_time_period_ns = 1000000" "query_profiler_cpu_time_period_ns = 1000000" \
        "log_processors_profiles = 0" "opentelemetry_start_trace_probability = 1" "log_query_threads = 1" \
        "log_query_views = 0" "log_queries = 0" "log_queries_min_type = 'EXCEPTION_WHILE_PROCESSING'" \
        "max_bytes_before_external_sort = 1000000" "max_bytes_ratio_before_external_group_by = 0.1" \
        "join_algorithm = 'grace_hash'" "join_algorithm = 'partial_merge'" "allow_hyperscan = 1"; do
        if out=$(chq "SELECT count() FROM numbers(10) SETTINGS $s" 2>&1) ||
          ! grep -Eq 'READONLY|SETTING_CONSTRAINT_VIOLATION' <<<"$out"; then
          echo "ClickHouse's read-only user may set $s (or the check failed otherwise): $out"
          exit 1
        fi
      done
      # No hyperscan: its compiled patterns stay in ClickHouse's memory
      # outside every limit.
      if out=$(chq "SELECT multiFuzzyMatchAny('hopcorder', 1, ['hopcordr'])" 2>&1) ||
        ! grep -q 'FUNCTION_NOT_ALLOWED' <<<"$out"; then
        echo "ClickHouse's read-only user may run hyperscan functions (or the check failed otherwise): $out"
        exit 1
      fi
      # sqlite() creates its file under user_files_path before it checks the
      # grant: that path is an empty directory ClickHouse cannot write, so a
      # refused call leaves no file (each would have taken an inode).
      if out=$(chq "SELECT * FROM sqlite('probe.db','t')" 2>&1); then
        echo "ClickHouse's read-only user may use sqlite(): $out"
        exit 1
      fi
      if ! kubectl exec statefulset/hopcorder-server-clickhouse -- sh -c \
        'test -z "$(ls -A /srv)" && test ! -e /var/lib/clickhouse/user_files/probe.db'; then
        echo "sqlite() left a file behind (user_files_path writable?): $out"
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
