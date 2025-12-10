#!/usr/bin/env bash
set -euo pipefail

# Make sure we use the real oc, not any wrapper/alias
unalias oc 2>/dev/null || true
unset -f oc 2>/dev/null || true

# --- ARG PARSING ---
USE_NETWORK=false
TOPPODS=""
NODE_FILTER=""

print_usage() {
  cat <<EOF
Usage: $(basename "$0") [--network] [--toppods N] [--node NODE]

Modes:
  (default)           Show worker node throughput (Mbps) and VM counts.
  --toppods N         Show top N pods by throughput (Mbps) across the cluster.
                      Combine with --node to restrict to a specific worker node.

Options:
  --network           Use network throughput (rx+tx) instead of disk throughput.
  --toppods N         Show top N pods by throughput instead of node summary.
  --node NODE         When used with --toppods, only include pods on NODE.
  -h, --help          Show this help message.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --network)
      USE_NETWORK=true
      shift
      ;;
    --toppods)
      if [[ $# -lt 2 ]]; then
        echo "Error: --toppods requires a number argument"
        print_usage
        exit 1
      fi
      TOPPODS="$2"
      if ! [[ "$TOPPODS" =~ ^[0-9]+$ ]]; then
        echo "Error: --toppods argument must be an integer"
        exit 1
      fi
      shift 2
      ;;
    --node)
      if [[ $# -lt 2 ]]; then
        echo "Error: --node requires a node name"
        print_usage
        exit 1
      fi
      NODE_FILTER="$2"
      shift 2
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      print_usage
      exit 1
      ;;
  esac
done

# --- CONFIG ---
NS_MON="openshift-monitoring"
PROM_SVC="prometheus-k8s"      # or "thanos-querier" if you prefer
PROM_SVC_PORT=9091             # from `oc get svc prometheus-k8s -o yaml`
PROM_PORT_LOCAL=9095           # local port (avoid 9090 since it's busy)
PROM_URL="https://127.0.0.1:${PROM_PORT_LOCAL}"
# -------------

MODE_LABEL="disk"
$USE_NETWORK && MODE_LABEL="network"

if [[ -n "$TOPPODS" ]]; then
  echo ">>> Mode: top ${TOPPODS} pods by ${MODE_LABEL} throughput"
  if [[ -n "$NODE_FILTER" ]]; then
    echo ">>> Node filter: ${NODE_FILTER}"
  fi
else
  echo ">>> Mode: ${MODE_LABEL} throughput per worker node"
fi

echo ">>> Getting bearer token..."
TOKEN=$(oc whoami -t)

echo ">>> Port-forwarding ${PROM_SVC} ${PROM_SVC_PORT} -> local ${PROM_PORT_LOCAL}..."
oc -n "${NS_MON}" port-forward "svc/${PROM_SVC}" "${PROM_PORT_LOCAL}:${PROM_SVC_PORT}" >/tmp/throughput-pf.log 2>&1 &
PF_PID=$!

cleanup() {
  kill "${PF_PID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sleep 5

# ============================================================
# TOP PODS MODE
# ============================================================
if [[ -n "$TOPPODS" ]]; then
  echo ">>> Getting pods list..."
  PODS_JSON=$(oc get pods -A -o json)

  # PromQL for pod-level throughput
  if $USE_NETWORK; then
    echo ">>> Querying Prometheus for *pod network* throughput (rx+tx bytes/sec)..."
    POD_PROMQL='topk(2000,
      sum by (namespace, pod) (
        rate(container_network_receive_bytes_total[5m])
        + rate(container_network_transmit_bytes_total[5m])
      )
    )'
  else
    echo ">>> Querying Prometheus for *pod disk* throughput (read+write bytes/sec)..."
    POD_PROMQL='topk(2000,
      sum by (namespace, pod) (
        rate(container_fs_reads_bytes_total[5m])
        + rate(container_fs_writes_bytes_total[5m])
      )
    )'
  fi

  ENCODED_QUERY=$(python3 - <<EOF
import urllib.parse
print(urllib.parse.quote('''${POD_PROMQL}'''))
EOF
  )

  RESP=$(curl -ks \
    -H "Authorization: Bearer ${TOKEN}" \
    "${PROM_URL}/api/v1/query?query=${ENCODED_QUERY}"
  )

  STATUS=$(echo "${RESP}" | jq -r '.status // "error"' 2>/dev/null || echo "error")
  COUNT=$(echo "${RESP}" | jq '.data.result | length' 2>/dev/null || echo 0)

  echo ">>> Prometheus status: ${STATUS}, pod series returned: ${COUNT}"

  if [ "${STATUS}" != "success" ]; then
    echo "Prometheus query failed or returned non-JSON. Raw response:"
    echo "${RESP}"
  fi

  # Build map: "ns/pod" -> nodeName from Kubernetes
  declare -A POD_NODE
  while IFS=$'\t' read -r ns pod node; do
    [[ -z "$ns" || -z "$pod" ]] && continue
    POD_NODE["$ns/$pod"]="$node"
  done < <(
    echo "${PODS_JSON}" | jq -r '
      .items[]
      | [.metadata.namespace, .metadata.name, .spec.nodeName]
      | @tsv
    '
  )

  # Build lines: Mbps \t namespace \t pod \t node
  RESULTS=""
  while IFS=$'\t' read -r ns pod v_raw; do
    [[ -z "$ns" || -z "$pod" ]] && continue
    key="${ns}/${pod}"
    node="${POD_NODE[$key]:-}"
    [[ -z "$node" ]] && continue

    # Optional node filter
    if [[ -n "$NODE_FILTER" && "$node" != "$NODE_FILTER" ]]; then
      continue
    fi

    # Convert bytes/sec -> Mbps
    mbps=$(python3 - <<EOF
v = float("${v_raw}") if "${v_raw}" not in ("", "NaN") else 0.0
print(v * 8.0 / 1_000_000.0)
EOF
    )

    RESULTS+=$(printf "%s\t%s\t%s\t%s\n" "$mbps" "$ns" "$pod" "$node")
    RESULTS+=$'\n'
  done < <(
    echo "${RESP}" | jq -r '
      if .status != "success" or (.data.result | not) then
        empty
      else
        .data.result[]
        | [.metric.namespace, .metric.pod, (.value[1])]
        | @tsv
      end
    '
  )

  if [[ -z "$RESULTS" ]]; then
    echo "No pod throughput data found (metrics missing or filters too strict)."
    exit 0
  fi

echo
echo "==============================================="
echo " Top ${TOPPODS} pods by ${MODE_LABEL} throughput (Mbps)"
if [[ -n "$NODE_FILTER" ]]; then
  echo " (filtered to node: ${NODE_FILTER})"
fi
echo "==============================================="

# Columns: NODE (30) | NAMESPACE (28) | POD (52) | THROUGHPUT_Mbps (16)
printf "%-30s %-28s %-52s %-16s\n" "NODE" "NAMESPACE" "POD" "THROUGHPUT_Mbps"
printf "%-30s %-28s %-52s %-16s\n" "------------------------------" "----------------------------" "----------------------------------------------------" "----------------"

echo "${RESULTS}" \
  | sort -k1,1nr \
  | head -n "${TOPPODS}" \
  | while IFS=$'\t' read -r mbps ns pod node; do
      printf "%-30s %-28s %-52s %-16.2f\n" "$node" "$ns" "$pod" "$mbps"
    done

echo "==============================================="

  exit 0
fi

# ============================================================
# NODE SUMMARY MODE (existing behavior, now in Mbps)
# ============================================================
echo ">>> Getting worker node list..."
WORKERS_JSON=$(oc get nodes -l node-role.kubernetes.io/worker -o json)

if [ "$(echo "$WORKERS_JSON" | jq '.items | length')" -eq 0 ]; then
  echo "No worker nodes found (label node-role.kubernetes.io/worker missing?)."
  exit 1
fi

echo ">>> Counting VMs per node (KubeVirt virt-launcher pods)..."
VM_COUNTS_JSON=$(
  oc get pods -A -l vm.kubevirt.io/name -o json 2>/dev/null \
  | jq '
      [ .items[]
        | select(.status.phase=="Running")
        | .spec.nodeName
      ]
      | group_by(.)
      | map({(.[0]): length})
      | add // {}
    ' || echo '{}'
)

# PromQL for node-level throughput
if $USE_NETWORK; then
  echo ">>> Querying Prometheus for *node network* throughput (rx+tx bytes/sec) by instance..."
  PROMQL='topk(1000,
    sum by (instance) (
      rate(node_network_receive_bytes_total[5m])
      + rate(node_network_transmit_bytes_total[5m])
    )
  )'
else
  echo ">>> Querying Prometheus for *node disk* throughput (read+write bytes/sec) by instance..."
  PROMQL='topk(1000,
    sum by (instance) (
      rate(node_disk_read_bytes_total[5m])
      + rate(node_disk_written_bytes_total[5m])
    )
  )'
fi

ENCODED_QUERY=$(python3 - <<EOF
import urllib.parse
print(urllib.parse.quote('''${PROMQL}'''))
EOF
)

RESP=$(curl -ks \
  -H "Authorization: Bearer ${TOKEN}" \
  "${PROM_URL}/api/v1/query?query=${ENCODED_QUERY}"
)

STATUS=$(echo "${RESP}" | jq -r '.status // "error"' 2>/dev/null || echo "error")
COUNT=$(echo "${RESP}" | jq '.data.result | length' 2>/dev/null || echo 0)

echo ">>> Prometheus status: ${STATUS}, node series returned: ${COUNT}"

if [ "${STATUS}" != "success" ]; then
  echo "Prometheus query failed or returned non-JSON. Raw response:"
  echo "${RESP}"
fi

# Build a map: host (from instance) -> throughput (Mbps)
THROUGHPUT_JSON=$(echo "${RESP}" | jq '
  if .status != "success" or (.data.result | not) then
    {}
  else
    .data.result
    | map(
        .metric.instance as $inst
        | ($inst | sub(":.*$"; "")) as $host   # strip port, keep host/FQDN
        | .value[1] as $v
        | { ($host): ( (($v | tonumber? // 0) * 8 / 1000000) ) }  # bytes/s -> Mbps
      )
    | add // {}
  end
' 2>/dev/null || echo '{}')

echo
echo "==============================================="
echo " Worker node ${MODE_LABEL} throughput & VM count"
echo "==============================================="
printf "%-30s %-20s %-10s\n" "NODE" "THROUGHPUT_Mbps" "VM_COUNT"
printf "%-30s %-20s %-10s\n" "------------------------------" "--------------------" "--------"

echo "${WORKERS_JSON}" | jq -r \
  --argjson vm_counts "${VM_COUNTS_JSON}" \
  --argjson thr "${THROUGHPUT_JSON}" '
    .items[]
    | .metadata.name as $name
    | ($thr[$name] // 0) as $tp
    | ($vm_counts[$name] // 0) as $vmc
    | "\($name) \($tp) \($vmc)"
  ' | while read -r NODE TP VMCOUNT; do
    printf "%-30s %-20.2f %-10s\n" "$NODE" "$TP" "$VMCOUNT"
  done

echo "==============================================="
