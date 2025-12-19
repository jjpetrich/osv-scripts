#!/usr/bin/env bash
set -euo pipefail

# Make sure we use the real oc, not any wrapper/alias
unalias oc 2>/dev/null || true
unset -f oc 2>/dev/null || true

# --- ARGS ---
USE_NETWORK=false
TOPPODS=""
NODE_FILTER=""
SORTCOL=""
INCLUDE_ALL_PODS=false

print_usage() {
  cat <<EOF
Usage: $(basename "$0") [--network] [--toppods N] [--node NODE] [--sortcol COL] [--include-all-pods]

Modes:
  (default)           Show worker node throughput (Mbps) and VM counts.
  --toppods N         Show top N pods by throughput (Mbps) across the cluster.
                      Combine with --node to restrict to a specific worker node.

Options:
  --network           Use network throughput (rx+tx) instead of disk throughput.
  --toppods N         Show top N pods by throughput instead of node summary.
  --node NODE         With --toppods: only include pods on NODE.
                      Without --toppods: only show that node in the node summary.
  --sortcol COL       Column to sort by:
                        Node mode: NODE | THROUGHPUT | VM_COUNT
                        Toppods:   NODE | NAMESPACE | POD | THROUGHPUT
                      Default is THROUGHPUT (numeric, highest first).
  --include-all-pods  In --toppods mode, include all pods instead of only VM (virt-launcher) pods.
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
    --sortcol)
      if [[ $# -lt 2 ]]; then
        echo "Error: --sortcol requires a column name"
        print_usage
        exit 1
      fi
      SORTCOL=$(echo "$2" | tr '[:lower:]' '[:upper:]')
      shift 2
      ;;
    --include-all-pods)
      INCLUDE_ALL_PODS=true
      shift
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
PROM_SVC="prometheus-k8s"      # or "thanos-querier"
PROM_SVC_PORT=9091             # from `oc get svc prometheus-k8s -o yaml`
PROM_PORT_LOCAL=9095           # local port (avoid 9090)
PROM_URL="https://127.0.0.1:${PROM_PORT_LOCAL}"

MODE_LABEL="disk"
$USE_NETWORK && MODE_LABEL="network"

if [[ -n "$TOPPODS" ]]; then
  echo ">>> Mode: top ${TOPPODS} pods by ${MODE_LABEL} throughput"
  if [[ -n "$NODE_FILTER" ]]; then
    echo ">>> Node filter: ${NODE_FILTER}"
  fi
  if ! $INCLUDE_ALL_PODS; then
    echo ">>> Limiting pods to VM (virt-launcher) pods by default"
  else
    echo ">>> Including all pods (infra + VM)"
  fi
else
  echo ">>> Mode: ${MODE_LABEL} throughput per worker node"
  if [[ -n "$NODE_FILTER" ]]; then
    echo ">>> Node filter: ${NODE_FILTER}"
  fi
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

###############################################
# Helper: bytes/sec -> Mbps
###############################################
to_mbps() {
  python3 - "$1" <<'EOF'
import sys
v = sys.argv[1]
try:
    bps = float(v)
except Exception:
    bps = 0.0
print(bps * 8.0 / 1000000.0)
EOF
}

###############################################
# TOP PODS MODE
###############################################
if [[ -n "$TOPPODS" ]]; then
  echo ">>> Getting pods list..."
  # Smaller pod list when we only care about VMs
  if $INCLUDE_ALL_PODS; then
    PODS_JSON=$(oc get pods -A -o json)
  else
    PODS_JSON=$(oc get pods -A -l vm.kubevirt.io/name -o json)
  fi

  # Build map: "ns/pod" -> nodeName
  declare -A POD_NODE

  while IFS=$'\t' read -r ns pod node; do
    [[ -z "$ns" || -z "$pod" || -z "$node" ]] && continue
    POD_NODE["$ns/$pod"]="$node"
  done < <(
    echo "${PODS_JSON}" | jq -r '
      .items[]
      | [.metadata.namespace, .metadata.name, .spec.nodeName]
      | @tsv
    '
  )

  # PromQL for pod-level read/write or rx/tx (bytes/sec) – TWO QUERIES (stable)
  if $USE_NETWORK; then
    echo ">>> Querying Prometheus for *pod network* rx/tx bytes/sec..."
    POD_READ_PROMQL='sum by (namespace, pod) (rate(container_network_receive_bytes_total[5m]))'
    POD_WRITE_PROMQL='sum by (namespace, pod) (rate(container_network_transmit_bytes_total[5m]))'
    LEFT_LABEL="RX_Mbps"
    RIGHT_LABEL="TX_Mbps"
  else
    echo ">>> Querying Prometheus for *pod disk* read/write bytes/sec..."
    POD_READ_PROMQL='sum by (namespace, pod) (rate(container_fs_reads_bytes_total[5m]))'
    POD_WRITE_PROMQL='sum by (namespace, pod) (rate(container_fs_writes_bytes_total[5m]))'
    LEFT_LABEL="READ_Mbps"
    RIGHT_LABEL="WRITE_Mbps"
  fi

  ENCODED_READ_QUERY=$(python3 - <<EOF
import urllib.parse
print(urllib.parse.quote('''${POD_READ_PROMQL}'''))
EOF
  )
  ENCODED_WRITE_QUERY=$(python3 - <<EOF
import urllib.parse
print(urllib.parse.quote('''${POD_WRITE_PROMQL}'''))
EOF
  )

  RESP_READ=$(curl -ks \
    -H "Authorization: Bearer ${TOKEN}" \
    "${PROM_URL}/api/v1/query?query=${ENCODED_READ_QUERY}"
  )
  RESP_WRITE=$(curl -ks \
    -H "Authorization: Bearer ${TOKEN}" \
    "${PROM_URL}/api/v1/query?query=${ENCODED_WRITE_QUERY}"
  )

  STATUS_R=$(echo "${RESP_READ}" | jq -r '.status // "error"' 2>/dev/null || echo "error")
  STATUS_W=$(echo "${RESP_WRITE}" | jq -r '.status // "error"' 2>/dev/null || echo "error")
  COUNT_R=$(echo "${RESP_READ}" | jq '.data.result | length' 2>/dev/null || echo 0)
  COUNT_W=$(echo "${RESP_WRITE}" | jq '.data.result | length' 2>/dev/null || echo 0)

  echo ">>> Prometheus status: read=${STATUS_R} (${COUNT_R} series), write=${STATUS_W} (${COUNT_W} series)"

  if [ "${STATUS_R}" != "success" ] || [ "${STATUS_W}" != "success" ]; then
    echo "Prometheus pod query failed or returned non-JSON."
    echo "Read response:"
    echo "${RESP_READ}"
    echo "Write response:"
    echo "${RESP_WRITE}"
  fi

  # Build read/write (rx/tx) Mbps maps: key = "ns/pod"
  declare -A POD_READ_Mbps POD_WRITE_Mbps

  READ_TSV=$(echo "${RESP_READ}" | jq -r '
    if .status != "success" or (.data.result | not) then
      empty
    else
      .data.result[]
      | [.metric.namespace, .metric.pod, .value[1]]
      | @tsv
    end
  ')
  while IFS=$'\t' read -r ns pod val; do
    [[ -z "$ns" || -z "$pod" ]] && continue
    mbps=$(to_mbps "$val")
    POD_READ_Mbps["$ns/$pod"]="$mbps"
  done <<< "$READ_TSV"

  WRITE_TSV=$(echo "${RESP_WRITE}" | jq -r '
    if .status != "success" or (.data.result | not) then
      empty
    else
      .data.result[]
      | [.metric.namespace, .metric.pod, .value[1]]
      | @tsv
    end
  ')
  while IFS=$'\t' read -r ns pod val; do
    [[ -z "$ns" || -z "$pod" ]] && continue
    mbps=$(to_mbps "$val")
    POD_WRITE_Mbps["$ns/$pod"]="$mbps"
  done <<< "$WRITE_TSV"

  # Build a temp file of: total \t namespace \t pod \t node \t left \t right
  TMP_PODS=$(mktemp)
  for key in "${!POD_NODE[@]}"; do
    node="${POD_NODE[$key]}"
    [[ -z "$node" ]] && continue

    if [[ -n "$NODE_FILTER" && "$node" != "$NODE_FILTER" ]]; then
      continue
    fi

    read_mbps="${POD_READ_Mbps[$key]:-0}"
    write_mbps="${POD_WRITE_Mbps[$key]:-0}"

    total_mbps=$(python3 - <<EOF
r = float("${read_mbps}") if "${read_mbps}" else 0.0
w = float("${write_mbps}") if "${write_mbps}" else 0.0
print(r + w)
EOF
    )

    ns="${key%%/*}"
    pod="${key#*/}"

    printf "%s\t%s\t%s\t%s\t%s\t%s\n" "$total_mbps" "$ns" "$pod" "$node" "$read_mbps" "$write_mbps" >> "$TMP_PODS"
  done

  if [[ ! -s "$TMP_PODS" ]]; then
    echo "No pod throughput data found (metrics missing, VM-only filter, or node filter too strict)."
    rm -f "$TMP_PODS"
    exit 0
  fi

  # Sorting logic for toppods mode
  # Fields: 1=THROUGHPUT, 2=NAMESPACE, 3=POD, 4=NODE
  SORT_KEY="1"
  SORT_MODE="nr"   # numeric, reverse by default
  case "$SORTCOL" in
    ""|"THROUGHPUT"|"THROUGHPUT_Mbps"|"THROUGHPUT_MBPS")
      SORT_KEY="1"; SORT_MODE="nr" ;;
    "NODE")
      SORT_KEY="4"; SORT_MODE="" ;;
    "NAMESPACE")
      SORT_KEY="2"; SORT_MODE="" ;;
    "POD")
      SORT_KEY="3"; SORT_MODE="" ;;
    *)
      echo "Unknown --sortcol value: $SORTCOL"
      echo "Valid (toppods): NODE, NAMESPACE, POD, THROUGHPUT"
      rm -f "$TMP_PODS"
      exit 1
      ;;
  esac

  echo
  echo "==============================================="
  echo " Top ${TOPPODS} pods by ${MODE_LABEL} throughput (Mbps)"
  if [[ -n "$NODE_FILTER" ]]; then
    echo " (filtered to node: ${NODE_FILTER})"
  fi
  if ! $INCLUDE_ALL_PODS; then
    echo " (VM pods only: vm.kubevirt.io/name)"
  fi
  echo "==============================================="
  # Columns: NODE | NAMESPACE | POD | LEFT (read/rx) | RIGHT (write/tx) | TOTAL
  printf "%-30s %-28s %-52s %-14s %-14s %-16s\n" "NODE" "NAMESPACE" "POD" "$LEFT_LABEL" "$RIGHT_LABEL" "THROUGHPUT_Mbps"
  printf "%-30s %-28s %-52s %-14s %-14s %-16s\n" "------------------------------" "----------------------------" "----------------------------------------------------" "-------------" "-------------" "----------------"

  sort -k${SORT_KEY},${SORT_KEY}${SORT_MODE} "$TMP_PODS" \
    | head -n "${TOPPODS}" \
    | while IFS=$'\t' read -r total_mbps ns pod node left_mbps right_mbps; do
        printf "%-30s %-28s %-52s %-14.2f %-14.2f %-16.2f\n" "$node" "$ns" "$pod" "$left_mbps" "$right_mbps" "$total_mbps"
      done

  echo "==============================================="
  rm -f "$TMP_PODS"
  exit 0
fi

###############################################
# NODE SUMMARY MODE
###############################################
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

# PromQL for node-level read/write or rx/tx (bytes/sec) – two queries (stable)
if $USE_NETWORK; then
  echo ">>> Querying Prometheus for *node network* rx/tx bytes/sec by instance..."
  NODE_READ_PROMQL='sum by (instance) (rate(node_network_receive_bytes_total[5m]))'
  NODE_WRITE_PROMQL='sum by (instance) (rate(node_network_transmit_bytes_total[5m]))'
  LEFT_LABEL="RX_Mbps"
  RIGHT_LABEL="TX_Mbps"
else
  echo ">>> Querying Prometheus for *node disk* read/write bytes/sec by instance..."
  NODE_READ_PROMQL='sum by (instance) (rate(node_disk_read_bytes_total[5m]))'
  NODE_WRITE_PROMQL='sum by (instance) (rate(node_disk_written_bytes_total[5m]))'
  LEFT_LABEL="READ_Mbps"
  RIGHT_LABEL="WRITE_Mbps"
fi

ENCODED_NODE_READ_QUERY=$(python3 - <<EOF
import urllib.parse
print(urllib.parse.quote('''${NODE_READ_PROMQL}'''))
EOF
)
ENCODED_NODE_WRITE_QUERY=$(python3 - <<EOF
import urllib.parse
print(urllib.parse.quote('''${NODE_WRITE_PROMQL}'''))
EOF
)

RESP_NODE_READ=$(curl -ks \
  -H "Authorization: Bearer ${TOKEN}" \
  "${PROM_URL}/api/v1/query?query=${ENCODED_NODE_READ_QUERY}"
)
RESP_NODE_WRITE=$(curl -ks \
  -H "Authorization: Bearer ${TOKEN}" \
  "${PROM_URL}/api/v1/query?query=${ENCODED_NODE_WRITE_QUERY}"
)

STATUS_NR=$(echo "${RESP_NODE_READ}" | jq -r '.status // "error"' 2>/dev/null || echo "error")
STATUS_NW=$(echo "${RESP_NODE_WRITE}" | jq -r '.status // "error"' 2>/dev/null || echo "error")
COUNT_NR=$(echo "${RESP_NODE_READ}" | jq '.data.result | length' 2>/dev/null || echo 0)
COUNT_NW=$(echo "${RESP_NODE_WRITE}" | jq '.data.result | length' 2>/dev/null || echo 0)

echo ">>> Prometheus status: read=${STATUS_NR} (${COUNT_NR} series), write=${STATUS_NW} (${COUNT_NW} series)"

if [ "${STATUS_NR}" != "success" ] || [ "${STATUS_NW}" != "success" ]; then
  echo "Prometheus node query failed or returned non-JSON."
  echo "Read response:"
  echo "${RESP_NODE_READ}"
  echo "Write response:"
  echo "${RESP_NODE_WRITE}"
fi

# Build read/write Mbps maps for nodes: key = hostname (instance without port)
declare -A NODE_READ_Mbps NODE_WRITE_Mbps

NODE_READ_TSV=$(echo "${RESP_NODE_READ}" | jq -r '
  if .status != "success" or (.data.result | not) then
    empty
  else
    .data.result[]
    | [(.metric.instance | sub(":.*$"; "")), .value[1]]
    | @tsv
  end
')
while IFS=$'\t' read -r host val; do
  [[ -z "$host" ]] && continue
  NODE_READ_Mbps["$host"]="$(to_mbps "$val")"
done <<< "$NODE_READ_TSV"

NODE_WRITE_TSV=$(echo "${RESP_NODE_WRITE}" | jq -r '
  if .status != "success" or (.data.result | not) then
    empty
  else
    .data.result[]
    | [(.metric.instance | sub(":.*$"; "")), .value[1]]
    | @tsv
  end
')
while IFS=$'\t' read -r host val; do
  [[ -z "$host" ]] && continue
  NODE_WRITE_Mbps["$host"]="$(to_mbps "$val")"
done <<< "$NODE_WRITE_TSV"

# Build temp file: node \t total \t vmcount \t left \t right
TMP_NODE=$(mktemp)
echo "${WORKERS_JSON}" \
  | jq -r --argjson vm_counts "${VM_COUNTS_JSON}" '
      .items[]
      | .metadata.name as $name
      | ($vm_counts[$name] // 0) as $vmc
      | "\($name)\t\($vmc)"
    ' \
  | while IFS=$'\t' read -r node vmc; do
      left_mbps="${NODE_READ_Mbps[$node]:-0}"
      right_mbps="${NODE_WRITE_Mbps[$node]:-0}"
      total_mbps=$(python3 - <<EOF
l = float("${left_mbps}") if "${left_mbps}" else 0.0
r = float("${right_mbps}") if "${right_mbps}" else 0.0
print(l + r)
EOF
      )
      printf "%s\t%s\t%s\t%s\t%s\n" "$node" "$total_mbps" "$vmc" "$left_mbps" "$right_mbps" >> "$TMP_NODE"
    done

# Sorting logic for node mode
# Fields in TMP_NODE: 1=node, 2=total, 3=vmc, 4=left, 5=right
NODE_SORT_KEY="2"
NODE_SORT_MODE="nr"   # numeric, reverse by default (THROUGHPUT)
case "$SORTCOL" in
  ""|"THROUGHPUT"|"THROUGHPUT_Mbps"|"THROUGHPUT_MBPS")
    NODE_SORT_KEY="2"; NODE_SORT_MODE="nr" ;;
  "NODE")
    NODE_SORT_KEY="1"; NODE_SORT_MODE="" ;;
  "VM_COUNT")
    NODE_SORT_KEY="3"; NODE_SORT_MODE="nr" ;;
  *)
    if [[ -n "$SORTCOL" ]]; then
      echo "Unknown --sortcol value: $SORTCOL"
      echo "Valid (node mode): NODE, THROUGHPUT, VM_COUNT"
      rm -f "$TMP_NODE"
      exit 1
    fi
    ;;
esac

echo
echo "==============================================="
echo " Worker node ${MODE_LABEL} throughput & VM count"
echo "==============================================="
# NODE | VM_COUNT | LEFT (read/rx) | RIGHT (write/tx) | TOTAL
printf "%-30s %-10s %-14s %-14s %-16s\n" "NODE" "VM_COUNT" "$LEFT_LABEL" "$RIGHT_LABEL" "THROUGHPUT_Mbps"
printf "%-30s %-10s %-14s %-14s %-16s\n" "------------------------------" "--------" "-------------" "-------------" "----------------"

sort -k${NODE_SORT_KEY},${NODE_SORT_KEY}${NODE_SORT_MODE} "$TMP_NODE" \
  | while IFS=$'\t' read -r node total_mbps vmc left_mbps right_mbps; do
      if [[ -n "$NODE_FILTER" && "$node" != "$NODE_FILTER" ]]; then
        continue
      fi
      printf "%-30s %-10s %-14.2f %-14.2f %-16.2f\n" "$node" "$vmc" "$left_mbps" "$right_mbps" "$total_mbps"
    done

rm -f "$TMP_NODE"
echo "==============================================="
