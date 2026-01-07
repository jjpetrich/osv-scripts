#!/usr/bin/env bash
set -euo pipefail

# likely-bsod-vms.sh
# Heuristic: likely wedged/BSOD-ish VMIs = high CPU + low network + low disk IO over a window.
#
# Automatically starts oc port-forward to openshift-monitoring (thanos-querier preferred).
# Uses HTTPS + Bearer token + POST (--data-urlencode) to avoid 400s.
#
# Requires: oc, jq, curl

WINDOW="${WINDOW:-10m}"
THRESH_PCT="${THRESH_PCT:-85}"
NET_MAX_BPS="${NET_MAX_BPS:-50000}"     # 50 KB/s combined rx+tx
DISK_MAX_BPS="${DISK_MAX_BPS:-50000}"   # 50 KB/s combined read+write
TOP_N="${TOP_N:-0}"                     # 0 = disabled
DEBUG=0

usage() {
  cat <<EOF
Usage: $0 [--window 10m] [--threshold 85] [--net-max-bps 50000] [--disk-max-bps 50000] [--top N] [--debug]

Env overrides:
  WINDOW=10m
  THRESH_PCT=85
  NET_MAX_BPS=50000
  DISK_MAX_BPS=50000
  TOP_N=0
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --window) WINDOW="$2"; shift 2;;
    --threshold) THRESH_PCT="$2"; shift 2;;
    --net-max-bps) NET_MAX_BPS="$2"; shift 2;;
    --disk-max-bps) DISK_MAX_BPS="$2"; shift 2;;
    --top) TOP_N="$2"; shift 2;;
    --debug) DEBUG=1; shift 1;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }; }
need oc; need jq; need curl

oc whoami >/dev/null 2>&1 || { echo "ERROR: oc not logged in. Run: oc login --web" >&2; exit 1; }
TOKEN="$(oc whoami -t 2>/dev/null || true)"
[[ -n "${TOKEN:-}" ]] || { echo "ERROR: oc whoami -t returned empty token" >&2; exit 1; }

detect_vmi_resource() {
  local candidates=("virtualmachineinstances" "vmi" "virtualmachineinstance")
  local r
  for r in "${candidates[@]}"; do
    if oc get "$r" -A --request-timeout=5s >/dev/null 2>&1; then
      echo "$r"; return 0
    fi
  done
  return 1
}
VMI_RES="$(detect_vmi_resource || true)"
[[ -n "${VMI_RES:-}" ]] || { echo "ERROR: No VMI API resource found" >&2; exit 1; }

# ----------------------------
# Start port-forward internally
# ----------------------------
PF_PID=""
PF_PORT=""
PF_SVC=""

cleanup() {
  [[ -n "${PF_PID:-}" ]] && kill "${PF_PID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

pick_free_port() {
  local p i
  for i in {1..80}; do
    p=$(( (RANDOM % 15000) + 20000 ))
    if ! curl --http1.1 -ksS "https://127.0.0.1:${p}/" >/dev/null 2>&1; then
      echo "$p"; return 0
    fi
  done
  echo 29091
}

mon_post() {
  local q="$1"
  curl --http1.1 -ksS \
    --connect-timeout 3 --max-time 20 \
    -H "Authorization: Bearer ${TOKEN}" \
    "https://127.0.0.1:${PF_PORT}/api/v1/query" \
    --data-urlencode "query=${q}"
}

start_pf() {
  local svc="$1"
  PF_PORT="$(pick_free_port)"
  [[ "$DEBUG" -eq 1 ]] && echo "DEBUG: starting port-forward svc/${svc} ${PF_PORT}:9091" >&2

  oc -n openshift-monitoring port-forward --address 127.0.0.1 "svc/${svc}" "${PF_PORT}:9091" >/dev/null 2>&1 &
  PF_PID="$!"
  PF_SVC="$svc"

  # Wait for "up" to succeed
  local i out
  for i in {1..80}; do
    out="$(mon_post "up" 2>/dev/null || true)"
    if jq -e '.status=="success"' >/dev/null 2>&1 <<<"$out"; then
      return 0
    fi
    sleep 0.25
  done

  kill "${PF_PID}" >/dev/null 2>&1 || true
  PF_PID=""; PF_PORT=""; PF_SVC=""
  return 1
}

if start_pf "thanos-querier"; then
  :
elif start_pf "prometheus-k8s"; then
  :
else
  cat >&2 <<'EOF'
ERROR: Could not start a working port-forward to monitoring.

Try manually:
  oc -n openshift-monitoring port-forward svc/thanos-querier 19091:9091
Then:
  TOKEN="$(oc whoami -t)"
  curl --http1.1 -ksS -H "Authorization: Bearer $TOKEN" \
    "https://127.0.0.1:19091/api/v1/query" --data-urlencode "query=up" | head
EOF
  exit 1
fi

[[ "$DEBUG" -eq 1 ]] && echo "DEBUG: monitoring via svc/${PF_SVC} on https://127.0.0.1:${PF_PORT}" >&2

# ----------------------------
# Metric selection (best-effort)
# ----------------------------
# KubeVirt metric names can vary; we try common candidates and pick the first that returns series.
pick_metric() {
  local expr="$1"   # expr uses METRIC placeholder
  shift
  local m out
  for m in "$@"; do
    out="$(mon_post "${expr//METRIC/$m}" 2>/dev/null || true)"
    if jq -e '.status=="success" and (.data.result|length)>0' >/dev/null 2>&1 <<<"$out"; then
      echo "$m"
      return 0
    fi
  done
  return 1
}

# CPU metric (you already have this one)
CPU_METRIC="kubevirt_vmi_cpu_usage_seconds_total"

# Network candidates (rx/tx bytes)
NET_RX_METRIC="$(pick_metric 'sum(rate(METRIC['"$WINDOW"'])) by (namespace,name)' \
  kubevirt_vmi_network_receive_bytes_total \
  kubevirt_vmi_network_receive_bytes \
  2>/dev/null || true)"

NET_TX_METRIC="$(pick_metric 'sum(rate(METRIC['"$WINDOW"'])) by (namespace,name)' \
  kubevirt_vmi_network_transmit_bytes_total \
  kubevirt_vmi_network_transmit_bytes \
  2>/dev/null || true)"

# Disk candidates (read/write bytes)
DISK_RD_METRIC="$(pick_metric 'sum(rate(METRIC['"$WINDOW"'])) by (namespace,name)' \
  kubevirt_vmi_storage_read_traffic_bytes_total \
  kubevirt_vmi_storage_read_bytes_total \
  kubevirt_vmi_storage_read_bytes \
  2>/dev/null || true)"

DISK_WR_METRIC="$(pick_metric 'sum(rate(METRIC['"$WINDOW"'])) by (namespace,name)' \
  kubevirt_vmi_storage_write_traffic_bytes_total \
  kubevirt_vmi_storage_write_bytes_total \
  kubevirt_vmi_storage_write_bytes \
  2>/dev/null || true)"

if [[ "$DEBUG" -eq 1 ]]; then
  echo "DEBUG: net rx metric: ${NET_RX_METRIC:-<none>}" >&2
  echo "DEBUG: net tx metric: ${NET_TX_METRIC:-<none>}" >&2
  echo "DEBUG: disk rd metric: ${DISK_RD_METRIC:-<none>}" >&2
  echo "DEBUG: disk wr metric: ${DISK_WR_METRIC:-<none>}" >&2
fi

# ----------------------------
# Gather inventory + query data
# ----------------------------
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"; cleanup' EXIT

VMIS_JSON="$TMPDIR/vmis.json"
WORKERS_JSON="$TMPDIR/workers.json"
CPU_JSON="$TMPDIR/cpu.json"
RX_JSON="$TMPDIR/rx.json"
TX_JSON="$TMPDIR/tx.json"
RD_JSON="$TMPDIR/rd.json"
WR_JSON="$TMPDIR/wr.json"

oc get nodes -l node-role.kubernetes.io/worker -o json > "$WORKERS_JSON" 2>/dev/null || true
oc get "${VMI_RES}" -A -o json > "$VMIS_JSON"

Q_CPU="sum by (namespace, name) (rate(${CPU_METRIC}[${WINDOW}]))"
mon_post "$Q_CPU" > "$CPU_JSON"

jq -e '.status=="success"' >/dev/null 2>&1 "$CPU_JSON" || {
  echo "ERROR: CPU query failed. Raw:" >&2
  cat "$CPU_JSON" >&2
  exit 1
}

# Net + Disk: if metric missing, write empty result sets (so script still runs)
if [[ -n "${NET_RX_METRIC:-}" ]]; then
  mon_post "sum by (namespace, name) (rate(${NET_RX_METRIC}[${WINDOW}]))" > "$RX_JSON"
else
  echo '{"status":"success","data":{"result":[]}}' > "$RX_JSON"
fi

if [[ -n "${NET_TX_METRIC:-}" ]]; then
  mon_post "sum by (namespace, name) (rate(${NET_TX_METRIC}[${WINDOW}]))" > "$TX_JSON"
else
  echo '{"status":"success","data":{"result":[]}}' > "$TX_JSON"
fi

if [[ -n "${DISK_RD_METRIC:-}" ]]; then
  mon_post "sum by (namespace, name) (rate(${DISK_RD_METRIC}[${WINDOW}]))" > "$RD_JSON"
else
  echo '{"status":"success","data":{"result":[]}}' > "$RD_JSON"
fi

if [[ -n "${DISK_WR_METRIC:-}" ]]; then
  mon_post "sum by (namespace, name) (rate(${DISK_WR_METRIC}[${WINDOW}]))" > "$WR_JSON"
else
  echo '{"status":"success","data":{"result":[]}}' > "$WR_JSON"
fi

CPU_SERIES="$(jq '.data.result|length' "$CPU_JSON")"
[[ "$DEBUG" -eq 1 ]] && echo "DEBUG: cpu series returned: ${CPU_SERIES}" >&2

# ----------------------------
# Output
# ----------------------------
echo "Monitoring: svc/${PF_SVC} via https://127.0.0.1:${PF_PORT}"
echo "Window=${WINDOW} | CPU>=${THRESH_PCT}% | net<=${NET_MAX_BPS} B/s | disk<=${DISK_MAX_BPS} B/s | top=${TOP_N}"
echo

jq -r --slurpfile vmis "$VMIS_JSON" \
      --slurpfile cpu "$CPU_JSON" \
      --slurpfile rx "$RX_JSON" \
      --slurpfile tx "$TX_JSON" \
      --slurpfile rd "$RD_JSON" \
      --slurpfile wr "$WR_JSON" \
      --slurpfile workers "$WORKERS_JSON" \
      --arg thresh "$THRESH_PCT" \
      --arg netmax "$NET_MAX_BPS" \
      --arg diskmax "$DISK_MAX_BPS" \
      --arg top "$TOP_N" '
  def vcpu_count(v):
    ((v.spec.domain.cpu.cores // 1) *
     (v.spec.domain.cpu.sockets // 1) *
     (v.spec.domain.cpu.threads // 1));
  def to_map(res):
    res.data.result
    | reduce .[] as $r ({}; . + { (($r.metric.namespace + "/" + $r.metric.name)): ($r.value[1]|tonumber) });
  def pct(a;b): if b <= 0 then 0 else (a / b * 100) end;

  ($workers[0].items // []) as $witems
  | (reduce $witems[] as $n ({}; . + {($n.metadata.name): true})) as $wset

  | ($vmis[0].items
     | reduce .[] as $v ({}; . + {
         (($v.metadata.namespace + "/" + $v.metadata.name)):
         {
           vcpus: (vcpu_count($v)|tonumber),
           phase: ($v.status.phase // ""),
           node: ($v.status.nodeName // ""),
           vmOwner: ((($v.metadata.ownerReferences // []) | map(select(.kind=="VirtualMachine"))[0].name) // "")
         }
       })
    ) as $vmap

  | (to_map($cpu[0])) as $cpuMap
  | (to_map($rx[0])) as $rxMap
  | (to_map($tx[0])) as $txMap
  | (to_map($rd[0])) as $rdMap
  | (to_map($wr[0])) as $wrMap

  | ($cpuMap
     | to_entries
     | map(.key as $k
         | ($vmap[$k] // null) as $v
         | select($v != null)
         | select(($v.phase == "" or $v.phase == "Running"))
         | select($v.node != "")
         | select((($wset|length)==0) or ($wset[$v.node] == true))
         | (.value|tonumber) as $cpuCores
         | ($v.vcpus|tonumber) as $vcpus
         | (pct($cpuCores; $vcpus)) as $cpuPct
         | (($rxMap[$k] // 0) + ($txMap[$k] // 0)) as $netBps
         | (($rdMap[$k] // 0) + ($wrMap[$k] // 0)) as $diskBps
         | {
             node: $v.node,
             ns: ($k|split("/")[0]),
             vmi: ($k|split("/")[1]),
             vmOwner: ($v.vmOwner // ""),
             vcpus: $vcpus,
             cpuCores: $cpuCores,
             cpuPct: $cpuPct,
             netBps: $netBps,
             diskBps: $diskBps
           }
       )
     | sort_by(-.cpuPct)
    ) as $rows

  | ["NODE","NAMESPACE","VMI","VM(owner)","vCPU","CPU(cores)","CPU(%)","NET(B/s)","DISK(B/s)"] | @tsv,
    (
      if ($top|tonumber) > 0 then
        ($rows[0:($top|tonumber)] | .[])
      else
        ($rows
          | map(select(.cpuPct >= ($thresh|tonumber)))
          | map(select(.netBps <= ($netmax|tonumber)))
          | map(select(.diskBps <= ($diskmax|tonumber)))
          | .[]
        )
      end
      | [
          .node, .ns, .vmi, .vmOwner,
          (.vcpus|tostring),
          (.cpuCores|tostring),
          (.cpuPct|tostring),
          (.netBps|tostring),
          (.diskBps|tostring)
        ] | @tsv
    )
' | (command -v column >/dev/null 2>&1 && column -t -s $'\t' || cat)
