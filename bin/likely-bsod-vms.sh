#!/usr/bin/env bash
set -euo pipefail

WINDOW="${WINDOW:-10m}"
THRESH_PCT="${THRESH_PCT:-90}"
MON_PORT="${MON_PORT:-19091}"
DEBUG=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --window) WINDOW="$2"; shift 2;;
    --threshold) THRESH_PCT="$2"; shift 2;;
    --port) MON_PORT="$2"; shift 2;;
    --debug) DEBUG=1; shift 1;;
    -h|--help)
      echo "Usage: $0 [--port 19091] [--window 10m] [--threshold 90] [--debug]"
      exit 0
      ;;
    *) echo "Unknown arg: $1" >&2; exit 2;;
  esac
done

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }; }
need oc; need jq; need curl

oc whoami >/dev/null 2>&1 || { echo "ERROR: oc not logged in"; exit 1; }
TOKEN="$(oc whoami -t 2>/dev/null || true)"
[[ -n "$TOKEN" ]] || { echo "ERROR: oc whoami -t returned empty token"; exit 1; }

# Detect VMI resource
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
[[ -n "$VMI_RES" ]] || { echo "ERROR: No VMI API resource found"; exit 1; }

uri_encode() { jq -rn --arg v "$1" '$v|@uri'; }

# Try common base path prefixes until query works
BASE_CANDIDATES=("" "/api" "/prometheus" "/thanos" "/api/prometheus")

mon_raw() {
  local path="$1"
  curl --http1.1 -ksS -H "Authorization: Bearer ${TOKEN}" \
    "https://127.0.0.1:${MON_PORT}${path}"
}

pick_base() {
  local b out
  for b in "${BASE_CANDIDATES[@]}"; do
    out="$(mon_raw "${b}/api/v1/query?query=$(uri_encode up)" 2>/dev/null || true)"
    if jq -e '.status=="success"' >/dev/null 2>&1 <<<"$out"; then
      echo "$b"
      return 0
    fi
  done
  return 1
}

BASE_PATH="$(pick_base || true)"
if [[ -z "${BASE_PATH:-}" ]]; then
  echo "ERROR: Could not find a working Prometheus-compatible base path on https://127.0.0.1:${MON_PORT}" >&2
  echo "Try debugging the response:" >&2
  echo "  TOKEN=\$(oc whoami -t)" >&2
  echo "  curl --http1.1 -kvsS -H \"Authorization: Bearer \$TOKEN\" https://127.0.0.1:${MON_PORT}/ 2>&1 | head -n 60" >&2
  exit 1
fi

[[ "$DEBUG" -eq 1 ]] && echo "DEBUG: using base path prefix: '${BASE_PATH}'" >&2

mon_query() {
  local q="$1"
  local enc; enc="$(uri_encode "$q")"
  mon_raw "${BASE_PATH}/api/v1/query?query=${enc}"
}

# temp files to avoid argv limits
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT
VMIS_JSON_FILE="${TMPDIR}/vmis.json"
CPU_JSON_FILE="${TMPDIR}/cpu.json"
IOWAIT_JSON_FILE="${TMPDIR}/iowait.json"
DELAY_JSON_FILE="${TMPDIR}/delay.json"
WORKERS_JSON_FILE="${TMPDIR}/workers.json"

oc get nodes -l node-role.kubernetes.io/worker -o json > "$WORKERS_JSON_FILE" 2>/dev/null || true
oc get "${VMI_RES}" -A -o json > "$VMIS_JSON_FILE"

Q_CPU="sum by (namespace, name) (rate(kubevirt_vmi_cpu_usage_seconds_total[${WINDOW}]))"
Q_IOWAIT="sum by (namespace, name) (rate(kubevirt_vmi_vcpu_wait_seconds_total[${WINDOW}]))"
Q_DELAY="sum by (namespace, name) (irate(kubevirt_vmi_vcpu_delay_seconds_total[${WINDOW}]))"

mon_query "$Q_CPU" > "$CPU_JSON_FILE"
mon_query "$Q_IOWAIT" > "$IOWAIT_JSON_FILE" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}' > "$IOWAIT_JSON_FILE"
mon_query "$Q_DELAY" > "$DELAY_JSON_FILE" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}' > "$DELAY_JSON_FILE"

jq -e '.status=="success"' >/dev/null 2>&1 "$CPU_JSON_FILE" || {
  echo "ERROR: CPU query failed; raw:" >&2
  cat "$CPU_JSON_FILE" >&2
  exit 1
}

# Output TSV
jq -r --slurpfile vmis "$VMIS_JSON_FILE" \
      --slurpfile cpu "$CPU_JSON_FILE" \
      --slurpfile iow "$IOWAIT_JSON_FILE" \
      --slurpfile dly "$DELAY_JSON_FILE" \
      --slurpfile workers "$WORKERS_JSON_FILE" \
      --arg thresh "$THRESH_PCT" '
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
  | (to_map($iow[0])) as $iowMap
  | (to_map($dly[0])) as $dlyMap

  | ["NODE","NAMESPACE","VMI","VM(owner)","vCPU","CPU(cores)","CPU(%)","IOwait","Delay"] | @tsv,
    ($cpuMap | to_entries[]
      | .key as $k
      | ($vmap[$k] // null) as $v
      | select($v != null)
      | select(($v.phase == "" or $v.phase == "Running"))
      | select($v.node != "")
      | select((($wset|length)==0) or ($wset[$v.node] == true))
      | .value as $cpuCores
      | ($v.vcpus|tonumber) as $vcpus
      | (pct($cpuCores; $vcpus)) as $cpuPct
      | select($cpuPct >= ($thresh|tonumber))
      | [
          $v.node,
          ($k|split("/")[0]),
          ($k|split("/")[1]),
          ($v.vmOwner // ""),
          ($vcpus|tostring),
          ($cpuCores|tostring),
          ($cpuPct|tostring),
          (($iowMap[$k] // 0)|tostring),
          (($dlyMap[$k] // 0)|tostring)
        ] | @tsv
    )
' | (command -v column >/dev/null 2>&1 && column -t -s $'\t' || cat)
