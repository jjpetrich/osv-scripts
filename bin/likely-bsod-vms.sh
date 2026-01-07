#!/usr/bin/env bash
set -euo pipefail

# likely-bsod-vms.sh
# Heuristic: list VMIs likely "wedged" by sustained high CPU usage.
#
# This version:
# - Avoids passing huge JSON blobs to jq via command-line args (prevents "Argument list too long")
# - Uses an EXISTING port-forward to monitoring (default 19091 -> thanos-querier:9091)
# - Uses HTTPS + Bearer token + curl --http1.1 (avoids HTTP/2 PROTOCOL_ERROR)
#
# Requirements: oc, jq, curl
#
# Usage:
#   # Terminal 1:
#   oc -n openshift-monitoring port-forward svc/thanos-querier 19091:9091
#
#   # Terminal 2:
#   WINDOW=10m THRESH_PCT=90 ./likely-bsod-vms.sh
#   ./likely-bsod-vms.sh --port 19091 --window 5m --threshold 95 --debug

WINDOW="${WINDOW:-10m}"
THRESH_PCT="${THRESH_PCT:-90}"
MON_PORT="${MON_PORT:-19091}"
DEBUG=0

usage() {
  cat <<EOF
Usage: $0 [--port 19091] [--window 10m] [--threshold 90] [--debug]

Env:
  WINDOW=10m
  THRESH_PCT=90
  MON_PORT=19091
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port) MON_PORT="$2"; shift 2;;
    --window) WINDOW="$2"; shift 2;;
    --threshold) THRESH_PCT="$2"; shift 2;;
    --debug) DEBUG=1; shift 1;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }; }
need oc
need jq
need curl

if ! oc whoami >/dev/null 2>&1; then
  echo "ERROR: oc is not logged in (oc whoami failed). Run: oc login --web" >&2
  exit 1
fi

TOKEN="$(oc whoami -t 2>/dev/null || true)"
if [[ -z "${TOKEN:-}" ]]; then
  echo "ERROR: Could not obtain token via 'oc whoami -t'." >&2
  exit 1
fi

# Detect VMI resource name (your cluster supports vmi/virtualmachineinstances)
detect_vmi_resource() {
  local candidates=("virtualmachineinstances" "vmi" "virtualmachineinstance")
  local r
  for r in "${candidates[@]}"; do
    if oc get "$r" -A --request-timeout=5s >/dev/null 2>&1; then
      echo "$r"
      return 0
    fi
  done
  return 1
}

VMI_RES="$(detect_vmi_resource || true)"
if [[ -z "${VMI_RES:-}" ]]; then
  echo "ERROR: Could not find VirtualMachineInstance API resource." >&2
  echo "Try: oc api-resources | egrep -i 'virtualmachineinstances|vmi|kubevirt'" >&2
  exit 1
fi

# Helpers
uri_encode() { jq -rn --arg v "$1" '$v|@uri'; }

mon_query() {
  local q="$1"
  local enc; enc="$(uri_encode "$q")"
  curl --http1.1 -ksS \
    -H "Authorization: Bearer ${TOKEN}" \
    "https://127.0.0.1:${MON_PORT}/api/v1/query?query=${enc}"
}

# Validate monitoring endpoint (requires your port-forward running in another terminal)
if [[ "$DEBUG" -eq 1 ]]; then
  echo "DEBUG: testing monitoring endpoint on https://127.0.0.1:${MON_PORT} ..." >&2
fi

UP_JSON="$(mon_query "up" 2>/dev/null || true)"
if ! jq -e '.status=="success"' >/dev/null 2>&1 <<<"$UP_JSON"; then
  cat >&2 <<EOF
ERROR: Could not query monitoring at https://127.0.0.1:${MON_PORT}

Make sure port-forward is running in another terminal:
  oc -n openshift-monitoring port-forward svc/thanos-querier ${MON_PORT}:9091

Manual test:
  TOKEN="\$(oc whoami -t)"
  curl --http1.1 -ksS -H "Authorization: Bearer \$TOKEN" \\
    "https://127.0.0.1:${MON_PORT}/api/v1/query?query=up" | head
EOF
  exit 1
fi

# Temp files (avoid argv limits)
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

VMIS_JSON_FILE="${TMPDIR}/vmis.json"
CPU_JSON_FILE="${TMPDIR}/cpu.json"
IOWAIT_JSON_FILE="${TMPDIR}/iowait.json"
DELAY_JSON_FILE="${TMPDIR}/delay.json"
WORKERS_JSON_FILE="${TMPDIR}/workers.json"

# Worker nodes set (for filtering)
oc get nodes -l node-role.kubernetes.io/worker -o json > "$WORKERS_JSON_FILE" 2>/dev/null || true

# VMIs cluster-wide
oc get "${VMI_RES}" -A -o json > "$VMIS_JSON_FILE"

# PromQL queries
Q_CPU="sum by (namespace, name) (rate(kubevirt_vmi_cpu_usage_seconds_total[${WINDOW}]))"
Q_IOWAIT="sum by (namespace, name) (rate(kubevirt_vmi_vcpu_wait_seconds_total[${WINDOW}]))"
Q_DELAY="sum by (namespace, name) (irate(kubevirt_vmi_vcpu_delay_seconds_total[${WINDOW}]))"

mon_query "$Q_CPU" > "$CPU_JSON_FILE"
mon_query "$Q_IOWAIT" > "$IOWAIT_JSON_FILE" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}' > "$IOWAIT_JSON_FILE"
mon_query "$Q_DELAY" > "$DELAY_JSON_FILE" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}' > "$DELAY_JSON_FILE"

if ! jq -e '.status=="success"' >/dev/null 2>&1 "$CPU_JSON_FILE"; then
  echo "ERROR: CPU query did not return success. Raw response:" >&2
  cat "$CPU_JSON_FILE" >&2
  exit 1
fi

CPU_SERIES="$(jq '.data.result|length' "$CPU_JSON_FILE")"
if [[ "$DEBUG" -eq 1 ]]; then
  echo "DEBUG: VMI resource: ${VMI_RES}" >&2
  echo "DEBUG: cpu series returned: ${CPU_SERIES}" >&2
fi

if [[ "$CPU_SERIES" -eq 0 ]]; then
  cat >&2 <<EOF
NOTE: CPU metric query returned 0 series.
This usually means kubevirt metrics are not being scraped under that metric name.

To discover available metric names (while port-forward is running):
  TOKEN="\$(oc whoami -t)"
  curl --http1.1 -ksS -H "Authorization: Bearer \$TOKEN" \\
    "https://127.0.0.1:${MON_PORT}/api/v1/label/__name__/values" \\
    | jq -r '.data[]' | egrep -i 'kubevirt.*vmi.*cpu|virt.*cpu' | head -n 50
EOF
  # Still continue; script will likely output "no matches"
fi

# Produce results as TSV (NODE first column)
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

  # workers set (if worker label missing, include all nodes by making empty set and accepting all)
  ($workers[0].items // []) as $witems
  | (reduce $witems[] as $n ({}; . + {($n.metadata.name): true})) as $wset

  # vmi lookup map keyed by ns/name
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
      # If worker set is empty, accept all; otherwise require membership
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
' | {
  if command -v column >/dev/null 2>&1; then
    column -t -s $'\t'
  else
    cat
  fi
}
