#!/usr/bin/env bash
set -euo pipefail

WINDOW="${WINDOW:-10m}"
THRESH_PCT="${THRESH_PCT:-90}"
MON_PORT="${MON_PORT:-19091}"
DEBUG=0

usage() {
  cat <<EOF
Usage: $0 [--port 19091] [--window 10m] [--threshold 90] [--debug]

Run port-forward in another terminal first:
  oc -n openshift-monitoring port-forward svc/thanos-querier 19091:9091
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
need oc; need jq; need curl

oc whoami >/dev/null 2>&1 || { echo "ERROR: oc not logged in (oc whoami failed)."; exit 1; }
TOKEN="$(oc whoami -t 2>/dev/null || true)"
[[ -n "${TOKEN:-}" ]] || { echo "ERROR: oc whoami -t returned empty token"; exit 1; }

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

# Monitoring query: use POST (per Red Hat examples)
mon_query() {
  local q="$1"
  curl --http1.1 -ksS \
    -H "Authorization: Bearer ${TOKEN}" \
    "https://127.0.0.1:${MON_PORT}/api/v1/query" \
    --data-urlencode "query=${q}"
}

# Validate monitoring connectivity
if [[ "$DEBUG" -eq 1 ]]; then
  echo "DEBUG: testing monitoring via https://127.0.0.1:${MON_PORT}/api/v1/query (POST) ..." >&2
fi

UP_JSON="$(mon_query "up" 2>/dev/null || true)"
if ! jq -e '.status=="success"' >/dev/null 2>&1 <<<"$UP_JSON"; then
  cat >&2 <<EOF
ERROR: Monitoring query failed (expected JSON status=success). Raw response:
$UP_JSON

Make sure port-forward is running:
  oc -n openshift-monitoring port-forward svc/thanos-querier ${MON_PORT}:9091

Manual test:
  TOKEN=\$(oc whoami -t)
  curl --http1.1 -ksS -H "Authorization: Bearer \$TOKEN" \\
    "https://127.0.0.1:${MON_PORT}/api/v1/query" \\
    --data-urlencode "query=up" | head
EOF
  exit 1
fi

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

VMIS_JSON="$TMPDIR/vmis.json"
WORKERS_JSON="$TMPDIR/workers.json"
CPU_JSON="$TMPDIR/cpu.json"
IOW_JSON="$TMPDIR/iow.json"
DLY_JSON="$TMPDIR/dly.json"

oc get nodes -l node-role.kubernetes.io/worker -o json > "$WORKERS_JSON" 2>/dev/null || true
oc get "${VMI_RES}" -A -o json > "$VMIS_JSON"

Q_CPU="sum by (namespace, name) (rate(kubevirt_vmi_cpu_usage_seconds_total[${WINDOW}]))"
Q_IOW="sum by (namespace, name) (rate(kubevirt_vmi_vcpu_wait_seconds_total[${WINDOW}]))"
Q_DLY="sum by (namespace, name) (irate(kubevirt_vmi_vcpu_delay_seconds_total[${WINDOW}]))"

mon_query "$Q_CPU" > "$CPU_JSON"
mon_query "$Q_IOW" > "$IOW_JSON" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}' > "$IOW_JSON"
mon_query "$Q_DLY" > "$DLY_JSON" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}' > "$DLY_JSON"

jq -e '.status=="success"' >/dev/null 2>&1 "$CPU_JSON" || {
  echo "ERROR: CPU query did not return success. Raw:" >&2
  cat "$CPU_JSON" >&2
  exit 1
}

CPU_SERIES="$(jq '.data.result|length' "$CPU_JSON")"
if [[ "$DEBUG" -eq 1 ]]; then
  echo "DEBUG: VMI resource: ${VMI_RES}" >&2
  echo "DEBUG: cpu series returned: ${CPU_SERIES}" >&2
fi

if [[ "$CPU_SERIES" -eq 0 ]]; then
  cat >&2 <<EOF
NOTE: CPU metric query returned 0 series.
This may mean the kubevirt metric name differs or isn't scraped.

Discover likely metric names:
  TOKEN=\$(oc whoami -t)
  curl --http1.1 -ksS -H "Authorization: Bearer \$TOKEN" \\
    "https://127.0.0.1:${MON_PORT}/api/v1/label/__name__/values" \\
  | jq -r '.data[]' | egrep -i 'kubevirt.*vmi.*cpu|virt.*cpu' | head -n 50
EOF
fi

jq -r --slurpfile vmis "$VMIS_JSON" \
      --slurpfile cpu "$CPU_JSON" \
      --slurpfile iow "$IOW_JSON" \
      --slurpfile dly "$DLY_JSON" \
      --slurpfile workers "$WORKERS_JSON" \
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
