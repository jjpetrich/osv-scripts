#!/usr/bin/env bash
set -euo pipefail

# likely-bsod-vms.sh
# Heuristic: VMIs with sustained high CPU usage (>= THRESH_PCT% of vCPU) over WINDOW.
# Cluster-wide; includes NODE so you can group by worker node.
#
# Monitoring access: port-forward to openshift-monitoring svc/thanos-querier:9091 (fallback prometheus-k8s:9091)
# Query endpoint is HTTPS and typically requires a Bearer token.
#
# Requires: oc, jq, curl

WINDOW="${WINDOW:-10m}"
THRESH_PCT="${THRESH_PCT:-90}"
DEBUG=0

usage() {
  cat <<EOF
Usage: $0 [--window 10m] [--threshold 90] [--debug]

Env:
  WINDOW=10m
  THRESH_PCT=90
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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
  echo "ERROR: Not logged in. Run: oc login --web" >&2
  exit 1
fi

TOKEN="$(oc whoami -t 2>/dev/null || true)"
if [[ -z "${TOKEN:-}" ]]; then
  echo "ERROR: Could not get token (oc whoami -t). Are you logged in with a token-capable auth?" >&2
  exit 1
fi

# -------------------------
# Detect VMI resource
# -------------------------
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
  cat >&2 <<'EOF'
ERROR: No VirtualMachineInstance API resource found.
Run:
  oc api-resources | egrep -i 'virtualmachineinstances|vmi|kubevirt'
EOF
  exit 1
fi

# -------------------------
# Port-forward helpers
# -------------------------
PF_PID=""
PF_LOCAL_PORT=""

cleanup_pf() {
  [[ -n "${PF_PID:-}" ]] && kill "${PF_PID}" >/dev/null 2>&1 || true
}
trap cleanup_pf EXIT

pick_free_port() {
  local p i
  for i in {1..50}; do
    p=$(( (RANDOM % 15000) + 20000 ))
    # If nothing is listening, curl will fail quickly; treat as "free enough"
    if ! curl -ksS "https://127.0.0.1:${p}/" >/dev/null 2>&1; then
      echo "$p"
      return 0
    fi
  done
  echo 29091
}

# Query over HTTPS with bearer token (OCP monitoring commonly requires this)
mon_get() {
  local path="$1"
  curl -ksS -H "Authorization: Bearer ${TOKEN}" "https://127.0.0.1:${PF_LOCAL_PORT}${path}"
}

start_port_forward() {
  local svc="$1" rport="$2"

  PF_LOCAL_PORT="$(pick_free_port)"

  [[ "$DEBUG" -eq 1 ]] && echo "DEBUG: port-forward svc/${svc} ${PF_LOCAL_PORT}:${rport}" >&2

  # Start port-forward in background
  oc -n openshift-monitoring port-forward "svc/${svc}" "${PF_LOCAL_PORT}:${rport}" >/dev/null 2>&1 &
  PF_PID="$!"

  # Wait until it responds to a simple Prom query
  local i out
  for i in {1..60}; do
    out="$(mon_get "/api/v1/query?query=up" 2>/dev/null || true)"
    if jq -e '.status=="success"' >/dev/null 2>&1 <<<"$out"; then
      return 0
    fi
    sleep 0.25
  done

  # Failed: stop and clear
  kill "${PF_PID}" >/dev/null 2>&1 || true
  PF_PID=""
  PF_LOCAL_PORT=""
  return 1
}

# Prefer thanos, fall back to prometheus
MON_SVC=""
if start_port_forward "thanos-querier" "9091"; then
  MON_SVC="thanos-querier"
elif start_port_forward "prometheus-k8s" "9091"; then
  MON_SVC="prometheus-k8s"
else
  cat >&2 <<'EOF'
ERROR: Could not query monitoring via port-forward.

Manual test (keep port-forward running in one terminal, curl in another):
  oc -n openshift-monitoring port-forward svc/thanos-querier 19091:9091
  TOKEN="$(oc whoami -t)"
  curl -ksS -H "Authorization: Bearer $TOKEN" \
    "https://127.0.0.1:19091/api/v1/query?query=up" | head
EOF
  exit 1
fi

prom_query() {
  local q="$1"
  local enc
  enc="$(jq -rn --arg v "$q" '$v|@uri')"
  mon_get "/api/v1/query?query=${enc}"
}

# -------------------------
# Inventory: workers + VMIs
# -------------------------
WORKERS_JSON="$(oc get nodes -l node-role.kubernetes.io/worker -o json)"
WORKERS_LIST="$(jq -r '.items[].metadata.name' <<<"$WORKERS_JSON" | sort)"
if [[ -z "${WORKERS_LIST}" ]]; then
  echo "WARN: No worker nodes found via label; using all schedulable nodes." >&2
  WORKERS_LIST="$(oc get nodes -o json | jq -r '.items[] | select((.spec.unschedulable // false)==false) | .metadata.name' | sort)"
fi
WORKERS_SET_JSON="$(jq -Rn --arg s "$WORKERS_LIST" '
  ($s | split("\n") | map(select(length>0))) as $a
  | reduce $a[] as $n ({}; . + {($n): true})
')"

VMI_JSON="$(oc get "${VMI_RES}" -A -o json)"

LOOKUP_JSON="$(jq -c '
  def vcpu_count:
    ((.spec.domain.cpu.cores   // 1)
    *(.spec.domain.cpu.sockets // 1)
    *(.spec.domain.cpu.threads // 1));

  reduce .items[] as $i ({}; . + {
    (($i.metadata.namespace + "/" + $i.metadata.name)):
    {
      vcpus: ( ($i | vcpu_count) | tonumber ),
      phase: ($i.status.phase // ""),
      node: ($i.status.nodeName // ""),
      vmOwner: (
        ($i.metadata.ownerReferences // [])
        | map(select(.kind=="VirtualMachine"))[0].name // ""
      )
    }
  })
' <<<"$VMI_JSON")"

# -------------------------
# Metrics (CPU heuristic)
# -------------------------
Q_CPU="sum by (namespace, name) (rate(kubevirt_vmi_cpu_usage_seconds_total[${WINDOW}]))"
Q_IOWAIT="sum by (namespace, name) (rate(kubevirt_vmi_vcpu_wait_seconds_total[${WINDOW}]))"
Q_DELAY="sum by (namespace, name) (irate(kubevirt_vmi_vcpu_delay_seconds_total[${WINDOW}]))"

CPU_JSON="$(prom_query "$Q_CPU" || true)"
if ! jq -e '.status=="success"' >/dev/null 2>&1 <<<"$CPU_JSON"; then
  echo "ERROR: CPU query did not return success. Raw response:" >&2
  echo "$CPU_JSON" >&2
  exit 1
fi

IOWAIT_JSON="$(prom_query "$Q_IOWAIT" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}')"
DELAY_JSON="$(prom_query "$Q_DELAY" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}')"

# If CPU metric exists but returns 0 series, tell you plainly (then we can switch metrics)
CPU_SERIES_COUNT="$(jq '.data.result|length' <<<"$CPU_JSON")"
if [[ "$CPU_SERIES_COUNT" -eq 0 ]]; then
  echo "NOTE: CPU metric query returned 0 series." >&2
  echo "      Either no VMIs are being scraped, or metric name differs in your cluster." >&2
  echo "      Quick discovery (prints matching metric names):" >&2
  echo "      curl -ksS -H \"Authorization: Bearer \$(oc whoami -t)\" \\" >&2
  echo "        \"https://127.0.0.1:${PF_LOCAL_PORT}/api/v1/label/__name__/values\" \\" >&2
  echo "        | jq -r '.data[]' | egrep -i 'kubevirt.*vmi.*cpu|virt.*cpu' | head -n 50" >&2
fi

to_map='
  .data.result
  | reduce .[] as $r ({}; . + { (($r.metric.namespace + "/" + $r.metric.name)): ($r.value[1]|tonumber) })
'
CPU_MAP="$(jq -c "$to_map" <<<"$CPU_JSON")"
IOWAIT_MAP="$(jq -c "$to_map" <<<"$IOWAIT_JSON" 2>/dev/null || echo '{}')"
DELAY_MAP="$(jq -c "$to_map" <<<"$DELAY_JSON" 2>/dev/null || echo '{}')"

RESULTS_JSON="$(jq -c --argjson lookup "$LOOKUP_JSON" \
                      --argjson cpuMap "$CPU_MAP" \
                      --argjson ioMap "$IOWAIT_MAP" \
                      --argjson dlyMap "$DELAY_MAP" \
                      --argjson workers "$WORKERS_SET_JSON" \
                      --arg thresh "$THRESH_PCT" '
  def pct(a;b): if b <= 0 then 0 else (a / b * 100) end;

  $cpuMap
  | to_entries
  | map(. as $e
    | $e.key as $k
    | ($lookup[$k] // {vcpus:1, phase:"", node:"", vmOwner:""}) as $li
    | ($li.vcpus|tonumber) as $vcpus
    | ($e.value|tonumber) as $cpuCores
    | (pct($cpuCores; $vcpus)) as $cpuPct
    | {
        ns: ($k|split("/")[0]),
        vmi: ($k|split("/")[1]),
        vmOwner: ($li.vmOwner // ""),
        phase: ($li.phase // ""),
        node: ($li.node // ""),
        vcpus: $vcpus,
        cpuCores: $cpuCores,
        cpuPct: $cpuPct,
        ioWait: ($ioMap[$k] // 0),
        delay: ($dlyMap[$k] // 0)
      }
  )
  | map(select(.phase == "" or .phase == "Running"))
  | map(select(.node != "" and ($workers[.node] // false)))
  | map(select(.cpuPct >= ($thresh|tonumber)))
  | sort_by(.node, -.cpuPct, .ns, .vmi)
')"

# -------------------------
# Output
# -------------------------
echo "Monitoring mode: port-forward svc/${MON_SVC} (https://127.0.0.1:${PF_LOCAL_PORT})"
echo "VMI resource: ${VMI_RES}"
echo "Window: ${WINDOW} | Threshold: >= ${THRESH_PCT}% of vCPU"
echo

if [[ "$DEBUG" -eq 1 ]]; then
  echo "DEBUG: worker nodes: $(wc -l <<<"$WORKERS_LIST" | awk '{print $1}')"
  echo "DEBUG: vmis total: $(jq '.items|length' <<<"$VMI_JSON")"
  echo "DEBUG: cpu series returned: $(jq '.data.result|length' <<<"$CPU_JSON")"
  echo
fi

if [[ "$(jq 'length' <<<"$RESULTS_JSON")" -eq 0 ]]; then
  echo "No VMIs matched the heuristic (high sustained CPU on worker nodes)."
  exit 0
fi

OUT_TSV="$(
  jq -r '
    ["NODE","NAMESPACE","VMI","VM(owner)","vCPU","CPU(cores)","CPU(%)","IOwait","Delay"] | @tsv,
    (.[] | [
      .node, .ns, .vmi, (.vmOwner // ""),
      (.vcpus|tostring),
      (.cpuCores|tostring),
      (.cpuPct|tostring),
      (.ioWait|tostring),
      (.delay|tostring)
    ] | @tsv)
  ' <<<"$RESULTS_JSON"
)"

if command -v column >/dev/null 2>&1; then
  printf "%s\n" "$OUT_TSV" | column -t -s $'\t'
else
  printf "%s\n" "$OUT_TSV"
fi
