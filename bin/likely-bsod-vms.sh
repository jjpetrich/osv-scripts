#!/usr/bin/env bash
set -euo pipefail

WINDOW="${WINDOW:-5m}"
THRESH_PCT="${THRESH_PCT:-90}"
DEBUG=0

usage() {
  cat <<EOF
Usage: $0 [--window 5m] [--threshold 90] [--debug]
Env: WINDOW, THRESH_PCT
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
need python3

if ! oc whoami >/dev/null 2>&1; then
  echo "ERROR: Not logged in. Run: oc login --web" >&2
  exit 1
fi

# -------------------------
# Detect VMI resource name
# -------------------------
detect_vmi_resource() {
  local candidates=("vmi" "virtualmachineinstances" "virtualmachineinstance")
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
ERROR: Could not find a VirtualMachineInstance (VMI) API resource in this cluster.

Run:
  oc api-resources | egrep -i 'kubevirt|virtualmachineinstance|virtualmachine\b|vmi\b'
  oc get virtualmachineinstances -A

If OpenShift Virtualization/KubeVirt is not installed on this cluster, there is no VMI to query.
EOF
  exit 1
fi

[[ "$DEBUG" -eq 1 ]] && echo "DEBUG: using VMI resource: ${VMI_RES}" >&2

# -------------------------
# Monitoring backend (proxy or port-forward)
# -------------------------
uri_encode() { jq -rn --arg v "$1" '$v|@uri'; }

PROXY_BASES=(
  "/api/v1/namespaces/openshift-monitoring/services/https:thanos-querier:9091/proxy"
  "/api/v1/namespaces/openshift-monitoring/services/thanos-querier:9091/proxy"
  "/api/v1/namespaces/openshift-monitoring/services/https:prometheus-k8s:9091/proxy"
  "/api/v1/namespaces/openshift-monitoring/services/prometheus-k8s:9091/proxy"
)

proxy_runtimeinfo_ok() {
  local base="$1"
  local out
  if out="$(oc -n openshift-monitoring get --raw "${base}/api/v1/status/runtimeinfo" 2>/dev/null)"; then
    jq -e '.status=="success"' >/dev/null 2>&1 <<<"$out"
  else
    return 1
  fi
}

pick_proxy_base() {
  local base
  for base in "${PROXY_BASES[@]}"; do
    if proxy_runtimeinfo_ok "$base"; then
      echo "$base"
      return 0
    fi
  done
  return 1
}

PROM_MODE=""   # "proxy" or "pf"
PROXY_BASE=""
PF_PID=""
PF_LOCAL_PORT=""

cleanup_pf() { [[ -n "${PF_PID:-}" ]] && kill "${PF_PID}" >/dev/null 2>&1 || true; }
trap cleanup_pf EXIT

start_port_forward() {
  local svc="$1"
  local rport="$2"
  PF_LOCAL_PORT="$(python3 - <<'PY'
import random
print(random.randint(20000, 40000))
PY
)"
  [[ "$DEBUG" -eq 1 ]] && echo "DEBUG: port-forward svc/${svc} ${PF_LOCAL_PORT}:${rport}" >&2

  oc -n openshift-monitoring port-forward "svc/${svc}" "${PF_LOCAL_PORT}:${rport}" >/dev/null 2>&1 &
  PF_PID="$!"

  local i
  for i in {1..25}; do
    if curl -fsS "http://127.0.0.1:${PF_LOCAL_PORT}/api/v1/status/runtimeinfo" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done

  kill "${PF_PID}" >/dev/null 2>&1 || true
  PF_PID=""
  PF_LOCAL_PORT=""
  return 1
}

if PROXY_BASE="$(pick_proxy_base || true)"; then
  PROM_MODE="proxy"
else
  if start_port_forward "thanos-querier" "9091"; then
    PROM_MODE="pf"
  elif start_port_forward "prometheus-k8s" "9091"; then
    PROM_MODE="pf"
  else
    cat >&2 <<'EOF'
ERROR: Could not access monitoring via service proxy OR via port-forward.

Proxy tests:
  oc -n openshift-monitoring get --raw \
    /api/v1/namespaces/openshift-monitoring/services/thanos-querier:9091/proxy/api/v1/status/runtimeinfo
  oc -n openshift-monitoring get --raw \
    /api/v1/namespaces/openshift-monitoring/services/prometheus-k8s:9091/proxy/api/v1/status/runtimeinfo

Port-forward test:
  oc -n openshift-monitoring port-forward svc/thanos-querier 19091:9091
  curl -s http://127.0.0.1:19091/api/v1/status/runtimeinfo | head
EOF
    exit 1
  fi
fi

prom_query() {
  local q="$1"
  local enc; enc="$(uri_encode "$q")"
  if [[ "$PROM_MODE" == "proxy" ]]; then
    oc -n openshift-monitoring get --raw "${PROXY_BASE}/api/v1/query?query=${enc}"
  else
    curl -fsS "http://127.0.0.1:${PF_LOCAL_PORT}/api/v1/query?query=${enc}"
  fi
}

# -------------------------
# Cluster inventory
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
# Metrics queries
# -------------------------
Q_CPU="sum by (namespace, name) (rate(kubevirt_vmi_cpu_usage_seconds_total[${WINDOW}]))"
Q_IOWAIT="sum by (namespace, name) (rate(kubevirt_vmi_vcpu_wait_seconds_total[${WINDOW}]))"
Q_DELAY="sum by (namespace, name) (irate(kubevirt_vmi_vcpu_delay_seconds_total[${WINDOW}]))"

CPU_JSON="$(prom_query "$Q_CPU")"
IOWAIT_JSON="$(prom_query "$Q_IOWAIT" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}')"
DELAY_JSON="$(prom_query "$Q_DELAY" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}')"

if ! jq -e '.status=="success"' >/dev/null 2>&1 <<<"$CPU_JSON"; then
  echo "ERROR: CPU monitoring query failed. Raw response:" >&2
  echo "$CPU_JSON" >&2
  exit 1
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
if [[ "$PROM_MODE" == "proxy" ]]; then
  echo "Monitoring mode: service-proxy (${PROXY_BASE})"
else
  echo "Monitoring mode: port-forward (localhost:${PF_LOCAL_PORT})"
fi
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
