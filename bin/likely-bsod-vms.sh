#!/usr/bin/env bash
set -euo pipefail

# likely-bsod-vms.sh
#
# Heuristic detector for "possibly blue-screened / wedged" Windows VMIs on OpenShift Virtualization.
# Cluster-wide, grouped by worker node (NODE column).
#
# Uses OpenShift default monitoring (Thanos/Prometheus) via API service proxy:
#   openshift-monitoring/{thanos-querier|prometheus-k8s}
#
# Flags VMIs with sustained CPU usage >= THRESH_PCT% of their vCPU capacity over WINDOW.
#
# Requires: oc, jq

WINDOW="${WINDOW:-5m}"
THRESH_PCT="${THRESH_PCT:-90}"
DEBUG=0

usage() {
  cat <<EOF
Usage: $0 [--window 5m] [--threshold 90] [--debug]

Environment variables:
  WINDOW=5m
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

if ! oc whoami >/dev/null 2>&1; then
  cat >&2 <<'EOF'
ERROR: Not logged in to the OpenShift API from this shell.

Try:
  oc login --web
or:
  oc login https://<api-server>:6443
EOF
  exit 1
fi

# ---- Monitoring proxy detection ----

uri_encode() { jq -rn --arg v "$1" '$v|@uri'; }

# Candidate monitoring proxy bases (OCP varies). Prefer Thanos for "cluster view".
CANDIDATES=(
  "/api/v1/namespaces/openshift-monitoring/services/https:thanos-querier:9091/proxy"
  "/api/v1/namespaces/openshift-monitoring/services/https:prometheus-k8s:9091/proxy"
)

pick_monitoring_base() {
  local base out
  for base in "${CANDIDATES[@]}"; do
    if out="$(oc -n openshift-monitoring get --raw "${base}/api/v1/status/runtimeinfo" 2>/dev/null)"; then
      if jq -e '.status=="success"' >/dev/null 2>&1 <<<"$out"; then
        echo "$base"
        return 0
      fi
    fi
  done
  return 1
}

MON_BASE="$(pick_monitoring_base || true)"
if [[ -z "${MON_BASE:-}" ]]; then
  cat >&2 <<'EOF'
ERROR: Could not access a monitoring query endpoint via openshift-monitoring service proxy.

This is almost always RBAC to services/proxy in openshift-monitoring, or a blocked monitoring proxy.

Diagnose:
  oc auth can-i get services/proxy -n openshift-monitoring
  oc -n openshift-monitoring get svc | egrep 'prometheus-k8s|thanos|alertmanager'

Test manually (pick one that exists):
  oc -n openshift-monitoring get --raw \
    /api/v1/namespaces/openshift-monitoring/services/https:thanos-querier:9091/proxy/api/v1/status/runtimeinfo
  oc -n openshift-monitoring get --raw \
    /api/v1/namespaces/openshift-monitoring/services/https:prometheus-k8s:9091/proxy/api/v1/status/runtimeinfo
EOF
  exit 1
fi

prom_query() {
  local q="$1"
  local enc; enc="$(uri_encode "$q")"
  oc -n openshift-monitoring get --raw "${MON_BASE}/api/v1/query?query=${enc}"
}

# ---- Gather cluster inventory ----

# Worker nodes set
WORKERS_JSON="$(oc get nodes -l node-role.kubernetes.io/worker -o json)"
WORKERS_LIST="$(jq -r '.items[].metadata.name' <<<"$WORKERS_JSON" | sort)"
if [[ -z "${WORKERS_LIST}" ]]; then
  echo "WARN: No worker nodes found via label node-role.kubernetes.io/worker; using all schedulable nodes." >&2
  WORKERS_LIST="$(oc get nodes -o json | jq -r '.items[]
    | select((.spec.unschedulable // false) == false)
    | .metadata.name' | sort)"
fi
WORKERS_SET_JSON="$(jq -Rn --arg s "$WORKERS_LIST" '
  ($s | split("\n") | map(select(length>0))) as $a
  | reduce $a[] as $n ({}; . + {($n): true})
')"

# VMIs lookup: vCPU count, phase, node, owning VM (if any)
VMI_JSON="$(oc get vmi -A -o json)"

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

# ---- Query metrics (KubeVirt / OpenShift Virtualization) ----

# CPU usage in "cores" (seconds/second) per VMI
Q_CPU="sum by (namespace, name) (rate(kubevirt_vmi_cpu_usage_seconds_total[${WINDOW}]))"

# Optional context metrics (these may be absent depending on your config/version)
Q_IOWAIT="sum by (namespace, name) (rate(kubevirt_vmi_vcpu_wait_seconds_total[${WINDOW}]))"
Q_DELAY="sum by (namespace, name) (irate(kubevirt_vmi_vcpu_delay_seconds_total[${WINDOW}]))"

CPU_JSON="$(prom_query "$Q_CPU")"

# Best effort for optional metrics; don’t fail if missing
IOWAIT_JSON="$(prom_query "$Q_IOWAIT" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}')"
DELAY_JSON="$(prom_query "$Q_DELAY" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}')"

# Validate Prometheus response for CPU
if ! jq -e '.status=="success"' >/dev/null 2>&1 <<<"$CPU_JSON"; then
  echo "ERROR: Monitoring query failed for CPU metric. Raw response:" >&2
  echo "$CPU_JSON" >&2
  exit 1
fi

# Convert result arrays into maps keyed by "namespace/name"
to_map='
  .data.result
  | reduce .[] as $r ({}; . + { (($r.metric.namespace + "/" + $r.metric.name)): ($r.value[1]|tonumber) })
'

CPU_MAP="$(jq -c "$to_map" <<<"$CPU_JSON")"
IOWAIT_MAP="$(jq -c "$to_map" <<<"$IOWAIT_JSON" 2>/dev/null || echo '{}')"
DELAY_MAP="$(jq -c "$to_map" <<<"$DELAY_JSON" 2>/dev/null || echo '{}')"

# ---- Build candidates and output ----

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
        key: $k,
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
  # focus on Running VMIs on worker nodes
  | map(select(.phase == "" or .phase == "Running"))
  | map(select(.node != "" and ($workers[.node] // false)))
  # apply heuristic
  | map(select(.cpuPct >= ($thresh|tonumber)))
  | sort_by(.node, -.cpuPct, .ns, .vmi)
  | group_by(.node)
  | map({node: .[0].node, items: .})
')"

echo "Monitoring endpoint: ${MON_BASE}"
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

# Output a single TSV table (safe usage of @tsv: only arrays)
OUT_TSV="$(
  jq -r '
    ["NODE","NAMESPACE","VMI","VM(owner)","vCPU","CPU(cores)","CPU(%)","IOwait","Delay"] | @tsv,
    (.[] | .node as $node
      | .items[]
      | [
          $node,
          .ns,
          .vmi,
          (.vmOwner // ""),
          (.vcpus|tostring),
          (.cpuCores|tostring),
          (.cpuPct|tostring),
          (.ioWait|tostring),
          (.delay|tostring)
        ] | @tsv
    )
  ' <<<"$RESULTS_JSON"
)"

if command -v column >/dev/null 2>&1; then
  printf "%s\n" "$OUT_TSV" | column -t -s $'\t'
else
  printf "%s\n" "$OUT_TSV"
fi
