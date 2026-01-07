#!/usr/bin/env bash
set -euo pipefail

# likely-bsod-vms.sh (node-grouped)
#
# Heuristic: VMIs that are Running but consume sustained high CPU (>= THRESH_PCT of vCPU) over WINDOW
# across the whole cluster, grouped by worker node they run on.
#
# Requires: oc, jq
#
# Defaults:
#   WINDOW=5m
#   THRESH_PCT=90
#
# Usage:
#   ./likely-bsod-vms.sh
#   WINDOW=10m THRESH_PCT=95 ./likely-bsod-vms.sh
#   ./likely-bsod-vms.sh --window 15m --threshold 85

WINDOW="${WINDOW:-5m}"
THRESH_PCT="${THRESH_PCT:-90}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --window) WINDOW="$2"; shift 2;;
    --threshold) THRESH_PCT="$2"; shift 2;;
    -h|--help)
      echo "Usage: $0 [--window 5m] [--threshold 90]"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }; }
need oc
need jq

if ! oc whoami >/dev/null 2>&1; then
  cat >&2 <<'EOF'
ERROR: Not logged in to the OpenShift API from this shell.

Try:
  oc login https://<api-server>:6443
or ensure your kubeconfig is set:
  export KUBECONFIG=~/.kube/config
Then verify:
  oc whoami
EOF
  exit 1
fi

# Prometheus proxy endpoint (OpenShift default monitoring)
PROM_PROXY_BASE='/api/v1/namespaces/openshift-monitoring/services/https:prometheus-k8s:9091/proxy/api/v1/query'

uri_encode() { jq -rn --arg v "$1" '$v|@uri'; }

prom_query() {
  local q="$1"
  local enc; enc="$(uri_encode "$q")"
  oc -n openshift-monitoring get --raw "${PROM_PROXY_BASE}?query=${enc}"
}

# 1) Get worker nodes (label-based)
# OCP typically labels workers with node-role.kubernetes.io/worker=""
WORKERS_JSON="$(oc get nodes -l node-role.kubernetes.io/worker -o json)"
WORKERS_LIST="$(jq -r '.items[].metadata.name' <<<"$WORKERS_JSON" | sort)"

if [[ -z "${WORKERS_LIST}" ]]; then
  echo "WARN: No worker nodes found via label node-role.kubernetes.io/worker." >&2
  echo "      Falling back to all schedulable nodes." >&2
  WORKERS_LIST="$(oc get nodes -o json | jq -r '.items[]
    | select((.spec.unschedulable // false) == false)
    | .metadata.name' | sort)"
fi

# Make a fast set for membership checks
WORKERS_SET_JSON="$(jq -Rn --arg s "$WORKERS_LIST" '
  ($s | split("\n") | map(select(length>0)) ) as $a
  | reduce $a[] as $n ({}; . + {($n): true})
')"

# 2) Fetch all VMIs cluster-wide and build lookup info incl nodeName and vCPU count
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

# 3) PromQL queries (cluster-wide)
Q_CPU="sum by (namespace, name) (rate(kubevirt_vmi_cpu_usage_seconds_total[${WINDOW}]))"
Q_IOWAIT="sum by (namespace, name) (rate(kubevirt_vmi_vcpu_wait_seconds_total[${WINDOW}]))"
Q_DELAY="sum by (namespace, name) (irate(kubevirt_vmi_vcpu_delay_seconds_total[${WINDOW}]))"

CPU_JSON="$(prom_query "$Q_CPU")"
IOWAIT_JSON="$(prom_query "$Q_IOWAIT" 2>/dev/null || true)"
DELAY_JSON="$(prom_query "$Q_DELAY" 2>/dev/null || true)"

# Convert metric result arrays into lookup maps keyed by "namespace/name"
to_map='
  .data.result
  | reduce .[] as $r ({}; . + { (($r.metric.namespace + "/" + $r.metric.name)): ($r.value[1]|tonumber) })
'

CPU_MAP="$(jq -c "$to_map" <<<"$CPU_JSON")"
IOWAIT_MAP="$(jq -c "$to_map" <<<"${IOWAIT_JSON:-{"data":{"result":[]}}}")" || IOWAIT_MAP='{}'
DELAY_MAP="$(jq -c "$to_map" <<<"${DELAY_JSON:-{"data":{"result":[]}}}")" || DELAY_MAP='{}'

# 4) Build candidate list, keep only worker-node hosted VMIs, group by node, and print
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
  | map(select(.phase == "" or .phase == "Running"))
  | map(select(.node != "" and ($workers[.node] // false)))
  | map(select(.cpuPct >= ($thresh|tonumber)))
  | sort_by(.node, -.cpuPct)
  | group_by(.node)
  | map({node: .[0].node, items: .})
' <<<"{}")"

# Print
echo "Window: ${WINDOW} | Threshold: >= ${THRESH_PCT}% of vCPU"
echo

# If no results, say so clearly
if [[ "$(jq 'length' <<<"$RESULTS_JSON")" -eq 0 ]]; then
  echo "No VMIs matched the heuristic (high sustained CPU on worker nodes)."
  exit 0
fi

jq -r '
  .[] |
  "NODE: \(.node)\n" +
  "NAMESPACE\tVMI\tVM(owner)\tvCPU\tCPU(cores)\tCPU(%)\tIOwait\tDelay\n" +
  ( .items[] |
    [
      .ns,
      .vmi,
      (.vmOwner // ""),
      (.vcpus|tostring),
      (.cpuCores|tostring),
      (.cpuPct|tostring),
      (.ioWait|tostring),
      (.delay|tostring)
    ] | @tsv
  ) + "\n"
' <<<"$RESULTS_JSON" | while IFS= read -r line; do
  # A tiny formatter: convert tabs in header/data to aligned columns using column if available
  if [[ "$line" == NODE:* ]]; then
    echo "$line"
  else
    # accumulate until blank line then print as a table
    buf=""
    buf+="$line"$'\n'
    while IFS= read -r l; do
      [[ -z "$l" ]] && break
      buf+="$l"$'\n'
    done
    if command -v column >/dev/null 2>&1; then
      printf "%s" "$buf" | column -t -s $'\t'
    else
      printf "%s" "$buf"
    fi
    echo
  fi
done
