#!/usr/bin/env bash
set -euo pipefail

# likely-bsod-vms.sh
#
# Heuristic detector for "possibly blue-screened / wedged" Windows VMIs on OpenShift Virtualization.
# It queries Prometheus via oc (no direct Prometheus login needed).
#
# Requires: oc, jq
#
# Defaults:
#   WINDOW=5m   (Prometheus rate window)
#   THRESH_PCT=90  (% of vCPU capacity used)
#
# Usage examples:
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

oc whoami >/dev/null 2>&1 || { echo "Not logged in. Run: oc login ..." >&2; exit 1; }

PROM_PROXY_BASE='/api/v1/namespaces/openshift-monitoring/services/https:prometheus-k8s:9091/proxy/api/v1/query'

uri_encode() {
  # jq does proper RFC3986-ish encoding via @uri
  jq -rn --arg v "$1" '$v|@uri'
}

prom_query() {
  local q="$1"
  local enc
  enc="$(uri_encode "$q")"
  oc -n openshift-monitoring get --raw "${PROM_PROXY_BASE}?query=${enc}"
}

# Pull all VMIs and compute a vCPU count per VMI (cores*sockets*threads; defaults to 1).
# Also keep owner VM name if present.
VMI_JSON="$(oc get vmi -A -o json)"

# Build a lookup JSON object:
# key = "namespace/name"
# value = { vcpus: <int>, phase: <string>, vmOwner: <string or empty> }
LOOKUP_JSON="$(jq -c '
  def vcpu_count:
    # Best-effort: domain.cpu.*; defaults to 1.
    # (Some setups may use other CPU specs; keep it simple & safe.)
    ((.spec.domain.cpu.cores   // 1)
    *(.spec.domain.cpu.sockets // 1)
    *(.spec.domain.cpu.threads // 1));

  reduce .items[] as $i ({}; . +
    {
      (($i.metadata.namespace + "/" + $i.metadata.name)):
      {
        vcpus: ( ($i | vcpu_count) | tonumber ),
        phase: ($i.status.phase // ""),
        vmOwner: (
          ($i.metadata.ownerReferences // [])
          | map(select(.kind=="VirtualMachine"))[0].name // ""
        )
      }
    }
  )
' <<<"$VMI_JSON")"

# PromQL:
# CPU cores used (rate of total CPU seconds per second) aggregated per VMI
Q_CPU="sum by (namespace, name) (rate(kubevirt_vmi_cpu_usage_seconds_total[${WINDOW}]))"

# Extra context:
# I/O wait (VMs stuck on storage can look "hung" too)
Q_IOWAIT="sum by (namespace, name) (rate(kubevirt_vmi_vcpu_wait_seconds_total[${WINDOW}]))"
# CPU steal/delay (host contention; can explain high apparent CPU issues)
Q_DELAY="sum by (namespace, name) (irate(kubevirt_vmi_vcpu_delay_seconds_total[${WINDOW}]))"

CPU_JSON="$(prom_query "$Q_CPU")"
IOWAIT_JSON="$(prom_query "$Q_IOWAIT")" || IOWAIT_JSON='{"data":{"result":[]}}'
DELAY_JSON="$(prom_query "$Q_DELAY")" || DELAY_JSON='{"data":{"result":[]}}'

# Convert metric result arrays into lookup maps keyed by "namespace/name"
to_map='
  .data.result
  | reduce .[] as $r ({}; . + { (($r.metric.namespace + "/" + $r.metric.name)): ($r.value[1]|tonumber) })
'

CPU_MAP="$(jq -c "$to_map" <<<"$CPU_JSON")"
IOWAIT_MAP="$(jq -c "$to_map" <<<"$IOWAIT_JSON")"
DELAY_MAP="$(jq -c "$to_map" <<<"$DELAY_JSON")"

# Print header
printf "%-35s %-35s %6s %10s %8s %10s %10s\n" "NAMESPACE" "VMI" "vCPU" "CPU(cores)" "CPU(%)" "IOwait" "Delay"
printf "%-35s %-35s %6s %10s %8s %10s %10s\n" "---------" "---" "----" "---------" "------" "------" "-----"

# Iterate over CPU results, join with VMI info, and flag those above threshold.
jq -r --argjson lookup "$LOOKUP_JSON" \
      --argjson cpuMap "$CPU_MAP" \
      --argjson ioMap "$IOWAIT_MAP" \
      --argjson dlyMap "$DELAY_MAP" \
      --arg window "$WINDOW" \
      --arg thresh "$THRESH_PCT" '
  def pct(a;b): if b <= 0 then 0 else (a / b * 100) end;

  $cpuMap
  | to_entries[]
  | .key as $k
  | ($lookup[$k] // {vcpus:1, phase:"", vmOwner:""}) as $li
  | ($li.vcpus | tonumber) as $vcpus
  | (.value | tonumber) as $cpuCores
  | (pct($cpuCores; $vcpus)) as $cpuPct
  | ($ioMap[$k] // 0) as $io
  | ($dlyMap[$k] // 0) as $dly
  | ($k | split("/") ) as $p
  | {
      ns: $p[0],
      name: $p[1],
      vcpus: $vcpus,
      cpuCores: $cpuCores,
      cpuPct: $cpuPct,
      io: $io,
      dly: $dly,
      phase: ($li.phase // ""),
      vmOwner: ($li.vmOwner // "")
    }
  | select(.phase == "" or .phase == "Running")   # keep focus on "still running" instances
  | select(.cpuPct >= ($thresh|tonumber))
  | @tsv "\(.ns)\t\(.name)\t\(.vcpus)\t\(.cpuCores)\t\(.cpuPct)\t\(.io)\t\(.dly)\t\(.vmOwner)"
' | while IFS=$'\t' read -r ns name vcpus cpuCores cpuPct io dly vmOwner; do
    # Pretty print + include owner VM if present
    vmiLabel="$name"
    if [[ -n "$vmOwner" ]]; then
      vmiLabel="${name} (VM:${vmOwner})"
    fi
    printf "%-35s %-35s %6s %10.2f %7.1f%% %10.4f %10.4f\n" \
      "$ns" "$vmiLabel" "$vcpus" "$cpuCores" "$cpuPct" "$io" "$dly"
  done

echo
echo "Flagged VMIs are those with >= ${THRESH_PCT}% vCPU utilization averaged over ${WINDOW}."
echo "Tip: raise WINDOW (e.g., 10m/15m) to reduce false positives from brief CPU spikes."
