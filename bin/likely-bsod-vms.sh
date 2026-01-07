#!/usr/bin/env bash
set -euo pipefail

THRESH_PCT="${THRESH_PCT:-90}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }; }
need oc
need jq

oc whoami >/dev/null 2>&1 || { echo "Not logged in: oc login ..." >&2; exit 1; }

# Get worker nodes set
WORKERS_SET_JSON="$(oc get nodes -l node-role.kubernetes.io/worker -o json | jq -c '
  reduce .items[] as $n ({}; . + {($n.metadata.name): true})
')"

# VMIs and their node
VMI_JSON="$(oc get vmi -A -o json)"

# Build list of VMIs with their virt-launcher pod (ownerRef/labels vary, so we search by label kubevirt.io/domain)
# kubevirt.io/domain is typically the VMI name.
jq -r --argjson workers "$WORKERS_SET_JSON" '
  .items[]
  | select((.status.phase // "")=="Running")
  | .metadata.namespace as $ns
  | .metadata.name as $vmi
  | (.status.nodeName // "") as $node
  | select($node != "" and ($workers[$node] // false))
  | @tsv "\($ns)\t\($vmi)\t\($node)"
' <<<"$VMI_JSON" | while IFS=$'\t' read -r ns vmi node; do
  # Find virt-launcher pod for this VMI
  pod="$(oc -n "$ns" get pod -l kubevirt.io=virt-launcher,kubevirt.io/domain="$vmi" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  [[ -z "$pod" ]] && continue

  # Pull live CPU usage from metrics (if available)
  # Output looks like: NAME CPU(cores) MEMORY(bytes)
  top="$(oc -n "$ns" adm top pod "$pod" --no-headers 2>/dev/null || true)"
  [[ -z "$top" ]] && continue

  cpu_cores="$(awk '{print $2}' <<<"$top")"  # e.g. 250m or 1
  # Normalize to millicores number
  if [[ "$cpu_cores" == *m ]]; then
    cpu_m="${cpu_cores%m}"
  else
    # whole cores -> *1000
    cpu_m="$(awk -v c="$cpu_cores" 'BEGIN{printf "%.0f", c*1000}')"
  fi

  # Get vCPU count best-effort (cores*sockets*threads)
  vcpus="$(oc -n "$ns" get vmi "$vmi" -o json | jq '
    ((.spec.domain.cpu.cores // 1) * (.spec.domain.cpu.sockets // 1) * (.spec.domain.cpu.threads // 1))
  ')"

  # Threshold in millicores = vcpus * 1000 * THRESH_PCT/100
  thresh_m="$(awk -v v="$vcpus" -v p="$THRESH_PCT" 'BEGIN{printf "%.0f", v*1000*p/100}')"

  if (( cpu_m >= thresh_m )); then
    printf "NODE=%s  NS=%s  VMI=%s  POD=%s  vCPU=%s  CPU=%sm (>= %sm)\n" \
      "$node" "$ns" "$vmi" "$pod" "$vcpus" "$cpu_m" "$thresh_m"
  fi
done
