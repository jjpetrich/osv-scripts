#!/usr/bin/env bash
set -euo pipefail

#
# v2: VM RAM allocation vs usage (KubeVirt) using oc / oc adm
#
# Improvements vs v1:
#  - MEMORY column detected dynamically from `oc adm top pod` header (no hard-coded $4)
#  - virt-launcher pod -> VM/VMI name resolved via pod labels (prefers kubevirt.io/domain)
#  - doesn't drop VMIs if metrics are missing (shows METRICS=no)
#  - quantity parser supports Ti in addition to Gi/Mi/Ki/etc
#
# Filters:
#   --percent-used <N>     (show only VMs with pct_used <= N)
#   --min-alloc-mem <GiB>  (show only VMs with allocated RAM >= GiB)
#
# Limit results:
#   TOP_N=30 ./vmmem-oc-v2.sh
#

TOP_N="${TOP_N:-}"
PCT_THRESH=""
MIN_ALLOC_GIB=""

print_help() {
  cat <<EOF
Usage: $0 [--percent-used N] [--min-alloc-mem GiB]

Options:
  --percent-used N, -p N
        Only show VMs using <= N percent of allocated RAM.

  --min-alloc-mem GiB, -m GiB
        Only show VMs whose allocated RAM is >= GiB.

  -h, --help
        Show help.

Environment:
  TOP_N=N
        Show only the first N sorted results.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --percent-used|-p) PCT_THRESH="$2"; shift 2 ;;
    --min-alloc-mem|-m) MIN_ALLOC_GIB="$2"; shift 2 ;;
    -h|--help) print_help; exit 0 ;;
    *) echo "ERROR: Unknown argument: $1" >&2; print_help >&2; exit 1 ;;
  esac
done

if [[ -n "$PCT_THRESH" && ! "$PCT_THRESH" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "ERROR: --percent-used requires a numeric value." >&2
  exit 1
fi
if [[ -n "$MIN_ALLOC_GIB" && ! "$MIN_ALLOC_GIB" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "ERROR: --min-alloc-mem requires a numeric GiB value." >&2
  exit 1
fi

require_bin() {
  for b in "$@"; do
    command -v "$b" >/dev/null 2>&1 || {
      echo "ERROR: Required binary '$b' not found." >&2
      exit 1
    }
  done
}

# Kubernetes-ish quantity -> MiB
quantity_to_mi_awk='
function toMi(q, v) {
  if (q == "" || q == "0") return 0;

  if (q ~ /Ti$/) { v = substr(q, 1, length(q)-2); return v * 1024 * 1024; }
  if (q ~ /T$/)  { v = substr(q, 1, length(q)-1); return v * 1024 * 1024; }

  if (q ~ /Gi$/) { v = substr(q, 1, length(q)-2); return v * 1024; }
  if (q ~ /G$/)  { v = substr(q, 1, length(q)-1); return v * 1024; }

  if (q ~ /Mi$/) { v = substr(q, 1, length(q)-2); return v; }
  if (q ~ /M$/)  { v = substr(q, 1, length(q)-1); return v; }

  if (q ~ /Ki$/) { v = substr(q, 1, length(q)-2); return v / 1024; }
  if (q ~ /K$/)  { v = substr(q, 1, length(q)-1); return v / 1024; }

  # assume raw bytes
  return q / 1024 / 1024;
}
'

require_bin oc jq awk sort join column

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

vmi_raw="${tmpdir}/vmi_raw.tsv"
vmi_alloc="${tmpdir}/vmi_alloc.tsv"

podmap_raw="${tmpdir}/podmap_raw.tsv"      # ns \t pod \t domain(label) \t fallbackVm
top_raw="${tmpdir}/top_raw.tsv"            # ns \t pod \t memQty
top_used_by_pod="${tmpdir}/top_used_by_pod.tsv"   # ns|pod \t usedMi
pod_to_vm_used="${tmpdir}/pod_to_vm_used.tsv"     # ns|vm \t usedMi \t hasMetrics(1)

joined="${tmpdir}/joined.tsv"

echo "# Collecting VMI info (allocated memory + node)..." >&2

# namespace, name, node, memQty
oc get vmi -A -o json \
  | jq -r '
      .items[]
      | select(.status.phase=="Running")
      | [
          .metadata.namespace,
          .metadata.name,
          (.status.nodeName // ""),
          (
            .spec.domain.memory.guest
            // .spec.domain.resources.requests.memory
            // "0"
          )
        ]
      | @tsv
    ' > "${vmi_raw}"

# Convert alloc to Mi; key = ns|name
awk -F'\t' "${quantity_to_mi_awk}
  NF >= 4 {
    ns   = \$1
    name = \$2
    node = \$3
    qty  = \$4
    allocMi = toMi(qty)
    key = ns \"|\" name
    printf \"%s\t%.3f\t%s\t%s\t%s\n\", key, allocMi, ns, name, node
  }
" "${vmi_raw}" > "${vmi_alloc}"

echo "# Collecting virt-launcher pod -> VM mapping (labels)..." >&2

# Build map keyed by ns|pod.
# Prefer kubevirt.io/domain label (you observed it's present).
# Keep a fallback heuristic VM name too, just in case.
oc get pod -A -l kubevirt.io=virt-launcher -o json \
  | jq -r '
      .items[]
      | .metadata as $m
      | [
          $m.namespace,
          $m.name,
          ($m.labels["kubevirt.io/domain"] // ""),
          $m.name
        ]
      | @tsv
    ' \
  | awk -F'\t' '
      {
        ns=$1; pod=$2; domain=$3; fb=$4;

        # fallback heuristic: strip virt-launcher- prefix and trailing -<suffix>
        vm=fb
        sub(/^virt-launcher-/, "", vm)
        sub(/-[^-]+$/, "", vm)

        key = ns "|" pod
        print key "\t" domain "\t" vm
      }
    ' > "${podmap_raw}"

echo "# Collecting memory usage from oc adm top pod..." >&2

# Parse top output robustly by discovering column indices from header.
oc adm top pod -A --selector kubevirt.io=virt-launcher 2>/dev/null \
  | awk '
      NR==1 {
        for (i=1; i<=NF; i++) {
          if ($i=="NAMESPACE") nscol=i
          else if ($i=="NAME") namecol=i
          else if ($i ~ /^MEMORY/) memcol=i
        }
        if (!nscol || !namecol || !memcol) {
          print "ERROR: Could not find NAMESPACE/NAME/MEMORY columns in oc adm top output" > "/dev/stderr"
          exit 1
        }
        next
      }
      NF>=1 {
        ns  = $nscol
        pod = $namecol
        mem = $memcol
        if (ns != "" && pod != "" && mem != "") print ns "\t" pod "\t" mem
      }
    ' > "${top_raw}"

# Convert to MiB, keyed by ns|pod (top output is already per-pod)
awk -F'\t' "${quantity_to_mi_awk}
  NF >= 3 {
    ns=\$1; pod=\$2; qty=\$3
    usedMi = toMi(qty)
    key = ns \"|\" pod
    printf \"%s\t%.3f\n\", key, usedMi
  }
" "${top_raw}" > "${top_used_by_pod}"

# Join podmap (ns|pod -> domain, fallbackVm) with top usage (ns|pod -> usedMi)
LC_ALL=C sort -t $'\t' -o "${podmap_raw}" "${podmap_raw}"
LC_ALL=C sort -t $'\t' -o "${top_used_by_pod}" "${top_used_by_pod}"

join -t $'\t' -j 1 "${podmap_raw}" "${top_used_by_pod}" \
  | awk -F'\t' '
      {
        # join output:
        # $1=ns|pod, $2=domain, $3=fallbackVm, $4=usedMi
        split($1, a, "|"); ns=a[1]
        domain=$2
        fbvm=$3
        usedMi=$4

        vm = (domain != "" ? domain : fbvm)
        key = ns "|" vm

        # If somehow multiple pods map to same vm, keep the max usage (safer than sum)
        if (!(key in max) || usedMi > max[key]) max[key]=usedMi
      }
      END {
        for (k in max) {
          printf "%s\t%.3f\t1\n", k, max[k]  # hasMetrics=1
        }
      }
    ' > "${pod_to_vm_used}"

# ---------- join alloc + used (keep alloc even if no metrics) ----------

LC_ALL=C sort -t $'\t' -o "${vmi_alloc}" "${vmi_alloc}"
LC_ALL=C sort -t $'\t' -o "${pod_to_vm_used}" "${pod_to_vm_used}"

# vmi_alloc: key, allocMi, ns, name, node
# pod_to_vm_used: key, usedMi, hasMetrics
join -t $'\t' -a 1 -e 0 -o '1.1 1.2 1.3 1.4 1.5 2.2 2.3' \
  "${vmi_alloc}" "${pod_to_vm_used}" \
  | awk -F'\t' -v pct_thresh="${PCT_THRESH:-}" -v min_alloc="${MIN_ALLOC_GIB:-}" '
      BEGIN { }
      {
        allocMi = $2 + 0
        ns      = $3
        name    = $4
        node    = $5
        usedMi  = $6 + 0
        has     = $7 + 0

        allocGi = allocMi / 1024.0
        usedGi  = usedMi  / 1024.0
        pct     = (allocMi > 0 ? (usedMi / allocMi * 100.0) : 0.0)

        if (min_alloc != "" && allocGi < min_alloc) next
        if (pct_thresh != "" && pct > pct_thresh) next

        metrics = (has > 0 ? "yes" : "no")

        printf "%s\t%s\t%s\t%8.2f\t%8.2f\t%8.2f\t%s\n",
               name, ns, node, allocGi, usedGi, pct, metrics
      }
    ' > "${joined}"

echo
echo "NAME                                          NAMESPACE         NODE                 ALLOC_GiB  USED_GiB  %_USED METRICS"
echo "------------------------------------------------------------------------------------------------------------------------"

if [[ -n "$TOP_N" ]]; then
  LC_ALL=C sort -t $'\t' -k4,4nr -k6,6n "${joined}" \
    | head -n "$TOP_N" \
    | column -t -s $'\t'
else
  LC_ALL=C sort -t $'\t' -k4,4nr -k6,6n "${joined}" \
    | column -t -s $'\t'
fi
