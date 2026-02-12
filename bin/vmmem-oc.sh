#!/usr/bin/env bash
set -euo pipefail

#
# vmmem-oc.sh (v2 full replacement)
#
# Purpose:
#   Show KubeVirt VMI allocated memory (from VMI spec) vs actual virt-launcher pod memory usage
#   (from `oc adm top pod`) for Running VMIs.
#
# Robustness improvements:
#   - MEMORY column detected dynamically (no hard-coded $4)
#   - virt-launcher pod -> VMI name resolved via:
#       1) pod ownerReferences kind=VirtualMachineInstance (most robust)
#       2) pod label kubevirt.io/domain (common)
#       3) heuristic from pod name (last resort)
#   - Does NOT drop VMIs when metrics are missing: prints USED=0 and METRICS=no
#   - Quantity parser supports Ti/T, Gi/G, Mi/M, Ki/K, and raw bytes
#
# Filters:
#   --percent-used N / -p N    => keep rows where pct_used <= N
#   --min-alloc-mem GiB / -m   => keep rows where alloc_gib >= GiB
#
# Output sorting:
#   alloc_gib desc, then pct_used asc
#
# Optional:
#   TOP_N=30      => show only first N rows after sorting
#   DEBUG=1       => keep temp files and print their paths
#

TOP_N="${TOP_N:-}"
DEBUG="${DEBUG:-0}"

PCT_THRESH=""
MIN_ALLOC_GIB=""

print_help() {
  cat <<EOF
Usage: $0 [--percent-used N] [--min-alloc-mem GiB]

Options:
  --percent-used N, -p N
        Only show VMIs with pct_used <= N.

  --min-alloc-mem GiB, -m GiB
        Only show VMIs with alloc_gib >= GiB.

  -h, --help
        Show help.

Environment:
  TOP_N=N
        Show only the first N sorted results.

  DEBUG=1
        Keep temp files and print their location.
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

if [[ -n "${PCT_THRESH}" && ! "${PCT_THRESH}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "ERROR: --percent-used requires a numeric value." >&2
  exit 1
fi
if [[ -n "${MIN_ALLOC_GIB}" && ! "${MIN_ALLOC_GIB}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
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

require_bin oc jq awk sort join column mktemp

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

tmpdir="$(mktemp -d)"
cleanup() {
  if [[ "${DEBUG}" == "1" ]]; then
    echo "# DEBUG=1: temp files kept in: ${tmpdir}" >&2
  else
    rm -rf "${tmpdir}"
  fi
}
trap cleanup EXIT

# Temp files
vmi_raw="${tmpdir}/vmi_raw.tsv"                 # ns  name  node  memQty
vmi_alloc="${tmpdir}/vmi_alloc.tsv"             # key(ns|vmi) allocMi ns name node

podmap_raw="${tmpdir}/podmap_raw.tsv"           # key(ns|pod) vmiNameResolved
top_raw="${tmpdir}/top_raw.tsv"                 # ns pod memQty
top_used_by_pod="${tmpdir}/top_used_by_pod.tsv" # key(ns|pod) usedMi

pod_to_vmi_used="${tmpdir}/pod_to_vmi_used.tsv" # key(ns|vmi) usedMi hasMetrics(1)
joined="${tmpdir}/joined.tsv"

echo "# Collecting VMI info (allocated memory + node)..." >&2

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

echo "# Collecting virt-launcher pod -> VMI mapping (ownerRefs/labels/fallback)..." >&2

# Map pod -> VMI name:
#  1) ownerReferences kind=VirtualMachineInstance (best)
#  2) label kubevirt.io/domain
#  3) heuristic from pod name
oc get pod -A -l kubevirt.io=virt-launcher -o json \
  | jq -r '
      .items[]
      | .metadata as $m
      | [
          $m.namespace,
          $m.name,
          ($m.labels["kubevirt.io/domain"] // ""),
          (
            ($m.ownerReferences // [])
            | map(select(.kind=="VirtualMachineInstance"))
            | .[0].name
            // ""
          )
        ]
      | @tsv
    ' \
  | awk -F'\t' '
      {
        ns=$1; pod=$2; domain=$3; ownerVmi=$4;

        # heuristic fallback from pod name
        hv=pod
        sub(/^virt-launcher-/, "", hv)
        sub(/-[^-]+$/, "", hv)

        vmi = (ownerVmi != "" ? ownerVmi : (domain != "" ? domain : hv))
        key = ns "|" pod
        print key "\t" vmi
      }
    ' > "${podmap_raw}"

echo "# Collecting memory usage from oc adm top pod..." >&2

# Always create these files so later steps won't crash
: > "${top_raw}"
: > "${top_used_by_pod}"

# Parse top output robustly; if it fails, proceed with empty metrics (METRICS=no)
if ! oc adm top pod -A --selector kubevirt.io=virt-launcher 2>/dev/null \
  | awk '
      NR==1 {
        for (i=1; i<=NF; i++) {
          if ($i=="NAMESPACE") nscol=i
          else if ($i=="NAME") namecol=i
          else if ($i ~ /^MEMORY/) memcol=i
        }
        if (!nscol || !namecol || !memcol) {
          print "WARN: Could not detect NAMESPACE/NAME/MEMORY columns in oc adm top output" > "/dev/stderr"
          exit 2
        }
        next
      }
      {
        ns=$nscol; pod=$namecol; mem=$memcol
        if (ns!="" && pod!="" && mem!="") print ns "\t" pod "\t" mem
      }
    ' > "${top_raw}"
then
  echo "WARN: oc adm top pod produced no parsable data; METRICS will be 'no'." >&2
  : > "${top_raw}"
fi

awk -F'\t' "${quantity_to_mi_awk}
  NF >= 3 {
    ns=\$1; pod=\$2; qty=\$3
    usedMi = toMi(qty)
    key = ns \"|\" pod
    printf \"%s\t%.3f\n\", key, usedMi
  }
" "${top_raw}" > "${top_used_by_pod}"

# Join pod->vmi with pod->usedMi => vmi->usedMi
LC_ALL=C sort -t $'\t' -o "${podmap_raw}" "${podmap_raw}"
LC_ALL=C sort -t $'\t' -o "${top_used_by_pod}" "${top_used_by_pod}"

join -t $'\t' -j 1 "${podmap_raw}" "${top_used_by_pod}" \
  | awk -F'\t' '
      {
        # $1 = ns|pod
        # $2 = vmi
        # $3 = usedMi
        split($1, a, "|"); ns=a[1]
        vmi=$2
        usedMi=$3 + 0

        if (vmi == "") next
        key = ns "|" vmi

        # If multiple pods map to same VMI, keep max usage (safer default)
        if (!(key in max) || usedMi > max[key]) max[key]=usedMi
      }
      END {
        for (k in max) printf "%s\t%.3f\t1\n", k, max[k]
      }
    ' > "${pod_to_vmi_used}"

# Join allocation + usage (keep alloc even if metrics missing)
LC_ALL=C sort -t $'\t' -o "${vmi_alloc}" "${vmi_alloc}"
LC_ALL=C sort -t $'\t' -o "${pod_to_vmi_used}" "${pod_to_vmi_used}"

# vmi_alloc: key allocMi ns name node
# pod_to_vmi_used: key usedMi hasMetrics
join -t $'\t' -a 1 -e 0 -o '1.1 1.2 1.3 1.4 1.5 2.2 2.3' \
  "${vmi_alloc}" "${pod_to_vmi_used}" \
  | awk -F'\t' -v pct_thresh="${PCT_THRESH:-}" -v min_alloc="${MIN_ALLOC_GIB:-}" '
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

        printf "%-44s\t%-15s\t%-18s\t%8.2f\t%8.2f\t%8.2f\t%s\n",
               name, ns, node, allocGi, usedGi, pct, metrics
      }
    ' > "${joined}"

echo
echo "NAME                                         NAMESPACE        NODE                ALLOC_GiB  USED_GiB   %_USED  METRICS"
echo "------------------------------------------------------------------------------------------------------------------------"

if [[ -n "${TOP_N}" ]]; then
  LC_ALL=C sort -t $'\t' -k4,4nr -k6,6n "${joined}" \
    | head -n "${TOP_N}" \
    | column -t -s $'\t'
else
  LC_ALL=C sort -t $'\t' -k4,4nr -k6,6n "${joined}" \
    | column -t -s $'\t'
fi

if [[ "${DEBUG}" == "1" ]]; then
  echo >&2
  echo "# DEBUG files:" >&2
  echo "#   ${vmi_raw}" >&2
  echo "#   ${vmi_alloc}" >&2
  echo "#   ${podmap_raw}" >&2
  echo "#   ${top_raw}" >&2
  echo "#   ${top_used_by_pod}" >&2
  echo "#   ${pod_to_vmi_used}" >&2
  echo "#   ${joined}" >&2
fi
