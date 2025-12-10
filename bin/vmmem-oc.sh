#!/usr/bin/env bash
set -euo pipefail

#
# Pure oc / oc adm script for VM RAM allocation vs usage.
#
# Supports filters:
#   --percent-used <N>     (show only VMs with pct_used <= N)
#   --min-alloc-mem <GiB>  (show only VMs with allocated RAM >= GiB)
#
# Supports limiting results:
#   TOP_N=30 ./vmmem-oc.sh
#

TOP_N="${TOP_N:-}"
PCT_THRESH=""
MIN_ALLOC_GIB=""

# ---------- arg parsing ----------

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
    --percent-used|-p)
      PCT_THRESH="$2"
      shift 2
      ;;
    --min-alloc-mem|-m)
      MIN_ALLOC_GIB="$2"
      shift 2
      ;;
    -h|--help)
      print_help
      exit 0
      ;;
    *)
      echo "ERROR: Unknown argument: $1" >&2
      print_help >&2
      exit 1
      ;;
  esac
done

# Validate numeric filters
if [[ -n "$PCT_THRESH" && ! "$PCT_THRESH" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "ERROR: --percent-used requires a numeric value." >&2
  exit 1
fi

if [[ -n "$MIN_ALLOC_GIB" && ! "$MIN_ALLOC_GIB" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "ERROR: --min-alloc-mem requires a numeric GiB value." >&2
  exit 1
fi

# ---------- required tools ----------

require_bin() {
  for b in "$@"; do
    command -v "$b" >/dev/null 2>&1 || {
      echo "ERROR: Required binary '$b' not found." >&2
      exit 1
    }
  done
}

quantity_to_mi_awk='
function toMi(q, v) {
  if (q == "" || q == "0") return 0;
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
top_raw="${tmpdir}/top_raw.tsv"
vmi_used="${tmpdir}/vmi_used.tsv"
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

echo "# Collecting memory usage from oc adm top pod..." >&2

oc adm top pod -A --selector kubevirt.io=virt-launcher 2>/dev/null \
  | awk '
      NR==1 && $1=="NAMESPACE" { next }
      NF>=4 {
        ns=$1; pod=$2; mem=$4
        print ns "\t" pod "\t" mem
      }
    ' > "${top_raw}"

# Convert to Mi + map launcher pod → VM name
awk -F'\t' "${quantity_to_mi_awk}
  NF >= 3 {
    ns = \$1
    pod = \$2
    qty = \$3

    usedMi = toMi(qty)

    vm = pod
    sub(/^virt-launcher-/, \"\", vm)
    sub(/-[^-]+$/, \"\", vm)

    key = ns \"|\" vm
    printf \"%s\t%.3f\n\", key, usedMi
  }
" "${top_raw}" > "${vmi_used}"

# ---------- join + filters ----------

LC_ALL=C sort -t $'\t' -o "${vmi_alloc}" "${vmi_alloc}"
LC_ALL=C sort -t $'\t' -o "${vmi_used}"  "${vmi_used}"

join -t $'\t' -j 1 "${vmi_alloc}" "${vmi_used}" \
  | awk -F'\t' -v pct_thresh="${PCT_THRESH:-}" -v min_alloc="${MIN_ALLOC_GIB:-}" '
      BEGIN { gib = 1024.0 }
      {
        allocMi = $2
        ns      = $3
        name    = $4
        node    = $5
        usedMi  = $6

        allocGi = allocMi / 1024.0
        usedGi  = usedMi  / 1024.0
        pct     = (allocMi > 0 ? (usedMi / allocMi * 100.0) : 0.0)

        # filter: min allocated GiB
        if (min_alloc != "" && allocGi < min_alloc) next

        # filter: percent used <= threshold
        if (pct_thresh != "" && pct > pct_thresh) next

        printf "%s\t%s\t%s\t%8.2f\t%8.2f\t%8.2f\n",
               name, ns, node, allocGi, usedGi, pct
      }
    ' > "${joined}"

echo
echo "NAME                   NAMESPACE         NODE                ALLOC_GiB  USED_GiB   PCT_USED"
echo "-------------------------------------------------------------------------------------------"

# Final sort
if [[ -n "$TOP_N" ]]; then
  LC_ALL=C sort -t $'\t' -k4,4nr -k6,6n "${joined}" \
    | head -n "$TOP_N" \
    | column -t -s $'\t'
else
  LC_ALL=C sort -t $'\t' -k4,4nr -k6,6n "${joined}" \
    | column -t -s $'\t'
fi
