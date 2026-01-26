#!/usr/bin/env bash
# psclean.sh - Reconcile Dell PowerStore CSI volumes vs OpenShift, produce CSV report,
# and optionally delete safe orphan volumes.
#
# Requirements:
#   - bash 4+, kubectl, jq, curl, awk, sort, comm
#   - PowerStore REST access
#   - OpenShift access with permissions to read PV/PVC/DataVolume objects
#
# Env vars required:
#   PSTORE_IP        PowerStore management IP/FQDN (no scheme)
#   PSTORE_TOKEN     Bearer token for PowerStore REST API
#
# Optional env vars:
#   CSI_DRIVER       default: csi-powerstore.dellemc.com
#   PSTORE_INSECURE  default: 1 (use -k). Set to 0 to verify TLS.
#
# Flags:
#   --dry-run | --dr                 Do not delete; report only
#   --delete-count N | --dc N        Max number of deletions to attempt (default 0 = none)
#   --namespace NS | --ns NS         Limit scope to a namespace (see notes below)
#   --help | -h                      Show usage
#
# Namespace scoping behavior:
#   - If --ns is set, the script will ONLY consider deleting orphan volumes that appear
#     to belong to that namespace based on PowerStore volume metadata:
#       metadata["csi.volume.kubernetes.io/pvc/namespace"] == NS
#     If metadata is missing, the volume will be reported but NOT deleted.
#
# Safety rules for delete candidates:
#   - PowerStore volume is not mapped to any host
#   - PowerStore volume ID not referenced by any PowerStore CSI PV volumeHandle
#   - And (if --ns is set) volume metadata indicates pvc namespace == NS
#
# Output:
#   - CSV report written to ./psclean_report_<timestamp>.csv

set -euo pipefail

CSI_DRIVER="${CSI_DRIVER:-csi-powerstore.dellemc.com}"
PSTORE_INSECURE="${PSTORE_INSECURE:-1}"

DRY_RUN=0
DELETE_COUNT=0
NAMESPACE=""

usage() {
  cat <<'USAGE'
Usage:
  PSTORE_IP=... PSTORE_TOKEN=... ./psclean.sh [--dr|--dry-run] [--dc N|--delete-count N] [--ns NS|--namespace NS]

Examples:
  # Report only (no deletes)
  PSTORE_IP=10.0.0.10 PSTORE_TOKEN=... ./psclean.sh --dr

  # Delete up to 25 safe orphans cluster-wide
  PSTORE_IP=10.0.0.10 PSTORE_TOKEN=... ./psclean.sh --dc 25

  # Report + delete up to 10 safe orphans in namespace "vm-imports"
  PSTORE_IP=10.0.0.10 PSTORE_TOKEN=... ./psclean.sh --ns vm-imports --dc 10

Notes:
  - --dc 0 means "no deletes" (report only).
  - If --ns is set, the script will only delete volumes whose PowerStore metadata
    indicates they belong to that namespace (pvc/namespace key). Volumes missing that
    metadata will be reported but not deleted.
USAGE
}

die() { echo "ERROR: $*" >&2; exit 1; }

# --- Parse args ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run|--dr) DRY_RUN=1; shift ;;
    --delete-count|--dc)
      [[ $# -ge 2 ]] || die "Missing value for $1"
      DELETE_COUNT="$2"
      [[ "$DELETE_COUNT" =~ ^[0-9]+$ ]] || die "--dc/--delete-count must be a non-negative integer"
      shift 2
      ;;
    --namespace|--ns)
      [[ $# -ge 2 ]] || die "Missing value for $1"
      NAMESPACE="$2"
      shift 2
      ;;
    --help|-h) usage; exit 0 ;;
    *) die "Unknown argument: $1" ;;
  esac
done

[[ -n "${PSTORE_IP:-}" ]] || die "PSTORE_IP env var is required"
[[ -n "${PSTORE_TOKEN:-}" ]] || die "PSTORE_TOKEN env var is required"

CURL_TLS=()
if [[ "$PSTORE_INSECURE" == "1" ]]; then
  CURL_TLS=(-k)
fi

TS="$(date +%Y%m%d_%H%M%S)"
REPORT="psclean_report_${TS}.csv"

TMPDIR="$(mktemp -d)"
cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

echo "Starting psclean..."
echo "  CSI_DRIVER     : $CSI_DRIVER"
echo "  Namespace scope: ${NAMESPACE:-<none>}"
echo "  Dry run        : $DRY_RUN"
echo "  Delete count   : $DELETE_COUNT"
echo "  Report         : $REPORT"
echo

# --- Helpers ---
# --- PowerStore session helpers (replace existing ps_api and ps_api_delete) ---
# Behavior:
#  - If PSTORE_COOKIEFILE and PSTORE_TOKEN are set, reuse them.
#  - Else if PSTORE_USER and PSTORE_PASS are set, create a login_session to get cookie + token.
#  - Subsequent calls reuse the same cookiefile / token for the run.

PSTORE_SESSION_COOKIEFILE="${PSTORE_SESSION_COOKIEFILE:-}"

_ps_login() {
  # create a cookiefile for this run
  PSTORE_SESSION_COOKIEFILE="$(mktemp)"
  # login and capture headers to extract DELL-EMC-TOKEN
  curl -k -s -X GET \
    -H "Accept: application/json" -H "Content-type: application/json" \
    -u "${PSTORE_USER}:${PSTORE_PASS}" -c "${PSTORE_SESSION_COOKIEFILE}" \
    "https://${PSTORE_IP}/api/rest/login_session" -D "${PSTORE_SESSION_COOKIEFILE}.hdr" >/dev/null
  # extract token
  PSTORE_TOKEN="$(awk -F': ' '/DELL-EMC-TOKEN/ {print $2; exit}' "${PSTORE_SESSION_COOKIEFILE}.hdr" | tr -d $'\r\n')"
  # export to environment for use by other pieces if desired
  export PSTORE_TOKEN
  export PSTORE_COOKIEFILE="${PSTORE_SESSION_COOKIEFILE}"
}

# ps_api: GET path -> prints body
ps_api() {
  local path="$1"

  # prefer explicit cookiefile + token if user provided them
  if [[ -n "${PSTORE_COOKIEFILE:-}" && -n "${PSTORE_TOKEN:-}" ]]; then
    curl -sS "${CURL_TLS[@]}" -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -H "Accept: application/json" -b "${PSTORE_COOKIEFILE}" "https://${PSTORE_IP}$path"
    return $?
  fi

  # else if we have credentials, login once
  if [[ -n "${PSTORE_USER:-}" && -n "${PSTORE_PASS:-}" && -z "${PSTORE_SESSION_COOKIEFILE:-}" ]]; then
    _ps_login
  fi

  # If after that we have a session cookie + token, use them
  if [[ -n "${PSTORE_SESSION_COOKIEFILE:-}" && -n "${PSTORE_TOKEN:-}" ]]; then
    curl -sS "${CURL_TLS[@]}" -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -H "Accept: application/json" -b "${PSTORE_SESSION_COOKIEFILE}" "https://${PSTORE_IP}$path"
    return $?
  fi

  # Fallback: try Authorization: Bearer if user really provided an external bearer token
  if [[ -n "${PSTORE_TOKEN:-}" ]]; then
    curl -sS "${CURL_TLS[@]}" -H "Authorization: Bearer ${PSTORE_TOKEN}" -H "Accept: application/json" "https://${PSTORE_IP}$path"
    return $?
  fi

  echo "ERROR: no PowerStore auth method available. Set PSTORE_COOKIEFILE & PSTORE_TOKEN, or PSTORE_USER & PSTORE_PASS" >&2
  return 2
}

# ps_api_delete: DELETE path
ps_api_delete() {
  local path="$1"

  if [[ -n "${PSTORE_COOKIEFILE:-}" && -n "${PSTORE_TOKEN:-}" ]]; then
    curl -sS "${CURL_TLS[@]}" -X DELETE -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -b "${PSTORE_COOKIEFILE}" -H "Accept: application/json" "https://${PSTORE_IP}$path"
    return $?
  fi

  if [[ -n "${PSTORE_USER:-}" && -n "${PSTORE_PASS:-}" && -z "${PSTORE_SESSION_COOKIEFILE:-}" ]]; then
    _ps_login
  fi

  if [[ -n "${PSTORE_SESSION_COOKIEFILE:-}" && -n "${PSTORE_TOKEN:-}" ]]; then
    curl -sS "${CURL_TLS[@]}" -X DELETE -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -b "${PSTORE_SESSION_COOKIEFILE}" -H "Accept: application/json" "https://${PSTORE_IP}$path"
    return $?
  fi

  if [[ -n "${PSTORE_TOKEN:-}" ]]; then
    curl -sS "${CURL_TLS[@]}" -X DELETE -H "Authorization: Bearer ${PSTORE_TOKEN}" -H "Accept: application/json" "https://${PSTORE_IP}$path"
    return $?
  fi

  echo "ERROR: no PowerStore auth method available for delete. Set PSTORE_COOKIEFILE & PSTORE_TOKEN, or PSTORE_USER & PSTORE_PASS" >&2
  return 2
}

csv_escape() {
  # Minimal CSV escaping: wrap in quotes, escape any quotes by doubling them
  local s="${1:-}"
  s="${s//\"/\"\"}"
  printf '"%s"' "$s"
}

# --- 1) Build in-use PowerStore volume IDs from OpenShift PVs ---
echo "Collecting PowerStore CSI PV volumeHandles from OpenShift..."
kubectl get pv -o json > "$TMPDIR/pv.json"

jq -r --arg d "$CSI_DRIVER" '
  .items[]
  | select(.spec.csi.driver == $d)
  | .spec.csi.volumeHandle
' "$TMPDIR/pv.json" | sort -u > "$TMPDIR/inuse_handles_all.txt"

INUSE_COUNT="$(wc -l < "$TMPDIR/inuse_handles_all.txt" | tr -d ' ')"
echo "  Found in-use volumeHandles (all namespaces): $INUSE_COUNT"

# Also capture PV -> claimRef (for reporting, if ever needed)
jq -r --arg d "$CSI_DRIVER" '
  .items[]
  | select(.spec.csi.driver == $d)
  | [
      .metadata.name,
      .spec.csi.volumeHandle,
      (.spec.claimRef.namespace // ""),
      (.spec.claimRef.name // "")
    ] | @tsv
' "$TMPDIR/pv.json" > "$TMPDIR/pv_map.tsv"

# --- 2) Pull all PowerStore volumes (id, name, created_timestamp, mapped) ---
echo "Collecting PowerStore volumes..."
ps_api "/api/rest/volume" > "$TMPDIR/volumes.json"

# Expecting an array; if PowerStore returns an object with "content"/etc, this will fail fast
jq -r '
  .[] | [
    .id,
    (.name // ""),
    (.created_timestamp // ""),
    (if (.mapped == true) then "true" else "false" end)
  ] | @tsv
' "$TMPDIR/volumes.json" > "$TMPDIR/volumes.tsv"

TOTAL_VOLS="$(wc -l < "$TMPDIR/volumes.tsv" | tr -d ' ')"
echo "  Total volumes on array: $TOTAL_VOLS"

# --- 3) Diff: volumes NOT referenced by any PV volumeHandle (raw orphans) ---
echo "Computing orphan candidates (not referenced by any PV volumeHandle)..."
# volumes.tsv field1 = volume_id
awk 'NR==FNR{a[$1]=1; next} !($1 in a){print}' \
  "$TMPDIR/inuse_handles_all.txt" \
  "$TMPDIR/volumes.tsv" > "$TMPDIR/orphans_raw.tsv"

RAW_ORPHANS="$(wc -l < "$TMPDIR/orphans_raw.tsv" | tr -d ' ')"
echo "  Raw orphan candidates (unreferenced): $RAW_ORPHANS"

# --- 4) Filter: only unmapped volumes are eligible (hard stop) ---
awk -F'\t' '$4=="false"{print}' "$TMPDIR/orphans_raw.tsv" > "$TMPDIR/orphans_unmapped.tsv"
UNMAPPED_ORPHANS="$(wc -l < "$TMPDIR/orphans_unmapped.tsv" | tr -d ' ')"
echo "  Unmapped orphan candidates: $UNMAPPED_ORPHANS"
echo

# --- 5) Create report header ---
{
  echo "run_timestamp,namespace_scope,dry_run,delete_count,action,volume_id,volume_name,created_timestamp,mapped,eligible_for_delete,reason,pv_name,pvc_namespace,pvc_name"
} > "$REPORT"

# --- 6) For each candidate, fetch metadata (for namespace scoping + richer reporting) ---
# PowerStore volume details may be needed to read .metadata keys.
# We only do this for candidates to keep runtime reasonable.
echo "Analyzing candidates and writing CSV report..."
DELETE_ATTEMPTED=0
DELETE_ELIGIBLE=0

while IFS=$'\t' read -r VOL_ID VOL_NAME CREATED_TS MAPPED; do
  # Defaults
  PVC_NS=""
  PVC_NAME=""
  PV_NAME=""

  # Pull per-volume details for metadata (fast enough for candidates)
  # If this fails, still report, but do not delete.
  DETAILS_JSON=""
  if DETAILS_JSON="$(ps_api "/api/rest/volume/${VOL_ID}" 2>/dev/null || true)"; then
    PVC_NS="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/namespace"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
    PVC_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
    PV_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pv/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
  fi

  ELIGIBLE="no"
  REASON="Unreferenced by PV volumeHandle and unmapped"
  ACTION="report"

  # Namespace scoping:
  if [[ -n "$NAMESPACE" ]]; then
    if [[ -z "$PVC_NS" ]]; then
      ELIGIBLE="no"
      REASON="Namespace-scoped run: missing PowerStore PVC namespace metadata; not deleting"
    elif [[ "$PVC_NS" != "$NAMESPACE" ]]; then
      ELIGIBLE="no"
      REASON="Namespace-scoped run: volume metadata namespace does not match; not deleting"
    else
      ELIGIBLE="yes"
      REASON="Eligible (namespace match), unreferenced by PV volumeHandle, unmapped"
    fi
  else
    # Cluster-wide mode: eligible if we at least confirm unmapped/unreferenced (already true here)
    ELIGIBLE="yes"
    REASON="Eligible (cluster-wide), unreferenced by PV volumeHandle, unmapped"
  fi

  # Write report row (always)
  {
    csv_escape "$TS"; echo -n ","
    csv_escape "${NAMESPACE:-}"; echo -n ","
    csv_escape "$DRY_RUN"; echo -n ","
    csv_escape "$DELETE_COUNT"; echo -n ","
    csv_escape "$ACTION"; echo -n ","
    csv_escape "$VOL_ID"; echo -n ","
    csv_escape "$VOL_NAME"; echo -n ","
    csv_escape "$CREATED_TS"; echo -n ","
    csv_escape "$MAPPED"; echo -n ","
    csv_escape "$ELIGIBLE"; echo -n ","
    csv_escape "$REASON"; echo -n ","
    csv_escape "$PV_NAME"; echo -n ","
    csv_escape "$PVC_NS"; echo -n ","
    csv_escape "$PVC_NAME"
    echo
  } >> "$REPORT"

  # Attempt delete?
  if [[ "$ELIGIBLE" == "yes" ]]; then
    DELETE_ELIGIBLE=$((DELETE_ELIGIBLE + 1))

    if [[ "$DELETE_COUNT" -gt 0 && "$DELETE_ATTEMPTED" -lt "$DELETE_COUNT" ]]; then
      if [[ "$DRY_RUN" == "1" ]]; then
        : # no-op
      else
        # Delete the volume
        if ps_api_delete "/api/rest/volume/${VOL_ID}" >/dev/null 2>&1; then
          DELETE_ATTEMPTED=$((DELETE_ATTEMPTED + 1))
          # Append a second row indicating deletion action
          {
            csv_escape "$TS"; echo -n ","
            csv_escape "${NAMESPACE:-}"; echo -n ","
            csv_escape "$DRY_RUN"; echo -n ","
            csv_escape "$DELETE_COUNT"; echo -n ","
            csv_escape "delete"; echo -n ","
            csv_escape "$VOL_ID"; echo -n ","
            csv_escape "$VOL_NAME"; echo -n ","
            csv_escape "$CREATED_TS"; echo -n ","
            csv_escape "$MAPPED"; echo -n ","
            csv_escape "yes"; echo -n ","
            csv_escape "Deleted via script"; echo -n ","
            csv_escape "$PV_NAME"; echo -n ","
            csv_escape "$PVC_NS"; echo -n ","
            csv_escape "$PVC_NAME"
            echo
          } >> "$REPORT"
        else
          # Append failure row
          {
            csv_escape "$TS"; echo -n ","
            csv_escape "${NAMESPACE:-}"; echo -n ","
            csv_escape "$DRY_RUN"; echo -n ","
            csv_escape "$DELETE_COUNT"; echo -n ","
            csv_escape "delete_failed"; echo -n ","
            csv_escape "$VOL_ID"; echo -n ","
            csv_escape "$VOL_NAME"; echo -n ","
            csv_escape "$CREATED_TS"; echo -n ","
            csv_escape "$MAPPED"; echo -n ","
            csv_escape "yes"; echo -n ","
            csv_escape "Delete attempt failed (see PowerStore logs / auth / RBAC)"; echo -n ","
            csv_escape "$PV_NAME"; echo -n ","
            csv_escape "$PVC_NS"; echo -n ","
            csv_escape "$PVC_NAME"
            echo
          } >> "$REPORT"
        fi
      fi
    fi
  fi

done < "$TMPDIR/orphans_unmapped.tsv"

echo
echo "Done."
echo "  Eligible candidates : $DELETE_ELIGIBLE"
if [[ "$DRY_RUN" == "1" ]]; then
  echo "  Deletes attempted   : 0 (dry-run)"
else
  echo "  Deletes attempted   : $DELETE_ATTEMPTED"
fi
echo "  CSV report          : $REPORT"
echo
echo "Tip: Review the CSV for volumes missing namespace metadata (if you used --ns),"
echo "or for anything surprising before increasing --dc."
