#!/usr/bin/env bash
# psclean.sh - Reconcile Dell PowerStore CSI volumes vs OpenShift, produce CSV report,
# and optionally delete safe orphan volumes.
#
# Changes in this version:
#  - Safe pagination for PowerStore volumes (limit=100, offset loop + dedupe)
#  - --relogin flag to force new login session
#  - --protect-dv flag to protect volumes referenced by KubeVirt DataVolumes
#
# Requirements:
#   bash 4+, kubectl, jq, curl, awk, sort, comm, date, mktemp
#
# Usage:
#   PSTORE_IP=... ./psclean.sh [--dr|--dry-run] [--dc N|--delete-count N] [--ns NS|--namespace NS]
#                              [--protect-dv] [--relogin]
#
set -euo pipefail

# --- Defaults ---
CSI_DRIVER="${CSI_DRIVER:-csi-powerstore.dellemc.com}"
PSTORE_INSECURE="${PSTORE_INSECURE:-1}"

DRY_RUN=0
DELETE_COUNT=0
NAMESPACE=""
PROTECT_DV=0
FORCE_RELOGIN=0

usage() {
  cat <<'USAGE'
Usage:
  PSTORE_IP=... ./psclean.sh [--dr|--dry-run] [--dc N|--delete-count N] [--ns NS|--namespace NS] [--protect-dv] [--relogin]

Flags:
  --dry-run | --dr                 Do not delete; report only
  --delete-count N | --dc N        Max number of deletions to attempt (default 0 = none)
  --namespace NS | --ns NS         Limit scope to a namespace (based on volume metadata)
  --protect-dv                     Check KubeVirt DataVolumes and protect referenced volumes
  --relogin                        Force a fresh login (removes stored session)
  --help | -h                      Show usage

Environment variables:
  PSTORE_IP            (required) PowerStore mgmt IP or FQDN (no scheme)
  PSTORE_USER/PSTORE_PASS (optional) credentials; if not present script will prompt
  PSTORE_COOKIEFILE    (optional) path to a pre-existing cookie file (skips login)
  PSTORE_TOKEN         (optional) DELL-EMC-TOKEN value (skips login if provided along with cookie)
  PSTORE_INSECURE      (optional) default 1 -> curl -k (set to 0 to verify TLS)
  PSTORE_SESSION_NO_STORE (optional) set to 1 to avoid persisting session files after login

Notes:
  - CSV reports are written to ./storage_reports in the same dir as the script.
  - Session files are stored in ./pstore_session/ (next to the script). Use --relogin to force a new login.
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
    --protect-dv) PROTECT_DV=1; shift ;;
    --relogin) FORCE_RELOGIN=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "Unknown argument: $1" ;;
  esac
done

[[ -n "${PSTORE_IP:-}" ]] || die "PSTORE_IP env var is required (PowerStore mgmt IP/FQDN)"

CURL_TLS=()
if [[ "$PSTORE_INSECURE" == "1" ]]; then
  CURL_TLS=(-k)
fi

# --- Locations relative to the script path ---
SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(dirname "$SCRIPT_PATH")"
SESSION_DIR="$SCRIPT_DIR/pstore_session"
REPORT_DIR="$SCRIPT_DIR/storage_reports"

mkdir -p "$REPORT_DIR"
mkdir -p "$SESSION_DIR"
chmod 700 "$SESSION_DIR"

TS_NOW="$(date +%Y%m%d_%H%M%S)"
REPORT="$REPORT_DIR/psclean_report_${TS_NOW}.csv"

TMPDIR="$(mktemp -d)"
cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

echo "Starting psclean..."
echo "  Script dir      : $SCRIPT_DIR"
echo "  CSI_DRIVER      : $CSI_DRIVER"
echo "  Namespace scope : ${NAMESPACE:-<none>}"
echo "  Dry run         : $DRY_RUN"
echo "  Delete count    : $DELETE_COUNT"
echo "  Protect DV      : $PROTECT_DV"
echo "  Force relogin   : $FORCE_RELOGIN"
echo "  Report          : $REPORT"
echo "  Session dir     : $SESSION_DIR"
echo

# --- Session files ---
COOKIE_FILE_ON_DISK="$SESSION_DIR/cookie.file"
TOKEN_FILE_ON_DISK="$SESSION_DIR/token.txt"

# Force relogin if requested
if [[ "$FORCE_RELOGIN" == "1" ]]; then
  echo "Forcing re-login: removing stored session if present..."
  rm -f "$COOKIE_FILE_ON_DISK" "$TOKEN_FILE_ON_DISK" || true
fi

# load session if exists
load_session_if_exists() {
  if [[ -f "$COOKIE_FILE_ON_DISK" && -f "$TOKEN_FILE_ON_DISK" ]]; then
    PSTORE_COOKIEFILE="$COOKIE_FILE_ON_DISK"
    PSTORE_TOKEN="$(<"$TOKEN_FILE_ON_DISK")"
    chmod 600 "$COOKIE_FILE_ON_DISK" "$TOKEN_FILE_ON_DISK" || true
    export PSTORE_COOKIEFILE PSTORE_TOKEN
    return 0
  fi
  return 1
}

# login and persist (or keep temp if PSTORE_SESSION_NO_STORE=1)
_ps_login_and_store() {
  if [[ -z "${PSTORE_USER:-}" ]]; then
    read -rp "PowerStore username: " PSTORE_USER
  fi
  if [[ -z "${PSTORE_PASS:-}" ]]; then
    read -s -rp "PowerStore password for ${PSTORE_USER}: " PSTORE_PASS
    echo
  fi

  local tmpcookie hdrfile token
  tmpcookie="$(mktemp)"
  hdrfile="${tmpcookie}.hdr"

  if ! curl "${CURL_TLS[@]}" -s -X GET \
      -H "Accept: application/json" -H "Content-Type: application/json" \
      -u "${PSTORE_USER}:${PSTORE_PASS}" -c "${tmpcookie}" \
      "https://${PSTORE_IP}/api/rest/login_session" -D "${hdrfile}" >/dev/null 2>&1; then
    rm -f "$tmpcookie" "$hdrfile"
    die "Failed to authenticate to PowerStore. Check credentials / network."
  fi

  token="$(awk -F': ' '/DELL-EMC-TOKEN/ {print $2; exit}' "$hdrfile" | tr -d $'\r\n' || true)"
  if [[ -z "$token" ]]; then
    rm -f "$tmpcookie" "$hdrfile"
    die "Login succeeded but no DELL-EMC-TOKEN returned. Check PowerStore version / API behavior."
  fi

  if [[ "${PSTORE_SESSION_NO_STORE:-}" == "1" ]]; then
    PSTORE_COOKIEFILE="$tmpcookie"
    PSTORE_TOKEN="$token"
    export PSTORE_COOKIEFILE PSTORE_TOKEN
    rm -f "$hdrfile"
    return 0
  fi

  mv "$tmpcookie" "$COOKIE_FILE_ON_DISK"
  chmod 600 "$COOKIE_FILE_ON_DISK"
  echo -n "$token" > "$TOKEN_FILE_ON_DISK"
  chmod 600 "$TOKEN_FILE_ON_DISK"
  rm -f "$hdrfile"

  PSTORE_COOKIEFILE="$COOKIE_FILE_ON_DISK"
  PSTORE_TOKEN="$token"
  export PSTORE_COOKIEFILE PSTORE_TOKEN
  echo "Stored session to $SESSION_DIR (cookie + token)."
}

# Ensure we have a valid session
ensure_session() {
  if [[ -n "${PSTORE_COOKIEFILE:-}" && -n "${PSTORE_TOKEN:-}" ]]; then
    return 0
  fi

  if load_session_if_exists; then
    return 0
  fi

  if [[ -n "${PSTORE_USER:-}" && -n "${PSTORE_PASS:-}" ]]; then
    _ps_login_and_store
    return 0
  fi

  echo "No existing PowerStore session found. Please enter credentials to create one."
  _ps_login_and_store
}

# PowerStore API helpers
ps_api_raw() {
  # raw request wrapper, returns HTTP body on success (no auth checks)
  local path="$1"
  curl -sS "${CURL_TLS[@]}" -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN:-}" -H "Accept: application/json" -b "${PSTORE_COOKIEFILE:-}" "https://${PSTORE_IP}$path"
}

ps_api() {
  local path="$1"
  ensure_session
  if [[ -n "${PSTORE_COOKIEFILE:-}" && -n "${PSTORE_TOKEN:-}" ]]; then
    curl -sS "${CURL_TLS[@]}" -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -H "Accept: application/json" -b "${PSTORE_COOKIEFILE}" "https://${PSTORE_IP}$path"
    return $?
  fi
  if [[ -n "${PSTORE_TOKEN:-}" ]]; then
    curl -sS "${CURL_TLS[@]}" -H "Authorization: Bearer ${PSTORE_TOKEN}" -H "Accept: application/json" "https://${PSTORE_IP}$path"
    return $?
  fi
  die "No valid PowerStore auth method available."
}

ps_api_delete() {
  local path="$1"
  ensure_session
  if [[ -n "${PSTORE_COOKIEFILE:-}" && -n "${PSTORE_TOKEN:-}" ]]; then
    curl -sS "${CURL_TLS[@]}" -X DELETE -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -H "Accept: application/json" -b "${PSTORE_COOKIEFILE}" "https://${PSTORE_IP}$path"
    return $?
  fi
  if [[ -n "${PSTORE_TOKEN:-}" ]]; then
    curl -sS "${CURL_TLS[@]}" -X DELETE -H "Authorization: Bearer ${PSTORE_TOKEN}" -H "Accept: application/json" "https://${PSTORE_IP}$path"
    return $?
  fi
  die "No valid PowerStore auth method available for delete."
}

# Helper to fetch all volumes, handling pagination safely using limit/offset + dedupe
fetch_all_volumes() {
  local limit=100
  local offset=0
  local seen_file="$TMPDIR/seen_ids.txt"
  : > "$TMPDIR/all_vols.json"   # will accumulate array elements (newline separated, not a JSON array)
  : > "$seen_file"
  local page_json new_count added=1 iterations=0

  while true; do
    iterations=$((iterations + 1))
    # request with limit & offset; many PowerStore instances honor this
    page_json="$(ps_api "/api/rest/volume?limit=${limit}&offset=${offset}")" || {
      echo "Warning: paginated request failed for limit=${limit}&offset=${offset}; trying non-paginated single call..."
      page_json="$(ps_api "/api/rest/volume")" || die "Failed to fetch volumes from PowerStore"
    }

    # Normalize: if it's wrapped under .content or .items, extract
    if ! jq -e '.[0]' <<<"$page_json" >/dev/null 2>&1; then
      if jq -e '.content // .items' <<<"$page_json" >/dev/null 2>&1; then
        page_json="$(jq -c '.content // .items' <<<"$page_json")"
      else
        # if this page returned empty array or unexpected response, break
        # but ensure we don't infinite loop
        if [[ "$iterations" -gt 1 ]]; then
          break
        else
          die "Unexpected response from PowerStore /api/rest/volume; login may have failed or API shape changed."
        fi
      fi
    fi

    # iterate each returned object and append if id unseen
    new_count=0
    while IFS= read -r obj; do
      volid="$(jq -r '.id' <<<"$obj" 2>/dev/null || true)"
      if [[ -z "$volid" || "$volid" == "null" ]]; then
        continue
      fi
      if ! grep -qFx "$volid" "$seen_file" 2>/dev/null; then
        echo "$volid" >> "$seen_file"
        echo "$obj" >> "$TMPDIR/all_vols.json"
        new_count=$((new_count + 1))
      fi
    done < <(jq -c '.[]' <<<"$page_json")

    if [[ "$new_count" -eq 0 ]]; then
      # no new volumes on this page -> assume we have collected everything
      break
    fi

    offset=$((offset + limit))
    # safety: break if offset grows excessively
    if [[ "$offset" -gt 1000000 ]]; then
      echo "Safety break: offset exceeded large threshold"
      break
    fi
  done

  # produce a JSON array from accumulated newline objects
  if [[ -s "$TMPDIR/all_vols.json" ]]; then
    jq -s '.' "$TMPDIR/all_vols.json"
  else
    echo "[]"
  fi
}

# CSV escaping
csv_escape() {
  local s="${1:-}"
  s="${s//\"/\"\"}"
  printf '"%s"' "$s"
}

# --- 1) Gather PV volumeHandles from OpenShift ---
echo "Collecting PowerStore CSI PV volumeHandles from OpenShift..."
kubectl get pv -o json > "$TMPDIR/pv.json"

jq -r --arg d "$CSI_DRIVER" '
  .items[]? | select(.spec.csi.driver == $d) | .spec.csi.volumeHandle
' "$TMPDIR/pv.json" | sort -u > "$TMPDIR/inuse_handles_all.txt"

INUSE_COUNT="$(wc -l < "$TMPDIR/inuse_handles_all.txt" | tr -d ' ')"
echo "  Found in-use volumeHandles (all namespaces): $INUSE_COUNT"

jq -r --arg d "$CSI_DRIVER" '
  .items[]? | select(.spec.csi.driver == $d) |
  [ .metadata.name, .spec.csi.volumeHandle, (.spec.claimRef.namespace // ""), (.spec.claimRef.name // "") ] | @tsv
' "$TMPDIR/pv.json" > "$TMPDIR/pv_map.tsv"

# --- Optional: gather DataVolume referenced PVs (if protect-dv) ---
DV_PROTECTED_SET="$TMPDIR/dv_protected.txt"
if [[ "$PROTECT_DV" == "1" ]]; then
  echo "Collecting KubeVirt DataVolumes to protect referenced PVs..."
  if kubectl get dv -A -o json > "$TMPDIR/dv.json" 2>/dev/null; then
    jq -r '
      .items[]?
      | (.spec.pvc.volumeName // empty)
    ' "$TMPDIR/dv.json" | sort -u > "$DV_PROTECTED_SET"
    DV_COUNT="$(wc -l < "$DV_PROTECTED_SET" | tr -d ' ')"
    echo "  DataVolumes referencing PVs: $DV_COUNT"
  else
    echo "  Warning: failed to fetch DataVolumes. Are CDI/KubeVirt present/accessible?"
    : > "$DV_PROTECTED_SET"
  fi
fi

# --- 2) Pull all volumes from PowerStore (paginated) ---
echo "Collecting PowerStore volumes (paginated)..."
VOL_JSON="$(fetch_all_volumes)" || die "Failed fetching volumes"
echo "$VOL_JSON" > "$TMPDIR/volumes.json"

jq -r '
  .[] | [ .id, (.name // ""), (.created_timestamp // ""), (if (.mapped == true) then "true" else "false" end) ] | @tsv
' "$TMPDIR/volumes.json" > "$TMPDIR/volumes.tsv"

TOTAL_VOLS="$(wc -l < "$TMPDIR/volumes.tsv" | tr -d ' ')"
echo "  Total volumes on array: $TOTAL_VOLS"

# --- 3) Diff to find unreferenced volumes (raw orphans) ---
echo "Computing orphan candidates (not referenced by any PV volumeHandle)..."
awk 'NR==FNR{a[$1]=1; next} !($1 in a){print}' \
  "$TMPDIR/inuse_handles_all.txt" \
  "$TMPDIR/volumes.tsv" > "$TMPDIR/orphans_raw.tsv"

RAW_ORPHANS="$(wc -l < "$TMPDIR/orphans_raw.tsv" | tr -d ' ')"
echo "  Raw orphan candidates (unreferenced): $RAW_ORPHANS"

# --- 4) Filter for unmapped volumes ---
awk -F'\t' '$4=="false"{print}' "$TMPDIR/orphans_raw.tsv" > "$TMPDIR/orphans_unmapped.tsv"
UNMAPPED_ORPHANS="$(wc -l < "$TMPDIR/orphans_unmapped.tsv" | tr -d ' ')"
echo "  Unmapped orphan candidates: $UNMAPPED_ORPHANS"
echo

# --- 5) Prepare CSV header ---
{
  echo "run_timestamp,namespace_scope,dry_run,delete_count,action,volume_id,volume_name,created_timestamp,mapped,eligible_for_delete,reason,pv_name,pvc_namespace,pvc_name,protected_by_datavolume"
} > "$REPORT"

# --- 6) Analyze each candidate, write report, optionally delete ---
DELETE_ATTEMPTED=0
DELETE_ELIGIBLE=0

while IFS=$'\t' read -r VOL_ID VOL_NAME CREATED_TS MAPPED; do
  PVC_NS=""
  PVC_NAME=""
  PV_NAME=""
  PROTECTED_BY_DV="no"

  # pull per-volume details for metadata
  DETAILS_JSON="$(ps_api "/api/rest/volume/${VOL_ID}" || true)"
  if [[ -n "$DETAILS_JSON" ]]; then
    PVC_NS="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/namespace"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
    PVC_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
    PV_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pv/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
  fi

  # Protect if DataVolume references this PV name (if enabled)
  if [[ "$PROTECT_DV" == "1" && -n "$PV_NAME" ]]; then
    if grep -qFx "$PV_NAME" "$DV_PROTECTED_SET" 2>/dev/null; then
      PROTECTED_BY_DV="yes"
    fi
  fi

  ELIGIBLE="no"
  REASON="Unreferenced by PV volumeHandle and unmapped"
  ACTION="report"

  # Namespace scoping rules
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
    ELIGIBLE="yes"
    REASON="Eligible (cluster-wide), unreferenced by PV volumeHandle, unmapped"
  fi

  # If protected by DataVolume, mark ineligible
  if [[ "$PROTECTED_BY_DV" == "yes" ]]; then
    ELIGIBLE="no"
    REASON="Protected: referenced by a DataVolume"
  fi

  # write CSV row (always)
  {
    csv_escape "$TS_NOW"; echo -n ","
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
    csv_escape "$PVC_NAME"; echo -n ","
    csv_escape "$PROTECTED_BY_DV"
    echo
  } >> "$REPORT"

  if [[ "$ELIGIBLE" == "yes" ]]; then
    DELETE_ELIGIBLE=$((DELETE_ELIGIBLE + 1))
    if [[ "$DELETE_COUNT" -gt 0 && "$DELETE_ATTEMPTED" -lt "$DELETE_COUNT" ]]; then
      if [[ "$DRY_RUN" == "1" ]]; then
        # no-op for dry run
        :
      else
        # attempt to delete
        if ps_api_delete "/api/rest/volume/${VOL_ID}" >/dev/null 2>&1; then
          DELETE_ATTEMPTED=$((DELETE_ATTEMPTED + 1))
          {
            csv_escape "$TS_NOW"; echo -n ","
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
            csv_escape "$PVC_NAME"; echo -n ","
            csv_escape "$PROTECTED_BY_DV"
            echo
          } >> "$REPORT"
        else
          {
            csv_escape "$TS_NOW"; echo -n ","
            csv_escape "${NAMESPACE:-}"; echo -n ","
            csv_escape "$DRY_RUN"; echo -n ","
            csv_escape "$DELETE_COUNT"; echo -n ","
            csv_escape "delete_failed"; echo -n ","
            csv_escape "$VOL_ID"; echo -n ","
            csv_escape "$VOL_NAME"; echo -n ","
            csv_escape "$CREATED_TS"; echo -n ","
            csv_escape "$MAPPED"; echo -n ","
            csv_escape "yes"; echo -n ","
            csv_escape "Delete attempt failed (check PowerStore logs / auth)"; echo -n ","
            csv_escape "$PV_NAME"; echo -n ","
            csv_escape "$PVC_NS"; echo -n ","
            csv_escape "$PVC_NAME"; echo -n ","
            csv_escape "$PROTECTED_BY_DV"
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
echo "Notes:"
echo " - If you used --ns, volumes missing PVC namespace metadata are reported but not deleted."
echo " - Session files are stored in $SESSION_DIR (cookie + token). Use --relogin to force re-login."
echo " - To avoid storing session files set PSTORE_SESSION_NO_STORE=1 in the environment."
