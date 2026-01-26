#!/usr/bin/env bash
# psclean.sh - Reconcile Dell PowerStore CSI volumes vs OpenShift, produce CSV report,
# and optionally delete safe orphan volumes.
#
# Features added:
#  - Prompts for PowerStore credentials if no session exists
#  - Persists cookie + DELL-EMC-TOKEN in a session directory adjacent to the script
#  - Reuses stored session automatically on subsequent runs
#  - Writes reports to ./storage_reports relative to script location
#
# Requirements:
#   - bash 4+, kubectl, jq, curl, awk, sort, comm, date, mktemp
#
# Usage:
#   PSTORE_IP=... ./psclean.sh [--dr|--dry-run] [--dc N|--delete-count N] [--ns NS|--namespace NS]
#
# Env vars:
#   PSTORE_IP            (required) PowerStore mgmt IP or FQDN (no scheme)
#   PSTORE_USER/PSTORE_PASS (optional) credentials; if not present script will prompt
#   PSTORE_COOKIEFILE    (optional) path to a pre-existing cookie file (skips login)
#   PSTORE_TOKEN         (optional) DELL-EMC-TOKEN value (skips login if provided along with cookie)
#   PSTORE_INSECURE      (optional) default 1 -> curl -k (set to 0 to verify TLS)
#   PSTORE_SESSION_NO_STORE (optional) set to 1 to avoid persisting session files after login
#
# Flags:
#   --dry-run | --dr                 Do not delete; report only
#   --delete-count N | --dc N        Max number of deletions to attempt (default 0 = none)
#   --namespace NS | --ns NS         Limit scope to a namespace (based on volume metadata)
#   --help | -h                      Show usage
#
set -euo pipefail

# --- Defaults ---
CSI_DRIVER="${CSI_DRIVER:-csi-powerstore.dellemc.com}"
PSTORE_INSECURE="${PSTORE_INSECURE:-1}"

DRY_RUN=0
DELETE_COUNT=0
NAMESPACE=""

usage() {
  cat <<'USAGE'
Usage:
  PSTORE_IP=... ./psclean.sh [--dr|--dry-run] [--dc N|--delete-count N] [--ns NS|--namespace NS]

Examples:
  # Report only (no deletes)
  PSTORE_IP=powerstore.mydomain.local ./psclean.sh --dr

  # Delete up to 25 safe orphans cluster-wide
  PSTORE_IP=powerstore.mydomain.local ./psclean.sh --dc 25

  # Report + delete up to 10 safe orphans in namespace "vm-imports"
  PSTORE_IP=powerstore.mydomain.local ./psclean.sh --ns vm-imports --dc 10

Environment variables:
  PSTORE_USER & PSTORE_PASS - optional (script will prompt if absent)
  PSTORE_COOKIEFILE & PSTORE_TOKEN - optional (use existing session)
  PSTORE_SESSION_NO_STORE - if set to 1, script will not persist session to disk
  PSTORE_INSECURE - set to 0 to enable TLS verification (default 1 -> -k)

Notes:
  - CSV reports are written to ./storage_reports in the same dir as the script.
  - Session files are stored in ./pstore_session/ (next to the script) by default and
    have restrictive permissions; remove them manually to force re-login.
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
echo "  Report          : $REPORT"
echo "  Session dir     : $SESSION_DIR"
echo

# --- Session management helpers ---
# Session files inside SESSION_DIR:
#   cookie.file     -> curl cookie jar
#   token.txt       -> DELL-EMC-TOKEN value (single-line)
COOKIE_FILE_ON_DISK="$SESSION_DIR/cookie.file"
TOKEN_FILE_ON_DISK="$SESSION_DIR/token.txt"

# load session if present
load_session_if_exists() {
  if [[ -f "$COOKIE_FILE_ON_DISK" && -f "$TOKEN_FILE_ON_DISK" ]]; then
    PSTORE_COOKIEFILE="$COOKIE_FILE_ON_DISK"
    PSTORE_TOKEN="$(<"$TOKEN_FILE_ON_DISK")"
    # ensure perms are restrictive
    chmod 600 "$COOKIE_FILE_ON_DISK" "$TOKEN_FILE_ON_DISK" || true
    export PSTORE_COOKIEFILE PSTORE_TOKEN
    return 0
  fi
  return 1
}

# perform login, persist session unless PSTORE_SESSION_NO_STORE=1
_ps_login_and_store() {
  # if PSTORE_USER/PSTORE_PASS present, use them; else prompt
  if [[ -z "${PSTORE_USER:-}" ]]; then
    read -rp "PowerStore username: " PSTORE_USER
  fi
  if [[ -z "${PSTORE_PASS:-}" ]]; then
    read -s -rp "PowerStore password for ${PSTORE_USER}: " PSTORE_PASS
    echo
  fi

  # temp cookie + header capture
  local tmpcookie hdrfile
  tmpcookie="$(mktemp)"
  hdrfile="${tmpcookie}.hdr"

  # Use GET /api/rest/login_session with basic auth to create a login session
  # (PowerStore returns DELL-EMC-TOKEN header and sets a cookie jar)
  if ! curl "${CURL_TLS[@]}" -s -X GET \
      -H "Accept: application/json" -H "Content-Type: application/json" \
      -u "${PSTORE_USER}:${PSTORE_PASS}" -c "${tmpcookie}" \
      "https://${PSTORE_IP}/api/rest/login_session" -D "${hdrfile}" >/dev/null 2>&1; then
    rm -f "$tmpcookie" "$hdrfile"
    die "Failed to authenticate to PowerStore. Check credentials / network."
  fi

  # extract token
  local token
  token="$(awk -F': ' '/DELL-EMC-TOKEN/ {print $2; exit}' "$hdrfile" | tr -d $'\r\n' || true)"
  if [[ -z "$token" ]]; then
    rm -f "$tmpcookie" "$hdrfile"
    die "Login succeeded but no DELL-EMC-TOKEN returned. Check PowerStore version / API behavior."
  fi

  # if the user asked not to persist session, keep token only in env and keep tmp cookie
  if [[ "${PSTORE_SESSION_NO_STORE:-}" == "1" ]]; then
    PSTORE_COOKIEFILE="$tmpcookie"
    PSTORE_TOKEN="$token"
    export PSTORE_COOKIEFILE PSTORE_TOKEN
    # still remove hdrfile
    rm -f "$hdrfile"
    return 0
  fi

  # persist cookie + token into SESSION_DIR
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

# wrapper to ensure session exists (either via provided env vars, stored files, or interactive login)
ensure_session() {
  # If user supplied PSTORE_COOKIEFILE & PSTORE_TOKEN in env, prefer that
  if [[ -n "${PSTORE_COOKIEFILE:-}" && -n "${PSTORE_TOKEN:-}" ]]; then
    return 0
  fi

  # Try to load an existing stored session
  if load_session_if_exists; then
    return 0
  fi

  # If PSTORE_USER and PSTORE_PASS provided in env, use them non-interactively
  if [[ -n "${PSTORE_USER:-}" && -n "${PSTORE_PASS:-}" ]]; then
    _ps_login_and_store
    return 0
  fi

  # interactive prompt to login and store session
  echo "No existing PowerStore session found. Please enter credentials to create one."
  _ps_login_and_store
}

# --- PowerStore API helpers (use cookie + DELL-EMC-TOKEN) ---
ps_api() {
  local path="$1"
  # ensure session exists
  ensure_session

  if [[ -n "${PSTORE_COOKIEFILE:-}" && -n "${PSTORE_TOKEN:-}" ]]; then
    curl -sS "${CURL_TLS[@]}" -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -H "Accept: application/json" -b "${PSTORE_COOKIEFILE}" "https://${PSTORE_IP}$path"
    return $?
  fi

  # fallback: try Authorization: Bearer (if user provided some external token)
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

# --- CSV helper ---
csv_escape() {
  local s="${1:-}"
  s="${s//\"/\"\"}"
  printf '"%s"' "$s"
}

# --- 1) Gather PV volumeHandles from OpenShift ---
echo "Collecting PowerStore CSI PV volumeHandles from OpenShift..."
kubectl get pv -o json > "$TMPDIR/pv.json"

jq -r --arg d "$CSI_DRIVER" '
  .items[]?
  | select(.spec.csi.driver == $d)
  | .spec.csi.volumeHandle
' "$TMPDIR/pv.json" | sort -u > "$TMPDIR/inuse_handles_all.txt"

INUSE_COUNT="$(wc -l < "$TMPDIR/inuse_handles_all.txt" | tr -d ' ')"
echo "  Found in-use volumeHandles (all namespaces): $INUSE_COUNT"

# preserve PV->claimRef map for reporting
jq -r --arg d "$CSI_DRIVER" '
  .items[]?
  | select(.spec.csi.driver == $d)
  | [
      .metadata.name,
      .spec.csi.volumeHandle,
      (.spec.claimRef.namespace // ""),
      (.spec.claimRef.name // "")
    ] | @tsv
' "$TMPDIR/pv.json" > "$TMPDIR/pv_map.tsv"

# --- 2) Pull volumes from PowerStore ---
echo "Collecting PowerStore volumes..."
VOL_JSON="$(ps_api "/api/rest/volume")" || die "Failed to fetch volumes from PowerStore. Check auth."
# validate we got an array-ish JSON
if ! jq -e '.[0]' <<<"$VOL_JSON" >/dev/null 2>&1; then
  # Some arrays come under "content" or other wrapper; try to find the array
  # Try .content, .items, or fail
  if jq -e '.content // .items' <<<"$VOL_JSON" >/dev/null 2>&1; then
    VOL_JSON="$(jq -c '.content // .items' <<<"$VOL_JSON")"
  else
    die "Unexpected response from PowerStore /api/rest/volume; login may have failed or API shape changed."
  fi
fi

# write normalized volumes array to file
echo "$VOL_JSON" > "$TMPDIR/volumes.json"

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
  echo "run_timestamp,namespace_scope,dry_run,delete_count,action,volume_id,volume_name,created_timestamp,mapped,eligible_for_delete,reason,pv_name,pvc_namespace,pvc_name"
} > "$REPORT"

# --- 6) Analyze each candidate, write report, optionally delete ---
DELETE_ATTEMPTED=0
DELETE_ELIGIBLE=0

while IFS=$'\t' read -r VOL_ID VOL_NAME CREATED_TS MAPPED; do
  PVC_NS=""
  PVC_NAME=""
  PV_NAME=""

  # pull per-volume details for metadata
  DETAILS_JSON="$(ps_api "/api/rest/volume/${VOL_ID}" || true)"
  if [[ -n "$DETAILS_JSON" ]]; then
    PVC_NS="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/namespace"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
    PVC_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
    PV_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pv/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
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

  # write a CSV row (always)
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
    csv_escape "$PVC_NAME"
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
            csv_escape "$PVC_NAME"
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
echo "Notes:"
echo " - If you used --ns, volumes missing PVC namespace metadata are reported but not deleted."
echo " - Session files are stored in $SESSION_DIR (cookie + token). Remove them to force re-login."
echo " - To avoid storing session files set PSTORE_SESSION_NO_STORE=1 in the environment."
