#!/usr/bin/env bash
# psclean.sh - Reconcile Dell PowerStore CSI volumes vs OpenShift PVs with safe deletion.
#
# IMPORTANT: On some PowerStore versions, /api/rest/volume supports select= but NOT properties like
# "mapped" or "metadata" on the collection endpoint. This script adapts:
#   - Bulk list uses select=id,name,created_timestamp (fallback to id only).
#   - "Safe to delete" (unmapped + metadata/namespace checks) is verified via per-volume GET
#     for a bounded number of candidates in dry-run, and always for any deletions.
#
# Requirements: bash 4+, kubectl, jq, curl, awk, sort, date, mktemp
#
set -euo pipefail

CSI_DRIVER="${CSI_DRIVER:-csi-powerstore.dellemc.com}"
PSTORE_INSECURE="${PSTORE_INSECURE:-1}"

# Login robustness (options 1 & 2 from earlier)
PSTORE_LOGIN_RETRIES="${PSTORE_LOGIN_RETRIES:-10}"
PSTORE_LOGIN_BACKOFF="${PSTORE_LOGIN_BACKOFF:-2}"       # seconds, exponential
PSTORE_LOGIN_POST_SLEEP="${PSTORE_LOGIN_POST_SLEEP:-1}" # seconds

# Default verification bound so dry-run completes
VERIFY_MAX=500

# Flags
DRY_RUN=0
DELETE_COUNT=0
NAMESPACE=""
PROTECT_DV=0
FORCE_RELOGIN=0
K8S_ONLY=0
PREVIEW_N=0
DEBUG=0

usage() {
  cat <<'USAGE'
Usage:
  PSTORE_IP=... ./psclean.sh [--dr|--dry-run] [--dc N|--delete-count N] [--ns NS|--namespace NS]
                             [--protect-dv] [--relogin] [--k8s-only] [--preview N]
                             [--verify-max N] [--debug]

Flags:
  --dry-run | --dr                 Report only (no deletes)
  --delete-count N | --dc N        Max number of deletions (default 0)
  --namespace NS | --ns NS         Limit scope to namespace (requires per-volume metadata checks)
  --protect-dv                     Protect volumes referenced by KubeVirt DataVolumes
  --relogin                        Force a fresh login (discard cached session)
  --k8s-only                       Limit candidates to volumes with name starting with "pvc-" (when name is available)
  --preview N                      Print first N "verified safe" candidates
  --verify-max N                   In dry-run, verify at most N candidates via per-volume GET (default 500)
  --debug                          Print login + first list page debug; also prints errors with response previews

Env:
  PSTORE_IP (required)             PowerStore mgmt FQDN/IP
  PSTORE_USER / PSTORE_PASS        Optional; otherwise prompted
  PSTORE_COOKIEFILE / PSTORE_TOKEN Optional; reuse existing session
  PSTORE_INSECURE=1                Default 1 uses -k for TLS
  PSTORE_LOGIN_RETRIES, PSTORE_LOGIN_BACKOFF, PSTORE_LOGIN_POST_SLEEP
USAGE
}

die() { echo "ERROR: $*" >&2; exit 2; }

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run|--dr) DRY_RUN=1; shift ;;
    --delete-count|--dc) [[ $# -ge 2 ]] || die "Missing value for $1"; DELETE_COUNT="$2"; shift 2 ;;
    --namespace|--ns) [[ $# -ge 2 ]] || die "Missing value for $1"; NAMESPACE="$2"; shift 2 ;;
    --protect-dv) PROTECT_DV=1; shift ;;
    --relogin) FORCE_RELOGIN=1; shift ;;
    --k8s-only) K8S_ONLY=1; shift ;;
    --preview) [[ $# -ge 2 ]] || die "Missing value for $1"; PREVIEW_N="$2"; shift 2 ;;
    --verify-max) [[ $# -ge 2 ]] || die "Missing value for $1"; VERIFY_MAX="$2"; shift 2 ;;
    --debug) DEBUG=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

[[ -n "${PSTORE_IP:-}" ]] || die "PSTORE_IP env var is required"

[[ "$DELETE_COUNT" =~ ^[0-9]+$ ]] || die "--dc must be a non-negative integer"
[[ "$VERIFY_MAX" =~ ^[0-9]+$ ]] || die "--verify-max must be a non-negative integer"

CURL_TLS=()
[[ "$PSTORE_INSECURE" == "1" ]] && CURL_TLS=(-k)

# Locations relative to script
SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(dirname "$SCRIPT_PATH")"
SESSION_DIR="$SCRIPT_DIR/pstore_session"
REPORT_DIR="$SCRIPT_DIR/storage_reports"
mkdir -p "$SESSION_DIR" "$REPORT_DIR"
chmod 700 "$SESSION_DIR"

TS_NOW="$(date +%Y%m%d_%H%M%S)"
REPORT="$REPORT_DIR/psclean_report_${TS_NOW}.csv"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

COOKIE_FILE_ON_DISK="$SESSION_DIR/cookie.file"
TOKEN_FILE_ON_DISK="$SESSION_DIR/token.txt"

echo "Starting psclean..."
echo "  Script dir      : $SCRIPT_DIR"
echo "  CSI_DRIVER      : $CSI_DRIVER"
echo "  Namespace scope : ${NAMESPACE:-<none>}"
echo "  Dry run         : $DRY_RUN"
echo "  Delete count    : $DELETE_COUNT"
echo "  Verify max      : $VERIFY_MAX"
echo "  Protect DV      : $PROTECT_DV"
echo "  Force relogin   : $FORCE_RELOGIN"
echo "  K8s-only filter : $K8S_ONLY"
echo "  Preview N       : $PREVIEW_N"
echo "  Debug           : $DEBUG"
echo "  Report          : $REPORT"
echo "  Session dir     : $SESSION_DIR"
echo

if [[ "$FORCE_RELOGIN" == "1" ]]; then
  echo "Forcing re-login: removing stored session if present..."
  rm -f "$COOKIE_FILE_ON_DISK" "$TOKEN_FILE_ON_DISK" || true
fi

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

# Login with retries + post-sleep + validation
_ps_login_and_store() {
  if [[ -z "${PSTORE_USER:-}" ]]; then
    read -rp "PowerStore username: " PSTORE_USER
  fi
  if [[ -z "${PSTORE_PASS:-}" ]]; then
    echo -n "PowerStore password for ${PSTORE_USER} (press Enter when done): "
    read -s PSTORE_PASS
    echo
  fi

  local tries=0 max="$PSTORE_LOGIN_RETRIES" backoff="$PSTORE_LOGIN_BACKOFF"
  local tmpcookie hdrfile token status body

  while (( tries < max )); do
    tries=$((tries+1))
    tmpcookie="$(mktemp)"
    hdrfile="${tmpcookie}.hdr"

    if [[ "$DEBUG" == "1" ]]; then
      echo "[debug] login attempt #$tries: GET /api/rest/login_session"
      curl "${CURL_TLS[@]}" -v -X GET \
        -H "Accept: application/json" -H "Content-Type: application/json" \
        -u "${PSTORE_USER}:${PSTORE_PASS}" -c "${tmpcookie}" \
        "https://${PSTORE_IP}/api/rest/login_session" -D "${hdrfile}" 2>&1 | sed -n '1,140p'
    else
      curl "${CURL_TLS[@]}" -s -X GET \
        -H "Accept: application/json" -H "Content-Type: application/json" \
        -u "${PSTORE_USER}:${PSTORE_PASS}" -c "${tmpcookie}" \
        "https://${PSTORE_IP}/api/rest/login_session" -D "${hdrfile}" >/dev/null 2>&1 || true
    fi

    token="$(awk -F': ' '/DELL-EMC-TOKEN/ {print $2; exit}' "$hdrfile" | tr -d $'\r\n' || true)"
    if [[ -z "$token" ]]; then
      rm -f "$tmpcookie" "$hdrfile"
      echo "  login attempt $tries: missing DELL-EMC-TOKEN; retry in ${backoff}s..."
      sleep "$backoff"; backoff=$((backoff*2))
      continue
    fi

    # post-login sleep (reduces race issues)
    if [[ "$PSTORE_LOGIN_POST_SLEEP" -gt 0 ]]; then
      sleep "$PSTORE_LOGIN_POST_SLEEP"
    fi

    # validate: request 1 volume id (should be 200 or 206)
    body="$(curl -sS "${CURL_TLS[@]}" -w "\nHTTP_STATUS:%{http_code}\n" \
      -H "DELL-EMC-TOKEN: ${token}" -b "$tmpcookie" \
      "https://${PSTORE_IP}/api/rest/volume?limit=1" 2>/dev/null || true)"
    status="$(awk -F: '/HTTP_STATUS/ {print $2}' <<<"$body" | tr -d ' \r\n' || true)"

    if [[ "$status" == "200" || "$status" == "206" ]]; then
      mv "$tmpcookie" "$COOKIE_FILE_ON_DISK"
      chmod 600 "$COOKIE_FILE_ON_DISK"
      printf "%s" "$token" > "$TOKEN_FILE_ON_DISK"
      chmod 600 "$TOKEN_FILE_ON_DISK"
      rm -f "$hdrfile"
      PSTORE_COOKIEFILE="$COOKIE_FILE_ON_DISK"
      PSTORE_TOKEN="$token"
      export PSTORE_COOKIEFILE PSTORE_TOKEN
      echo "Stored session to $SESSION_DIR (login attempts: $tries)."
      return 0
    fi

    rm -f "$tmpcookie" "$hdrfile"
    echo "  login attempt $tries: validation failed (HTTP $status); retry in ${backoff}s..."
    if [[ "$DEBUG" == "1" ]]; then
      echo "[debug] validation response preview:"
      printf '%s\n' "$body" | head -n 40
    fi
    sleep "$backoff"; backoff=$((backoff*2))
  done

  die "Failed to create a valid PowerStore session after ${max} attempts."
}

ensure_session() {
  if [[ -n "${PSTORE_COOKIEFILE:-}" && -n "${PSTORE_TOKEN:-}" ]]; then return 0; fi
  if load_session_if_exists; then return 0; fi
  _ps_login_and_store
}

# PowerStore API GET helper, captures Content-Range from headers
LAST_CONTENT_RANGE_FILE="$TMPDIR/last_content_range.txt"

ps_api() {
  local path="$1"
  ensure_session
  local hdrf bodyf
  hdrf="$(mktemp)"
  bodyf="$(mktemp)"

  if ! curl -sS "${CURL_TLS[@]}" -D "$hdrf" \
      -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -H "Accept: application/json" \
      -b "${PSTORE_COOKIEFILE}" "https://${PSTORE_IP}${path}" -o "$bodyf"; then
    if [[ "$DEBUG" == "1" ]]; then
      echo "[debug] GET $path failed; header preview:"
      sed -n '1,80p' "$hdrf" || true
      echo "[debug] body preview:"
      head -c 512 "$bodyf" || true
    fi
    rm -f "$hdrf" "$bodyf"
    return 1
  fi

  awk 'BEGIN{IGNORECASE=1} /content-range/ {print $2}' "$hdrf" | tr -d '\r\n' > "$LAST_CONTENT_RANGE_FILE" || true

  # Debug: only show for the FIRST list page + errors (controlled by caller)
  cat "$bodyf"
  rm -f "$hdrf" "$bodyf"
}

ps_api_delete() {
  local path="$1"
  ensure_session
  curl -sS "${CURL_TLS[@]}" -X DELETE \
    -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" \
    -b "${PSTORE_COOKIEFILE}" "https://${PSTORE_IP}${path}"
}

# Bulk fetch volumes with pagination.
# We try select=id,name,created_timestamp first (mapped/metadata are not supported on your array list API).
# If select fails, fall back to id-only.
fetch_all_volumes() {
  local limit=200 offset=0
  local allfile="$TMPDIR/vols.ndjson"
  : > "$allfile"

  local page first=1 content_range total
  while true; do
    if [[ "$first" == "1" ]]; then
      page="$(ps_api "/api/rest/volume?limit=${limit}&offset=${offset}&select=id,name,created_timestamp" || true)"
      if [[ "$DEBUG" == "1" ]]; then
        echo "---- [debug] first list page (select=id,name,created_timestamp) preview ----"
        printf '%s\n' "$page" | head -n 40
        echo "---- [debug] end ----"
      fi
    else
      page="$(ps_api "/api/rest/volume?limit=${limit}&offset=${offset}&select=id,name,created_timestamp" || true)"
    fi

    # If select failed (PowerStore returns {"messages":[...]}), fall back
    if jq -e '.messages? // empty' <<<"$page" >/dev/null 2>&1; then
      if [[ "$first" == "1" ]]; then
        echo "  Warning: select=id,name,created_timestamp failed; falling back to id-only list."
      fi
      page="$(ps_api "/api/rest/volume?limit=${limit}&offset=${offset}" || die "Failed fetching volumes")"
    fi

    # normalize array
    if ! jq -e '.[0]' <<<"$page" >/dev/null 2>&1; then
      if jq -e '.content // .items' <<<"$page" >/dev/null 2>&1; then
        page="$(jq -c '.content // .items' <<<"$page")"
      else
        # empty or unexpected
        break
      fi
    fi

    # content-range total (if available)
    if [[ "$first" == "1" && -f "$LAST_CONTENT_RANGE_FILE" ]]; then
      content_range="$(<"$LAST_CONTENT_RANGE_FILE")"
      total="$(awk -F'/' '{print $2}' <<<"$content_range" | tr -d '\r\n' || true)"
      [[ -n "$total" ]] && echo "  PowerStore API reports total volumes: $total"
      first=0
    fi

    # Append objects
    jq -c '.[]' <<<"$page" >> "$allfile"

    # Stop if fewer than limit returned
    local count
    count="$(jq 'length' <<<"$page")"
    if [[ "$count" -lt "$limit" ]]; then
      break
    fi
    offset=$((offset + limit))
    if [[ "$offset" -gt 2000000 ]]; then
      echo "  Safety break: offset too large"
      break
    fi
  done

  jq -s '.' "$allfile"
}

csv_escape(){ local s="${1:-}"; s="${s//\"/\"\"}"; printf '"%s"' "$s"; }

# --- OpenShift PV handles ---
echo "Collecting PowerStore CSI PV volumeHandles from OpenShift..."
kubectl get pv -o json > "$TMPDIR/pv.json"
jq -r --arg d "$CSI_DRIVER" '.items[]? | select(.spec.csi.driver==$d) | .spec.csi.volumeHandle' "$TMPDIR/pv.json" | sort -u > "$TMPDIR/inuse_handles_all.txt"
INUSE_COUNT="$(wc -l < "$TMPDIR/inuse_handles_all.txt" | tr -d ' ')"
echo "  Found in-use volumeHandles (all namespaces): $INUSE_COUNT"

# --- Optional DataVolume protection ---
DV_PROTECTED_SET="$TMPDIR/dv_protected.txt"
if [[ "$PROTECT_DV" == "1" ]]; then
  echo "Collecting KubeVirt DataVolumes to protect referenced PVs..."
  if kubectl get dv -A -o json > "$TMPDIR/dv.json" 2>/dev/null; then
    jq -r '.items[]? | (.spec.pvc.volumeName // empty)' "$TMPDIR/dv.json" | sort -u > "$DV_PROTECTED_SET"
    echo "  DataVolumes referencing PVs: $(wc -l < "$DV_PROTECTED_SET" | tr -d ' ')"
  else
    echo "  Warning: failed to fetch DataVolumes. Continuing."
    : > "$DV_PROTECTED_SET"
  fi
fi

# --- PowerStore volumes ---
echo "Collecting PowerStore volumes (paginated)..."
VOL_JSON="$(fetch_all_volumes)" || die "Failed fetching volumes"
echo "$VOL_JSON" > "$TMPDIR/volumes.json"
TOTAL_VOLS="$(jq 'length' "$TMPDIR/volumes.json")"
echo "  Total volumes fetched from list API: $TOTAL_VOLS"

# Determine if list includes name
HAS_NAME=0
if jq -e '.[0] | has("name")' "$TMPDIR/volumes.json" >/dev/null 2>&1; then
  HAS_NAME=1
fi

# Apply --k8s-only using name prefix when available
if [[ "$K8S_ONLY" == "1" ]]; then
  if [[ "$HAS_NAME" == "1" ]]; then
    jq '[ .[] | select((.name // "" | test("^pvc-"))) ]' "$TMPDIR/volumes.json" > "$TMPDIR/volumes_k8s.json"
    mv "$TMPDIR/volumes_k8s.json" "$TMPDIR/volumes.json"
    echo "  After --k8s-only filter (name starts with pvc-): $(jq 'length' "$TMPDIR/volumes.json")"
  else
    echo "  Warning: --k8s-only requested but volume names are not present in list response; skipping k8s-only filter."
  fi
fi

# Build list of volume IDs from PowerStore
jq -r '.[].id' "$TMPDIR/volumes.json" | sort -u > "$TMPDIR/ps_ids.txt"
PS_ID_COUNT="$(wc -l < "$TMPDIR/ps_ids.txt" | tr -d ' ')"

# Orphan-by-reference: IDs on array not referenced by any PV volumeHandle
# (this is fast and does not require per-volume calls)
comm -23 "$TMPDIR/ps_ids.txt" "$TMPDIR/inuse_handles_all.txt" > "$TMPDIR/orphan_ids.txt"
ORPHAN_ID_COUNT="$(wc -l < "$TMPDIR/orphan_ids.txt" | tr -d ' ')"
echo "Computing orphan candidates (not referenced by any PV volumeHandle)..."
echo "  Orphan IDs (unreferenced): $ORPHAN_ID_COUNT"
echo

# CSV header
{
  echo "run_timestamp,namespace_scope,dry_run,delete_count,verify_max,action,volume_id,volume_name,created_timestamp,mapped,age_days,verified,eligible_for_delete,reason,pv_name,pvc_namespace,pvc_name,protected_by_datavolume"
} > "$REPORT"

# Helper: lookup name/created in volumes.json without extra API calls
lookup_list_fields() {
  local vid="$1"
  jq -c --arg id "$vid" '.[] | select(.id==$id) | {id, name, created_timestamp}' "$TMPDIR/volumes.json" 2>/dev/null | head -n 1
}

# Verify a volume via per-volume endpoint
# Returns JSON on stdout (or {} on failure)
get_volume_details() {
  local vid="$1"
  local resp
  resp="$(ps_api "/api/rest/volume/${vid}" || true)"
  if [[ -z "$resp" ]]; then echo "{}"; return 0; fi
  # If PowerStore returns messages error, treat as empty
  if jq -e '.messages? // empty' <<<"$resp" >/dev/null 2>&1; then
    if [[ "$DEBUG" == "1" ]]; then
      echo "[debug] /volume/${vid} returned messages; preview:"
      printf '%s\n' "$resp" | head -n 40
    fi
    echo "{}"
    return 0
  fi
  echo "$resp"
}

DELETE_ATTEMPTED=0
VERIFIED_COUNT=0
ELIGIBLE_VERIFIED=0

# For preview printing
VERIFIED_ELIGIBLE_IDS=()

echo "Analyzing candidates and writing CSV report..."
while IFS= read -r VOL_ID; do
  # Default fields (from list)
  LIST_OBJ="$(lookup_list_fields "$VOL_ID" || true)"
  VOL_NAME="$(jq -r '.name // ""' <<<"${LIST_OBJ:-{}}")"
  CREATED_TS="$(jq -r '.created_timestamp // ""' <<<"${LIST_OBJ:-{}}")"

  # Default verification outputs
  MAPPED="unknown"
  PVC_NS=""
  PVC_NAME=""
  PV_NAME=""
  PROTECTED_BY_DV="no"
  VERIFIED="no"
  ELIGIBLE="no"
  REASON="Unreferenced by PV volumeHandle (not yet verified)"

  # Decide whether to verify via per-volume GET:
  # - Always verify if we might delete (DELETE_COUNT>0) and we haven't reached limit
  # - In dry-run, verify up to VERIFY_MAX to keep runtime bounded
  DO_VERIFY=0
  if [[ "$DELETE_COUNT" -gt 0 ]]; then
    # if we are going to delete, we must verify candidates until we hit DELETE_COUNT deletes
    DO_VERIFY=1
  elif [[ "$DRY_RUN" == "1" && "$VERIFIED_COUNT" -lt "$VERIFY_MAX" ]]; then
    DO_VERIFY=1
  fi

  if [[ "$DO_VERIFY" == "1" ]]; then
    DETAILS_JSON="$(get_volume_details "$VOL_ID")"
    if [[ "$DETAILS_JSON" != "{}" ]]; then
      VERIFIED="yes"
      VERIFIED_COUNT=$((VERIFIED_COUNT + 1))

      # mapped is available on detail endpoint (typically)
      MAPPED="$(jq -r 'if has("mapped") then (if .mapped==true then "true" else "false" end) else "unknown" end' <<<"$DETAILS_JSON" 2>/dev/null || echo "unknown")"
      PVC_NS="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/namespace"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
      PVC_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
      PV_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pv/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"

      # DV protection
      if [[ "$PROTECT_DV" == "1" && -n "$PV_NAME" ]]; then
        if grep -qFx "$PV_NAME" "$DV_PROTECTED_SET" 2>/dev/null; then
          PROTECTED_BY_DV="yes"
        fi
      fi

      # Determine eligibility (safe delete)
      # Must be unmapped + (if namespace scoped, metadata ns must match) + not protected by DV
      if [[ "$MAPPED" == "false" ]]; then
        if [[ -n "$NAMESPACE" ]]; then
          if [[ -z "$PVC_NS" ]]; then
            ELIGIBLE="no"
            REASON="Verified unmapped, but namespace-scoped run and PVC namespace metadata missing"
          elif [[ "$PVC_NS" != "$NAMESPACE" ]]; then
            ELIGIBLE="no"
            REASON="Verified unmapped, but PVC namespace does not match --ns"
          else
            ELIGIBLE="yes"
            REASON="Verified unmapped, unreferenced, namespace match"
          fi
        else
          ELIGIBLE="yes"
          REASON="Verified unmapped and unreferenced"
        fi

        if [[ "$PROTECTED_BY_DV" == "yes" ]]; then
          ELIGIBLE="no"
          REASON="Verified unmapped but protected by DataVolume"
        fi
      else
        ELIGIBLE="no"
        REASON="Verified but not unmapped (mapped=${MAPPED})"
      fi
    else
      VERIFIED="no"
      REASON="Failed to verify volume details (per-volume GET failed)"
    fi
  fi

  # age_days (best-effort)
  age_days=""
  if [[ -n "$CREATED_TS" ]]; then
    if date -d "$CREATED_TS" >/dev/null 2>&1; then
      age_days=$(( ( $(date +%s) - $(date -d "$CREATED_TS" +%s) ) / 86400 ))
    fi
  fi

  # Record CSV row
  {
    csv_escape "$TS_NOW"; echo -n ","
    csv_escape "${NAMESPACE:-}"; echo -n ","
    csv_escape "$DRY_RUN"; echo -n ","
    csv_escape "$DELETE_COUNT"; echo -n ","
    csv_escape "$VERIFY_MAX"; echo -n ","
    csv_escape "report"; echo -n ","
    csv_escape "$VOL_ID"; echo -n ","
    csv_escape "$VOL_NAME"; echo -n ","
    csv_escape "$CREATED_TS"; echo -n ","
    csv_escape "$MAPPED"; echo -n ","
    csv_escape "$age_days"; echo -n ","
    csv_escape "$VERIFIED"; echo -n ","
    csv_escape "$ELIGIBLE"; echo -n ","
    csv_escape "$REASON"; echo -n ","
    csv_escape "$PV_NAME"; echo -n ","
    csv_escape "$PVC_NS"; echo -n ","
    csv_escape "$PVC_NAME"; echo -n ","
    csv_escape "$PROTECTED_BY_DV"
    echo
  } >> "$REPORT"

  # Track verified eligible for preview
  if [[ "$VERIFIED" == "yes" && "$ELIGIBLE" == "yes" ]]; then
    ELIGIBLE_VERIFIED=$((ELIGIBLE_VERIFIED + 1))
    VERIFIED_ELIGIBLE_IDS+=("$VOL_ID")
  fi

  # If deletions requested and not dry-run, perform delete only after verification says eligible
  if [[ "$DRY_RUN" == "0" && "$DELETE_COUNT" -gt 0 && "$DELETE_ATTEMPTED" -lt "$DELETE_COUNT" ]]; then
    if [[ "$VERIFIED" != "yes" ]]; then
      # Must verify before deletion; if verify was skipped due to logic, force it here
      DETAILS_JSON="$(get_volume_details "$VOL_ID")"
      if [[ "$DETAILS_JSON" == "{}" ]]; then
        continue
      fi
      MAPPED="$(jq -r 'if has("mapped") then (if .mapped==true then "true" else "false" end) else "unknown" end' <<<"$DETAILS_JSON" 2>/dev/null || echo "unknown")"
      PVC_NS="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/namespace"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
      PVC_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
      PV_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pv/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
      PROTECTED_BY_DV="no"
      if [[ "$PROTECT_DV" == "1" && -n "$PV_NAME" ]]; then
        if grep -qFx "$PV_NAME" "$DV_PROTECTED_SET" 2>/dev/null; then
          PROTECTED_BY_DV="yes"
        fi
      fi
      ELIGIBLE="no"
      if [[ "$MAPPED" == "false" && "$PROTECTED_BY_DV" == "no" ]]; then
        if [[ -n "$NAMESPACE" ]]; then
          [[ -n "$PVC_NS" && "$PVC_NS" == "$NAMESPACE" ]] && ELIGIBLE="yes"
        else
          ELIGIBLE="yes"
        fi
      fi
    fi

    if [[ "$ELIGIBLE" == "yes" ]]; then
      if ps_api_delete "/api/rest/volume/${VOL_ID}" >/dev/null 2>&1; then
        DELETE_ATTEMPTED=$((DELETE_ATTEMPTED + 1))
        {
          csv_escape "$TS_NOW"; echo -n ","
          csv_escape "${NAMESPACE:-}"; echo -n ","
          csv_escape "$DRY_RUN"; echo -n ","
          csv_escape "$DELETE_COUNT"; echo -n ","
          csv_escape "$VERIFY_MAX"; echo -n ","
          csv_escape "delete"; echo -n ","
          csv_escape "$VOL_ID"; echo -n ","
          csv_escape "$VOL_NAME"; echo -n ","
          csv_escape "$CREATED_TS"; echo -n ","
          csv_escape "$MAPPED"; echo -n ","
          csv_escape "$age_days"; echo -n ","
          csv_escape "yes"; echo -n ","
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
          csv_escape "$VERIFY_MAX"; echo -n ","
          csv_escape "delete_failed"; echo -n ","
          csv_escape "$VOL_ID"; echo -n ","
          csv_escape "$VOL_NAME"; echo -n ","
          csv_escape "$CREATED_TS"; echo -n ","
          csv_escape "$MAPPED"; echo -n ","
          csv_escape "$age_days"; echo -n ","
          csv_escape "yes"; echo -n ","
          csv_escape "yes"; echo -n ","
          csv_escape "Delete attempt failed"; echo -n ","
          csv_escape "$PV_NAME"; echo -n ","
          csv_escape "$PVC_NS"; echo -n ","
          csv_escape "$PVC_NAME"; echo -n ","
          csv_escape "$PROTECTED_BY_DV"
          echo
        } >> "$REPORT"
      fi
    fi
  fi

done < "$TMPDIR/orphan_ids.txt"

echo
echo "Done."
echo "  Orphan IDs (unreferenced)            : $ORPHAN_ID_COUNT"
echo "  Verified candidates (bounded)        : $VERIFIED_COUNT (max $VERIFY_MAX in dry-run)"
echo "  Verified eligible (safe) candidates  : $ELIGIBLE_VERIFIED"
if [[ "$DRY_RUN" == "1" ]]; then
  echo "  Deletes attempted                    : 0 (dry-run)"
else
  echo "  Deletes attempted                    : $DELETE_ATTEMPTED"
fi
echo "  CSV report                           : $REPORT"

if [[ "$PREVIEW_N" -gt 0 ]]; then
  echo
  echo "Preview: first $PREVIEW_N verified-eligible volume IDs:"
  printf '%s\n' "${VERIFIED_ELIGIBLE_IDS[@]}" | head -n "$PREVIEW_N"
fi

echo
echo "Notes:"
echo " - Bulk list endpoint on your array does not support select=mapped or select=metadata."
echo "   Safe deletion is therefore verified via per-volume GET for a bounded number in dry-run,"
echo "   and always for any volume actually deleted."
echo " - Increase --verify-max if you want more verified rows in the CSV (at the cost of time)."
echo " - Use --dc with small values for safe batch deletions."
