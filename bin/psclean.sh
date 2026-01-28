#!/usr/bin/env bash
# psclean.sh - PowerStore/OpenShift reconciliation (patched dump-details unbound-var fix)
set -euo pipefail

CSI_DRIVER="${CSI_DRIVER:-csi-powerstore.dellemc.com}"
PSTORE_INSECURE="${PSTORE_INSECURE:-1}"
PSTORE_LOGIN_RETRIES="${PSTORE_LOGIN_RETRIES:-10}"
PSTORE_LOGIN_BACKOFF="${PSTORE_LOGIN_BACKOFF:-2}"
PSTORE_LOGIN_POST_SLEEP="${PSTORE_LOGIN_POST_SLEEP:-1}"

# Flags/defaults
DRY_RUN=0
DELETE_COUNT=0
VERIFY_MAX=500
NAMESPACE=""
PROTECT_DV=0
FORCE_RELOGIN=0
K8S_ONLY=0
PREVIEW_N=0
DEBUG=0
DUMP_DETAILS=0
DUMP_MAX=0
USE_ADMIN=0

usage(){
  cat <<'USAGE'
Usage:
  PSTORE_IP=... ./psclean.sh [--dr|--dry-run] [--dc N|--delete-count N] [--ns NS]
                             [--protect-dv] [--relogin] [--k8s-only] [--preview N]
                             [--verify-max N] [--debug] [--dump-details N] [--admin-user]
USAGE
}

die(){ echo "ERROR: $*" >&2; exit 2; }

# parse args
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
    --dump-details) [[ $# -ge 2 ]] || die "Missing value for $1"; DUMP_DETAILS=1; DUMP_MAX="$2"; shift 2 ;;
    --admin-user) USE_ADMIN=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

[[ -n "${PSTORE_IP:-}" ]] || die "PSTORE_IP required (PowerStore mgmt IP/FQDN)"
[[ "$DELETE_COUNT" =~ ^[0-9]+$ ]] || die "--dc must be integer"
[[ "$VERIFY_MAX" =~ ^[0-9]+$ ]] || die "--verify-max must be integer"
[[ "$DUMP_MAX" =~ ^[0-9]*$ ]] || die "--dump-details must be integer"

CURL_TLS=(); [[ "$PSTORE_INSECURE" == "1" ]] && CURL_TLS=(-k)

SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(dirname "$SCRIPT_PATH")"
SESSION_DIR="$SCRIPT_DIR/pstore_session"
REPORT_DIR="$SCRIPT_DIR/storage_reports"
DETAIL_DIR="$REPORT_DIR/details"
mkdir -p "$SESSION_DIR" "$REPORT_DIR" "$DETAIL_DIR"
chmod 700 "$SESSION_DIR"

TS_NOW="$(date +%Y%m%d_%H%M%S)"
REPORT="$REPORT_DIR/psclean_report_${TS_NOW}.csv"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

COOKIE_FILE_ON_DISK="$SESSION_DIR/cookie.file"
TOKEN_FILE_ON_DISK="$SESSION_DIR/token.txt"
ADMIN_COOKIE="$SESSION_DIR/admin_cookie.file"
ADMIN_TOKEN_FILE="$SESSION_DIR/admin_token.txt"

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
echo "  Dump details    : $DUMP_DETAILS (N=${DUMP_MAX:-0})"
echo "  Use admin       : $USE_ADMIN"
echo "  Report          : $REPORT"
echo "  Session dir     : $SESSION_DIR"
echo

if [[ "$FORCE_RELOGIN" == "1" ]]; then
  echo "Forcing re-login: removing stored session if present..."
  rm -f "$COOKIE_FILE_ON_DISK" "$TOKEN_FILE_ON_DISK" "$ADMIN_COOKIE" "$ADMIN_TOKEN_FILE" || true
fi

load_session_if_exists(){
  if [[ -f "$COOKIE_FILE_ON_DISK" && -f "$TOKEN_FILE_ON_DISK" ]]; then
    PSTORE_COOKIEFILE="$COOKIE_FILE_ON_DISK"
    PSTORE_TOKEN="$(<"$TOKEN_FILE_ON_DISK")"
    chmod 600 "$COOKIE_FILE_ON_DISK" "$TOKEN_FILE_ON_DISK" || true
    export PSTORE_COOKIEFILE PSTORE_TOKEN
    return 0
  fi
  return 1
}
load_admin_session_if_exists(){
  if [[ -f "$ADMIN_COOKIE" && -f "$ADMIN_TOKEN_FILE" ]]; then
    PSTORE_ADMIN_COOKIE="$ADMIN_COOKIE"
    PSTORE_ADMIN_TOKEN="$(<"$ADMIN_TOKEN_FILE")"
    chmod 600 "$ADMIN_COOKIE" "$ADMIN_TOKEN_FILE" || true
    export PSTORE_ADMIN_COOKIE PSTORE_ADMIN_TOKEN
    return 0
  fi
  return 1
}

_login_and_store_session(){
  local user="$1" pass="$2" cookie_out="$3" token_out="$4" debug="$5"
  local tries=0 max="$PSTORE_LOGIN_RETRIES" backoff="$PSTORE_LOGIN_BACKOFF"
  while (( tries < max )); do
    tries=$((tries+1))
    tmpcookie="$(mktemp)"
    hdrfile="${tmpcookie}.hdr"
    if [[ "$debug" == "1" ]]; then
      curl "${CURL_TLS[@]}" -v -X GET -H "Accept: application/json" -H "Content-Type: application/json" \
        -u "${user}:${pass}" -c "${tmpcookie}" "https://${PSTORE_IP}/api/rest/login_session" -D "${hdrfile}" 2>&1 | sed -n '1,160p'
    else
      curl "${CURL_TLS[@]}" -s -X GET -H "Accept: application/json" -H "Content-Type: application/json" \
        -u "${user}:${pass}" -c "${tmpcookie}" "https://${PSTORE_IP}/api/rest/login_session" -D "${hdrfile}" >/dev/null 2>&1 || true
    fi
    token="$(awk -F': ' '/DELL-EMC-TOKEN/ {print $2; exit}' "$hdrfile" | tr -d $'\r\n' || true)"
    if [[ -z "$token" ]]; then
      rm -f "$tmpcookie" "$hdrfile"
      echo "  login attempt $tries: missing token; retrying after ${backoff}s..."
      sleep "$backoff"; backoff=$((backoff*2))
      continue
    fi
    [[ "$PSTORE_LOGIN_POST_SLEEP" -gt 0 ]] && sleep "$PSTORE_LOGIN_POST_SLEEP"
    status="$(curl -sS "${CURL_TLS[@]}" -w "%{http_code}" -o /dev/null -H "DELL-EMC-TOKEN: ${token}" -b "$tmpcookie" "https://${PSTORE_IP}/api/rest/volume?limit=1" 2>/dev/null || true)"
    if [[ "$status" == "200" || "$status" == "206" ]]; then
      mv "$tmpcookie" "$cookie_out"
      printf "%s" "$token" > "$token_out"
      chmod 600 "$cookie_out" "$token_out"
      rm -f "$hdrfile"
      return 0
    fi
    rm -f "$tmpcookie" "$hdrfile"
    echo "  login attempt $tries: validation HTTP $status; retrying after ${backoff}s..."
    sleep "$backoff"; backoff=$((backoff*2))
  done
  return 1
}

ensure_session(){
  if [[ -n "${PSTORE_COOKIEFILE:-}" && -n "${PSTORE_TOKEN:-}" ]]; then return 0; fi
  if load_session_if_exists; then return 0; fi
  if [[ -n "${PSTORE_USER:-}" && -n "${PSTORE_PASS:-}" ]]; then
    _login_and_store_session "$PSTORE_USER" "$PSTORE_PASS" "$COOKIE_FILE_ON_DISK" "$TOKEN_FILE_ON_DISK" "$DEBUG" || die "Failed to login (user session)"
    PSTORE_COOKIEFILE="$COOKIE_FILE_ON_DISK"; PSTORE_TOKEN="$(<"$TOKEN_FILE_ON_DISK")"; export PSTORE_COOKIEFILE PSTORE_TOKEN
    return 0
  fi
  read -rp "PowerStore username: " PSTORE_USER
  read -s -rp "PowerStore password for ${PSTORE_USER}: " PSTORE_PASS; echo
  _login_and_store_session "$PSTORE_USER" "$PSTORE_PASS" "$COOKIE_FILE_ON_DISK" "$TOKEN_FILE_ON_DISK" "$DEBUG" || die "Failed to login (user session)"
  PSTORE_COOKIEFILE="$COOKIE_FILE_ON_DISK"; PSTORE_TOKEN="$(<"$TOKEN_FILE_ON_DISK")"; export PSTORE_COOKIEFILE PSTORE_TOKEN
}

ensure_admin_session(){
  if load_admin_session_if_exists; then return 0; fi
  read -rp "PowerStore admin user: " PSTORE_ADMIN_USER
  read -s -rp "PowerStore admin password: " PSTORE_ADMIN_PASS; echo
  if _login_and_store_session "$PSTORE_ADMIN_USER" "$PSTORE_ADMIN_PASS" "$ADMIN_COOKIE" "$ADMIN_TOKEN_FILE" "$DEBUG"; then
    PSTORE_ADMIN_COOKIE="$ADMIN_COOKIE"; PSTORE_ADMIN_TOKEN="$(<"$ADMIN_TOKEN_FILE")"; export PSTORE_ADMIN_COOKIE PSTORE_ADMIN_TOKEN
    echo "Stored admin session (for verification) in $SESSION_DIR"
    return 0
  fi
  return 1
}

ps_api(){
  local path="$1"
  ensure_session
  local hdrf bodyf
  hdrf="$(mktemp)"; bodyf="$(mktemp)"
  if ! curl -sS "${CURL_TLS[@]}" -D "$hdrf" -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -H "Accept: application/json" -b "${PSTORE_COOKIEFILE}" "https://${PSTORE_IP}${path}" -o "$bodyf"; then
    echo "[PowerStore] GET $path failed (curl)." >&2
    [[ "$DEBUG" == "1" ]] && { sed -n '1,80p' "$hdrf" || true; head -c 512 "$bodyf" || true; }
    rm -f "$hdrf" "$bodyf"; return 1
  fi
  if ! jq -e . "$bodyf" >/dev/null 2>&1; then
    echo "[PowerStore] GET $path returned NON-JSON; preview (200 chars):" >&2
    head -c 200 "$bodyf"; echo >&2
    rm -f "$hdrf" "$bodyf"; return 1
  fi
  cat "$bodyf"; rm -f "$hdrf" "$bodyf"
}

ps_api_admin(){
  local path="$1"
  if [[ -n "${PSTORE_ADMIN_TOKEN:-}" && -n "${PSTORE_ADMIN_COOKIE:-}" ]]; then
    local hdrf bodyf
    hdrf="$(mktemp)"; bodyf="$(mktemp)"
    if ! curl -sS "${CURL_TLS[@]}" -D "$hdrf" -H "DELL-EMC-TOKEN: ${PSTORE_ADMIN_TOKEN}" -H "Accept: application/json" -b "${PSTORE_ADMIN_COOKIE}" "https://${PSTORE_IP}${path}" -o "$bodyf"; then
      echo "[PowerStore(admin)] GET $path failed (curl)." >&2
      [[ "$DEBUG" == "1" ]] && { sed -n '1,80p' "$hdrf" || true; head -c 512 "$bodyf" || true; }
      rm -f "$hdrf" "$bodyf"; return 1
    fi
    if ! jq -e . "$bodyf" >/dev/null 2>&1; then
      echo "[PowerStore(admin)] GET $path returned NON-JSON; preview (200 chars):" >&2
      head -c 200 "$bodyf"; echo >&2
      rm -f "$hdrf" "$bodyf"; return 1
    fi
    cat "$bodyf"; rm -f "$hdrf" "$bodyf"; return 0
  else
    ps_api "$path"
  fi
}

ps_api_delete(){
  local path="$1"
  ensure_session
  curl -sS "${CURL_TLS[@]}" -X DELETE -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -b "${PSTORE_COOKIEFILE}" "https://${PSTORE_IP}${path}"
}

fetch_vol_page(){
  local limit="$1" offset="$2" resp
  resp="$(ps_api "/api/rest/volume?limit=${limit}&offset=${offset}&select=id,name" || true)"
  if [[ -n "$resp" ]] && jq -e 'type=="array"' <<<"$resp" >/dev/null 2>&1; then
    echo "$resp"; return 0
  fi
  ps_api "/api/rest/volume?limit=${limit}&offset=${offset}"
}

fetch_all_volumes(){
  local limit=200 offset=0 all="$TMPDIR/vols.ndjson"; : > "$all"
  local page count first=1
  while true; do
    page="$(fetch_vol_page "$limit" "$offset")" || die "Failed fetching volumes"
    if ! jq -e 'type=="array"' <<<"$page" >/dev/null 2>&1; then
      if jq -e '.content // .items' <<<"$page" >/dev/null 2>&1; then
        page="$(jq -c '.content // .items' <<<"$page")"
      else
        die "Unexpected volume list response shape at offset=$offset"
      fi
    fi
    if [[ "$first" == "1" && "$DEBUG" == "1" ]]; then
      echo "---- [debug] first list page preview ----"
      printf '%s\n' "$page" | head -n 40
      echo "---- [debug] end ----"
    fi
    first=0
    count="$(jq 'length' <<<"$page")"
    jq -c '.[]' <<<"$page" >> "$all"
    if [[ "$count" -lt "$limit" ]]; then break; fi
    offset=$((offset + limit))
    if [[ "$offset" -gt 2000000 ]]; then echo "Safety break: offset too large"; break; fi
  done
  jq -s '.' "$all"
}

csv_escape(){ local s="${1:-}"; s="${s//\"/\"\"}"; printf '"%s"' "$s"; }

echo "Collecting PowerStore CSI PV volumeHandles from OpenShift..."
kubectl get pv -o json > "$TMPDIR/pv.json"
jq -r --arg d "$CSI_DRIVER" '.items[]? | select(.spec.csi.driver==$d) | .spec.csi.volumeHandle' "$TMPDIR/pv.json" | sort -u > "$TMPDIR/inuse_handles_all.txt"
INUSE_COUNT="$(wc -l < "$TMPDIR/inuse_handles_all.txt" | tr -d ' ')"
echo "  Found in-use volumeHandles (all namespaces): $INUSE_COUNT"

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

echo "Collecting PowerStore volumes (paginated)..."
VOL_JSON="$(fetch_all_volumes)" || die "Failed fetching volumes"
echo "$VOL_JSON" > "$TMPDIR/volumes.json"
TOTAL_VOLS="$(jq 'length' "$TMPDIR/volumes.json")"
echo "  Total volumes fetched from list API: $TOTAL_VOLS"

HAS_NAME=0
if jq -e '.[0] | has("name")' "$TMPDIR/volumes.json" >/dev/null 2>&1; then HAS_NAME=1; fi

if [[ "$K8S_ONLY" == "1" ]]; then
  if [[ "$HAS_NAME" == "1" ]]; then
    jq '[ .[] | select((.name // "" | test("^pvc-"))) ]' "$TMPDIR/volumes.json" > "$TMPDIR/volumes_k8s.json"
    mv "$TMPDIR/volumes_k8s.json" "$TMPDIR/volumes.json"
    echo "  After --k8s-only filter (pvc-* names): $(jq 'length' "$TMPDIR/volumes.json")"
  else
    echo "  Warning: --k8s-only requested but list response lacks 'name'; skipping filter."
  fi
fi

jq -r '.[].id' "$TMPDIR/volumes.json" | sort -u > "$TMPDIR/ps_ids.txt"
comm -23 "$TMPDIR/ps_ids.txt" "$TMPDIR/inuse_handles_all.txt" > "$TMPDIR/orphan_ids.txt"
ORPHAN_ID_COUNT="$(wc -l < "$TMPDIR/orphan_ids.txt" | tr -d ' ')"
echo "Computing orphan candidates (not referenced by any PV volumeHandle)..."
echo "  Orphan IDs (unreferenced): $ORPHAN_ID_COUNT"
echo

{
  echo "run_timestamp,namespace_scope,dry_run,delete_count,verify_max,action,volume_id,volume_name,mapped,verified,eligible_for_delete,reason,pv_name,pvc_namespace,pvc_name,protected_by_datavolume"
} > "$REPORT"

lookup_name(){
  local vid="$1"
  jq -r --arg id "$vid" 'first(.[] | select(.id==$id) | (.name // "")) // ""' "$TMPDIR/volumes.json" 2>/dev/null || true
}

get_volume_details(){
  local vid="${1:-}"
  if [[ -z "$vid" ]]; then echo "{}"; return 0; fi
  if [[ "$USE_ADMIN" == "1" ]]; then
    if ! load_admin_session_if_exists; then
      if ! ensure_admin_session; then
        echo "Warning: admin session unavailable; falling back to normal session for verification." >&2
        ps_api "/api/rest/volume/${vid}" || echo "{}"
        return 0
      fi
    fi
    ps_api_admin "/api/rest/volume/${vid}" || echo "{}"
  else
    ps_api "/api/rest/volume/${vid}" || echo "{}"
  fi
}

dump_detail_for_vid(){
  local vid="${1:-}"
  if [[ -z "$vid" ]]; then
    echo "[dump] called without vid; skipping" >&2
    return 0
  fi
  # ensure session exists before we attempt verbose curl
  if ! ensure_session >/dev/null 2>&1; then
    echo "[dump] no session available for vid=$vid; skipping" >&2
    return 0
  fi
  local out="$DETAIL_DIR/${vid}.txt"
  {
    echo "=== DUMP: $vid ==="
    echo "REQUEST: GET /api/rest/volume/${vid} (verbose output)"
    echo
  } > "$out"
  # verbose headers+body preview
  curl "${CURL_TLS[@]}" -v -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN:-}" -b "${PSTORE_COOKIEFILE:-}" "https://${PSTORE_IP}/api/rest/volume/${vid}" 2>&1 | sed -n '1,400p' >> "$out" || true
  echo >> "$out"
  echo "REQUEST: GET /api/rest/volume/${vid}?select=id,name,mapped,host_mappings (probe)" >> "$out"
  curl "${CURL_TLS[@]}" -s -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN:-}" -b "${PSTORE_COOKIEFILE:-}" \
    "https://${PSTORE_IP}/api/rest/volume/${vid}?select=id,name,mapped,host_mappings" | sed -n '1,200p' >> "$out" || true
  echo >> "$out"
  echo "REQUEST: GET /api/rest/volume/${vid} (admin probe, if admin session present)" >> "$out"
  if load_admin_session_if_exists || [[ -n "${PSTORE_ADMIN_TOKEN:-}" ]]; then
    curl "${CURL_TLS[@]}" -v -H "DELL-EMC-TOKEN: ${PSTORE_ADMIN_TOKEN:-}" -b "${PSTORE_ADMIN_COOKIE:-}" \
      "https://${PSTORE_IP}/api/rest/volume/${vid}" 2>&1 | sed -n '1,200p' >> "$out" || true
  else
    echo "(no admin session available)" >> "$out"
  fi
}

VERIFIED_COUNT=0
ELIGIBLE_VERIFIED=0
DELETE_ATTEMPTED=0
VERIFIED_ELIGIBLE_IDS=()
DUMPED=0

echo "Analyzing candidates and writing CSV report..."
while IFS= read -r VOL_ID; do
  VOL_NAME="$(lookup_name "$VOL_ID")"
  MAPPED="unknown"
  PVC_NS=""; PVC_NAME=""; PV_NAME=""
  PROTECTED_BY_DV="no"
  VERIFIED="no"; ELIGIBLE="no"; REASON="Unreferenced by PV volumeHandle (not verified)"

  # if dump-details requested and under limit, capture raw responses
  if [[ "$DUMP_DETAILS" == "1" && "$DUMPED" -lt "${DUMP_MAX:-0}" ]]; then
    # spawn background dump but guard against unset vars inside function
    dump_detail_for_vid "$VOL_ID" &
    DUMPED=$((DUMPED+1))
  fi

  DO_VERIFY=0
  if [[ "$DELETE_COUNT" -gt 0 ]]; then DO_VERIFY=1; fi
  if [[ "$DRY_RUN" == "1" && "$VERIFIED_COUNT" -lt "$VERIFY_MAX" ]]; then DO_VERIFY=1; fi

  if [[ "$DO_VERIFY" == "1" ]]; then
    DETAILS_JSON="$(get_volume_details "$VOL_ID")"
    if [[ -n "$DETAILS_JSON" && "$DETAILS_JSON" != "{}" ]]; then
      VERIFIED="yes"
      VERIFIED_COUNT=$((VERIFIED_COUNT+1))
      MAPPED="$(jq -r 'if has("mapped") then (if .mapped==true then "true" else "false" end) else "unknown" end' <<<"$DETAILS_JSON" 2>/dev/null || echo "unknown")"
      PV_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pv/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
      PVC_NS="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/namespace"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
      PVC_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
      if [[ "$PROTECT_DV" == "1" && -n "$PV_NAME" && -f "$DV_PROTECTED_SET" ]]; then
        if grep -qFx "$PV_NAME" "$DV_PROTECTED_SET" 2>/dev/null; then PROTECTED_BY_DV="yes"; fi
      fi
      if [[ "$MAPPED" == "false" ]]; then
        if [[ -n "$NAMESPACE" ]]; then
          if [[ -z "$PVC_NS" ]]; then ELIGIBLE="no"; REASON="Namespace-scoped run: missing PVC namespace metadata"; fi
          if [[ -n "$PVC_NS" && "$PVC_NS" != "$NAMESPACE" ]]; then ELIGIBLE="no"; REASON="Namespace mismatch"; fi
          if [[ -n "$PVC_NS" && "$PVC_NS" == "$NAMESPACE" ]]; then ELIGIBLE="yes"; REASON="Verified unmapped and namespace match"; fi
        else
          ELIGIBLE="yes"; REASON="Verified unmapped and unreferenced"
        fi
        if [[ "$PROTECTED_BY_DV" == "yes" ]]; then ELIGIBLE="no"; REASON="Protected by DataVolume"; fi
      else
        ELIGIBLE="no"; REASON="Verified but mapped=${MAPPED}"
      fi
    else
      VERIFIED="no"
      REASON="Per-volume verification failed or returned minimal data"
    fi
  fi

  {
    csv_escape "$TS_NOW"; echo -n ","
    csv_escape "${NAMESPACE:-}"; echo -n ","
    csv_escape "$DRY_RUN"; echo -n ","
    csv_escape "$DELETE_COUNT"; echo -n ","
    csv_escape "$VERIFY_MAX"; echo -n ","
    csv_escape "report"; echo -n ","
    csv_escape "$VOL_ID"; echo -n ","
    csv_escape "$VOL_NAME"; echo -n ","
    csv_escape "$MAPPED"; echo -n ","
    csv_escape "$VERIFIED"; echo -n ","
    csv_escape "$ELIGIBLE"; echo -n ","
    csv_escape "$REASON"; echo -n ","
    csv_escape "$PV_NAME"; echo -n ","
    csv_escape "$PVC_NS"; echo -n ","
    csv_escape "$PVC_NAME"; echo -n ","
    csv_escape "$PROTECTED_BY_DV"
    echo
  } >> "$REPORT"

  if [[ "$VERIFIED" == "yes" && "$ELIGIBLE" == "yes" ]]; then
    ELIGIBLE_VERIFIED=$((ELIGIBLE_VERIFIED+1))
    VERIFIED_ELIGIBLE_IDS+=("$VOL_ID")
  fi

  if [[ "$DRY_RUN" == "0" && "$DELETE_COUNT" -gt 0 && "$DELETE_ATTEMPTED" -lt "$DELETE_COUNT" ]]; then
    if [[ "$VERIFIED" != "yes" ]]; then
      DETAILS_JSON="$(get_volume_details "$VOL_ID")"
      if [[ "$DETAILS_JSON" == "{}" ]]; then continue; fi
      MAPPED="$(jq -r 'if has("mapped") then (if .mapped==true then "true" else "false" end) else "unknown" end' <<<"$DETAILS_JSON" 2>/dev/null || echo "unknown")"
      PV_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pv/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
      PVC_NS="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/namespace"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
      PVC_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
      if [[ "$MAPPED" == "false" && "$PROTECTED_BY_DV" == "no" ]]; then
        if [[ -n "$NAMESPACE" ]]; then
          [[ -n "$PVC_NS" && "$PVC_NS" == "$NAMESPACE" ]] && ELIGIBLE="yes" || ELIGIBLE="no"
        else
          ELIGIBLE="yes"
        fi
      else
        ELIGIBLE="no"
      fi
    fi

    if [[ "$ELIGIBLE" == "yes" ]]; then
      if ps_api_delete "/api/rest/volume/${VOL_ID}" >/dev/null 2>&1; then
        DELETE_ATTEMPTED=$((DELETE_ATTEMPTED+1))
        {
          csv_escape "$TS_NOW"; echo -n ","
          csv_escape "${NAMESPACE:-}"; echo -n ","
          csv_escape "$DRY_RUN"; echo -n ","
          csv_escape "$DELETE_COUNT"; echo -n ","
          csv_escape "$VERIFY_MAX"; echo -n ","
          csv_escape "delete"; echo -n ","
          csv_escape "$VOL_ID"; echo -n ","
          csv_escape "$VOL_NAME"; echo -n ","
          csv_escape "$MAPPED"; echo -n ","
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
          csv_escape "$MAPPED"; echo -n ","
          csv_escape "yes"; echo -n ","
          csv_escape "no"; echo -n ","
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

wait || true

echo
echo "Done."
echo "  Orphan IDs (unreferenced)           : $ORPHAN_ID_COUNT"
echo "  Verified candidates (bounded)       : $VERIFIED_COUNT (max $VERIFY_MAX in dry-run)"
echo "  Verified eligible (safe) candidates : $ELIGIBLE_VERIFIED"
if [[ "$DRY_RUN" == "1" ]]; then
  echo "  Deletes attempted                    : 0 (dry-run)"
else
  echo "  Deletes attempted                    : $DELETE_ATTEMPTED"
fi
echo "  CSV report                           : $REPORT"
if [[ "$DUMP_DETAILS" == "1" ]]; then
  echo "  Detail dumps saved to               : $DETAIL_DIR (first ${DUMP_MAX:-0} attempted)"
fi

echo
echo "Notes:"
echo " - Bulk list endpoint may not expose mapping/metadata. The admin session (if provided) is used for per-volume verification."
echo " - Volumes with insufficient detail are NOT auto-deleted (conservative safety)."
echo " - Increase --verify-max to verify more candidates (longer run time). Use --dc for small batch deletes."
