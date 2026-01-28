#!/usr/bin/env bash
set -euo pipefail

# psclean.sh
# Reconcile PowerStore volumes vs OpenShift PV volumeHandles.
# On PowerStoreOS 4.1 where per-volume detail fields may be unavailable, this script can
# safely rely on PowerStore DELETE guardrails:
#   - 204 = deleted
#   - 422 = NOT deleted (reason in JSON body; often "attached to <host_id>")
#   - 401 = auth/session issue; script will re-login and retry once
#
# REQUIREMENTS: bash, curl, jq, kubectl
# This script determines “potentially orphaned” volumes purely by Kubernetes reference, then optionally confirms safety via PowerStore delete guardrails.
# How “potentially orphaned” is computed
# From OpenShift, it collects all PVs provisioned by the PowerStore CSI driver and extracts their spec.csi.volumeHandle.
#
# Example:
#
# Given a specific volumeHandle: 093189c9-.../PSe3e88a4bb089/scsi
# The script takes the VID part before the first /: 093189c9-...
# So it builds a set: IN_USE_VIDS = “volumes Kubernetes knows about”.
# From PowerStore, it lists volumes via:
# GET /api/rest/volume?limit=…&offset=… (paged)
# and collects their id values into a set: ARRAY_VIDS.
# Then it computes:
# ORPHAN_CANDIDATES = ARRAY_VIDS − IN_USE_VIDS
# So, in plain terms:
# A PowerStore volume is “potentially orphaned” if it exists on the array but is not referenced by any Kubernetes PV volumeHandle.
# Does it check if a volume is mapped to a host?
# I tried, but couldn't figure out how to do this directly 
# (because you can’t reliably fetch mapped / host_mappings / etc. via the PowerStore REST API).
# 
# When you run the script with --dc N (and not --dr), it does something a bit stronger:
# It attempts DELETE /api/rest/volume/<vid>
# If PowerStore returns 204, the volume was deletable (and is now deleted).
# If PowerStore returns 422, it was not deletable, and the response tells you why (e.g., “attached to <host_id>”).
# That 422 response is effectively PowerStore saying:
# “This volume is not orphaned / not safe to delete because it’s still attached / in use.”
# 
# Because PowerStore 4.1 REST responses don't seem to return a valid “mapping state”, the safest practical method is:
# Use Kubernetes to find “unreferenced by cluster”
# Use PowerStore DELETE to enforce “safe/unsafe” (422 blocks unsafe)
#
# I will work on a better version of this script that will query the PowerStore via ssh cli commands
# to get a list of volumes that aren't mapped to any hosts and cross reference those to openshift instead

########################################
# Defaults / env
########################################
CSI_DRIVER="${CSI_DRIVER:-csi-powerstore.dellemc.com}"
PSTORE_INSECURE="${PSTORE_INSECURE:-1}"                     # 1 => curl -k
PSTORE_CURL_TIMEOUT="${PSTORE_CURL_TIMEOUT:-20}"            # seconds
PSTORE_PAGE_LIMIT="${PSTORE_PAGE_LIMIT:-200}"               # volume list page size
PSTORE_LOGIN_POST_SLEEP="${PSTORE_LOGIN_POST_SLEEP:-1}"     # seconds
PSTORE_SESSION_NO_STORE="${PSTORE_SESSION_NO_STORE:-0}"     # 1 => don't store token/cookie on disk

########################################
# Flags
########################################
DRY_RUN=0
DELETE_COUNT=0
VERIFY_MAX=500             # cap rows processed into CSV in dry-run (and for --dump-details)
NAMESPACE=""
PROTECT_DV=0
FORCE_RELOGIN=0
K8S_ONLY=0
PREVIEW_N=0
DEBUG=0
DUMP_DETAILS=0
DUMP_N=0
YES=0                      # --yes => skip interactive confirmation for deletions

usage(){
  cat <<'USAGE'
Usage:
  PSTORE_IP=... ./psclean.sh [options]

Options:
  --dr | --dry-run              Report-only. No deletes.
  --dc | --delete-count N       Attempt up to N deletions (requires NOT --dr).
  --yes                         Skip interactive confirmation when deleting.
  --ns | --namespace NS         Scope to PVs bound to claims in this namespace (report-only if set).
  --protect-dv                  Protect PVs referenced by KubeVirt DataVolumes (best-effort).
  --relogin                     Force re-login (delete cached cookie/token).
  --k8s-only                    Only consider PowerStore volumes with names starting with 'pvc-'.
  --verify-max N                Cap rows processed into CSV (default 500).
  --preview N                   Print first N orphan candidates to stdout.
  --dump-details N              Dump list pagination headers + N sample per-volume GETs into storage_reports/details/.
  --debug                       Extra logging.

Environment:
  PSTORE_IP                     Required (PowerStore management FQDN/IP)
  CSI_DRIVER                    Default: csi-powerstore.dellemc.com
  PSTORE_INSECURE               Default: 1 (curl -k)
  PSTORE_PAGE_LIMIT             Default: 200
  PSTORE_CURL_TIMEOUT           Default: 20
  PSTORE_SESSION_NO_STORE       Set to 1 to avoid writing cookie/token files

Notes:
  - On your array, GET /api/rest/volume/<id> may return only {"id":...}. That's fine.
  - "Safe to delete" is determined by PowerStore's own DELETE guardrails:
      204 => deleted, 422 => blocked (attached/mapped/etc.), message_l10n explains why.
  - If you use --ns, this script will NOT delete (it will report only), because array-side namespace metadata is unavailable.
USAGE
}

die(){ echo "ERROR: $*" >&2; exit 2; }

########################################
# Parse args
########################################
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dr|--dry-run) DRY_RUN=1; shift ;;
    --dc|--delete-count) [[ $# -ge 2 ]] || die "Missing value for $1"; DELETE_COUNT="$2"; shift 2 ;;
    --yes) YES=1; shift ;;
    --ns|--namespace) [[ $# -ge 2 ]] || die "Missing value for $1"; NAMESPACE="$2"; shift 2 ;;
    --protect-dv) PROTECT_DV=1; shift ;;
    --relogin) FORCE_RELOGIN=1; shift ;;
    --k8s-only) K8S_ONLY=1; shift ;;
    --verify-max) [[ $# -ge 2 ]] || die "Missing value for $1"; VERIFY_MAX="$2"; shift 2 ;;
    --preview) [[ $# -ge 2 ]] || die "Missing value for $1"; PREVIEW_N="$2"; shift 2 ;;
    --dump-details) [[ $# -ge 2 ]] || die "Missing value for $1"; DUMP_DETAILS=1; DUMP_N="$2"; shift 2 ;;
    --debug) DEBUG=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

[[ -n "${PSTORE_IP:-}" ]] || die "PSTORE_IP required"
[[ "$DELETE_COUNT" =~ ^[0-9]+$ ]] || die "--dc must be integer"
[[ "$VERIFY_MAX" =~ ^[0-9]+$ ]] || die "--verify-max must be integer"
[[ "$PREVIEW_N" =~ ^[0-9]+$ ]] || die "--preview must be integer"
[[ "$DUMP_N" =~ ^[0-9]+$ ]] || die "--dump-details must be integer"

if [[ "$DRY_RUN" == "1" && "$DELETE_COUNT" -gt 0 ]]; then
  echo "Warning: --dr is set; --dc will be ignored (no deletes in dry-run)." >&2
fi
if [[ -n "$NAMESPACE" && "$DRY_RUN" == "0" && "$DELETE_COUNT" -gt 0 ]]; then
  echo "Warning: --ns was set; this script will force report-only (no deletes) for safety." >&2
  DRY_RUN=1
  DELETE_COUNT=0
fi

########################################
# Paths
########################################
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

COOKIE_FILE="$SESSION_DIR/cookie.file"
TOKEN_FILE="$SESSION_DIR/token.txt"

csv_escape(){ local s="${1:-}"; s="${s//\"/\"\"}"; printf '"%s"' "$s"; }

log(){ echo "$*"; }
dbg(){ [[ "$DEBUG" == "1" ]] && echo "DEBUG: $*" >&2 || true; }

########################################
# Auth helpers (token + cookie are a pair)
########################################
pstore_validate_session(){
  # returns 0 if session works (HTTP 200/206), else nonzero
  local status
  status="$(curl -sS "${CURL_TLS[@]}" -m "$PSTORE_CURL_TIMEOUT" -w "%{http_code}" -o /dev/null \
           -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -b "${PSTORE_COOKIEFILE}" \
           "https://${PSTORE_IP}/api/rest/volume?limit=1" 2>/dev/null || true)"
  [[ "$status" == "200" || "$status" == "206" ]]
}

pstore_login(){
  local user pass tmpcookie hdrfile token status

  read -rp "PowerStore username: " user
  read -s -rp "PowerStore password for ${user}: " pass; echo

  tmpcookie="$(mktemp)"
  hdrfile="${tmpcookie}.hdr"

  # login_session returns token header + sets auth_cookie
  curl -sS "${CURL_TLS[@]}" -m "$PSTORE_CURL_TIMEOUT" \
    -H "Accept: application/json" -H "Content-Type: application/json" \
    -u "${user}:${pass}" -c "${tmpcookie}" \
    "https://${PSTORE_IP}/api/rest/login_session" -D "${hdrfile}" >/dev/null

  token="$(awk -F': ' '/DELL-EMC-TOKEN/ {print $2; exit}' "$hdrfile" | tr -d $'\r\n' || true)"
  [[ -n "$token" ]] || die "Login failed (missing DELL-EMC-TOKEN)."

  [[ "$PSTORE_LOGIN_POST_SLEEP" -gt 0 ]] && sleep "$PSTORE_LOGIN_POST_SLEEP"

  # validate
  status="$(curl -sS "${CURL_TLS[@]}" -m "$PSTORE_CURL_TIMEOUT" -w "%{http_code}" -o /dev/null \
           -H "DELL-EMC-TOKEN: ${token}" -b "$tmpcookie" \
           "https://${PSTORE_IP}/api/rest/volume?limit=1" 2>/dev/null || true)"
  [[ "$status" == "200" || "$status" == "206" ]] || die "Login validation failed (HTTP $status)."

  PSTORE_COOKIEFILE="$tmpcookie"
  PSTORE_TOKEN="$token"
  export PSTORE_COOKIEFILE PSTORE_TOKEN

  # store (unless disabled)
  if [[ "$PSTORE_SESSION_NO_STORE" != "1" ]]; then
    mv "$tmpcookie" "$COOKIE_FILE"
    printf "%s" "$token" > "$TOKEN_FILE"
    chmod 600 "$COOKIE_FILE" "$TOKEN_FILE"
    PSTORE_COOKIEFILE="$COOKIE_FILE"
    export PSTORE_COOKIEFILE
    rm -f "$hdrfile"
  else
    rm -f "$hdrfile"
  fi
}

ensure_session(){
  # loads cached cookie/token if present, else logs in; validates; retries once
  if [[ "$FORCE_RELOGIN" == "1" ]]; then
    dbg "Forcing relogin: removing cached session files"
    rm -f "$COOKIE_FILE" "$TOKEN_FILE" || true
    FORCE_RELOGIN=0
  fi

  if [[ -f "$COOKIE_FILE" && -f "$TOKEN_FILE" ]]; then
    PSTORE_COOKIEFILE="$COOKIE_FILE"
    PSTORE_TOKEN="$(<"$TOKEN_FILE")"
    export PSTORE_COOKIEFILE PSTORE_TOKEN
    if pstore_validate_session; then
      dbg "Cached session valid"
      return 0
    fi
    dbg "Cached session invalid; relogin required"
    rm -f "$COOKIE_FILE" "$TOKEN_FILE" || true
  fi

  pstore_login
}

########################################
# PowerStore GET helpers
########################################
ps_get_with_headers(){
  local path="$1" hdr_out="$2" body_out="$3"
  curl -sS "${CURL_TLS[@]}" -m "$PSTORE_CURL_TIMEOUT" -D "$hdr_out" -o "$body_out" \
    -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -H "Accept: application/json" \
    -b "${PSTORE_COOKIEFILE}" \
    "https://${PSTORE_IP}${path}"
}

ps_get(){
  local path="$1"
  curl -sS "${CURL_TLS[@]}" -m "$PSTORE_CURL_TIMEOUT" \
    -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -H "Accept: application/json" \
    -b "${PSTORE_COOKIEFILE}" \
    "https://${PSTORE_IP}${path}"
}

########################################
# Fetch volumes (paged)
########################################
fetch_all_volumes(){
  # Tries select=id,name. If unsupported, falls back to id-only.
  local limit="$PSTORE_PAGE_LIMIT"
  local offset=0
  local used_select=1
  : > "$TMPDIR/vols.ndjson"
  : > "$TMPDIR/list_headers.txt"

  while true; do
    local hdr="$TMPDIR/page_${offset}.hdr"
    local body="$TMPDIR/page_${offset}.json"
    local path="/api/rest/volume?limit=${limit}&offset=${offset}&select=id,name"

    ps_get_with_headers "$path" "$hdr" "$body" || die "Failed fetching volumes at offset=$offset"

    # record content-range if present
    local cr
    cr="$(awk -F': ' 'tolower($1)=="content-range" {print $2}' "$hdr" | tr -d $'\r\n' || true)"
    [[ -n "$cr" ]] && echo "offset=${offset} content-range=${cr}" >> "$TMPDIR/list_headers.txt"

    # if body is an error message about select/url, fall back once to id-only
    if jq -e '.messages? and (.messages|length>0)' "$body" >/dev/null 2>&1; then
      local msg
      msg="$(jq -r '.messages[0].message_l10n // empty' "$body" 2>/dev/null || true)"
      dbg "List API returned messages at offset=$offset: ${msg}"
      if [[ "$used_select" == "1" ]]; then
        used_select=0
        dbg "Falling back to id-only list (no select=id,name)"
        # retry same offset without select
        path="/api/rest/volume?limit=${limit}&offset=${offset}"
        ps_get_with_headers "$path" "$hdr" "$body" || die "Failed fetching volumes (fallback) at offset=$offset"
      fi
    fi

    # Validate JSON
    if ! jq -e . "$body" >/dev/null 2>&1; then
      [[ "$DEBUG" == "1" ]] && { echo "Non-JSON body preview:"; head -c 300 "$body"; echo; }
      die "Volume list returned non-JSON at offset=$offset"
    fi

    # Expected: array of objects
    local count
    if jq -e 'type=="array"' "$body" >/dev/null 2>&1; then
      count="$(jq 'length' "$body")"
      jq -c '.[]' "$body" >> "$TMPDIR/vols.ndjson"
    else
      # Some versions wrap content/items
      if jq -e '.content? or .items?' "$body" >/dev/null 2>&1; then
        count="$(jq '.content // .items | length' "$body")"
        jq -c '.content // .items | .[]' "$body" >> "$TMPDIR/vols.ndjson"
      else
        die "Unexpected list shape at offset=$offset"
      fi
    fi

    [[ "$count" -gt 0 ]] || break
    if [[ "$count" -lt "$limit" ]]; then break; fi
    offset=$((offset + limit))
    [[ "$offset" -lt 2000000 ]] || break
  done

  # materialize
  jq -s '.' "$TMPDIR/vols.ndjson"
  echo "$used_select" > "$TMPDIR/used_select.flag"
}

########################################
# K8s PV / DV collection
########################################
collect_inuse_handles(){
  # Writes:
  #   $TMPDIR/inuse_handles.txt
  # Also writes:
  #   $TMPDIR/pv_json.json
  # And optionally dv-derived handles to protect
  local pv_json="$TMPDIR/pv.json"
  kubectl get pv -o json > "$pv_json"

  # If --ns: only PVs bound to claimRef.namespace == NAMESPACE
  if [[ -n "$NAMESPACE" ]]; then
    jq -r --arg d "$CSI_DRIVER" --arg ns "$NAMESPACE" '
      .items[]? |
      select(.spec.csi.driver==$d) |
      select((.spec.claimRef.namespace // "") == $ns) |
      .spec.csi.volumeHandle
    ' "$pv_json" | sort -u > "$TMPDIR/inuse_handles.txt"
  else
    jq -r --arg d "$CSI_DRIVER" '
      .items[]? |
      select(.spec.csi.driver==$d) |
      .spec.csi.volumeHandle
    ' "$pv_json" | sort -u > "$TMPDIR/inuse_handles.txt"
  fi

  # Normalize to just the VID portion (before first '/')
  awk -F'/' '{print $1}' "$TMPDIR/inuse_handles.txt" | sort -u > "$TMPDIR/inuse_vids.txt"

  # Protect DataVolumes (best-effort): DV -> spec.pvc.volumeName (PV name) -> PV volumeHandle -> VID
  if [[ "$PROTECT_DV" == "1" ]]; then
    if kubectl get dv -A -o json > "$TMPDIR/dv.json" 2>/dev/null; then
      jq -r '.items[]? | (.spec.pvc.volumeName // empty)' "$TMPDIR/dv.json" | sort -u > "$TMPDIR/dv_pvnames.txt"
      # Map DV PV names to volumeHandles and add to in-use VIDs
      jq -r '
        .items[]? |
        [.metadata.name, (.spec.csi.volumeHandle // empty)] | @tsv
      ' "$pv_json" | awk -F'\t' 'NF==2 && $2!="" {print $1 "\t" $2}' > "$TMPDIR/pvname_to_handle.tsv"

      > "$TMPDIR/dv_handles.txt"
      while IFS= read -r pvname; do
        awk -v pv="$pvname" -F'\t' '$1==pv {print $2}' "$TMPDIR/pvname_to_handle.tsv" >> "$TMPDIR/dv_handles.txt" || true
      done < "$TMPDIR/dv_pvnames.txt"

      awk -F'/' '{print $1}' "$TMPDIR/dv_handles.txt" | sort -u > "$TMPDIR/dv_vids.txt"
      cat "$TMPDIR/inuse_vids.txt" "$TMPDIR/dv_vids.txt" | sort -u > "$TMPDIR/inuse_vids_protected.txt"
      mv "$TMPDIR/inuse_vids_protected.txt" "$TMPDIR/inuse_vids.txt"
      dbg "Protected DV-derived VIDs: $(wc -l < "$TMPDIR/dv_vids.txt" | tr -d ' ')"
    else
      dbg "Failed to fetch DataVolumes; continuing without DV protection."
    fi
  fi
}

########################################
# Delete attempt (captures body + status)
########################################
pstore_delete_volume(){
  # Args: vid
  # Output: prints body (maybe empty) + final line HTTP_STATUS:<code>
  local vid="$1"
  curl -sS "${CURL_TLS[@]}" -m "$PSTORE_CURL_TIMEOUT" -X DELETE \
    -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" \
    -b "${PSTORE_COOKIEFILE}" \
    -w "\nHTTP_STATUS:%{http_code}\n" \
    "https://${PSTORE_IP}/api/rest/volume/${vid}" || true
}

########################################
# Start
########################################
log "Starting psclean..."
log "  Script dir      : $SCRIPT_DIR"
log "  CSI_DRIVER      : $CSI_DRIVER"
log "  Namespace scope : ${NAMESPACE:-<none>}"
log "  Dry run         : $DRY_RUN"
log "  Delete count    : $DELETE_COUNT"
log "  Verify max      : $VERIFY_MAX"
log "  Protect DV      : $PROTECT_DV"
log "  Force relogin   : $FORCE_RELOGIN"
log "  K8s-only filter : $K8S_ONLY"
log "  Preview N       : $PREVIEW_N"
log "  Debug           : $DEBUG"
log "  Dump details    : $DUMP_DETAILS (N=${DUMP_N})"
log "  Report          : $REPORT"
log "  Session dir     : $SESSION_DIR"
log

if [[ "$FORCE_RELOGIN" == "1" ]]; then
  log "Forcing re-login: removing stored session if present..."
  rm -f "$COOKIE_FILE" "$TOKEN_FILE" || true
fi

log "Collecting PowerStore CSI PV volumeHandles from OpenShift..."
collect_inuse_handles
INUSE_COUNT="$(wc -l < "$TMPDIR/inuse_vids.txt" | tr -d ' ')"
log "  Found in-use VIDs (${NAMESPACE:-all namespaces}): $INUSE_COUNT"

log "Collecting PowerStore volumes (paginated)..."
ensure_session
VOL_JSON="$(fetch_all_volumes)"
echo "$VOL_JSON" > "$TMPDIR/volumes.json"
USED_SELECT="$(cat "$TMPDIR/used_select.flag" 2>/dev/null || echo 1)"
TOTAL_VOLS="$(jq 'length' "$TMPDIR/volumes.json")"
log "  Total volumes fetched from list API: $TOTAL_VOLS"
[[ -s "$TMPDIR/list_headers.txt" ]] && log "  (Pagination headers captured)"

# optional dump-details: save list headers and N sample GETs (serial, bounded)
if [[ "$DUMP_DETAILS" == "1" && "$DUMP_N" -gt 0 ]]; then
  dbg "Dumping list headers + ${DUMP_N} sample GET /volume/<id> responses to $DETAIL_DIR"
  cp -f "$TMPDIR/list_headers.txt" "$DETAIL_DIR/list_headers_${TS_NOW}.txt" 2>/dev/null || true
fi

# Apply --k8s-only filter if we have names available
if [[ "$K8S_ONLY" == "1" ]]; then
  if [[ "$USED_SELECT" == "1" ]] && jq -e '.[0].name? or (length==0)' "$TMPDIR/volumes.json" >/dev/null 2>&1; then
    jq '[ .[] | select((.name // "" | test("^pvc-"))) ]' "$TMPDIR/volumes.json" > "$TMPDIR/volumes_k8s.json"
    mv "$TMPDIR/volumes_k8s.json" "$TMPDIR/volumes.json"
    log "  After --k8s-only filter (pvc-* names): $(jq 'length' "$TMPDIR/volumes.json")"
  else
    log "  Warning: --k8s-only requested but volume names not available from list API; skipping name filter."
  fi
fi

# Build array VID list
jq -r '.[].id' "$TMPDIR/volumes.json" | sort -u > "$TMPDIR/ps_vids.txt"

# Orphans = on array but not in in-use PV handles
comm -23 "$TMPDIR/ps_vids.txt" "$TMPDIR/inuse_vids.txt" > "$TMPDIR/orphan_vids.txt"
ORPHAN_COUNT="$(wc -l < "$TMPDIR/orphan_vids.txt" | tr -d ' ')"
log "Computing orphan candidates (not referenced by PV volumeHandle)..."
log "  Orphan VIDs (unreferenced): $ORPHAN_COUNT"
log

# Preview
if [[ "$PREVIEW_N" -gt 0 ]]; then
  log "Previewing first $PREVIEW_N orphan candidates:"
  head -n "$PREVIEW_N" "$TMPDIR/orphan_vids.txt" | nl -ba
  log
fi

# CSV header
# We keep TSV-ish metadata columns as commas, and store delete response summary as columns.
echo "run_timestamp,namespace_scope,dry_run,delete_count,verify_max,action,volume_id,volume_name,referenced_by_k8s,delete_http_status,delete_code,delete_message_l10n,delete_arguments" > "$REPORT"

lookup_name(){
  local vid="$1"
  jq -r --arg id "$vid" 'first(.[] | select(.id==$id) | (.name // "")) // ""' "$TMPDIR/volumes.json" 2>/dev/null || true
}

# Confirm deletes
if [[ "$DRY_RUN" == "0" && "$DELETE_COUNT" -gt 0 && "$YES" != "1" ]]; then
  echo "You are about to attempt up to ${DELETE_COUNT} PowerStore DELETE operations." >&2
  echo "PowerStore will block unsafe deletes (422), but 204 will permanently delete volumes." >&2
  read -rp "Type DELETE to continue: " confirm
  [[ "$confirm" == "DELETE" ]] || die "Confirmation failed. Aborting."
fi

log "Analyzing candidates and writing CSV report..."
PROCESSED=0
DELETES_ATTEMPTED=0

# Process each orphan VID (bounded by VERIFY_MAX for report completeness in dry-run)
while IFS= read -r vid; do
  [[ -n "$vid" ]] || continue
  PROCESSED=$((PROCESSED+1))

  name="$(lookup_name "$vid")"
  referenced="no"

  action="report"
  http_status=""
  del_code=""
  del_msg=""
  del_args=""

  # Optional detail dumps (sample N only)
  if [[ "$DUMP_DETAILS" == "1" && "$DUMP_N" -gt 0 && "$PROCESSED" -le "$DUMP_N" ]]; then
    ps_get "/api/rest/volume/${vid}" > "$DETAIL_DIR/volume_${vid}_${TS_NOW}.json" 2>/dev/null || true
  fi

  # Delete attempt if allowed
  if [[ "$DRY_RUN" == "0" && "$DELETE_COUNT" -gt 0 && "$DELETES_ATTEMPTED" -lt "$DELETE_COUNT" ]]; then
    action="delete_attempt"

    # Make sure session is valid; if not, re-login
    if ! pstore_validate_session; then
      dbg "Session invalid before delete; re-login"
      FORCE_RELOGIN=1
      ensure_session
    fi

    resp="$(pstore_delete_volume "$vid")"
    http_status="$(echo "$resp" | tail -n 1 | sed 's/^HTTP_STATUS://')"
    body="$(echo "$resp" | head -n -1)"

    # If 401, re-login once and retry
    if [[ "$http_status" == "401" ]]; then
      dbg "DELETE got 401; re-login and retry once"
      FORCE_RELOGIN=1
      ensure_session
      resp="$(pstore_delete_volume "$vid")"
      http_status="$(echo "$resp" | tail -n 1 | sed 's/^HTTP_STATUS://')"
      body="$(echo "$resp" | head -n -1)"
    fi

    if [[ "$http_status" == "204" ]]; then
      DELETES_ATTEMPTED=$((DELETES_ATTEMPTED+1))
    fi

    # Parse body if JSON
    if [[ -n "${body//[[:space:]]/}" ]]; then
      if echo "$body" | jq -e . >/dev/null 2>&1; then
        del_code="$(echo "$body" | jq -r '.messages[0].code // ""' 2>/dev/null || true)"
        del_msg="$(echo "$body" | jq -r '.messages[0].message_l10n // ""' 2>/dev/null || true)"
        # arguments array -> pipe-joined
        del_args="$(echo "$body" | jq -r '.messages[0].arguments // [] | map(tostring) | join("|")' 2>/dev/null || true)"
      else
        del_msg="$(echo "$body" | tr -d $'\r\n' | head -c 200)"
      fi
    fi
  fi

  # Write CSV row (continuous, so you never end up with only a header)
  {
    csv_escape "$TS_NOW"; echo -n ","
    csv_escape "${NAMESPACE:-}"; echo -n ","
    csv_escape "$DRY_RUN"; echo -n ","
    csv_escape "$DELETE_COUNT"; echo -n ","
    csv_escape "$VERIFY_MAX"; echo -n ","
    csv_escape "$action"; echo -n ","
    csv_escape "$vid"; echo -n ","
    csv_escape "$name"; echo -n ","
    csv_escape "$referenced"; echo -n ","
    csv_escape "$http_status"; echo -n ","
    csv_escape "$del_code"; echo -n ","
    csv_escape "$del_msg"; echo -n ","
    csv_escape "$del_args"
    echo
  } >> "$REPORT"

  # Cap processing to keep runs fast and predictable (especially in dry-run)
  if [[ "$PROCESSED" -ge "$VERIFY_MAX" ]]; then
    dbg "Reached --verify-max cap ($VERIFY_MAX); stopping processing loop."
    break
  fi

done < "$TMPDIR/orphan_vids.txt"

log
log "Done."
log "  Orphan VIDs (unreferenced) : $ORPHAN_COUNT"
log "  Rows written to CSV        : $PROCESSED"
if [[ "$DRY_RUN" == "1" ]]; then
  log "  Deletes attempted          : 0 (dry-run)"
else
  log "  Deletes attempted          : $DELETES_ATTEMPTED (max requested: $DELETE_COUNT)"
fi
log "  CSV report                 : $REPORT"
if [[ "$DUMP_DETAILS" == "1" && "$DUMP_N" -gt 0 ]]; then
  log "  Details dir                : $DETAIL_DIR"
fi

log
log "Notes:"
log " - 204 means volume deleted; 422 means PowerStore blocked delete (message_l10n explains why)."
log " - If you use --ns, this script stays report-only for safety."
log " - If your list API does not provide names, --k8s-only cannot filter by pvc-*."
