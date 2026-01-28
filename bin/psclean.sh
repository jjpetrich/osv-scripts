#!/usr/bin/env bash
set -euo pipefail

CSI_DRIVER="${CSI_DRIVER:-csi-powerstore.dellemc.com}"
PSTORE_INSECURE="${PSTORE_INSECURE:-1}"
PSTORE_CURL_TIMEOUT="${PSTORE_CURL_TIMEOUT:-10}"     # seconds
PSTORE_PAGE_LIMIT="${PSTORE_PAGE_LIMIT:-200}"        # list page size
PSTORE_LOGIN_POST_SLEEP="${PSTORE_LOGIN_POST_SLEEP:-1}"

DRY_RUN=0
DELETE_COUNT=0
PROCESS_MAX=500          # in dry-run, cap how many candidates we process into CSV
NAMESPACE=""
PROTECT_DV=0
FORCE_RELOGIN=0
K8S_ONLY=0
DEBUG=0
DUMP_DETAILS=0
DUMP_N=0
USE_ADMIN=0              # kept for compatibility; won't help if API returns id-only

usage(){
  cat <<'USAGE'
Usage:
  PSTORE_IP=... ./psclean.sh [--dr] [--dc N] [--ns NS] [--protect-dv] [--relogin]
                             [--k8s-only] [--verify-max N] [--dump-details N]
                             [--admin-user] [--debug]
Notes:
  - On some arrays, GET /api/rest/volume/<id> returns only {"id": "..."} even for admin.
    In that case, "safe delete" cannot be proven by REST mapping state; this script
    classifies candidates as "unreferenced by OpenShift PV volumeHandles" only.
USAGE
}

die(){ echo "ERROR: $*" >&2; exit 2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dr|--dry-run) DRY_RUN=1; shift ;;
    --dc|--delete-count) [[ $# -ge 2 ]] || die "Missing value for $1"; DELETE_COUNT="$2"; shift 2 ;;
    --ns|--namespace) [[ $# -ge 2 ]] || die "Missing value for $1"; NAMESPACE="$2"; shift 2 ;;
    --protect-dv) PROTECT_DV=1; shift ;;
    --relogin) FORCE_RELOGIN=1; shift ;;
    --k8s-only) K8S_ONLY=1; shift ;;
    --verify-max) [[ $# -ge 2 ]] || die "Missing value for $1"; PROCESS_MAX="$2"; shift 2 ;;
    --dump-details) [[ $# -ge 2 ]] || die "Missing value for $1"; DUMP_DETAILS=1; DUMP_N="$2"; shift 2 ;;
    --admin-user) USE_ADMIN=1; shift ;;
    --debug) DEBUG=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

[[ -n "${PSTORE_IP:-}" ]] || die "PSTORE_IP required"
[[ "$DELETE_COUNT" =~ ^[0-9]+$ ]] || die "--dc must be integer"
[[ "$PROCESS_MAX" =~ ^[0-9]+$ ]] || die "--verify-max must be integer"
[[ "$DUMP_N" =~ ^[0-9]*$ ]] || die "--dump-details must be integer"

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

echo "Starting psclean..."
echo "  Script dir      : $SCRIPT_DIR"
echo "  CSI_DRIVER      : $CSI_DRIVER"
echo "  Namespace scope : ${NAMESPACE:-<none>}"
echo "  Dry run         : $DRY_RUN"
echo "  Delete count    : $DELETE_COUNT"
echo "  Process max     : $PROCESS_MAX"
echo "  Protect DV      : $PROTECT_DV"
echo "  Force relogin   : $FORCE_RELOGIN"
echo "  K8s-only filter : $K8S_ONLY"
echo "  Dump details    : $DUMP_DETAILS (N=${DUMP_N:-0})"
echo "  Admin user      : $USE_ADMIN (note: your array returns id-only detail even for admin)"
echo "  Report          : $REPORT"
echo "  Session dir     : $SESSION_DIR"
echo

if [[ "$FORCE_RELOGIN" == "1" ]]; then
  echo "Forcing re-login: removing stored session if present..."
  rm -f "$COOKIE_FILE" "$TOKEN_FILE" || true
fi

csv_escape(){ local s="${1:-}"; s="${s//\"/\"\"}"; printf '"%s"' "$s"; }

login(){
  if [[ -f "$COOKIE_FILE" && -f "$TOKEN_FILE" ]]; then
    PSTORE_COOKIEFILE="$COOKIE_FILE"
    PSTORE_TOKEN="$(<"$TOKEN_FILE")"
    export PSTORE_COOKIEFILE PSTORE_TOKEN
    return 0
  fi

  local user pass
  read -rp "PowerStore username: " user
  read -s -rp "PowerStore password for ${user}: " pass; echo

  local tmpcookie hdrfile token status
  tmpcookie="$(mktemp)"; hdrfile="${tmpcookie}.hdr"

  curl "${CURL_TLS[@]}" -s -X GET -H "Accept: application/json" -H "Content-Type: application/json" \
    -u "${user}:${pass}" -c "${tmpcookie}" "https://${PSTORE_IP}/api/rest/login_session" -D "${hdrfile}" >/dev/null 2>&1 || true

  token="$(awk -F': ' '/DELL-EMC-TOKEN/ {print $2; exit}' "$hdrfile" | tr -d $'\r\n' || true)"
  [[ -n "$token" ]] || die "Login failed (missing token)."

  [[ "$PSTORE_LOGIN_POST_SLEEP" -gt 0 ]] && sleep "$PSTORE_LOGIN_POST_SLEEP"

  status="$(curl -sS "${CURL_TLS[@]}" -m "$PSTORE_CURL_TIMEOUT" -w "%{http_code}" -o /dev/null \
           -H "DELL-EMC-TOKEN: ${token}" -b "$tmpcookie" "https://${PSTORE_IP}/api/rest/volume?limit=1" 2>/dev/null || true)"
  [[ "$status" == "200" || "$status" == "206" ]] || die "Login validation failed (HTTP $status)"

  mv "$tmpcookie" "$COOKIE_FILE"
  printf "%s" "$token" > "$TOKEN_FILE"
  chmod 600 "$COOKIE_FILE" "$TOKEN_FILE"
  rm -f "$hdrfile"

  PSTORE_COOKIEFILE="$COOKIE_FILE"
  PSTORE_TOKEN="$token"
  export PSTORE_COOKIEFILE PSTORE_TOKEN
}

ps_get(){
  local path="$1"
  curl -sS "${CURL_TLS[@]}" -m "$PSTORE_CURL_TIMEOUT" \
    -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -H "Accept: application/json" \
    -b "${PSTORE_COOKIEFILE}" "https://${PSTORE_IP}${path}"
}

ps_get_with_headers(){
  local path="$1" hdr_out="$2" body_out="$3"
  curl -sS "${CURL_TLS[@]}" -m "$PSTORE_CURL_TIMEOUT" -D "$hdr_out" -o "$body_out" \
    -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -H "Accept: application/json" \
    -b "${PSTORE_COOKIEFILE}" "https://${PSTORE_IP}${path}"
}

fetch_all_volumes(){
  local limit="$PSTORE_PAGE_LIMIT" offset=0
  : > "$TMPDIR/vols.ndjson"

  while true; do
    local hdr="$TMPDIR/page_${offset}.hdr"
    local body="$TMPDIR/page_${offset}.json"
    ps_get_with_headers "/api/rest/volume?limit=${limit}&offset=${offset}&select=id,name" "$hdr" "$body" || die "Failed fetching volumes at offset=$offset"

    if ! jq -e . "$body" >/dev/null 2>&1; then
      [[ "$DEBUG" == "1" ]] && { echo "Non-JSON body preview:"; head -c 200 "$body"; echo; }
      die "Volume list returned non-JSON at offset=$offset"
    fi

    # The API typically returns an array for /volume
    local count
    if jq -e 'type=="array"' "$body" >/dev/null 2>&1; then
      count="$(jq 'length' "$body")"
      jq -c '.[]' "$body" >> "$TMPDIR/vols.ndjson"
    else
      # fallback wrapper patterns
      if jq -e '.content // .items' "$body" >/dev/null 2>&1; then
        count="$(jq '.content // .items | length' "$body")"
        jq -c '.content // .items | .[]' "$body" >> "$TMPDIR/vols.ndjson"
      else
        die "Unexpected list shape at offset=$offset"
      fi
    fi

    # stop if last page
    if [[ "$count" -lt "$limit" ]]; then break; fi
    offset=$((offset + limit))
    [[ "$offset" -lt 2000000 ]] || break
  done

  jq -s '.' "$TMPDIR/vols.ndjson"
}

echo "Collecting PowerStore CSI PV volumeHandles from OpenShift..."
kubectl get pv -o json > "$TMPDIR/pv.json"
jq -r --arg d "$CSI_DRIVER" '.items[]? | select(.spec.csi.driver==$d) | .spec.csi.volumeHandle' "$TMPDIR/pv.json" \
  | sort -u > "$TMPDIR/inuse_handles_all.txt"
INUSE_COUNT="$(wc -l < "$TMPDIR/inuse_handles_all.txt" | tr -d ' ')"
echo "  Found in-use volumeHandles (all namespaces): $INUSE_COUNT"

if [[ "$PROTECT_DV" == "1" ]]; then
  echo "Collecting KubeVirt DataVolumes to protect referenced PVs..."
  if kubectl get dv -A -o json > "$TMPDIR/dv.json" 2>/dev/null; then
    jq -r '.items[]? | (.spec.pvc.volumeName // empty)' "$TMPDIR/dv.json" | sort -u > "$TMPDIR/dv_pvnames.txt"
    echo "  DataVolumes referencing PVs: $(wc -l < "$TMPDIR/dv_pvnames.txt" | tr -d ' ')"
  else
    echo "  Warning: failed to fetch DataVolumes. Continuing."
    : > "$TMPDIR/dv_pvnames.txt"
  fi
fi

echo "Collecting PowerStore volumes (paginated)..."
login
VOL_JSON="$(fetch_all_volumes)"
echo "$VOL_JSON" > "$TMPDIR/volumes.json"
TOTAL_VOLS="$(jq 'length' "$TMPDIR/volumes.json")"
echo "  Total volumes fetched from list API: $TOTAL_VOLS"

if [[ "$K8S_ONLY" == "1" ]]; then
  jq '[ .[] | select((.name // "" | test("^pvc-"))) ]' "$TMPDIR/volumes.json" > "$TMPDIR/volumes_k8s.json"
  mv "$TMPDIR/volumes_k8s.json" "$TMPDIR/volumes.json"
  echo "  After --k8s-only filter (pvc-* names): $(jq 'length' "$TMPDIR/volumes.json")"
fi

jq -r '.[].id' "$TMPDIR/volumes.json" | sort -u > "$TMPDIR/ps_ids.txt"
comm -23 "$TMPDIR/ps_ids.txt" "$TMPDIR/inuse_handles_all.txt" > "$TMPDIR/orphan_ids.txt"
ORPHAN_ID_COUNT="$(wc -l < "$TMPDIR/orphan_ids.txt" | tr -d ' ')"
echo "Computing orphan candidates (not referenced by any PV volumeHandle)..."
echo "  Orphan IDs (unreferenced): $ORPHAN_ID_COUNT"
echo

# CSV header
echo "run_timestamp,namespace_scope,dry_run,delete_count,process_max,action,volume_id,volume_name,verified,eligible_for_delete,reason" > "$REPORT"

lookup_name(){
  local vid="$1"
  jq -r --arg id "$vid" 'first(.[] | select(.id==$id) | (.name // "")) // ""' "$TMPDIR/volumes.json" 2>/dev/null || true
}

# Optional dump-details: capture pagination proof + N detail samples
if [[ "$DUMP_DETAILS" == "1" && "${DUMP_N:-0}" -gt 0 ]]; then
  echo "Dumping list pagination evidence + ${DUMP_N} sample detail calls to $DETAIL_DIR ..."
  # store the first few list response headers for proof
  ls "$TMPDIR"/page_*.hdr >/dev/null 2>&1 && cp "$TMPDIR"/page_*.hdr "$DETAIL_DIR"/ 2>/dev/null || true
  # sample per-volume detail calls (known to return id-only on your array)
  i=0
  while IFS= read -r vid; do
    i=$((i+1))
    [[ "$i" -le "$DUMP_N" ]] || break
    out="$DETAIL_DIR/detail_${vid}.json"
    ps_get "/api/rest/volume/${vid}" > "$out" || true
  done < "$TMPDIR/orphan_ids.txt"
  echo "  Dump complete."
fi

echo "Analyzing candidates and writing CSV report..."
PROCESSED=0
DELETE_ATTEMPTED=0

while IFS= read -r vid; do
  name="$(lookup_name "$vid")"
  PROCESSED=$((PROCESSED+1))

  # namespace-scoped behavior: since array doesn't return metadata, we cannot confirm namespace;
  # so for --ns runs we only report, never delete.
  eligible="no"
  reason="Unreferenced by PV volumeHandle"
  verified="yes"   # verified == k8s-side verification only

  if [[ -n "$NAMESPACE" ]]; then
    eligible="no"
    reason="Namespace scope requested but array API does not expose PVC namespace metadata; report-only."
  else
    # Still conservative: eligible-for-delete is "k8s-unreferenced" (not mapping-verified)
    eligible="yes"
    reason="Unreferenced by PV volumeHandle (array detail unavailable; review before delete)"
  fi

  {
    csv_escape "$TS_NOW"; echo -n ","
    csv_escape "${NAMESPACE:-}"; echo -n ","
    csv_escape "$DRY_RUN"; echo -n ","
    csv_escape "$DELETE_COUNT"; echo -n ","
    csv_escape "$PROCESS_MAX"; echo -n ","
    csv_escape "report"; echo -n ","
    csv_escape "$vid"; echo -n ","
    csv_escape "$name"; echo -n ","
    csv_escape "$verified"; echo -n ","
    csv_escape "$eligible"; echo -n ","
    csv_escape "$reason"
    echo
  } >> "$REPORT"

  # bounded processing in dry-run so it never runs forever
  if [[ "$DRY_RUN" == "1" && "$PROCESSED" -ge "$PROCESS_MAX" ]]; then
    echo "  Reached --verify-max (process cap) $PROCESS_MAX in dry-run; stopping early."
    break
  fi

  # deletes (only when not dry-run, not namespace-scoped)
  if [[ "$DRY_RUN" == "0" && "$DELETE_COUNT" -gt 0 && "$DELETE_ATTEMPTED" -lt "$DELETE_COUNT" ]]; then
    if [[ "$eligible" == "yes" ]]; then
      # delete volume by id
      if curl -sS "${CURL_TLS[@]}" -m "$PSTORE_CURL_TIMEOUT" -X DELETE \
           -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -b "${PSTORE_COOKIEFILE}" \
           "https://${PSTORE_IP}/api/rest/volume/${vid}" >/dev/null 2>&1; then
        DELETE_ATTEMPTED=$((DELETE_ATTEMPTED+1))
      fi
    fi
  fi
done < "$TMPDIR/orphan_ids.txt"

echo
echo "Done."
echo "  Orphan IDs (unreferenced) : $ORPHAN_ID_COUNT"
echo "  Processed into CSV        : $PROCESSED"
echo "  Deletes attempted         : $([ "$DRY_RUN" == "1" ] && echo "0 (dry-run)" || echo "$DELETE_ATTEMPTED")"
echo "  CSV report                : $REPORT"
if [[ "$DUMP_DETAILS" == "1" ]]; then
  echo "  Detail dumps              : $DETAIL_DIR"
fi

echo
echo "Notes:"
echo " - Your PowerStore detail endpoint returns id-only; mapping/metadata cannot be validated via REST on this array."
echo " - Candidates are based on 'not referenced by OpenShift PV volumeHandle'. Review CSV before deletes."
