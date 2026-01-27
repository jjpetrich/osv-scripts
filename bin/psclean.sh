#!/usr/bin/env bash
set -euo pipefail

CSI_DRIVER="${CSI_DRIVER:-csi-powerstore.dellemc.com}"
PSTORE_INSECURE="${PSTORE_INSECURE:-1}"

PSTORE_LOGIN_RETRIES="${PSTORE_LOGIN_RETRIES:-10}"
PSTORE_LOGIN_BACKOFF="${PSTORE_LOGIN_BACKOFF:-2}"
PSTORE_LOGIN_POST_SLEEP="${PSTORE_LOGIN_POST_SLEEP:-1}"

DRY_RUN=0
DELETE_COUNT=0
VERIFY_MAX=500
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
USAGE
}

die(){ echo "ERROR: $*" >&2; exit 2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run|--dr) DRY_RUN=1; shift ;;
    --delete-count|--dc) DELETE_COUNT="${2:-}"; shift 2 ;;
    --verify-max) VERIFY_MAX="${2:-}"; shift 2 ;;
    --namespace|--ns) NAMESPACE="${2:-}"; shift 2 ;;
    --protect-dv) PROTECT_DV=1; shift ;;
    --relogin) FORCE_RELOGIN=1; shift ;;
    --k8s-only) K8S_ONLY=1; shift ;;
    --preview) PREVIEW_N="${2:-}"; shift 2 ;;
    --debug) DEBUG=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

[[ -n "${PSTORE_IP:-}" ]] || die "PSTORE_IP is required"
[[ "${DELETE_COUNT}" =~ ^[0-9]+$ ]] || die "--dc must be non-negative int"
[[ "${VERIFY_MAX}" =~ ^[0-9]+$ ]] || die "--verify-max must be non-negative int"
[[ "${PREVIEW_N}" =~ ^[0-9]+$ ]] || die "--preview must be non-negative int"

CURL_TLS=(); [[ "$PSTORE_INSECURE" == "1" ]] && CURL_TLS=(-k)

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

ps_login_and_store() {
  if [[ -z "${PSTORE_USER:-}" ]]; then read -rp "PowerStore username: " PSTORE_USER; fi
  if [[ -z "${PSTORE_PASS:-}" ]]; then read -s -rp "PowerStore password for ${PSTORE_USER}: " PSTORE_PASS; echo; fi

  local tries=0 backoff="$PSTORE_LOGIN_BACKOFF"
  while (( tries < PSTORE_LOGIN_RETRIES )); do
    tries=$((tries+1))
    local tmpcookie hdrfile token status
    tmpcookie="$(mktemp)"
    hdrfile="${tmpcookie}.hdr"

    if [[ "$DEBUG" == "1" ]]; then
      echo "[debug] login attempt #$tries"
      curl "${CURL_TLS[@]}" -v -X GET \
        -H "Accept: application/json" -H "Content-Type: application/json" \
        -u "${PSTORE_USER}:${PSTORE_PASS}" -c "${tmpcookie}" \
        "https://${PSTORE_IP}/api/rest/login_session" -D "${hdrfile}" 2>&1 | sed -n '1,120p'
    else
      curl "${CURL_TLS[@]}" -s -X GET \
        -H "Accept: application/json" -H "Content-Type: application/json" \
        -u "${PSTORE_USER}:${PSTORE_PASS}" -c "${tmpcookie}" \
        "https://${PSTORE_IP}/api/rest/login_session" -D "${hdrfile}" >/dev/null 2>&1 || true
    fi

    token="$(awk -F': ' '/DELL-EMC-TOKEN/ {print $2; exit}' "$hdrfile" | tr -d $'\r\n' || true)"
    if [[ -z "$token" ]]; then
      rm -f "$tmpcookie" "$hdrfile"
      echo "  login attempt $tries: missing token; retry in ${backoff}s..."
      sleep "$backoff"; backoff=$((backoff*2))
      continue
    fi

    [[ "$PSTORE_LOGIN_POST_SLEEP" -gt 0 ]] && sleep "$PSTORE_LOGIN_POST_SLEEP"

    # validate with a trivial call
    status="$(curl -sS "${CURL_TLS[@]}" -w "%{http_code}" -o /dev/null \
      -H "DELL-EMC-TOKEN: ${token}" -b "$tmpcookie" \
      "https://${PSTORE_IP}/api/rest/volume?limit=1" 2>/dev/null || true)"

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
    echo "  login attempt $tries: validation HTTP $status; retry in ${backoff}s..."
    sleep "$backoff"; backoff=$((backoff*2))
  done

  die "Failed to create a valid PowerStore session after ${PSTORE_LOGIN_RETRIES} attempts."
}

ensure_session() {
  if [[ -n "${PSTORE_COOKIEFILE:-}" && -n "${PSTORE_TOKEN:-}" ]]; then return 0; fi
  if load_session_if_exists; then return 0; fi
  ps_login_and_store
}

# --- JSON-safe GET helper ---
ps_api() {
  local path="$1"
  ensure_session
  local hdrf bodyf
  hdrf="$(mktemp)"
  bodyf="$(mktemp)"
  if ! curl -sS "${CURL_TLS[@]}" -D "$hdrf" \
      -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -H "Accept: application/json" \
      -b "${PSTORE_COOKIEFILE}" "https://${PSTORE_IP}${path}" -o "$bodyf"; then
    echo "[PowerStore] GET $path failed (curl)."
    if [[ "$DEBUG" == "1" ]]; then
      echo "[debug] header preview:"; sed -n '1,40p' "$hdrf" || true
      echo "[debug] body preview:"; head -c 512 "$bodyf" || true
    fi
    rm -f "$hdrf" "$bodyf"
    return 1
  fi

  # Validate JSON before returning
  if ! jq -e . "$bodyf" >/dev/null 2>&1; then
    echo "[PowerStore] GET $path returned NON-JSON response; cannot parse."
    echo "  Preview (first 200 chars):"
    head -c 200 "$bodyf"; echo
    rm -f "$hdrf" "$bodyf"
    return 1
  fi

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

# --- Volume list fetch ---
# We probe support for select=id,name. If unsupported, fall back to id-only list.
fetch_vol_page() {
  local limit="$1" offset="$2"
  local resp

  resp="$(ps_api "/api/rest/volume?limit=${limit}&offset=${offset}&select=id,name" || true)"
  if [[ -n "$resp" ]] && jq -e 'type=="array"' <<<"$resp" >/dev/null 2>&1; then
    echo "$resp"
    return 0
  fi
  if [[ -n "$resp" ]] && jq -e '.messages? // empty' <<<"$resp" >/dev/null 2>&1; then
    # select unsupported, fall through
    :
  fi

  # fallback
  ps_api "/api/rest/volume?limit=${limit}&offset=${offset}"
}

fetch_all_volumes() {
  local limit=200 offset=0 all="$TMPDIR/vols.ndjson"
  : > "$all"

  local page count
  local first=1

  while true; do
    page="$(fetch_vol_page "$limit" "$offset")" || die "Failed fetching volumes page (offset=$offset)"

    # normalize wrapper if needed
    if ! jq -e 'type=="array"' <<<"$page" >/dev/null 2>&1; then
      if jq -e '.content // .items' <<<"$page" >/dev/null 2>&1; then
        page="$(jq -c '.content // .items' <<<"$page")"
      else
        die "Unexpected volume list response shape at offset=$offset"
      fi
    fi

    if [[ "$first" == "1" && "$DEBUG" == "1" ]]; then
      echo "---- [debug] first volume list page preview ----"
      printf '%s\n' "$page" | head -n 40
      echo "---- [debug] end ----"
    fi
    first=0

    count="$(jq 'length' <<<"$page")"
    jq -c '.[]' <<<"$page" >> "$all"

    if [[ "$count" -lt "$limit" ]]; then break; fi
    offset=$((offset + limit))
    if [[ "$offset" -gt 2000000 ]]; then echo "Safety break (offset too large)"; break; fi
  done

  jq -s '.' "$all"
}

csv_escape(){ local s="${1:-}"; s="${s//\"/\"\"}"; printf '"%s"' "$s"; }

# --- OpenShift PV handles ---
echo "Collecting PowerStore CSI PV volumeHandles from OpenShift..."
kubectl get pv -o json > "$TMPDIR/pv.json"
jq -r --arg d "$CSI_DRIVER" '.items[]? | select(.spec.csi.driver==$d) | .spec.csi.volumeHandle' "$TMPDIR/pv.json" | sort -u > "$TMPDIR/inuse_handles_all.txt"
INUSE_COUNT="$(wc -l < "$TMPDIR/inuse_handles_all.txt" | tr -d ' ')"
echo "  Found in-use volumeHandles (all namespaces): $INUSE_COUNT"

# --- Volumes ---
echo "Collecting PowerStore volumes (paginated)..."
VOL_JSON="$(fetch_all_volumes)" || die "Failed fetching volumes"
echo "$VOL_JSON" > "$TMPDIR/volumes.json"
TOTAL_VOLS="$(jq 'length' "$TMPDIR/volumes.json")"
echo "  Total volumes fetched from list API: $TOTAL_VOLS"

# Determine if name exists in list objects
HAS_NAME=0
if jq -e '.[0] | has("name")' "$TMPDIR/volumes.json" >/dev/null 2>&1; then HAS_NAME=1; fi

if [[ "$K8S_ONLY" == "1" ]]; then
  if [[ "$HAS_NAME" == "1" ]]; then
    jq '[ .[] | select((.name // "" | test("^pvc-"))) ]' "$TMPDIR/volumes.json" > "$TMPDIR/vols_k8s.json"
    mv "$TMPDIR/vols_k8s.json" "$TMPDIR/volumes.json"
    echo "  After --k8s-only filter (pvc-* names): $(jq 'length' "$TMPDIR/volumes.json")"
  else
    echo "  Warning: volume names not present in list response; skipping --k8s-only filter."
  fi
fi

jq -r '.[].id' "$TMPDIR/volumes.json" | sort -u > "$TMPDIR/ps_ids.txt"
comm -23 "$TMPDIR/ps_ids.txt" "$TMPDIR/inuse_handles_all.txt" > "$TMPDIR/orphan_ids.txt"
ORPHAN_ID_COUNT="$(wc -l < "$TMPDIR/orphan_ids.txt" | tr -d ' ')"
echo "Computing orphan candidates (not referenced by any PV volumeHandle)..."
echo "  Orphan IDs (unreferenced): $ORPHAN_ID_COUNT"
echo

# --- CSV header ---
{
  echo "run_timestamp,namespace_scope,dry_run,delete_count,verify_max,action,volume_id,volume_name,mapped,verified,eligible_for_delete,reason,pv_name,pvc_namespace,pvc_name"
} > "$REPORT"

lookup_name() {
  local vid="$1"
  jq -r --arg id "$vid" 'first(.[] | select(.id==$id) | (.name // "")) // ""' "$TMPDIR/volumes.json" 2>/dev/null || true
}

get_volume_details() {
  local vid="$1"
  local resp
  resp="$(ps_api "/api/rest/volume/${vid}" || true)"
  if [[ -z "$resp" ]]; then echo "{}"; return 0; fi
  if jq -e '.messages? // empty' <<<"$resp" >/dev/null 2>&1; then echo "{}"; return 0; fi
  echo "$resp"
}

VERIFIED_COUNT=0
ELIGIBLE_VERIFIED=0
DELETE_ATTEMPTED=0
VERIFIED_ELIGIBLE_IDS=()

echo "Analyzing candidates and writing CSV report..."
while IFS= read -r VOL_ID; do
  VOL_NAME="$(lookup_name "$VOL_ID")"
  MAPPED="unknown"
  VERIFIED="no"
  ELIGIBLE="no"
  PV_NAME=""
  PVC_NS=""
  PVC_NAME=""
  REASON="Unreferenced by PV volumeHandle (not verified)"

  DO_VERIFY=0
  if [[ "$DELETE_COUNT" -gt 0 ]]; then
    DO_VERIFY=1
  elif [[ "$DRY_RUN" == "1" && "$VERIFIED_COUNT" -lt "$VERIFY_MAX" ]]; then
    DO_VERIFY=1
  fi

  if [[ "$DO_VERIFY" == "1" ]]; then
    DETAILS="$(get_volume_details "$VOL_ID")"
    if [[ "$DETAILS" != "{}" ]]; then
      VERIFIED="yes"
      VERIFIED_COUNT=$((VERIFIED_COUNT + 1))
      MAPPED="$(jq -r 'if has("mapped") then (if .mapped==true then "true" else "false" end) else "unknown" end' <<<"$DETAILS" 2>/dev/null || echo "unknown")"
      PV_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pv/name"] // ""' <<<"$DETAILS" 2>/dev/null || true)"
      PVC_NS="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/namespace"] // ""' <<<"$DETAILS" 2>/dev/null || true)"
      PVC_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/name"] // ""' <<<"$DETAILS" 2>/dev/null || true)"

      if [[ "$MAPPED" == "false" ]]; then
        if [[ -n "$NAMESPACE" ]]; then
          if [[ -n "$PVC_NS" && "$PVC_NS" == "$NAMESPACE" ]]; then
            ELIGIBLE="yes"; REASON="Verified unmapped + namespace match"
          else
            ELIGIBLE="no"; REASON="Verified unmapped but namespace mismatch or missing metadata"
          fi
        else
          ELIGIBLE="yes"; REASON="Verified unmapped"
        fi
      else
        ELIGIBLE="no"; REASON="Verified but mapped=${MAPPED}"
      fi
    else
      REASON="Per-volume verification failed (detail endpoint error)"
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
    csv_escape "$PVC_NAME"
    echo
  } >> "$REPORT"

  if [[ "$VERIFIED" == "yes" && "$ELIGIBLE" == "yes" ]]; then
    ELIGIBLE_VERIFIED=$((ELIGIBLE_VERIFIED + 1))
    VERIFIED_ELIGIBLE_IDS+=("$VOL_ID")
  fi

done < "$TMPDIR/orphan_ids.txt"

echo
echo "Done."
echo "  Orphan IDs (unreferenced)           : $ORPHAN_ID_COUNT"
echo "  Verified candidates (bounded)       : $VERIFIED_COUNT (max $VERIFY_MAX in dry-run)"
echo "  Verified eligible (safe) candidates : $ELIGIBLE_VERIFIED"
echo "  Deletes attempted                   : 0 (dry-run)"
echo "  CSV report                          : $REPORT"

if [[ "$PREVIEW_N" -gt 0 ]]; then
  echo
  echo "Preview: first $PREVIEW_N verified-eligible volume IDs:"
  printf '%s\n' "${VERIFIED_ELIGIBLE_IDS[@]}" | head -n "$PREVIEW_N"
fi

echo
echo "Notes:"
echo " - Your array's /api/rest/volume collection does not support select=mapped or select=created_timestamp."
echo " - This script probes select=id,name; if unsupported it falls back to id-only list."
echo " - Safe delete is determined via per-volume GET for up to --verify-max candidates in dry-run."
