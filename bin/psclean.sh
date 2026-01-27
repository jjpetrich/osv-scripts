#!/usr/bin/env bash
# psclean.sh - PowerStore/OpenShift reconciliation (patched)
# Features:
#  - robust login + retry (prevents --relogin race)
#  - reads Content-Range for total count
#  - --k8s-only to limit to PVC-named / CSI-metadata volumes
#  - --protect-dv, --relogin, --preview N, --dr, --dc, --ns supported
#

#
export PSTORE_IP=powerstore.byu.edu
read -rp "PowerStore username: " PSTORE_USER
read -s -rp "PowerStore password: " PSTORE_PASS; echo

COOKIEFILE="$(mktemp)"
HDRFILE="${COOKIEFILE}.hdr"

curl -k -s -X GET \
  -H "Accept: application/json" -H "Content-Type: application/json" \
  -u "${PSTORE_USER}:${PSTORE_PASS}" -c "${COOKIEFILE}" \
  "https://${PSTORE_IP}/api/rest/login_session" -D "${HDRFILE}"

# extract token
PSTORE_TOKEN="$(awk -F': ' '/DELL-EMC-TOKEN/ {print $2; exit}' "${HDRFILE}" | tr -d $'\r\n')"

# quick check (optional)
curl -k -s -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -b "${COOKIEFILE}" \
  "https://${PSTORE_IP}/api/rest/volume?limit=1" -w "\nHTTP_STATUS:%{http_code}\n"

# export for the script to use
export PSTORE_COOKIEFILE="${COOKIEFILE}"
export PSTORE_TOKEN="${PSTORE_TOKEN}"
# keep PSTORE_USER/PSTORE_PASS unset if you don't want the script to re-login

set -euo pipefail

CSI_DRIVER="${CSI_DRIVER:-csi-powerstore.dellemc.com}"
PSTORE_INSECURE="${PSTORE_INSECURE:-1}"

DRY_RUN=0
DELETE_COUNT=0
NAMESPACE=""
PROTECT_DV=0
FORCE_RELOGIN=0
K8S_ONLY=0
PREVIEW_N=0

usage(){ cat <<'USAGE'
Usage: PSTORE_IP=... ./psclean.sh [--dr|--dry-run] [--dc N|--delete-count N] [--ns NS]
                                 [--protect-dv] [--relogin] [--k8s-only] [--preview N]
Flags:
  --k8s-only       Only consider volumes likely created by Kubernetes CSI (name pvc- or CSI metadata)
  --preview N      Print first N candidate volume IDs before any deletion
Other flags as before.
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
    --help|-h) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

[[ -n "${PSTORE_IP:-}" ]] || die "PSTORE_IP required"

CURL_TLS=(); [[ "$PSTORE_INSECURE" == "1" ]] && CURL_TLS=(-k)

# locations
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

echo "Starting psclean..."
echo "  Script dir      : $SCRIPT_DIR"
echo "  CSI_DRIVER      : $CSI_DRIVER"
echo "  Namespace scope : ${NAMESPACE:-<none>}"
echo "  Dry run         : $DRY_RUN"
echo "  Delete count    : $DELETE_COUNT"
echo "  Protect DV      : $PROTECT_DV"
echo "  Force relogin   : $FORCE_RELOGIN"
echo "  K8s-only filter : $K8S_ONLY"
echo "  Preview N       : $PREVIEW_N"
echo "  Report          : $REPORT"
echo

COOKIE_FILE_ON_DISK="$SESSION_DIR/cookie.file"
TOKEN_FILE_ON_DISK="$SESSION_DIR/token.txt"

# relogin
if [[ "$FORCE_RELOGIN" == "1" ]]; then
  rm -f "$COOKIE_FILE_ON_DISK" "$TOKEN_FILE_ON_DISK" || true
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

# robust login: attempt up to 5 times with backoff and validate by requesting a small page
_ps_login_and_store(){
  # interactive prompt if missing
  if [[ -z "${PSTORE_USER:-}" ]]; then read -rp "PowerStore username: " PSTORE_USER; fi
  if [[ -z "${PSTORE_PASS:-}" ]]; then read -s -rp "PowerStore password for ${PSTORE_USER}: " PSTORE_PASS; echo; fi

  local tries=0 max=5 wait=1 tmpcookie hdrfile token ok
  while (( tries < max )); do
    tries=$((tries+1))
    tmpcookie="$(mktemp)"
    hdrfile="${tmpcookie}.hdr"
    if ! curl "${CURL_TLS[@]}" -s -X GET -H "Accept: application/json" -H "Content-Type: application/json" -u "${PSTORE_USER}:${PSTORE_PASS}" -c "${tmpcookie}" "https://${PSTORE_IP}/api/rest/login_session" -D "${hdrfile}" >/dev/null 2>&1; then
      rm -f "$tmpcookie" "$hdrfile"
      sleep $wait
      wait=$((wait*2))
      continue
    fi
    token="$(awk -F': ' '/DELL-EMC-TOKEN/ {print $2; exit}' "$hdrfile" | tr -d $'\r\n' || true)"
    if [[ -z "$token" ]]; then
      rm -f "$tmpcookie" "$hdrfile"
      sleep $wait
      wait=$((wait*2))
      continue
    fi

    # quick validation: request 1 item and check HTTP status or content-range
    http_out="$(curl -sS -w "\nHTTP_STATUS:%{http_code}\n" -H "DELL-EMC-TOKEN: ${token}" -b "$tmpcookie" "https://${PSTORE_IP}/api/rest/volume?limit=1" 2>/dev/null || true)"
    http_status="$(awk -F: '/HTTP_STATUS/ {print $2}' <<<"$http_out" | tr -d ' \r\n' || true)"
    if [[ "$http_status" == "200" || "$http_status" == "206" ]]; then
      # persist unless user requested no-store
      if [[ "${PSTORE_SESSION_NO_STORE:-}" == "1" ]]; then
        PSTORE_COOKIEFILE="$tmpcookie"
        PSTORE_TOKEN="$token"
        export PSTORE_COOKIEFILE PSTORE_TOKEN
        rm -f "$hdrfile"
        return 0
      fi
      mv "$tmpcookie" "$COOKIE_FILE_ON_DISK"
      chmod 600 "$COOKIE_FILE_ON_DISK"
      printf "%s" "$token" > "$TOKEN_FILE_ON_DISK"
      chmod 600 "$TOKEN_FILE_ON_DISK"
      rm -f "$hdrfile"
      PSTORE_COOKIEFILE="$COOKIE_FILE_ON_DISK"
      PSTORE_TOKEN="$token"
      export PSTORE_COOKIEFILE PSTORE_TOKEN
      echo "Stored session to $SESSION_DIR"
      return 0
    else
      rm -f "$tmpcookie" "$hdrfile" || true
      sleep $wait
      wait=$((wait*2))
    fi
  done
  die "Failed to create a valid PowerStore session after multiple attempts."
}

ensure_session(){
  if [[ -n "${PSTORE_COOKIEFILE:-}" && -n "${PSTORE_TOKEN:-}" ]]; then return 0; fi
  if load_session_if_exists; then return 0; fi
  if [[ -n "${PSTORE_USER:-}" && -n "${PSTORE_PASS:-}" ]]; then _ps_login_and_store; return 0; fi
  echo "No existing PowerStore session found. Please enter credentials."
  _ps_login_and_store
}

# ps_api: runs authenticated GET and prints body and saves the last Content-Range (if any)
LAST_CONTENT_RANGE_FILE="$TMPDIR/last_content_range.txt"
ps_api() {
  local path="$1"
  ensure_session
  # attach headers to a temp file to capture content-range
  local hdrf=$(mktemp)
  local body
  body="$(curl -sS "${CURL_TLS[@]}" -D "$hdrf" -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -H "Accept: application/json" -b "${PSTORE_COOKIEFILE}" "https://${PSTORE_IP}$path")" || return 1
  awk 'BEGIN{IGNORECASE=1} /content-range/ {print $2}' "$hdrf" | tr -d '\r\n' > "$LAST_CONTENT_RANGE_FILE" || true
  rm -f "$hdrf"
  printf '%s' "$body"
  return 0
}

ps_api_delete() {
  local path="$1"
  ensure_session
  curl -sS "${CURL_TLS[@]}" -X DELETE -H "DELL-EMC-TOKEN: ${PSTORE_TOKEN}" -b "${PSTORE_COOKIEFILE}" "https://${PSTORE_IP}$path"
}

# fetch all volumes with pagination; returns JSON array
fetch_all_volumes() {
  local limit=100 offset=0 seen="$TMPDIR/seen_ids.txt" allfile="$TMPDIR/all_objs.ndjson"
  : > "$seen"
  : > "$allfile"
  local page_json new_count iterations=0 content_range

  while true; do
    iterations=$((iterations+1))
    # call page
    page_json="$(ps_api "/api/rest/volume?limit=${limit}&offset=${offset}")" || die "Failed fetching volumes"
    # normalize array wrapper
    if ! jq -e '.[0]' <<<"$page_json" >/dev/null 2>&1; then
      if jq -e '.content // .items' <<<"$page_json" >/dev/null 2>&1; then
        page_json="$(jq -c '.content // .items' <<<"$page_json")"
      else
        # if zero results on first page, break
        if [[ "$iterations" -gt 1 ]]; then break; else die "Unexpected response from PowerStore volume API"; fi
      fi
    fi

    # capture content-range once (from ps_api)
    if [[ -f "$LAST_CONTENT_RANGE_FILE" ]]; then
      content_range="$(<"$LAST_CONTENT_RANGE_FILE")"
      # try to parse total if present (format 0-4/5040)
      if [[ -n "$content_range" && "$iterations" -eq 1 ]]; then
        total="$(awk -F'/' '{print $2}' <<<"$content_range" | tr -d '\r\n')" || true
        if [[ -n "$total" ]]; then echo "  PowerStore API reports total volumes: $total"; fi
      fi
    fi

    new_count=0
    # iterate objects
    while IFS= read -r obj; do
      volid="$(jq -r '.id' <<<"$obj" 2>/dev/null || true)"
      [[ -n "$volid" && "$volid" != "null" ]] || continue
      if ! grep -qFx "$volid" "$seen" 2>/dev/null; then
        echo "$volid" >> "$seen"
        echo "$obj" >> "$allfile"
        new_count=$((new_count+1))
      fi
    done < <(jq -c '.[]' <<<"$page_json")

    if [[ "$new_count" -eq 0 ]]; then break; fi
    offset=$((offset + limit))
    if [[ "$offset" -gt 1000000 ]]; then echo "Safety break"; break; fi
  done

  if [[ -s "$allfile" ]]; then jq -s '.' "$allfile"; else echo "[]"; fi
}

# CSV helper
csv_escape(){ local s="${1:-}"; s="${s//\"/\"\"}"; printf '"%s"' "$s"; }

# 1) gather PV volumeHandles
echo "Collecting PowerStore CSI PV volumeHandles from OpenShift..."
kubectl get pv -o json > "$TMPDIR/pv.json"
jq -r --arg d "$CSI_DRIVER" '.items[]? | select(.spec.csi.driver==$d) | .spec.csi.volumeHandle' "$TMPDIR/pv.json" | sort -u > "$TMPDIR/inuse_handles_all.txt"
INUSE_COUNT="$(wc -l < "$TMPDIR/inuse_handles_all.txt" | tr -d ' ')"
echo "  Found in-use volumeHandles (all namespaces): $INUSE_COUNT"
jq -r --arg d "$CSI_DRIVER" '.items[]? | select(.spec.csi.driver==$d) | [.metadata.name, .spec.csi.volumeHandle, (.spec.claimRef.namespace//""), (.spec.claimRef.name//"")] | @tsv' "$TMPDIR/pv.json" > "$TMPDIR/pv_map.tsv"

# 2) optionally gather DataVolumes (protect)
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

# 3) Pull all volumes (paginated)
echo "Collecting PowerStore volumes (paginated)..."
VOL_JSON="$(fetch_all_volumes)" || die "Failed fetching volumes"
echo "$VOL_JSON" > "$TMPDIR/volumes.json"
jq -r '.[] | [ .id, (.name//""), (.created_timestamp//""), (if (.mapped==true) then "true" else "false" end) ] | @tsv' "$TMPDIR/volumes.json" > "$TMPDIR/volumes.tsv"
TOTAL_VOLS="$(wc -l < "$TMPDIR/volumes.tsv" | tr -d ' ')"
echo "  Total volumes returned by API pages: $TOTAL_VOLS"

# 4) If K8S_ONLY, create filtered volumes.json -> volumes_k8s.json
if [[ "$K8S_ONLY" == "1" ]]; then
  jq '[ .[] | select((.name // "" | test("^pvc-")) or (.metadata? | has("csi.volume.kubernetes.io/pvc/name") or has("csi.volume.kubernetes.io/pv/name"))) ]' "$TMPDIR/volumes.json" > "$TMPDIR/volumes_k8s.json"
  K8S_COUNT="$(jq 'length' "$TMPDIR/volumes_k8s.json")"
  echo "  After --k8s-only filter: $K8S_COUNT volumes (only PVC-like / CSI-metadata volumes kept)"
  mv "$TMPDIR/volumes_k8s.json" "$TMPDIR/volumes.json"
  jq -r '.[] | [ .id, (.name//""), (.created_timestamp//""), (if (.mapped==true) then "true" else "false" end) ] | @tsv' "$TMPDIR/volumes.json" > "$TMPDIR/volumes.tsv"
  TOTAL_VOLS="$(wc -l < "$TMPDIR/volumes.tsv" | tr -d ' ')"
fi

# 5) raw diff: volumes not referenced by any PV volumeHandle
awk 'NR==FNR{a[$1]=1; next} !($1 in a){print}' "$TMPDIR/inuse_handles_all.txt" "$TMPDIR/volumes.tsv" > "$TMPDIR/orphans_raw.tsv"
RAW_ORPHANS="$(wc -l < "$TMPDIR/orphans_raw.tsv" | tr -d ' ')"
echo "  Raw orphan candidates (unreferenced): $RAW_ORPHANS"

# 6) filter unmapped only
awk -F'\t' '$4=="false"{print}' "$TMPDIR/orphans_raw.tsv" > "$TMPDIR/orphans_unmapped.tsv"
UNMAPPED_ORPHANS="$(wc -l < "$TMPDIR/orphans_unmapped.tsv" | tr -d ' ')"
echo "  Unmapped orphan candidates: $UNMAPPED_ORPHANS"
echo

# 7) CSV header
{
  echo "run_timestamp,namespace_scope,dry_run,delete_count,action,volume_id,volume_name,created_timestamp,mapped,age_days,eligible_for_delete,reason,pv_name,pvc_namespace,pvc_name,protected_by_datavolume"
} > "$REPORT"

# 8) analyze each candidate
DELETE_ATTEMPTED=0
DELETE_ELIGIBLE=0
while IFS=$'\t' read -r VOL_ID VOL_NAME CREATED_TS MAPPED; do
  PVC_NS=""; PVC_NAME=""; PV_NAME=""; PROTECTED_BY_DV="no"
  DETAILS_JSON="$(ps_api "/api/rest/volume/${VOL_ID}" || true)"
  if [[ -n "$DETAILS_JSON" ]]; then
    PVC_NS="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/namespace"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
    PVC_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pvc/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
    PV_NAME="$(jq -r '.metadata["csi.volume.kubernetes.io/pv/name"] // ""' <<<"$DETAILS_JSON" 2>/dev/null || true)"
  fi
  if [[ "$PROTECT_DV" == "1" && -n "$PV_NAME" ]]; then
    if grep -qFx "$PV_NAME" "$DV_PROTECTED_SET" 2>/dev/null; then PROTECTED_BY_DV="yes"; fi
  fi

  ELIGIBLE="no"; REASON="Unreferenced by PV volumeHandle and unmapped"; ACTION="report"
  if [[ -n "$NAMESPACE" ]]; then
    if [[ -z "$PVC_NS" ]]; then
      ELIGIBLE="no"; REASON="Namespace-scoped run: missing PowerStore PVC namespace metadata; not deleting"
    elif [[ "$PVC_NS" != "$NAMESPACE" ]]; then
      ELIGIBLE="no"; REASON="Namespace-scoped run: volume metadata namespace does not match; not deleting"
    else
      ELIGIBLE="yes"; REASON="Eligible (namespace match), unreferenced, unmapped"
    fi
  else
    ELIGIBLE="yes"; REASON="Eligible (cluster-wide), unreferenced, unmapped"
  fi
  if [[ "$PROTECTED_BY_DV" == "yes" ]]; then ELIGIBLE="no"; REASON="Protected: referenced by a DataVolume"; fi

  # age in days
  age_days=""
  if [[ -n "$CREATED_TS" ]]; then
    # created_timestamp often like "2025-11-17T16:45:00.000Z" -> convert with date
    if date -d "$CREATED_TS" >/dev/null 2>&1; then
      age_days=$(( ( $(date +%s) - $(date -d "$CREATED_TS" +%s) ) / 86400 ))
    fi
  fi

  # write row
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
    csv_escape "$age_days"; echo -n ","
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
      if [[ "$DRY_RUN" == "1" ]]; then :; else
        if ps_api_delete "/api/rest/volume/${VOL_ID}" >/dev/null 2>&1; then
          DELETE_ATTEMPTED=$((DELETE_ATTEMPTED + 1))
          # deletion row
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
            csv_escape "$age_days"; echo -n ","
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
            csv_escape "$age_days"; echo -n ","
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
  fi

done < "$TMPDIR/orphans_unmapped.tsv"

# Preview if requested
if [[ "$PREVIEW_N" -gt 0 ]]; then
  echo
  echo "Preview: first $PREVIEW_N eligible candidate IDs:"
  awk -F',' 'NR>1 && $11=="yes" {print $6}' "$REPORT" | head -n "$PREVIEW_N"
  echo
fi

echo "Done."
echo "  Eligible candidates : $DELETE_ELIGIBLE"
if [[ "$DRY_RUN" == "1" ]]; then echo "  Deletes attempted   : 0 (dry-run)"; else echo "  Deletes attempted   : $DELETE_ATTEMPTED"; fi
echo "  CSV report          : $REPORT"
echo
