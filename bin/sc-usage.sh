#!/usr/bin/env bash
set -euo pipefail

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }; }
need oc
need jq
need curl
need python3

# Convert Kubernetes quantities to bytes (best-effort).
to_bytes() {
  python3 - "$1" <<'PY'
import re, sys
s=sys.argv[1].strip()
m=re.match(r'^([0-9.]+)([a-zA-Z]+)?$', s)
if not m:
    print(0); sys.exit(0)
num=float(m.group(1))
unit=(m.group(2) or "")

bin_units={"Ki":1024,"Mi":1024**2,"Gi":1024**3,"Ti":1024**4,"Pi":1024**5,"Ei":1024**6}
dec_units={"K":1000,"M":1000**2,"G":1000**3,"T":1000**4,"P":1000**5,"E":1000**6}

if unit in bin_units:
    print(int(num*bin_units[unit]))
elif unit in dec_units:
    print(int(num*dec_units[unit]))
elif unit == "":
    print(int(num))
else:
    print(0)
PY
}

bytes_to_gib() {
  python3 - "$1" <<'PY'
import sys
b=int(sys.argv[1])
print(f"{b/(1024**3):.2f}")
PY
}

# ---- Thanos / Prom query ----
THANOS_NS="${THANOS_NS:-openshift-monitoring}"
THANOS_ROUTE_NAME="${THANOS_ROUTE_NAME:-thanos-querier}"

THANOS_HOST="$(oc -n "$THANOS_NS" get route "$THANOS_ROUTE_NAME" -o jsonpath='{.spec.host}' 2>/dev/null || true)"
TOKEN="$(oc whoami -t 2>/dev/null || true)"

PROM_AVAILABLE=0
if [[ -n "$THANOS_HOST" && -n "$TOKEN" ]]; then
  PROM_AVAILABLE=1
fi

prom_query() {
  local q="$1"
  local url="https://${THANOS_HOST}/api/v1/query"

  # Capture body and status so failures are visible/debuggable.
  local tmp
  tmp="$(mktemp)"
  local http
  http="$(curl -k -sS -G \
      -H "Authorization: Bearer ${TOKEN}" \
      --data-urlencode "query=${q}" \
      -o "$tmp" \
      -w "%{http_code}" \
      "$url" || true)"

  if [[ "$http" != "200" ]]; then
    echo "WARN: Prometheus query failed (HTTP $http) for: $q" >&2
    echo "WARN: Response (first 300 chars):" >&2
    head -c 300 "$tmp" >&2 || true
    echo >&2
    rm -f "$tmp"
    return 1
  fi

  cat "$tmp"
  rm -f "$tmp"
  return 0
}

# ---- Load StorageClasses ----
mapfile -t SC_LIST < <(oc get storageclass -o json | jq -r '.items[].metadata.name' | sort)
if [[ ${#SC_LIST[@]} -eq 0 ]]; then
  echo "No StorageClasses found."
  exit 0
fi

# ---- Get all PVCs once ----
PVC_JSON="$(oc get pvc -A -o json)"

# ---- Provisioned per SC ----
declare -A PROV_BYTES
while IFS=$'\t' read -r ns name sc req phase; do
  [[ "$phase" != "Bound" ]] && continue
  [[ -z "$sc" || "$sc" == "null" ]] && continue
  b="$(to_bytes "$req")"
  PROV_BYTES["$sc"]=$(( ${PROV_BYTES["$sc"]:-0} + b ))
done < <(echo "$PVC_JSON" | jq -r '
  .items[]
  | [
      .metadata.namespace,
      .metadata.name,
      (.spec.storageClassName // ""),
      (.spec.resources.requests.storage // "0"),
      (.status.phase // "")
    ] | @tsv
')

# ---- Used per SC (via kubelet stats -> PVC -> SC) ----
declare -A SC_USED_BYTES
if [[ "$PROM_AVAILABLE" -eq 1 ]]; then
  USED_JSON="$(prom_query 'sum(kubelet_volume_stats_used_bytes) by (namespace, persistentvolumeclaim)' || true)"

  if [[ -n "${USED_JSON:-}" ]]; then
    # Build lookup ns/pvc -> used_bytes
    declare -A USED_BYTES
    while IFS=$'\t' read -r ns pvc val; do
      [[ -z "$ns" || -z "$pvc" ]] && continue
      iv="$(python3 - "$val" <<'PY'
import sys
try:
    print(int(float(sys.argv[1])))
except Exception:
    print(0)
PY
)"
      USED_BYTES["$ns/$pvc"]="$iv"
    done < <(echo "$USED_JSON" | jq -r '
      .data.result[]
      | [
          (.metric.namespace // ""),
          (.metric.persistentvolumeclaim // ""),
          (.value[1] // "0")
        ] | @tsv
    ')

    # Map PVC -> SC and aggregate
    while IFS=$'\t' read -r ns name sc phase; do
      [[ "$phase" != "Bound" ]] && continue
      [[ -z "$sc" || "$sc" == "null" ]] && continue
      key="$ns/$name"
      u="${USED_BYTES["$key"]:-0}"
      SC_USED_BYTES["$sc"]=$(( ${SC_USED_BYTES["$sc"]:-0} + u ))
    done < <(echo "$PVC_JSON" | jq -r '
      .items[]
      | [
          .metadata.namespace,
          .metadata.name,
          (.spec.storageClassName // ""),
          (.status.phase // "")
        ] | @tsv
    ')
  else
    echo "WARN: No kubelet_volume_stats_used_bytes returned. Used-capacity will be N/A." >&2
    PROM_AVAILABLE=0
  fi
else
  echo "WARN: Thanos route/token not available; Used-capacity will be N/A." >&2
fi

# ---- Output ----
printf "%-25s %-22s %-14s %-14s %-8s\n" "STORAGECLASS" "PROVISIONER" "PROV_GiB" "USED_GiB" "USED_%"
printf "%-25s %-22s %-14s %-14s %-8s\n" "-------------------------" "----------------------" "--------" "--------" "------"

# Add provisioner column (nice for your PowerStore/Primera/NFS mix)
SC_INFO="$(oc get sc -o json | jq -r '.items[] | [.metadata.name, .provisioner] | @tsv')"
declare -A SC_PROV
while IFS=$'\t' read -r sc prov; do
  SC_PROV["$sc"]="$prov"
done <<< "$SC_INFO"

for sc in "${SC_LIST[@]}"; do
  p="${PROV_BYTES["$sc"]:-0}"
  prov="${SC_PROV["$sc"]:-unknown}"
  p_gib="$(bytes_to_gib "$p")"

  if [[ "$PROM_AVAILABLE" -eq 1 ]]; then
    u="${SC_USED_BYTES["$sc"]:-0}"
    u_gib="$(bytes_to_gib "$u")"
    used_pct="$(python3 - "$p" "$u" <<'PY'
import sys
p=int(sys.argv[1]); u=int(sys.argv[2])
print("0.0" if p<=0 else f"{(u/p)*100:.1f}")
PY
)"
  else
    u_gib="N/A"
    used_pct="N/A"
  fi

  printf "%-25s %-22s %-14s %-14s %-8s\n" "$sc" "$prov" "$p_gib" "$u_gib" "$used_pct"
done
