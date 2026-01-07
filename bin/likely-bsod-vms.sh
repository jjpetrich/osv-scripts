#!/usr/bin/env bash
set -euo pipefail

WINDOW="${WINDOW:-10m}"
TOP_N="${TOP_N:-20}"
MODE="${MODE:-top}"   # top | wedge
THRESH_PCT="${THRESH_PCT:-85}"
NET_MAX_BPS="${NET_MAX_BPS:-50000}"
DISK_MAX_BPS="${DISK_MAX_BPS:-50000}"
DEBUG=0

usage() {
  cat <<EOF
Usage:
  $0 --top 20 --window 10m [--debug]
  MODE=wedge THRESH_PCT=85 NET_MAX_BPS=50000 DISK_MAX_BPS=50000 $0 --top 200 --window 10m

EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --window) WINDOW="$2"; shift 2;;
    --top) TOP_N="$2"; shift 2;;
    --debug) DEBUG=1; shift 1;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }; }
need oc; need jq; need curl

oc whoami >/dev/null 2>&1 || { echo "ERROR: oc not logged in. Run: oc login --web" >&2; exit 1; }
TOKEN="$(oc whoami -t 2>/dev/null || true)"
[[ -n "${TOKEN:-}" ]] || { echo "ERROR: oc whoami -t returned empty token" >&2; exit 1; }

detect_vmi_resource() {
  local candidates=("virtualmachineinstances" "vmi" "virtualmachineinstance")
  local r
  for r in "${candidates[@]}"; do
    if oc get "$r" -A --request-timeout=5s >/dev/null 2>&1; then
      echo "$r"; return 0
    fi
  done
  return 1
}
VMI_RES="$(detect_vmi_resource || true)"
[[ -n "${VMI_RES:-}" ]] || { echo "ERROR: No VMI API resource found" >&2; exit 1; }

PF_PID=""
PF_PORT=""
PF_SVC=""

cleanup() { [[ -n "${PF_PID:-}" ]] && kill "${PF_PID}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

pick_free_port() {
  local p i
  for i in {1..120}; do
    p=$(( (RANDOM % 15000) + 20000 ))
    # if nothing is listening, this will fail -> treat as "free enough"
    if ! curl --ipv4 --http1.1 -ksS "https://127.0.0.1:${p}/" >/dev/null 2>&1; then
      echo "$p"; return 0
    fi
  done
  echo 29091
}

mon_post() {
  local q="$1"
  curl --ipv4 --http1.1 -ksS \
    --connect-timeout 2 --max-time 20 \
    -H "Authorization: Bearer ${TOKEN}" \
    "https://127.0.0.1:${PF_PORT}/api/v1/query" \
    --data-urlencode "query=${q}"
}

wait_listening() {
  # Wait until the port-forward binds (connection refused stops)
  local i
  for i in {1..120}; do
    # We don't care about TLS/HTTP correctness here—only whether the socket is accepting connections.
    if curl --ipv4 --http1.1 -k -sS "https://127.0.0.1:${PF_PORT}/" >/dev/null 2>&1; then
      return 0
    fi
    # If it's still refusing, keep waiting
    sleep 0.1
  done
  return 1
}

start_pf() {
  local svc="$1"
  PF_PORT="$(pick_free_port)"
  [[ "$DEBUG" -eq 1 ]] && echo "DEBUG: starting port-forward svc/${svc} ${PF_PORT}:9091" >&2

  oc -n openshift-monitoring port-forward --address 127.0.0.1 "svc/${svc}" "${PF_PORT}:9091" >/dev/null 2>&1 &
  PF_PID="$!"
  PF_SVC="$svc"

  # Ensure the local port is actually bound
  if ! wait_listening; then
    [[ "$DEBUG" -eq 1 ]] && echo "DEBUG: port ${PF_PORT} never started listening for svc/${svc}" >&2
    kill "${PF_PID}" >/dev/null 2>&1 || true
    PF_PID=""; PF_PORT=""; PF_SVC=""
    return 1
  fi

  # Now verify Prometheus API works (POST up)
  local i out
  for i in {1..80}; do
    out="$(mon_post "up" 2>/dev/null || true)"
    if jq -e '.status=="success"' >/dev/null 2>&1 <<<"$out"; then
      return 0
    fi
    sleep 0.2
  done

  [[ "$DEBUG" -eq 1 ]] && echo "DEBUG: port-forward listening but Prom query didn't succeed for svc/${svc}" >&2
  kill "${PF_PID}" >/dev/null 2>&1 || true
  PF_PID=""; PF_PORT=""; PF_SVC=""
  return 1
}

if start_pf "thanos-querier"; then :; elif start_pf "prometheus-k8s"; then :; else
  echo "ERROR: Could not establish monitoring port-forward (thanos-querier or prometheus-k8s)." >&2
  echo "Try manual test:" >&2
  echo "  oc -n openshift-monitoring port-forward svc/thanos-querier 19091:9091" >&2
  echo "  TOKEN=\$(oc whoami -t)" >&2
  echo "  curl --http1.1 -ksS -H \"Authorization: Bearer \$TOKEN\" https://127.0.0.1:19091/api/v1/query --data-urlencode \"query=up\"" >&2
  exit 1
fi

[[ "$DEBUG" -eq 1 ]] && echo "DEBUG: monitoring via svc/${PF_SVC} on https://127.0.0.1:${PF_PORT}" >&2

# ---- Fast: server-side topk first ----
CPU_EXPR="sum by (namespace, name) (rate(kubevirt_vmi_cpu_usage_seconds_total[${WINDOW}]))"
TOP_CPU_Q="topk(${TOP_N}, ${CPU_EXPR})"
TOP_CPU_JSON="$(mon_post "$TOP_CPU_Q")"

jq -e '.status=="success"' >/dev/null 2>&1 <<<"$TOP_CPU_JSON" || {
  echo "ERROR: topk CPU query failed. Raw:" >&2
  echo "$TOP_CPU_JSON" >&2
  exit 1
}

TOP_LIST_TSV="$(
  jq -r '.data.result[] | [.metric.namespace, .metric.name, (.value[1]|tonumber)] | @tsv' <<<"$TOP_CPU_JSON"
)"

if [[ -z "$TOP_LIST_TSV" ]]; then
  echo "No results from topk query."
  exit 0
fi

WANTED_KEYS_JSON="$(jq -Rn --arg s "$TOP_LIST_TSV" '
  ($s | split("\n") | map(select(length>0))) as $lines
  | reduce $lines[] as $l ({}; ($l|split("\t")) as $p | . + { ($p[0]+"/"+$p[1]): ($p[2]|tonumber) })
')"

# Optional wedge metrics (maps) — only if MODE=wedge
RX_MAP="{}"; TX_MAP="{}"; RD_MAP="{}"; WR_MAP="{}"
if [[ "$MODE" == "wedge" ]]; then
  to_map='
    .data.result
    | reduce .[] as $r ({}; . + { (($r.metric.namespace + "/" + $r.metric.name)): ($r.value[1]|tonumber) })
  '
  RX_JSON="$(mon_post "sum by (namespace, name) (rate(kubevirt_vmi_network_receive_bytes_total[${WINDOW}]))" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}')"
  TX_JSON="$(mon_post "sum by (namespace, name) (rate(kubevirt_vmi_network_transmit_bytes_total[${WINDOW}]))" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}')"
  RD_JSON="$(mon_post "sum by (namespace, name) (rate(kubevirt_vmi_storage_read_traffic_bytes_total[${WINDOW}]))" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}')"
  WR_JSON="$(mon_post "sum by (namespace, name) (rate(kubevirt_vmi_storage_write_traffic_bytes_total[${WINDOW}]))" 2>/dev/null || echo '{"status":"success","data":{"result":[]}}')"

  RX_MAP="$(jq -c "$to_map" <<<"$RX_JSON" 2>/dev/null || echo '{}')"
  TX_MAP="$(jq -c "$to_map" <<<"$TX_JSON" 2>/dev/null || echo '{}')"
  RD_MAP="$(jq -c "$to_map" <<<"$RD_JSON" 2>/dev/null || echo '{}')"
  WR_MAP="$(jq -c "$to_map" <<<"$WR_JSON" 2>/dev/null || echo '{}')"
fi

# Pull VMIs once and filter to wanted keys (fast)
VMI_JSON="$(oc get "${VMI_RES}" -A -o json)"

echo "Monitoring: svc/${PF_SVC} via https://127.0.0.1:${PF_PORT}"
echo "Mode=${MODE} Window=${WINDOW} Top=${TOP_N}"
if [[ "$MODE" == "wedge" ]]; then
  echo "Wedge filters: CPU>=${THRESH_PCT}% net<=${NET_MAX_BPS}B/s disk<=${DISK_MAX_BPS}B/s"
fi
echo

HEADER=$'NODE\tNAMESPACE\tVMI\tVM(owner)\tvCPU\tCPU(cores)\tCPU(%)\tNET(B/s)\tDISK(B/s)'
printf "%s\n" "$HEADER" | (command -v column >/dev/null 2>&1 && column -t -s $'\t' || cat)

jq -r --argjson wanted "$WANTED_KEYS_JSON" \
      --argjson rx "$RX_MAP" --argjson tx "$TX_MAP" --argjson rd "$RD_MAP" --argjson wr "$WR_MAP" \
      --arg mode "$MODE" --arg thresh "$THRESH_PCT" --arg netmax "$NET_MAX_BPS" --arg diskmax "$DISK_MAX_BPS" '
  def vcpu_count(v):
    ((v.spec.domain.cpu.cores // 1) *
     (v.spec.domain.cpu.sockets // 1) *
     (v.spec.domain.cpu.threads // 1));
  def pct(a;b): if b <= 0 then 0 else (a / b * 100) end;

  .items[]
  | (.metadata.namespace + "/" + .metadata.name) as $k
  | ($wanted[$k] // null) as $cpuCores
  | select($cpuCores != null)
  | (.status.nodeName // "") as $node
  | select($node != "")
  | (vcpu_count(.)|tonumber) as $vcpus
  | (pct($cpuCores; $vcpus)) as $cpuPct
  | (($rx[$k] // 0) + ($tx[$k] // 0)) as $netBps
  | (($rd[$k] // 0) + ($wr[$k] // 0)) as $diskBps
  | ( ((.metadata.ownerReferences // []) | map(select(.kind=="VirtualMachine"))[0].name) // "" ) as $vmOwner
  | if $mode == "wedge" then
      select($cpuPct >= ($thresh|tonumber))
      | select($netBps <= ($netmax|tonumber))
      | select($diskBps <= ($diskmax|tonumber))
    else .
    end
  | [
      $node,
      .metadata.namespace,
      .metadata.name,
      $vmOwner,
      ($vcpus|tostring),
      ($cpuCores|tostring),
      ($cpuPct|tostring),
      ($netBps|tostring),
      ($diskBps|tostring)
    ] | @tsv
' <<<"$VMI_JSON" \
| (command -v column >/dev/null 2>&1 && column -t -s $'\t' || cat)
