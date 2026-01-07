#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Defaults (override via flags)
# -----------------------------
DEFAULT_MODE="wedge"          # wedge | top
DEFAULT_WINDOW="10m"
DEFAULT_TOP_N=200

DEFAULT_CPU_MIN_PCT=80
DEFAULT_NET_MAX_BPS=20000
DEFAULT_DISK_MAX_BPS=20000

DEFAULT_IOWAIT_MIN_CORES=0
DEFAULT_DELAY_MIN_CORES=0

DEFAULT_STEADY=1
DEFAULT_JITTER_MAX_PCT=15
DEFAULT_PEAKAVG_MAX_PCT=15

DEFAULT_LIMIT=0

# -----------------------------
# Parsed args -> variables
# -----------------------------
MODE="$DEFAULT_MODE"
WINDOW="$DEFAULT_WINDOW"
TOP_N="$DEFAULT_TOP_N"
CPU_MIN_PCT="$DEFAULT_CPU_MIN_PCT"
NET_MAX_BPS="$DEFAULT_NET_MAX_BPS"
DISK_MAX_BPS="$DEFAULT_DISK_MAX_BPS"
IOWAIT_MIN_CORES="$DEFAULT_IOWAIT_MIN_CORES"
DELAY_MIN_CORES="$DEFAULT_DELAY_MIN_CORES"
STEADY="$DEFAULT_STEADY"
JITTER_MAX_PCT="$DEFAULT_JITTER_MAX_PCT"
PEAKAVG_MAX_PCT="$DEFAULT_PEAKAVG_MAX_PCT"
LIMIT="$DEFAULT_LIMIT"
DEBUG=0
INSPECT_NS=""
INSPECT_VMI=""

usage() {
  cat <<EOF
likely-bsod-vms.sh

Modes:
  --mode wedge   (default) High avg CPU% + low net + low disk + (optionally) steady CPU, iowait/delay thresholds
  --mode top     Show top VMIs by avg CPU% (no wedge filtering)

Defaults (override via flags):
  --window $DEFAULT_WINDOW
  --top $DEFAULT_TOP_N
  --cpu-min $DEFAULT_CPU_MIN_PCT
  --net-max $DEFAULT_NET_MAX_BPS
  --disk-max $DEFAULT_DISK_MAX_BPS
  --steady / --no-steady
  --jitter-max $DEFAULT_JITTER_MAX_PCT
  --peakavg-max $DEFAULT_PEAKAVG_MAX_PCT
  --iowait-min $DEFAULT_IOWAIT_MIN_CORES
  --delay-min $DEFAULT_DELAY_MIN_CORES
  --limit $DEFAULT_LIMIT   (0 = unlimited)

Inspect:
  --inspect <namespace> <vmi>

Examples:
  ./likely-bsod-vms.sh
  ./likely-bsod-vms.sh --mode top --top 30
  ./likely-bsod-vms.sh --cpu-min 90 --net-max 10000 --disk-max 10000
  ./likely-bsod-vms.sh --no-steady
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2;;
    --window) WINDOW="$2"; shift 2;;
    --top) TOP_N="$2"; shift 2;;
    --cpu-min) CPU_MIN_PCT="$2"; shift 2;;
    --net-max) NET_MAX_BPS="$2"; shift 2;;
    --disk-max) DISK_MAX_BPS="$2"; shift 2;;
    --iowait-min) IOWAIT_MIN_CORES="$2"; shift 2;;
    --delay-min) DELAY_MIN_CORES="$2"; shift 2;;
    --steady) STEADY=1; shift 1;;
    --no-steady) STEADY=0; shift 1;;
    --jitter-max) JITTER_MAX_PCT="$2"; shift 2;;
    --peakavg-max) PEAKAVG_MAX_PCT="$2"; shift 2;;
    --limit) LIMIT="$2"; shift 2;;
    --inspect) INSPECT_NS="$2"; INSPECT_VMI="$3"; shift 3;;
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

# -----------------------------
# Port-forward (auto)
# -----------------------------
PF_PID=""
PF_PORT=""
PF_SVC=""
cleanup() { [[ -n "${PF_PID:-}" ]] && kill "${PF_PID}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

pick_free_port() {
  local p i
  for i in {1..120}; do
    p=$(( (RANDOM % 15000) + 20000 ))
    if ! curl --ipv4 --http1.1 -ksS "https://127.0.0.1:${p}/" >/dev/null 2>&1; then
      echo "$p"; return 0
    fi
  done
  echo 29091
}

mon_post() {
  local q="$1"
  curl --ipv4 --http1.1 -ksS \
    --connect-timeout 2 --max-time 40 \
    -H "Authorization: Bearer ${TOKEN}" \
    "https://127.0.0.1:${PF_PORT}/api/v1/query" \
    --data-urlencode "query=${q}"
}

wait_listening() {
  local i
  for i in {1..120}; do
    if curl --ipv4 --http1.1 -k -sS "https://127.0.0.1:${PF_PORT}/" >/dev/null 2>&1; then
      return 0
    fi
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

  if ! wait_listening; then
    kill "${PF_PID}" >/dev/null 2>&1 || true
    PF_PID=""; PF_PORT=""; PF_SVC=""
    return 1
  fi

  local i out
  for i in {1..80}; do
    out="$(mon_post "up" 2>/dev/null || true)"
    if jq -e '.status=="success"' >/dev/null 2>&1 <<<"$out"; then
      return 0
    fi
    sleep 0.2
  done

  kill "${PF_PID}" >/dev/null 2>&1 || true
  PF_PID=""; PF_PORT=""; PF_SVC=""
  return 1
}

if start_pf "thanos-querier"; then :; elif start_pf "prometheus-k8s"; then :; else
  echo "ERROR: Could not establish monitoring port-forward." >&2
  exit 1
fi

[[ "$DEBUG" -eq 1 ]] && echo "DEBUG: monitoring via svc/${PF_SVC} on https://127.0.0.1:${PF_PORT}" >&2

# -----------------------------
# Inspect mode
# -----------------------------
if [[ -n "${INSPECT_NS:-}" && -n "${INSPECT_VMI:-}" ]]; then
  echo "Monitoring: svc/${PF_SVC} via https://127.0.0.1:${PF_PORT}"
  echo "Inspecting labels for ${INSPECT_NS}/${INSPECT_VMI} over WINDOW=${WINDOW}"
  echo

  QUERIES=(
    "CPU: kubevirt_vmi_cpu_usage_seconds_total{namespace=\"${INSPECT_NS}\",name=\"${INSPECT_VMI}\"}"
    "IOWAIT: kubevirt_vmi_vcpu_wait_seconds_total{namespace=\"${INSPECT_NS}\",name=\"${INSPECT_VMI}\"}"
    "DELAY: kubevirt_vmi_vcpu_delay_seconds_total{namespace=\"${INSPECT_NS}\",name=\"${INSPECT_VMI}\"}"
    "NET RX: kubevirt_vmi_network_receive_bytes_total{namespace=\"${INSPECT_NS}\",name=\"${INSPECT_VMI}\"}"
    "NET TX: kubevirt_vmi_network_transmit_bytes_total{namespace=\"${INSPECT_NS}\",name=\"${INSPECT_VMI}\"}"
    "DISK RD: kubevirt_vmi_storage_read_traffic_bytes_total{namespace=\"${INSPECT_NS}\",name=\"${INSPECT_VMI}\"}"
    "DISK WR: kubevirt_vmi_storage_write_traffic_bytes_total{namespace=\"${INSPECT_NS}\",name=\"${INSPECT_VMI}\"}"
  )
  for item in "${QUERIES[@]}"; do
    title="${item%%:*}"
    q="${item#*: }"
    echo "== ${title} =="
    mon_post "$q" | jq -r '
      if .status!="success" then "query failed"
      else (.data.result | if length==0 then "no series"
            else (.[].metric | to_entries | sort_by(.key) | map("\(.key)=\(.value)") | join(", "))
            end)
      end
    '
    echo
  done
  exit 0
fi

# -----------------------------
# PromQL building blocks
# -----------------------------
CPU_CORES_EXPR="sum by (namespace, name) (rate(kubevirt_vmi_cpu_usage_seconds_total[${WINDOW}]))"

# Use SUBQUERY form [WINDOW:] which is allowed on expressions
CPU_AVG_CORES_Q="avg_over_time((${CPU_CORES_EXPR})[${WINDOW}:])"
CPU_MAX_CORES_Q="max_over_time((${CPU_CORES_EXPR})[${WINDOW}:])"
CPU_STD_CORES_Q="stddev_over_time((${CPU_CORES_EXPR})[${WINDOW}:])"

TOP_CPU_Q="topk(${TOP_N}, ${CPU_AVG_CORES_Q})"
[[ "$DEBUG" -eq 1 ]] && echo "DEBUG: TOP_CPU_Q=${TOP_CPU_Q}" >&2

TOP_CPU_JSON="$(mon_post "$TOP_CPU_Q")"
jq -e '.status=="success"' >/dev/null 2>&1 <<<"$TOP_CPU_JSON" || {
  echo "ERROR: topk CPU query failed. Raw:" >&2
  echo "$TOP_CPU_JSON" >&2
  exit 1
}

TOP_LIST_TSV="$(jq -r '.data.result[] | [.metric.namespace, .metric.name, (.value[1]|tonumber)] | @tsv' <<<"$TOP_CPU_JSON")"
[[ -n "$TOP_LIST_TSV" ]] || { echo "No results from topk query."; exit 0; }

WANTED_AVG_CPU_MAP="$(jq -Rn --arg s "$TOP_LIST_TSV" '
  ($s | split("\n") | map(select(length>0))) as $lines
  | reduce $lines[] as $l ({}; ($l|split("\t")) as $p | . + { ($p[0]+"/"+$p[1]): ($p[2]|tonumber) })
')"

to_map='
  .data.result
  | reduce .[] as $r ({}; . + { (($r.metric.namespace + "/" + $r.metric.name)): ($r.value[1]|tonumber) })
'

CPU_MAX_MAP="$(mon_post "${CPU_MAX_CORES_Q}" | jq -c "$to_map" 2>/dev/null || echo '{}')"
CPU_STD_MAP="$(mon_post "${CPU_STD_CORES_Q}" | jq -c "$to_map" 2>/dev/null || echo '{}')"

IOW_MAP="$(mon_post "sum by (namespace, name) (rate(kubevirt_vmi_vcpu_wait_seconds_total[${WINDOW}]))" | jq -c "$to_map" 2>/dev/null || echo '{}')"
DLY_MAP="$(mon_post "sum by (namespace, name) (irate(kubevirt_vmi_vcpu_delay_seconds_total[${WINDOW}]))" | jq -c "$to_map" 2>/dev/null || echo '{}')"

RX_MAP="{}"; TX_MAP="{}"; RD_MAP="{}"; WR_MAP="{}"
if [[ "$MODE" == "wedge" ]]; then
  RX_MAP="$(mon_post "sum by (namespace, name) (rate(kubevirt_vmi_network_receive_bytes_total[${WINDOW}]))" | jq -c "$to_map" 2>/dev/null || echo '{}')"
  TX_MAP="$(mon_post "sum by (namespace, name) (rate(kubevirt_vmi_network_transmit_bytes_total[${WINDOW}]))" | jq -c "$to_map" 2>/dev/null || echo '{}')"
  RD_MAP="$(mon_post "sum by (namespace, name) (rate(kubevirt_vmi_storage_read_traffic_bytes_total[${WINDOW}]))" | jq -c "$to_map" 2>/dev/null || echo '{}')"
  WR_MAP="$(mon_post "sum by (namespace, name) (rate(kubevirt_vmi_storage_write_traffic_bytes_total[${WINDOW}]))" | jq -c "$to_map" 2>/dev/null || echo '{}')"
fi

VMI_JSON="$(oc get "${VMI_RES}" -A -o json)"

echo "Monitoring: svc/${PF_SVC} via https://127.0.0.1:${PF_PORT}"
echo "Mode=${MODE} Window=${WINDOW} TopCandidates=${TOP_N}"
if [[ "$MODE" == "wedge" ]]; then
  echo "Filters: CPU>=${CPU_MIN_PCT}% net<=${NET_MAX_BPS} disk<=${DISK_MAX_BPS} steady=${STEADY} jitter<=${JITTER_MAX_PCT}% peak-avg<=${PEAKAVG_MAX_PCT}% iowait>=${IOWAIT_MIN_CORES} delay>=${DELAY_MIN_CORES}"
fi
echo

HEADER=$'SCORE\tNODE\tNAMESPACE\tVMI\tVM(owner)\tvCPU\tAVG_CPU(%)\tAVG_CPU(cores)\tMAX_CPU(%)\tJITTER(%)\tPEAK-AVG(%)\tIOWAIT(cores)\tDELAY(cores)\tNET(B/s)\tDISK(B/s)'
printf "%s\n" "$HEADER" | (command -v column >/dev/null 2>&1 && column -t -s $'\t' || cat)

jq -r \
  --argjson avgCpu "$WANTED_AVG_CPU_MAP" \
  --argjson cpuMax "$CPU_MAX_MAP" \
  --argjson cpuStd "$CPU_STD_MAP" \
  --argjson rx "$RX_MAP" --argjson tx "$TX_MAP" --argjson rd "$RD_MAP" --argjson wr "$WR_MAP" \
  --argjson iow "$IOW_MAP" --argjson dly "$DLY_MAP" \
  --arg mode "$MODE" \
  --arg cpuMin "$CPU_MIN_PCT" \
  --arg netMax "$NET_MAX_BPS" \
  --arg diskMax "$DISK_MAX_BPS" \
  --arg steady "$STEADY" \
  --arg jitterMax "$JITTER_MAX_PCT" \
  --arg peakavgMax "$PEAKAVG_MAX_PCT" \
  --arg iowMin "$IOWAIT_MIN_CORES" \
  --arg dlyMin "$DELAY_MIN_CORES" \
  --arg limit "$LIMIT" '
  def vcpu_count(v):
    ((v.spec.domain.cpu.cores // 1) *
     (v.spec.domain.cpu.sockets // 1) *
     (v.spec.domain.cpu.threads // 1));
  def pct(a;b): if b <= 0 then 0 else (a / b * 100) end;

  def f2(x): (x|tonumber) as $v | ( ($v*100 | round) / 100 );
  def i0(x): (x|tonumber | round);

  def score(avgPct; net; disk; jitter; peakavg; iow; dly):
    (avgPct)
    + ( (20000 - (net|tonumber)) / 4000 )
    + ( (20000 - (disk|tonumber)) / 4000 )
    + ( (15 - (jitter|tonumber)) / 2 )
    + ( (15 - (peakavg|tonumber)) / 2 )
    + (iow*20) + (dly*20);

  [
    .items[]
    | (.metadata.namespace + "/" + .metadata.name) as $k
    | ($avgCpu[$k] // null) as $avgCores
    | select($avgCores != null)
    | (.status.nodeName // "") as $node
    | select($node != "")
    | (vcpu_count(.)|tonumber) as $vcpus

    | ($avgCores|tonumber) as $avgC
    | (($cpuMax[$k] // $avgC)|tonumber) as $maxC
    | (($cpuStd[$k] // 0)|tonumber) as $stdC

    | (pct($avgC; $vcpus)) as $avgPct
    | (pct($maxC; $vcpus)) as $maxPct

    | (if $avgC <= 0 then 0 else (100 * ($stdC / $avgC)) end) as $jitterPct
    | (($maxPct - $avgPct)) as $peakavgPct

    | (($rx[$k] // 0) + ($tx[$k] // 0)) as $netBps
    | (($rd[$k] // 0) + ($wr[$k] // 0)) as $diskBps
    | (($iow[$k] // 0)|tonumber) as $iowC
    | (($dly[$k] // 0)|tonumber) as $dlyC

    | ( ((.metadata.ownerReferences // []) | map(select(.kind=="VirtualMachine"))[0].name) // "" ) as $vmOwner

    | (score($avgPct; $netBps; $diskBps; $jitterPct; $peakavgPct; $iowC; $dlyC)) as $score

    | {
        score:$score, node:$node, ns:.metadata.namespace, vmi:.metadata.name, owner:$vmOwner,
        vcpus:$vcpus,
        avgPct:$avgPct, avgC:$avgC,
        maxPct:$maxPct,
        jitter:$jitterPct,
        peakavg:$peakavgPct,
        iow:$iowC, dly:$dlyC,
        net:$netBps, disk:$diskBps
      }
  ] as $rows
  | (
      if $mode == "wedge" then
        $rows
        | map(select(.avgPct >= ($cpuMin|tonumber)))
        | map(select(.net <= ($netMax|tonumber)))
        | map(select(.disk <= ($diskMax|tonumber)))
        | (if ($iowMin|tonumber) > 0 then map(select(.iow >= ($iowMin|tonumber))) else . end)
        | (if ($dlyMin|tonumber) > 0 then map(select(.dly >= ($dlyMin|tonumber))) else . end)
        | (if ($steady|tonumber) == 1 then
            map(select(.jitter <= ($jitterMax|tonumber)))
            | map(select(.peakavg <= ($peakavgMax|tonumber)))
          else .
          end)
      else
        $rows
      end
    )
  | sort_by(-.score)
  | (if ($limit|tonumber) > 0 then .[0:($limit|tonumber)] else . end)
  | .[]
  | [
      (f2(.score)|tostring),
      .node, .ns, .vmi, .owner,
      (.vcpus|tostring),
      (f2(.avgPct)|tostring),
      (f2(.avgC)|tostring),
      (f2(.maxPct)|tostring),
      (f2(.jitter)|tostring),
      (f2(.peakavg)|tostring),
      (f2(.iow)|tostring),
      (f2(.dly)|tostring),
      (i0(.net)|tostring),
      (i0(.disk)|tostring)
    ] | @tsv
' <<<"$VMI_JSON" \
| (command -v column >/dev/null 2>&1 && column -t -s $'\t' || cat)
