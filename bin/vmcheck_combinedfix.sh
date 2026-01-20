#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Defaults (override via flags)
# -----------------------------
DEFAULT_MODE="wedge"          # wedge | quiet | top
DEFAULT_WINDOW="10m"
DEFAULT_TOP_N=200

DEFAULT_CPU_MIN_PCT=80

# Combined output (wedge OR quiet) based on score threshold
DEFAULT_SCORE_MIN=""

# Option A (strict defaults)
DEFAULT_NET_MAX_BPS=5000
DEFAULT_DISK_MAX_BPS=15000

DEFAULT_IOWAIT_MIN_CORES=0
DEFAULT_DELAY_MIN_CORES=0

DEFAULT_STEADY=1
DEFAULT_JITTER_MAX_PCT=3
DEFAULT_PEAKAVG_MAX_PCT=3
DEFAULT_STEADY_ONLY=0

DEFAULT_REQUIRE_IO_METRICS=0
DEFAULT_LIMIT=0

# display widths (truncate long fields)
DEFAULT_W_NODE=18
DEFAULT_W_NS=18
DEFAULT_W_VMI=28
DEFAULT_W_VM=28

# suggest-quiet percentile (LOW tail)
DEFAULT_SUGGEST_PCT=20

# -----------------------------
# Parsed args -> variables
# -----------------------------
MODE="$DEFAULT_MODE"
WINDOW="$DEFAULT_WINDOW"
TOP_N="$DEFAULT_TOP_N"
CPU_MIN_PCT="$DEFAULT_CPU_MIN_PCT"
SCORE_MIN="$DEFAULT_SCORE_MIN"
SCORE_MIN_SET=0
NET_MAX_BPS="$DEFAULT_NET_MAX_BPS"
DISK_MAX_BPS="$DEFAULT_DISK_MAX_BPS"
IOWAIT_MIN_CORES="$DEFAULT_IOWAIT_MIN_CORES"
DELAY_MIN_CORES="$DEFAULT_DELAY_MIN_CORES"
STEADY="$DEFAULT_STEADY"
JITTER_MAX_PCT="$DEFAULT_JITTER_MAX_PCT"
PEAKAVG_MAX_PCT="$DEFAULT_PEAKAVG_MAX_PCT"
STEADY_ONLY="$DEFAULT_STEADY_ONLY"
REQUIRE_IO_METRICS="$DEFAULT_REQUIRE_IO_METRICS"
LIMIT="$DEFAULT_LIMIT"
DEBUG=0
INSPECT_NS=""
INSPECT_VMI=""

# Quiet selector flags:
# With your change request, quiet defaults to NET AND DISK.
QUIET_NET_ONLY=0
QUIET_DISK_ONLY=0

SUGGEST_QUIET=0
SUGGEST_PCT="$DEFAULT_SUGGEST_PCT"
QUIET_SUGGESTED=0

# Track whether user explicitly set cpu-min
CPU_MIN_EXPLICIT=0
# Track whether user explicitly set net/disk thresholds (so quiet-suggested can override only if desired)
NET_MAX_EXPLICIT=0
DISK_MAX_EXPLICIT=0

W_NODE="$DEFAULT_W_NODE"
W_NS="$DEFAULT_W_NS"
W_VMI="$DEFAULT_W_VMI"
W_VM="$DEFAULT_W_VM"

script_name() {
  # show how the user invoked it, otherwise default
  basename "${0:-vmcheck.sh}"
}

usage() {
  local me
  me="$(script_name)"
  cat <<EOF
${me}

USAGE:
  ./${me} [options]
  ./${me} help
  ./${me} -h|--help

MODES:
  --mode wedge   (default)  CPU>=cpu-min AND NET<=net-max AND DISK<=disk-max (+ steady if enabled)
  --mode quiet             Quiet by IO (default selector is NET quiet AND DISK quiet)
  --mode top               Top N by CPU; no wedge/quiet filters

QUIET MODE NOTE:
  If you run --mode quiet and you do NOT explicitly set --cpu-min,
  CPU filtering is disabled (treated as --cpu-min 0).

QUIET SELECTOR FLAGS (only affect --mode quiet):
  --quiet-net-only         quiet if NET<=net-max (disk ignored)
  --quiet-disk-only        quiet if DISK<=disk-max (net ignored)
  (If neither flag is set: quiet requires NET quiet AND DISK quiet)

THRESHOLDS:
  --score <0-100>          output combined (wedge OR quiet) list with SCORE>=value (ignores --mode)
  --window <dur>           default ${DEFAULT_WINDOW}
  --top <N>                default ${DEFAULT_TOP_N}
  --limit <N>              default 0 (unlimited)
  --cpu-min <pct>          default ${DEFAULT_CPU_MIN_PCT} (quiet mode becomes 0 if not set)
  --net-max <bps>          default ${DEFAULT_NET_MAX_BPS}
  --disk-max <bps>         default ${DEFAULT_DISK_MAX_BPS}
  --jitter-max <pct>       default ${DEFAULT_JITTER_MAX_PCT}
  --peakavg-max <pct>      default ${DEFAULT_PEAKAVG_MAX_PCT}
  --iowait-min <cores>     default ${DEFAULT_IOWAIT_MIN_CORES}
  --delay-min <cores>      default ${DEFAULT_DELAY_MIN_CORES}

STEADY:
  --steady / --no-steady
  --steady-only / --no-steady-only

METRIC PRESENCE:
  --require-io-metrics / --no-require-io-metrics

DISPLAY:
  --w-node <N>             truncate NODE to N chars (default ${DEFAULT_W_NODE})
  --w-ns <N>               truncate NS to N chars (default ${DEFAULT_W_NS})
  --w-vmi <N>              truncate VMI to N chars (default ${DEFAULT_W_VMI})
  --w-vm <N>               truncate VM to N chars (default ${DEFAULT_W_VM})

HELPERS:
  --suggest-quiet           suggest IO quiet thresholds using LOW percentile (default p${DEFAULT_SUGGEST_PCT}) of candidate set
  --suggest-pct <N>         percentile for suggest-quiet (1..99), default ${DEFAULT_SUGGEST_PCT}
  --quiet-suggested         (quiet mode) auto-compute thresholds (as in --suggest-quiet) and use them for THIS run

INSPECT:
  --inspect <namespace> <vmi>

DEBUG:
  --debug

EXAMPLES:
  ./${me} --mode quiet
  ./${me} --mode quiet --quiet-suggested --limit 50
  ./${me} --mode quiet --net-max 5000 --disk-max 15000
  ./${me} --mode quiet --quiet-net-only --net-max 1000
  ./${me} --score 80
  ./${me} --suggest-quiet
  ./${me} --suggest-quiet --suggest-pct 15
EOF
}

if [[ "${1:-}" == "help" ]]; then usage; exit 0; fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2;;
    --window) WINDOW="$2"; shift 2;;
    --top) TOP_N="$2"; shift 2;;
    --limit) LIMIT="$2"; shift 2;;
    --score) SCORE_MIN="$2"; SCORE_MIN_SET=1; shift 2;;
    --cpu-min) CPU_MIN_PCT="$2"; CPU_MIN_EXPLICIT=1; shift 2;;
    --net-max) NET_MAX_BPS="$2"; NET_MAX_EXPLICIT=1; shift 2;;
    --disk-max) DISK_MAX_BPS="$2"; DISK_MAX_EXPLICIT=1; shift 2;;
    --iowait-min) IOWAIT_MIN_CORES="$2"; shift 2;;
    --delay-min) DELAY_MIN_CORES="$2"; shift 2;;
    --steady) STEADY=1; shift 1;;
    --no-steady) STEADY=0; shift 1;;
    --jitter-max) JITTER_MAX_PCT="$2"; shift 2;;
    --peakavg-max) PEAKAVG_MAX_PCT="$2"; shift 2;;
    --steady-only) STEADY_ONLY=1; shift 1;;
    --no-steady-only) STEADY_ONLY=0; shift 1;;
    --require-io-metrics) REQUIRE_IO_METRICS=1; shift 1;;
    --no-require-io-metrics) REQUIRE_IO_METRICS=0; shift 1;;

    --quiet-net-only) QUIET_NET_ONLY=1; shift 1;;
    --quiet-disk-only) QUIET_DISK_ONLY=1; shift 1;;

    --inspect) INSPECT_NS="$2"; INSPECT_VMI="$3"; shift 3;;

    --suggest-quiet) SUGGEST_QUIET=1; shift 1;;
    --suggest-pct) SUGGEST_PCT="$2"; shift 2;;
    --quiet-suggested) QUIET_SUGGESTED=1; shift 1;;

    --w-node) W_NODE="$2"; shift 2;;
    --w-ns) W_NS="$2"; shift 2;;
    --w-vmi) W_VMI="$2"; shift 2;;
    --w-vm) W_VM="$2"; shift 2;;

    --debug) DEBUG=1; shift 1;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

# Validate --score (combined output)
if [[ "$SCORE_MIN_SET" -eq 1 ]]; then
  if ! [[ "$SCORE_MIN" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "ERROR: --score must be a number 0..100" >&2
    exit 2
  fi
  # clamp / validate range (allow decimals but treat as numeric)
  if awk -v s="$SCORE_MIN" 'BEGIN{exit !(s>=0 && s<=100)}'; then :; else
    echo "ERROR: --score must be between 0 and 100" >&2
    exit 2
  fi
  MODE="combined"
fi

# Quiet mode: default cpu-min to 0 unless explicitly set
if [[ "$MODE" == "quiet" && "$CPU_MIN_EXPLICIT" -eq 0 ]]; then
  CPU_MIN_PCT=0
fi

# validate suggest pct
if [[ "$SUGGEST_QUIET" -eq 1 || "$QUIET_SUGGESTED" -eq 1 ]]; then
  if ! [[ "$SUGGEST_PCT" =~ ^[0-9]+$ ]] || [[ "$SUGGEST_PCT" -lt 1 || "$SUGGEST_PCT" -gt 99 ]]; then
    echo "ERROR: --suggest-pct must be an integer 1..99" >&2
    exit 2
  fi
fi

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
    --connect-timeout 2 --max-time 60 \
    -H "Authorization: Bearer ${TOKEN}" \
    "https://127.0.0.1:${PF_PORT}/api/v1/query" \
    --data-urlencode "query=${q}"
}

wait_listening() {
  local i
  for i in {1..160}; do
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
  for i in {1..120}; do
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
  echo "Inspecting series labels for ${INSPECT_NS}/${INSPECT_VMI} over WINDOW=${WINDOW}"
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
# PromQL blocks
# -----------------------------

# In combined mode, --top is most useful as an output-size knob, but we also want to
# avoid missing quiet candidates that won't show up in a small CPU-based topk.
# So we (a) treat --top as an implicit --limit when limit is 0, and (b) increase the
# internal fetch Top used for Prometheus topk() queries.
FETCH_TOP_N="$TOP_N"
if [[ "$MODE" == "combined" ]]; then
  if [[ "$LIMIT" == "0" || -z "$LIMIT" ]]; then
    LIMIT="$TOP_N"
  fi
  # Multiply by 20 (with a sensible floor) to catch low-CPU quiet VMs.
  FETCH_TOP_N=$(( TOP_N * 20 ))
  if [[ "$FETCH_TOP_N" -lt 500 ]]; then FETCH_TOP_N=500; fi
  if [[ "$FETCH_TOP_N" -gt 5000 ]]; then FETCH_TOP_N=5000; fi
fi

CPU_CORES_EXPR="sum by (namespace, name) (rate(kubevirt_vmi_cpu_usage_seconds_total[${WINDOW}]))"
CPU_AVG_CORES_Q="avg_over_time((${CPU_CORES_EXPR})[${WINDOW}:])"
CPU_MAX_CORES_Q="max_over_time((${CPU_CORES_EXPR})[${WINDOW}:])"
CPU_STD_CORES_Q="stddev_over_time((${CPU_CORES_EXPR})[${WINDOW}:])"

TOP_CPU_Q="topk(${FETCH_TOP_N:-$TOP_N}, ${CPU_AVG_CORES_Q})"
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

PRESENT_RX="$(mon_post "count by (namespace, name) (rate(kubevirt_vmi_network_receive_bytes_total[${WINDOW}]))" | jq -c "$to_map" 2>/dev/null || echo '{}')"
PRESENT_TX="$(mon_post "count by (namespace, name) (rate(kubevirt_vmi_network_transmit_bytes_total[${WINDOW}]))" | jq -c "$to_map" 2>/dev/null || echo '{}')"
PRESENT_RD="$(mon_post "count by (namespace, name) (rate(kubevirt_vmi_storage_read_traffic_bytes_total[${WINDOW}]))" | jq -c "$to_map" 2>/dev/null || echo '{}')"
PRESENT_WR="$(mon_post "count by (namespace, name) (rate(kubevirt_vmi_storage_write_traffic_bytes_total[${WINDOW}]))" | jq -c "$to_map" 2>/dev/null || echo '{}')"

RX_MAP="$(mon_post "sum by (namespace, name) (rate(kubevirt_vmi_network_receive_bytes_total[${WINDOW}]))" | jq -c "$to_map" 2>/dev/null || echo '{}')"
TX_MAP="$(mon_post "sum by (namespace, name) (rate(kubevirt_vmi_network_transmit_bytes_total[${WINDOW}]))" | jq -c "$to_map" 2>/dev/null || echo '{}')"
RD_MAP="$(mon_post "sum by (namespace, name) (rate(kubevirt_vmi_storage_read_traffic_bytes_total[${WINDOW}]))" | jq -c "$to_map" 2>/dev/null || echo '{}')"
WR_MAP="$(mon_post "sum by (namespace, name) (rate(kubevirt_vmi_storage_write_traffic_bytes_total[${WINDOW}]))" | jq -c "$to_map" 2>/dev/null || echo '{}')"

VMI_JSON="$(oc get "${VMI_RES}" -A -o json)"

# -----------------------------
# Suggest quiet thresholds helper (low percentile over candidate set)
# -----------------------------
suggest_quiet_thresholds() {
  local pct="$1"

  local data_tsv net_ser disk_ser net_p disk_p

  data_tsv="$(
    jq -r \
      --argjson avgCpu "$WANTED_AVG_CPU_MAP" \
      --argjson rx "$RX_MAP" --argjson tx "$TX_MAP" --argjson rd "$RD_MAP" --argjson wr "$WR_MAP" \
      --argjson prx "$PRESENT_RX" --argjson ptx "$PRESENT_TX" --argjson prd "$PRESENT_RD" --argjson pwr "$PRESENT_WR" '
      .items[]
      | (.metadata.namespace + "/" + .metadata.name) as $k
      | select(($avgCpu[$k] // null) != null)

      | ((($prx[$k] // null) != null) or (($ptx[$k] // null) != null)) as $hasNet
      | ((($prd[$k] // null) != null) or (($pwr[$k] // null) != null)) as $hasDisk

      | (if $hasNet then (($rx[$k] // 0) + ($tx[$k] // 0)) else 0 end) as $net
      | (if $hasDisk then (($rd[$k] // 0) + ($wr[$k] // 0)) else 0 end) as $disk

      | [$net, $disk] | @tsv
    ' <<<"$VMI_JSON"
  )"

  if [[ -z "$data_tsv" ]]; then
    echo "0 0"
    return 0
  fi

  suggest_pct() {
    local pct="$1"
    awk -v P="$pct" '
      { a[NR]=$1 }
      END {
        if (NR==0) { print 0; exit }
        for (i=1;i<=NR;i++) for (j=i+1;j<=NR;j++) if (a[i]>a[j]) { t=a[i]; a[i]=a[j]; a[j]=t }
        idx=int(NR*(P/100.0)); if (idx<1) idx=1; if (idx>NR) idx=NR;
        printf "%.0f\n", a[idx]
      }
    '
  }

  net_ser="$(printf "%s\n" "$data_tsv" | awk '{print $1}')"
  disk_ser="$(printf "%s\n" "$data_tsv" | awk '{print $2}')"

  # Prefer non-zero samples if enough exist (avoid suggesting 0)
  if printf "%s\n" "$net_ser" | awk '($1>0){c++} END{exit !(c>=20)}' >/dev/null 2>&1; then
    net_p="$(printf "%s\n" "$net_ser" | awk '($1>0){print $1}' | suggest_pct "$pct")"
  else
    net_p="$(printf "%s\n" "$net_ser" | suggest_pct "$pct")"
  fi

  if printf "%s\n" "$disk_ser" | awk '($1>0){c++} END{exit !(c>=20)}' >/dev/null 2>&1; then
    disk_p="$(printf "%s\n" "$disk_ser" | awk '($1>0){print $1}' | suggest_pct "$pct")"
  else
    disk_p="$(printf "%s\n" "$disk_ser" | suggest_pct "$pct")"
  fi

  # guardrails
  if [[ "$net_p" -lt 100 ]]; then net_p=100; fi
  if [[ "$disk_p" -lt 100 ]]; then disk_p=100; fi

  echo "${net_p} ${disk_p}"
}

# --suggest-quiet standalone output
if [[ "$SUGGEST_QUIET" -eq 1 ]]; then
  read -r NET_P DISK_P < <(suggest_quiet_thresholds "$SUGGEST_PCT")
  echo "Suggested quiet thresholds from top-candidates over WINDOW=${WINDOW} using p${SUGGEST_PCT} (low tail):"
  echo "  --net-max  ${NET_P}"
  echo "  --disk-max ${DISK_P}"
  echo
  echo "Try:"
  echo "  ./$(script_name) --mode quiet --cpu-min 0 --net-max ${NET_P} --disk-max ${DISK_P} --limit 50"
  exit 0
fi

# --quiet-suggested (quiet mode) => override thresholds for this run unless user explicitly set them
if [[ "$QUIET_SUGGESTED" -eq 1 ]]; then
  if [[ "$MODE" != "quiet" ]]; then
    echo "ERROR: --quiet-suggested is intended for --mode quiet" >&2
    exit 2
  fi
  read -r NET_P DISK_P < <(suggest_quiet_thresholds "$SUGGEST_PCT")
  [[ "$DEBUG" -eq 1 ]] && echo "DEBUG: quiet-suggested thresholds: net-max=${NET_P} disk-max=${DISK_P}" >&2

  if [[ "$NET_MAX_EXPLICIT" -eq 0 ]]; then NET_MAX_BPS="$NET_P"; fi
  if [[ "$DISK_MAX_EXPLICIT" -eq 0 ]]; then DISK_MAX_BPS="$DISK_P"; fi
fi

# Quiet selector description (now AND by default)
QUIET_DESC="net AND disk"
if [[ "$QUIET_NET_ONLY" -eq 1 && "$QUIET_DISK_ONLY" -eq 0 ]]; then QUIET_DESC="net only"; fi
if [[ "$QUIET_NET_ONLY" -eq 0 && "$QUIET_DISK_ONLY" -eq 1 ]]; then QUIET_DESC="disk only"; fi
if [[ "$QUIET_NET_ONLY" -eq 1 && "$QUIET_DISK_ONLY" -eq 1 ]]; then QUIET_DESC="net AND disk"; fi

echo "Monitoring: svc/${PF_SVC} via https://127.0.0.1:${PF_PORT}"
echo "Mode=${MODE} Window=${WINDOW} Top=${TOP_N} Limit=${LIMIT}"
echo "Thresholds: cpu-min=${CPU_MIN_PCT}% net-max=${NET_MAX_BPS} disk-max=${DISK_MAX_BPS} jitter-max=${JITTER_MAX_PCT}% peakavg-max=${PEAKAVG_MAX_PCT}%"
echo "Flags: steady=${STEADY} steady-only=${STEADY_ONLY} require-io-metrics=${REQUIRE_IO_METRICS} quiet-selector=${QUIET_DESC}"
echo

# Header + rows
{
  if [[ "$MODE" == "combined" ]]; then
    echo -e "SCORE\tCAT\tNODE\tNS\tVMI\tVM\tvCPU\tCPU_AVG%\tJIT%\tPKAVG%\tNET_BPS\tDSK_BPS"
  else
    echo -e "SCORE\tS_CPU\tS_NET\tS_DSK\tS_STD\tNODE\tNS\tVMI\tVM\tvCPU\tCPU_AVG%\tCPU_AVG\tCPU_MAX%\tJIT%\tPKAVG%\tIOW\tDLY\tNET_BPS\tDSK_BPS"
  fi

  jq -r \
    --argjson avgCpu "$WANTED_AVG_CPU_MAP" \
    --argjson cpuMax "$CPU_MAX_MAP" \
    --argjson cpuStd "$CPU_STD_MAP" \
    --argjson rx "$RX_MAP" --argjson tx "$TX_MAP" --argjson rd "$RD_MAP" --argjson wr "$WR_MAP" \
    --argjson prx "$PRESENT_RX" --argjson ptx "$PRESENT_TX" --argjson prd "$PRESENT_RD" --argjson pwr "$PRESENT_WR" \
    --argjson iow "$IOW_MAP" --argjson dly "$DLY_MAP" \
    --arg mode "$MODE" \
    --arg cpuMin "$CPU_MIN_PCT" \
    --arg netMax "$NET_MAX_BPS" \
    --arg diskMax "$DISK_MAX_BPS" \
    --arg steady "$STEADY" \
    --arg steadyOnly "$STEADY_ONLY" \
    --arg jitterMax "$JITTER_MAX_PCT" \
    --arg peakavgMax "$PEAKAVG_MAX_PCT" \
    --arg iowMin "$IOWAIT_MIN_CORES" \
    --arg dlyMin "$DELAY_MIN_CORES" \
    --arg requireIo "$REQUIRE_IO_METRICS" \
    --arg qNetOnly "$QUIET_NET_ONLY" \
    --arg qDiskOnly "$QUIET_DISK_ONLY" \
    --arg limit "$LIMIT" \
    --arg scoreMin "${SCORE_MIN:-0}" \
    --arg wNode "$W_NODE" --arg wNs "$W_NS" --arg wVmi "$W_VMI" --arg wVm "$W_VM" '
    def vcpu_count(v):
      ((v.spec.domain.cpu.cores // 1) *
       (v.spec.domain.cpu.sockets // 1) *
       (v.spec.domain.cpu.threads // 1));
    def pct(a;b): if b <= 0 then 0 else (a / b * 100) end;
    def f2(x): (x|tonumber) as $v | ( ($v*100 | round) / 100 );
    def i0(x): (x|tonumber | round);
    def clamp01(x): if x < 0 then 0 elif x > 1 then 1 else x end;

    def trunc(s; n):
      (s // "") as $s
      | (n|tonumber) as $n
      | if ($n<=0) then $s
        elif ($s|length) <= $n then $s
        else ($s[0:($n-1)] + "…")
        end;

    def cpuS(avgPct): 60 * clamp01((avgPct|tonumber)/100);
    def netS(net; thr): 15 * (if (thr|tonumber) <= 0 then 0 else clamp01(((thr|tonumber) - (net|tonumber)) / (thr|tonumber)) end);
    def diskS(disk; thr): 15 * (if (thr|tonumber) <= 0 then 0 else clamp01(((thr|tonumber) - (disk|tonumber)) / (thr|tonumber)) end);
    def steadyS(jitter; peakavg; jitThr; peakThr):
      (5 * (if (jitThr|tonumber) <= 0 then 0 else clamp01(((jitThr|tonumber) - (jitter|tonumber)) / (jitThr|tonumber)) end))
      + (5 * (if (peakThr|tonumber) <= 0 then 0 else clamp01(((peakThr|tonumber) - (peakavg|tonumber)) / (peakThr|tonumber)) end));

    def totalScore(avgPct; net; disk; jitter; peakavg; netThr; diskThr; jitThr; peakThr):
      cpuS(avgPct) + netS(net; netThr) + diskS(disk; diskThr) + steadyS(jitter; peakavg; jitThr; peakThr);

    # Quiet match: AND by default; flags allow only net or only disk
    def quietMatch(net; disk; netThr; diskThr; qNetOnly; qDiskOnly):
      (if (qNetOnly|tonumber)==1 and (qDiskOnly|tonumber)==1 then
          ((net|tonumber) <= (netThr|tonumber)) and ((disk|tonumber) <= (diskThr|tonumber))
       elif (qNetOnly|tonumber)==1 then
          ((net|tonumber) <= (netThr|tonumber))
       elif (qDiskOnly|tonumber)==1 then
          ((disk|tonumber) <= (diskThr|tonumber))
       else
          ((net|tonumber) <= (netThr|tonumber)) and ((disk|tonumber) <= (diskThr|tonumber))
       end);

    # Wedge match: mirrors --mode wedge filters (including steady when enabled)
    def wedgeMatch(avgPct; net; disk; jitter; peakavg; iow; dly;
                   cpuMin; netMax; diskMax; iowMin; dlyMin; steady; jitterMax; peakavgMax):
      ((avgPct|tonumber) >= (cpuMin|tonumber))
      and ((net|tonumber) <= (netMax|tonumber))
      and ((disk|tonumber) <= (diskMax|tonumber))
      and (if (iowMin|tonumber) > 0 then (iow|tonumber) >= (iowMin|tonumber) else true end)
      and (if (dlyMin|tonumber) > 0 then (dly|tonumber) >= (dlyMin|tonumber) else true end)
      and (if (steady|tonumber) == 1 then
            ((jitter|tonumber) <= (jitterMax|tonumber)) and ((peakavg|tonumber) <= (peakavgMax|tonumber))
           else true end);

    def cat(w; q):
      (if w and q then "both" elif w then "wedge" elif q then "quiet" else "none" end);

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

      | ((($prx[$k] // null) != null) or (($ptx[$k] // null) != null)) as $hasNetSeries
      | ((($prd[$k] // null) != null) or (($pwr[$k] // null) != null)) as $hasDiskSeries
      | ($hasNetSeries or $hasDiskSeries) as $hasAnyIo
      | (if ($requireIo|tonumber) == 1 then select($hasAnyIo) else . end)

      | (if $hasNetSeries then (($rx[$k] // 0) + ($tx[$k] // 0)) else 0 end) as $netBps
      | (if $hasDiskSeries then (($rd[$k] // 0) + ($wr[$k] // 0)) else 0 end) as $diskBps

      | (($iow[$k] // 0)|tonumber) as $iowC
      | (($dly[$k] // 0)|tonumber) as $dlyC

      | ( ((.metadata.ownerReferences // []) | map(select(.kind=="VirtualMachine"))[0].name) // "" ) as $vmOwner

      | (cpuS($avgPct)) as $cS
      | (netS($netBps; ($netMax|tonumber))) as $nS
      | (diskS($diskBps; ($diskMax|tonumber))) as $dS
      | (steadyS($jitterPct; $peakavgPct; ($jitterMax|tonumber); ($peakavgMax|tonumber))) as $sS
      | (totalScore($avgPct; $netBps; $diskBps; $jitterPct; $peakavgPct;
                    ($netMax|tonumber); ($diskMax|tonumber); ($jitterMax|tonumber); ($peakavgMax|tonumber))) as $score

      | (quietMatch($netBps; $diskBps; ($netMax|tonumber); ($diskMax|tonumber); $qNetOnly; $qDiskOnly)) as $qOK
      | (wedgeMatch($avgPct; $netBps; $diskBps; $jitterPct; $peakavgPct; $iowC; $dlyC;
                   ($cpuMin|tonumber); ($netMax|tonumber); ($diskMax|tonumber);
                   ($iowMin|tonumber); ($dlyMin|tonumber); ($steady|tonumber);
                   ($jitterMax|tonumber); ($peakavgMax|tonumber))) as $wOK

      | {
          score:$score, cS:$cS, nS:$nS, dS:$dS, sS:$sS,
          wedgeOk:$wOK, quietOk:$qOK,
          node:$node, ns:.metadata.namespace, vmi:.metadata.name, owner:$vmOwner,
          vcpus:$vcpus,
          avgPct:$avgPct, avgC:$avgC, maxPct:$maxPct,
          jitter:$jitterPct, peakavg:$peakavgPct,
          iow:$iowC, dly:$dlyC,
          net:$netBps, disk:$diskBps
        }
    ] as $rows

    | (
        if $mode == "wedge" then
          $rows
          | map(select(.wedgeOk))

        elif $mode == "quiet" then
          $rows
          | map(select(.quietOk))
          | (if ($cpuMin|tonumber) > 0 then map(select(.avgPct >= ($cpuMin|tonumber))) else . end)
          | (if ($iowMin|tonumber) > 0 then map(select(.iow >= ($iowMin|tonumber))) else . end)
          | (if ($dlyMin|tonumber) > 0 then map(select(.dly >= ($dlyMin|tonumber))) else . end)
          | (if ($steady|tonumber) == 1 then
              map(select(.jitter <= ($jitterMax|tonumber)))
              | map(select(.peakavg <= ($peakavgMax|tonumber)))
            else .
            end)

        elif $mode == "combined" then
          $rows
          | map(select((.wedgeOk or .quietOk) and (.score >= ($scoreMin|tonumber))))

        else
          $rows
        end
      )

    | (if ($steadyOnly|tonumber) == 1 then
         map(select(.jitter <= ($jitterMax|tonumber)))
         | map(select(.peakavg <= ($peakavgMax|tonumber)))
       else .
       end)

    | sort_by(-.score)
    | (if ($limit|tonumber) > 0 then .[0:($limit|tonumber)] else . end)
    | .[]
    | (
        if $mode == "combined" then
          [
            (f2(.score)|tostring),
            (cat(.wedgeOk; .quietOk)),
            (trunc(.node; $wNode)),
            (trunc(.ns; $wNs)),
            (trunc(.vmi; $wVmi)),
            (trunc(.owner; $wVm)),
            (.vcpus|tostring),
            (f2(.avgPct)|tostring),
            (f2(.jitter)|tostring),
            (f2(.peakavg)|tostring),
            (i0(.net)|tostring),
            (i0(.disk)|tostring)
          ] | @tsv
        else
          [
            (f2(.score)|tostring),
            (f2(.cS)|tostring),
            (f2(.nS)|tostring),
            (f2(.dS)|tostring),
            (f2(.sS)|tostring),
            (trunc(.node; $wNode)),
            (trunc(.ns; $wNs)),
            (trunc(.vmi; $wVmi)),
            (trunc(.owner; $wVm)),
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
        end
      )
  ' <<<"$VMI_JSON"
} | (command -v column >/dev/null 2>&1 && column -t -s $'\t' || cat)
