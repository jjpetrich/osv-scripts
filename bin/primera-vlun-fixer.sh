#!/usr/bin/env bash
#
# CSI-aware Primera VLUN reconciler
#
# Uses:
#   - HPENodeInfo CRDs to map Kubernetes nodes -> WWPNs
#   - Primera showhost to map WWPNs -> Primera hosts
#   - PV/PVC/Pod relationships to find which PVs are *actually in use*
# Then:
#   - For each (PV, node) pair, ensures a VLUN exists from VV=PV to the
#     Primera host that corresponds to that node.
#
# Usage:
#   ARRAY=primera.byu.edu USER=3paradm ./reconcile_primera_vluns_csi_aware.sh [--dry-run]
#
set -euo pipefail

DRY_RUN=false
NODE_FILTER=""

DRY_RUN=false
NODE_FILTER=""
NODE_CHECK=false

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --node)
      NODE_FILTER="$2"
      shift 2
      ;;
    --node-check)
      NODE_CHECK=true
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: ARRAY=... USER=... $0 [--dry-run] [--node <nodeName>] [--node-check]" >&2
      exit 1
      ;;
  esac
done

: "${ARRAY:?Set ARRAY to Primera array hostname/IP}"
: "${USER:?Set USER to Primera CLI username (e.g. 3paradm)}"

WORKDIR="${TMPDIR:-/tmp}/primera_csi_reconcile"
mkdir -p "${WORKDIR}"

LOG_FILE="${WORKDIR}/reconcile.log"

echo "Working directory: ${WORKDIR}"
echo "Log file: ${LOG_FILE}"
echo

log() {
  echo "[$(date +'%F %T')] $*" | tee -a "${LOG_FILE}"
}

###############################################################################
# 0) Tooling checks and Primera password
###############################################################################
for cmd in oc jq sshpass; do
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: '${cmd}' command not found in PATH." >&2
    exit 1
  fi
done

if ! oc whoami >/dev/null 2>&1; then
  echo "ERROR: Not logged into OpenShift (oc whoami failed). Run 'oc login' and retry." >&2
  exit 1
fi

# Primera password
if [[ -z "${PRIMERA_PASS:-}" ]]; then
  read -s -p "Enter Primera password for ${USER}@${ARRAY}: " PRIMERA_PASS
  echo
fi

ssh_primera() {
  sshpass -p "${PRIMERA_PASS}" ssh -n -o StrictHostKeyChecking=no "${USER}@${ARRAY}" "$@"
}

###############################################################################
# 1) Build node -> WWPN map from HPENodeInfo
###############################################################################
log "Discovering HPENodeInfo CRDs..."

NODE_INFO_JSON="${WORKDIR}/hpenodeinfo.json"
oc get hpenodeinfo.storage.hpe.com -o json > "${NODE_INFO_JSON}" 2>/dev/null || \
  oc get hpenodeinfo -o json > "${NODE_INFO_JSON}"

NODE_WWPN_MAP="${WORKDIR}/node_wwpns.tsv"
jq -r '
  .items[]
  | select(.spec.wwpns != null)
  | {name: .metadata.name, wwpns: .spec.wwpns}
  | .name as $n
  | .wwpns[]
  | "\($n)\t\(. )"
' "${NODE_INFO_JSON}" \
  | awk 'BEGIN{OFS="\t"} { $2=toupper($2); print }' \
  > "${NODE_WWPN_MAP}"

if [[ ! -s "${NODE_WWPN_MAP}" ]]; then
  log "WARN: No HPENodeInfo entries with WWPNs found. Exiting."
  exit 1
fi

log "HPENodeInfo node -> WWPN entries:"
head -n 10 "${NODE_WWPN_MAP}" | tee -a "${LOG_FILE}"
echo

###############################################################################
# 2) Build WWPN -> Primera host map from showhost -d
###############################################################################
log "Discovering Primera hosts and WWPNs (showhost -d)..."

HOST_WWPN_MAP="${WORKDIR}/primera_host_wwpns.tsv"

ssh_primera 'showhost -d' \
  | awk '
      /^Id / {next}          # header
      /^-+$/ {next}          # separator
      NF >= 4 && $1 ~ /^[0-9]+$/ {
        host=$2; wwpn=toupper($4);
        if (wwpn != "N/A") {
          print host "\t" wwpn
        }
      }
    ' \
  | sort -u > "${HOST_WWPN_MAP}"

if [[ ! -s "${HOST_WWPN_MAP}" ]]; then
  log "ERROR: No Primera host WWPNs discovered from showhost -d."
  exit 1
fi

log "Primera host -> WWPN entries (sample):"
head -n 10 "${HOST_WWPN_MAP}" | tee -a "${LOG_FILE}"
echo

###############################################################################
# 3) Join node WWPNs with Primera host WWPNs -> node -> Primera host map
###############################################################################
log "Building node -> Primera host map (via WWPN matching)..."

NODE_HOST_MAP="${WORKDIR}/node_to_primera_host.tsv"
> "${NODE_HOST_MAP}"

# For each node/WWPN, find matching Primera host
while IFS=$'\t' read -r node wwpn; do
  host=$(awk -v w="${wwpn}" '$2 == w {print $1}' "${HOST_WWPN_MAP}" | head -n1 || true)
  if [[ -n "${host}" ]]; then
    echo -e "${node}\t${host}" >> "${NODE_HOST_MAP}"
  fi
done < "${NODE_WWPN_MAP}"

if [[ ! -s "${NODE_HOST_MAP}" ]]; then
  log "ERROR: Could not match any HPENodeInfo WWPNs to Primera hosts."
  exit 1
fi

log "Node -> Primera host map (raw):"
cat "${NODE_HOST_MAP}" | sort -u | tee -a "${LOG_FILE}"
echo

# Unique node -> Primera host mapping
NODE_HOST_UNIQ="${WORKDIR}/node_to_primera_host_uniq.tsv"
sort -u "${NODE_HOST_MAP}" > "${NODE_HOST_UNIQ}"


###############################################################################
# 4) Discover CSI Primera PVs and which nodes are actually using them
###############################################################################
log "Discovering CSI Primera PVs and current pod usage..."

PV_JSON="${WORKDIR}/pvs.json"
oc get pv -o json > "${PV_JSON}"

# Build PV list (Primera + HPE CSI driver)
PV_LIST="${WORKDIR}/primera_pvs.txt"
jq -r '
  .items[]
  | select(.spec.csi.driver=="csi.hpe.com")
  | select(.spec.storageClassName=="primera")
  | .metadata.name
' "${PV_JSON}" | sort -u > "${PV_LIST}"

PV_COUNT=$(wc -l < "${PV_LIST}" || echo 0)
log "Found ${PV_COUNT} Primera CSI PVs."

if [[ "${PV_COUNT}" -eq 0 ]]; then
  log "No Primera CSI PVs found. Exiting."
  exit 0
fi

# PVC -> PV map
PVC_PV_MAP="${WORKDIR}/pvc_to_pv.tsv"
jq -r '
  .items[]
  | select(.spec.csi.driver=="csi.hpe.com")
  | select(.spec.storageClassName=="primera")
  | . as $pv
  | .spec.claimRef.namespace as $ns
  | .spec.claimRef.name as $pvc
  | select($ns != null and $pvc != null)
  | "\($ns)\t\($pvc)\t\($pv.metadata.name)"
' "${PV_JSON}" > "${PVC_PV_MAP}"

log "Sample PVC -> PV mappings:"
head -n 10 "${PVC_PV_MAP}" | tee -a "${LOG_FILE}"
echo

# Pods using these PVCs
POD_JSON="${WORKDIR}/pods.json"
oc get pods -A -o json > "${POD_JSON}"

PV_NODE_MAP_RAW="${WORKDIR}/pv_node_raw.tsv"
> "${PV_NODE_MAP_RAW}"

jq -r '
  .items[]
  | select(.spec.nodeName != null)
  | . as $pod
  | ($pod.spec.volumes // [])[]        # safely iterate over volumes, even if null
  | select(.persistentVolumeClaim != null)
  | "\($pod.metadata.namespace)\t\(.persistentVolumeClaim.claimName)\t\($pod.spec.nodeName)"
' "${POD_JSON}" > "${WORKDIR}/pod_pvc_node.tsv"

# Join (ns, pvc) with PVC -> PV map => PV, node
while IFS=$'\t' read -r ns pvc node; do
  pv=$(awk -v n="${ns}" -v p="${pvc}" '$1==n && $2==p {print $3}' "${PVC_PV_MAP}" | head -n1 || true)
  if [[ -n "${pv}" ]]; then
    echo -e "${pv}\t${node}" >> "${PV_NODE_MAP_RAW}"
  fi
done < "${WORKDIR}/pod_pvc_node.tsv"

if [[ ! -s "${PV_NODE_MAP_RAW}" ]]; then
  log "WARN: No active pods using Primera CSI PVCs were found. Nothing to reconcile."
  exit 0
fi

PV_NODE_MAP="${WORKDIR}/pv_to_node.tsv"
sort -u "${PV_NODE_MAP_RAW}" > "${PV_NODE_MAP}"

log "PV -> node usage (from running pods):"
head -n 20 "${PV_NODE_MAP}" | tee -a "${LOG_FILE}"
echo

###############################################################################
# Node-check mode: summarize each node, even if it has no host mapping
###############################################################################
if [[ "${NODE_CHECK}" == true ]]; then
  log "Node-check mode: generating per-node summary only (no VLUN operations will be performed)."

  # Build list of all nodes that actually use Primera PVs
  ALL_NODES="${WORKDIR}/nodes_from_pv.tsv"
  awk '{print $2}' "${PV_NODE_MAP}" | sort -u > "${ALL_NODES}"

  # For each node that is using at least one PV
  while read -r nodename; do
    [[ -z "${nodename}" ]] && continue

    # All PVs that this node is currently using
    pv_list_for_node=$(awk -v n="${nodename}" '$2==n {print $1}' "${PV_NODE_MAP}" | sort -u)
    pv_count=$(echo "${pv_list_for_node}" | grep -v '^$' | wc -l)

    # Look up Primera host for this node (if any)
    host=$(awk -v n="${nodename}" '$1==n {print $2}' "${NODE_HOST_UNIQ}" | head -n1 || true)

    if [[ -z "${host}" ]]; then
      # Node uses Primera PVs, but we have no HPENodeInfo/WWPN -> host mapping
      log "Node: ${nodename}"
      log "  Primera host: (none — no HPENodeInfo/WWPN match)"
      log "  PVs in use: ${pv_count}"
      log "  PVs missing VLUNs on this node: (unknown; cannot check without host mapping)"
      echo
      continue
    fi

    # We have a Primera host mapping; check VLUNs for each PV on this node
    missing_count=0

    for pv in ${pv_list_for_node}; do
      vv="${pv:0:31}"
      HOSTS_FOR_VV="${WORKDIR}/hosts_for_${pv}.txt"

      # Get host list for this VV (once per PV)
      ssh_primera "showvlun -v ${vv}" \
        | awk '
            /^Active VLUNs/ {section="active"; next}
            /^VLUN Templates/ {section="tmpl"; next}
            /^-{3,}/ {next}
            /^$/ {section=""; next}
            section != "" && $1 ~ /^[0-9]+$/ {print $3}
          ' \
        | sort -u > "${HOSTS_FOR_VV}" || true

      if ! grep -qx "${host}" "${HOSTS_FOR_VV}" 2>/dev/null; then
        missing_count=$((missing_count+1))
      fi
    done

    log "Node: ${nodename}"
    log "  Primera host: ${host}"
    log "  PVs in use: ${pv_count}"
    log "  PVs missing VLUNs on this node: ${missing_count}"
    echo

  done < "${ALL_NODES}"

  log "Node-check summary complete."
  exit 0
fi



# If a specific node filter is set, restrict PV->node map to that node only
if [[ -n "${NODE_FILTER}" ]]; then
  log "Node filter enabled: only reconciling VLUNs for node '${NODE_FILTER}'"

  # Support both shortname and FQDN: if user gave "radium02", treat that as suffix match
  if grep -q "${NODE_FILTER}" "${PV_NODE_MAP}"; then
    # Exact or substring match; keep only lines that contain the node string
    awk -v n="${NODE_FILTER}" '$2==n' "${PV_NODE_MAP}" > "${PV_NODE_MAP}.tmp" || true
  else
    # Try with domain appended (common case)
    awk -v n="${NODE_FILTER}.byu.edu" '$2==n' "${PV_NODE_MAP}" > "${PV_NODE_MAP}.tmp" || true
  fi

  mv "${PV_NODE_MAP}.tmp" "${PV_NODE_MAP}" || true

  if [[ ! -s "${PV_NODE_MAP}" ]]; then
    log "WARN: After applying node filter '${NODE_FILTER}', no PV usage entries remain. Nothing to do."
    exit 0
  fi

  log "PV -> node usage for filtered node '${NODE_FILTER}':"
  head -n 20 "${PV_NODE_MAP}" | tee -a "${LOG_FILE}"
  echo
fi


###############################################################################
# 5) Reconcile VLUNs: for each (PV, node) ensure VLUN VV=PV to Primera host
###############################################################################
log "Starting VLUN reconciliation based on CSI usage..."

PVS_TOUCHED=0
VLUN_CREATED=0

# Build helper: node -> primeraHost (unique)
NODE_HOST_UNIQ="${WORKDIR}/node_to_primera_host_uniq.tsv"
sort -u "${NODE_HOST_MAP}" > "${NODE_HOST_UNIQ}"

# For efficiency, we will process per-PV and cache the showvlun result
while read -r pv; do
  # Get all nodes that currently use this PV
  nodes_for_pv=$(awk -v v="${pv}" '$1==v {print $2}' "${PV_NODE_MAP}" | sort -u)
  if [[ -z "${nodes_for_pv}" ]]; then
    continue
  fi

  log "Processing PV (VV): ${pv}"

  # Get existing hosts that see this VV from showvlun
  vv="${pv:0:31}"
HOSTS_FOR_VV="${WORKDIR}/hosts_for_${pv}.txt"
  ssh_primera "showvlun -v ${vv}" \
    | awk '
        /^Active VLUNs/ {section="active"; next}
        /^VLUN Templates/ {section="tmpl"; next}
        /^-{3,}/ {next}
        /^$/ {section=""; next}
        section != "" && $1 ~ /^[0-9]+$/ {print $3}
      ' | sort -u > "${HOSTS_FOR_VV}" || true

  PV_CHANGED=false

  for node in ${nodes_for_pv}; do
    # Find Primera host for this node
    host=$(awk -v n="${node}" '$1==n {print $2}' "${NODE_HOST_UNIQ}" | head -n1 || true)
    if [[ -z "${host}" ]]; then
      log "  WARN: No Primera host mapping found for node '${node}' (skipping)."
      continue
    fi

    if grep -qx "${host}" "${HOSTS_FOR_VV}" 2>/dev/null; then
      log "  Host ${host} already has VLUN for ${pv} (node=${node}) - OK"
      continue
    fi

    PV_CHANGED=true
    VLUN_CREATED=$((VLUN_CREATED+1))

    if "${DRY_RUN}"; then
      log "  DRY-RUN: would create VLUN for ${pv} on host ${host} (node=${node}) LUN=auto"
    else
      log "  Creating VLUN for ${pv} on host ${host} (node=${node}) LUN=auto..."
      vv="${pv:0:31}"
      if ssh_primera "createvlun -f ${vv} auto ${host}"; then
        log "  SUCCESS: VLUN created for ${pv} on ${host} (node=${node})"
      else
        log "  ERROR: Failed to create VLUN for ${pv} on ${host} (node=${node})"
      fi
    fi
  done

  if [[ "${PV_CHANGED}" == "true" ]]; then
    PVS_TOUCHED=$((PVS_TOUCHED+1))
  fi

  echo
done < "${PV_LIST}"

log "VLUN reconciliation based on CSI usage complete."
log "PVs modified (had at least one missing host mapping): ${PVS_TOUCHED}/${PV_COUNT}"
log "VLUNs created (or would be, in dry-run): ${VLUN_CREATED}"
if "${DRY_RUN}"; then
  log "NOTE: DRY-RUN mode was enabled, no actual VLUNs were created."
fi

echo
log "Detailed logs: ${LOG_FILE}"
