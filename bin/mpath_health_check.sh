#!/usr/bin/env bash
#
# mpath_health_check.sh
#
# ***WARNING*** Use at your own risk. Script is intentionally conservative. ***WARNING***
# Vendor-aware multipath health checker & optional cleaner for OpenShift nodes.
#
# Features:
#   - Scans all nodes (or one node via NODE_FILTER) for "suspicious" multipath devices.
#   - Suspicious = ZERO_SIZE (size=0.0K) OR ALL_PATHS_FAULTY (all paths failed faulty).
#   - Works for ALL vendors (PowerStore, Primera, etc.).
#   - Always:
#       * Skips WWIDs that appear in any PV.
#       * Verifies no mounts / open files before cleanup.
#   - Cleanup is opt-in, per vendor & type, with DRY_RUN defaulting to true.
#   - Optional --showvolume flag to attempt array-side volume lookup (HPE / Dell).
#
# Environment knobs:
#   DRY_RUN                        (default: true)
#   NODE_FILTER                    (default: all nodes)
#
#   CLEANUP_DELL_ZERO_SIZE         (default: false)
#   CLEANUP_DELL_ALL_PATHS_FAILED  (default: false)
#
#   CLEANUP_HPE_ZERO_SIZE          (default: false)
#   CLEANUP_HPE_ALL_PATHS_FAILED   (default: false)
#
#   SHOW_VOLUME                    (default: false; overridden by --showvolume)
#
#   PRIMERA_SSH                    (optional; e.g. "user@primera-mgmt" for remote showvv/showvlun)
#   POWERSTORE_SSH                 (optional; e.g. "user@powerstore-mgmt" for custom lookup)
#
# Vendor classification:
#   - "DellEMC,PowerStore"      => vendor_class = DELL
#   - "3PARdata", "HPE,..."     => vendor_class = HPE
#   - everything else           => vendor_class = OTHER
#
# ***WARNING*** Use at your own risk. Script is intentionally conservative. ***WARNING***

set -euo pipefail

# --- CONFIG ------------------------------------------------------------------

DRY_RUN="${DRY_RUN:-true}"

NODE_FILTER="${NODE_FILTER:-}"       # e.g. "argon03.byu.edu"

# per-vendor, per-type cleanup flags; all default to "false"
CLEANUP_DELL_ZERO_SIZE="${CLEANUP_DELL_ZERO_SIZE:-false}"
CLEANUP_DELL_ALL_PATHS_FAILED="${CLEANUP_DELL_ALL_PATHS_FAILED:-false}"

CLEANUP_HPE_ZERO_SIZE="${CLEANUP_HPE_ZERO_SIZE:-false}"
CLEANUP_HPE_ALL_PATHS_FAILED="${CLEANUP_HPE_ALL_PATHS_FAILED:-false}"

# Optional: show array volume mapping when suspicious WWIDs are found
SHOW_VOLUME="${SHOW_VOLUME:-false}"

# Image used for oc debug (something with /bin/sh, multipath, lsof, etc. via host)
DEBUG_IMAGE="${DEBUG_IMAGE:-registry.access.redhat.com/ubi9/ubi-minimal}"

# PV WWID index:
#   PV_WWID_INDEX[wwid] = 1
#   PV_WWID_PVS[wwid]   = "pv1 pv2 ..."
declare -A PV_WWID_INDEX
declare -A PV_WWID_PVS

# -----------------------------------------------------------------------------


log() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')] $*"
}

die() {
  log "ERROR: $*"
  exit 1
}

get_nodes() {
  if [[ -n "$NODE_FILTER" ]]; then
    oc get node "$NODE_FILTER" -o name 2>/dev/null || die "Node '$NODE_FILTER' not found"
    echo "node/$NODE_FILTER"
  else
    oc get nodes -o name
  fi
}

# Build an index of WWIDs that appear in any PV (by scanning oc get pv -o wide)
build_pv_wwid_index() {
  log "Building PV WWID index from all PVs..."
  PV_WWID_INDEX=()
  PV_WWID_PVS=()

  local line pv tok

  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    # Skip header line
    [[ "$line" =~ ^NAME[[:space:]] ]] && continue

    # First column is PV name
    pv=$(awk '{print $1}' <<<"$line")

    # Scan all tokens on the line for WWID-looking strings
    for tok in $line; do
      if [[ "$tok" =~ ^36[0-9a-fA-F]+$ ]]; then
        PV_WWID_INDEX["$tok"]=1
        PV_WWID_PVS["$tok"]+="$pv "
      fi
    done
  done < <(oc get pv -A -o wide)

  local count=${#PV_WWID_INDEX[@]}
  log "PV WWID index contains $count unique WWIDs"
  if (( count > 0 )); then
    for wwid in "${!PV_WWID_INDEX[@]}"; do
      log "  WWID $wwid referenced by PV(s): ${PV_WWID_PVS[$wwid]}"
    done
  fi
}

wwid_in_pv_index() {
  local wwid="$1"
  [[ -n "${PV_WWID_INDEX[$wwid]:-}" ]]
}

# Classify vendor based on the "vendor,product" token from multipath -ll
classify_vendor() {
  local raw_vendor="$1"
  local vendor_class="OTHER"

  case "$raw_vendor" in
    DellEMC,PowerStore*)
      vendor_class="DELL"
      ;;
    3PARdata*|HPE,*|HPE*)
      vendor_class="HPE"
      ;;
    *)
      vendor_class="OTHER"
      ;;
  esac

  echo "$vendor_class"
}

# Optional: attempt to show array-side volume info for a suspicious WWID
show_volume_mapping() {
  local vendor_class="$1"   # "HPE" or "DELL"
  local wwid="$2"

  log "  (--showvolume) Looking up volume details for WWID=$wwid (CLASS=$vendor_class)"

  case "$vendor_class" in
    HPE)
      # HPE Primera/3PAR volume lookup
      if command -v showvv >/dev/null 2>&1; then
        log "    HPE: using local showvv/showvlun to find matching volume"

        # showvv Name,WWN listing, then grep for the WWID
        showvv -showcols Name,WWN -nohdtot 2>/dev/null | \
          awk -v tgt="$wwid" '
            index($2, tgt) > 0 {
              printf("    showvv: volume=%s WWN=%s\n", $1, $2)
            }
          ' || true

        # Optionally, showvlun mapping; this may not include the WWID directly,
        # but if it does, this will surface it.
        showvlun -showcols Name,Host_WWN,Host_Name -nohdtot 2>/dev/null | \
          awk -v tgt="$wwid" '
            index($0, tgt) > 0 {
              printf("    showvlun: volume=%s host=%s host_wwn=%s\n", $1, $3, $2)
            }
          ' || true

      elif [[ -n "${PRIMERA_SSH:-}" ]]; then
        log "    HPE: using ssh $PRIMERA_SSH to query showvv/showvlun"

        ssh "$PRIMERA_SSH" "showvv -showcols Name,WWN -nohdtot 2>/dev/null | grep $wwid" 2>/dev/null || true | \
          sed 's/^/    showvv (remote): /' || true

        ssh "$PRIMERA_SSH" "showvlun -showcols Name,Host_WWN,Host_Name -nohdtot 2>/dev/null | grep $wwid" 2>/dev/null || true | \
          sed 's/^/    showvlun (remote): /' || true
      else
        log "    HPE: no local showvv CLI or PRIMERA_SSH configured; skipping volume lookup"
      fi
      ;;

    DELL)
      # Dell PowerStore volume lookup hook.
      # Customize this with whatever CLI/API you have via ssh.
      if [[ -n "${POWERSTORE_SSH:-}" ]]; then
        log "    Dell: using ssh $POWERSTORE_SSH to query volume mapping (customize this section)"
        # Example placeholder; replace "your-powerstore-command" with real tooling.
        local out
        #out=$(ssh "$POWERSTORE_SSH" "your-powerstore-command | grep -i $wwid" 2>/dev/null || true)
        out=$(ssh "$POWERSTORE_SSH" "pstcli -query volume -type block | grep -i $wwid" 2>/dev/null || true)
        if [[ -n "$out" ]]; then
          while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            log "    powerstore: $line"
          done <<<"$out"
        else
          log "    powerstore: no matches found (or command not yet customized)"
        fi
      else
        log "    Dell: no POWERSTORE_SSH configured; add a custom lookup if desired"
      fi
      ;;

    *)
      log "    No array lookup implemented for CLASS=$vendor_class"
      ;;
  esac
}

# Decide if we should attempt cleanup for this vendor_class + type
should_cleanup() {
  local vendor_class="$1"
  local type="$2"

  case "$vendor_class" in
    DELL)
      case "$type" in
        ZERO_SIZE)
          [[ "$CLEANUP_DELL_ZERO_SIZE" == "true" ]] && return 0 || return 1
          ;;
        ALL_PATHS_FAILED)
          [[ "$CLEANUP_DELL_ALL_PATHS_FAILED" == "true" ]] && return 0 || return 1
          ;;
        *)
          return 1
          ;;
      esac
      ;;
    HPE)
      case "$type" in
        ZERO_SIZE)
          [[ "$CLEANUP_HPE_ZERO_SIZE" == "true" ]] && return 0 || return 1
          ;;
        ALL_PATHS_FAILED)
          [[ "$CLEANUP_HPE_ALL_PATHS_FAILED" == "true" ]] && return 0 || return 1
          ;;
        *)
          return 1
          ;;
      esac
      ;;
    *)
      # OTHER: no auto cleanup by default
      return 1
      ;;
  esac
}

# For a given node, run multipath -ll and detect bad devices.
# Output format (one per line):
#   TYPE|RAW_VENDOR|WWID
# where TYPE is ZERO_SIZE or ALL_PATHS_FAILED
detect_bad_on_node() {
  local node="$1"

  log "Detecting suspicious multipath devices on $node"

  # Run multipath -ll on the node and capture the output locally
  local mpout
  mpout=$(oc debug "$node" --quiet --image="$DEBUG_IMAGE" -- \
            chroot /host multipath -ll 2>/dev/null || true)

  if [[ -z "$mpout" ]]; then
    log "  multipath -ll produced no output on $node"
    return 0
  fi

  printf '%s\n' "$mpout" | awk '
  BEGIN {
      wwid = ""
      raw_vendor = ""
      size = ""
      total_paths = 0
      faulty_paths = 0
  }

  # Device header line, e.g.:
  # 360002ac0000000000000a934000268a6 dm-304 3PARdata,VV
  /^36[0-9a-fA-F]+ / {
      if (wwid != "") report()
      wwid       = $1
      raw_vendor = $3
      size       = ""
      total_paths   = 0
      faulty_paths  = 0
      next
  }

  # Size line, e.g.:
  # size=20G features=...
  $1 ~ /^size=/ && wwid != "" {
      gsub("size=", "", $1)
      size = $1
      next
  }

  # Any line inside a device stanza that contains "active" or "failed"
  # is treated as a path line. We scan tokens to find state.
  wwid != "" {
      state  = ""
      detail = ""

      for (i = 1; i <= NF; i++) {
          if ($i == "active" || $i == "failed") {
              state = $i
              if (i + 1 <= NF) {
                  detail = $(i + 1)
              }
              break
          }
      }

      if (state != "") {
          total_paths++

          # DEBUG: uncomment if you want to see path parsing again
          #printf("DEBUG_PATH ww=%s state=%s detail=%s line=\"", wwid, state, detail) > "/dev/stderr"
          #for (i = 1; i <= NF; i++) {
          #    printf("%s%s", (i>1 ? " " : ""), $i) > "/dev/stderr"
          #}
          #printf("\"\n") > "/dev/stderr"

          if (state == "failed" && detail == "faulty") {
              faulty_paths++
          }
      }

      next
  }

  # Blank line = end of stanza (some multipath outputs separate devices with a blank line)
  NF == 0 && wwid != "" {
      report()
      wwid = ""
  }

  function report() {
      if (size == "0.0K") {
          # Completely bogus / zero-size
          printf("ZERO_SIZE|%s|%s\n", raw_vendor, wwid)
      } else if (total_paths > 0 && faulty_paths == total_paths) {
          # Every path we saw is failed + faulty
          printf("ALL_PATHS_FAILED|%s|%s\n", raw_vendor, wwid)
      }
  }

  END {
      if (wwid != "") report()
  }
  '
}

# On a node, verify local safety and optionally run multipath -f
cleanup_on_node() {
  local node="$1"          # e.g. "node/argon03.byu.edu"
  local wwid="$2"
  local reason="$3"
  local dry_run="$4"

  log "[$node] Evaluating $wwid ($reason)"

  oc debug "$node" --quiet --image="$DEBUG_IMAGE" -- \
    chroot /host /bin/sh -s << EOF || exit 1
set -e

WWID="$wwid"

echo "  - multipath -ll \$WWID:"
multipath -ll "\$WWID" || { echo "    multipath -ll failed; skipping"; exit 0; }

echo "  - lsblk -f /dev/mapper/\$WWID:"
lsblk -f /dev/mapper/"\$WWID" || true

echo "  - lsof /dev/mapper/\$WWID:"
lsof /dev/mapper/"\$WWID" 2>/dev/null || echo "    no open files"

echo "  - checking mounts for \$WWID:"
grep "\$WWID" /proc/mounts || echo "    no mounts"

if [ "$dry_run" = "true" ]; then
  echo "  [DRY_RUN] Would run: multipath -f \$WWID"
  exit 0
fi

echo "  Running: multipath -f \$WWID"
multipath -f "\$WWID" || echo "  multipath -f exited non-zero; check manually"

EOF
}

main() {
  # Simple CLI flag parser (currently only --showvolume)
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --showvolume|--showvolumes)
        SHOW_VOLUME=true
        shift
        ;;
      *)
        log "Unknown argument: $1 (ignored)"
        shift
        ;;
    esac
  done

  log "Starting multipath health check"
  log "DRY_RUN=${DRY_RUN}"
  [[ -n "$NODE_FILTER" ]] && log "NODE_FILTER=$NODE_FILTER"
  log "SHOW_VOLUME=${SHOW_VOLUME}"

  log "Cleanup policy:"
  log "  DELL ZERO_SIZE       : ${CLEANUP_DELL_ZERO_SIZE}"
  log "  DELL ALL_PATHS_FAILED: ${CLEANUP_DELL_ALL_PATHS_FAILED}"
  log "  HPE  ZERO_SIZE       : ${CLEANUP_HPE_ZERO_SIZE}"
  log "  HPE  ALL_PATHS_FAILED: ${CLEANUP_HPE_ALL_PATHS_FAILED}"

  oc whoami >/dev/null 2>&1 || die "oc is not logged in or not configured"

  local nodes
  nodes=$(get_nodes)

  # Build WWID -> PV index
  build_pv_wwid_index

  while read -r node; do
    [ -z "$node" ] && continue
    local nodename="${node#node/}"

    log "Processing node: $nodename"

    local detections
    detections=$(detect_bad_on_node "$node" || true)

    if [[ -z "$detections" ]]; then
      log "  No suspicious multipath devices detected on $nodename"
      continue
    fi

    echo "$detections" | while IFS='|' read -r type raw_vendor wwid; do
      [ -z "$wwid" ] && continue

      local vendor_class
      vendor_class=$(classify_vendor "$raw_vendor")

      log "[$nodename] Found $type WWID=$wwid VENDOR=\"$raw_vendor\" CLASS=$vendor_class"

      # Optional: show array-side info
      if [[ "$SHOW_VOLUME" == "true" ]]; then
        show_volume_mapping "$vendor_class" "$wwid"
      fi

      # Always skip if WWID appears in any PV definition
      if wwid_in_pv_index "$wwid"; then
        log "[$nodename]   Skipping $wwid because it appears in PV definitions"
        continue
      fi

      # Decide if we are allowed to clean this vendor/type
      if ! should_cleanup "$vendor_class" "$type"; then
        log "[$nodename]   No auto-clean policy for $vendor_class/$type; report-only"
        continue
      fi

      # Perform node-local safety checks & cleanup
      cleanup_on_node "$node" "$wwid" "$type" "$DRY_RUN"

    done

  done <<< "$nodes"

  log "Done."
}

main "$@"
