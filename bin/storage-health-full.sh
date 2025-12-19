#!/usr/bin/env bash
#
# storage-health-full.sh
#
# Manager-side storage health runner (full, updated)
# - Runs checks on all worker nodes via `oc debug node/... -- chroot /host ...`
# - Writes per-node plaintext reports into ./storage_reports/
# - Generates interactive HTML (accordion + search + sortable table) with embedded details
# - Generates CSV summary and tarball archive for the run
# - Copies latest.html (not a symlink) for robustness (WSL/Windows)
# - Removes per-run plaintext files after archiving (unless --keep-plaintext)
# - Optional --strict to escalate vendor/policy mismatches to WARN
# - Configurable retention via --retention-days
#
set -uo pipefail

# --------------------
# Defaults & CLI args
# --------------------
OUT_DIR="$(pwd)/storage_reports"
mkdir -p "${OUT_DIR}"

TS="$(date +%Y%m%d-%H%M%S)"
RETENTION_DAYS=30
KEEP_PLAINTEXT=0
STRICT=0

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --keep-plaintext         Do not remove per-run plaintext files after archiving.
  --strict                 Treat vendor/policy mismatches as real issues (escalate to WARN).
  --retention-days N       Prune run artifacts older than N days (default: ${RETENTION_DAYS}).
  --help                   Show this help.
EOF
  exit 1
}

# parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-plaintext) KEEP_PLAINTEXT=1; shift ;;
    --strict) STRICT=1; shift ;;
    --retention-days)
      shift
      if [[ $# -eq 0 ]]; then echo "Missing arg for --retention-days"; usage; fi
      RETENTION_DAYS="$1"; shift
      ;;
    --help) usage ;;
    *) echo "Unknown arg: $1"; usage ;;
  esac
done

TAR_PATH="${OUT_DIR}/storage_health_${TS}.tar.gz"
HTML_PATH="${OUT_DIR}/storage_health_${TS}.html"
CSV_PATH="${OUT_DIR}/storage_health_${TS}.csv"

# --------------------
# Helpers
# --------------------
run_on_node() {
  local node="$1"; shift
  local cmd="$*"
  # run a compact command on the host via oc debug
  oc debug "node/${node}" -- chroot /host /bin/bash -c "${cmd}"
}

# analyze multipath - ll (local)
# Writes a plaintext report to $report, a small HTML block to $html_block,
# and echoes a CSV line: node,total_luns,total_ok,total_issues,total_bad_paths,status
analyze_multipath() {
  local node="$1"
  local mpfile="$2"
  local report="$3"
  local html_block="$4"

  local total_luns=0 total_ok=0 total_issues=0 total_bad_paths=0

  {
    echo "Storage health report for node: ${node}"
    echo "Generated at: $(date)"
    echo "=================================================="
    echo

    if [[ ! -s "${mpfile}" ]]; then
      echo "multipath -ll: NO DATA (file '${mpfile}' is empty or missing)"
      echo "Skipping LUN analysis."
      echo "OVERALL STATUS : UNKNOWN (no multipath output)"
      echo
    else
      # split into LUN blocks
      awk -v RS= -v ORS="\n---\n" 'NF' "${mpfile}" > "${mpfile}.blocks"

      while IFS= read -r block; do
        [[ -z "${block}" ]] && continue
        # identify LUN header lines by WWID+dm-x presence
        if ! grep -qE '^[0-9a-f]{32,} dm-[0-9]+' <<< "${block}"; then
          continue
        fi

        # count LUN once per block
        ((total_luns++))

        header="$(printf '%s\n' "${block}" | head -n1)"
        wwid="$(awk '{print $1}' <<< "${header}")"
        dm="$(awk '{print $2}' <<< "${header}")"
        vendorprod="$(awk '{print $3}' <<< "${header}" || true)"
        vendor="${vendorprod%%,*}"
        product="${vendorprod#*,}"

        size_line="$(printf '%s\n' "${block}" | sed -n '2p')"
        size="$(awk '{for(i=1;i<=NF;i++) if($i ~ /^size=/) {sub("size=","",$i); print $i}}' <<< "${size_line}" || true)"

        local array_type="Other"
        if [[ "${vendor}" == "DellEMC" && "${product}" == "PowerStore" ]]; then array_type="PowerStore"; fi
        if [[ "${vendor}" == "3PARdata" && "${product}" == "VV" ]]; then array_type="3PAR"; fi

        echo "LUN: ${wwid} (${dm})"
        echo "  Vendor/Product: ${vendor}/${product}"
        echo "  Size: ${size}"
        echo "  Type: ${array_type}"

        local lun_bad_paths=0
        local lun_has_issue=0

        # capture path-group header lines (may not always exist in a consistent format)
        pg_lines="$(printf '%s\n' "${block}" | grep -E "policy=" || true)"

        if [[ -z "${pg_lines}" ]]; then
          # Parser couldn't find explicit groups — but we should still search for path lines
          echo "  NOTE: parser could not find path-group headers for this LUN."
          # We'll still scan the entire block for path lines and determine bad paths
          following_lines="$(printf '%s\n' "${block}")"
        else
          following_lines="$(printf '%s\n' "${block}")"
        fi

        # scan for path lines in the block and count bad paths
        while IFS= read -r pline; do
          [[ -z "${pline}" ]] && continue
          if ! grep -qE '[0-9]+:[0-9]+:[0-9]+:[0-9]+' <<< "${pline}"; then
            continue
          fi
          addr="$(awk '{print $1}' <<< "${pline}")"
          dev="$(awk '{print $2}' <<< "${pline}")"
          st1="$(awk '{print $4}' <<< "${pline}")"
          st2="$(awk '{print $5}' <<< "${pline}")"
          st3="$(awk '{print $6}' <<< "${pline}")"
          if [[ "${st1}" != "active" || "${st2}" != "ready" || "${st3}" != "running" ]]; then
            ((lun_bad_paths++))
            echo "    ${addr} ${dev} ${st1} ${st2} ${st3}  <<< ISSUE"
          else
            echo "    ${addr} ${dev} ${st1} ${st2} ${st3}"
          fi
        done <<< "${following_lines}"

        # Check vendor/policy heuristics *only* as notes by default; in strict mode they escalate
        if [[ -n "${pg_lines}" ]]; then
          while IFS= read -r pg; do
            [[ -z "${pg}" ]] && continue
            policy="$(awk -F"policy=" '{print $2}' <<< "${pg}" | sed -E "s/^'?(.*)'?$/\1/; s/ .*$//")"
            prio="$(awk '{for(i=1;i<=NF;i++) if($i ~ /^prio=/) {sub("prio=","",$i); print $i}}' <<< "${pg}" || true)"
            echo "  Path group note: policy=${policy} prio=${prio}"
            if [[ "${STRICT}" -eq 1 ]]; then
              # Escalate vendor/policy mismatches to an "issue"
              if [[ "${array_type}" == "PowerStore" ]]; then
                if [[ "${policy}" != "queue-length 0" || ( "${prio}" != "50" && "${prio}" != "10" ) ]]; then
                  lun_has_issue=1
                fi
              elif [[ "${array_type}" == "3PAR" ]]; then
                if [[ "${policy}" != "round-robin 0" ]]; then lun_has_issue=1; fi
              fi
            fi
          done <<< "${pg_lines}"
        fi

        # final LUN classification
        if [[ "${lun_bad_paths}" -gt 0 ]] || [[ "${lun_has_issue}" -eq 1 && "${STRICT}" -eq 1 ]]; then
          echo "  LUN SUMMARY: *** ISSUES DETECTED (bad_paths=${lun_bad_paths}) ***"
          ((total_issues++))
          ((total_bad_paths+=lun_bad_paths))
        else
          echo "  LUN SUMMARY: OK"
          ((total_ok++))
        fi
        echo

      done < "${mpfile}.blocks"
    fi

    echo "Per-node summary for ${node}:"
    echo "  Total LUNs scanned  : ${total_luns}"
    echo "  OK LUNs             : ${total_ok}"
    echo "  LUNs w/ issues      : ${total_issues}"
    echo "  Total bad paths     : ${total_bad_paths}"

    local status_out="UNKNOWN"
    if [[ "${total_luns}" -eq 0 ]]; then status_out="UNKNOWN"
    elif [[ "${total_bad_paths}" -gt 0 || ( "${STRICT}" -eq 1 && "${total_issues}" -gt 0 ) ]]; then status_out="WARN"
    else status_out="HEALTHY"; fi
    echo "  OVERALL STATUS      : ${status_out}"
    echo

  } > "${report}"

  # Build HTML block (accordion) for embedding
  {
    echo "<div class=\"node-block node-${status_out,,}\">"
    if [[ "${status_out}" == "WARN" ]]; then
      echo "<button class=\"accordion\" aria-expanded=\"true\">Node: ${node} &nbsp; <span class=\"status ${status_out,,}\">${status_out}</span></button>"
      echo "<div class=\"panel\" style=\"display:block\">"
    else
      echo "<button class=\"accordion\" aria-expanded=\"false\">Node: ${node} &nbsp; <span class=\"status ${status_out,,}\">${status_out}</span></button>"
      echo "<div class=\"panel\">"
    fi
    echo "<pre>"
    sed 's/&/\&amp;/g; s/</\&lt;/g' "${report}"
    echo "</pre>"
    echo "</div>"
    echo "</div>"
  } > "${html_block}"

  # emit CSV line
  echo "${node},${total_luns},${total_ok},${total_issues},${total_bad_paths},${status_out}"
}

# --------------------
# Main
# --------------------
NODES=$(oc get nodes -l node-role.kubernetes.io/worker -o name | cut -d/ -f2)

echo "Running storage health checks on worker nodes:"
echo "${NODES}"
echo

# CSV header
echo "node,total_luns,total_ok,total_issues,total_bad_paths,status" > "${CSV_PATH}"

# tmp accumulator for HTML blocks
HTML_BLOCKS_TMP="$(mktemp)"
> "${HTML_BLOCKS_TMP}"
declare -a SUMMARY_LINES=()

for node in ${NODES}; do
  echo "=== ${node} ==="

  REPORT_BASE="${OUT_DIR}/storage_health_${node}_${TS}"
  REPORT_FINAL="${REPORT_BASE}.txt"
  MPFILE="${OUT_DIR}/multipath_ll_${node}_${TS}.txt"
  HTML_BLOCK="${REPORT_BASE}.htmlblk"

  {
    echo "Node: ${node}"
    echo
    echo "1) OS info (/etc/os-release)"
    run_on_node "${node}" "cat /etc/os-release || echo 'no /etc/os-release'" || echo "  (os-release read failed)"
    echo
    echo "2) multipathd status"
    run_on_node "${node}" "systemctl is-active multipathd || echo 'systemctl failed'" || echo "  (systemctl check failed)"
    echo
    echo "3) Capturing multipath -ll"
    if run_on_node "${node}" "multipath -ll" > "${MPFILE}" 2>/dev/null; then
      echo "   Saved raw multipath output to ${MPFILE}"
    else
      echo "   multipath -ll failed (see ${MPFILE} if anything was written)"
    fi
    echo
  } > "${REPORT_FINAL}.tmp" || true

  csv_line="$(analyze_multipath "${node}" "${MPFILE}" "${REPORT_FINAL}.mp" "${HTML_BLOCK}")" || csv_line="${node},0,0,0,0,UNKNOWN"

  # combine plaintext report
  cat "${REPORT_FINAL}.tmp" "${REPORT_FINAL}.mp" > "${REPORT_FINAL}" || true
  rm -f "${REPORT_FINAL}.tmp" "${REPORT_FINAL}.mp" "${MPFILE}.blocks" 2>/dev/null || true

  # append HTML block
  cat "${HTML_BLOCK}" >> "${HTML_BLOCKS_TMP}" || true
  rm -f "${HTML_BLOCK}" 2>/dev/null || true

  # append CSV
  echo "${csv_line}" >> "${CSV_PATH}"

  SUMMARY_LINES+=("${csv_line}")

  node_name="$(awk -F, '{print $1}' <<< "${csv_line}")"
  status="$(awk -F, '{print $6}' <<< "${csv_line}")"
  printf "  %s : %s\n" "${node_name}" "${status}"
done

# --------------------
# Build HTML
# --------------------
HTML_TMP="$(mktemp)"
{
cat <<'HTML_HEAD'
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Storage Health Report</title>
<style>
body { font-family: Arial, Helvetica, sans-serif; margin: 18px; color:#222; }
.header { display:flex; align-items:center; justify-content:space-between; gap:12px; }
.search { margin: 12px 0; }
table.summary { border-collapse: collapse; width:100%; margin-bottom: 18px; }
th, td { border: 1px solid #ddd; padding: 8px; text-align:left; vertical-align: top; }
th { background:#0b5f9e; color: #fff; cursor:pointer; user-select:none; }
tr.ok { background: #e9f7ef; }
tr.warn { background: #fff4e5; }
tr.unknown { background: #f3f3f3; }
.status.healthy { color: #0a7d3a; font-weight: bold; }
.status.warn { color: #a36100; font-weight: bold; }
.status.unknown { color: #666; font-weight: bold; }

/* Accordion styles */
.accordion { background:#fff; color:#222; cursor:pointer; padding:12px 14px; width:100%; text-align:left; border:1px solid #ddd; border-radius:6px; outline:none; font-size:14px; display:flex; justify-content:space-between; align-items:center; }
.accordion:after { content: ''; width: 0; height: 0; border-left: 6px solid transparent; border-right: 6px solid transparent; border-top: 8px solid #444; display:inline-block; margin-left:8px; transition: transform 0.15s ease-in-out; transform-origin: center; }
.accordion[aria-expanded="true"]:after { transform: rotate(180deg); }
.panel { padding: 8px 12px; display:none; border-left:1px solid #eee; border-right:1px solid #eee; border-bottom:1px solid #eee; margin-bottom:12px; background:#fafafa; border-radius:0 0 6px 6px; }
.node-block { margin-bottom:8px; }
.node-healthy .accordion { border-left: 6px solid rgba(10,125,58,0.12); }
.node-warn .accordion { border-left: 6px solid rgba(163,97,0,0.12); }
.node-unknown .accordion { border-left: 6px solid rgba(100,100,100,0.08); }
.node-block pre { background:#f6f6f6; padding:8px; overflow:auto; max-height:420px; border-radius:4px; }

.controls { display:flex; gap:12px; align-items:center; margin-bottom:12px; }
.small { font-size:0.9em; color:#555; }
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>Storage Health Report</h1>
    <p class="small">Generated at: __GEN_DATE__</p>
  </div>
  <div>
    <p class="small">Reports directory: <code>__OUTDIR__</code></p>
    <p class="small">Archive: <code>__TARPATH__</code></p>
  </div>
</div>

<div class="controls">
  <div class="search">
    <label for="nodeFilter">Filter nodes: </label>
    <input id="nodeFilter" type="search" placeholder="type to filter nodes (e.g. argon, barium)" style="padding:6px; width:320px;">
  </div>
  <div>
    <button id="expandAll" style="padding:8px;">Expand All</button>
    <button id="collapseAll" style="padding:8px;">Collapse All</button>
  </div>
</div>

<h2>Per-node summary</h2>
<table id="summaryTable" class="summary">
<thead>
<tr><th data-col="node">Node</th><th data-col="total_luns">Total LUNs</th><th data-col="total_ok">OK LUNs</th><th data-col="total_issues">LUNs w/ issues</th><th data-col="bad_paths">Bad paths</th><th data-col="status">Overall</th></tr>
</thead>
<tbody>
HTML_HEAD
} > "${HTML_TMP}"

# append table rows
for s in "${SUMMARY_LINES[@]}"; do
  node="$(awk -F, '{print $1}' <<< "${s}")"
  total_luns="$(awk -F, '{print $2}' <<< "${s}")"
  total_ok="$(awk -F, '{print $3}' <<< "${s}")"
  total_issues="$(awk -F, '{print $4}' <<< "${s}")"
  total_bad_paths="$(awk -F, '{print $5}' <<< "${s}")"
  status="$(awk -F, '{print $6}' <<< "${s}")"
  rowclass="unknown"
  if [[ "${status}" == "HEALTHY" ]]; then rowclass="ok"; fi
  if [[ "${status}" == "WARN" ]]; then rowclass="warn"; fi
  printf '<tr class="%s"><td class="col-node">%s</td><td class="col-total_luns">%s</td><td class="col-total_ok">%s</td><td class="col-total_issues">%s</td><td class="col-bad_paths">%s</td><td class="col-status">%s</td></tr>\n' \
    "${rowclass}" "${node}" "${total_luns}" "${total_ok}" "${total_issues}" "${total_bad_paths}" "${status}" >> "${HTML_TMP}"
done

# finish header and insert node blocks
cat >> "${HTML_TMP}" <<'HTML_MID'
</tbody>
</table>

<h2>Per-node details (click header to expand)</h2>
HTML_MID

# append the built node blocks
cat "${HTML_BLOCKS_TMP}" >> "${HTML_TMP}"

# append JS (sorting/filter/accordion)
cat >> "${HTML_TMP}" <<'HTML_TAIL'
<script>
// Simple client-side sort, filter, and accordion behavior
document.addEventListener('DOMContentLoaded', function () {
  const table = document.getElementById('summaryTable');
  const headers = table.querySelectorAll('th[data-col]');
  headers.forEach((th, idx) => {
    let asc = true;
    th.addEventListener('click', function () {
      const tbody = table.tBodies[0];
      const rows = Array.from(tbody.rows);
      rows.sort((a,b) => {
        const A = a.cells[idx].innerText.trim();
        const B = b.cells[idx].innerText.trim();
        const An = parseFloat(A.replace(/[^0-9.-]/g,'')), Bn = parseFloat(B.replace(/[^0-9.-]/g,''));
        if (!isNaN(An) && !isNaN(Bn)) return asc ? An - Bn : Bn - An;
        return asc ? A.localeCompare(B) : B.localeCompare(A);
      });
      rows.forEach(r => table.tBodies[0].appendChild(r));
      asc = !asc;
    });
  });

  // filter input
  const filterInput = document.getElementById('nodeFilter');
  filterInput.addEventListener('input', function () {
    const q = filterInput.value.trim().toLowerCase();
    const rows = table.tBodies[0].rows;
    for (let r of rows) {
      const node = r.querySelector('.col-node').innerText.toLowerCase();
      const match = q === '' || node.indexOf(q) !== -1;
      r.style.display = match ? '' : 'none';
    }
    document.querySelectorAll('.node-block').forEach(function (blk) {
      const btn = blk.querySelector('.accordion');
      const nodeText = btn ? btn.innerText.toLowerCase() : '';
      const match = q === '' || nodeText.indexOf(q) !== -1;
      blk.style.display = match ? '' : 'none';
    });
  });

  // accordion wiring
  const accs = document.querySelectorAll('.accordion');
  accs.forEach(function (acc) {
    acc.addEventListener('click', function () {
      const expanded = acc.getAttribute('aria-expanded') === 'true';
      acc.setAttribute('aria-expanded', expanded ? 'false' : 'true');
      const panel = acc.nextElementSibling;
      if (!panel) return;
      panel.style.display = expanded ? 'none' : 'block';
    });
    acc.addEventListener('keydown', function(e) {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        acc.click();
      }
    });
  });

  // expand/collapse all
  document.getElementById('expandAll').addEventListener('click', function () {
    document.querySelectorAll('.accordion').forEach(function (acc) {
      acc.setAttribute('aria-expanded','true');
      const panel = acc.nextElementSibling;
      if (panel) panel.style.display = 'block';
    });
  });
  document.getElementById('collapseAll').addEventListener('click', function () {
    document.querySelectorAll('.accordion').forEach(function (acc) {
      acc.setAttribute('aria-expanded','false');
      const panel = acc.nextElementSibling;
      if (panel) panel.style.display = 'none';
    });
  });
});
</script>

</body>
</html>
HTML_TAIL

# Replace placeholders and write final HTML
GEN_DATE="$(date)"
sed "s|__GEN_DATE__|${GEN_DATE}|g; s|__OUTDIR__|${OUT_DIR}|g; s|__TARPATH__|${TAR_PATH}|g" "${HTML_TMP}" > "${HTML_PATH}"
rm -f "${HTML_TMP}"

# Create tarball with this run's artifacts
echo "Creating tarball ${TAR_PATH} ..."
tar -C "${OUT_DIR}" -czf "${TAR_PATH}" \
  "$(basename "${HTML_PATH}")" \
  "$(basename "${CSV_PATH}")" \
  storage_health_*_"${TS}".txt \
  multipath_ll_*_"${TS}".txt 2>/dev/null || echo "tar: some files may be missing; archive incomplete"

# Copy latest.html (not symlink) for robustness (WSL/Windows)
cp -f "${HTML_PATH}" "${OUT_DIR}/latest.html"

# Optionally remove per-run plaintext files (keep only HTML, CSV, tar)
if [[ "${KEEP_PLAINTEXT}" -eq 0 ]]; then
  echo "Removing per-run plaintext files for this run..."
  shopt -s nullglob
  rm -f "${OUT_DIR}"/storage_health_*_"${TS}".txt "${OUT_DIR}"/multipath_ll_*_"${TS}".txt
  shopt -u nullglob
else
  echo "Keeping per-run plaintext files (per --keep-plaintext)."
fi

# Prune old artifacts older than RETENTION_DAYS
if [[ "${RETENTION_DAYS}" -gt 0 ]]; then
  echo "Pruning files older than ${RETENTION_DAYS} days in ${OUT_DIR} ..."
  find "${OUT_DIR}" -maxdepth 1 -type f -mtime +"${RETENTION_DAYS}" -name 'storage_health_*' -print -delete 2>/dev/null || true
fi

# Cleanup temp
rm -f "${HTML_BLOCKS_TMP}"

echo
echo "Done."
echo "HTML summary: ${HTML_PATH}"
echo "CSV summary : ${CSV_PATH}"
echo "Archive     : ${TAR_PATH}"
echo "Latest HTML : ${OUT_DIR}/latest.html"
if [[ "${KEEP_PLAINTEXT}" -eq 1 ]]; then
  echo "Note: per-run plaintext files were preserved (--keep-plaintext)."
fi
if [[ "${STRICT}" -eq 1 ]]; then
  echo "Note: running in STRICT mode (vendor/policy mismatches escalate to WARN)."
fi
