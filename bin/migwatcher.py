#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
migwatcher.py — Watch MTV/Forklift migrations with TYPE, STAGE, and better progress.

Usage examples:

  python3 migwatcher.py
  python3 migwatcher.py --style table
  python3 migwatcher.py --ns openshift-mtv --interval 30
  python3 migwatcher.py --show-completed
  python3 migwatcher.py --style pretty --errors-only

Env defaults (optional):
  NS=openshift-mtv
  INTERVAL=40
  STYLE=pretty|table
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# ---------- CLI & ENV ----------

def env_default(name: str, default: str) -> str:
    return os.environ.get(name, default)

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Watch MTV/Forklift migrations")
    p.add_argument("--ns", default=env_default("NS", "openshift-mtv"),
                   help="MTV namespace (default: openshift-mtv)")
    p.add_argument("--interval", type=int,
                   default=int(env_default("INTERVAL", "40")),
                   help="Refresh interval seconds")
    p.add_argument("--style", choices=["pretty", "table"],
                   default=env_default("STYLE", "table"),
                   help="Output style (default: table)")
    p.add_argument("--errors-only", action="store_true",
                   help="In pretty mode, show only VMs with errors/failures")
    p.add_argument("--show-completed", action="store_true",
                   help="Include VMs in Completed phase (default: hide)")
    p.add_argument("plans", nargs="*", help="Limit to these plan names")
    return p.parse_args()

# ---------- Terminal helpers ----------

def is_tty() -> bool:
    return sys.stdout.isatty()

def term_width(default: int = 100) -> int:
    try:
        return shutil.get_terminal_size().columns
    except Exception:
        return default

class Colors:
    def __init__(self, enable: bool):
        if enable:
            try:
                import curses  # noqa: F401
            except Exception:
                enable = False
        self.BOLD = "\033[1m" if enable else ""
        self.DIM = "\033[2m" if enable else ""
        self.RST = "\033[0m" if enable else ""
        self.RED = "\033[31m" if enable else ""
        self.GRN = "\033[32m" if enable else ""
        self.YEL = "\033[33m" if enable else ""
        self.CYN = "\033[36m" if enable else ""
        self.MAG = "\033[35m" if enable else ""

C = Colors(is_tty())

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

def strip_ansi(s: str) -> str:
    return ANSI_RE.sub("", s)

def visible_len(s: str) -> int:
    return len(strip_ansi(s))

def pad_cell(s: str, width: int) -> str:
    extra = width - visible_len(s)
    if extra > 0:
        return s + " " * extra
    return s

def hr(ch: str = "=", width: Optional[int] = None) -> str:
    w = width or term_width()
    return ch * max(1, w)

# ---------- oc helpers ----------

def oc_json(args: List[str]) -> Dict[str, Any]:
    """Run `oc ... -o json` and return parsed JSON; on error return {}."""
    cmd = ["oc"] + args + ["-o", "json"]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL)
        return json.loads(out.decode("utf-8") or "{}")
    except Exception:
        return {}

def get_all(ns: str) -> Dict[str, Any]:
    # Mixed list: migration, plan
    return oc_json(["-n", ns, "get", "migration,plan"])

def get_all_dv() -> Dict[str, Any]:
    return oc_json(["get", "datavolume", "-A"])

# ---------- time / size utils ----------

def parse_rfc3339(ts: str) -> Optional[datetime]:
    try:
        if ts.endswith("Z"):
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return datetime.fromisoformat(ts)
    except Exception:
        return None

def age_str(ts: str) -> str:
    t = parse_rfc3339(ts)
    if not t:
        return "?"
    delta = datetime.now(timezone.utc) - t.astimezone(timezone.utc)
    s = int(delta.total_seconds())
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s//60}m"
    return f"{s//3600}h"

def human_bytes(n: float) -> str:
    """Pretty-print bytes with SI-ish units."""
    try:
        n = float(n)
    except Exception:
        return "0B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    while n >= 1000 and i < len(units) - 1:
        n /= 1000.0
        i += 1
    if n >= 100:
        return f"{n:,.0f}{units[i]}"
    if n >= 10:
        return f"{n:,.1f}{units[i]}"
    return f"{n:,.2f}{units[i]}"

def to_bytes(cap: Any) -> float:
    """Parse storage like 10Gi, 100G, 512Mi, 20GB into bytes."""
    if cap is None:
        return 0.0
    if isinstance(cap, (int, float)):
        return float(cap)
    s = str(cap).strip()
    m = re.match(r"^\s*([0-9.]+)\s*([A-Za-z]+)?\s*$", s)
    if not m:
        return 0.0
    n = float(m.group(1))
    u = (m.group(2) or "").lower()
    mult = {
        "b": 1, "": 1,
        "k": 1_000, "kb": 1_000,
        "ki": 1024, "kib": 1024,
        "m": 1_000_000, "mb": 1_000_000,
        "mi": 1024**2, "mib": 1024**2,
        "g": 1_000_000_000, "gb": 1_000_000_000,
        "gi": 1024**3, "gib": 1024**3,
        "t": 1_000_000_000_000, "tb": 1_000_000_000_000,
        "ti": 1024**4, "tib": 1024**4,
        "p": 1_000_000_000_000_000, "pb": 1_000_000_000_000_000,
        "pi": 1024**5, "pib": 1024**5,
    }
    return n * mult.get(u, 1)

# ---------- Data modeling ----------

def index_plans_target_ns(all_items: List[Dict[str, Any]]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for it in all_items:
        if it.get("kind") != "Plan":
            continue
        name = it.get("metadata", {}).get("name")
        tgt = it.get("spec", {}).get("targetNamespace")
        if name and tgt:
            m[name] = tgt
    return m

def index_plans_warm(all_items: List[Dict[str, Any]]) -> Dict[str, bool]:
    """Return plan name -> True/False for spec.warm."""
    m: Dict[str, bool] = {}
    for it in all_items:
        if it.get("kind") != "Plan":
            continue
        name = it.get("metadata", {}).get("name")
        if not name:
            continue
        warm = it.get("spec", {}).get("warm")
        m[name] = bool(warm)
    return m

def latest_migrations_by_plan(all_items: List[Dict[str, Any]],
                              plan_filter: List[str]) -> List[Dict[str, Any]]:
    """Group by spec.plan.name and take the latest Migration per plan."""
    by_plan: Dict[str, List[Dict[str, Any]]] = {}
    for it in all_items:
        if it.get("kind") != "Migration":
            continue
        plan_name = it.get("spec", {}).get("plan", {}).get("name")
        if not plan_name:
            continue
        if plan_filter and plan_name not in plan_filter:
            continue
        by_plan.setdefault(plan_name, []).append(it)

    latest: List[Dict[str, Any]] = []
    for plan, lst in by_plan.items():
        lst_sorted = sorted(
            lst,
            key=lambda x: x.get("metadata", {}).get("creationTimestamp", "")
        )
        if lst_sorted:
            latest.append(lst_sorted[-1])
    return sorted(
        latest,
        key=lambda m: m.get("spec", {}).get("plan", {}).get("name", "")
    )

def dv_selector_matches(dv: Dict[str, Any], vm_name: str) -> bool:
    """Heuristics to link a DV to a VM by labels/owner/name."""
    meta = dv.get("metadata", {}) or {}
    labels = meta.get("labels", {}) or {}
    name = meta.get("name", "") or ""
    # MTV / Forklift labels
    if labels.get("vmName") == vm_name:
        return True
    if labels.get("forklift.konveyor.io/vmName") == vm_name:
        return True
    # OwnerRef VirtualMachine
    for ref in meta.get("ownerReferences", []) or []:
        if ref.get("kind") == "VirtualMachine" and ref.get("name") == vm_name:
            return True
    # Last resort: DV name contains VM name
    if vm_name and vm_name in name:
        return True
    return False

def dv_progress_for_vm(
    dvas_items: List[Dict[str, Any]],
    target_ns: str,
    vm_name: str,
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Return (percent, copiedBytes, totalBytes) for VMs based on matching DataVolumes.

    - If DV sizes are known: percent + bytes.
    - If sizes are unknown: percent only (copiedBytes/totalBytes are None).
    """
    dv_candidates = [
        dv for dv in dvas_items
        if dv.get("metadata", {}).get("namespace") == target_ns
        and dv_selector_matches(dv, vm_name)
    ]
    if not dv_candidates:
        return (None, None, None)

    total_bytes = 0.0
    copied_bytes = 0.0
    any_size = False
    pct_values: List[float] = []

    for dv in dv_candidates:
        st = dv.get("status", {}) or {}
        p_str = (st.get("progress") or "0%").rstrip("%")
        pct: Optional[float]
        try:
            pct = float(p_str)
            pct_values.append(pct)
        except Exception:
            pct = None

        spec = dv.get("spec", {}) or {}
        storage = (
            spec.get("storage", {})
                .get("resources", {})
                .get("requests", {})
                .get("storage")
        )
        if storage is None:
            # Common CDI layout: spec.pvc.resources.requests.storage
            storage = (
                spec.get("pvc", {})
                    .get("resources", {})
                    .get("requests", {})
                    .get("storage")
            )

        cap = to_bytes(storage)
        if cap > 0 and pct is not None:
            any_size = True
            total_bytes += cap
            copied_bytes += cap * (pct / 100.0)

    if not pct_values:
        return (None, None, None)

    avg_pct = sum(pct_values) / len(pct_values)

    if any_size and total_bytes > 0:
        return (avg_pct, copied_bytes, total_bytes)
    else:
        # We know a percent, but not absolute bytes
        return (avg_pct, None, None)

def first_pipeline_progress(vm: Dict[str, Any]) -> Optional[Tuple[int, int]]:
    """
    Return (copiedBytes, totalBytes) from MTV pipeline, if available.
    """
    for stage in vm.get("pipeline", []) or []:
        prog = stage.get("progress") or {}
        total = prog.get("totalBytes", prog.get("total"))
        copied = prog.get("copiedBytes", prog.get("current"))
        if isinstance(total, int) and total > 0 and isinstance(copied, int) and copied >= 0:
            return (copied, total)
    return None

# ---------- Rendering ----------

def phase_color(phase: str) -> str:
    p = phase or "Unknown"
    if any(x in p for x in ["Completed", "Succeeded"]):
        return C.GRN
    if any(x in p for x in ["Failed", "Error", "Canceled"]):
        return C.RED
    if any(x in p for x in ["Running", "Copying", "Executing", "Precopy", "CopyDisks", "PostProcessing"]):
        return C.YEL
    return C.MAG

def color_phase_text(phase: str) -> str:
    p = phase or "Unknown"
    return f"{phase_color(p)}{p}{C.RST}"

def render_table(
    migs: List[Dict[str, Any]],
    plan_ns_map: Dict[str, str],
    plan_warm_map: Dict[str, bool],
    dvas: Dict[str, Any],
    show_completed: bool,
) -> None:
    """
    Compact table view:

      PLAN  MIGRATION  AGE  VM  TYPE  PHASE  STAGE  PROGRESS  ERROR

    TYPE  = Cold/Warm (Plan.spec.warm or Migration.spec.warm)
    PHASE = VM status.phase
    STAGE = last pipeline stage name, or "-"
    """
    headers = ["PLAN", "MIGRATION", "AGE", "VM", "TYPE", "PHASE", "STAGE", "PROGRESS", "ERROR"]
    rows: List[List[str]] = []
    dvas_items = dvas.get("items", []) or []

    for m in migs:
        plan = m.get("spec", {}).get("plan", {}).get("name", "")
        mig_name = m.get("metadata", {}).get("name", "")
        age = age_str(m.get("metadata", {}).get("creationTimestamp", ""))
        tgt_ns = plan_ns_map.get(plan) or m.get("spec", {}).get("targetNamespace") or ""

        # TYPE: Cold vs Warm
        mig_warm = m.get("spec", {}).get("warm")
        plan_warm = plan_warm_map.get(plan)
        mig_type = "Warm" if (mig_warm is True or plan_warm is True) else "Cold"

        for vm in (m.get("status", {}).get("vms") or []):
            vmname = vm.get("name", "")
            phase = vm.get("phase", "Unknown")

            if phase == "Completed" and not show_completed:
                continue

            # STAGE: last pipeline stage name, "-" if none
            pipeline = vm.get("pipeline") or []
            if pipeline:
                last_stage = pipeline[-1]
                stage_name = last_stage.get("name") or ""
                stage = stage_name if stage_name else "-"
            else:
                stage = "-"

            # PROGRESS:
            # 1) CDI/DataVolume percent (preferred)
            # 2) MTV pipeline bytes (fallback)
            progress = ""
            pct, copied_b, total_b = dv_progress_for_vm(dvas_items, tgt_ns, vmname)
            if pct is not None:
                if total_b:
                    progress = f"{pct:.0f}% {human_bytes(copied_b or 0)}/{human_bytes(total_b)}"
                else:
                    progress = f"{pct:.0f}%"
            else:
                mtv_prog = first_pipeline_progress(vm)
                if mtv_prog:
                    copied, total = mtv_prog
                    progress = f"{human_bytes(copied)}/{human_bytes(total)}"

            err = (vm.get("error") or {}).get("message", "")

            rows.append([
                plan,
                mig_name,
                age,
                vmname,
                mig_type,
                color_phase_text(phase),
                stage,
                progress,
                err,
            ])

    # Widths
    cols = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            cols[i] = max(cols[i], visible_len(str(cell)))

    def fmt_row(row: List[str]) -> str:
        return "  ".join(pad_cell(str(cell), width) for cell, width in zip(row, cols))

    print(fmt_row(headers))
    for r in rows:
        print(fmt_row(r))

def render_pretty(
    migs: List[Dict[str, Any]],
    plan_ns_map: Dict[str, str],
    plan_warm_map: Dict[str, bool],
    dvas: Dict[str, Any],
    errors_only: bool,
    show_completed: bool,
) -> None:
    """
    Verbose view: per-plan header, then VMs with TYPE, pipeline, CDI fallback, etc.
    """
    dvas_items = dvas.get("items", []) or []

    for m in migs:
        plan = m.get("spec", {}).get("plan", {}).get("name", "")
        mig_name = m.get("metadata", {}).get("name", "")
        age = age_str(m.get("metadata", {}).get("creationTimestamp", ""))

        print(f"{C.BOLD}{plan}  ({mig_name})  age:{age}{C.RST}")
        print(hr("="))

        tgt_ns = plan_ns_map.get(plan) or m.get("spec", {}).get("targetNamespace") or ""
        mig_warm = m.get("spec", {}).get("warm")
        plan_warm = plan_warm_map.get(plan)
        mig_type = "Warm" if (mig_warm is True or plan_warm is True) else "Cold"

        for vm in (m.get("status", {}).get("vms") or []):
            vmname = vm.get("name", "")
            phase = vm.get("phase", "Unknown")
            if phase == "Completed" and not show_completed:
                continue

            err = (vm.get("error") or {}).get("message", "")
            if errors_only and not (err or any(x in phase for x in ["Failed", "Error", "Canceled"])):
                continue

            ph = color_phase_text(phase)
            line = f"  {ph}  vm={vmname}  type={mig_type}"
            if err:
                line += f"  {C.RED}error={err}{C.RST}"
            print(line)

            pipeline = vm.get("pipeline") or []
            if pipeline:
                for st in pipeline:
                    n = st.get("name", "stage")
                    s = st.get("status", "unknown")
                    seg = f"    ↳ {n}: {s}"
                    prog = st.get("progress") or {}
                    tbytes = prog.get("totalBytes", prog.get("total"))
                    cbytes = prog.get("copiedBytes", prog.get("current"))
                    if isinstance(tbytes, int) and tbytes > 0 and isinstance(cbytes, int):
                        seg += f"  {human_bytes(cbytes)}/{human_bytes(tbytes)}"
                    print(f"{C.DIM}{seg}{C.RST}")
            else:
                print(f"{C.DIM}    ↳ (no pipeline reported){C.RST}")

            # CDI fallback
            if not first_pipeline_progress(vm):
                pct, copied_b, total_b = dv_progress_for_vm(dvas_items, tgt_ns, vmname)
                if pct is not None:
                    if total_b:
                        print(
                            f"{C.CYN}    progress (CDI): "
                            f"{pct:.0f}% {human_bytes(copied_b or 0)}/{human_bytes(total_b)}{C.RST}"
                        )
                    else:
                        print(f"{C.CYN}    progress (CDI): {pct:.0f}% (size unknown){C.RST}")
                else:
                    print(f"{C.CYN}    progress (CDI): n/a{C.RST}")

            print()

        print()

# ---------- Main loop ----------

def main():
    args = parse_args()

    # Basic sanity checks
    for cmd in ("oc",):
        if shutil.which(cmd) is None:
            print(f"{C.RED}ERROR:{C.RST} '{cmd}' command not found in PATH", file=sys.stderr)
            sys.exit(1)

    print(f"Watching namespace: {args.ns}")
    if args.plans:
        print("Limit to plans:", ", ".join(args.plans))
    print(f"Refresh interval: {args.interval}s, style: {args.style}")
    if not args.show_completed:
        print("Completed VMs are hidden (use --show-completed to include)")
    print("Press Ctrl-C to exit.")
    time.sleep(1)

    while True:
        try:
            if sys.stdout.isatty():
                os.system("clear")

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"{now_str}")
            print()

            all_obj = get_all(args.ns)
            items = all_obj.get("items", []) or []
            if not items:
                print(f"{C.DIM}(no plan/migration objects found in {args.ns}){C.RST}")
                time.sleep(max(1, args.interval))
                continue

            plan_ns_map = index_plans_target_ns(items)
            plan_warm_map = index_plans_warm(items)
            dvas = get_all_dv()
            latest = latest_migrations_by_plan(items, args.plans)

            if not latest:
                print(f"{C.DIM}(no migrations found yet){C.RST}")
            else:
                if args.style == "table":
                    render_table(latest, plan_ns_map, plan_warm_map, dvas, args.show_completed)
                else:
                    render_pretty(latest, plan_ns_map, plan_warm_map, dvas,
                                  errors_only=args.errors_only,
                                  show_completed=args.show_completed)

            time.sleep(max(1, args.interval))
        except KeyboardInterrupt:
            print("\nBye.")
            return
        except Exception as e:
            print(f"{C.RED}[error]{C.RST} {e}", file=sys.stderr)
            time.sleep(max(1, args.interval))

if __name__ == "__main__":
    main()
