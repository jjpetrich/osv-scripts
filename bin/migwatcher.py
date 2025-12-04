#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
migatcher.py — Watch Konveyor/MTV migrations with deep detail & CDI progress fallback.

Usage:
  python3 watch_migration.py [--ns openshift-mtv] [--interval 40] [--style pretty|table]
                             [--errors-only] [--show-events] [PLAN ...]
Examples:
  python3 migatcher.py
  python3 migatcher.py --style table
  python3 migatcher.py --errors-only PLAN_A PLAN_B
  NS=openshift-mtv INTERVAL=40 STYLE=pretty SHOW_EVENTS=1 python3 migatcher.py

Requires:
  - Python 3.8+
  - `oc` logged in with access to your MTV namespace
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
    p = argparse.ArgumentParser(description="Watch MTV migrations with CDI progress fallback")
    p.add_argument("--ns", default=env_default("NS", "openshift-mtv"), help="MTV namespace (default: openshift-mtv)")
    p.add_argument("--interval", type=int, default=int(env_default("INTERVAL", "40")), help="Refresh interval seconds")
    p.add_argument("--style", choices=["pretty", "table"], default=env_default("STYLE", "pretty"), help="Output style")
    p.add_argument("--errors-only", action="store_true", default=env_default("SHOW_ERRORS_ONLY", "0") == "1",
                   help="Show only VMs with errors/failures")
    p.add_argument("--show-events", action="store_true", default=env_default("SHOW_EVENTS", "0") == "1",
                   help="Show recent Events tail for the MTV namespace")
    p.add_argument(
        "--show-completed",
        action="store_true",
        help="Include VMs in Completed phase",
    )
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
            import curses
            try:
                curses.setupterm()
            except Exception:
                enable = False
        self.BOLD = "\033[1m" if enable else ""
        self.DIM = "\033[2m" if enable else ""
        self.RST = "\033[0m" if enable else ""
        self.RED = "\033[31m" if enable else ""
        self.GRN = "\033[32m" if enable else ""
        self.YEL = "\033[33m" if enable else ""
        self.BLU = "\033[34m" if enable else ""
        self.MAG = "\033[35m" if enable else ""
        self.CYN = "\033[36m" if enable else ""

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
    # Mixed list: migration, plan, provider
    return oc_json(["-n", ns, "get", "migration,plan,provider"])

def get_all_dv() -> Dict[str, Any]:
    return oc_json(["get", "datavolume", "-A"])

def get_events(ns: str, limit: int = 200) -> List[Dict[str, Any]]:
    ev = oc_json(["-n", ns, "get", "events", "--sort-by=.metadata.creationTimestamp"])
    items = ev.get("items", [])
    items.reverse()
    return items[:limit]

# ---------- utils ----------
def parse_rfc3339(ts: str) -> Optional[datetime]:
    try:
        # 2025-11-21T01:11:27Z or with offset
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
    # Use SI-ish units (B, KB, MB, GB, TB) without colors
    try:
        n = float(n)
    except Exception:
        return "0B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    while n >= 1000 and i < len(units)-1:
        n /= 1000.0
        i += 1
    if n >= 100:
        return f"{n:,.0f}{units[i]}"
    if n >= 10:
        return f"{n:,.1f}{units[i]}"
    return f"{n:,.2f}{units[i]}"

def to_bytes(cap: Any) -> float:
    # Accept number or strings like "10Gi", "100G", "512Mi", "20GB"
    if cap is None:
        return 0.0
    if isinstance(cap, (int, float)):
        return float(cap)
    s = str(cap).strip()
    # Extract number and unit
    m = re.match(r"^\s*([0-9.]+)\s*([A-Za-z]+)?\s*$", s)
    if not m:
        return 0.0
    n = float(m.group(1))
    u = (m.group(2) or "").lower()
    # IEC and SI multipliers
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

def latest_migrations_by_plan(all_items: List[Dict[str, Any]], plan_filter: List[str]) -> List[Dict[str, Any]]:
    # group by spec.plan.name and take latest by metadata.creationTimestamp
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
        lst_sorted = sorted(lst, key=lambda x: x.get("metadata", {}).get("creationTimestamp", ""))
        if lst_sorted:
            latest.append(lst_sorted[-1])
    # sort by plan name for deterministic order
    return sorted(latest, key=lambda m: m.get("spec", {}).get("plan", {}).get("name", ""))

def dv_selector_matches(dv: Dict[str, Any], vm_name: str) -> bool:
    meta = dv.get("metadata", {})
    labels = meta.get("labels", {}) or {}
    ns = meta.get("namespace")
    name = meta.get("name", "")
    # Heuristics from your jq: multiple label possibilities, ownerRef, or name contains
    if labels.get("vmID") and labels.get("vmName", "") == vm_name:
        return True
    if labels.get("forklift.konveyor.io/vmName", "") == vm_name:
        return True
    for ref in meta.get("ownerReferences", []) or []:
        if ref.get("kind") == "VirtualMachine" and ref.get("name") == vm_name:
            return True
    if vm_name and vm_name in name:
        return True
    return False

def dv_progress_for_vm(dvas_items: List[Dict[str, Any]], target_ns: str, vm_name: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Returns (percent, copiedBytes, totalBytes) using DataVolume.status.progress and DV size.
    """
    dv_candidates = [dv for dv in dvas_items if dv.get("metadata", {}).get("namespace") == target_ns and dv_selector_matches(dv, vm_name)]
    if not dv_candidates:
        return (None, None, None)

    total = 0.0
    copied = 0.0
    any_size = False
    for dv in dv_candidates:
        st = dv.get("status", {}) or {}
        # DV percent like "37%"
        p_str = st.get("progress", "0%").rstrip("%")
        try:
            pct = float(p_str)
        except Exception:
            pct = 0.0
        # try several places for total capacity
        spec_total = (
            dv.get("spec", {})
              .get("storage", {})
              .get("resources", {})
              .get("requests", {})
              .get("storage")
        )
        # Some operators may annotate size elsewhere; fallback to 0
        cap = to_bytes(spec_total)
        if cap > 0:
            any_size = True
            total += cap
            copied += cap * (pct / 100.0)

    if not any_size or total <= 0:
        return (None, None, None)

    percent = (copied / total * 100.0) if total > 0 else None
    return (percent, copied, total)

def first_pipeline_progress(vm: Dict[str, Any]) -> Optional[Tuple[int, int]]:
    """
    Returns (copiedBytes, totalBytes) from MTV pipeline if available and totalBytes > 0.
    """
    for stage in vm.get("pipeline", []) or []:
        prog = stage.get("progress") or {}
        # Some versions use totalBytes/current, others total/current
        total = prog.get("totalBytes")
        if total is None:
            total = prog.get("total")
        copied = prog.get("copiedBytes")
        if copied is None:
            copied = prog.get("current")
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
    if any(x in p for x in ["Running", "Copying", "Executing", "PostProcessing", "CopyDisks", "Precopy"]):
        return C.YEL
    return C.MAG

def paint_phase(phase: str) -> str:
    p = phase or "Unknown"
    color = phase_color(p)
    icon = "•"
    if color == C.GRN:
        icon = "✅"
    elif color == C.RED:
        icon = "❌"
    elif color == C.YEL:
        icon = "⏳"
    return f"{color}{icon} {p}{C.RST}"

def color_phase_text(phase: str) -> str:
    p = phase or "Unknown"
    return f"{phase_color(p)}{p}{C.RST}"

def render_table(migs: List[Dict[str, Any]], plan_ns_map: Dict[str, str], dvas: Dict[str, Any], show_completed: bool) -> None:
    headers = ["PLAN", "MIGRATION", "AGE", "VM", "PHASE", "STAGE", "PROGRESS", "ERROR"]
    rows: List[List[str]] = []
    dvas_items = dvas.get("items", []) or []
    for m in migs:
        plan = m.get("spec", {}).get("plan", {}).get("name", "")
        mig_name = m.get("metadata", {}).get("name", "")
        age = age_str(m.get("metadata", {}).get("creationTimestamp", ""))
        tgt_ns = plan_ns_map.get(plan) or m.get("spec", {}).get("targetNamespace") or ""
        for vm in (m.get("status", {}).get("vms") or []):
            vmname = vm.get("name", "")
            phase = vm.get("phase", "Unknown")
            if (phase == "Completed") and not show_completed:
                continue
            stage = ""
            for s in vm.get("pipeline", []) or []:
                if s.get("status"):
                    stage = f"{s.get('name','stage')}:{s.get('status')}"
                    break
            progress = ""
            mtv_prog = first_pipeline_progress(vm)
            if mtv_prog:
                copied, total = mtv_prog
                progress = f"{human_bytes(copied)}/{human_bytes(total)}"
            else:
                pct, copied_b, total_b = dv_progress_for_vm(dvas_items, tgt_ns, vmname)
                if total_b and total_b > 0:
                    progress = f"{pct:.0f}% {human_bytes(copied_b or 0)}/{human_bytes(total_b)}"
            err = (vm.get("error") or {}).get("message", "")
            rows.append([plan, mig_name, age, vmname, color_phase_text(phase), stage, progress, err])

    # Print as aligned table
    cols = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            cols[i] = max(cols[i], visible_len(str(cell)))
    def fmt_row(row: List[str]) -> str:
        return "  ".join(pad_cell(str(cell), width) for cell, width in zip(row, cols))
    print(fmt_row(headers))
    for r in rows:
        print(fmt_row(r))

def render_pretty(migs: List[Dict[str, Any]], plan_ns_map: Dict[str, str], dvas: Dict[str, Any], errors_only: bool, show_completed: bool) -> None:
    dvas_items = dvas.get("items", []) or []
    for m in migs:
        plan = m.get("spec", {}).get("plan", {}).get("name", "")
        mig_name = m.get("metadata", {}).get("name", "")
        age = age_str(m.get("metadata", {}).get("creationTimestamp", ""))
        print(f"{C.BOLD}{plan}  ({mig_name})  age:{age}{C.RST}")
        print(hr("="))
        # Migration conditions (brief)
        conds = m.get("status", {}).get("conditions") or []
        if conds:
            parts = []
            for c in conds:
                t = c.get("type"); s = c.get("status"); msg = c.get("message")
                if t and s:
                    parts.append(f"{t}={s}" + (f"({msg})" if msg else ""))
            if parts:
                print(f"{C.DIM}   conditions: {', '.join(parts)}{C.RST}")
        print()

        tgt_ns = plan_ns_map.get(plan) or m.get("spec", {}).get("targetNamespace") or ""

        for vm in (m.get("status", {}).get("vms") or []):
            vmname = vm.get("name", "")
            phase = vm.get("phase", "Unknown")
            if (phase == "Completed") and not show_completed:
                continue
            err = (vm.get("error") or {}).get("message")
            if errors_only and not (err or any(x in phase for x in ["Failed", "Error", "Canceled"])):
                continue

            # Colorize phase symbol
            sym = paint_phase(phase)
            line = f"  {sym} {vmname}  phase={color_phase_text(phase)}"
            if err:
                line += f"  {C.RED}error={err}{C.RST}"
            print(line)

            pipeline = vm.get("pipeline") or []
            if pipeline:
                for st in pipeline:
                    n = st.get("name", "stage")
                    s = st.get("status", "unknown")
                    seg = f"    ↳ {n}: {s}"
                    if st.get("started"):
                        seg += f"  start={st['started']}"
                    if st.get("ended"):
                        seg += f"  end={st['ended']}"
                    prog = st.get("progress") or {}
                    tbytes = prog.get("totalBytes", prog.get("total"))
                    cbytes = prog.get("copiedBytes", prog.get("current"))
                    if isinstance(tbytes, int) and tbytes > 0 and isinstance(cbytes, int):
                        seg += f"  progress={human_bytes(cbytes)}/{human_bytes(tbytes)}"
                    print(f"{C.DIM}{seg}{C.RST}")
            else:
                print(f"{C.DIM}    ↳ (no pipeline reported){C.RST}")

            # CDI fallback line if needed
            if not first_pipeline_progress(vm):
                pct, copied_b, total_b = dv_progress_for_vm(dvas_items, tgt_ns, vmname)
                if total_b and total_b > 0:
                    print(f"{C.CYN}    progress (CDI): {pct:.0f}%  {human_bytes(copied_b or 0)}/{human_bytes(total_b)}{C.RST}")
                else:
                    print(f"{C.CYN}    progress (CDI): n/a{C.RST}")

            # Hooks & VM conditions
            hooks = vm.get("hooks") or []
            for h in hooks:
                hname = h.get("name") or h.get("type") or "hook"
                hst = h.get("status", "unknown")
                msg = h.get("message")
                print(f"{C.CYN}    hook {hname}: {hst}" + (f"  {msg}" if msg else "") + C.RST)
            vconds = vm.get("conditions") or []
            if vconds:
                parts = []
                for c in vconds:
                    t = c.get("type"); s = c.get("status"); msg = c.get("message")
                    if t and s:
                        parts.append(f"{t}={s}" + (f"({msg})" if msg else ""))
                if parts:
                    print(f"{C.DIM}    conditions: {', '.join(parts)}{C.RST}")
            print()

def print_events(ns: str, limit: int = 50) -> None:
    ev = get_events(ns, 200)
    if not ev:
        print("(no recent events)")
        return
    print(f"{C.BOLD}Recent Events (last ~{limit}) in {ns}{C.RST}")
    # show latest first
    shown = 0
    now = datetime.now(timezone.utc)
    print(f"{'AGE':<6}  {'TYPE':<8}  {'REASON':<20}  {'OBJECT':<40}  MESSAGE")
    for e in ev[:limit]:
        # Choose a timestamp field
        ts = e.get("eventTime", {}).get("time") or e.get("lastTimestamp") or e.get("firstTimestamp") or e.get("metadata", {}).get("creationTimestamp")
        a = age_str(ts) if ts else "?"
        et = e.get("type", "")
        rs = e.get("reason", "")
        obj = f"{e.get('involvedObject',{}).get('kind','')}/{e.get('involvedObject',{}).get('name','')}"
        msg = e.get("message", "") or ""
        print(f"{a:<6}  {et:<8}  {rs:<20}  {obj:<40}  {msg}")
        shown += 1
        if shown >= limit:
            break
    print()

# ---------- Main loop ----------
def main():
    args = parse_args()
    print(f"Watching namespace: {args.ns} ({'all plans' if not args.plans else 'plans: ' + ' '.join(args.plans)}), refresh {args.interval}s")
    print("Press Ctrl-C to exit.")
    while True:
        try:
            # Clear screen
            if sys.stdout.isatty():
                os.system("clear")
            print(datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z"))
            print()

            all_obj = get_all(args.ns)
            items = all_obj.get("items", []) or []
            plan_ns_map = index_plans_target_ns(items)
            dvas = get_all_dv()
            latest = latest_migrations_by_plan(items, args.plans)

            if args.style == "table":
                render_table(latest, plan_ns_map, dvas, show_completed=args.show_completed)
            else:
                render_pretty(latest, plan_ns_map, dvas, errors_only=args.errors_only, show_completed=args.show_completed)

            if args.show_events:
                print()
                print_events(args.ns, limit=50)

            time.sleep(max(1, args.interval))
        except KeyboardInterrupt:
            print("\nBye.")
            return
        except Exception as e:
            print(f"{C.RED}[error]{C.RST} {e}", file=sys.stderr)
            time.sleep(max(1, args.interval))

if __name__ == "__main__":
    main()
