#!/usr/bin/env python3
"""Equal vs. demand-proportional CPU allocation, side by side, for the paper.

    .venv/bin/python analysis/compare_alloc.py --equal kind --prop kind-prop

Writes paper/generated/tab-alloc.tex (table body). Both summaries must exist (analysis/analyze.py).
"""
import argparse
import json
import pathlib

from analyze import fmt

ROOT = pathlib.Path(__file__).resolve().parent.parent
CFG = {"A": "A (monolith)", "B": "B (4 services)", "C": "C (8 services)", "D": "D (8 + Istio)"}


def peak(s, cfg, wl):
    return s["usl"].get(cfg, {}).get(wl, {}).get("xmax")


def bottleneck(s, cfg, wl, n=64):
    row = next((r for r in s["closed"].get(cfg, {}).get(wl, []) if r["n"] == n), None)
    conts = (row or {}).get("cpu", {}) or {}
    conts = conts.get("containers", {})
    if not conts:
        return "--"
    name, v = max(conts.items(), key=lambda kv: kv[1].get("util", 0))
    return f"\\texttt{{{name.split('/')[0]}}} {fmt(100 * v.get('util', 0), 2)}\\%"


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--equal", default="kind")
    p.add_argument("--prop", default="kind-prop")
    a = p.parse_args()
    eq = json.loads((ROOT / "results" / a.equal / "summary.json").read_text())
    pr = json.loads((ROOT / "results" / a.prop / "summary.json").read_text())
    rows = []
    for wl in ("browse", "checkout"):
        a_eq, a_pr = peak(eq, "A", wl), peak(pr, "A", wl)
        for cfg in ("A", "B", "C", "D"):
            x_eq, x_pr = peak(eq, cfg, wl), peak(pr, cfg, wl)
            cross = pr["crossover"].get(wl, {}).get(cfg, {}) if cfg != "A" else {}
            rows.append(" & ".join([
                wl.capitalize() if cfg == "A" else "", CFG[cfg],
                fmt(x_eq), fmt(x_pr),
                "--" if cfg == "A" else fmt(x_eq / a_eq), "--" if cfg == "A" else fmt(x_pr / a_pr),
                "--" if cfg == "A" else ("none" if not cross.get("measured_wins") else ", ".join(map(str, cross["measured_wins"]))),
                bottleneck(eq, cfg, wl), bottleneck(pr, cfg, wl)]) + " \\\\")
        rows.append("\\midrule")
    out = ROOT / "paper" / "generated" / "tab-alloc.tex"
    out.write_text("\n".join(rows[:-1]) + "\n")
    print(f"wrote {out}")
