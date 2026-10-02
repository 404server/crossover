#!/usr/bin/env python3
"""Local vs. cloud environment, side by side, for the paper.

    .venv/bin/python analysis/compare_env.py --a kind --b eks

Writes paper/generated/tab-env.tex (table body): per workload and configuration, the p99 at the lowest
offered rate and its ratio to the monolith (H1), the measured peak throughput and its ratio to the monolith (H2),
and the inter-service transit per hop, for both environments. Both summaries must exist (analysis/analyze.py).
"""
import argparse
import json
import pathlib

from analyze import fmt

ROOT = pathlib.Path(__file__).resolve().parent.parent
CFG = {"A": "A (monolith)", "B": "B (4 services)", "C": "C (8 services)", "D": "D (8 + Istio)"}


def cols(s, cfg, wl):
    low = (s["open"].get(cfg, {}).get(wl) or [{}])[0]
    p99 = (low.get("p99") or {}).get("med")
    h1 = next((t for t in s["hypotheses"]["H1"]["tests"] if t["workload"] == wl and t["config"] == cfg), None)
    xmax, xmax_a = ((s.get("peak", {}).get(c, {}).get(wl) or {}).get("est") for c in (cfg, "A"))  # measured peaks
    hop = s.get("perhop", {}).get(wl, {}).get(cfg, {}).get("net")
    return [fmt(p99), "--" if cfg == "A" else fmt(h1["est"]) if h1 else "n/a", fmt(xmax),
            "--" if cfg == "A" else fmt(xmax / xmax_a) if xmax and xmax_a else "n/a", "--" if cfg == "A" else fmt(hop)]


def rows(a, b):
    out = []
    for wl in ("browse", "checkout"):
        for cfg in CFG:
            if cfg in a["configs"] or cfg in b["configs"]:
                out.append(" & ".join([wl.capitalize() if cfg == "A" else "", CFG[cfg], *cols(a, cfg, wl), *cols(b, cfg, wl)]) + " \\\\")
        out.append("\\midrule")
    return out[:-1]


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--a", default="kind")
    p.add_argument("--b", default="eks")
    args = p.parse_args()
    a, b = (json.loads((ROOT / "results" / e / "summary.json").read_text()) for e in (args.a, args.b))
    lines = rows(a, b)
    assert len(lines) == 9 and all(l.count("&") == 11 for l in lines if l != "\\midrule"), "unexpected table shape"
    out = ROOT / "paper" / "generated" / "tab-env.tex"
    out.write_text("\n".join(lines) + "\n")
    print(f"wrote {out}")
