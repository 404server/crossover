#!/usr/bin/env python3
"""Demand-proportional CPU shares for the allocation treatment, derived from a finished campaign.

    .venv/bin/python analysis/alloc.py --from kind > run/alloc-prop.json

For every decomposed configuration and workload, each pod's share of its tier's budget is its measured
CPU use (median across repetitions) at the highest offered rate every configuration sustained, divided
by the tier's total. Shares are per workload, so this is the best static allocation for that workload:
an upper bound on what right-sizing can do for a decomposed deployment.
"""
import argparse
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent


def shares(s, cfg, wl, rate):
    row = next(r for r in s["open"][cfg][wl] if r["rate"] == rate)
    used = {"app": {}, "db": {}}
    for key, v in row["cpu"]["containers"].items():
        svc, container = key.split("/")
        if container == "app":
            used["app"][svc] = v["cores"]
        elif container == "postgres":
            used["db"][svc.removesuffix("-db")] = v["cores"]
    return {tier: {svc: c / sum(d.values()) for svc, c in d.items()} for tier, d in used.items()}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="env", default="kind")
    a = p.parse_args()
    s = json.loads((ROOT / "results" / a.env / "summary.json").read_text())
    out = {"source": f"results/{a.env}/summary.json ({s['generated']})", "rates": {}, "shares": {}}
    for wl in ("browse", "checkout"):
        common = set.intersection(*[{r["rate"] for r in s["open"][c][wl] if r["sat"] == 0} for c in s["configs"]])
        rate = max(common)
        out["rates"][wl] = rate
        for cfg in s["configs"]:
            if cfg != "A":  # one pod per tier: the monolith's allocation cannot change
                out["shares"].setdefault(cfg, {})[wl] = shares(s, cfg, wl, rate)
    print(json.dumps(out, indent=1))
