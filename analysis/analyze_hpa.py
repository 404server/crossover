#!/usr/bin/env python3
"""Analysis of the autoscaling (HPA ramp) treatment.

    .venv/bin/python analysis/analyze_hpa.py --env eks-hpa

Each ramp level is held for ramp_step seconds and only its last ramp_window seconds are recorded, after the
autoscaler has had time to react. A level is sustained if, in that window, at most 5% of requests failed,
throughput reached 90% of the offered rate and p99 met the SLO (slo_ms). Two measures are compared under the
shared CPU quota: the highest sustained rate, and the CPU requested by all pods at each rate. Writes results/<env>/hpa.json,
paper/generated/<env>/hpa-<workload>.csv and \\xodef numbers (prefixed hpa/) appended to
paper/generated/<env>/numbers-hpa.tex.
"""
import argparse
import collections
import datetime
import json
import math
import pathlib

import numpy as np

import webdata
from analyze import ROOT, WORKLOADS, clean, fmt, incidents, load_step

ERR_MAX = 0.05


def window_stats(path, rate, window, slo_ms):
    s = load_step(path)
    t_end = s["t"].max()
    keep = s["t"] >= t_end - window          # steady state: the last `window` seconds of sends
    lat, ok = s["lat"][keep], s["ok"][keep]
    span = max(window, 1e-9)
    thr = ok.sum() / span
    err = 1 - ok.mean() if len(ok) else 1.0
    return {"p50": float(np.percentile(lat, 50)), "p99": float(np.percentile(lat, 99)), "thr": float(thr), "err": float(err),
            "saturated": bool(err > ERR_MAX or thr < 0.9 * rate or np.percentile(lat, 99) > slo_ms), "n": int(len(lat))}


def scaling(cell):
    out = {}
    f = cell / "scaling.jsonl"
    if not f.exists():
        return out
    for line in f.read_text().splitlines():
        row = json.loads(line)
        samples = row["samples"]
        if not samples:
            continue
        last = samples[-1]
        reps = {k: v[0] for k, v in last["deploy"].items()}
        first_final = next((x["t"] - row["t0"] for x in samples if {k: v[0] for k, v in x["deploy"].items()} == reps), None)
        out[row["step"]] = {"replicas": reps, "total": sum(reps.values()), "quota_used": last.get("quota", {}).get("used"),
                            "settled_s": first_final}
    return out


def millicores(q):
    if not q:
        return None
    return int(q[:-1]) if q.endswith("m") else int(float(q) * 1000)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--env", default="eks-hpa")
    p.add_argument("--window", type=int, help="seconds analysed per level (default: the recorded ramp_window)")
    a = p.parse_args()
    raw = ROOT / "results" / "raw" / a.env
    rows = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(list)))
    meta = None
    for cell in sorted(raw.glob("rep*/*")):
        if not (cell / "done").exists():
            continue
        cfg = cell.name
        meta = meta or json.loads((cell / "meta.json").read_text())
        args = meta["args"]
        a.window = a.window or args.get("ramp_window", 60)
        slo = args.get("slo_ms", 2000)  # runs before the SLO existed stopped at a p99 of 2 s
        bad = incidents(cell)
        sc = scaling(cell)
        for wl in WORKLOADS:
            for f in sorted((cell / wl / "ramp").glob("*.json.gz"), key=lambda q: int(q.name.split(".")[0])):
                rate = int(f.name.split(".")[0])
                step = f"{wl}/ramp/{rate}"
                if step in bad:
                    continue
                st = window_stats(f, rate, a.window, slo)
                st.update(sc.get(step, {}))
                rows[cfg][wl][rate].append(st)
    if not rows:
        raise SystemExit(f"no finished cells under {raw}")

    out = {"env": a.env, "generated": datetime.datetime.now().isoformat(timespec="seconds"), "window_s": a.window,
           "args": meta["args"] if meta else {}, "configs": sorted(rows), "ramp": {}, "sustained": {}}
    for cfg, by_wl in rows.items():
        out["ramp"][cfg], out["sustained"][cfg] = {}, {}
        for wl, by_rate in by_wl.items():
            series = []
            sus = None
            for rate in sorted(by_rate):
                st = by_rate[rate]
                sat_frac = sum(x["saturated"] for x in st) / len(st)
                series.append({"rate": rate, "reps": len(st), "sat": sat_frac,
                               "p50": float(np.median([x["p50"] for x in st])), "p99": float(np.median([x["p99"] for x in st])),
                               "p99_lo": float(min(x["p99"] for x in st)), "p99_hi": float(max(x["p99"] for x in st)),
                               "thr": float(np.median([x["thr"] for x in st])),
                               "replicas": float(np.median([x.get("total", math.nan) for x in st])),
                               "replicas_by_service": {k: float(np.median([x.get("replicas", {}).get(k, math.nan) for x in st]))
                                                       for k in (st[0].get("replicas") or {})},
                               "quota_used_m": float(np.median([millicores(x.get("quota_used")) or math.nan for x in st])),
                               "quota_used_all": [millicores(x.get("quota_used")) for x in st],
                               "settled_s": float(np.median([x.get("settled_s") if x.get("settled_s") is not None else math.nan for x in st]))})
            for r in series:  # highest rate with no saturated repetition at it or any lower rate
                if r["sat"] > 0:
                    break
                sus = r["rate"]
            out["ramp"][cfg][wl] = series
            out["sustained"][cfg][wl] = sus

    # H2 under elasticity: does any decomposed configuration sustain a higher rate than the monolith?
    wins = []
    for wl in WORKLOADS:
        a_sus = out["sustained"].get("A", {}).get(wl)
        for cfg in out["configs"]:
            if cfg == "A":
                continue
            x_sus = out["sustained"].get(cfg, {}).get(wl)
            if a_sus is not None and x_sus is not None and x_sus > a_sus:
                wins.append({"workload": wl, "config": cfg, "sustained": x_sus, "sustained_A": a_sus})
    # efficiency: at a rate both sustained in every repetition, did X request less CPU than A in every repetition?
    cheaper = []
    for wl in WORKLOADS:
        by_rate = lambda c: {r["rate"]: r for r in out["ramp"].get(c, {}).get(wl, []) if r["sat"] == 0}
        a_rows = by_rate("A")
        for cfg in out["configs"]:
            if cfg == "A":
                continue
            for rate, r in sorted(by_rate(cfg).items()):
                ra = a_rows.get(rate)
                xs, as_ = [q for q in r["quota_used_all"] if q], [q for q in (ra or {}).get("quota_used_all", []) if q]
                if ra and xs and as_ and max(xs) < min(as_):
                    cheaper.append({"workload": wl, "config": cfg, "rate": rate, "cpu": max(xs) / 1000, "cpu_A": min(as_) / 1000})
    out["h2_elastic"] = {"verdict": "supported" if wins or cheaper else "falsified", "wins": wins, "cheaper": cheaper,
                         "rule": "supported if, under the same CPU quota, a decomposed configuration sustains a higher offered "
                                 "rate than the monolith, or sustains a rate the monolith also sustains while requesting less CPU "
                                 "in every repetition"}
    out = clean(out)
    (ROOT / "results" / a.env).mkdir(parents=True, exist_ok=True)
    (ROOT / "results" / a.env / "hpa.json").write_text(json.dumps(out))

    gen = ROOT / "paper" / "generated" / a.env
    gen.mkdir(parents=True, exist_ok=True)
    nums = {"hpa/verdict": out["h2_elastic"]["verdict"], "hpa/window": a.window, "hpa/slo": fmt(slo),
            "hpa/cheaper": len(out["h2_elastic"]["cheaper"]),
            "hpa/step": out["args"].get("ramp_step", "--"), "hpa/cap": fmt((out["args"].get("cpu_cap") or 0) / 1000, 2),
            "hpa/reps": max((r["reps"] for c in out["ramp"].values() for s in c.values() for r in s), default=0)}
    for cfg in out["configs"]:
        for wl in WORKLOADS:
            nums[f"hpa/sustained/{wl}/{cfg}"] = out["sustained"].get(cfg, {}).get(wl) or "--"
            series = out["ramp"].get(cfg, {}).get(wl, [])
            for r in series:
                key = f"{wl}/{cfg}/{r['rate']}"
                nums[f"hpa/p99/{key}"] = fmt(r["p99"])
                nums[f"hpa/replicas/{key}"] = fmt(r["replicas"])
                nums[f"hpa/quota/{key}"] = fmt((r["quota_used_m"] or math.nan) / 1000, 2)
    (gen / "numbers-hpa.tex").write_text("% generated by analysis/analyze_hpa.py; do not edit\n" +
                                         "\n".join(f"\\xodef{{{k}}}{{{v}}}" for k, v in sorted(nums.items())) + "\n")
    for wl in WORKLOADS:
        rates = sorted({r["rate"] for c in out["ramp"].values() for r in c.get(wl, [])})
        head = ["rate"] + [f"{c}{k}" for c in out["configs"] for k in ("p99", "lo", "hi", "rep", "cap")]
        lines = [" ".join(head)]
        for rate in rates:
            row = [str(rate)]
            for c in out["configs"]:
                r = next((x for x in out["ramp"].get(c, {}).get(wl, []) if x["rate"] == rate), None)
                ok = r and r["sat"] == 0
                row += [f"{r[k]:.6g}" if ok else "nan" for k in ("p99", "p99_lo", "p99_hi")]
                row += [f"{r['replicas']:.6g}" if r and r["replicas"] is not None else "nan",
                        f"{r['quota_used_m'] / 1000:.6g}" if r and r["quota_used_m"] is not None else "nan"]
            lines.append(" ".join(row))
        (gen / f"hpa-{wl}.csv").write_text("\n".join(lines) + "\n")
    webdata.write(ROOT)
    print(f"H2 (elastic): {out['h2_elastic']['verdict']}; sustained: {out['sustained']}")


if __name__ == "__main__":
    main()
