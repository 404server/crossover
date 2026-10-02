#!/usr/bin/env python3
"""Turns raw campaign results into the dashboard data and the paper's generated numbers.

    .venv/bin/python analysis/analyze.py --env kind [--boot 1000]

Reads   results/raw/<env>/rep*/<cfg>/...
Writes  results/<env>/summary.json          everything, machine-readable
        paper/generated/<env>/numbers.tex    \\xodef{key}{value} for every number the paper cites
        paper/generated/<env>/*.csv          pgfplots data
        web/data.js                          all environments' summaries for the dashboard

Statistics follow two rules. The unit of replication is the repetition (a fresh deployment),
never the individual request, because requests within a run are autocorrelated. Spreads across
repetitions are reported as ranges. Tail-latency ratios use a two-level bootstrap in the spirit of
Kalibera & Jones: repetitions (paired by repetition index), then moving blocks of consecutive
requests within each, so serial dependence inside a run is kept.
"""
import argparse
import base64
import collections
import datetime
import gzip
import hashlib
import json
import math
import pathlib
import re

import numpy as np
from scipy import optimize

import webdata

ROOT = pathlib.Path(__file__).resolve().parent.parent
CONFIGS = ["A", "B", "C", "D"]
WORKLOADS = ["browse", "checkout"]
LAYERS = ["app", "db", "ser", "net", "oth"]          # server-reported; edge = e2e - tot
ST = re.compile(r"(\w+);dur=(-?[\d.]+)")
ERR_MAX = 0.05                                        # a step with more errors is saturated
SLO_MS = 100                                          # nor is an open-loop step whose p99 exceeds this sustained
B_H1 = 10_000                                         # bootstrap replicates for the H1 intervals
PCTS = [50, 90, 95, 99, 99.9]


# ------------------------------------------------------------------ loading

def load_step(path, bodies=False):
    lat, code, ts, lay, calls, seq, body = [], [], [], [], [], [], {}
    with gzip.open(path, "rt") as f:
        for line in f:
            r = json.loads(line)
            lat.append(r["latency"])
            code.append(r["code"])
            seq.append(r["seq"])
            ts.append(r["timestamp"].rstrip("Z"))
            h = (r.get("headers") or {}).get("Server-Timing")
            d = dict(ST.findall(h[0])) if h else {}
            lay.append([float(d.get(k, "nan")) for k in (*LAYERS, "tot")])
            calls.append(int(float(d["calls"])) if "calls" in d else -1)
            if bodies:  # keyed by the request's sequence number: results arrive in completion order
                body[r["seq"]] = hashlib.sha256(base64.b64decode(r["body"] or "")).hexdigest() if r["code"] == 200 else None
    t = np.array(ts, dtype="datetime64[ns]").astype(np.int64) / 1e9
    s = {"lat": np.array(lat, float) / 1e6, "ok": np.array(code) == 200, "t": t,
         "lay": np.array(lay, float).reshape(-1, len(LAYERS) + 1), "calls": np.array(calls), "seq": np.array(seq)}
    if bodies:
        s["body"] = body
    return s


def summarize(s, rate=None):
    n, ok = len(s["lat"]), s["ok"]
    t0 = s["t"].min()
    span = max((s["t"] + s["lat"] / 1e3).max() - t0, 1e-9)
    thr = ok.sum() / span
    L = s["lay"][ok]
    lay = {k: float(np.nanmean(L[:, i])) if len(L) else math.nan for i, k in enumerate(LAYERS)}
    lay["edge"] = float(np.nanmean(s["lat"][ok] - L[:, -1])) if len(L) else math.nan
    good_calls = s["calls"][ok & (s["calls"] >= 0)]
    err = 1 - ok.mean()
    p99 = float(np.percentile(s["lat"], 99))
    tail = ok & (s["lat"] >= p99)  # the slowest 1%: what the p99 is made of
    T = s["lay"][tail]
    tail_lay = {k: float(np.nanmean(T[:, i])) if len(T) else math.nan for i, k in enumerate(LAYERS)}
    tail_lay["edge"] = float(np.nanmean(s["lat"][tail] - T[:, -1])) if len(T) else math.nan
    lag = None
    if rate:  # Vegeta stamps a request when it is sent; its lag behind the constant-rate schedule
        first = s["t"][s["seq"].argmin()]
        lag = (s["t"] - (first + (s["seq"] - s["seq"].min()) / rate)) * 1e3
    return {"n": n, "thr": thr, "err": err, "span": span,
            "tail_layers": tail_lay,
            "lag_p50": float(np.percentile(lag, 50)) if lag is not None else None,
            "lag_p99": float(np.percentile(lag, 99)) if lag is not None else None,
            "send": (n - 1) / max(s["t"].max() - t0, 1e-9),
            "mean": float(s["lat"].mean()),
            **{f"p{p:g}": float(np.percentile(s["lat"], p)) for p in PCTS},
            "layers": lay,
            "calls": int(np.bincount(good_calls).argmax()) if len(good_calls) else None,
            "saturated": bool(err > ERR_MAX or (rate is not None and (thr < 0.9 * rate or p99 > SLO_MS)))}


def service(key):
    """'search-77866d8669-d5zzv/app' -> 'search/app' (pod name minus ReplicaSet hash and suffix)."""
    pod, container = key.rsplit("/", 1)
    return f"{pod.rsplit('-', 2)[0]}/{container}"


def cores(v):
    if not v:
        return None
    return int(v[:-1]) / 1000 if v.endswith("m") else float(v)


def cpu_rows(cells, cfg, wl, mode, level, seconds):
    """Per container: CPU used (cores) and its share of the limit, CFS throttling (share of periods
    throttled, throttled ms per second); per tier: CPU milliseconds spent per successful request."""
    per, tiers = collections.defaultdict(lambda: collections.defaultdict(list)), collections.defaultdict(list)
    for (rep, c), cell in cells.items():
        step = cell["cgroup"].get(f"{wl}/{mode}/{level}")
        summ = cell[mode].get(wl, {}).get(level)
        if c != cfg or not step or not summ:
            continue
        seconds = summ[0].get("span") or seconds  # first send to last response, incl. requests still in flight
        done = summ[0]["thr"] * seconds
        tier_ms = collections.Counter()
        for k, d in step.items():
            used = d.get("usage_usec", 0) / 1e6 / seconds
            lim = cell["limits_wl"].get(wl, cell["limits"]).get(k)
            per[k]["cores"].append(used)
            if lim:
                per[k]["util"].append(used / lim)
            if d.get("nr_periods"):
                per[k]["thr"].append(d.get("nr_throttled", 0) / d["nr_periods"])
            per[k]["thr_ms"].append(d.get("throttled_usec", 0) / 1e3 / seconds)
            tier = "mesh" if k.endswith("/istio-proxy") else "db" if k.endswith("/postgres") else "app"
            tier_ms[tier] += d.get("usage_usec", 0) / 1e3
        for t, ms in tier_ms.items():
            tiers[t].append(ms / done if done else math.nan)
    if not per:
        return None
    return {"containers": {k: {m: float(np.median(v)) for m, v in d.items()} for k, d in per.items()},
            "cpu_ms_per_req": {t: float(np.median(v)) for t, v in tiers.items()}}


def incidents(cell):
    f = cell / "incidents.jsonl"
    return {json.loads(l)["step"] for l in f.read_text().splitlines()} if f.exists() else set()


def load_env(raw):
    """-> cells[(rep, cfg)] = {'meta', 'open': {wl: {rate: (summary, lat)}}, 'closed': ..., 'equiv', 'top'}"""
    cells = {}
    for cell in sorted(raw.glob("rep*/*")):
        if not (cell / "done").exists():
            continue
        rep, cfg = int(cell.parent.name[3:]), cell.name
        bad = incidents(cell)
        c = {"meta": json.loads((cell / "meta.json").read_text()), "open": {}, "closed": {}, "equiv": {}, "top": [], "dropped": sorted(bad)}
        for wl in WORKLOADS:
            for mode in ("open", "closed"):
                for f in sorted((cell / wl / mode).glob("*.json.gz"), key=lambda p: int(p.name.split(".")[0])):
                    level = int(f.name.split(".")[0])
                    if f"{wl}/{mode}/{level}" in bad:
                        continue
                    s = load_step(f)
                    lat = s["lat"] if mode == "open" else None
                    c[mode].setdefault(wl, {})[level] = (summarize(s, level if mode == "open" else None), lat)
            eq = cell / "equiv" / f"{wl}.json.gz"
            if eq.exists():
                c["equiv"][wl] = load_step(eq, bodies=True)["body"]
        if (cell / "top.jsonl").exists():
            c["top"] = [json.loads(l) for l in (cell / "top.jsonl").read_text().splitlines()]
        c["cgroup"] = {}
        if (cell / "cgroup.jsonl").exists():
            for l in (cell / "cgroup.jsonl").read_text().splitlines():
                row = json.loads(l)
                c["cgroup"][row["step"]] = {service(k): v for k, v in row["delta"].items()}
        lim = lambda pods: {service(f"{p['metadata']['name']}/{k['name']}"): cores(k.get("resources", {}).get("limits", {}).get("cpu"))
                            for p in pods["items"] for k in p["spec"]["containers"]}
        c["limits"] = lim(c["meta"]["pods"])
        c["limits_wl"] = {wl: lim(pods) for wl, pods in c["meta"].get("pods_wl", {}).items()}
        cells[(rep, cfg)] = c
    return cells


# ------------------------------------------------------------------ statistics

def agg(vals):
    """Median across repetitions and the range (min, max). With five or fewer repetitions a bootstrap
    interval of the median is the range anyway, so the range is what is reported."""
    v = np.array([x for x in vals if x is not None and not math.isnan(x)], float)
    if len(v) == 0:
        return None
    return {"med": float(np.median(v)), "lo": float(v.min()), "hi": float(v.max()), "n": len(v), "kind": "range"}


def blocks(r, L):
    """Moving-block resample of one run's requests (in file order, i.e. completion order)."""
    n = len(r)
    L = max(1, min(int(L), n // 20))  # at least 20 blocks
    if L == 1:
        return r[RNG.integers(n, size=n)]
    starts = RNG.integers(0, n - L + 1, size=-(-n // L))
    return r[(starts[:, None] + np.arange(L)).ravel()[:n]]


def window(r, cap):
    if not cap or len(r) <= cap:
        return r
    s0 = RNG.integers(0, len(r) - cap + 1)
    return r[s0:s0 + cap]


def ratio_ci(x_reps, a_reps, q=99, B=1000, alpha=0.05, block=1, cap=None):
    """Bootstrap interval of pooled-percentile(X) / pooled-percentile(A). x_reps, a_reps: {rep: latencies}.
    Repetitions are resampled with replacement, paired by repetition index when both sides have the same
    repetitions (each repetition block ran its configurations close together in time); within each, requests
    are resampled in moving blocks of `block` consecutive requests. cap: each repetition contributes a
    contiguous window of at most cap requests (used for the descriptive ratios at high rates)."""
    keys_x, keys_a = sorted(x_reps), sorted(a_reps)
    xr = [window(x_reps[k], cap) for k in keys_x]
    ar = [window(a_reps[k], cap) for k in keys_a]
    est = float(np.percentile(np.concatenate(xr), q) / np.percentile(np.concatenate(ar), q))
    paired = keys_x == keys_a
    r = np.empty(B)
    for b in range(B):
        px = RNG.integers(len(xr), size=len(xr))
        pa = px if paired else RNG.integers(len(ar), size=len(ar))
        r[b] = (np.percentile(np.concatenate([blocks(xr[i], block) for i in px]), q)
                / np.percentile(np.concatenate([blocks(ar[i], block) for i in pa]), q))
    return {"est": est, "lo": float(np.percentile(r, 100 * alpha / 2)), "hi": float(np.percentile(r, 100 * (1 - alpha / 2))),
            "level": 1 - alpha, "reps": [len(xr), len(ar)], "paired": paired, "B": B, "block": block}


def peak_ci(curves, B=10_000):
    """Measured capacity: the highest median throughput over the concurrency levels every repetition ran,
    with a percentile interval from resampling whole repetitions."""
    ns = sorted(set.intersection(*[set(c) for c in curves]))
    M = np.array([[c[n] for n in ns] for c in curves], float)
    med = np.median(M, axis=0)
    boots = np.median(M[RNG.integers(len(curves), size=(B, len(curves)))], axis=1).max(axis=1)
    return {"est": float(med.max()), "n": ns[int(med.argmax())], "lo": float(np.percentile(boots, 2.5)),
            "hi": float(np.percentile(boots, 97.5)), "reps": len(curves)}


def bound(row):
    """Operational bottleneck bound at an open-loop step: container k at utilisation U_k of its limit while
    serving X requests/s saturates at X / U_k, so X <= min_k X / U_k. Returns (bound, container)."""
    if not row or not row.get("cpu") or not row.get("thr"):
        return None
    conts = {k: v for k, v in row["cpu"]["containers"].items() if v.get("util")}
    if not conts:
        return None
    k, v = min(conts.items(), key=lambda kv: row["thr"]["med"] / kv[1]["util"])
    return {"bound": row["thr"]["med"] / v["util"], "container": k, "rate": row["rate"], "util": v["util"]}


def usl(n, lam, a, b):
    return lam * n / (1 + a * (n - 1) + b * n * (n - 1))


def fit_usl(n, x):
    n, x = np.asarray(n, float), np.asarray(x, float)
    p0 = [max(x[n == n.min()].mean(), 1e-3), 0.05, 1e-4]
    (lam, a, b), _ = optimize.curve_fit(usl, n, x, p0=p0, bounds=([0, 0, 0], [np.inf, 1, 1]), maxfev=50000)
    pred = usl(n, lam, a, b)
    ss_res, ss_tot = ((x - pred) ** 2).sum(), ((x - x.mean()) ** 2).sum()
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else math.nan
    dof = len(n) - 3
    nstar = math.sqrt((1 - a) / b) if b > 1e-9 else math.inf  # no retrograde region: N* undefined
    xmax = usl(nstar, lam, a, b) if math.isfinite(nstar) else (lam / a if a > 0 else math.inf)  # else the alpha asymptote
    return {"lambda": lam, "alpha": a, "beta": b, "nstar": nstar, "xmax": xmax, "r2": r2,
            "r2adj": 1 - (1 - r2) * (len(n) - 1) / dof if dof > 0 else math.nan, "points": len(n)}


def usl_boot(curves, B):
    """Refit on whole repetitions drawn with replacement, so the repetition stays the unit."""
    out = []
    for _ in range(B):
        pick = [curves[i] for i in RNG.integers(len(curves), size=len(curves))]
        try:
            out.append(fit_usl([n for c in pick for n in c], [c[n] for c in pick for n in c]))
        except (RuntimeError, ValueError):
            pass
    return out


def pct_ci(vals):
    v = np.array([x for x in vals if math.isfinite(x)])
    return (float(np.percentile(v, 2.5)), float(np.percentile(v, 97.5))) if len(v) > 10 else (math.nan, math.nan)


# ------------------------------------------------------------------ analysis

def analyse(env, cells, B):
    cfgs = [c for c in CONFIGS if any(k[1] == c for k in cells)]
    reps = sorted({k[0] for k in cells})
    anymeta = next(iter(cells.values()))["meta"]
    out = {"env": env, "generated": datetime.datetime.now().isoformat(timespec="seconds"), "configs": cfgs,
           "meta": meta_block(cells, anymeta, reps), "hops": {}, "open": {}, "closed": {}, "usl": {},
           "compare": {}, "crossover": {}, "mesh": {}, "equivalence": {}, "cpu": {}, "dropped": {}, "peak": {}, "bound": {}}

    for (rep, cfg), c in cells.items():
        if c["dropped"]:
            out["dropped"][f"rep{rep}/{cfg}"] = c["dropped"]

    for cfg in cfgs:
        mine = [cells[k] for k in cells if k[1] == cfg]
        out["hops"][cfg], out["open"][cfg], out["closed"][cfg], out["usl"][cfg] = {}, {}, {}, {}
        out["peak"][cfg], out["bound"][cfg] = {}, {}
        for wl in WORKLOADS:
            rates = sorted({r for c in mine for r in c["open"].get(wl, {})})
            rows = []
            for r in rates:
                steps = [c["open"][wl][r] for c in mine if r in c["open"].get(wl, {})]
                sums = [s[0] for s in steps]
                pooled = np.concatenate([s[1] for s in steps])
                rows.append({"rate": r, "reps": len(sums),
                             **{f"p{p:g}": agg([s[f"p{p:g}"] for s in sums]) for p in PCTS},
                             "mean": agg([s["mean"] for s in sums]), "thr": agg([s["thr"] for s in sums]),
                             "err": agg([s["err"] for s in sums]),
                             "sat": sum(s["saturated"] for s in sums) / len(sums),
                             "layers": {k: agg([s["layers"][k] for s in sums]) for k in (*LAYERS, "edge")},
                             "tail_layers": {k: agg([s["tail_layers"][k] for s in sums]) for k in (*LAYERS, "edge")},
                             "lag_p99": agg([s["lag_p99"] for s in sums]),
                             "cdf": [float(v) for v in np.percentile(pooled, [*range(0, 100), 99.5, 99.9, 100])],
                             "cpu": cpu_rows(cells, cfg, wl, "open", r, anymeta["args"]["open_step"])})
                calls = [s["calls"] for s in sums if s["calls"] is not None]
                if calls:
                    out["hops"][cfg][wl] = collections.Counter(calls).most_common(1)[0][0]
            if rows:
                out["open"][cfg][wl] = rows
                sustained = [r for r in rows if r["sat"] == 0]
                out["bound"][cfg][wl] = bound(sustained[-1]) if sustained else None
            by_n = collections.defaultdict(list)
            for c in mine:
                for n, s in c["closed"].get(wl, {}).items():
                    by_n[n].append(s[0]["thr"])
            if by_n:
                out["closed"][cfg][wl] = [{"n": n, "x": agg(by_n[n]),
                                           "r": agg([c["closed"][wl][n][0]["mean"] for c in mine if n in c["closed"].get(wl, {})]),
                                           "err": agg([c["closed"][wl][n][0]["err"] for c in mine if n in c["closed"].get(wl, {})]),
                                           "cpu": cpu_rows(cells, cfg, wl, "closed", n, anymeta["args"]["closed_step"])}
                                          for n in sorted(by_n)]
                ns = [n for n in sorted(by_n) for _ in by_n[n]]
                xs = [x for n in sorted(by_n) for x in by_n[n]]
                curves = [{n: s[0]["thr"] for n, s in c["closed"][wl].items()} for c in mine if c["closed"].get(wl)]
                out["peak"][cfg][wl] = peak_ci(curves)
                try:
                    f = fit_usl(ns, xs)
                    boots = usl_boot(curves, B) if len(curves) > 1 else []
                    for k in ("alpha", "beta", "nstar", "xmax"):
                        f[f"{k}_ci"] = pct_ci([b[k] for b in boots]) if boots else [math.nan, math.nan]
                    grid = np.unique(np.round(np.geomspace(1, max(by_n), 60), 3))
                    f["curve"] = [[float(g), float(usl(g, f["lambda"], f["alpha"], f["beta"]))] for g in grid]
                    out["usl"][cfg][wl] = f
                except (RuntimeError, ValueError) as e:
                    out["usl"][cfg][wl] = {"error": str(e)}
        out["equivalence"][cfg] = equivalence(cells, cfg)
        out["cpu"][cfg] = cpu(mine)

    if "A" in cfgs:
        for wl in WORKLOADS:
            out["compare"][wl], out["crossover"][wl] = {}, {}
            for cfg in cfgs:
                if cfg == "A":
                    continue
                out["compare"][wl][cfg] = compare(cells, cfg, wl, B)
                out["crossover"][wl][cfg] = crossover(out, cfg, wl)
    if "C" in cfgs and "D" in cfgs:
        out["mesh"] = mesh(cells, out)
    out["perhop"] = perhop(out)
    out["hypotheses"] = hypotheses(out, cells, B)
    return out


def perhop(out):
    """The price of one synchronous hop, relative to the monolith: inter-service transit and extra
    serialisation per hop at the lowest rate, extra CPU per hop at the highest rate all configurations
    sustained (where idle overheads are amortised)."""
    res = {}
    if "A" not in out["configs"]:
        return res
    for wl in WORKLOADS:
        rows_a = out["open"]["A"].get(wl, [])
        if not rows_a:
            continue
        lo = rows_a[0]
        common = set.intersection(*[{r["rate"] for r in out["open"][c].get(wl, []) if r["sat"] == 0} for c in out["configs"]])
        hi = max(common) if common else None
        a_hi = next((r for r in rows_a if r["rate"] == hi), None)
        for cfg in out["configs"]:
            h = out["hops"].get(cfg, {}).get(wl)
            if cfg == "A" or not h:
                continue
            x_lo = next((r for r in out["open"][cfg][wl] if r["rate"] == lo["rate"]), None)
            x_hi = next((r for r in out["open"][cfg][wl] if r["rate"] == hi), None)
            med = lambda r, k: (r["layers"].get(k) or {}).get("med") if r else None
            cpu_ms = lambda r: sum((r.get("cpu") or {}).get("cpu_ms_per_req", {}).values()) if r and r.get("cpu") else None
            d = {"hops": h, "rate_low": lo["rate"], "rate_high": hi,
                 "net": med(x_lo, "net") / h if med(x_lo, "net") is not None else None,
                 "net_high": med(x_hi, "net") / h if med(x_hi, "net") is not None else None,
                 "ser": (med(x_lo, "ser") - med(lo, "ser")) / h if med(x_lo, "ser") is not None and med(lo, "ser") is not None else None,
                 "p50": (x_lo["p50"]["med"] - lo["p50"]["med"]) / h if x_lo else None,
                 "cpu": (cpu_ms(x_hi) - cpu_ms(a_hi)) / h if cpu_ms(x_hi) is not None and cpu_ms(a_hi) is not None else None}
            res.setdefault(wl, {})[cfg] = d
    return res


def meta_block(cells, m, reps):
    nodes = m["nodes"]["items"]
    a = m["args"]
    starts = [c["meta"]["started"] for c in cells.values()]
    ends = [c["meta"]["finished"] for c in cells.values()]
    return {"reps": len(reps), "cells": len(cells), "alloc": "proportional" if a.get("alloc") else "equal",
            "open_step_s": a["open_step"], "closed_step_s": a["closed_step"],
            "warmup_s": a["warmup"], "budget": {k: a[k] for k in ("app_cpu", "app_mem", "db_cpu", "db_mem")},
            "hpa": a["hpa"], "conc": a["conc"], "rates": a["rates"],
            "k8s": m["kubectl"].get("serverVersion", {}).get("gitVersion"),
            "nodes": [{"name": n["metadata"]["name"], "role": n["metadata"]["labels"].get("crossover/role", "control-plane"),
                       "cpu": n["status"]["capacity"]["cpu"], "memory": n["status"]["capacity"]["memory"],
                       "kernel": n["status"]["nodeInfo"]["kernelVersion"], "runtime": n["status"]["nodeInfo"]["containerRuntimeVersion"]}
                      for n in nodes],
            "started": datetime.datetime.fromtimestamp(min(starts)).isoformat(timespec="minutes"),
            "finished": datetime.datetime.fromtimestamp(max(ends)).isoformat(timespec="minutes"),
            "hours": (max(ends) - min(starts)) / 3600, "topology": {c: cells[k]["meta"]["topology"] for k in cells for c in [k[1]]}}


def equivalence(cells, cfg):
    res = {}
    if cfg == "A":  # the reference; comparing it with itself would only inflate the count
        return res
    for wl in WORKLOADS:
        match = total = 0
        for (rep, c), cell in cells.items():
            if c != cfg or wl not in cell["equiv"]:
                continue
            ref = cells.get((rep, "A")) or next((v for k, v in cells.items() if k[1] == "A"), None)
            if not ref or wl not in ref["equiv"]:
                continue
            mine, theirs = cell["equiv"][wl], ref["equiv"][wl]
            for seq, h in mine.items():
                total += 1
                match += h is not None and h == theirs.get(seq)
        res[wl] = {"match": match, "total": total}
    return res


def cpu(mine):
    """Median (over repetitions) CPU in millicores and memory in MiB by tier, from kubectl top at the end of each step."""
    acc = collections.defaultdict(lambda: collections.defaultdict(list))
    for c in mine:
        for row in c["top"]:
            tiers = collections.Counter()
            for pod, container, cpu_, mem, *_ in row["rows"]:
                tier = "mesh" if container == "istio-proxy" else "db" if "-db-" in pod else "app"
                tiers[f"{tier}_mcpu"] += int(cpu_.rstrip("m"))
                tiers[f"{tier}_mib"] += int(mem.rstrip("Mi")) if mem.endswith("Mi") else 0
            for t, v in tiers.items():
                acc[row["step"]][t].append(v)
    return {step: {t: float(np.median(v)) for t, v in d.items()} for step, d in acc.items()}


def compare(cells, cfg, wl, B):
    """p99 ratio cfg/A at every open-loop rate both ran without saturation (descriptive, 95%)."""
    rows = []
    rates = sorted({r for (rep, c), cell in cells.items() if c in (cfg, "A") for r in cell["open"].get(wl, {})})
    for r in rates:
        x = {rep: cell["open"][wl][r] for (rep, c), cell in cells.items() if c == cfg and r in cell["open"].get(wl, {})}
        a = {rep: cell["open"][wl][r] for (rep, c), cell in cells.items() if c == "A" and r in cell["open"].get(wl, {})}
        if not x or not a:
            continue
        sat_x, sat_a = any(s[0]["saturated"] for s in x.values()), any(s[0]["saturated"] for s in a.values())
        row = {"rate": r, "sat_x": sat_x, "sat_a": sat_a}
        if not sat_x and not sat_a:
            lx, la = {k: v[1] for k, v in x.items()}, {k: v[1] for k, v in a.items()}
            row["p99"] = ratio_ci(lx, la, 99, B, block=r, cap=5000)
            row["p50"] = ratio_ci(lx, la, 50, B, block=r, cap=5000)
        rows.append(row)
    return rows


def crossover(out, cfg, wl):
    res = {"throughput_n": None, "latency_rate": None, "xmax_ratio": None, "peak_ratio": None}
    pa, px = out["peak"].get("A", {}).get(wl), out["peak"].get(cfg, {}).get(wl)
    if pa and px:
        res["peak_ratio"] = px["est"] / pa["est"]
    ua, ux = out["usl"].get("A", {}).get(wl), out["usl"].get(cfg, {}).get(wl)
    if ua and ux and "error" not in ua and "error" not in ux:
        res["xmax_ratio"] = ux["xmax"] / ua["xmax"]
        nmax = max(r["n"] for r in out["closed"][cfg][wl])
        grid = np.geomspace(1, nmax, 400)
        d = usl(grid, ux["lambda"], ux["alpha"], ux["beta"]) - usl(grid, ua["lambda"], ua["alpha"], ua["beta"])
        hit = np.nonzero(d > 0)[0]
        res["throughput_n"] = float(grid[hit[0]]) if len(hit) else None
    ca = {r["n"]: r["x"]["med"] for r in out["closed"].get("A", {}).get(wl, [])}
    res["measured_wins"] = [r["n"] for r in out["closed"].get(cfg, {}).get(wl, []) if r["n"] in ca and r["x"]["med"] > ca[r["n"]]]
    oa = {r["rate"]: r for r in out["open"].get("A", {}).get(wl, [])}
    for r in out["open"].get(cfg, {}).get(wl, []):
        a = oa.get(r["rate"])
        if not a:
            continue
        a_p99 = math.inf if a["sat"] > 0.5 else a["p99"]["med"]
        x_p99 = math.inf if r["sat"] > 0.5 else r["p99"]["med"]
        if x_p99 < a_p99:
            res["latency_rate"] = r["rate"]
            break
    return res


def mesh(cells, out):
    """Istio cost = D minus C at equal rate: inter-service net per hop, DB round trips, end to end."""
    res = {}
    for wl in WORKLOADS:
        hops = out["hops"].get("D", {}).get(wl) or out["hops"].get("C", {}).get(wl)
        rows = []
        rc = {r["rate"]: r for r in out["open"].get("C", {}).get(wl, [])}
        def throttled(row):  # share of CFS periods throttled in the row's busiest application container
            apps = [v for k, v in ((row.get("cpu") or {}).get("containers", {})).items() if k.endswith("/app")]
            return max((v.get("thr", 0) for v in apps), default=0)
        for rd in out["open"].get("D", {}).get(wl, []):
            c = rc.get(rd["rate"])
            # D - C isolates the mesh only while neither configuration is quota-bound
            if not c or c["sat"] > 0 or rd["sat"] > 0 or throttled(c) > 0.01 or throttled(rd) > 0.01:
                continue
            per_rep = lambda cfg, k: [cell["open"][wl][rd["rate"]][0]["layers"][k] if k != "e2e" else cell["open"][wl][rd["rate"]][0]["mean"]
                                      for (rep, cc), cell in cells.items() if cc == cfg and rd["rate"] in cell["open"].get(wl, {})]
            row = {"rate": rd["rate"], "hops": hops}
            for k in ("net", "db", "e2e"):
                dv, cv = per_rep("D", k), per_rep("C", k)
                dv, cv = np.array(dv, float), np.array(cv, float)
                diff = (np.median(dv[RNG.integers(len(dv), size=(10_000, len(dv)))], axis=1)
                        - np.median(cv[RNG.integers(len(cv), size=(10_000, len(cv)))], axis=1))  # interval of the median difference
                row[k] = {"est": float(np.median(dv) - np.median(cv)), "lo": float(np.percentile(diff, 2.5)), "hi": float(np.percentile(diff, 97.5))}
            row["net_per_hop"] = {k: v / hops for k, v in row["net"].items()} if hops else None
            row["share"] = row["e2e"]["est"] / float(np.median(per_rep("D", "e2e")))
            rows.append(row)
        res[wl] = rows
    return res


def hypotheses(out, cells, B):
    """Verdicts under the falsification conditions stated in the paper (Section 4)."""
    h = {}
    micro = [c for c in out["configs"] if c != "A"]
    # H1: at the lowest load level of each workload, A has lower p99 than every microservice config.
    if "A" in out["configs"] and micro:
        k = len(micro) * len(WORKLOADS)
        tests = []
        for wl in WORKLOADS:
            rates = [r["rate"] for r in out["open"].get("A", {}).get(wl, [])]
            if not rates:
                continue
            r0 = min(rates)
            for cfg in micro:
                x = {rep: cell["open"][wl][r0][1] for (rep, c), cell in cells.items() if c == cfg and r0 in cell["open"].get(wl, {})}
                a = {rep: cell["open"][wl][r0][1] for (rep, c), cell in cells.items() if c == "A" and r0 in cell["open"].get(wl, {})}
                if x and a:
                    tests.append({"workload": wl, "rate": r0, "config": cfg, **ratio_ci(x, a, 99, B_H1, alpha=0.05 / k, block=r0)})
        falsified = [t for t in tests if t["hi"] < 1]
        supported = tests and all(t["lo"] > 1 for t in tests)
        h["H1"] = {"verdict": "falsified" if falsified else "supported" if supported else "inconclusive",
                   "tests": tests, "family": k,
                   "rule": "falsified if any microservice configuration has significantly lower p99 than A at the lowest load level "
                           f"(paired two-level block bootstrap CI of the p99 ratio, {B_H1} replicates, Bonferroni over {k} comparisons)"}
    # H2: capacity crossover under the equal budget, and lower coherency (beta) for microservices.
    if "A" in out["configs"] and micro:
        rows, wins, beta_lower = [], False, []
        for wl in WORKLOADS:
            ua = out["usl"].get("A", {}).get(wl, {})
            for cfg in micro:
                ux = out["usl"].get(cfg, {}).get(wl, {})
                cr = out["crossover"].get(wl, {}).get(cfg, {})
                if "beta" not in ux or "beta" not in ua:
                    continue
                wins |= bool(cr.get("measured_wins")) or cr.get("throughput_n") is not None
                beta_lower.append(ux["beta"] < ua["beta"])
                rows.append({"workload": wl, "config": cfg, "beta": ux["beta"], "beta_A": ua["beta"], "xmax_ratio": cr.get("xmax_ratio"),
                             "peak_ratio": cr.get("peak_ratio"),
                             "throughput_n": cr.get("throughput_n"), "measured_wins": cr.get("measured_wins")})
        verdict = "inconclusive" if not rows else "falsified" if not wins or not any(beta_lower) else "supported"
        h["H2"] = {"verdict": verdict, "rows": rows,
                   "rule": "falsified if A matches or exceeds every microservice configuration at all tested concurrency levels, "
                           "or if microservice USL fits show beta >= beta_A"}
    # H3: mesh overhead >= 25% of end-to-end latency for >= 5 hops, and at least linear in hop count.
    if out["mesh"]:
        deep = [r for r in out["mesh"].get("checkout", []) if r["hops"] and r["hops"] >= 5]
        shallow = [r for r in out["mesh"].get("browse", []) if r["net_per_hop"]]
        share = deep[0]["share"] if deep else math.nan  # at the lowest rate, as the condition states
        share_median = float(np.median([r["share"] for r in deep])) if deep else math.nan
        # per-hop cost at each workload's lowest rate, so load does not masquerade as depth
        ph_deep = deep[0]["net_per_hop"]["est"] if deep else math.nan
        ph_shallow = shallow[0]["net_per_hop"]["est"] if shallow else math.nan
        sublinear = ph_deep < 0.8 * ph_shallow  # per-hop cost shrinking by >20% as the chain doubles
        verdict = "inconclusive" if math.isnan(share) else "falsified" if share < 0.25 or sublinear else "supported"
        h["H3"] = {"verdict": verdict, "share_deep": share, "share_deep_median": share_median, "rate_deep": deep[0]["rate"] if deep else None,
                   "per_hop_deep_ms": ph_deep, "per_hop_shallow_ms": ph_shallow,
                   "hops_deep": deep[0]["hops"] if deep else None, "hops_shallow": shallow[0]["hops"] if shallow else None,
                   "rule": "falsified if mesh overhead is below 25% of end-to-end latency for call graphs of five or more hops, "
                           "or if it grows sub-linearly with hop count (both at the workload's lowest clean rate)"}
    else:
        h["H3"] = {"verdict": "not tested", "rule": "needs configurations C and D"}
    return h


# ------------------------------------------------------------------ outputs

def clean(o):
    if isinstance(o, dict):
        return {str(k): clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [clean(v) for v in o]
    if isinstance(o, (np.floating, float)):
        return None if not math.isfinite(o) else float(f"{o:.6g}")  # 6 significant digits: beta is ~1e-6
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.bool_):
        return bool(o)
    return o


def fmt(v, digits=3):
    if v is None or (isinstance(v, float) and not math.isfinite(v)):
        return "--"
    if isinstance(v, str):
        return v
    if v == 0:
        return "0"
    mag = math.floor(math.log10(abs(v)))
    d = max(digits - 1 - mag, 0)
    return f"{v:,.{d}f}".replace(",", "{,}")


def paper_outputs(s, gen_dir):
    gen_dir.mkdir(parents=True, exist_ok=True)
    nums = {}
    m = s["meta"]
    nums.update({"reps": m["reps"], "cells": m["cells"], "hours": fmt(m["hours"], 2), "k8s": m["k8s"] or "--",
                 "openstep": m["open_step_s"], "closedstep": m["closed_step_s"], "warmup": m["warmup_s"],
                 "appcpu": fmt(m["budget"]["app_cpu"] / 1000, 2), "dbcpu": fmt(m["budget"]["db_cpu"] / 1000, 2),
                 "appmem": fmt(m["budget"]["app_mem"] / 1024, 2), "dbmem": fmt(m["budget"]["db_mem"] / 1024, 2)})
    for cfg, d in s["hops"].items():
        for wl, v in d.items():
            nums[f"hops/{wl}/{cfg}"] = v
    for cfg, d in s["open"].items():
        for wl, rows in d.items():
            for r in rows:
                for p in ("p50", "p99", "mean", "thr"):
                    if r[p]:
                        nums[f"{p}/{wl}/{cfg}/{r['rate']}"] = fmt(r[p]["med"])
                for k, v in r["layers"].items():
                    if v:
                        nums[f"layer/{wl}/{cfg}/{r['rate']}/{k}"] = fmt(v["med"])
                if r["p99"]:
                    nums[f"p99lo/{wl}/{cfg}/{r['rate']}"] = fmt(r["p99"]["lo"])
                    nums[f"p99hi/{wl}/{cfg}/{r['rate']}"] = fmt(r["p99"]["hi"])
                c = r.get("cpu") or {}
                apps = {k: v for k, v in c.get("containers", {}).items() if k.endswith("/app")}
                if apps:  # the busiest application container: the bottleneck candidate
                    name, v = max(apps.items(), key=lambda kv: kv[1].get("util", 0))
                    key = f"{wl}/{cfg}/{r['rate']}"
                    nums[f"hot/{key}/name"] = name.split("/")[0]
                    nums[f"hot/{key}/util"] = fmt(100 * v.get("util", math.nan), 2)
                    nums[f"hot/{key}/thr"] = fmt(100 * v.get("thr", math.nan), 2)
                    nums[f"hot/{key}/thrms"] = fmt(v.get("thr_ms", math.nan))
                mem = s["cpu"].get(cfg, {}).get(f"{wl}/open/{r['rate']}", {})
                if mem:
                    nums[f"mem/{wl}/{cfg}/{r['rate']}"] = fmt(sum(v for k, v in mem.items() if k.endswith("_mib")) / 1024)
                for tier, ms in c.get("cpu_ms_per_req", {}).items():
                    nums[f"cpums/{wl}/{cfg}/{r['rate']}/{tier}"] = fmt(ms)
                if c.get("cpu_ms_per_req"):
                    nums[f"cpums/{wl}/{cfg}/{r['rate']}/total"] = fmt(sum(c["cpu_ms_per_req"].values()))
    for cfg, d in s["open"].items():
        for wl, rows in d.items():
            ok = [r["rate"] for r in rows]
            sus = None
            for r in rows:  # highest rate with no saturated repetition at it or below
                if r["sat"] > 0:
                    break
                sus = r["rate"]
            nums[f"sustained/{wl}/{cfg}"] = sus if sus is not None else "--"
            nums[f"gridtop/{wl}"] = max(ok + [nums.get(f"gridtop/{wl}", 0)])
    for cfg, d in s["closed"].items():
        for wl, rows in d.items():
            for r in rows:
                conts = (r.get("cpu") or {}).get("containers", {})
                if conts:  # busiest container of any tier in the closed-loop step
                    name, v = max(conts.items(), key=lambda kv: kv[1].get("util", 0))
                    key = f"{wl}/{cfg}/{r['n']}"
                    nums[f"chot/{key}/name"] = name.replace("/", "~")
                    nums[f"chot/{key}/util"] = fmt(100 * v.get("util", math.nan), 2)
                    nums[f"chot/{key}/thr"] = fmt(100 * v.get("thr", math.nan), 2)
                    app = {k: vv for k, vv in conts.items() if k.endswith("/app")}
                    if app:
                        an, av = max(app.items(), key=lambda kv: kv[1].get("util", 0))
                        nums[f"chot/{key}/appname"] = an.split("/")[0]
                        nums[f"chot/{key}/apputil"] = fmt(100 * av.get("util", math.nan), 2)
                nums[f"x/{wl}/{cfg}/{r['n']}"] = fmt(r["x"]["med"])
    for cfg, d in s["usl"].items():
        for wl, f in d.items():
            for k in ("alpha", "beta", "nstar", "xmax", "r2", "r2adj", "lambda"):
                if k in f:
                    nums[f"usl/{wl}/{cfg}/{k}"] = fmt(f[k])
    for wl, d in s["crossover"].items():
        for cfg, c in d.items():
            nums[f"xmaxratio/{wl}/{cfg}"] = fmt(c.get("xmax_ratio"))
            nums[f"xmaxpct/{wl}/{cfg}"] = fmt(100 * c["xmax_ratio"], 2) if c.get("xmax_ratio") else "--"
            nums[f"crossn/{wl}/{cfg}"] = fmt(c["throughput_n"]) if c.get("throughput_n") else "none"
            nums[f"crossrate/{wl}/{cfg}"] = c["latency_rate"] if c.get("latency_rate") else "none"
    for wl, d in s["compare"].items():
        for cfg, rows in d.items():
            for r in rows:
                if "p99" in r:
                    for k in ("est", "lo", "hi"):
                        nums[f"ratio/{wl}/{cfg}/{r['rate']}/{k}"] = fmt(r["p99"][k])
    for wl, rows in s["mesh"].items():
        for r in rows:
            for k in ("net", "db", "e2e"):
                nums[f"mesh/{wl}/{r['rate']}/{k}"] = fmt(r[k]["est"])
            if r["net_per_hop"]:
                nums[f"mesh/{wl}/{r['rate']}/perhop"] = fmt(r["net_per_hop"]["est"])
            nums[f"mesh/{wl}/{r['rate']}/share"] = fmt(100 * r["share"], 2)
    for wl, d in s.get("perhop", {}).items():
        for cfg, v in d.items():
            for k in ("net", "net_high", "ser", "p50", "cpu"):
                nums[f"perhop/{wl}/{cfg}/{k.replace('_', '')}"] = fmt(v.get(k))
        for k in ("net", "net_high", "ser", "p50", "cpu"):  # ranges over the configurations without a mesh
            vals = [v[k] for c, v in d.items() if c != "D" and v.get(k) is not None]
            if vals:
                nums[f"perhop/{wl}/{k.replace('_', '')}/min"], nums[f"perhop/{wl}/{k.replace('_', '')}/max"] = fmt(min(vals)), fmt(max(vals))
    eq_m = sum(v["match"] for d in s["equivalence"].values() for v in d.values())
    eq_t = sum(v["total"] for d in s["equivalence"].values() for v in d.values())
    nums.update({"equiv/match": eq_m, "equiv/total": eq_t})
    for hname, hv in s["hypotheses"].items():
        nums[f"verdict/{hname}"] = hv["verdict"]
        for key in ("share_deep", "share_deep_median", "per_hop_deep_ms", "per_hop_shallow_ms", "rate_deep"):
            if key in hv:
                nums[f"{hname}/{key}"] = fmt(100 * hv[key] if key.startswith("share") else hv[key], 3)
    h1 = [t["est"] for t in s["hypotheses"].get("H1", {}).get("tests", [])]
    if h1:
        nums.update({"h1/min": fmt(min(h1)), "h1/max": fmt(max(h1))})
    # capacity: measured peaks (primary), bottleneck bounds, use of the budget at 64 clients
    ratios = []
    for cfg, d in s.get("peak", {}).items():
        for wl, pk in d.items():
            key = f"{wl}/{cfg}"
            nums.update({f"peak/{key}": fmt(pk["est"]), f"peaklo/{key}": fmt(pk["lo"]), f"peakhi/{key}": fmt(pk["hi"]),
                         f"peakn/{key}": pk["n"]})
            pa = s["peak"].get("A", {}).get(wl)
            if cfg != "A" and pa:
                ratios.append(pk["est"] / pa["est"])
                nums[f"peakratio/{key}"] = fmt(pk["est"] / pa["est"])
                nums[f"peakpct/{key}"] = fmt(100 * pk["est"] / pa["est"], 2)
            b = s.get("bound", {}).get(cfg, {}).get(wl)
            if b:
                nums.update({f"bound/{key}": fmt(b["bound"]), f"boundname/{key}": b["container"].replace("/", "~"),
                             f"boundrate/{key}": b["rate"]})
            row64 = next((r for r in s["closed"].get(cfg, {}).get(wl, []) if r["n"] == 64), None)
            conts = ((row64 or {}).get("cpu") or {}).get("containers", {})
            if conts:
                used = sum(v.get("cores", 0) for k, v in conts.items() if not k.endswith("/istio-proxy"))
                nums[f"budgetuse/{key}"] = fmt(100 * used / ((m["budget"]["app_cpu"] + m["budget"]["db_cpu"]) / 1000), 2)
    if ratios:
        nums.update({"peakpct/min": fmt(100 * min(ratios), 2), "peakpct/max": fmt(100 * max(ratios), 2),
                     "peakfactor/min": fmt(1 / max(ratios), 2), "peakfactor/max": fmt(1 / min(ratios), 2)})
    # what the p99 is made of: layers of the slowest 1% at the lowest rate, and the generator's dispatch lag
    for wl in WORKLOADS:
        low = {cfg: s["open"][cfg][wl][0] for cfg in s["configs"] if s["open"].get(cfg, {}).get(wl)}
        tot = lambda r, field: sum((r[field].get(k) or {}).get("med", 0) for k in (*LAYERS, "edge"))
        for cfg, r in low.items():
            for k in (*LAYERS, "edge"):
                if r["tail_layers"].get(k):
                    nums[f"tail/{wl}/{cfg}/{k}"] = fmt(r["tail_layers"][k]["med"])
            if r.get("lag_p99"):
                nums[f"lag/{wl}/{cfg}"] = fmt(r["lag_p99"]["med"])
            nums[f"tailtot/{wl}/{cfg}"] = fmt(tot(r, "tail_layers"))
            if cfg != "A" and "A" in low:  # the slowest 1% of X minus the slowest 1% of A: total and transit, in ms
                nums[f"taildiff/{wl}/{cfg}/tot"] = fmt(tot(r, "tail_layers") - tot(low["A"], "tail_layers"))
                nums[f"taildiff/{wl}/{cfg}/net"] = fmt((r["tail_layers"]["net"] or {}).get("med", 0) - (low["A"]["tail_layers"]["net"] or {}).get("med", 0))
        lags = [r["lag_p99"]["med"] for r in low.values() if r.get("lag_p99")]
        if lags:
            nums[f"lagmax/{wl}"] = fmt(max(lags))
    lines = [f"% generated by analysis/analyze.py from results/raw/{s['env']} on {s['generated']}; do not edit"]
    lines += [f"\\xodef{{{k}}}{{{v}}}" for k, v in sorted(nums.items())]
    (gen_dir / "numbers.tex").write_text("\n".join(lines) + "\n")

    def csv(name, header, rows):
        (gen_dir / name).write_text(" ".join(header) + "\n" + "\n".join(" ".join("nan" if v is None else f"{v:.6g}" for v in r) for r in rows) + "\n")

    for wl in WORKLOADS:
        rates = sorted({r["rate"] for cfg in s["open"] for r in s["open"][cfg].get(wl, [])})
        header, rows = ["rate"], []
        for cfg in s["configs"]:
            header += [f"{cfg}p50", f"{cfg}p99", f"{cfg}lo", f"{cfg}hi"]
        for rate in rates:
            row = [rate]
            for cfg in s["configs"]:
                r = next((x for x in s["open"].get(cfg, {}).get(wl, []) if x["rate"] == rate), None)
                ok = r and r["sat"] == 0
                row += [r["p50"]["med"] if ok else None, r["p99"]["med"] if ok else None, r["p99"]["lo"] if ok else None, r["p99"]["hi"] if ok else None]
            rows.append(row)
        csv(f"open-{wl}.csv", header, rows)
        ns = sorted({r["n"] for cfg in s["closed"] for r in s["closed"][cfg].get(wl, [])})
        header, rows = ["n"], []
        for cfg in s["configs"]:
            header += [f"{cfg}x", f"{cfg}lo", f"{cfg}hi"]
        for n in ns:
            row = [n]
            for cfg in s["configs"]:
                r = next((x for x in s["closed"].get(cfg, {}).get(wl, []) if x["n"] == n), None)
                row += [r["x"]["med"], r["x"]["lo"], r["x"]["hi"]] if r else [None] * 3
            rows.append(row)
        csv(f"closed-{wl}.csv", header, rows)
        grid = np.unique(np.round(np.geomspace(1, max(ns), 80), 3)) if ns else []
        header, rows = ["n"] + [f"{cfg}fit" for cfg in s["configs"]], []
        for g in grid:
            rows.append([g] + [usl(g, f["lambda"], f["alpha"], f["beta"]) if "beta" in (f := s["usl"].get(cfg, {}).get(wl, {})) else None for cfg in s["configs"]])
        csv(f"usl-{wl}.csv", header, rows)
        lowest = {cfg: s["open"][cfg][wl][0] for cfg in s["configs"] if s["open"].get(cfg, {}).get(wl)}
        csv(f"layers-{wl}.csv", ["cfg", "rate", *LAYERS, "edge"],
            [[i, r["rate"], *[(r["layers"][k] or {}).get("med") for k in (*LAYERS, "edge")]] for i, (cfg, r) in enumerate(lowest.items())])


def ref_rates(s):
    """Per workload: the lowest rate, and the highest rate up to which every configuration sustained every level
    (the smallest of the configurations' sustained rates)."""
    out = {}
    for wl in WORKLOADS:
        sus = []
        for cfg in s["configs"]:
            rows, last = s["open"].get(cfg, {}).get(wl, []), None
            for r in rows:
                if r["sat"] > 0:
                    break
                last = r["rate"]
            sus.append(last)
        if sus and None not in sus:
            out[wl] = (s["open"][s["configs"][0]][wl][0]["rate"], min(sus))
    return out


def tables(s, gen_dir):
    CFG = {"A": "A (monolith)", "B": "B (4 services)", "C": "C (8 services)", "D": "D (8 + Istio)"}
    ci = lambda v: f"[{fmt(v[0])}, {fmt(v[1])}]" if v and v[0] is not None else ""
    refs = ref_rates(s)
    rows = []
    for wl in WORKLOADS:
        for cfg in s["configs"]:
            u = s["usl"].get(cfg, {}).get(wl, {})
            if "beta" not in u:
                continue
            x = s["crossover"].get(wl, {}).get(cfg, {})
            beta = "0" if u["beta"] < 1e-9 else fmt(u["beta"])
            rows.append(f"{wl.capitalize() if cfg == s['configs'][0] else ''} & {CFG[cfg]} & {fmt(u['lambda'])} & {fmt(u['alpha'])} {ci(u.get('alpha_ci'))} & "
                        f"{beta} {ci(u.get('beta_ci')) if u['beta'] >= 1e-9 else ''} & {fmt(u['nstar']) if u.get('nstar') else '--'} & {fmt(u['xmax'])} & "
                        f"{fmt(u['r2adj'])} \\\\")
        rows.append("\\midrule")
    (gen_dir / "tab-usl.tex").write_text("\n".join(rows[:-1]) + "\n")
    cap = []
    for wl in WORKLOADS:
        pa = s["peak"].get("A", {}).get(wl)
        for cfg in s["configs"]:
            pk, b = s["peak"].get(cfg, {}).get(wl), s["bound"].get(cfg, {}).get(wl)
            if not pk:
                continue
            row64 = next((r for r in s["closed"].get(cfg, {}).get(wl, []) if r["n"] == 64), None)
            conts = ((row64 or {}).get("cpu") or {}).get("containers", {})
            used = sum(v.get("cores", 0) for k, v in conts.items() if not k.endswith("/istio-proxy")) if conts else None
            budget = (s["meta"]["budget"]["app_cpu"] + s["meta"]["budget"]["db_cpu"]) / 1000
            cap.append(f"{wl.capitalize() if cfg == s['configs'][0] else ''} & {CFG[cfg]} & {fmt(pk['est'])} [{fmt(pk['lo'])}, {fmt(pk['hi'])}] & {pk['n']} & "
                       f"{'--' if cfg == 'A' or not pa else fmt(pk['est'] / pa['est'])} & "
                       f"{fmt(b['bound']) + ' (' + chr(92) + 'texttt{' + b['container'].split('/')[0] + '})' if b else '--'} & "
                       f"{fmt(100 * used / budget, 2) + chr(92) + '%' if used is not None else '--'} \\\\")
        cap.append("\\midrule")
    (gen_dir / "tab-capacity.tex").write_text("\n".join(cap[:-1]) + "\n")
    h1 = s["hypotheses"].get("H1", {}).get("tests", [])
    (gen_dir / "tab-h1.tex").write_text("\n".join(
        f"{t['workload'].capitalize()} & {t['rate']} & {CFG[t['config']]} & {fmt(t['est'])} & [{fmt(t['lo'])}, {fmt(t['hi'])}] \\\\" for t in h1) + "\n")
    mrows = []
    for wl in WORKLOADS:
        for r in s["mesh"].get(wl, []):
            mrows.append(f"{wl.capitalize()} & {r['rate']} & {r['hops']} & {fmt(r['net_per_hop']['est'])} [{fmt(r['net_per_hop']['lo'])}, {fmt(r['net_per_hop']['hi'])}] & "
                         f"{fmt(r['db']['est'])} & {fmt(r['e2e']['est'])} & {fmt(100 * r['share'], 2)}\\% \\\\")
    (gen_dir / "tab-mesh.tex").write_text("\n".join(mrows) + "\n")
    crow = []
    for wl, (lo, hi) in refs.items():
        for cfg in s["configs"]:
            r = next((x for x in s["open"][cfg][wl] if x["rate"] == hi), None)
            c = (r or {}).get("cpu") or {}
            ms = c.get("cpu_ms_per_req", {})
            conts = c.get("containers", {})
            apps = {k: v for k, v in conts.items() if k.endswith("/app")}
            hot = max(apps.items(), key=lambda kv: kv[1].get("util", 0)) if apps else None
            mem = s["cpu"].get(cfg, {}).get(f"{wl}/open/{hi}", {})
            crow.append(f"{wl.capitalize() + f' ({hi}/s)' if cfg == s['configs'][0] else ''} & {CFG[cfg]} & {fmt(ms.get('app'))} & {fmt(ms.get('db'))} & "
                        f"{fmt(ms.get('mesh')) if 'mesh' in ms else '--'} & {fmt(sum(v for k, v in mem.items() if k.endswith('_mib')) / 1024 if mem else None)} & "
                        f"{hot[0].split('/')[0] if hot else '--'} & {fmt(100 * hot[1].get('util', math.nan), 2) if hot else '--'}\\% & "
                        f"{fmt(100 * hot[1].get('thr', math.nan), 2) if hot else '--'}\\% & {fmt(r['p99']['med']) if r else '--'} \\\\")
        crow.append("\\midrule")
    (gen_dir / "tab-cost.tex").write_text("\n".join(crow[:-1]) + "\n")
    lines = []
    for wl, (lo, hi) in refs.items():
        lines += [f"\\xodef{{ref/{wl}/low}}{{{lo}}}", f"\\xodef{{ref/{wl}/high}}{{{hi}}}"]
        for cfg in s["configs"]:  # resource cost at the reference rate, so the text need not name the rate
            r = next((x for x in s["open"][cfg][wl] if x["rate"] == hi), None)
            ms = ((r or {}).get("cpu") or {}).get("cpu_ms_per_req", {})
            for tier, v in [*ms.items(), ("total", sum(ms.values()) if ms else None)]:
                lines.append(f"\\xodef{{cpumsref/{wl}/{cfg}/{tier}}}{{{fmt(v)}}}")
            mem = s["cpu"].get(cfg, {}).get(f"{wl}/open/{hi}", {})
            if mem:
                lines.append(f"\\xodef{{memref/{wl}/{cfg}}}{{{fmt(sum(v for k, v in mem.items() if k.endswith('_mib')) / 1024)}}}")
    with open(gen_dir / "numbers.tex", "a") as f:
        f.write("\n".join(lines) + "\n")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--env", default="kind")
    p.add_argument("--boot", type=int, default=1000, help="bootstrap replicates for USL fits and descriptive ratios (H1 uses B_H1)")
    p.add_argument("--outputs-only", action="store_true", help="rewrite paper numbers and tables from the existing summary.json")
    a = p.parse_args()
    if a.outputs_only:
        s = json.loads((ROOT / "results" / a.env / "summary.json").read_text())
        paper_outputs(s, ROOT / "paper" / "generated" / a.env)
        tables(s, ROOT / "paper" / "generated" / a.env)
        return
    raw = ROOT / "results" / "raw" / a.env
    cells = load_env(raw)
    if not cells:
        raise SystemExit(f"no finished cells under {raw}")
    s = clean(analyse(a.env, cells, a.boot))
    (ROOT / "results" / a.env).mkdir(parents=True, exist_ok=True)
    (ROOT / "results" / a.env / "summary.json").write_text(json.dumps(s))
    paper_outputs(s, ROOT / "paper" / "generated" / a.env)
    tables(s, ROOT / "paper" / "generated" / a.env)
    webdata.write(ROOT)
    for k, v in s["hypotheses"].items():
        print(f"{k}: {v['verdict']}")
    print(f"cells={len(cells)} -> results/{a.env}/summary.json, paper/generated/{a.env}/, web/data.js")


RNG = np.random.default_rng(20260930)

if __name__ == "__main__":
    main()
