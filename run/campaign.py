#!/usr/bin/env python3
"""Runs the experiment matrix on the current kubectl context (kind, bare-metal or GKE).

For every repetition the configurations run in a shuffled order, and every (rep, config)
gets a fresh deployment, so repetitions are independent samples. The run is resumable:
finished (rep, config) cells are skipped. Layout of the output directory:

    <out>/rep<r>/<cfg>/meta.json               deployment, placement, versions, parameters
    <out>/rep<r>/<cfg>/equiv/<wl>.json.gz      responses with bodies, for the equivalence check
    <out>/rep<r>/<cfg>/<wl>/open/<rate>.json.gz    open-loop step (Vegeta JSON results)
    <out>/rep<r>/<cfg>/<wl>/closed/<N>.json.gz     closed-loop step with N workers
    <out>/rep<r>/<cfg>/top.jsonl               per-container CPU/memory after each step
"""
import argparse
import datetime
import json
import os
import pathlib
import random
import shutil
import subprocess
import sys
import threading
import time

import gen

os.environ.setdefault("KUBECTL_REMOTE_COMMAND_WEBSOCKETS", "false")  # SPDY exec streams; see push()
LG =["kubectl", "exec", "-i", "-n", gen.LOAD_NS, "deploy/loadgen", "--", "sh", "-c"]


def sh(cmd, stdin=None, check=True):
    r = subprocess.run(cmd, input=stdin, capture_output=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)[:200]}\n{r.stderr.decode()[-2000:]}")
    return r.stdout


def kube(*args, stdin=None, check=True):
    return sh(["kubectl", *args], stdin.encode() if isinstance(stdin, str) else stdin, check).decode()


def log(msg):
    print(f"{datetime.datetime.now():%H:%M:%S} {msg}", flush=True)


def targets(cfg, workload, n=2000, seed=1):
    """Identical request sequence for every configuration; only the edge host differs."""
    rnd, base, out = random.Random(seed), gen.edge_url(cfg, workload), []
    for _ in range(n):
        user = rnd.randrange(10_000)
        if workload == "browse":
            out.append(f"GET {base}/api/browse?category={rnd.randrange(50)}&page={rnd.randrange(5)}\nAuthorization: Bearer t{user}\n")
        else:
            out.append(f"POST {base}/api/checkout\nAuthorization: Bearer t{user}\n")
    return "\n".join(out) + "\n"


def attack(tfile, duration, out=None, rate=None, workers=None, bodies=False):
    """rate -> open loop (constant arrivals, no coordinated omission); workers -> closed loop.
    One exec per step: the report goes to stderr, the encoded results (if kept) to stdout, which
    matters when the cluster is a long round trip away."""
    flags = f"-targets={tfile} -duration={duration}s -timeout=10s"
    flags += f" -rate={rate}" if rate else f" -rate=0 -workers={workers} -max-workers={workers}"
    flags += "" if bodies else " -max-body=0"
    script = f"vegeta attack {flags} > /work/r.bin && vegeta report -type=json < /work/r.bin >&2"
    if out:
        script += " && vegeta encode --to json < /work/r.bin | gzip -1"
    r = subprocess.run(LG + [script], capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(f"attack failed: {r.stderr.decode()[-2000:]}")
    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(r.stdout)
    report = next(l for l in reversed(r.stderr.decode().splitlines()) if l.startswith("{"))
    return json.loads(report)


def push(data, path):
    """Copies a file into the load generator. kubectl exec has lost the tail of stdin in
    practice (a 200 KB target file arrived as 16 KB), so the size is verified."""
    for _ in range(5):
        if sh(LG + [f"cat > {path}; wc -c < {path}"], data).strip() == str(len(data)).encode():
            return
    raise RuntimeError(f"could not copy {path} into the load generator intact")


def top(d, step):
    try:
        rows = [l.split() for l in kube("top", "pods", "-n", gen.NS, "--containers", "--no-headers").splitlines()]
        # the load generator's own CPU, to show it never became the bottleneck
        load = [l.split() for l in kube("top", "pods", "-n", gen.LOAD_NS, "--no-headers", check=False).splitlines()]
        with open(d / "top.jsonl", "a") as f:
            f.write(json.dumps({"step": step, "t": time.time(), "rows": rows, "loadgen": load}) + "\n")
    except RuntimeError:
        pass  # metrics-server not ready yet; utilisation is auxiliary


def sut_pods():
    return json.loads(kube("get", "pods", "-n", gen.NS, "-o", "json"))["items"]


def restarts(pods=None):
    pods = sut_pods() if pods is None else pods
    return {p["metadata"]["name"]: sum(c.get("restartCount", 0) for c in p["status"].get("containerStatuses", [])) for p in pods}


def cgroup_cpu(pods=None):
    """cpu.stat of every SUT container (cgroup v2): usage and CFS throttling counters."""
    pods = sut_pods() if pods is None else pods
    procs = {}
    for p in pods:
        # Istio injects its sidecar as a native sidecar (an init container that keeps running)
        for c in p["spec"]["containers"] + [i for i in p["spec"].get("initContainers", []) if i.get("restartPolicy") == "Always"]:
            procs[(p["metadata"]["name"], c["name"])] = subprocess.Popen(
                ["kubectl", "exec", "-n", gen.NS, p["metadata"]["name"], "-c", c["name"], "--", "cat", "/sys/fs/cgroup/cpu.stat"],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    out = {}
    for (pod, c), pr in procs.items():
        text = pr.communicate()[0].decode()
        out[f"{pod}/{c}"] = {k: int(v) for k, v in (l.split() for l in text.splitlines() if l.strip())}
    return out


def guarded(d, step, run):
    """Runs one step; if any pod restarted meanwhile (e.g. OOM), records an incident so the
    analysis drops the step, and waits for the deployment to recover before continuing."""
    p0 = sut_pods()
    before, cg0 = restarts(p0), cgroup_cpu(p0)
    wall0, up0 = time.time(), time.clock_gettime(time.CLOCK_UPTIME_RAW)  # uptime stops while the host sleeps
    result = run()
    slept = (time.time() - wall0) - (time.clock_gettime(time.CLOCK_UPTIME_RAW) - up0)
    p1 = sut_pods()
    after, cg1 = restarts(p1), cgroup_cpu(p1)
    if slept > 5:
        log(f"  !! host slept {slept:.0f}s during {step}; step dropped")
        with open(d / "incidents.jsonl", "a") as f:
            f.write(json.dumps({"step": step, "t": time.time(), "slept_s": slept}) + "\n")
    with open(d / "cgroup.jsonl", "a") as f:
        f.write(json.dumps({"step": step, "delta": {k: {m: v - cg0[k].get(m, 0) for m, v in cg1[k].items()}
                                                   for k in cg1 if k in cg0}}) + "\n")
    if after != before:
        log(f"  !! pod restart during {step}: {[k for k in after if after[k] != before.get(k)]}")
        with open(d / "incidents.jsonl", "a") as f:
            f.write(json.dumps({"step": step, "t": time.time(), "before": before, "after": after}) + "\n")
        kube("-n", gen.NS, "wait", "--for=condition=Available", "deploy", "--all", "--timeout=900s")
    top(d, step)
    return result


def recover(tfile, tries=30):
    """After an overloaded step, wait until a short low-rate probe succeeds completely, so the
    backlog of the previous step does not leak into the next measurement."""
    for i in range(tries):
        rep = attack(tfile, 3, rate=20)
        if rep["success"] == 1 and rep["latencies"]["99th"] < 500e6:
            if i:
                log(f"  recovered after {i} probe(s)")
            return
        time.sleep(10)
    raise RuntimeError("system under test did not recover from overload")


def manifests(cfg, a, shares=None, hpa=0):
    return gen.manifests(cfg, a.image, a.app_cpu, a.app_mem, a.db_cpu, a.db_mem, hpa, a.mem_floor, shares, cpu_cap=a.cpu_cap)


def deploy(cfg, a, shares=None):
    kube("delete", "ns", gen.NS, "--ignore-not-found", "--wait=true", "--timeout=600s")
    # in ramp mode the autoscalers are added only after the warm-up (see ramp())
    kube("apply", "-f", "-", stdin=json.dumps(manifests(cfg, a, shares, 0 if a.ramp else a.hpa)))
    kube("-n", gen.NS, "wait", "--for=condition=Available", "deploy", "--all", "--timeout=900s")


def scaling_state():
    """Replicas, autoscaler view and quota use of the system under test, in one API call."""
    items = json.loads(kube("get", "deploy,hpa,resourcequota", "-n", gen.NS, "-o", "json"))["items"]
    out = {"deploy": {}, "hpa": {}, "quota": {}}
    for i in items:
        name = i["metadata"]["name"]
        if i["kind"] == "Deployment" and i["metadata"]["labels"].get("tier") == "app":
            out["deploy"][name] = [i["spec"].get("replicas", 0), i["status"].get("readyReplicas", 0)]
        elif i["kind"] == "HorizontalPodAutoscaler":
            m = (i["status"].get("currentMetrics") or [{}])[0].get("resource", {}).get("current", {})
            out["hpa"][name] = [i["status"].get("desiredReplicas"), m.get("averageUtilization")]
        elif i["kind"] == "ResourceQuota":
            out["quota"] = {"used": i["status"].get("used", {}).get("requests.cpu"), "hard": i["status"].get("hard", {}).get("requests.cpu")}
    return out


def ramp(d, cfg, wl, tfile, a, shares):
    """Autoscaling treatment: switch the HPAs on after the warm-up, then raise the open-loop rate in long
    steps, sampling replicas every 10 s. CPU deltas run from one step's end to the next, so replicas
    created during a step are counted from their first second."""
    hpas = [i for i in manifests(cfg, a, shares, a.hpa)["items"] if i["kind"] == "HorizontalPodAutoscaler"]
    kube("apply", "-f", "-", stdin=json.dumps({"apiVersion": "v1", "kind": "List", "items": hpas}))
    time.sleep(30)  # let the autoscalers read their first metrics with the system idle
    prev_pods = sut_pods()
    prev_cg, prev_rs = cgroup_cpu(prev_pods), restarts(prev_pods)
    saturated = 0
    for r in a.ramp_rates[wl]:
        step = f"{wl}/ramp/{r}"
        samples, stop = [], threading.Event()

        def sample():
            while not stop.is_set():
                try:
                    samples.append({"t": time.time(), **scaling_state()})
                except RuntimeError:
                    pass
                stop.wait(10)

        th = threading.Thread(target=sample, daemon=True)
        th.start()
        wall0, up0 = time.time(), time.clock_gettime(time.CLOCK_UPTIME_RAW)
        rep_ = attack(tfile, a.ramp_step, d / wl / "ramp" / f"{r}.json.gz", rate=r)
        slept = (time.time() - wall0) - (time.clock_gettime(time.CLOCK_UPTIME_RAW) - up0)
        stop.set()
        th.join()
        pods = sut_pods()
        cg, rs = cgroup_cpu(pods), restarts(pods)
        with open(d / "cgroup.jsonl", "a") as f:
            f.write(json.dumps({"step": step, "delta": {k: {m: v - prev_cg.get(k, {}).get(m, 0) for m, v in cg[k].items()} for k in cg}}) + "\n")
        with open(d / "scaling.jsonl", "a") as f:
            f.write(json.dumps({"step": step, "t0": wall0, "samples": samples}) + "\n")
        restarted = [k for k in rs if k in prev_rs and rs[k] != prev_rs[k]]
        if restarted or slept > 5:
            log(f"  !! incident during {step}: restarted={restarted} slept={slept:.0f}s")
            with open(d / "incidents.jsonl", "a") as f:
                f.write(json.dumps({"step": step, "t": time.time(), "restarted": restarted, "slept_s": slept}) + "\n")
        prev_cg, prev_rs = cg, rs
        top(d, step)
        last = samples[-1] if samples else {}
        p99 = rep_["latencies"]["99th"] / 1e6
        log(f"  {wl} ramp {r:>5}/s  ok={rep_['success']:.3f}  p99={p99:8.1f} ms  replicas={ {k: v[0] for k, v in last.get('deploy', {}).items()} }"
            f"  quota={last.get('quota', {}).get('used')}")
        saturated = saturated + 1 if rep_["success"] < 0.95 or p99 > 2000 else 0
        if saturated >= 2:
            break


def cell(rep, cfg, a, out):
    d = out / f"rep{rep}" / cfg
    shutil.rmtree(d, ignore_errors=True)  # an unfinished cell from an interrupted run starts over clean
    d.mkdir(parents=True)
    shares = a.alloc_map.get(cfg)  # demand-proportional treatment: one deployment per workload
    log(f"rep {rep} config {cfg}: deploying" + (" (proportional allocation per workload)" if shares else ""))
    deploy(cfg, a, shares and shares[a.workloads[0]])
    meta = {"config": cfg, "rep": rep, "env": a.env, "started": time.time(), "args": {k: (str(v) if isinstance(v, pathlib.Path) else v) for k, v in vars(a).items()},
            "topology": gen.TOPOLOGY[cfg], "pods": json.loads(kube("get", "pods", "-n", gen.NS, "-o", "json")),
            "nodes": json.loads(kube("get", "nodes", "-o", "json")), "kubectl": json.loads(kube("version", "-o", "json")),
            "alloc": shares, "pods_wl": {}}
    for i, wl in enumerate(a.workloads):
        if shares and i:
            log(f"  {wl}: redeploying with this workload's allocation")
            deploy(cfg, a, shares[wl])
        meta["pods_wl"][wl] = json.loads(kube("get", "pods", "-n", gen.NS, "-o", "json"))
        tfile = f"/work/{wl}.txt"
        push(targets(cfg, wl).encode(), tfile)
        sh(LG + [f"head -n 300 {tfile} > /work/eq-{wl}.txt"])
        attack(f"/work/eq-{wl}.txt", 5, d / "equiv" / f"{wl}.json.gz", rate=20, bodies=True)
        # Closed-loop warm-up: runs at each configuration's own capacity, so every JVM sees tens of
        # thousands of requests (JIT, pools, caches) without the overload an open-loop warm-up risks.
        log(f"  {wl}: warm-up {a.warmup}s, closed loop N={a.warm_workers}")
        attack(tfile, a.warmup, workers=a.warm_workers)
        if a.ramp:
            ramp(d, cfg, wl, tfile, a, shares and shares[wl])
            continue
        saturated = 0
        for r in a.rates[wl]:
            rep_ = guarded(d, f"{wl}/open/{r}", lambda: attack(tfile, a.open_step, d / wl / "open" / f"{r}.json.gz", rate=r))
            p99 = rep_["latencies"]["99th"] / 1e6
            log(f"  {wl} open  {r:>5}/s  ok={rep_['success']:.3f}  p99={p99:8.1f} ms")
            saturated = saturated + 1 if rep_["success"] < 0.95 or p99 > 2000 else 0
            if saturated >= 2:
                break
            time.sleep(a.pause)
            if saturated:
                recover(tfile)
        recover(tfile)
        for n in a.conc:
            rep_ = guarded(d, f"{wl}/closed/{n}", lambda: attack(tfile, a.closed_step, d / wl / "closed" / f"{n}.json.gz", workers=n))
            log(f"  {wl} closed N={n:>4}  X={rep_['throughput']:8.1f}/s  ok={rep_['success']:.3f}")
            time.sleep(a.pause)
    meta["finished"] = time.time()
    (d / "meta.json").write_text(json.dumps(meta))
    (d / "done").touch()


def ints(s):
    return [int(x) for x in s.split(",")]


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--env", default="kind", help="environment label stored with the results")
    p.add_argument("--out", type=pathlib.Path, help="default: ../results/raw/<env>")
    p.add_argument("--configs", default="A,B,C,D")
    p.add_argument("--workloads", default="browse,checkout")
    p.add_argument("--reps", type=int, default=5)
    p.add_argument("--seed", type=int, default=2026)
    p.add_argument("--browse-rates", type=ints, default=ints("50,100,200,300,400,500,600,800,1000,1400,1800,2200,2600,3200"))
    p.add_argument("--checkout-rates", type=ints, default=ints("25,50,100,150,200,300,400,500,700,900,1200,1500,1800"))
    p.add_argument("--conc", type=ints, default=ints("1,2,4,8,16,32,64,128,256"))
    p.add_argument("--open-step", type=int, default=30, help="seconds per open-loop rate level")
    p.add_argument("--closed-step", type=int, default=20, help="seconds per closed-loop concurrency level")
    p.add_argument("--warmup", type=int, default=60)
    p.add_argument("--warm-workers", type=int, default=16)
    p.add_argument("--pause", type=int, default=5)
    p.add_argument("--image", default="crossover:1.0")
    p.add_argument("--loadgen-image", default="crossover-loadgen:1.0")
    p.add_argument("--app-cpu", type=int, default=4000)
    p.add_argument("--app-mem", type=int, default=4096)
    p.add_argument("--db-cpu", type=int, default=2000)
    p.add_argument("--db-mem", type=int, default=2048)
    p.add_argument("--mem-floor", type=int, default=1024)
    p.add_argument("--hpa", type=int, default=0)
    p.add_argument("--alloc", type=pathlib.Path, help="JSON from analysis/alloc.py: demand-proportional CPU shares")
    p.add_argument("--ramp", action="store_true", help="autoscaling treatment: HPA on, long open-loop steps")
    p.add_argument("--ramp-step", type=int, default=180, help="seconds per ramp level")
    p.add_argument("--browse-ramp", type=ints, default=ints("500,1000,1500,2000,3000,4000,5000,6000,8000"))
    p.add_argument("--checkout-ramp", type=ints, default=ints("200,400,600,800,1200,1600,2000,2400"))
    p.add_argument("--cpu-cap", type=int, help="namespace CPU quota (millicores); in ramp mode default 3x app + db budget")
    a = p.parse_args()
    a.ramp_rates = {"browse": a.browse_ramp, "checkout": a.checkout_ramp}
    if a.ramp and a.cpu_cap is None:
        a.cpu_cap = 3 * a.app_cpu + a.db_cpu  # the same elastic ceiling for every configuration
    if a.ramp and not a.hpa:
        p.error("--ramp needs --hpa MAX_REPLICAS")
    a.alloc_map = json.loads(a.alloc.read_text())["shares"] if a.alloc else {}
    a.configs, a.workloads = a.configs.split(","), a.workloads.split(",")
    a.rates = {"browse": a.browse_rates, "checkout": a.checkout_rates}
    out = a.out or pathlib.Path(__file__).resolve().parent.parent / "results" / "raw" / a.env
    a.out = str(out)
    if "D" in a.configs and "istio-system" not in kube("get", "ns"):
        sys.exit("config D needs Istio: istioctl install --set profile=minimal -y")
    kube("apply", "-f", "-", stdin=json.dumps(gen.loadgen(a.loadgen_image)))
    kube("-n", gen.LOAD_NS, "wait", "--for=condition=Available", "deploy/loadgen", "--timeout=300s")
    for rep in range(a.reps):
        order = a.configs[:]
        random.Random(a.seed + rep).shuffle(order)
        for cfg in order:
            if (out / f"rep{rep}" / cfg / "done").exists():
                continue
            cell(rep, cfg, a, out)
    kube("delete", "ns", gen.NS, "--ignore-not-found", "--wait=false")
    log("campaign complete")
