#!/usr/bin/env python3
"""Smoke-test verdict for the autoscaling mode: the cell finished, replicas were sampled, and the
autoscaler actually added replicas to some service. Exit status 0 means pass.

    python3 hpa_check.py ../results/raw/kind-hpa-smoke/rep0/C
"""
import json
import pathlib
import sys

cell = pathlib.Path(sys.argv[1])
assert (cell / "done").exists(), "cell did not finish"
rows = [json.loads(l) for l in (cell / "scaling.jsonl").read_text().splitlines()]
samples = [s for r in rows for s in r["samples"]]
assert samples, "no scaling samples recorded"
peak = max(max(v[0] for v in s["deploy"].values()) for s in samples)
quota = [s["quota"].get("used") for s in samples if s.get("quota")]
print(f"steps={len(rows)} samples={len(samples)} peak replicas of one service={peak} quota used (last)={quota[-1] if quota else None}")
sys.exit(0 if peak > 1 and quota else 1)
