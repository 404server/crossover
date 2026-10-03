#!/usr/bin/env python3
"""Downloads one environment's results from the in-cluster controller (run/cloud-runner.yaml) into
results/raw/<env>: packs them into an uncompressed tar on the node (the step files are gzipped already),
copies it with resumed, size-checked transfers, verifies every gzip file and unpacks.

    KUBECONFIG=~/.kube/eks-crossover python3 run/pull-results.py eks
"""
import gzip
import pathlib
import subprocess
import sys
import tarfile
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
env = sys.argv[1]


def ex(script, timeout=1800, partial=False):
    pod = subprocess.run(["kubectl", "-n", "runner", "get", "pods", "-l", "job-name=programme", "-o",
                          "jsonpath={.items[0].metadata.name}"], capture_output=True, text=True, timeout=120).stdout
    for _ in range(100):  # the link to the cluster may drop; every call here is idempotent
        try:
            r = subprocess.run(["kubectl", "-n", "runner", "exec", pod, "--", "sh", "-c", f"({script}); rc=$?; sleep 2; exit $rc"],
                               capture_output=True, timeout=timeout)
            if r.returncode == 0 or (partial and r.stdout):  # a broken stream still delivered a prefix of the range
                return r.stdout
        except subprocess.TimeoutExpired:
            pass
        time.sleep(15)
    raise SystemExit(f"giving up on: {script}")


if not (ROOT / "results" / f"{env}.tar.part").exists():  # a resumed download must not repack the tar under it
    ex(f"cd /app/results/raw && tar -cf /app/results/{env}.tar {env}")
size = int(ex(f"wc -c < /app/results/{env}.tar").strip())
dest = ROOT / "results" / f"{env}.tar.part"
data = dest.read_bytes() if dest.exists() else b""
chunk = 8 << 20
while len(data) < size:
    part = ex(f"tail -c +{len(data) + 1} /app/results/{env}.tar | head -c {chunk}", partial=True)
    data += part
    dest.write_bytes(data)
    print(f"{len(data) / 1e6:.0f} / {size / 1e6:.0f} MB", flush=True)
assert len(data) == size, (len(data), size)
out = ROOT / "results" / "raw"
with tarfile.open(dest) as t:
    t.extractall(out, filter="data")
bad = [p for p in (out / env).rglob("*.json.gz") if not (lambda q: (gzip.decompress(q.read_bytes()), True))(p)]
print(f"unpacked {env}: {sum(1 for _ in (out / env).rglob('done'))} finished cells, {len(bad)} damaged files")
dest.unlink()
