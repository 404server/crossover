#!/usr/bin/env bash
# Waits for the given local campaign PID to finish, smoke-tests the autoscaling mode on the local kind
# cluster, and on success creates run/hpa.ready so that eks-after.sh runs the treatment on EKS. Usage:
#   nohup run/hpa-smoke.sh <pid of the local campaign> >> results/hpa-smoke.log 2>&1 &
cd "$(dirname "$0")"
# refuse to start without a live PID: with an empty one the wait would end at once
if [ -z "$1" ] || ! kill -0 "$1" 2>/dev/null; then echo "usage: $0 <pid of a running campaign>"; exit 1; fi
exec caffeinate -dimsu bash -c '
  while kill -0 "$0" 2>/dev/null; do sleep 30; done
  echo "$(date +%H:%M:%S) local campaign finished; smoke-testing the autoscaling mode"
  rm -rf ../results/raw/kind-hpa-smoke
  python3 campaign.py --env kind-hpa-smoke --ramp --hpa 24 --configs C --reps 1 --workloads browse --app-cpu 2000 --db-cpu 1000 \
    --ramp-step 60 --ramp-window 30 --browse-ramp 300,1000,2000 --warmup 20 --alloc alloc-prop.json || { echo "smoke run failed"; exit 1; }
  python3 hpa_check.py ../results/raw/kind-hpa-smoke/rep0/C || { echo "autoscaler did not scale"; exit 1; }
  ../.venv/bin/python ../analysis/analyze_hpa.py --env kind-hpa-smoke || { echo "analysis failed"; exit 1; }
  touch hpa.ready
  echo "$(date +%H:%M:%S) smoke test passed: run/hpa.ready created"' "$1"
