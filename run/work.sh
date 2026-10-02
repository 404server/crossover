#!/usr/bin/env bash
# Service-demand treatment on the local kind cluster: every module operation burns 1 ms of CPU
# (WORK_US=1000, image crossover:1.1). Runs the equal split, derives demand-proportional shares from it,
# then runs the proportional allocation. Usage:
#   nohup run/work.sh >> results/kind-work.log 2>&1 &
cd "$(dirname "$0")" || exit 1
common="--work-us 1000 --image crossover:1.1 --configs A,B,C --reps 3"
exec caffeinate -dimsu bash -c "
  python3 campaign.py --env kind-work $common || exit 1
  ../.venv/bin/python ../analysis/analyze.py --env kind-work || exit 1
  ../.venv/bin/python ../analysis/alloc.py --from kind-work > alloc-work.json || exit 1
  python3 campaign.py --env kind-work-prop --alloc alloc-work.json $common
  echo \"\$(date +%H:%M:%S) service-demand treatment finished\""
