#!/usr/bin/env bash
# Resumes the local runs after the laptop was away: the proportional service-demand campaign (finished
# cells are skipped, an unfinished one starts over), then the autoscaling smoke test. Run on mains power:
#   nohup run/resume-local.sh >> results/kind-work.log 2>&1 &
cd "$(dirname "$0")" || exit 1
exec caffeinate -dimsu bash -c '
  python3 campaign.py --env kind-work-prop --alloc alloc-work.json --work-us 1000 --image crossover:1.1 --configs A,B,C --reps 3 || exit 1
  echo "$(date +%H:%M:%S) service-demand treatment finished; smoke-testing the autoscaling mode"
  rm -rf ../results/raw/kind-hpa-smoke
  python3 campaign.py --env kind-hpa-smoke --ramp --hpa 24 --configs C --reps 1 --workloads browse --app-cpu 2000 --db-cpu 1000 \
    --ramp-step 60 --ramp-window 30 --browse-ramp 300,1000,2000 --warmup 20 --alloc alloc-prop.json || exit 1
  python3 hpa_check.py ../results/raw/kind-hpa-smoke/rep0/C && touch hpa.ready && echo "$(date +%H:%M:%S) smoke test passed"'
