#!/usr/bin/env bash
# Entry point of the in-cluster controller (run/cloud-runner.yaml): the equal-budget campaign, then the
# autoscaling treatment on three 8-vCPU nodes, then the system-under-test nodes are scaled to zero.
# Results stay on the load generator's node (hostPath /var/lib/crossover) until they are downloaded.
# Both campaigns are resumable, so a restarted pod continues where it stopped.
set -u
cd /app/run
exec > >(tee -a /app/results/cloud-run.log) 2>&1
img="$REG/crossover:1.0"; lg="$REG/crossover-loadgen:1.0"
scale() { until aws eks update-nodegroup-config --region "$REGION" --cluster-name "$CLUSTER" --nodegroup-name "$1" \
            --scaling-config "minSize=$2,maxSize=3,desiredSize=$3" >/dev/null; do sleep 60; done; }
attempt() {  # a campaign that exits early is restarted (it resumes); five exits in a row end the programme
  for i in 1 2 3 4 5; do python3 campaign.py "$@" && return 0; echo "$(date -u +%H:%M:%S) campaign exited; restart $i"; sleep 60; done
  return 1
}
if [ ! -f /app/results/MAIN_DONE ]; then
  attempt --env eks --reps 5 --image "$img" --loadgen-image "$lg" && touch /app/results/MAIN_DONE
fi
if [ -f /app/results/MAIN_DONE ] && [ ! -f /app/results/HPA_DONE ]; then
  scale sut-8 1 3
  for i in $(seq 180); do [ "$(kubectl get nodes -l crossover/role=sut --no-headers | grep -c " Ready")" -ge 4 ] && break; sleep 20; done
  kubectl get nodes -l crossover/role=sut --no-headers
  # 3.5-core replicas so that two fit an 8-vCPU node; a 4-core database tier; quota 3 x 3.5 + 4 = 14.5 cores
  attempt --env eks-hpa --ramp --hpa 24 --configs A,B,C --reps 3 --alloc alloc-prop.json --app-cpu 3500 --db-cpu 4000 \
    --image "$img" --loadgen-image "$lg" && touch /app/results/HPA_DONE
fi
echo "$(date -u +%H:%M:%S) programme finished; scaling the system-under-test nodes to zero"
scale sut-8 0 0; scale sut-4 0 0
touch /app/results/ALL_DONE
sleep infinity  # keep the pod, and with it easy access to the results, until they are downloaded
