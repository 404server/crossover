#!/usr/bin/env bash
# Takes over from eks-run.sh: waits for the running EKS campaign to finish, then, if run/hpa.ready
# exists (the autoscaling mode was tested), adds capacity and runs the autoscaling treatment.
# Whatever happens, the cluster is deleted at the end so it cannot keep billing. Usage:
#   nohup run/eks-after.sh <pid of the running campaign> >> results/eks-hpa.log 2>&1 &
cd "$(dirname "$0")"
# refuse to start without a live PID: with an empty one the wait would end at once
if [ -z "$1" ] || ! kill -0 "$1" 2>/dev/null; then echo "usage: $0 <pid of a running campaign>"; exit 1; fi
export AWS_PROFILE=${AWS_PROFILE:-crossover} KUBECONFIG=${KUBECONFIG:-$HOME/.kube/eks-crossover-sa}
exec caffeinate -dimsu bash -c '
  while kill -0 "$0" 2>/dev/null; do sleep 30; done
  echo "$(date +%H:%M:%S) main EKS campaign finished"
  if [ -f hpa.ready ]; then
    account=$(aws sts get-caller-identity --query Account --output text)
    reg="$account.dkr.ecr.ap-southeast-2.amazonaws.com"
    for i in 1 2 3 4 5; do eksctl create nodegroup -f eks-hpa.yaml && break; sleep 60; done
    python3 campaign.py --env eks-hpa --ramp --hpa 24 --configs A,B,C --reps 3 --alloc alloc-prop.json \
      --image "$reg/crossover:1.0" --loadgen-image "$reg/crossover-loadgen:1.0"
    echo "$(date +%H:%M:%S) autoscaling campaign exited with $?"
  else
    echo "$(date +%H:%M:%S) run/hpa.ready missing: skipping the autoscaling treatment"
  fi
  # retried until AWS confirms the cluster is gone: a delete during a network outage fails quietly
  until aws eks describe-cluster --name crossover --region ap-southeast-2 2>&1 | grep -q ResourceNotFoundException; do
    eksctl delete cluster -f eks.yaml --wait
    sleep 60
  done
  echo "$(date +%H:%M:%S) cluster deleted (confirmed by AWS)"' "$1"
