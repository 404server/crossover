#!/usr/bin/env bash
# Runs the campaign on the EKS cluster of run/eks.yaml. Results land in results/raw/eks (resumable:
# rerun after recreating the cluster). eks-after.sh, started on this script's PID, runs the autoscaling
# treatment afterwards and deletes the cluster whatever the outcome. Usage:
#   nohup run/eks-run.sh >> results/eks.log 2>&1 &
#   nohup run/eks-after.sh $! >> results/eks-hpa.log 2>&1 &
cd "$(dirname "$0")"
# kubectl uses a service-account token (eks-crossover-sa): the default EKS kubeconfig runs
# `aws eks get-token` on every call, which is slow and loads the Mac during a parallel local run.
export AWS_PROFILE=${AWS_PROFILE:-crossover} KUBECONFIG=${KUBECONFIG:-$HOME/.kube/eks-crossover-sa}
account=$(aws sts get-caller-identity --query Account --output text) || exit 1
reg="$account.dkr.ecr.ap-southeast-2.amazonaws.com"
exec caffeinate -dimsu python3 campaign.py --env eks --reps 5 \
  --image "$reg/crossover:1.0" --loadgen-image "$reg/crossover-loadgen:1.0" "$@"
