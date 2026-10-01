#!/usr/bin/env bash
# Runs the campaign on the EKS cluster of run/eks.yaml, then deletes the cluster whatever the outcome,
# so nothing keeps billing after the run. Results land in results/raw/eks (resumable: rerun after
# recreating the cluster). Usage:
#   nohup run/eks-run.sh >> results/eks.log 2>&1 &
cd "$(dirname "$0")"
# kubectl uses a service-account token (eks-crossover-sa): the default EKS kubeconfig runs
# `aws eks get-token` on every call, which is slow and loads the Mac during a parallel local run.
export AWS_PROFILE=${AWS_PROFILE:-crossover} KUBECONFIG=${KUBECONFIG:-$HOME/.kube/eks-crossover-sa}
account=$(aws sts get-caller-identity --query Account --output text)
reg="$account.dkr.ecr.ap-southeast-2.amazonaws.com"
caffeinate -dimsu python3 campaign.py --env eks --reps 5 \
  --image "$reg/crossover:1.0" --loadgen-image "$reg/crossover-loadgen:1.0" "$@"
echo "$(date +%H:%M:%S) campaign exited with $?; deleting the EKS cluster"
eksctl delete cluster -f eks.yaml --wait
echo "$(date +%H:%M:%S) cluster deleted"
