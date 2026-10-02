# Crossover: monolith vs. microservices testbed

Testbed, campaign runner, analysis, dashboard and journal manuscript for
"Where Does Decomposition Pay Off? A Controlled, Layer-Resolved Performance Comparison of Monolithic and
Microservice Deployments on Kubernetes".

```
bench/      Spring Boot service: one jar, any topology (A monolith … D fine + Istio)
run/        gen.py (Kubernetes manifests), campaign.py (experiment matrix), kind.yaml, eks.yaml + eks-*.sh
analysis/   analyze.py: raw Vegeta results -> summary.json, paper numbers/CSVs, web/data.js
web/        index.html: results dashboard (open directly, no server needed)
paper/      journal manuscript; paper/generated/<env>/ is written by analyze.py
notes/      control-run notes
results/    <env>/summary.json (analysed); raw/<env>/rep<r>/<cfg>/… is the campaign output
```

## Data

Raw Vegeta results of every cell (several GB) are archived on Zenodo: **[DOI to be added]** (CC BY 4.0);
`results/DATA.md` describes the files and `run/archive.sh` builds the upload set.
Unpack them into `results/raw/` and run `analysis/analyze.py --env <env>` to regenerate every summary,
paper number and the dashboard data. Environments: `kind` (equal CPU split) and `kind-prop`
(demand-proportional CPU allocation from `run/alloc-prop.json`, derived by `analysis/alloc.py`) on the
workstation, and `eks` (equal CPU split) on Amazon EKS.

## History

The measurement protocol (four configurations, per-layer latency decomposition, USL-based crossover analysis,
hypotheses H1–H3) was designed in spring 2026 and presented as a talk at a KBTU university conference, without
proceedings. The testbed, campaign runner and analysis pipeline in this repository were implemented, and the local
and cloud campaigns were run, in September–October 2026. A first cloud campaign was discarded because result files
arrived truncated (see "Run on Amazon EKS"). Building the testbed also changed parts of the protocol (see the
paper's Discussion, "The protocol, revisited").

## How the testbed isolates the architecture

* **Identical code.** Eight business modules (Auth, Profiles, Search, Products, Stock,
  Reservations, Carts, Orders) live in one artifact. `SERVICE_MODULES` says which modules a
  process hosts; every other module is called over JSON/HTTP 1.1. A = all eight in one process,
  B = four services of two modules, C = eight services, D = C plus Istio sidecars.
* **Equal CPU budget.** Every configuration gets the same total CPU for its application tier
  (default 4 cores) and database tier (2 cores), split equally. Memory has a 1 GiB per-pod
  floor and is reported as a measured cost.
* **Database per service.** Each service owns a Postgres instance with only its modules' tables.
* **Per-layer latency without clock sync.** Every process returns a W3C `Server-Timing` header
  (app, db, ser, net, oth, tot) aggregated over its callees. Per hop, net = client-observed
  round trip minus the callee's own total. The edge layer is Vegeta's latency minus `tot`.
* **Two load modes.** Open loop (constant arrival rate) for latency, free of coordinated
  omission; closed loop (N workers, no think time) for the throughput curve the USL is fitted to.
* **Equivalence check.** The same 100 requests per workload; response bodies hashed and
  compared with the monolith's.

## Run locally (kind on this Mac)

```bash
kind create cluster --config run/kind.yaml
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
kubectl -n kube-system patch deploy metrics-server --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
istioctl install --set profile=minimal -y                      # needed for config D

(cd bench && mvn -q -DskipTests package) && docker build -t crossover:1.0 bench
docker build -t crossover-loadgen:1.0 -f run/loadgen.Dockerfile run
kind load docker-image crossover:1.0 crossover-loadgen:1.0 --name crossover

python3 run/campaign.py --env kind --reps 5                    # resumable; hours
python3 -m venv .venv && .venv/bin/pip install -r analysis/requirements.txt
.venv/bin/python analysis/analyze.py --env kind --boot 1000
open web/index.html
```

## Run on Amazon EKS (cloud environment)

`run/eks.yaml` describes the cluster: Kubernetes 1.35 in ap-southeast-2, Graviton c7g nodes in one zone (an 8- and
a 4-vCPU node for the system under test, a 4-vCPU node for the load generator; 16 vCPUs of on-demand quota).

```bash
export AWS_PROFILE=<profile> AWS_REGION=ap-southeast-2
eksctl create cluster -f run/eks.yaml --kubeconfig ~/.kube/eks-crossover
aws eks create-addon --cluster-name crossover --addon-name metrics-server
KUBECONFIG=~/.kube/eks-crossover istioctl install --set profile=minimal -y

# images: the Mac builds arm64, which is what Graviton runs
acct=$(aws sts get-caller-identity --query Account --output text); reg=$acct.dkr.ecr.ap-southeast-2.amazonaws.com
aws ecr create-repository --repository-name crossover; aws ecr create-repository --repository-name crossover-loadgen
aws ecr get-login-password | docker login --username AWS --password-stdin $reg
docker tag crossover:1.0 $reg/crossover:1.0 && docker push $reg/crossover:1.0
docker tag crossover-loadgen:1.0 $reg/crossover-loadgen:1.0 && docker push $reg/crossover-loadgen:1.0
```

The default EKS kubeconfig runs `aws eks get-token` on every `kubectl` call, which is slow when the campaign
makes thousands of them. The runner therefore uses a service-account token: create a `kube-system/crossover-admin`
service account bound to `cluster-admin`, a `kubernetes.io/service-account-token` Secret for it, and a kubeconfig
`~/.kube/eks-crossover-sa` with that token and the cluster's endpoint and CA.

```bash
nohup run/eks-run.sh >> results/eks.log 2>&1 &              # campaign.py --env eks --reps 5 (resumable)
nohup run/eks-after.sh $! >> results/eks-hpa.log 2>&1 &     # then: optional autoscaling run, delete the cluster
```

`eks-after.sh` deletes the cluster when the campaign ends, retrying until AWS confirms it is gone. A cost guard
that does not depend on the laptop is a CronJob in the cluster that scales every node group to zero at a fixed
time. It runs the `aws-cli` image on the load-generator node, whose IAM role needs `eks:UpdateNodegroupConfig` and
`eks:DescribeNodegroup` on `nodegroup/crossover/*`. Delete the cluster yourself with
`eksctl delete cluster -f run/eks.yaml --wait` if anything goes wrong.

Two properties of the remote link shaped the runner. `kubectl exec` streams to a distant cluster can end before all
output has arrived, without an error, so every load-generator command waits 2 s before exiting, and every result
file is size-checked, resumed if short and gzip-verified. The runner also survives a lost connection: a cell that
fails waits for the API server and starts over clean.

## Treatments

* **Demand-proportional allocation.** `analysis/alloc.py --from kind > run/alloc-prop.json` derives per-workload
  CPU shares from a finished equal-budget campaign; `campaign.py --alloc run/alloc-prop.json` redeploys every
  decomposed configuration per workload with those shares. Compare with `analysis/compare_alloc.py`.
* **Autoscaling.** `campaign.py --ramp --hpa 24 --alloc run/alloc-prop.json` adds a CPU-based HorizontalPodAutoscaler
  (70 % target) per service after the warm-up and a namespace CPU quota (default: 3 × application budget +
  database budget), then raises the open-loop rate in 180 s levels. Analyse with `analysis/analyze_hpa.py`.
  `run/hpa-smoke.sh` tests this mode on kind first.

## Analysis outputs

```bash
.venv/bin/python analysis/analyze.py --env <env>          # results/<env>/summary.json, paper/generated/<env>/, web/data.js
.venv/bin/python analysis/compare_alloc.py                # equal vs proportional table
.venv/bin/python analysis/compare_env.py --a kind --b eks # local vs cloud table
.venv/bin/python analysis/analyze_hpa.py --env eks-hpa    # autoscaling numbers and CSVs
cd paper && tectonic -X compile main.tex
run/archive.sh                                            # Zenodo upload set in zenodo/
```

## Run on another cluster (bare metal, other clouds)

1. Label nodes: one for the load generator, the rest for the system under test.
   `kubectl label node <n> crossover/role=loadgen` and `crossover/role=sut`.
2. Push both images to a registry the cluster can pull from and pass `--image` and `--loadgen-image`
   (use `docker buildx build --platform linux/amd64` for x86 nodes).
   On bare metal, pin CPU frequency and idle states on the SUT nodes first
   (`cpupower frequency-set -g performance`, `cpupower idle-set -D 10`): the local run showed a low-load
   latency penalty from idle-state exits (see `notes/control-lowload.md`).
3. Install metrics-server and, for D, Istio (`istioctl install --set profile=minimal -y`).
4. `python3 run/campaign.py --env <name> --reps 5`, then `analysis/analyze.py --env <name>`.
   The dashboard shows every analysed environment.
