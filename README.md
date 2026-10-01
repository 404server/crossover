# Crossover: monolith vs. microservices testbed

Testbed, campaign runner, analysis, dashboard and journal manuscript for
"A Framework for Empirical Performance Comparison of Microservice and Monolithic Architectures".

```
bench/      Spring Boot service: one jar, any topology (A monolith … D fine + Istio)
run/        gen.py (Kubernetes manifests), campaign.py (experiment matrix), kind.yaml
analysis/   analyze.py: raw Vegeta results -> summary.json, paper numbers/CSVs, web/data.js
web/        index.html: results dashboard (open directly, no server needed)
paper/      journal manuscript; paper/generated/<env>/ is written by analyze.py
notes/      control-run notes
results/    <env>/summary.json (analysed); raw/<env>/rep<r>/<cfg>/… is the campaign output
```

## Data

Raw Vegeta results of every cell (≈1 GB) are archived on Zenodo: **[DOI to be added]** (CC BY 4.0).
Unpack them into `results/raw/` and run `analysis/analyze.py --env <env>` to regenerate every summary,
paper number and the dashboard data. Environments: `kind` (equal CPU split) and `kind-prop`
(demand-proportional CPU allocation from `run/alloc-prop.json`, derived by `analysis/alloc.py`).

## History

The measurement protocol (four configurations, per-layer latency decomposition, USL-based crossover analysis,
hypotheses H1–H3) was designed in spring 2026 and presented as a talk at a KBTU university conference, without
proceedings. The testbed, campaign runner and analysis pipeline in this repository were implemented, and the local
campaign was run, in September–October 2026; building the testbed changed parts of the protocol (see the paper's
Discussion, "The protocol, revisited").

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

## Run on a bare-metal cluster or GKE

1. Label nodes: one for the load generator, the rest for the system under test.
   `kubectl label node <n> crossover/role=loadgen` and `crossover/role=sut`.
2. Push both images to a registry the cluster can pull from and pass `--image` and `--loadgen-image`
   (both images build for the host architecture; use `docker buildx build --platform linux/amd64` for x86 nodes).
   On bare metal, pin CPU frequency and idle states on the SUT nodes first
   (`cpupower frequency-set -g performance`, `cpupower idle-set -D 10`): the local run showed a low-load
   latency penalty from idle-state exits (see `notes/control-lowload.md`).
3. Install metrics-server and, for D, Istio (`istioctl install --set profile=minimal -y`).
4. `python3 run/campaign.py --env baremetal --reps 8` (or `--env gke`), then
   `analysis/analyze.py --env baremetal`. The dashboard shows every analysed environment.

Autoscaling experiments: `--hpa 8` adds a CPU-based HorizontalPodAutoscaler (70%) per service.
Demand-proportional allocation: `--alloc run/alloc-prop.json` redeploys every decomposed configuration per
workload with CPU shares proportional to measured demand.
