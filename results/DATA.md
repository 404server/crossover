# Raw results: monolith vs. microservices on Kubernetes

These are the raw measurements behind the article *Where Does Decomposition Pay Off? A Controlled,
Layer-Resolved Performance Comparison of Monolithic and Microservice Deployments on Kubernetes*.
The testbed, campaign runner and analysis pipeline are in `code.tar.gz`, a snapshot of
https://github.com/404server/crossover. Every number in the article can be regenerated from these files
with `analysis/analyze.py` (see the repository README).

## Files

| File | Contents |
|---|---|
| `raw-kind.tar` | local kind cluster, equal CPU budget, configs A–D, 5 repetitions |
| `raw-kind-prop.tar` | local kind cluster, demand-proportional CPU allocation, configs A–D, 5 repetitions |
| `raw-eks.tar` | Amazon EKS (Graviton c7g), equal CPU budget, configs A–D, 5 repetitions |
| `raw-eks-hpa.tar` | Amazon EKS, Horizontal Pod Autoscaler under a shared CPU quota, configs A–C, 3 repetitions |
| `raw-calib.tar` | calibration campaign with earlier settings, mentioned in the article; no reported number comes from it |
| `logs.tar.gz` | console logs of every campaign |
| `code.tar.gz` | the repository at the commit that produced and analysed the data |
| `SHA256SUMS` | checksums of all files above |

Unpack a tar into `results/raw/` of the repository to rerun the analysis, e.g.
`tar -xf raw-eks.tar -C results/raw && .venv/bin/python analysis/analyze.py --env eks`.

## Layout of one environment

A *cell* is one configuration in one repetition, with a fresh deployment:

    <env>/rep<r>/<cfg>/meta.json                  parameters, versions, nodes and pod placement (kubectl JSON)
    <env>/rep<r>/<cfg>/equiv/<workload>.json.gz   first 100 responses with bodies, for the equivalence check
    <env>/rep<r>/<cfg>/<workload>/open/<rate>.json.gz    open-loop step at <rate> requests/s
    <env>/rep<r>/<cfg>/<workload>/closed/<N>.json.gz     closed-loop step with N workers
    <env>/rep<r>/<cfg>/<workload>/ramp/<rate>.json.gz    autoscaling ramp level (eks-hpa only)
    <env>/rep<r>/<cfg>/cgroup.jsonl     per-step deltas of every container's cgroup v2 cpu.stat
    <env>/rep<r>/<cfg>/top.jsonl        kubectl top of every container after each step
    <env>/rep<r>/<cfg>/scaling.jsonl    replicas, autoscaler state and quota use every 10 s (eks-hpa only)
    <env>/rep<r>/<cfg>/incidents.jsonl  steps excluded from the analysis (pod restart or host sleep), if any
    <env>/rep<r>/<cfg>/done             marks a finished cell; cells without it are incomplete

`<workload>` is `browse` or `checkout`. `<cfg>` is A (monolith), B (4 services), C (8 services) or
D (8 services with Istio sidecars).

## Record format

Each `*.json.gz` step file holds Vegeta 12.13 results encoded as JSON, one request per line:
`seq`, `timestamp` (send time), `latency` (ns), `code`, `error`, `bytes_in`, `bytes_out`, `url` and
the response `headers`. The `Server-Timing` header carries the per-layer breakdown of the request in ms:
`app`, `db`, `ser`, `net`, `oth` and `tot` summed over the whole call tree, plus `calls`, the number of
remote calls. Bodies are kept only in the `equiv` files.

## Licence

Data: CC BY 4.0. Code: MIT (see `LICENSE` in `code.tar.gz`).
