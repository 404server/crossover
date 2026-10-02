#!/usr/bin/env python3
"""Kubernetes manifests for configurations A-D under an equal resource budget.

    gen.py A|B|C|D [--image crossover:1.0] [--hpa 8] | kubectl apply -f -
    gen.py loadgen | kubectl apply -f -

Every configuration gets the same total CPU for its application tier and for its database
tier, split equally across its services (requests = limits, Guaranteed QoS). Memory is split
the same way but never below a per-pod floor: a JVM service needs ~300 MiB RSS at idle, so an
equal split (512 MiB for eight services) is OOM-prone under overload. Memory is therefore a
measured cost, not an equalised input.
"""
import argparse
import json

MODULES = ["Auth", "Profiles", "Search", "Products", "Stock", "Reservations", "Carts", "Orders"]
FINE = {m.lower(): [m] for m in MODULES}
TOPOLOGY = {
    "A": {"mono": MODULES},
    "B": {"user": ["Auth", "Profiles"], "catalog": ["Search", "Products"],
          "inventory": ["Stock", "Reservations"], "order": ["Carts", "Orders"]},
    "C": FINE,
    "D": FINE,  # C plus Istio sidecars on every pod
}
EDGE = {"browse": "Search", "checkout": "Orders"}  # module that owns each edge operation
NS, LOAD_NS = "sut", "load"
PG = "postgres:17-alpine"


def host(cfg, module):
    return next(s for s, mods in TOPOLOGY[cfg].items() if module in mods)


def edge_url(cfg, workload):
    return f"http://{host(cfg, EDGE[workload])}.{NS}.svc.cluster.local:8080"


def res(cpu_m, mem_mi):
    r = {"cpu": f"{cpu_m}m", "memory": f"{mem_mi}Mi"}
    return {"requests": r, "limits": r}


def deployment(name, ns, tier, spec, annotations=None, role="sut"):
    labels = {"app": name, "tier": tier}
    meta = {"labels": labels, **({"annotations": annotations} if annotations else {})}
    return {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name, "namespace": ns, "labels": labels},
            "spec": {"replicas": 1, "selector": {"matchLabels": {"app": name}},
                     "template": {"metadata": meta, "spec": {"nodeSelector": {"crossover/role": role}, **spec}}}}


def service(name, port, protocol):
    return {"apiVersion": "v1", "kind": "Service", "metadata": {"name": name, "namespace": NS},
            "spec": {"selector": {"app": name}, "ports": [{"name": protocol, "port": port, "targetPort": port, "appProtocol": protocol}]}}


MESH = {  # sidecar resources are outside the application budget and reported separately as mesh cost
    "sidecar.istio.io/proxyCPU": "100m", "sidecar.istio.io/proxyCPULimit": "1000m",
    "sidecar.istio.io/proxyMemory": "64Mi", "sidecar.istio.io/proxyMemoryLimit": "256Mi",
    "proxy.istio.io/config": '{"holdApplicationUntilProxyStarts": true}',
}


def split(total, n, share=None, floor=0):
    """Equal split, or floor plus a share of what is left above the floors (shares sum to 1)."""
    return total // n if share is None else int(floor + (total - n * floor) * share)


def manifests(cfg, image, app_cpu, app_mem, db_cpu, db_mem, hpa=0, mem_floor=1024, shares=None, app_floor=150, db_floor=100,
              cpu_cap=None):
    """shares: {"app": {service: fraction}, "db": {service: fraction}} for demand-proportional CPU; None = equal split.
    cpu_cap: millicores the whole namespace may request (ResourceQuota). With hpa it is the shared elastic
    ceiling: the autoscaler may add replicas until the configuration's pods together hit the cap."""
    svcs = TOPOLOGY[cfg]
    n = len(svcs)
    share = lambda tier, svc: shares[tier].get(svc, 0.0) if shares else None
    mesh = MESH if cfg == "D" else None
    ns_labels = {"istio-injection": "enabled"} if cfg == "D" else {}
    items = [{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS, "labels": {"crossover/config": cfg, **ns_labels}}}]
    if cpu_cap:
        items.append({"apiVersion": "v1", "kind": "ResourceQuota", "metadata": {"name": "cpu-cap", "namespace": NS},
                      "spec": {"hard": {"requests.cpu": f"{cpu_cap}m"}}})
    for s, mods in svcs.items():
        env = [{"name": "SERVICE_MODULES", "value": ",".join(mods)},
               {"name": "DB_URL", "value": f"jdbc:postgresql://{s}-db:5432/crossover"}]
        env += [{"name": f"URL_{m}", "value": f"http://{host(cfg, m)}:8080"} for m in MODULES if m not in mods]
        items.append(deployment(s, NS, "app", {"containers": [{
            "name": "app", "image": image, "imagePullPolicy": "IfNotPresent", "env": env,
            "ports": [{"containerPort": 8080}], "resources": res(split(app_cpu, n, share("app", s), app_floor), max(app_mem // n, mem_floor)),
            "readinessProbe": {"httpGet": {"path": "/health", "port": 8080}, "periodSeconds": 2, "failureThreshold": 120}}]}, mesh))
        items.append(service(s, 8080, "http"))
        items.append(deployment(f"{s}-db", NS, "db", {
            "containers": [{"name": "postgres", "image": PG, "args": ["-c", "max_connections=200"],
                            "env": [{"name": k, "value": "crossover"} for k in ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")],
                            "ports": [{"containerPort": 5432}], "resources": res(split(db_cpu, n, share("db", s), db_floor), db_mem // n),
                            "volumeMounts": [{"name": "data", "mountPath": "/var/lib/postgresql/data"}],
                            "readinessProbe": {"exec": {"command": ["pg_isready", "-U", "crossover"]}, "periodSeconds": 2}}],
            "volumes": [{"name": "data", "emptyDir": {}}]}, mesh))
        items.append(service(f"{s}-db", 5432, "tcp"))
        if hpa:
            items.append({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                          "metadata": {"name": s, "namespace": NS},
                          "spec": {"scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": s},
                                   "minReplicas": 1, "maxReplicas": hpa,
                                   "metrics": [{"type": "Resource", "resource": {"name": "cpu",
                                                "target": {"type": "Utilization", "averageUtilization": 70}}}]}})
    return {"apiVersion": "v1", "kind": "List", "items": items}


def loadgen(image):
    return {"apiVersion": "v1", "kind": "List", "items": [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": LOAD_NS}},
        deployment("loadgen", LOAD_NS, "load", {"containers": [{
            "name": "vegeta", "image": image, "imagePullPolicy": "IfNotPresent",
            "resources": {"requests": {"cpu": "2", "memory": "1Gi"}}}]}, role="loadgen")]}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("config", choices=[*TOPOLOGY, "loadgen"])
    p.add_argument("--image", default="crossover:1.0")
    p.add_argument("--loadgen-image", default="crossover-loadgen:1.0")
    p.add_argument("--app-cpu", type=int, default=4000, help="millicores for the whole application tier")
    p.add_argument("--app-mem", type=int, default=4096, help="MiB for the whole application tier")
    p.add_argument("--db-cpu", type=int, default=2000)
    p.add_argument("--db-mem", type=int, default=2048)
    p.add_argument("--mem-floor", type=int, default=1024, help="minimum MiB per application pod")
    p.add_argument("--hpa", type=int, default=0, help="enable CPU-based HPA with this maxReplicas per service")
    p.add_argument("--cpu-cap", type=int, help="namespace CPU quota in millicores")
    a = p.parse_args()
    out = loadgen(a.loadgen_image) if a.config == "loadgen" else manifests(a.config, a.image, a.app_cpu, a.app_mem, a.db_cpu, a.db_mem, a.hpa, a.mem_floor, cpu_cap=a.cpu_cap)
    print(json.dumps(out, indent=1))
