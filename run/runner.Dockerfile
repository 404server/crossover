# The campaign controller as a container, so the whole cloud programme runs inside the cluster and does not
# depend on the laptop's network. Build for the Graviton nodes:
#   docker buildx build --platform linux/arm64 -t <registry>/crossover-runner:1.0 -f run/runner.Dockerfile run --push
FROM python:3.14-slim
ARG KUBECTL=v1.35.2
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/* \
 && curl -fsSLo /usr/local/bin/kubectl https://dl.k8s.io/release/${KUBECTL}/bin/linux/arm64/kubectl && chmod +x /usr/local/bin/kubectl \
 && pip install --no-cache-dir awscli
WORKDIR /app/run
COPY campaign.py gen.py alloc-prop.json cloud-run.sh ./
ENTRYPOINT ["bash", "cloud-run.sh"]
