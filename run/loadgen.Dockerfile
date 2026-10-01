FROM alpine:3.22
ARG TARGETARCH
ARG VEGETA=12.13.0
RUN wget -qO- https://github.com/tsenart/vegeta/releases/download/v${VEGETA}/vegeta_${VEGETA}_linux_${TARGETARCH}.tar.gz \
    | tar -xz -C /usr/local/bin vegeta && mkdir /work
WORKDIR /work
CMD ["sleep", "infinity"]
