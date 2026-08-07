# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
# Multi-stage builds: https://docs.docker.com/build/building/multi-stage/
FROM golang:1.26.5-bookworm@sha256:6c5605ab3a9a9fb3c4eafe5b3d63cdbf3881caf113262b67862547b54a9db599 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY contract ./contract
COPY internal ./internal
COPY cmd ./cmd
RUN CGO_ENABLED=1 go build -trimpath -ldflags='-s -w' -o /out/go-bemu ./cmd/emulator

FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241
LABEL org.opencontainers.image.source="https://github.com/leeyh0216/go-bemu"
ARG BQEMU_CONFIG_SOURCE=configs/bqemu.yaml
RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates curl libgomp1 libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 65532 bqemu \
    && useradd --system --uid 65532 --gid bqemu --home-dir /nonexistent bqemu \
    && install -d -o bqemu -g bqemu /data /tmp/bqemu /etc/bqemu

COPY --from=build /out/go-bemu /usr/local/bin/go-bemu
COPY --chown=root:bqemu --chmod=0440 ${BQEMU_CONFIG_SOURCE} /etc/bqemu/bqemu.yaml

USER 65532:65532
EXPOSE 9050 9060 9051
VOLUME ["/data"]
ENV BQEMU_CONFIG=/etc/bqemu/bqemu.yaml \
    BQEMU_HEALTHCHECK_URL=http://127.0.0.1:9050/readyz \
    HOME=/tmp
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=5s --timeout=2s --retries=24 \
  CMD curl --fail --silent --show-error "${BQEMU_HEALTHCHECK_URL}" >/dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/go-bemu"]
