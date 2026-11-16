# Two images from one build: `--target agent` and `--target server` (the
# default). Static binaries on scratch, cross-compiled for the target platform
# (docker buildx build --platform linux/amd64,linux/arm64 ...).
#
# No `# syntax=` line: the builder's own Dockerfile frontend is used, not an
# image pulled by a moving tag at every build (CI pins the BuildKit image).
# The Go image is pinned by digest (Dependabot updates it). Its tag names the
# Go release, which must equal go.mod's toolchain line (CI checks): release
# binaries are built with that toolchain, the images with this one.
FROM --platform=$BUILDPLATFORM golang:1.27.4@sha256:fe517acaa580cc7915457080f850c37e618b3c1e750f98b58fc6429b605147a1 AS build
WORKDIR /src
COPY . .
ARG TARGETOS TARGETARCH
ARG VERSION=""
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/hopcorder ./cmd/hopcorder \
 && mkdir -p /out/state/agent /out/state/server

FROM scratch AS base
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/hopcorder /hopcorder

# The agent needs CAP_NET_RAW, which container runtimes only grant to root
# (Kubernetes has no ambient capabilities): run it as uid 0 with every other
# capability dropped (deploy/docker-compose.yml, deploy/helm/hopcorder-agent).
FROM base AS agent
COPY --from=build /out/state/agent /var/lib/hopcorder
USER 0:0
ENTRYPOINT ["/hopcorder", "agent"]
CMD ["--config", "/etc/hopcorder/agent.yaml"]

FROM base AS server
COPY --from=build --chown=65532:65532 /out/state/server /var/lib/hopcorder-server
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/hopcorder", "server"]
CMD ["--listen", ":8080"]
