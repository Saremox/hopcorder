# syntax=docker/dockerfile:1
# One static image for both commands: `agent` and `server` (the default).
# Used by deploy/docker-compose.yml; build with `docker build -t hopcorder .`.
FROM golang:1.27 AS build
WORKDIR /src
COPY . .
ARG VERSION=""
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/hopcorder ./cmd/hopcorder \
 && mkdir -p /out/state/agent /out/state/server

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/hopcorder /hopcorder
# The agent needs CAP_NET_RAW, which Docker only grants to root: run it as
# user 0 with every other capability dropped (see deploy/docker-compose.yml).
# Its spool directory is root's; the server's is the unprivileged user's.
COPY --from=build /out/state/agent /var/lib/hopcorder
COPY --from=build --chown=65532:65532 /out/state/server /var/lib/hopcorder-server
USER 65532:65532
ENTRYPOINT ["/hopcorder"]
CMD ["server", "--config", "/etc/hopcorder/server.yaml"]
