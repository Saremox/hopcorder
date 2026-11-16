GO      ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build test race vet lint rawsock clickhouse grafana up down ui ui-test e2e setcap clean

all: vet lint test build

build:
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o hopcorder ./cmd/hopcorder

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...
	$(GO) vet -tags rawsock,clickhouse,grafana ./...

lint:
	golangci-lint run
	golangci-lint run --build-tags rawsock,clickhouse,grafana

# Raw-socket tests. They skip themselves unless the test binary has CAP_NET_RAW;
# the netns test additionally needs CAP_SYS_ADMIN and iproute2.
rawsock:
	$(GO) test -tags rawsock -count=1 ./internal/probe/...

# Server tests against a real ClickHouse, with a password even on 127.0.0.1:
# any web page open in your browser can send SQL there, and ClickHouse lets
# it read the answer (Access-Control-Allow-Origin: *). E.g.
#   export HOPCORDER_CLICKHOUSE_PASSWORD=$(openssl rand -hex 16)
#   docker run -d --name hopcorder-clickhouse -p 127.0.0.1:8123:8123 \
#     -e CLICKHOUSE_PASSWORD="$HOPCORDER_CLICKHOUSE_PASSWORD" clickhouse/clickhouse-server:25.8
#   make clickhouse
# The tests read HOPCORDER_CLICKHOUSE_URL, _USER and _PASSWORD.
clickhouse: export HOPCORDER_CLICKHOUSE_URL ?= http://127.0.0.1:8123
clickhouse: export HOPCORDER_CLICKHOUSE_USER ?= default
clickhouse:
	$(GO) test -tags clickhouse -count=1 ./internal/server/... ./cmd/hopcorder/...

# The web UI (ui/). `make ui` rebuilds internal/ui/dist, which is committed
# so that the Go build needs no Node.js; commit it with the source change.
ui:
	cd ui && npm ci && npm run build

ui-test:
	cd ui && npm ci && npm run typecheck && npm test

# Browser tests against a running server (with ui/e2e/asn.tsv as its ASN
# database for the AS assertions), e.g. HOPCORDER_URL=http://127.0.0.1:8080/
e2e:
	cd ui && npx playwright test

# The docker-compose try-out stack (ClickHouse, server, agent, Grafana).
COMPOSE ?= docker compose -f deploy/docker-compose.yml
up:
	$(COMPOSE) up -d --build --wait
down:
	$(COMPOSE) down

# Every dashboard query through Grafana and the ClickHouse plugin, against
# the running stack (make up).
grafana:
	HOPCORDER_SERVER_URL=http://127.0.0.1:8080 HOPCORDER_GRAFANA_URL=http://127.0.0.1:3000 \
		$(GO) test -tags grafana -count=1 ./dashboards/

# Grant the built binary the one capability it needs (no root at runtime).
setcap: build
	sudo setcap cap_net_raw+ep ./hopcorder

clean:
	rm -f hopcorder
