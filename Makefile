GO      ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build test race vet lint rawsock setcap clean

all: vet lint test build

build:
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o hopcorder ./cmd/hopcorder

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...
	$(GO) vet -tags rawsock ./...

lint:
	golangci-lint run
	golangci-lint run --build-tags rawsock

# Raw-socket tests. They skip themselves unless the test binary has CAP_NET_RAW;
# the netns test additionally needs CAP_SYS_ADMIN and iproute2.
rawsock:
	$(GO) test -tags rawsock -count=1 ./internal/probe/...

# Grant the built binary the one capability it needs (no root at runtime).
setcap: build
	sudo setcap cap_net_raw+ep ./hopcorder

clean:
	rm -f hopcorder
