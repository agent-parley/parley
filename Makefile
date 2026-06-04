.PHONY: build test vet run prototype clean test-race test-integration test-live-m4 test-live-m5 test-live-m5-loop

BIN_DIR := bin
MANAGER := $(BIN_DIR)/parley
RUNNER := $(BIN_DIR)/parley-runner

# The module requires Go 1.26; default to auto so a host on an older toolchain
# fetches it instead of failing with GOTOOLCHAIN=local. This Makefile assignment
# wins over an inherited environment value; override with `make GOTOOLCHAIN=local`.
export GOTOOLCHAIN := auto

# Live/integration test config — override on the command line, e.g.
#   make test-live-m4 VALIDATION_NETWORK=none
VALIDATION_IMAGE   ?= docker.io/library/golang:1.26
VALIDATION_NETWORK ?= bridge
PI_AUTH_VOLUME     ?= pi-auth
PI_IMAGE           ?= localhost/parley-pi-worker:0.78.0
PI_NETWORK         ?= bridge
M5_CANCEL_IMAGE    ?= docker.io/library/alpine:3.20

build:
	mkdir -p $(BIN_DIR)
	go build -o $(MANAGER) ./cmd/parley
	go build -o $(RUNNER) ./cmd/parley-runner

vet:
	go vet ./...

test:
	go test ./...

# Race detector — needs a cgo-capable host (gcc).
test-race:
	CGO_ENABLED=1 go test -race ./internal/...

# All integration-tagged tests (guarded ones still self-skip without their env).
test-integration:
	go test -tags=integration ./...

# Full M4 idea->pr_ready loop vs a real Pi worker + sandboxed validation.
# Needs podman, the Pi worker image, and the $(PI_AUTH_VOLUME) volume.
test-live-m4:
	PARLEY_M4_LIVE=1 \
	PARLEY_VALIDATION_IMAGE=$(VALIDATION_IMAGE) \
	PARLEY_VALIDATION_NETWORK=$(VALIDATION_NETWORK) \
	PARLEY_PI_AUTH_JSON="$$(podman volume inspect $(PI_AUTH_VOLUME) --format '{{ .Mountpoint }}')/agent/auth.json" \
	go test -tags=integration ./internal/integration -run TestM4PiFullLoopLive -count=1 -v

# Guarded M5 lifecycle live tests: real container cancel + real runner-child kill.
# Needs podman (+ $(M5_CANCEL_IMAGE)); no Pi auth required.
test-live-m5: build
	PARLEY_M5_LIVE=1 \
	PARLEY_RUNNER_BIN=$$(pwd)/$(RUNNER) \
	PARLEY_M5_CANCEL_IMAGE=$(M5_CANCEL_IMAGE) \
	go test -tags=integration ./internal/manager/runnerclient ./internal/runner/provider -run TestM5Live -count=1 -v

# Guarded M5 end-to-end runner-death race: real manager + spawned runner + Pi container.
# Needs podman, the Pi worker image, and the $(PI_AUTH_VOLUME) volume.
test-live-m5-loop: build
	PARLEY_M5_LOOP_LIVE=1 \
	PARLEY_RUNNER_BIN=$$(pwd)/$(RUNNER) \
	PARLEY_PI_AUTH_JSON="$$(podman volume inspect $(PI_AUTH_VOLUME) --format '{{ .Mountpoint }}')/agent/auth.json" \
	PARLEY_PI_IMAGE=$(PI_IMAGE) \
	PARLEY_PI_NETWORK=$(PI_NETWORK) \
	go test -tags=integration ./internal/manager -run TestM5LivePiRunnerKillInFlight -count=1 -v

run: build
	PARLEY_RUNNER_BIN=$$(pwd)/$(RUNNER) ./$(MANAGER)

prototype:
	go run ./cmd/parley-prototype

clean:
	rm -rf $(BIN_DIR) .parley-data
