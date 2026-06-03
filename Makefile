.PHONY: build test vet run clean

BIN_DIR := bin
MANAGER := $(BIN_DIR)/parley
RUNNER := $(BIN_DIR)/parley-runner

build:
	mkdir -p $(BIN_DIR)
	go build -o $(MANAGER) ./cmd/parley
	go build -o $(RUNNER) ./cmd/parley-runner

vet:
	go vet ./...

test:
	go test ./...

run: build
	PARLEY_RUNNER_BIN=$$(pwd)/$(RUNNER) ./$(MANAGER)

clean:
	rm -rf $(BIN_DIR) .parley-data
