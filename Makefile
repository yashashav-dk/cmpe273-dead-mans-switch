SHELL := /bin/bash
GOFLAGS := -trimpath
PROTO_FILES := proto/heartbeat.proto

.PHONY: all build proto test clean run-demo

all: build

build:
	go build $(GOFLAGS) -o bin/monitor ./cmd/monitor
	go build $(GOFLAGS) -o bin/worker  ./cmd/worker

proto:
	protoc \
	  --go_out=gen --go_opt=paths=source_relative \
	  --go-grpc_out=gen --go-grpc_opt=paths=source_relative \
	  --proto_path=proto $(PROTO_FILES)

test:
	go test ./...

clean:
	rm -rf bin gen/deadman *.jsonl *.csv

run-demo:
	bash scripts/run_demo.sh
