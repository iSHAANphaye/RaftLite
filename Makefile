.PHONY: all proto build test lint clean

all: proto build test

proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/raftlite/v1/raft.proto proto/raftlite/v1/kv.proto

build:
	mkdir -p bin
	go build -o bin/raftd ./cmd/raftd
	go build -o bin/raftctl ./cmd/raftctl

test:
	go test -v -race ./...

lint:
	go vet ./...

clean:
	rm -rf bin/
