SHELL := /bin/bash
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -s -w -X main.version=$(VERSION)
TEST_IMAGE_TAG ?= yougpu-agent:test

.PHONY: build build-linux build-test-image test vet lint clean run install-tools

build:
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/yougpu-agent ./cmd/yougpu-agent

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o bin/yougpu-agent-linux-amd64 ./cmd/yougpu-agent

# Сначала собираем linux-бинарь на хосте, потом запекаем в ubuntu:24.04 + rclone + fuse3.
# Используется backend'овским e2e harness'ом для testcontainers.
build-test-image: build-linux
	docker build -f Dockerfile.test -t $(TEST_IMAGE_TAG) .

test:
	go test ./...

vet:
	go vet ./...

lint: vet
	go test ./... -count=1 -race

run:
	go run ./cmd/yougpu-agent

clean:
	rm -rf bin/
