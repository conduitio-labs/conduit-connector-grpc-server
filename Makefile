VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-grpc-server.version=${VERSION}'" -o conduit-connector-grpc-server cmd/connector/main.go

.PHONY: test
test:
	go test $(GOTEST_FLAGS) -race ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: generate
generate:
	go generate ./...

.PHONY: proto-generate
proto-generate:
	cd proto && buf generate

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools/go.mod
	@go list -modfile=tools/go.mod tool | xargs -I % go list -modfile=tools/go.mod -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy

.PHONY: generate-certs
generate-certs:
	sh test/generate-certs.sh
