BINARY := dbfabric

.PHONY: build run test tidy

build:
	go build -o bin/$(BINARY) ./cmd/dbfabric

run:
	go run ./cmd/dbfabric

test:
	go test ./...

tidy:
	go mod tidy
