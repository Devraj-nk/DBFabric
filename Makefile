BINARY := dbfabric

.PHONY: build run test chaos tidy

build:
	go build -o bin/$(BINARY) ./cmd/dbfabric

run:
	go run ./cmd/dbfabric

# Hermetic: needs no PostgreSQL.
test:
	go test ./...

# Failure-injection harness against a real primary + streaming replica.
# Needs PostgreSQL server binaries (initdb, pg_ctl, pg_basebackup): set
# PG_BIN to their directory if they aren't on PATH. Takes a few minutes.
chaos:
	go test -tags chaos -count=1 -v -timeout 15m ./internal/chaos

tidy:
	go mod tidy
