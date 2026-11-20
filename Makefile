.PHONY: fmt vet lint test bench bench-perf test-chaos proto vuln sec audit-logging check

SRC_DIRS := $(wildcard cmd internal pkg)

fmt:
	gofmt -s -w .

vet:
	go vet ./...

lint:
	golangci-lint run ./...

test:
	go test -v -race ./...

bench:
	go test -run=^$$ -bench=. -benchmem ./test/unit/...

bench-perf:
	go test -tags=performance -v -bench=. -benchmem ./test/performance/...

test-chaos:
	go test -tags=chaos -v -race ./test/chaos/...

proto:
	buf lint
	buf generate

vuln:
	govulncheck ./...

sec:
	gosec -exclude-generated ./...

audit-logging:
	go test -C .github/scripts -v ./...
ifneq ($(strip $(SRC_DIRS)),)
	go run -C .github/scripts . $(addprefix ../../,$(SRC_DIRS))
endif

check: fmt vet lint vuln sec audit-logging test
