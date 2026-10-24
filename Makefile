GOOS := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)
EXT := so
ifeq ($(GOOS),darwin)
EXT := dylib
endif
ifeq ($(GOOS),windows)
EXT := dll
endif
LIB := dist/$(GOOS)/$(GOARCH)/cpa-distill.$(EXT)
SQLC_VERSION := v1.31.1

.PHONY: build test generate smoke panel-test check
build:
	@mkdir -p $(dir $(LIB))
	CGO_ENABLED=1 go build -trimpath -buildmode=c-shared -o $(LIB) ./cmd/cpa-distill

test:
	go test -race ./...

generate:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION) generate

smoke: build
	python3 scripts/smoke.py $(LIB)

panel-test:
	node scripts/panel-smoke.cjs

check: test smoke panel-test
