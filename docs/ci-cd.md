# CI/CD Pipeline Reference

This project relies on a single source of truth for verification: the `make ci` target. Local developers and GitHub Actions share the exact same steps, which keeps feedback loops short and reproducible.

## Local Verification

- `make fmt-check`: Fails if any Go file (excluding `gen/`) is not formatted by `goimports`.
- `make proto-lint`: Runs `buf lint` against every protobuf definition below `api/`.
- `make lint`: Executes `golangci-lint run` with the default rule set.
- `make vet`: Runs `go vet ./...`.
- `make test-unit`: Executes unit tests with race detection and coverage.
- `make test-integration`: Spins up Elasticsearch, Qdrant, and Kafka via `testcontainers-go` and runs integration suites (requires Docker).
- `make ci`: Aggregates `fmt-check`, `proto-lint`, `lint`, `vet`, and `test-unit` in the same order as CI.
- `docker build -t search-service:local .`: Validates Docker image creation when testing locally.

### Tooling Requirements

Install the following once per development machine:

```bash
go install golang.org/x/tools/cmd/goimports@latest
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

Buf is provisioned in CI. Locally, install it with `brew install bufbuild/buf/buf` or by downloading from the official release page.

## GitHub Actions Workflow

File: `.github/workflows/ci.yaml`

1. Checkout repository and set up Go `1.24.7` with dependency caching.
2. Install the Buf CLI for protobuf linting.
3. Add `${HOME}/go/bin` to `PATH`, download Go modules, and install `goimports` plus `golangci-lint`.
4. Run `make ci`, which in turn performs:
   - `goimports` formatting check
   - `buf lint`
   - `golangci-lint run`
   - `go vet ./...`
   - `go test -race -cover ./...`
5. Execute `make test-integration` (with `TESTCONTAINERS_RYUK_DISABLED=true`) to verify the asynchronous Elasticsearch/Qdrant/Kafka flow.
6. On pushes to `main`, build the Docker image (`docker build -t search-service:${GITHUB_SHA} .`) to keep the delivery pipeline green.

Any failure in these stages blocks merges, ensuring that formatting, protobuf contracts, linting, tests, and container builds stay synchronized.
