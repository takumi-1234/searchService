# Repository Strategy and Layout

This service follows a polyrepo model: each deployable service is kept in its own repository. Shared utilities must be published as standalone Go modules and consumed via versioned tags; never import code directly from another service with `replace` directives.

## Directory Template

Every new component or generated artifact must live under one of the predefined roots. The tree below shows the canonical layout (omitting temporary files and build artifacts):

```
searchService/
├── api/                 # API contracts (OpenAPI/proto) and schemas
│   └── proto/           # Protobuf sources compiled by Buf
├── cmd/                 # Entrypoints (one package per binary)
│   └── server/main.go
├── config/              # Default configuration (override via env)
├── docs/                # Architecture and workflow documentation
├── gen/                 # Generated code (protoc, buf, etc.)
├── internal/            # Non-exported application packages
│   ├── adapter/         # gRPC, Kafka and other I/O adapters
│   ├── config/          # Runtime configuration loader
│   ├── port/            # Hexagonal ports (service/repository interfaces)
│   ├── repository/      # Data access implementations (ES, Qdrant, …)
│   └── service/         # Domain logic
├── tests/
│   ├── integration/     # Testcontainers-based integration tests
│   └── e2e/             # Reserved for end-to-end flows
├── deployments/         # Deployment manifests (Kubernetes, Helm, etc.)
├── scripts/             # Developer tooling and automation
├── .github/workflows/   # CI/CD pipelines
├── Dockerfile
├── docker-compose.yml
├── go.mod / go.sum
└── Makefile
```

## Guardrails

- Only files under `internal/` may depend on adapters or repositories; keep business logic behind `internal/service`.
- Generated code always lives under `gen/` and must never be edited manually. Regenerate via `buf generate` followed by `go generate` when applicable.
- New APIs require updates in **both** `api/proto` and the corresponding adapter/service packages before implementation work starts.
- Tests are mandatory for new features:
  - Unit tests in the closest package (e.g., `internal/service/..._test.go`).
  - Integration tests in `tests/integration` when an external system (Elasticsearch, Qdrant, Kafka) is involved.
- Automation defaults:
  - `make lint` runs `golangci-lint`.
  - `make test-unit` executes `go test -race ./...`.
  - `make test-integration` runs tagged integration suites (`testcontainers-go`).

Keep this template in sync with the CI pipeline and developer onboarding docs whenever the structure changes.
