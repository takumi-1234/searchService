# ==============================================================================
# Go variables
# ==============================================================================
BINARY_NAME=search-service
BINARY_PATH=./cmd/server/main.go
OUTPUT_DIR=./bin
GO_SOURCES := $(shell find . -name '*.go' -not -path './gen/*' -not -path './bin/*' -not -path './.git/*')
K6_VUS ?= 10
K6_DURATION ?= 30s
GRPC_TARGET ?= 127.0.0.1:50071

# ==============================================================================
# Tools
# ==============================================================================
# .PHONY ディレクティブは、同名のファイルが存在してもターゲットを実行するようにします。
.PHONY: all init test test-unit test-integration test-e2e test-load lint fmt fmt-check vet proto-lint ci build run clean docker-build docker-up docker-down docker-logs

# デフォルトターゲット (make とだけ打った時に実行される)
all: build

# ==============================================================================
# Development
# ==============================================================================
# init: 開発に必要なツールをインストールします
init:
	@echo ">> Installing development tools..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
	go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# test: 全てのテスト（ユニット + インテグレーション）を実行します
test: test-unit test-integration

# test-unit: ユニットテストを実行します (レースコンディション検出とカバレッジレポート付き)
test-unit:
	@echo ">> Running unit tests..."
	go test -race -cover ./...

# test-e2e: E2Eテストを実行します (ビルドタグ e2e を利用)
test-e2e:
	@echo ">> Running end-to-end tests..."
	go test -tags=e2e ./tests/e2e/...

# test-integration: 統合テストを実行します
test-integration:
	@echo ">> Running integration tests..."
	go test -tags=integration ./tests/integration/...

# test-load: k6 を使った負荷テストを実行します
test-load:
	@echo ">> Running k6 load test..."
	@bash -c '\
		set -euo pipefail; \
		GRPC_TARGET_VALUE="$${GRPC_TARGET:-127.0.0.1:50071}"; \
		if ! echo "$$GRPC_TARGET_VALUE" | grep -q ":"; then \
			GRPC_TARGET_VALUE="$$GRPC_TARGET_VALUE:50071"; \
		fi; \
		GRPC_PORT="$${GRPC_TARGET_VALUE##*:}"; \
		case "$$GRPC_PORT" in \
			*[!0-9]*) \
				echo "Invalid GRPC_TARGET port in $$GRPC_TARGET_VALUE" >&2; \
				exit 1; \
				;; \
		esac; \
		if command -v k6 >/dev/null 2>&1; then \
			PERF_GRPC_PORT=$$GRPC_PORT go run ./tests/perf/harness >/tmp/search-service-perf.log 2>&1 & \
			HARNESS_PID=$$!; \
			trap "kill $$HARNESS_PID >/dev/null 2>&1 || true" EXIT INT TERM; \
			sleep 2; \
			K6_VUS=$(K6_VUS) K6_DURATION=$(K6_DURATION) GRPC_TARGET=$$GRPC_TARGET_VALUE k6 run tests/perf/k6/search.js; \
			elif command -v docker >/dev/null 2>&1; then \
				echo "k6 CLI not found. Falling back to grafana/k6 container image..."; \
				RUN_ID=$$(date +%s); \
				DOCKER_NET="search-service-perf-net-$$RUN_ID"; \
				HARNESS_CONTAINER="search-service-perf-harness-$$RUN_ID"; \
				mkdir -p .cache/gomod .cache/gocache; \
				cleanup() { \
					docker rm -f "$$HARNESS_CONTAINER" >/dev/null 2>&1 || true; \
					docker network rm "$$DOCKER_NET" >/dev/null 2>&1 || true; \
				}; \
				trap cleanup EXIT INT TERM; \
				docker network create "$$DOCKER_NET" >/dev/null; \
				HARNESS_ID=$$(docker run -d \
					--name "$$HARNESS_CONTAINER" \
					--network "$$DOCKER_NET" \
					-v "$(PWD)":/src \
					-v "$(PWD)/.cache/gomod":/go/pkg/mod \
					-v "$(PWD)/.cache/gocache":/root/.cache/go-build \
					-w /src \
					-e GOMODCACHE=/go/pkg/mod \
					-e PERF_GRPC_PORT=$$GRPC_PORT \
					golang:1 go run ./tests/perf/harness); \
				WAIT_SECONDS=0; \
				while [ $$WAIT_SECONDS -lt 90 ]; do \
					if [ "$$(docker inspect -f '{{.State.Running}}' "$$HARNESS_CONTAINER" 2>/dev/null)" != "true" ]; then \
						docker logs "$$HARNESS_CONTAINER" || true; \
						exit 1; \
					fi; \
					if docker logs "$$HARNESS_CONTAINER" 2>&1 | grep -q "perf harness gRPC server listening"; then \
						break; \
					fi; \
					sleep 1; \
					WAIT_SECONDS=$$((WAIT_SECONDS + 1)); \
				done; \
				if [ $$WAIT_SECONDS -ge 90 ]; then \
					echo "timed out waiting for harness to start" >&2; \
					docker logs "$$HARNESS_CONTAINER" || true; \
					exit 1; \
				fi; \
				docker run --rm \
					--network "$$DOCKER_NET" \
					-e K6_VUS=$(K6_VUS) \
					-e K6_DURATION=$(K6_DURATION) \
				-e GRPC_TARGET=$$HARNESS_CONTAINER:$$GRPC_PORT \
				-v "$(PWD)":/src \
				-w /src \
				grafana/k6:0.49.0 run tests/perf/k6/search.js; \
		else \
			echo "k6 CLI is missing and Docker is unavailable. Install k6 (https://grafana.com/docs/k6/latest/setup/)" >&2; \
			exit 1; \
		fi \
	'

# lint: golangci-lint を使って静的解析を実行します
lint:
	@echo ">> Running linter..."
	golangci-lint run

# fmt-check: goimports を使ってコードのフォーマット崩れを検出します
fmt-check:
	@echo ">> Checking goimports formatting..."
	@command -v goimports >/dev/null 2>&1 || { \
		echo "goimports is not installed. Run 'go install golang.org/x/tools/cmd/goimports@latest' first."; \
		exit 1; \
	}
	@UNFORMATTED=$$(goimports -l $(GO_SOURCES)); \
	if [ -n "$$UNFORMATTED" ]; then \
		echo "The following files are not properly formatted (run 'make fmt'):"; \
		echo "$$UNFORMATTED"; \
		exit 1; \
	fi

# fmt: goimports を使ってコードをフォーマットします
fmt:
	@echo ">> Formatting code..."
	@command -v goimports >/dev/null 2>&1 || { \
		echo "goimports is not installed. Run 'go install golang.org/x/tools/cmd/goimports@latest' first."; \
		exit 1; \
	}
	goimports -w .

# vet: go vet を実行して静的解析を行います
vet:
	@echo ">> Running go vet..."
	go vet ./...

# proto-lint: Buf を用いて protobuf の lint を実行します
proto-lint:
	@echo ">> Running buf lint..."
	buf lint

# ci: CIで実行する検証をまとめて実行します
ci:
	@echo ">> Running aggregated CI checks..."
	$(MAKE) fmt-check
	$(MAKE) proto-lint
	$(MAKE) lint
	$(MAKE) vet
	$(MAKE) test-unit
	$(MAKE) test-e2e

# ==============================================================================
# Build & Run
# ==============================================================================
# build: Goバイナリをビルドします
build:
	@echo ">> Building binary..."
	go build -o $(OUTPUT_DIR)/$(BINARY_NAME) $(BINARY_PATH)

# run: アプリケーションをローカルで実行します
run:
	@echo ">> Running application..."
	go run $(BINARY_PATH)

# clean: ビルドされたバイナリを削除します
clean:
	@echo ">> Cleaning up..."
	rm -f $(OUTPUT_DIR)/$(BINARY_NAME)

# ==============================================================================
# Docker
# ==============================================================================
# docker-build: Dockerイメージをビルドします
docker-build:
	@echo ">> Building Docker image..."
	docker-compose build

# docker-up: Dockerコンテナをバックグラウンドで起動します
docker-up:
	@echo ">> Starting Docker containers..."
	docker-compose up -d

# docker-down: Dockerコンテナを停止・削除します
docker-down:
	@echo ">> Stopping Docker containers..."
	docker-compose down

# docker-logs: サービスのログを表示します
docker-logs:
	@echo ">> Tailing service logs..."
	docker-compose logs -f search-service
