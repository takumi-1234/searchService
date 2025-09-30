# ==============================================================================
# Go variables
# ==============================================================================
BINARY_NAME=search-service
BINARY_PATH=./cmd/server/main.go
OUTPUT_DIR=./bin

# ==============================================================================
# Tools
# ==============================================================================
# .PHONY ディレクティブは、同名のファイルが存在してもターゲットを実行するようにします。
.PHONY: all init test test-unit test-integration lint fmt build run clean docker-build docker-up docker-down docker-logs

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

# test-integration: 統合テストを実行します
test-integration:
	@echo ">> Running integration tests..."
	go test -tags=integration ./tests/integration/...

# lint: golangci-lint を使って静的解析を実行します
lint:
	@echo ">> Running linter..."
	golangci-lint run

# fmt: goimports を使ってコードをフォーマットします
fmt:
	@echo ">> Formatting code..."
	goimports -w .

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