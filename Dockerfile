FROM golang:1.23-bullseye AS builder

ENV GOTOOLCHAIN=go1.24.7

# ビルドに必要なツールをインストール（cgo/librdkafka 用のツールチェーンを含む）
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        curl \
        build-essential \
        pkg-config \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# buf CLIをインストール
RUN curl -sSL "https://github.com/bufbuild/buf/releases/latest/download/buf-Linux-x86_64" -o "/usr/local/bin/buf" && \
    chmod +x "/usr/local/bin/buf"

WORKDIR /app

# go.mod と go.sum を先にコピーして依存関係をキャッシュ
COPY go.mod go.sum ./
RUN go mod download

# proto定義とbuf設定ファイルをコピー
COPY buf.yaml buf.gen.yaml ./
# 修正点: `proto` ディレクトリは `api` ディレクトリ配下にあるため、`api` ディレクトリをコピーします。
COPY api ./api

# buf の依存関係をダウンロード
RUN buf dep update
# buf を使ってコードを生成
# 修正点: --include-imports は外部依存関係のファイルまで `gen/` に出力してしまうため、通常は不要です。
RUN buf generate

# 残りのソースコードをコピー
COPY . .

# アプリケーションをビルド（Kafka クライアントは cgo が必須のため有効化）
RUN CGO_ENABLED=1 GOOS=linux go build -o /search-service ./cmd/server

FROM debian:bullseye-slim

WORKDIR /

# 依存ライブラリをインストール
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /search-service /search-service

EXPOSE 50051

ENTRYPOINT ["/search-service"]
