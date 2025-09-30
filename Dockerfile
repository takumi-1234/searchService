FROM golang:1.24.7-alpine AS builder

# ビルドに必要なツールをインストール
RUN apk add --no-cache git curl

# buf CLIをインストール
RUN curl -sSL "https://github.com/bufbuild/buf/releases/latest/download/buf-Linux-x86_64" -o "/usr/local/bin/buf" && \
    chmod +x "/usr/local/bin/buf"

WORKDIR /app

# go.modとgo.sumを先にコピーして依存関係をキャッシュ
COPY go.mod ./
# go mod tidy で go.sum を生成し、依存関係をダウンロード
RUN go mod tidy
COPY go.sum ./

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

# アプリケーションをビルド
RUN CGO_ENABLED=0 GOOS=linux go build -o /search-service ./cmd/server

FROM alpine:latest

WORKDIR /

COPY --from=builder /search-service /search-service

EXPOSE 50051

ENTRYPOINT ["/search-service"]