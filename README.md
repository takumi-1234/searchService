# searchService

searchService は、Elasticsearch のキーワード検索と Qdrant のベクトル検索を組み合わせたハイブリッド検索マイクロサービスです。gRPC API で検索リクエストを受け付け、Kafka からのインデックス更新イベントを取り込みながら、一貫した観測性と運用性を提供します。

## 概要

- gRPC/JSON (gRPC-Gateway) を介した検索 API (`api/proto/search/v1/search.proto`)
- Elasticsearch + Qdrant によるハイブリッドスコアリング (Keyword / Vector を重み付け統合)
- Kafka からの非同期インデックス更新 (Upsert/Delete) と DLQ 再送フロー
- OpenTelemetry を使ったトレース・メトリクス出力 (Prometheus, OTLP)
- Docker Compose によるローカル開発スタック (Elasticsearch, Qdrant, Kafka, Jaeger, Prometheus)

## アーキテクチャ概要

- **Entrypoint:** `cmd/server/main.go` が設定読込 → ロガー初期化 → gRPC サーバー起動 → Kafka コンシューマ → /metrics HTTP サーバーを同時起動します。
- **Adapters:** `internal/adapter/grpc` が gRPC ハンドラとメトリクスインターセプタを提供、`internal/adapter/message` が Kafka コンシューマ/プロデューサをラップします。
- **Domain & Repository:** `internal/service` がビジネスロジック (ハイブリッド検索・インデックス管理)、`internal/repository` が Elasticsearch / Qdrant との通信を担当します。
- **Configuration:** `config/config.yaml` をベースに `SEARCH_*` 環境変数で上書きできます。`internal/config` で読み込みを一元化しています。
- **Observability:** `pkg/observability` で OpenTelemetry トレーシングと Prometheus エクスポートを初期化。`deployments/prometheus/alerts.yml` にアラートルールが定義されています。

## リポジトリ構成

代表的なディレクトリは以下の通りです。詳細は `docs/repository-structure.md` を参照してください。

```
api/            Protobuf 定義とスキーマ
cmd/            バイナリエントリポイント (server)
config/         デフォルト設定ファイル
internal/       アプリケーションのドメイン・アダプタ実装
tests/          ユニット / インテグレーション / 負荷テスト
deployments/    Prometheus など運用マニフェスト
docs/           CI/CD・セキュリティ・運用ドキュメント
```

## 前提条件

- Go 1.24.x (toolchain `go1.24.7`)
- Docker & Docker Compose (ローカルスタック/テストで使用)
- Buf CLI (`buf lint` / `buf generate`)
- Optional: k6 (負荷テスト), grpcurl (API デバッグ)

### 初期セットアップ

開発ツールをインストールする場合は以下を実行します。

```sh
make init
```

## クイックスタート (Docker Compose)

1. 依存サービスとアプリケーションを起動します。
   ```sh
   make docker-up
   ```
2. gRPC サーバー: `localhost:50051`  
   Metrics: `http://localhost:9464/metrics`  
   Jaeger UI: `http://localhost:16686`
3. 停止する場合は以下を実行します。
   ```sh
   make docker-down
   ```

## ローカル開発

- バイナリのビルド: `make build` (出力先 `./bin/search-service`)
- ホスト上で実行: `make run` (既定で `config/config.yaml` を読み込み)
- 設定をカスタマイズする場合は `SEARCH_` プレフィックスの環境変数で上書きできます。
  - 例: `SEARCH_ELASTICSEARCH_ADDRESSES=http://localhost:9200`

### 設定項目

| 環境変数 | 説明 | 既定値 |
| --- | --- | --- |
| `SEARCH_GRPC_PORT` | gRPC リッスンポート | `50051` |
| `SEARCH_ELASTICSEARCH_ADDRESSES` | Elasticsearch エンドポイント (カンマ区切り) | `http://localhost:9200` |
| `SEARCH_QDRANT_ADDRESS` | Qdrant gRPC エンドポイント | `localhost:6334` |
| `SEARCH_KAFKA_BROKERS` | Kafka ブローカリスト | `localhost:9092` |
| `SEARCH_KAFKA_GROUPID` | Kafka コンシューマグループ | `search-service-consumer` |
| `SEARCH_KAFKA_TOPIC` | インデックス更新トピック | `search.indexing.requests` |
| `SEARCH_OBSERVABILITY_METRICS_ADDRESS` | Prometheus メトリクスエンドポイント | `:9464` |
| `SEARCH_OBSERVABILITY_TRACING_ENDPOINT` | OTLP HTTP エンドポイント | `localhost:4318` |

その他の詳細は `config/config.yaml` と `internal/config/config.go` を参照してください。

## API とメッセージフロー

- **gRPC / HTTP**: `api/proto/search/v1/search.proto`  
  - `SearchDocuments` (ハイブリッド検索)  
  - `CreateIndex` (Elasticsearch/Qdrant コレクションの作成)
- **Kafka**: `internal/adapter/message` が `SEARCH_KAFKA_TOPIC` を購読し、インデックス作成・削除をトリガーします。DLQ/リトライのポリシーは `internal/adapter/message` を参照してください。
- Protobuf の再生成: `buf generate` (設定は `buf.gen.yaml`)

## 観測性

- `/metrics`: Prometheus テキストフォーマット (`SEARCH_OBSERVABILITY_METRICS_ADDRESS`)
- OpenTelemetry トレース: OTLP/HTTP エクスポーターに送信 (`SEARCH_OBSERVABILITY_TRACING_ENDPOINT`)
- 代表的なメトリクス:
  - `grpc_server_request_duration_seconds` (レイテンシ)
  - `search_kafka_consumer_messages_total` (取り込み件数)
  - `search_index_ops_total` (Elasticsearch/Qdrant 操作)
- 監視ルールとダッシュボード: `deployments/prometheus/alerts.yml`、`deployments/prometheus/prometheus.yml`

## テストと品質ゲート

- ユニットテスト: `make test-unit`
- インテグレーションテスト (Testcontainers 使用): `make test-integration`
- E2E テスト (タグ `e2e`): `make test-e2e`
- 負荷テスト: `make test-load` (`k6` または `grafana/k6` コンテナを利用)
- CI バンドル: `make ci` (goimports チェック・Buf lint/breaking・golangci-lint・go vet・テスト一式)

GitHub Actions での運用フローは `docs/ci-cd.md`、運用準備とセキュリティ要件は `docs/operational-readiness.md` と `docs/security.md` を参照してください。

## 運用上の注意

- ログには PII やトークンを出力しないでください。マスキングポリシーは `docs/security.md` を参照。
- インデックススキーマを変更する場合は、Elasticsearch と Qdrant の両方で移行計画を立ててからデプロイします。
- SLO: P95 レイテンシ ≤ 500ms / エラー率 ≤ 0.1%。Prometheus アラートで監視してください。

## ライセンス

プロジェクト全体のライセンスが未設定の場合は、組織ポリシーに従って追加してください。
