package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	t.Run("正常系: ファイルから全ての値が読み込めること", func(t *testing.T) {
		configYAML := `
grpc:
  port: 8080
logger:
  level: "debug"
elasticsearch:
  addresses:
    - "http://localhost:9200"
qdrant:
  address: "localhost:1234"
  apiKey: "test-key"
kafka:
  brokers:
    - "kafka:9092"
  groupId: "test-group"
  topic: "test-topic"
search:
  hybrid:
    weights:
      keyword: 0.7
      vector: 0.3
`
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "config.yaml")
		err := os.WriteFile(configPath, []byte(configYAML), 0600)
		require.NoError(t, err)

		cfg, err := Load(tempDir)
		require.NoError(t, err)

		assert.Equal(t, 8080, cfg.GRPC.Port)
		assert.Equal(t, "debug", cfg.Logger.Level)
		assert.Equal(t, []string{"http://localhost:9200"}, cfg.Elasticsearch.Addresses)
		assert.Equal(t, "localhost:1234", cfg.Qdrant.Address)
		assert.Equal(t, "test-key", cfg.Qdrant.APIKey)
		assert.Equal(t, []string{"kafka:9092"}, cfg.Kafka.Brokers)
		assert.Equal(t, "test-group", cfg.Kafka.GroupID)
		assert.Equal(t, "test-topic", cfg.Kafka.Topic)
		assert.InDelta(t, 0.7, cfg.Search.Hybrid.Weights.Keyword, 1e-9)
		assert.InDelta(t, 0.3, cfg.Search.Hybrid.Weights.Vector, 1e-9)
	})

	t.Run("正常系: 環境変数によってKafkaの値が上書きされること", func(t *testing.T) {
		tempDir := t.TempDir()

		t.Setenv("SEARCH_KAFKA_BROKERS", "kafka1:9092,kafka2:9092")
		t.Setenv("SEARCH_KAFKA_GROUPID", "env-group")
		t.Setenv("SEARCH_KAFKA_TOPIC", "env-topic")

		cfg, err := Load(tempDir)
		require.NoError(t, err)

		assert.Equal(t, []string{"kafka1:9092", "kafka2:9092"}, cfg.Kafka.Brokers)
		assert.Equal(t, "env-group", cfg.Kafka.GroupID)
		assert.Equal(t, "env-topic", cfg.Kafka.Topic)
	})

	t.Run("正常系: デフォルト値が適用されること", func(t *testing.T) {
		tempDir := t.TempDir()

		cfg, err := Load(tempDir)
		require.NoError(t, err)

		assert.Equal(t, 50051, cfg.GRPC.Port)
		assert.Equal(t, "info", cfg.Logger.Level)
		assert.Equal(t, "localhost:6334", cfg.Qdrant.Address)
		assert.Equal(t, []string{"localhost:9092"}, cfg.Kafka.Brokers)
		assert.Equal(t, "search-service-consumer", cfg.Kafka.GroupID)
		assert.Equal(t, "search.indexing.requests", cfg.Kafka.Topic)
		assert.InDelta(t, 0.5, cfg.Search.Hybrid.Weights.Keyword, 1e-9)
		assert.InDelta(t, 0.5, cfg.Search.Hybrid.Weights.Vector, 1e-9)
	})

	t.Run("エラー系: 不正な形式のファイル", func(t *testing.T) {
		configYAML := `grpc: port: invalid`
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "config.yaml")
		err := os.WriteFile(configPath, []byte(configYAML), 0600)
		require.NoError(t, err)

		_, err = Load(tempDir)
		assert.Error(t, err)
	})
}
