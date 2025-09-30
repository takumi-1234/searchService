package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	t.Run("正常系: ファイルから正しく値が読み込めること", func(t *testing.T) {
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
	})

	t.Run("正常系: 環境変数によって値が正しく上書きされること", func(t *testing.T) {
		configYAML := `
grpc:
  port: 8080
qdrant:
  address: "file-address"
`
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "config.yaml")
		err := os.WriteFile(configPath, []byte(configYAML), 0600)
		require.NoError(t, err)

		t.Setenv("SEARCH_GRPC_PORT", "9999")
		t.Setenv("SEARCH_ELASTICSEARCH_ADDRESSES", "http://es1:9200,http://es2:9200")
		t.Setenv("SEARCH_QDRANT_ADDRESS", "env-address:6333")
		t.Setenv("SEARCH_QDRANT_APIKEY", "env-key")

		cfg, err := Load(tempDir)
		require.NoError(t, err)

		assert.Equal(t, 9999, cfg.GRPC.Port)
		assert.Equal(t, []string{"http://es1:9200", "http://es2:9200"}, cfg.Elasticsearch.Addresses)
		assert.Equal(t, "env-address:6333", cfg.Qdrant.Address)
		assert.Equal(t, "env-key", cfg.Qdrant.APIKey)
	})

	t.Run("正常系: デフォルト値が適用されること", func(t *testing.T) {
		tempDir := t.TempDir()

		cfg, err := Load(tempDir)
		require.NoError(t, err)

		assert.Equal(t, 50051, cfg.GRPC.Port)
		assert.Equal(t, "info", cfg.Logger.Level)
		assert.Equal(t, "localhost:6333", cfg.Qdrant.Address)
		assert.Equal(t, "", cfg.Qdrant.APIKey)
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
