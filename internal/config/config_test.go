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
		// 1. 一時的な設定ファイルを作成
		configYAML := `
grpc:
  port: 8080
logger:
  level: "debug"
elasticsearch:
  addresses:
    - "http://localhost:9200"
`
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "config.yaml")
		err := os.WriteFile(configPath, []byte(configYAML), 0600)
		require.NoError(t, err)

		// 2. 設定を読み込む
		cfg, err := Load(tempDir)
		require.NoError(t, err)

		// 3. 値を検証
		assert.Equal(t, 8080, cfg.GRPC.Port)
		assert.Equal(t, "debug", cfg.Logger.Level)
		assert.Equal(t, []string{"http://localhost:9200"}, cfg.Elasticsearch.Addresses)
	})

	t.Run("正常系: 環境変数によって値が正しく上書きされること", func(t *testing.T) {
		// 1. 一時的な設定ファイルを作成
		configYAML := `
grpc:
  port: 8080
logger:
  level: "debug"
`
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "config.yaml")
		err := os.WriteFile(configPath, []byte(configYAML), 0600)
		require.NoError(t, err)

		// 2. 環境変数を設定
		t.Setenv("SEARCH_GRPC_PORT", "9999")
		t.Setenv("SEARCH_ELASTICSEARCH_ADDRESSES", "http://es1:9200,http://es2:9200")

		// 3. 設定を読み込む
		cfg, err := Load(tempDir)
		require.NoError(t, err)

		// 4. 値を検証
		assert.Equal(t, 9999, cfg.GRPC.Port)                                                         // 環境変数で上書き
		assert.Equal(t, "debug", cfg.Logger.Level)                                                   // ファイルの値
		assert.Equal(t, []string{"http://es1:9200", "http://es2:9200"}, cfg.Elasticsearch.Addresses) // 環境変数の値
	})

	t.Run("正常系: デフォルト値が適用されること", func(t *testing.T) {
		tempDir := t.TempDir() // 空のディレクトリ

		cfg, err := Load(tempDir)
		require.NoError(t, err)

		assert.Equal(t, 50051, cfg.GRPC.Port)
		assert.Equal(t, "info", cfg.Logger.Level)
	})

	t.Run("エラー系: 不正な形式のファイル", func(t *testing.T) {
		configYAML := `grpc: port: 8080` // 不正なYAML
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "config.yaml")
		err := os.WriteFile(configPath, []byte(configYAML), 0600)
		require.NoError(t, err)

		_, err = Load(tempDir)
		assert.Error(t, err)
	})
}
