package config

import (
	"strings"

	"github.com/spf13/viper"
)

// Config はアプリケーション全体の設定を保持します。
type Config struct {
	GRPC          GRPCConfig          `mapstructure:"grpc"`
	Logger        LoggerConfig        `mapstructure:"logger"`
	Elasticsearch ElasticsearchConfig `mapstructure:"elasticsearch"`
	Qdrant        QdrantConfig        `mapstructure:"qdrant"` // Qdrant設定を追加
}

// GRPCConfig はgRPCサーバーの設定です。
type GRPCConfig struct {
	Port int `mapstructure:"port"`
}

// LoggerConfig はロガーの設定です。
type LoggerConfig struct {
	Level string `mapstructure:"level"`
}

// ElasticsearchConfig はElasticsearchの接続設定です。
type ElasticsearchConfig struct {
	Addresses []string `mapstructure:"addresses"`
	Username  string   `mapstructure:"username"`
	Password  string   `mapstructure:"password"`
}

// QdrantConfig はQdrantの接続設定です。
type QdrantConfig struct {
	Address string `mapstructure:"address"`
	APIKey  string `mapstructure:"apiKey"`
}

// Load は設定ファイルと環境変数から設定を読み込みます。
func Load(path string) (*Config, error) {
	v := viper.New()

	// デフォルト値の設定
	v.SetDefault("grpc.port", 50051)
	v.SetDefault("logger.level", "info")
	v.SetDefault("elasticsearch.username", "")
	v.SetDefault("elasticsearch.password", "")
	v.SetDefault("qdrant.address", "localhost:6333") // Qdrantのデフォルト値
	v.SetDefault("qdrant.apiKey", "")

	// 設定ファイルのパスと名前を設定
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(path)

	// 環境変数の設定
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.SetEnvPrefix("SEARCH")
	v.AutomaticEnv()

	// 設定ファイルの読み込み
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, err
		}
	}

	// 構造体へのアンマーシャル
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	// 環境変数から読み取ったカンマ区切りの文字列をスライスに手動で変換
	if addrs := v.GetString("elasticsearch.addresses"); addrs != "" {
		cfg.Elasticsearch.Addresses = strings.Split(addrs, ",")
	}

	return &cfg, nil
}
