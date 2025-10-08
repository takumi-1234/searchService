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
	Qdrant        QdrantConfig        `mapstructure:"qdrant"`
	Kafka         KafkaConfig         `mapstructure:"kafka"`
	Search        SearchConfig        `mapstructure:"search"`
	Observability ObservabilityConfig `mapstructure:"observability"`
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

// KafkaConfig はKafkaの接続設定です。
type KafkaConfig struct {
	Brokers []string `mapstructure:"brokers"`
	GroupID string   `mapstructure:"groupId"`
	Topic   string   `mapstructure:"topic"`
}

// SearchConfig は検索全般の設定です。
type SearchConfig struct {
	Hybrid HybridConfig `mapstructure:"hybrid"`
}

// HybridConfig はハイブリッド検索に関する設定です。
type HybridConfig struct {
	Weights HybridWeightsConfig `mapstructure:"weights"`
}

// HybridWeightsConfig はキーワード検索とベクトル検索の重みを定義します。
type HybridWeightsConfig struct {
	Keyword float64 `mapstructure:"keyword"`
	Vector  float64 `mapstructure:"vector"`
}

// ObservabilityConfig はメトリクスおよびトレーシングの設定です。
type ObservabilityConfig struct {
	Metrics MetricsConfig `mapstructure:"metrics"`
	Tracing TracingConfig `mapstructure:"tracing"`
}

// MetricsConfig はメトリクスエンドポイントの設定です。
type MetricsConfig struct {
	Address string `mapstructure:"address"`
}

// TracingConfig はトレースエクスポーターの設定です。
type TracingConfig struct {
	Endpoint string `mapstructure:"endpoint"`
	Insecure bool   `mapstructure:"insecure"`
}

// Load は設定ファイルと環境変数から設定を読み込みます。
func Load(path string) (*Config, error) {
	v := viper.New()

	// デフォルト値の設定
	v.SetDefault("grpc.port", 50051)
	v.SetDefault("logger.level", "info")
	v.SetDefault("elasticsearch.username", "")
	v.SetDefault("elasticsearch.password", "")
	v.SetDefault("qdrant.address", "localhost:6334")
	v.SetDefault("qdrant.apiKey", "")
	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.groupId", "search-service-consumer")
	v.SetDefault("kafka.topic", "search.indexing.requests")
	v.SetDefault("search.hybrid.weights.keyword", 0.5)
	v.SetDefault("search.hybrid.weights.vector", 0.5)
	v.SetDefault("observability.metrics.address", ":9464")
	v.SetDefault("observability.tracing.endpoint", "localhost:4318")
	v.SetDefault("observability.tracing.insecure", true)

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
	if brokers := v.GetString("kafka.brokers"); brokers != "" {
		cfg.Kafka.Brokers = strings.Split(brokers, ",")
	}

	return &cfg, nil
}
