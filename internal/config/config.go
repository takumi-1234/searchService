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

// internal/config/config.go の Load 関数を再修正
func Load(path string) (*Config, error) {
	v := viper.New()

	v.SetDefault("grpc.port", 50051)
	v.SetDefault("logger.level", "info")
	v.SetDefault("elasticsearch.username", "") // デフォルトは空
	v.SetDefault("elasticsearch.password", "") // デフォルトは空

	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(path)

	// 環境変数の設定を先に行う
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.SetEnvPrefix("SEARCH")
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, err
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	// 環境変数から読み取ったカンマ区切りの文字列をスライスに手動で変換する
	if addrs := v.GetString("elasticsearch.addresses"); addrs != "" {
		cfg.Elasticsearch.Addresses = strings.Split(addrs, ",")
	}

	return &cfg, nil
}
