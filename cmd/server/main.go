package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os/signal"
	"strings"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/elastic/go-elasticsearch/v9"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	searchv1 "github.com/takumi-1234/searchService/gen/proto/search/v1"
	grpc_adapter "github.com/takumi-1234/searchService/internal/adapter/grpc"
	message_adapter "github.com/takumi-1234/searchService/internal/adapter/message"
	"github.com/takumi-1234/searchService/internal/config"
	"github.com/takumi-1234/searchService/internal/repository"
	"github.com/takumi-1234/searchService/internal/service"
)

func main() {
	cfg, err := config.Load("./config")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	loggerCfg := zap.NewProductionConfig()
	if err := loggerCfg.Level.UnmarshalText([]byte(cfg.Logger.Level)); err != nil {
		log.Printf("invalid logger level %q, falling back to info: %v", cfg.Logger.Level, err)
		loggerCfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	logger, err := loggerCfg.Build()
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}
	defer func() {
		if err := logger.Sync(); err != nil {
			log.Printf("failed to sync logger: %v", err)
		}
	}()

	logger.Info("starting search-service...")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Elasticsearchクライアントの初期化
	esCfg := elasticsearch.Config{
		Addresses: cfg.Elasticsearch.Addresses,
		Username:  cfg.Elasticsearch.Username,
		Password:  cfg.Elasticsearch.Password,
	}
	esTypedClient, err := elasticsearch.NewTypedClient(esCfg)
	if err != nil {
		logger.Fatal("failed to create elasticsearch client", zap.Error(err))
	}

	// Qdrantクライアントの初期化
	qdrantConn, err := grpc.NewClient(cfg.Qdrant.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("failed to connect to qdrant", zap.Error(err))
	}
	defer qdrantConn.Close()

	// 統合リポジトリのインスタンス化
	searchRepo := repository.NewSearchRepository(esTypedClient, qdrantConn)

	searchSvc := service.NewSearchService(searchRepo, logger)
	grpcServer := grpc_adapter.NewServer(searchSvc, logger)

	// Kafkaコンシューマの初期化
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.Kafka.Brokers, ","),
		"group.id":          cfg.Kafka.GroupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		logger.Fatal("failed to create kafka consumer", zap.Error(err))
	}

	msgConsumer := message_adapter.NewConsumer(consumer, searchSvc, logger, cfg.Kafka.Topic)

	port := cfg.GRPC.Port
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Fatal("failed to listen", zap.Int("port", port), zap.Error(err))
	}

	s := grpc.NewServer()
	searchv1.RegisterSearchServiceServer(s, grpcServer)
	reflection.Register(s)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		logger.Info("gRPC server listening", zap.String("address", lis.Addr().String()))
		if err := s.Serve(lis); err != nil {
			if errors.Is(err, grpc.ErrServerStopped) {
				return nil
			}
			return fmt.Errorf("failed to serve gRPC server: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		if err := msgConsumer.Run(gctx); err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("kafka consumer run failed: %w", err)
		}
		return nil
	})

	<-ctx.Done()

	logger.Info("shutting down gRPC server...")
	s.GracefulStop()
	logger.Info("waiting for background workers to stop...")
	if err := g.Wait(); err != nil {
		logger.Error("background worker exited with error", zap.Error(err))
	}
	logger.Info("server gracefully stopped")
}
