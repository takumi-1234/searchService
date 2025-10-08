package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/elastic/go-elasticsearch/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
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
	"github.com/takumi-1234/searchService/pkg/observability"
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

	obsProvider, err := observability.Setup(ctx, observability.Config{
		ServiceName:     "search-service",
		TracingEndpoint: cfg.Observability.Tracing.Endpoint,
		TracingInsecure: cfg.Observability.Tracing.Insecure,
	})
	if err != nil {
		logger.Fatal("failed to setup observability", zap.Error(err))
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := obsProvider.Shutdown(shutdownCtx); err != nil {
			logger.Warn("failed to shutdown observability provider cleanly", zap.Error(err))
		}
	}()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", obsProvider.MetricsHandler())
	metricsServer := &http.Server{
		Addr:    cfg.Observability.Metrics.Address,
		Handler: metricsMux,
	}

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

	searchSvc := service.NewSearchService(searchRepo, logger, service.HybridWeights{
		Keyword: cfg.Search.Hybrid.Weights.Keyword,
		Vector:  cfg.Search.Hybrid.Weights.Vector,
	})
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

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.Kafka.Brokers, ","),
	})
	if err != nil {
		logger.Fatal("failed to create kafka producer", zap.Error(err))
	}

	msgConsumer := message_adapter.NewConsumer(consumer, producer, searchSvc, logger, cfg.Kafka.Topic)

	port := cfg.GRPC.Port
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Fatal("failed to listen", zap.Int("port", port), zap.Error(err))
	}

	meter := otel.Meter("github.com/takumi-1234/searchService/internal/adapter/grpc")
	metricsInterceptor, err := grpc_adapter.NewUnaryMetricsInterceptor(meter)
	if err != nil {
		logger.Fatal("failed to create metrics interceptor", zap.Error(err))
	}

	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			metricsInterceptor,
		),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
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

	g.Go(func() error {
		logger.Info("metrics server listening", zap.String("address", metricsServer.Addr))
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("metrics server failed: %w", err)
		}
		return nil
	})

	<-ctx.Done()

	logger.Info("shutting down gRPC server...")
	s.GracefulStop()
	logger.Info("shutting down metrics server...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := metricsServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Warn("failed to shutdown metrics server cleanly", zap.Error(err))
	}
	cancel()
	logger.Info("waiting for background workers to stop...")
	if err := g.Wait(); err != nil {
		logger.Error("background worker exited with error", zap.Error(err))
	}
	logger.Info("server gracefully stopped")
}
