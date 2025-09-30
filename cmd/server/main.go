package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os/signal"
	"syscall"

	"github.com/elastic/go-elasticsearch/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	searchv1 "github.com/takumi-1234/searchService/gen/proto/search/v1"
	grpc_adapter "github.com/takumi-1234/searchService/internal/adapter/grpc"
	"github.com/takumi-1234/searchService/internal/config"
	"github.com/takumi-1234/searchService/internal/repository"
	"github.com/takumi-1234/searchService/internal/service"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}
	defer func() {
		if err := logger.Sync(); err != nil {
			log.Printf("failed to sync logger: %v", err)
		}
	}()

	logger.Info("starting search-service...")

	cfg, err := config.Load("./config")
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
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

	searchSvc := service.NewSearchService(searchRepo, logger)
	grpcServer := grpc_adapter.NewServer(searchSvc, logger)

	port := cfg.GRPC.Port
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Fatal("failed to listen", zap.Int("port", port), zap.Error(err))
	}

	s := grpc.NewServer()
	searchv1.RegisterSearchServiceServer(s, grpcServer)
	reflection.Register(s)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("gRPC server listening", zap.String("address", lis.Addr().String()))
		if err := s.Serve(lis); err != nil {
			logger.Fatal("failed to serve gRPC server", zap.Error(err))
		}
	}()

	<-ctx.Done()

	logger.Info("shutting down gRPC server...")
	s.GracefulStop()
	logger.Info("server gracefully stopped")
}
