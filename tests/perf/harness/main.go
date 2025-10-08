package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	searchv1 "github.com/takumi-1234/searchService/gen/proto/search/v1"
	grpcadapter "github.com/takumi-1234/searchService/internal/adapter/grpc"
	"github.com/takumi-1234/searchService/internal/service"
	"github.com/takumi-1234/searchService/tests/support/inmemory"
)

func main() {
	port := os.Getenv("PERF_GRPC_PORT")
	if port == "" {
		port = "50071"
	}

	repo := inmemory.NewRepository()
	if err := inmemory.SeedSampleData(repo); err != nil {
		log.Fatalf("failed to seed repository: %v", err)
	}

	logger := zap.NewNop()
	svc := service.NewSearchService(repo, logger, service.HybridWeights{
		Keyword: 0.6,
		Vector:  0.4,
	})
	server := grpcadapter.NewServer(svc, logger)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", port, err)
	}

	grpcServer := grpc.NewServer()
	searchv1.RegisterSearchServiceServer(grpcServer, server)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("perf harness gRPC server listening on %s", lis.Addr().String())
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server terminated: %v", err)
		}
	}()

	<-ctx.Done()

	grpcServer.GracefulStop()
	log.Println("perf harness gRPC server stopped")
}
