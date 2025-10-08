//go:build e2e

package e2e

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"

	searchv1 "github.com/takumi-1234/searchService/gen/proto/search/v1"
	grpcadapter "github.com/takumi-1234/searchService/internal/adapter/grpc"
	"github.com/takumi-1234/searchService/internal/service"
	"github.com/takumi-1234/searchService/tests/support/inmemory"
)

func TestSearchDocumentsEndToEnd(t *testing.T) {
	t.Parallel()

	repo := inmemory.NewRepository()
	require.NoError(t, inmemory.SeedSampleData(repo))

	logger := zap.NewNop()
	svc := service.NewSearchService(repo, logger, service.HybridWeights{
		Keyword: 0.6,
		Vector:  0.4,
	})
	server := grpcadapter.NewServer(svc, logger)

	conn, stop := startGRPCServer(t, server)
	defer stop()

	client := searchv1.NewSearchServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	firstResp, err := client.SearchDocuments(ctx, &searchv1.SearchDocumentsRequest{
		IndexName: "books",
		QueryText: "calculus",
		PageSize:  1,
		Filters: []*searchv1.Filter{
			{
				Field:    "category",
				Operator: searchv1.Filter_OPERATOR_EQUAL,
				Value:    structpb.NewStringValue("math"),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, firstResp.GetResults(), 1)
	assert.Equal(t, "book-1", firstResp.GetResults()[0].GetDocumentId())
	assert.NotEmpty(t, firstResp.GetNextPageToken())

	secondResp, err := client.SearchDocuments(ctx, &searchv1.SearchDocumentsRequest{
		IndexName:   "books",
		QueryText:   "linear",
		PageSize:    1,
		PageToken:   firstResp.GetNextPageToken(),
		QueryVector: []float32{0.5, 0.2, 0.1},
		Filters: []*searchv1.Filter{
			{
				Field:    "category",
				Operator: searchv1.Filter_OPERATOR_EQUAL,
				Value:    structpb.NewStringValue("math"),
			},
		},
		SortBy: &searchv1.SortBy{
			Field: "ranking",
			Order: searchv1.SortBy_ORDER_DESC,
		},
	})
	require.NoError(t, err)
	require.Len(t, secondResp.GetResults(), 1)
	assert.Equal(t, "book-2", secondResp.GetResults()[0].GetDocumentId())
	assert.Empty(t, secondResp.GetNextPageToken())
	assert.GreaterOrEqual(t, secondResp.GetTotalCount(), int64(1))
}

const bufconnSize = 1024 * 1024

func startGRPCServer(t *testing.T, server searchv1.SearchServiceServer) (*grpc.ClientConn, func()) {
	t.Helper()

	lis := bufconn.Listen(bufconnSize)

	grpcServer := grpc.NewServer()
	searchv1.RegisterSearchServiceServer(grpcServer, server)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Logf("gRPC server stopped unexpectedly: %v", err)
		}
	}()

	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	return conn, func() {
		_ = conn.Close()
		grpcServer.GracefulStop()
	}
}
