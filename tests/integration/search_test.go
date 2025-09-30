//go:build integration

package integration

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/typedapi/types/enums/refresh"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	searchv1 "github.com/takumi-1234/searchService/gen/proto/search/v1"
	grpc_adapter "github.com/takumi-1234/searchService/internal/adapter/grpc"
	"github.com/takumi-1234/searchService/internal/repository"
	"github.com/takumi-1234/searchService/internal/service"
)

func setupElasticsearch(ctx context.Context) (*elasticsearch.TypedClient, func(), error) {
	req := testcontainers.ContainerRequest{
		Image:        "docker.elastic.co/elasticsearch/elasticsearch:9.1.4",
		ExposedPorts: []string{"9200/tcp"},
		Env: map[string]string{
			"discovery.type":         "single-node",
			"xpack.security.enabled": "false",
		},
		WaitingFor: wait.ForHTTP("/").
			WithPort("9200").
			WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK }).
			WithStartupTimeout(120 * time.Second),
	}

	esContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start elasticsearch container: %w", err)
	}

	cleanup := func() {
		if err := esContainer.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate elasticsearch container: %v", err)
		}
	}

	host, err := esContainer.Host(ctx)
	if err != nil {
		return nil, cleanup, err
	}
	port, err := esContainer.MappedPort(ctx, "9200")
	if err != nil {
		return nil, cleanup, err
	}
	esURL := fmt.Sprintf("http://%s:%s", host, port.Port())

	esCfg := elasticsearch.Config{
		Addresses: []string{esURL},
	}
	esClient, err := elasticsearch.NewTypedClient(esCfg)
	if err != nil {
		return nil, cleanup, err
	}

	return esClient, cleanup, nil
}

func startTestGRPCServer(t *testing.T, esClient *elasticsearch.TypedClient) (string, func()) {
	repo := repository.NewElasticsearchRepository(esClient)
	svc := service.NewSearchService(repo, zap.NewNop())
	grpcSrv := grpc_adapter.NewServer(svc, zap.NewNop())

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	searchv1.RegisterSearchServiceServer(server, grpcSrv)

	go func() {
		if err := server.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	cleanup := func() {
		server.GracefulStop()
	}

	return lis.Addr().String(), cleanup
}

func TestSearchDocuments_Integration(t *testing.T) {
	ctx := context.Background()

	esClient, esCleanup, err := setupElasticsearch(ctx)
	require.NoError(t, err, "failed to setup elasticsearch")
	defer esCleanup()

	grpcAddr, grpcCleanup := startTestGRPCServer(t, esClient)
	defer grpcCleanup()

	indexName := "test-index"
	docID := "test-doc-1"
	docContent := map[string]string{
		"content": "A test document for integration test",
		"title":   "Integration Test",
	}

	_, err = esClient.Index(indexName).
		Id(docID).
		Request(docContent).
		Refresh(refresh.Waitfor).
		Do(ctx)
	require.NoError(t, err, "failed to index test document")

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := searchv1.NewSearchServiceClient(conn)

	req := &searchv1.SearchDocumentsRequest{
		IndexName: indexName,
		QueryText: "integration test",
	}
	res, err := client.SearchDocuments(ctx, req)

	require.NoError(t, err, "SearchDocuments RPC failed")
	require.NotNil(t, res)
	assert.Equal(t, int64(1), res.TotalCount, "should find one document")
	require.Len(t, res.Results, 1, "results should contain one item")

	resultDoc := res.Results[0]
	assert.Equal(t, docID, resultDoc.DocumentId)

	fields := resultDoc.Fields.AsMap()
	assert.Equal(t, docContent["content"], fields["content"])
	assert.Equal(t, docContent["title"], fields["title"])
}
