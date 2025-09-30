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
	"github.com/qdrant/go-client/qdrant"
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
		Image:        "docker.elastic.co/elasticsearch/elasticsearch:9.1.0",
		ExposedPorts: []string{"9200/tcp"},
		Env: map[string]string{
			"discovery.type":         "single-node",
			"xpack.security.enabled": "false",
		},
		WaitingFor: wait.ForHTTP("/").WithPort("9200").WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK }).WithStartupTimeout(120 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start elasticsearch container: %w", err)
	}

	cleanup := func() {
		if err := container.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate elasticsearch container: %v", err)
		}
	}

	endpoint, err := container.Endpoint(ctx, "http")
	if err != nil {
		return nil, cleanup, err
	}

	esClient, err := elasticsearch.NewTypedClient(elasticsearch.Config{Addresses: []string{endpoint}})
	if err != nil {
		return nil, cleanup, err
	}

	return esClient, cleanup, nil
}

func setupQdrant(ctx context.Context) (*grpc.ClientConn, func(), error) {
	req := testcontainers.ContainerRequest{
		Image:        "qdrant/qdrant:latest",
		ExposedPorts: []string{"6334/tcp"}, // gRPC port (Qdrant公式: 6334)
		WaitingFor:   wait.ForLog("Actix runtime found").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start qdrant container: %w", err)
	}

	cleanup := func() {
		if err := container.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate qdrant container: %v", err)
		}
	}

	port, err := container.MappedPort(ctx, "6334")
	if err != nil {
		return nil, cleanup, err
	}
	endpoint := fmt.Sprintf("localhost:%s", port.Port())
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, cleanup, err
	}
	return conn, cleanup, nil
}

func startTestGRPCServer(t *testing.T, esClient *elasticsearch.TypedClient, qdrantConn *grpc.ClientConn) (string, func()) {
	repo := repository.NewSearchRepository(esClient, qdrantConn)
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

	return lis.Addr().String(), func() { server.GracefulStop() }
}

func TestSearchDocuments_HybridSearch_Integration(t *testing.T) {
	ctx := context.Background()

	// 1. 外部サービスをセットアップ
	esClient, esCleanup, err := setupElasticsearch(ctx)
	require.NoError(t, err, "failed to setup elasticsearch")
	defer esCleanup()

	qdrantConn, qdrantCleanup, err := setupQdrant(ctx)
	require.NoError(t, err, "failed to setup qdrant")
	defer qdrantCleanup()

	// 2. テスト用gRPCサーバーを起動
	grpcAddr, grpcCleanup := startTestGRPCServer(t, esClient, qdrantConn)
	defer grpcCleanup()

	// 3. テストデータを準備
	indexName := "hybrid-test-index"
	vectorA := []float32{0.1, 0.2, 0.7}
	vectorB := []float32{0.8, 0.1, 0.1} // Doc B用
	vectorC := []float32{0.1, 0.2, 0.6} // Doc C用

	// UUID形式のIDを用意
	docAID := "11111111-1111-1111-1111-111111111111"
	docBID := "22222222-2222-2222-2222-222222222222"
	docCID := "33333333-3333-3333-3333-333333333333"

	// 3a. Elasticsearchにデータを投入
	type esDoc struct {
		Content string `json:"content"`
	}
	_, err = esClient.Index(indexName).Id(docAID).Request(esDoc{Content: "document for hybrid test"}).Do(ctx)
	require.NoError(t, err)
	_, err = esClient.Index(indexName).Id(docBID).Request(esDoc{Content: "document for search test"}).Do(ctx)
	require.NoError(t, err)
	_, err = esClient.Index(indexName).Id(docCID).Request(esDoc{Content: "document for hybrid test C"}).Refresh(refresh.Waitfor).Do(ctx)
	require.NoError(t, err)

	// 3b. Qdrantにデータを投入
	qdrantCollectionsClient := qdrant.NewCollectionsClient(qdrantConn)
	wait := true
	_, err = qdrantCollectionsClient.Create(ctx, &qdrant.CreateCollection{
		CollectionName: indexName,
		VectorsConfig: &qdrant.VectorsConfig{
			Config: &qdrant.VectorsConfig_Params{
				Params: &qdrant.VectorParams{
					Size:     3,
					Distance: qdrant.Distance_Cosine,
				},
			},
		},
	})
	// already existsエラーは許容
	if err != nil && !assert.Contains(t, err.Error(), "already exists") {
		require.NoError(t, err, "failed to create qdrant collection")
	}

	qdrantPointsClient := qdrant.NewPointsClient(qdrantConn)
	_, err = qdrantPointsClient.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: indexName,
		Wait:           &wait,
		Points: []*qdrant.PointStruct{
			{Id: &qdrant.PointId{PointIdOptions: &qdrant.PointId_Uuid{Uuid: docAID}}, Vectors: &qdrant.Vectors{VectorsOptions: &qdrant.Vectors_Vector{Vector: &qdrant.Vector{Data: vectorA}}}},
			{Id: &qdrant.PointId{PointIdOptions: &qdrant.PointId_Uuid{Uuid: docBID}}, Vectors: &qdrant.Vectors{VectorsOptions: &qdrant.Vectors_Vector{Vector: &qdrant.Vector{Data: vectorB}}}},
			{Id: &qdrant.PointId{PointIdOptions: &qdrant.PointId_Uuid{Uuid: docCID}}, Vectors: &qdrant.Vectors{VectorsOptions: &qdrant.Vectors_Vector{Vector: &qdrant.Vector{Data: vectorC}}}},
		},
	})
	require.NoError(t, err, "failed to upsert points to qdrant")

	// 4. gRPCクライアントを作成
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := searchv1.NewSearchServiceClient(conn)

	// 5. ハイブリッド検索を実行
	req := &searchv1.SearchDocumentsRequest{
		IndexName:   indexName,
		QueryText:   "hybrid search",
		QueryVector: vectorA,
	}
	res, err := client.SearchDocuments(ctx, req)

	// 6. レスポンスを検証
	require.NoError(t, err, "SearchDocuments RPC failed")
	require.NotNil(t, res)
	assert.Len(t, res.Results, 3, "should find three documents in total")

	// 各ドキュメントが存在することを確認 (順序は保証されない場合があるためIDで確認)
	foundIDs := make(map[string]bool)
	for _, r := range res.Results {
		foundIDs[r.DocumentId] = true
	}
	assert.True(t, foundIDs[docAID], "Doc A should be in the results")
	assert.True(t, foundIDs[docBID], "Doc B should be in the results")
	assert.True(t, foundIDs[docCID], "Doc C should be in the results")
}
