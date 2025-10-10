//go:build integration

package integration

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/typedapi/types/enums/refresh"
	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	searchv1 "github.com/takumi-1234/searchService/gen/proto/search/v1"
	grpc_adapter "github.com/takumi-1234/searchService/internal/adapter/grpc"
	message_adapter "github.com/takumi-1234/searchService/internal/adapter/message"
	"github.com/takumi-1234/searchService/internal/port"
	"github.com/takumi-1234/searchService/internal/repository"
	"github.com/takumi-1234/searchService/internal/service"
)

// setupElasticsearch は Elasticsearch の testcontainer をセットアップします。
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
		return nil, cleanup, fmt.Errorf("failed to get elasticsearch endpoint: %w", err)
	}

	esClient, err := elasticsearch.NewTypedClient(elasticsearch.Config{Addresses: []string{endpoint}})
	if err != nil {
		return nil, cleanup, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	return esClient, cleanup, nil
}

// setupQdrant は Qdrant の testcontainer をセットアップします。
func setupQdrant(ctx context.Context) (*grpc.ClientConn, func(), error) {
	req := testcontainers.ContainerRequest{
		Image:        "qdrant/qdrant:latest",
		ExposedPorts: []string{"6334/tcp"}, // Qdrant の gRPC ポート
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

	// gRPCエンドポイントを取得
	port, err := container.MappedPort(ctx, "6334")
	if err != nil {
		return nil, cleanup, fmt.Errorf("failed to get qdrant mapped port: %w", err)
	}
	endpoint := fmt.Sprintf("localhost:%s", port.Port())

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, cleanup, fmt.Errorf("failed to connect to qdrant: %w", err)
	}
	return conn, cleanup, nil
}

// getFreePort はローカルホスト上の空きポートを取得します。
func getFreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()

	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("failed to assert listener address as *net.TCPAddr")
	}
	return addr.Port, nil
}

// setupKafka は KRaft モードの Kafka testcontainer をセットアップします。
func setupKafka(ctx context.Context) (string, func(), error) {
	const (
		containerKafkaPort       = "9092/tcp"
		containerControllerPort  = "9093/tcp"
		controllerListenerName   = "CONTROLLER"
		controllerListenerConfig = "CONTROLLER://0.0.0.0:9093"
		kafkaReadyLog            = "Kafka Server started"
	)

	clusterUUID := uuid.New()
	clusterIDBase64 := base64.RawStdEncoding.EncodeToString(clusterUUID[:])

	kafkaHostPort, err := getFreePort()
	if err != nil {
		return "", nil, fmt.Errorf("failed to get free port for kafka: %w", err)
	}

	kafkaReq := testcontainers.ContainerRequest{
		Image: "confluentinc/cp-kafka:7.6.1",
		Env: map[string]string{
			"KAFKA_ENABLE_KRAFT":                             "true",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"CLUSTER_ID":                                     clusterIDBase64,
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_LISTENERS":                                fmt.Sprintf("PLAINTEXT://0.0.0.0:9092,%s", controllerListenerConfig),
			"KAFKA_ADVERTISED_LISTENERS":                     fmt.Sprintf("PLAINTEXT://127.0.0.1:%d", kafkaHostPort),
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                controllerListenerName,
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_LOG_DIRS":                                 "/tmp/kraft-combined-logs",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "true",
			"KAFKA_HEAP_OPTS":                                "-Xms256m -Xmx256m",
			"KAFKA_ALLOW_PLAINTEXT_LISTENER":                 "yes",
		},
		ExposedPorts: []string{containerKafkaPort, containerControllerPort},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(containerKafkaPort).WithStartupTimeout(300*time.Second),
			wait.ForLog(kafkaReadyLog).WithStartupTimeout(300*time.Second),
		),
		HostConfigModifier: func(hc *container.HostConfig) {
			if hc.PortBindings == nil {
				hc.PortBindings = nat.PortMap{}
			}
			clientPort := nat.Port(containerKafkaPort)
			hc.PortBindings[clientPort] = []nat.PortBinding{
				{
					HostIP:   "127.0.0.1",
					HostPort: strconv.Itoa(kafkaHostPort),
				},
			}
			controllerPort := nat.Port(containerControllerPort)
			if _, ok := hc.PortBindings[controllerPort]; !ok {
				hc.PortBindings[controllerPort] = []nat.PortBinding{
					{
						HostIP:   "0.0.0.0",
						HostPort: "0",
					},
				}
			}
		},
	}

	kafkaContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: kafkaReq, Started: true})
	if err != nil {
		return "", nil, fmt.Errorf("failed to start kafka container: %w", err)
	}

	cleanup := func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate kafka container: %v", err)
		}
	}

	return fmt.Sprintf("127.0.0.1:%d", kafkaHostPort), cleanup, nil
}

func ensureKafkaTopic(ctx context.Context, bootstrapServers, topic string) error {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return fmt.Errorf("failed to create kafka admin client: %w", err)
	}
	defer admin.Close()

	spec := kafka.TopicSpecification{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{spec})
	if err != nil {
		return fmt.Errorf("failed to request topic creation: %w", err)
	}
	for _, res := range results {
		if res.Error.Code() != kafka.ErrNoError && res.Error.Code() != kafka.ErrTopicAlreadyExists {
			return fmt.Errorf("topic creation failed: %v", res.Error)
		}
	}

	metadataProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return fmt.Errorf("failed to create metadata producer: %w", err)
	}
	defer metadataProducer.Close()

	deadline := time.Now().Add(30 * time.Second)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for topic %q metadata", topic)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled while waiting for topic metadata: %w", ctx.Err())
		default:
		}

		metadata, err := metadataProducer.GetMetadata(&topic, false, 5000)
		if err == nil {
			if topicMetadata, ok := metadata.Topics[topic]; ok && topicMetadata.Error.Code() == kafka.ErrNoError && len(topicMetadata.Partitions) > 0 {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// startTestGRPCServer はテスト用のgRPCサーバーを起動します。
func startTestGRPCServer(t *testing.T, esClient *elasticsearch.TypedClient, qdrantConn *grpc.ClientConn) (string, func()) {
	t.Helper()
	repo := repository.NewSearchRepository(esClient, qdrantConn)
	svc := service.NewSearchService(repo, zap.NewNop(), service.HybridWeights{
		Keyword: 0.5,
		Vector:  0.5,
	})
	grpcSrv := grpc_adapter.NewServer(svc, zap.NewNop())

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	searchv1.RegisterSearchServiceServer(server, grpcSrv)

	go func() {
		if err := server.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Printf("gRPC server error during test: %v", err)
		}
	}()

	return lis.Addr().String(), func() { server.GracefulStop() }
}

func TestCreateIndex_Integration(t *testing.T) {
	ctx := context.Background()

	esClient, esCleanup, err := setupElasticsearch(ctx)
	require.NoError(t, err, "failed to setup elasticsearch")
	defer esCleanup()

	qdrantConn, qdrantCleanup, err := setupQdrant(ctx)
	require.NoError(t, err, "failed to setup qdrant")
	defer qdrantCleanup()

	grpcAddr, grpcCleanup := startTestGRPCServer(t, esClient, qdrantConn)
	defer grpcCleanup()

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := searchv1.NewSearchServiceClient(conn)

	indexName := fmt.Sprintf("create-index-test-%d", time.Now().UnixNano())
	req := &searchv1.CreateIndexRequest{
		IndexName: indexName,
		Schema: &searchv1.IndexSchema{
			Fields: []*searchv1.FieldDefinition{
				{Name: "content", Type: searchv1.FieldDefinition_FIELD_TYPE_TEXT},
				{Name: "category", Type: searchv1.FieldDefinition_FIELD_TYPE_KEYWORD},
			},
			VectorConfig: &searchv1.VectorConfig{
				Dimension: 3,
				Distance:  searchv1.VectorConfig_DISTANCE_COSINE,
			},
		},
	}

	res, err := client.CreateIndex(ctx, req)
	require.NoError(t, err, "CreateIndex RPC failed")
	require.NotNil(t, res)
	assert.True(t, res.GetSuccess())

	exists, err := esClient.Indices.Exists(indexName).Do(ctx)
	require.NoError(t, err, "failed to check elasticsearch index existence")
	assert.True(t, exists, "elasticsearch index should exist after CreateIndex")

	collectionsClient := qdrant.NewCollectionsClient(qdrantConn)
	info, err := collectionsClient.Get(ctx, &qdrant.GetCollectionInfoRequest{CollectionName: indexName})
	require.NoError(t, err, "failed to get qdrant collection info")
	require.NotNil(t, info.GetResult())

	collectionConfig := info.GetResult().GetConfig()
	require.NotNil(t, collectionConfig, "collection config must not be nil")

	params := collectionConfig.GetParams()
	require.NotNil(t, params, "collection params must not be nil")

	vectorsConfig := params.GetVectorsConfig()
	require.NotNil(t, vectorsConfig, "vectors config must not be nil")

	vectorParams := vectorsConfig.GetParams()
	require.NotNil(t, vectorParams, "vector params must not be nil")
	assert.EqualValues(t, 3, vectorParams.GetSize())
	assert.Equal(t, qdrant.Distance_Cosine, vectorParams.GetDistance())
}

// TestSearchDocuments_HybridSearch_Integration はハイブリッド検索の統合テストです。
func TestSearchDocuments_HybridSearch_Integration(t *testing.T) {
	ctx := context.Background()

	// 1. 外部サービス(Elasticsearch, Qdrant)をセットアップ
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
	indexName := "hybrid-search-test-index"
	vectorA := []float32{0.1, 0.2, 0.8} // DocAのベクトル
	vectorC := []float32{0.1, 0.2, 0.7} // DocCのベクトル (Aに近い)

	docAID := "11111111-1111-1111-1111-111111111111"
	docBID := "22222222-2222-2222-2222-222222222222"
	docCID := "33333333-3333-3333-3333-333333333333"

	// 4. データを各DBに投入
	// 4a. Elasticsearch: DocAとDocBを投入
	type esDoc struct {
		Content string `json:"content"`
	}
	_, err = esClient.Index(indexName).Id(docAID).Request(esDoc{Content: "This is a document about hybrid."}).Do(ctx)
	require.NoError(t, err)
	_, err = esClient.Index(indexName).Id(docBID).Request(esDoc{Content: "A document focused on search."}).Refresh(refresh.Waitfor).Do(ctx)
	require.NoError(t, err)

	// 4b. Qdrant: DocAとDocCを投入
	qCollectionsClient := qdrant.NewCollectionsClient(qdrantConn)
	_, err = qCollectionsClient.Create(ctx, &qdrant.CreateCollection{
		CollectionName: indexName,
		VectorsConfig: &qdrant.VectorsConfig{Config: &qdrant.VectorsConfig_Params{
			Params: &qdrant.VectorParams{Size: 3, Distance: qdrant.Distance_Cosine},
		}},
	})
	if err != nil && !assert.Contains(t, err.Error(), "already exists") {
		require.NoError(t, err, "failed to create qdrant collection")
	}

	qPointsClient := qdrant.NewPointsClient(qdrantConn)
	waitUpsert := true
	_, err = qPointsClient.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: indexName,
		Wait:           &waitUpsert,
		Points: []*qdrant.PointStruct{
			{Id: &qdrant.PointId{PointIdOptions: &qdrant.PointId_Uuid{Uuid: docAID}}, Vectors: &qdrant.Vectors{VectorsOptions: &qdrant.Vectors_Vector{Vector: &qdrant.Vector{Data: vectorA}}}},
			{Id: &qdrant.PointId{PointIdOptions: &qdrant.PointId_Uuid{Uuid: docCID}}, Vectors: &qdrant.Vectors{VectorsOptions: &qdrant.Vectors_Vector{Vector: &qdrant.Vector{Data: vectorC}}}},
		},
	})
	require.NoError(t, err, "failed to upsert points to qdrant")

	// 5. gRPCクライアントを作成してハイブリッド検索を実行
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := searchv1.NewSearchServiceClient(conn)

	req := &searchv1.SearchDocumentsRequest{
		IndexName:   indexName,
		QueryText:   "hybrid search", // "hybrid"はDocAに、"search"はDocBにマッチするはず
		QueryVector: vectorA,         // DocAとDocCにマッチするはず
	}
	res, err := client.SearchDocuments(ctx, req)

	// 6. レスポンスを検証
	require.NoError(t, err, "SearchDocuments RPC failed")
	require.NotNil(t, res)
	require.Len(t, res.Results, 3, "should find three documents in total (A, B, C)")

	// DocAがキーワードとベクトルの両方でヒットするため、スコアが最も高くなるはず
	assert.Equal(t, docAID, res.Results[0].DocumentId, "Doc A should have the highest score and be the first result")

	// 残りのドキュメントのスコアより高いことを確認
	assert.True(t, res.Results[0].Score > res.Results[1].Score, "Doc A score should be greater than the second result")
	assert.True(t, res.Results[0].Score > res.Results[2].Score, "Doc A score should be greater than the third result")

	// DocBとDocCが結果に含まれていることを確認（順序は問わない）
	foundIDs := make(map[string]bool)
	for _, r := range res.Results {
		foundIDs[r.DocumentId] = true
	}
	assert.True(t, foundIDs[docBID], "Doc B should be in the results")
	assert.True(t, foundIDs[docCID], "Doc C should be in the results")
}

func TestSearchDocuments_FilterSortPageSize_Integration(t *testing.T) {
	ctx := context.Background()

	esClient, esCleanup, err := setupElasticsearch(ctx)
	require.NoError(t, err, "failed to setup elasticsearch")
	defer esCleanup()

	qdrantConn, qdrantCleanup, err := setupQdrant(ctx)
	require.NoError(t, err, "failed to setup qdrant")
	defer qdrantCleanup()

	grpcAddr, grpcCleanup := startTestGRPCServer(t, esClient, qdrantConn)
	defer grpcCleanup()

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := searchv1.NewSearchServiceClient(conn)

	indexName := fmt.Sprintf("filter-sort-test-%d", time.Now().UnixNano())
	queryVector := []float32{0.4, 0.3, 0.5}

	const (
		filterDocAID = "11111111-1111-1111-1111-111111111110"
		filterDocBID = "11111111-1111-1111-1111-111111111111"
		filterDocEID = "22222222-2222-2222-2222-222222222222"
		filterDocCID = "33333333-3333-3333-3333-333333333333"
		filterDocDID = "44444444-4444-4444-4444-444444444444"
	)

	type testDoc struct {
		ID       string
		Category string
		Score    float64
		Content  string
		Vector   []float32
	}

	docs := []testDoc{
		{
			ID:       filterDocAID,
			Category: "math",
			Score:    95,
			Content:  "advanced math concepts",
			Vector:   []float32{0.4, 0.3, 0.5},
		},
		{
			ID:       filterDocBID,
			Category: "math",
			Score:    85,
			Content:  "introduction to math analysis",
			Vector:   []float32{0.35, 0.28, 0.52},
		},
		{
			ID:       filterDocEID,
			Category: "math",
			Score:    78,
			Content:  "math practice problems",
			Vector:   []float32{0.31, 0.3, 0.45},
		},
		{
			ID:       filterDocCID,
			Category: "history",
			Score:    92,
			Content:  "world history timeline",
			Vector:   []float32{0.1, 0.2, 0.2},
		},
		{
			ID:       filterDocDID,
			Category: "math",
			Score:    60,
			Content:  "basic math review",
			Vector:   []float32{0.2, 0.1, 0.1},
		},
	}

	// Index documents into Elasticsearch
	for i, doc := range docs {
		_, err := esClient.Index(indexName).Id(doc.ID).Request(map[string]interface{}{
			"content":  doc.Content,
			"category": doc.Category,
			"score":    doc.Score,
		}).Do(ctx)
		require.NoErrorf(t, err, "failed to index document %d into elasticsearch", i)
	}
	_, err = esClient.Indices.Refresh().Index(indexName).Do(ctx)
	require.NoError(t, err, "failed to refresh index")

	// Prepare Qdrant collection and vectors
	qCollectionsClient := qdrant.NewCollectionsClient(qdrantConn)
	_, err = qCollectionsClient.Create(ctx, &qdrant.CreateCollection{
		CollectionName: indexName,
		VectorsConfig: &qdrant.VectorsConfig{Config: &qdrant.VectorsConfig_Params{
			Params: &qdrant.VectorParams{Size: 3, Distance: qdrant.Distance_Cosine},
		}},
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		require.NoError(t, err, "failed to create qdrant collection")
	}

	pointsClient := qdrant.NewPointsClient(qdrantConn)
	wait := true
	points := make([]*qdrant.PointStruct, 0, len(docs))
	for _, doc := range docs {
		payload := map[string]*qdrant.Value{
			"content": {
				Kind: &qdrant.Value_StringValue{StringValue: doc.Content},
			},
			"category": {
				Kind: &qdrant.Value_StringValue{StringValue: doc.Category},
			},
			"score": {
				Kind: &qdrant.Value_DoubleValue{DoubleValue: doc.Score},
			},
		}
		points = append(points, &qdrant.PointStruct{
			Id: &qdrant.PointId{
				PointIdOptions: &qdrant.PointId_Uuid{Uuid: doc.ID},
			},
			Vectors: &qdrant.Vectors{
				VectorsOptions: &qdrant.Vectors_Vector{Vector: &qdrant.Vector{Data: doc.Vector}},
			},
			Payload: payload,
		})
	}

	_, err = pointsClient.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: indexName,
		Points:         points,
		Wait:           &wait,
	})
	require.NoError(t, err, "failed to upsert vectors to qdrant")

	req := &searchv1.SearchDocumentsRequest{
		IndexName:   indexName,
		QueryText:   "math",
		QueryVector: queryVector,
		PageSize:    2,
		Filters: []*searchv1.Filter{
			{
				Field:    "category",
				Operator: searchv1.Filter_OPERATOR_EQUAL,
				Value:    structpb.NewStringValue("math"),
			},
			{
				Field:    "score",
				Operator: searchv1.Filter_OPERATOR_GREATER_THAN,
				Value:    structpb.NewNumberValue(70),
			},
		},
		SortBy: &searchv1.SortBy{
			Field: "score",
			Order: searchv1.SortBy_ORDER_DESC,
		},
	}

	res, err := client.SearchDocuments(ctx, req)
	require.NoError(t, err, "SearchDocuments RPC failed")
	require.Len(t, res.Results, 2, "page size should limit results to 2 documents")
	assert.Equal(t, int64(2), res.TotalCount, "total count should reflect filtered documents returned")

	ids := []string{res.Results[0].DocumentId, res.Results[1].DocumentId}
	assert.NotContains(t, ids, filterDocEID, "result should not include third math document due to page size limit")

	score1, _ := res.Results[0].Fields.AsMap()["score"].(float64)
	score2, _ := res.Results[1].Fields.AsMap()["score"].(float64)

	assert.GreaterOrEqual(t, score1, score2, "results should be sorted by descending score field")

	for _, r := range res.Results {
		fieldsMap := r.Fields.AsMap()
		category, _ := fieldsMap["category"].(string)
		scoreValue, _ := fieldsMap["score"].(float64)

		assert.Equal(t, "math", category, "filter should restrict category to math")
		assert.Greater(t, scoreValue, float64(70), "score filter should exclude low scores")
	}
}

func TestSearchDocuments_CursorPagination_Integration(t *testing.T) {
	ctx := context.Background()

	esClient, esCleanup, err := setupElasticsearch(ctx)
	require.NoError(t, err, "failed to setup elasticsearch")
	defer esCleanup()

	qdrantConn, qdrantCleanup, err := setupQdrant(ctx)
	require.NoError(t, err, "failed to setup qdrant")
	defer qdrantCleanup()

	grpcAddr, grpcCleanup := startTestGRPCServer(t, esClient, qdrantConn)
	defer grpcCleanup()

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := searchv1.NewSearchServiceClient(conn)

	indexName := fmt.Sprintf("pagination-test-%d", time.Now().UnixNano())
	queryVector := []float32{1.0, 0.0, 0.0}

	type paginationDoc struct {
		ID      string
		Content string
		Vector  []float32
	}

	docs := []paginationDoc{
		{ID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", Content: "pagination test document 1", Vector: []float32{1.0, 0.0, 0.0}},
		{ID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2", Content: "pagination test document 2", Vector: []float32{0.95, 0.05, 0.0}},
		{ID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa3", Content: "pagination test document 3", Vector: []float32{0.9, 0.1, 0.0}},
		{ID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa4", Content: "pagination test document 4", Vector: []float32{0.85, 0.15, 0.0}},
		{ID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa5", Content: "pagination test document 5", Vector: []float32{0.8, 0.2, 0.0}},
	}

	for i, doc := range docs {
		_, err := esClient.Index(indexName).Id(doc.ID).Request(map[string]interface{}{
			"content": doc.Content,
		}).Do(ctx)
		require.NoErrorf(t, err, "failed to index document %d into elasticsearch", i)
	}
	_, err = esClient.Indices.Refresh().Index(indexName).Do(ctx)
	require.NoError(t, err, "failed to refresh index")

	qCollectionsClient := qdrant.NewCollectionsClient(qdrantConn)
	_, err = qCollectionsClient.Create(ctx, &qdrant.CreateCollection{
		CollectionName: indexName,
		VectorsConfig: &qdrant.VectorsConfig{Config: &qdrant.VectorsConfig_Params{
			Params: &qdrant.VectorParams{Size: 3, Distance: qdrant.Distance_Cosine},
		}},
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		require.NoError(t, err, "failed to create qdrant collection")
	}

	pointsClient := qdrant.NewPointsClient(qdrantConn)
	wait := true
	points := make([]*qdrant.PointStruct, 0, len(docs))
	for _, doc := range docs {
		points = append(points, &qdrant.PointStruct{
			Id: &qdrant.PointId{
				PointIdOptions: &qdrant.PointId_Uuid{Uuid: doc.ID},
			},
			Vectors: &qdrant.Vectors{
				VectorsOptions: &qdrant.Vectors_Vector{Vector: &qdrant.Vector{Data: doc.Vector}},
			},
			Payload: map[string]*qdrant.Value{
				"content": {
					Kind: &qdrant.Value_StringValue{StringValue: doc.Content},
				},
			},
		})
	}

	_, err = pointsClient.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: indexName,
		Points:         points,
		Wait:           &wait,
	})
	require.NoError(t, err, "failed to upsert vectors to qdrant")

	firstPage, err := client.SearchDocuments(ctx, &searchv1.SearchDocumentsRequest{
		IndexName:   indexName,
		QueryText:   "pagination",
		QueryVector: queryVector,
		PageSize:    2,
	})
	require.NoError(t, err, "SearchDocuments first page failed")
	require.Len(t, firstPage.Results, 2, "first page should return two documents")
	require.NotEmpty(t, firstPage.NextPageToken, "first page should include next page token")
	assert.Equal(t, int64(2), firstPage.TotalCount, "first page total count should match returned documents")

	secondPage, err := client.SearchDocuments(ctx, &searchv1.SearchDocumentsRequest{
		IndexName:   indexName,
		QueryText:   "pagination",
		QueryVector: queryVector,
		PageSize:    2,
		PageToken:   firstPage.NextPageToken,
	})
	require.NoError(t, err, "SearchDocuments second page failed")
	require.Len(t, secondPage.Results, 2, "second page should return two documents")
	require.NotEmpty(t, secondPage.NextPageToken, "second page should include next page token")
	assert.Equal(t, int64(2), secondPage.TotalCount, "second page total count should match returned documents")
	assert.NotEqual(t, firstPage.NextPageToken, secondPage.NextPageToken, "page tokens should advance between pages")

	thirdPage, err := client.SearchDocuments(ctx, &searchv1.SearchDocumentsRequest{
		IndexName:   indexName,
		QueryText:   "pagination",
		QueryVector: queryVector,
		PageSize:    2,
		PageToken:   secondPage.NextPageToken,
	})
	require.NoError(t, err, "SearchDocuments third page failed")
	require.Len(t, thirdPage.Results, 1, "third page should return the remaining document")
	assert.Empty(t, thirdPage.NextPageToken, "third page should not include next page token")
	assert.Equal(t, int64(1), thirdPage.TotalCount, "third page total count should match returned documents")

	collectedIDs := make([]string, 0, len(docs))
	for _, page := range [][]*searchv1.SearchResult{firstPage.Results, secondPage.Results, thirdPage.Results} {
		for _, doc := range page {
			collectedIDs = append(collectedIDs, doc.DocumentId)
		}
	}

	expectedOrder := []string{
		docs[0].ID,
		docs[1].ID,
		docs[2].ID,
		docs[3].ID,
		docs[4].ID,
	}
	assert.Equal(t, expectedOrder, collectedIDs, "documents should be returned in descending similarity order across pages")

	uniqueIDs := make(map[string]struct{}, len(collectedIDs))
	for _, id := range collectedIDs {
		uniqueIDs[id] = struct{}{}
	}
	assert.Len(t, uniqueIDs, len(docs), "documents should not repeat across pages")
}

// TestKafkaIndexing_Integration は Kafka コンシューマを介した非同期インデックス処理の統合テストです。
func TestKafkaIndexing_Integration(t *testing.T) {
	ctx := context.Background()

	// 外部依存のセットアップ
	esClient, esCleanup, err := setupElasticsearch(ctx)
	require.NoError(t, err, "failed to setup elasticsearch")
	defer esCleanup()

	qdrantConn, qdrantCleanup, err := setupQdrant(ctx)
	require.NoError(t, err, "failed to setup qdrant")
	defer qdrantCleanup()

	kafkaBootstrap, kafkaCleanup, err := setupKafka(ctx)
	require.NoError(t, err, "failed to setup kafka")
	defer kafkaCleanup()

	// サービスの初期化
	logger := zap.NewNop()
	repo := repository.NewSearchRepository(esClient, qdrantConn)
	svc := service.NewSearchService(repo, logger, service.HybridWeights{
		Keyword: 0.5,
		Vector:  0.5,
	})

	topic := "search.indexing.requests"
	groupID := fmt.Sprintf("search-service-integration-%d", time.Now().UnixNano())

	require.NoError(t, ensureKafkaTopic(ctx, kafkaBootstrap, topic), "failed to prepare kafka topic")

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrap,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err, "failed to create kafka consumer")

	dlqProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrap,
	})
	require.NoError(t, err, "failed to create kafka dlq producer")

	consumerCtx, cancel := context.WithCancel(ctx)
	consumerErrCh := make(chan error, 1)
	go func() {
		consumerErrCh <- message_adapter.NewConsumer(consumer, dlqProducer, svc, logger, topic).Run(consumerCtx)
	}()

	defer func() {
		cancel()
		select {
		case err := <-consumerErrCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("kafka consumer returned error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("kafka consumer did not stop within timeout")
		}
	}()

	indexName := fmt.Sprintf("kafka-index-test-%d", time.Now().UnixNano())
	vector := []float32{0.12, 0.34, 0.56}

	require.NoError(t, svc.CreateIndex(ctx, port.CreateIndexParams{
		IndexName: indexName,
		Fields: []port.FieldDefinition{
			{Name: "content", Type: port.FieldTypeText},
		},
		VectorConfig: &port.VectorConfig{
			Dimension: len(vector),
			Distance:  port.VectorDistanceCosine,
		},
	}), "failed to create search index")

	// Kafka プロデューサーでメッセージを送信
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrap,
	})
	require.NoError(t, err, "failed to create kafka producer")
	defer producer.Close()

	docID := "44444444-4444-4444-4444-444444444444"
	payload := map[string]interface{}{
		"payload": map[string]interface{}{
			"index_name":  indexName,
			"document_id": docID,
			"action":      "UPSERT",
			"fields": map[string]interface{}{
				"content": "Kafka integration test document",
			},
			"vector": vector,
		},
	}

	value, err := json.Marshal(payload)
	require.NoError(t, err, "failed to marshal kafka payload")

	deliveryChan := make(chan kafka.Event, 1)
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}, deliveryChan)
	require.NoError(t, err, "failed to produce kafka message")

	select {
	case ev := <-deliveryChan:
		m, ok := ev.(*kafka.Message)
		require.True(t, ok, "expected kafka message event")
		require.NoError(t, m.TopicPartition.Error, "kafka delivery error")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for kafka delivery report")
	}
	close(deliveryChan)
	producer.Flush(5000)

	// Elasticsearch への反映を待つ
	require.Eventually(t, func() bool {
		docs, err := repo.KeywordSearch(context.Background(), port.KeywordSearchParams{
			IndexName: indexName,
			Query:     "Kafka",
		})
		if err != nil {
			return false
		}
		for _, d := range docs {
			if d.ID == docID {
				return true
			}
		}
		return false
	}, 30*time.Second, 1*time.Second, "document should be indexed in Elasticsearch via Kafka consumer")

	// Qdrant への反映を待つ
	require.Eventually(t, func() bool {
		docs, err := repo.VectorSearch(context.Background(), port.VectorSearchParams{
			IndexName: indexName,
			Vector:    vector,
		})
		if err != nil {
			return false
		}
		for _, d := range docs {
			if d.ID == docID {
				return true
			}
		}
		return false
	}, 30*time.Second, 1*time.Second, "document should be indexed in Qdrant via Kafka consumer")
}

type failingSearchService struct {
	mu        sync.Mutex
	callCount int
	err       error
}

func newFailingSearchService(err error) *failingSearchService {
	if err == nil {
		err = errors.New("intentional failure")
	}
	return &failingSearchService{err: err}
}

func (f *failingSearchService) Search(context.Context, port.SearchParams) (*port.SearchResult, error) {
	return nil, errors.New("not implemented")
}

func (f *failingSearchService) IndexDocument(context.Context, port.IndexDocumentParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callCount++
	return f.err
}

func (f *failingSearchService) DeleteDocument(context.Context, string, string) error {
	return nil
}

func (f *failingSearchService) CreateIndex(context.Context, port.CreateIndexParams) error {
	return nil
}

func (f *failingSearchService) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callCount
}

// TestKafkaConsumer_RetryAndDLQ は処理失敗時にリトライ後DLQへメッセージが転送されることを検証します。
func TestKafkaConsumer_RetryAndDLQ(t *testing.T) {
	ctx := context.Background()

	kafkaBootstrap, kafkaCleanup, err := setupKafka(ctx)
	require.NoError(t, err, "failed to setup kafka")
	defer kafkaCleanup()

	topic := fmt.Sprintf("search.retry.%d", time.Now().UnixNano())
	dlqTopic := topic + ".dlq"
	require.NoError(t, ensureKafkaTopic(ctx, kafkaBootstrap, topic), "failed to prepare kafka topic")
	require.NoError(t, ensureKafkaTopic(ctx, kafkaBootstrap, dlqTopic), "failed to prepare dlq topic")

	groupID := fmt.Sprintf("search-service-retry-%d", time.Now().UnixNano())
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrap,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err, "failed to create kafka consumer")

	dlqProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrap,
	})
	require.NoError(t, err, "failed to create kafka dlq producer")

	failErr := errors.New("forced failure for retry test")
	svc := newFailingSearchService(failErr)
	logger := zap.NewNop()

	consumerCtx, cancel := context.WithCancel(ctx)
	consumerErrCh := make(chan error, 1)
	retryCfg := message_adapter.RetryConfig{
		MaxRetries:     2,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		Multiplier:     2,
	}
	go func() {
		consumerErrCh <- message_adapter.NewConsumer(
			consumer,
			dlqProducer,
			svc,
			logger,
			topic,
			message_adapter.WithRetryConfig(retryCfg),
		).Run(consumerCtx)
	}()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrap,
	})
	require.NoError(t, err, "failed to create kafka producer")
	defer producer.Close()

	payload := map[string]interface{}{
		"payload": map[string]interface{}{
			"index_name":  "retry-test-index",
			"document_id": "retry-doc-1",
			"action":      "UPSERT",
			"fields": map[string]interface{}{
				"name": "retry test",
			},
			"vector": []float32{0.1, 0.2},
		},
	}
	value, err := json.Marshal(payload)
	require.NoError(t, err, "failed to marshal kafka payload")

	deliveryChan := make(chan kafka.Event, 1)
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}, deliveryChan)
	require.NoError(t, err, "failed to produce kafka message")

	select {
	case ev := <-deliveryChan:
		m, ok := ev.(*kafka.Message)
		require.True(t, ok, "expected kafka message event")
		require.NoError(t, m.TopicPartition.Error, "kafka delivery error")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for kafka delivery report")
	}
	close(deliveryChan)
	producer.Flush(5000)

	// リトライが実行されることを待機
	require.Eventually(t, func() bool {
		return svc.Calls() >= retryCfg.MaxRetries+1
	}, 10*time.Second, 50*time.Millisecond, "IndexDocument should be called multiple times")

	// DLQにメッセージが送られることを確認
	dlqReader, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrap,
		"group.id":          fmt.Sprintf("dlq-reader-%d", time.Now().UnixNano()),
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err, "failed to create dlq reader consumer")
	defer dlqReader.Close()
	require.NoError(t, dlqReader.Subscribe(dlqTopic, nil), "failed to subscribe dlq topic")

	var dlqMessage *kafka.Message
	require.Eventually(t, func() bool {
		msg, err := dlqReader.ReadMessage(500 * time.Millisecond)
		if err != nil {
			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrTimedOut {
				return false
			}
			require.NoError(t, err)
			return false
		}
		dlqMessage = msg
		return true
	}, 10*time.Second, 200*time.Millisecond, "expected message in DLQ")

	require.NotNil(t, dlqMessage, "dlq message should not be nil")
	assert.Equal(t, value, dlqMessage.Value, "dlq message should contain original payload")

	getHeader := func(headers []kafka.Header, key string) []byte {
		for _, h := range headers {
			if strings.EqualFold(h.Key, key) {
				return h.Value
			}
		}
		return nil
	}

	assert.Equal(t, []byte(topic), getHeader(dlqMessage.Headers, "x-original-topic"))
	assert.Equal(t, []byte(failErr.Error()), getHeader(dlqMessage.Headers, "x-error"))

	cancel()
	select {
	case err := <-consumerErrCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("kafka consumer returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("kafka consumer did not stop within timeout")
	}
}
