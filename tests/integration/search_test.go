//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
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
	message_adapter "github.com/takumi-1234/searchService/internal/adapter/message"
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

// setupKafka は Kafka (および必要な Zookeeper) の testcontainer をセットアップします。
func setupKafka(ctx context.Context) (string, func(), error) {
	const (
		containerKafkaPortExternal = "29092/tcp"
		containerKafkaPortInternal = "9092/tcp"
		containerZkPort            = "2181/tcp"
	)

	networkName := fmt.Sprintf("kafka-net-%d", time.Now().UnixNano())
	network, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Name:   networkName,
			Driver: "bridge",
		},
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to create network: %w", err)
	}

	kafkaHostPort, err := getFreePort()
	if err != nil {
		return "", nil, fmt.Errorf("failed to get free port for kafka: %w", err)
	}

	// Zookeeper コンテナの起動
	zkReq := testcontainers.ContainerRequest{
		Image: "confluentinc/cp-zookeeper:7.6.1",
		Env: map[string]string{
			"ZOOKEEPER_CLIENT_PORT": "2181",
			"ZOOKEEPER_TICK_TIME":   "2000",
		},
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{networkName: {"zookeeper"}},
		ExposedPorts:   []string{containerZkPort},
		WaitingFor:     wait.ForListeningPort(containerZkPort).WithStartupTimeout(120 * time.Second),
	}
	zkContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: zkReq, Started: true})
	if err != nil {
		network.Remove(ctx) //nolint:errcheck // best effort cleanup
		return "", nil, fmt.Errorf("failed to start zookeeper container: %w", err)
	}

	// Kafka コンテナの起動
	kafkaReq := testcontainers.ContainerRequest{
		Image: "confluentinc/cp-kafka:7.6.1",
		Env: map[string]string{
			"KAFKA_BROKER_ID":                                "1",
			"KAFKA_ZOOKEEPER_CONNECT":                        "zookeeper:2181",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
			"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092",
			"KAFKA_ADVERTISED_LISTENERS":                     fmt.Sprintf("PLAINTEXT://kafka:9092,PLAINTEXT_HOST://127.0.0.1:%d", kafkaHostPort),
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "true",
			"KAFKA_HEAP_OPTS":                                "-Xms256m -Xmx256m",
		},
		ExposedPorts:   []string{containerKafkaPortExternal, containerKafkaPortInternal},
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{networkName: {"kafka"}},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(containerKafkaPortInternal),
			wait.ForListeningPort(containerKafkaPortExternal),
		).WithStartupTimeout(180 * time.Second),
		HostConfigModifier: func(hc *container.HostConfig) {
			if hc.PortBindings == nil {
				hc.PortBindings = nat.PortMap{}
			}
			internalPort := nat.Port(containerKafkaPortInternal)
			if _, ok := hc.PortBindings[internalPort]; !ok {
				hc.PortBindings[internalPort] = []nat.PortBinding{
					{
						HostIP:   "0.0.0.0",
						HostPort: "0",
					},
				}
			}
			externalPort := nat.Port(containerKafkaPortExternal)
			hc.PortBindings[externalPort] = []nat.PortBinding{
				{
					HostIP:   "127.0.0.1",
					HostPort: strconv.Itoa(kafkaHostPort),
				},
			}
		},
	}

	kafkaContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: kafkaReq, Started: true})
	if err != nil {
		_ = zkContainer.Terminate(ctx)
		network.Remove(ctx) //nolint:errcheck // best effort cleanup
		return "", nil, fmt.Errorf("failed to start kafka container: %w", err)
	}

	cleanup := func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate kafka container: %v", err)
		}
		if err := zkContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate zookeeper container: %v", err)
		}
		if err := network.Remove(ctx); err != nil {
			log.Printf("failed to remove test network: %v", err)
		}
	}

	return fmt.Sprintf("127.0.0.1:%d", kafkaHostPort), cleanup, nil
}

// startTestGRPCServer はテスト用のgRPCサーバーを起動します。
func startTestGRPCServer(t *testing.T, esClient *elasticsearch.TypedClient, qdrantConn *grpc.ClientConn) (string, func()) {
	t.Helper()
	repo := repository.NewSearchRepository(esClient, qdrantConn)
	svc := service.NewSearchService(repo, zap.NewNop())
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
	svc := service.NewSearchService(repo, logger)

	topic := "search.indexing.requests"
	groupID := fmt.Sprintf("search-service-integration-%d", time.Now().UnixNano())

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrap,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err, "failed to create kafka consumer")

	consumerCtx, cancel := context.WithCancel(ctx)
	consumerErrCh := make(chan error, 1)
	go func() {
		consumerErrCh <- message_adapter.NewConsumer(consumer, svc, logger, topic).Run(consumerCtx)
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

	// Qdrant コレクションを準備
	indexName := fmt.Sprintf("kafka-index-test-%d", time.Now().UnixNano())
	vector := []float32{0.12, 0.34, 0.56}

	qCollectionsClient := qdrant.NewCollectionsClient(qdrantConn)
	_, err = qCollectionsClient.Create(ctx, &qdrant.CreateCollection{
		CollectionName: indexName,
		VectorsConfig: &qdrant.VectorsConfig{Config: &qdrant.VectorsConfig_Params{
			Params: &qdrant.VectorParams{Size: uint64(len(vector)), Distance: qdrant.Distance_Cosine},
		}},
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		require.NoError(t, err, "failed to create qdrant collection")
	}

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
		docs, err := repo.KeywordSearch(context.Background(), indexName, "Kafka")
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
		docs, err := repo.VectorSearch(context.Background(), indexName, vector)
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
