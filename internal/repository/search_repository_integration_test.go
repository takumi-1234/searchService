package repository

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/takumi-1234/searchService/internal/port"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newTestElasticsearchClient(t *testing.T, handler roundTripFunc) *elasticsearch.TypedClient {
	t.Helper()

	cfg := elasticsearch.Config{
		Transport: handler,
		Addresses: []string{"http://localhost:9200"},
	}
	client, err := elasticsearch.NewTypedClient(cfg)
	if err != nil {
		t.Fatalf("failed to create typed client: %v", err)
	}
	return client
}

type testPointsServer struct {
	qdrant.UnimplementedPointsServer
	searchFunc func(context.Context, *qdrant.SearchPoints) (*qdrant.SearchResponse, error)
}

func (s *testPointsServer) Search(ctx context.Context, req *qdrant.SearchPoints) (*qdrant.SearchResponse, error) {
	if s.searchFunc != nil {
		return s.searchFunc(ctx, req)
	}
	return &qdrant.SearchResponse{}, nil
}

func newTestQdrantConn(t *testing.T, pointsServer qdrant.PointsServer) (*grpc.ClientConn, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	qdrant.RegisterPointsServer(grpcServer, pointsServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		grpcServer.Stop()
		if cerr := lis.Close(); cerr != nil {
			t.Logf("failed to close listener: %v", cerr)
		}
		t.Fatalf("failed to dial test server: %v", err)
	}

	cleanup := func() {
		if cerr := conn.Close(); cerr != nil {
			t.Logf("failed to close test connection: %v", cerr)
		}
		grpcServer.Stop()
		if cerr := lis.Close(); cerr != nil {
			t.Logf("failed to close listener: %v", cerr)
		}
	}

	return conn, cleanup
}

func TestSearchRepositoryKeywordSearch(t *testing.T) {
	var capturedBody map[string]interface{}

	esClient := newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost {
			t.Fatalf("expected POST method, got %s", req.Method)
		}
		if req.URL.Path != "/books/_search" {
			t.Fatalf("unexpected path: %s", req.URL.Path)
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("failed to read body: %v", err)
		}
		defer func() {
			if cerr := req.Body.Close(); cerr != nil {
				t.Fatalf("failed to close request body: %v", cerr)
			}
		}()

		if err := json.Unmarshal(body, &capturedBody); err != nil {
			t.Fatalf("failed to unmarshal request body: %v", err)
		}

		respJSON := `{
			"hits": {
				"hits": [
					{
						"_id": "doc-1",
						"_score": 1.23,
						"_source": {
							"title": "Calculus 101"
						}
					}
				]
			}
		}`

		resp := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(respJSON)),
			Header:     make(http.Header),
		}
		resp.Header.Set("X-Elastic-Product", "Elasticsearch")
		return resp, nil
	}))

	pointsServer := &testPointsServer{
		searchFunc: func(ctx context.Context, req *qdrant.SearchPoints) (*qdrant.SearchResponse, error) {
			t.Fatalf("unexpected vector search call")
			return nil, nil
		},
	}

	conn, cleanup := newTestQdrantConn(t, pointsServer)
	defer cleanup()

	repo := NewSearchRepository(esClient, conn)

	docs, err := repo.KeywordSearch(context.Background(), port.KeywordSearchParams{
		IndexName: "books",
		Query:     "calculus",
		PageSize:  5,
	})
	if err != nil {
		t.Fatalf("KeywordSearch returned error: %v", err)
	}

	if len(docs) != 1 {
		t.Fatalf("expected 1 document, got %d", len(docs))
	}
	if docs[0].ID != "doc-1" {
		t.Fatalf("unexpected document ID: %s", docs[0].ID)
	}
	if title := docs[0].Fields["title"]; title != "Calculus 101" {
		t.Fatalf("unexpected title value: %v", title)
	}

	// verify request payload contains match query
	query, ok := capturedBody["query"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected query in request payload, got %v", capturedBody["query"])
	}
	boolQuery, ok := query["bool"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected bool query, got %v", query)
	}
	must, ok := boolQuery["must"].([]interface{})
	if !ok || len(must) == 0 {
		t.Fatalf("expected must clause, got %v", boolQuery["must"])
	}
}

func TestSearchRepositoryVectorSearch(t *testing.T) {
	esClient := newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("unexpected Elasticsearch request to %s", req.URL.Path)
		return nil, errors.New("unexpected call")
	}))

	pointsServer := &testPointsServer{
		searchFunc: func(ctx context.Context, req *qdrant.SearchPoints) (*qdrant.SearchResponse, error) {
			if req.GetCollectionName() != "books" {
				t.Fatalf("unexpected collection name: %s", req.GetCollectionName())
			}
			if len(req.GetVector()) != 3 {
				t.Fatalf("unexpected vector length: %d", len(req.GetVector()))
			}

			return &qdrant.SearchResponse{
				Result: []*qdrant.ScoredPoint{
					{
						Id: &qdrant.PointId{
							PointIdOptions: &qdrant.PointId_Uuid{Uuid: "vec-1"},
						},
						Score: 0.91,
						Payload: map[string]*qdrant.Value{
							"title": {Kind: &qdrant.Value_StringValue{StringValue: "Vector Calculus"}},
						},
					},
				},
			}, nil
		},
	}

	conn, cleanup := newTestQdrantConn(t, pointsServer)
	defer cleanup()

	repo := NewSearchRepository(esClient, conn)

	docs, err := repo.VectorSearch(context.Background(), port.VectorSearchParams{
		IndexName: "books",
		Vector:    []float32{0.1, 0.2, 0.3},
		PageSize:  3,
	})
	if err != nil {
		t.Fatalf("VectorSearch returned error: %v", err)
	}

	if len(docs) != 1 {
		t.Fatalf("expected 1 document, got %d", len(docs))
	}
	if docs[0].ID != "vec-1" {
		t.Fatalf("unexpected document ID: %s", docs[0].ID)
	}
	if docs[0].Score <= 0 {
		t.Fatalf("expected positive score, got %f", docs[0].Score)
	}
	if title := docs[0].Fields["title"]; title != "Vector Calculus" {
		t.Fatalf("unexpected title value: %v", title)
	}
}
