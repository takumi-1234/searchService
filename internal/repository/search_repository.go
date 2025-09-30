package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v9/typedapi/types"
	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"

	"github.com/takumi-1234/searchService/internal/port"
)

// searchRepository は Elasticsearch と Qdrant の両方との通信を担当します。
type searchRepository struct {
	esClient     *elasticsearch.TypedClient
	qdrantClient qdrant.PointsClient
}

// NewSearchRepository は新しい統合 searchRepository のインスタンスを生成します。
func NewSearchRepository(es *elasticsearch.TypedClient, qdrantConn *grpc.ClientConn) port.SearchRepository {
	return &searchRepository{
		esClient:     es,
		qdrantClient: qdrant.NewPointsClient(qdrantConn),
	}
}

// KeywordSearch は Elasticsearch を使用してキーワード検索を実行します。
func (r *searchRepository) KeywordSearch(ctx context.Context, indexName, query string) ([]port.Document, error) {
	req := &search.Request{
		Query: &types.Query{
			Match: map[string]types.MatchQuery{
				"content": {Query: query},
			},
		},
	}

	res, err := r.esClient.Search().Index(indexName).Request(req).Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch search request failed: %w", err)
	}

	documents := make([]port.Document, 0, len(res.Hits.Hits))
	for _, hit := range res.Hits.Hits {
		var fields map[string]interface{}
		if err := json.Unmarshal(hit.Source_, &fields); err != nil {
			log.Printf("warn: failed to unmarshal document source: %v", err)
			continue
		}

		var score float32
		if hit.Score_ != nil {
			score = float32(*hit.Score_)
		}

		documents = append(documents, port.Document{
			ID:     *hit.Id_,
			Score:  score,
			Fields: fields,
		})
	}

	return documents, nil
}

// VectorSearch は Qdrant を使用してベクトル検索を実行します。
func (r *searchRepository) VectorSearch(ctx context.Context, indexName string, vector []float32) ([]port.Document, error) {
	req := &qdrant.SearchPoints{
		CollectionName: indexName,
		Vector:         vector,
		Limit:          10,
		WithPayload: &qdrant.WithPayloadSelector{
			SelectorOptions: &qdrant.WithPayloadSelector_Enable{Enable: true},
		},
	}

	res, err := r.qdrantClient.Search(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("qdrant search request failed: %w", err)
	}

	documents := make([]port.Document, 0, len(res.GetResult()))
	for _, hit := range res.GetResult() {
		fields := make(map[string]interface{})
		for k, v := range hit.GetPayload() {
			fields[k] = valueFromQdrant(v)
		}

		var idStr string
		if hit.GetId().GetNum() != 0 {
			idStr = fmt.Sprintf("%d", hit.GetId().GetNum())
		} else {
			idStr = hit.GetId().GetUuid()
		}

		documents = append(documents, port.Document{
			ID:     idStr,
			Score:  hit.GetScore(),
			Fields: fields,
		})
	}

	return documents, nil
}

// valueFromQdrant は qdrant.Value を interface{} に変換するヘルパー関数です。
func valueFromQdrant(v *qdrant.Value) interface{} {
	switch kind := v.GetKind().(type) {
	case *qdrant.Value_NullValue:
		return nil
	case *qdrant.Value_DoubleValue:
		return kind.DoubleValue
	case *qdrant.Value_IntegerValue:
		return kind.IntegerValue
	case *qdrant.Value_StringValue:
		return kind.StringValue
	case *qdrant.Value_BoolValue:
		return kind.BoolValue
	case *qdrant.Value_StructValue:
		m := make(map[string]interface{})
		for k, sv := range kind.StructValue.GetFields() {
			m[k] = valueFromQdrant(sv)
		}
		return m
	case *qdrant.Value_ListValue:
		l := make([]interface{}, 0, len(kind.ListValue.GetValues()))
		for _, lv := range kind.ListValue.GetValues() {
			l = append(l, valueFromQdrant(lv))
		}
		return l
	default:
		return nil
	}
}
