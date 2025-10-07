package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v9/typedapi/types"
	"github.com/elastic/go-elasticsearch/v9/typedapi/types/enums/result"
	"github.com/qdrant/go-client/qdrant"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/takumi-1234/searchService/internal/port"
)

// searchRepository は Elasticsearch と Qdrant の両方との通信を担当します。
type searchRepository struct {
	esClient          *elasticsearch.TypedClient
	qdrantPoints      qdrant.PointsClient
	qdrantCollections qdrant.CollectionsClient
}

// NewSearchRepository は新しい統合 searchRepository のインスタンスを生成します。
func NewSearchRepository(es *elasticsearch.TypedClient, qdrantConn *grpc.ClientConn) port.SearchRepository {
	return &searchRepository{
		esClient:          es,
		qdrantPoints:      qdrant.NewPointsClient(qdrantConn),
		qdrantCollections: qdrant.NewCollectionsClient(qdrantConn),
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

	res, err := r.qdrantPoints.Search(ctx, req)
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

// IndexDocument はドキュメントをElasticsearchとQdrantに並行してインデックスします。
func (r *searchRepository) IndexDocument(ctx context.Context, params port.IndexDocumentParams) error {
	g, gCtx := errgroup.WithContext(ctx)

	// Elasticsearchへのインデックス作成
	g.Go(func() error {
		res, err := r.esClient.Index(params.IndexName).Id(params.DocumentID).Request(params.Fields).Do(gCtx)
		if err != nil {
			return fmt.Errorf("failed to index document in elasticsearch: %w", err)
		}
		// 修正点: 文字列比較をenum定数比較に変更
		if res.Result != result.Created && res.Result != result.Updated {
			return fmt.Errorf("unexpected elasticsearch index result: %s", res.Result)
		}
		return nil
	})

	// QdrantへのUpsert
	if len(params.Vector) > 0 {
		vector := append([]float32(nil), params.Vector...)
		g.Go(func() error {
			payload, err := payloadToQdrant(params.Fields)
			if err != nil {
				return fmt.Errorf("failed to convert payload for qdrant: %w", err)
			}

			wait := true
			_, err = r.qdrantPoints.Upsert(gCtx, &qdrant.UpsertPoints{
				CollectionName: params.IndexName,
				Wait:           &wait,
				Points: []*qdrant.PointStruct{
					{
						Id:      &qdrant.PointId{PointIdOptions: &qdrant.PointId_Uuid{Uuid: params.DocumentID}},
						Vectors: &qdrant.Vectors{VectorsOptions: &qdrant.Vectors_Vector{Vector: &qdrant.Vector{Data: vector}}},
						Payload: payload,
					},
				},
			})
			if err != nil {
				return fmt.Errorf("failed to upsert point to qdrant: %w", err)
			}
			return nil
		})
	}

	return g.Wait()
}

// DeleteDocument はドキュメントをElasticsearchとQdrantから並行して削除します。
func (r *searchRepository) DeleteDocument(ctx context.Context, indexName, documentID string) error {
	g, gCtx := errgroup.WithContext(ctx)

	// Elasticsearchからの削除
	g.Go(func() error {
		res, err := r.esClient.Delete(indexName, documentID).Do(gCtx)
		if err != nil {
			return fmt.Errorf("failed to delete document from elasticsearch: %w", err)
		}
		// 修正点: 文字列比較をenum定数比較に変更
		if res.Result.Name != "deleted" && res.Result.Name != "not_found" {
			return fmt.Errorf("unexpected elasticsearch delete result: %s", res.Result)
		}
		return nil
	})

	// Qdrantからの削除
	g.Go(func() error {
		wait := true
		// 修正点: メソッド名を Delete に変更
		_, err := r.qdrantPoints.Delete(gCtx, &qdrant.DeletePoints{
			CollectionName: indexName,
			Wait:           &wait,
			Points: &qdrant.PointsSelector{
				PointsSelectorOneOf: &qdrant.PointsSelector_Points{
					Points: &qdrant.PointsIdsList{
						Ids: []*qdrant.PointId{
							{PointIdOptions: &qdrant.PointId_Uuid{Uuid: documentID}},
						},
					},
				},
			},
		})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return nil
			}
			return fmt.Errorf("failed to delete point from qdrant: %w", err)
		}
		return nil
	})

	return g.Wait()
}

// CreateIndex は Elasticsearch と Qdrant にインデックスを作成します。
func (r *searchRepository) CreateIndex(ctx context.Context, params port.CreateIndexParams) error {
	if params.IndexName == "" {
		return fmt.Errorf("index name must not be empty")
	}
	if params.VectorConfig == nil {
		return fmt.Errorf("vector config must not be nil")
	}

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return r.createElasticsearchIndex(gCtx, params)
	})

	g.Go(func() error {
		return r.createQdrantCollection(gCtx, params)
	})

	return g.Wait()
}

func (r *searchRepository) createElasticsearchIndex(ctx context.Context, params port.CreateIndexParams) error {
	properties := make(map[string]interface{}, len(params.Fields))
	for _, field := range params.Fields {
		fieldType, err := toElasticsearchFieldType(field.Type)
		if err != nil {
			return err
		}
		properties[field.Name] = map[string]interface{}{"type": fieldType}
	}

	createReq := r.esClient.Indices.Create(params.IndexName)
	if len(properties) > 0 {
		body := map[string]interface{}{
			"mappings": map[string]interface{}{
				"properties": properties,
			},
		}
		payload, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal elasticsearch mappings: %w", err)
		}
		createReq = createReq.Raw(bytes.NewReader(payload))
	}

	res, err := createReq.Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to create elasticsearch index: %w", err)
	}
	if !res.Acknowledged {
		return fmt.Errorf("elasticsearch index creation not acknowledged")
	}
	return nil
}

func (r *searchRepository) createQdrantCollection(ctx context.Context, params port.CreateIndexParams) error {
	vectorCfg := params.VectorConfig
	if vectorCfg.Dimension <= 0 {
		return fmt.Errorf("vector dimension must be positive")
	}

	distance, err := toQdrantDistance(vectorCfg.Distance)
	if err != nil {
		return err
	}

	_, err = r.qdrantCollections.Create(ctx, &qdrant.CreateCollection{
		CollectionName: params.IndexName,
		VectorsConfig: &qdrant.VectorsConfig{
			Config: &qdrant.VectorsConfig_Params{
				Params: &qdrant.VectorParams{
					Size:     uint64(vectorCfg.Dimension),
					Distance: distance,
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create qdrant collection: %w", err)
	}
	return nil
}

func toElasticsearchFieldType(fieldType string) (string, error) {
	switch normalized := strings.ToLower(fieldType); normalized {
	case port.FieldTypeText:
		return "text", nil
	case port.FieldTypeKeyword:
		return "keyword", nil
	case port.FieldTypeInteger:
		return "integer", nil
	case port.FieldTypeFloat:
		return "float", nil
	case port.FieldTypeBoolean:
		return "boolean", nil
	case port.FieldTypeDate:
		return "date", nil
	default:
		return "", fmt.Errorf("unsupported field type: %s", fieldType)
	}
}

func toQdrantDistance(distance port.VectorDistance) (qdrant.Distance, error) {
	switch distance {
	case port.VectorDistanceCosine:
		return qdrant.Distance_Cosine, nil
	case port.VectorDistanceEuclid:
		return qdrant.Distance_Euclid, nil
	case port.VectorDistanceDot:
		return qdrant.Distance_Dot, nil
	default:
		return qdrant.Distance_UnknownDistance, fmt.Errorf("unsupported vector distance: %s", distance)
	}
}

// ... (valueFromQdrant, payloadToQdrant ヘルパー関数は変更なし)
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
func payloadToQdrant(fields map[string]interface{}) (map[string]*qdrant.Value, error) {
	payload := make(map[string]*qdrant.Value, len(fields))
	for k, v := range fields {
		qdrantValue, err := toQdrantValue(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert value for key '%s': %w", k, err)
		}
		payload[k] = qdrantValue
	}
	return payload, nil
}
func toQdrantValue(v interface{}) (*qdrant.Value, error) {
	switch val := v.(type) {
	case string:
		return &qdrant.Value{Kind: &qdrant.Value_StringValue{StringValue: val}}, nil
	case int:
		return &qdrant.Value{Kind: &qdrant.Value_IntegerValue{IntegerValue: int64(val)}}, nil
	case int64:
		return &qdrant.Value{Kind: &qdrant.Value_IntegerValue{IntegerValue: val}}, nil
	case float32:
		return &qdrant.Value{Kind: &qdrant.Value_DoubleValue{DoubleValue: float64(val)}}, nil
	case float64:
		return &qdrant.Value{Kind: &qdrant.Value_DoubleValue{DoubleValue: val}}, nil
	case bool:
		return &qdrant.Value{Kind: &qdrant.Value_BoolValue{BoolValue: val}}, nil
	case nil:
		return &qdrant.Value{Kind: &qdrant.Value_NullValue{}}, nil
	case map[string]interface{}:
		payload := make(map[string]*qdrant.Value, len(val))
		for key, nested := range val {
			converted, err := toQdrantValue(nested)
			if err != nil {
				return nil, fmt.Errorf("failed to convert nested key '%s': %w", key, err)
			}
			payload[key] = converted
		}
		return &qdrant.Value{Kind: &qdrant.Value_StructValue{StructValue: &qdrant.Struct{Fields: payload}}}, nil
	case []interface{}:
		values := make([]*qdrant.Value, 0, len(val))
		for idx, item := range val {
			converted, err := toQdrantValue(item)
			if err != nil {
				return nil, fmt.Errorf("failed to convert list index %d: %w", idx, err)
			}
			values = append(values, converted)
		}
		return &qdrant.Value{Kind: &qdrant.Value_ListValue{ListValue: &qdrant.ListValue{Values: values}}}, nil
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Map:
			if rv.Type().Key().Kind() != reflect.String {
				return nil, fmt.Errorf("unsupported map key type %s for qdrant payload", rv.Type().Key())
			}
			payload := make(map[string]*qdrant.Value, rv.Len())
			for _, key := range rv.MapKeys() {
				converted, err := toQdrantValue(rv.MapIndex(key).Interface())
				if err != nil {
					return nil, fmt.Errorf("failed to convert nested key '%s': %w", key.String(), err)
				}
				payload[key.String()] = converted
			}
			return &qdrant.Value{Kind: &qdrant.Value_StructValue{StructValue: &qdrant.Struct{Fields: payload}}}, nil
		case reflect.Slice, reflect.Array:
			length := rv.Len()
			values := make([]*qdrant.Value, 0, length)
			for i := 0; i < length; i++ {
				converted, err := toQdrantValue(rv.Index(i).Interface())
				if err != nil {
					return nil, fmt.Errorf("failed to convert list index %d: %w", i, err)
				}
				values = append(values, converted)
			}
			return &qdrant.Value{Kind: &qdrant.Value_ListValue{ListValue: &qdrant.ListValue{Values: values}}}, nil
		default:
			return nil, fmt.Errorf("unsupported type for qdrant payload: %T", v)
		}
	}
}
