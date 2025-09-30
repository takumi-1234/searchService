package repository

import (
	"context"
	"fmt"

	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"

	"github.com/takumi-1234/searchService/internal/port"
)

// qdrantRepository は Qdrant との通信を担当します。
type qdrantRepository struct {
	pointsClient qdrant.PointsClient
}

// NewQdrantRepository は新しい qdrantRepository のインスタンスを生成します。
func NewQdrantRepository(conn *grpc.ClientConn) port.SearchRepository {
	return &qdrantRepository{
		pointsClient: qdrant.NewPointsClient(conn),
	}
}

// KeywordSearch は qdrantRepository では実装されません。
// 複合リポジトリで利用するためにダミーメソッドとして定義します。
func (r *qdrantRepository) KeywordSearch(ctx context.Context, indexName, query string) ([]port.Document, error) {
	return nil, fmt.Errorf("KeywordSearch is not implemented for qdrantRepository")
}

// VectorSearch は Qdrant を使用してベクトル検索を実行します。
func (r *qdrantRepository) VectorSearch(ctx context.Context, indexName string, vector []float32) ([]port.Document, error) {
	// 1. 検索リクエストを構築
	req := &qdrant.SearchPoints{
		CollectionName: indexName,
		Vector:         vector,
		Limit:          10, // デフォルトの検索上限 (要件に応じて変更)
		WithPayload: &qdrant.WithPayloadSelector{
			SelectorOptions: &qdrant.WithPayloadSelector_Enable{
				Enable: true,
			},
		},
	}

	// 2. 検索リクエストを実行
	// import に fmt, maybe errors, etc.

	res, err := r.pointsClient.Search(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("qdrant search request failed: %w", err)
	}

	documents := make([]port.Document, 0, len(res.GetResult()))
	for _, hit := range res.GetResult() {
		// Payload を map[string]interface{} に変換
		fields := make(map[string]interface{}, len(hit.GetPayload()))
		for k, v := range hit.GetPayload() {
			// v は *qdrant.Value
			// v.Kind を使って型判定
			switch v.Kind.(type) {
			case *qdrant.Value_StringValue:
				fields[k] = v.GetStringValue()
			case *qdrant.Value_IntegerValue:
				fields[k] = v.GetIntegerValue()
			case *qdrant.Value_DoubleValue:
				fields[k] = v.GetDoubleValue()
			case *qdrant.Value_BoolValue:
				fields[k] = v.GetBoolValue()
			case *qdrant.Value_StructValue:
				m := make(map[string]interface{}, len(v.GetStructValue().GetFields()))
				for fk, fv := range v.GetStructValue().GetFields() {
					// 簡易変換：文字列 or 数値のみ対応例
					if fv.GetStringValue() != "" {
						m[fk] = fv.GetStringValue()
					} else if fv.GetIntegerValue() != 0 {
						m[fk] = fv.GetIntegerValue()
					} else {
						m[fk] = nil
					}
				}
				fields[k] = m
			case *qdrant.Value_ListValue:
				listVals := v.GetListValue().GetValues()
				arr := make([]interface{}, len(listVals))
				for i, lv := range listVals {
					if lv.GetStringValue() != "" {
						arr[i] = lv.GetStringValue()
					} else if lv.GetIntegerValue() != 0 {
						arr[i] = lv.GetIntegerValue()
					} else {
						arr[i] = nil
					}
				}
				fields[k] = arr
			default:
				// 値なし or null 相当
				fields[k] = nil
			}
		}

		// ID の取得
		pid := hit.GetId()
		var idStr string
		// 優先的に数値 ID を文字列化、もし 0 なら UUID を使う
		if pid.GetNum() != 0 {
			idStr = fmt.Sprintf("%d", pid.GetNum())
		} else if pid.GetUuid() != "" {
			idStr = pid.GetUuid()
		} else {
			idStr = "" // fallback
		}

		documents = append(documents, port.Document{
			ID:     idStr,
			Score:  hit.GetScore(),
			Fields: fields,
		})
	}

	return documents, nil
}
