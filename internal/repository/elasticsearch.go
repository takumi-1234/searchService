package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v9/typedapi/types"

	"github.com/takumi-1234/searchService/internal/port"
)

// elasticsearchRepository は Elasticsearch との通信を担当します。
type elasticsearchRepository struct {
	es *elasticsearch.TypedClient
}

// NewElasticsearchRepository は新しい elasticsearchRepository のインスタンスを生成します。
func NewElasticsearchRepository(client *elasticsearch.TypedClient) port.SearchRepository {
	return &elasticsearchRepository{es: client}
}

// KeywordSearch は Elasticsearch を使用してキーワード検索を実行します。
func (r *elasticsearchRepository) KeywordSearch(ctx context.Context, indexName, query string) ([]port.Document, error) {
	// 1. 型安全な検索リクエストを構築
	req := &search.Request{
		Query: &types.Query{
			Match: map[string]types.MatchQuery{
				// "content" フィールドを検索対象とする (要件に応じて変更)
				"content": {Query: query},
			},
		},
	}

	// 2. 検索リクエストを実行
	res, err := r.es.Search().
		Index(indexName).
		Request(req).
		Do(ctx)

	if err != nil {
		return nil, fmt.Errorf("elasticsearch search request failed: %w", err)
	}

	// 3. port.Document にマッピング
	documents := make([]port.Document, 0, len(res.Hits.Hits))
	for _, hit := range res.Hits.Hits {
		var fields map[string]interface{}
		// hit.Source_ は json.RawMessage なのでアンマーシャルが必要
		if err := json.Unmarshal(hit.Source_, &fields); err != nil {
			// 一つのドキュメントのパースに失敗しても、他のドキュメントは返し続ける (要件に応じて変更)
			// 本番ではロギングを推奨
			continue
		}

		var score float32
		if hit.Score_ != nil {
			// hit.Score_ は *float64 なので、float32 にキャスト
			score = float32(*hit.Score_)
		}

		var id string
		if hit.Id_ != nil {
			id = *hit.Id_
		}
		documents = append(documents, port.Document{
			ID:     id,
			Score:  score,
			Fields: fields,
		})
	}

	return documents, nil
}
