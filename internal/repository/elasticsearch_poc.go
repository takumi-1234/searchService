package repository

import (
	"context"
	"fmt"

	"github.com/takumi-1234/searchService/internal/port"
)

// elasticsearchPocRepository は SearchRepository のPoC向け実装です。
type elasticsearchPocRepository struct{}

// NewElasticsearchPocRepository は elasticsearchPocRepository の新しいインスタンスを生成します。
func NewElasticsearchPocRepository() port.SearchRepository {
	return &elasticsearchPocRepository{}
}

// KeywordSearch はハードコードされた検索結果を返します。
func (r *elasticsearchPocRepository) KeywordSearch(ctx context.Context, indexName, query string) ([]port.Document, error) {
	// PoCのため、常に固定の正常なレスポンスを返す
	return []port.Document{
		{
			ID:    "doc-123",
			Score: 0.99,
			Fields: map[string]interface{}{
				"title": "PoC Document",
			},
		},
	}, nil
}

func (r *elasticsearchPocRepository) VectorSearch(ctx context.Context, indexName string, vector []float32) ([]port.Document, error) {
	return nil, fmt.Errorf("VectorSearch is not implemented for elasticsearchPocRepository")
}
