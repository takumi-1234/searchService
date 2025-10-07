package port

import "context"

// SearchParamsは検索のパラメータです。
type SearchParams struct {
	IndexName   string
	QueryText   string
	QueryVector []float32
}

// SearchResultはビジネスロジックの検索結果です。
type SearchResult struct {
	TotalCount int64
	Documents  []Document
}

// SearchServiceはビジネスロジックのインターフェースです。
type SearchService interface {
	Search(ctx context.Context, params SearchParams) (*SearchResult, error)
	IndexDocument(ctx context.Context, params IndexDocumentParams) error
	DeleteDocument(ctx context.Context, indexName, documentID string) error
	CreateIndex(ctx context.Context, params CreateIndexParams) error
}
