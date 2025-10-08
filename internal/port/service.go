package port

import "context"

// SearchParamsは検索のパラメータです。
type SearchParams struct {
	IndexName   string
	QueryText   string
	QueryVector []float32
	Filters     []SearchFilter
	Sort        *SearchSort
	PageSize    int
	PageToken   string
}

// SearchFilterは検索フィルタ条件を表します。
type SearchFilter struct {
	Field    string
	Operator string
	Value    interface{}
}

// SearchSortは検索のソート条件を表します。
type SearchSort struct {
	Field string
	Order string
}

// SearchResultはビジネスロジックの検索結果です。
type SearchResult struct {
	TotalCount    int64
	Documents     []Document
	NextPageToken string
}

// SearchServiceはビジネスロジックのインターフェースです。
type SearchService interface {
	Search(ctx context.Context, params SearchParams) (*SearchResult, error)
	IndexDocument(ctx context.Context, params IndexDocumentParams) error
	DeleteDocument(ctx context.Context, indexName, documentID string) error
	CreateIndex(ctx context.Context, params CreateIndexParams) error
}
