package port

import "context"

// SearchParamsは検索のパラメータです。
type SearchParams struct {
	IndexName string
	QueryText string
}

// SearchResultはビジネスロジックの検索結果です。
type SearchResult struct {
	TotalCount int64
	Documents  []Document
}

// SearchServiceはビジネスロジックのインターフェースです。
type SearchService interface {
	Search(ctx context.Context, params SearchParams) (*SearchResult, error)
}
