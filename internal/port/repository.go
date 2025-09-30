package port

import "context"

// Documentは検索結果のドキュメントを表します。
type Document struct {
	ID     string
	Score  float32
	Fields map[string]interface{}
}

// SearchRepositoryはデータストアへのアクセスを抽象化します。
type SearchRepository interface {
	KeywordSearch(ctx context.Context, indexName, query string) ([]Document, error)
}
