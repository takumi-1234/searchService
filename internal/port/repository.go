package port

import "context"

// Documentは検索結果のドキュメントを表します。
type Document struct {
	ID     string
	Score  float32
	Fields map[string]interface{}
}

// IndexDocumentParams はドキュメントをインデックスする際のパラメータです。
type IndexDocumentParams struct {
	IndexName  string
	DocumentID string
	Fields     map[string]interface{}
	Vector     []float32
}

// SearchRepositoryはデータストアへのアクセスを抽象化します。
type SearchRepository interface {
	KeywordSearch(ctx context.Context, indexName, query string) ([]Document, error)
	VectorSearch(ctx context.Context, indexName string, vector []float32) ([]Document, error)
	IndexDocument(ctx context.Context, params IndexDocumentParams) error
	DeleteDocument(ctx context.Context, indexName, documentID string) error
}
