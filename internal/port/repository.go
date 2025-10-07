package port

import "context"

// Documentは検索結果のドキュメントを表します。
type Document struct {
	ID     string
	Score  float32
	Fields map[string]interface{}
}

// CreateIndexParams はインデックス作成時のパラメータを表します。
type CreateIndexParams struct {
	IndexName    string
	Fields       []FieldDefinition
	VectorConfig *VectorConfig
}

// FieldDefinition はインデックス内の単一フィールドの定義です。
type FieldDefinition struct {
	Name string
	Type string
}

const (
	// FieldTypeText は全文検索用の text フィールドを示します。
	FieldTypeText = "text"
	// FieldTypeKeyword は keyword フィールドを示します。
	FieldTypeKeyword = "keyword"
	// FieldTypeInteger は integer フィールドを示します。
	FieldTypeInteger = "integer"
	// FieldTypeFloat は float フィールドを示します。
	FieldTypeFloat = "float"
	// FieldTypeBoolean は boolean フィールドを示します。
	FieldTypeBoolean = "boolean"
	// FieldTypeDate は date フィールドを示します。
	FieldTypeDate = "date"
)

// VectorConfig はベクトル検索用の設定を表します。
type VectorConfig struct {
	Dimension int
	Distance  VectorDistance
}

// VectorDistance は Qdrant で利用可能な距離関数を示します。
type VectorDistance string

const (
	// VectorDistanceCosine はコサイン距離を示します。
	VectorDistanceCosine VectorDistance = "cosine"
	// VectorDistanceEuclid はユークリッド距離を示します。
	VectorDistanceEuclid VectorDistance = "euclid"
	// VectorDistanceDot はドット積を示します。
	VectorDistanceDot VectorDistance = "dot"
)

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
	CreateIndex(ctx context.Context, params CreateIndexParams) error
}
