package service

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/takumi-1234/searchService/internal/port"
)

// searchService は SearchRepository に依存する。
type searchService struct {
	repo   port.SearchRepository
	logger *zap.Logger
}

// NewSearchService は searchService の新しいインスタンスを生成する。
func NewSearchService(repo port.SearchRepository, logger *zap.Logger) port.SearchService {
	return &searchService{
		repo:   repo,
		logger: logger,
	}
}

// CreateIndex は新しい検索インデックスを作成します。
func (s *searchService) CreateIndex(ctx context.Context, params port.CreateIndexParams) error {
	if err := validateCreateIndexParams(params); err != nil {
		return err
	}

	s.logger.Info("creating index",
		zap.String("index_name", params.IndexName),
		zap.Int("field_count", len(params.Fields)),
		zap.Int("vector_dimension", params.VectorConfig.Dimension),
		zap.String("vector_distance", string(params.VectorConfig.Distance)),
	)

	if err := s.repo.CreateIndex(ctx, params); err != nil {
		s.logger.Error("failed to create index", zap.String("index_name", params.IndexName), zap.Error(err))
		return err
	}

	s.logger.Info("index created successfully", zap.String("index_name", params.IndexName))
	return nil
}

// IndexDocument はドキュメントをインデックスに追加または更新します。
func (s *searchService) IndexDocument(ctx context.Context, params port.IndexDocumentParams) error {
	return s.repo.IndexDocument(ctx, params)
}

func (s *searchService) DeleteDocument(ctx context.Context, indexName, documentID string) error {
	return s.repo.DeleteDocument(ctx, indexName, documentID)
}

// Search はキーワード検索とベクトル検索を並列に実行し、スコアを加算してマージ、スコア降順で返却する。
func (s *searchService) Search(ctx context.Context, params port.SearchParams) (*port.SearchResult, error) {
	s.logger.Info("starting search process",
		zap.String("index_name", params.IndexName),
		zap.String("query", params.QueryText),
	)

	var (
		keywordRes []port.Document
		vectorRes  []port.Document
	)

	g, ctx := errgroup.WithContext(ctx)

	// キーワード検索は常に呼ぶ
	g.Go(func() error {
		docs, err := s.repo.KeywordSearch(ctx, params.IndexName, params.QueryText)
		if err != nil {
			s.logger.Error("keyword search failed", zap.Error(err))
			return err
		}
		if docs == nil {
			keywordRes = []port.Document{}
		} else {
			keywordRes = docs
		}
		return nil
	})

	// VectorSearch は QueryVector が与えられている場合のみ並列で呼ぶ
	if len(params.QueryVector) > 0 {
		// capture vector
		vector := params.QueryVector
		g.Go(func() error {
			docs, err := s.repo.VectorSearch(ctx, params.IndexName, vector)
			if err != nil {
				s.logger.Error("vector search failed", zap.Error(err))
				return err
			}
			if docs == nil {
				vectorRes = []port.Document{}
			} else {
				vectorRes = docs
			}
			return nil
		})
	} else {
		// ensure zero value rather than nil for later processing
		vectorRes = []port.Document{}
	}

	// Wait for both goroutines (or the single one + optional vector) to finish
	if err := g.Wait(); err != nil {
		// エラーはそのまま返す（呼び出し側でラップする場合は変更可）
		return nil, err
	}

	// スコア統合ロジック
	merged := make(map[string]port.Document)

	// まずキーワード結果を入れる
	for _, d := range keywordRes {
		// make a copy to avoid aliasing
		merged[d.ID] = port.Document{
			ID:     d.ID,
			Score:  d.Score,
			Fields: d.Fields,
		}
	}

	// ベクトル結果をマージ（存在すればスコアを加算、存在しなければ新規追加）
	for _, vd := range vectorRes {
		if existing, ok := merged[vd.ID]; ok {
			// スコアを加算。Fields は既存（キーワード優先）に残すが、無ければベクトル側を使う。
			existing.Score = existing.Score + vd.Score
			if len(existing.Fields) == 0 {
				existing.Fields = vd.Fields
			}
			merged[vd.ID] = existing
		} else {
			merged[vd.ID] = port.Document{
				ID:     vd.ID,
				Score:  vd.Score,
				Fields: vd.Fields,
			}
		}
	}

	// マップをスライスに変換してソート
	finalDocs := make([]port.Document, 0, len(merged))
	for _, d := range merged {
		finalDocs = append(finalDocs, d)
	}

	// スコア降順ソート（高いものが先）
	sort.Slice(finalDocs, func(i, j int) bool {
		return finalDocs[i].Score > finalDocs[j].Score
	})

	result := &port.SearchResult{
		TotalCount: int64(len(finalDocs)),
		Documents:  finalDocs,
	}

	s.logger.Info("search process finished successfully",
		zap.Int64("hit_count", result.TotalCount),
	)

	return result, nil
}

func validateCreateIndexParams(params port.CreateIndexParams) error {
	if params.IndexName == "" {
		return fmt.Errorf("index name is required")
	}
	if params.VectorConfig == nil {
		return fmt.Errorf("vector config is required")
	}
	if params.VectorConfig.Dimension <= 0 {
		return fmt.Errorf("vector dimension must be positive")
	}
	if !isValidVectorDistance(params.VectorConfig.Distance) {
		return fmt.Errorf("unsupported vector distance: %s", params.VectorConfig.Distance)
	}
	seen := make(map[string]struct{}, len(params.Fields))
	for _, field := range params.Fields {
		if field.Name == "" {
			return fmt.Errorf("field name is required")
		}
		key := strings.ToLower(field.Name)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate field name: %s", field.Name)
		}
		if !isValidFieldType(field.Type) {
			return fmt.Errorf("unsupported field type: %s", field.Type)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func isValidFieldType(fieldType string) bool {
	switch strings.ToLower(fieldType) {
	case port.FieldTypeText,
		port.FieldTypeKeyword,
		port.FieldTypeInteger,
		port.FieldTypeFloat,
		port.FieldTypeBoolean,
		port.FieldTypeDate:
		return true
	default:
		return false
	}
}

func isValidVectorDistance(distance port.VectorDistance) bool {
	switch distance {
	case port.VectorDistanceCosine, port.VectorDistanceEuclid, port.VectorDistanceDot:
		return true
	default:
		return false
	}
}
