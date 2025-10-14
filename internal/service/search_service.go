package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/ttokunaga-jp/searchService/internal/port"
)

// HybridWeights はハイブリッド検索における各スコアの重みを表現します。
type HybridWeights struct {
	Keyword float64
	Vector  float64
}

// searchService は SearchRepository に依存する。
type searchService struct {
	repo    port.SearchRepository
	logger  *zap.Logger
	weights HybridWeights
}

const (
	defaultPageSize      = 10
	minNormalizedScore   = 1e-6
	oneMinusMinScoreBase = 1 - minNormalizedScore
)

// NewSearchService は searchService の新しいインスタンスを生成する。
func NewSearchService(repo port.SearchRepository, logger *zap.Logger, weights HybridWeights) port.SearchService {
	return &searchService{
		repo:    repo,
		logger:  logger,
		weights: sanitizeHybridWeights(weights),
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

// Search はキーワード検索とベクトル検索を並列に実行し、スコアを加算してマージ後カーソルに従って返却する。
func (s *searchService) Search(ctx context.Context, params port.SearchParams) (*port.SearchResult, error) {
	s.logger.Info("starting search process",
		zap.String("index_name", params.IndexName),
		zap.String("query", params.QueryText),
	)

	offset := 0
	if params.PageToken != "" {
		decodedOffset, err := decodePageToken(params.PageToken)
		if err != nil {
			s.logger.Warn("invalid page token supplied",
				zap.String("page_token", params.PageToken),
				zap.Error(err),
			)
			return nil, fmt.Errorf("invalid page token: %w", err)
		}
		offset = decodedOffset
	}

	pageSize := params.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}

	fetchLimit := pageSize + offset + 1

	var (
		keywordRes []port.Document
		vectorRes  []port.Document
	)

	g, ctx := errgroup.WithContext(ctx)

	// キーワード検索は常に呼ぶ
	g.Go(func() error {
		docs, err := s.repo.KeywordSearch(ctx, port.KeywordSearchParams{
			IndexName: params.IndexName,
			Query:     params.QueryText,
			Filters:   params.Filters,
			Sort:      params.Sort,
			PageSize:  fetchLimit,
		})
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
			docs, err := s.repo.VectorSearch(ctx, port.VectorSearchParams{
				IndexName: params.IndexName,
				Vector:    vector,
				Filters:   params.Filters,
				PageSize:  fetchLimit,
			})
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

	keywordNormalized := normalizeScores(keywordRes)
	vectorNormalized := normalizeScores(vectorRes)

	merged := make(map[string]port.Document, len(keywordRes)+len(vectorRes))

	for _, d := range keywordRes {
		score := weightedScore(keywordNormalized[d.ID], vectorNormalized[d.ID], s.weights)
		merged[d.ID] = port.Document{
			ID:     d.ID,
			Score:  score,
			Fields: d.Fields,
		}
	}

	for _, vd := range vectorRes {
		score := weightedScore(keywordNormalized[vd.ID], vectorNormalized[vd.ID], s.weights)
		if existing, ok := merged[vd.ID]; ok {
			existing.Score = score
			if len(existing.Fields) == 0 {
				existing.Fields = vd.Fields
			}
			merged[vd.ID] = existing
			continue
		}
		merged[vd.ID] = port.Document{
			ID:     vd.ID,
			Score:  score,
			Fields: vd.Fields,
		}
	}

	// マップをスライスに変換してソート
	finalDocs := make([]port.Document, 0, len(merged))
	for _, d := range merged {
		finalDocs = append(finalDocs, d)
	}

	// スコア降順ソート（高いものが先）、スコアが同じ場合はID昇順で安定させる
	sort.Slice(finalDocs, func(i, j int) bool {
		if math.Abs(float64(finalDocs[i].Score-finalDocs[j].Score)) < 1e-9 {
			return finalDocs[i].ID < finalDocs[j].ID
		}
		return finalDocs[i].Score > finalDocs[j].Score
	})

	start := offset
	if start > len(finalDocs) {
		start = len(finalDocs)
	}

	remaining := finalDocs[start:]
	var pageDocs []port.Document
	if len(remaining) > 0 {
		limit := pageSize
		if len(remaining) < limit {
			limit = len(remaining)
		}
		pageDocs = make([]port.Document, limit)
		copy(pageDocs, remaining[:limit])
	} else {
		pageDocs = []port.Document{}
	}

	nextPageToken := ""
	if len(remaining) > len(pageDocs) {
		nextOffset := offset + len(pageDocs)
		token, err := encodePageToken(nextOffset)
		if err != nil {
			s.logger.Error("failed to encode next page token", zap.Error(err))
			return nil, fmt.Errorf("failed to encode next page token: %w", err)
		}
		nextPageToken = token
	}

	result := &port.SearchResult{
		TotalCount:    int64(len(pageDocs)),
		Documents:     pageDocs,
		NextPageToken: nextPageToken,
	}

	s.logger.Info("search process finished successfully",
		zap.Int64("returned_count", result.TotalCount),
		zap.Int("offset", offset),
		zap.Int("page_size", pageSize),
		zap.Bool("has_next_page", nextPageToken != ""),
	)

	return result, nil
}

type pageCursor struct {
	Offset int `json:"offset"`
}

func decodePageToken(token string) (int, error) {
	payload, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0, fmt.Errorf("decode base64: %w", err)
	}

	var cursor pageCursor
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return 0, fmt.Errorf("unmarshal json: %w", err)
	}
	if cursor.Offset < 0 {
		return 0, fmt.Errorf("offset must not be negative")
	}

	return cursor.Offset, nil
}

func encodePageToken(offset int) (string, error) {
	cursor := pageCursor{
		Offset: offset,
	}
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("marshal json: %w", err)
	}
	return base64.StdEncoding.EncodeToString(payload), nil
}

func normalizeScores(docs []port.Document) map[string]float64 {
	normalized := make(map[string]float64, len(docs))
	if len(docs) == 0 {
		return normalized
	}

	minScore := docs[0].Score
	maxScore := docs[0].Score
	for i := 1; i < len(docs); i++ {
		score := docs[i].Score
		if score < minScore {
			minScore = score
		}
		if score > maxScore {
			maxScore = score
		}
	}

	if maxScore == minScore {
		for _, doc := range docs {
			normalized[doc.ID] = 1.0
		}
		return normalized
	}

	rangeScore := float64(maxScore - minScore)
	if rangeScore == 0 {
		for _, doc := range docs {
			normalized[doc.ID] = 1.0
		}
		return normalized
	}

	for _, doc := range docs {
		value := (float64(doc.Score) - float64(minScore)) / rangeScore
		value = math.Max(0, math.Min(1, value))
		normalized[doc.ID] = minNormalizedScore + oneMinusMinScoreBase*value
	}
	return normalized
}

func weightedScore(keywordScore, vectorScore float64, weights HybridWeights) float32 {
	return float32(keywordScore*weights.Keyword + vectorScore*weights.Vector)
}

func sanitizeHybridWeights(weights HybridWeights) HybridWeights {
	if math.IsNaN(weights.Keyword) {
		weights.Keyword = 0
	}
	if math.IsNaN(weights.Vector) {
		weights.Vector = 0
	}
	if weights.Keyword < 0 {
		weights.Keyword = 0
	}
	if weights.Vector < 0 {
		weights.Vector = 0
	}
	if weights.Keyword == 0 && weights.Vector == 0 {
		weights.Keyword = 0.5
		weights.Vector = 0.5
	}
	return weights
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
