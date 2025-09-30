package service

import (
	"context"

	"go.uber.org/zap"

	"github.com/takumi-1234/searchService/internal/port"
)

type searchService struct {
	repo   port.SearchRepository
	logger *zap.Logger
}

// NewSearchService は searchService の新しいインスタンスを生成します。
func NewSearchService(repo port.SearchRepository, logger *zap.Logger) port.SearchService {
	return &searchService{
		repo:   repo,
		logger: logger,
	}
}

// Search はドキュメントを検索するビジネスロジックを実行します。
func (s *searchService) Search(ctx context.Context, params port.SearchParams) (*port.SearchResult, error) {
	s.logger.Info("starting search process",
		zap.String("index_name", params.IndexName),
		zap.String("query", params.QueryText),
	)

	documents, err := s.repo.KeywordSearch(ctx, params.IndexName, params.QueryText)
	if err != nil {
		s.logger.Error("failed to perform keyword search in repository", zap.Error(err))
		return nil, err // エラーは適切にラップされるべきだが、ここではそのまま返す
	}

	result := &port.SearchResult{
		TotalCount: int64(len(documents)),
		Documents:  make([]port.Document, len(documents)),
	}

	// port.Document (repository) を port.Document (service) に変換
	for i, doc := range documents {
		result.Documents[i] = port.Document(doc)
	}

	s.logger.Info("search process finished successfully",
		zap.Int64("hit_count", result.TotalCount),
	)

	return result, nil
}
