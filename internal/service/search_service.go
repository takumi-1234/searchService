package service

import (
	"context"
	"sort"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

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

// Search はハイブリッド検索を実行します。
func (s *searchService) Search(ctx context.Context, params port.SearchParams) (*port.SearchResult, error) {
	s.logger.Info("starting hybrid search process",
		zap.String("index_name", params.IndexName),
		zap.String("query_text", params.QueryText),
		zap.Int("vector_len", len(params.QueryVector)),
	)

	g, gCtx := errgroup.WithContext(ctx)

	var keywordDocs []port.Document
	var vectorDocs []port.Document

	// KeywordSearch を並行実行
	g.Go(func() error {
		var err error
		keywordDocs, err = s.repo.KeywordSearch(gCtx, params.IndexName, params.QueryText)
		if err != nil {
			s.logger.Error("failed to perform keyword search", zap.Error(err))
			return err
		}
		return nil
	})

	// VectorSearch を並行実行 (ベクトルが存在する場合のみ)
	if len(params.QueryVector) > 0 {
		g.Go(func() error {
			var err error
			vectorDocs, err = s.repo.VectorSearch(gCtx, params.IndexName, params.QueryVector)
			if err != nil {
				s.logger.Error("failed to perform vector search", zap.Error(err))
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// スコア統合ロジック
	mergedDocs := make(map[string]port.Document)

	// キーワード検索結果をマージ
	for _, doc := range keywordDocs {
		mergedDocs[doc.ID] = doc
	}

	// ベクトル検索結果をマージ (スコアを加算)
	for _, doc := range vectorDocs {
		if existingDoc, ok := mergedDocs[doc.ID]; ok {
			existingDoc.Score += doc.Score
			mergedDocs[doc.ID] = existingDoc
		} else {
			mergedDocs[doc.ID] = doc
		}
	}

	// マップからスライスに変換
	finalDocs := make([]port.Document, 0, len(mergedDocs))
	for _, doc := range mergedDocs {
		finalDocs = append(finalDocs, doc)
	}

	// スコアの降順でソート
	sort.Slice(finalDocs, func(i, j int) bool {
		return finalDocs[i].Score > finalDocs[j].Score
	})

	result := &port.SearchResult{
		TotalCount: int64(len(finalDocs)),
		Documents:  finalDocs,
	}

	s.logger.Info("hybrid search process finished successfully",
		zap.Int64("hit_count", result.TotalCount),
	)

	return result, nil
}
