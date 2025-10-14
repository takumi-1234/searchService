package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/ttokunaga-jp/searchService/internal/port"
)

// MockSearchRepository は port.SearchRepository のモック。
type MockSearchRepository struct {
	mock.Mock
}

var defaultWeights = HybridWeights{Keyword: 0.5, Vector: 0.5}

func (m *MockSearchRepository) KeywordSearch(ctx context.Context, params port.KeywordSearchParams) ([]port.Document, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]port.Document), args.Error(1)
}

func (m *MockSearchRepository) VectorSearch(ctx context.Context, params port.VectorSearchParams) ([]port.Document, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]port.Document), args.Error(1)
}

func (m *MockSearchRepository) DeleteDocument(ctx context.Context, indexName, documentID string) error {
	args := m.Called(ctx, indexName, documentID)
	return args.Error(0)
}

func (m *MockSearchRepository) IndexDocument(ctx context.Context, params port.IndexDocumentParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *MockSearchRepository) CreateIndex(ctx context.Context, params port.CreateIndexParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func TestSearchService_CreateIndex(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	validParams := port.CreateIndexParams{
		IndexName: "test-index",
		Fields: []port.FieldDefinition{
			{Name: "content", Type: port.FieldTypeText},
			{Name: "published_at", Type: port.FieldTypeDate},
		},
		VectorConfig: &port.VectorConfig{
			Dimension: 3,
			Distance:  port.VectorDistanceCosine,
		},
	}

	t.Run("success", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger, defaultWeights)

		mockRepo.On("CreateIndex", ctx, validParams).Return(nil).Once()

		err := service.CreateIndex(ctx, validParams)
		assert.NoError(t, err)

		mockRepo.AssertExpectations(t)
	})

	t.Run("validation error prevents repository call", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger, defaultWeights)

		invalidParams := port.CreateIndexParams{
			IndexName: "",
			VectorConfig: &port.VectorConfig{
				Dimension: 3,
				Distance:  port.VectorDistanceCosine,
			},
		}

		err := service.CreateIndex(ctx, invalidParams)
		assert.Error(t, err)

		mockRepo.AssertNotCalled(t, "CreateIndex", mock.Anything, mock.Anything)
	})

	t.Run("repository error is returned", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger, defaultWeights)

		expectedErr := errors.New("repo failure")
		mockRepo.On("CreateIndex", ctx, validParams).Return(expectedErr).Once()

		err := service.CreateIndex(ctx, validParams)
		assert.ErrorIs(t, err, expectedErr)

		mockRepo.AssertExpectations(t)
	})
}

func TestSearchService_Search(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	t.Run("キーワード検索のみ実行されるケース", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger, defaultWeights)

		repoDocs := []port.Document{
			{ID: "doc-1", Score: 0.9, Fields: map[string]interface{}{"title": "Test"}},
		}
		params := port.SearchParams{IndexName: "test-index", QueryText: "test query"}
		expectedKeywordParams := port.KeywordSearchParams{
			IndexName: "test-index",
			Query:     "test query",
			PageSize:  defaultPageSize + 1,
		}

		mockRepo.On("KeywordSearch", mock.Anything, expectedKeywordParams).Return(repoDocs, nil).Once()
		// VectorSearch は呼ばれないことを確認するため、呼ばれたらテスト失敗にする（Times(0) は設定できないため AssertNotCalled を使用）
		// 実行
		result, err := service.Search(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(1), result.TotalCount)
		assert.Equal(t, "doc-1", result.Documents[0].ID)
		mockRepo.AssertExpectations(t)
		mockRepo.AssertNotCalled(t, "VectorSearch", mock.Anything, mock.Anything)
	})

	t.Run("キーワード検索とベクトル検索の両方が実行され、正しくマージ・ソートされるケース", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger, defaultWeights)

		// キーワード側の結果
		keywordDocs := []port.Document{
			{ID: "A", Score: 1.0, Fields: map[string]interface{}{"title": "A-key"}},
			{ID: "B", Score: 0.4, Fields: map[string]interface{}{"title": "B-key"}},
			{ID: "C", Score: 0.1, Fields: map[string]interface{}{"title": "C-key"}},
		}
		// ベクトル側の結果（A が重複、C は新規）
		vectorDocs := []port.Document{
			{ID: "A", Score: 0.2, Fields: map[string]interface{}{"title": "A-vec"}},
			{ID: "C", Score: 0.9, Fields: map[string]interface{}{"title": "C-vec"}},
			{ID: "D", Score: 0.1, Fields: map[string]interface{}{"title": "D-vec"}},
		}

		params := port.SearchParams{
			IndexName:   "test-index",
			QueryText:   "hybrid query",
			QueryVector: []float32{0.1, 0.2},
		}

		mockRepo.On("KeywordSearch", mock.Anything, port.KeywordSearchParams{
			IndexName: "test-index",
			Query:     "hybrid query",
			PageSize:  defaultPageSize + 1,
		}).Return(keywordDocs, nil).Once()
		mockRepo.On("VectorSearch", mock.Anything, port.VectorSearchParams{
			IndexName: "test-index",
			Vector:    []float32{0.1, 0.2},
			PageSize:  defaultPageSize + 1,
		}).Return(vectorDocs, nil).Once()

		result, err := service.Search(ctx, params)
		assert.NoError(t, err)
		if assert.NotNil(t, result) {
			assert.Equal(t, int64(4), result.TotalCount)

			expectedOrder := []string{"A", "C", "B", "D"}
			for i, id := range expectedOrder {
				if assert.True(t, i < len(result.Documents)) {
					assert.Equal(t, id, result.Documents[i].ID)
				}
			}

			assert.InDelta(t, 0.5625, result.Documents[0].Score, 1e-6)
			assert.InDelta(t, 0.5, result.Documents[1].Score, 1e-6)
			assert.InDelta(t, 0.166666, result.Documents[2].Score, 1e-6)
			assert.InDelta(t, 0.0, result.Documents[3].Score, 1e-6)
		}

		mockRepo.AssertExpectations(t)
	})

	t.Run("重み付けの変更によって検索結果順位が変わること", func(t *testing.T) {
		params := port.SearchParams{
			IndexName:   "test-index",
			QueryText:   "weighted query",
			QueryVector: []float32{0.5, 0.6},
		}
		expectedKeywordParams := port.KeywordSearchParams{
			IndexName: "test-index",
			Query:     "weighted query",
			PageSize:  defaultPageSize + 1,
		}
		expectedVectorParams := port.VectorSearchParams{
			IndexName: "test-index",
			Vector:    []float32{0.5, 0.6},
			PageSize:  defaultPageSize + 1,
		}

		keywordDocs := []port.Document{
			{ID: "doc-keyword", Score: 0.9, Fields: map[string]interface{}{"title": "keyword-dominant"}},
			{ID: "doc-vector", Score: 0.4, Fields: map[string]interface{}{"title": "vector-dominant"}},
		}
		vectorDocs := []port.Document{
			{ID: "doc-keyword", Score: 0.2, Fields: map[string]interface{}{"title": "keyword-dominant-vector"}},
			{ID: "doc-vector", Score: 0.8, Fields: map[string]interface{}{"title": "vector-dominant-vector"}},
		}

		setupService := func(weights HybridWeights) (*MockSearchRepository, port.SearchService) {
			mockRepo := new(MockSearchRepository)
			mockRepo.On("KeywordSearch", mock.Anything, expectedKeywordParams).Return(keywordDocs, nil).Once()
			mockRepo.On("VectorSearch", mock.Anything, expectedVectorParams).Return(vectorDocs, nil).Once()
			return mockRepo, NewSearchService(mockRepo, logger, weights)
		}

		t.Run("keyword優先の重みではdoc-keywordが先頭になる", func(t *testing.T) {
			mockRepo, svc := setupService(HybridWeights{Keyword: 0.8, Vector: 0.2})

			result, err := svc.Search(ctx, params)
			assert.NoError(t, err)
			if assert.NotNil(t, result) {
				assert.Equal(t, int64(2), result.TotalCount)
				assert.Equal(t, "doc-keyword", result.Documents[0].ID)
				assert.Equal(t, "doc-vector", result.Documents[1].ID)
				assert.Greater(t, result.Documents[0].Score, result.Documents[1].Score)
			}

			mockRepo.AssertExpectations(t)
		})

		t.Run("vector優先の重みではdoc-vectorが先頭になる", func(t *testing.T) {
			mockRepo, svc := setupService(HybridWeights{Keyword: 0.2, Vector: 0.8})

			result, err := svc.Search(ctx, params)
			assert.NoError(t, err)
			if assert.NotNil(t, result) {
				assert.Equal(t, int64(2), result.TotalCount)
				assert.Equal(t, "doc-vector", result.Documents[0].ID)
				assert.Equal(t, "doc-keyword", result.Documents[1].ID)
				assert.Greater(t, result.Documents[0].Score, result.Documents[1].Score)
			}

			mockRepo.AssertExpectations(t)
		})
	})

	t.Run("エラー系: 片方の検索でエラーが発生するケース", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger, defaultWeights)

		keywordDocs := []port.Document{
			{ID: "doc-1", Score: 0.5, Fields: map[string]interface{}{"title": "Test"}},
		}
		expectedErr := errors.New("vector search failed")

		params := port.SearchParams{
			IndexName:   "test-index",
			QueryText:   "will error",
			QueryVector: []float32{0.1},
		}

		mockRepo.On("KeywordSearch", mock.Anything, port.KeywordSearchParams{
			IndexName: "test-index",
			Query:     "will error",
			PageSize:  defaultPageSize + 1,
		}).Return(keywordDocs, nil).Once()
		mockRepo.On("VectorSearch", mock.Anything, port.VectorSearchParams{
			IndexName: "test-index",
			Vector:    []float32{0.1},
			PageSize:  defaultPageSize + 1,
		}).Return(nil, expectedErr).Once()

		result, err := service.Search(ctx, params)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, expectedErr, err)

		mockRepo.AssertExpectations(t)
	})
}
