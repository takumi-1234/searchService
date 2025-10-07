package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/takumi-1234/searchService/internal/port"
)

// MockSearchRepository は port.SearchRepository のモック。
type MockSearchRepository struct {
	mock.Mock
}

func (m *MockSearchRepository) KeywordSearch(ctx context.Context, indexName, query string) ([]port.Document, error) {
	args := m.Called(ctx, indexName, query)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]port.Document), args.Error(1)
}

func (m *MockSearchRepository) VectorSearch(ctx context.Context, indexName string, vector []float32) ([]port.Document, error) {
	args := m.Called(ctx, indexName, vector)
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
		service := NewSearchService(mockRepo, logger)

		mockRepo.On("CreateIndex", ctx, validParams).Return(nil).Once()

		err := service.CreateIndex(ctx, validParams)
		assert.NoError(t, err)

		mockRepo.AssertExpectations(t)
	})

	t.Run("validation error prevents repository call", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger)

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
		service := NewSearchService(mockRepo, logger)

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
		service := NewSearchService(mockRepo, logger)

		repoDocs := []port.Document{
			{ID: "doc-1", Score: 0.9, Fields: map[string]interface{}{"title": "Test"}},
		}
		params := port.SearchParams{IndexName: "test-index", QueryText: "test query", QueryVector: nil}

		mockRepo.On("KeywordSearch", mock.Anything, params.IndexName, params.QueryText).Return(repoDocs, nil).Once()
		// VectorSearch は呼ばれないことを確認するため、呼ばれたらテスト失敗にする（Times(0) は設定できないため AssertNotCalled を使用）
		// 実行
		result, err := service.Search(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(1), result.TotalCount)
		assert.Equal(t, "doc-1", result.Documents[0].ID)
		mockRepo.AssertExpectations(t)
		mockRepo.AssertNotCalled(t, "VectorSearch", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("キーワード検索とベクトル検索の両方が実行され、正しくマージ・ソートされるケース", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger)

		// キーワード側の結果
		keywordDocs := []port.Document{
			{ID: "A", Score: 0.6, Fields: map[string]interface{}{"title": "A-key"}},
			{ID: "B", Score: 0.4, Fields: map[string]interface{}{"title": "B-key"}},
		}
		// ベクトル側の結果（A が重複、C は新規）
		vectorDocs := []port.Document{
			{ID: "A", Score: 0.8, Fields: map[string]interface{}{"title": "A-vec"}},
			{ID: "C", Score: 0.7, Fields: map[string]interface{}{"title": "C-vec"}},
		}

		params := port.SearchParams{
			IndexName:   "test-index",
			QueryText:   "hybrid query",
			QueryVector: []float32{0.1, 0.2},
		}

		mockRepo.On("KeywordSearch", mock.Anything, params.IndexName, params.QueryText).Return(keywordDocs, nil).Once()
		mockRepo.On("VectorSearch", mock.Anything, params.IndexName, params.QueryVector).Return(vectorDocs, nil).Once()

		result, err := service.Search(ctx, params)
		assert.NoError(t, err)
		if assert.NotNil(t, result) {
			assert.Equal(t, int64(3), result.TotalCount)

			// 期待される最終スコア:
			// A: 0.6 + 0.8 = 1.4
			// B: 0.4
			// C: 0.7
			// ソート順: A (1.4), C (0.7), B (0.4)

			// IDs in order:
			assert.Equal(t, "A", result.Documents[0].ID)
			assert.Equal(t, "C", result.Documents[1].ID)
			assert.Equal(t, "B", result.Documents[2].ID)

			// Scores
			assert.InDelta(t, 1.4, result.Documents[0].Score, 1e-6)
			assert.InDelta(t, 0.7, result.Documents[1].Score, 1e-6)
			assert.InDelta(t, 0.4, result.Documents[2].Score, 1e-6)
		}

		mockRepo.AssertExpectations(t)
	})

	t.Run("エラー系: 片方の検索でエラーが発生するケース", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger)

		keywordDocs := []port.Document{
			{ID: "doc-1", Score: 0.5, Fields: map[string]interface{}{"title": "Test"}},
		}
		expectedErr := errors.New("vector search failed")

		params := port.SearchParams{
			IndexName:   "test-index",
			QueryText:   "will error",
			QueryVector: []float32{0.1},
		}

		mockRepo.On("KeywordSearch", mock.Anything, params.IndexName, params.QueryText).Return(keywordDocs, nil).Once()
		mockRepo.On("VectorSearch", mock.Anything, params.IndexName, params.QueryVector).Return(nil, expectedErr).Once()

		result, err := service.Search(ctx, params)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, expectedErr, err)

		mockRepo.AssertExpectations(t)
	})
}
