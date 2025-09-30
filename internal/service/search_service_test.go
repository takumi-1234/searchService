package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/takumi-1234/searchService/internal/port"
)

// MockSearchRepository は port.SearchRepository のモックです。
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

func TestSearchService_Search(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	t.Run("正常系: キーワード検索のみ実行", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger)

		keywordDocs := []port.Document{
			{ID: "doc-1", Score: 0.9, Fields: map[string]interface{}{"title": "Keyword Only"}},
		}
		params := port.SearchParams{IndexName: "test-index", QueryText: "test"}

		mockRepo.On("KeywordSearch", mock.Anything, params.IndexName, params.QueryText).Return(keywordDocs, nil).Once()

		result, err := service.Search(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(1), result.TotalCount)
		assert.Equal(t, "doc-1", result.Documents[0].ID)
		mockRepo.AssertExpectations(t)
	})

	t.Run("正常系: ハイブリッド検索で結果がマージ・ソートされる", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger)

		keywordDocs := []port.Document{
			{ID: "doc-1", Score: 0.6}, // 重複
			{ID: "doc-2", Score: 0.8}, // キーワードのみ
		}
		vectorDocs := []port.Document{
			{ID: "doc-1", Score: 0.8}, // 重複
			{ID: "doc-3", Score: 0.9}, // ベクトルのみ
		}
		vector := []float32{0.1, 0.2}
		params := port.SearchParams{IndexName: "test-index", QueryText: "test", QueryVector: vector}

		mockRepo.On("KeywordSearch", mock.Anything, params.IndexName, params.QueryText).Return(keywordDocs, nil).Once()
		mockRepo.On("VectorSearch", mock.Anything, params.IndexName, params.QueryVector).Return(vectorDocs, nil).Once()

		result, err := service.Search(ctx, params)

		assert.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, int64(3), result.TotalCount)

		// ソート順を検証
		// doc-1 (0.6 + 0.8 = 1.4)
		// doc-3 (0.9)
		// doc-2 (0.8)
		assert.Equal(t, "doc-1", result.Documents[0].ID)
		assert.InDelta(t, 1.4, result.Documents[0].Score, 0.01)

		assert.Equal(t, "doc-3", result.Documents[1].ID)
		assert.InDelta(t, 0.9, result.Documents[1].Score, 0.01)

		assert.Equal(t, "doc-2", result.Documents[2].ID)
		assert.InDelta(t, 0.8, result.Documents[2].Score, 0.01)

		mockRepo.AssertExpectations(t)
	})

	t.Run("エラー系: キーワード検索でエラーが発生", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger)

		expectedErr := errors.New("keyword search error")
		vector := []float32{0.1, 0.2}
		params := port.SearchParams{IndexName: "test-index", QueryText: "test", QueryVector: vector}

		mockRepo.On("KeywordSearch", mock.Anything, params.IndexName, params.QueryText).Return(nil, expectedErr).Once()
		// VectorSearchは成功するかもしれないが、errgroupにより全体がエラーになる
		mockRepo.On("VectorSearch", mock.Anything, params.IndexName, params.QueryVector).Return([]port.Document{}, nil).Maybe()

		result, err := service.Search(ctx, params)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Nil(t, result)
		mockRepo.AssertExpectations(t)
	})

	t.Run("エラー系: ベクトル検索でエラーが発生", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger)

		expectedErr := errors.New("vector search error")
		vector := []float32{0.1, 0.2}
		params := port.SearchParams{IndexName: "test-index", QueryText: "test", QueryVector: vector}

		mockRepo.On("KeywordSearch", mock.Anything, params.IndexName, params.QueryText).Return([]port.Document{}, nil).Once()
		mockRepo.On("VectorSearch", mock.Anything, params.IndexName, params.QueryVector).Return(nil, expectedErr).Once()

		result, err := service.Search(ctx, params)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Nil(t, result)
		mockRepo.AssertExpectations(t)
	})
}
