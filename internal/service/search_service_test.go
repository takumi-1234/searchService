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

func TestSearchService_Search(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	t.Run("正常系: 検索が成功する", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger)

		repoDocs := []port.Document{
			{ID: "doc-1", Score: 0.9, Fields: map[string]interface{}{"title": "Test"}},
		}
		params := port.SearchParams{IndexName: "test-index", QueryText: "test query"}

		mockRepo.On("KeywordSearch", ctx, params.IndexName, params.QueryText).Return(repoDocs, nil).Once()

		result, err := service.Search(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(1), result.TotalCount)
		assert.Equal(t, "doc-1", result.Documents[0].ID)
		mockRepo.AssertExpectations(t)
	})

	t.Run("エラー系: リポジトリがエラーを返す", func(t *testing.T) {
		mockRepo := new(MockSearchRepository)
		service := NewSearchService(mockRepo, logger)

		expectedErr := errors.New("repository error")
		params := port.SearchParams{IndexName: "test-index", QueryText: "error query"}

		mockRepo.On("KeywordSearch", ctx, params.IndexName, params.QueryText).Return(nil, expectedErr).Once()

		result, err := service.Search(ctx, params)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, expectedErr, err)
		mockRepo.AssertExpectations(t)
	})
}
