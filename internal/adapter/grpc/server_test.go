package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	searchv1 "github.com/takumi-1234/searchService/gen/proto/search/v1"
	"github.com/takumi-1234/searchService/internal/port"
)

// MockSearchService は port.SearchService のモックです。
type MockSearchService struct {
	mock.Mock
}

func (m *MockSearchService) Search(ctx context.Context, params port.SearchParams) (*port.SearchResult, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*port.SearchResult), args.Error(1)
}

func TestServer_SearchDocuments(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	t.Run("正常系: 検索が成功する", func(t *testing.T) {
		mockSvc := new(MockSearchService)
		server := NewServer(mockSvc, logger)

		req := &searchv1.SearchDocumentsRequest{
			IndexName: "test-index",
			QueryText: "test query",
		}
		params := port.SearchParams{
			IndexName: req.GetIndexName(),
			QueryText: req.GetQueryText(),
		}
		serviceResult := &port.SearchResult{
			TotalCount: 1,
			Documents: []port.Document{
				{ID: "doc-1", Score: 0.9, Fields: map[string]interface{}{"title": "Result Title"}},
			},
		}

		mockSvc.On("Search", ctx, params).Return(serviceResult, nil).Once()

		res, err := server.SearchDocuments(ctx, req)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, int64(1), res.TotalCount)
		assert.Equal(t, "doc-1", res.Results[0].DocumentId)
		title, _ := res.Results[0].Fields.AsMap()["title"].(string)
		assert.Equal(t, "Result Title", title)

		mockSvc.AssertExpectations(t)
	})

	t.Run("エラー系: index_name が空", func(t *testing.T) {
		server := NewServer(nil, logger)
		req := &searchv1.SearchDocumentsRequest{IndexName: ""}

		res, err := server.SearchDocuments(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, res)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("エラー系: サービス層がエラーを返す", func(t *testing.T) {
		mockSvc := new(MockSearchService)
		server := NewServer(mockSvc, logger)

		req := &searchv1.SearchDocumentsRequest{
			IndexName: "test-index",
			QueryText: "error query",
		}
		params := port.SearchParams{
			IndexName: req.GetIndexName(),
			QueryText: req.GetQueryText(),
		}
		expectedErr := errors.New("service error")

		mockSvc.On("Search", ctx, params).Return(nil, expectedErr).Once()

		res, err := server.SearchDocuments(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, res)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.Internal, st.Code())

		mockSvc.AssertExpectations(t)
	})
}
