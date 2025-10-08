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
	"google.golang.org/protobuf/types/known/structpb"

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

func (m *MockSearchService) IndexDocument(ctx context.Context, params port.IndexDocumentParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *MockSearchService) DeleteDocument(ctx context.Context, indexName, documentID string) error {
	args := m.Called(ctx, indexName, documentID)
	return args.Error(0)
}

func (m *MockSearchService) CreateIndex(ctx context.Context, params port.CreateIndexParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
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
			PageSize:  int(req.GetPageSize()),
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
			PageSize:  int(req.GetPageSize()),
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

	t.Run("正常系: フィルタとソート、ページサイズを変換できる", func(t *testing.T) {
		mockSvc := new(MockSearchService)
		server := NewServer(mockSvc, logger)

		req := &searchv1.SearchDocumentsRequest{
			IndexName: "advanced-index",
			QueryText: "advanced query",
			QueryVector: []float32{
				0.1, 0.2,
			},
			PageSize: 5,
			Filters: []*searchv1.Filter{
				{
					Field:    "category",
					Operator: searchv1.Filter_OPERATOR_EQUAL,
					Value:    structpb.NewStringValue("math"),
				},
				{
					Field:    "score",
					Operator: searchv1.Filter_OPERATOR_GREATER_THAN,
					Value:    structpb.NewNumberValue(80),
				},
			},
			SortBy: &searchv1.SortBy{
				Field: "published_at",
				Order: searchv1.SortBy_ORDER_DESC,
			},
		}

		params := port.SearchParams{
			IndexName: "advanced-index",
			QueryText: "advanced query",
			QueryVector: []float32{
				0.1, 0.2,
			},
			PageSize: 5,
			Filters: []port.SearchFilter{
				{Field: "category", Operator: "eq", Value: "math"},
				{Field: "score", Operator: "gt", Value: float64(80)},
			},
			Sort: &port.SearchSort{
				Field: "published_at",
				Order: "desc",
			},
		}

		mockSvc.On("Search", ctx, params).Return(&port.SearchResult{}, nil).Once()

		res, err := server.SearchDocuments(ctx, req)
		assert.NoError(t, err)
		assert.NotNil(t, res)

		mockSvc.AssertExpectations(t)
	})
}

func TestServer_CreateIndex(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	t.Run("正常系: インデックス作成が成功する", func(t *testing.T) {
		mockSvc := new(MockSearchService)
		server := NewServer(mockSvc, logger)

		req := &searchv1.CreateIndexRequest{
			IndexName: "test-index",
			Schema: &searchv1.IndexSchema{
				Fields: []*searchv1.FieldDefinition{
					{Name: "content", Type: searchv1.FieldDefinition_FIELD_TYPE_TEXT},
					{Name: "published_at", Type: searchv1.FieldDefinition_FIELD_TYPE_DATE},
				},
				VectorConfig: &searchv1.VectorConfig{
					Dimension: 3,
					Distance:  searchv1.VectorConfig_DISTANCE_COSINE,
				},
			},
		}

		expectedParams := port.CreateIndexParams{
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

		mockSvc.On("CreateIndex", ctx, expectedParams).Return(nil).Once()

		res, err := server.CreateIndex(ctx, req)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.True(t, res.Success)
		assert.Contains(t, res.Message, "test-index")

		mockSvc.AssertExpectations(t)
	})

	t.Run("エラー系: index_name が空", func(t *testing.T) {
		mockSvc := new(MockSearchService)
		server := NewServer(mockSvc, logger)
		req := &searchv1.CreateIndexRequest{IndexName: ""}

		res, err := server.CreateIndex(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, res)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())

		mockSvc.AssertNotCalled(t, "CreateIndex", mock.Anything, mock.Anything)
		mockSvc.AssertExpectations(t)
	})

	t.Run("エラー系: サービス層がエラーを返す", func(t *testing.T) {
		mockSvc := new(MockSearchService)
		server := NewServer(mockSvc, logger)

		req := &searchv1.CreateIndexRequest{
			IndexName: "existing-index",
			Schema: &searchv1.IndexSchema{
				Fields: []*searchv1.FieldDefinition{
					{Name: "content", Type: searchv1.FieldDefinition_FIELD_TYPE_TEXT},
				},
				VectorConfig: &searchv1.VectorConfig{
					Dimension: 3,
					Distance:  searchv1.VectorConfig_DISTANCE_COSINE,
				},
			},
		}

		expectedParams := port.CreateIndexParams{
			IndexName: "existing-index",
			Fields: []port.FieldDefinition{
				{Name: "content", Type: port.FieldTypeText},
			},
			VectorConfig: &port.VectorConfig{
				Dimension: 3,
				Distance:  port.VectorDistanceCosine,
			},
		}

		expectedErr := errors.New("index already exists")
		mockSvc.On("CreateIndex", ctx, expectedParams).Return(expectedErr).Once()

		res, err := server.CreateIndex(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, res)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.AlreadyExists, st.Code())

		mockSvc.AssertExpectations(t)
	})
}
