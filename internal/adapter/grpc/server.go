package grpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"go.uber.org/zap"

	searchv1 "github.com/takumi-1234/searchService/gen/proto/search/v1"
	"github.com/takumi-1234/searchService/internal/port"
)

// Server は gRPC サーバーの実装です。
type Server struct {
	searchv1.UnimplementedSearchServiceServer
	svc    port.SearchService
	logger *zap.Logger
}

// NewServer は新しい Server インスタンスを生成します。
func NewServer(svc port.SearchService, logger *zap.Logger) *Server {
	return &Server{
		svc:    svc,
		logger: logger,
	}
}

// SearchDocuments はドキュメント検索のgRPCリクエストを処理します。
func (s *Server) SearchDocuments(ctx context.Context, req *searchv1.SearchDocumentsRequest) (*searchv1.SearchDocumentsResponse, error) {
	if req.GetIndexName() == "" {
		s.logger.Warn("index_name is empty")
		return nil, status.Error(codes.InvalidArgument, "index_name is a required field")
	}

	params := port.SearchParams{
		IndexName: req.GetIndexName(),
		QueryText: req.GetQueryText(),
	}

	searchResult, err := s.svc.Search(ctx, params)
	if err != nil {
		s.logger.Error("error from search service", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to search documents")
	}

	// port.SearchResult を gRPC レスポンスに変換
	results := make([]*searchv1.SearchResult, len(searchResult.Documents))
	for i, doc := range searchResult.Documents {
		fields, err := structpb.NewStruct(doc.Fields)
		if err != nil {
			s.logger.Error("failed to convert fields to protobuf struct", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to process document fields")
		}
		results[i] = &searchv1.SearchResult{
			DocumentId: doc.ID,
			Score:      doc.Score,
			Fields:     fields,
		}
	}

	return &searchv1.SearchDocumentsResponse{
		Results:    results,
		TotalCount: searchResult.TotalCount,
	}, nil
}
