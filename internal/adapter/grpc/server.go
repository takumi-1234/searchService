package grpc

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"go.uber.org/zap"

	searchv1 "github.com/ttokunaga-jp/searchService/gen/proto/search/v1"
	"github.com/ttokunaga-jp/searchService/internal/port"
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

	params, err := buildSearchParams(req)
	if err != nil {
		s.logger.Warn("invalid search request", zap.Error(err))
		return nil, status.Error(codes.InvalidArgument, err.Error())
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
		Results:       results,
		TotalCount:    searchResult.TotalCount,
		NextPageToken: searchResult.NextPageToken,
	}, nil
}

func buildSearchParams(req *searchv1.SearchDocumentsRequest) (port.SearchParams, error) {
	params := port.SearchParams{
		IndexName: req.GetIndexName(),
		QueryText: req.GetQueryText(),
		QueryVector: func() []float32 {
			if len(req.GetQueryVector()) == 0 {
				return nil
			}
			vec := make([]float32, len(req.GetQueryVector()))
			copy(vec, req.GetQueryVector())
			return vec
		}(),
		PageSize:  int(req.GetPageSize()),
		PageToken: req.GetPageToken(),
	}

	if params.PageSize < 0 {
		return port.SearchParams{}, fmt.Errorf("page_size must be positive")
	}

	if len(req.GetFilters()) > 0 {
		params.Filters = make([]port.SearchFilter, 0, len(req.GetFilters()))
		for _, f := range req.GetFilters() {
			if f.GetField() == "" {
				return port.SearchParams{}, fmt.Errorf("filter field is required")
			}
			op, err := convertProtoFilterOperator(f.GetOperator())
			if err != nil {
				return port.SearchParams{}, err
			}
			val := interface{}(nil)
			if f.GetValue() != nil {
				val = f.GetValue().AsInterface()
			}
			params.Filters = append(params.Filters, port.SearchFilter{
				Field:    f.GetField(),
				Operator: op,
				Value:    val,
			})
		}
	}

	if req.GetSortBy() != nil {
		if req.GetSortBy().GetField() == "" {
			return port.SearchParams{}, fmt.Errorf("sort field is required")
		}
		sortOrder, err := convertProtoSortOrder(req.GetSortBy().GetOrder())
		if err != nil {
			return port.SearchParams{}, err
		}
		params.Sort = &port.SearchSort{
			Field: req.GetSortBy().GetField(),
			Order: sortOrder,
		}
	}

	return params, nil
}

// CreateIndex は CreateIndex RPC を処理します。
func (s *Server) CreateIndex(ctx context.Context, req *searchv1.CreateIndexRequest) (*searchv1.CreateIndexResponse, error) {
	if req.GetIndexName() == "" {
		s.logger.Warn("index_name is empty")
		return nil, status.Error(codes.InvalidArgument, "index_name is a required field")
	}

	params, err := buildCreateIndexParams(req)
	if err != nil {
		s.logger.Warn("invalid create index request", zap.Error(err))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := s.svc.CreateIndex(ctx, params); err != nil {
		s.logger.Error("failed to create index", zap.String("index_name", params.IndexName), zap.Error(err))
		return nil, status.Error(grpcCodeForCreateIndexError(err), "failed to create index")
	}

	return &searchv1.CreateIndexResponse{
		Success: true,
		Message: fmt.Sprintf("index %s created", params.IndexName),
	}, nil
}

func buildCreateIndexParams(req *searchv1.CreateIndexRequest) (port.CreateIndexParams, error) {
	schema := req.GetSchema()
	if schema == nil {
		return port.CreateIndexParams{}, fmt.Errorf("schema is required")
	}

	fields := make([]port.FieldDefinition, 0, len(schema.GetFields()))
	for _, field := range schema.GetFields() {
		if field.GetName() == "" {
			return port.CreateIndexParams{}, fmt.Errorf("field name is required")
		}
		fieldType, err := convertProtoFieldType(field.GetType())
		if err != nil {
			return port.CreateIndexParams{}, err
		}
		fields = append(fields, port.FieldDefinition{
			Name: field.GetName(),
			Type: fieldType,
		})
	}

	vectorCfg := schema.GetVectorConfig()
	if vectorCfg == nil {
		return port.CreateIndexParams{}, fmt.Errorf("vector_config is required")
	}
	if vectorCfg.GetDimension() <= 0 {
		return port.CreateIndexParams{}, fmt.Errorf("vector dimension must be positive")
	}

	distance, err := convertProtoVectorDistance(vectorCfg.GetDistance())
	if err != nil {
		return port.CreateIndexParams{}, err
	}

	return port.CreateIndexParams{
		IndexName: req.GetIndexName(),
		Fields:    fields,
		VectorConfig: &port.VectorConfig{
			Dimension: int(vectorCfg.GetDimension()),
			Distance:  distance,
		},
	}, nil
}

func convertProtoFieldType(fieldType searchv1.FieldDefinition_FieldType) (string, error) {
	switch fieldType {
	case searchv1.FieldDefinition_FIELD_TYPE_TEXT:
		return port.FieldTypeText, nil
	case searchv1.FieldDefinition_FIELD_TYPE_KEYWORD:
		return port.FieldTypeKeyword, nil
	case searchv1.FieldDefinition_FIELD_TYPE_INTEGER:
		return port.FieldTypeInteger, nil
	case searchv1.FieldDefinition_FIELD_TYPE_FLOAT:
		return port.FieldTypeFloat, nil
	case searchv1.FieldDefinition_FIELD_TYPE_BOOLEAN:
		return port.FieldTypeBoolean, nil
	case searchv1.FieldDefinition_FIELD_TYPE_DATE:
		return port.FieldTypeDate, nil
	default:
		return "", fmt.Errorf("unsupported field type: %s", fieldType.String())
	}
}

func convertProtoVectorDistance(distance searchv1.VectorConfig_Distance) (port.VectorDistance, error) {
	switch distance {
	case searchv1.VectorConfig_DISTANCE_COSINE:
		return port.VectorDistanceCosine, nil
	case searchv1.VectorConfig_DISTANCE_EUCLID:
		return port.VectorDistanceEuclid, nil
	case searchv1.VectorConfig_DISTANCE_DOT_PRODUCT:
		return port.VectorDistanceDot, nil
	default:
		return "", fmt.Errorf("unsupported vector distance: %s", distance.String())
	}
}

func convertProtoFilterOperator(op searchv1.Filter_Operator) (string, error) {
	switch op {
	case searchv1.Filter_OPERATOR_EQUAL:
		return "eq", nil
	case searchv1.Filter_OPERATOR_NOT_EQUAL:
		return "neq", nil
	case searchv1.Filter_OPERATOR_GREATER_THAN:
		return "gt", nil
	case searchv1.Filter_OPERATOR_LESS_THAN:
		return "lt", nil
	case searchv1.Filter_OPERATOR_UNSPECIFIED:
		return "", fmt.Errorf("filter operator is required")
	default:
		return "", fmt.Errorf("unsupported filter operator: %s", op.String())
	}
}

func convertProtoSortOrder(order searchv1.SortBy_Order) (string, error) {
	switch order {
	case searchv1.SortBy_ORDER_UNSPECIFIED, searchv1.SortBy_ORDER_ASC:
		return "asc", nil
	case searchv1.SortBy_ORDER_DESC:
		return "desc", nil
	default:
		return "", fmt.Errorf("unsupported sort order: %s", order.String())
	}
}

func grpcCodeForCreateIndexError(err error) codes.Code {
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "already exists") {
		return codes.AlreadyExists
	}
	return codes.Internal
}
