package inmemory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/takumi-1234/searchService/internal/port"
)

// ErrIndexAlreadyExists is returned when an index with the same name already exists.
var ErrIndexAlreadyExists = errors.New("in-memory index already exists")

// ErrIndexNotFound is returned when an operation references a non-existent index.
var ErrIndexNotFound = errors.New("in-memory index not found")

// DocumentSeed represents a document that can be injected into the in-memory repository.
type DocumentSeed struct {
	ID           string
	Fields       map[string]interface{}
	KeywordScore float32
	VectorScore  float32
}

type indexData struct {
	fields map[string]struct{}
	docs   map[string]*DocumentSeed
}

// Repository provides a thread-safe in-memory implementation of port.SearchRepository.
type Repository struct {
	mu      sync.RWMutex
	indexes map[string]*indexData
}

// NewRepository creates an empty in-memory repository.
func NewRepository() *Repository {
	return &Repository{
		indexes: make(map[string]*indexData),
	}
}

// CreateIndex registers a new index definition.
func (r *Repository) CreateIndex(_ context.Context, params port.CreateIndexParams) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.indexes[params.IndexName]; ok {
		return ErrIndexAlreadyExists
	}

	fieldSet := make(map[string]struct{}, len(params.Fields))
	for _, field := range params.Fields {
		fieldSet[field.Name] = struct{}{}
	}

	r.indexes[params.IndexName] = &indexData{
		fields: fieldSet,
		docs:   make(map[string]*DocumentSeed),
	}
	return nil
}

// IndexDocument stores or updates a document inside the selected index.
func (r *Repository) IndexDocument(_ context.Context, params port.IndexDocumentParams) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	idx, ok := r.indexes[params.IndexName]
	if !ok {
		return ErrIndexNotFound
	}

	doc := &DocumentSeed{
		ID:           params.DocumentID,
		Fields:       copyFields(params.Fields),
		KeywordScore: 1.0,
		VectorScore:  float32(defaultVectorScore(params.Vector)),
	}

	idx.docs[params.DocumentID] = doc
	return nil
}

// KeywordSearch returns documents ranked by the pre-configured keyword score.
func (r *Repository) KeywordSearch(_ context.Context, params port.KeywordSearchParams) ([]port.Document, error) {
	r.mu.RLock()
	idx, ok := r.indexes[params.IndexName]
	r.mu.RUnlock()
	if !ok {
		return nil, ErrIndexNotFound
	}

	matched := make([]*DocumentSeed, 0, len(idx.docs))
	for _, doc := range idx.docs {
		if matchesFilters(doc.Fields, params.Filters) {
			matched = append(matched, doc)
		}
	}

	sort.Slice(matched, func(i, j int) bool {
		if params.Sort != nil {
			return compareByField(matched[i], matched[j], params.Sort.Field, params.Sort.Order)
		}
		if matched[i].KeywordScore == matched[j].KeywordScore {
			return matched[i].ID < matched[j].ID
		}
		return matched[i].KeywordScore > matched[j].KeywordScore
	})

	limit := len(matched)
	if params.PageSize > 0 && params.PageSize < limit {
		limit = params.PageSize
	}

	results := make([]port.Document, 0, limit)
	for i := 0; i < limit; i++ {
		doc := matched[i]
		results = append(results, port.Document{
			ID:     doc.ID,
			Score:  doc.KeywordScore,
			Fields: copyFields(doc.Fields),
		})
	}
	return results, nil
}

// VectorSearch returns documents ranked by the pre-configured vector score.
func (r *Repository) VectorSearch(_ context.Context, params port.VectorSearchParams) ([]port.Document, error) {
	r.mu.RLock()
	idx, ok := r.indexes[params.IndexName]
	r.mu.RUnlock()
	if !ok {
		return nil, ErrIndexNotFound
	}

	matched := make([]*DocumentSeed, 0, len(idx.docs))
	for _, doc := range idx.docs {
		if matchesFilters(doc.Fields, params.Filters) {
			matched = append(matched, doc)
		}
	}

	sort.Slice(matched, func(i, j int) bool {
		if matched[i].VectorScore == matched[j].VectorScore {
			return matched[i].ID < matched[j].ID
		}
		return matched[i].VectorScore > matched[j].VectorScore
	})

	limit := len(matched)
	if params.PageSize > 0 && params.PageSize < limit {
		limit = params.PageSize
	}

	results := make([]port.Document, 0, limit)
	for i := 0; i < limit; i++ {
		doc := matched[i]
		results = append(results, port.Document{
			ID:     doc.ID,
			Score:  doc.VectorScore,
			Fields: copyFields(doc.Fields),
		})
	}
	return results, nil
}

// DeleteDocument removes a document from the index.
func (r *Repository) DeleteDocument(_ context.Context, indexName, documentID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	idx, ok := r.indexes[indexName]
	if !ok {
		return ErrIndexNotFound
	}
	delete(idx.docs, documentID)
	return nil
}

// SeedDocuments injects pre-defined documents into an index.
func (r *Repository) SeedDocuments(indexName string, docs []DocumentSeed) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	idx, ok := r.indexes[indexName]
	if !ok {
		return ErrIndexNotFound
	}

	for _, doc := range docs {
		copyDoc := doc
		copyDoc.Fields = copyFields(doc.Fields)
		idx.docs[doc.ID] = &copyDoc
	}
	return nil
}

func copyFields(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return nil
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func matchesFilters(fields map[string]interface{}, filters []port.SearchFilter) bool {
	for _, filter := range filters {
		value, ok := fields[filter.Field]
		if !ok {
			return false
		}
		if !evaluateFilter(value, filter.Operator, filter.Value) {
			return false
		}
	}
	return true
}

func evaluateFilter(fieldValue interface{}, operator string, expected interface{}) bool {
	switch operator {
	case "eq":
		return compare(fieldValue, expected) == 0
	case "neq":
		return compare(fieldValue, expected) != 0
	case "gt":
		return compare(fieldValue, expected) > 0
	case "lt":
		return compare(fieldValue, expected) < 0
	default:
		return false
	}
}

func compare(a, b interface{}) int {
	switch va := a.(type) {
	case string:
		vb := fmt.Sprintf("%v", b)
		switch {
		case va < vb:
			return -1
		case va > vb:
			return 1
		default:
			return 0
		}
	case time.Time:
		vb, ok := b.(time.Time)
		if !ok {
			return 1
		}
		if va.Before(vb) {
			return -1
		}
		if va.After(vb) {
			return 1
		}
		return 0
	default:
		fa, ok := toFloat64(a)
		if !ok {
			return 1
		}
		fb, ok := toFloat64(b)
		if !ok {
			return 1
		}
		switch {
		case fa < fb:
			return -1
		case fa > fb:
			return 1
		default:
			return 0
		}
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch value := v.(type) {
	case int:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case float32:
		return float64(value), true
	case float64:
		return value, true
	default:
		return 0, false
	}
}

func compareByField(a, b *DocumentSeed, field, order string) bool {
	va, okA := a.Fields[field]
	vb, okB := b.Fields[field]
	if !okA && !okB {
		return a.ID < b.ID
	}
	if !okA {
		return false
	}
	if !okB {
		return true
	}

	cmp := compare(va, vb)
	if order == "desc" {
		cmp = -cmp
	}
	if cmp == 0 {
		return a.ID < b.ID
	}
	return cmp < 0
}

func defaultVectorScore(vector []float32) float64 {
	if len(vector) == 0 {
		return 1
	}
	var sum float64
	for _, v := range vector {
		sum += float64(v)
	}
	return sum / float64(len(vector))
}
