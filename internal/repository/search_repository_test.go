package repository

import (
	"encoding/json"
	"testing"

	"github.com/elastic/go-elasticsearch/v9/typedapi/types"
	"github.com/elastic/go-elasticsearch/v9/typedapi/types/enums/sortorder"
	"github.com/qdrant/go-client/qdrant"

	"github.com/ttokunaga-jp/searchService/internal/port"
)

func TestBuildElasticsearchBoolQuery(t *testing.T) {
	t.Parallel()

	params := port.KeywordSearchParams{
		IndexName: "books",
		Query:     "",
		Filters: []port.SearchFilter{
			{Field: "category", Operator: "eq", Value: "math"},
			{Field: "ranking", Operator: "gt", Value: 90},
			{Field: "flagged", Operator: "neq", Value: true},
		},
	}

	boolQuery, err := buildElasticsearchBoolQuery(params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(boolQuery.Must) == 0 {
		t.Fatalf("expected Match clause when query empty")
	}
	if len(boolQuery.Filter) != 2 {
		t.Fatalf("expected 2 filter clauses, got %d", len(boolQuery.Filter))
	}
	if len(boolQuery.MustNot) != 1 {
		t.Fatalf("expected 1 must_not clause, got %d", len(boolQuery.MustNot))
	}
}

func TestBuildElasticsearchTermQuery(t *testing.T) {
	t.Parallel()

	query := buildElasticsearchTermQuery("category", "math")
	if query.Term == nil {
		t.Fatalf("expected term query")
	}
	if query.Term["category"].Value != "math" {
		t.Fatalf("unexpected term value: %+v", query.Term["category"].Value)
	}
}

func TestBuildElasticsearchBoolQueryWithQuery(t *testing.T) {
	t.Parallel()

	boolQuery, err := buildElasticsearchBoolQuery(port.KeywordSearchParams{
		Query: "calculus",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(boolQuery.Must) != 1 {
		t.Fatalf("expected single must clause, got %d", len(boolQuery.Must))
	}
	if boolQuery.Must[0].Match == nil {
		t.Fatalf("expected Match query when query text is provided")
	}
}

func TestBuildElasticsearchBoolQueryNilValueError(t *testing.T) {
	t.Parallel()

	_, err := buildElasticsearchBoolQuery(port.KeywordSearchParams{
		Query: "",
		Filters: []port.SearchFilter{
			{Field: "category", Operator: "eq", Value: nil},
		},
	})
	if err == nil {
		t.Fatal("expected error when eq filter has nil value")
	}

	res, err := buildElasticsearchBoolQuery(port.KeywordSearchParams{
		Query: "",
		Filters: []port.SearchFilter{
			{Field: "category", Operator: "neq", Value: nil},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error for neq nil filter: %v", err)
	}
	if len(res.MustNot) != 1 {
		t.Fatalf("expected MustNot clause for neq filter, got %d", len(res.MustNot))
	}
}

func TestBuildElasticsearchBoolQueryInvalidOperator(t *testing.T) {
	t.Parallel()

	_, err := buildElasticsearchBoolQuery(port.KeywordSearchParams{
		Filters: []port.SearchFilter{
			{Field: "category", Operator: "between", Value: "math"},
		},
	})
	if err == nil {
		t.Fatal("expected error for unsupported operator")
	}
}

func TestBuildElasticsearchSort(t *testing.T) {
	t.Parallel()

	result, err := buildElasticsearchSort(&port.SearchSort{Field: "_score", Order: "desc"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected single sort combination, got %d", len(result))
	}
	scoreSort, ok := result[0].(*types.SortOptions)
	if !ok {
		t.Fatalf("expected SortOptions, got %T", result[0])
	}
	if scoreSort.Score_ == nil || *scoreSort.Score_.Order != sortorder.Desc {
		t.Fatalf("expected _score descending sort, got %+v", scoreSort.Score_)
	}

	result, err = buildElasticsearchSort(&port.SearchSort{Field: "ranking", Order: "asc"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fieldSort, ok := result[0].(*types.SortOptions)
	if !ok {
		t.Fatalf("expected SortOptions, got %T", result[0])
	}
	if _, ok := fieldSort.SortOptions["ranking"]; !ok {
		t.Fatalf("expected field sort for ranking, got %+v", fieldSort.SortOptions)
	}
}

func TestParseSortOrderInvalid(t *testing.T) {
	t.Parallel()

	if _, err := parseSortOrder("sideways"); err == nil {
		t.Fatal("expected error for unsupported sort order")
	}
}

func TestToElasticsearchFieldType(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		port.FieldTypeText:    "text",
		port.FieldTypeKeyword: "keyword",
		port.FieldTypeInteger: "integer",
		port.FieldTypeFloat:   "float",
		port.FieldTypeBoolean: "boolean",
		port.FieldTypeDate:    "date",
	}

	for input, want := range tests {
		got, err := toElasticsearchFieldType(input)
		if err != nil {
			t.Fatalf("unexpected error for %s: %v", input, err)
		}
		if got != want {
			t.Fatalf("expected %s => %s, got %s", input, want, got)
		}
	}

	if _, err := toElasticsearchFieldType("binary"); err == nil {
		t.Fatal("expected error for unsupported field type")
	}
}

func TestToQdrantDistance(t *testing.T) {
	t.Parallel()

	if _, err := toQdrantDistance("invalid"); err == nil {
		t.Fatal("expected error for unsupported distance")
	}

	d, err := toQdrantDistance(port.VectorDistanceCosine)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != qdrant.Distance_Cosine {
		t.Fatalf("unexpected distance: %v", d)
	}
}

func TestBuildQdrantFilter(t *testing.T) {
	t.Parallel()

	filter, err := buildQdrantFilter([]port.SearchFilter{
		{Field: "category", Operator: "eq", Value: "math"},
		{Field: "rating", Operator: "gt", Value: 4.5},
		{Field: "flagged", Operator: "neq", Value: true},
		{Field: "published_year", Operator: "lt", Value: 2025},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(filter.Must) != 3 {
		t.Fatalf("expected 3 Must conditions, got %d", len(filter.Must))
	}
	if len(filter.MustNot) != 1 {
		t.Fatalf("expected 1 MustNot condition, got %d", len(filter.MustNot))
	}

	if _, err := buildQdrantFilter([]port.SearchFilter{
		{Field: "invalid", Operator: "contains", Value: "x"},
	}); err == nil {
		t.Fatal("expected error for unsupported operator")
	}
}

func TestQdrantEqualityConditionNonInteger(t *testing.T) {
	t.Parallel()

	cond, err := qdrantEqualityCondition("rating", 4.5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	field := cond.GetField()
	rng := field.GetRange()
	if rng == nil {
		t.Fatalf("expected range condition for non-integer numeric equality")
	}
	if rng.Gte == nil || *rng.Gte != 4.5 {
		t.Fatalf("expected gte == 4.5, got %+v", rng.Gte)
	}
	if rng.Lte == nil || *rng.Lte != 4.5 {
		t.Fatalf("expected lte == 4.5, got %+v", rng.Lte)
	}
}

func TestQdrantEqualityRangeCondition(t *testing.T) {
	t.Parallel()

	cond := qdrantEqualityRangeCondition("rating", 3.3)
	rng := cond.GetField().GetRange()
	if rng == nil || rng.GetGte() != 3.3 || rng.GetLte() != 3.3 {
		t.Fatalf("expected inclusive range 3.3, got %+v", rng)
	}
}

func TestQdrantEqualityConditionBoolean(t *testing.T) {
	t.Parallel()

	cond, err := qdrantEqualityCondition("flagged", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cond.GetField().GetMatch().GetBoolean() != true {
		t.Fatalf("expected boolean match, got %+v", cond.GetField().GetMatch())
	}
}

func TestQdrantRangeCondition(t *testing.T) {
	t.Parallel()

	greaterCond, err := qdrantRangeCondition("score", 1.5, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := greaterCond.GetField().GetRange().GetGt(); got != 1.5 {
		t.Fatalf("expected gt == 1.5, got %v", got)
	}

	lessCond, err := qdrantRangeCondition("score", 0.5, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := lessCond.GetField().GetRange().GetLt(); got != 0.5 {
		t.Fatalf("expected lt == 0.5, got %v", got)
	}

	if _, err := qdrantRangeCondition("score", map[string]int{}, true); err == nil {
		t.Fatal("expected error for non-numeric range value")
	}
}

func TestBuildElasticsearchRangeQuery(t *testing.T) {
	t.Parallel()

	q, err := buildElasticsearchRangeQuery(port.SearchFilter{Field: "ranking", Operator: "gt", Value: 10})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.Range == nil {
		t.Fatalf("expected range query, got nil")
	}

	if _, err := buildElasticsearchRangeQuery(port.SearchFilter{Field: "ranking", Operator: "eq", Value: 10}); err == nil {
		t.Fatal("expected error for unsupported operator")
	}

	q, err = buildElasticsearchRangeQuery(port.SearchFilter{Field: "ranking", Operator: "lt", Value: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := q.Range["ranking"].(*types.NumberRangeQuery).Lt; got == nil || *got != 2 {
		t.Fatalf("expected lt == 2, got %+v", got)
	}
}

func TestConvertToQdrantMatch(t *testing.T) {
	t.Parallel()

	match, err := convertToQdrantMatch(true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if match.GetBoolean() != true {
		t.Fatalf("expected boolean match, got %+v", match)
	}

	match, err = convertToQdrantMatch(42.0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if match.GetInteger() != 42 {
		t.Fatalf("expected integer coercion, got %d", match.GetInteger())
	}

	if _, err := convertToQdrantMatch([]int{1}); err == nil {
		t.Fatal("expected error for unsupported type")
	}

	if _, err := convertToQdrantMatch(float32(3.14)); err == nil {
		t.Fatal("expected error for non-integer float32")
	}

	match, err = convertToQdrantMatch(float32(8))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if match.GetInteger() != 8 {
		t.Fatalf("expected integer match, got %d", match.GetInteger())
	}
}

func TestCoerceToFloat64(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		value interface{}
		want  float64
	}{
		{"float64", 3.14, 3.14},
		{"string", "2.5", 2.5},
		{"jsonNumber", json.Number("7.5"), 7.5},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := coerceToFloat64(tc.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}

	if _, err := coerceToFloat64([]byte{1}); err == nil {
		t.Fatal("expected error for unsupported type")
	}
}

func TestPayloadToQdrant(t *testing.T) {
	t.Parallel()

	payload, err := payloadToQdrant(map[string]interface{}{
		"title":   "Advanced Calculus",
		"ranking": 98,
		"nested": map[string]interface{}{
			"flag": true,
		},
		"tags": []interface{}{"math", 2022},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(payload) != 4 {
		t.Fatalf("expected 4 entries in payload, got %d", len(payload))
	}
	if _, ok := payload["title"].GetKind().(*qdrant.Value_StringValue); !ok {
		t.Fatal("expected title to be encoded as string value")
	}
	if _, ok := payload["nested"].GetKind().(*qdrant.Value_StructValue); !ok {
		t.Fatal("expected nested to be encoded as struct value")
	}
	if _, ok := payload["tags"].GetKind().(*qdrant.Value_ListValue); !ok {
		t.Fatal("expected tags to be encoded as list value")
	}

	if _, err := payloadToQdrant(map[string]interface{}{
		"invalid": struct{ A int }{A: 1},
	}); err == nil {
		t.Fatal("expected error for unsupported struct type")
	}

	if _, err := payloadToQdrant(map[string]interface{}{
		"bad": map[interface{}]interface{}{1: "value"},
	}); err == nil {
		t.Fatal("expected error for map with non-string keys")
	}

	values, err := payloadToQdrant(map[string]interface{}{
		"numbers": []interface{}{1, 2.5},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := values["numbers"].GetKind().(*qdrant.Value_ListValue); !ok {
		t.Fatalf("expected list value, got %+v", values["numbers"])
	}
}

func TestValueFromQdrant(t *testing.T) {
	t.Parallel()

	val := valueFromQdrant(&qdrant.Value{
		Kind: &qdrant.Value_ListValue{
			ListValue: &qdrant.ListValue{
				Values: []*qdrant.Value{
					{Kind: &qdrant.Value_StringValue{StringValue: "math"}},
					{Kind: &qdrant.Value_IntegerValue{IntegerValue: 2022}},
				},
			},
		},
	})

	list, ok := val.([]interface{})
	if !ok || len(list) != 2 {
		t.Fatalf("expected []interface{} of length 2, got %#v", val)
	}
	if list[0] != "math" || list[1] != int64(2022) {
		t.Fatalf("unexpected list contents: %#v", list)
	}

	val = valueFromQdrant(&qdrant.Value{
		Kind: &qdrant.Value_StructValue{
			StructValue: &qdrant.Struct{
				Fields: map[string]*qdrant.Value{
					"flag": {Kind: &qdrant.Value_BoolValue{BoolValue: true}},
				},
			},
		},
	})
	m, ok := val.(map[string]interface{})
	if !ok || len(m) != 1 || m["flag"] != true {
		t.Fatalf("unexpected struct contents: %#v", val)
	}
}

func TestToQdrantValue(t *testing.T) {
	t.Parallel()

	v, err := toQdrantValue(map[string]interface{}{"nested": []interface{}{"a", 1}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := v.GetKind().(*qdrant.Value_StructValue); !ok {
		t.Fatalf("expected struct value, got %+v", v)
	}

	if _, err := toQdrantValue(make(chan int)); err == nil {
		t.Fatal("expected error for unsupported type")
	}
}
