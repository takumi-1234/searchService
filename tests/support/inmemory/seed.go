package inmemory

import (
	"context"
	"errors"

	"github.com/ttokunaga-jp/searchService/internal/port"
)

// SeedSampleData populates the repository with a deterministic set of data that
// can be used across E2E tests and load testing harnesses.
func SeedSampleData(repo *Repository) error {
	ctx := context.Background()

	err := repo.CreateIndex(ctx, port.CreateIndexParams{
		IndexName: "books",
		Fields: []port.FieldDefinition{
			{Name: "title", Type: port.FieldTypeText},
			{Name: "category", Type: port.FieldTypeKeyword},
			{Name: "ranking", Type: port.FieldTypeFloat},
			{Name: "published_year", Type: port.FieldTypeInteger},
		},
		VectorConfig: &port.VectorConfig{
			Dimension: 3,
			Distance:  port.VectorDistanceCosine,
		},
	})
	if err != nil && !errors.Is(err, ErrIndexAlreadyExists) {
		return err
	}

	return repo.SeedDocuments("books", []DocumentSeed{
		{
			ID: "book-1",
			Fields: map[string]interface{}{
				"title":          "Advanced Calculus",
				"category":       "math",
				"ranking":        98.0,
				"published_year": 2022,
			},
			KeywordScore: 9.0,
			VectorScore:  2.0,
		},
		{
			ID: "book-2",
			Fields: map[string]interface{}{
				"title":          "Linear Algebra Essentials",
				"category":       "math",
				"ranking":        92.0,
				"published_year": 2021,
			},
			KeywordScore: 8.0,
			VectorScore:  8.0,
		},
		{
			ID: "book-3",
			Fields: map[string]interface{}{
				"title":          "World History Overview",
				"category":       "history",
				"ranking":        85.0,
				"published_year": 2019,
			},
			KeywordScore: 4.0,
			VectorScore:  1.0,
		},
	})
}
