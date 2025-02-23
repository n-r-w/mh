package mh

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/n-r-w/testdock/v2"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ComplexDocument represents a complex document structure
// to make decoding more CPU-intensive.
type ComplexDocument struct {
	ID            string                `bson:"_id"`
	Name          string                `bson:"name"`
	Description   string                `bson:"description"`
	CreatedAt     time.Time             `bson:"created_at"`
	UpdatedAt     time.Time             `bson:"updated_at"`
	Tags          []string              `bson:"tags"`
	Metadata      map[string]string     `bson:"metadata"`
	NestedObjects []NestedObject        `bson:"nested_objects"`
	Properties    map[string]Properties `bson:"properties"`
	Stats         Stats                 `bson:"stats"`
}

type NestedObject struct {
	ID       string    `bson:"id"`
	Name     string    `bson:"name"`
	Value    float64   `bson:"value"`
	Date     time.Time `bson:"date"`
	SubItems []SubItem `bson:"sub_items"`
}

type SubItem struct {
	Key   string `bson:"key"`
	Value any    `bson:"value"`
}

type Properties struct {
	Type       string         `bson:"type"`
	Value      any            `bson:"value"`
	Attributes map[string]any `bson:"attributes"`
	ValidFrom  time.Time      `bson:"valid_from"`
	ValidTo    time.Time      `bson:"valid_to"`
}

type Stats struct {
	Count       int64     `bson:"count"`
	TotalValue  float64   `bson:"total_value"`
	LastUpdated time.Time `bson:"last_updated"`
	History     []float64 `bson:"history"`
}

func generateComplexDocument(id string) ComplexDocument {
	now := time.Now()
	return ComplexDocument{
		ID:          id,
		Name:        "Test Document " + id,
		Description: "A complex test document with nested structures and arrays",
		CreatedAt:   now,
		UpdatedAt:   now,
		Tags:        []string{"test", "benchmark", "complex", "nested"},
		Metadata: map[string]string{
			"source":      "benchmark",
			"environment": "test",
			"version":     "1.0.0",
		},
		NestedObjects: []NestedObject{
			{
				ID:    "nested1",
				Name:  "Nested Object 1",
				Value: 123.456,
				Date:  now,
				SubItems: []SubItem{
					{Key: "key1", Value: "value1"},
					{Key: "key2", Value: 42},
					{Key: "key3", Value: true},
				},
			},
			{
				ID:    "nested2",
				Name:  "Nested Object 2",
				Value: 789.012,
				Date:  now,
				SubItems: []SubItem{
					{Key: "key4", Value: "value4"},
					{Key: "key5", Value: 84},
					{Key: "key6", Value: false},
				},
			},
		},
		Properties: map[string]Properties{
			"prop1": {
				Type:  "string",
				Value: "test value",
				Attributes: map[string]any{
					"attr1": "value1",
					"attr2": 42,
					"attr3": true,
				},
				ValidFrom: now,
				ValidTo:   now.Add(24 * time.Hour),
			},
			"prop2": {
				Type:  "number",
				Value: 42.0,
				Attributes: map[string]any{
					"attr4": "value4",
					"attr5": 84,
					"attr6": false,
				},
				ValidFrom: now,
				ValidTo:   now.Add(48 * time.Hour),
			},
		},
		Stats: Stats{
			Count:       100,
			TotalValue:  12345.67,
			LastUpdated: now,
			History:     []float64{1.1, 2.2, 3.3, 4.4, 5.5},
		},
	}
}

func setupTestBenchmarkCollection(b *testing.B, db *mongo.Database) *mongo.Collection {
	collection := db.Collection("benchmark_test")

	// Insert test documents
	const docNum = 1000
	docs := make([]any, docNum)
	for i := range docNum {
		docs[i] = generateComplexDocument(fmt.Sprintf("doc_%d", i))
	}

	_, err := collection.InsertMany(context.Background(), docs)
	require.NoError(b, err)

	return collection
}

func BenchmarkMongoDBDecoding(b *testing.B) {
	ctx := context.Background()
	db, _ := testdock.GetMongoDatabase(b, testdock.DefaultMongoDSN)

	collection := setupTestBenchmarkCollection(b, db)

	b.Run("Sequential", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			var results []ComplexDocument
			cursor, err := collection.Find(ctx, bson.D{})
			require.NoError(b, err)
			defer cursor.Close(ctx) //nolint:errcheck // ignore for the purpose of benchmarking

			err = cursor.All(ctx, &results)
			require.NoError(b, err)
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			var results []ComplexDocument
			err := ParallelFind(ctx, DefaultParallel, &results, nil, collection, bson.D{})
			require.NoError(b, err)
		}
	})
}
