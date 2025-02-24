package mh

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/n-r-w/testdock/v2"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestParallelFind(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, _ := testdock.GetMongoDatabase(t, testdock.DefaultMongoDSN)

	// prepare test collection
	collection := db.Collection("test")

	const docNum = 1000
	for range docNum {
		_, err := collection.InsertOne(ctx, bson.D{{Key: "a", Value: 1}})
		require.NoError(t, err)
	}
	_, err := collection.InsertOne(ctx, bson.D{{Key: "b", Value: 2}})
	require.NoError(t, err)

	t.Run("ParallelFind succeeds", func(t *testing.T) {
		t.Parallel()

		var res []struct {
			KeyA *int `bson:"a"`
			KeyB *int `bson:"b"`
		}

		require.NoError(t, ParallelFind(ctx, 2, 2, &res,
			func(err error) error {
				t.Fail()
				return nil
			},
			collection,
			bson.D{{Key: "a", Value: 1}},
			options.Find().SetBatchSize(100),
		))
		require.Len(t, res, docNum)

		res = res[:0]
		require.NoError(t, ParallelFind(ctx, 2, 2, &res, nil,
			collection,
			bson.D{{Key: "b", Value: 2}},
		))
		require.Len(t, res, 1)
	})

	t.Run("ParallelFind errors", func(t *testing.T) {
		t.Parallel()

		var res []struct {
			KeyA *string `bson:"a"`
			KeyB *int    `bson:"b"`
		}

		var errCount atomic.Int32

		require.NoError(t, ParallelFind(ctx, 2, 2, &res,
			func(err error) error {
				errCount.Add(1)
				return nil
			},
			collection,
			bson.D{{Key: "a", Value: 1}},
		))
		require.Empty(t, res)
		require.Equal(t, int32(docNum), errCount.Load())
	})

	t.Run("ParallelFindPtr succeeds", func(t *testing.T) {
		t.Parallel()

		var res []*struct {
			KeyA *int `bson:"a"`
			KeyB *int `bson:"b"`
		}

		require.NoError(t, ParallelFindPtr(ctx, 2, 2, &res,
			func(err error) error {
				t.Fail()
				return nil
			},
			collection,
			bson.D{{Key: "a", Value: 1}},
		))
		require.Len(t, res, docNum)

		res = res[:0]
		require.NoError(t, ParallelFindPtr(ctx, 2, 2, &res, nil,
			collection,
			bson.D{{Key: "b", Value: 2}},
		))
		require.Len(t, res, 1)
	})

	t.Run("ParallelFindPtr errors", func(t *testing.T) {
		t.Parallel()

		var res []*struct {
			KeyA *string `bson:"a"`
			KeyB *int    `bson:"b"`
		}

		var errCount atomic.Int32

		require.NoError(t, ParallelFindPtr(ctx, 2, 2, &res,
			func(err error) error {
				errCount.Add(1)
				return nil
			},
			collection,
			bson.D{{Key: "a", Value: 1}},
		))
		require.Empty(t, res)
		require.Equal(t, int32(docNum), errCount.Load())
	})

	t.Run("ParallelFind chunk", func(t *testing.T) {
		t.Parallel()

		var res []struct {
			KeyA *int `bson:"a"`
			KeyB *int `bson:"b"`
		}

		require.NoError(t, ParallelFind(ctx, 2, 2, &res,
			func(err error) error {
				t.Fail()
				return nil
			},
			collection,
			[]bson.D{
				{{Key: "a", Value: 1}},
				{{Key: "b", Value: 2}},
			},
			options.Find().SetBatchSize(100),
		))
		require.Len(t, res, docNum+1)
	})
}

// TestBigDocument check the correctness of work with the buffer.
func TestBigDocument(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, _ := testdock.GetMongoDatabase(t, testdock.DefaultMongoDSN)

	// prepare test collection
	collection := db.Collection("test_big")

	// create document with size more than allowed in sync.Pool
	var value1 []byte
	for len(value1) < maxBufferSize {
		value1 = append(value1, uuid.New().String()...)
	}
	_, err := collection.InsertOne(ctx, bson.D{{Key: "a", Value: value1}})
	require.NoError(t, err)

	// create document with size more than default sync.Pool buffer size
	var value2 []byte
	for len(value2) < maxBufferSize/2 {
		value2 = append(value2, uuid.New().String()...)
	}
	_, err = collection.InsertOne(ctx, bson.D{{Key: "a", Value: value2}})
	require.NoError(t, err)

	var res []struct {
		Value []byte `bson:"a"`
	}

	require.NoError(t, ParallelFind(ctx, 2, 2, &res,
		func(err error) error {
			t.Fail()
			return nil
		},
		collection,
		bson.D{},
	))
	require.Len(t, res, 2)
	if len(res[0].Value) > len(res[1].Value) {
		require.Equal(t, value1, res[0].Value)
		require.Equal(t, value2, res[1].Value)
	} else {
		require.Equal(t, value2, res[0].Value)
		require.Equal(t, value1, res[1].Value)
	}
}
