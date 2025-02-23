package mh

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

// Finder - interface for search (implementation in mongo.Collection).
type Finder interface {
	Find(ctx context.Context, filter any, opts ...*options.FindOptions) (cur *mongo.Cursor, err error)
}

// DefaultParallel - the default maximum number of parallel operations.
// It makes sense to set the maximum number of parallel operations based
// on the Find batch size, which defaults to 101 documents.
// https://www.mongodb.com/docs/v5.0/reference/method/cursor.batchSize/
const DefaultParallel = 101

// ParallelFind performs a search and parallel processing of mongo results.
// The result is a non-sorted slice of values.
// fError - error handling function. If nil is returned, the error is ignored. Can be nil.
func ParallelFind[T any](
	ctx context.Context, limit int,
	res *[]T,
	fError func(index int, err error) error,
	finder Finder,
	filter any,
	opts ...*options.FindOptions,
) (err error) {
	if err = parallelFindHelper(ctx, limit, func(value T) {
		*res = append(*res, value)
	}, fError, finder, filter, opts...); err != nil {
		return fmt.Errorf("parallel find: %w", err)
	}

	return nil
}

// ParallelFindPtr performs a search and parallel processing of mongo results.
// The result is a non-sorted slice of pointers.
// fError - error handling function. If nil is returned, the error is ignored. Can be nil.
// WARNING: If this function is used, it is likely necessary to get rid of pointers
// and use ParallelFind instead.
func ParallelFindPtr[T any](
	ctx context.Context, limit int,
	res *[]*T,
	fError func(index int, err error) error,
	finder Finder,
	filter any,
	opts ...*options.FindOptions,
) (err error) {
	if err = parallelFindHelper(ctx, limit, func(value T) {
		*res = append(*res, &value)
	}, fError, finder, filter, opts...); err != nil {
		return fmt.Errorf("parallel find ptr: %w", err)
	}

	return nil
}

func parallelFindHelper[T any](ctx context.Context, limit int,
	fAdd func(value T),
	fError func(index int, err error) error,
	finder Finder,
	filter any,
	opts ...*options.FindOptions,
) (err error) {
	if limit <= 0 {
		limit = DefaultParallel
	}

	resChan := make(chan T, limit)
	var wg sync.WaitGroup
	wg.Add(2) //nolint:mnd // 2 goroutines

	go func() {
		defer wg.Done()
		for value := range resChan {
			fAdd(value)
		}
	}()

	go func() {
		defer wg.Done()
		defer close(resChan)

		err = ParallelFindFunc(ctx, limit, func(_ int, value T) error {
			select {
			case resChan <- value:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, fError, finder, filter, opts...)
	}()

	wg.Wait()

	if err != nil {
		return fmt.Errorf("parallel find helper: %w", err)
	}

	return nil
}

// ParallelFindFunc performs a search and parallel processing of mongo results using a function.
// fAdd - function to add to the result.
// fError - error handling function. If nil is returned, the error is ignored. Can be nil.
func ParallelFindFunc[T any](
	ctx context.Context, limit int,
	fAdd func(index int, value T) error,
	fError func(index int, err error) error,
	finder Finder,
	filter any,
	opts ...*options.FindOptions,
) (err error) {
	cur, err := finder.Find(ctx, filter, opts...)
	if err != nil {
		return fmt.Errorf("parallel find func: %w", err)
	}

	return ParallelDecode(ctx, cur, limit, fAdd, fError)
}

// ParallelDecode performs parallel processing of mongo cursor elements and closes it.
// fAdd - function to add to the result.
// fError - error handling function. If nil is returned, the error is ignored.
func ParallelDecode[T any](
	ctx context.Context,
	cur *mongo.Cursor,
	limit int,
	fAdd func(index int, value T) error,
	fError func(index int, err error) error,
) (err error) {
	if limit <= 0 {
		limit = DefaultParallel
	}

	defer func() {
		err = errors.Join(err, cur.Close(ctx))
	}()

	var (
		errGroup errgroup.Group
		index    int
	)
	errGroup.SetLimit(limit)

	for cur.Next(ctx) {
		var (
			indexGroup = index
			buf        *[]byte
		)

		if len(cur.Current) <= maxBufferSize {
			buf, _ = bufferPool.Get().(*[]byte)
			if cap(*buf) >= len(cur.Current) {
				*buf = (*buf)[:len(cur.Current)]
			} else {
				b := make([]byte, len(cur.Current))
				buf = &b
			}
		} else {
			b := make([]byte, len(cur.Current))
			buf = &b
		}

		copy(*buf, cur.Current)

		errGroup.Go(func() error {
			if len(*buf) <= maxBufferSize {
				defer bufferPool.Put(buf)
			}

			var value T
			if errBson := bson.Unmarshal(*buf, &value); errBson != nil {
				if fError == nil {
					return fmt.Errorf("parallel decode unmarshal: %w", errBson)
				}
				return fError(indexGroup, errBson)
			}

			if errAdd := fAdd(indexGroup, value); errAdd != nil {
				return errAdd
			}

			return nil
		})

		index++
	}

	if err = errGroup.Wait(); err != nil {
		return fmt.Errorf("parallel decode: %w", err)
	}
	if err = cur.Err(); err != nil {
		return fmt.Errorf("parallel decode cursor: %w", err)
	}

	return nil
}

const (
	// defaultBufferSize - default buffer size for the cursor.
	defaultBufferSize = 1 << 10 // 1 KB
	// maxBufferSize - maximum buffer size for the cursor.
	maxBufferSize = 1 << 20 // 1 MB
)

var bufferPool = sync.Pool{ //nolint:gochecknoglobals // ok for the pool
	New: func() any {
		b := make([]byte, 0, defaultBufferSize)
		return &b
	},
}
