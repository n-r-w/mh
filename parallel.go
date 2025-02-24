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

// DefaultParallelDecode - the default maximum number of parallel operations for decoding.
// It makes sense to set the maximum number of parallel operations based
// on the Find batch size, which defaults to 101 documents.
// https://www.mongodb.com/docs/v5.0/reference/method/cursor.batchSize/
const DefaultParallelDecode = 101

// ParallelFind performs a search and parallel processing of mongo results.
// The result is a non-sorted slice of values.
// limitDecode - maximum number of parallel operations for decoding.
// limitParallel - maximum number of parallel operations for the search.
// fError - error handling function. If nil is returned, the error is ignored. Can be nil.
// filter - bson.D or []bson.D.
func ParallelFind[T any](
	ctx context.Context,
	limitDecode, limitParallel int,
	res *[]T,
	fError func(err error) error,
	finder Finder,
	filter any,
	opts ...*options.FindOptions,
) (err error) {
	if err = parallelFindHelper(ctx, limitDecode, limitParallel,
		func(value T) {
			*res = append(*res, value)
		}, fError, finder, filter, opts...); err != nil {
		return fmt.Errorf("parallel find: %w", err)
	}

	return nil
}

// ParallelFindPtr performs a search and parallel processing of mongo results.
// The result is a non-sorted slice of pointers.
// limitDecode - maximum number of parallel operations for decoding.
// limitParallel - maximum number of parallel operations for the search.
// fError - error handling function. If nil is returned, the error is ignored. Can be nil.
// filter - bson.D or []bson.D.
// WARNING: If this function is used, it is likely necessary to get rid of pointers
// and use ParallelFind instead.
func ParallelFindPtr[T any](
	ctx context.Context,
	limitDecode, limitParallel int,
	res *[]*T,
	fError func(err error) error,
	finder Finder,
	filter any,
	opts ...*options.FindOptions,
) (err error) {
	if err = parallelFindHelper(ctx, limitDecode, limitParallel,
		func(value T) {
			*res = append(*res, &value)
		}, fError, finder, filter, opts...); err != nil {
		return fmt.Errorf("parallel find ptr: %w", err)
	}

	return nil
}

func parallelFindHelper[T any](ctx context.Context,
	limitDecode, limitParallel int,
	fAdd func(value T),
	fError func(err error) error,
	finder Finder,
	filter any,
	opts ...*options.FindOptions,
) (err error) {
	if limitDecode <= 0 {
		return errors.New("limitDecode must be greater than 0")
	}
	if limitParallel <= 0 {
		return errors.New("limitParallel must be greater than 0")
	}

	resChan := make(chan T, limitDecode)
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

		err = ParallelFindFunc(ctx, limitDecode, limitParallel,
			func(value T) error {
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
// limitDecode - maximum number of parallel operations for decoding.
// limitParallel - maximum number of parallel operations for the search.
// fAdd - function to add to the result.
// fError - error handling function. If nil is returned, the error is ignored. Can be nil.
// filter - bson.D or []bson.D.
func ParallelFindFunc[T any](
	ctx context.Context,
	limitDecode, limitParallel int,
	fAdd func(value T) error,
	fError func(err error) error,
	finder Finder,
	filter any,
	opts ...*options.FindOptions,
) (err error) {
	// if filter contains []bson.D - use parallel query
	filters, ok := filter.([]bson.D)
	if !ok {
		// single bson.D
		cur, err := finder.Find(ctx, filter, opts...)
		if err != nil {
			return fmt.Errorf("parallel find func: %w", err)
		}

		return ParallelDecode(ctx, cur, limitDecode, fAdd, fError)
	}

	errGroup, errCtx := errgroup.WithContext(ctx)
	errGroup.SetLimit(limitParallel)

	for _, f := range filters {
		errGroup.Go(func() error {
			cur, err := finder.Find(errCtx, f, opts...)
			if err != nil {
				return fmt.Errorf("parallel find func: %w", err)
			}

			return ParallelDecode(errCtx, cur, limitDecode, fAdd, fError)
		})
	}

	if err := errGroup.Wait(); err != nil {
		return fmt.Errorf("parallel find func: %w", err)
	}

	return nil
}

// ParallelDecode performs parallel processing of mongo cursor elements and closes it.
// fAdd - function to add to the result.
// fError - error handling function. If nil is returned, the error is ignored.
func ParallelDecode[T any](
	ctx context.Context,
	cur *mongo.Cursor,
	limitDecode int,
	fAdd func(value T) error,
	fError func(err error) error,
) (err error) {
	if limitDecode <= 0 {
		limitDecode = DefaultParallelDecode
	}

	defer func() {
		err = errors.Join(err, cur.Close(ctx))
	}()

	var errGroup errgroup.Group
	errGroup.SetLimit(limitDecode)

	for cur.Next(ctx) {
		var buf *[]byte

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
				return fError(errBson)
			}

			if errAdd := fAdd(value); errAdd != nil {
				return errAdd
			}

			return nil
		})
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
