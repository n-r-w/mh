# MongoDB Helpers

A set of useful functions for working with MongoDB.

[![Go Reference](https://pkg.go.dev/badge/github.com/n-r-w/mh.svg)](https://pkg.go.dev/github.com/n-r-w/mh)
![CI Status](https://github.com/n-r-w/mh/actions/workflows/go.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/n-r-w/mh)](https://goreportcard.com/report/github.com/n-r-w/mh)

## Parallel Data Decoding

Parallel Data Decoding allows decoding data from a cursor in separate threads, which speeds up the processing of data for complex structures.

### Benchmark results

```txt
BenchmarkMongoDBDecoding/Sequential-32                63          16592513 ns/op         8728794 B/op     209168 allocs/op
BenchmarkMongoDBDecoding/Parallel-32                 208           5427981 ns/op         9055874 B/op     211637 allocs/op
```

### Usage

```go
import (
  "github.com/n-r-w/mh"
  "go.mongodb.org/mongo-driver/mongo/options"
)

// docoding to slice
var results []ComplexDocument
err = mh.ParallelFind(ctx, mh.DefaultParallel, &results, nil, collection, bson.D{}, options.Find().SetBatchSize(100))

// decoding to slice of pointers
var resultsPtr []*ComplexDocument
err = mh.ParallelFindPtr(ctx, mh.DefaultParallel, &resultsPtr, nil, collection, bson.D{})

// decoding using function
err = mh.ParallelFindFunc(ctx, mh.DefaultParallel, 
  func(index int, value ComplexDocument) error {
    // data processing function
  }, 
  func(index int, err error) error {
    // error processing function
  }
  , collection, bson.D{})
```
