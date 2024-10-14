# AtlasGurus Batcher

AtlasGurus Batcher is a Go module that provides efficient batching and memoization functionality for various operations, with a specific implementation for GORM database operations. It consists of three main packages and a utility adapter:

1. `batcher`: A generic batching package
2. `gorm`: A GORM-specific implementation using the batcher package
3. `memoize`: A generic, concurrent-safe memoization package
4. GORM v1 to v2 adapter: A utility for converting GORM v1 connections to v2

## Features

- Generic batching functionality with configurable batch size and wait time
- GORM-specific batching for both insert and update operations
- Generic, concurrent-safe memoization with support for size limits and time-based expiration
- Support for both GORM v1 and v2
- Concurrent operation support
- Type-safe implementations using Go generics
- Support for composite primary keys
- Dynamic database connection handling

## Installation

To install AtlasGurus Batcher, use `go get`:

```sh
go get github.com/atlasgurus/batcher
```

## Usage

### Generic Batcher Package

The `batcher` package provides a generic batching mechanism that can be used for any type of operation:

```go
import (
    "context"
    "time"
    "github.com/atlasgurus/batcher/batcher"
)

// Create a new batch processor
processor := batcher.NewBatchProcessor(
    100, // maxBatchSize
    5*time.Second, // maxWaitTime
    context.Background(),
    func(items []YourType) error {
        // Process the batch of items
        return nil
    },
)

// Submit an item for processing
err := processor.SubmitAndWait(item)
if err != nil {
    // Handle error
}
```

### GORM Batcher Package

The `gorm` package provides GORM-specific batching for insert and update operations:

```go
import (
    "context"
    "time"
    "github.com/atlasgurus/batcher/gorm"
    "gorm.io/gorm"
)

// Define a database provider function
dbProvider := func() (*gorm.DB, error) {
    // Your logic to get the current active database connection
    return getCurrentDBConnection()
}

// Create an insert batcher
insertBatcher := gorm.NewInsertBatcher[*YourModel](dbProvider, 100, 5*time.Second, context.Background())

// Use the insert batcher
err := insertBatcher.Insert(&YourModel{...}, &YourModel{...})
if err != nil {
    // Handle error
}

// Create an update batcher
updateBatcher := gorm.NewUpdateBatcher[*YourModel](dbProvider, 100, 5*time.Second, context.Background())

// Use the update batcher
err = updateBatcher.Update([*]YourModel{...}, []string{"FieldToUpdate"})
if err != nil {
    // Handle error
}
```

### Memoize Package

The `memoize` package provides a generic, concurrent-safe memoization solution for Go functions:

```go
import (
    "time"
    "github.com/atlasgurus/batcher/memoize"
)

func expensiveFunction(x int) int {
    // Some expensive computation
    return x * 2
}

// Create a memoized version of the function
memoizedFunc := memoize.Memoize(
    expensiveFunction,
    memoize.WithMaxSize(1000),
    memoize.WithExpiration(5 * time.Minute),
)

// Use the memoized function
result := memoizedFunc(5) // Computes and caches the result
result = memoizedFunc(5)  // Returns the cached result
```

### GORM v1 to v2 Adapter

If you're using GORM v1, you can use the provided adapter to convert your v1 connection to v2:

```go
import (
    gormv1 "github.com/jinzhu/gorm"
    "github.com/atlasgurus/batcher/gorm"
)

// Open a GORM v1 connection
v1DB, _ := gormv1.Open("mysql", "connection_string")

// Convert to GORM v2
v2DB, _ := gorm.GormV1ToV2Adapter(v1DB)

// Now you can use v2DB with the GORM batcher or any other GORM v2 operations
```

## Configuration

### Generic Batcher

`NewBatchProcessor` takes the following parameters:

- `maxBatchSize`: The maximum number of items to include in a single batch
- `maxWaitTime`: The maximum time to wait before processing a batch
- `ctx`: A context for cancellation
- `processFn`: A function to process the batched items

### GORM Batcher

Both `NewInsertBatcher` and `NewUpdateBatcher` take the following parameters:

- `dbProvider`: A function that returns a GORM v2 database instance and an error
- `maxBatchSize`: The maximum number of items to include in a single batch
- `maxWaitTime`: The maximum time to wait before processing a batch
- `ctx`: A context for cancellation

### Memoize Package

The `memoize` package provides a generic, concurrent-safe memoization solution for Go functions:

```go
import (
    "time"
    "github.com/atlasgurus/batcher/memoize"
)

func expensiveFunction(x int) int {
    // Some expensive computation
    return x * 2
}

// Create a memoized version of the function
memoizedFunc := memoize.Memoize(
    expensiveFunction,
    memoize.WithMaxSize(1000),
    memoize.WithExpiration(5 * time.Minute),
)

// Use the memoized function
result := memoizedFunc(5) // Computes and caches the result
result = memoizedFunc(5)  // Returns the cached result
```

#### Features

- Generic implementation: Works with any function type
- Concurrent-safe: Safe for use in multi-goroutine environments
- Size-limited cache: Prevent unbounded memory growth
- Time-based expiration: Automatically expire cached results
- Least Recently Used (LRU) eviction policy
- Metrics collection: Support for custom metrics collectors, including Prometheus

#### Options

`Memoize` function accepts the following options:

- `WithMaxSize(size int)`: Sets the maximum number of results to cache
- `WithExpiration(d time.Duration)`: Sets the expiration time for cached results
- `WithMetrics(collector MetricsCollector)`: Sets a custom metrics collector

#### Prometheus Metrics

The package includes a built-in Prometheus metrics collector:

```go
import (
    "github.com/atlasgurus/batcher/memoize"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "net/http"
)

func main() {
    promCollector := memoize.NewPrometheusMetricsCollector("my_function")
    
    memoizedFunc := memoize.Memoize(expensiveFunction,
        memoize.WithMaxSize(1000),
        memoize.WithExpiration(5*time.Minute),
        memoize.WithMetrics(promCollector))

    // Use memoizedFunc as needed

    // Expose Prometheus metrics
    http.Handle("/metrics", promhttp.Handler())
    http.ListenAndServe(":8080", nil)
}
```

This setup creates the following Prometheus metrics:

- `my_function_memoize_hits_total`: Total number of cache hits
- `my_function_memoize_misses_total`: Total number of cache misses
- `my_function_memoize_evictions_total`: Total number of cache evictions
- `my_function_memoize_total_items`: Current number of items in the cache

#### Custom Metrics Collectors

You can implement your own metrics collector by implementing the `MetricsCollector` interface:

```go
type MetricsCollector interface {
    Setup(function interface{})
    Collect(metrics *MemoMetrics)
}
```

This allows for integration with various monitoring systems beyond Prometheus.

#### Examples

Memoizing a Function with Multiple Arguments and Return Values:

```go
func complexFunc(a int, b string) (int, error) {
    // Some complex computation
    return len(b) + a, nil
}

memoized := memoize.Memoize(complexFunc)

result, err := memoized(5, "hello")
// result is cached
result, err = memoized(5, "hello") // Returns cached result
```

Using Expiration:

```go
memoized := memoize.Memoize(
    expensiveFunction,
    memoize.WithExpiration(1 * time.Minute),
)

result := memoized(10)
// Result is cached for 1 minute
time.Sleep(2 * time.Minute)
result = memoized(10) // Recomputes after expiration
```

#### Best Practices

1. Use memoization for pure functions (same input always produces the same output).
2. Be mindful of memory usage when memoizing functions with large return values.
3. Choose appropriate size limits and expiration times based on your use case.
4. Avoid memoizing functions with side effects.
5. Use metrics to monitor cache performance and adjust settings as needed.

#### Limitations

- The cache key is based on function arguments, so be cautious with functions that take pointer or reference types as arguments.
- Very large caches may impact performance due to the overhead of cache management.
- Metric collection may have a small performance impact, especially with high-throughput functions.
## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
