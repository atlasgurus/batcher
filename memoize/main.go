package memoize

import (
	"container/list"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type cacheEntry struct {
	key       string
	result    []reflect.Value
	ready     chan struct{}
	expireAt  time.Time
	element   *list.Element
	computing bool
}

type Option func(*memoizeOptions)

type memoizeOptions struct {
	maxSize       int
	expiration    time.Duration
	metrics       MetricsCollector
	ignoreParams  []int
	memoizeErrors bool
}

func WithMaxSize(size int) Option {
	return func(o *memoizeOptions) {
		o.maxSize = size
	}
}

func WithExpiration(d time.Duration) Option {
	return func(o *memoizeOptions) {
		o.expiration = d
	}
}

func WithIgnoreParams(indices []int) Option {
	return func(o *memoizeOptions) {
		o.ignoreParams = indices
	}
}

func WithMemoizeErrors(memoize bool) Option {
	return func(o *memoizeOptions) {
		o.memoizeErrors = memoize
	}
}

type MemoMetrics struct {
	Hits       atomic.Int64
	Misses     atomic.Int64
	Evictions  atomic.Int64
	TotalItems int
}

type MetricsCollector interface {
	Setup(function interface{})
	Collect(metrics *MemoMetrics)
}

func WithMetrics(collector MetricsCollector) Option {
	return func(o *memoizeOptions) {
		o.metrics = collector
	}
}

// MemoizeWithInvalidate wraps a function with a caching layer that stores and reuses computed results.
// It returns two values:
//   - A memoized version of the input function with identical signature
//   - An invalidation function that clears the cache when called
//
// The memoized function caches results based on the string representation of input arguments.
// Cache entries expire based on time and size limits set via options.
//
// Options:
//   - WithMaxSize: Sets maximum number of cached results (default 100)
//   - WithExpiration: Sets how long cached results remain valid (default 1 hour)
//   - WithIgnoreParams: Specifies argument indices to exclude from cache key
//   - WithMemoizeErrors: Controls whether error results are cached (default false)
//   - WithMetrics: Provides a collector for cache performance metrics
//
// Example:
//
//	memoizedFn, invalidate := Memoize(expensiveFunc, WithMaxSize(1000))
//	result := memoizedFn(arg) // Result is computed and cached
//	invalidate() // Clears the cache
func MemoizeWithInvalidate[F any](f F, options ...Option) (F, func()) {
	ft := reflect.TypeOf(f)
	if ft.Kind() != reflect.Func {
		panic("Memoize: argument must be a function")
	}

	opts := memoizeOptions{
		maxSize:    100,
		expiration: time.Hour,
	}
	for _, option := range options {
		option(&opts)
	}

	cache := make(map[string]*list.Element)
	lru := list.New()
	var mutex sync.Mutex

	metrics := &MemoMetrics{}

	if opts.metrics != nil {
		opts.metrics.Setup(f)
	}

	// Pre-compute whether the function returns an error as its last value
	hasErrorReturn := false
	if ft.NumOut() > 0 {
		lastReturn := ft.Out(ft.NumOut() - 1)
		hasErrorReturn = lastReturn.Implements(reflect.TypeOf((*error)(nil)).Elem())
	}

	cleanup := func() {
		now := time.Now()
		for lru.Len() > opts.maxSize || (lru.Len() > 0 && now.After(lru.Back().Value.(*cacheEntry).expireAt)) {
			oldest := lru.Back()
			if oldest != nil {
				lru.Remove(oldest)
				delete(cache, oldest.Value.(*cacheEntry).key)
				metrics.Evictions.Add(1)
			}
		}
		metrics.TotalItems = lru.Len()
	}

	wrapped := reflect.MakeFunc(ft, func(args []reflect.Value) []reflect.Value {
		defer func() {
			if opts.metrics != nil {
				opts.metrics.Collect(metrics)
			}
		}()

		key := makeKey(args, opts.ignoreParams)

		mutex.Lock()
		element, found := cache[key]
		now := time.Now()

		if found {
			entry := element.Value.(*cacheEntry)
			if now.Before(entry.expireAt) {
				lru.MoveToFront(element)
				readyChan := entry.ready
				mutex.Unlock()
				<-readyChan // Wait for the result to be ready
				metrics.Hits.Add(1)
				return entry.result
			}
			// Entry has expired, remove it
			lru.Remove(element)
			delete(cache, key)
		}

		// Create a new entry or reuse the expired one
		entry := &cacheEntry{
			key:       key,
			ready:     make(chan struct{}),
			computing: true,
			expireAt:  now.Add(opts.expiration),
		}
		element = lru.PushFront(entry)
		cache[key] = element
		mutex.Unlock()

		// Compute the result
		result := reflect.ValueOf(f).Call(args)

		// Check if result has error and if we should skip memoization
		shouldMemoize := true
		if hasErrorReturn && !opts.memoizeErrors {
			if !result[len(result)-1].IsNil() {
				shouldMemoize = false
			}
		}

		if shouldMemoize {
			mutex.Lock()
			entry.result = result
			entry.computing = false
			cleanup()          // Clean up after adding new entry
			close(entry.ready) // Signal that the result is ready
			mutex.Unlock()
		} else {
			mutex.Lock()
			delete(cache, key)
			lru.Remove(element)
			mutex.Unlock()
		}

		metrics.Misses.Add(1)
		return result
	})

	invalidate := func() {
		mutex.Lock()
		defer mutex.Unlock()
		cache = make(map[string]*list.Element)
		lru.Init()
		metrics.TotalItems = 0
	}

	return wrapped.Interface().(F), invalidate
}

func Memoize[F any](f F, options ...Option) F {
	memoized, _ := MemoizeWithInvalidate(f, options...)
	return memoized
}

func makeKey(args []reflect.Value, ignoreParams []int) string {
	var key strings.Builder
	for i, arg := range args {
		if !contains(ignoreParams, i) {
			if key.Len() > 0 {
				key.WriteString(",")
			}
			key.WriteString(makeOneKey(arg))
		}
	}
	return key.String()
}

func makeOneKey(v reflect.Value) string {
	return fmt.Sprintf("%#v", v.Interface())
}

func contains(slice []int, item int) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
