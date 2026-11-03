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

// cacheEntry is one key's value, or the computation producing it. Callers that
// find an entry still computing wait on ready, which the computing caller
// closes however the computation ends.
type cacheEntry struct {
	key       string
	result    []reflect.Value
	ready     chan struct{}
	expireAt  time.Time // set when the value is ready; meaningless while computing
	element   *list.Element
	computing bool
	// failed means the computation panicked and produced no result; callers
	// waiting on it compute again.
	failed bool
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

// MetricsCollector receives the cache's metrics after every call. Collect runs
// under the cache's lock, one call at a time, so it must not call the memoized
// function.
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

	// An entry is in the cache exactly while cache[key] points at its list
	// element; the map and the list change together, under mutex. A
	// computation that finishes after its entry was invalidated, expired or
	// evicted therefore finds that it no longer owns it, and leaves the cache
	// alone. Removing an element that is not in lru would corrupt the list.
	owned := func(entry *cacheEntry) bool {
		return cache[entry.key] == entry.element
	}
	remove := func(entry *cacheEntry) {
		lru.Remove(entry.element)
		delete(cache, entry.key)
	}

	// cleanup evicts entries over the size limit, then expired ones, oldest
	// first. An entry still computing has no value to expire.
	cleanup := func() {
		now := time.Now()
		for lru.Len() > 0 {
			oldest := lru.Back().Value.(*cacheEntry)
			if lru.Len() <= opts.maxSize && (oldest.computing || now.Before(oldest.expireAt)) {
				break
			}
			remove(oldest)
			metrics.Evictions.Add(1)
		}
		metrics.TotalItems = lru.Len()
	}

	fv := reflect.ValueOf(f)

	// compute runs f for an entry this caller created, stores the result if it
	// is to be kept and the entry is still the cache's, and releases waiters.
	compute := func(entry *cacheEntry, args []reflect.Value) []reflect.Value {
		returned := false
		defer func() {
			if returned {
				return
			}
			// f panicked. Forget the entry and release the waiters, which
			// compute again; the panic continues to this caller.
			mutex.Lock()
			entry.failed = true
			entry.computing = false
			if owned(entry) {
				remove(entry)
			}
			close(entry.ready)
			mutex.Unlock()
		}()
		result := fv.Call(args)
		returned = true

		keep := !hasErrorReturn || opts.memoizeErrors || result[len(result)-1].IsNil()

		mutex.Lock()
		entry.result = result
		entry.computing = false
		if owned(entry) {
			if keep {
				entry.expireAt = time.Now().Add(opts.expiration)
			} else {
				remove(entry)
			}
		}
		// Waiters get this result even when it is an error that is not kept.
		close(entry.ready)
		cleanup()
		mutex.Unlock()

		metrics.Misses.Add(1)
		return result
	}

	wrapped := reflect.MakeFunc(ft, func(args []reflect.Value) []reflect.Value {
		defer func() {
			if opts.metrics != nil {
				// Under the mutex: TotalItems is written under it, and a
				// collector need not be safe for concurrent use. A collector
				// must therefore not call the memoized function.
				mutex.Lock()
				opts.metrics.Collect(metrics)
				mutex.Unlock()
			}
		}()

		key := makeKey(args, opts.ignoreParams)

		for {
			mutex.Lock()
			if element, found := cache[key]; found {
				entry := element.Value.(*cacheEntry)
				if entry.computing || time.Now().Before(entry.expireAt) {
					lru.MoveToFront(element)
					mutex.Unlock()
					<-entry.ready
					if entry.failed {
						continue // the computation panicked; compute again
					}
					metrics.Hits.Add(1)
					return entry.result
				}
				remove(entry) // expired
			}

			entry := &cacheEntry{
				key:       key,
				ready:     make(chan struct{}),
				computing: true,
			}
			entry.element = lru.PushFront(entry)
			cache[key] = entry.element
			mutex.Unlock()

			return compute(entry, args)
		}
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
