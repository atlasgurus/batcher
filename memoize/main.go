package memoize

import (
	"container/list"
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type cacheEntry struct {
	key      string
	result   []reflect.Value
	ready    chan struct{}
	expireAt time.Time
	element  *list.Element
}

type Option func(*memoizeOptions)

type memoizeOptions struct {
	maxSize        int
	expiration     time.Duration
	metricCallback MetricCallback
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

// MemoMetrics now uses atomic int64 for counters
type MemoMetrics struct {
	Hits       atomic.Int64
	Misses     atomic.Int64
	Evictions  atomic.Int64
	TotalItems int // This is set during cleanup, so it doesn't need to be atomic
}

type MetricCallback func(*MemoMetrics)

func WithMetricCallback(callback MetricCallback) Option {
	return func(o *memoizeOptions) {
		o.metricCallback = callback
	}
}

func Memoize[F any](f F, options ...Option) F {
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

	cache := &sync.Map{}
	lru := list.New()
	var mutex sync.Mutex

	metrics := MemoMetrics{}

	cleanup := func() {
		mutex.Lock()
		defer mutex.Unlock()

		now := time.Now()
		for lru.Len() > 0 {
			oldest := lru.Back()
			entry := oldest.Value.(*cacheEntry)
			if now.After(entry.expireAt) || lru.Len() > opts.maxSize {
				lru.Remove(oldest)
				cache.Delete(entry.key)
				metrics.Evictions.Add(1)
			} else {
				break
			}
		}

		metrics.TotalItems = lru.Len()
		if opts.metricCallback != nil {
			opts.metricCallback(&metrics)
		}
		// Reset counters after reporting
		metrics.Hits.Store(0)
		metrics.Misses.Store(0)
		metrics.Evictions.Store(0)
	}

	go func() {
		for {
			time.Sleep(opts.expiration / 10)
			cleanup()
		}
	}()

	wrapped := reflect.MakeFunc(ft, func(args []reflect.Value) []reflect.Value {
		key := makeKey(args)

		now := time.Now()
		if value, found := cache.Load(key); found {
			entry := value.(*cacheEntry)
			if now.Before(entry.expireAt) {
				<-entry.ready // Wait for the result to be ready
				mutex.Lock()
				lru.MoveToFront(entry.element)
				mutex.Unlock()
				metrics.Hits.Add(1)
				return entry.result
			}
			// Entry has expired, remove it
			mutex.Lock()
			lru.Remove(entry.element)
			cache.Delete(entry.key)
			mutex.Unlock()
			metrics.Evictions.Add(1)
		}

		// Compute the result
		entry := &cacheEntry{
			key:      key,
			ready:    make(chan struct{}),
			expireAt: now.Add(opts.expiration),
		}
		if actual, loaded := cache.LoadOrStore(key, entry); loaded {
			entry = actual.(*cacheEntry)
			<-entry.ready // Wait for the result to be ready
			if now.Before(entry.expireAt) {
				metrics.Hits.Add(1)
				return entry.result
			}
			// Entry has expired, remove it and recompute
			mutex.Lock()
			lru.Remove(entry.element)
			cache.Delete(entry.key)
			mutex.Unlock()
			metrics.Evictions.Add(1)
		} else {
			defer close(entry.ready) // Signal that the result is ready
			entry.result = reflect.ValueOf(f).Call(args)

			mutex.Lock()
			if lru.Len() >= opts.maxSize {
				oldest := lru.Back()
				oldestEntry := oldest.Value.(*cacheEntry)
				lru.Remove(oldest)
				cache.Delete(oldestEntry.key)
				metrics.Evictions.Add(1)
			}
			entry.element = lru.PushFront(entry)
			mutex.Unlock()

			metrics.Misses.Add(1)
			return entry.result
		}

		// Recompute if entry was expired
		newEntry := &cacheEntry{
			key:      key,
			ready:    make(chan struct{}),
			expireAt: now.Add(opts.expiration),
		}
		cache.Store(key, newEntry)
		defer close(newEntry.ready)
		newEntry.result = reflect.ValueOf(f).Call(args)

		mutex.Lock()
		if lru.Len() >= opts.maxSize {
			oldest := lru.Back()
			oldestEntry := oldest.Value.(*cacheEntry)
			lru.Remove(oldest)
			cache.Delete(oldestEntry.key)
			metrics.Evictions.Add(1)
		}
		newEntry.element = lru.PushFront(newEntry)
		mutex.Unlock()

		metrics.Misses.Add(1)
		return newEntry.result
	})

	return wrapped.Interface().(F)
}

func makeKey(args []reflect.Value) string {
	var key strings.Builder
	for i, arg := range args {
		if i > 0 {
			key.WriteString(",")
		}
		if true {
			key.WriteString(makeShallowKey(arg))
		} else {
			key.WriteString(makeDeepKey(arg))
		}
	}
	return key.String()
}

// in case we want to start using a deep key
func makeDeepKey(v reflect.Value) string {
	var key string
	switch v.Kind() {
	case reflect.Array, reflect.Slice, reflect.Map, reflect.Struct:
		// Use a string representation for complex types
		key += fmt.Sprintf("%#v", v.Interface())
	default:
		// Use the value directly for simple types
		key += fmt.Sprintf("%v", v.Interface())
	}
	return key
}

func makeShallowKey(v reflect.Value) string {
	// Special handling for context.Context
	if v.Type().Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
		return "context" // We return a constant string for all contexts
	}

	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return "nil"
		}
		return fmt.Sprintf("ptr(%p):%s", v.Interface(), makeShallowKey(v.Elem()))
	case reflect.Struct:
		var fieldKeys []string
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			if t.Field(i).IsExported() {
				field := v.Field(i)
				fieldKeys = append(fieldKeys, fmt.Sprintf("%s:%s", t.Field(i).Name, makeShallowKey(field)))
			}
		}
		return fmt.Sprintf("%s{%s}", v.Type().Name(), strings.Join(fieldKeys, ","))
	case reflect.Slice:
		var elemKeys []string
		for i := 0; i < v.Len(); i++ {
			elemKeys = append(elemKeys, fmt.Sprintf("%v", v.Index(i).Interface()))
		}
		return fmt.Sprintf("%s[%s]", v.Type().Name(), strings.Join(elemKeys, ","))
	case reflect.Map:
		// For maps, we still use length and address to avoid deep comparison
		return fmt.Sprintf("%s(len=%d,addr=%p)", v.Type().Name(), v.Len(), v.Interface())
	default:
		return fmt.Sprintf("%v", v.Interface())
	}
}
