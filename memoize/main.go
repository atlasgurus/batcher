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
	key       string
	result    []reflect.Value
	ready     chan struct{}
	expireAt  time.Time
	element   *list.Element
	computing bool
}

type Option func(*memoizeOptions)

type memoizeOptions struct {
	maxSize    int
	expiration time.Duration
	metrics    MetricsCollector
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

	cache := make(map[string]*list.Element)
	lru := list.New()
	var mutex sync.Mutex

	metrics := &MemoMetrics{}

	if opts.metrics != nil {
		opts.metrics.Setup(f)
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

		key := makeKey(args)

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
			expireAt:  now.Add(opts.expiration), // Set the expiration time when creating the entry
		}
		element = lru.PushFront(entry)
		cache[key] = element
		mutex.Unlock()

		// Compute the result
		result := reflect.ValueOf(f).Call(args)

		mutex.Lock()
		entry.result = result
		entry.computing = false
		cleanup()          // Clean up after adding new entry
		close(entry.ready) // Signal that the result is ready
		mutex.Unlock()

		metrics.Misses.Add(1)
		return result
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
