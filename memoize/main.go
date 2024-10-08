package memoize

import (
	"container/list"
	"fmt"
	"reflect"
	"sync"
	"time"
)

type cacheEntry struct {
	key      string
	result   []reflect.Value
	ready    chan struct{}
	expireAt time.Time
	element  *list.Element
}

type MemoizeOption func(*memoizeOptions)

type memoizeOptions struct {
	maxSize    int
	expiration time.Duration
}

func WithMaxSize(size int) MemoizeOption {
	return func(o *memoizeOptions) {
		o.maxSize = size
	}
}

func WithExpiration(d time.Duration) MemoizeOption {
	return func(o *memoizeOptions) {
		o.expiration = d
	}
}

// Memoize takes a function of any type and returns a memoized version of it.
func Memoize[F any](f F, options ...MemoizeOption) F {
	ft := reflect.TypeOf(f)
	if ft.Kind() != reflect.Func {
		panic("Memoize: argument must be a function")
	}

	opts := memoizeOptions{
		maxSize:    100,       // Default max size
		expiration: time.Hour, // Default expiration
	}
	for _, option := range options {
		option(&opts)
	}

	cache := &sync.Map{}
	lru := list.New()
	var mutex sync.Mutex

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
			} else {
				break
			}
		}
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
				return entry.result
			}
			// Entry has expired, remove it
			mutex.Lock()
			lru.Remove(entry.element)
			cache.Delete(entry.key)
			mutex.Unlock()
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
				return entry.result
			}
			// Entry has expired, remove it and recompute
			mutex.Lock()
			lru.Remove(entry.element)
			cache.Delete(entry.key)
			mutex.Unlock()
		} else {
			defer close(entry.ready) // Signal that the result is ready
			entry.result = reflect.ValueOf(f).Call(args)

			mutex.Lock()
			if lru.Len() >= opts.maxSize {
				oldest := lru.Back()
				oldestEntry := oldest.Value.(*cacheEntry)
				lru.Remove(oldest)
				cache.Delete(oldestEntry.key)
			}
			entry.element = lru.PushFront(entry)
			mutex.Unlock()

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
		}
		newEntry.element = lru.PushFront(newEntry)
		mutex.Unlock()

		return newEntry.result
	})

	return wrapped.Interface().(F)
}

func makeKey(args []reflect.Value) string {
	var key string
	for i, arg := range args {
		if i > 0 {
			key += ","
		}
		switch arg.Kind() {
		case reflect.Array, reflect.Slice, reflect.Map, reflect.Struct:
			// Use a string representation for complex types
			key += fmt.Sprintf("%#v", arg.Interface())
		default:
			// Use the value directly for simple types
			key += fmt.Sprintf("%v", arg.Interface())
		}
	}
	return key
}
