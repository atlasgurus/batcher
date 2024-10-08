package memoize

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoizeMultipleArgsMultipleReturns(t *testing.T) {
	calls := 0
	f := func(a, b int) (int, string) {
		calls++
		return a + b, fmt.Sprintf("%d+%d", a, b)
	}

	memoized := Memoize(f)

	for i := 0; i < 2; i++ {
		sum, str := memoized(3, 4)
		if sum != 7 || str != "3+4" {
			t.Errorf("Expected (7, '3+4'), got (%d, '%s')", sum, str)
		}
	}

	if calls != 1 {
		t.Errorf("Expected 1 call, got %d", calls)
	}
}

func TestMemoizeDifferentArguments(t *testing.T) {
	calls := 0
	f := func(x int) int {
		calls++
		return x * 2
	}

	memoized := Memoize(f)

	results := []int{memoized(5), memoized(5), memoized(3), memoized(3)}
	expected := []int{10, 10, 6, 6}

	for i, result := range results {
		if result != expected[i] {
			t.Errorf("Expected %d, got %d", expected[i], result)
		}
	}

	if calls != 2 {
		t.Errorf("Expected 2 calls, got %d", calls)
	}
}

func TestMemoizeWithMultipleGoroutinesDifferentArgs(t *testing.T) {
	var calls int64
	f := func(x int) int {
		atomic.AddInt64(&calls, 1)
		time.Sleep(10 * time.Millisecond) // Simulate some work
		return x * 2
	}

	memoized := Memoize(f)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			result := memoized(val)
			if result != val*2 {
				t.Errorf("Expected %d, got %d", val*2, result)
			}
		}(i)
	}
	wg.Wait()

	if calls != 100 {
		t.Errorf("Expected 100 calls, got %d", calls)
	}
}

func TestMemoizeWithSliceArgument(t *testing.T) {
	calls := 0
	f := func(x []int) int {
		calls++
		sum := 0
		for _, v := range x {
			sum += v
		}
		return sum
	}

	memoized := Memoize(f)

	slice1 := []int{1, 2, 3}
	slice2 := []int{1, 2, 3} // Same content as slice1, but different slice

	result1 := memoized(slice1)
	result2 := memoized(slice2)

	if result1 != 6 || result2 != 6 {
		t.Errorf("Expected both results to be 6, got %d and %d", result1, result2)
	}

	if calls != 1 {
		t.Errorf("Expected 1 call, got %d", calls)
	}
}

func TestMemoizeWithErrorReturn(t *testing.T) {
	calls := 0
	f := func(x int) (int, error) {
		calls++
		if x < 0 {
			return 0, errors.New("negative input")
		}
		return x * 2, nil
	}

	memoized := Memoize(f)

	// Test successful case
	result, err := memoized(5)
	if err != nil || result != 10 {
		t.Errorf("Expected (10, nil), got (%d, %v)", result, err)
	}

	result, err = memoized(5)
	if err != nil || result != 10 {
		t.Errorf("Expected (10, nil), got (%d, %v)", result, err)
	}

	// Test error case
	result, err = memoized(-1)
	if err == nil || err.Error() != "negative input" || result != 0 {
		t.Errorf("Expected (0, 'negative input'), got (%d, %v)", result, err)
	}

	result, err = memoized(-1)
	if err == nil || err.Error() != "negative input" || result != 0 {
		t.Errorf("Expected (0, 'negative input'), got (%d, %v)", result, err)
	}

	if calls != 2 {
		t.Errorf("Expected 2 calls, got %d", calls)
	}
}

func TestMemoizeWithMultipleReturnsAndError(t *testing.T) {
	calls := 0
	f := func(x, y int) (int, int, error) {
		calls++
		if x < 0 || y < 0 {
			return 0, 0, errors.New("negative input")
		}
		return x + y, x * y, nil
	}

	memoized := Memoize(f)

	// Test successful case
	sum, product, err := memoized(3, 4)
	if err != nil || sum != 7 || product != 12 {
		t.Errorf("Expected (7, 12, nil), got (%d, %d, %v)", sum, product, err)
	}

	sum, product, err = memoized(3, 4)
	if err != nil || sum != 7 || product != 12 {
		t.Errorf("Expected (7, 12, nil), got (%d, %d, %v)", sum, product, err)
	}

	// Test error case
	sum, product, err = memoized(-1, 4)
	if err == nil || err.Error() != "negative input" || sum != 0 || product != 0 {
		t.Errorf("Expected (0, 0, 'negative input'), got (%d, %d, %v)", sum, product, err)
	}

	if calls != 2 {
		t.Errorf("Expected 2 calls, got %d", calls)
	}
}

func TestMemoizeSingleArgSingleReturn(t *testing.T) {
	calls := 0
	f := func(x int) int {
		calls++
		return x * 2
	}

	memoized := Memoize(f)

	for i := 0; i < 2; i++ {
		result := memoized(5)
		if result != 10 {
			t.Errorf("Expected 10, got %d", result)
		}
	}

	if calls != 1 {
		t.Errorf("Expected 1 call, got %d", calls)
	}
}

func TestMemoizeWithSizeLimit(t *testing.T) {
	calls := 0
	f := func(x int) int {
		calls++
		return x * 2
	}

	memoized := Memoize(f, WithMaxSize(2))

	memoized(1)
	memoized(2)
	memoized(3) // This should evict the result for 1
	memoized(2) // This should hit the cache
	memoized(1) // This should miss the cache and recompute

	if calls != 4 {
		t.Errorf("Expected 4 calls, got %d", calls)
	}
}

func TestMemoizeWithExpiration(t *testing.T) {
	calls := 0
	f := func(x int) int {
		calls++
		return x * 2
	}

	memoized := Memoize(f, WithExpiration(100*time.Millisecond))

	// First call
	result := memoized(1)
	if result != 2 || calls != 1 {
		t.Errorf("First call: Expected result 2 and calls 1, got result %d and calls %d", result, calls)
	}

	// Second call (should hit cache)
	result = memoized(1)
	if result != 2 || calls != 1 {
		t.Errorf("Second call: Expected result 2 and calls 1, got result %d and calls %d", result, calls)
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Third call (should recompute)
	result = memoized(1)
	if result != 2 || calls != 2 {
		t.Errorf("Third call: Expected result 2 and calls 2, got result %d and calls %d", result, calls)
	}

	// Fourth call (should hit cache again)
	result = memoized(1)
	if result != 2 || calls != 2 {
		t.Errorf("Fourth call: Expected result 2 and calls 2, got result %d and calls %d", result, calls)
	}
}
func TestMemoizeWithSizeLimitAndExpiration(t *testing.T) {
	var calls int64
	f := func(x int) int {
		atomic.AddInt64(&calls, 1)
		return x * 2
	}

	memoized := Memoize(f, WithMaxSize(100), WithExpiration(50*time.Millisecond))

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			memoized(val % 150) // This will cause some values to be evicted due to size limit
		}(i)
	}
	wg.Wait()

	time.Sleep(60 * time.Millisecond) // Wait for entries to expire

	for i := 0; i < 100; i++ {
		memoized(i) // These should all miss the cache and recompute
	}

	finalCalls := atomic.LoadInt64(&calls)
	if finalCalls <= 200 || finalCalls >= 300 {
		t.Errorf("Expected between 200 and 300 calls, got %d", finalCalls)
	}
}

func TestMemoizeConcurrency(t *testing.T) {
	var calls int64
	f := func(x int) int {
		atomic.AddInt64(&calls, 1)
		time.Sleep(10 * time.Millisecond) // Simulate some work
		return x * 2
	}

	memoized := Memoize(f)

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := memoized(5)
			if result != 10 {
				t.Errorf("Expected 10, got %d", result)
			}
		}()
	}
	wg.Wait()

	if calls != 1 {
		t.Errorf("Expected 1 call, got %d", calls)
	}
}
