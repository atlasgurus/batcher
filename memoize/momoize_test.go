package memoize

import (
	"context"
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
	if finalCalls <= 200 || finalCalls > 300 {
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

// MockDB is a simple mock to simulate *gorm.DB for testing purposes
type MockDB struct {
	connectionString string
}

func TestMemoizeWithPointerArguments(t *testing.T) {
	calls := 0
	f := func(db *MockDB) string {
		calls++
		return db.connectionString
	}

	memoized := Memoize(f)

	db1 := &MockDB{connectionString: "database1"}
	db2 := &MockDB{connectionString: "database1"} // Same connection string as db1
	db3 := &MockDB{connectionString: "database2"}

	// First call with db1
	result1 := memoized(db1)
	if result1 != "database1" || calls != 1 {
		t.Errorf("Expected result 'database1' and 1 call, got %s and %d calls", result1, calls)
	}

	// Second call with db1 (should hit cache)
	result2 := memoized(db1)
	if result2 != "database1" || calls != 1 {
		t.Errorf("Expected cached result 'database1' and still 1 call, got %s and %d calls", result2, calls)
	}

	// Call with db2 (different pointer, same content - should not hit cache)
	result3 := memoized(db2)
	if result3 != "database1" || calls != 2 {
		t.Errorf("Expected new result 'database1' and 2 calls, got %s and %d calls", result3, calls)
	}

	// Call with db3 (different content)
	result4 := memoized(db3)
	if result4 != "database2" || calls != 3 {
		t.Errorf("Expected result 'database2' and 3 calls, got %s and %d calls", result4, calls)
	}
}

func TestMemoizeWithContext(t *testing.T) {
	// Test function with context as first argument
	func1Calls := 0
	func1 := func(ctx context.Context, a int, b string) string {
		func1Calls++
		return fmt.Sprintf("%d-%s", a, b)
	}
	memoFunc1 := Memoize(func1, WithMaxSize(100), WithExpiration(time.Minute))

	// Test function with context as middle argument
	func2Calls := 0
	func2 := func(a int, ctx context.Context, b string) string {
		func2Calls++
		return fmt.Sprintf("%d-%s", a, b)
	}
	memoFunc2 := Memoize(func2, WithMaxSize(100), WithExpiration(time.Minute))

	// Test function with context as last argument
	func3Calls := 0
	func3 := func(a int, b string, ctx context.Context) string {
		func3Calls++
		return fmt.Sprintf("%d-%s", a, b)
	}
	memoFunc3 := Memoize(func3, WithMaxSize(100), WithExpiration(time.Minute))

	ctx1 := context.Background()
	ctx2 := context.WithValue(context.Background(), "key", "value")

	// Test cases
	testCases := []struct {
		name     string
		memoFunc interface{}
		args     []interface{}
		expected string
		calls    *int
	}{
		{"Func1 First Call", memoFunc1, []interface{}{ctx1, 1, "a"}, "1-a", &func1Calls},
		{"Func1 Same Args Different Context", memoFunc1, []interface{}{ctx2, 1, "a"}, "1-a", &func1Calls},
		{"Func1 Different Args", memoFunc1, []interface{}{ctx1, 2, "a"}, "2-a", &func1Calls},

		{"Func2 First Call", memoFunc2, []interface{}{1, ctx1, "a"}, "1-a", &func2Calls},
		{"Func2 Same Args Different Context", memoFunc2, []interface{}{1, ctx2, "a"}, "1-a", &func2Calls},
		{"Func2 Different Args", memoFunc2, []interface{}{2, ctx1, "a"}, "2-a", &func2Calls},

		{"Func3 First Call", memoFunc3, []interface{}{1, "a", ctx1}, "1-a", &func3Calls},
		{"Func3 Same Args Different Context", memoFunc3, []interface{}{1, "a", ctx2}, "1-a", &func3Calls},
		{"Func3 Different Args", memoFunc3, []interface{}{2, "a", ctx1}, "2-a", &func3Calls},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			initialCalls := *tc.calls
			var result string
			switch f := tc.memoFunc.(type) {
			case func(context.Context, int, string) string:
				result = f(tc.args[0].(context.Context), tc.args[1].(int), tc.args[2].(string))
			case func(int, context.Context, string) string:
				result = f(tc.args[0].(int), tc.args[1].(context.Context), tc.args[2].(string))
			case func(int, string, context.Context) string:
				result = f(tc.args[0].(int), tc.args[1].(string), tc.args[2].(context.Context))
			}

			if result != tc.expected {
				t.Errorf("Expected result %s, got %s", tc.expected, result)
			}

			expectedCalls := initialCalls
			if tc.name == "Func1 First Call" || tc.name == "Func2 First Call" || tc.name == "Func3 First Call" ||
				tc.name == "Func1 Different Args" || tc.name == "Func2 Different Args" || tc.name == "Func3 Different Args" {
				expectedCalls++
			}

			if *tc.calls != expectedCalls {
				t.Errorf("Expected %d calls, got %d", expectedCalls, *tc.calls)
			}
		})
	}
}

type MockMetricsCollector struct {
	metrics      MemoMetrics
	setupCalled  bool
	collectCalls int
}

func (m *MockMetricsCollector) Setup(function interface{}) {
	m.setupCalled = true
}

func (m *MockMetricsCollector) Collect(metrics *MemoMetrics) {
	m.collectCalls++
	m.metrics.Hits.Store(metrics.Hits.Load())
	m.metrics.Misses.Store(metrics.Misses.Load())
	m.metrics.Evictions.Store(metrics.Evictions.Load())
	m.metrics.TotalItems = metrics.TotalItems
}

func TestMemoizeWithMetrics(t *testing.T) {
	calls := 0
	testFunc := func(x int) int {
		calls++
		return x * 2
	}

	mockCollector := &MockMetricsCollector{}

	memoized := Memoize(testFunc,
		WithMaxSize(2),
		WithExpiration(50*time.Millisecond),
		WithMetrics(mockCollector))

	// Use the memoized function
	memoized(1) // Miss
	memoized(1) // Hit
	memoized(2) // Miss
	memoized(3) // Miss, evicts 1
	memoized(2) // Hit

	// Wait for metrics to be collected
	time.Sleep(60 * time.Millisecond)

	// Verify that Setup was called
	if !mockCollector.setupCalled {
		t.Error("Setup was not called on the MetricsCollector")
	}

	// Verify metrics
	expectedHits := int64(2)
	expectedMisses := int64(3)
	expectedEvictions := int64(1) // 1 eviction when 3 is added

	if hits := mockCollector.metrics.Hits.Load(); hits != expectedHits {
		t.Errorf("Expected %d hits, got %d", expectedHits, hits)
	}
	if misses := mockCollector.metrics.Misses.Load(); misses != expectedMisses {
		t.Errorf("Expected %d misses, got %d", expectedMisses, misses)
	}
	if evictions := mockCollector.metrics.Evictions.Load(); evictions != expectedEvictions {
		t.Errorf("Expected %d evictions, got %d", expectedEvictions, evictions)
	}

	// Verify the number of actual function calls
	expectedCalls := 3 // 1 for initial, 1 for 2, 1 for 3
	if calls != expectedCalls {
		t.Errorf("Expected %d calls to original function, got %d", expectedCalls, calls)
	}

	// Verify that Collect was called at least once
	if mockCollector.collectCalls == 0 {
		t.Error("Collect was not called on the MetricsCollector")
	}

	// Log the total items for informational purposes
	t.Logf("Total items after test: %d", mockCollector.metrics.TotalItems)
}
