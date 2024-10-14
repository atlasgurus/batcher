package memoize

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestPrometheusMetricsCollector(t *testing.T) {
	calls := 0
	testFunc := func(x int) int {
		calls++
		return x * 2
	}

	collector := NewPrometheusMetricsCollector("test_function")

	memoized := Memoize(testFunc,
		WithMaxSize(2),
		WithExpiration(50*time.Millisecond),
		WithMetrics(collector))

	// Use the memoized function
	memoized(1) // Miss
	memoized(1) // Hit
	memoized(2) // Miss
	memoized(3) // Miss, evicts 1
	memoized(2) // Hit

	// Wait for metrics to be collected
	time.Sleep(60 * time.Millisecond)

	// Collect all metrics
	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	// Helper function to find a specific metric
	findMetric := func(name string) *dto.Metric {
		for _, mf := range metricFamilies {
			if mf.GetName() == name && len(mf.Metric) > 0 {
				return mf.Metric[0]
			}
		}
		return nil
	}

	// Check hits
	hitsMetric := findMetric("test_function_memoize_hits_total")
	if hitsMetric == nil {
		t.Fatal("Hits metric not found")
	}
	if hitsMetric.Counter.GetValue() != 2 {
		t.Errorf("Expected 2 hits, got %v", hitsMetric.Counter.GetValue())
	}

	// Check misses
	missesMetric := findMetric("test_function_memoize_misses_total")
	if missesMetric == nil {
		t.Fatal("Misses metric not found")
	}
	if missesMetric.Counter.GetValue() != 3 {
		t.Errorf("Expected 3 misses, got %v", missesMetric.Counter.GetValue())
	}

	// Check evictions
	evictionsMetric := findMetric("test_function_memoize_evictions_total")
	if evictionsMetric == nil {
		t.Fatal("Evictions metric not found")
	}
	if evictionsMetric.Counter.GetValue() != 1 {
		t.Errorf("Expected 1 eviction, got %v", evictionsMetric.Counter.GetValue())
	}

	// Check total items
	totalItemsMetric := findMetric("test_function_memoize_total_items")
	if totalItemsMetric == nil {
		t.Fatal("Total items metric not found")
	}
	if totalItemsMetric.Gauge.GetValue() != 2 {
		t.Errorf("Expected 2 total items, got %v", totalItemsMetric.Gauge.GetValue())
	}

	// Additional test: Check metric output format
	expectedOutput := `
# HELP test_function_memoize_hits_total The total number of cache hits for the memoized function
# TYPE test_function_memoize_hits_total counter
test_function_memoize_hits_total{function=""} 2
# HELP test_function_memoize_misses_total The total number of cache misses for the memoized function
# TYPE test_function_memoize_misses_total counter
test_function_memoize_misses_total{function=""} 3
# HELP test_function_memoize_evictions_total The total number of cache evictions for the memoized function
# TYPE test_function_memoize_evictions_total counter
test_function_memoize_evictions_total{function=""} 1
# HELP test_function_memoize_total_items The current number of items in the cache for the memoized function
# TYPE test_function_memoize_total_items gauge
test_function_memoize_total_items{function=""} 2
`

	err = testutil.CollectAndCompare(collector.hits, strings.NewReader(expectedOutput), "test_function_memoize_hits_total")
	if err != nil {
		t.Errorf("Unexpected collecting result for hits: %s", err)
	}

	err = testutil.CollectAndCompare(collector.misses, strings.NewReader(expectedOutput), "test_function_memoize_misses_total")
	if err != nil {
		t.Errorf("Unexpected collecting result for misses: %s", err)
	}

	err = testutil.CollectAndCompare(collector.evictions, strings.NewReader(expectedOutput), "test_function_memoize_evictions_total")
	if err != nil {
		t.Errorf("Unexpected collecting result for evictions: %s", err)
	}

	err = testutil.CollectAndCompare(collector.totalItems, strings.NewReader(expectedOutput), "test_function_memoize_total_items")
	if err != nil {
		t.Errorf("Unexpected collecting result for total items: %s", err)
	}
}
