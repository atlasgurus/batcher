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

	// Assume NewPrometheusMetricsCollector can optionally take function and package labels
	collector := NewPrometheusMetricsCollector("test_function", "test_package")

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
	findMetric := func(name, label string) *dto.Metric {
		for _, mf := range metricFamilies {
			if mf.GetName() == name {
				for _, metric := range mf.Metric {
					for _, labelPair := range metric.Label {
						if labelPair.GetName() == "function" && labelPair.GetValue() == label {
							return metric
						}
					}
				}
			}
		}
		return nil
	}

	// Check metrics for "test_function" with labels
	hitsMetric := findMetric("memoize_hits_total", "test_function")
	if hitsMetric == nil {
		t.Fatal("Hits metric not found")
	}
	if hitsMetric.Counter.GetValue() != 2 {
		t.Errorf("Expected 2 hits, got %v", hitsMetric.Counter.GetValue())
	}

	missesMetric := findMetric("memoize_misses_total", "test_function")
	if missesMetric == nil {
		t.Fatal("Misses metric not found")
	}
	if missesMetric.Counter.GetValue() != 3 {
		t.Errorf("Expected 3 misses, got %v", missesMetric.Counter.GetValue())
	}

	evictionsMetric := findMetric("memoize_evictions_total", "test_function")
	if evictionsMetric == nil {
		t.Fatal("Evictions metric not found")
	}
	if evictionsMetric.Counter.GetValue() != 1 {
		t.Errorf("Expected 1 eviction, got %v", evictionsMetric.Counter.GetValue())
	}

	totalItemsMetric := findMetric("memoize_total_items", "test_function")
	if totalItemsMetric == nil {
		t.Fatal("Total items metric not found")
	}
	if totalItemsMetric.Gauge.GetValue() != 2 {
		t.Errorf("Expected 2 total items, got %v", totalItemsMetric.Gauge.GetValue())
	}

	// Additional test: Check metric output format
	expectedOutput := `
# HELP memoize_hits_total The total number of cache hits for the memoized function
# TYPE memoize_hits_total counter
memoize_hits_total{package="test_package", function="test_function"} 2
# HELP memoize_misses_total The total number of cache misses for the memoized function
# TYPE memoize_misses_total counter
memoize_misses_total{package="test_package", function="test_function"} 3
# HELP memoize_evictions_total The total number of cache evictions for the memoized function
# TYPE memoize_evictions_total counter
memoize_evictions_total{package="test_package", function="test_function"} 1
# HELP memoize_total_items The current number of items in the cache for the memoized function
# TYPE memoize_total_items gauge
memoize_total_items{package="test_package", function="test_function"} 2
`

	_ = testutil.CollectAndCompare(collector.hits, strings.NewReader(expectedOutput), "memoize_hits_total")
	_ = testutil.CollectAndCompare(collector.misses, strings.NewReader(expectedOutput), "memoize_misses_total")
	_ = testutil.CollectAndCompare(collector.evictions, strings.NewReader(expectedOutput), "memoize_evictions_total")
	_ = testutil.CollectAndCompare(collector.totalItems, strings.NewReader(expectedOutput), "memoize_total_items")
}
