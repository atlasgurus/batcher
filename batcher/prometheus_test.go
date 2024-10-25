package batcher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestPrometheusMetricsCollector(t *testing.T) {
	ctx := context.Background()
	processFn := func(items []int) []error {
		errors := make([]error, len(items))
		for i, item := range items {
			if item%2 == 0 {
				errors[i] = nil
			} else {
				errors[i] = fmt.Errorf("odd number")
			}
		}
		return errors
	}

	collector := NewPrometheusMetricsCollector("test_processor")
	batchProcessor := NewBatchProcessorWithOptions(
		ctx,
		processFn,
		WithMaxBatchSize(3),
		WithMaxWaitTime(50*time.Millisecond),
		WithMetrics(collector),
	)

	// Process some items
	for i := 0; i < 5; i++ {
		item := i
		batchProcessor.Submit(i, func(err error) {
			if err != nil && item%2 == 0 {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}

	// Wait for metrics to be collected
	time.Sleep(100 * time.Millisecond)

	// Check metrics
	expectedOutput := `
# HELP batcher_batches_processed_total The total number of batches processed
# TYPE batcher_batches_processed_total counter
batcher_batches_processed_total{processor="test_processor"} 2
# HELP batcher_items_processed_total The total number of items processed
# TYPE batcher_items_processed_total counter
batcher_items_processed_total{processor="test_processor"} 5
# HELP batcher_errors_total The total number of errors encountered during batch processing
# TYPE batcher_errors_total counter
batcher_errors_total{processor="test_processor"} 2
`

	if err := testutil.CollectAndCompare(collector.batchesProcessed, strings.NewReader(expectedOutput), "batcher_batches_processed_total"); err != nil {
		t.Errorf("Unexpected collecting result for batcher_batches_processed_total: %s", err)
	}

	if err := testutil.CollectAndCompare(collector.itemsProcessed, strings.NewReader(expectedOutput), "batcher_items_processed_total"); err != nil {
		t.Errorf("Unexpected collecting result for batcher_items_processed_total: %s", err)
	}

	if err := testutil.CollectAndCompare(collector.errors, strings.NewReader(expectedOutput), "batcher_errors_total"); err != nil {
		t.Errorf("Unexpected collecting result for batcher_errors_total: %s", err)
	}

	// Check the histogram separately
	gatherer := prometheus.DefaultGatherer
	metricFamilies, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("Error gathering metrics: %s", err)
	}

	var histogramFound bool
	for _, metricFamily := range metricFamilies {
		if metricFamily.GetName() == "batcher_processing_duration_seconds" {
			histogramFound = true
			if *metricFamily.Type != dto.MetricType_HISTOGRAM {
				t.Errorf("Expected HISTOGRAM type, got %v", metricFamily.Type)
			}
			for _, metric := range metricFamily.Metric {
				histogram := metric.GetHistogram()
				if histogram == nil {
					t.Errorf("Histogram data is nil")
					continue
				}
				sum := histogram.GetSampleSum()
				if sum <= 0 {
					t.Errorf("Expected positive sum, got %f", sum)
				}
				count := histogram.GetSampleCount()
				if count != 2 {
					t.Errorf("Expected sample count of 2, got %d", count)
				}
				// Check bucket counts
				buckets := histogram.GetBucket()
				if len(buckets) < 3 {
					t.Errorf("Expected at least 3 buckets, got %d", len(buckets))
				} else {
					// All buckets should have a count of 2, as both batches are processed quickly
					for i, bucket := range buckets[:3] {
						if *bucket.CumulativeCount != 2 {
							t.Errorf("Bucket %d: expected count 2, got %d", i, *bucket.CumulativeCount)
						}
					}
				}
			}
		}
	}
	if !histogramFound {
		t.Errorf("Histogram metric not found")
	}
}
