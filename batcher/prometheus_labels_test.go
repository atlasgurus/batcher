package batcher

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

// Error messages often carry row values, such as MySQL's "Duplicate entry
// '123' for key". Used as label values without a bound, every distinct
// message became a new time series for the life of the process.
func TestErrorLabelsAreBounded(t *testing.T) {
	// Built by hand on an unregistered vector, so this test leaves the global
	// registry, which TestPrometheusMetricsCollector reads, untouched.
	c := &PrometheusMetricsCollector{
		batchesProcessed:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "batches"}, nil),
		itemsProcessed:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "items"}, nil),
		processingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "duration"}, nil),
		errors:             prometheus.NewCounterVec(prometheus.CounterOpts{Name: "errors"}, []string{"error"}),
	}
	const extra = 50
	for i := 0; i < maxErrorLabels+extra; i++ {
		c.Collect(BatchMetrics{Errors: map[string]int64{fmt.Sprintf("Duplicate entry '%d' for key 'PRIMARY'", i): 1}})
	}

	assert.LessOrEqual(t, testutil.CollectAndCount(c.errors), maxErrorLabels+1, "at most the cap plus the overflow label")
	assert.Equal(t, float64(extra), testutil.ToFloat64(c.errors.WithLabelValues(otherErrorLabel)), "errors past the cap are still counted")
	assert.Equal(t, float64(1), testutil.ToFloat64(c.errors.WithLabelValues("Duplicate entry '0' for key 'PRIMARY'")), "a message seen early keeps its own label")
}
