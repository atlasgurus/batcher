package batcher

import (
	"sync"

	"github.com/atlasgurus/batcher/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// maxErrorLabels caps the distinct values of the "error" label one
	// collector creates. Error messages often carry row values, and each
	// distinct label value is a time series kept for the life of the process.
	maxErrorLabels = 100
	// otherErrorLabel counts the errors whose message arrived after the cap
	// was reached.
	otherErrorLabel = "other"
)

type PrometheusMetricsCollector struct {
	batchesProcessed   *prometheus.CounterVec
	itemsProcessed     *prometheus.CounterVec
	processingDuration *prometheus.HistogramVec
	errors             *prometheus.CounterVec

	mu          sync.Mutex
	errorLabels map[string]struct{} // the error label values in use, at most maxErrorLabels
}

// errorLabel returns the label value to count an error message under: the
// message itself while fewer than maxErrorLabels are in use, otherErrorLabel
// after that.
func (p *PrometheusMetricsCollector) errorLabel(message string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.errorLabels[message]; ok {
		return message
	}
	if len(p.errorLabels) >= maxErrorLabels {
		return otherErrorLabel
	}
	if p.errorLabels == nil {
		p.errorLabels = make(map[string]struct{})
	}
	p.errorLabels[message] = struct{}{}
	return message
}

func NewPrometheusMetricsCollector(processorName string) *PrometheusMetricsCollector {
	return &PrometheusMetricsCollector{
		batchesProcessed: metrics.SafeNewCounterVec(
			prometheus.CounterOpts{
				Name:        "batcher_batches_processed_total",
				Help:        "The total number of batches processed",
				ConstLabels: prometheus.Labels{"processor": processorName},
			},
			[]string{},
		),
		itemsProcessed: metrics.SafeNewCounterVec(
			prometheus.CounterOpts{
				Name:        "batcher_items_processed_total",
				Help:        "The total number of items processed",
				ConstLabels: prometheus.Labels{"processor": processorName},
			},
			[]string{},
		),
		processingDuration: metrics.SafeNewHistogramVec(
			prometheus.HistogramOpts{
				Name:        "batcher_processing_duration_seconds",
				Help:        "The duration of batch processing operations",
				Buckets:     prometheus.ExponentialBuckets(0.001, 2, 16),
				ConstLabels: prometheus.Labels{"processor": processorName},
			},
			[]string{},
		),
		errors: metrics.SafeNewCounterVec(
			prometheus.CounterOpts{
				Name:        "batcher_errors_total",
				Help:        "The total number of errors encountered during batch processing",
				ConstLabels: prometheus.Labels{"processor": processorName},
			},
			[]string{"error"},
		),
	}
}

func (p *PrometheusMetricsCollector) Collect(metrics BatchMetrics) {
	p.batchesProcessed.WithLabelValues().Add(float64(metrics.BatchesProcessed))
	p.itemsProcessed.WithLabelValues().Add(float64(metrics.ItemsProcessed))
	p.processingDuration.WithLabelValues().Observe(metrics.TotalProcessingTime.Seconds())
	for err, count := range metrics.Errors {
		p.errors.WithLabelValues(p.errorLabel(err)).Add(float64(count))
	}
}
