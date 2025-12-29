package batcher

import (
	"github.com/atlasgurus/batcher/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type PrometheusMetricsCollector struct {
	batchesProcessed   *prometheus.CounterVec
	itemsProcessed     *prometheus.CounterVec
	processingDuration *prometheus.HistogramVec
	errors             *prometheus.CounterVec
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
		p.errors.WithLabelValues(err).Add(float64(count))
	}
}
