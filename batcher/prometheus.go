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
	processorName      string
}

func NewPrometheusMetricsCollector(processorName string) *PrometheusMetricsCollector {
	return &PrometheusMetricsCollector{
		batchesProcessed: metrics.SafeNewCounterVec(
			prometheus.CounterOpts{
				Name: "batcher_batches_processed_total",
				Help: "The total number of batches processed",
			},
			[]string{"processor"},
		),
		itemsProcessed: metrics.SafeNewCounterVec(
			prometheus.CounterOpts{
				Name: "batcher_items_processed_total",
				Help: "The total number of items processed",
			},
			[]string{"processor"},
		),
		processingDuration: metrics.SafeNewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "batcher_processing_duration_seconds",
				Help:    "The duration of batch processing operations",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
			},
			[]string{"processor"},
		),
		errors: metrics.SafeNewCounterVec(
			prometheus.CounterOpts{
				Name: "batcher_errors_total",
				Help: "The total number of errors encountered during batch processing",
			},
			[]string{"processor"},
		),
		processorName: processorName,
	}
}

func (p *PrometheusMetricsCollector) Collect(metrics BatchMetrics) {
	p.batchesProcessed.WithLabelValues(p.processorName).Add(float64(metrics.BatchesProcessed))
	p.itemsProcessed.WithLabelValues(p.processorName).Add(float64(metrics.ItemsProcessed))
	p.processingDuration.WithLabelValues(p.processorName).Observe(metrics.TotalProcessingTime.Seconds())
	p.errors.WithLabelValues(p.processorName).Add(float64(metrics.Errors))
}
