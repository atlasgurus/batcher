package memoize

import (
	"reflect"
	"runtime"
	"strings"

	"github.com/atlasgurus/batcher/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type PrometheusMetricsCollector struct {
	hits       *prometheus.CounterVec
	misses     *prometheus.CounterVec
	evictions  *prometheus.CounterVec
	totalItems *prometheus.GaugeVec
	funcLabel  string
	pkgLabel   string
}

func NewPrometheusMetricsCollector(labels ...string) *PrometheusMetricsCollector {
	collector := &PrometheusMetricsCollector{}
	if len(labels) > 0 {
		collector.funcLabel = labels[0]
		if len(labels) > 1 {
			collector.pkgLabel = labels[1]
		}
	}
	return collector
}

func (p *PrometheusMetricsCollector) Setup(function interface{}) {
	pkgName, funcName := getFunctionName(function)
	if p.funcLabel == "" {
		p.funcLabel = funcName
	}
	if p.pkgLabel == "" {
		p.pkgLabel = pkgName
	}

	p.hits = metrics.SafeNewCounterVec(
		prometheus.CounterOpts{
			Name: "memoize_hits_total",
			Help: "The total number of cache hits for the memoized function",
		},
		[]string{"package", "function"},
	)

	p.misses = metrics.SafeNewCounterVec(
		prometheus.CounterOpts{
			Name: "memoize_misses_total",
			Help: "The total number of cache misses for the memoized function",
		},
		[]string{"package", "function"},
	)

	p.evictions = metrics.SafeNewCounterVec(
		prometheus.CounterOpts{
			Name: "memoize_evictions_total",
			Help: "The total number of cache evictions for the memoized function",
		},
		[]string{"package", "function"},
	)

	p.totalItems = metrics.SafeNewGaugeVec(
		prometheus.GaugeOpts{
			Name: "memoize_total_items",
			Help: "The current number of items in the cache for the memoized function",
		},
		[]string{"package", "function"},
	)
}

func (p *PrometheusMetricsCollector) Collect(metrics *MemoMetrics) {
	p.hits.WithLabelValues(p.pkgLabel, p.funcLabel).Add(float64(metrics.Hits.Swap(0)))
	p.misses.WithLabelValues(p.pkgLabel, p.funcLabel).Add(float64(metrics.Misses.Swap(0)))
	p.evictions.WithLabelValues(p.pkgLabel, p.funcLabel).Add(float64(metrics.Evictions.Swap(0)))
	p.totalItems.WithLabelValues(p.pkgLabel, p.funcLabel).Set(float64(metrics.TotalItems))
}

func getFunctionName(i interface{}) (string, string) {
	fullName := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	lastSlash := strings.LastIndexByte(fullName, '/')
	if lastSlash < 0 {
		lastSlash = 0
	}
	lastDot := strings.LastIndexByte(fullName[lastSlash:], '.')
	if lastDot < 0 {
		return "", fullName
	}
	pkgName := fullName[:lastSlash+lastDot]
	funcName := fullName[lastSlash+lastDot+1:]
	return pkgName, funcName
}
