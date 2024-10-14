package memoize

import (
	"reflect"
	"runtime"
	"strings"
	"unicode"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type PrometheusMetricsCollector struct {
	hits       *prometheus.CounterVec
	misses     *prometheus.CounterVec
	evictions  *prometheus.CounterVec
	totalItems *prometheus.GaugeVec
	customName string
}

func NewPrometheusMetricsCollector(customName ...string) *PrometheusMetricsCollector {
	collector := &PrometheusMetricsCollector{}
	if len(customName) > 0 {
		collector.customName = customName[0]
	}
	return collector
}

func (p *PrometheusMetricsCollector) Setup(function interface{}) {
	pkgName, funcName := getFunctionName(function)
	metricName := p.getMetricName(pkgName, funcName)

	p.hits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricName + "_memoize_hits_total",
			Help: "The total number of cache hits for the memoized function",
		},
		[]string{"function"},
	)

	p.misses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricName + "_memoize_misses_total",
			Help: "The total number of cache misses for the memoized function",
		},
		[]string{"function"},
	)

	p.evictions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricName + "_memoize_evictions_total",
			Help: "The total number of cache evictions for the memoized function",
		},
		[]string{"function"},
	)

	p.totalItems = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: metricName + "_memoize_total_items",
			Help: "The current number of items in the cache for the memoized function",
		},
		[]string{"function"},
	)
}

func (p *PrometheusMetricsCollector) Collect(metrics *MemoMetrics) {
	p.hits.WithLabelValues("").Add(float64(metrics.Hits.Swap(0)))
	p.misses.WithLabelValues("").Add(float64(metrics.Misses.Swap(0)))
	p.evictions.WithLabelValues("").Add(float64(metrics.Evictions.Swap(0)))
	p.totalItems.WithLabelValues("").Set(float64(metrics.TotalItems))
}

func (p *PrometheusMetricsCollector) getMetricName(pkgName, funcName string) string {
	if p.customName != "" {
		return sanitizeMetricName(p.customName)
	}
	return sanitizeMetricName(pkgName + "_" + funcName)
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

func sanitizeMetricName(name string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			return r
		}
		return '_'
	}, name)
}
