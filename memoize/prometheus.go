package memoize

import (
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type PrometheusMetricsCollector struct {
	hits       *prometheus.CounterVec
	misses     *prometheus.CounterVec
	evictions  *prometheus.CounterVec
	totalItems *prometheus.GaugeVec
	funcLabel  string // User-defined function name label
	pkgLabel   string // User-defined package name label
	setupOnce  sync.Once
}

// NewPrometheusMetricsCollector initializes a collector with optional custom labels for function and package.
func NewPrometheusMetricsCollector(labels ...string) *PrometheusMetricsCollector {
	collector := &PrometheusMetricsCollector{}
	if len(labels) > 0 {
		collector.funcLabel = labels[0] // Custom function label
		if len(labels) > 1 {
			collector.pkgLabel = labels[1] // Custom package label
		}
	}
	return collector
}

func (p *PrometheusMetricsCollector) Setup(function interface{}) {
	p.setupOnce.Do(func() {
		pkgName, funcName := getFunctionName(function)
		if p.funcLabel == "" {
			p.funcLabel = funcName // Use actual function name if no custom label is provided
		}
		if p.pkgLabel == "" {
			p.pkgLabel = pkgName // Use actual package name if no custom label is provided
		}

		// Metric names are constant, independent of function or package name
		p.hits = p.safeNewCounterVec(
			prometheus.CounterOpts{
				Name: "memoize_hits_total",
				Help: "The total number of cache hits for the memoized function",
			},
			[]string{"package", "function"},
		)

		p.misses = p.safeNewCounterVec(
			prometheus.CounterOpts{
				Name: "memoize_misses_total",
				Help: "The total number of cache misses for the memoized function",
			},
			[]string{"package", "function"},
		)

		p.evictions = p.safeNewCounterVec(
			prometheus.CounterOpts{
				Name: "memoize_evictions_total",
				Help: "The total number of cache evictions for the memoized function",
			},
			[]string{"package", "function"},
		)

		p.totalItems = p.safeNewGaugeVec(
			prometheus.GaugeOpts{
				Name: "memoize_total_items",
				Help: "The current number of items in the cache for the memoized function",
			},
			[]string{"package", "function"},
		)
	})
}

func (p *PrometheusMetricsCollector) Collect(metrics *MemoMetrics) {
	p.hits.WithLabelValues(p.pkgLabel, p.funcLabel).Add(float64(metrics.Hits.Swap(0)))
	p.misses.WithLabelValues(p.pkgLabel, p.funcLabel).Add(float64(metrics.Misses.Swap(0)))
	p.evictions.WithLabelValues(p.pkgLabel, p.funcLabel).Add(float64(metrics.Evictions.Swap(0)))
	p.totalItems.WithLabelValues(p.pkgLabel, p.funcLabel).Set(float64(metrics.TotalItems))
}

func (p *PrometheusMetricsCollector) safeNewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	cv := prometheus.NewCounterVec(opts, labelNames)
	if err := prometheus.Register(cv); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector.(*prometheus.CounterVec)
		}
		// If it's another error, log it or handle it as appropriate for your application
	}
	return cv
}

func (p *PrometheusMetricsCollector) safeNewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	gv := prometheus.NewGaugeVec(opts, labelNames)
	if err := prometheus.Register(gv); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector.(*prometheus.GaugeVec)
		}
		// If it's another error, log it or handle it as appropriate for your application
	}
	return gv
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
