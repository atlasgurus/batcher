package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

func SafeNewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	cv := prometheus.NewCounterVec(opts, labelNames)
	if err := prometheus.Register(cv); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector.(*prometheus.CounterVec)
		}
		// Handle other errors as appropriate for your application
	}
	return cv
}

func SafeNewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	gv := prometheus.NewGaugeVec(opts, labelNames)
	if err := prometheus.Register(gv); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector.(*prometheus.GaugeVec)
		}
		// Handle other errors as appropriate for your application
	}
	return gv
}

func SafeNewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec {
	hv := prometheus.NewHistogramVec(opts, labelNames)
	if err := prometheus.Register(hv); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector.(*prometheus.HistogramVec)
		}
		// Handle other errors as appropriate for your application
	}
	return hv
}
