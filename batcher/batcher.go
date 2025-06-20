package batcher

import (
	"context"
	"time"
)

// MetricsCollector defines the interface for collecting batch processing metrics
type MetricsCollector interface {
	Collect(metrics BatchMetrics)
}

// BatchProcessorConfig holds all configuration options for the batch processor
type BatchProcessorConfig struct {
	MaxBatchSize     int
	MaxWaitTime      time.Duration
	MetricsCollector MetricsCollector
	DecomposeFields  bool
}

// DefaultConfig returns a BatchProcessorConfig with default values
func DefaultConfig() BatchProcessorConfig {
	return BatchProcessorConfig{
		MaxBatchSize: 100,
		MaxWaitTime:  time.Second,
	}
}

// BatchProcessorInterface defines the common interface for batch processors
type BatchProcessorInterface[T any] interface {
	SubmitAndWait(item T) error
	Submit(item T, callback func(error))
}

// BatchProcessor is a generic batch processor
type BatchProcessor[T any] struct {
	input     chan batchItem[T]
	config    BatchProcessorConfig
	processFn func([]T) []error
	ctx       context.Context
	metrics   *BatchMetrics
}

type batchItem[T any] struct {
	item T
	resp chan error
}

// Option defines a function type for modifying BatchProcessorConfig
type Option func(*BatchProcessorConfig)

// WithMaxBatchSize sets the maximum batch size
func WithMaxBatchSize(size int) Option {
	return func(config *BatchProcessorConfig) {
		config.MaxBatchSize = size
	}
}

// WithMaxWaitTime sets the maximum wait time
func WithMaxWaitTime(duration time.Duration) Option {
	return func(config *BatchProcessorConfig) {
		config.MaxWaitTime = duration
	}
}

// WithMaxWaitTime sets the maximum wait time
func WithDecomposeFields(decomposeFields bool) Option {
	return func(config *BatchProcessorConfig) {
		config.DecomposeFields = decomposeFields
	}
}

// WithMetrics sets the metrics collector
func WithMetrics(collector MetricsCollector) Option {
	return func(config *BatchProcessorConfig) {
		config.MetricsCollector = collector
	}
}

// NewBatchProcessor creates a new BatchProcessor
func NewBatchProcessor[T any](
	maxBatchSize int,
	maxWaitTime time.Duration,
	ctx context.Context,
	processFn func([]T) []error,
) BatchProcessorInterface[T] {
	return NewBatchProcessorWithOptions(
		ctx,
		processFn,
		WithMaxBatchSize(maxBatchSize),
		WithMaxWaitTime(maxWaitTime),
	)
}

func NewBatchProcessorWithOptions[T any](
	ctx context.Context,
	processFn func([]T) []error,
	options ...Option,
) BatchProcessorInterface[T] {
	config := DefaultConfig()

	// Apply all options
	for _, option := range options {
		option(&config)
	}

	bp := &BatchProcessor[T]{
		input:     make(chan batchItem[T]),
		config:    config,
		processFn: processFn,
		ctx:       ctx,
		metrics:   &BatchMetrics{},
	}

	go bp.run()

	return bp
}

// SubmitAndWait submits an item for processing and waits for the result
func (bp *BatchProcessor[T]) SubmitAndWait(item T) error {
	respChan := make(chan error, 1)
	select {
	case bp.input <- batchItem[T]{item: item, resp: respChan}:
		return <-respChan
	case <-bp.ctx.Done():
		// Process the item directly when the context is canceled
		return bp.processFn([]T{item})[0]
	}
}

// Submit submits an item for processing and calls the callback function when done
func (bp *BatchProcessor[T]) Submit(item T, callback func(error)) {
	respChan := make(chan error, 1)
	select {
	case bp.input <- batchItem[T]{item: item, resp: respChan}:
		go func() {
			callback(<-respChan)
		}()
	case <-bp.ctx.Done():
		go func() {
			callback(bp.processFn([]T{item})[0])
		}()
	}
}

func (bp *BatchProcessor[T]) run() {
	var batch []T
	var respChans []chan error
	timer := time.NewTimer(bp.config.MaxWaitTime)
	timer.Stop() // Immediately stop the timer as it's not needed yet
	timerActive := false

	processBatch := func() {
		if len(batch) == 0 {
			return
		}
		startTime := time.Now()
		errs := bp.processFn(batch)
		for i, ch := range respChans {
			ch <- errs[i]
		}

		// Collect metrics after processing the batch
		if bp.config.MetricsCollector != nil {
			errorCount := make(map[string]int64)
			for _, err := range errs {
				if err != nil {
					errorCount[err.Error()]++
				}
			}

			bp.config.MetricsCollector.Collect(BatchMetrics{
				BatchesProcessed:    1,
				ItemsProcessed:      int64(len(batch)),
				TotalProcessingTime: time.Since(startTime),
				Errors:              errorCount,
			})
		}
		batch = nil
		respChans = nil
		if timerActive {
			timer.Stop()
			timerActive = false
		}
	}

	for {
		select {
		case item := <-bp.input:
			batch = append(batch, item.item)
			respChans = append(respChans, item.resp)
			if len(batch) == 1 && !timerActive {
				timer.Reset(bp.config.MaxWaitTime)
				timerActive = true
			}
			if len(batch) >= bp.config.MaxBatchSize {
				processBatch()
			}
		case <-timer.C:
			processBatch()
			timerActive = false
		case <-bp.ctx.Done():
			processBatch() // Process any remaining items
			return
		}
	}
}

func RepeatErr(n int, err error) []error {
	result := make([]error, n)
	for i := 0; i < n; i++ {
		result[i] = err
	}
	return result
}

type BatchMetrics struct {
	BatchesProcessed    int64
	ItemsProcessed      int64
	TotalProcessingTime time.Duration
	Errors              map[string]int64
}
