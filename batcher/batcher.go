package batcher

import (
	"context"
	"time"
)

// BatchProcessorInterface defines the common interface for batch processors
type BatchProcessorInterface[T any] interface {
	SubmitAndWait(item T) error
	Submit(item T, callback func(error))
}

// BatchProcessor is a generic batch processor
type MetricsCollector interface {
	Setup(batcher *BatchProcessor[any])
	Collect(metrics BatchMetrics)
}

type BatchProcessor[T any] struct {
	input            chan batchItem[T]
	maxBatchSize     int
	maxWaitTime      time.Duration
	processFn        func([]T) []error
	ctx              context.Context
	metrics          *BatchMetrics
	metricsCollector MetricsCollector
}

type batchItem[T any] struct {
	item T
	resp chan error
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
		WithMaxBatchSize[T](maxBatchSize),
		WithMaxWaitTime[T](maxWaitTime),
	)
}

type BatchProcessorOption[T any] func(*BatchProcessor[T])

func WithMaxBatchSize[T any](size int) BatchProcessorOption[T] {
	return func(bp *BatchProcessor[T]) {
		bp.maxBatchSize = size
	}
}

func WithMaxWaitTime[T any](duration time.Duration) BatchProcessorOption[T] {
	return func(bp *BatchProcessor[T]) {
		bp.maxWaitTime = duration
	}
}

func NewBatchProcessorWithOptions[T any](
	ctx context.Context,
	processFn func([]T) []error,
	options ...BatchProcessorOption[T],
) BatchProcessorInterface[T] {
	bp := &BatchProcessor[T]{
		input:            make(chan batchItem[T]),
		maxBatchSize:     100,         // Default max batch size
		maxWaitTime:      time.Second, // Default max wait time
		processFn:        processFn,
		ctx:              ctx,
		metrics:          &BatchMetrics{},
		metricsCollector: nil, // Initialize with nil
	}

	for _, option := range options {
		option(bp)
	}

	go bp.run()

	return bp
}

// SubmitAndWait submits an item for processing and waits for the result
// If the context is canceled, it processes the item directly
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

// Submit submits an item for processing and calls the callback function when done.
// If the context is canceled, it processes the item directly.
// The function is non-blocking
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
	timer := time.NewTimer(bp.maxWaitTime)
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
		if bp.metricsCollector != nil {
			errorCount := 0
			for _, err := range errs {
				if err != nil {
					errorCount++
				}
			}

			bp.metricsCollector.Collect(BatchMetrics{
				BatchesProcessed:    1,
				ItemsProcessed:      int64(len(batch)),
				TotalProcessingTime: time.Since(startTime),
				Errors:              int64(errorCount),
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
				timer.Reset(bp.maxWaitTime)
				timerActive = true
			}
			if len(batch) >= bp.maxBatchSize {
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
	Errors              int64
}

func WithMetrics[T any](collector MetricsCollector) BatchProcessorOption[T] {
	return func(bp *BatchProcessor[T]) {
		bp.metricsCollector = collector
	}
}
