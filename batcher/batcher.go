package batcher

import (
	"context"
	"time"
)

// BatchProcessorInterface defines the common interface for batch processors
type BatchProcessorInterface[T any] interface {
	SubmitAndWait(item T) error
}

// DetailedBatchProcessor is a generic batch processor that supports individual error responses
type DetailedBatchProcessor[MT any, RT any] struct {
	input        chan detailedBatchItem[MT, RT]
	maxBatchSize int
	maxWaitTime  time.Duration
	processFn    func([]MT) RT
	errorParseFn func(MT, RT) error
	ctx          context.Context
}

type detailedBatchItem[MT any, RT any] struct {
	item MT
	resp chan error
}

// NewDetailedBatchProcessor creates a new DetailedBatchProcessor
func NewDetailedBatchProcessor[MT any, RT any](
	maxBatchSize int,
	maxWaitTime time.Duration,
	ctx context.Context,
	processFn func([]MT) RT,
	errorParseFn func(MT, RT) error,
) *DetailedBatchProcessor[MT, RT] {
	bp := &DetailedBatchProcessor[MT, RT]{
		input:        make(chan detailedBatchItem[MT, RT]),
		maxBatchSize: maxBatchSize,
		maxWaitTime:  maxWaitTime,
		processFn:    processFn,
		errorParseFn: errorParseFn,
		ctx:          ctx,
	}

	go bp.run()

	return bp
}

// SubmitAndWait submits an item for processing and waits for the result
// If the context is canceled, it processes the item directly
func (bp *DetailedBatchProcessor[MT, RT]) SubmitAndWait(item MT) error {
	respChan := make(chan error, 1)
	select {
	case bp.input <- detailedBatchItem[MT, RT]{item: item, resp: respChan}:
		return <-respChan
	case <-bp.ctx.Done():
		// Process the item directly when the context is canceled
		result := bp.processFn([]MT{item})
		return bp.errorParseFn(item, result)
	}
}

func (bp *DetailedBatchProcessor[MT, RT]) run() {
	var batch []MT
	var items []detailedBatchItem[MT, RT]
	timer := time.NewTimer(bp.maxWaitTime)
	timer.Stop() // Immediately stop the timer as it's not needed yet
	timerActive := false

	processBatch := func() {
		if len(batch) == 0 {
			return
		}
		result := bp.processFn(batch)
		for _, item := range items {
			err := bp.errorParseFn(item.item, result)
			item.resp <- err
		}
		batch = nil
		items = nil
		if timerActive {
			timer.Stop()
			timerActive = false
		}
	}

	for {
		select {
		case item := <-bp.input:
			batch = append(batch, item.item)
			items = append(items, item)
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

// BatchProcessor is the original implementation, now implemented using DetailedBatchProcessor
type BatchProcessor[T any] struct {
	detailed *DetailedBatchProcessor[T, error]
}

// NewBatchProcessor creates a new BatchProcessor using DetailedBatchProcessor
func NewBatchProcessor[T any](
	maxBatchSize int,
	maxWaitTime time.Duration,
	ctx context.Context,
	processFn func([]T) error,
) *BatchProcessor[T] {
	detailedProcessFn := func(batch []T) error {
		return processFn(batch)
	}
	errorParseFn := func(_ T, err error) error {
		return err
	}
	detailed := NewDetailedBatchProcessor(maxBatchSize, maxWaitTime, ctx, detailedProcessFn, errorParseFn)
	return &BatchProcessor[T]{detailed: detailed}
}

// SubmitAndWait submits an item for processing and waits for the result
func (bp *BatchProcessor[T]) SubmitAndWait(item T) error {
	return bp.detailed.SubmitAndWait(item)
}
