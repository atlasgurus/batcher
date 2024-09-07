package batcher

import (
	"context"
	"time"
)

// BatchProcessor is a generic batch processor
type BatchProcessor[T any] struct {
	input        chan batchItem[T]
	maxBatchSize int
	maxWaitTime  time.Duration
	processFn    func([]T) error
	ctx          context.Context
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
	processFn func([]T) error,
) *BatchProcessor[T] {
	bp := &BatchProcessor[T]{
		input:        make(chan batchItem[T]),
		maxBatchSize: maxBatchSize,
		maxWaitTime:  maxWaitTime,
		processFn:    processFn,
		ctx:          ctx,
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
		return bp.processFn([]T{item})
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
		err := bp.processFn(batch)
		for _, ch := range respChans {
			ch <- err
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

// dummy changes to test the workflow
