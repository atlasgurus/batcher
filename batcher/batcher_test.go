package batcher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchProcessor(t *testing.T) {
	t.Run("ProcessSingleItem", func(t *testing.T) {
		ctx := context.Background()
		processor := NewBatchProcessor(
			5,
			100*time.Millisecond,
			ctx,
			func(items []int) []error {
				return make([]error, len(items))
			},
		)

		err := processor.SubmitAndWait(1)
		assert.NoError(t, err)
	})

	t.Run("ProcessBatch", func(t *testing.T) {
		ctx := context.Background()
		processed := make([]int, 0)
		processor := NewBatchProcessor(
			3,
			100*time.Millisecond,
			ctx,
			func(items []int) []error {
				processed = append(processed, items...)
				return make([]error, len(items))
			},
		)

		var wg sync.WaitGroup
		for i := 1; i <= 5; i++ {
			wg.Add(1)
			go func(item int) {
				defer wg.Add(-1)
				err := processor.SubmitAndWait(item)
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()

		assert.Len(t, processed, 5)
		assert.ElementsMatch(t, []int{1, 2, 3, 4, 5}, processed)
	})

	t.Run("ProcessByTimeout", func(t *testing.T) {
		ctx := context.Background()
		processed := make([]int, 0)
		processor := NewBatchProcessor(
			5,
			50*time.Millisecond,
			ctx,
			func(items []int) []error {
				processed = append(processed, items...)
				return make([]error, len(items))
			},
		)

		err := processor.SubmitAndWait(1)
		assert.NoError(t, err)

		err = processor.SubmitAndWait(2)
		assert.NoError(t, err)

		assert.Len(t, processed, 2)
		assert.Equal(t, []int{1, 2}, processed)
	})

	t.Run("ProcessorError", func(t *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("processing error")
		processor := NewBatchProcessor(
			5,
			100*time.Millisecond,
			ctx,
			func(items []int) []error {
				result := make([]error, len(items))
				for i := range items {
					result[i] = expectedError
				}
				return result
			},
		)

		err := processor.SubmitAndWait(1)
		assert.Equal(t, expectedError, err)
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		processed := make([]int, 0)
		processor := NewBatchProcessor(
			5,
			100*time.Millisecond,
			ctx,
			func(items []int) []error {
				processed = append(processed, items...)
				return make([]error, len(items))
			},
		)

		// Submit one item normally
		err := processor.SubmitAndWait(1)
		require.NoError(t, err)

		// Cancel the context
		cancel()

		// Submit another item after cancellation
		err = processor.SubmitAndWait(2)
		require.NoError(t, err)

		assert.Len(t, processed, 2)
		assert.Equal(t, []int{1, 2}, processed)
	})

	t.Run("ProcessAsync", func(t *testing.T) {
		ctx := context.Background()
		processor := NewBatchProcessor(
			5,
			100*time.Millisecond,
			ctx,
			func(items []int) []error {
				return make([]error, len(items))
			},
		)

		done := make(chan bool)
		processor.Submit(1, func(err error) {
			assert.NoError(t, err)
			done <- true
		})
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("timeout waiting for callback")
		}
	})

	t.Run("ProcessAsyncContextCancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		processor := NewBatchProcessor(
			5,
			100*time.Millisecond,
			ctx,
			func(items []int) []error {
				return make([]error, len(items))
			},
		)

		done := make(chan bool)
		cancel()
		processor.Submit(1, func(err error) {
			assert.NoError(t, err)
			done <- true
		})
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("timeout waiting for callback")
		}
	})

	t.Run("ProcessAsyncError", func(t *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("processing error")
		processor := NewBatchProcessor(
			5,
			100*time.Millisecond,
			ctx,
			func(items []int) []error {
				return RepeatErr(len(items), expectedError)
			},
		)

		done := make(chan bool)
		processor.Submit(1, func(err error) {
			assert.Equal(t, expectedError, err)
			done <- true
		})
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("timeout waiting for callback")
		}
	})

	t.Run("ProcessAsyncMultipleWithErrorsAndSuccess", func(t *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("processing error")
		processor := NewBatchProcessor(
			5,
			100*time.Millisecond,
			ctx,
			func(items []int) []error {
				errs := make([]error, len(items))
				for i, item := range items {
					if item%3 == 0 {
						errs[i] = expectedError
					} else {
						errs[i] = nil
					}
				}
				return errs
			},
		)

		const items = 100
		done := make([]chan bool, items)
		for i := range done {
			done[i] = make(chan bool)
		}

		for i := 0; i < items; i++ {
			go func(index int) {
				processor.Submit(index, func(err error) {
					if index%3 == 0 {
						assert.Equal(t, expectedError, err)
					} else {
						assert.NoError(t, err)
					}
					done[index] <- true
				})
			}(i)
		}

		timeout := time.After(5 * time.Second)
		for i := 0; i < items; i++ {
			select {
			case <-done[i]:
			case <-timeout:
				t.Error("timeout waiting for all callbacks")
				return
			}
		}
	})

	t.Run("WaitForShutdown", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var mu sync.Mutex
		processed := make([]int, 0)
		processor := NewBatchProcessor(
			5,
			100*time.Millisecond,
			ctx,
			func(items []int) []error {
				mu.Lock()
				processed = append(processed, items...)
				mu.Unlock()
				return make([]error, len(items))
			},
		)

		// Submit some items
		var wg sync.WaitGroup
		for i := 1; i <= 3; i++ {
			wg.Add(1)
			go func(item int) {
				defer wg.Done()
				err := processor.SubmitAndWait(item)
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()

		// Cancel context to initiate shutdown
		cancel()

		// Wait for shutdown to complete
		shutdownDone := make(chan struct{})
		go func() {
			processor.WaitForShutdown()
			close(shutdownDone)
		}()

		// Should complete within reasonable time
		select {
		case <-shutdownDone:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("WaitForShutdown did not complete in time")
		}

		// Verify all items were processed
		mu.Lock()
		assert.Len(t, processed, 3)
		mu.Unlock()
	})

	t.Run("WaitForShutdownMultipleBatchers", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// Create multiple batchers
		batchers := make([]BatchProcessorInterface[int], 3)
		processedCounts := make([]int, 3)
		var mus [3]sync.Mutex

		for i := 0; i < 3; i++ {
			idx := i
			batchers[i] = NewBatchProcessor(
				5,
				100*time.Millisecond,
				ctx,
				func(items []int) []error {
					mus[idx].Lock()
					processedCounts[idx] += len(items)
					mus[idx].Unlock()
					return make([]error, len(items))
				},
			)
		}

		// Submit items to each batcher
		for i, bp := range batchers {
			for j := 0; j < 5; j++ {
				go func(processor BatchProcessorInterface[int], val int) {
					err := processor.SubmitAndWait(val)
					assert.NoError(t, err)
				}(bp, i*10+j)
			}
		}

		// Give time for submissions
		time.Sleep(50 * time.Millisecond)

		// Cancel context to initiate shutdown
		cancel()

		// Wait for all batchers to shut down
		var wg sync.WaitGroup
		for _, bp := range batchers {
			wg.Add(1)
			go func(processor BatchProcessorInterface[int]) {
				defer wg.Done()
				processor.WaitForShutdown()
			}(bp)
		}

		// Wait with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("WaitForShutdown did not complete for all batchers in time")
		}

		// Verify all items were processed
		for i := 0; i < 3; i++ {
			mus[i].Lock()
			assert.Equal(t, 5, processedCounts[i], "batcher %d should have processed 5 items", i)
			mus[i].Unlock()
		}
	})

	t.Run("WaitForShutdownWithPendingItems", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var mu sync.Mutex
		processed := make([]int, 0)
		processingStarted := make(chan struct{})
		once := sync.Once{}

		processor := NewBatchProcessor(
			10,
			1*time.Second, // Long wait time so items stay pending
			ctx,
			func(items []int) []error {
				once.Do(func() { close(processingStarted) })
				mu.Lock()
				processed = append(processed, items...)
				mu.Unlock()
				return make([]error, len(items))
			},
		)

		// Submit items but don't reach batch size
		var wg sync.WaitGroup
		for i := 1; i <= 3; i++ {
			wg.Add(1)
			go func(item int) {
				defer wg.Done()
				err := processor.SubmitAndWait(item)
				assert.NoError(t, err)
			}(i)
		}

		// Give time for all submissions to be queued
		time.Sleep(50 * time.Millisecond)

		// Cancel context to trigger shutdown with pending items
		cancel()

		// Wait for processing to start
		select {
		case <-processingStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("processing did not start")
		}

		// Wait for all items to be processed
		wg.Wait()

		// Wait for shutdown
		shutdownDone := make(chan struct{})
		go func() {
			processor.WaitForShutdown()
			close(shutdownDone)
		}()

		select {
		case <-shutdownDone:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("WaitForShutdown did not complete in time")
		}

		// Verify all pending items were processed before shutdown
		mu.Lock()
		assert.Len(t, processed, 3)
		mu.Unlock()
	})
}
