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
}
