package batcher

import (
	"context"
	"errors"
	"fmt"
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
			func(items []int) error {
				return nil
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
			func(items []int) error {
				processed = append(processed, items...)
				return nil
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
			func(items []int) error {
				processed = append(processed, items...)
				return nil
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
			func(items []int) error {
				return expectedError
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
			func(items []int) error {
				processed = append(processed, items...)
				return nil
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
}

func TestDetailedBatchProcessor(t *testing.T) {
	t.Run("ProcessSingleItem", func(t *testing.T) {
		ctx := context.Background()
		processor := NewDetailedBatchProcessor(
			5,
			100*time.Millisecond,
			ctx,
			func(items []int) []int {
				return items
			},
			func(_ int, result []int) error {
				return nil
			},
		)

		err := processor.SubmitAndWait(1)
		assert.NoError(t, err)
	})

	t.Run("ProcessBatch", func(t *testing.T) {
		ctx := context.Background()
		processed := make([]int, 0)
		processor := NewDetailedBatchProcessor(
			3,
			100*time.Millisecond,
			ctx,
			func(items []int) []int {
				processed = append(processed, items...)
				return items
			},
			func(_ int, _ []int) error {
				return nil
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
		processor := NewDetailedBatchProcessor(
			5,
			50*time.Millisecond,
			ctx,
			func(items []int) []int {
				processed = append(processed, items...)
				return items
			},
			func(_ int, _ []int) error {
				return nil
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
		processor := NewDetailedBatchProcessor(
			5,
			100*time.Millisecond,
			ctx,
			func(items []int) []int {
				return items
			},
			func(item int, result []int) error {
				return fmt.Errorf("error for item %d", item)
			},
		)

		err := processor.SubmitAndWait(1)
		assert.EqualError(t, err, "error for item 1")
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		processed := make([]int, 0)
		processor := NewDetailedBatchProcessor(
			5,
			100*time.Millisecond,
			ctx,
			func(items []int) []int {
				processed = append(processed, items...)
				return items
			},
			func(_ int, _ []int) error {
				return nil
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

	t.Run("IndividualErrors", func(t *testing.T) {
		ctx := context.Background()
		var processor BatchProcessorInterface[int] = NewDetailedBatchProcessor(
			5,
			100*time.Millisecond,
			ctx,
			func(items []int) []int {
				return items
			},
			func(item int, _ []int) error {
				if item%2 == 0 {
					return fmt.Errorf("even number: %d", item)
				}
				return nil
			},
		)

		var wg sync.WaitGroup
		results := make(map[int]error)
		var resultsMu sync.Mutex

		for i := 1; i <= 5; i++ {
			wg.Add(1)
			go func(item int) {
				defer wg.Add(-1)
				err := processor.SubmitAndWait(item)
				resultsMu.Lock()
				results[item] = err
				resultsMu.Unlock()
			}(i)
		}
		wg.Wait()

		for i := 1; i <= 5; i++ {
			if i%2 == 0 {
				assert.EqualError(t, results[i], fmt.Sprintf("even number: %d", i))
			} else {
				assert.NoError(t, results[i])
			}
		}
	})
}
