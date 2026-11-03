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

func TestSubmitDoesNotWaitForBatchInFlight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	processor := NewBatchProcessor(
		2,
		time.Millisecond,
		ctx,
		func(items []int) []error {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return make([]error, len(items))
		},
	)

	// The first submit starts a batch that blocks until release is closed.
	processor.Submit(0, func(error) {})
	<-started

	// While that batch is in flight, a full batch's worth of submits must
	// return without waiting for processFn.
	done := make(chan struct{})
	go func() {
		processor.Submit(1, func(error) {})
		processor.Submit(2, func(error) {})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Submit waited for the batch in flight")
	}
	close(release)
}

func TestShutdownProcessesBufferedItems(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	var mu sync.Mutex
	var processed []int
	processor := NewBatchProcessor(
		5,
		time.Hour,
		ctx,
		func(items []int) []error {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			mu.Lock()
			processed = append(processed, items...)
			mu.Unlock()
			return make([]error, len(items))
		},
	)

	var callbacks sync.WaitGroup
	submit := func(i int) {
		callbacks.Add(1)
		processor.Submit(i, func(err error) {
			assert.NoError(t, err)
			callbacks.Done()
		})
	}

	// Fill one batch so processFn starts and blocks.
	for i := 0; i < 5; i++ {
		submit(i)
	}
	<-started
	// These sit in the buffer behind the blocked batch.
	for i := 5; i < 8; i++ {
		submit(i)
	}

	cancel()
	close(release)
	processor.WaitForShutdown()
	callbacks.Wait()

	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, processed)
}

func TestSubmitAfterShutdownProcessesDirectly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	var processed []int
	processor := NewBatchProcessor(
		5,
		time.Hour,
		ctx,
		func(items []int) []error {
			mu.Lock()
			processed = append(processed, items...)
			mu.Unlock()
			return make([]error, len(items))
		},
	)
	cancel()
	processor.WaitForShutdown()

	require.NoError(t, processor.SubmitAndWait(1))
	errCh := make(chan error, 1)
	processor.Submit(2, func(err error) { errCh <- err })
	require.NoError(t, <-errCh)

	mu.Lock()
	defer mu.Unlock()
	assert.ElementsMatch(t, []int{1, 2}, processed)
}

// TestShutdownLosesNothingUnderConcurrentSubmits races submitters against
// shutdown. Every item must be processed exactly once and every caller must
// get an answer, whether its item went through the batch, the final drain, or
// the direct path.
func TestShutdownLosesNothingUnderConcurrentSubmits(t *testing.T) {
	for round := 0; round < 50; round++ {
		ctx, cancel := context.WithCancel(context.Background())
		var mu sync.Mutex
		seen := make(map[int]int)
		processor := NewBatchProcessor(
			4,
			time.Millisecond,
			ctx,
			func(items []int) []error {
				mu.Lock()
				for _, it := range items {
					seen[it]++
				}
				mu.Unlock()
				return make([]error, len(items))
			},
		)

		const submitters, perSubmitter = 8, 25
		var wg sync.WaitGroup
		for s := 0; s < submitters; s++ {
			wg.Add(1)
			go func(s int) {
				defer wg.Done()
				for i := 0; i < perSubmitter; i++ {
					item := s*perSubmitter + i
					if i%2 == 0 {
						assert.NoError(t, processor.SubmitAndWait(item))
						continue
					}
					errCh := make(chan error, 1)
					processor.Submit(item, func(err error) { errCh <- err })
					select {
					case err := <-errCh:
						assert.NoError(t, err)
					case <-time.After(5 * time.Second):
						t.Errorf("item %d: callback never ran", item)
					}
				}
			}(s)
		}
		time.Sleep(time.Duration(round%5) * time.Millisecond)
		cancel()
		wg.Wait()
		processor.WaitForShutdown()

		mu.Lock()
		require.Len(t, seen, submitters*perSubmitter, "round %d", round)
		for item, n := range seen {
			require.Equal(t, 1, n, "round %d item %d processed %d times", round, item, n)
		}
		mu.Unlock()
	}
}
