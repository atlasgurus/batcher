//go:build !sqlite

package gorm

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUpdateBatcherStressWithData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	createNewTable := false

	// Clean up and pre-fill the table
	numIDs := 10000

	if createNewTable {
		db.Exec("DELETE FROM test_models")
		numBatches := 100
		startBatch := 0
		for j := startBatch; j < startBatch+numBatches; j++ {
			bulkInsert := make([]*TestModel, numIDs)
			for i := 0; i < numIDs; i++ {
				bulkInsert[i] = &TestModel{
					ID:        uint(j*numIDs + i + 1),
					Name:      fmt.Sprintf("Initial %d", j*numIDs+i+1),
					MyValue:   i,
					UpdatedAt: time.Now(),
				}
			}
			result := db.Create(&bulkInsert)
			assert.NoError(t, result.Error)
		}
	}

	t.Run("CASE Update", func(t *testing.T) {
		testStressUpdates(t, numIDs)
	})
}

func testStressUpdates(t *testing.T, numIDs int) {
	const poolSize = 100
	numRoutines := 1000
	updatesPerRoutine := 20
	var wg sync.WaitGroup
	// written counts writes still in flight. The test waits for them too, so
	// it does not return while its batchers still hold connections that the
	// next test needs, or log after it has finished.
	var written sync.WaitGroup
	var updateErrors int32
	start := time.Now()

	var batchPool [poolSize]*UpdateBatcher[*TestModel]
	for r := 0; r < numRoutines; r++ {
		wg.Add(1)
		if r < poolSize {
			batcher, err := NewUpdateBatcher[*TestModel](getDBProvider(), 50, 100*time.Millisecond, context.Background())
			assert.NoError(t, err)
			batchPool[r%poolSize] = batcher
		}
		go func(routine int, batcher *UpdateBatcher[*TestModel]) {
			defer wg.Done()

			updates := make([]*TestModel, 0, 10)
			for i := 0; i < updatesPerRoutine; i++ {
				id := uint(rand.Intn(numIDs) + i*numIDs + 1)
				model := &TestModel{
					ID:        id,
					Name:      fmt.Sprintf("Update from routine %d - %d", routine, i),
					MyValue:   routine*updatesPerRoutine + i,
					UpdatedAt: time.Now(),
				}
				updates = append(updates, model)

				written.Add(1)
				batcher.UpdateAsync(func(err error) {
					defer written.Done()
					if err != nil {
						atomic.AddInt32(&updateErrors, 1)
						t.Logf("Case-Update error: %v", err)
					}
				}, updates, []string{"name", "my_value", "updated_at"})
				updates = updates[:0]
			}
		}(r, batchPool[r%poolSize])
	}

	wg.Wait()
	written.Wait()
	duration := time.Since(start)

	assert.Zero(t, atomic.LoadInt32(&updateErrors))

	t.Logf("Duration: %v", duration)
	t.Logf("Errors: %d", atomic.LoadInt32(&updateErrors))
	t.Logf("Updates/sec: %.2f", float64(numRoutines*updatesPerRoutine)/duration.Seconds())
}
