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
	"gorm.io/gorm"
)

func TestUpdateBatcherStressWithData(t *testing.T) {
	t.Skip("Skip this test as it is too slow")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher, err := NewUpdateBatcher[*TestModel](getDBProvider(), 10, 100*time.Millisecond, ctx)
	assert.NoError(t, err)
	createNewTable := false

	// Clean up and pre-fill the table
	numIDs := 10000

	if createNewTable {
		//db.Exec("DELETE FROM test_models")
		numBatches := 100000
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
		testStressUpdates(t, batcher, numIDs)
	})
}

func testStressUpdates(t *testing.T, batcher *UpdateBatcher[*TestModel], numIDs int) {
	numRoutines := 50
	updatesPerRoutine := 100000
	var wg sync.WaitGroup
	var updateErrors int32
	start := time.Now()

	for r := 0; r < numRoutines; r++ {
		wg.Add(1)
		go func(routine int) {
			defer wg.Done()

			updates := make([]*TestModel, 0, 10)
			for i := 0; i < updatesPerRoutine; i++ {
				id := uint(rand.Intn(numIDs) + 1)
				model := &TestModel{
					ID:        id,
					Name:      fmt.Sprintf("Update from routine %d - %d", routine, i),
					MyValue:   routine*updatesPerRoutine + i,
					UpdatedAt: time.Now(),
				}
				updates = append(updates, model)

				if len(updates) == 10 || i == updatesPerRoutine-1 {
					batcher.UpdateAsync(func(err error) {
						if err != nil {
							atomic.AddInt32(&updateErrors, 1)
							t.Logf("Case-Update error: %v", err)
						}
					}, updates, []string{"name", "my_value", "updated_at"})
					updates = updates[:0]
				}
			}
		}(r)
	}

	wg.Wait()
	duration := time.Since(start)

	t.Logf("Duration: %v", duration)
	t.Logf("Errors: %d", atomic.LoadInt32(&updateErrors))
	t.Logf("Updates/sec: %.2f", float64(numRoutines*updatesPerRoutine)/duration.Seconds())
}

func batchInsertUpdate(db *gorm.DB, models []*TestModel) error {
	if len(models) == 0 {
		return nil
	}

	base := `INSERT INTO test_models(id, name, my_value, updated_at) VALUES %s`
	values := "(?,?,?,?)"
	args := []interface{}{models[0].ID, models[0].Name, models[0].MyValue, models[0].UpdatedAt}

	for i := 1; i < len(models); i++ {
		values += ",(?,?,?,?)"
		args = append(args, models[i].ID, models[i].Name, models[i].MyValue, models[i].UpdatedAt)
	}

	base += " ON DUPLICATE KEY UPDATE name = VALUES(name), my_value = VALUES(my_value), updated_at = VALUES(updated_at)"
	return db.Exec(fmt.Sprintf(base, values), args...).Error
}
