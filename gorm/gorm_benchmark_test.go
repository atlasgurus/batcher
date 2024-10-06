package gorm

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type BenchTestModel struct {
	ID       uint   `gorm:"primary_key"`
	Name     string `gorm:"type:varchar(100)"`
	MyValue1 int    `gorm:"type:int"`
	MyValue2 int    `gorm:"type:int"`
}

func BenchmarkGORMBatcher(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configuration
	numRoutines := 100
	operationsPerRoutine := 99
	maxBatchSize := 30
	maxWaitTime := 1 * time.Millisecond

	// Create batchers
	insertBatcher := NewInsertBatcher[*BenchTestModel](getDBProvider(), maxBatchSize, maxWaitTime, ctx)
	updateBatcher, err := NewUpdateBatcher[*BenchTestModel](getDBProvider(), maxBatchSize, maxWaitTime, ctx)
	if err != nil {
		b.Fatalf("Failed to create update batcher: %v", err)
	}
	selectBatcher, err := NewSelectBatcher[*BenchTestModel](getDBProvider(), maxBatchSize, maxWaitTime, ctx, []string{"id", "name", "my_value1", "my_value2"})
	if err != nil {
		b.Fatalf("Failed to create select batcher: %v", err)
	}

	// Migrate the schema
	err = db.AutoMigrate(&BenchTestModel{}, &CompositeKeyModel{})
	if err != nil {
		panic(fmt.Sprintf("failed to migrate database: %v", err))
	}

	// Clean up the table before the benchmark
	db.Exec("DELETE FROM bench_test_models")

	// Prepare for benchmark
	var wg sync.WaitGroup
	startTime := time.Now()

	// Statistics
	var insertCount, updateCount, selectCount int64
	var insertErrors, updateErrors, selectErrors int64
	var insertDuration, updateDuration, selectDuration time.Duration
	var statsMutex sync.Mutex

	// Run benchmark
	b.ResetTimer()
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < operationsPerRoutine; j++ {
				operationType := j % 3 // 0: Insert, 1: Update, 2: Select
				id := routineID*operationsPerRoutine + j/3 + 1

				start := time.Now()
				var err error

				switch operationType {
				case 0: // Insert
					model := &BenchTestModel{ID: uint(id), Name: fmt.Sprintf("Test %d-%d", routineID, j), MyValue1: j}
					err = insertBatcher.Insert(model)
					statsMutex.Lock()
					insertCount++
					insertDuration += time.Since(start)
					if err != nil {
						insertErrors++
					}
					statsMutex.Unlock()
				case 1: // Update
					model := &BenchTestModel{ID: uint(id), MyValue1: j * 10, MyValue2: j * 20}
					if routineID%2 == 0 {
						err = updateBatcher.Update([]*BenchTestModel{model}, []string{"my_value1"})
					} else {
						err = updateBatcher.Update([]*BenchTestModel{model}, []string{"my_value1", "my_value2"})
					}
					statsMutex.Lock()
					updateCount++
					updateDuration += time.Since(start)
					if err != nil {
						updateErrors++
					}
					statsMutex.Unlock()
				case 2: // Select
					_, err = selectBatcher.Select("id = ?", id)
					statsMutex.Lock()
					selectCount++
					selectDuration += time.Since(start)
					if err != nil {
						selectErrors++
					}
					statsMutex.Unlock()
				}

				if err != nil {
					b.Logf("Operation error (type %d): %v", operationType, err)
				}
			}
		}(i)
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	b.StopTimer()

	// Verify data integrity
	var count int64
	db.Model(&BenchTestModel{}).Count(&count)
	expectedCount := int64(numRoutines * operationsPerRoutine / 3) // One-third of operations are inserts

	var sumValue1, sumValue2 int64
	db.Model(&BenchTestModel{}).Select("SUM(my_value1) as sum_value1, SUM(my_value2) as sum_value2").Row().Scan(&sumValue1, &sumValue2)

	// Calculate expected sum
	expectedSum := int64(0)
	for routineID := 0; routineID < numRoutines; routineID++ {
		for j := 0; j < operationsPerRoutine; j++ {
			if j%3 == 1 { // Update operations
				expectedSum += int64(j * 10)
			}
		}
	}

	// Print statistics and verification results
	b.Logf("Benchmark Statistics:")
	b.Logf("Total Duration: %v", totalDuration)
	b.Logf("Total Operations: %d", numRoutines*operationsPerRoutine)

	b.Logf("\nInsert Operations:")
	b.Logf("  Count: %d", insertCount)
	b.Logf("  Errors: %d", insertErrors)
	b.Logf("  Total Duration: %v", insertDuration)
	b.Logf("  Average Duration: %v", insertDuration/time.Duration(insertCount))

	b.Logf("\nUpdate Operations:")
	b.Logf("  Count: %d", updateCount)
	b.Logf("  Errors: %d", updateErrors)
	b.Logf("  Total Duration: %v", updateDuration)
	b.Logf("  Average Duration: %v", updateDuration/time.Duration(updateCount))

	b.Logf("\nSelect Operations:")
	b.Logf("  Count: %d", selectCount)
	b.Logf("  Errors: %d", selectErrors)
	b.Logf("  Total Duration: %v", selectDuration)
	b.Logf("  Average Duration: %v", selectDuration/time.Duration(selectCount))

	b.Logf("\nOverall:")
	b.Logf("  Operations per second: %.2f", float64(numRoutines*operationsPerRoutine)/totalDuration.Seconds())
	b.Logf("  Average time per operation: %v", totalDuration/time.Duration(numRoutines*operationsPerRoutine))

	b.Logf("\nData Integrity:")
	b.Logf("  Total records in database: %d (Expected: %d)", count, expectedCount)
	b.Logf("  Sum of all MyValue1 in database: %d (Expected: %d)", sumValue1, expectedSum)
	b.Logf("  Sum of all MyValue2 in database: %d (Expected: %d)", sumValue2, expectedSum)

	// Assertions
	if count != expectedCount {
		b.Errorf("Record count mismatch. Got: %d, Expected: %d", count, expectedCount)
	}
	if sumValue1 != expectedSum {
		b.Errorf("Sum of MyValue1 mismatch. Got: %d, Expected: %d", sumValue1, expectedSum)
	}
	if sumValue2 != expectedSum {
		b.Errorf("Sum of MyValue2 mismatch. Got: %d, Expected: %d", sumValue2, expectedSum)
	}
}
