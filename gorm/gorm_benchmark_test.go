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
	selectBatcher, err := NewSelectBatcher[*BenchTestModel](getDBProvider(), maxBatchSize, maxWaitTime, ctx)
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

	// Run benchmark
	b.ResetTimer()
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < operationsPerRoutine; j++ {
				operationType := j % 3 // 0: Insert, 1: Update, 2: Select
				id := routineID*operationsPerRoutine + j/3 + 1

				switch operationType {
				case 0: // Insert
					model := &BenchTestModel{ID: uint(id), Name: fmt.Sprintf("Test %d-%d", routineID, j), MyValue1: j}
					err := insertBatcher.Insert(model)
					if err != nil {
						b.Logf("Insert error: %v", err)
					}
				case 1: // Update
					model := &BenchTestModel{ID: uint(id), MyValue1: j * 10, MyValue2: j * 20}
					var updateErr error
					if routineID%2 == 0 {
						updateErr = updateBatcher.Update([]*BenchTestModel{model}, []string{"my_value1"})
					} else {
						updateErr = updateBatcher.Update([]*BenchTestModel{model}, []string{"my_value1", "my_value2"})
					}
					if updateErr != nil {
						b.Logf("Update error: %v", updateErr)
					}
				case 2: // Select
					results, err := selectBatcher.Select("id = ?", id)
					if err != nil {
						b.Logf("Select error: %v", err)
					}
					if len(results) == 0 {
						b.Logf("No results found for id: %d", id)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	b.StopTimer()

	// Verify data integrity
	var count int64
	db.Model(&BenchTestModel{}).Count(&count)
	expectedCount := int64(numRoutines * operationsPerRoutine / 3) // One-third of operations are inserts

	var sumValue1 int64
	db.Model(&BenchTestModel{}).Select("SUM(my_value1)").Row().Scan(&sumValue1)

	var sumValue2 int64
	db.Model(&BenchTestModel{}).Select("SUM(my_value2)").Row().Scan(&sumValue2)

	// Calculate expected sum
	expectedSum := int64(0)
	for routineId := 0; routineId < numRoutines; routineId++ {
		for j := 0; j < operationsPerRoutine; j++ {
			if j%3 == 1 { // Update operations
				expectedSum += int64(j * 10)
			}
		}
	}

	// Print statistics and verification results
	b.Logf("Benchmark Statistics:")
	b.Logf("Total Duration: %v", duration)
	b.Logf("Total Operations: %d", numRoutines*operationsPerRoutine)
	b.Logf("Inserts: %d", numRoutines*operationsPerRoutine/3)
	b.Logf("Updates: %d", numRoutines*operationsPerRoutine/3)
	b.Logf("Selects: %d", numRoutines*operationsPerRoutine/3)
	b.Logf("Operations per second: %.2f", float64(numRoutines*operationsPerRoutine)/duration.Seconds())
	b.Logf("Average time per operation: %v", duration/time.Duration(numRoutines*operationsPerRoutine))
	b.Logf("Total records in database: %d (Expected: %d)", count, expectedCount)
	b.Logf("Sum of all MyValue1 in database: %d (Expected: %d)", sumValue1, expectedSum)
	b.Logf("Sum of all MyValue2 in database: %d (Expected: %d)", sumValue2, expectedSum)

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
