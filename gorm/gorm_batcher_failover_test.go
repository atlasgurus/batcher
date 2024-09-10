package gorm

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	gormv2 "gorm.io/gorm"
)

func TestBatcherWithFailover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	failoverCount := 0
	maxFailovers := 2

	mockDBProvider := func() (*gormv2.DB, error) {
		if failoverCount < maxFailovers {
			failoverCount++
			return nil, fmt.Errorf("simulated connection failure %d", failoverCount)
		}
		return db, nil // Return the actual test database after simulated failures
	}

	batcher := NewInsertBatcher[*TestModel](mockDBProvider, 3, 100*time.Millisecond, ctx)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Try to insert items (this should fail twice and then succeed)
	for i := 1; i <= 5; i++ {
		err := batcher.Insert(&TestModel{Name: fmt.Sprintf("Test %d", i), MyValue: i})
		if i <= 2 {
			assert.Error(t, err, "Expected error for first two insertions")
			assert.Contains(t, err.Error(), "simulated connection failure")
		} else {
			assert.NoError(t, err, "Expected success after simulated failures")
		}
	}

	// Verify that items were inserted after the failover
	var count int64
	db.Model(&TestModel{}).Count(&count)
	assert.Equal(t, int64(3), count, "Expected 3 items to be inserted after failover")

	var insertedModels []TestModel
	db.Find(&insertedModels)
	assert.Len(t, insertedModels, 3)
	for i, model := range insertedModels {
		assert.Equal(t, fmt.Sprintf("Test %d", i+3), model.Name)
		assert.Equal(t, i+3, model.MyValue)
	}
}

func TestUpdateBatcherWithFailover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	failoverCount := 0
	maxFailovers := 2

	mockDBProvider := func() (*gormv2.DB, error) {
		if failoverCount < maxFailovers {
			failoverCount++
			return nil, fmt.Errorf("simulated connection failure %d", failoverCount)
		}
		return db, nil // Return the actual test database after simulated failures
	}

	batcher := NewUpdateBatcher[*TestModel](mockDBProvider, 3, 100*time.Millisecond, ctx)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert initial data
	initialModels := []*TestModel{
		{Name: "Initial 1", MyValue: 10},
		{Name: "Initial 2", MyValue: 20},
		{Name: "Initial 3", MyValue: 30},
		{Name: "Initial 4", MyValue: 40},
		{Name: "Initial 5", MyValue: 50},
	}
	db.Create(&initialModels)

	// Try to update items (this should fail twice and then succeed)
	for i := 0; i < 5; i++ {
		initialModels[i].MyValue += 5
		err := batcher.Update([]*TestModel{initialModels[i]}, []string{"MyValue"})
		if i < 2 {
			assert.Error(t, err, "Expected error for first two updates")
			assert.Contains(t, err.Error(), "simulated connection failure")
		} else {
			assert.NoError(t, err, "Expected success after simulated failures")
		}
	}

	// Verify that items were updated after the failover
	var updatedModels []TestModel
	db.Find(&updatedModels)
	assert.Len(t, updatedModels, 5)
	for i, model := range updatedModels {
		if i < 2 {
			assert.Equal(t, initialModels[i].MyValue-5, model.MyValue, "First two items should not have been updated")
		} else {
			assert.Equal(t, initialModels[i].MyValue, model.MyValue, "Last three items should have been updated")
		}
		assert.Equal(t, fmt.Sprintf("Initial %d", i+1), model.Name)
	}
}
