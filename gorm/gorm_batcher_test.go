//go:build !sqlite

package gorm

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/atlasgurus/batcher/batcher"
	gormv1 "github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/stretchr/testify/assert"
	gormv2 "gorm.io/gorm"
)

type TestModel struct {
	ID        uint   `gorm:"primaryKey"`
	Name      string `gorm:"type:varchar(100)"`
	MyValue   int    `gorm:"type:int"`
	UpdatedAt time.Time
}

type CompositeKeyModel struct {
	ID1     int    `gorm:"primaryKey"`
	ID2     string `gorm:"primaryKey"`
	Name    string
	MyValue int
}

var (
	db      *gormv2.DB
	dialect string
)

func TestMain(m *testing.M) {
	// Get the DSN and dialect from environment variables
	dsn := os.Getenv("DSN")
	dialect = os.Getenv("DIALECT")
	if dsn == "" {
		panic("DSN environment variable must be set")
	}
	if dialect == "" {
		dialect = "mysql" // Default to MySQL if DIALECT is not set
	}

	var err error
	var v1DB *gormv1.DB

	// Open a GORM v1 connection based on the dialect
	switch dialect {
	case "mysql", "postgres", "sqlite3":
		v1DB, err = gormv1.Open(dialect, dsn)
	default:
		panic("Unsupported dialect: " + dialect)
	}

	if err != nil {
		panic(fmt.Sprintf("failed to connect database: %v", err))
	}

	// Convert to GORM v2
	db, err = GormV1ToV2Adapter(v1DB)
	if err != nil {
		panic(fmt.Sprintf("failed to convert GORM v1 to v2: %v", err))
	}

	// Migrate the schema
	err = db.AutoMigrate(&TestModel{}, &CompositeKeyModel{})
	if err != nil {
		panic(fmt.Sprintf("failed to migrate database: %v", err))
	}

	// Run the tests
	code := m.Run()

	v1DB.Close()

	os.Exit(code)
}

func getDBProvider() DBProvider {
	return func() (*gormv2.DB, error) {
		return db.Debug(), nil
	}
}

func TestInsertBatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewInsertBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert single items
	for i := 1; i <= 3; i++ {
		id := uint(rand.Uint32())
		model := &TestModel{ID: id, Name: fmt.Sprintf("Single %d", i), MyValue: i}
		err := batcher.Insert(model)
		assert.NoError(t, err)
		assert.Equal(t, id, model.ID)
	}

	// Insert multiple items at once
	multipleItems := []*TestModel{
		{Name: "Multiple 1", MyValue: 4},
		{Name: "Multiple 2", MyValue: 5},
	}
	err := batcher.Insert(multipleItems...)
	assert.NoError(t, err)

	// Check if all items were inserted
	var count int64
	db.Model(&TestModel{}).Count(&count)
	assert.Equal(t, int64(5), count)

	var insertedModels []TestModel
	db.Order("my_value asc").Find(&insertedModels)
	assert.Len(t, insertedModels, 5)
	for i, model := range insertedModels {
		if i < 3 {
			assert.Equal(t, fmt.Sprintf("Single %d", i+1), model.Name)
			assert.Equal(t, i+1, model.MyValue)
		} else {
			assert.Equal(t, fmt.Sprintf("Multiple %d", i-2), model.Name)
			assert.Equal(t, i+1, model.MyValue)
		}
	}
}

func TestInsertBatcherAsync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewInsertBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert single items asynchronously
	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		id := uint(rand.Uint32())
		model := &TestModel{ID: id, Name: fmt.Sprintf("Async Single %d", i), MyValue: i}
		batcher.InsertAsync(func(err error) {
			assert.NoError(t, err)
			assert.Equal(t, id, model.ID)
			wg.Done()
		}, model)
	}

	// Insert multiple items at once asynchronously
	multipleItems := []*TestModel{
		{Name: "Async Multiple 1", MyValue: 4},
		{Name: "Async Multiple 2", MyValue: 5},
	}
	wg.Add(1)
	batcher.InsertAsync(func(err error) {
		assert.NoError(t, err)
		wg.Done()
	}, multipleItems...)

	// Wait for all async operations to complete
	wg.Wait()

	// Check if all items were inserted
	var count int64
	db.Model(&TestModel{}).Count(&count)
	assert.Equal(t, int64(5), count)

	var insertedModels []TestModel
	db.Order("my_value asc").Find(&insertedModels)
	assert.Len(t, insertedModels, 5)
	for i, model := range insertedModels {
		if i < 3 {
			assert.Equal(t, fmt.Sprintf("Async Single %d", i+1), model.Name)
			assert.Equal(t, i+1, model.MyValue)
		} else {
			assert.Equal(t, fmt.Sprintf("Async Multiple %d", i-2), model.Name)
			assert.Equal(t, i+1, model.MyValue)
		}
	}
}

func TestInsertBatcherAsyncError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a batcher with a faulty DBProvider
	faultyDBProvider := func() (*gormv2.DB, error) {
		return nil, fmt.Errorf("simulated database connection error")
	}
	batcher := NewInsertBatcher[*TestModel](faultyDBProvider, 3, 100*time.Millisecond, ctx)

	// Attempt to insert an item asynchronously
	var wg sync.WaitGroup
	wg.Add(1)
	model := &TestModel{ID: uint(rand.Uint32()), Name: "Faulty Insert", MyValue: 1}
	batcher.InsertAsync(func(err error) {
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "simulated database connection error")
		wg.Done()
	}, model)

	// Wait for the async operation to complete
	wg.Wait()
}

func TestUpdateBatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher, err := NewUpdateBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)
	assert.NoError(t, err)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert some initial data
	initialModels := []*TestModel{
		{Name: "Test 1", MyValue: 10},
		{Name: "Test 2", MyValue: 20},
		{Name: "Test 3", MyValue: 30},
		{Name: "Test 4", MyValue: 40},
		{Name: "Test 5", MyValue: 50},
	}
	db.Create(&initialModels)

	// Update single items
	for i := 0; i < 3; i++ {
		initialModels[i].MyValue += 5
		err := batcher.Update([]*TestModel{initialModels[i]}, []string{"MyValue"})
		assert.NoError(t, err)
	}

	// Update multiple items at once
	for i := 3; i < 5; i++ {
		initialModels[i].MyValue += 10
	}
	err = batcher.Update([]*TestModel{initialModels[3], initialModels[4]}, []string{"my_value"})
	assert.NoError(t, err)

	// Check if all items were updated correctly
	var updatedModels []TestModel
	db.Find(&updatedModels)
	assert.Len(t, updatedModels, 5)
	for i, model := range updatedModels {
		if i < 3 {
			assert.Equal(t, initialModels[i].MyValue, model.MyValue)
		} else {
			assert.Equal(t, initialModels[i].MyValue, model.MyValue)
		}
		assert.Equal(t, fmt.Sprintf("Test %d", i+1), model.Name)
	}
}

func TestUpdateBatcherAsync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher, err := NewUpdateBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)
	assert.NoError(t, err)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert some initial data
	initialModels := []*TestModel{
		{Name: "Test 1", MyValue: 10},
		{Name: "Test 2", MyValue: 20},
		{Name: "Test 3", MyValue: 30},
		{Name: "Test 4", MyValue: 40},
		{Name: "Test 5", MyValue: 50},
	}
	db.Create(&initialModels)

	// Update single items asynchronously
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		initialModels[i].MyValue += 5
		batcher.UpdateAsync(func(err error) {
			assert.NoError(t, err)
			wg.Done()
		}, []*TestModel{initialModels[i]}, []string{"MyValue"})
	}

	// Update multiple items at once asynchronously
	for i := 3; i < 5; i++ {
		initialModels[i].MyValue += 10
	}
	wg.Add(1)
	batcher.UpdateAsync(func(err error) {
		assert.NoError(t, err)
		wg.Done()
	}, []*TestModel{initialModels[3], initialModels[4]}, []string{"MyValue"})

	// Wait for all async operations to complete
	wg.Wait()

	// Check if all items were updated correctly
	var updatedModels []TestModel
	db.Find(&updatedModels)
	assert.Len(t, updatedModels, 5)
	for i, model := range updatedModels {
		assert.Equal(t, initialModels[i].MyValue, model.MyValue)
		assert.Equal(t, fmt.Sprintf("Test %d", i+1), model.Name)
	}
}

func TestUpdateBatcherBadInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher, err := NewUpdateBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)
	assert.NoError(t, err)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert some initial data
	initialModels := []*TestModel{
		{Name: "Test 1", MyValue: 10},
	}
	db.Create(&initialModels)

	initialModels[0].MyValue += 5
	err = batcher.Update([]*TestModel{initialModels[0]}, []string{"MyValue", "NonExistentField"})
	assert.ErrorContains(t, err, "field NonExistentField not found")
}

func TestUpdateAsyncBatcherBadInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher, err := NewUpdateBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)
	assert.NoError(t, err)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert some initial data
	initialModels := []*TestModel{
		{Name: "Test 1", MyValue: 10},
	}
	db.Create(&initialModels)

	initialModels[0].MyValue += 5
	var wg sync.WaitGroup
	wg.Add(1)
	batcher.UpdateAsync(func(err error) {
		assert.ErrorContains(t, err, "field NonExistentField not found")
		wg.Done()
	}, []*TestModel{initialModels[0]}, []string{"MyValue", "NonExistentField"})
	wg.Wait()
}

func TestConcurrentOperations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	insertBatcher := NewInsertBatcher[*TestModel](getDBProvider(), 10, 100*time.Millisecond, ctx)
	updateBatcher, err := NewUpdateBatcher[*TestModel](getDBProvider(), 10, 100*time.Millisecond, ctx)
	assert.NoError(t, err)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	var wg sync.WaitGroup
	operationCount := 100

	// Concurrent inserts
	for i := 1; i <= operationCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := insertBatcher.Insert(&TestModel{Name: fmt.Sprintf("Test %d", i), MyValue: i})
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Check if all items were inserted
	var count int64
	db.Model(&TestModel{}).Count(&count)
	assert.Equal(t, int64(operationCount), count)

	// Concurrent updates
	var updatedModels []TestModel
	db.Find(&updatedModels)

	for i := range updatedModels {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			updatedModels[i].MyValue += 1000
			err := updateBatcher.Update([]*TestModel{&updatedModels[i]}, nil) // Update all fields
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Check if all items were updated
	db.Find(&updatedModels)
	assert.Len(t, updatedModels, operationCount)
	for _, model := range updatedModels {
		assert.True(t, model.MyValue > 1000, "Expected MyValue to be greater than 1000, got %d for ID %d", model.MyValue, model.ID)
	}
}

func TestUpdateBatcher_AllFields(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher, err := NewUpdateBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)
	assert.NoError(t, err)

	db.Exec("DELETE FROM test_models")

	initialModels := []TestModel{
		{Name: "Test 1", MyValue: 10},
		{Name: "Test 2", MyValue: 20},
		{Name: "Test 3", MyValue: 30},
	}
	db.Create(&initialModels)

	updatedModels := make([]*TestModel, len(initialModels))
	for i := range initialModels {
		updatedModels[i] = &TestModel{
			ID:      initialModels[i].ID,
			Name:    fmt.Sprintf("Updated %d", i+1),
			MyValue: initialModels[i].MyValue + 5,
		}
	}

	err = batcher.Update(updatedModels, nil) // Update all fields
	assert.NoError(t, err)

	var finalModels []TestModel
	db.Order("id asc").Find(&finalModels)
	assert.Len(t, finalModels, 3)

	for i, model := range finalModels {
		assert.Equal(t, fmt.Sprintf("Updated %d", i+1), model.Name)
		assert.Equal(t, initialModels[i].MyValue+5, model.MyValue)
	}
}

func TestUpdateBatcher_SpecificFields(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher, err := NewUpdateBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)
	assert.NoError(t, err)

	db.Exec("DELETE FROM test_models")

	initialModels := []TestModel{
		{Name: "Test 1", MyValue: 10},
		{Name: "Test 2", MyValue: 20},
		{Name: "Test 3", MyValue: 30},
	}
	db.Create(&initialModels)

	updatedModels := make([]*TestModel, len(initialModels))
	for i := range initialModels {
		updatedModels[i] = &TestModel{
			ID:      initialModels[i].ID,
			Name:    fmt.Sprintf("Should Not Update %d", i+1),
			MyValue: initialModels[i].MyValue + 10,
		}
	}

	err = batcher.Update(updatedModels, []string{"MyValue"})
	assert.NoError(t, err)

	var finalModels []TestModel
	db.Order("id asc").Find(&finalModels)
	assert.Len(t, finalModels, 3)

	for i, model := range finalModels {
		assert.Equal(t, fmt.Sprintf("Test %d", i+1), model.Name, "Name should not have been updated")
		assert.Equal(t, initialModels[i].MyValue+10, model.MyValue, "MyValue should have been updated")
	}
}

func TestUpdateBatcher_CompositeKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher, err := NewUpdateBatcher[*CompositeKeyModel](getDBProvider(), 3, 100*time.Millisecond, ctx)
	assert.NoError(t, err)

	db.Exec("DELETE FROM composite_key_models")

	initialModels := []CompositeKeyModel{
		{ID1: 1, ID2: "A", Name: "Test 1", MyValue: 10},
		{ID1: 1, ID2: "B", Name: "Test 2", MyValue: 20},
		{ID1: 2, ID2: "A", Name: "Test 3", MyValue: 30},
		// Add cases that would fail with imprecise comparison
		{ID1: 2, ID2: "B", Name: "Test 4", MyValue: 40},
		{ID1: 3, ID2: "A", Name: "Test 5", MyValue: 50},
	}
	db.Create(&initialModels)

	// Create updates that would incorrectly modify data if using IN clause per column
	updatedModels := []*CompositeKeyModel{
		{
			ID1:     1,
			ID2:     "A",
			Name:    "Updated 1",
			MyValue: 15,
		},
		{
			ID1:     2,
			ID2:     "B",
			Name:    "Updated 4",
			MyValue: 45,
		},
		{
			ID1:     3,
			ID2:     "A",
			Name:    "Updated 5",
			MyValue: 55,
		},
	}

	err = batcher.Update(updatedModels, []string{"Name", "MyValue"})
	assert.NoError(t, err)

	var finalModels []CompositeKeyModel
	db.Order("id1 asc, id2 asc").Find(&finalModels)
	assert.Len(t, finalModels, 5)

	// Records that should be updated
	expectedUpdates := map[string]CompositeKeyModel{
		"1|A": {ID1: 1, ID2: "A", Name: "Updated 1", MyValue: 15},
		"2|B": {ID1: 2, ID2: "B", Name: "Updated 4", MyValue: 45},
		"3|A": {ID1: 2, ID2: "A", Name: "Updated 5", MyValue: 55},
	}

	// Records that should remain unchanged
	expectedUnchanged := map[string]CompositeKeyModel{
		"1|B": {ID1: 1, ID2: "B", Name: "Test 2", MyValue: 20},
		"2|A": {ID1: 2, ID2: "A", Name: "Test 3", MyValue: 30},
	}

	for _, model := range finalModels {
		key := fmt.Sprintf("%d|%s", model.ID1, model.ID2)

		if expected, ok := expectedUpdates[key]; ok {
			assert.Equal(t, expected.Name, model.Name, "Updated record mismatch for key %s", key)
			assert.Equal(t, expected.MyValue, model.MyValue, "Updated record mismatch for key %s", key)
		} else if expected, ok := expectedUnchanged[key]; ok {
			assert.Equal(t, expected.Name, model.Name, "Unchanged record was modified for key %s", key)
			assert.Equal(t, expected.MyValue, model.MyValue, "Unchanged record was modified for key %s", key)
		} else {
			assert.Fail(t, "Unexpected record found", "Key: %s", key)
		}
	}
}

func TestUpdateBatcher_UpdatedAt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type ModelWithUpdatedAt struct {
		ID        uint `gorm:"primaryKey"`
		Name      string
		MyValue   int
		UpdatedAt time.Time
	}

	// Migrate the schema for the new model
	err := db.AutoMigrate(&ModelWithUpdatedAt{})
	assert.NoError(t, err)

	batcher, err := NewUpdateBatcher[*ModelWithUpdatedAt](getDBProvider(), 3, 100*time.Millisecond, ctx)
	assert.NoError(t, err)

	db.Exec("DELETE FROM model_with_updated_ats")

	// Insert initial data
	initialTime := time.Now().Add(-1 * time.Hour) // Set initial time to 1 hour ago
	initialModels := []ModelWithUpdatedAt{
		{Name: "Test 1", MyValue: 10, UpdatedAt: initialTime},
		{Name: "Test 2", MyValue: 20, UpdatedAt: initialTime},
		{Name: "Test 3", MyValue: 30, UpdatedAt: initialTime},
	}
	db.Create(&initialModels)

	// Sleep to ensure there's a noticeable time difference
	time.Sleep(100 * time.Millisecond)

	// Update models
	updatedModels := make([]*ModelWithUpdatedAt, len(initialModels))
	for i := range initialModels {
		updatedModels[i] = &ModelWithUpdatedAt{
			ID:      initialModels[i].ID,
			Name:    fmt.Sprintf("Updated %d", i+1),
			MyValue: initialModels[i].MyValue + 5,
			// Note: We're not setting UpdatedAt here
		}
	}

	// Perform update
	err = batcher.Update(updatedModels, []string{"Name", "MyValue"})
	assert.NoError(t, err)

	// Retrieve updated models
	var finalModels []ModelWithUpdatedAt
	db.Order("id asc").Find(&finalModels)
	assert.Len(t, finalModels, 3)

	for i, model := range finalModels {
		assert.Equal(t, fmt.Sprintf("Updated %d", i+1), model.Name)
		assert.Equal(t, initialModels[i].MyValue+5, model.MyValue)

		// Check that UpdatedAt has been changed
		assert.True(t, model.UpdatedAt.After(initialTime), "UpdatedAt should be after the initial time")
		assert.True(t, model.UpdatedAt.After(initialModels[i].UpdatedAt), "UpdatedAt should be updated")

		// Check that UpdatedAt is recent
		assert.True(t, time.Since(model.UpdatedAt) < 5*time.Second, "UpdatedAt should be very recent")
	}
}

func TestSelectBatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("TestModel", func(t *testing.T) {
		runTestModelTests(t, ctx)
		runTestModelAsyncTests(t, ctx)
	})

	t.Run("CompositeKeyModel", func(t *testing.T) {
		runCompositeKeyModelTests(t, ctx)
		runCompositeKeyModelAsyncTests(t, ctx)
	})
}

func runTestModelTests(t *testing.T, ctx context.Context) {
	selectBatcher, err := NewSelectBatcher[TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx, []string{"id", "name", "my_value"})
	assert.NoError(t, err)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert some test data
	testModels := []TestModel{
		{Name: "Test 1", MyValue: 10},
		{Name: "Test 2", MyValue: 20},
		{Name: "Test 3", MyValue: 30},
		{Name: "Test 4", MyValue: 40},
		{Name: "Test 5", MyValue: 50},
	}
	result := db.Create(&testModels)
	assert.NoError(t, result.Error)
	assert.Equal(t, int64(5), result.RowsAffected)

	// Run basic tests
	runBasicTests(t, selectBatcher,
		func(m TestModel) string { return m.Name },
		func(m TestModel) int { return m.MyValue },
	)

	// Run concurrent tests
	runConcurrentTests(t, selectBatcher)
}

func runCompositeKeyModelTests(t *testing.T, ctx context.Context) {
	selectBatcher, err := NewSelectBatcher[CompositeKeyModel](getDBProvider(), 3, 100*time.Millisecond, ctx, []string{"id1", "id2", "name", "my_value"})
	assert.NoError(t, err)

	// Clean up the table before the test
	db.Exec("DELETE FROM composite_key_models")

	// Insert some test data
	testModels := []CompositeKeyModel{
		{ID1: 1, ID2: "A", Name: "Test 1", MyValue: 10},
		{ID1: 1, ID2: "B", Name: "Test 2", MyValue: 20},
		{ID1: 2, ID2: "A", Name: "Test 3", MyValue: 30},
		{ID2: "B", Name: "Test 4", MyValue: 40},
		{ID1: 3, ID2: "A", Name: "Test 5", MyValue: 50},
	}
	result := db.Create(&testModels)
	assert.NoError(t, result.Error)
	assert.Equal(t, int64(5), result.RowsAffected)

	// Run basic tests
	runBasicTests(t, selectBatcher,
		func(m CompositeKeyModel) string { return m.Name },
		func(m CompositeKeyModel) int { return m.MyValue },
	)

	// Run composite key specific tests
	runCompositeKeySpecificTests(t, selectBatcher)

	// Run concurrent tests
	runConcurrentTests(t, selectBatcher)
}

func runBasicTests[T any](t *testing.T, selectBatcher *SelectBatcher[T], getNameFunc func(T) string, getMyValueFunc func(T) int) {
	// Test single result
	t.Run("SingleResult", func(t *testing.T) {
		results, err := selectBatcher.Select("name = ?", "Test 1")
		assert.NoError(t, err)
		assert.Len(t, results, 1, "Expected 1 result, got %d", len(results))
		if len(results) > 0 {
			assert.Equal(t, "Test 1", getNameFunc(results[0]))
			assert.Equal(t, 10, getMyValueFunc(results[0]))
		}
	})

	// Test multiple results
	t.Run("MultipleResults", func(t *testing.T) {
		results, err := selectBatcher.Select("my_value > ?", 25)
		assert.NoError(t, err)
		assert.Len(t, results, 3, "Expected 3 results, got %d", len(results))
		for _, result := range results {
			assert.True(t, getMyValueFunc(result) > 25, "Expected MyValue > 25, got %d", getMyValueFunc(result))
		}
	})

	// Test no results
	t.Run("NoResults", func(t *testing.T) {
		results, err := selectBatcher.Select("name = ?", "Nonexistent")
		assert.NoError(t, err)
		assert.Len(t, results, 0, "Expected 0 results, got %d", len(results))
	})

	// Test error handling
	t.Run("ErrorHandling", func(t *testing.T) {
		_, err := selectBatcher.Select("invalid_column = ?", "value")
		assert.Error(t, err)
	})
}

func runCompositeKeySpecificTests(t *testing.T, selectBatcher *SelectBatcher[CompositeKeyModel]) {
	// Test single result with composite key
	t.Run("SingleResultCompositeKey", func(t *testing.T) {
		results, err := selectBatcher.Select("id1 = ? AND id2 = ?", 1, "A")
		assert.NoError(t, err)
		assert.Len(t, results, 1, "Expected 1 result, got %d", len(results))
		if len(results) > 0 {
			assert.Equal(t, 1, results[0].ID1)
			assert.Equal(t, "A", results[0].ID2)
			assert.Equal(t, "Test 1", results[0].Name)
			assert.Equal(t, 10, results[0].MyValue)
		}
	})
}

func runConcurrentTests[T any](t *testing.T, selectBatcher *SelectBatcher[T]) {
	t.Run("ConcurrentSelects", func(t *testing.T) {
		var wg sync.WaitGroup
		numGoroutines := 100
		results := make([][]T, numGoroutines)
		errors := make([]error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				results[index], errors[index] = selectBatcher.Select("my_value > ?", rand.Intn(40))
			}(i)
		}

		wg.Wait()

		for i := 0; i < numGoroutines; i++ {
			assert.NoError(t, errors[i])
			assert.NotNil(t, results[i])
		}
	})
}

func runTestModelAsyncTests(t *testing.T, ctx context.Context) {
	selectBatcher, err := NewSelectBatcher[TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx, []string{"id", "name", "my_value"})
	assert.NoError(t, err)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert some test data
	testModels := []TestModel{
		{Name: "Test 1", MyValue: 10},
		{Name: "Test 2", MyValue: 20},
		{Name: "Test 3", MyValue: 30},
		{Name: "Test 4", MyValue: 40},
		{Name: "Test 5", MyValue: 50},
	}
	result := db.Create(&testModels)
	assert.NoError(t, result.Error)
	assert.Equal(t, int64(5), result.RowsAffected)

	// Run async tests
	runAsyncTests(t, selectBatcher,
		func(m TestModel) string { return m.Name },
		func(m TestModel) int { return m.MyValue },
	)
}

func runCompositeKeyModelAsyncTests(t *testing.T, ctx context.Context) {
	selectBatcher, err := NewSelectBatcher[CompositeKeyModel](getDBProvider(), 3, 100*time.Millisecond, ctx, []string{"id1", "id2", "name", "my_value"})
	assert.NoError(t, err)

	// Clean up the table before the test
	db.Exec("DELETE FROM composite_key_models")

	// Insert some test data
	testModels := []CompositeKeyModel{
		{ID1: 1, ID2: "A", Name: "Test 1", MyValue: 10},
		{ID1: 1, ID2: "B", Name: "Test 2", MyValue: 20},
		{ID1: 2, ID2: "A", Name: "Test 3", MyValue: 30},
		{ID2: "B", Name: "Test 4", MyValue: 40},
		{ID1: 3, ID2: "A", Name: "Test 5", MyValue: 50},
	}
	result := db.Create(&testModels)
	assert.NoError(t, result.Error)
	assert.Equal(t, int64(5), result.RowsAffected)

	// Run async tests
	runAsyncTests(t, selectBatcher,
		func(m CompositeKeyModel) string { return m.Name },
		func(m CompositeKeyModel) int { return m.MyValue },
	)
}

func runAsyncTests[T any](t *testing.T, selectBatcher *SelectBatcher[T], getNameFunc func(T) string, getMyValueFunc func(T) int) {
	// Test single result async
	t.Run("SingleResultAsync", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)
		selectBatcher.SelectAsync(func(results []T, err error) {
			assert.NoError(t, err)
			assert.Len(t, results, 1, "Expected 1 result, got %d", len(results))
			if len(results) > 0 {
				assert.Equal(t, "Test 1", getNameFunc(results[0]))
				assert.Equal(t, 10, getMyValueFunc(results[0]))
			}
			wg.Done()
		}, "name = ?", "Test 1")
		wg.Wait()
	})

	// Test multiple results async
	t.Run("MultipleResultsAsync", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)
		selectBatcher.SelectAsync(func(results []T, err error) {
			assert.NoError(t, err)
			assert.Len(t, results, 3, "Expected 3 results, got %d", len(results))
			for _, result := range results {
				assert.True(t, getMyValueFunc(result) > 25, "Expected MyValue > 25, got %d", getMyValueFunc(result))
			}
			wg.Done()
		}, "my_value > ?", 25)
		wg.Wait()
	})

	// Test no results async
	t.Run("NoResultsAsync", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)
		selectBatcher.SelectAsync(func(results []T, err error) {
			assert.NoError(t, err)
			assert.Len(t, results, 0, "Expected 0 results, got %d", len(results))
			wg.Done()
		}, "name = ?", "Nonexistent")
		wg.Wait()
	})

	// Test error handling async
	t.Run("ErrorHandlingAsync", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)
		selectBatcher.SelectAsync(func(results []T, err error) {
			assert.Error(t, err)
			wg.Done()
		}, "invalid_column = ?", "value")
		wg.Wait()
	})
}

type RelatedModel struct {
	ID       uint `gorm:"primaryKey"`
	Name     string
	ParentID uint
}

type ParentModel struct {
	ID          uint   `gorm:"primaryKey"`
	Name        string `gorm:"type:varchar(100)"`
	MyValue     int
	RelatedData []RelatedModel // Intentionally without foreign key constraint
}

func TestUpdateBatcherWithRelationships(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Get DB
	db, err := getDBProvider()()
	assert.NoError(t, err)

	// Drop tables if they exist
	db.Exec("DROP TABLE IF EXISTS related_models")
	db.Exec("DROP TABLE IF EXISTS parent_models")

	// Create tables manually
	createTableSQL := `
    CREATE TABLE parent_models (
        id INTEGER PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(100),
        my_value INTEGER
    );
    
    CREATE TABLE related_models (
        id INTEGER PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(100),
        parent_id INTEGER
    );`

	// Split and execute each statement
	for _, stmt := range strings.Split(createTableSQL, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		err = db.Exec(stmt).Error
		assert.NoError(t, err, "Failed to create table with statement: %s", stmt)
	}

	// Insert test data directly
	insertParentSQL := "INSERT INTO parent_models (name, my_value) VALUES (?, ?)"
	result := db.Exec(insertParentSQL, "Test Parent", 10)
	assert.NoError(t, result.Error)

	parentID := uint(0)
	row := db.Raw("SELECT LAST_INSERT_ID()").Row()
	err = row.Scan(&parentID)
	assert.NoError(t, err)
	assert.NotZero(t, parentID)

	// Insert related data
	insertRelatedSQL := "INSERT INTO related_models (name, parent_id) VALUES (?, ?), (?, ?)"
	result = db.Exec(insertRelatedSQL, "Related 1", parentID, "Related 2", parentID)
	assert.NoError(t, result.Error)

	// Now use the UpdateBatcher
	batcher, err := NewUpdateBatcher[*ParentModel](getDBProvider(), 3, 100*time.Millisecond, ctx)
	assert.NoError(t, err)

	// Update using batcher
	updateModel := &ParentModel{
		ID:      parentID,
		MyValue: 20,
	}
	err = batcher.Update([]*ParentModel{updateModel}, []string{"my_value"})
	assert.NoError(t, err)

	// Verify update directly with SQL
	var myValue int
	var relatedCount int
	err = db.Raw("SELECT my_value FROM parent_models WHERE id = ?", parentID).Row().Scan(&myValue)
	assert.NoError(t, err)
	assert.Equal(t, 20, myValue)

	err = db.Raw("SELECT COUNT(*) FROM related_models WHERE parent_id = ?", parentID).Row().Scan(&relatedCount)
	assert.NoError(t, err)
	assert.Equal(t, 2, relatedCount)
}

func TestUpdateBatcherDuplicateIDs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher, err := NewUpdateBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)
	assert.NoError(t, err)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert initial model
	initialTime := time.Date(2024, 10, 29, 21, 0, 0, 0, time.UTC)
	initialModel := &TestModel{
		ID:        1,
		Name:      "Initial",
		MyValue:   10,
		UpdatedAt: initialTime,
	}
	result := db.Create(initialModel)
	assert.NoError(t, result.Error)

	// Queue multiple updates to the same ID with different timestamps
	var wg sync.WaitGroup
	wg.Add(3)

	time1 := time.Date(2024, 10, 29, 21, 2, 42, 0, time.UTC)
	batcher.UpdateAsync(func(err error) {
		assert.NoError(t, err)
		wg.Done()
	}, []*TestModel{{
		ID:        1,
		Name:      "Update 1",
		MyValue:   20,
		UpdatedAt: time1,
	}}, []string{"name", "my_value", "updated_at"})

	time2 := time.Date(2024, 10, 29, 21, 6, 56, 0, time.UTC)
	batcher.UpdateAsync(func(err error) {
		assert.NoError(t, err)
		wg.Done()
	}, []*TestModel{{
		ID:        1,
		Name:      "Update 2",
		MyValue:   30,
		UpdatedAt: time2,
	}}, []string{"name", "my_value", "updated_at"})

	time3 := time.Date(2024, 10, 29, 21, 8, 36, 725000000, time.UTC)
	batcher.UpdateAsync(func(err error) {
		assert.NoError(t, err)
		wg.Done()
	}, []*TestModel{{
		ID:        1,
		Name:      "Update 3",
		MyValue:   40,
		UpdatedAt: time3,
	}}, []string{"name", "my_value", "updated_at"})

	// Wait for callbacks
	wg.Wait()

	// Wait a bit longer than the batch interval to ensure all updates are processed
	time.Sleep(150 * time.Millisecond)

	// Verify final state
	var finalModel TestModel
	err = db.First(&finalModel, 1).Error
	assert.NoError(t, err)

	// The CASE statement ordering means the last matching condition wins
	assert.Equal(t, "Update 3", finalModel.Name)
	assert.Equal(t, 40, finalModel.MyValue)
	assert.Equal(t, time3, finalModel.UpdatedAt)
}

func TestUpdateBatcherFieldMerging(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher, err := NewUpdateBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)
	assert.NoError(t, err)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert initial model
	initialModel := &TestModel{
		ID:        1,
		Name:      "Initial",
		MyValue:   10,
		UpdatedAt: time.Date(2024, 10, 29, 21, 0, 0, 0, time.UTC),
	}
	result := db.Create(initialModel)
	assert.NoError(t, result.Error)

	// Queue multiple updates to the same ID affecting different fields
	var wg sync.WaitGroup
	wg.Add(3)

	// First update changes Name only
	batcher.UpdateAsync(func(err error) {
		assert.NoError(t, err)
		wg.Done()
	}, []*TestModel{{
		ID:        1,
		Name:      "Update Name",
		MyValue:   10, // unchanged
		UpdatedAt: time.Date(2024, 10, 29, 21, 2, 42, 0, time.UTC),
	}}, []string{"name", "updated_at"})

	// Second update changes MyValue only
	batcher.UpdateAsync(func(err error) {
		assert.NoError(t, err)
		wg.Done()
	}, []*TestModel{{
		ID:        1,
		Name:      "Update Name", // same as previous update
		MyValue:   20,            // changed
		UpdatedAt: time.Date(2024, 10, 29, 21, 6, 56, 0, time.UTC),
	}}, []string{"my_value", "updated_at"})

	// Third update changes both
	batcher.UpdateAsync(func(err error) {
		assert.NoError(t, err)
		wg.Done()
	}, []*TestModel{{
		ID:        1,
		Name:      "Final Name",
		MyValue:   30,
		UpdatedAt: time.Date(2024, 10, 29, 21, 8, 36, 725000000, time.UTC),
	}}, []string{"name", "my_value", "updated_at"})

	// Wait for callbacks
	wg.Wait()

	// Wait a bit longer than the batch interval to ensure all updates are processed
	time.Sleep(150 * time.Millisecond)

	// Verify final state
	var finalModel TestModel
	err = db.First(&finalModel, 1).Error
	assert.NoError(t, err)

	// The last update in the batch should take precedence
	assert.Equal(t, "Final Name", finalModel.Name)
	assert.Equal(t, 30, finalModel.MyValue)
	assert.Equal(t, time.Date(2024, 10, 29, 21, 8, 36, 725000000, time.UTC), finalModel.UpdatedAt)
}

func TestUpdateBatcherFieldMergingMultipleIDs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher, err := NewUpdateBatcher[*TestModel](getDBProvider(), 10, 100*time.Millisecond, ctx)
	assert.NoError(t, err)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert initial models
	initialModels := make([]*TestModel, 10)
	for i := 0; i < 10; i++ {
		initialModels[i] = &TestModel{
			ID:        uint(i + 1),
			Name:      fmt.Sprintf("Initial %d", i+1),
			MyValue:   i * 10,
			UpdatedAt: time.Date(2024, 10, 29, 21, 0, 0, 0, time.UTC),
		}
	}
	result := db.Create(&initialModels)
	assert.NoError(t, result.Error)

	// Queue multiple updates for each ID affecting different fields
	var wg sync.WaitGroup
	wg.Add(30) // 3 updates for each of the 10 IDs

	for i := 0; i < 10; i++ {
		id := uint(i + 1)

		// First update changes Name only
		batcher.UpdateAsync(func(err error) {
			assert.NoError(t, err)
			wg.Done()
		}, []*TestModel{{
			ID:        id,
			Name:      fmt.Sprintf("Update Name %d", id),
			MyValue:   initialModels[i].MyValue, // unchanged
			UpdatedAt: time.Date(2024, 10, 29, 21, 2, 42, 0, time.UTC),
		}}, []string{"name", "updated_at"})

		// Second update changes MyValue only
		batcher.UpdateAsync(func(err error) {
			assert.NoError(t, err)
			wg.Done()
		}, []*TestModel{{
			ID:        id,
			Name:      fmt.Sprintf("Update Name %d", id), // same as previous update
			MyValue:   initialModels[i].MyValue + 5,      // changed
			UpdatedAt: time.Date(2024, 10, 29, 21, 6, 56, 0, time.UTC),
		}}, []string{"my_value", "updated_at"})

		// Third update changes both
		batcher.UpdateAsync(func(err error) {
			assert.NoError(t, err)
			wg.Done()
		}, []*TestModel{{
			ID:        id,
			Name:      fmt.Sprintf("Final Name %d", id),
			MyValue:   initialModels[i].MyValue + 10,
			UpdatedAt: time.Date(2024, 10, 29, 21, 8, 36, 725000000, time.UTC),
		}}, []string{"name", "my_value", "updated_at"})
	}

	// Wait for callbacks
	wg.Wait()

	// Verify final state for all records
	var finalModels []TestModel
	err = db.Order("id").Find(&finalModels).Error
	assert.NoError(t, err)
	assert.Equal(t, 10, len(finalModels))

	// Verify each record
	for i, model := range finalModels {
		assert.Equal(t, uint(i+1), model.ID)
		assert.Equal(t, fmt.Sprintf("Final Name %d", i+1), model.Name)
		assert.Equal(t, initialModels[i].MyValue+10, model.MyValue)
		assert.Equal(t, time.Date(2024, 10, 29, 21, 8, 36, 725000000, time.UTC), model.UpdatedAt)
	}
}

func TestInsertBatcherWithDecomposeFields(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test model that decomposes into existing models
	type DecomposeTestModel struct {
		TestModelData    TestModel
		CompositeKeyData CompositeKeyModel
	}

	// Create batcher with DecomposeFields enabled
	batcher := NewInsertBatcherWithOptions[*DecomposeTestModel](
		getDBProvider(),
		ctx,
		batcher.WithDecomposeFields(true),
		batcher.WithMaxBatchSize(3),
		batcher.WithMaxWaitTime(100*time.Millisecond),
	)

	// Clean up tables before the test
	db.Exec("DELETE FROM test_models")
	db.Exec("DELETE FROM composite_key_models")

	// Create test data
	testModels := []*DecomposeTestModel{
		{
			TestModelData: TestModel{
				ID:      1,
				Name:    "Decomposed Test 1",
				MyValue: 100,
			},
			CompositeKeyData: CompositeKeyModel{
				ID1:     1,
				ID2:     "A",
				Name:    "Decomposed Composite 1",
				MyValue: 200,
			},
		},
		{
			TestModelData: TestModel{
				ID:      2,
				Name:    "Decomposed Test 2",
				MyValue: 101,
			},
			CompositeKeyData: CompositeKeyModel{
				ID1:     2,
				ID2:     "B",
				Name:    "Decomposed Composite 2",
				MyValue: 201,
			},
		},
	}

	// Insert test models - should decompose into separate tables
	err := batcher.Insert(testModels...)
	assert.NoError(t, err)

	// Verify that data was inserted into both tables
	var testModelCount, compositeKeyCount int64

	db.Model(&TestModel{}).Count(&testModelCount)
	assert.Equal(t, int64(2), testModelCount, "Expected 2 test model records")

	db.Model(&CompositeKeyModel{}).Count(&compositeKeyCount)
	assert.Equal(t, int64(2), compositeKeyCount, "Expected 2 composite key records")

	// Verify the actual data in TestModel table
	var testModelsFromDB []TestModel
	db.Order("id").Find(&testModelsFromDB)
	assert.Len(t, testModelsFromDB, 2)
	assert.Equal(t, "Decomposed Test 1", testModelsFromDB[0].Name)
	assert.Equal(t, 100, testModelsFromDB[0].MyValue)
	assert.Equal(t, "Decomposed Test 2", testModelsFromDB[1].Name)
	assert.Equal(t, 101, testModelsFromDB[1].MyValue)

	// Verify the actual data in CompositeKeyModel table
	var compositeModelsFromDB []CompositeKeyModel
	db.Order("id1, id2").Find(&compositeModelsFromDB)
	assert.Len(t, compositeModelsFromDB, 2)
	assert.Equal(t, 1, compositeModelsFromDB[0].ID1)
	assert.Equal(t, "A", compositeModelsFromDB[0].ID2)
	assert.Equal(t, "Decomposed Composite 1", compositeModelsFromDB[0].Name)
	assert.Equal(t, 200, compositeModelsFromDB[0].MyValue)
	assert.Equal(t, 2, compositeModelsFromDB[1].ID1)
	assert.Equal(t, "B", compositeModelsFromDB[1].ID2)
	assert.Equal(t, "Decomposed Composite 2", compositeModelsFromDB[1].Name)
	assert.Equal(t, 201, compositeModelsFromDB[1].MyValue)
}

func TestInsertBatcherWithoutDecomposeFields(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create batcher with DecomposeFields disabled (default)
	batcher := NewInsertBatcherWithOptions[*TestModel](
		getDBProvider(),
		ctx,
		batcher.WithDecomposeFields(false),
		batcher.WithMaxBatchSize(3),
		batcher.WithMaxWaitTime(100*time.Millisecond),
	)

	// Clean up table before the test
	db.Exec("DELETE FROM test_models")

	// Create test data
	testModels := []*TestModel{
		{Name: "Normal Test 1", MyValue: 10},
		{Name: "Normal Test 2", MyValue: 20},
	}

	// Insert test models - should use normal insertion
	err := batcher.Insert(testModels...)
	assert.NoError(t, err)

	// Verify that data was inserted into the main table normally
	var count int64
	db.Model(&TestModel{}).Count(&count)
	assert.Equal(t, int64(2), count, "Expected 2 test model records")

	var modelsFromDB []TestModel
	db.Order("my_value").Find(&modelsFromDB)
	assert.Len(t, modelsFromDB, 2)
	assert.Equal(t, "Normal Test 1", modelsFromDB[0].Name)
	assert.Equal(t, "Normal Test 2", modelsFromDB[1].Name)
}

func TestConnectionPoolPollution(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert initial data that will create lock contention
	initialModels := []*TestModel{
		{ID: 1, Name: "Test 1", MyValue: 10},
		{ID: 2, Name: "Test 2", MyValue: 20},
		{ID: 3, Name: "Test 3", MyValue: 30},
	}
	result := db.Create(&initialModels)
	assert.NoError(t, result.Error)

	// Create an update batcher that will use retryWithDeadlockDetection
	updateBatcher, err := NewUpdateBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)
	assert.NoError(t, err)

	// First, check the default timeout value
	var defaultTimeout int
	err = db.Raw("SELECT @@SESSION.innodb_lock_wait_timeout").Row().Scan(&defaultTimeout)
	assert.NoError(t, err)
	t.Logf("Default innodb_lock_wait_timeout: %d", defaultTimeout)

	// Perform an update operation that will trigger retryWithDeadlockDetection
	// This should set innodb_lock_wait_timeout = 1 and then reset it
	updateModel := &TestModel{
		ID:      1,
		Name:    "Updated Test 1",
		MyValue: 15,
	}
	err = updateBatcher.Update([]*TestModel{updateModel}, []string{"Name", "MyValue"})
	assert.NoError(t, err)

	// Now check if the timeout was properly reset
	// If the bug exists, this will show timeout = 1
	// If the fix works, this should show the default timeout (usually 50)
	var timeoutAfterUpdate int
	err = db.Raw("SELECT @@SESSION.innodb_lock_wait_timeout").Row().Scan(&timeoutAfterUpdate)
	assert.NoError(t, err)
	t.Logf("innodb_lock_wait_timeout after update: %d", timeoutAfterUpdate)

	// The timeout should be reset to default, not stuck at 1
	assert.Equal(t, defaultTimeout, timeoutAfterUpdate,
		"innodb_lock_wait_timeout should be reset to default (%d) but was %d",
		defaultTimeout, timeoutAfterUpdate)

	// Additional test: simulate getting a "polluted" connection from the pool
	// by directly checking multiple times
	for i := 0; i < 5; i++ {
		// Get a fresh connection from the pool
		freshDB, err := getDBProvider()()
		assert.NoError(t, err)

		var currentTimeout int
		err = freshDB.Raw("SELECT @@SESSION.innodb_lock_wait_timeout").Row().Scan(&currentTimeout)
		assert.NoError(t, err)
		t.Logf("Connection %d timeout: %d", i+1, currentTimeout)

		// Each connection from the pool should have the default timeout
		assert.Equal(t, defaultTimeout, currentTimeout,
			"Connection %d from pool should have default timeout (%d) but had %d",
			i+1, defaultTimeout, currentTimeout)
	}
}
