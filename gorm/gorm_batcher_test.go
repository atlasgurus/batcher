package gorm

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	gormv1 "github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/stretchr/testify/assert"
	gormv2 "gorm.io/gorm"
)

type TestModel struct {
	ID      uint   `gorm:"primaryKey"`
	Name    string `gorm:"type:varchar(100)"`
	MyValue int    `gorm:"type:int"`
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

	// Clean up
	v1DB.Close()

	os.Exit(code)
}

func getDBProvider() DBProvider {
	return func() (*gormv2.DB, error) {
		return db, nil
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
	}
	db.Create(&initialModels)

	updatedModels := make([]*CompositeKeyModel, len(initialModels))
	for i := range initialModels {
		updatedModels[i] = &CompositeKeyModel{
			ID1:     initialModels[i].ID1,
			ID2:     initialModels[i].ID2,
			Name:    fmt.Sprintf("Updated %d", i+1),
			MyValue: initialModels[i].MyValue + 5,
		}
	}

	err = batcher.Update(updatedModels, []string{"Name", "MyValue"})
	assert.NoError(t, err)

	var finalModels []CompositeKeyModel
	db.Order("id1 asc, id2 asc").Find(&finalModels)
	assert.Len(t, finalModels, 3)

	for i, model := range finalModels {
		assert.Equal(t, fmt.Sprintf("Updated %d", i+1), model.Name)
		assert.Equal(t, initialModels[i].MyValue+5, model.MyValue)
		assert.Equal(t, initialModels[i].ID1, model.ID1)
		assert.Equal(t, initialModels[i].ID2, model.ID2)
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
	db.Create(&testModels)

	// Test single result
	t.Run("SingleResult", func(t *testing.T) {
		results, err := selectBatcher.Select("name = ?", "Test 1")
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, "Test 1", results[0].Name)
		assert.Equal(t, 10, results[0].MyValue)
	})

	// Test multiple results
	t.Run("MultipleResults", func(t *testing.T) {
		results, err := selectBatcher.Select("my_value > ?", 25)
		assert.NoError(t, err)
		assert.Len(t, results, 3)
		for _, result := range results {
			assert.True(t, result.MyValue > 25)
		}
	})

	// Test no results
	t.Run("NoResults", func(t *testing.T) {
		results, err := selectBatcher.Select("name = ?", "Nonexistent")
		assert.NoError(t, err)
		assert.Len(t, results, 0)
	})

	// Test error handling
	t.Run("ErrorHandling", func(t *testing.T) {
		_, err := selectBatcher.Select("invalid_column = ?", "value")
		assert.Error(t, err)
	})
}

func TestSelectBatcherConcurrent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selectBatcher, err := NewSelectBatcher[TestModel](getDBProvider(), 10, 100*time.Millisecond, ctx, []string{"id", "name", "my_value"})
	assert.NoError(t, err)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert test data
	testModels := make([]TestModel, 100)
	for i := range testModels {
		testModels[i] = TestModel{Name: fmt.Sprintf("Test %d", i+1), MyValue: (i + 1) * 10}
	}
	db.Create(&testModels)

	var wg sync.WaitGroup
	operationCount := 50

	results := make([][]TestModel, operationCount)
	errors := make([]error, operationCount)

	for i := 0; i < operationCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errors[i] = selectBatcher.Select("my_value > ?", rand.Intn(500))
		}(i)
	}

	wg.Wait()

	// Verify results
	for i := 0; i < operationCount; i++ {
		assert.NoError(t, errors[i])
		if len(results[i]) > 0 {
			for _, result := range results[i] {
				assert.NotEmpty(t, result.Name)
				assert.NotZero(t, result.MyValue)
			}
		}
	}
}

func TestSelectBatcherLargeDataset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selectBatcher, err := NewSelectBatcher[TestModel](getDBProvider(), 100, 200*time.Millisecond, ctx, []string{"id", "name", "my_value"})
	assert.NoError(t, err)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert a large number of test records
	batchSize := 1000
	totalRecords := 10000
	for i := 0; i < totalRecords; i += batchSize {
		batch := make([]TestModel, batchSize)
		for j := range batch {
			batch[j] = TestModel{Name: fmt.Sprintf("Test %d", i+j+1), MyValue: (i + j + 1) * 10}
		}
		db.Create(&batch)
	}

	// Perform a select operation that returns a large number of results
	results, err := selectBatcher.Select("my_value > ?", 0)
	assert.NoError(t, err)
	assert.Len(t, results, totalRecords)

	// Verify a sample of the results
	assert.Equal(t, "Test 1", results[0].Name)
	assert.Equal(t, 10, results[0].MyValue)
	assert.Equal(t, fmt.Sprintf("Test %d", totalRecords), results[totalRecords-1].Name)
	assert.Equal(t, totalRecords*10, results[totalRecords-1].MyValue)
}
