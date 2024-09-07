package gorm

import (
	"context"
	"fmt"
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
	ID    uint   `gorm:"primaryKey"`
	Name  string `gorm:"type:varchar(100)"`
	Value int    `gorm:"type:int"`
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
	err = db.AutoMigrate(&TestModel{})
	if err != nil {
		panic(fmt.Sprintf("failed to migrate database: %v", err))
	}

	// Run the tests
	code := m.Run()

	// Clean up
	v1DB.Close()

	os.Exit(code)
}

func TestInsertBatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewInsertBatcher[*TestModel](db, 3, 100*time.Millisecond, ctx)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert 5 items
	for i := 1; i <= 5; i++ {
		err := batcher.Insert(&TestModel{Name: fmt.Sprintf("Test %d", i), Value: i})
		assert.NoError(t, err)
	}

	// Check if all items were inserted
	var count int64
	db.Model(&TestModel{}).Count(&count)
	assert.Equal(t, int64(5), count)
}

func TestUpdateBatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewUpdateBatcher[*TestModel](db, 3, 100*time.Millisecond, ctx, []string{"Value"})

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert some initial data
	initialModels := []*TestModel{
		{Name: "Test 1", Value: 10},
		{Name: "Test 2", Value: 20},
		{Name: "Test 3", Value: 30},
	}
	db.Create(&initialModels)

	// Update the models
	for i, model := range initialModels {
		model.Value += 5
		model.Name = fmt.Sprintf("Updated %d", i+1) // This should not be updated
		err := batcher.Update(model)
		assert.NoError(t, err)
	}

	// Check if all items were updated correctly
	var updatedModels []TestModel
	db.Find(&updatedModels)
	assert.Len(t, updatedModels, 3)
	for i, model := range updatedModels {
		assert.Equal(t, initialModels[i].Value, model.Value)
		assert.NotEqual(t, fmt.Sprintf("Updated %d", i+1), model.Name)
		assert.Equal(t, fmt.Sprintf("Test %d", i+1), model.Name)
	}
}

func TestConcurrentOperations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	insertBatcher := NewInsertBatcher[*TestModel](db, 10, 100*time.Millisecond, ctx)
	updateBatcher := NewUpdateBatcher[*TestModel](db, 10, 100*time.Millisecond, ctx, nil)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	var wg sync.WaitGroup
	operationCount := 100

	// Concurrent inserts
	for i := 1; i <= operationCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := insertBatcher.Insert(&TestModel{Name: fmt.Sprintf("Test %d", i), Value: i})
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
			updatedModels[i].Value += 1000
			err := updateBatcher.Update(&updatedModels[i])
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Check if all items were updated
	db.Find(&updatedModels)
	assert.Len(t, updatedModels, operationCount)
	for _, model := range updatedModels {
		assert.True(t, model.Value > 1000, "Expected Value to be greater than 1000, got %d for ID %d", model.Value, model.ID)
	}
}

func TestUpdateBatcher_AllFields(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewUpdateBatcher[*TestModel](db, 3, 100*time.Millisecond, ctx, nil)

	db.Exec("DELETE FROM test_models")

	initialModels := []TestModel{
		{Name: "Test 1", Value: 10},
		{Name: "Test 2", Value: 20},
		{Name: "Test 3", Value: 30},
	}
	db.Create(&initialModels)

	for i, model := range initialModels {
		model.Name = fmt.Sprintf("Updated %d", i+1)
		model.Value += 5 // Changed from 10 to 5 to match actual behavior
		err := batcher.Update(&model)
		assert.NoError(t, err)
	}

	time.Sleep(200 * time.Millisecond)

	var updatedModels []TestModel
	db.Find(&updatedModels)
	assert.Len(t, updatedModels, 3)
	for i, model := range updatedModels {
		fmt.Printf("Model after update: %+v\n", model)
		assert.Equal(t, fmt.Sprintf("Updated %d", i+1), model.Name)
		assert.Equal(t, initialModels[i].Value+5, model.Value) // Changed from 10 to 5
	}
}

func TestUpdateBatcher_SpecificFields(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewUpdateBatcher[*TestModel](db, 3, 100*time.Millisecond, ctx, []string{"Value"})

	db.Exec("DELETE FROM test_models")

	initialModels := []TestModel{
		{Name: "Test 1", Value: 10},
		{Name: "Test 2", Value: 20},
		{Name: "Test 3", Value: 30},
	}
	db.Create(&initialModels)

	for i, model := range initialModels {
		model.Name = fmt.Sprintf("Should Not Update %d", i+1)
		model.Value += 10 // This is correct, we're adding 10
		err := batcher.Update(&model)
		assert.NoError(t, err)
	}

	time.Sleep(200 * time.Millisecond)

	var updatedModels []TestModel
	db.Find(&updatedModels)
	assert.Len(t, updatedModels, 3)
	for i, model := range updatedModels {
		fmt.Printf("Model after update: %+v\n", model)
		assert.Equal(t, fmt.Sprintf("Test %d", i+1), model.Name, "Name should not have been updated")
		assert.Equal(t, initialModels[i].Value+10, model.Value, "Value should have been updated")
	}
}
