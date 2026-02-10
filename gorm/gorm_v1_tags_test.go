package gorm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestModel_GORMv1_UniqueIndex uses GORM v1 tag format (unique_index with underscore)
type TestModel_GORMv1_UniqueIndex struct {
	ID        uint   `gorm:"primary_key"`
	Email     string `gorm:"not null;unique_index"` // GORM v1 format!
	Name      string
	MyValue   int
	UpdatedAt time.Time
}

func (TestModel_GORMv1_UniqueIndex) TableName() string {
	return "test_model_with_unique_indices" // Reuse existing table
}

// This test reproduces the user's bug with GORM v1 tags
func TestGORMv1_UniqueIndex_IDPopulation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewInsertBatcher[*TestModel_GORMv1_UniqueIndex](getDBProvider(), 10, 1*time.Millisecond, ctx)

	// Check if uniqueIndexes are detected
	if len(batcher.uniqueIndexes) == 0 {
		t.Errorf("❌ BUG: uniqueIndexes not detected for GORM v1 'unique_index' tag!")
		t.Errorf("   This means ID reload will NOT work")
	} else {
		t.Logf("✅ uniqueIndexes detected: %d indexes", len(batcher.uniqueIndexes))
	}

	db, err := getDBProvider()()
	require.NoError(t, err)

	// Clean up
	db.Exec("DELETE FROM test_model_with_unique_indices WHERE email = 'gormv1@example.com'")
	defer db.Exec("DELETE FROM test_model_with_unique_indices WHERE email = 'gormv1@example.com'")

	// Pre-insert existing record
	existing := &TestModel_GORMv1_UniqueIndex{
		Email:   "gormv1@example.com",
		Name:    "Existing",
		MyValue: 100,
	}
	err = db.Create(existing).Error
	require.NoError(t, err)
	expectedID := existing.ID
	t.Logf("Pre-inserted with ID=%d", expectedID)

	// Try to insert with same email (IODKU)
	newRecord := &TestModel_GORMv1_UniqueIndex{
		Email:   "gormv1@example.com", // Same email!
		Name:    "Updated",
		MyValue: 200,
	}

	require.Zero(t, newRecord.ID, "New record starts with ID=0")

	err = batcher.Insert(newRecord)
	require.NoError(t, err)

	t.Logf("After Insert: newRecord.ID=%d (expected=%d)", newRecord.ID, expectedID)

	// THE BUG: Without the fix, ID will be 0 because uniqueIndexes is empty
	if newRecord.ID == 0 {
		t.Errorf("❌ BUG REPRODUCED: ID is 0 after IODKU!")
		t.Errorf("   GORM v1 'unique_index' tag not recognized")
	} else if newRecord.ID != expectedID {
		t.Errorf("❌ Wrong ID: got %d, expected %d", newRecord.ID, expectedID)
	} else {
		t.Logf("✅ SUCCESS: ID correctly populated with %d", newRecord.ID)
	}

	assert.NotZero(t, newRecord.ID, "ID must be populated after IODKU")
	assert.Equal(t, expectedID, newRecord.ID, "ID must match existing record")
}
