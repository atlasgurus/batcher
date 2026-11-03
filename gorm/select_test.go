package gorm

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/atlasgurus/batcher/batcher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SelectTypes covers column types the select batcher used to fail on: a
// MySQL boolean arrives as an integer, a decimal as bytes, and a NULL as nil.
type SelectTypes struct {
	ID     uint `gorm:"primaryKey"`
	Active bool
	Label  string
	Amount float64 `gorm:"type:decimal(10,2)"`
	Seen   time.Time
}

func TestSelectBatcher_ConvertsValuesLikeGORM(t *testing.T) {
	migrateForTest(t, &SelectTypes{})
	seen := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	require.NoError(t, db.Exec("INSERT INTO select_types (id, active, label, amount, seen) VALUES (?, ?, NULL, ?, ?)",
		1, true, 12.5, seen).Error)

	b, err := NewSelectBatcher[SelectTypes](getDBProvider(), 10, time.Millisecond, newTestContext(t),
		[]string{"id", "active", "label", "amount", "seen"})
	require.NoError(t, err)
	got, err := b.Select("id = ?", 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.True(t, got[0].Active, "boolean")
	assert.Equal(t, "", got[0].Label, "NULL reads as the zero value")
	assert.Equal(t, 12.5, got[0].Amount, "decimal")
	assert.True(t, seen.Equal(got[0].Seen.UTC()), "time: got %v", got[0].Seen)
}

type SelectMany struct {
	ID   uint `gorm:"primaryKey"`
	Name string
}

// More than ten selects in one batch: each must get its own rows. On
// Postgres the index used to be cast to a one-character CHAR, so selects 10
// and up were handed to the select numbered by their first digit.
func TestSelectBatcher_ManySelectsInOneBatch(t *testing.T) {
	migrateForTest(t, &SelectMany{})
	const n = 12
	for i := 1; i <= n; i++ {
		require.NoError(t, db.Create(&SelectMany{ID: uint(i), Name: fmt.Sprintf("row %d", i)}).Error)
	}

	b, err := NewSelectBatcherWithOptions[SelectMany](getDBProvider(), newTestContext(t), []string{"id", "name"},
		batcher.WithMaxBatchSize(n), batcher.WithMaxWaitTime(500*time.Millisecond))
	require.NoError(t, err)

	var wg sync.WaitGroup
	results := make([][]SelectMany, n+1)
	errs := make([]error, n+1)
	for i := 1; i <= n; i++ {
		wg.Add(1)
		b.SelectAsync(func(rows []SelectMany, err error) {
			results[i], errs[i] = rows, err
			wg.Done()
		}, "id = ?", i)
	}
	wg.Wait()
	for i := 1; i <= n; i++ {
		require.NoError(t, errs[i], "select %d", i)
		require.Len(t, results[i], 1, "select %d gets exactly its own row", i)
		assert.Equal(t, uint(i), results[i][0].ID, "select %d", i)
	}
}
