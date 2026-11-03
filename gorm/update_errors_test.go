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

type UpdateErrorRow struct {
	ID   uint   `gorm:"primaryKey"`
	Code string `gorm:"uniqueIndex;type:varchar(50)"`
}

// updateTwoCallersInOneBatch submits two updates from different callers into
// one batch and returns each caller's error.
func updateTwoCallersInOneBatch(t *testing.T, first, second UpdateItem[*UpdateErrorRow]) (error, error) {
	t.Helper()
	b, err := NewUpdateBatcherWithOptions[*UpdateErrorRow](getDBProvider(), newTestContext(t),
		batcher.WithMaxBatchSize(10), batcher.WithMaxWaitTime(500*time.Millisecond))
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, u := range []UpdateItem[*UpdateErrorRow]{first, second} {
		wg.Add(1)
		b.UpdateAsync(func(err error) {
			errs[i] = err
			wg.Done()
		}, []*UpdateErrorRow{u.Item}, u.UpdateFields)
	}
	wg.Wait()
	return errs[0], errs[1]
}

// seedUpdateErrorRows creates rows 1, 2 and 3 with codes code-1, code-2 and
// code-3.
func seedUpdateErrorRows(t *testing.T) {
	t.Helper()
	migrateForTest(t, &UpdateErrorRow{})
	for id := uint(1); id <= 3; id++ {
		require.NoError(t, db.Create(&UpdateErrorRow{ID: id, Code: fmt.Sprintf("code-%d", id)}).Error)
	}
}

func codeOf(t *testing.T, id uint) string {
	t.Helper()
	var row UpdateErrorRow
	require.NoError(t, db.First(&row, id).Error)
	return row.Code
}

// One caller naming a field the model does not have used to fail the whole
// batch, every caller in it included.
func TestUpdateBatcher_UnknownFieldFailsOnlyItsCaller(t *testing.T) {
	seedUpdateErrorRows(t)
	errBad, errGood := updateTwoCallersInOneBatch(t,
		UpdateItem[*UpdateErrorRow]{Item: &UpdateErrorRow{ID: 1, Code: "new-1"}, UpdateFields: []string{"NoSuchField"}},
		UpdateItem[*UpdateErrorRow]{Item: &UpdateErrorRow{ID: 2, Code: "new-2"}, UpdateFields: []string{"Code"}},
	)
	assert.Error(t, errBad)
	assert.NoError(t, errGood, "the other caller's update is written")
	assert.Equal(t, "code-1", codeOf(t, 1))
	assert.Equal(t, "new-2", codeOf(t, 2))
}

// When the batched statement fails, rows are written one by one. A row that
// still fails used to fail every caller, including those whose rows were
// written, which invited retries of writes that had succeeded. Here caller A
// takes a code row 3 already holds, a unique-index violation on every
// database and in every SQL mode.
func TestUpdateBatcher_FailedRowFailsOnlyItsCaller(t *testing.T) {
	seedUpdateErrorRows(t)
	errBad, errGood := updateTwoCallersInOneBatch(t,
		UpdateItem[*UpdateErrorRow]{Item: &UpdateErrorRow{ID: 1, Code: "code-3"}, UpdateFields: []string{"Code"}},
		UpdateItem[*UpdateErrorRow]{Item: &UpdateErrorRow{ID: 2, Code: "new-2"}, UpdateFields: []string{"Code"}},
	)
	assert.Error(t, errBad)
	assert.NoError(t, errGood, "the other caller's row was written, so it gets no error")
	assert.Equal(t, "code-1", codeOf(t, 1))
	assert.Equal(t, "new-2", codeOf(t, 2))
}
