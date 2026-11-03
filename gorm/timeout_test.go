//go:build !sqlite

package gorm

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/atlasgurus/batcher/batcher"
	gormv1 "github.com/jinzhu/gorm"
	"github.com/stretchr/testify/require"
	gormv2 "gorm.io/gorm"
)

// singleConnectionDB opens a pool limited to one connection, so a session
// variable left on the connection is visible to the next query.
func singleConnectionDB(t *testing.T) *gormv2.DB {
	t.Helper()
	if dialect != "mysql" {
		t.Skip("innodb_lock_wait_timeout is MySQL only")
	}
	v1, err := gormv1.Open("mysql", os.Getenv("DSN"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = v1.Close() })
	v1.DB().SetMaxOpenConns(1)
	v2, err := GormV1ToV2Adapter(v1)
	require.NoError(t, err)
	return v2
}

func sessionLockWaitTimeout(t *testing.T, db *gormv2.DB) int {
	t.Helper()
	var seconds int
	require.NoError(t, db.Raw("SELECT @@SESSION.innodb_lock_wait_timeout").Scan(&seconds).Error)
	return seconds
}

type timeoutProbe struct {
	ID          uint `gorm:"primaryKey"`
	Name        string
	seenTimeout int `gorm:"-"`
}

// BeforeCreate records the lock wait timeout in force inside the insert's
// transaction.
func (p *timeoutProbe) BeforeCreate(tx *gormv2.DB) error {
	return tx.Raw("SELECT @@SESSION.innodb_lock_wait_timeout").Scan(&p.seenTimeout).Error
}

// WithTimeout used to be ignored by the insert batcher.
func TestInsertBatcher_TimeoutApplies(t *testing.T) {
	sdb := singleConnectionDB(t)
	require.NoError(t, sdb.AutoMigrate(&timeoutProbe{}))
	t.Cleanup(func() { _ = sdb.Migrator().DropTable(&timeoutProbe{}) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := NewInsertBatcherWithOptions[*timeoutProbe](func() (*gormv2.DB, error) { return sdb, nil }, ctx,
		batcher.WithMaxBatchSize(1), batcher.WithMaxWaitTime(time.Millisecond), batcher.WithTimeout(7))

	probe := &timeoutProbe{Name: "probe"}
	require.NoError(t, b.Insert(probe))
	require.Equal(t, 7, probe.seenTimeout, "the insert ran under the configured lock wait timeout")
	require.NotEqual(t, 7, sessionLockWaitTimeout(t, sdb), "and the connection was reset afterwards")
}

type missingTableModel struct {
	ID   uint `gorm:"primaryKey"`
	Name string
}

func (missingTableModel) TableName() string { return "batcher_table_that_does_not_exist" }

// A write that failed rolled back without resetting the session's lock wait
// timeout, so the pooled connection kept it for whatever query used it next.
func TestUpdateBatcher_FailedWriteResetsLockWaitTimeout(t *testing.T) {
	sdb := singleConnectionDB(t)
	before := sessionLockWaitTimeout(t, sdb)
	require.NotEqual(t, 7, before)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := NewUpdateBatcherWithOptions[*missingTableModel](func() (*gormv2.DB, error) { return sdb, nil }, ctx,
		batcher.WithMaxBatchSize(1), batcher.WithMaxWaitTime(time.Millisecond), batcher.WithTimeout(7))
	require.NoError(t, err)

	require.Error(t, b.Update([]*missingTableModel{{ID: 1, Name: "x"}}, []string{"Name"}))
	require.Equal(t, before, sessionLockWaitTimeout(t, sdb), "the failed write left the connection's timeout as it found it")
}
