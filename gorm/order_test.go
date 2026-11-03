package gorm

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/atlasgurus/batcher/batcher"
	gormv1 "github.com/jinzhu/gorm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gormv2 "gorm.io/gorm"
)

type orderedRecord struct {
	ID   uint `gorm:"primaryKey"`
	Name string
}

// Deduplication used to rebuild the batch from a map, so rows went out in
// random order, and two batches touching the same rows locked them in
// different orders. Rows now keep the position where their key first
// appeared, with the last value; rows without a key keep their own position.
func TestDeduplicateRecordsKeepsFirstSeenOrder(t *testing.T) {
	pk, _, err := getPrimaryKeyInfo(reflect.TypeOf(orderedRecord{}))
	require.NoError(t, err)
	in := []orderedRecord{
		{ID: 5, Name: "five"},
		{ID: 3, Name: "three, first"},
		{ID: 0, Name: "no key"},
		{ID: 9, Name: "nine"},
		{ID: 3, Name: "three, last"},
		{ID: 1, Name: "one"},
	}
	want := []orderedRecord{
		{ID: 5, Name: "five"},
		{ID: 3, Name: "three, last"},
		{ID: 0, Name: "no key"},
		{ID: 9, Name: "nine"},
		{ID: 1, Name: "one"},
	}
	for i := 0; i < 20; i++ {
		assert.Equal(t, want, deduplicateRecords(in, pk))
	}
}

type decomposedParent struct {
	ID   uint `gorm:"primaryKey"`
	Name string
}

type decomposedChild struct {
	ID   uint `gorm:"primaryKey"`
	Name string
}

type decomposedRow struct {
	Parent decomposedParent
	Child  decomposedChild
}

// With field decomposition, each field's type is inserted into its own table.
// Those inserts used to run in map order, so a child table could be written
// before the parent its foreign key points at. They now run in the order the
// fields are declared.
func TestDecomposedFieldsInsertInDeclarationOrder(t *testing.T) {
	v1, err := gormv1.Open(dialect, os.Getenv("DSN"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = v1.Close() })
	tdb, err := GormV1ToV2Adapter(v1) // its own callbacks, separate from the shared db
	require.NoError(t, err)
	require.NoError(t, tdb.AutoMigrate(&decomposedParent{}, &decomposedChild{}))
	t.Cleanup(func() { _ = tdb.Migrator().DropTable(&decomposedParent{}, &decomposedChild{}) })

	var tables []string
	require.NoError(t, tdb.Callback().Create().Before("gorm:create").Register("test:record_table", func(d *gormv2.DB) {
		tables = append(tables, d.Statement.Table)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := NewInsertBatcherWithOptions[*decomposedRow](func() (*gormv2.DB, error) { return tdb, nil }, ctx,
		batcher.WithMaxBatchSize(1), batcher.WithMaxWaitTime(time.Millisecond), batcher.WithDecomposeFields(true))

	for i := uint(1); i <= 20; i++ {
		require.NoError(t, b.Insert(&decomposedRow{
			Parent: decomposedParent{ID: i, Name: "parent"},
			Child:  decomposedChild{ID: i, Name: "child"},
		}))
	}

	require.Len(t, tables, 40)
	for i := 0; i < len(tables); i += 2 {
		assert.Equal(t, []string{"decomposed_parents", "decomposed_children"}, tables[i:i+2], "batch %d", i/2)
	}
}
