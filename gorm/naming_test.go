package gorm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gormv2 "gorm.io/gorm"
)

// These pin that the batchers name tables and columns, and find primary keys
// and unique indexes, exactly as GORM does, instead of guessing.

func migrateForTest(t *testing.T, models ...interface{}) {
	t.Helper()
	require.NoError(t, db.Migrator().DropTable(models...))
	require.NoError(t, db.AutoMigrate(models...))
	t.Cleanup(func() { _ = db.Migrator().DropTable(models...) })
}

func newTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}

// NamingCategory has no TableName method; GORM stores it in naming_categories.
type NamingCategory struct {
	ID   uint `gorm:"primaryKey"`
	Name string
}

func TestUpdateBatcher_UsesGORMTableName(t *testing.T) {
	migrateForTest(t, &NamingCategory{})
	row := &NamingCategory{Name: "before"}
	require.NoError(t, db.Create(row).Error)

	b, err := NewUpdateBatcher[*NamingCategory](getDBProvider(), 10, time.Millisecond, newTestContext(t))
	require.NoError(t, err)
	require.NoError(t, b.Update([]*NamingCategory{{ID: row.ID, Name: "after"}}, []string{"Name"}))

	var got NamingCategory
	require.NoError(t, db.First(&got, row.ID).Error)
	assert.Equal(t, "after", got.Name)
}

// EmbeddedModelRow embeds GORM v2's gorm.Model, whose ID is tagged
// "primarykey" in lower case, and whose UpdatedAt is promoted.
type EmbeddedModelRow struct {
	gormv2.Model
	Name string
}

func TestUpdateBatcher_EmbeddedGormModel(t *testing.T) {
	migrateForTest(t, &EmbeddedModelRow{})
	row := &EmbeddedModelRow{Name: "before"}
	require.NoError(t, db.Create(row).Error)

	b, err := NewUpdateBatcher[*EmbeddedModelRow](getDBProvider(), 10, time.Millisecond, newTestContext(t))
	require.NoError(t, err, "the primary key inside gorm.Model is found")
	require.NoError(t, b.Update([]*EmbeddedModelRow{{Model: gormv2.Model{ID: row.ID}, Name: "after"}}, []string{"Name"}))

	var got EmbeddedModelRow
	require.NoError(t, db.First(&got, row.ID).Error)
	assert.Equal(t, "after", got.Name)
}

// CommentTaggedRow has a tag mentioning "column" without a column: setting,
// which used to panic the name lookup.
type CommentTaggedRow struct {
	ID   uint   `gorm:"primaryKey"`
	Note string `gorm:"comment:column note"`
}

func TestUpdateBatcher_CommentMentioningColumn(t *testing.T) {
	migrateForTest(t, &CommentTaggedRow{})
	row := &CommentTaggedRow{Note: "before"}
	require.NoError(t, db.Create(row).Error)

	b, err := NewUpdateBatcher[*CommentTaggedRow](getDBProvider(), 10, time.Millisecond, newTestContext(t))
	require.NoError(t, err)
	require.NoError(t, b.Update([]*CommentTaggedRow{{ID: row.ID, Note: "after"}}, []string{"Note"}))

	var got CommentTaggedRow
	require.NoError(t, db.First(&got, row.ID).Error)
	assert.Equal(t, "after", got.Note)
}

// UniqueByIndexTag declares its unique index in GORM's index:name,unique form.
type UniqueByIndexTag struct {
	ID   uint   `gorm:"primaryKey"`
	Code string `gorm:"index:idx_unique_by_index_tag_code,unique;type:varchar(50)"`
	Name string
}

func TestInsertBatcher_UniqueIndexTagFormReloadsID(t *testing.T) {
	migrateForTest(t, &UniqueByIndexTag{})
	existing := &UniqueByIndexTag{Code: "a", Name: "existing"}
	require.NoError(t, db.Create(existing).Error)

	b := NewInsertBatcher[*UniqueByIndexTag](getDBProvider(), 10, time.Millisecond, newTestContext(t))
	upsert := &UniqueByIndexTag{Code: "a", Name: "upserted"}
	require.NoError(t, b.Insert(upsert))
	assert.Equal(t, existing.ID, upsert.ID, "the record carries the id of the row it updated")

	// A new row and an update in one statement: ids assigned in sequence from
	// the first inserted id are wrong for the updated row unless it is
	// reloaded by its unique index.
	fresh := &UniqueByIndexTag{Code: "b", Name: "fresh"}
	again := &UniqueByIndexTag{Code: "a", Name: "upserted again"}
	require.NoError(t, b.Insert(fresh, again))
	assert.Equal(t, existing.ID, again.ID, "the updated row's id is reloaded")
	var stored UniqueByIndexTag
	require.NoError(t, db.Where("code = ?", "b").First(&stored).Error)
	assert.Equal(t, stored.ID, fresh.ID, "the new row's id is its own")
}

// UniquePair has a composite unique index whose column order is set by
// priority, which used to make each field look like its own index.
type UniquePair struct {
	ID    uint   `gorm:"primaryKey"`
	Left  string `gorm:"uniqueIndex:idx_unique_pair,priority:2;type:varchar(50)"`
	Right string `gorm:"uniqueIndex:idx_unique_pair,priority:1;type:varchar(50)"`
	Name  string
}

func TestInsertBatcher_CompositeUniqueIndexReloadsTheRightRow(t *testing.T) {
	migrateForTest(t, &UniquePair{})
	other := &UniquePair{Left: "x", Right: "1", Name: "other"}
	target := &UniquePair{Left: "x", Right: "2", Name: "target"}
	require.NoError(t, db.Create(other).Error)
	require.NoError(t, db.Create(target).Error)

	b := NewInsertBatcher[*UniquePair](getDBProvider(), 10, time.Millisecond, newTestContext(t))
	for i := 0; i < 10; i++ {
		upsert := &UniquePair{Left: "x", Right: "2", Name: "upserted"}
		require.NoError(t, b.Insert(upsert))
		require.Equal(t, target.ID, upsert.ID, "attempt %d: the id of the row matching both columns", i)
	}
}

// SelectRow has a column named by tag and a field GORM ignores.
type SelectRow struct {
	ID     uint   `gorm:"primaryKey"`
	Label  string `gorm:"column:display_label"`
	Cached string `gorm:"-"`
}

func TestSelectBatcher_CustomColumnName(t *testing.T) {
	migrateForTest(t, &SelectRow{})
	row := &SelectRow{Label: "shown"}
	require.NoError(t, db.Create(row).Error)

	b, err := NewSelectBatcher[SelectRow](getDBProvider(), 10, time.Millisecond, newTestContext(t), []string{"id", "display_label"})
	require.NoError(t, err)
	got, err := b.Select("id = ?", row.ID)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "shown", got[0].Label)
}

func TestSelectBatcher_RejectsIgnoredField(t *testing.T) {
	_, err := NewSelectBatcher[SelectRow](getDBProvider(), 10, time.Millisecond, newTestContext(t), []string{"id", "cached"})
	assert.Error(t, err, "a field tagged gorm:\"-\" has no column")
}
