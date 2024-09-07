package gorm

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/atlasgurus/batcher/batcher"
	"gorm.io/gorm"
)

// InsertBatcher is a GORM batcher for batch inserts
type InsertBatcher[T any] struct {
	db      *gorm.DB
	batcher *batcher.BatchProcessor[T]
}

// UpdateBatcher is a GORM batcher for batch updates
type UpdateBatcher[T any] struct {
	db           *gorm.DB
	batcher      *batcher.BatchProcessor[T]
	updateFields []string
}

// NewInsertBatcher creates a new GORM insert batcher
func NewInsertBatcher[T any](db *gorm.DB, maxBatchSize int, maxWaitTime time.Duration, ctx context.Context) *InsertBatcher[T] {
	return &InsertBatcher[T]{
		db:      db,
		batcher: batcher.NewBatchProcessor(maxBatchSize, maxWaitTime, ctx, batchInsert[T](db)),
	}
}

// NewUpdateBatcher creates a new GORM update batcher
func NewUpdateBatcher[T any](db *gorm.DB, maxBatchSize int, maxWaitTime time.Duration, ctx context.Context, updateFields []string) *UpdateBatcher[T] {
	return &UpdateBatcher[T]{
		db:           db,
		batcher:      batcher.NewBatchProcessor(maxBatchSize, maxWaitTime, ctx, batchUpdate[T](db, updateFields)),
		updateFields: updateFields,
	}
}

// Insert submits an item for batch insertion
func (b *InsertBatcher[T]) Insert(item T) error {
	return b.batcher.SubmitAndWait(item)
}

// Update submits an item for batch update
func (b *UpdateBatcher[T]) Update(item T) error {
	return b.batcher.SubmitAndWait(item)
}

func batchInsert[T any](db *gorm.DB) func([]T) error {
	return func(items []T) error {
		if len(items) == 0 {
			return nil
		}
		return db.Create(items).Error
	}
}

func batchUpdate[T any](db *gorm.DB, updateFields []string) func([]T) error {
	return func(items []T) error {
		if len(items) == 0 {
			return nil
		}

		// Start a transaction
		tx := db.Begin()
		if tx.Error != nil {
			return tx.Error
		}

		// Create a map to hold the updates for each field
		updates := make(map[string]interface{})
		for _, field := range updateFields {
			updates[field] = gorm.Expr("CASE")
		}

		// Iterate over the items and populate the map with the updates
		for _, item := range items {
			itemValue := reflect.ValueOf(item)
			idValue := itemValue.FieldByName("ID")
			if !idValue.IsValid() {
				tx.Rollback()
				return fmt.Errorf("item does not have an ID field")
			}
			id := idValue.Interface()

			for _, field := range updateFields {
				value := itemValue.FieldByName(field).Interface()
				updates[field] = gorm.Expr(fmt.Sprintf("%s WHEN id = ? THEN ?", updates[field]), id, value)
			}
		}

		// Construct a single update query using the map
		for field, expr := range updates {
			updates[field] = gorm.Expr(fmt.Sprintf("%s ELSE %s END", expr, field))
		}

		// Execute the update query
		if err := tx.Model(&items[0]).Updates(updates).Error; err != nil {
			tx.Rollback()
			return err
		}

		// Commit the transaction
		return tx.Commit().Error
	}
}
