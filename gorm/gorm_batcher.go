package gorm

import (
	"context"
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

		for _, item := range items {
			query := tx.Model(item)
			if len(updateFields) > 0 {
				query = query.Select(updateFields)
			}
			if err := query.Updates(item).Error; err != nil {
				tx.Rollback()
				return err
			}
		}

		return tx.Commit().Error
	}
}
