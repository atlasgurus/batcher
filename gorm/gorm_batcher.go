package gorm

import (
	"context"
	"fmt"
	"reflect"
	"strings"
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
			// Get the primary key field and value
			primaryKey, primaryKeyValue := getPrimaryKeyAndValue(item)
			if primaryKey == "" {
				tx.Rollback()
				return fmt.Errorf("primary key not found for item")
			}

			// Create a map of fields to update
			updateMap := make(map[string]interface{})
			v := reflect.ValueOf(item)
			t := v.Type()
			if t.Kind() == reflect.Ptr {
				v = v.Elem()
				t = v.Type()
			}

			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				if len(updateFields) == 0 || contains(updateFields, field.Name) {
					updateMap[field.Name] = v.Field(i).Interface()
				}
			}

			// Perform the update
			result := tx.Model(new(T)).Where(primaryKey+" = ?", primaryKeyValue).Updates(updateMap)
			if result.Error != nil {
				tx.Rollback()
				return result.Error
			}
		}

		return tx.Commit().Error
	}
}

// Helper function to check if a string is in a slice
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// getPrimaryKeyAndValue uses reflection to find the primary key field and its value
func getPrimaryKeyAndValue(item interface{}) (string, interface{}) {
	t := reflect.TypeOf(item)
	v := reflect.ValueOf(item)

	// If it's a pointer, get the underlying element
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if tag := field.Tag.Get("gorm"); strings.Contains(tag, "primaryKey") {
			return field.Name, v.Field(i).Interface()
		}
	}

	return "", nil
}
