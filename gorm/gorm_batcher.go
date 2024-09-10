package gorm

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/atlasgurus/batcher/batcher"
	"gorm.io/gorm"
)

// DBProvider is a function type that returns the current database connection and an error
type DBProvider func() (*gorm.DB, error)

// InsertBatcher is a GORM batcher for batch inserts
type InsertBatcher[T any] struct {
	dbProvider DBProvider
	batcher    *batcher.BatchProcessor[[]T]
}

// UpdateBatcher is a GORM batcher for batch updates
type UpdateBatcher[T any] struct {
	dbProvider DBProvider
	batcher    *batcher.BatchProcessor[[]UpdateItem[T]]
}

type UpdateItem[T any] struct {
	Item         T
	UpdateFields []string
}

// NewInsertBatcher creates a new GORM insert batcher
func NewInsertBatcher[T any](dbProvider DBProvider, maxBatchSize int, maxWaitTime time.Duration, ctx context.Context) *InsertBatcher[T] {
	return &InsertBatcher[T]{
		dbProvider: dbProvider,
		batcher:    batcher.NewBatchProcessor(maxBatchSize, maxWaitTime, ctx, batchInsert[T](dbProvider)),
	}
}

// NewUpdateBatcher creates a new GORM update batcher
func NewUpdateBatcher[T any](dbProvider DBProvider, maxBatchSize int, maxWaitTime time.Duration, ctx context.Context) *UpdateBatcher[T] {
	return &UpdateBatcher[T]{
		dbProvider: dbProvider,
		batcher:    batcher.NewBatchProcessor(maxBatchSize, maxWaitTime, ctx, batchUpdate[T](dbProvider)),
	}
}

// Insert submits one or more items for batch insertion
func (b *InsertBatcher[T]) Insert(items ...T) error {
	return b.batcher.SubmitAndWait(items)
}

// Update submits one or more items for batch update
func (b *UpdateBatcher[T]) Update(items []T, updateFields []string) error {
	updateItems := make([]UpdateItem[T], len(items))
	for i, item := range items {
		updateItems[i] = UpdateItem[T]{Item: item, UpdateFields: updateFields}
	}
	return b.batcher.SubmitAndWait(updateItems)
}

func batchInsert[T any](dbProvider DBProvider) func([][]T) error {
	return func(batches [][]T) error {
		if len(batches) == 0 {
			return nil
		}

		db, err := dbProvider()
		if err != nil {
			return fmt.Errorf("failed to get database connection: %w", err)
		}

		tx := db.Begin()
		if tx.Error != nil {
			return tx.Error
		}

		for _, batch := range batches {
			if err := tx.Create(batch).Error; err != nil {
				tx.Rollback()
				return err
			}
		}

		return tx.Commit().Error
	}
}

func batchUpdate[T any](dbProvider DBProvider) func([][]UpdateItem[T]) error {
	return func(batches [][]UpdateItem[T]) error {
		if len(batches) == 0 {
			return nil
		}

		db, err := dbProvider()
		if err != nil {
			return fmt.Errorf("failed to get database connection: %w", err)
		}

		tx := db.Begin()
		if tx.Error != nil {
			return tx.Error
		}

		for _, batch := range batches {
			for _, updateItem := range batch {
				item := updateItem.Item
				updateFields := convertFromSnakeCase(updateItem.UpdateFields)

				primaryKeys, primaryKeyValues := getPrimaryKeyAndValues(item)
				if len(primaryKeys) == 0 {
					tx.Rollback()
					return fmt.Errorf("no primary key found for item")
				}

				updateMap := make(map[string]interface{})
				v := reflect.ValueOf(item)
				t := v.Type()
				if t.Kind() == reflect.Ptr {
					v = v.Elem()
					t = v.Type()
				}

				fieldMatchCount := 0
				for i := 0; i < t.NumField(); i++ {
					field := t.Field(i)
					if len(updateFields) == 0 || contains(updateFields, field.Name) {
						updateMap[field.Name] = v.Field(i).Interface()
						fieldMatchCount++
					}
				}
				if fieldMatchCount != len(updateFields) && len(updateFields) > 0 {
					tx.Rollback()
					return fmt.Errorf("not all update fields found")
				}

				query := tx.Model(new(T))
				for i, key := range primaryKeys {
					query = query.Where(key+" = ?", primaryKeyValues[i])
				}

				if err := query.Updates(updateMap).Error; err != nil {
					tx.Rollback()
					return err
				}
			}
		}

		return tx.Commit().Error
	}
}

func convertFromSnakeCase(fields []string) []string {
	convertedFields := make([]string, len(fields))
	for i, field := range fields {
		convertedFields[i] = toPascalCase(field)
	}
	return convertedFields
}

func toPascalCase(s string) string {
	parts := strings.Split(s, "_")
	for i, part := range parts {
		if len(part) > 0 {
			r := []rune(part)
			r[0] = unicode.ToUpper(r[0])
			parts[i] = string(r)
		}
	}
	return strings.Join(parts, "")
}

// Helper function to check if a string is in a slice
func contains(slice []string, item string) bool {
	for _, s := range slice {
		// TODO: This is case-insensitive for now, but we may want to make it more nuanced
		if strings.ToLower(s) == strings.ToLower(item) {
			return true
		}
	}
	return false
}

// getPrimaryKeyAndValues uses reflection to find the primary key fields and their values
func getPrimaryKeyAndValues(item interface{}) ([]string, []interface{}) {
	t := reflect.TypeOf(item)
	v := reflect.ValueOf(item)

	// If it's a pointer, get the underlying element
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	var keys []string
	var values []interface{}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if tag := field.Tag.Get("gorm"); strings.Contains(tag, "primaryKey") || strings.Contains(tag, "primary_key") {
			keys = append(keys, field.Name)
			values = append(values, v.Field(i).Interface())
		}
	}

	return keys, values
}
