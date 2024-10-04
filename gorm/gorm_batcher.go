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
	"gorm.io/gorm/clause"
)

// DBProvider is a function type that returns the current database connection and an error
type DBProvider func() (*gorm.DB, error)

// InsertBatcher is a GORM batcher for batch inserts
type InsertBatcher[T any] struct {
	dbProvider DBProvider
	batcher    batcher.BatchProcessorInterface[[]T]
}

// UpdateBatcher is a GORM batcher for batch updates
type UpdateBatcher[T any] struct {
	dbProvider DBProvider
	batcher    batcher.BatchProcessorInterface[[]UpdateItem[T]]
	tableName  string
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

type batchUpdater struct {
	primaryKeyField reflect.StructField
	primaryKeyName  string
	tableName       string
}

// NewUpdateBatcher creates a new GORM update batcher
func NewUpdateBatcher[T any](dbProvider DBProvider, maxBatchSize int, maxWaitTime time.Duration, ctx context.Context) (*UpdateBatcher[T], error) {
	// Create a temporary DB connection to get the table name
	db, err := dbProvider()
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %w", err)
	}

	// Use a new instance of T to get the table name
	var model T
	stmt := &gorm.Statement{DB: db}
	err = stmt.Parse(&model)
	if err != nil {
		return nil, fmt.Errorf("failed to parse model for table name: %w", err)
	}

	primaryKeyField, primaryKeyName, keyErr := getPrimaryKeyInfo(reflect.TypeOf(model))
	if keyErr != nil {
		return nil, keyErr
	}

	return &UpdateBatcher[T]{
		dbProvider: dbProvider,
		batcher:    batcher.NewBatchProcessor(maxBatchSize, maxWaitTime, ctx, batchUpdate[T](dbProvider, stmt.Table, primaryKeyField, primaryKeyName)),
		tableName:  stmt.Table,
	}, nil
}

func isPrimaryKey(field reflect.StructField) bool {
	gormTag := field.Tag.Get("gorm")
	return strings.Contains(gormTag, "primaryKey") || strings.Contains(gormTag, "primary_key")
}

func getPrimaryKeyInfo(t reflect.Type) ([]reflect.StructField, []string, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	var primaryKeyFields []reflect.StructField
	var primaryKeyNames []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if isPrimaryKey(field) {
			primaryKeyFields = append(primaryKeyFields, field)
			columnName := field.Tag.Get("gorm")
			if strings.Contains(columnName, "column:") {
				columnName = strings.Split(strings.Split(columnName, "column:")[1], ";")[0]
			} else {
				columnName = toSnakeCase(field.Name)
			}
			primaryKeyNames = append(primaryKeyNames, columnName)
		}
	}
	if len(primaryKeyFields) == 0 {
		return nil, nil, fmt.Errorf("no primary key found for type %v", t)
	}
	return primaryKeyFields, primaryKeyNames, nil
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

func batchInsert[T any](dbProvider DBProvider) func([][]T) []error {
	return func(batches [][]T) []error {
		if len(batches) == 0 {
			return nil
		}

		db, err := dbProvider()
		if err != nil {
			return batcher.RepeatErr(len(batches), fmt.Errorf("failed to get database connection: %w", err))
		}

		// Flatten all batches into a single slice
		var allRecords []T
		for _, batch := range batches {
			allRecords = append(allRecords, batch...)
		}

		if len(allRecords) == 0 {
			return batcher.RepeatErr(len(batches), nil)
		}

		// Perform a single bulk upsert for all records, replacing all fields on duplicate
		err = db.Clauses(clause.OnConflict{
			UpdateAll: true,
		}).CreateInBatches(allRecords, len(allRecords)).Error
		if err != nil {
			return batcher.RepeatErr(len(batches), fmt.Errorf("failed to upsert records: %w", err))
		}

		return batcher.RepeatErr(len(batches), nil)
	}
}

func batchUpdate[T any](
	dbProvider DBProvider,
	tableName string,
	primaryKeyFields []reflect.StructField,
	primaryKeyNames []string) func([][]UpdateItem[T]) []error {
	return func(batches [][]UpdateItem[T]) []error {
		if len(batches) == 0 {
			return nil
		}

		db, err := dbProvider()
		if err != nil {
			return batcher.RepeatErr(len(batches), fmt.Errorf("failed to get database connection: %w", err))
		}

		var allUpdateItems []UpdateItem[T]
		for _, batch := range batches {
			allUpdateItems = append(allUpdateItems, batch...)
		}

		if len(allUpdateItems) == 0 {
			return batcher.RepeatErr(len(batches), nil)
		}

		mapping, err := createFieldMapping(allUpdateItems[0].Item)
		if err != nil {
			return batcher.RepeatErr(len(batches), err)
		}

		updateFieldsMap := make(map[string]bool)
		var updateFields []string
		var casesPerField = make(map[string][]string)
		var valuesPerField = make(map[string][]interface{})

		for _, updateItem := range allUpdateItems {
			item := updateItem.Item
			itemValue := reflect.ValueOf(item)
			if itemValue.Kind() == reflect.Ptr {
				itemValue = itemValue.Elem()
			}
			itemType := itemValue.Type()

			fieldsToUpdate := updateItem.UpdateFields
			if len(fieldsToUpdate) == 0 {
				// Update all fields except the primary key
				for i := 0; i < itemType.NumField(); i++ {
					field := itemType.Field(i)
					if !isPrimaryKey(field) {
						fieldsToUpdate = append(fieldsToUpdate, field.Name)
					}
				}
			}

			// Always include updated_at field if it exists
			_, updatedAtExists := itemType.FieldByName("UpdatedAt")
			if updatedAtExists && !contains(fieldsToUpdate, "UpdatedAt") {
				fieldsToUpdate = append(fieldsToUpdate, "UpdatedAt")
			}

			for _, fieldName := range fieldsToUpdate {
				structFieldName, dbFieldName, getFieldErr := getFieldNames(fieldName, mapping)
				if getFieldErr != nil {
					return batcher.RepeatErr(len(batches), getFieldErr)
				}

				if !updateFieldsMap[dbFieldName] {
					updateFieldsMap[dbFieldName] = true
					updateFields = append(updateFields, dbFieldName)
				}

				var caseBuilder strings.Builder
				caseBuilder.WriteString("WHEN ")
				var caseValues []interface{}
				for i, pkField := range primaryKeyFields {
					if i > 0 {
						caseBuilder.WriteString(" AND ")
					}
					caseBuilder.WriteString(fmt.Sprintf("%s = ?", primaryKeyNames[i]))
					caseValues = append(caseValues, itemValue.FieldByName(pkField.Name).Interface())
				}
				caseBuilder.WriteString(" THEN ?")

				var fieldValue interface{}
				if fieldName == "UpdatedAt" {
					fieldValue = time.Now() // Use current time for updated_at
				} else {
					fieldValue = itemValue.FieldByName(structFieldName).Interface()
				}
				caseValues = append(caseValues, fieldValue)

				casesPerField[dbFieldName] = append(casesPerField[dbFieldName], caseBuilder.String())
				valuesPerField[dbFieldName] = append(valuesPerField[dbFieldName], caseValues...)
			}
		}

		if len(updateFields) == 0 {
			return batcher.RepeatErr(len(batches), fmt.Errorf("no fields to update"))
		}

		var queryBuilder strings.Builder
		queryBuilder.WriteString(fmt.Sprintf("UPDATE %s SET ", tableName))
		var allValues []interface{}

		for i, field := range updateFields {
			if i > 0 {
				queryBuilder.WriteString(", ")
			}
			queryBuilder.WriteString(fmt.Sprintf("%s = CASE %s ELSE %s END",
				field, strings.Join(casesPerField[field], " "), field))
			allValues = append(allValues, valuesPerField[field]...)
		}

		queryBuilder.WriteString(" WHERE ")
		for i, pkName := range primaryKeyNames {
			if i > 0 {
				queryBuilder.WriteString(" AND ")
			}
			queryBuilder.WriteString(fmt.Sprintf("%s IN (?)", pkName))
		}

		for _, pkField := range primaryKeyFields {
			pkValues := getPrimaryKeyValues(allUpdateItems, pkField.Name)
			allValues = append(allValues, pkValues)
		}

		if execErr := db.Exec(queryBuilder.String(), allValues...).Error; execErr != nil {
			return batcher.RepeatErr(len(batches), execErr)
		}

		return batcher.RepeatErr(len(batches), nil)
	}
}

// Helper function to check if a slice contains a string
func contains(slice []string, str string) bool {
	for _, v := range slice {
		if v == str {
			return true
		}
	}
	return false
}

type fieldMapping struct {
	structToDb map[string]string
	dbToStruct map[string]string
}

func createFieldMapping(item interface{}) (fieldMapping, error) {
	mapping := fieldMapping{
		structToDb: make(map[string]string),
		dbToStruct: make(map[string]string),
	}
	t := reflect.TypeOf(item)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		dbFieldName := getDBFieldName(field)
		mapping.structToDb[field.Name] = dbFieldName
		mapping.dbToStruct[dbFieldName] = field.Name
	}

	return mapping, nil
}

func getFieldNames(fieldName string, mapping fieldMapping) (string, string, error) {
	if structFieldName, ok := mapping.dbToStruct[fieldName]; ok {
		return structFieldName, fieldName, nil
	}
	if dbFieldName, ok := mapping.structToDb[fieldName]; ok {
		return fieldName, dbFieldName, nil
	}
	return "", "", fmt.Errorf("field %s not found", fieldName)
}

func getDBFieldName(field reflect.StructField) string {
	if dbName := field.Tag.Get("gorm"); dbName != "" {
		if strings.Contains(dbName, "column") {
			return strings.Split(strings.Split(dbName, "column:")[1], ";")[0]
		}
	}
	return toSnakeCase(field.Name)
}

func getPrimaryKeyValues[T any](items []UpdateItem[T], primaryKeyFieldName string) []interface{} {
	var values []interface{}
	for _, item := range items {
		itemValue := reflect.ValueOf(item.Item)
		if itemValue.Kind() == reflect.Ptr {
			itemValue = itemValue.Elem()
		}
		values = append(values, itemValue.FieldByName(primaryKeyFieldName).Interface())
	}
	return values
}

func toSnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 {
			prevIsLower := unicode.IsLower(rune(s[i-1]))
			currIsUpper := unicode.IsUpper(r)
			nextIsLower := i+1 < len(s) && unicode.IsLower(rune(s[i+1]))

			if (prevIsLower && currIsUpper) || (currIsUpper && nextIsLower) {
				result.WriteRune('_')
			}
		}
		result.WriteRune(unicode.ToLower(r))
	}
	return result.String()
}
