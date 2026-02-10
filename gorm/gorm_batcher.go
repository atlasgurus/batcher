package gorm

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
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
	dbProvider       DBProvider
	batcher          batcher.BatchProcessorInterface[[]T]
	config           *batcher.BatchProcessorConfig // Add access to config
	validationError  error
	timeout          int
	primaryKeyFields []reflect.StructField
	uniqueIndexes    [][]reflect.StructField // Each element is a set of fields forming a unique index
	tableName        string
}

func (ib *InsertBatcher[T]) Error() error {
	return ib.validationError
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

// NewInsertBatcher creates a new InsertBatcher
func NewInsertBatcher[T any](dbProvider DBProvider, maxBatchSize int, maxWaitTime time.Duration, ctx context.Context) *InsertBatcher[T] {
	return NewInsertBatcherWithOptions[T](
		dbProvider,
		ctx,
		batcher.WithMaxBatchSize(maxBatchSize),
		batcher.WithMaxWaitTime(maxWaitTime),
	)
}

// NewInsertBatcherWithOptions creates a new InsertBatcher with custom options
func NewInsertBatcherWithOptions[T any](dbProvider DBProvider, ctx context.Context, opts ...batcher.Option) *InsertBatcher[T] {
	// Create config and apply options to determine if decompose is enabled
	config := batcher.DefaultConfig()
	for _, opt := range opts {
		opt(&config)
	}

	var model T
	modelType := reflect.TypeOf(model)

	ib := &InsertBatcher[T]{
		dbProvider: dbProvider,
		config:     &config,
		tableName:  getTableNameFromType(modelType),
	}

	// Get primary key fields for deduplication (if available)
	primaryKeyFields, _, keyErr := getPrimaryKeyInfo(modelType)
	if keyErr == nil {
		// Only set if primary key exists, otherwise deduplication will be skipped
		ib.primaryKeyFields = primaryKeyFields
	}

	// Get unique index fields for ID reloading after IODKU
	ib.uniqueIndexes = getUniqueIndexes(modelType)

	ib.batcher = batcher.NewBatchProcessorWithOptions(ctx, ib.createBatchInsertFunc(), opts...)
	return ib
}

func (ib *InsertBatcher[T]) createBatchInsertFunc() func([][]T) []error {
	// Perform reflection once if decomposition is enabled
	var fieldToTypeMap map[string]reflect.Type // fieldName -> reflect.Type

	if ib.config.DecomposeFields {
		var model T
		modelType := reflect.TypeOf(model)
		if modelType.Kind() == reflect.Ptr {
			modelType = modelType.Elem()
		}

		// Pre-compute field names and their types once
		if modelType.Kind() == reflect.Struct {
			fieldToTypeMap = make(map[string]reflect.Type)
			for i := 0; i < modelType.NumField(); i++ {
				field := modelType.Field(i)
				if field.IsExported() {
					// Check if we can interface with this field type
					fieldValue := reflect.New(field.Type).Elem()
					if !fieldValue.CanInterface() {
						ib.validationError = fmt.Errorf("field %s of type %s cannot be interfaced for decomposition", field.Name, field.Type)
						break
					}
					fieldToTypeMap[field.Name] = field.Type
				}
			}
		}
	}

	return func(batches [][]T) []error {
		if len(batches) == 0 {
			return nil
		}

		// Return validation error if field decomposition setup failed
		if ib.validationError != nil {
			return batcher.RepeatErr(len(batches), ib.validationError)
		}

		var allRecords []T
		for _, batch := range batches {
			allRecords = append(allRecords, batch...)
		}

		if len(allRecords) == 0 {
			return batcher.RepeatErr(len(batches), nil)
		}

		// Deduplicate records by primary key (last value wins)
		allRecords = dedupeInsertItems(allRecords, ib.primaryKeyFields)

		// Track which records had zero IDs before insert (indicating potential IODKU update)
		// GORM will populate IDs incorrectly for updated records, so we need to reload them
		var recordsNeedingReload []int
		for i, record := range allRecords {
			if hasZeroPrimaryKey(record, ib.primaryKeyFields) {
				recordsNeedingReload = append(recordsNeedingReload, i)
			}
		}

		err := retryWithDeadlockDetection(maxRetries, ib.timeout, ib.dbProvider, func(tx *gorm.DB) error {
			if ib.config.DecomposeFields {
				return insertByFields(tx, allRecords, fieldToTypeMap)
			} else {
				// Build OnConflict clause with proper conflict targets
				onConflict := ib.buildOnConflictClause(tx)
				return tx.Clauses(onConflict).CreateInBatches(allRecords, len(allRecords)).Error
			}
		})

		if err != nil {
			return batcher.RepeatErr(len(batches), err)
		}

		// CRITICAL FIX: Reload records that had zero IDs before insert
		// GORM populates IDs using LastInsertID(), which is incorrect for IODKU updates
		// We must reload these records from the database to get correct IDs
		if len(recordsNeedingReload) > 0 && len(ib.uniqueIndexes) > 0 {
			if reloadErr := ib.reloadRecordsByUniqueIndexes(allRecords, recordsNeedingReload); reloadErr != nil {
				return batcher.RepeatErr(len(batches), fmt.Errorf("failed to reload IDs after insert: %w", reloadErr))
			}
		}

		return batcher.RepeatErr(len(batches), nil)
	}
}

func insertByFields[T any](tx *gorm.DB, records []T, fieldToTypeMap map[string]reflect.Type) error {
	if len(records) == 0 {
		return nil
	}

	// Group model instances by their precomputed reflect.Type
	typeBatches := make(map[reflect.Type][]interface{})

	for _, record := range records {
		rv := reflect.ValueOf(record)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}

		// Use precomputed field-to-type mapping - all fields should be valid
		for fieldName, fieldType := range fieldToTypeMap {
			fieldValue := rv.FieldByName(fieldName)
			if !fieldValue.IsValid() {
				return fmt.Errorf("field %s is not valid on record instance", fieldName)
			}
			modelInstance := fieldValue.Interface()
			typeBatches[fieldType] = append(typeBatches[fieldType], modelInstance)
		}
	}

	// Insert each type batch separately within the same transaction
	for modelType, modelInstances := range typeBatches {
		if len(modelInstances) > 0 {
			// Deduplicate child records by their primary keys
			deduplicatedInstances := deduplicateChildRecords(modelInstances, modelType)

			// Create a properly typed slice for GORM
			sliceType := reflect.SliceOf(modelType)
			typedSlice := reflect.MakeSlice(sliceType, len(deduplicatedInstances), len(deduplicatedInstances))

			for i, instance := range deduplicatedInstances {
				typedSlice.Index(i).Set(reflect.ValueOf(instance))
			}

			// Convert back to interface{} but now it's a properly typed slice
			typedSliceInterface := typedSlice.Interface()

			// Build OnConflict clause for this specific model type
			onConflict := buildOnConflictClauseForType(tx, modelType)
			err := tx.Clauses(onConflict).CreateInBatches(typedSliceInterface, len(deduplicatedInstances)).Error
			if err != nil {
				return fmt.Errorf("failed to insert field batch for type %s: %w", modelType.Name(), err)
			}
		}
	}
	return nil
}

// Helper function to get table name from struct type
func getTableName[T any]() string {
	var model T
	return getTableNameFromType(reflect.TypeOf(model))
}

func getTableNameFromType(fieldType reflect.Type) string {
	// Handle pointer types
	if fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}

	// Check if the type implements TableName method
	if tn, ok := reflect.New(fieldType).Interface().(interface{ TableName() string }); ok {
		return tn.TableName()
	}

	// Default to struct name converted to snake case plural
	return toSnakeCase(fieldType.Name()) + "s"
}

// Legacy constructor calls the new options-based one
func NewUpdateBatcher[T any](dbProvider DBProvider, maxBatchSize int, maxWaitTime time.Duration, ctx context.Context) (*UpdateBatcher[T], error) {
	return NewUpdateBatcherWithOptions[T](
		dbProvider,
		ctx,
		batcher.WithMaxBatchSize(maxBatchSize),
		batcher.WithMaxWaitTime(maxWaitTime),
	)
}

// NewUpdateBatcherWithOptions primary constructor using options
func NewUpdateBatcherWithOptions[T any](dbProvider DBProvider, ctx context.Context, opts ...batcher.Option) (*UpdateBatcher[T], error) {
	// Create config and apply options to determine if decompose is enabled
	config := batcher.DefaultConfig()
	for _, opt := range opts {
		opt(&config)
	}

	tableName := getTableName[T]()
	var model T
	primaryKeyField, primaryKeyName, keyErr := getPrimaryKeyInfo(reflect.TypeOf(model))
	if keyErr != nil {
		return nil, keyErr
	}

	return &UpdateBatcher[T]{
		dbProvider: dbProvider,
		batcher:    batcher.NewBatchProcessorWithOptions(ctx, batchUpdate[T](dbProvider, tableName, primaryKeyField, primaryKeyName, config.Timeout), opts...),
		tableName:  tableName,
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

// getUniqueIndexes returns all unique indexes defined on the struct
// Each element in the returned slice is a set of fields that form a unique constraint
func getUniqueIndexes(t reflect.Type) [][]reflect.StructField {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Map of index name to fields
	indexMap := make(map[string][]reflect.StructField)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		gormTag := field.Tag.Get("gorm")

		// Skip primary keys
		if isPrimaryKey(field) {
			continue
		}

		// Check for unique or uniqueIndex tags
		if strings.Contains(gormTag, "unique") || strings.Contains(gormTag, "uniqueIndex") {
			// Parse the tag to get index name
			indexName := ""
			tags := strings.Split(gormTag, ";")
			for _, tag := range tags {
				tag = strings.TrimSpace(tag)
				if strings.HasPrefix(tag, "uniqueIndex:") {
					indexName = strings.TrimPrefix(tag, "uniqueIndex:")
				} else if strings.HasPrefix(tag, "unique_index:") {
					// GORM v1 format
					indexName = strings.TrimPrefix(tag, "unique_index:")
				} else if tag == "unique" || tag == "uniqueIndex" || tag == "unique_index" {
					// Single field unique constraint (both v1 and v2 formats)
					indexName = field.Name
				}
			}

			if indexName != "" {
				indexMap[indexName] = append(indexMap[indexName], field)
			}
		}
	}

	// Convert map to slice
	var uniqueIndexes [][]reflect.StructField
	for _, fields := range indexMap {
		uniqueIndexes = append(uniqueIndexes, fields)
	}

	return uniqueIndexes
}

// Insert submits one or more items for batch insertion
func (b *InsertBatcher[T]) Insert(items ...T) error {
	return b.batcher.SubmitAndWait(items)
}

// InsertAsync non-blocking version of Insert with a callback
func (b *InsertBatcher[T]) InsertAsync(callback func(error), items ...T) {
	b.batcher.Submit(items, callback)
}

// Update submits one or more items for batch update
func (b *UpdateBatcher[T]) Update(items []T, updateFields []string) error {
	updateItems := make([]UpdateItem[T], len(items))
	for i, item := range items {
		updateItems[i] = UpdateItem[T]{Item: item, UpdateFields: updateFields}
	}
	return b.batcher.SubmitAndWait(updateItems)
}

// UpdateAsync non-blocking version of Update with a callback
func (b *UpdateBatcher[T]) UpdateAsync(callback func(error), items []T, updateFields []string) {
	updateItems := make([]UpdateItem[T], len(items))
	for i, item := range items {
		updateItems[i] = UpdateItem[T]{Item: item, UpdateFields: updateFields}
	}
	b.batcher.Submit(updateItems, callback)
}

const (
	maxRetries = 3
	baseDelay  = 100 * time.Millisecond
)

func executeIndividualUpdates[T any](
	tx *gorm.DB,
	tableName string,
	primaryKeyFields []reflect.StructField,
	primaryKeyNames []string,
	updateItem UpdateItem[T],
	mapping fieldMapping,
) error {
	item := updateItem.Item
	itemValue := reflect.ValueOf(item)
	if itemValue.Kind() == reflect.Ptr {
		itemValue = itemValue.Elem()
	}
	itemType := itemValue.Type()

	// Determine fields to update
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

	// Add UpdatedAt if it exists and isn't already included
	_, updatedAtExists := itemType.FieldByName("UpdatedAt")
	if updatedAtExists && !contains(fieldsToUpdate, "UpdatedAt") && !contains(fieldsToUpdate, "updated_at") {
		fieldsToUpdate = append(fieldsToUpdate, "UpdatedAt")
	}

	// Build the update query
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("UPDATE %s SET ", tableName))

	var values []interface{}

	// Add SET clause
	for i, fieldName := range fieldsToUpdate {
		structFieldName, dbFieldName, err := getFieldNames(fieldName, mapping)
		if err != nil {
			return err
		}

		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		queryBuilder.WriteString(fmt.Sprintf("%s = ?", dbFieldName))

		var fieldValue interface{}
		if fieldName == "UpdatedAt" {
			fieldValue = time.Now()
		} else {
			fieldValue = itemValue.FieldByName(structFieldName).Interface()
		}
		values = append(values, fieldValue)
	}

	// Add WHERE clause for primary keys
	queryBuilder.WriteString(" WHERE ")
	for i, pkField := range primaryKeyFields {
		if i > 0 {
			queryBuilder.WriteString(" AND ")
		}
		queryBuilder.WriteString(fmt.Sprintf("%s = ?", primaryKeyNames[i]))
		values = append(values, itemValue.FieldByName(pkField.Name).Interface())
	}

	return tx.Exec(queryBuilder.String(), values...).Error
}

func executeUpdatesBatchAsSeparateStatements[T any](
	tx *gorm.DB,
	tableName string,
	primaryKeyFields []reflect.StructField,
	primaryKeyNames []string,
	updateItems []UpdateItem[T],
	mapping fieldMapping,
) error {
	var queryBuilder strings.Builder
	var allValues []interface{}

	for itemIndex, updateItem := range updateItems {
		itemValue := reflect.ValueOf(updateItem.Item)
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

		_, updatedAtExists := itemType.FieldByName("UpdatedAt")
		if updatedAtExists && !contains(fieldsToUpdate, "UpdatedAt") && !contains(fieldsToUpdate, "updated_at") {
			fieldsToUpdate = append(fieldsToUpdate, "UpdatedAt")
		}

		for i, fieldName := range fieldsToUpdate {
			if itemIndex > 0 || i > 0 {
				queryBuilder.WriteString("; ")
			}

			structFieldName, dbFieldName, err := getFieldNames(fieldName, mapping)
			if err != nil {
				return err
			}

			queryBuilder.WriteString(fmt.Sprintf("UPDATE %s SET %s = ? WHERE (", tableName, dbFieldName))
			queryBuilder.WriteString(strings.Join(primaryKeyNames, ", "))
			queryBuilder.WriteString(") = (")
			queryBuilder.WriteString(strings.Repeat("?,", len(primaryKeyNames)-1) + "?")
			queryBuilder.WriteString(")")

			// Add the value to update
			var fieldValue interface{}
			if fieldName == "UpdatedAt" {
				fieldValue = time.Now()
			} else {
				fieldValue = itemValue.FieldByName(structFieldName).Interface()
			}
			allValues = append(allValues, fieldValue)

			// Add primary key values
			for _, pkField := range primaryKeyFields {
				field := itemValue.FieldByName(pkField.Name)
				if !field.IsValid() {
					return fmt.Errorf("primary key field %s not found in struct", pkField.Name)
				}
				allValues = append(allValues, field.Interface())
			}
		}
	}

	if len(allValues) == 0 {
		return nil
	}

	return tx.Exec(queryBuilder.String(), allValues...).Error
}

// useMultipleStatements a flag to enable multiple statements in a single query.  Experimental and has not been tested.
var useMultipleStatements = false // Set to true if sql_mode has ALLOW_MULTIPLE_STATEMENTS

func batchUpdate[T any](
	dbProvider DBProvider,
	tableName string,
	primaryKeyFields []reflect.StructField,
	primaryKeyNames []string,
	timeout int) func([][]UpdateItem[T]) []error {
	return func(batches [][]UpdateItem[T]) []error {
		if len(batches) == 0 {
			return nil
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

		// Replace original slice with deduplicated one
		allUpdateItems = dedupeUpdateItems(allUpdateItems, primaryKeyFields)

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

			_, updatedAtExists := itemType.FieldByName("UpdatedAt")
			if updatedAtExists && !contains(fieldsToUpdate, "UpdatedAt") && !contains(fieldsToUpdate, "updated_at") {
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

		queryBuilder.WriteString(" WHERE (")
		queryBuilder.WriteString(strings.Join(primaryKeyNames, ", "))
		queryBuilder.WriteString(") IN (")

		var placeholders []string
		for range allUpdateItems {
			placeholders = append(placeholders, "("+strings.Repeat("?,", len(primaryKeyNames)-1)+"?)")
		}
		queryBuilder.WriteString(strings.Join(placeholders, ", "))
		queryBuilder.WriteString(")")

		// Create tuples of composite key values
		for _, updateItem := range allUpdateItems {
			itemValue := reflect.ValueOf(updateItem.Item)
			if itemValue.Kind() == reflect.Ptr {
				itemValue = itemValue.Elem()
			}

			for _, pkField := range primaryKeyFields {
				field := itemValue.FieldByName(pkField.Name)
				if !field.IsValid() {
					return batcher.RepeatErr(len(batches), fmt.Errorf("primary key field %s not found in struct", pkField.Name))
				}
				allValues = append(allValues, field.Interface())
			}
		}

		var batchErr error
		if useMultipleStatements {
			batchErr = retryWithDeadlockDetection(maxRetries, timeout, dbProvider, func(tx *gorm.DB) error {
				return executeUpdatesBatchAsSeparateStatements(
					tx,
					tableName,
					primaryKeyFields,
					primaryKeyNames,
					allUpdateItems,
					mapping)
			})

			// If batch update succeeds, return success
			if batchErr == nil {
				return batcher.RepeatErr(len(batches), nil)
			}

		} else {
			// Try batch update first
			batchErr = retryWithDeadlockDetection(maxRetries, timeout, dbProvider, func(tx *gorm.DB) error {
				return tx.Exec(queryBuilder.String(), allValues...).Error
			})

			// If batch update succeeds, return success
			if batchErr == nil {
				return batcher.RepeatErr(len(batches), nil)
			}
		}

		// If batch update fails, try individual updates
		var fallbackErrors []error
		for _, updateItem := range allUpdateItems {
			if retryErr := retryWithDeadlockDetection(maxRetries, timeout, dbProvider, func(tx *gorm.DB) error {
				return executeIndividualUpdates(
					tx,
					tableName,
					primaryKeyFields,
					primaryKeyNames,
					updateItem,
					mapping,
				)
			}); retryErr != nil {
				fallbackErrors = append(fallbackErrors, fmt.Errorf("failed individual update: %w", retryErr))
			}
		}
		if len(fallbackErrors) > 0 {
			// Both batch and individual updates failed
			fallbackErr := fmt.Errorf("%d individual updates failed: (%v)", len(fallbackErrors), fallbackErrors)
			return batcher.RepeatErr(len(batches), fmt.Errorf("batch update failed: %v, individual updates failed: %v", batchErr, fallbackErr))
		}

		// Individual updates succeeded
		return batcher.RepeatErr(len(batches), nil)
	}
}
func dedupeUpdateItems[T any](allUpdateItems []UpdateItem[T], primaryKeyFields []reflect.StructField) []UpdateItem[T] {
	// Track both the latest values and all fields to update for each ID
	type combinedUpdate struct {
		item         T
		updateFields map[string]bool
	}
	latestUpdates := make(map[string]combinedUpdate)

	for _, updateItem := range allUpdateItems {
		item := updateItem.Item
		itemValue := reflect.ValueOf(item)
		if itemValue.Kind() == reflect.Ptr {
			itemValue = itemValue.Elem()
		}

		// Create composite key for the item
		var keyParts []string
		for _, pkField := range primaryKeyFields {
			keyParts = append(keyParts, fmt.Sprint(itemValue.FieldByName(pkField.Name).Interface()))
		}
		compositeKey := strings.Join(keyParts, "-")

		// Get or create the combined update
		combined, exists := latestUpdates[compositeKey]
		if !exists {
			combined = combinedUpdate{
				item:         item,
				updateFields: make(map[string]bool),
			}
		}
		// Keep the latest value
		combined.item = item
		// Merge the fields to update
		for _, field := range updateItem.UpdateFields {
			combined.updateFields[field] = true
		}
		latestUpdates[compositeKey] = combined
	}

	// Convert back to UpdateItems
	deduplicatedItems := make([]UpdateItem[T], 0, len(latestUpdates))
	for _, combined := range latestUpdates {
		fields := make([]string, 0, len(combined.updateFields))
		for field := range combined.updateFields {
			fields = append(fields, field)
		}
		deduplicatedItems = append(deduplicatedItems, UpdateItem[T]{
			Item:         combined.item,
			UpdateFields: fields,
		})
	}
	return deduplicatedItems
}

// buildCompositeKey returns a composite key for records with non-zero PKs, or empty string for zero PKs
func buildCompositeKey(itemValue reflect.Value, primaryKeyFields []reflect.StructField) (string, bool) {
	if itemValue.Kind() == reflect.Ptr {
		itemValue = itemValue.Elem()
	}

	// Check if all primary key fields have non-zero values
	var keyParts []string
	hasNonZeroPK := true

	for _, pkField := range primaryKeyFields {
		fieldValue := itemValue.FieldByName(pkField.Name)
		keyParts = append(keyParts, fmt.Sprint(fieldValue.Interface()))

		// Check if field is zero value (for auto-increment PKs)
		if fieldValue.IsZero() {
			hasNonZeroPK = false
		}
	}

	if !hasNonZeroPK {
		return "", false // Zero PK, should not be deduped
	}

	return strings.Join(keyParts, "-"), true
}

// deduplicateRecords is the core deduplication logic that works with any record type
// It deduplicates records by primary key (last value wins) and preserves records with zero PKs
func deduplicateRecords[T any](records []T, primaryKeyFields []reflect.StructField) []T {
	if len(primaryKeyFields) == 0 {
		// No primary key, cannot deduplicate
		return records
	}

	// Track the latest value for each primary key
	latestRecords := make(map[string]T)
	var recordsWithoutPK []T

	for _, record := range records {
		compositeKey, hasKey := buildCompositeKey(reflect.ValueOf(record), primaryKeyFields)

		if !hasKey {
			// Zero PK - will be auto-generated, don't deduplicate
			recordsWithoutPK = append(recordsWithoutPK, record)
			continue
		}

		// Keep the latest value (last one wins)
		latestRecords[compositeKey] = record
	}

	// Convert back to slice: deduplicated records + records without PK
	deduplicatedRecords := make([]T, 0, len(latestRecords)+len(recordsWithoutPK))
	for _, record := range latestRecords {
		deduplicatedRecords = append(deduplicatedRecords, record)
	}
	deduplicatedRecords = append(deduplicatedRecords, recordsWithoutPK...)

	return deduplicatedRecords
}

func deduplicateChildRecords(instances []interface{}, modelType reflect.Type) []interface{} {
	// Get primary key info for this child type
	primaryKeyFields, _, err := getPrimaryKeyInfo(modelType)
	if err != nil || len(primaryKeyFields) == 0 {
		// No primary key, cannot deduplicate
		return instances
	}

	// Use the generic deduplication function
	return deduplicateRecords(instances, primaryKeyFields)
}

func dedupeInsertItems[T any](allRecords []T, primaryKeyFields []reflect.StructField) []T {
	// Use the generic deduplication function
	return deduplicateRecords(allRecords, primaryKeyFields)
}

func isDeadlockError(err error) bool {
	return strings.Contains(err.Error(), "Deadlock found when trying to get lock") ||
		strings.Contains(err.Error(), "database table is locked") ||
		strings.Contains(err.Error(), "Lock wait timeout exceeded")
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

type SelectBatcher[T any] struct {
	dbProvider DBProvider
	batcher    batcher.BatchProcessorInterface[[]SelectItem[T]]
	tableName  string
	columns    []string
}

type SelectItem[T any] struct {
	Condition string
	Args      []interface{}
	Results   *[]T
}

// NewSelectBatcher creates a new SelectBatcher
func NewSelectBatcher[T any](dbProvider DBProvider, maxBatchSize int, maxWaitTime time.Duration, ctx context.Context, columns []string) (*SelectBatcher[T], error) {
	return NewSelectBatcherWithOptions[T](
		dbProvider,
		ctx,
		columns,
		batcher.WithMaxBatchSize(maxBatchSize),
		batcher.WithMaxWaitTime(maxWaitTime),
	)
}

// NewSelectBatcherWithOptions creates a new SelectBatcher with custom options
func NewSelectBatcherWithOptions[T any](dbProvider DBProvider, ctx context.Context, columns []string, opts ...batcher.Option) (*SelectBatcher[T], error) {
	// Create config and apply options to determine if decompose is enabled
	config := batcher.DefaultConfig()
	for _, opt := range opts {
		opt(&config)
	}

	tableName := getTableName[T]()

	// Validate columns against model
	var model T
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	allColumns, err := getColumnNames(model)
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %w", err)
	}

	columnSet := make(map[string]bool)
	for _, col := range allColumns {
		columnSet[col] = true
	}

	for _, col := range columns {
		if !columnSet[col] {
			return nil, fmt.Errorf("column %s does not exist in model", col)
		}
	}

	return &SelectBatcher[T]{
		dbProvider: dbProvider,
		batcher:    batcher.NewBatchProcessorWithOptions(ctx, batchSelect[T](dbProvider, tableName, columns, config.Timeout), opts...),
		tableName:  tableName,
		columns:    columns,
	}, nil
}

func (b *SelectBatcher[T]) Select(condition string, args ...interface{}) ([]T, error) {
	var results []T
	item := SelectItem[T]{
		Condition: condition,
		Args:      args,
		Results:   &results,
	}
	err := b.batcher.SubmitAndWait([]SelectItem[T]{item})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// SelectAsync non-blocking version of Select with a callback
func (b *SelectBatcher[T]) SelectAsync(callback func([]T, error), condition string, args ...interface{}) {
	var results []T
	item := SelectItem[T]{
		Condition: condition,
		Args:      args,
		Results:   &results,
	}
	b.batcher.Submit([]SelectItem[T]{item}, func(err error) {
		callback(results, err)
	})
}

func processRows[T any](rows *sql.Rows, columns []string, allItems []SelectItem[T]) error {
	// Prepare a slice of slices to hold all results
	results := make([][]T, len(allItems))
	for i := range results {
		results[i] = make([]T, 0)
	}

	// Scan the results
	for rows.Next() {
		values := make([]interface{}, len(columns)+1) // +1 for __index
		for i := range values {
			values[i] = new(interface{})
		}

		if err := rows.Scan(values...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Get the index
		indexValue := reflect.ValueOf(values[0]).Elem().Interface()
		index, err := convertToInt(indexValue)
		if err != nil {
			return fmt.Errorf("failed to parse index: %w", err)
		}

		// Create a new instance of T and scan into it
		var result T
		if err := scanIntoStruct(&result, columns, values[1:]); err != nil {
			return fmt.Errorf("failed to scan into struct: %w", err)
		}

		// Append the result to the corresponding slice
		results[index] = append(results[index], result)
	}

	// Assign results back to the original items
	for i, item := range allItems {
		*item.Results = results[i]
	}

	return nil
}

func batchSelect[T any](dbProvider DBProvider, tableName string, columns []string, timeout int) func([][]SelectItem[T]) []error {
	return func(batches [][]SelectItem[T]) (errs []error) {
		defer func() {
			if r := recover(); r != nil {
				errs = batcher.RepeatErr(len(batches), fmt.Errorf("panic in batchSelect: %v", r))
			}
		}()

		if len(batches) == 0 {
			return batcher.RepeatErr(len(batches), nil)
		}

		db, err := dbProvider()
		if err != nil {
			return batcher.RepeatErr(len(batches), fmt.Errorf("failed to get database connection: %w", err))
		}

		var allItems []SelectItem[T]
		for _, batch := range batches {
			allItems = append(allItems, batch...)
		}

		if len(allItems) == 0 {
			return batcher.RepeatErr(len(batches), nil)
		}

		// Build the query
		var queryBuilder strings.Builder
		var args []interface{}

		dialectName := db.Dialector.Name()

		for i, item := range allItems {
			if i > 0 {
				queryBuilder.WriteString(" UNION ALL ")
			}
			selectColumns := append([]string{fmt.Sprintf("CAST(? AS CHAR) AS %s", quoteIdentifier("__index", dialectName))}, columns...)
			queryBuilder.WriteString(fmt.Sprintf("SELECT %s FROM %s WHERE %s",
				strings.Join(selectColumns, ", "),
				quoteIdentifier(tableName, dialectName),
				item.Condition))
			args = append(args, i) // Add index as an argument
			args = append(args, item.Args...)
		}

		queryBuilder.WriteString(fmt.Sprintf(" ORDER BY %s", quoteIdentifier("__index", dialectName)))

		var processingErr error
		err = retryWithDeadlockDetection(maxRetries, timeout, dbProvider, func(tx *gorm.DB) error {
			rows, err := tx.Raw(queryBuilder.String(), args...).Rows()
			if err != nil {
				return err
			}
			defer rows.Close()

			// Process the rows within the transaction
			processingErr = processRows(rows, columns, allItems)
			return nil
		})

		if err != nil {
			return batcher.RepeatErr(len(batches), fmt.Errorf("failed to execute select query: %w", err))
		}

		if processingErr != nil {
			return batcher.RepeatErr(len(batches), processingErr)
		}

		return batcher.RepeatErr(len(batches), nil)
	}
}

func convertToInt(value interface{}) (int, error) {
	switch v := value.(type) {
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case uint:
		return int(v), nil
	case uint64:
		return int(v), nil
	case []uint8:
		return strconv.Atoi(string(v))
	case string:
		return strconv.Atoi(v)
	default:
		return 0, fmt.Errorf("unexpected index type: %T", value)
	}
}

func quoteIdentifier(identifier string, dialect string) string {
	switch dialect {
	case "mysql":
		return "`" + strings.Replace(identifier, "`", "``", -1) + "`"
	case "postgres":
		return `"` + strings.Replace(identifier, `"`, `""`, -1) + `"`
	case "sqlite":
		return `"` + strings.Replace(identifier, `"`, `""`, -1) + `"`
	default:
		return identifier
	}
}

func getColumnNames(model interface{}) ([]string, error) {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model must be a struct or a pointer to a struct")
	}

	var columns []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Anonymous {
			continue // Skip embedded fields
		}
		gormTag := field.Tag.Get("gorm")
		column := ""
		if gormTag != "" && gormTag != "-" {
			tagParts := strings.Split(gormTag, ";")
			for _, part := range tagParts {
				if strings.HasPrefix(part, "column:") {
					column = strings.TrimPrefix(part, "column:")
					break
				}
			}
		}
		if column == "" {
			column = toSnakeCase(field.Name)
		}
		columns = append(columns, column)
	}
	return columns, nil
}

func scanIntoStruct(result interface{}, columns []string, values []interface{}) error {
	resultValue := reflect.ValueOf(result)
	if resultValue.Kind() != reflect.Ptr {
		return fmt.Errorf("result must be a pointer")
	}
	resultValue = resultValue.Elem()

	if resultValue.Kind() == reflect.Ptr {
		if resultValue.IsNil() {
			resultValue.Set(reflect.New(resultValue.Type().Elem()))
		}
		resultValue = resultValue.Elem()
	}

	if resultValue.Kind() != reflect.Struct {
		return fmt.Errorf("result must be a pointer to a struct")
	}

	resultType := resultValue.Type()

	for i, column := range columns {
		value := reflect.ValueOf(values[i]).Elem().Interface()

		field := resultValue.FieldByNameFunc(func(name string) bool {
			field, _ := resultType.FieldByName(name)
			dbName := field.Tag.Get("gorm")
			if dbName == column {
				return true
			}
			return strings.EqualFold(name, column) ||
				strings.EqualFold(toSnakeCase(name), column) ||
				strings.EqualFold(name, strings.ReplaceAll(column, "_", ""))
		})

		if !field.IsValid() {
			return fmt.Errorf("no field found for column %s", column)
		}

		if !field.CanSet() {
			return fmt.Errorf("field %s cannot be set (might be unexported)", column)
		}

		switch field.Kind() {
		case reflect.String:
			switch v := value.(type) {
			case []uint8:
				field.SetString(string(v))
			case string:
				field.SetString(v)
			default:
				return fmt.Errorf("cannot set string field %s with value of type %T", column, value)
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			switch v := value.(type) {
			case int64:
				field.SetUint(uint64(v))
			case uint64:
				field.SetUint(v)
			default:
				return fmt.Errorf("cannot set uint field %s with value of type %T", column, value)
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			switch v := value.(type) {
			case int64:
				field.SetInt(v)
			case int:
				field.SetInt(int64(v))
			default:
				return fmt.Errorf("cannot set int field %s with value of type %T", column, value)
			}
		case reflect.Float32, reflect.Float64:
			v, ok := value.(float64)
			if !ok {
				return fmt.Errorf("cannot set float field %s with value of type %T", column, value)
			}
			field.SetFloat(v)
		case reflect.Bool:
			v, ok := value.(bool)
			if !ok {
				return fmt.Errorf("cannot set bool field %s with value of type %T", column, value)
			}
			field.SetBool(v)
		default:
			return fmt.Errorf("unsupported field type for %s: %v", column, field.Kind())
		}
	}
	return nil
}

func retryWithDeadlockDetection(maxRetries int, timeout int, dbProvider DBProvider, operation func(*gorm.DB) error) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		db, err := dbProvider()
		if err != nil {
			return fmt.Errorf("failed to get database connection: %w", err)
		}

		// For batch update we do not want to lock all updated rows at once.
		// Start transaction with READ COMMITTED isolation level to avoid locking all rows.
		tx := db.Begin(&sql.TxOptions{
			Isolation: sql.LevelReadCommitted,
		})
		if tx.Error != nil {
			return fmt.Errorf("failed to begin transaction: %w", tx.Error)
		}

		// Create a session within transaction to avoid updating the global session
		sessionDB := tx.Session(&gorm.Session{})

		if timeout > 0 && db.Dialector.Name() == "mysql" {
			// Set lock wait timeout for this session
			if timeoutSetErr := sessionDB.Exec(fmt.Sprintf("SET SESSION innodb_lock_wait_timeout = %d", timeout)).Error; timeoutSetErr != nil {
				sessionDB.Rollback()
				return fmt.Errorf("failed to set session timeout: %w", timeoutSetErr)
			}
		}

		// Execute the operation
		err = operation(sessionDB)
		if err != nil {
			sessionDB.Rollback()
			if !isDeadlockError(err) {
				return fmt.Errorf("operation failed: %w", err)
			}
			fmt.Printf("Error detected (retrying): %v\n", err)
			delayNextAttempt(attempt)
			lastErr = err
			continue
		}

		// Reset timeout before committing, so we can still use the session
		if timeout > 0 && db.Dialector.Name() == "mysql" {
			if resetErr := sessionDB.Exec("SET SESSION innodb_lock_wait_timeout = DEFAULT").Error; resetErr != nil {
				// Log the error but don't fail the operation
				fmt.Printf("Warning: failed to reset session timeout: %v\n", resetErr)
			}
		}

		// Commit the transaction
		if err = sessionDB.Commit().Error; err != nil {
			sessionDB.Rollback()
			if !isDeadlockError(err) {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}
			fmt.Printf("Error detected during commit (retrying): %v\n", err)
			delayNextAttempt(attempt)
			lastErr = err
			continue
		}

		return nil
	}

	return fmt.Errorf("operation failed after %d retries: %w", maxRetries, lastErr)
}

func delayNextAttempt(attempt int) {
	delay := baseDelay * time.Duration(1<<uint(attempt)) * time.Duration(1+rand.Intn(100)) / 100
	time.Sleep(delay)
}

// hasZeroPrimaryKey checks if all primary key fields have zero values
func hasZeroPrimaryKey[T any](record T, primaryKeyFields []reflect.StructField) bool {
	if len(primaryKeyFields) == 0 {
		return false
	}

	rv := reflect.ValueOf(record)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	for _, pkField := range primaryKeyFields {
		fieldValue := rv.FieldByName(pkField.Name)
		if !fieldValue.IsZero() {
			return false
		}
	}

	return true
}

// buildOnConflictClauseForType builds the appropriate OnConflict clause for a given model type
func buildOnConflictClauseForType(tx *gorm.DB, modelType reflect.Type) clause.OnConflict {
	dialect := tx.Dialector.Name()

	// MySQL: ON DUPLICATE KEY UPDATE works with any unique constraint
	if dialect == "mysql" {
		return clause.OnConflict{UpdateAll: true}
	}

	// PostgreSQL/SQLite: ON CONFLICT requires targeting a SINGLE unique constraint
	// Strategy: Try unique indexes first (for auto-increment PKs), fall back to PK

	uniqueIndexes := getUniqueIndexes(modelType)
	primaryKeyFields, _, _ := getPrimaryKeyInfo(modelType)

	// If we have unique indexes, use the first one as conflict target
	// This handles the common case where ID=0 (auto-increment) conflicts on unique index
	if len(uniqueIndexes) > 0 {
		var conflictColumns []clause.Column
		for _, field := range uniqueIndexes[0] {
			columnName := getDBFieldName(field)
			conflictColumns = append(conflictColumns, clause.Column{Name: columnName})
		}
		return clause.OnConflict{
			Columns:   conflictColumns,
			UpdateAll: true,
		}
	}

	// No unique indexes, use primary key
	if len(primaryKeyFields) > 0 {
		var conflictColumns []clause.Column
		for _, pkField := range primaryKeyFields {
			columnName := getDBFieldName(pkField)
			conflictColumns = append(conflictColumns, clause.Column{Name: columnName})
		}
		return clause.OnConflict{
			Columns:   conflictColumns,
			UpdateAll: true,
		}
	}

	// Fallback: let GORM handle it
	return clause.OnConflict{UpdateAll: true}
}

// buildOnConflictClause builds the appropriate OnConflict clause based on database dialect
func (ib *InsertBatcher[T]) buildOnConflictClause(tx *gorm.DB) clause.OnConflict {
	var model T
	return buildOnConflictClauseForType(tx, reflect.TypeOf(model))
}

// reloadRecordsByUniqueIndexes reloads records from database using unique indexes
func (ib *InsertBatcher[T]) reloadRecordsByUniqueIndexes(allRecords []T, recordIndices []int) error {
	if len(recordIndices) == 0 || len(ib.uniqueIndexes) == 0 {
		return nil
	}

	db, err := ib.dbProvider()
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}

	// For each record that needs reloading, query by unique indexes
	for _, idx := range recordIndices {
		record := allRecords[idx]
		rv := reflect.ValueOf(record)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}

		// Try each unique index until we find a match
		var reloadedRecord T
		found := false

		for _, uniqueIndex := range ib.uniqueIndexes {
			// Build WHERE clause for this unique index
			// Use Model() instead of Table() to get correct table name from GORM's schema
			query := db.Model(new(T))
			allFieldsSet := true

			for _, field := range uniqueIndex {
				fieldValue := rv.FieldByName(field.Name)
				if fieldValue.IsZero() {
					// Can't use this index if any field is zero
					allFieldsSet = false
					break
				}

				columnName := getDBFieldName(field)
				query = query.Where(fmt.Sprintf("%s = ?", columnName), fieldValue.Interface())
			}

			if !allFieldsSet {
				continue
			}

			// Execute query
			result := query.First(&reloadedRecord)
			if result.Error == nil {
				// Found the record, copy primary key fields back to original
				reloadedRV := reflect.ValueOf(reloadedRecord)
				if reloadedRV.Kind() == reflect.Ptr {
					reloadedRV = reloadedRV.Elem()
				}

				// Update primary key fields in the original record
				for _, pkField := range ib.primaryKeyFields {
					pkValue := reloadedRV.FieldByName(pkField.Name)
					rv.FieldByName(pkField.Name).Set(pkValue)
				}

				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("failed to reload record at index %d: no matching unique index found", idx)
		}
	}

	return nil
}
