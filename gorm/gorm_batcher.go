package gorm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/atlasgurus/batcher/batcher"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// DBProvider is a function type that returns the current database connection and an error
type DBProvider func() (*gorm.DB, error)

// ErrInsertCommittedIDsUnverified reports that an insert batch was committed
// but the ids of some of its records could not be read back afterwards, so
// those records may carry wrong ids. The rows are in the database: a caller
// that retries on this error inserts them again. Test for it with errors.Is.
var ErrInsertCommittedIDsUnverified = errors.New("batcher: insert committed, but record ids could not be verified")

// InsertBatcher is a GORM batcher for batch inserts
type InsertBatcher[T any] struct {
	dbProvider       DBProvider
	batcher          batcher.BatchProcessorInterface[[]T]
	config           *batcher.BatchProcessorConfig // Add access to config
	validationError  error
	timeout          int
	primaryKeyFields []reflect.StructField
	// uniqueIndexes are the model's unique indexes as parsed when the batcher
	// was created. Reloading ids re-reads them with the database's naming.
	uniqueIndexes [][]*schema.Field
	models        modelCache
}

func (ib *InsertBatcher[T]) Error() error {
	return ib.validationError
}

// UpdateBatcher is a GORM batcher for batch updates
type UpdateBatcher[T any] struct {
	dbProvider DBProvider
	batcher    batcher.BatchProcessorInterface[[]UpdateItem[T]]
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
		timeout:    config.Timeout,
	}

	// Primary key fields drive deduplication, and unique indexes drive id
	// reloading after an upsert. A model GORM cannot parse, such as a
	// decomposed wrapper of several models, has neither.
	if info, err := parseModel(nil, modelType); err == nil {
		ib.primaryKeyFields = info.primaryKeyStructFields()
		ib.uniqueIndexes = info.uniqueIndexes
	}

	ib.batcher = batcher.NewBatchProcessorWithOptions(ctx, ib.createBatchInsertFunc(), opts...)
	return ib
}

func (ib *InsertBatcher[T]) createBatchInsertFunc() func([][]T) []error {
	// Perform reflection once if decomposition is enabled
	var decomposed []decomposedField // in declaration order

	if ib.config.DecomposeFields {
		var model T
		modelType := reflect.TypeOf(model)
		if modelType.Kind() == reflect.Ptr {
			modelType = modelType.Elem()
		}

		// Pre-compute field names and their types once
		if modelType.Kind() == reflect.Struct {
			for i := 0; i < modelType.NumField(); i++ {
				field := modelType.Field(i)
				if field.IsExported() {
					// Check if we can interface with this field type
					fieldValue := reflect.New(field.Type).Elem()
					if !fieldValue.CanInterface() {
						ib.validationError = fmt.Errorf("field %s of type %s cannot be interfaced for decomposition", field.Name, field.Type)
						break
					}
					decomposed = append(decomposed, decomposedField{name: field.Name, typ: field.Type})
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
				return insertByFields(tx, allRecords, decomposed)
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
				return batcher.RepeatErr(len(batches), fmt.Errorf("%w: %w", ErrInsertCommittedIDsUnverified, reloadErr))
			}
		}

		return batcher.RepeatErr(len(batches), nil)
	}
}

// decomposedField is one field of a model whose fields are inserted into their
// own tables.
type decomposedField struct {
	name string
	typ  reflect.Type
}

// insertByFields inserts each field's value into the table of its type, one
// type at a time, in the order the fields are declared, so a parent declared
// before its child is written first.
func insertByFields[T any](tx *gorm.DB, records []T, fields []decomposedField) error {
	if len(records) == 0 {
		return nil
	}

	// Group model instances by type, keeping the order types first appear in.
	typeBatches := make(map[reflect.Type][]interface{})
	var typeOrder []reflect.Type

	for _, record := range records {
		rv := reflect.ValueOf(record)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}

		for _, f := range fields {
			fieldValue := rv.FieldByName(f.name)
			if !fieldValue.IsValid() {
				return fmt.Errorf("field %s is not valid on record instance", f.name)
			}
			if _, seen := typeBatches[f.typ]; !seen {
				typeOrder = append(typeOrder, f.typ)
			}
			typeBatches[f.typ] = append(typeBatches[f.typ], fieldValue.Interface())
		}
	}

	// Insert each type batch separately within the same transaction
	for _, modelType := range typeOrder {
		modelInstances := typeBatches[modelType]
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

	var model T
	info, err := parseModel(nil, reflect.TypeOf(model))
	if err != nil {
		return nil, err
	}
	if len(info.primaryKeys) == 0 {
		return nil, fmt.Errorf("no primary key found for type %v", reflect.TypeOf(model))
	}

	return &UpdateBatcher[T]{
		dbProvider: dbProvider,
		batcher:    batcher.NewBatchProcessorWithOptions(ctx, batchUpdate[T](dbProvider, config.Timeout), opts...),
	}, nil
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
		fieldsToUpdate = mapping.allUpdatable()
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
			fieldsToUpdate = mapping.allUpdatable()
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

func batchUpdate[T any](dbProvider DBProvider, timeout int) func([][]UpdateItem[T]) []error {
	var models modelCache
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

		// Table, column and key names come from GORM's schema for the model,
		// with the naming strategy of the database the batch is written to.
		namingDB, err := dbProvider()
		if err != nil {
			return batcher.RepeatErr(len(batches), fmt.Errorf("failed to get database connection: %w", err))
		}
		info, err := models.get(namingDB, reflect.TypeOf(allUpdateItems[0].Item))
		if err != nil {
			return batcher.RepeatErr(len(batches), err)
		}
		tableName := info.schema.Table
		primaryKeyFields := info.primaryKeyStructFields()
		primaryKeyNames := make([]string, len(info.primaryKeys))
		for i, f := range info.primaryKeys {
			primaryKeyNames[i] = f.DBName
		}
		mapping := createFieldMapping(info.schema)

		// Each caller gets the errors of its own rows only. errs holds one
		// entry per caller; a caller whose field names do not resolve fails
		// alone and its rows are left out, and failValid gives an error that
		// concerns the whole batch to every caller still in it.
		errs := make([]error, len(batches))
		failValid := func(err error) []error {
			for b := range errs {
				if errs[b] == nil {
					errs[b] = err
				}
			}
			return errs
		}
		var validItems []UpdateItem[T]
		var validOrigins []int
		for b, batch := range batches {
			if err := checkUpdateFields(batch, mapping); err != nil {
				errs[b] = err
				continue
			}
			for _, u := range batch {
				validItems = append(validItems, u)
				validOrigins = append(validOrigins, b)
			}
		}
		if len(validItems) == 0 {
			return errs
		}

		// Replace original slice with deduplicated one; rowOrigins[i] lists
		// the callers whose updates make up allUpdateItems[i].
		allUpdateItems, rowOrigins, err := dedupeUpdateItems(validItems, validOrigins, primaryKeyFields, mapping)
		if err != nil {
			return failValid(err)
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
				fieldsToUpdate = mapping.allUpdatable()
			}

			_, updatedAtExists := itemType.FieldByName("UpdatedAt")
			if updatedAtExists && !contains(fieldsToUpdate, "UpdatedAt") && !contains(fieldsToUpdate, "updated_at") {
				fieldsToUpdate = append(fieldsToUpdate, "UpdatedAt")
			}

			for _, fieldName := range fieldsToUpdate {
				structFieldName, dbFieldName, getFieldErr := getFieldNames(fieldName, mapping)
				if getFieldErr != nil {
					return failValid(getFieldErr)
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
			return failValid(fmt.Errorf("no fields to update"))
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
					return failValid(fmt.Errorf("primary key field %s not found in struct", pkField.Name))
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
				return errs
			}

		} else {
			// Try batch update first
			batchErr = retryWithDeadlockDetection(maxRetries, timeout, dbProvider, func(tx *gorm.DB) error {
				return tx.Exec(queryBuilder.String(), allValues...).Error
			})

			// If batch update succeeds, return success
			if batchErr == nil {
				return errs
			}
		}

		// If batch update fails, try individual updates. A row that still
		// fails fails only the callers whose updates it carries; the others'
		// rows were written.
		for i, updateItem := range allUpdateItems {
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
				rowErr := fmt.Errorf("batch update failed: %v, individual update failed: %w", batchErr, retryErr)
				for _, b := range rowOrigins[i] {
					if errs[b] == nil {
						errs[b] = rowErr
					}
				}
			}
		}
		return errs
	}
}

// checkUpdateFields reports whether every field name one caller's updates
// name exists in the model.
func checkUpdateFields[T any](updates []UpdateItem[T], mapping fieldMapping) error {
	for _, u := range updates {
		for _, f := range u.UpdateFields {
			if _, _, err := getFieldNames(f, mapping); err != nil {
				return err
			}
		}
	}
	return nil
}

// dedupeUpdateItems merges the updates of one row into a single update. The
// batched UPDATE takes the first CASE branch that matches a row, so without the
// merge every later update of that row would be dropped.
//
// Each update contributes only the fields it names, with the values from its
// own item, applied in submission order: a field ends with the value of the
// last update that named it, and a field no update named is not written. An
// update with no field list names every field except the primary key. Field
// names may be struct or column names: a field named both ways is one field,
// and the merged update keeps the spelling of the last update that named it,
// because batchUpdate treats "UpdatedAt" (current time) and "updated_at" (the
// item's value) differently.
//
// A row updated once keeps its original UpdateItem. A merged row gets a new
// item; the callers' items are never modified. Rows keep the order in which
// they were first seen.
//
// origins[i] names the caller that submitted allUpdateItems[i]; the second
// result lists, for each returned update, the callers whose updates it carries.
func dedupeUpdateItems[T any](allUpdateItems []UpdateItem[T], origins []int, primaryKeyFields []reflect.StructField, mapping fieldMapping) ([]UpdateItem[T], [][]int, error) {
	type namedField struct {
		structName string // the field the value is copied from and to
		spelling   string // the name as the caller gave it
	}
	type mergedRow struct {
		original UpdateItem[T]     // the row's first update, used as is if it stays alone
		value    reflect.Value     // addressable copy of the row, once a second update arrives
		fields   []string          // struct field names named so far, in first-named order
		spelling map[string]string // struct field name -> spelling of the last update that named it
		origins  []int             // callers whose updates the row carries, each once
	}

	// namedFields returns the fields an update writes.
	namedFields := func(u UpdateItem[T], itemValue reflect.Value) ([]namedField, error) {
		if len(u.UpdateFields) == 0 {
			all := mapping.allUpdatable()
			named := make([]namedField, len(all))
			for i, name := range all {
				named[i] = namedField{structName: name, spelling: name}
			}
			return named, nil
		}
		named := make([]namedField, 0, len(u.UpdateFields))
		for _, f := range u.UpdateFields {
			structName, _, err := getFieldNames(f, mapping)
			if err != nil {
				return nil, err
			}
			named = append(named, namedField{structName: structName, spelling: f})
		}
		return named, nil
	}

	rows := make(map[string]*mergedRow)
	var result []*mergedRow
	var unkeyed []UpdateItem[T]
	var unkeyedOrigins [][]int
	for i, u := range allUpdateItems {
		itemValue := reflect.ValueOf(u.Item)
		if itemValue.Kind() == reflect.Pointer {
			itemValue = itemValue.Elem()
		}
		key, ok := buildCompositeKey(itemValue, primaryKeyFields)
		if !ok {
			// A zero primary key names no particular row; nothing to merge.
			unkeyed = append(unkeyed, u)
			unkeyedOrigins = append(unkeyedOrigins, []int{origins[i]})
			continue
		}
		fields, err := namedFields(u, itemValue)
		if err != nil {
			return nil, nil, err
		}

		row, seen := rows[key]
		if !seen {
			row = &mergedRow{original: u, spelling: make(map[string]string)}
			rows[key] = row
			result = append(result, row)
		} else {
			if !row.value.IsValid() {
				// Second update of this row: start from a copy of the first.
				first := reflect.ValueOf(row.original.Item)
				if first.Kind() == reflect.Pointer {
					first = first.Elem()
				}
				row.value = reflect.New(first.Type()).Elem()
				row.value.Set(first)
			}
			for _, f := range fields {
				dst := row.value.FieldByName(f.structName)
				if !dst.CanSet() {
					return nil, nil, fmt.Errorf("field %s cannot be set when merging updates of one row", f.structName)
				}
				dst.Set(itemValue.FieldByName(f.structName))
			}
		}
		for _, f := range fields {
			if _, named := row.spelling[f.structName]; !named {
				row.fields = append(row.fields, f.structName)
			}
			row.spelling[f.structName] = f.spelling
		}
		if !containsInt(row.origins, origins[i]) {
			row.origins = append(row.origins, origins[i])
		}
	}

	deduplicated := make([]UpdateItem[T], 0, len(result)+len(unkeyed))
	rowOrigins := make([][]int, 0, len(result)+len(unkeyed))
	for _, row := range result {
		rowOrigins = append(rowOrigins, row.origins)
		if !row.value.IsValid() {
			deduplicated = append(deduplicated, row.original)
			continue
		}
		var item T
		if reflect.TypeOf(row.original.Item).Kind() == reflect.Pointer {
			item = row.value.Addr().Interface().(T)
		} else {
			item = row.value.Interface().(T)
		}
		fields := make([]string, len(row.fields))
		for i, name := range row.fields {
			fields[i] = row.spelling[name]
		}
		deduplicated = append(deduplicated, UpdateItem[T]{Item: item, UpdateFields: fields})
	}
	return append(deduplicated, unkeyed...), append(rowOrigins, unkeyedOrigins...), nil
}

func containsInt(values []int, v int) bool {
	for _, x := range values {
		if x == v {
			return true
		}
	}
	return false
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
		// Length-prefixed, so ("a-b", "c") and ("a", "b-c") stay distinct keys.
		part := fmt.Sprint(fieldValue.Interface())
		keyParts = append(keyParts, strconv.Itoa(len(part))+":"+part)

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

// deduplicateRecords keeps one record per primary key, the last one given, at
// the position where that key first appeared. Records with a zero primary key
// get one generated and are kept as they are, in their own positions. The
// order is deterministic, so concurrent batches touching the same rows lock
// them in the same order.
func deduplicateRecords[T any](records []T, primaryKeyFields []reflect.StructField) []T {
	if len(primaryKeyFields) == 0 {
		// No primary key, cannot deduplicate
		return records
	}

	deduplicated := make([]T, 0, len(records))
	position := make(map[string]int)
	for _, record := range records {
		compositeKey, hasKey := buildCompositeKey(reflect.ValueOf(record), primaryKeyFields)
		if !hasKey {
			deduplicated = append(deduplicated, record)
			continue
		}
		if i, seen := position[compositeKey]; seen {
			deduplicated[i] = record // last value wins, first position kept
			continue
		}
		position[compositeKey] = len(deduplicated)
		deduplicated = append(deduplicated, record)
	}
	return deduplicated
}

func deduplicateChildRecords(instances []interface{}, modelType reflect.Type) []interface{} {
	info, err := parseModel(nil, modelType)
	if err != nil || len(info.primaryKeys) == 0 {
		// No primary key, cannot deduplicate
		return instances
	}
	return deduplicateRecords(instances, info.primaryKeyStructFields())
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

	// Every column must be a column of the model as GORM maps it, named by
	// its column name.
	var model T
	info, err := parseModel(nil, reflect.TypeOf(model))
	if err != nil {
		return nil, err
	}
	for _, col := range columns {
		if f := info.schema.LookUpField(col); f == nil || f.DBName != col {
			return nil, fmt.Errorf("column %s does not exist in model", col)
		}
	}

	return &SelectBatcher[T]{
		dbProvider: dbProvider,
		batcher:    batcher.NewBatchProcessorWithOptions(ctx, batchSelect[T](dbProvider, columns, config.Timeout), opts...),
		tableName:  info.schema.Table,
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

// processRows assigns each row to the select it answers, by the __index
// column, and sets the selected columns with GORM's own field setters, which
// convert driver values the way GORM's queries do: integers to booleans, bytes
// to numbers, strings and times, and NULL to the zero value.
func processRows[T any](rows *sql.Rows, fields []*schema.Field, allItems []SelectItem[T]) error {
	results := make([][]T, len(allItems))
	for i := range results {
		results[i] = make([]T, 0)
	}

	ctx := context.Background()
	for rows.Next() {
		values := make([]interface{}, len(fields)+1) // +1 for __index
		for i := range values {
			values[i] = new(interface{})
		}
		if err := rows.Scan(values...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		index, err := convertToInt(*values[0].(*interface{}))
		if err != nil {
			return fmt.Errorf("failed to parse index: %w", err)
		}
		if index < 0 || index >= len(results) {
			return fmt.Errorf("row index %d out of range for %d selects", index, len(results))
		}

		var result T
		rv := reflect.ValueOf(&result).Elem()
		if rv.Kind() == reflect.Pointer {
			rv.Set(reflect.New(rv.Type().Elem()))
			rv = rv.Elem()
		}
		for i, f := range fields {
			if err := f.Set(ctx, rv, *values[i+1].(*interface{})); err != nil {
				return fmt.Errorf("failed to set column %s: %w", f.DBName, err)
			}
		}
		results[index] = append(results[index], result)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read rows: %w", err)
	}

	for i, item := range allItems {
		*item.Results = results[i]
	}
	return nil
}

// selectIndexExpression is the placeholder that numbers each select of a
// batch, cast to an integer type the dialect supports. An unsized CHAR is one
// character on Postgres, which truncated indexes of 10 and above.
func selectIndexExpression(dialect string) string {
	if dialect == "mysql" {
		return "CAST(? AS SIGNED)"
	}
	return "CAST(? AS INTEGER)"
}

func batchSelect[T any](dbProvider DBProvider, columns []string, timeout int) func([][]SelectItem[T]) []error {
	var models modelCache
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

		var model T
		info, err := models.get(db, reflect.TypeOf(model))
		if err != nil {
			return batcher.RepeatErr(len(batches), err)
		}
		fields := make([]*schema.Field, len(columns))
		for i, col := range columns {
			if fields[i] = info.schema.LookUpField(col); fields[i] == nil {
				return batcher.RepeatErr(len(batches), fmt.Errorf("column %s does not exist in model", col))
			}
		}

		// Build the query
		var queryBuilder strings.Builder
		var args []interface{}

		dialectName := db.Dialector.Name()
		indexColumn := selectIndexExpression(dialectName) + " AS " + quoteIdentifier("__index", dialectName)

		for i, item := range allItems {
			if i > 0 {
				queryBuilder.WriteString(" UNION ALL ")
			}
			selectColumns := append([]string{indexColumn}, columns...)
			queryBuilder.WriteString(fmt.Sprintf("SELECT %s FROM %s WHERE %s",
				strings.Join(selectColumns, ", "),
				quoteIdentifier(info.schema.Table, dialectName),
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
			processingErr = processRows(rows, fields, allItems)
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

		// The lock wait timeout is a session variable, so it stays on the
		// pooled connection after this transaction. It is restored to the
		// session's previous value on every path out of the transaction,
		// before the connection goes back to the pool.
		restoreTimeout := func() {}
		if timeout > 0 && db.Dialector.Name() == "mysql" {
			var previous int
			if readErr := sessionDB.Raw("SELECT @@SESSION.innodb_lock_wait_timeout").Scan(&previous).Error; readErr != nil {
				sessionDB.Rollback()
				return fmt.Errorf("failed to read session timeout: %w", readErr)
			}
			if timeoutSetErr := sessionDB.Exec(fmt.Sprintf("SET SESSION innodb_lock_wait_timeout = %d", timeout)).Error; timeoutSetErr != nil {
				sessionDB.Rollback()
				return fmt.Errorf("failed to set session timeout: %w", timeoutSetErr)
			}
			restoreTimeout = func() {
				if resetErr := sessionDB.Exec(fmt.Sprintf("SET SESSION innodb_lock_wait_timeout = %d", previous)).Error; resetErr != nil {
					fmt.Printf("Warning: failed to reset session timeout: %v\n", resetErr)
				}
			}
		}

		// Execute the operation
		err = operation(sessionDB)
		if err != nil {
			restoreTimeout()
			sessionDB.Rollback()
			if !isDeadlockError(err) {
				return fmt.Errorf("operation failed: %w", err)
			}
			fmt.Printf("Error detected (retrying): %v\n", err)
			delayNextAttempt(attempt)
			lastErr = err
			continue
		}

		// Restore before committing, while the session is still ours.
		restoreTimeout()

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

	info, err := parseModel(tx, modelType)
	if err != nil {
		return clause.OnConflict{UpdateAll: true} // let GORM handle it
	}

	// If we have unique indexes, use the first one as conflict target. They
	// are sorted by name, so the target is the same on every batch.
	// This handles the common case where ID=0 (auto-increment) conflicts on unique index
	target := info.primaryKeys
	if len(info.uniqueIndexes) > 0 {
		target = info.uniqueIndexes[0]
	}
	if len(target) == 0 {
		return clause.OnConflict{UpdateAll: true} // let GORM handle it
	}
	conflictColumns := make([]clause.Column, len(target))
	for i, f := range target {
		conflictColumns[i] = clause.Column{Name: f.DBName}
	}
	return clause.OnConflict{
		Columns:   conflictColumns,
		UpdateAll: true,
	}
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
	// Column names with the naming strategy of the database being read.
	var model T
	info, err := ib.models.get(db, reflect.TypeOf(model))
	if err != nil {
		return err
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

		for _, uniqueIndex := range info.uniqueIndexes {
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

				// clause.Eq quotes the column, which may be a reserved word.
				query = query.Where(clause.Eq{Column: clause.Column{Name: field.DBName}, Value: fieldValue.Interface()})
			}

			if !allFieldsSet {
				continue
			}

			// Execute query
			result := query.First(&reloadedRecord)
			if result.Error != nil && !errors.Is(result.Error, gorm.ErrRecordNotFound) {
				// A database error says nothing about whether the row exists.
				return fmt.Errorf("reload record at index %d: %w", idx, result.Error)
			}
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
