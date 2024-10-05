package gorm

import (
	"context"
	"fmt"
	"math/rand"
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

const (
	maxRetries = 3
	baseDelay  = 100 * time.Millisecond
)

func batchInsert[T any](dbProvider DBProvider) func([][]T) []error {
	return func(batches [][]T) []error {
		if len(batches) == 0 {
			return nil
		}

		db, err := dbProvider()
		if err != nil {
			return batcher.RepeatErr(len(batches), fmt.Errorf("failed to get database connection: %w", err))
		}

		var allRecords []T
		for _, batch := range batches {
			allRecords = append(allRecords, batch...)
		}

		if len(allRecords) == 0 {
			return batcher.RepeatErr(len(batches), nil)
		}

		var lastErr error
		for attempt := 0; attempt < maxRetries; attempt++ {
			err = db.Transaction(func(tx *gorm.DB) error {
				return tx.Clauses(clause.OnConflict{
					UpdateAll: true,
				}).CreateInBatches(allRecords, len(allRecords)).Error
			})

			if err == nil {
				return batcher.RepeatErr(len(batches), nil)
			}

			if !isDeadlockError(err) {
				lastErr = fmt.Errorf("failed to upsert records: %w", err)
				break
			}

			delay := baseDelay * time.Duration(1<<uint(attempt)) * time.Duration(1+rand.Intn(100)) / 100
			time.Sleep(delay)
		}

		if lastErr != nil {
			return batcher.RepeatErr(len(batches), lastErr)
		}

		return batcher.RepeatErr(len(batches), fmt.Errorf("failed to upsert records after %d retries", maxRetries))
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

		var lastErr error
		for attempt := 0; attempt < maxRetries; attempt++ {
			err = db.Transaction(func(tx *gorm.DB) error {
				mapping, err := createFieldMapping(allUpdateItems[0].Item)
				if err != nil {
					return err
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
							return getFieldErr
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
					return fmt.Errorf("no fields to update")
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

				return tx.Exec(queryBuilder.String(), allValues...).Error
			})

			if err == nil {
				return batcher.RepeatErr(len(batches), nil)
			}

			if !isDeadlockError(err) {
				lastErr = fmt.Errorf("failed to update records: %w", err)
				break
			}

			delay := baseDelay * time.Duration(1<<uint(attempt)) * time.Duration(1+rand.Intn(100)) / 100
			time.Sleep(delay)
		}

		if lastErr != nil {
			return batcher.RepeatErr(len(batches), lastErr)
		}

		return batcher.RepeatErr(len(batches), fmt.Errorf("failed to update records after %d retries", maxRetries))
	}
}

func isDeadlockError(err error) bool {
	return strings.Contains(err.Error(), "Deadlock found when trying to get lock")
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

// SelectBatcher is a GORM batcher for batch selects
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

// NewSelectBatcher creates a new GORM select batcher
func NewSelectBatcher[T any](dbProvider DBProvider, maxBatchSize int, maxWaitTime time.Duration, ctx context.Context, columns []string) (*SelectBatcher[T], error) {
	db, err := dbProvider()
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %w", err)
	}

	var model T
	stmt := &gorm.Statement{DB: db}
	err = stmt.Parse(&model)
	if err != nil {
		return nil, fmt.Errorf("failed to parse model for table name: %w", err)
	}

	return &SelectBatcher[T]{
		dbProvider: dbProvider,
		batcher:    batcher.NewBatchProcessor(maxBatchSize, maxWaitTime, ctx, batchSelect[T](dbProvider, stmt.Table, columns)),
		tableName:  stmt.Table,
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

func batchSelect[T any](dbProvider DBProvider, tableName string, columns []string) func([][]SelectItem[T]) []error {
	return func(batches [][]SelectItem[T]) []error {
		if len(batches) == 0 {
			return nil
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
		var queryParts []string
		var args []interface{}

		for i, item := range allItems {
			selectColumns := append([]string{fmt.Sprintf("%d AS __index", i)}, columns...)
			queryPart := fmt.Sprintf("(SELECT %s FROM %s WHERE %s)",
				strings.Join(selectColumns, ", "),
				tableName,
				item.Condition)
			queryParts = append(queryParts, queryPart)
			args = append(args, item.Args...)
		}

		query := strings.Join(queryParts, " UNION ALL ")
		query += " ORDER BY __index"

		// Execute the query
		rows, err := db.Raw(query, args...).Rows()
		if err != nil {
			return batcher.RepeatErr(len(batches), fmt.Errorf("failed to execute select query: %w", err))
		}
		defer rows.Close()

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
				return batcher.RepeatErr(len(batches), fmt.Errorf("failed to scan row: %w", err))
			}

			// Get the index
			index := int((*values[0].(*interface{})).(int64))

			// Create a new instance of T and scan into it
			var result T
			if err := scanIntoStruct(&result, columns, values[1:]); err != nil {
				return batcher.RepeatErr(len(batches), fmt.Errorf("failed to scan into struct: %w", err))
			}

			// Append the result to the corresponding slice
			results[index] = append(results[index], result)
		}

		// Assign results back to the original items
		for i, item := range allItems {
			*item.Results = results[i]
		}

		return batcher.RepeatErr(len(batches), nil)
	}
}

func scanIntoStruct(result interface{}, columns []string, values []interface{}) error {
	resultValue := reflect.ValueOf(result).Elem()
	resultType := resultValue.Type()

	for i, column := range columns {
		value := reflect.ValueOf(values[i]).Elem().Interface()

		field := resultValue.FieldByNameFunc(func(name string) bool {
			field, _ := resultType.FieldByName(name)
			dbName := field.Tag.Get("gorm")
			if dbName == column {
				return true
			}
			return strings.EqualFold(name, column) || strings.EqualFold(toSnakeCase(name), column)
		})

		if field.IsValid() && field.CanSet() {

			switch field.Kind() {
			case reflect.String:
				switch v := value.(type) {
				case []uint8:
					field.SetString(string(v))
				case string:
					field.SetString(v)
				default:
					return fmt.Errorf("failed to set string field %s: unexpected type %T", column, value)
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				switch v := value.(type) {
				case int64:
					field.SetUint(uint64(v))
				case uint64:
					field.SetUint(v)
				default:
					return fmt.Errorf("failed to set uint field %s: unexpected type %T", column, value)
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				switch v := value.(type) {
				case int64:
					field.SetInt(v)
				case int:
					field.SetInt(int64(v))
				default:
					return fmt.Errorf("failed to set int field %s: unexpected type %T", column, value)
				}
			case reflect.Float32, reflect.Float64:
				v, ok := value.(float64)
				if !ok {
					return fmt.Errorf("failed to set float field %s: unexpected type %T", column, value)
				}
				field.SetFloat(v)
			case reflect.Bool:
				v, ok := value.(bool)
				if !ok {
					return fmt.Errorf("failed to set bool field %s: unexpected type %T", column, value)
				}
				field.SetBool(v)
			default:
				return fmt.Errorf("unsupported field type for %s: %v", column, field.Kind())
			}
		} else {
			return fmt.Errorf("Field %s is not valid or cannot be set\n", column)
		}
	}
	return nil
}
