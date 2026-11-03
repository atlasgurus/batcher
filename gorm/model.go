package gorm

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// modelInfo is what the batchers need to know about a model type, read from
// GORM's own schema parser, so table names, column names, primary keys,
// embedded structs and ignored fields follow exactly the rules GORM applies
// when it creates and queries the table.
type modelInfo struct {
	schema      *schema.Schema
	primaryKeys []*schema.Field
	// uniqueIndexes lists each unique index's fields, in index column order.
	// The indexes are sorted by name, so the first one is always the same.
	uniqueIndexes [][]*schema.Field
}

// constructionSchemas caches GORM's schemas parsed when a batcher is created,
// before any database handle is at hand, and constructionModels the results,
// so a model GORM only partly parses is parsed, and logged, once per type.
var (
	constructionSchemas sync.Map
	constructionModels  sync.Map // reflect.Type -> *modelInfo
)

// modelCache keeps one batcher's parse of its model with the naming strategy
// of the database it writes to, made on the batcher's first batch.
type modelCache struct {
	info atomic.Pointer[modelInfo]
}

func (c *modelCache) get(db *gorm.DB, modelType reflect.Type) (*modelInfo, error) {
	if info := c.info.Load(); info != nil {
		return info, nil
	}
	info, err := parseModel(db, modelType)
	if err != nil {
		return nil, err
	}
	c.info.Store(info)
	return info, nil
}

// modelValue returns a new zero value of the struct behind T, as GORM's
// parser wants it.
func modelValue(modelType reflect.Type) interface{} {
	for modelType.Kind() == reflect.Pointer {
		modelType = modelType.Elem()
	}
	return reflect.New(modelType).Interface()
}

// parseModel parses modelType with db's naming strategy, or with GORM's
// default naming strategy when db is nil.
func parseModel(db *gorm.DB, modelType reflect.Type) (*modelInfo, error) {
	if db == nil {
		if info, ok := constructionModels.Load(modelType); ok {
			return info.(*modelInfo), nil
		}
	}
	model := modelValue(modelType)
	var s *schema.Schema
	var err error
	if db != nil {
		stmt := &gorm.Statement{DB: db}
		err = stmt.Parse(model)
		s = stmt.Schema
	} else {
		s, err = schema.Parse(model, &constructionSchemas, schema.NamingStrategy{})
	}
	// GORM rejects a model whose relationships it cannot resolve, but returns
	// the schema with its table, columns and primary key already complete;
	// relationships are not something the batchers use.
	if s == nil || len(s.Fields) == 0 {
		if err == nil {
			err = fmt.Errorf("no fields")
		}
		return nil, fmt.Errorf("parse model %v: %w", modelType, err)
	}
	info := &modelInfo{
		schema:        s,
		primaryKeys:   s.PrimaryFields,
		uniqueIndexes: uniqueIndexesOf(s),
	}
	if db == nil {
		constructionModels.Store(modelType, info)
	}
	return info, nil
}

// primaryKeyStructFields returns the struct fields of the primary key, for
// reading key values off records with reflect.Value.FieldByName.
func (m *modelInfo) primaryKeyStructFields() []reflect.StructField {
	fields := make([]reflect.StructField, len(m.primaryKeys))
	for i, f := range m.primaryKeys {
		fields[i] = f.StructField
	}
	return fields
}

// uniqueIndexesOf collects the unique indexes GORM parses from index tags
// (uniqueIndex, index:name,unique, with priority ordering), plus the GORM v1
// unique_index tag and the unique field constraint, which GORM v2's index
// parser does not treat as indexes. Primary key fields are left out.
func uniqueIndexesOf(s *schema.Schema) [][]*schema.Field {
	byName := map[string][]*schema.Field{}
	for name, idx := range s.ParseIndexes() {
		if idx.Class != "UNIQUE" {
			continue
		}
		var fields []*schema.Field
		for _, opt := range idx.Fields {
			if opt.Field != nil && !opt.PrimaryKey {
				fields = append(fields, opt.Field)
			}
		}
		if len(fields) > 0 {
			byName["index:"+name] = fields
		}
	}
	for _, f := range s.Fields {
		if f.PrimaryKey || f.DBName == "" {
			continue
		}
		if name, ok := f.TagSettings["UNIQUE_INDEX"]; ok {
			if name == "" || name == "UNIQUE_INDEX" {
				name = f.Name
			}
			byName["unique_index:"+name] = append(byName["unique_index:"+name], f)
		}
		if f.Unique {
			byName["unique:"+f.Name] = []*schema.Field{f}
		}
	}

	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	indexes := make([][]*schema.Field, len(names))
	for i, name := range names {
		indexes[i] = byName[name]
	}
	return indexes
}

// fieldMapping translates the field names callers pass, struct or column
// names, for one model.
type fieldMapping struct {
	structToDb map[string]string
	dbToStruct map[string]string
	// updatable lists, in declaration order, the struct fields an update with
	// no field list writes: every column GORM may update except the primary
	// key.
	updatable []string
}

func createFieldMapping(s *schema.Schema) fieldMapping {
	mapping := fieldMapping{
		structToDb: make(map[string]string),
		dbToStruct: make(map[string]string),
	}
	for _, f := range s.Fields {
		if f.DBName == "" {
			continue // ignored with gorm:"-", or a relationship
		}
		mapping.structToDb[f.Name] = f.DBName
		mapping.dbToStruct[f.DBName] = f.Name
		if !f.PrimaryKey && f.Updatable {
			mapping.updatable = append(mapping.updatable, f.Name)
		}
	}
	return mapping
}

// allUpdatable returns a copy of updatable, so a caller may append to it.
func (m fieldMapping) allUpdatable() []string {
	return append([]string(nil), m.updatable...)
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
