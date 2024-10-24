//go:build !sqlite

package gorm

import gormv2 "gorm.io/gorm"

func getSQLiteDialector(sqlDB interface{}) gormv2.Dialector {
	panic("SQLite support not built in")
}
