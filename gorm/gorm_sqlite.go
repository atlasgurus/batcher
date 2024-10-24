//go:build sqlite

package gorm

import "gorm.io/driver/sqlite"
import gormv2 "gorm.io/gorm"

func getSQLiteDialector(sqlDB interface{}) gormv2.Dialector {
	return &sqlite.Dialector{
		Conn: sqlDB,
	}
}
