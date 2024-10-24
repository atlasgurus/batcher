package gorm

import (
	"fmt"

	gormv1 "github.com/jinzhu/gorm"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlserver"
	gormv2 "gorm.io/gorm"
)

// GormV1ToV2Adapter adapts a GORM v1 connection to a GORM v2 connection
func GormV1ToV2Adapter(v1DB *gormv1.DB) (*gormv2.DB, error) {
	dialect := v1DB.Dialect().GetName()
	sqlDB := v1DB.DB()

	var dialector gormv2.Dialector

	switch dialect {
	case "mysql":
		dialector = mysql.New(mysql.Config{
			Conn: sqlDB,
		})
	case "postgres":
		dialector = postgres.New(postgres.Config{
			Conn: sqlDB,
		})
	case "sqlite3":
		dialector = getSQLiteDialector(sqlDB)
	case "mssql":
		dialector = sqlserver.New(sqlserver.Config{
			Conn: sqlDB,
		})
	default:
		return nil, fmt.Errorf("unsupported dialect: %s", dialect)
	}

	return gormv2.Open(dialector, &gormv2.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
		ConnPool:               sqlDB, // This ensures the same connection pool is used
	})
}
