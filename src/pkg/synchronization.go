package pkg

import (
	"database/sql"
	"fmt"
)

var config DBConfig

func ConnectToDB(config DBConfig) (*sql.DB, error) {
	var dsn string

	switch config.DBType {
	case "mysql":
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/", config.DBUserName, config.DBPassword, config.DBHost, config.DBPort)
	case "Postgres":
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%s/?sslmode=disable", config.DBUserName, config.DBPassword, config.DBHost, config.DBPort)
	case "MSSQL":
		dsn = fmt.Sprintf("sqlserver://%s:%s@%s:%s", config.DBUserName, config.DBPassword, config.DBHost, config.DBPort)
	case "Oracle":
		dsn = fmt.Sprintf("oracle://%s:%s@%s:%s", config.DBUserName, config.DBPassword, config.DBHost, config.DBPort)
	default:
		return nil, fmt.Errorf("unsupported database type")
	}

	db, err := sql.Open(config.DBType, dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}
