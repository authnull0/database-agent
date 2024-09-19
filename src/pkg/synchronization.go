package pkg

import (
	"database/sql"
	"fmt"
	"log"
)

//var config DBConfig

func ConnectToDB(config DBConfig, dbUserName string, dbPassword string, dbHost string) (*sql.DB, error) {
	var dsn string

	dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/", dbUserName, dbPassword, dbHost, config.Port)

	db, err := sql.Open(config.DBType, dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// checks if a given database is a system default database
func isSystemDatabase(dbName, dbType string) bool {
	systemDatabases := map[string][]string{
		"mysql":    {"mysql", "information_schema", "performance_schema", "sys"},
		"Postgres": {"postgres", "template0", "template1"},
		"MSSQL":    {"master", "tempdb", "model", "msdb"},
		"Oracle":   {"SYSTEM", "SYSAUX"},
	}

	// Get the list of system default databases
	if systemDbs, ok := systemDatabases[dbType]; ok {
		for _, systemDb := range systemDbs {
			if dbName == systemDb {
				return true
			}
		}
	}
	return false
}

// FetchDatabaseDetails fetches the database names, statuses, and table privileges
// skipping system databases
func FetchDatabaseDetails(db *sql.DB, config DBConfig, dbHost string, apiKey string) error {
	var databases []string

	// Fetch database names
	databasesQuery := ""

	databasesQuery = "SHOW DATABASES"

	// Execute query for database names
	rows, err := db.Query(databasesQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return err
		}

		// Skip system databases
		if isSystemDatabase(dbName, config.DBType) {
			log.Printf("Skipping system database: %s", dbName)
			continue
		}

		databases = append(databases, dbName)

		// Fetch database status
		err = FetchDatabaseStatus(db, dbName, config, dbHost, apiKey)

		if err != nil {
			log.Printf("Failed to fetch status for database %s: %v", dbName, err)
		}
		log.Println("FetchDatabaseStatus Ended")

		// Fetch tables and privileges for each database
		err = FetchTablePrivileges(db, dbName, config)
		if err != nil {
			log.Printf("Failed to fetch table privileges for database %s: %v", dbName, err)
		}
		log.Println("FetchDatabasePrivileges Ended")
	}

	return nil
}
