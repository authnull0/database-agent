package pkg

import (
	"database/sql"
	"fmt"
	"log"
)

func ConnectToDB(config DBConfig) (*sql.DB, error) {
	var dsn string

	// PostgreSQL connection string format
	dsn = fmt.Sprintf("host=%s port=%s user=%s password=%s sslmode=disable",
		config.Host, config.Port, config.User, config.Password)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func ConnectToProxysqlDB(config DBConfig) (*sql.DB, error) {
	var dsn string
	dsn = fmt.Sprintf("admin,test:%s@tcp(%s:%s)/", "admin", "127.0.0.1", "6032")

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
		"postgres": {"postgres", "template0", "template1", "information_schema"},
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
func FetchDatabaseDetails(db *sql.DB, config DBConfig) error {
	var databases []string

	// PostgreSQL query to list databases
	databasesQuery := "SELECT datname FROM pg_database WHERE datistemplate = false"

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

		// Register the database agent
		instanceId := RegisterAgent(db, dbName, config)
		log.Default().Printf("Instance Id: %v", instanceId)

		if err != nil {
			log.Printf("Failed to register agent for the database %s: %v", dbName, err)
		}
		log.Println("Register Agent Ended")

		// Last Active Time Function call
		err = LastActive(instanceId, db, dbName, config)

		if err != nil {
			log.Printf("Failed to get last active time of the database %s: %v", dbName, err)
		}
		log.Println("Last Active Time call Ended")

		// Fetch database status
		err = FetchDatabaseStatus(db, dbName, config, instanceId)

		if err != nil {
			log.Printf("Failed to fetch status for database %s: %v", dbName, err)
		}
		log.Println("FetchDatabaseStatus Ended")

		// Fetch tables and privileges for each database
		err = FetchTablePrivileges(db, dbName, config, instanceId)
		if err != nil {
			log.Printf("Failed to fetch table privileges for database %s: %v", dbName, err)
		}
		log.Println("FetchDatabasePrivileges Ended")

		// Fetch table and column names
		err = FetchTables(db, dbName, config, instanceId)
		if err != nil {
			log.Printf("Failed to fetch tables and columns: %v", err)
		}
		log.Println("FetchTables Ended")

		err = PollCheckoutJob(db, dbName, config)
		if err != nil {
			log.Printf("Failed to poll checkout job: %v", err)
		}
	}

	return nil
}
