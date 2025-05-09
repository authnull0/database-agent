package pkg

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq" // Import PostgreSQL driver
)

// DBConfig holds the database connection configuration

func ConnectToDB(config DBConfig) (*sql.DB, error) {
	var dsn string

	// PostgreSQL connection string format
	dsn = fmt.Sprintf("host=%s port=%s user=%s password=%s sslmode=disable",
		config.Host, config.Port, config.User, config.Password)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	// Test the connection
	err = db.Ping()
	if err != nil {
		log.Printf("Cannot ping database: %v", err)
		return nil, err
	}

	log.Printf("Successfully connected to PostgreSQL server at %s:%s", config.Host, config.Port)
	return db, nil
}

func ConnectToProxysqlDB(config DBConfig) (*sql.DB, error) {
	// Another approach - pass the username as a parameter instead of in the main DSN
	dsn := fmt.Sprintf(":%s@tcp(%s:%s)/?user=%s",
		"admin",      // password
		"127.0.0.1",  // host
		"6032",       // port
		"admin,test") // username as a parameter

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error opening DB connection: %v", err)
	}

	// Test the connection
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("error pinging ProxySQL: %v", err)
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
		log.Printf("Error querying databases: %v", err)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			log.Printf("Error scanning database name: %v", err)
			return err
		}

		// Skip system databases
		if isSystemDatabase(dbName, config.DBType) {
			log.Printf("Skipping system database: %s", dbName)
			continue
		}

		databases = append(databases, dbName)
		log.Printf("Processing database: %s", dbName)

		// Register the database agent and get its ID
		instanceId := RegisterAgent(db, dbName, config)
		log.Printf("Registered agent for database %s with Instance ID: %s", dbName, instanceId)

		if instanceId == "" {
			log.Printf("Failed to register agent for the database %s", dbName)
			continue // Skip this database if registration failed
		}

		err = LastActive(instanceId, db, dbName, config)
		if err != nil {
			log.Printf("Failed to get last active time of the database %s: %v", dbName, err)
		}
		log.Println("Last Active Time call completed")

		// Fetch database status
		err = FetchDatabaseStatus(db, dbName, config, instanceId)
		if err != nil {
			log.Printf("Failed to fetch status for database %s: %v", dbName, err)
		}
		log.Println("FetchDatabaseStatus completed")

		// Fetch tables and privileges for each database
		err = FetchTablePrivileges(db, dbName, config, instanceId)
		if err != nil {
			log.Printf("Failed to fetch table privileges for database %s: %v", dbName, err)
		}
		log.Println("FetchTablePrivileges completed")

		// Fetch table and column names with the main database connection for querying db_synchronization
		err = FetchTables(db, dbName, config, instanceId)
		if err != nil {
			log.Printf("Failed to fetch tables and columns for %s: %v", dbName, err)
		}
		log.Println("FetchTables completed")

		err = PollCheckoutJob(db, dbName, config)
		if err != nil {
			log.Printf("Failed to poll checkout job: %v", err)
		}
		log.Println("All operations completed for database:", dbName)
	}

	return nil
}
