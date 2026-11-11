package pkg

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	_ "github.com/go-sql-driver/mysql" // MySQL targets
	_ "github.com/lib/pq"              // Import PostgreSQL driver
)

// ConnectToDB connects to the target as the agent: a PostgreSQL target in its default 'postgres'
// database, a MySQL target in none (discovery reads information_schema).
func ConnectToDB(config DBConfig) (*sql.DB, error) {
	if isMySQL(config) {
		db, err := sql.Open("mysql", mysqlDSN(config, ""))
		if err != nil {
			return nil, err
		}
		if err := db.Ping(); err != nil {
			db.Close()
			log.Printf("Cannot ping database: %v", err)
			return nil, err
		}
		log.Printf("Successfully connected to MySQL server at %s:%d", config.Host, targetPort(config))
		return db, nil
	}

	var dsn string

	// PostgreSQL connection string format
	// Connect to the default 'postgres' database initially
	dsn = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=postgres sslmode=disable",
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

var mysqlDirectAccessNote sync.Once

// FetchDatabaseDetails fetches the database names, statuses, and table privileges
// skipping system databases
func FetchDatabaseDetails(db *sql.DB, config DBConfig) error {
	log.Println("Fetching database details...")
	log.Default().Printf("%v", config)

	databases, err := listDatabases(db, config)
	if err != nil {
		return err
	}

	// Can anything but the proxy log in as a managed role? Once per sync: pg_hba.conf is per server.
	// pg_hba.conf is PostgreSQL's: a MySQL target has no such check (yet), and reports none.
	var directAccess *DirectAccessReport
	if isMySQL(config) {
		mysqlDirectAccessNote.Do(func() {
			log.Printf("Direct-access check skipped for MySQL targets: it reads pg_hba.conf and is PostgreSQL-only")
		})
	} else {
		directAccess = CheckDirectAccessForHost(db, config, databases)
	}

	for _, dbName := range databases {
		log.Printf("Processing database: %s", dbName)

		// Register the database agent and get its ID
		instanceId, agentStatus := RegisterAgent(db, dbName, config)
		log.Printf("Registered agent for database %s with Instance ID: %s and Agent Status: %s", dbName, instanceId, agentStatus)
		// If the agent is marked as deleted, skip further processing for this database
		if agentStatus == "DELETED" {
			log.Printf("Agent for database %s is deleted, skipping further processing", dbName)
			continue
		}

		if instanceId == "" {
			log.Printf("Failed to register agent for the database %s", dbName)
			continue // Skip this database if registration failed
		}

		err = LastActive(instanceId, db, dbName, config, directAccess)
		if err != nil {
			log.Printf("Failed to get last active time of the database %s: %v", dbName, err)
		}
		log.Println("Last Active Time call completed")

		// Fetch database status
		hostGroupId, databaseStatus, err := FetchDatabaseStatus(db, dbName, config, instanceId)
		if err != nil {
			log.Printf("Failed to fetch status for database %s: %v", dbName, err)
		}
		log.Println("FetchDatabaseStatus completed for host group Id:", hostGroupId)
		log.Println("Database Status from API:", databaseStatus)

		// If the database is marked as deleted in the API, skip further processing
		if databaseStatus == "DELETED" {
			log.Printf("Database %s is marked as deleted in the API, skipping further processing", dbName)
			continue
		}

		// The configured port, not a literal 5432: a database on another port was registered at
		// the wrong address and never matched. Without one, the engine's default.
		dbPort := targetPort(config)
		err = UpdateHostInProxySQL(config, config.Host, hostGroupId, dbPort)
		if err != nil {
			log.Printf("Failed to update host in ProxySQL for database %s: %v", dbName, err)
		}
		log.Println("UpdateHostInProxySQL completed")
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

		// Policy roles are no longer polled for here: the backend's reconcile sends apply_policy_role /
		// drop_policy_role jobs through ProcessVaultJobs (authnull-service docs/DB-POLICY-ROLES.md).
		log.Println("All operations completed for database:", dbName)
	}

	return nil
}
