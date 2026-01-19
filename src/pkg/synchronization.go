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

func ConnectToProxysqlDB(config DBConfig) (*sql.DB, error) {
	// Very simple, direct MySQL connection to ProxySQL
	dsn := "admin,test:admin@tcp(127.0.0.1:6032)/"

	// Explicitly use "mysql" driver for ProxySQL
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error opening MySQL connection: %v", err)
	}

	// Test the connection with a simple ping
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("error pinging ProxySQL: %v", err)
	}

	log.Println("Successfully connected to ProxySQL")
	return db, nil
}

// InitializeProxySQL runs one-time setup queries when the agent starts
// For legacy single-host mode, it registers the default host
func InitializeProxySQL(config DBConfig) error {
	proxysqlDB, err := ConnectToProxysqlDB(config)
	if err != nil {
		return fmt.Errorf("failed to connect to ProxySQL for initialization: %w", err)
	}
	defer proxysqlDB.Close()

	// One-time initialization queries for base configuration
	initQueries := []string{
		"SET pgsql-authentication_method = 1",
		"LOAD PGSQL VARIABLES TO RUNTIME",
		"SAVE PGSQL VARIABLES TO DISK",
	}

	for _, query := range initQueries {
		_, err := proxysqlDB.Exec(query)
		if err != nil {
			log.Printf("ProxySQL init query warning: %s - %v", query, err)
		} else {
			log.Printf("ProxySQL init query success: %s", query)
		}
	}

	// Legacy single-host mode: register default host with hostgroup 0
	// In multi-host mode, hosts are registered dynamically via RegisterHostInProxySQL
	if config.Host != "" && config.AgentVMIP == "" {
		err = RegisterHostInProxySQL(config, config.Host, 0, 5432)
		if err != nil {
			log.Printf("Warning: Failed to register default host in ProxySQL: %v", err)
		}
	}

	log.Println("ProxySQL initialization completed")
	return nil
}

// RegisterHostInProxySQL registers a database host in ProxySQL pgsql_servers table
// hostgroupID determines which hostgroup this server belongs to for routing
func RegisterHostInProxySQL(config DBConfig, hostname string, hostgroupID int, port int) error {
	proxysqlDB, err := ConnectToProxysqlDB(config)
	if err != nil {
		return fmt.Errorf("failed to connect to ProxySQL: %w", err)
	}
	defer proxysqlDB.Close()

	// Check if server already exists in this hostgroup
	var count int
	checkQuery := fmt.Sprintf("SELECT COUNT(*) FROM pgsql_servers WHERE hostgroup_id = %d AND hostname = '%s' AND port = %d", hostgroupID, hostname, port)
	err = proxysqlDB.QueryRow(checkQuery).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check existing server: %w", err)
	}

	if count == 0 {
		// Insert new server
		insertQuery := fmt.Sprintf("INSERT INTO pgsql_servers (hostgroup_id, hostname, port) VALUES (%d, '%s', %d)", hostgroupID, hostname, port)
		_, err = proxysqlDB.Exec(insertQuery)
		if err != nil {
			return fmt.Errorf("failed to insert server: %w", err)
		}
		log.Printf("Registered new ProxySQL server: hostgroup=%d, hostname=%s, port=%d", hostgroupID, hostname, port)
	} else {
		log.Printf("ProxySQL server already exists: hostgroup=%d, hostname=%s, port=%d", hostgroupID, hostname, port)
	}

	// Load and save to persist changes
	_, err = proxysqlDB.Exec("LOAD PGSQL SERVERS TO RUNTIME")
	if err != nil {
		return fmt.Errorf("failed to load servers to runtime: %w", err)
	}
	_, err = proxysqlDB.Exec("SAVE PGSQL SERVERS TO DISK")
	if err != nil {
		return fmt.Errorf("failed to save servers to disk: %w", err)
	}

	return nil
}

// RemoveHostFromProxySQL removes a database host from ProxySQL pgsql_servers table
func RemoveHostFromProxySQL(config DBConfig, hostname string, hostgroupID int, port int) error {
	proxysqlDB, err := ConnectToProxysqlDB(config)
	if err != nil {
		return fmt.Errorf("failed to connect to ProxySQL: %w", err)
	}
	defer proxysqlDB.Close()

	deleteQuery := fmt.Sprintf("DELETE FROM pgsql_servers WHERE hostgroup_id = %d AND hostname = '%s' AND port = %d", hostgroupID, hostname, port)
	_, err = proxysqlDB.Exec(deleteQuery)
	if err != nil {
		return fmt.Errorf("failed to delete server: %w", err)
	}

	log.Printf("Removed ProxySQL server: hostgroup=%d, hostname=%s, port=%d", hostgroupID, hostname, port)

	// Load and save to persist changes
	_, err = proxysqlDB.Exec("LOAD PGSQL SERVERS TO RUNTIME")
	if err != nil {
		return fmt.Errorf("failed to load servers to runtime: %w", err)
	}
	_, err = proxysqlDB.Exec("SAVE PGSQL SERVERS TO DISK")
	if err != nil {
		return fmt.Errorf("failed to save servers to disk: %w", err)
	}

	return nil
}

// ListProxySQLServers lists all registered servers in ProxySQL
func ListProxySQLServers(config DBConfig) ([]map[string]interface{}, error) {
	proxysqlDB, err := ConnectToProxysqlDB(config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ProxySQL: %w", err)
	}
	defer proxysqlDB.Close()

	rows, err := proxysqlDB.Query("SELECT hostgroup_id, hostname, port, status, weight FROM pgsql_servers")
	if err != nil {
		return nil, fmt.Errorf("failed to query servers: %w", err)
	}
	defer rows.Close()

	var servers []map[string]interface{}
	for rows.Next() {
		var hostgroupID int
		var hostname string
		var port int
		var status string
		var weight int
		err = rows.Scan(&hostgroupID, &hostname, &port, &status, &weight)
		if err != nil {
			continue
		}
		servers = append(servers, map[string]interface{}{
			"hostgroup_id": hostgroupID,
			"hostname":     hostname,
			"port":         port,
			"status":       status,
			"weight":       weight,
		})
	}

	return servers, nil
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
