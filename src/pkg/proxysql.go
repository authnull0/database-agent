package pkg

import (
	"database/sql"
	"fmt"
	"log"
)

// InitializeProxySQL runs one-time setup queries when the agent starts
// For legacy single-host mode, it registers the default host
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
func InitializeProxySQL(config DBConfig, hostgroup int) error {
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
	if config.Host != "" {
		err = RegisterHostInProxySQL(config, config.Host, hostgroup, 5432)
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

func UpdateHostInProxySQL(config DBConfig, hostname string, hostgroupID int, port int) error {
	proxysqlDB, err := ConnectToProxysqlDB(config)
	if err != nil {
		return fmt.Errorf("failed to connect to ProxySQL: %w", err)
	}
	defer proxysqlDB.Close()

	// Check if server already exists with host group id 0
	var count int
	checkQuery := fmt.Sprintf("SELECT COUNT(*) FROM pgsql_servers WHERE hostgroup_id = %d AND hostname = '%s' AND port = %d", 0, hostname, port)
	err = proxysqlDB.QueryRow(checkQuery).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check existing server: %w", err)
	}

	if count != 0 {

		log.Printf("ProxySQL server already exists: hostgroup=%d, hostname=%s, port=%d", hostgroupID, hostname, port)
		log.Default().Printf("Update the server details ")
		// Update existing server details
		updateQuery := fmt.Sprintf("UPDATE pgsql_servers SET hostgroup_id = %d, port = %d WHERE hostname = '%s'", hostgroupID, port, hostname)
		_, err = proxysqlDB.Exec(updateQuery)

		// Load and save to persist changes
		_, err = proxysqlDB.Exec("LOAD PGSQL SERVERS TO RUNTIME")
		if err != nil {
			return fmt.Errorf("failed to load servers to runtime: %w", err)
		}
		_, err = proxysqlDB.Exec("SAVE PGSQL SERVERS TO DISK")
		if err != nil {
			return fmt.Errorf("failed to save servers to disk: %w", err)
		}
	} else {
		log.Printf("No Need to update ProxySQL server: hostgroup=%d, hostname=%s, port=%d", hostgroupID, hostname, port)
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

func ConfigureProxySQL(db Databases, password string, hostgroup int) error {
	admin, err := sql.Open(
		"mysql",
		"admin:admin@tcp(127.0.0.1:6032)/",
	)
	if err != nil {
		return err
	}
	defer admin.Close()

	// Insert server
	_, _ = admin.Exec(`
		INSERT IGNORE INTO pgsql_servers
		(hostgroup_id, hostname, port)
		VALUES (?, ?, ?)`,
		hostgroup, db.Host, db.Port,
	)

	// Insert user
	_, _ = admin.Exec(`
		INSERT INTO pgsql_users
		(username, password, active, default_hostgroup,
		 transaction_persistent, backend, frontend)
		VALUES (?, ?, 1, ?, 1, 1, 1)
		ON DUPLICATE KEY UPDATE
		  password = VALUES(password),
		  default_hostgroup = VALUES(default_hostgroup),
		  active = 1`,
		db.Username, password, hostgroup,
	)

	// Apply changes
	queries := []string{
		"LOAD PGSQL SERVERS TO RUNTIME",
		"SAVE PGSQL SERVERS TO DISK",
		"LOAD PGSQL USERS TO RUNTIME",
		"SAVE PGSQL USERS TO DISK",
	}

	for _, q := range queries {
		if _, err := admin.Exec(q); err != nil {
			return err
		}
	}

	log.Printf("ProxySQL configured for %s (HG=%d)", db.Host, hostgroup)
	return nil
}
