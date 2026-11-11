package pkg

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// InitializeProxySQL runs one-time setup queries when the agent starts
// For legacy single-host mode, it registers the default host
func ConnectToProxysqlDB(config DBConfig) (*sql.DB, error) {
	// The admin login comes from db.env (proxyadmin.go), never the source.
	warnDefaultAdmin(config)
	db, err := sql.Open("mysql", proxysqlAdminDSN(config))
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

// proxyTables are ProxySQL's tables and the keyword of its LOAD/SAVE commands for an engine:
// pgsql_users / pgsql_servers / PGSQL for PostgreSQL, mysql_users / mysql_servers / MYSQL for MySQL.
// The two families are separate, so a MySQL and a PostgreSQL target on one IP never share a row.
type proxyTables struct {
	users, servers, keyword string
}

func proxyTablesFor(engine string) proxyTables {
	if engine == EngineMySQL {
		return proxyTables{users: "mysql_users", servers: "mysql_servers", keyword: "MYSQL"}
	}
	return proxyTables{users: "pgsql_users", servers: "pgsql_servers", keyword: "PGSQL"}
}

// loadAndSave makes what changed in a table live, and persists it.
func loadAndSave(proxysqlDB *sql.DB, keyword, what string) error {
	if _, err := proxysqlDB.Exec(fmt.Sprintf("LOAD %s %s TO RUNTIME", keyword, what)); err != nil {
		return fmt.Errorf("failed to load %s to runtime: %w", strings.ToLower(what), err)
	}
	if _, err := proxysqlDB.Exec(fmt.Sprintf("SAVE %s %s TO DISK", keyword, what)); err != nil {
		return fmt.Errorf("failed to save %s to disk: %w", strings.ToLower(what), err)
	}
	return nil
}

// InitializeProxySQL runs one-time setup queries when the agent starts
// For legacy single-host mode, it registers the default host
func InitializeProxySQL(config DBConfig, hostgroup int) error {
	proxysqlDB, err := ConnectToProxysqlDB(config)
	if err != nil {
		return fmt.Errorf("failed to connect to ProxySQL for initialization: %w", err)
	}
	defer proxysqlDB.Close()
	initializeProxySQL(proxysqlDB, config, hostgroup)
	return nil
}

// initializeProxySQL sets PostgreSQL's clear-text auth (the MFA hook's path) and registers the
// target, at its own port, among its engine's servers.
func initializeProxySQL(proxysqlDB *sql.DB, config DBConfig, hostgroup int) {
	// One-time initialization queries for base configuration
	var initQueries []string
	if !isMySQL(config) {
		initQueries = []string{
			"SET pgsql-authentication_method = 1",
			"LOAD PGSQL VARIABLES TO RUNTIME",
			"SAVE PGSQL VARIABLES TO DISK",
		}
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
		if err := registerHost(proxysqlDB, config, config.Host, hostgroup, targetPort(config)); err != nil {
			log.Printf("Warning: Failed to register default host in ProxySQL: %v", err)
		}
	}

	log.Println("ProxySQL initialization completed")
}

// RegisterHostInProxySQL registers a database host among ProxySQL's servers for the target's
// engine (pgsql_servers or mysql_servers). hostgroupID determines which hostgroup this server
// belongs to for routing
func RegisterHostInProxySQL(config DBConfig, hostname string, hostgroupID int, port int) error {
	proxysqlDB, err := ConnectToProxysqlDB(config)
	if err != nil {
		return fmt.Errorf("failed to connect to ProxySQL: %w", err)
	}
	defer proxysqlDB.Close()
	return registerHost(proxysqlDB, config, hostname, hostgroupID, port)
}

func registerHost(proxysqlDB *sql.DB, config DBConfig, hostname string, hostgroupID int, port int) error {
	t := proxyTablesFor(Engine(config.DBType))
	host, err := sqliteLiteral(hostname)
	if err != nil {
		return err
	}
	// Check if server already exists in this hostgroup
	var count int
	checkQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE hostname = %s AND port = %d", t.servers, host, port)
	err = proxysqlDB.QueryRow(checkQuery).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check existing server: %w", err)
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (hostgroup_id, hostname, port) VALUES (%d, %s, %d)", t.servers, hostgroupID, host, port)
	if count == 0 {
		// Insert new server
		_, err = proxysqlDB.Exec(insertQuery)
		if err != nil {
			return fmt.Errorf("failed to insert server: %w", err)
		}
		log.Printf("Registered new ProxySQL server: hostgroup=%d, hostname=%s, port=%d", hostgroupID, hostname, port)
	} else {
		//Delete existing server with host group id 0 if exists
		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE hostname = %s AND port = %d", t.servers, host, port)
		_, err = proxysqlDB.Exec(deleteQuery)
		if err != nil {
			return fmt.Errorf("failed to delete server: %w", err)
		}
		// Insert server with new host group id
		_, err = proxysqlDB.Exec(insertQuery)
		if err != nil {
			return fmt.Errorf("failed to insert server: %w", err)
		}
		log.Printf("ProxySQL server registered after deleting the existing one: hostgroup=%d, hostname=%s, port=%d", hostgroupID, hostname, port)
	}

	// Load and save to persist changes
	return loadAndSave(proxysqlDB, t.keyword, "SERVERS")
}

// UpdateHostInProxySQL makes ProxySQL match what the backend says about this host. It runs on
// every sync cycle, so it must ADD a missing server, not only update one that is there.
//
// It used to do nothing when the server was absent ("No Need to update"), which left the only
// insert in InitializeProxySQL at agent start. The installer starts the agent BEFORE it installs
// ProxySQL, so that insert always failed on a fresh install and was never retried -- and the same
// happened after any ProxySQL reinstall or reset. Every user then routed to an empty hostgroup.
//
// The same start-up-only problem applied to the clear-text auth setting, so it is re-asserted here
// (for PostgreSQL: it is a pgsql- variable). A MySQL target goes to mysql_servers.
func UpdateHostInProxySQL(config DBConfig, hostname string, hostgroupID int, port int) error {
	proxysqlDB, err := ConnectToProxysqlDB(config)
	if err != nil {
		return fmt.Errorf("failed to connect to ProxySQL: %w", err)
	}
	defer proxysqlDB.Close()
	return updateHost(proxysqlDB, config, hostname, hostgroupID, port)
}

func updateHost(proxysqlDB *sql.DB, config DBConfig, hostname string, hostgroupID int, port int) error {
	t := proxyTablesFor(Engine(config.DBType))
	if !isMySQL(config) {
		ensureClearTextAuth(proxysqlDB)
	}
	host, err := sqliteLiteral(hostname)
	if err != nil {
		return err
	}

	// Match on hostname and port only. The hostgroup is what the backend may have changed, and
	// matching on it too would insert a duplicate row for a server that has moved hostgroup.
	var count int
	checkQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE hostname = %s AND port = %d", t.servers, host, port)
	if err = proxysqlDB.QueryRow(checkQuery).Scan(&count); err != nil {
		return fmt.Errorf("failed to check existing server: %w", err)
	}

	if count == 0 {
		insertQuery := fmt.Sprintf("INSERT INTO %s (hostgroup_id, hostname, port) VALUES (%d, %s, %d)", t.servers, hostgroupID, host, port)
		if _, err = proxysqlDB.Exec(insertQuery); err != nil {
			return fmt.Errorf("failed to insert server: %w", err)
		}
		log.Printf("ProxySQL server was missing, registered: hostgroup=%d, hostname=%s, port=%d", hostgroupID, hostname, port)
	} else {
		var current int
		hgQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE hostgroup_id = %d AND hostname = %s AND port = %d", t.servers, hostgroupID, host, port)
		if err = proxysqlDB.QueryRow(hgQuery).Scan(&current); err != nil {
			return fmt.Errorf("failed to check server hostgroup: %w", err)
		}
		if current != 0 {
			log.Printf("No Need to update ProxySQL server: hostgroup=%d, hostname=%s, port=%d", hostgroupID, hostname, port)
			return nil
		}
		updateQuery := fmt.Sprintf("UPDATE %s SET hostgroup_id = %d WHERE hostname = %s AND port = %d", t.servers, hostgroupID, host, port)
		if _, err = proxysqlDB.Exec(updateQuery); err != nil {
			return fmt.Errorf("failed to update server hostgroup: %w", err)
		}
		log.Printf("ProxySQL server moved to hostgroup=%d: hostname=%s, port=%d", hostgroupID, hostname, port)
	}

	return loadAndSave(proxysqlDB, t.keyword, "SERVERS")
}

// ensureClearTextAuth keeps pgsql-authentication_method at 1 (clear text). The AuthNull MFA hook
// in ProxySQL runs only on the clear-text path; at the default (3, SCRAM-SHA-256) every login is
// refused with "Access denied ... (using password: YES)" and authn-service is never called.
// Best effort: a failure is logged, and the next cycle tries again.
func ensureClearTextAuth(proxysqlDB *sql.DB) {
	var current string
	err := proxysqlDB.QueryRow("SELECT variable_value FROM runtime_global_variables WHERE variable_name = 'pgsql-authentication_method'").Scan(&current)
	if err == nil && current == "1" {
		return
	}
	for _, q := range []string{
		"SET pgsql-authentication_method = 1",
		"LOAD PGSQL VARIABLES TO RUNTIME",
		"SAVE PGSQL VARIABLES TO DISK",
	} {
		if _, err := proxysqlDB.Exec(q); err != nil {
			log.Printf("ProxySQL: could not set clear-text auth (%s): %v", q, err)
			return
		}
	}
	log.Printf("ProxySQL: pgsql-authentication_method was %q, set to 1 (clear text)", current)
}

// RemoveHostFromProxySQL removes a database host from its engine's ProxySQL servers
func RemoveHostFromProxySQL(config DBConfig, hostname string, hostgroupID int, port int) error {
	proxysqlDB, err := ConnectToProxysqlDB(config)
	if err != nil {
		return fmt.Errorf("failed to connect to ProxySQL: %w", err)
	}
	defer proxysqlDB.Close()

	t := proxyTablesFor(Engine(config.DBType))
	host, err := sqliteLiteral(hostname)
	if err != nil {
		return err
	}
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE hostgroup_id = %d AND hostname = %s AND port = %d", t.servers, hostgroupID, host, port)
	_, err = proxysqlDB.Exec(deleteQuery)
	if err != nil {
		return fmt.Errorf("failed to delete server: %w", err)
	}

	log.Printf("Removed ProxySQL server: hostgroup=%d, hostname=%s, port=%d", hostgroupID, hostname, port)

	// Load and save to persist changes
	return loadAndSave(proxysqlDB, t.keyword, "SERVERS")
}

// ListProxySQLServers lists the servers registered in ProxySQL for the target's engine
func ListProxySQLServers(config DBConfig) ([]map[string]interface{}, error) {
	proxysqlDB, err := ConnectToProxysqlDB(config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ProxySQL: %w", err)
	}
	defer proxysqlDB.Close()

	rows, err := proxysqlDB.Query("SELECT hostgroup_id, hostname, port, status, weight FROM " + proxyTablesFor(Engine(config.DBType)).servers)
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
