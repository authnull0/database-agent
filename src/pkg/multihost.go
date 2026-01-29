package pkg

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
)

// HostConfig represents a single database host configuration

// LoadHostsFromFile loads multiple host configurations from a JSON file
func LoadHostsFromFile(filePath string) ([]HostConfig, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read hosts file: %w", err)
	}

	var hostsFile HostsFile
	if err := json.Unmarshal(data, &hostsFile); err != nil {
		return nil, fmt.Errorf("failed to parse hosts file: %w", err)
	}

	return hostsFile.Hosts, nil
}

// ConnectToHostDB connects to a specific database host
func ConnectToHostDB(host HostConfig, dbType string) (*sql.DB, error) {
	port := host.Port
	if port == "" {
		port = "5432"
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=postgres sslmode=disable",
		host.HostVMIP, port, host.Username, host.Password)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	log.Printf("Successfully connected to PostgreSQL server at %s:%s", host.HostVMIP, port)
	return db, nil
}

// ProcessMultipleHosts processes all configured database hosts
// This function is the entry point for multi-host mode
func ProcessMultipleHosts(config DBConfig, hostsFilePath string) error {
	// Try to load hosts from JSON file
	hosts, err := LoadHostsFromFile(hostsFilePath)
	if err != nil {
		log.Printf("Could not load hosts file (%v), falling back to single-host mode", err)
		return nil // Return nil to allow fallback to single-host mode
	}

	if len(hosts) == 0 {
		log.Printf("No hosts found in hosts file, falling back to single-host mode")
		return nil
	}

	log.Printf("Multi-host mode: Processing %d database hosts", len(hosts))

	// Process each host
	for i, host := range hosts {
		hostgroupID := i + 1 // Hostgroup IDs start at 1
		log.Printf("Processing host %d: %s:%s (hostgroup: %d)", i+1, host.HostVMIP, host.Port, hostgroupID)

		// Register host in ProxySQL
		port, _ := strconv.Atoi(host.Port)
		if port == 0 {
			port = 5432
		}
		err := RegisterHostInProxySQL(config, host.HostVMIP, hostgroupID, port)
		if err != nil {
			log.Printf("Warning: Failed to register host %s in ProxySQL: %v", host.HostVMIP, err)
			continue
		}

		// Connect to the database host
		db, err := ConnectToHostDB(host, config.DBType)
		if err != nil {
			log.Printf("Warning: Failed to connect to host %s: %v", host.HostVMIP, err)
			continue
		}

		// Create a host-specific config for syncing
		hostConfig := DBConfig{
			OrgID:        config.OrgID,
			TenantID:     config.TenantID,
			DBType:       config.DBType,
			Port:         host.Port,
			TimeInterval: config.TimeInterval,
			API:          config.API,
			Host:         host.HostVMIP,
			User:         host.Username,
			Password:     host.Password,
			Key:          config.Key,
			MachineKey:   config.MachineKey,
			AgentVMIP:    config.AgentVMIP,
		}

		// Fetch database details for this host
		err = FetchDatabaseDetails(db, hostConfig)
		if err != nil {
			log.Printf("Warning: Failed to fetch database details for host %s: %v", host.HostVMIP, err)
		}

		db.Close()
	}

	return nil
}

// SyncMultipleHosts is called periodically to sync all configured hosts
// Returns true if multi-host mode is active, false otherwise
func SyncMultipleHosts(config DBConfig, hostsFilePath string) bool {
	hosts, err := LoadHostsFromFile(hostsFilePath)
	if err != nil || len(hosts) == 0 {
		return false // Fall back to single-host mode
	}

	log.Printf("Multi-host sync: Processing %d database hosts", len(hosts))

	for i, host := range hosts {
		hostgroupID := i + 1
		log.Printf("Syncing host %d (hostgroup %d): %s:%s", i+1, hostgroupID, host.HostVMIP, host.Port)

		// Connect to the database host
		db, err := ConnectToHostDB(host, config.DBType)
		if err != nil {
			log.Printf("Warning: Failed to connect to host %s during sync: %v", host.HostVMIP, err)
			continue
		}

		// Create host-specific config
		hostConfig := DBConfig{
			OrgID:        config.OrgID,
			TenantID:     config.TenantID,
			DBType:       config.DBType,
			Port:         host.Port,
			TimeInterval: config.TimeInterval,
			API:          config.API,
			Host:         host.HostVMIP,
			User:         host.Username,
			Password:     host.Password,
			Key:          config.Key,
			MachineKey:   config.MachineKey,
			AgentVMIP:    config.AgentVMIP,
		}

		// Fetch database details for this host
		err = FetchDatabaseDetails(db, hostConfig)
		if err != nil {
			log.Printf("Warning: Failed to sync host %s: %v", host.HostVMIP, err)
		}

		// Poll checkout jobs for this host
		// Get the list of databases for this host and poll for each
		databases := getHostDatabases(db, config.DBType)
		for _, dbName := range databases {
			err = PollCheckoutJob(db, dbName, hostConfig)
			if err != nil {
				log.Printf("Warning: Failed to poll checkout jobs for host %s, db %s: %v",
					host.HostVMIP, dbName, err)
			}
		}

		db.Close()
	}

	return true
}

// getHostDatabases gets list of non-system databases from a host
// dbType parameter reserved for future MySQL/MSSQL support
func getHostDatabases(db *sql.DB, _ string) []string {
	var databases []string

	// PostgreSQL query - will be extended for other DB types in future
	query := "SELECT datname FROM pg_database WHERE datistemplate = false"
	rows, err := db.Query(query)
	if err != nil {
		log.Printf("Error querying databases: %v", err)
		return databases
	}
	defer rows.Close()

	systemDatabases := map[string]bool{
		"postgres":           true,
		"template0":          true,
		"template1":          true,
		"information_schema": true,
	}

	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			continue
		}
		if !systemDatabases[dbName] {
			databases = append(databases, dbName)
		}
	}

	return databases
}
