package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

// FetchDatabaseStatus fetches the status of a database
func FetchDatabaseStatus(db *sql.DB, dbName string, config DBConfig) error {
	var query string

	orgID, _ := strconv.Atoi(config.OrgID)
	log.Printf("Org Id: %d", orgID)
	tenantID, _ := strconv.Atoi(config.TenantID)
	log.Printf("Tenant Id: %d", tenantID)

	switch config.DBType {
	case "mysql":
		query = "SHOW STATUS LIKE 'Uptime'"
	case "Postgres":
		query = "SELECT pg_is_in_recovery() AS is_in_recovery"
	case "MSSQL":
		query = "SELECT state_desc FROM sys.databases WHERE name = DB_NAME()"
	case "Oracle":
		query = "SELECT open_mode FROM v$database"
	default:
		return fmt.Errorf("unsupported database type")
	}

	// Execute query for database status
	row := db.QueryRow(query)
	var status string

	switch config.DBType {
	case "mysql":
		var uptime int
		if err := row.Scan(&status, &uptime); err != nil {
			log.Printf("Database: %s STATUS: %s", dbName, "INACTIVE")
			return err
		}
		log.Printf("Database: %s Active: %d seconds", dbName, uptime)

	case "Postgres":
		var isInRecovery bool
		if err := row.Scan(&isInRecovery); err != nil {
			return err
		}
		status = map[bool]string{true: "In Recovery", false: "Active"}[isInRecovery]
		log.Printf("Database Status: %s", status)

	case "MSSQL":
		if err := row.Scan(&status); err != nil {
			return err
		}
		log.Printf("Database Status: %s", status)

	case "Oracle":
		if err := row.Scan(&status); err != nil {
			return err
		}
		log.Printf("Database Status: %s", status)
	}

	// Sync database information with the API
	payload := map[string]interface{}{
		"OrgID":        orgID,
		"tenantId":     tenantID,
		"databaseType": config.DBType,
		"databaseName": dbName,
		"status":       status,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error while marshalling the payload: %v", err)

	}

	apiURL := config.API + "/api/v1/dbSync"
	httpReq, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	log.Println("Payload Sent:")

	log.Println(string(payloadBytes))
	if err != nil {
		log.Printf("Error while creating request: %v", err)

	}

	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	httpResp, err := client.Do(httpReq)
	if err != nil {
		log.Printf("Error while making request: %v", err)

	}
	defer httpResp.Body.Close()

	body, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		log.Printf("Error while reading response body: %v", err)

	}
	log.Default().Println("Response from external service: %v", string(body))
	return nil
}
