package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/authnull0/database-agent/utils"
)

// FetchDatabaseStatus fetches the status of a PostgreSQL database
func FetchDatabaseStatus(db *sql.DB, dbName string, config DBConfig, instanceId string) error {
	var query string

	orgID, _ := strconv.Atoi(config.OrgID)
	log.Printf("Org Id: %d", orgID)
	tenantID, _ := strconv.Atoi(config.TenantID)
	log.Printf("Tenant Id: %d", tenantID)

	// PostgreSQL query to check uptime
	query = "SELECT EXTRACT(EPOCH FROM current_timestamp - pg_postmaster_start_time())::integer AS uptime"

	// Execute query for database status
	row := db.QueryRow(query)
	var status string
	var uptime int

	if err := row.Scan(&uptime); err != nil {
		log.Printf("Database: %s STATUS: %s", dbName, "Inactive")
		status = "Inactive"
	} else {
		status = "Active"
		log.Printf("Database: %s is Active", dbName)
	}
	log.Printf("Database: %s Active: %d seconds", dbName, uptime)

	instanceName, _ := os.Hostname()
	log.Default().Println("Register Agent", instanceName)

	ipAddr, err := utils.GetPublicIP()
	if err != nil {
		fmt.Println("Failed to get PublicIp Address", err)
		//return ""
	}
	log.Default().Println("IP Address:", ipAddr)

	// Sync database information with the API
	payload := map[string]interface{}{
		"orgId":        orgID,
		"tenantId":     tenantID,
		"databaseType": "postgres",
		"databaseName": dbName,
		"port":         config.Port,
		"host":         ipAddr,
		"status":       status,
		"uuid":         config.Key,
		"instanceId":   instanceId,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error while marshalling the payload: %v", err)
	}

	apiURL := config.API + "/api/v1/databaseService/dbSync"
	httpReq, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	log.Printf("Payload Sent: %s", string(payloadBytes))

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

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		log.Printf("Error while reading response body: %v", err)
	}
	log.Default().Printf("Response from external service: %v", string(body))
	return nil
}
