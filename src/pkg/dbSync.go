package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

// FetchDatabaseStatus fetches the status of a database
func FetchDatabaseStatus(db *sql.DB, dbName string, config DBConfig, dbHost string, apiKey string) error {
	var query string

	orgID, _ := strconv.Atoi(config.OrgID)
	log.Printf("Org Id: %d", orgID)
	tenantID, _ := strconv.Atoi(config.TenantID)
	log.Printf("Tenant Id: %d", tenantID)

	query = "SHOW STATUS LIKE 'Uptime'"
	// Execute query for database status
	row := db.QueryRow(query)
	var status string

	var uptime int
	if err := row.Scan(&status, &uptime); err != nil {
		log.Printf("Database: %s STATUS: %s", dbName, "Inactive")
		status = "Inactive"
	} else {
		status = "Active"
		log.Printf("Database: %s is Active", dbName)
	}
	log.Printf("Database: %s Active: %d seconds", dbName, uptime)

	// Sync database information with the API
	payload := map[string]interface{}{
		"orgId":        orgID,
		"tenantId":     tenantID,
		"databaseType": config.DBType,
		"databaseName": dbName,
		"port":         config.Port,
		"host":         dbHost,
		"status":       status,
		"uuid":         apiKey,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error while marshalling the payload: %v", err)

	}

	apiURL := config.API + "/api/v1/databaseService/dbSync"
	httpReq, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	log.Println("Payload Sent: %s", string(payloadBytes))

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
