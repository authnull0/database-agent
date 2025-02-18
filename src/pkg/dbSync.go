package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
)

// FetchDatabaseStatus fetches the status of a database
func FetchDatabaseStatus(db *sql.DB, dbName string, config DBConfig) error {
	var query string

	orgID, _ := strconv.Atoi(config.OrgID)
	log.Printf("Org Id: %d", orgID)
	tenantID, _ := strconv.Atoi(config.TenantID)
	log.Printf("Tenant Id: %d", tenantID)
	log.Printf("UUID is : %s", config.APIKey)

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

  hostname, err := os.Hostname()
  if err != nil {
    log.Printf("Cannot retrieve hostname")
  }

	// Sync database information with the API
	payload := map[string]interface{}{
		"orgId":        orgID,
		"tenantId":     tenantID,
		"databaseType": config.DBType,
		"databaseName": dbName,
		"port":         config.Port,
		"host":         config.Host,
		"status":       status,
		"uuid":         config.APIKey,
    "publicIp":     GetOutboundIP(),
    "hostname":     hostname,
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

func GetOutboundIP() net.IP {
  conn, err := net.Dial("udp", "8.8.8.8:80")
  if err != nil {
    log.Fatal(err)
  }
  defer conn.Close()

  localAddr := conn.LocalAddr().(*net.UDPAddr)
  return localAddr.IP
}
