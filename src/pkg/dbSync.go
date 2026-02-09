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
func FetchDatabaseStatus(db *sql.DB, dbName string, config DBConfig, instanceId string) (int, error) {
	var query string
	var data DbSyncResponse

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

	// Get the public IP of the agent VM (for legacy mode fallback)
	publicIP, err := utils.GetPublicIP()
	if err != nil {
		fmt.Println("Failed to get PublicIp Address", err)
	}

	// Determine the actual database host IP
	// In multi-host mode, config.Host contains the actual database host IP
	// In legacy single-host mode, use the public IP
	hostIP := config.Host
	if hostIP == "" {
		hostIP = publicIP
	}
	log.Default().Println("Database Host IP:", hostIP)

	// Sync database information with the API
	// Include agent_vm_ip for multi-host mode
	agentVMIP := config.AgentVMIP
	if agentVMIP == "" {
		agentVMIP = publicIP // Legacy mode: agent VM IP is same as public IP
	}
	log.Default().Println("Agent VM IP:", agentVMIP)

	payload := map[string]interface{}{
		"orgId":        orgID,
		"tenantId":     tenantID,
		"databaseType": "postgres",
		"databaseName": dbName,
		"port":         config.Port,
		"host":         hostIP,    // Host VM IP (where database runs)
		"agentVmIp":    agentVMIP, // Agent VM IP (where proxysql runs)
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

	errMarsh := json.Unmarshal([]byte(string(body)), &data)

	if errMarsh != nil {
		log.Println("UnMarshalling Error !", errMarsh)
	}

	log.Default().Println("DbSync Response", string(body), data.HostGroupId, data.Code)

	log.Default().Println("Printing Data Obj ", data)

	hostGroupId := data.HostGroupId
	log.Default().Println("Database Synchronized with host group Id:", hostGroupId)
	return hostGroupId, nil
}
