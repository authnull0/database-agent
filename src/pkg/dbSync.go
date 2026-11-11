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

// FetchDatabaseStatus reports a database, and whether its server is up, and returns the hostgroup
// the backend gives it
func FetchDatabaseStatus(db *sql.DB, dbName string, config DBConfig, instanceId string) (int, string, error) {
	var data DbSyncResponse

	orgID, _ := strconv.Atoi(config.OrgID)
	logDebugf("Org Id: %d", orgID)
	tenantID, _ := strconv.Atoi(config.TenantID)
	logDebugf("Tenant Id: %d", tenantID)

	// How long the server has been up: PostgreSQL's postmaster start, MySQL's Uptime status.
	uptime, scanErr := serverUptime(db, config)
	var status string
	if scanErr != nil {
		log.Printf("Database: %s on %s STATUS: INACTIVE (%v)", dbName, config.Host, scanErr)
		status = "INACTIVE"
	} else {
		status = "ACTIVE"
		logDebugf("Database: %s is Active", dbName)
	}
	logDebugf("Database: %s Active: %d seconds", dbName, uptime)

	instanceName, _ := os.Hostname()
	logDebugf("Register Agent %s", instanceName)

	// Agent VM IP (AGENT_VM_IP -> private IP; never the public/ipify address)
	publicIP := AgentIdentity(config)

	// Determine the actual database host IP
	// In multi-host mode, config.Host contains the actual database host IP
	// In legacy single-host mode, use the public IP
	hostIP := config.Host
	if hostIP == "" {
		hostIP = publicIP
	}
	logDebugf("Database Host IP: %s", hostIP)

	// Sync database information with the API
	// Include agent_vm_ip for multi-host mode
	agentVMIP := AgentIdentity(config)
	logDebugf("Agent VM IP: %s", agentVMIP)

	// Get the private IP of the agent
	privateIP, err := utils.GetPrivateIP()
	if err != nil {
		log.Printf("Failed to get Private IP: %v", err)
		privateIP = ""
	}
	logDebugf("Agent Private IP: %s", privateIP)

	payload := map[string]interface{}{
		"orgId":        orgID,
		"tenantId":     tenantID,
		"databaseType": Engine(config.DBType),
		"databaseName": dbName,
		"port":         config.Port,
		"host":         hostIP,       // Host VM IP (where database runs)
		"agentVmIp":    agentVMIP,    // Agent VM IP (where proxysql runs)
		"hostName":     instanceName, // Hostname of the agent
		"privateIp":    privateIP,    // Private IP of the agent
		"status":       status,
		"uuid":         config.Key,
		"instanceId":   instanceId,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return 0, "", fmt.Errorf("dbSync: marshal: %w", err)
	}

	apiURL := config.API + "/api/v1/databaseService/dbSync"
	httpReq, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return 0, "", fmt.Errorf("dbSync: request: %w", err)
	}
	logDebugf("Payload Sent: %s", jsonForLog(payloadBytes))

	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	httpResp, err := doAPI(client, httpReq)
	if err != nil {
		return 0, "", fmt.Errorf("dbSync: %w", err)
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return 0, "", fmt.Errorf("dbSync: read response: %w", err)
	}
	logDebugf("Response from external service: %v", jsonForLog(body))
	if httpResp.StatusCode != http.StatusOK {
		log.Printf("dbSync for %s on %s: the control plane answered %d", dbName, config.Host, httpResp.StatusCode)
	}

	errMarsh := json.Unmarshal(body, &data)

	if errMarsh != nil {
		log.Println("UnMarshalling Error !", errMarsh)
	}

	hostGroupId := data.HostGroupId
	databaseStatus := data.DatabaseStatus
	logDebugf("Database Status from API: %s, host group Id: %d", databaseStatus, hostGroupId)
	return hostGroupId, databaseStatus, nil
}
