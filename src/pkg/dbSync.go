package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
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
	log.Printf("Org Id: %d", orgID)
	tenantID, _ := strconv.Atoi(config.TenantID)
	log.Printf("Tenant Id: %d", tenantID)

	// How long the server has been up: PostgreSQL's postmaster start, MySQL's Uptime status.
	uptime, scanErr := serverUptime(db, config)
	var status string
	if scanErr != nil {
		log.Printf("Database: %s STATUS: %s", dbName, "INACTIVE")
		status = "INACTIVE"
	} else {
		status = "ACTIVE"
		log.Printf("Database: %s is Active", dbName)
	}
	log.Printf("Database: %s Active: %d seconds", dbName, uptime)

	instanceName, _ := os.Hostname()
	log.Default().Println("Register Agent", instanceName)

	// Agent VM IP (AGENT_VM_IP -> private IP; never the public/ipify address)
	publicIP := AgentIdentity(config)

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
	agentVMIP := AgentIdentity(config)
	log.Default().Println("Agent VM IP:", agentVMIP)

	// Get the private IP of the agent
	privateIP, err := utils.GetPrivateIP()
	if err != nil {
		log.Printf("Failed to get Private IP: %v", err)
		privateIP = ""
	}
	log.Default().Println("Agent Private IP:", privateIP)

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
		log.Printf("Error while marshalling the payload: %v", err)
	}

	apiURL := config.API + "/api/v1/databaseService/dbSync"
	httpReq, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	log.Printf("Payload Sent: %s", jsonForLog(payloadBytes))

	if err != nil {
		log.Printf("Error while creating request: %v", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	httpResp, err := doAPI(client, httpReq)
	if err != nil {
		log.Printf("Error while making request: %v", err)
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		log.Printf("Error while reading response body: %v", err)
	}
	log.Default().Printf("Response from external service: %v", jsonForLog(body))

	errMarsh := json.Unmarshal([]byte(string(body)), &data)

	if errMarsh != nil {
		log.Println("UnMarshalling Error !", errMarsh)
	}

	log.Default().Println("DbSync Response", jsonForLog(body), data.HostGroupId, data.Code)

	log.Default().Println("Printing Data Obj ", data)

	hostGroupId := data.HostGroupId
	databaseStatus := data.DatabaseStatus
	log.Default().Println("Database Status from API:", databaseStatus)
	log.Default().Println("Database Synchronized with host group Id:", hostGroupId)
	return hostGroupId, databaseStatus, nil
}
