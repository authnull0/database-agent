package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/authnull0/database-agent/utils"
)

// RegisterAgent registers the database agent with the central service
// Compatible with both MySQL and PostgreSQL
func RegisterAgent(db *sql.DB, dbName string, config DBConfig) (string, string) {

	orgID, _ := strconv.Atoi(config.OrgID)
	log.Printf("Org Id: %d", orgID)
	tenantID, _ := strconv.Atoi(config.TenantID)
	log.Printf("Tenant Id: %d", tenantID)

	data := InstanceCreatedResponse{}

	instanceName, _ := os.Hostname()
	log.Default().Println("Register Agent", instanceName)

	ipAddr := AgentIdentity(config)
	log.Default().Println("Agent VM IP:", ipAddr)

	privateIp, err := utils.GetPrivateIP()
	if err != nil {
		log.Default().Println("Failed to get Private IP Address")
	} else {
		log.Default().Println("Private IP Address:", privateIp)
	}

	// Sync database information with the API
	payload := map[string]interface{}{
		"orgId":        orgID,
		"tenantId":     tenantID,
		"os":           2,
		"status":       "ACTIVE",
		"uuid":         config.Key,
		"machineKey":   config.MachineKey,
		"publicIp":     ipAddr,
		"instanceName": instanceName,
		"dbType":       config.DBType, // This will be "postgres" for PostgreSQL
		"privateIp":    privateIp,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error while marshalling the payload: %v", err)

	}
	log.Default().Println("====================================")

	log.Default().Println("Database Register Agent Payload", payloadForLog(payload))

	log.Default().Println("====================================")

	apiURL := config.API + "/api/v1/databaseService/registerDbAgent"
	httpReq, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	log.Printf("Payload Sent: %s", jsonForLog(payloadBytes))

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

	errMarsh := json.Unmarshal([]byte(string(body)), &data)

	if errMarsh != nil {
		log.Println("UnMarshalling Error !", errMarsh)
	}

	// Not the raw body: what an operator needs is the instance and the result.
	log.Printf("Machine registered: instance %v, code %v", data.InstanceId, data.Code)

	instanceId := data.InstanceId
	agentStatus := data.AgentStatus
	fmt.Println("Instance Id returned from register:", instanceId)
	// NB: instance ID is the machine_id of epm_machines table.
	return instanceId, agentStatus
}

// LastActive reports the agent alive for a database. directAccess, when not nil, is this sync's
// verdict on direct access to the server (directaccess.go), shown in the console.
//
// The response carries the tenant's password rotation interval, set in the console (AUTH-233),
// which the agent then uses. Anything short of a good answer keeps the interval in use.
func LastActive(instanceId string, db *sql.DB, dbName string, config DBConfig, directAccess *DirectAccessReport) error {
	log.Default().Printf("Instance Id after last active api call: %v", instanceId)

	orgID, _ := strconv.Atoi(config.OrgID)
	tenantID, _ := strconv.Atoi(config.TenantID)

	payload := map[string]interface{}{
		"orgId":      orgID,
		"tenantId":   tenantID,
		"instanceId": instanceId,
		"dbType":     config.DBType, // This will be "postgres" for PostgreSQL
	}
	if directAccess != nil {
		payload["directAccess"] = directAccess
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("last active: marshal: %w", err)
	}

	apiURL := config.API + "/api/v1/databaseService/updateLastActive"
	httpReq, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("last active: request: %w", err)
	}
	log.Printf("Payload Sent: %s", jsonForLog(payloadBytes))
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := (&http.Client{Timeout: 30 * time.Second}).Do(httpReq)
	if err != nil {
		return fmt.Errorf("last active: %w", err)
	}
	defer httpResp.Body.Close()

	body, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return fmt.Errorf("last active: read response: %w", err)
	}
	log.Default().Printf("Response from external service: %v", jsonForLog(body))
	if httpResp.StatusCode != http.StatusOK {
		return fmt.Errorf("last active: status %d", httpResp.StatusCode)
	}
	applyRotationInterval(body)
	return nil
}

// applyRotationInterval takes passwordRotationHours from a last-active response. Absent (an
// older control plane, or one that could not read the setting), malformed or out of range, the
// interval in use is kept.
func applyRotationInterval(body []byte) {
	var resp struct {
		PasswordRotationHours *int `json:"passwordRotationHours"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.PasswordRotationHours == nil {
		return
	}
	if err := SetRotationHours(*resp.PasswordRotationHours); err != nil {
		log.Printf("Ignoring the console's password rotation interval: %v", err)
	}
}
