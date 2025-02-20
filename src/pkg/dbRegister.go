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

	"github.com/authnull0/database-agent/utils"
)

// FetchDatabaseStatus fetches the status of a database
func RegisterAgent(db *sql.DB, dbName string, config DBConfig) string {

	orgID, _ := strconv.Atoi(config.OrgID)
	log.Printf("Org Id: %d", orgID)
	tenantID, _ := strconv.Atoi(config.TenantID)
	log.Printf("Tenant Id: %d", tenantID)

	data := InstanceCreatedResponse{}

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
		"os":           "Windows",
		"status":       "Active",
		"uuid":         config.Key,
		"publicIp":     ipAddr,
		"instanceName": instanceName,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error while marshalling the payload: %v", err)

	}
	log.Default().Println("====================================")

	log.Default().Println("Database Register Agent Payload", payload)

	log.Default().Println("====================================")

	apiURL := config.API + "/api/v1/databaseService/registerDbAgent"
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

	body, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		log.Printf("Error while reading response body: %v", err)

	}

	errMarsh := json.Unmarshal([]byte(string(body)), &data)

	if errMarsh != nil {
		log.Println("UnMarshalling Error !", errMarsh)
	}

	fmt.Println("Machine Registered Response", string(body), data.InstanceId, data.Code)

	fmt.Println("Printing Data Obj ", data)

	instanceId := data.InstanceId
	fmt.Println("Instance Id returned from register", instanceId)
	// NB: instance ID is the machine_id of epm_machines table.
	return instanceId

	//log.Default().Printf("Response from external service: %v", string(body))
	//return nil
}

// Function to call Last Active Time API
func LastActive(instanceId string, db *sql.DB, dbName string, config DBConfig) error {

	orgID, _ := strconv.Atoi(config.OrgID)
	log.Printf("Org Id: %d", orgID)
	tenantID, _ := strconv.Atoi(config.TenantID)
	log.Printf("Tenant Id: %d", tenantID)

	// Sync database information with the API
	payload := map[string]interface{}{
		"orgId":      orgID,
		"tenantId":   tenantID,
		"instanceId": instanceId,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error while marshalling the payload: %v", err)

	}
	log.Default().Println("====================================")

	log.Default().Println("Last Active Time Payload", payload)

	log.Default().Println("====================================")

	apiURL := config.API + "/api/v1/databaseService/updatelastActive"
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

	body, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		log.Printf("Error while reading response body: %v", err)

	}
	log.Default().Printf("Response from external service: %v", string(body))
	return nil

}
