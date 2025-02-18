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
		"host":         config.Host,
		"status":       status,
		"uuid":         config.Key,
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

// CheckDatabaseStatus calls the API and creates missing databases
func CheckDatabaseStatus(db *sql.DB, config DBConfig) error {
	orgID, _ := strconv.Atoi(config.OrgID)
	tenantID, _ := strconv.Atoi(config.TenantID)

	// Prepare request payload
	payload := map[string]interface{}{
		"orgId":        orgID,
		"tenantId":     tenantID,
		"databaseType": config.DBType,
		"port":         config.Port,
		"host":         config.Host,
		"uuid":         config.Key,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error marshalling payload: %v", err)
		return err
	}

	apiURL := config.API + "/api/v1/databaseService/checkDatabaseStatus"
	httpReq, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		log.Printf("Error creating request: %v", err)
		return err
	}

	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	httpResp, err := client.Do(httpReq)
	if err != nil {
		log.Printf("Error making request: %v", err)
		return err
	}
	defer httpResp.Body.Close()

	// Read response body
	body, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		log.Printf("Error reading response: %v", err)
		return err
	}

	log.Printf("Response from API: %s", string(body))

	// Parse response
	var apiResp CheckDBResponse
	err = json.Unmarshal(body, &apiResp)
	if err != nil {
		log.Printf("Error parsing API response: %v", err)
		return err
	}

	if !apiResp.Success {
		log.Printf("API error: %s", apiResp.Message)
		return fmt.Errorf("API error: %s", apiResp.Message)
	}

	// Create databases if they do not exist
	for _, dbName := range apiResp.Databases {
		if err := createDatabase(db, dbName); err != nil {
			log.Printf("Failed to create database %s: %v", dbName, err)
		} else {
			log.Printf("Database %s created successfully", dbName)
		}
	}

	return nil
}

// createDatabase checks if a database exists, and if not, creates it
func createDatabase(db *sql.DB, dbName string) error {
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName))
	return err
}
