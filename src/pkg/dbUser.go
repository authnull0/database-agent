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

// FetchDatabaseUsers fetches the users of a database and their tables
func FetchDatabaseUsers(db *sql.DB, dbName string, config DBConfig) error {
	log.Println("Entered FetchDatabaseUers")
	// Define the query to fetch user information based on the database type
	var query string
	switch config.DBType {
	case "mysql":
		query = `
			SELECT table_name, table_schema, user() 
			FROM mysql.user
			WHERE table_schema = ?`
	case "postgres":
		query = `
				SELECT table_name, current_user 
				FROM information_schema.tables 
				WHERE table_schema = 'public'`
	case "mssql":
		query = `
				SELECT table_name, table_schema, s.name AS user_name 
				FROM information_schema.tables t 
				JOIN sys.schemas s ON t.table_schema = s.schema_id`
	case "oracle":
		query = `
				SELECT table_name, owner, user 
				FROM all_tables 
				WHERE owner = UPPER(?)`
	default:
		return fmt.Errorf("unsupported database type: %s", config.DBType)
	}

	rows, err := db.Query(query, dbName)
	if err != nil {
		return err
	}
	defer rows.Close()

	orgID, _ := strconv.Atoi(config.OrgID)
	tenantID, _ := strconv.Atoi(config.TenantID)

	for rows.Next() {
		var tableName, userName, databaseName string
		if err := rows.Scan(&tableName, &databaseName, &userName); err != nil {
			return err
		}
		// Sync user information with the API
		payload := map[string]interface{}{
			"OrgID":        orgID,
			"tenantId":     tenantID,
			"databaseType": config.DBType,
			"databaseName": dbName,
			"tableName":    tableName,
			"userName":     userName,
		}

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			log.Printf("Error while marshalling the payload: %v", err)

		}

		apiURL := config.API + "/api/v1/dbUser"
		log.Printf("Sending Payload to the API %s", apiURL)

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

	}
	return nil
}
