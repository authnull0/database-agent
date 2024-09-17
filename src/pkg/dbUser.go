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
				SELECT user 
				FROM mysql.user`
	case "postgres":
		query = `
				SELECT usename 
				FROM pg_user`
	case "mssql":
		query = `
				SELECT name AS user_name 
				FROM sys.database_principals 
				WHERE type = 'S' OR type = 'U'`
	case "oracle":
		query = `
				SELECT username 
				FROM all_users`
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
		var userName, databaseName string
		if err := rows.Scan(&databaseName, &userName); err != nil {
			return err
		}
		// Sync user information with the API
		payload := map[string]interface{}{
			"OrgID":        orgID,
			"tenantId":     tenantID,
			"databaseType": config.DBType,
			"databaseName": dbName,
			//"tableName":    tableName,
			"userName": userName,
		}

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			log.Printf("Error while marshalling the payload: %v", err)

		}

		apiURL := config.API + "/api/v1/databaseService/dbUser"
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
		log.Default().Println("Response from dbSync API: %v", string(body))

	}
	return nil
}
