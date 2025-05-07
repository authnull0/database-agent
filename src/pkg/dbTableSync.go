package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
)

func FetchTables(db *sql.DB, dbName string, config DBConfig, instanceId string) error {
	orgID, _ := strconv.Atoi(config.OrgID)
	log.Printf("Org Id: %d", orgID)
	tenantID, _ := strconv.Atoi(config.TenantID)
	log.Printf("Tenant Id: %d", tenantID)
	log.Printf("Fetching table names FROM...")
	tables := make(map[string][]string)
	log.Printf(dbName)

	// PostgreSQL query to list tables in the schema
	tableQuery := "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'"
	log.Println(tableQuery)

	rows, err := db.Query(tableQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}

		// PostgreSQL query to list columns in a table
		columnsQuery := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '%s'", tableName)
		columnRows, err := db.Query(columnsQuery)
		if err != nil {
			return err
		}
		defer columnRows.Close()

		var columns []string
		for columnRows.Next() {
			var columnName string
			if err := columnRows.Scan(&columnName); err != nil {
				log.Println(err)
				return err
			}
			columns = append(columns, columnName)
		}
		tables[tableName] = columns
	}

	log.Printf("Fetched table names: %v", tables)

	payload := map[string]interface{}{
		"orgId":        orgID,
		"tenantId":     tenantID,
		"databaseType": "postgres", // Changed from config.DBType to explicitly specify postgres
		"databaseName": dbName,
		"tables":       tables,
		"instanceId":   instanceId,
	}

	log.Printf("Payload created: %v", payload)

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error while marshalling the payload: %v", err)
		return err
	}

	api := config.API + "/api/v1/databaseService/dbTable"
	log.Printf("Sending table names to API %s", api)

	client := &http.Client{}
	httpReq, err := http.NewRequest("POST", api, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	res, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	log.Println(string(bodyBytes))
	log.Println("Payload Sent: ")
	log.Println(string(payloadBytes))

	return nil
}
