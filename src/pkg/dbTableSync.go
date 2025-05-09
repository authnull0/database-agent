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

	_ "github.com/lib/pq" // Import PostgreSQL driver
)

func FetchTables(mainDb *sql.DB, dbName string, config DBConfig, instanceId string) error {
	orgID, _ := strconv.Atoi(config.OrgID)
	log.Printf("Org Id: %d", orgID)
	tenantID, _ := strconv.Atoi(config.TenantID)
	log.Printf("Tenant Id: %d", tenantID)
	log.Printf("Fetching table names FROM database: %s", dbName)
	tables := make(map[string][]string)

	// Create a new connection to the specific database
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.Host, config.Port, config.User, config.Password, dbName)

	specificDb, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Printf("Error connecting to specific database %s: %v", dbName, err)
		return err
	}
	defer specificDb.Close()

	// Test the connection
	err = specificDb.Ping()
	if err != nil {
		log.Printf("Cannot ping database %s: %v", dbName, err)
		return err
	}

	log.Printf("Successfully connected to database: %s", dbName)

	// PostgreSQL query to list tables in the schema
	tableQuery := "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'"
	log.Println("Executing query:", tableQuery)

	rows, err := specificDb.Query(tableQuery)
	if err != nil {
		log.Printf("Error querying tables: %v", err)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			log.Printf("Error scanning table name: %v", err)
			return err
		}

		// PostgreSQL query to list columns in a table
		columnsQuery := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '%s'", tableName)
		columnRows, err := specificDb.Query(columnsQuery)
		if err != nil {
			log.Printf("Error querying columns for table %s: %v", tableName, err)
			return err
		}

		var columns []string
		for columnRows.Next() {
			var columnName string
			if err := columnRows.Scan(&columnName); err != nil {
				log.Printf("Error scanning column name: %v", err)
				columnRows.Close()
				return err
			}
			columns = append(columns, columnName)
		}
		columnRows.Close()
		tables[tableName] = columns
	}

	log.Printf("Fetched tables for %s: %v", dbName, tables)

	// Skip sending if no tables were found
	if len(tables) == 0 {
		log.Printf("No tables found in database %s, skipping API call", dbName)
		return nil
	}

	payload := map[string]interface{}{
		"orgId":        orgID,
		"tenantId":     tenantID,
		"databaseType": "postgres",
		"databaseName": dbName,
		"tables":       tables,
		"instanceId":   instanceId,
	}

	log.Printf("Payload created for %s: %+v", dbName, payload)

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
	log.Println("API Response:", string(bodyBytes))
	log.Println("Payload Sent:", string(payloadBytes))

	return nil
}
