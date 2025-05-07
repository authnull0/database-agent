package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
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
	log.Printf("Database Name: %s", dbName)

	log.Printf("Fetching table names from PostgreSQL...")
	tables := make(map[string][]string)

	// First, verify the current schema search path
	var currentSchema string
	schemaQuery := "SELECT current_schema();"
	err := db.QueryRow(schemaQuery).Scan(&currentSchema)
	if err != nil {
		log.Printf("Error getting current schema: %v", err)
		// Continue anyway, using default queries
	} else {
		log.Printf("Current schema: %s", currentSchema)
	}

	// More robust query - try multiple approaches to find tables
	// This checks both the current schema and public schema
	tableQuery := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE (table_schema = 'public' OR table_schema = current_schema()) 
		AND table_type = 'BASE TABLE' 
		AND table_catalog = $1`

	log.Println("Executing query:", tableQuery)
	rows, err := db.Query(tableQuery, dbName)
	if err != nil {
		log.Printf("Error querying tables: %v", err)

		// Fallback query without catalog filter (older PostgreSQL versions)
		tableQuery = `
			SELECT table_name 
			FROM information_schema.tables 
			WHERE (table_schema = 'public' OR table_schema = current_schema()) 
			AND table_type = 'BASE TABLE'`

		log.Println("Trying fallback query:", tableQuery)
		rows, err = db.Query(tableQuery)
		if err != nil {
			log.Printf("Error with fallback query: %v", err)

			// Last resort - try querying pg_tables directly
			tableQuery = `
				SELECT tablename 
				FROM pg_catalog.pg_tables 
				WHERE schemaname = 'public' OR schemaname = current_schema()`

			log.Println("Trying last resort query:", tableQuery)
			rows, err = db.Query(tableQuery)
			if err != nil {
				log.Printf("All table queries failed: %v", err)
				return err
			}
		}
	}
	defer rows.Close()

	tableCount := 0
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			log.Printf("Error scanning table name: %v", err)
			return err
		}

		tableCount++
		log.Printf("Found table: %s", tableName)

		// PostgreSQL query to list columns in a table
		columnsQuery := `
			SELECT column_name 
			FROM information_schema.columns 
			WHERE (table_schema = 'public' OR table_schema = current_schema())
			AND table_name = $1 
			ORDER BY ordinal_position`

		columnRows, err := db.Query(columnsQuery, tableName)
		if err != nil {
			log.Printf("Error querying columns for table %s: %v", tableName, err)

			// Fallback column query using pg_attribute
			columnsQuery = `
				SELECT a.attname as column_name
				FROM pg_catalog.pg_attribute a
				JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
				JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
				WHERE c.relname = $1
				AND a.attnum > 0
				AND NOT a.attisdropped
				ORDER BY a.attnum`

			columnRows, err = db.Query(columnsQuery, tableName)
			if err != nil {
				log.Printf("Fallback column query failed: %v", err)
				continue // Skip this table but continue with others
			}
		}

		var columns []string
		columnCount := 0

		for columnRows.Next() {
			var columnName string
			if err := columnRows.Scan(&columnName); err != nil {
				log.Printf("Error scanning column name: %v", err)
				columnRows.Close()
				continue
			}
			columns = append(columns, columnName)
			columnCount++
		}
		columnRows.Close() // Close the column rows explicitly

		log.Printf("Table %s has %d columns", tableName, columnCount)
		tables[tableName] = columns
	}

	if tableCount == 0 {
		log.Printf("WARNING: No tables found in database %s", dbName)

		// Debug: Let's check if we can at least get schema information
		var schemaCount int
		err = db.QueryRow("SELECT COUNT(*) FROM information_schema.schemata").Scan(&schemaCount)
		if err != nil {
			log.Printf("Error checking schemas: %v", err)
		} else {
			log.Printf("Database has %d schemas", schemaCount)
		}
	}

	log.Printf("Fetched table names: %v", tables)

	payload := map[string]interface{}{
		"orgId":        orgID,
		"tenantId":     tenantID,
		"databaseType": "postgres",
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
		log.Printf("Error creating HTTP request: %v", err)
		return err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	res, err := client.Do(httpReq)
	if err != nil {
		log.Printf("Error sending HTTP request: %v", err)
		return err
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		log.Printf("Error reading response body: %v", err)
	} else {
		log.Println("API Response:", string(bodyBytes))
	}

	log.Println("Payload Sent:", string(payloadBytes))

	return nil
}
