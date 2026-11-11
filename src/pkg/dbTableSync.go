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
	logDebugf("Org Id: %d", orgID)
	tenantID, _ := strconv.Atoi(config.TenantID)
	logDebugf("Tenant Id: %d", tenantID)
	logDebugf("Fetching table names FROM database: %s", dbName)

	tables, err := databaseTables(mainDb, config, dbName)
	if err != nil {
		log.Printf("Error reading tables of %s: %v", dbName, err)
		return err
	}

	logDebugf("Fetched tables for %s: %v", dbName, tables)

	// Skip sending if no tables were found
	if len(tables) == 0 {
		logDebugf("No tables found in database %s, skipping API call", dbName)
		return nil
	}

	payload := map[string]interface{}{
		"orgId":        orgID,
		"tenantId":     tenantID,
		"databaseType": Engine(config.DBType),
		"databaseName": dbName,
		"tables":       tables,
		"instanceId":   instanceId,
		"host":         config.Host,           // Database host VM IP
		"agentVmIp":    AgentIdentity(config), // Agent VM IP (AGENT_VM_IP -> private IP)
	}

	logDebugf("Payload created for %s: %+v", dbName, payloadForLog(payload))

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error while marshalling the payload: %v", err)
		return err
	}

	api := config.API + "/api/v1/databaseService/dbTable"
	logDebugf("Sending table names to API %s", api)

	client := &http.Client{}
	httpReq, err := http.NewRequest("POST", api, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	res, err := doAPI(client, httpReq)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("dbTable: read response: %w", err)
	}
	logDebugf("API Response: %s", jsonForLog(bodyBytes))
	logDebugf("Payload Sent: %s", jsonForLog(payloadBytes))
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("dbTable: the control plane answered %d", res.StatusCode)
	}
	return nil
}

// pgTables lists the tables in schema public with their columns.
func pgTables(specificDb *sql.DB) (map[string][]string, error) {
	tables := make(map[string][]string)
	// PostgreSQL query to list tables in the schema
	tableQuery := "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'"
	logDebugf("Executing query: %s", tableQuery)

	rows, err := specificDb.Query(tableQuery)
	if err != nil {
		log.Printf("Error querying tables: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			log.Printf("Error scanning table name: %v", err)
			return nil, err
		}

		// PostgreSQL query to list columns in a table. The name is a bind parameter: a table name
		// is data, and one with a quote in it used to break (or rewrite) the query.
		columnRows, err := specificDb.Query("SELECT column_name FROM information_schema.columns "+
			"WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position", tableName)
		if err != nil {
			log.Printf("Error querying columns for table %s: %v", tableName, err)
			return nil, err
		}

		var columns []string
		for columnRows.Next() {
			var columnName string
			if err := columnRows.Scan(&columnName); err != nil {
				log.Printf("Error scanning column name: %v", err)
				columnRows.Close()
				return nil, err
			}
			columns = append(columns, columnName)
		}
		columnRows.Close()
		tables[tableName] = columns
	}
	return tables, rows.Err()
}
