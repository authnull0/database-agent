package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
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
	if errors.Is(err, errNoConnect) {
		return nil // logged once by openPGDatabase
	}
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

// pgTablesQuery is every table (partitioned ones included) of every schema but the system ones,
// with its columns in order, in one pass: a database with hundreds of tables used to cost a query
// per table.
const pgTablesQuery = `
	SELECT t.schemaname, t.tablename, c.column_name
	FROM pg_catalog.pg_tables t
	LEFT JOIN information_schema.columns c
	       ON c.table_schema = t.schemaname AND c.table_name = t.tablename
	WHERE t.schemaname NOT IN ('pg_catalog', 'information_schema')
	  AND t.schemaname NOT LIKE 'pg\_toast%'
	  AND t.schemaname NOT LIKE 'pg\_temp\_%'
	ORDER BY t.schemaname, t.tablename, c.ordinal_position`

// pgTableName is how a table is reported, as policies name tables (backendrole.go
// canonicalTables): a table in schema public by its bare name, as before -- the names already
// stored and the policies and field masking that name them keep working -- any other as
// "schema.table".
func pgTableName(schema, table string) string {
	if schema == "public" {
		return table
	}
	return schema + "." + table
}

// pgTables lists the tables of every non-system schema with their columns (pgTablesQuery).
func pgTables(specificDb *sql.DB) (map[string][]string, error) {
	rows, err := specificDb.Query(pgTablesQuery)
	if err != nil {
		log.Printf("Error querying tables: %v", err)
		return nil, err
	}
	defer rows.Close()
	tables := make(map[string][]string)
	for rows.Next() {
		var schema, table string
		var column sql.NullString
		if err := rows.Scan(&schema, &table, &column); err != nil {
			return nil, err
		}
		name := pgTableName(schema, table)
		if column.Valid {
			tables[name] = append(tables[name], column.String)
		} else if _, ok := tables[name]; !ok {
			tables[name] = nil // a table with no columns (or none the agent may see)
		}
	}
	return tables, rows.Err()
}
