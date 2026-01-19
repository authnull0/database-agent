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

// FetchTablePrivileges fetches the privileges for users at the database level for PostgreSQL
func FetchTablePrivileges(db *sql.DB, dbName string, config DBConfig, instanceId string) error {
	var query string
	orgID, _ := strconv.Atoi(config.OrgID)
	tenantID, _ := strconv.Atoi(config.TenantID)

	// PostgreSQL uses different system catalogs for privilege information
	query = `
	SELECT
		r.rolname AS username,
		'%' AS host,  -- PostgreSQL doesn't have host concepts like MySQL, using % for compatibility
		CASE 
			WHEN r.rolsuper THEN 'ALL PRIVILEGES'
			ELSE string_agg(
				CASE
					WHEN has_database_privilege(r.rolname, current_database(), 'CREATE') THEN 'CREATE'
					WHEN has_database_privilege(r.rolname, current_database(), 'CONNECT') THEN 'CONNECT'
					WHEN has_database_privilege(r.rolname, current_database(), 'TEMPORARY') THEN 'TEMPORARY'
					ELSE ''
				END, ', '
			)
		END AS privileges,
		CASE 
			WHEN r.rolsuper THEN 'Admin'
			WHEN r.rolcreaterole OR r.rolcreatedb THEN 'Admin'
			ELSE 'User'
		END AS role
	FROM 
		pg_roles r
	WHERE 
		r.rolname NOT IN ('postgres', 'pg_signal_backend', 'pg_read_all_settings', 
						  'pg_read_all_stats', 'pg_stat_scan_tables', 'pg_monitor', 
						  'pg_database_owner')
		AND r.rolcanlogin = true
	GROUP BY 
		r.rolname, r.rolsuper, r.rolcreaterole, r.rolcreatedb
	ORDER BY 
		r.rolname;
	`

	// Execute query to get user privileges at the database level
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var username, host, role, privileges string
		if err := rows.Scan(&username, &host, &privileges, &role); err != nil {
			return err
		}

		log.Printf("Database Name: %s, User: %s, Host: %s, Privileges: %s, Role: %s", dbName, username, host, privileges, role)

		// Send the username to the dbUser API
		userPayload := map[string]interface{}{
			"orgId":        orgID,
			"tenantId":     tenantID,
			"databaseType": config.DBType,
			"databaseName": dbName,
			"userName":     username,
			"host":         host,
			"role":         role,
			"privilege":    privileges,
			"instanceId":   instanceId,
			"agentVmIp":    config.AgentVMIP, // Multi-host: Agent VM IP (where ProxySQL runs)
			"hostVmIp":     config.Host,      // Multi-host: Database host VM IP
		}

		userPayloadBytes, err := json.Marshal(userPayload)
		if err != nil {
			log.Printf("Error while marshalling the user payload: %v", err)
			continue
		}

		userAPIURL := config.API + "/api/v1/databaseService/dbUser"
		log.Printf("Sending User Payload to API %s", userAPIURL)

		httpReq, err := http.NewRequest("POST", userAPIURL, bytes.NewBuffer(userPayloadBytes))
		log.Println("Payload Sent:")
		log.Println(string(userPayloadBytes))

		if err != nil {
			log.Printf("Error while creating user request: %v", err)
			continue
		}

		httpReq.Header.Set("Content-Type", "application/json")
		client := &http.Client{}
		httpResp, err := client.Do(httpReq)
		if err != nil {
			log.Printf("Error while sending user request: %v", err)
			continue
		}
		defer httpResp.Body.Close()

		userResponseBody, err := io.ReadAll(httpResp.Body)
		if err != nil {
			log.Printf("Error while reading user response body: %v", err)
			continue
		}
		log.Printf("Response from dbUser API: %v", string(userResponseBody))
	}

	return nil
}
