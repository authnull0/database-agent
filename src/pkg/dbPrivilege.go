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

// FetchTablePrivileges fetches the privileges for tables in a database
func FetchTablePrivileges(db *sql.DB, dbName string, config DBConfig) error {
	var query string
	orgID, _ := strconv.Atoi(config.OrgID)
	tenantID, _ := strconv.Atoi(config.TenantID)

	switch config.DBType {
	case "mysql":
		query = `
			SELECT 
				t.table_schema AS database_name, 
				t.table_name AS table_name, 
				SUBSTRING_INDEX(p.grantee, '@', 1) AS username,
				SUBSTRING_INDEX(p.grantee, '@', -1) AS host,
				p.privilege_type
			FROM 
			information_schema.tables t 
				
			JOIN 
				information_schema.table_privileges p
			ON 
				t.table_schema = p.table_schema 
				AND t.table_name = p.table_name
			WHERE 
				t.table_schema = ?`
	case "Postgres":
		query = `
			SELECT 
				n.nspname AS database_name, 
				c.relname AS table_name, 
				u.usename AS username,
				'pghost' AS host, -- Change as per your requirement
				COALESCE(array_agg(DISTINCT p.privilege_type), '{}') AS privileges
			FROM 
				information_schema.role_table_grants p
			JOIN 
				information_schema.tables t ON p.table_name = t.table_name
			JOIN 
				pg_catalog.pg_class c ON t.table_name = c.relname
			JOIN 
				pg_catalog.pg_namespace n ON c.relnamespace = n.oid
			JOIN 
				pg_catalog.pg_user u ON p.grantee = u.usename
			WHERE 
				n.nspname NOT IN ('pg_catalog', 'information_schema') 
				AND n.nspname = ? 
			GROUP BY 
				n.nspname, c.relname, u.usename`
	case "MSSQL":
		query = `
			SELECT 
				s.name AS database_name, 
				t.name AS table_name, 
				p.name AS username,
				s.name AS host,
				priv.permission_name AS privilege_type
			FROM 
				sys.database_permissions priv
			JOIN 
				sys.objects t ON priv.major_id = t.object_id
			JOIN 
				sys.schemas s ON t.schema_id = s.schema_id
			JOIN 
				sys.database_principals p ON priv.grantee_principal_id = p.principal_id
			WHERE 
				s.name = ?`
	case "Oracle":
		query = `
			SELECT 
				t.owner AS database_name, 
				t.table_name AS table_name, 
				p.grantee AS username,
				'pghost' AS host, -- Change as per your requirement
				p.privilege AS privilege_type
			FROM 
				dba_tab_privs p
			JOIN 
				dba_tables t ON p.table_name = t.table_name
			WHERE 
				t.owner = ?`
	default:
		return fmt.Errorf("unsupported database type")
	}

	// Execute query for table privileges
	rows, err := db.Query(query, dbName)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var dbName, tableName, username, host, privilegeType string
		if err := rows.Scan(&dbName, &tableName, &username, &host, &privilegeType); err != nil {
			return err
		}

		log.Printf("Database: %s, Table: %s, User: %s, Host: %s, Privilege: %s", dbName, tableName, username, host, privilegeType)

		// If there are no privileges, log that as well
		if privilegeType == "" {
			log.Printf("Database: %s, Table: %s has no privileges associated.", dbName, tableName)
		}

		// Sync user information with the API
		payload := map[string]interface{}{
			"OrgID":        orgID,
			"tenantId":     tenantID,
			"databaseType": config.DBType,
			"databaseName": dbName,
			"tableName":    tableName,
			"userName":     username,
			"host":         host,
			"privileges":   privilegeType,
		}

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			log.Printf("Error while marshalling the payload: %v", err)

		}

		apiURL := config.API + "/api/v1/dbPrivilege"
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
