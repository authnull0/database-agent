package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/lib/pq"
)

// FetchTablePrivileges reports each login with its privileges on the database (PostgreSQL roles,
// MySQL accounts)
func FetchTablePrivileges(db *sql.DB, dbName string, config DBConfig, instanceId string) error {
	// Agent VM IP for agent_vm_ip (AGENT_VM_IP -> private IP; never the public/ipify address)
	publicIP := AgentIdentity(config)

	users, err := databaseUsers(db, config, dbName)
	if err != nil {
		return err
	}

	for _, u := range users {
		log.Printf("Database Name: %s, User: %s, Host: %s, Privileges: %s, Role: %s, Tables: %d",
			dbName, u.Username, u.Host, u.Privileges, u.Role, len(u.TablePrivileges))

		// Send the username to the dbUser API
		userPayload := dbUserPayload(config, dbName, instanceId, publicIP, u)

		userPayloadBytes, err := json.Marshal(userPayload)
		if err != nil {
			log.Printf("Error while marshalling the user payload: %v", err)
			continue
		}

		userAPIURL := config.API + "/api/v1/databaseService/dbUser"
		log.Printf("Sending User Payload to API %s", userAPIURL)

		httpReq, err := http.NewRequest("POST", userAPIURL, bytes.NewBuffer(userPayloadBytes))
		log.Println("Payload Sent:")
		log.Println(jsonForLog(userPayloadBytes))

		if err != nil {
			log.Printf("Error while creating user request: %v", err)
			continue
		}

		httpReq.Header.Set("Content-Type", "application/json")
		client := &http.Client{}
		httpResp, err := doAPI(client, httpReq)
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

// rolePrivilegesQuery lists the login roles with their privileges on the database $1 (CONNECT,
// CREATE and TEMPORARY: each one held, through PUBLIC or a role it inherits from included), the
// console's Admin/User label, and the attributes privilegeLevel reads. A superuser holds ALL
// PRIVILEGES. The table privileges are read separately, from a connection to that database
// (pgTablePrivilegesQuery).
var rolePrivilegesQuery = `
	SELECT
		r.rolname AS username,
		'%' AS host,  -- PostgreSQL doesn't have host concepts like MySQL, using % for compatibility
		CASE 
			WHEN r.rolsuper THEN 'ALL PRIVILEGES'
			ELSE array_to_string(ARRAY(
				SELECT p FROM unnest(ARRAY['CONNECT', 'CREATE', 'TEMPORARY']) AS p
				WHERE has_database_privilege(r.oid, $1::text, p)
				ORDER BY p
			), ', ')
		END AS privileges,
		CASE 
			WHEN r.rolsuper THEN 'Admin'
			WHEN r.rolcreaterole OR r.rolcreatedb THEN 'Admin'
			ELSE 'User'
		END AS role` + roleAttributesColumns + `
	FROM 
		pg_roles r
	WHERE 
		r.rolname NOT IN ('postgres', 'pg_signal_backend', 'pg_read_all_settings', 
						  'pg_read_all_stats', 'pg_stat_scan_tables', 'pg_monitor', 
						  'pg_database_owner')
		AND r.rolcanlogin = true
	ORDER BY 
		r.rolname;
	`

// pgTablePrivilegesQuery is, in one pass over the connected database, each login role (superusers
// aside: they hold everything) with each table, view, materialized view or foreign table it holds
// a privilege on and the privileges it holds there, those it has through a role it inherits from
// included. The system schemas (pg_catalog, information_schema, TOAST and temporary ones) are left
// out.
const pgTablePrivilegesQuery = `
	SELECT rolname, tbl, privs FROM (
		SELECT r.rolname, n.nspname || '.' || c.relname AS tbl,
		       ARRAY(SELECT p FROM unnest(ARRAY['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE',
		                                        'REFERENCES', 'TRIGGER']) AS p
		             WHERE has_table_privilege(r.oid, c.oid, p) ORDER BY p) AS privs
		FROM pg_roles r
		CROSS JOIN pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE r.rolcanlogin AND NOT r.rolsuper
		  AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		  AND n.nspname NOT LIKE 'pg\_toast%'
		  AND n.nspname NOT LIKE 'pg\_temp\_%'
	) t
	WHERE cardinality(privs) > 0
	ORDER BY rolname, tbl`

// pgTablePrivileges reads pgTablePrivilegesQuery from conn, a connection to the database: each
// login's tables with their privileges.
func pgTablePrivileges(conn *sql.DB) (map[string][]tablePrivilege, error) {
	rows, err := conn.Query(pgTablePrivilegesQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]tablePrivilege{}
	for rows.Next() {
		var role string
		var tp tablePrivilege
		if err := rows.Scan(&role, &tp.Table, pq.Array(&tp.Privileges)); err != nil {
			return nil, err
		}
		out[role] = append(out[role], tp)
	}
	return out, rows.Err()
}
