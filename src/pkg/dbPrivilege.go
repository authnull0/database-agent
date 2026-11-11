package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/lib/pq"
)

// FetchTablePrivileges reports each login with its privileges on the database (PostgreSQL roles,
// MySQL accounts)
func FetchTablePrivileges(db *sql.DB, dbName string, config DBConfig, instanceId string) error {
	users, err := databaseUsers(db, config, dbName)
	if err != nil {
		return err
	}
	reportDatabaseUsers(users, dbName, config, instanceId)
	return nil
}

// syncSummary is the one line a sync logs per database at the default level.
func syncSummary(dbName string, config DBConfig, users, failed int) string {
	return fmt.Sprintf("Synced %s (%s, %s:%d): %d users, %d failed",
		dbName, Engine(config.DBType), config.Host, targetPort(config), users, failed)
}

// reportDatabaseUsers posts each login to the control plane's dbUser API. A failure is logged with
// its user (and status, for an answer other than 200); the per-user detail only at LOG_LEVEL=debug;
// and then one summary line for the database.
func reportDatabaseUsers(users []databaseUser, dbName string, config DBConfig, instanceId string) {
	// Agent VM IP for agent_vm_ip (AGENT_VM_IP -> private IP; never the public/ipify address)
	publicIP := AgentIdentity(config)
	userAPIURL := config.API + "/api/v1/databaseService/dbUser"
	client := &http.Client{}
	failed := 0
	for _, u := range users {
		if err := postDatabaseUser(client, userAPIURL, dbUserPayload(config, dbName, instanceId, publicIP, u), u, dbName); err != nil {
			failed++
			log.Printf("Reporting user %s of %s on %s: %v", u.Username, dbName, config.Host, err)
		}
	}
	log.Print(syncSummary(dbName, config, len(users), failed))
}

// postDatabaseUser sends one login's payload. Anything but a 200 is an error.
func postDatabaseUser(client *http.Client, userAPIURL string, userPayload interface{}, u databaseUser, dbName string) error {
	logDebugf("Database Name: %s, User: %s, Host: %s, Privileges: %s, Role: %s, Tables: %d",
		dbName, u.Username, u.Host, u.Privileges, u.Role, len(u.TablePrivileges))

	userPayloadBytes, err := json.Marshal(userPayload)
	if err != nil {
		return fmt.Errorf("marshalling the payload: %w", err)
	}

	logDebugf("Sending User Payload to API %s", userAPIURL)
	httpReq, err := http.NewRequest("POST", userAPIURL, bytes.NewBuffer(userPayloadBytes))
	if err != nil {
		return fmt.Errorf("creating the request: %w", err)
	}
	logDebugf("Payload Sent: %s", jsonForLog(userPayloadBytes))

	httpReq.Header.Set("Content-Type", "application/json")
	httpResp, err := doAPI(client, httpReq)
	if err != nil {
		return fmt.Errorf("sending: %w", err)
	}
	defer httpResp.Body.Close()

	userResponseBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return fmt.Errorf("reading the response: %w", err)
	}
	logDebugf("Response from dbUser API: %v", string(userResponseBody))
	if httpResp.StatusCode != http.StatusOK {
		return fmt.Errorf("the control plane answered %d: %s", httpResp.StatusCode, jsonForLog(userResponseBody))
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
