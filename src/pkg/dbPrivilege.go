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

// FetchUserPrivileges fetches the privileges for users at the database level
func FetchTablePrivileges(db *sql.DB, dbName string, config DBConfig) error {
	var query string
	orgID, _ := strconv.Atoi(config.OrgID)
	tenantID, _ := strconv.Atoi(config.TenantID)

	switch config.DBType {
	case "mysql":
		query = `
			SELECT 
    		SUBSTRING_INDEX(p.grantee, '@', 1) AS username,
    		SUBSTRING_INDEX(p.grantee, '@', -1) AS host,
    		p.privilege_type AS privilege,
    CASE 
        	WHEN p.privilege_type IN ('SUPER', 'CREATE USER', 'GRANT OPTION') THEN 'Admin'
        	ELSE 'User' 
    END AS role,
	GROUP_CONCAT(DISTINCT p.privilege_type) AS privileges
	FROM 
    information_schema.user_privileges p;

`
	case "postgres":
		query = `
			SELECT 
    		r.rolname AS username,
    		'pghost' AS host, -- Change this based on your requirements
    		r.rolname AS privilege,
    CASE 
       		 WHEN rolsuper THEN 'Admin'
       		 ELSE 'User' 
    END AS role
	FROM 
    		pg_catalog.pg_roles r
	WHERE 
    		r.rolname NOT LIKE 'pg_%';
`
	case "mssql":
		query = `
			SELECT 
    		p.name AS username,
    		'localhost' AS host, -- Replace this with actual host if required
    		dp.permission_name AS privilege,
    	CASE 
        	WHEN IS_SRVROLEMEMBER('sysadmin', p.name) = 1 THEN 'Admin'
        	ELSE 'User' 
    	END AS role
	FROM 
    	sys.database_principals p
	LEFT JOIN 
    	sys.database_permissions dp ON dp.grantee_principal_id = p.principal_id
	WHERE 
    	p.type_desc = 'SQL_USER';
`
	case "oracle":
		query = `
			SELECT 
    		grantee AS username,
    		'pghost' AS host, -- Change as per your requirement
    		privilege AS privilege,
    CASE 
        	WHEN granted_role = 'DBA' THEN 'Admin'
        	ELSE 'User' 
    END AS role 
	FROM 
    		dba_sys_privs
	WHERE 
    		grantee NOT IN ('SYS', 'SYSTEM');
`
	default:
		return fmt.Errorf("unsupported database type")
	}

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

		log.Printf("Database Name: %s, User: %s, Host: %s, Privileges: %s ,Role: %s", dbName, username, host, privileges, role)

		// Send the username to the dbUser API
		userPayload := map[string]interface{}{
			"orgID":        orgID,
			"tenantId":     tenantID,
			"databaseType": config.DBType,
			"databaseName": dbName,
			"userName":     username,
			"role":         role,
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

		userResponseBody, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			log.Printf("Error while reading user response body: %v", err)
			continue
		}
		log.Printf("Response from dbUser API: %v", string(userResponseBody))

		// Send the username and privileges to the dbPrivileges API
		privilegePayload := map[string]interface{}{
			"orgID":         orgID,
			"tenantId":      tenantID,
			"databaseType":  config.DBType,
			"databaseName":  dbName,
			"userName":      username,
			"host":          host,
			"privilegeType": privileges,
		}

		privilegePayloadBytes, err := json.Marshal(privilegePayload)
		if err != nil {
			log.Printf("Error while marshalling the privilege payload: %v", err)
			continue
		}

		privilegeAPIURL := config.API + "/api/v1/databaseService/dbPrivilege"
		log.Printf("Sending Privilege Payload to API %s", privilegeAPIURL)

		httpReq, err = http.NewRequest("POST", privilegeAPIURL, bytes.NewBuffer(privilegePayloadBytes))
		log.Println("Payload Sent:")
		log.Println(string(privilegePayloadBytes))

		if err != nil {
			log.Printf("Error while creating privilege request: %v", err)
			continue
		}

		httpReq.Header.Set("Content-Type", "application/json")
		httpResp, err = client.Do(httpReq)
		if err != nil {
			log.Printf("Error while sending privilege request: %v", err)
			continue
		}
		defer httpResp.Body.Close()

		privilegeResponseBody, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			log.Printf("Error while reading privilege response body: %v", err)
			continue
		}
		log.Printf("Response from dbPrivilege API: %v", string(privilegeResponseBody))
	}

	return nil
}
