package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
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

	query = `
			SELECT 
    SUBSTRING_INDEX(p.grantee, '@', 1) AS username,
    SUBSTRING_INDEX(p.grantee, '@', -1) AS host,
    GROUP_CONCAT(p.privilege_type ORDER BY p.privilege_type SEPARATOR ', ') AS privileges,
    CASE 
        WHEN FIND_IN_SET('SUPER', GROUP_CONCAT(p.privilege_type)) > 0 
          OR FIND_IN_SET('CREATE USER', GROUP_CONCAT(p.privilege_type)) > 0
          OR FIND_IN_SET('GRANT OPTION', GROUP_CONCAT(p.privilege_type)) > 0 
        THEN 'Admin'
        ELSE 'User'
    END AS role
FROM 
    information_schema.user_privileges p
GROUP BY 
    username, host;

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

		log.Printf("Database Name: %s, User: %s, Host: %s, Privileges: %s ,Role: %s", dbName, username, host, privileges, role)

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

	}

	return nil
}
