package pkg

import (
	//"bytes"
	"database/sql"
	//"encoding/json"
	"fmt"
	//"io/ioutil"
	"log"
	//"net/http"

	"strings"
)

var systemDatabases = map[string][]string{
	"mysql":    {"mysql", "information_schema", "performance_schema", "sys"},
	"Postgres": {"postgres", "template0", "template1"},
	"MSSQL":    {"master", "tempdb", "model", "msdb"},
	"Oracle":   {"SYSTEM", "SYSAUX"},
}

func FetchDb(db *sql.DB, config DBConfig) error {
	var query string
	var excludeDBs string

	// orgID, _ := strconv.Atoi(config.OrgID)
	// tenantID, _ := strconv.Atoi(config.TenantID)

	// Get the system databases for the current DB type and join them into a single string for exclusion
	if sysDBs, ok := systemDatabases[config.DBType]; ok {
		excludeDBs = "'" + strings.Join(sysDBs, "','") + "'" // Format as a string "'db1','db2','db3'"
	}

	switch config.DBType {
	case "mysql":
		query = `
			SELECT 
    table_schema AS database_name,
    CASE 
        WHEN @@read_only = 1 THEN 'In Recovery'
        ELSE 'Active'
    END AS database_status,
    current_user() AS current_user,
    (SELECT 
        CASE 
            WHEN super_priv = 'Y' THEN 'Admin'
            ELSE 'User'
        END 
     FROM mysql.user 
     WHERE user = SUBSTRING_INDEX(current_user(), '@', 1)
     LIMIT 1) AS role,
    table_name,
    privilege_type
FROM 
    information_schema.tables
JOIN 
    information_schema.schema_privileges 
ON 
    tables.table_schema = schema_privileges.table_schema
WHERE 
    table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
ORDER BY 
    table_schema, table_name;
	`
	case "Postgres":
		query = fmt.Sprintf(`
			SELECT 
			    datname AS database_name, 
			    CASE WHEN pg_is_in_recovery() THEN 'In Recovery' ELSE 'Active' END AS status,
			    usename AS current_user,
			    CASE 
			        WHEN pg_roles.rolsuper = TRUE THEN 'Admin' 
			        ELSE 'User' 
			    END AS role,
			    has_table_privilege(usename, tablename, 'SELECT') AS has_select_privilege,
			    has_table_privilege(usename, tablename, 'INSERT') AS has_insert_privilege
			FROM pg_database
			JOIN pg_user ON usename = CURRENT_USER
			JOIN pg_roles ON pg_roles.rolname = usename
			WHERE datname NOT IN (%s);
		`, excludeDBs)
	case "MSSQL":
		query = fmt.Sprintf(`
			SELECT 
			    db.name AS database_name,
			    db.state_desc AS status,
			    sp.name AS username,
			    CASE 
			        WHEN dp.name = 'db_owner' THEN 'Admin' 
			        ELSE 'User' 
			    END AS role,
			    perm.permission_name AS privilege
			FROM sys.databases db
			JOIN sys.database_permissions perm ON perm.major_id = db.database_id
			JOIN sys.database_principals sp ON sp.principal_id = perm.grantee_principal_id
			JOIN sys.database_role_members rm ON rm.member_principal_id = sp.principal_id
			JOIN sys.database_principals dp ON dp.principal_id = rm.role_principal_id
			WHERE db.name NOT IN (%s);
		`, excludeDBs)
	case "Oracle":
		query = fmt.Sprintf(`
			SELECT 
			    d.name AS database_name,
			    CASE WHEN d.open_mode = 'READ WRITE' THEN 'Active' ELSE 'Inactive' END AS status,
			    u.username,
			    (SELECT 
			        CASE 
			            WHEN role = 'DBA' THEN 'Admin' 
			            ELSE 'User' 
			        END 
			     FROM dba_role_privs 
			     WHERE grantee = u.username AND ROWNUM = 1) AS role,
			    p.privilege
			FROM v$database d
			JOIN dba_users u ON u.username = SYS_CONTEXT('USERENV', 'SESSION_USER')
			JOIN dba_tab_privs p ON p.grantee = u.username
			WHERE d.name NOT IN (%s);
		`, excludeDBs)
	default:
		return fmt.Errorf("unsupported database type")
	}

	// Execute the query
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var databaseName, status, userName, role, privilege, tableName string
		if err := rows.Scan(&databaseName, &status, &userName, &role, &privilege, &tableName); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		log.Printf("Database: %s, Status: %s, User: %s, Role: %s, Privilege: %s, Table: %s", databaseName, status, userName, role, privilege, tableName)

		// 	// Sync database information with the API
		// 	payload := map[string]interface{}{
		// 		"orgId":        orgID,
		// 		"tenantId":     tenantID,
		// 		"databaseType": config.DBType,
		// 		"databaseName": databaseName,
		// 		"host":         config.DBHost,
		// 		"port":         config.DBPort,
		// 		"status":       status,
		// 		"uuid":         config.API_KEY,
		// 	}

		// 	payloadBytes, err := json.Marshal(payload)
		// 	if err != nil {
		// 		log.Printf("Error while marshalling the payload: %v", err)

		// 	}

		// 	apiURL := config.API + "/api/v1/databaseService/dbSync"
		// 	httpReq, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
		// 	log.Println("Payload Sent: %s", string(payloadBytes))

		// 	if err != nil {
		// 		log.Printf("Error while creating request: %v", err)

		// 	}

		// 	httpReq.Header.Set("Content-Type", "application/json")

		// 	client := &http.Client{}
		// 	httpResp, err := client.Do(httpReq)
		// 	if err != nil {
		// 		log.Printf("Error while making request: %v", err)

		// 	}
		// 	defer httpResp.Body.Close()

		// 	body, err := ioutil.ReadAll(httpResp.Body)
		// 	if err != nil {
		// 		log.Printf("Error while reading response body: %v", err)

		// 	}

		// 	log.Default().Println("Response from external service: %v", string(body))
	}

	return nil
}
