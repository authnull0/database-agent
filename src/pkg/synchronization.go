package pkg

import (
	"database/sql"
	"fmt"
	"log"
)

var config DBConfig

func ConnectToDB(config DBConfig) error {
	log.Default().Println("Inside the connect to db")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", config.DBUserName, config.DBPassword, config.DBHost, config.DBPort)

	db, err := sql.Open(config.DBType, dsn)
	if err != nil {
		log.Default().Printf("Error opening DB connection: %v", err)
		return err
	}
	log.Default().Println("DB connection opened")

	query := `SELECT 
    SUBSTRING_INDEX(p.grantee, '@', 1) AS username,
    SUBSTRING_INDEX(p.grantee, '@', -1) AS host,
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM information_schema.user_privileges 
            WHERE grantee = p.grantee 
              AND privilege_type IN ('GRANT OPTION', 'CREATE', 'DROP', 'ALTER', 'INDEX')
        ) THEN 'Admin'
        ELSE 'User'
    END AS role,
    GROUP_CONCAT(DISTINCT p.privilege_type) AS privileges,
    p.table_schema AS database_name
FROM 
    information_schema.schema_privileges p
WHERE 
    p.table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')  -- Exclude specific system databases
GROUP BY 
    p.grantee, p.table_schema
ORDER BY 
    p.table_schema, username;`

	rows, err := db.Query(query)
	if err != nil {
		log.Default().Printf("Error executing query: %v", err)
		return err
	}
	defer rows.Close() // Move defer to immediately after checking for query error

	log.Default().Println("Query Executed..")

	// Check if we have any rows
	if !rows.Next() {
		log.Println("No rows returned from the query.")
		return nil // or return an appropriate error
	}

	// If we reach here, we have rows to process
	for {
		var databasename, username, host, role, privileges string
		if err := rows.Scan(&username, &host, &role, &privileges, &databasename); err != nil {
			return err
		}
		log.Printf("Database Name: %s, User: %s, Host: %s, Privileges: %s, Role: %s", databasename, username, host, privileges, role)

		if !rows.Next() {
			break // Exit the loop if there are no more rows
		}
	}

	if err = rows.Err(); err != nil {
		return err
	}

	return nil
}
