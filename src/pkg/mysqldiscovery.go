package pkg

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// What a sync reads from a MySQL target: the same payloads as for PostgreSQL, from MySQL's own
// catalog. The agent's account needs SELECT on mysql.* (accounts and grants) and PROCESS.

// databaseUser is one login as the dbUser payload carries it.
type databaseUser struct {
	Username, Host string
	Privileges     string // the database's privileges, "SELECT, INSERT" or "ALL PRIVILEGES"
	Role           string // the console's label: Admin or User
	Level          string // privilegeLevel: admin, elevated or standard
}

// mysqlSystemAccount is an account MySQL creates for itself (mysql.sys, mysql.session, ...).
func mysqlSystemAccount(user string) bool { return strings.HasPrefix(user, "mysql.") }

// listDatabases is the target's databases, without the system ones.
func listDatabases(db *sql.DB, config DBConfig) ([]string, error) {
	// PostgreSQL query to list databases
	databasesQuery := "SELECT datname FROM pg_database WHERE datistemplate = false"
	systemKey := config.DBType
	if isMySQL(config) {
		databasesQuery = "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME"
		systemKey = EngineMySQL
	}
	databases, err := queryStrings(db, databasesQuery)
	if err != nil {
		log.Printf("Error querying databases: %v", err)
		return nil, err
	}
	var out []string
	for _, dbName := range databases {
		// Skip system databases
		if isSystemDatabase(dbName, systemKey) {
			log.Printf("Skipping system database: %s", dbName)
			continue
		}
		out = append(out, dbName)
	}
	return out, nil
}

// serverUptime is how long the server has been up, in seconds.
func serverUptime(db *sql.DB, config DBConfig) (int, error) {
	var uptime int
	if isMySQL(config) {
		var name string
		err := db.QueryRow("SHOW GLOBAL STATUS LIKE 'Uptime'").Scan(&name, &uptime)
		return uptime, err
	}
	// PostgreSQL query to check uptime
	err := db.QueryRow("SELECT EXTRACT(EPOCH FROM current_timestamp - pg_postmaster_start_time())::integer AS uptime").Scan(&uptime)
	return uptime, err
}

// databaseUsers lists the logins with their privileges on dbName.
func databaseUsers(db *sql.DB, config DBConfig, dbName string) ([]databaseUser, error) {
	if isMySQL(config) {
		return mysqlDatabaseUsers(db, dbName)
	}
	rows, err := db.Query(rolePrivilegesQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []databaseUser
	for rows.Next() {
		var u databaseUser
		var attrs roleAttributes
		if err := rows.Scan(&u.Username, &u.Host, &u.Privileges, &u.Role,
			&attrs.Super, &attrs.CreateRole, &attrs.CreateDB, &attrs.Replication, &attrs.BypassRLS,
			&attrs.ServerAccess, &attrs.AllData); err != nil {
			return nil, err
		}
		u.Level = privilegeLevel(attrs)
		out = append(out, u)
	}
	return out, rows.Err()
}

// databaseTables lists dbName's tables with their columns: PostgreSQL's schema public, through a
// connection to that database; MySQL's tables (views aside), through the server connection.
func databaseTables(db *sql.DB, config DBConfig, dbName string) (map[string][]string, error) {
	if isMySQL(config) {
		return mysqlTables(db, dbName)
	}
	// Create a new connection to the specific database
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.Host, config.Port, config.User, config.Password, dbName)

	specificDb, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Printf("Error connecting to specific database %s: %v", dbName, err)
		return nil, err
	}
	defer specificDb.Close()

	// Test the connection
	err = specificDb.Ping()
	if err != nil {
		log.Printf("Cannot ping database %s: %v", dbName, err)
		return nil, err
	}

	log.Printf("Successfully connected to database: %s", dbName)
	return pgTables(specificDb)
}

// --- MySQL ---

// mysqlDatabasePrivileges are the privileges a grant on a database can hold, in the order the
// payload lists them. GRANT OPTION is not among them: it is a flag on a grant, not a privilege.
var mysqlDatabasePrivileges = []string{
	"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "REFERENCES", "INDEX", "ALTER",
	"CREATE TEMPORARY TABLES", "LOCK TABLES", "EXECUTE", "CREATE VIEW", "SHOW VIEW",
	"CREATE ROUTINE", "ALTER ROUTINE", "EVENT", "TRIGGER",
}

// mysqlPrivilegeString is the payload's privilege string for what an account holds on a database:
// the database-level privileges among privs, in mysqlDatabasePrivileges' order, or ALL PRIVILEGES
// when it holds every one -- as the PostgreSQL path says for a superuser.
func mysqlPrivilegeString(privs []string) string {
	held := map[string]bool{}
	for _, p := range privs {
		held[strings.ToUpper(strings.TrimSpace(p))] = true
	}
	var out []string
	for _, p := range mysqlDatabasePrivileges {
		if held[p] {
			out = append(out, p)
		}
	}
	if len(out) == len(mysqlDatabasePrivileges) {
		return "ALL PRIVILEGES"
	}
	return strings.Join(out, ", ")
}

// mysqlRoleAttributes reads an account's global privileges (on *.*) as the attributes
// privilegeLevel weighs, so MySQL accounts get the same three levels as PostgreSQL roles:
//
//	Super         SUPER, CREATE USER (it can change any account's password), or any global
//	              privilege WITH GRANT OPTION. ALL on *.* includes SUPER.
//	ServerAccess  FILE: reads and writes the server's files, as pg_read/write_server_files
//	CreateRole    CREATE ROLE or ROLE_ADMIN
//	CreateDB      CREATE on *.*
//	Replication   REPLICATION SLAVE: reads the binary log, so every change to every table
//	AllData       SELECT, INSERT, UPDATE or DELETE on *.*: every table, as pg_read/write_all_data
//
// Privileges an account has only through a MySQL role are not counted (roles are activated per
// session, and SET ROLE needs no further grant to be refused).
func mysqlRoleAttributes(global []string, grantable bool) roleAttributes {
	var a roleAttributes
	a.Super = grantable
	for _, p := range global {
		switch strings.ToUpper(strings.TrimSpace(p)) {
		case "SUPER", "CREATE USER":
			a.Super = true
		case "FILE":
			a.ServerAccess = true
		case "CREATE ROLE", "ROLE_ADMIN":
			a.CreateRole = true
		case "CREATE":
			a.CreateDB = true
		case "REPLICATION SLAVE":
			a.Replication = true
		case "SELECT", "INSERT", "UPDATE", "DELETE":
			a.AllData = true
		}
	}
	return a
}

// roleLabel is the console's Admin/User label, by the rule of the PostgreSQL query: a superuser, or
// an account that can create roles or databases, is an Admin.
func roleLabel(a roleAttributes) string {
	if a.Super || a.CreateRole || a.CreateDB {
		return "Admin"
	}
	return "User"
}

// mysqlGrantee is how information_schema names an account: 'user'@'host', unescaped.
func mysqlGrantee(user, host string) string { return "'" + user + "'@'" + host + "'" }

// mysqlDatabaseUsers lists the accounts that can log in (not locked, so not MySQL roles either;
// not the anonymous or system accounts) with what they hold on dbName: their global privileges,
// and their grants on any database pattern dbName matches (LIKE, as the server matches them).
func mysqlDatabaseUsers(db *sql.DB, dbName string) ([]databaseUser, error) {
	global := map[string][]string{}
	grantable := map[string]bool{}
	rows, err := db.Query("SELECT GRANTEE, PRIVILEGE_TYPE, IS_GRANTABLE FROM information_schema.USER_PRIVILEGES")
	if err != nil {
		return nil, fmt.Errorf("reading global privileges: %w", err)
	}
	for rows.Next() {
		var grantee, priv, isGrantable string
		if err := rows.Scan(&grantee, &priv, &isGrantable); err != nil {
			rows.Close()
			return nil, err
		}
		global[grantee] = append(global[grantee], priv)
		if isGrantable == "YES" {
			grantable[grantee] = true
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	onDB := map[string][]string{}
	rows, err = db.Query("SELECT GRANTEE, PRIVILEGE_TYPE FROM information_schema.SCHEMA_PRIVILEGES WHERE ? LIKE TABLE_SCHEMA", dbName)
	if err != nil {
		return nil, fmt.Errorf("reading database privileges: %w", err)
	}
	for rows.Next() {
		var grantee, priv string
		if err := rows.Scan(&grantee, &priv); err != nil {
			rows.Close()
			return nil, err
		}
		onDB[grantee] = append(onDB[grantee], priv)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = db.Query("SELECT User, Host FROM mysql.user WHERE account_locked = 'N' AND User <> '' ORDER BY User, Host")
	if err != nil {
		return nil, fmt.Errorf("reading accounts: %w", err)
	}
	defer rows.Close()
	var out []databaseUser
	for rows.Next() {
		var u databaseUser
		if err := rows.Scan(&u.Username, &u.Host); err != nil {
			return nil, err
		}
		if mysqlSystemAccount(u.Username) {
			continue
		}
		g := mysqlGrantee(u.Username, u.Host)
		attrs := mysqlRoleAttributes(global[g], grantable[g])
		u.Privileges = mysqlPrivilegeString(append(append([]string{}, global[g]...), onDB[g]...))
		u.Role = roleLabel(attrs)
		u.Level = privilegeLevel(attrs)
		out = append(out, u)
	}
	return out, rows.Err()
}

// mysqlTables lists dbName's base tables with their columns in table order.
func mysqlTables(db *sql.DB, dbName string) (map[string][]string, error) {
	rows, err := db.Query(`SELECT c.TABLE_NAME, c.COLUMN_NAME FROM information_schema.COLUMNS c
		JOIN information_schema.TABLES t ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
		WHERE c.TABLE_SCHEMA = ? AND t.TABLE_TYPE = 'BASE TABLE'
		ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION`, dbName)
	if err != nil {
		return nil, fmt.Errorf("reading tables: %w", err)
	}
	defer rows.Close()
	tables := map[string][]string{}
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			return nil, err
		}
		tables[table] = append(tables[table], column)
	}
	return tables, rows.Err()
}
