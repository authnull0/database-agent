package pkg

import (
	"database/sql"
	"net"
	"os"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// MySQL integration tests are gated on DBAGENT_TEST_MYSQL_DSN, a go-sql-driver DSN for a DISPOSABLE
// MySQL 8.0 database whose account can CREATE USER, holds ROLE_ADMIN, CONNECTION_ADMIN, PROCESS,
// SELECT on mysql.*, and ALL on the database WITH GRANT OPTION:
//
//	DBAGENT_TEST_MYSQL_DSN='agent_it:pw@tcp(127.0.0.1:3306)/agent_it_db' go test ./src/pkg/ -run MySQL -v
//
// The tests create users and tables whose names start with agent_it or an_, and drop them again.

// mysqlTestTarget is the server as an admin connection, the agent's DBConfig for it, and the database.
func mysqlTestTarget(t *testing.T) (*sql.DB, DBConfig, string) {
	t.Helper()
	dsn := os.Getenv("DBAGENT_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("DBAGENT_TEST_MYSQL_DSN is not set; skipping the MySQL round trip")
	}
	c, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("DBAGENT_TEST_MYSQL_DSN: %v", err)
	}
	host, port, err := net.SplitHostPort(c.Addr)
	if err != nil {
		t.Fatalf("DBAGENT_TEST_MYSQL_DSN address: %v", err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, DBConfig{DBType: "mysql", Host: host, Port: port, User: c.User, Password: c.Passwd}, c.DBName
}
