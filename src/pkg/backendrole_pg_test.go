package pkg

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/lib/pq"
)

// Restricted roles against a real Postgres, logging in AS the role: proof that the database refuses
// what the policy did not grant, including a write hidden inside a function -- the gap no
// query-text check can close.
//
// Gated on AGENT_TEST_DSN and skipped when unset. Point it at a DISPOSABLE database whose user has
// CREATEROLE and may create tables (the tables it creates are owned by it, so it can grant on
// them). The roles it makes are cluster-wide and are dropped at the end.
//
//	AGENT_TEST_DSN='postgres://agent_it:pw@localhost:5432/agent_it_db?sslmode=disable' \
//	  go test ./src/pkg/ -run TestPostgresBackendRole -v
func agentTestDB(t *testing.T) (*sql.DB, *url.URL) {
	t.Helper()
	dsn := os.Getenv("AGENT_TEST_DSN")
	if dsn == "" {
		t.Skip("AGENT_TEST_DSN is not set; skipping the Postgres round trip")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("AGENT_TEST_DSN: %v", err)
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, s := range []string{
		"DROP TABLE IF EXISTS br_orders, br_customers, br_secret",
		"DROP FUNCTION IF EXISTS br_sneaky_write()",
		"CREATE TABLE br_customers (id serial PRIMARY KEY, name text)",
		"CREATE TABLE br_orders (id serial PRIMARY KEY, amount int)",
		"CREATE TABLE br_secret (id int)",
		"INSERT INTO br_customers (name) VALUES ('a'), ('b')",
		// A function that writes, called from a SELECT: invisible to a query-text check.
		`CREATE FUNCTION br_sneaky_write() RETURNS int LANGUAGE sql AS
		   'INSERT INTO br_customers (name) VALUES (''sneaky'') RETURNING id'`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("setup %q: %v", s, err)
		}
	}
	return db, u
}

// loginAs connects as role with a password the test sets (ProvisionBackendRole keeps its own,
// handing it only to ProxySQL).
func loginAs(t *testing.T, admin *sql.DB, base *url.URL, role string) *sql.DB {
	t.Helper()
	pw := "it_" + strings.TrimPrefix(role, "an_")
	if _, err := admin.Exec(fmt.Sprintf("ALTER ROLE %s PASSWORD '%s'", pq.QuoteIdentifier(role), pw)); err != nil {
		t.Fatalf("set test password: %v", err)
	}
	u := *base
	u.User = url.UserPassword(role, pw)
	db, err := sql.Open("postgres", u.String())
	if err != nil {
		t.Fatalf("open as %s: %v", role, err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("log in as %s: %v", role, err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// dropRole removes a test role. DROP OWNED BY needs the role's own rights, which the test user
// (CREATEROLE, not superuser) lacks, so the grants are revoked explicitly first.
func dropRole(t *testing.T, admin *sql.DB, role string) {
	t.Cleanup(func() {
		q := pq.QuoteIdentifier(role)
		var db string
		admin.QueryRow("SELECT current_database()").Scan(&db)
		for _, s := range []string{
			"REVOKE ALL ON ALL TABLES IN SCHEMA public FROM " + q,
			"REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM " + q,
			"REVOKE ALL ON SCHEMA public FROM " + q,
			"REVOKE ALL ON DATABASE " + pq.QuoteIdentifier(db) + " FROM " + q,
			"DROP ROLE IF EXISTS " + q,
		} {
			if _, err := admin.Exec(s); err != nil {
				t.Errorf("cleanup %q: %v", s, err)
			}
		}
	})
}

func mustFail(t *testing.T, db *sql.DB, what, sql string) {
	t.Helper()
	if _, err := db.Exec(sql); err == nil {
		t.Errorf("%s: %q succeeded, want permission denied", what, sql)
	} else if !strings.Contains(err.Error(), "permission denied") && !strings.Contains(err.Error(), "must be owner") {
		// DDL on another role's table is refused as "must be owner of table ...".
		t.Errorf("%s: %q failed with %v, want permission denied", what, sql, err)
	}
}

func mustWork(t *testing.T, db *sql.DB, what, sql string) {
	t.Helper()
	if _, err := db.Exec(sql); err != nil {
		t.Errorf("%s: %q: %v", what, sql, err)
	}
}

func TestPostgresBackendRoleRead(t *testing.T) {
	admin, base := agentTestDB(t)
	dbName := strings.TrimPrefix(base.Path, "/")
	privs, tables := []string{"READ"}, []string{"br_customers", "br_orders"}

	name, err := ProvisionBackendRole(admin, nil, dbName, "app_role", privs, tables, 0)
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	if want, _ := BackendRoleName(dbName, "app_role", privs, tables); name != want {
		t.Fatalf("name %q, want %q", name, want)
	}
	dropRole(t, admin, name)

	// Idempotent: a second run (every checkout) succeeds and changes nothing that matters.
	if again, err := ProvisionBackendRole(admin, nil, dbName, "app_role", privs, tables, 0); err != nil || again != name {
		t.Fatalf("second provision: %q %v", again, err)
	}

	// Every restricted role is in one group, so pg_hba.conf needs a single line for all of them:
	//	host <db> +authnull_backend_roles <proxy-ip>/32 scram-sha-256
	var member, groupLogin bool
	if err := admin.QueryRow(`SELECT pg_has_role($1, $2, 'MEMBER'),
		(SELECT rolcanlogin FROM pg_roles WHERE rolname = $2)`, name, BackendRolesGroup).Scan(&member, &groupLogin); err != nil {
		t.Fatalf("membership: %v", err)
	}
	if !member || groupLogin {
		t.Errorf("%s member of %s: %v (want true); group can log in: %v (want false)", name, BackendRolesGroup, member, groupLogin)
	}

	as := loginAs(t, admin, base, name)
	mustWork(t, as, "read a granted table", "SELECT count(*) FROM br_customers")
	mustWork(t, as, "read a granted table", "SELECT count(*) FROM br_orders")
	mustFail(t, as, "read a table outside the policy", "SELECT count(*) FROM br_secret")
	mustFail(t, as, "insert", "INSERT INTO br_customers (name) VALUES ('x')")
	mustFail(t, as, "update", "UPDATE br_customers SET name = 'x'")
	mustFail(t, as, "delete", "DELETE FROM br_customers")
	mustFail(t, as, "write hidden in a function", "SELECT br_sneaky_write()")
	mustFail(t, as, "writing CTE", "WITH d AS (INSERT INTO br_customers (name) VALUES ('x') RETURNING id) SELECT id FROM d")
	mustFail(t, as, "DDL", "DROP TABLE br_orders")
}

func TestPostgresBackendRoleReadWrite(t *testing.T) {
	admin, base := agentTestDB(t)
	dbName := strings.TrimPrefix(base.Path, "/")
	privs, tables := []string{"READ", "WRITE"}, []string{"br_customers"}

	name, err := ProvisionBackendRole(admin, nil, dbName, "app_role", privs, tables, 0)
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	dropRole(t, admin, name)

	as := loginAs(t, admin, base, name)
	mustWork(t, as, "insert using the serial sequence", "INSERT INTO br_customers (name) VALUES ('rw')")
	mustWork(t, as, "update", "UPDATE br_customers SET name = 'rw2' WHERE name = 'rw'")
	mustWork(t, as, "delete", "DELETE FROM br_customers WHERE name = 'rw2'")
	mustFail(t, as, "write a table outside the policy", "INSERT INTO br_orders (amount) VALUES (1)")
	mustFail(t, as, "DDL", "DROP TABLE br_customers")
}

// EXECUTE keeps the policy's own role: nothing is created.
func TestPostgresBackendRoleNotForExecute(t *testing.T) {
	admin, base := agentTestDB(t)
	dbName := strings.TrimPrefix(base.Path, "/")
	name, err := ProvisionBackendRole(admin, nil, dbName, "app_role", []string{"READ", "WRITE", "EXECUTE"}, []string{"br_customers"}, 0)
	if err != nil || name != "" {
		t.Fatalf("EXECUTE policy: %q %v, want no role", name, err)
	}
}
