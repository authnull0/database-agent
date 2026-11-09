package pkg

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

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

// provisionNoProxy provisions without registering in ProxySQL, as most tests here need.
func provisionNoProxy(admin *sql.DB, dbName, role string, privs, tables []string) (string, error) {
	return ProvisionBackendRole(admin, nil, "test-host", dbName, role, privs, tables, 0, time.Now(), DefaultPasswordRotation)
}

// canLoginAs reports whether role can log in with password, without changing anything.
func canLoginAs(t *testing.T, base *url.URL, role, password string) bool {
	t.Helper()
	u := *base
	u.User = url.UserPassword(role, password)
	db, err := sql.Open("postgres", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	return db.Ping() == nil
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

	name, err := provisionNoProxy(admin, dbName, "app_role", privs, tables)
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	if want, _ := BackendRoleName(dbName, "app_role", privs, tables); name != want {
		t.Fatalf("name %q, want %q", name, want)
	}
	dropRole(t, admin, name)

	// Idempotent: a second run (every checkout) succeeds and changes nothing that matters.
	if again, err := provisionNoProxy(admin, dbName, "app_role", privs, tables); err != nil || again != name {
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

	name, err := provisionNoProxy(admin, dbName, "app_role", privs, tables)
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
	name, err := provisionNoProxy(admin, dbName, "app_role", []string{"READ", "WRITE", "EXECUTE"}, []string{"br_customers"})
	if err != nil || name != "" {
		t.Fatalf("EXECUTE policy: %q %v, want no role", name, err)
	}
}

// The restricted role's password is a managed role password like any other (S4): 32 characters
// from crypto/rand, kept across checkouts until it is due, then rotated -- at checkout or by the
// scheduled sweep, which finds the role through the rotation record in pgsql_users.
func TestPostgresBackendRolePasswordRotation(t *testing.T) {
	admin, base := agentTestDB(t)
	dbName := strings.TrimPrefix(base.Path, "/")
	privs, tables := []string{"READ"}, []string{"br_customers"}
	proxy := newFakeProxy()
	t0 := time.Now()
	day := 24 * time.Hour

	name, err := ProvisionBackendRole(admin, proxy, "h1", dbName, "app_role", privs, tables, 7, t0, day)
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	dropRole(t, admin, name)
	u1, found, _ := proxy.Get(name)
	if !found || len(u1.Password) != rolePasswordLength || u1.DefaultHostgroup != 7 {
		t.Fatalf("ProxySQL row %+v (found %v), want a %d-character password on hostgroup 7", u1, found, rolePasswordLength)
	}
	if h, rotated, ok := parseRotationComment(u1.Comment); !ok || h != "h1" || !rotated.Equal(time.Unix(t0.Unix(), 0)) {
		t.Fatalf("rotation record %q, want host h1 rotated at %d", u1.Comment, t0.Unix())
	}
	if !canLoginAs(t, base, name, u1.Password) {
		t.Fatal("the role does not accept the password ProxySQL holds")
	}

	// A checkout before the password is due keeps it and its rotation time.
	if _, err := ProvisionBackendRole(admin, proxy, "h1", dbName, "app_role", privs, tables, 7, t0.Add(time.Hour), day); err != nil {
		t.Fatalf("second provision: %v", err)
	}
	if u2, _, _ := proxy.Get(name); u2.Password != u1.Password || u2.Comment != u1.Comment {
		t.Errorf("a checkout before the password was due changed it: %+v -> %+v", u1, u2)
	}

	// A checkout after it is due rotates it; the old one stops working.
	if _, err := ProvisionBackendRole(admin, proxy, "h1", dbName, "app_role", privs, tables, 7, t0.Add(25*time.Hour), day); err != nil {
		t.Fatalf("provision when due: %v", err)
	}
	u3, _, _ := proxy.Get(name)
	if u3.Password == u1.Password {
		t.Fatal("a checkout after the password was due kept it")
	}
	if !canLoginAs(t, base, name, u3.Password) || canLoginAs(t, base, name, u1.Password) {
		t.Fatal("after rotation the role must take the new password and refuse the old one")
	}

	// The scheduled sweep rotates it too, and the role stays a restricted one.
	rotated, err := RotateDuePasswords(admin, proxy, "h1", t0.Add(50*time.Hour), day)
	if err != nil || len(rotated) != 1 || rotated[0] != name {
		t.Fatalf("scheduled rotation: %v %v, want [%s]", rotated, err, name)
	}
	u4, _, _ := proxy.Get(name)
	if u4.Password == u3.Password || u4.DefaultHostgroup != 7 || !canLoginAs(t, base, name, u4.Password) {
		t.Fatalf("after the sweep: %+v", u4)
	}
	var inherit, member bool
	if err := admin.QueryRow(`SELECT rolinherit, pg_has_role($1, $2, 'MEMBER') FROM pg_roles WHERE rolname = $1`,
		name, BackendRolesGroup).Scan(&inherit, &member); err != nil {
		t.Fatal(err)
	}
	if inherit || !member {
		t.Errorf("after rotation: inherit %v (want false), member of %s %v (want true)", inherit, BackendRolesGroup, member)
	}
	mustFail(t, loginAs(t, admin, base, name), "insert after rotation", "INSERT INTO br_customers (name) VALUES ('x')")
}

// A policy granting SELECT and INSERT (custom privileges): its restricted role can insert and
// cannot update or delete -- the database refuses, whatever ProxySQL lets through.
func TestPostgresBackendRoleVerbs(t *testing.T) {
	admin, base := agentTestDB(t)
	dbName := strings.TrimPrefix(base.Path, "/")
	name, err := provisionNoProxy(admin, dbName, "verbs_role", []string{"SELECT", "INSERT"}, []string{"br_customers"})
	if err != nil || name == "" {
		t.Fatalf("provision: %q %v", name, err)
	}
	dropRole(t, admin, name)
	db := loginAs(t, admin, base, name)
	if _, err := db.Exec("INSERT INTO br_customers (name) VALUES ('verbs')"); err != nil {
		t.Errorf("INSERT granted: %v", err)
	}
	if _, err := db.Exec("SELECT count(*) FROM br_customers"); err != nil {
		t.Errorf("SELECT granted: %v", err)
	}
	for _, q := range []string{"UPDATE br_customers SET name = 'x'", "DELETE FROM br_customers", "TRUNCATE br_customers"} {
		if _, err := db.Exec(q); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s: %v, want permission denied", q, err)
		}
	}
}
