package pkg

import (
	"database/sql"
	"net/url"
	"strings"
	"testing"
	"time"
)

// Console-created roles against a real Postgres, logging in AS the role: what the payload grants
// works, what it does not is refused, a narrower payload takes back the rest, future tables follow
// the grant, a role the console did not create is never touched, and a drop ends the role's
// sessions. Gated on AGENT_TEST_DSN, as backendrole_pg_test.go.
func TestPostgresConsoleRole(t *testing.T) {
	db, u := agentTestDB(t)
	dbName := strings.TrimPrefix(u.Path, "/")
	const role, plain = "ac_console_it", "ac_plain_it"
	open := func(name string) (*sql.DB, error) {
		v := *u
		v.Path = "/" + name
		return sql.Open("postgres", v.String())
	}
	asRole := func(pw string) *sql.DB {
		v := *u
		v.User = url.UserPassword(role, pw)
		c, err := sql.Open("postgres", v.String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	cleanup := func() {
		db.Exec("DROP TABLE IF EXISTS br_later, br_later2")
		for _, r := range []string{role, plain} {
			db.Exec("GRANT " + r + " TO CURRENT_USER")
			db.Exec("REASSIGN OWNED BY " + r + " TO CURRENT_USER")
			db.Exec("DROP OWNED BY " + r)
			db.Exec("DROP ROLE IF EXISTS " + r)
		}
	}
	cleanup()
	t.Cleanup(cleanup)
	proxy := newFakeProxy()
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	pw, _ := GenerateRandomPassword(32)

	// Create: SELECT on one table, nothing else.
	p := rolePayload{Version: 1, Create: true, ValidUntil: "2030-01-01T00:00:00Z", Grants: roleGrants{Databases: []roleDBGrants{{Database: dbName, Objects: []roleObjectGrant{
		{Type: "schema", Schema: "public", Privileges: []string{"USAGE"}},
		{Type: "table", Schema: "public", Name: "br_customers", Privileges: []string{"SELECT"}},
	}}}}}
	if warn, err := applyConsoleRole(db, open, proxy, "192.168.122.175", 7, role, p, pw, now); err != nil || warn != "" {
		t.Fatalf("create: %q %v", warn, err)
	}
	var comment string
	var inherit bool
	var until *time.Time
	db.QueryRow(`SELECT shobj_description(oid, 'pg_authid'), rolinherit, rolvaliduntil FROM pg_roles WHERE rolname = $1`, role).
		Scan(&comment, &inherit, &until)
	if comment != consoleRoleMark || inherit || until == nil || until.Year() != 2030 {
		t.Errorf("role attributes: comment %q inherit %v until %v", comment, inherit, until)
	}
	if g, _ := roleGroups(db, role); !g[ManagedGroup] {
		t.Errorf("not in %s: pg_hba would let it in directly", ManagedGroup)
	}
	if u, ok := proxy.users[role]; !ok || u.Password != pw || u.DefaultHostgroup != 7 || !isVaultManaged(u.Comment) {
		t.Errorf("ProxySQL row: %+v %v", u, ok)
	}
	c := asRole(pw)
	mustRun := func(why, q string) {
		t.Helper()
		if _, err := c.Exec(q); err != nil {
			t.Errorf("%s: %s: %v", why, q, err)
		}
	}
	mustFail := func(why, q string) {
		t.Helper()
		if _, err := c.Exec(q); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s: %s: %v, want permission denied", why, q, err)
		}
	}
	mustRun("granted", "SELECT count(*) FROM br_customers")
	mustFail("not granted", "SELECT count(*) FROM br_orders")
	mustFail("not granted", "INSERT INTO br_customers (name) VALUES ('x')")

	// Read-write on every table, now and future; no new password.
	p = rolePayload{Version: 2, Grants: roleGrants{Databases: []roleDBGrants{{Database: dbName, Objects: []roleObjectGrant{
		{Type: "schema", Schema: "public", Privileges: []string{"USAGE"}},
		{Type: "all_tables", Schema: "public", Privileges: []string{"DELETE", "INSERT", "SELECT", "UPDATE"}, Future: true},
		{Type: "all_sequences", Schema: "public", Privileges: []string{"SELECT", "USAGE"}, Future: true},
	}}}}}
	if warn, err := applyConsoleRole(db, open, proxy, "192.168.122.175", 7, role, p, "", now); err != nil {
		t.Fatalf("read-write: %v", err)
	} else if warn != "" {
		t.Logf("read-write: %s", warn)
	}
	mustRun("read-write", "INSERT INTO br_customers (name) VALUES ('from the console role')")
	mustRun("read-write", "SELECT count(*) FROM br_orders")
	if _, err := db.Exec("CREATE TABLE br_later (id int)"); err != nil {
		t.Fatal(err)
	}
	mustRun("a table created later", "SELECT count(*) FROM br_later")

	// Narrow again: only SELECT on br_orders. Everything else is taken back, future grants too.
	p = rolePayload{Version: 3, Grants: roleGrants{Databases: []roleDBGrants{{Database: dbName, Objects: []roleObjectGrant{
		{Type: "schema", Schema: "public", Privileges: []string{"USAGE"}},
		{Type: "table", Schema: "public", Name: "br_orders", Privileges: []string{"SELECT"}},
	}}}}}
	if _, err := applyConsoleRole(db, open, proxy, "192.168.122.175", 7, role, p, "", now); err != nil {
		t.Fatalf("narrow: %v", err)
	}
	mustRun("still granted", "SELECT count(*) FROM br_orders")
	mustFail("taken back", "SELECT count(*) FROM br_customers")
	mustFail("taken back", "SELECT count(*) FROM br_later")
	if _, err := db.Exec("CREATE TABLE br_later2 (id int)"); err != nil {
		t.Fatal(err)
	}
	mustFail("future grant taken back", "SELECT count(*) FROM br_later2")

	// A role the console did not create is refused, whatever the job says.
	if _, err := db.Exec("CREATE ROLE " + plain + " NOLOGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err := applyConsoleRole(db, open, proxy, "h", 7, plain, p, "x", now); err == nil || !strings.Contains(err.Error(), "not created by AuthNull") {
		t.Errorf("apply to an unmarked role: %v", err)
	}
	if err := dropConsoleRole(db, open, proxy, plain, dropPayload{Databases: []string{dbName}}); err == nil {
		t.Error("dropped an unmarked role")
	}

	// Drop: the open session ends, the role and its ProxySQL row go.
	if err := c.Ping(); err != nil {
		t.Fatalf("the role's session before the drop: %v", err)
	}
	if err := dropConsoleRole(db, open, proxy, role, dropPayload{Databases: []string{dbName}}); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if exists, _ := pgRoleExists(db, role); exists {
		t.Error("the role is still there")
	}
	if _, ok := proxy.users[role]; ok {
		t.Error("the ProxySQL row is still there")
	}
	if _, err := c.Exec("SELECT 1"); err == nil {
		t.Error("the role's session survived the drop")
	}
	// Dropping again is done, not an error.
	if err := dropConsoleRole(db, open, proxy, role, dropPayload{Databases: []string{dbName}}); err != nil {
		t.Errorf("drop twice: %v", err)
	}
}

// apply_role and drop_role through the agent's job loop, as the backend sends them: the role is
// created with the job's password and its grants, the result is reported, and the drop removes it.
func TestConsoleRoleJobsThroughTheLoop(t *testing.T) {
	db, u := agentTestDB(t)
	dbName := strings.TrimPrefix(u.Path, "/")
	const role = "ac_loop_it"
	drop := func() {
		db.Exec("GRANT " + role + " TO CURRENT_USER")
		db.Exec("DROP OWNED BY " + role)
		db.Exec("DROP ROLE IF EXISTS " + role)
	}
	drop()
	t.Cleanup(drop)
	prev := openRoleDatabase
	t.Cleanup(func() { openRoleDatabase = prev })
	openRoleDatabase = func(_ DBConfig, name string) (*sql.DB, error) {
		v := *u
		v.Path = "/" + name
		return sql.Open("postgres", v.String())
	}
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeVaultBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	cfg := DBConfig{API: srv.URL, Host: "192.168.122.175", Port: "5432", DBType: "postgres"}
	fp := newFakeProxy()
	hg := fakeHostgroups{"192.168.122.175": 3}
	const pw = "ConsoleRolePassword0123456789abc"
	fb.password = pw
	payload := map[string]any{"version": 1, "create": true, "grants": map[string]any{"databases": []any{map[string]any{
		"database": dbName, "objects": []any{
			map[string]any{"type": "schema", "schema": "public", "privileges": []string{"USAGE"}},
			map[string]any{"type": "table", "schema": "public", "name": "br_customers", "privileges": []string{"SELECT"}},
		}}}}}
	fb.jobs = []map[string]any{
		{"id": "job-apply", "kind": "apply_role", "host": "192.168.122.175", "port": 5432, "dbType": "postgres", "role": role, "payload": payload},
		{"id": "job-bad", "kind": "apply_role", "host": "192.168.122.175", "port": 5432, "dbType": "postgres", "role": role + "_x",
			"payload": map[string]any{"version": 1, "create": true, "grants": map[string]any{"databases": []any{}}}},
	}
	n, err := ProcessVaultJobs(db, fp, hg, cfg, time.Unix(1793800000, 0))
	if err != nil || n != 1 {
		t.Fatalf("applied %d, %v", n, err)
	}
	got := map[string]map[string]any{}
	for _, r := range fb.results {
		got[r["jobId"].(string)] = r
	}
	if got["job-apply"]["ok"] != true {
		t.Fatalf("apply: %v", got["job-apply"])
	}
	if r := got["job-bad"]; r["ok"] != false || !strings.Contains(r["error"].(string), "grants nothing") {
		t.Errorf("a bad payload: %v", r)
	}
	if !canLoginAs(t, u, role, pw) {
		t.Fatal("the new role cannot log in with the job's password")
	}
	if fp.users[role].Password != pw || fp.users[role].DefaultHostgroup != 3 {
		t.Errorf("ProxySQL row: %+v", fp.users[role])
	}

	fb.results = nil
	fb.jobs = []map[string]any{{"id": "job-drop", "kind": "drop_role", "host": "192.168.122.175", "port": 5432, "dbType": "postgres",
		"role": role, "payload": map[string]any{"databases": []string{dbName}}}}
	if n, err := ProcessVaultJobs(db, fp, hg, cfg, time.Now()); err != nil || n != 1 || fb.results[0]["ok"] != true {
		t.Fatalf("drop: %d %v %v", n, err, fb.results)
	}
	if exists, _ := pgRoleExists(db, role); exists {
		t.Error("the role survived its drop job")
	}
	if _, ok := fp.users[role]; ok {
		t.Error("the ProxySQL row survived the drop job")
	}
}
