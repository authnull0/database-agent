package pkg

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

// apply_policy_role and drop_policy_role through the agent's job loop, against a real Postgres
// (AGENT_TEST_DSN): the backend's reconcile names a policy's restricted role; the agent builds it
// -- only if the name is what the payload's shape gives -- and drops only AuthNull's own.
func TestPolicyRoleJobsThroughTheLoop(t *testing.T) {
	db, u := agentTestDB(t)
	dbName := strings.TrimPrefix(u.Path, "/")
	privs, tables := []string{"SELECT", "INSERT"}, []string{"br_customers"}
	name, ok := BackendRoleName(dbName, "br_source", privs, tables)
	if !ok {
		t.Fatal("no name")
	}
	const plain = "br_plain_it"
	cleanup := func() {
		for _, r := range []string{name, plain} {
			db.Exec("GRANT " + r + " TO CURRENT_USER")
			db.Exec("DROP OWNED BY " + r)
			db.Exec("DROP ROLE IF EXISTS " + r)
		}
	}
	cleanup()
	t.Cleanup(cleanup)
	prev := openRoleDatabase
	t.Cleanup(func() { openRoleDatabase = prev })
	openRoleDatabase = func(_ DBConfig, n string) (*sql.DB, error) {
		v := *u
		v.Path = "/" + n
		return sql.Open("postgres", v.String())
	}
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeVaultBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	cfg := DBConfig{API: srv.URL, Host: "192.168.122.175", Port: "5432", DBType: "postgres"}
	fp := newFakeProxy()
	hg := fakeHostgroups{"192.168.122.175": 6}
	payload := map[string]any{"database": dbName, "sourceRole": "br_source", "privileges": privs, "tables": tables}
	job := func(id, kind, role string, p any) map[string]any {
		return map[string]any{"id": id, "kind": kind, "host": "192.168.122.175", "port": 5432, "dbType": "postgres", "role": role, "payload": p}
	}
	results := func() map[string]map[string]any {
		out := map[string]map[string]any{}
		for _, r := range fb.results {
			out[r["jobId"].(string)] = r
		}
		fb.results = nil
		return out
	}

	fb.jobs = []map[string]any{
		job("apply", "apply_policy_role", name, payload),
		job("forged", "apply_policy_role", "an_0000000000000000", payload),
	}
	if _, err := ProcessVaultJobs(db, fp, hg, cfg, time.Now()); err != nil {
		t.Fatal(err)
	}
	got := results()
	if got["apply"]["ok"] != true {
		t.Fatalf("apply: %v", got["apply"])
	}
	if r := got["forged"]; r["ok"] != false || !strings.Contains(r["error"].(string), "does not match") {
		t.Errorf("a name the shape does not give: %v", r)
	}
	if exists, _ := pgRoleExists(db, "an_0000000000000000"); exists {
		t.Error("a forged name was created")
	}
	// Built as the policy says: in ProxySQL under the server's hostgroup; SELECT and INSERT, no more.
	if row, ok := fp.users[name]; !ok || row.DefaultHostgroup != 6 {
		t.Fatalf("ProxySQL row: %+v %v", row, ok)
	}
	as := loginAs(t, db, u, name)
	if _, err := as.Exec("INSERT INTO br_customers (name) VALUES ('policy role')"); err != nil {
		t.Errorf("INSERT granted: %v", err)
	}
	if _, err := as.Exec("UPDATE br_customers SET name = 'x'"); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("UPDATE not granted: %v", err)
	}

	// Drop: only AuthNull's restricted roles.
	if _, err := db.Exec("CREATE ROLE " + plain + " LOGIN"); err != nil {
		t.Fatal(err)
	}
	fb.jobs = []map[string]any{
		job("drop", "drop_policy_role", name, map[string]any{"databases": []string{dbName}}),
		job("not-ours", "drop_policy_role", plain, map[string]any{"databases": []string{dbName}}),
	}
	if _, err := ProcessVaultJobs(db, fp, hg, cfg, time.Now()); err != nil {
		t.Fatal(err)
	}
	got = results()
	if got["drop"]["ok"] != true {
		t.Fatalf("drop: %v", got["drop"])
	}
	if exists, _ := pgRoleExists(db, name); exists {
		t.Error("the restricted role survived its drop")
	}
	if _, ok := fp.users[name]; ok {
		t.Error("its ProxySQL row survived")
	}
	if r := got["not-ours"]; r["ok"] != false || !strings.Contains(r["error"].(string), "not an AuthNull restricted role") {
		t.Errorf("dropping another role: %v", r)
	}
	if exists, _ := pgRoleExists(db, plain); !exists {
		t.Error("a role that is not AuthNull's was dropped")
	}
}
