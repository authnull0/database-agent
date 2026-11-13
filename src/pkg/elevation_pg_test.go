package pkg

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

// elevate and unelevate through the agent's job loop, against a real Postgres (AGENT_TEST_DSN):
// an owner's own account gains a role -- an existing one, or a bundle the agent builds from the
// policy's privileges -- for the grant, and loses it, with its open sessions, when it ends.
func TestElevationJobsThroughTheLoop(t *testing.T) {
	db, u := agentTestDB(t)
	dbName := strings.TrimPrefix(u.Path, "/")
	const owner, owner2, rw, danger, files = "el_owner_it", "el_owner2_it", "el_rw_it", "el_danger_it", "el_files_it"
	bundle, ok := ElevationBundleName(dbName, []string{"SELECT"}, []string{"br_customers"})
	if !ok {
		t.Fatal("no bundle name")
	}
	cleanup := func() {
		for _, r := range []string{owner, owner2, rw, danger, files, bundle} {
			db.Exec("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM " + r)
			db.Exec("REVOKE ALL ON SCHEMA public FROM " + r)
			db.Exec("REVOKE ALL ON DATABASE " + dbName + " FROM " + r)
			db.Exec("DROP ROLE IF EXISTS " + r)
		}
	}
	cleanup()
	t.Cleanup(cleanup)
	for _, s := range []string{
		"CREATE ROLE " + owner + " LOGIN",
		"CREATE ROLE " + owner2 + " LOGIN",
		"CREATE ROLE " + rw + " NOLOGIN",
		"GRANT USAGE ON SCHEMA public TO " + rw,
		"GRANT SELECT, INSERT ON br_customers TO " + rw,
		"GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO " + rw,
		"CREATE ROLE " + danger + " NOLOGIN CREATEROLE",
		"CREATE ROLE " + files + " NOLOGIN",
		"GRANT pg_read_server_files TO " + files,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
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
	job := func(id, kind, role string, p any) map[string]any {
		return map[string]any{"id": id, "kind": kind, "host": "192.168.122.175", "port": 5432, "dbType": "postgres", "role": role, "payload": p}
	}
	run := func(jobs ...map[string]any) map[string]map[string]any {
		t.Helper()
		fb.jobs = jobs
		if _, err := ProcessVaultJobs(db, fp, hg, cfg, time.Now()); err != nil {
			t.Fatal(err)
		}
		out := map[string]map[string]any{}
		for _, r := range fb.results {
			out[r["jobId"].(string)] = r
		}
		fb.results = nil
		return out
	}
	custom := map[string]any{"database": dbName, "privileges": []string{"SELECT"}, "tables": []string{"br_customers"}}

	// Base privileges: the owner's account holds nothing on the table.
	as := loginAs(t, db, u, owner)
	if _, err := as.Exec("SELECT 1 FROM br_customers"); err == nil {
		t.Fatal("the owner reads the table before any elevation")
	}

	got := run(
		job("rw", "elevate", owner, map[string]any{"grant": rw}),
		job("custom", "elevate", owner2, custom),
		job("danger", "elevate", owner, map[string]any{"grant": danger}),
		job("files", "elevate", owner, map[string]any{"grant": files}),
		job("missing", "elevate", "el_nobody_it", map[string]any{"grant": rw}),
	)
	if got["rw"]["ok"] != true || got["custom"]["ok"] != true {
		t.Fatalf("elevations: %v %v", got["rw"], got["custom"])
	}
	for _, id := range []string{"danger", "files"} {
		if r := got[id]; r["ok"] != false || !strings.Contains(r["error"].(string), "cannot be granted") {
			t.Errorf("%s: %v", id, r)
		}
	}
	if r := got["missing"]; r["ok"] != false || !strings.Contains(r["error"].(string), "does not exist") {
		t.Errorf("a missing account: %v", r)
	}
	// A membership the account holds already is refused, not taken over: ending the grant would
	// otherwise take away standing access nobody gave through AuthNull.
	if r := run(job("again", "elevate", owner, map[string]any{"grant": rw}))["again"]; r["ok"] != false || !strings.Contains(r["error"].(string), "already") {
		t.Errorf("an existing membership: %v", r)
	}

	// Elevated: the owner reads and writes through rw, in a session opened before the grant too.
	if _, err := as.Exec("INSERT INTO br_customers (name) VALUES ('elevated')"); err != nil {
		t.Errorf("INSERT through the granted role: %v", err)
	}
	as2 := loginAs(t, db, u, owner2)
	if _, err := as2.Exec("SELECT 1 FROM br_customers"); err != nil {
		t.Errorf("SELECT through the bundle: %v", err)
	}
	if _, err := as2.Exec("INSERT INTO br_customers (name) VALUES ('x')"); err == nil {
		t.Error("the bundle grants INSERT")
	}
	var comment sql.NullString
	db.QueryRow(`SELECT shobj_description(oid, 'pg_authid') FROM pg_roles WHERE rolname = $1`, bundle).Scan(&comment)
	if comment.String != elevationBundleMark {
		t.Errorf("bundle comment %q", comment.String)
	}
	var canLogin bool
	db.QueryRow(`SELECT rolcanlogin FROM pg_roles WHERE rolname = $1`, bundle).Scan(&canLogin)
	if canLogin {
		t.Error("the bundle can log in")
	}

	// Ended: the membership goes, and so do the owner's open sessions.
	var pid int
	if err := as.QueryRow("SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	got = run(
		job("end", "unelevate", owner, map[string]any{"grant": rw}),
		job("end2", "unelevate", owner2, custom),
		job("twice", "unelevate", owner, map[string]any{"grant": rw}),
	)
	for _, id := range []string{"end", "end2", "twice"} {
		if got[id]["ok"] != true {
			t.Errorf("%s: %v", id, got[id])
		}
	}
	var alive bool
	for i := 0; i < 20; i++ {
		db.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)", pid).Scan(&alive)
		if !alive {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if alive {
		t.Error("the owner's session survived the end of the grant")
	}
	// The agent took membership of the account only to end its sessions.
	if member, _, _ := membership(db, owner, currentUser(db)); member {
		t.Error("the agent kept membership of the owner's account")
	}
	as = loginAs(t, db, u, owner)
	if _, err := as.Exec("SELECT 1 FROM br_customers"); err == nil {
		t.Error("the owner still reads the table after the grant ended")
	}
	as2 = loginAs(t, db, u, owner2)
	if _, err := as2.Exec("SELECT 1 FROM br_customers"); err == nil {
		t.Error("owner2 still reads the table after the grant ended")
	}
	// The bundle stays for the next grant: an ended one only loses its member.
	if exists, _ := pgRoleExists(db, bundle); !exists {
		t.Error("the bundle was dropped")
	}
}
