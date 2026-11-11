package pkg

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// mysqlCanLogin: does user log in to the target with password?
func mysqlCanLogin(t *testing.T, cfg DBConfig, user, password string) bool {
	t.Helper()
	c := cfg
	c.User, c.Password = user, password
	db, err := sql.Open("mysql", mysqlDSN(c, ""))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	return db.Ping() == nil
}

// mysqlShowGrants is SHOW GRANTS for user@'%', sorted.
func mysqlShowGrants(t *testing.T, db *sql.DB, user string) []string {
	t.Helper()
	acct, _ := mysqlAccount(user, "%")
	grants, err := queryStrings(db, "SHOW GRANTS FOR "+acct)
	if err != nil {
		t.Fatalf("SHOW GRANTS FOR %s: %v", acct, err)
	}
	sort.Strings(grants)
	return grants
}

func mysqlDropUsers(db *sql.DB, users ...string) {
	for _, u := range users {
		for _, h := range []string{"%", "localhost"} {
			acct, _ := mysqlAccount(u, h)
			db.Exec("DROP USER IF EXISTS " + acct)
		}
	}
}

func mysqlUserCount(t *testing.T, db *sql.DB, user string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM mysql.user WHERE User = ?", user).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func jobResults(fb *fakeVaultBackend) map[string]map[string]any {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	got := map[string]map[string]any{}
	for _, r := range fb.results {
		got[r["jobId"].(string)] = r
	}
	return got
}

func payloadJSON(t *testing.T, v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// rotate_password on a MySQL target: every host row of the account gets the vault's password, the
// old one stops working, ProxySQL's mysql_users row gets it with the vault mark and host:port --
// and the account the agent logs in as is refused before any password is fetched.
func TestMySQLRotatePasswordJob(t *testing.T) {
	admin, cfg, _ := mysqlTestTarget(t)
	t.Cleanup(resetInstallCredentialForTest)
	const user, missing = "agent_it_rot", "agent_it_rot_missing"
	mysqlDropUsers(admin, user, missing)
	t.Cleanup(func() { mysqlDropUsers(admin, user, missing) })
	for _, s := range []string{
		"CREATE USER 'agent_it_rot'@'%' IDENTIFIED BY 'OldPassword0123456789'",
		"CREATE USER 'agent_it_rot'@'localhost' IDENTIFIED BY 'OldPassword0123456789'",
	} {
		if _, err := admin.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	hashes := map[string]string{}
	rows, err := admin.Query("SELECT Host, authentication_string FROM mysql.user WHERE User = ?", user)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var h, a string
		rows.Scan(&h, &a)
		hashes[h] = a
	}
	rows.Close()
	if len(hashes) != 2 {
		t.Fatalf("host rows: %d", len(hashes))
	}
	fb, srv := newFakeVaultBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	cfg.API = srv.URL
	const newPassword = "VaultHeldMySQLPassword0123456789"
	fb.password = newPassword
	fb.jobs = []map[string]any{
		{"id": "rot", "kind": "rotate_password", "host": cfg.Host, "dbType": "mysql", "role": user},
		{"id": "missing", "kind": "rotate_password", "host": cfg.Host, "dbType": "mysql", "role": missing},
		{"id": "own", "kind": "rotate_password", "host": cfg.Host, "dbType": "mysql", "role": cfg.User},
	}
	fp := newFakeProxy()
	now := time.Unix(1793800000, 0)
	n, err := ProcessVaultJobs(admin, fp, fakeHostgroups{cfg.Host: 6}, cfg, now)
	if err != nil {
		t.Fatal(err)
	}
	res := jobResults(fb)
	if n != 1 || res["rot"]["ok"] != true {
		t.Fatalf("applied %d: %v", n, res)
	}
	if !mysqlCanLogin(t, cfg, user, newPassword) {
		t.Fatal("the account cannot log in with the vault's password")
	}
	if mysqlCanLogin(t, cfg, user, "OldPassword0123456789") {
		t.Fatal("the old password still works")
	}
	// Every host row: neither kept its old hash.
	for h, before := range hashes {
		var after string
		admin.QueryRow("SELECT authentication_string FROM mysql.user WHERE User = ? AND Host = ?", user, h).Scan(&after)
		if after == before {
			t.Errorf("%s@%s kept the old password", user, h)
		}
	}
	u := fp.users[user]
	if u.Password != newPassword || u.DefaultHostgroup != 6 || !isVaultManaged(u.Comment) {
		t.Fatalf("ProxySQL row: hostgroup %d vault %v", u.DefaultHostgroup, isVaultManaged(u.Comment))
	}
	if h, _, _ := parseRotationComment(u.Comment); h != cfg.Host+":"+cfg.Port {
		t.Errorf("comment host %q", h)
	}
	if r := res["missing"]; r["ok"] != false || !strings.Contains(r["error"].(string), "does not exist") {
		t.Errorf("missing: %v", r)
	}
	if mysqlUserCount(t, admin, missing) != 0 {
		t.Error("a rotate job created an account")
	}
	if r := res["own"]; r["ok"] != false || !strings.Contains(r["error"].(string), "agent logs in as") {
		t.Errorf("own login: %v", r)
	}
}

// apply_role / drop_role on MySQL: the account is created with the console mark and exactly the
// payload's grants; a narrower payload takes back the rest; an account the console did not create
// is refused; drop ends its sessions and removes it, and its ProxySQL row.
func TestMySQLConsoleRole(t *testing.T) {
	admin, cfg, dbName := mysqlTestTarget(t)
	t.Cleanup(resetInstallCredentialForTest)
	const user, plain = "agent_it_cr", "agent_it_cr_plain"
	cleanup := func() {
		mysqlDropUsers(admin, user, plain)
		admin.Exec("DROP TABLE IF EXISTS agent_it_cr_t, agent_it_cr_other")
		admin.Exec("DROP PROCEDURE IF EXISTS agent_it_cr_p")
	}
	cleanup()
	t.Cleanup(cleanup)
	for _, s := range []string{
		"CREATE TABLE agent_it_cr_t (id INT)",
		"CREATE TABLE agent_it_cr_other (id INT)",
		"CREATE PROCEDURE agent_it_cr_p() BEGIN SELECT 1; END",
		"CREATE USER 'agent_it_cr_plain'@'%' IDENTIFIED BY 'PlainPassword0123456789'",
	} {
		if _, err := admin.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	fb, srv := newFakeVaultBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	cfg.API = srv.URL
	const pw = "ConsoleUserPassword0123456789ab"
	fb.password = pw
	fp := newFakeProxy()
	hg := fakeHostgroups{cfg.Host: 5}
	run := func(jobs ...map[string]any) map[string]map[string]any {
		t.Helper()
		fb.mu.Lock()
		fb.jobs, fb.results = jobs, nil
		fb.mu.Unlock()
		if _, err := ProcessVaultJobs(admin, fp, hg, cfg, time.Now()); err != nil {
			t.Fatal(err)
		}
		return jobResults(fb)
	}
	job := func(id, kind, role string, payload any) map[string]any {
		return map[string]any{"id": id, "kind": kind, "host": cfg.Host, "dbType": "mysql", "role": role, "payload": payloadJSON(t, payload)}
	}
	wide := map[string]any{"version": 1, "create": true, "validUntil": "2030-01-01T00:00:00Z", "grants": map[string]any{"databases": []any{
		map[string]any{"database": dbName, "objects": []any{
			map[string]any{"type": "database", "privileges": []string{"SELECT", "SHOW VIEW"}},
			map[string]any{"type": "table", "name": "agent_it_cr_t", "privileges": []string{"SELECT", "INSERT"}},
			map[string]any{"type": "procedure", "name": "agent_it_cr_p", "privileges": []string{"EXECUTE"}},
		}},
	}}}
	res := run(job("create", "apply_role", user, wide))
	if res["create"]["ok"] != true {
		t.Fatalf("create: %v", res["create"])
	}
	esc := strings.ReplaceAll(dbName, "_", `\_`)
	want := []string{
		"GRANT EXECUTE ON PROCEDURE `" + dbName + "`.`agent_it_cr_p` TO `agent_it_cr`@`%`",
		"GRANT SELECT, INSERT ON `" + dbName + "`.`agent_it_cr_t` TO `agent_it_cr`@`%`",
		"GRANT SELECT, SHOW VIEW ON `" + esc + "`.* TO `agent_it_cr`@`%`",
		"GRANT USAGE ON *.* TO `agent_it_cr`@`%`",
	}
	if got := mysqlShowGrants(t, admin, user); !reflect.DeepEqual(got, want) {
		t.Fatalf("grants after create:\n got %q\nwant %q", got, want)
	}
	var attr string
	admin.QueryRow("SELECT ATTRIBUTE FROM information_schema.USER_ATTRIBUTES WHERE USER = ? AND HOST = '%'", user).Scan(&attr)
	if !strings.Contains(attr, `"authnull": "console"`) {
		t.Errorf("attribute %q", attr)
	}
	if !mysqlCanLogin(t, cfg, user, pw) {
		t.Fatal("the new account cannot log in")
	}
	if u := fp.users[user]; u.Password != pw || u.DefaultHostgroup != 5 || !isVaultManaged(u.Comment) {
		t.Fatalf("ProxySQL row: hostgroup %d", u.DefaultHostgroup)
	}

	// Narrower: SELECT on the other table only. Everything else is taken back.
	narrow := map[string]any{"version": 1, "grants": map[string]any{"databases": []any{
		map[string]any{"database": dbName, "objects": []any{
			map[string]any{"type": "table", "name": "agent_it_cr_other", "privileges": []string{"SELECT"}},
		}},
	}}}
	if res := run(job("narrow", "apply_role", user, narrow)); res["narrow"]["ok"] != true {
		t.Fatalf("narrow: %v", res["narrow"])
	}
	want = []string{
		"GRANT SELECT ON `" + dbName + "`.`agent_it_cr_other` TO `agent_it_cr`@`%`",
		"GRANT USAGE ON *.* TO `agent_it_cr`@`%`",
	}
	if got := mysqlShowGrants(t, admin, user); !reflect.DeepEqual(got, want) {
		t.Fatalf("grants after narrowing:\n got %q\nwant %q", got, want)
	}
	if !mysqlCanLogin(t, cfg, user, pw) {
		t.Fatal("an update without a password changed the password")
	}

	// An account the console did not create: neither changed nor dropped.
	res = run(job("plain-apply", "apply_role", plain, wide), job("plain-drop", "drop_role", plain, map[string]any{"databases": []string{dbName}}))
	for _, id := range []string{"plain-apply", "plain-drop"} {
		if r := res[id]; r["ok"] != false || !strings.Contains(r["error"].(string), "not created by AuthNull") {
			t.Errorf("%s: %v", id, r)
		}
	}
	if got := mysqlShowGrants(t, admin, plain); len(got) != 1 {
		t.Errorf("the unmarked account's grants changed: %q", got)
	}

	// Drop: a session open as the account ends, the account and its ProxySQL row go.
	c := cfg
	c.User, c.Password = user, pw
	sessionDB, err := sql.Open("mysql", mysqlDSN(c, ""))
	if err != nil {
		t.Fatal(err)
	}
	defer sessionDB.Close()
	conn, err := sessionDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if res := run(job("drop", "drop_role", user, map[string]any{"databases": []string{dbName}})); res["drop"]["ok"] != true {
		t.Fatalf("drop: %v", res["drop"])
	}
	if mysqlUserCount(t, admin, user) != 0 {
		t.Fatal("the account is still there")
	}
	if err := conn.PingContext(context.Background()); err == nil {
		t.Error("the account's session survived the drop")
	}
	if _, ok := fp.users[user]; ok {
		t.Error("the ProxySQL row survived the drop")
	}
	// Dropping again is done, not an error.
	if res := run(job("again", "drop_role", user, map[string]any{"databases": []string{dbName}})); res["again"]["ok"] != true {
		t.Errorf("second drop: %v", res["again"])
	}
}

// apply_policy_role / drop_policy_role on MySQL: the restricted account holds exactly the policy's
// privileges on the policy's tables (or its database), carries the policy mark, takes the source
// role's hostgroup, and logs in with the password ProxySQL holds.
func TestMySQLPolicyRole(t *testing.T) {
	admin, cfg, dbName := mysqlTestTarget(t)
	t.Cleanup(resetInstallCredentialForTest)
	const source, plain = "agent_it_src", "agent_it_pr_plain"
	tableRole, _ := BackendRoleName(dbName, source, []string{"READ"}, []string{"agent_it_pr_t"})
	dbRole, _ := BackendRoleName(dbName, source, []string{"WRITE", "TRUNCATE"}, nil)
	cleanup := func() {
		mysqlDropUsers(admin, tableRole, dbRole, plain)
		admin.Exec("DROP TABLE IF EXISTS agent_it_pr_t, agent_it_pr_secret")
	}
	cleanup()
	t.Cleanup(cleanup)
	for _, s := range []string{
		"CREATE TABLE agent_it_pr_t (id INT)",
		"CREATE TABLE agent_it_pr_secret (id INT)",
		"INSERT INTO agent_it_pr_t VALUES (1)",
		"CREATE USER 'agent_it_pr_plain'@'%' IDENTIFIED BY 'PlainPassword0123456789'",
	} {
		if _, err := admin.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	fb, srv := newFakeVaultBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	cfg.API = srv.URL
	fp := newFakeProxy()
	fp.users[source] = proxyUser{Username: source, Password: "x", DefaultHostgroup: 7}
	job := func(id, kind, role string, payload any) map[string]any {
		return map[string]any{"id": id, "kind": kind, "host": cfg.Host, "dbType": "mysql", "role": role, "payload": payloadJSON(t, payload)}
	}
	tablePolicy := map[string]any{"database": dbName, "sourceRole": source, "privileges": []string{"READ"}, "tables": []string{"agent_it_pr_t"}}
	dbPolicy := map[string]any{"database": dbName, "sourceRole": source, "privileges": []string{"WRITE", "TRUNCATE"}}
	fb.jobs = []map[string]any{
		job("table", "apply_policy_role", tableRole, tablePolicy),
		job("db", "apply_policy_role", dbRole, dbPolicy),
		job("misnamed", "apply_policy_role", "an_0000000000000000", tablePolicy),
	}
	if _, err := ProcessVaultJobs(admin, fp, fakeHostgroups{cfg.Host: 2}, cfg, time.Now()); err != nil {
		t.Fatal(err)
	}
	res := jobResults(fb)
	if res["table"]["ok"] != true || res["db"]["ok"] != true {
		t.Fatalf("results: %v", res)
	}
	if r := res["misnamed"]; r["ok"] != false || !strings.Contains(r["error"].(string), "does not match") {
		t.Errorf("misnamed: %v", r)
	}
	esc := strings.ReplaceAll(dbName, "_", `\_`)
	if got, want := mysqlShowGrants(t, admin, tableRole), []string{
		"GRANT SELECT ON `" + dbName + "`.`agent_it_pr_t` TO `" + tableRole + "`@`%`",
		"GRANT USAGE ON *.* TO `" + tableRole + "`@`%`",
	}; !reflect.DeepEqual(got, want) {
		t.Errorf("table policy grants:\n got %q\nwant %q", got, want)
	}
	if got, want := mysqlShowGrants(t, admin, dbRole), []string{
		"GRANT INSERT, UPDATE, DELETE, DROP ON `" + esc + "`.* TO `" + dbRole + "`@`%`",
		"GRANT USAGE ON *.* TO `" + dbRole + "`@`%`",
	}; !reflect.DeepEqual(got, want) {
		t.Errorf("database policy grants:\n got %q\nwant %q", got, want)
	}
	var attr string
	admin.QueryRow("SELECT ATTRIBUTE FROM information_schema.USER_ATTRIBUTES WHERE USER = ? AND HOST = '%'", tableRole).Scan(&attr)
	if !strings.Contains(attr, `"authnull": "policy"`) {
		t.Errorf("attribute %q", attr)
	}
	row := fp.users[tableRole]
	if row.DefaultHostgroup != 7 || row.Password == "" {
		t.Fatalf("ProxySQL row: hostgroup %d", row.DefaultHostgroup)
	}
	if h, _, ok := parseRotationComment(row.Comment); !ok || h != cfg.Host+":"+cfg.Port || isVaultManaged(row.Comment) {
		t.Errorf("comment: host %q ok %v", h, ok)
	}

	// The account itself: reads its table, nothing else.
	c := cfg
	c.User, c.Password = tableRole, row.Password
	as, err := sql.Open("mysql", mysqlDSN(c, dbName))
	if err != nil {
		t.Fatal(err)
	}
	defer as.Close()
	var id int
	if err := as.QueryRow("SELECT id FROM agent_it_pr_t").Scan(&id); err != nil {
		t.Errorf("reading the policy's table: %v", err)
	}
	if _, err := as.Exec("INSERT INTO agent_it_pr_t VALUES (2)"); err == nil {
		t.Error("a READ account wrote")
	}
	if err := as.QueryRow("SELECT id FROM agent_it_pr_secret").Scan(&id); err == nil || strings.Contains(err.Error(), "no rows") {
		t.Errorf("reading another table: %v", err)
	}
	as.Close()

	// Applying again keeps the password ProxySQL has (it is not due).
	fb.jobs = []map[string]any{job("again", "apply_policy_role", tableRole, tablePolicy)}
	fb.results = nil
	ProcessVaultJobs(admin, fp, fakeHostgroups{cfg.Host: 2}, cfg, time.Now())
	if fp.users[tableRole].Password != row.Password || !mysqlCanLogin(t, cfg, tableRole, row.Password) {
		t.Error("a re-apply changed the password")
	}

	// Drop: an account that is not a restricted one is refused; ours go.
	fb.jobs = []map[string]any{
		job("drop-plain", "drop_policy_role", plain, map[string]any{"databases": []string{dbName}}),
		job("drop-table", "drop_policy_role", tableRole, map[string]any{"databases": []string{dbName}}),
		job("drop-db", "drop_policy_role", dbRole, map[string]any{"databases": []string{dbName}}),
	}
	fb.results = nil
	ProcessVaultJobs(admin, fp, fakeHostgroups{cfg.Host: 2}, cfg, time.Now())
	res = jobResults(fb)
	if r := res["drop-plain"]; r["ok"] != false || !strings.Contains(r["error"].(string), "not an AuthNull restricted role") {
		t.Errorf("drop-plain: %v", r)
	}
	if res["drop-table"]["ok"] != true || res["drop-db"]["ok"] != true {
		t.Fatalf("drops: %v", res)
	}
	for _, r := range []string{tableRole, dbRole} {
		if mysqlUserCount(t, admin, r) != 0 {
			t.Errorf("%s is still there", r)
		}
		if _, ok := fp.users[r]; ok {
			t.Errorf("%s still in ProxySQL", r)
		}
	}
	if mysqlUserCount(t, admin, plain) != 1 {
		t.Error("the unmarked account was dropped")
	}
}

// The scheduled sweep on a MySQL target rotates the due accounts recorded for host:port -- and
// leaves a row recorded for the bare host (a PostgreSQL target on the same IP) alone.
func TestMySQLScheduledRotation(t *testing.T) {
	admin, cfg, _ := mysqlTestTarget(t)
	const user = "agent_it_sched"
	mysqlDropUsers(admin, user)
	t.Cleanup(func() { mysqlDropUsers(admin, user) })
	if _, err := admin.Exec("CREATE USER 'agent_it_sched'@'%' IDENTIFIED BY 'OldPassword0123456789'"); err != nil {
		t.Fatal(err)
	}
	fp := newFakeProxy()
	long := time.Now().Add(-48 * time.Hour)
	fp.users[user] = proxyUser{Username: user, Password: "OldPassword0123456789", DefaultHostgroup: 3, Comment: rotationComment(managedHost(cfg), long)}
	fp.users["agent_it_pgside"] = proxyUser{Username: "agent_it_pgside", Password: "p", DefaultHostgroup: 3, Comment: rotationComment(cfg.Host, long)}
	rotated, err := rotateDuePasswordsFor(admin, fp, cfg, time.Now(), 24*time.Hour)
	if err != nil || !reflect.DeepEqual(rotated, []string{user}) {
		t.Fatalf("rotated %v, %v", rotated, err)
	}
	pw := fp.users[user].Password
	if pw == "OldPassword0123456789" || !mysqlCanLogin(t, cfg, user, pw) {
		t.Fatal("the account does not have ProxySQL's new password")
	}
	if fp.users["agent_it_pgside"].Password != "p" {
		t.Error("the sweep touched a row of another target")
	}
}
