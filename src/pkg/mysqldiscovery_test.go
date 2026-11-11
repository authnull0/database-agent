package pkg

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
)

// How MySQL's global privileges read as the policy engine's levels (privilegelevel.go).
func TestMySQLPrivilegeLevels(t *testing.T) {
	cases := []struct {
		global    []string
		grantable bool
		want      string
		role      string
	}{
		{[]string{"USAGE"}, false, PrivilegeStandard, "User"},
		{[]string{"PROCESS", "RELOAD", "SHOW DATABASES"}, false, PrivilegeStandard, "User"},
		{[]string{"SUPER"}, false, PrivilegeAdmin, "Admin"},
		{[]string{"CREATE USER"}, false, PrivilegeAdmin, "Admin"},
		{[]string{"PROCESS"}, true, PrivilegeAdmin, "Admin"}, // WITH GRANT OPTION on *.*
		{[]string{"FILE"}, false, PrivilegeAdmin, "User"},
		{[]string{"CREATE"}, false, PrivilegeElevated, "Admin"},
		{[]string{"CREATE ROLE"}, false, PrivilegeElevated, "Admin"},
		{[]string{"ROLE_ADMIN"}, false, PrivilegeElevated, "Admin"},
		{[]string{"REPLICATION SLAVE"}, false, PrivilegeElevated, "User"},
		{[]string{"SELECT"}, false, PrivilegeElevated, "User"},
		{[]string{"UPDATE"}, false, PrivilegeElevated, "User"},
	}
	for _, c := range cases {
		a := mysqlRoleAttributes(c.global, c.grantable)
		if got := privilegeLevel(a); got != c.want {
			t.Errorf("%v grantable=%v: level %q, want %q", c.global, c.grantable, got, c.want)
		}
		if got := roleLabel(a); got != c.role {
			t.Errorf("%v grantable=%v: role %q, want %q", c.global, c.grantable, got, c.role)
		}
	}
}

// A database's privileges as the dbUser payload's string: the database-level ones an account holds
// there, globally or on the database, sorted; all of them read as ALL PRIVILEGES.
func TestMySQLDatabasePrivilegeString(t *testing.T) {
	if got := mysqlPrivilegeString([]string{"SELECT", "INSERT", "PROCESS", "USAGE", "SELECT"}); got != "INSERT, SELECT" {
		t.Errorf("got %q", got)
	}
	if got := mysqlPrivilegeString(mysqlDatabasePrivileges); got != "ALL PRIVILEGES" {
		t.Errorf("got %q", got)
	}
	if got := mysqlPrivilegeString(nil); got != "" {
		t.Errorf("got %q", got)
	}
}

// Against a real MySQL: the databases, uptime, accounts with their levels, and tables a sync sends.
func TestMySQLDiscovery(t *testing.T) {
	admin, cfg, dbName := mysqlTestTarget(t)
	const user = "agent_it_disc"
	cleanup := func() {
		admin.Exec("DROP USER IF EXISTS 'agent_it_disc'@'%'")
		admin.Exec("DROP TABLE IF EXISTS agent_it_disc_t")
	}
	cleanup()
	t.Cleanup(cleanup)
	for _, s := range []string{
		"CREATE USER 'agent_it_disc'@'%' IDENTIFIED BY 'DiscPassword0123456789'",
		"GRANT SELECT, INSERT ON " + mysqlDBGrantTarget(dbName) + " TO 'agent_it_disc'@'%'",
		"CREATE TABLE agent_it_disc_t (id INT PRIMARY KEY, name VARCHAR(20), created DATE)",
	} {
		if _, err := admin.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}

	dbs, err := listDatabases(admin, cfg)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, d := range dbs {
		seen[d] = true
	}
	if !seen[dbName] {
		t.Errorf("databases %v miss %s", dbs, dbName)
	}
	for _, sys := range []string{"mysql", "information_schema", "performance_schema", "sys"} {
		if seen[sys] {
			t.Errorf("system database %s listed", sys)
		}
	}

	if up, err := serverUptime(admin, cfg); err != nil || up <= 0 {
		t.Errorf("uptime %d, %v", up, err)
	}

	users, err := databaseUsers(admin, cfg, dbName)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]databaseUser{}
	for _, u := range users {
		byName[u.Username+"@"+u.Host] = u
		if len(u.Username) > 6 && u.Username[:6] == "mysql." {
			t.Errorf("system account listed: %+v", u)
		}
	}
	if u := byName[user+"@%"]; u.Privileges != "INSERT, SELECT" || u.Level != PrivilegeStandard || u.Role != "User" {
		t.Errorf("%s: %+v", user, u)
	}
	// The test's own account is the agent's login here: never brought into AuthNull.
	if u, listed := byName[cfg.User+"@%"]; listed {
		t.Errorf("the agent's own login was listed: %+v", u)
	}

	tables, err := databaseTables(admin, cfg, dbName)
	if err != nil {
		t.Fatal(err)
	}
	if got := tables["agent_it_disc_t"]; !reflect.DeepEqual(got, []string{"id", "name", "created"}) {
		t.Errorf("columns: %v (tables %v)", got, tables)
	}
}

// The status sync names the engine and the target's port: a MySQL server is not reported as Postgres.
func TestMySQLStatusSyncNamesTheEngine(t *testing.T) {
	admin, cfg, dbName := mysqlTestTarget(t)
	var mu sync.Mutex
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/api/v1/databaseService/dbSync" {
			json.NewDecoder(r.Body).Decode(&got)
			w.Write([]byte(`{"hostgroup_id": 4, "code": 200, "database_status": "ACTIVE"}`))
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()
	cfg.API = srv.URL
	hg, _, err := FetchDatabaseStatus(admin, dbName, cfg, "inst-1")
	if err != nil || hg != 4 {
		t.Fatalf("hostgroup %d, %v", hg, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got["databaseType"] != "mysql" || got["port"] != cfg.Port || got["status"] != "ACTIVE" {
		t.Fatalf("payload: type %v port %v status %v", got["databaseType"], got["port"], got["status"])
	}
}
