package pkg

import (
	"strings"
	"testing"
	"time"
)

// The engine's built-in admin and the agent's own login are never brought into AuthNull; every
// other account, admins included, is.
func TestExcludedAccount(t *testing.T) {
	pg := DBConfig{DBType: "postgres", User: "dbmfa_admin"}
	my := DBConfig{DBType: "mysql", User: "authnull_agent"}
	for name, tc := range map[string]struct {
		cfg  DBConfig
		user string
		want bool
	}{
		"postgres built-in admin":           {pg, "postgres", true},
		"postgres agent login":              {pg, "dbmfa_admin", true},
		"postgres other superuser":          {pg, "dba_alice", false},
		"postgres root is just a role":      {pg, "root", false},
		"postgres name differs in case":     {pg, "Postgres", false},
		"postgres empty type is postgres":   {DBConfig{User: "agent"}, "postgres", true},
		"postgres agent login, padded":      {DBConfig{DBType: "postgres", User: " agent "}, "agent", true},
		"postgres no agent login":           {DBConfig{DBType: "postgres"}, "", false},
		"mysql built-in admin":              {my, "root", true},
		"mysql agent login":                 {my, "authnull_agent", true},
		"mysql system account":              {my, "mysql.sys", true},
		"mysql other admin":                 {my, "dba_bob", false},
		"mysql postgres is just an account": {my, "postgres", false},
		"mysql name differs in case":        {my, "ROOT", false},
		"mariadb built-in admin":            {DBConfig{DBType: "mariadb"}, "root", true},
	} {
		if got := excludedAccount(tc.cfg, tc.user); got != tc.want {
			t.Errorf("%s: excludedAccount(%q) = %v, want %v", name, tc.user, got, tc.want)
		}
	}
}

// filterExcludedAccounts drops the excluded logins from what a sync reports, whatever their host
// part, and keeps every other one in order.
func TestFilterExcludedAccounts(t *testing.T) {
	cfg := DBConfig{DBType: "mysql", User: "authnull_agent"}
	in := []databaseUser{
		{Username: "root", Host: "%"},
		{Username: "app", Host: "%"},
		{Username: "root", Host: "localhost"},
		{Username: "authnull_agent", Host: "192.168.122.15"},
		{Username: "dba", Host: "%", Role: "Admin"},
	}
	got := filterExcludedAccounts(cfg, in)
	var names []string
	for _, u := range got {
		names = append(names, u.Username+"@"+u.Host)
	}
	if strings.Join(names, " ") != "app@% dba@%" {
		t.Errorf("reported %v", names)
	}
}

// A policy (or a stale console entry) naming the engine's built-in admin is refused before
// anything is asked of the backend or the database.
func TestVaultJobsLeaveTheBuiltinAdminAlone(t *testing.T) {
	for _, tc := range []struct{ dbType, role string }{{"postgres", "postgres"}, {"mysql", "root"}} {
		t.Run(tc.dbType, func(t *testing.T) {
			t.Cleanup(resetInstallCredentialForTest)
			fb, srv := newFakeVaultBackend(t)
			SetInstallCredential(srv.URL, testClientID, testClientSecret)
			cfg := DBConfig{API: srv.URL, Host: "192.168.122.175", DBType: tc.dbType, User: "authnull_agent"}
			fb.password = "NeverUsed0123456789abcdefghijklm"
			fb.jobs = []map[string]any{
				{"id": "builtin", "kind": "rotate_password", "host": "192.168.122.175", "dbType": tc.dbType, "role": tc.role},
			}
			if _, err := ProcessVaultJobs(nil, newFakeProxy(), fakeHostgroups{}, cfg, time.Now()); err != nil {
				t.Fatal(err)
			}
			if len(fb.results) != 1 || fb.results[0]["ok"] != false || !strings.Contains(fb.results[0]["error"].(string), "built-in admin") {
				t.Fatalf("result: %v", fb.results)
			}
			for _, c := range fb.calls {
				if strings.HasSuffix(c, "/secret") {
					t.Error("the password was fetched for the built-in admin")
				}
			}
		})
	}
}
