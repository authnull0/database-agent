package pkg

import "testing"

// Against a real server (AGENT_TEST_DSN): a sync leaves out the built-in postgres role and the role
// the agent logs in as, and reports that same role -- an Admin -- once it is not the agent's login.
func TestPostgresDiscoveryLeavesOutBuiltinAndOwnLogin(t *testing.T) {
	admin, _ := agentTestDB(t)
	var dbName, self string
	if err := admin.QueryRow("SELECT current_database(), current_user").Scan(&dbName, &self); err != nil {
		t.Fatal(err)
	}
	listed := func(agentLogin string) map[string]databaseUser {
		t.Helper()
		users, err := databaseUsers(admin, DBConfig{DBType: "postgres", User: agentLogin}, dbName)
		if err != nil {
			t.Fatal(err)
		}
		byName := map[string]databaseUser{}
		for _, u := range users {
			byName[u.Username] = u
		}
		return byName
	}

	asAgent := listed(self)
	if _, ok := asAgent["postgres"]; ok {
		t.Error("the built-in postgres role was listed")
	}
	if _, ok := asAgent[self]; ok {
		t.Errorf("the agent's own login %s was listed", self)
	}

	// The test's account can create roles: an Admin, still reported when the agent is someone else.
	other := listed("agent_it_someone_else")
	if u, ok := other[self]; !ok || u.Role != "Admin" {
		t.Errorf("admin %s: %+v listed=%v", self, u, ok)
	}
	if _, ok := other["postgres"]; ok {
		t.Error("the built-in postgres role was listed")
	}
}

// Against a real server (DBAGENT_TEST_MYSQL_DSN): a sync leaves out root, whatever its host part,
// and the account the agent logs in as, and reports that same account -- an Admin -- once it is not
// the agent's login.
func TestMySQLDiscoveryLeavesOutBuiltinAndOwnLogin(t *testing.T) {
	admin, cfg, dbName := mysqlTestTarget(t)
	cleanup := func() { admin.Exec("DROP USER IF EXISTS 'root'@'agent-it.invalid'") }
	cleanup()
	t.Cleanup(cleanup)
	if _, err := admin.Exec("CREATE USER 'root'@'agent-it.invalid' IDENTIFIED BY 'RootElsewhere0123456789'"); err != nil {
		t.Fatal(err)
	}
	listed := func(agentLogin string) []databaseUser {
		t.Helper()
		c := cfg
		c.User = agentLogin
		users, err := databaseUsers(admin, c, dbName)
		if err != nil {
			t.Fatal(err)
		}
		return users
	}

	for _, u := range listed(cfg.User) {
		if u.Username == "root" || u.Username == cfg.User {
			t.Errorf("listed %s@%s", u.Username, u.Host)
		}
	}

	// The test's account holds CREATE USER on *.* and ALL on its database: an Admin, still reported
	// when the agent is someone else.
	found := false
	for _, u := range listed("agent_it_someone_else") {
		if u.Username == "root" {
			t.Errorf("listed %s@%s", u.Username, u.Host)
		}
		if u.Username == cfg.User && u.Host == "%" {
			found = true
			if u.Privileges != "ALL PRIVILEGES" || u.Level != PrivilegeAdmin || u.Role != "Admin" {
				t.Errorf("admin %s: %+v", cfg.User, u)
			}
		}
	}
	if !found {
		t.Errorf("admin %s not listed", cfg.User)
	}
}
