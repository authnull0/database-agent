package pkg

import (
	"reflect"
	"strings"
	"testing"
)

// Against a real MySQL (DBAGENT_TEST_MYSQL_DSN): a grant on one table is a tablePrivileges entry
// and counts in the privilege string; a grant on the whole database is in the privilege string
// only, since it covers every table.
func TestMySQLTablePrivileges(t *testing.T) {
	admin, cfg, dbName := mysqlTestTarget(t)
	cleanup := func() {
		admin.Exec("DROP USER IF EXISTS 'agent_it_tp_user'@'%'")
		admin.Exec("DROP USER IF EXISTS 'agent_it_tp_dbwide'@'%'")
		admin.Exec("DROP TABLE IF EXISTS agent_it_tp_a, agent_it_tp_b")
	}
	cleanup()
	t.Cleanup(cleanup)
	for _, s := range []string{
		"CREATE TABLE agent_it_tp_a (id INT PRIMARY KEY)",
		"CREATE TABLE agent_it_tp_b (id INT PRIMARY KEY)",
		"CREATE USER 'agent_it_tp_user'@'%' IDENTIFIED BY 'TpPassword0123456789'",
		"CREATE USER 'agent_it_tp_dbwide'@'%' IDENTIFIED BY 'TpPassword0123456789'",
		"GRANT SELECT ON " + mysqlIdent(dbName) + "." + mysqlIdent("agent_it_tp_a") + " TO 'agent_it_tp_user'@'%'",
		"GRANT SELECT, INSERT ON " + mysqlDBGrantTarget(dbName) + " TO 'agent_it_tp_dbwide'@'%'",
	} {
		if _, err := admin.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}

	// The agent's own login is never reported: read as if the agent were someone else, so the test's
	// account shows up as any other admin would.
	other := cfg
	other.User = "agent_it_someone_else"
	users, err := databaseUsers(admin, other, dbName)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]databaseUser{}
	for _, u := range users {
		byName[u.Username+"@"+u.Host] = u
	}

	u := byName["agent_it_tp_user@%"]
	if u.Privileges != "SELECT" {
		t.Errorf("table grant: privilege %q", u.Privileges)
	}
	want := []tablePrivilege{{Table: dbName + ".agent_it_tp_a", Privileges: []string{"SELECT"}}}
	if !reflect.DeepEqual(u.TablePrivileges, want) {
		t.Errorf("table grant: tablePrivileges %+v, want %+v", u.TablePrivileges, want)
	}

	u = byName["agent_it_tp_dbwide@%"]
	if u.Privileges != "INSERT, SELECT" || len(u.TablePrivileges) != 0 {
		t.Errorf("database grant: privilege %q, tablePrivileges %+v", u.Privileges, u.TablePrivileges)
	}

	// The test's own account holds everything on its database: ALL PRIVILEGES, no table list.
	if u := byName[cfg.User+"@%"]; u.Privileges != "ALL PRIVILEGES" || len(u.TablePrivileges) != 0 {
		t.Errorf("%s: privilege %q, tablePrivileges %+v", cfg.User, u.Privileges, u.TablePrivileges)
	}
	for k, u := range byName {
		for _, tp := range u.TablePrivileges {
			if !strings.HasPrefix(tp.Table, dbName+".") {
				t.Errorf("%s: a table of another database: %s", k, tp.Table)
			}
		}
	}
}
