package pkg

import (
	"reflect"
	"strings"
	"testing"

	"github.com/lib/pq"
)

// Against a real server (AGENT_TEST_DSN): a login's privilege string lists every database privilege
// it holds and its table privileges, and tablePrivileges names each table it holds something on --
// directly or through a group role it inherits from.
func TestPostgresTablePrivileges(t *testing.T) {
	admin, _ := agentTestDB(t)
	var dbName, self string
	if err := admin.QueryRow("SELECT current_database(), current_user").Scan(&dbName, &self); err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		admin.Exec("DROP TABLE IF EXISTS agent_it_tp_a, agent_it_tp_b")
		admin.Exec("DROP ROLE IF EXISTS agent_it_tp_user")
		admin.Exec("DROP ROLE IF EXISTS agent_it_tp_member")
		admin.Exec("DROP ROLE IF EXISTS agent_it_tp_group")
	}
	cleanup()
	t.Cleanup(cleanup)
	for _, s := range []string{
		"CREATE ROLE agent_it_tp_user LOGIN",
		"CREATE ROLE agent_it_tp_group NOLOGIN",
		"CREATE ROLE agent_it_tp_member LOGIN IN ROLE agent_it_tp_group",
		"CREATE TABLE agent_it_tp_a (id int)",
		"CREATE TABLE agent_it_tp_b (id int)",
		"GRANT SELECT, INSERT ON agent_it_tp_a TO agent_it_tp_user",
		"GRANT UPDATE ON agent_it_tp_b TO agent_it_tp_group",
		"GRANT TEMPORARY ON DATABASE " + pq.QuoteIdentifier(dbName) + " TO agent_it_tp_user",
	} {
		if _, err := admin.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	t.Cleanup(func() {
		admin.Exec("REVOKE TEMPORARY ON DATABASE " + pq.QuoteIdentifier(dbName) + " FROM agent_it_tp_user")
	})

	// Whether the database gives CREATE to PUBLIC decides whether the logins hold it: on the test
	// server it does not (PUBLIC has CONNECT and TEMPORARY, the default).
	var publicCreate bool
	if err := admin.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_database d, aclexplode(d.datacl) a
		WHERE d.datname = current_database() AND a.grantee = 0 AND a.privilege_type = 'CREATE')`).Scan(&publicCreate); err != nil {
		t.Fatal(err)
	}

	users, err := databaseUsers(admin, DBConfig{DBType: "postgres"}, dbName)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]databaseUser{}
	for _, u := range users {
		byName[u.Username] = u
	}
	if _, listed := byName["agent_it_tp_group"]; listed {
		t.Errorf("a NOLOGIN group role was listed")
	}

	check := func(name, want string, wantTables []tablePrivilege) {
		t.Helper()
		u, ok := byName[name]
		if !ok {
			t.Fatalf("%s not listed", name)
		}
		if publicCreate {
			want = privilegeSummary(append(strings.Split(want, ", "), "CREATE"), nil)
		}
		if u.Privileges != want {
			t.Errorf("%s privilege %q, want %q", name, u.Privileges, want)
		}
		if !reflect.DeepEqual(u.TablePrivileges, wantTables) {
			t.Errorf("%s tablePrivileges %+v, want %+v", name, u.TablePrivileges, wantTables)
		}
		if u.Role != "User" || u.Level != PrivilegeStandard {
			t.Errorf("%s role %q level %q", name, u.Role, u.Level)
		}
	}
	check("agent_it_tp_user", "CONNECT, INSERT, SELECT, TEMPORARY",
		[]tablePrivilege{{Table: "public.agent_it_tp_a", Privileges: []string{"INSERT", "SELECT"}}})
	check("agent_it_tp_member", "CONNECT, TEMPORARY, UPDATE",
		[]tablePrivilege{{Table: "public.agent_it_tp_b", Privileges: []string{"UPDATE"}}})

	// The test's own account owns both tables: every table privilege on each.
	all := []string{"DELETE", "INSERT", "REFERENCES", "SELECT", "TRIGGER", "TRUNCATE", "UPDATE"}
	seen := 0
	for _, tp := range byName[self].TablePrivileges {
		if tp.Table == "public.agent_it_tp_a" || tp.Table == "public.agent_it_tp_b" {
			seen++
			if !reflect.DeepEqual(tp.Privileges, all) {
				t.Errorf("owner on %s: %v", tp.Table, tp.Privileges)
			}
		}
	}
	if seen != 2 {
		t.Errorf("owner %s: %+v", self, byName[self].TablePrivileges)
	}
}
