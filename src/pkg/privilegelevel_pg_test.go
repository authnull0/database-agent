package pkg

import (
	"testing"
)

// The agent's own role query against a real server (AGENT_TEST_DSN): each role's level comes out
// as privilegeLevel says. The test user has CREATEROLE, so it can make CREATEDB and CREATEROLE
// roles, but not superusers: the superuser case is the server's own bootstrap role, if listed.
func TestPostgresRolePrivilegeLevels(t *testing.T) {
	admin, _ := agentTestDB(t)
	roles := map[string]string{
		"authnull_pl_test_std":   "LOGIN",
		"authnull_pl_test_db":    "LOGIN CREATEDB",
		"authnull_pl_test_roles": "LOGIN CREATEROLE",
	}
	for r, attrs := range roles {
		admin.Exec("DROP ROLE IF EXISTS " + r)
		if _, err := admin.Exec("CREATE ROLE " + r + " " + attrs); err != nil {
			t.Fatal(err)
		}
		role := r
		t.Cleanup(func() { admin.Exec("DROP ROLE IF EXISTS " + role) })
	}
	rows, err := admin.Query(rolePrivilegesQuery)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var username, host, privileges, role string
		var a roleAttributes
		if err := rows.Scan(&username, &host, &privileges, &role, &a.Super, &a.CreateRole, &a.CreateDB,
			&a.Replication, &a.BypassRLS, &a.ServerAccess, &a.AllData); err != nil {
			t.Fatal(err)
		}
		got[username] = privilegeLevel(a)
	}
	for r, want := range map[string]string{
		"authnull_pl_test_std": PrivilegeStandard, "authnull_pl_test_db": PrivilegeElevated, "authnull_pl_test_roles": PrivilegeElevated,
	} {
		if got[r] != want {
			t.Errorf("%s: %q, want %q", r, got[r], want)
		}
	}
	if len(got) < 3 {
		t.Fatalf("query listed %d roles", len(got))
	}
}
