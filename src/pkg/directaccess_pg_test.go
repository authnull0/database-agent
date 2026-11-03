package pkg

import (
	"strings"
	"testing"
	"time"
)

// Against a real server (AGENT_TEST_DSN, see backendrole_pg_test.go): the managed group, the
// group lookup the check relies on, the proxy address, and what a non-superuser agent is told.
func TestPostgresManagedGroup(t *testing.T) {
	admin, base := agentTestDB(t)
	dbName := strings.TrimPrefix(base.Path, "/")
	const role = "authnull_s1_test_role"
	admin.Exec("DROP ROLE IF EXISTS " + role)
	if _, err := admin.Exec("CREATE ROLE " + role + " LOGIN"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Exec("DROP ROLE IF EXISTS " + role) })

	for i := 0; i < 2; i++ { // idempotent: every checkout calls it
		if err := ensureManagedGroup(admin, role); err != nil {
			t.Fatalf("ensureManagedGroup (run %d): %v", i+1, err)
		}
	}
	groups, err := roleGroups(admin, role)
	if err != nil || !groups[ManagedGroup] {
		t.Fatalf("groups of %s: %v %v", role, groups, err)
	}
	var canLogin bool
	admin.QueryRow("SELECT rolcanlogin FROM pg_roles WHERE rolname = $1", ManagedGroup).Scan(&canLogin)
	if canLogin {
		t.Errorf("%s can log in", ManagedGroup)
	}

	// A restricted role is a managed role too, and the group is found through it.
	name, err := ProvisionBackendRole(admin, nil, "test-host", dbName, "app_role", []string{"READ"}, nil, 0, nowForTest(), DefaultPasswordRotation)
	if err != nil {
		t.Fatal(err)
	}
	dropRole(t, admin, name)
	if g, err := roleGroups(admin, name); err != nil || !g[ManagedGroup] || !g[BackendRolesGroup] {
		t.Fatalf("groups of %s: %v %v", name, g, err)
	}

	if ip, err := proxyAddress(admin); err != nil || ip == nil {
		t.Fatalf("proxy address: %v %v", ip, err)
	}
	// Reading the rules needs superuser or an explicit grant; either way the agent says which.
	if _, err := readHbaRules(admin); err != nil && !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("pg_hba_file_rules: %v", err)
	}
}

func nowForTest() time.Time { return time.Now() }
