package pkg

import (
	"bytes"
	"log"
	"os"
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

// A ProxySQL user whose role was dropped from the server is skipped by the check, not tried (and
// logged) every minute; a role still there is put in the group.
func TestDirectAccessSkipsRolesNoLongerOnTheServer(t *testing.T) {
	admin, base := agentTestDB(t)
	dbName := strings.TrimPrefix(base.Path, "/")
	const live, gone, host = "da_live_it", "da_gone_it", "192.168.122.175"
	admin.Exec("DROP ROLE IF EXISTS " + gone)
	admin.Exec("DROP ROLE IF EXISTS " + live)
	if _, err := admin.Exec("CREATE ROLE " + live + " LOGIN"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Exec("DROP ROLE IF EXISTS " + live) })
	fp := newFakeProxy()
	for _, r := range []string{live, gone} {
		fp.users[r] = proxyUser{Username: r, Comment: rotationComment(host, time.Now())}
	}
	var logged bytes.Buffer
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	CheckDirectAccess(admin, fp, DBConfig{Host: host}, []string{dbName}, time.Now())
	if strings.Contains(logged.String(), gone) {
		t.Errorf("a role no longer on the server was tried: %s", logged.String())
	}
	if g, err := roleGroups(admin, live); err != nil || !g[ManagedGroup] {
		t.Errorf("the live role is not in %s: %v %v", ManagedGroup, g, err)
	}
}
