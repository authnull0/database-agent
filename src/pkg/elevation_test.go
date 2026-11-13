package pkg

import (
	"strings"
	"testing"
)

// A custom elevation bundle's name comes from its shape, like a restricted role's: the same
// database, privileges and tables always name the same role, in any order and case.
func TestElevationBundleName(t *testing.T) {
	a, ok := ElevationBundleName("salesdb", []string{"SELECT", "insert"}, []string{"Orders", "public.customers"})
	if !ok {
		t.Fatal("no name")
	}
	if !strings.HasPrefix(a, "authnull_elev_") || len(a) != len("authnull_elev_")+16 {
		t.Errorf("name %q", a)
	}
	b, _ := ElevationBundleName("SalesDB", []string{"INSERT", "SELECT"}, []string{"customers", "orders"})
	if a != b {
		t.Errorf("same shape, two names: %s %s", a, b)
	}
	c, _ := ElevationBundleName("salesdb", []string{"SELECT"}, []string{"orders", "customers"})
	if a == c {
		t.Error("different privileges, one name")
	}
	for _, bad := range [][]string{nil, {"EXECUTE"}, {"SELECT", "ADMIN"}} {
		if _, ok := ElevationBundleName("salesdb", bad, nil); ok {
			t.Errorf("%v: named", bad)
		}
	}
}

// The agent's own check of an elevation job: a role to grant, or a bundle's shape -- never both,
// never neither, and never one of AuthNull's own or the server's built-in roles.
func TestCheckElevatePayload(t *testing.T) {
	for _, ok := range []elevatePayload{
		{Grant: "app_rw"},
		{Database: "salesdb", Privileges: []string{"SELECT"}},
		{Database: "salesdb", Privileges: []string{"READ", "WRITE"}, Tables: []string{"orders"}},
	} {
		if err := checkElevatePayload(ok); err != nil {
			t.Errorf("%+v refused: %v", ok, err)
		}
	}
	for _, bad := range []elevatePayload{
		{},
		{Grant: "app_rw", Database: "salesdb", Privileges: []string{"SELECT"}},
		{Grant: "pg_read_server_files"},
		{Grant: "pg_signal_backend"},
		{Grant: ManagedGroup},
		{Grant: BackendRolesGroup},
		{Grant: "an_0123456789abcdef"},
		{Grant: "jit_dev_3f9a2c1e"},
		{Grant: "postgres"},
		{Grant: "a,b"},
		{Database: "salesdb", Privileges: []string{"EXECUTE"}},
		{Database: "template1", Privileges: []string{"SELECT"}},
		{Database: "salesdb"},
	} {
		if err := checkElevatePayload(bad); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
}
