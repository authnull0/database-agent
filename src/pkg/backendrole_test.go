package pkg

import (
	"strings"
	"testing"
)

// The same vectors are asserted by proxysql-v3-alpha's test/authnull/authnull_backend_role_test.cpp:
// the agent names the role and ProxySQL computes the name independently, so the two must agree
// exactly. Change one side and these fail on that side.
// Computed independently (Python hashlib over the canonical string) before either implementation:
//
//	v1|<database, lower case>|<role>|<privileges, upper case, sorted>|<tables, normalised, sorted>
//
// where a table in schema public is its bare lower-case name and any other is schema.table.
var backendRoleVectors = []struct {
	db, role   string
	privileges []string
	tables     []string
	want       string
}{
	{"dbmfa_test", "dbmfa_writer", []string{"READ"}, []string{"customers", "employees", "orders", "priv_scratch"}, "an_8f3a1492e83ffc13"},
	{"dbmfa_test", "dbmfa_writer", []string{"READ", "WRITE"}, []string{"customers", "employees", "orders", "priv_scratch"}, "an_7722746c2680046f"},
	{"salesdb", "readonly_app", []string{"READ"}, nil, "an_2f3f12694f5ff113"},
	// Custom privileges (AUTH-227): whole groups spelled as verbs keep the v1 name; other verb sets
	// are v2 over the sorted verbs. ProxySQL asserts the same (test/authnull/authnull_backend_role_test.cpp).
	{"dbmfa_test", "dbmfa_writer", []string{"SELECT"}, []string{"customers", "employees", "orders", "priv_scratch"}, "an_8f3a1492e83ffc13"},
	{"dbmfa_test", "dbmfa_writer", []string{"SELECT", "INSERT", "UPDATE", "DELETE"}, []string{"customers", "employees", "orders", "priv_scratch"}, "an_7722746c2680046f"},
	{"dbmfa_test", "dbmfa_writer", []string{"SELECT", "INSERT"}, []string{"customers", "employees", "orders", "priv_scratch"}, "an_f0e09c5084ba5b55"},
	{"dbmfa_test", "dbmfa_writer", []string{"read", " insert"}, []string{"customers", "employees", "orders", "priv_scratch"}, "an_f0e09c5084ba5b55"},
	{"salesdb", "readonly_app", []string{"UPDATE", "TRUNCATE", "SELECT"}, nil, "an_937b3d2d6c1ba244"},
}

// The table privileges a restricted role gets: the policy's verbs, the groups expanded.
func TestBackendRoleVerbs(t *testing.T) {
	for _, c := range []struct {
		in   []string
		want string
	}{
		{[]string{"READ"}, "SELECT"},
		{[]string{"READ", "WRITE"}, "SELECT, INSERT, UPDATE, DELETE"},
		{[]string{"select", "INSERT"}, "SELECT, INSERT"},
		{[]string{"TRUNCATE", "UPDATE"}, "UPDATE, TRUNCATE"},
	} {
		if got := strings.Join(backendRoleVerbs(c.in), ", "); got != c.want {
			t.Errorf("%v: %q, want %q", c.in, got, c.want)
		}
	}
}

func TestBackendRoleNameIsCanonical(t *testing.T) {
	a, ok := BackendRoleName("DBMFA_TEST ", "dbmfa_writer", []string{"write", "READ", "READ"}, []string{"Orders", "customers", " employees", "priv_scratch"})
	if !ok {
		t.Fatal("READ+WRITE gave no role")
	}
	b, _ := BackendRoleName("dbmfa_test", "dbmfa_writer", []string{"READ", "WRITE"}, []string{"customers", "employees", "orders", "priv_scratch"})
	if a != b {
		t.Errorf("order, case and duplicates changed the name: %s vs %s", a, b)
	}
	if !strings.HasPrefix(a, "an_") || len(a) != 19 {
		t.Errorf("name %q: want an_ + 16 hex", a)
	}
	// Different shapes are different roles.
	r, _ := BackendRoleName("dbmfa_test", "dbmfa_writer", []string{"READ"}, []string{"customers", "employees", "orders", "priv_scratch"})
	other, _ := BackendRoleName("dbmfa_test", "dbmfa_writer", []string{"READ"}, []string{"customers"})
	if r == a || r == other {
		t.Errorf("distinct shapes share a name: %s %s %s", r, a, other)
	}
}

// EXECUTE (DDL and the rest) keeps the policy's own role: there is no restricted role to make.
func TestBackendRoleNameOnlyForReadAndWrite(t *testing.T) {
	for _, privs := range [][]string{{"READ", "EXECUTE"}, {"EXECUTE"}, {}, {"READ", "ADMIN"}} {
		if name, ok := BackendRoleName("d", "r", privs, []string{"t"}); ok {
			t.Errorf("privileges %v gave %q, want none", privs, name)
		}
	}
}

func TestBackendRoleNameVectors(t *testing.T) {
	for _, v := range backendRoleVectors {
		got, ok := BackendRoleName(v.db, v.role, v.privileges, v.tables)
		if !ok || got != v.want {
			t.Errorf("BackendRoleName(%q,%q,%v,%v) = %q,%v; want %q (update the C++ vectors too if this is deliberate)",
				v.db, v.role, v.privileges, v.tables, got, ok, v.want)
		}
	}
}

func TestSplitTable(t *testing.T) {
	cases := map[string][2]string{
		"customers":        {"public", "customers"},
		"sales.orders":     {"sales", "orders"},
		" Sales . Orders ": {"sales", "orders"},
	}
	for in, want := range cases {
		s, tb := splitTable(in)
		if s != want[0] || tb != want[1] {
			t.Errorf("splitTable(%q) = %q,%q want %q,%q", in, s, tb, want[0], want[1])
		}
	}
}
