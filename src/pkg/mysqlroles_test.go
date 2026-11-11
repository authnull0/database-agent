package pkg

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// The comment host of a managed user: the host alone for PostgreSQL, as always; host:port for MySQL,
// so two MySQL servers on one IP never claim each other's rows.
func TestManagedHost(t *testing.T) {
	if got := managedHost(DBConfig{DBType: "postgres", Host: "10.0.0.5", Port: "5432"}); got != "10.0.0.5" {
		t.Errorf("postgres: %q", got)
	}
	if got := managedHost(DBConfig{DBType: "mysql", Host: "10.0.0.5"}); got != "10.0.0.5:3306" {
		t.Errorf("mysql: %q", got)
	}
}

func TestMySQLUserNames(t *testing.T) {
	for _, ok := range []string{"agent_it_x", "an_0123456789abcdef", strings.Repeat("a", 32)} {
		if err := validateMySQLUserName(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", strings.Repeat("a", 33), "a,b", "a\x00b"} {
		if err := validateMySQLUserName(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func mysqlPayload(objs ...roleObjectGrant) rolePayload {
	return rolePayload{Version: 1, Grants: roleGrants{Databases: []roleDBGrants{{Database: "shop", Objects: objs}}}}
}

// The agent's own check of a MySQL console role's grants: only these object kinds, names and
// privileges can ever reach a GRANT.
func TestCheckMySQLRolePayload(t *testing.T) {
	good := mysqlPayload(
		roleObjectGrant{Type: "database", Privileges: []string{"SELECT", "SHOW VIEW", "CREATE TEMPORARY TABLES"}},
		roleObjectGrant{Type: "table", Name: "Orders_2024", Privileges: []string{"SELECT", "INSERT"}},
		roleObjectGrant{Type: "function", Name: "total", Privileges: []string{"EXECUTE"}},
		roleObjectGrant{Type: "procedure", Name: "refund", Privileges: []string{"EXECUTE", "ALTER ROUTINE"}},
	)
	good.RevokeDatabases = []string{"old_shop"}
	good.ValidUntil = "2030-01-01T00:00:00Z"
	if err := checkMySQLRolePayload(good); err != nil {
		t.Fatalf("good payload: %v", err)
	}
	bad := map[string]rolePayload{
		"nothing":          {Version: 1},
		"schema":           mysqlPayload(roleObjectGrant{Type: "table", Schema: "public", Name: "t", Privileges: []string{"SELECT"}}),
		"pg type":          mysqlPayload(roleObjectGrant{Type: "all_tables", Privileges: []string{"SELECT"}}),
		"exec on table":    mysqlPayload(roleObjectGrant{Type: "table", Name: "t", Privileges: []string{"EXECUTE"}}),
		"grant option":     mysqlPayload(roleObjectGrant{Type: "database", Privileges: []string{"GRANT OPTION"}}),
		"all":              mysqlPayload(roleObjectGrant{Type: "database", Privileges: []string{"ALL"}}),
		"no privileges":    mysqlPayload(roleObjectGrant{Type: "database"}),
		"named database":   mysqlPayload(roleObjectGrant{Type: "database", Name: "x", Privileges: []string{"SELECT"}}),
		"unnamed table":    mysqlPayload(roleObjectGrant{Type: "table", Privileges: []string{"SELECT"}}),
		"backtick":         mysqlPayload(roleObjectGrant{Type: "table", Name: "t`; DROP", Privileges: []string{"SELECT"}}),
		"system database":  {Version: 1, Grants: roleGrants{Databases: []roleDBGrants{{Database: "mysql", Objects: []roleObjectGrant{{Type: "database", Privileges: []string{"SELECT"}}}}}}},
		"bad revoke":       func() rolePayload { p := good; p.RevokeDatabases = []string{"sys"}; return p }(),
		"bad validUntil":   func() rolePayload { p := good; p.ValidUntil = "soon"; return p }(),
		"lower-case privs": mysqlPayload(roleObjectGrant{Type: "database", Privileges: []string{"select"}}),
	}
	for name, p := range bad {
		if err := checkMySQLRolePayload(p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The statements that make an account's grants in one database exactly the wanted ones: missing
// privileges granted, extra ones (GRANT OPTION and column grants included) revoked, nothing said
// about what already matches.
func TestMySQLGrantDiff(t *testing.T) {
	held := mysqlGrants{
		{"database", `shop`}:      {"SELECT": true, "INSERT": true, "GRANT OPTION": true}, // the wildcard form
		{"database", `shop\_eu`}:  {"SELECT": true},
		{"table", "orders"}:       {"SELECT": true, "DELETE": true},
		{"table", "same"}:         {"SELECT": true},
		{"procedure", "refund"}:   {"EXECUTE": true},
		{"function", "untouched"}: {"EXECUTE": true},
	}
	cols := []mysqlColumnGrant{{Table: "orders", Column: "note", Privilege: "UPDATE"}}
	wanted := mysqlGrants{
		{"database", `shop`}:      {"SELECT": true},
		{"table", "orders"}:       {"SELECT": true, "UPDATE": true},
		{"table", "same"}:         {"SELECT": true},
		{"function", "total"}:     {"EXECUTE": true},
		{"function", "untouched"}: {"EXECUTE": true},
	}
	got := mysqlGrantDiff("'r'@'%'", "shop", held, cols, wanted)
	want := []string{
		"GRANT EXECUTE ON FUNCTION `shop`.`total` TO 'r'@'%'",
		"GRANT UPDATE ON `shop`.`orders` TO 'r'@'%'",
		"REVOKE INSERT, GRANT OPTION ON `shop`.* FROM 'r'@'%'",
		"REVOKE SELECT ON `shop\\_eu`.* FROM 'r'@'%'",
		"REVOKE EXECUTE ON PROCEDURE `shop`.`refund` FROM 'r'@'%'",
		"REVOKE DELETE ON `shop`.`orders` FROM 'r'@'%'",
		"REVOKE UPDATE (`note`) ON `shop`.`orders` FROM 'r'@'%'",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("statements:\n got %q\nwant %q", got, want)
	}
}

// The payload's objects as grants: a database-level grant under its escaped pattern, so it covers
// this database alone.
func TestMySQLWantedGrants(t *testing.T) {
	got := mysqlWantedGrants(roleDBGrants{Database: "agent_it_db", Objects: []roleObjectGrant{
		{Type: "database", Privileges: []string{"SELECT"}},
		{Type: "table", Name: "t", Privileges: []string{"SELECT"}},
		{Type: "table", Name: "t", Privileges: []string{"INSERT"}},
	}})
	want := mysqlGrants{
		{"database", `agent\_it\_db`}: {"SELECT": true},
		{"table", "t"}:                {"SELECT": true, "INSERT": true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

// A policy's verbs as MySQL privileges: TRUNCATE TABLE needs DROP in MySQL.
func TestMySQLPolicyPrivileges(t *testing.T) {
	for in, want := range map[string][]string{
		"READ":           {"SELECT"},
		"WRITE":          {"INSERT", "UPDATE", "DELETE"},
		"READ,TRUNCATE":  {"SELECT", "DROP"},
		"DELETE,INSERT":  {"INSERT", "DELETE"},
		"READ,SELECT":    {"SELECT"},
		"TRUNCATE,WRITE": {"INSERT", "UPDATE", "DELETE", "DROP"},
	} {
		if got := mysqlPolicyPrivileges(strings.Split(in, ",")); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: %v, want %v", in, got, want)
		}
	}
}

// A policy's table is in the policy's database: bare, under PostgreSQL's default schema name, or
// under the database's own name -- case kept, as MySQL table names are case-sensitive on Linux.
func TestMySQLPolicyTable(t *testing.T) {
	for in, want := range map[string]string{
		"Orders": "`shop`.`Orders`", "public.Orders": "`shop`.`Orders`", "SHOP.Orders": "`shop`.`Orders`", " o`dd ": "`shop`.`o``dd`",
	} {
		got, err := mysqlPolicyTable("shop", in)
		if err != nil || got != want {
			t.Errorf("%q: %q %v, want %q", in, got, err, want)
		}
	}
	if _, err := mysqlPolicyTable("shop", "other.orders"); err == nil {
		t.Error("a table in another database was accepted")
	}
}

// Jobs for an engine the agent does not serve are not claimed.
func TestVaultJobsOnlyForKnownEngines(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeVaultBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	n, err := ProcessVaultJobs(nil, newFakeProxy(), fakeHostgroups{}, DBConfig{API: srv.URL, Host: "h", DBType: "oracle"}, time.Now())
	if err != nil || n != 0 || len(fb.calls) != 0 {
		t.Fatalf("processed %d, err %v, calls %v", n, err, fb.calls)
	}
}
