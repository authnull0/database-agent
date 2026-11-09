package pkg

import (
	"reflect"
	"strings"
	"testing"
)

func rwPublic() roleDBGrants {
	return roleDBGrants{Database: "appdb", Objects: []roleObjectGrant{
		{Type: "schema", Schema: "public", Privileges: []string{"USAGE"}},
		{Type: "all_tables", Schema: "public", Privileges: []string{"DELETE", "INSERT", "SELECT", "UPDATE"}, Future: true},
		{Type: "all_sequences", Schema: "public", Privileges: []string{"SELECT", "USAGE"}, Future: true},
		{Type: "table", Schema: "sales", Name: "orders", Privileges: []string{"SELECT"}},
		{Type: "function", Schema: "public", Name: "report", Privileges: []string{"EXECUTE"}},
		{Type: "sequence", Schema: "public", Name: "ids", Privileges: []string{"USAGE"}},
	}}
}

func TestConsoleRoleGrantStatements(t *testing.T) {
	owners := map[string][]string{"public": {"app_owner", "etl"}}
	got := grantStatements("app_rw", rwPublic(), owners)
	want := []string{
		`GRANT CONNECT ON DATABASE "appdb" TO "app_rw"`,
		`GRANT USAGE ON SCHEMA "public" TO "app_rw"`,
		`GRANT DELETE, INSERT, SELECT, UPDATE ON ALL TABLES IN SCHEMA "public" TO "app_rw"`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "public" GRANT DELETE, INSERT, SELECT, UPDATE ON TABLES TO "app_rw"`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "etl" IN SCHEMA "public" GRANT DELETE, INSERT, SELECT, UPDATE ON TABLES TO "app_rw"`,
		`GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA "public" TO "app_rw"`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "public" GRANT SELECT, USAGE ON SEQUENCES TO "app_rw"`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "etl" IN SCHEMA "public" GRANT SELECT, USAGE ON SEQUENCES TO "app_rw"`,
		`GRANT SELECT ON TABLE "sales"."orders" TO "app_rw"`,
		`GRANT EXECUTE ON FUNCTION "public"."report" TO "app_rw"`,
		`GRANT USAGE ON SEQUENCE "public"."ids" TO "app_rw"`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("grants:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	// A role name that needs quoting is quoted, never interpolated.
	for _, s := range grantStatements(`we"ird`, rwPublic(), nil) {
		if !strings.Contains(s, `"we""ird"`) {
			t.Errorf("unquoted role: %s", s)
		}
	}
}

func TestConsoleRoleRevokeStatements(t *testing.T) {
	got := revokeStatements("app_rw", "appdb", []string{"public", "sales"},
		[]defaultACL{{Owner: "app_owner", Schema: "public", ObjType: "r"}, {Owner: "etl", Schema: "sales", ObjType: "S"}, {Owner: "x", Schema: "public", ObjType: "f"}})
	want := []string{
		`REVOKE ALL ON ALL TABLES IN SCHEMA "public" FROM "app_rw"`,
		`REVOKE ALL ON ALL SEQUENCES IN SCHEMA "public" FROM "app_rw"`,
		`REVOKE ALL ON ALL FUNCTIONS IN SCHEMA "public" FROM "app_rw"`,
		`REVOKE ALL ON SCHEMA "public" FROM "app_rw"`,
		`REVOKE ALL ON ALL TABLES IN SCHEMA "sales" FROM "app_rw"`,
		`REVOKE ALL ON ALL SEQUENCES IN SCHEMA "sales" FROM "app_rw"`,
		`REVOKE ALL ON ALL FUNCTIONS IN SCHEMA "sales" FROM "app_rw"`,
		`REVOKE ALL ON SCHEMA "sales" FROM "app_rw"`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "public" REVOKE ALL ON TABLES FROM "app_rw"`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "etl" IN SCHEMA "sales" REVOKE ALL ON SEQUENCES FROM "app_rw"`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "x" IN SCHEMA "public" REVOKE ALL ON FUNCTIONS FROM "app_rw"`,
		`REVOKE ALL ON DATABASE "appdb" FROM "app_rw"`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("revokes:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// The agent checks the payload itself: whatever reaches it, only these grants can be made.
func TestConsoleRolePayloadChecked(t *testing.T) {
	ok := rolePayload{Version: 1, Create: true, Grants: roleGrants{Databases: []roleDBGrants{rwPublic()}}}
	if err := checkRolePayload(ok); err != nil {
		t.Fatalf("a good payload: %v", err)
	}
	bad := map[string]func(p *rolePayload){
		"no databases":       func(p *rolePayload) { p.Grants.Databases = nil },
		"template database":  func(p *rolePayload) { p.Grants.Databases[0].Database = "template1" },
		"quoted database":    func(p *rolePayload) { p.Grants.Databases[0].Database = `a"b` },
		"system schema":      func(p *rolePayload) { p.Grants.Databases[0].Objects[0].Schema = "pg_catalog" },
		"unknown type":       func(p *rolePayload) { p.Grants.Databases[0].Objects[0].Type = "database" },
		"ALL":                func(p *rolePayload) { p.Grants.Databases[0].Objects[1].Privileges = []string{"ALL"} },
		"injection":          func(p *rolePayload) { p.Grants.Databases[0].Objects[1].Privileges = []string{"SELECT ON x TO y; --"} },
		"execute on a table": func(p *rolePayload) { p.Grants.Databases[0].Objects[3].Privileges = []string{"EXECUTE"} },
		"named all_":         func(p *rolePayload) { p.Grants.Databases[0].Objects[1].Name = "t" },
		"bad table name":     func(p *rolePayload) { p.Grants.Databases[0].Objects[3].Name = "a b" },
		"bad revoke db":      func(p *rolePayload) { p.RevokeDatabases = []string{"template0"} },
		"bad expiry":         func(p *rolePayload) { p.ValidUntil = "tomorrow" },
	}
	for name, mutate := range bad {
		p := ok
		p.Grants = roleGrants{Databases: []roleDBGrants{rwPublic()}}
		mutate(&p)
		if err := checkRolePayload(p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestConsoleRoleMark(t *testing.T) {
	if !isConsoleRole(consoleRoleMark) || isConsoleRole("") || isConsoleRole("authnull:host=1.2.3.4") {
		t.Error("the console mark")
	}
}
