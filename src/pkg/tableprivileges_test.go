package pkg

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

// The dbUser payload carries tablePrivileges as an array of {table, privileges}, never null, next
// to the fields it always had.
func TestDBUserPayloadTablePrivileges(t *testing.T) {
	cfg := DBConfig{OrgID: "3", TenantID: "7", DBType: "postgres", Host: "10.0.0.9"}
	u := databaseUser{Username: "alice", Host: "%", Privileges: "CONNECT, INSERT, SELECT", Role: "User", Level: PrivilegeStandard,
		TablePrivileges: []tablePrivilege{{Table: "public.orders", Privileges: []string{"INSERT", "SELECT"}}}}
	b, err := json.Marshal(dbUserPayload(cfg, "shop", "inst-1", "10.0.0.1", u))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if s := string(got["tablePrivileges"]); s != `[{"table":"public.orders","privileges":["INSERT","SELECT"]}]` {
		t.Errorf("tablePrivileges: %s", s)
	}
	if s := string(got["privilege"]); s != `"CONNECT, INSERT, SELECT"` {
		t.Errorf("privilege: %s", s)
	}
	for _, k := range []string{"orgId", "tenantId", "databaseType", "databaseName", "userName", "host", "role",
		"privilegeLevel", "instanceId", "agentVmIp", "hostVmIp"} {
		if _, ok := got[k]; !ok {
			t.Errorf("payload lost %s: %s", k, b)
		}
	}

	// None held (or a superuser): an empty array, not null.
	u.TablePrivileges = nil
	b, _ = json.Marshal(dbUserPayload(cfg, "shop", "inst-1", "10.0.0.1", u))
	json.Unmarshal(b, &got)
	if s := string(got["tablePrivileges"]); s != `[]` {
		t.Errorf("no table privileges: %s", s)
	}
}

// Table privileges are upper-cased, de-duplicated and sorted, tables sorted, empty ones dropped,
// and at most maxTablePrivileges tables are sent.
func TestNormalizeTablePrivileges(t *testing.T) {
	got := normalizeTablePrivileges([]tablePrivilege{
		{Table: "public.b", Privileges: []string{"select", "insert", "SELECT"}},
		{Table: "public.empty"},
		{Table: "public.a", Privileges: []string{"UPDATE"}},
	})
	want := []tablePrivilege{
		{Table: "public.a", Privileges: []string{"UPDATE"}},
		{Table: "public.b", Privileges: []string{"INSERT", "SELECT"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v", got)
	}

	var many []tablePrivilege
	for i := maxTablePrivileges + 100; i > 0; i-- {
		many = append(many, tablePrivilege{Table: fmt.Sprintf("public.t%04d", i), Privileges: []string{"SELECT"}})
	}
	capped := normalizeTablePrivileges(many)
	if len(capped) != maxTablePrivileges || maxTablePrivileges != 500 {
		t.Fatalf("capped to %d (max %d)", len(capped), maxTablePrivileges)
	}
	if capped[0].Table != "public.t0001" || capped[499].Table != "public.t0500" {
		t.Errorf("cap kept %s..%s", capped[0].Table, capped[499].Table)
	}
}

// The privilege string: the database-level privileges and every table privilege, once each, sorted.
func TestPrivilegeSummary(t *testing.T) {
	got := privilegeSummary([]string{"TEMPORARY", "connect"},
		[]tablePrivilege{{Table: "public.a", Privileges: []string{"SELECT", "INSERT"}}, {Table: "public.b", Privileges: []string{"SELECT"}}})
	if got != "CONNECT, INSERT, SELECT, TEMPORARY" {
		t.Errorf("got %q", got)
	}
	if got := privilegeSummary(nil, nil); got != "" {
		t.Errorf("nothing held: %q", got)
	}
}
