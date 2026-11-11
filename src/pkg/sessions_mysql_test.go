package pkg

import (
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// A MySQL target's sessions come from stats_mysql_processlist (its database column is db), and a
// kill is the admin interface's own KILL CONNECTION: OK, or an error that is the failed kill's reason.
func TestMySQLSessionAdmin(t *testing.T) {
	db, f := newFakeAdmin(t, func(q string) ([]string, [][]driver.Value, error) {
		switch {
		case strings.Contains(q, "stats_mysql_processlist"):
			return []string{"SessionID", "user", "db", "cli_host", "cli_port", "hostgroup", "time_ms", "command"},
				[][]driver.Value{{int64(12), "app", "shop", "10.0.0.9", int64(51000), int64(4), int64(2500), "Query"}}, nil
		case q == "KILL CONNECTION 13":
			return nil, nil, errors.New("Error 1094: Unknown thread id: 13")
		}
		return nil, nil, nil
	})
	admin := adminSessions{db: db, engine: EngineMySQL}
	rows, err := admin.List()
	if err != nil {
		t.Fatal(err)
	}
	want := []processRow{{ID: 12, User: "app", Database: "shop", ClientHost: "10.0.0.9", ClientPort: 51000, Hostgroup: 4, TimeMs: 2500, Command: "Query"}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows: %+v", rows)
	}
	if q := f.statements()[0]; !strings.Contains(q, "coalesce(db, '')") || strings.Contains(q, "info") || strings.Contains(q, "pgsql") {
		t.Fatalf("query: %s", q)
	}
	if ok, _, err := admin.Kill(12); !ok || err != nil {
		t.Fatalf("kill 12: %v %v", ok, err)
	}
	if ok, _, err := admin.Kill(13); ok || err == nil || !strings.Contains(err.Error(), "Unknown thread") {
		t.Fatalf("kill 13: %v %v", ok, err)
	}
	if got := f.statements()[1:]; !reflect.DeepEqual(got, []string{"KILL CONNECTION 12", "KILL CONNECTION 13"}) {
		t.Fatalf("kills: %q", got)
	}
}

// A MySQL target's sessions are reported like a PostgreSQL target's (they used to be skipped).
func TestSessionsReportForMySQL(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeSessionBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	cfg := DBConfig{API: srv.URL, Host: "192.168.122.175", Port: "3306", DBType: "mysql"}
	admin := &fakeSessionAdmin{rows: []processRow{{ID: 3, User: "app", Database: "shop", Hostgroup: 4, TimeMs: 10}}}
	if err := (&sessionsTracker{}).round(admin, cfg, 4, false); err != nil {
		t.Fatal(err)
	}
	if len(fb.reports) != 1 || fb.reports[0]["host"] != "192.168.122.175:3306" {
		t.Fatalf("reports: %v", fb.reports)
	}
	if ids := reportedIDs(t, fb.reports[0]); len(ids) != 1 || ids[0] != 3 {
		t.Fatalf("ids %v", ids)
	}
	cfg.DBType = "oracle"
	if err := (&sessionsTracker{}).round(admin, cfg, 4, false); err != nil || len(fb.reports) != 1 {
		t.Fatalf("an unknown engine was reported: %v", fb.reports)
	}
}

// "This agent serves one server" is per engine: ProxySQL's MySQL and PostgreSQL sessions are in
// separate lists, so a session not yet in a hostgroup is the one server's of its engine.
func TestSingleServerPerEngine(t *testing.T) {
	cases := []struct {
		types []string
		want  []bool
	}{
		{[]string{"postgres"}, []bool{true}},
		{[]string{"postgres", "mysql"}, []bool{true, true}},
		{[]string{"postgres", "", "mysql"}, []bool{false, false, true}},
		{[]string{"mysql", "mariadb"}, []bool{false, false}},
	}
	for _, c := range cases {
		var targets []DBConfig
		for _, ty := range c.types {
			targets = append(targets, DBConfig{DBType: ty})
		}
		if got := singleServerPerEngine(targets); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%v: %v, want %v", c.types, got, c.want)
		}
	}
}
