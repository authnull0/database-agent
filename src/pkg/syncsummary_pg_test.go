package pkg

import (
	"fmt"
	"strings"
	"testing"
)

// Against a real server (AGENT_TEST_DSN): FetchTablePrivileges posts every login and logs one
// summary line at the default level, none of the per-user ones.
func TestPostgresFetchTablePrivilegesLogsOneSummary(t *testing.T) {
	admin, u := agentTestDB(t)
	var dbName string
	if err := admin.QueryRow("SELECT current_database()").Scan(&dbName); err != nil {
		t.Fatal(err)
	}
	srv, posts := fakeDBUserBackend(t)
	buf := captureLog(t, false)
	cfg := DBConfig{API: srv.URL, Host: u.Hostname(), Port: u.Port(), DBType: "postgres"}

	if err := FetchTablePrivileges(admin, dbName, cfg, "inst-1"); err != nil {
		t.Fatal(err)
	}
	if posts() == 0 {
		t.Fatal("no users posted")
	}
	out := buf.String()
	want := fmt.Sprintf("Synced %s (postgres, %s:%d): %d users, 0 failed", dbName, cfg.Host, targetPort(cfg), posts())
	if !strings.Contains(out, want) {
		t.Errorf("want %q in:\n%s", want, out)
	}
	if strings.Contains(out, "Payload Sent") || strings.Contains(out, "Database Name:") {
		t.Errorf("per-user lines at the default level:\n%s", out)
	}
}
