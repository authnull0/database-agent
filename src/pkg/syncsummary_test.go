package pkg

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeDBUserBackend answers /dbUser with 200, or 500 for the names in fail; it counts the posts.
func fakeDBUserBackend(t *testing.T, fail ...string) (*httptest.Server, func() int) {
	t.Helper()
	t.Cleanup(resetInstallCredentialForTest)
	var mu sync.Mutex
	posts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/databaseService/dbUser" {
			w.WriteHeader(404)
			return
		}
		var p map[string]any
		json.NewDecoder(r.Body).Decode(&p)
		mu.Lock()
		posts++
		mu.Unlock()
		for _, f := range fail {
			if p["userName"] == f {
				w.WriteHeader(500)
				w.Write([]byte(`{"code":500,"message":"boom"}`))
				return
			}
		}
		w.Write([]byte(`{"code":200,"message":"ok"}`))
	}))
	t.Cleanup(srv.Close)
	return srv, func() int { mu.Lock(); defer mu.Unlock(); return posts }
}

var summaryUsers = []databaseUser{
	{Username: "alice", Host: "%", Privileges: "CONNECT", Role: "User", Level: PrivilegeStandard},
	{Username: "bob", Host: "%", Privileges: "CONNECT", Role: "User", Level: PrivilegeStandard},
	{Username: "carol", Host: "%", Privileges: "ALL PRIVILEGES", Role: "Admin", Level: PrivilegeAdmin},
}

// The per-database summary: name, engine, address, users, failures.
func TestSyncSummaryLine(t *testing.T) {
	pg := DBConfig{Host: "192.168.122.175", Port: "5432", DBType: "postgres"}
	if got, want := syncSummary("appdb", pg, 42, 0), "Synced appdb (postgres, 192.168.122.175:5432): 42 users, 0 failed"; got != want {
		t.Errorf("got %q\nwant %q", got, want)
	}
	my := DBConfig{Host: "10.0.0.11", DBType: "mysql"}
	if got, want := syncSummary("shop", my, 1, 1), "Synced shop (mysql, 10.0.0.11:3306): 1 users, 1 failed"; got != want {
		t.Errorf("got %q\nwant %q", got, want)
	}
}

// At the default level a sync logs one summary per database and each failure with its status,
// and none of the per-user lines.
func TestReportDatabaseUsersQuietAtDefault(t *testing.T) {
	srv, posts := fakeDBUserBackend(t, "bob")
	buf := captureLog(t, false)
	cfg := DBConfig{API: srv.URL, Host: "192.168.122.175", Port: "5432", DBType: "postgres"}

	reportDatabaseUsers(summaryUsers, "appdb", cfg, "inst-1")

	if posts() != 3 {
		t.Fatalf("posts: %d", posts())
	}
	out := buf.String()
	if !strings.Contains(out, "Synced appdb (postgres, 192.168.122.175:5432): 3 users, 1 failed") {
		t.Errorf("summary missing:\n%q", out)
	}
	if !strings.Contains(out, "bob") || !strings.Contains(out, "500") {
		t.Errorf("the failure for bob with its status is missing:\n%s", out)
	}
	for _, noise := range []string{"Database Name:", "Sending User Payload", "Payload Sent", "Response from dbUser API", "alice", "carol"} {
		if strings.Contains(out, noise) {
			t.Errorf("%q logged at the default level:\n%s", noise, out)
		}
	}
	if n := strings.Count(out, "\n"); n != 2 {
		t.Errorf("want 2 lines (one failure, one summary), got %d:\n%s", n, out)
	}
}

// With LOG_LEVEL=debug the per-user detail comes back.
func TestReportDatabaseUsersDetailAtDebug(t *testing.T) {
	srv, _ := fakeDBUserBackend(t)
	buf := captureLog(t, true)
	cfg := DBConfig{API: srv.URL, Host: "192.168.122.175", Port: "5432", DBType: "postgres"}

	reportDatabaseUsers(summaryUsers, "appdb", cfg, "inst-1")

	out := buf.String()
	for _, want := range []string{"User: alice", "Payload Sent", "Response from dbUser API",
		"Synced appdb (postgres, 192.168.122.175:5432): 3 users, 0 failed"} {
		if !strings.Contains(out, want) {
			t.Errorf("%q missing at debug:\n%s", want, out)
		}
	}
}

// A backend that cannot be reached: every user fails, and the summary says so.
func TestReportDatabaseUsersUnreachable(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	buf := captureLog(t, false)
	cfg := DBConfig{API: srv.URL, Host: "h", Port: "5432", DBType: "postgres"}
	reportDatabaseUsers(summaryUsers[:2], "appdb", cfg, "inst-1")
	if !strings.Contains(buf.String(), "Synced appdb (postgres, h:5432): 2 users, 2 failed") {
		t.Fatalf("summary:\n%s", buf.String())
	}
}
