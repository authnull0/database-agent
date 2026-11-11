package pkg

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type fakeRecordingBackend struct {
	mu      sync.Mutex
	uploads []map[string]any
	fail    bool
}

func newFakeRecordingBackend(t *testing.T) (*fakeRecordingBackend, *httptest.Server) {
	fb := &fakeRecordingBackend{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fb.mu.Lock()
		defer fb.mu.Unlock()
		if r.URL.Path == "/api/v1/databaseService/proxy/token" {
			json.NewEncoder(w).Encode(map[string]any{"accessToken": "dpt.2.s", "tokenType": "Bearer", "expiresIn": 900})
			return
		}
		if r.URL.Path != "/api/v1/databaseService/agent/recordings/upload" {
			w.WriteHeader(404)
			return
		}
		if fb.fail {
			w.WriteHeader(500)
			return
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		fb.uploads = append(fb.uploads, body)
		n := len(body["statements"].([]any))
		json.NewEncoder(w).Encode(map[string]any{"stored": n})
	}))
	t.Cleanup(srv.Close)
	return fb, srv
}

func spoolFile(t *testing.T, dir string, bucket int64, lines ...string) string {
	t.Helper()
	p := filepath.Join(dir, fmt.Sprintf("%d.jsonl", bucket))
	data := ""
	for _, l := range lines {
		data += l + "\n"
	}
	if err := os.WriteFile(p, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func exists(p string) bool { _, err := os.Stat(p); return err == nil }

func TestRecordingsShippedPerServerAndDeleted(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeRecordingBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	dir := t.TempDir()
	now := time.Unix(1794303800, 0)
	closed := spoolFile(t, dir, 1794303775,
		`{"sid":4,"at":1794303776901,"hg":6,"stmt":"select x from t;","allowed":true,"rows":1}`,
		`{"sid":5,"at":1794303776902,"hg":9,"stmt":"select 1;","allowed":true,"rows":1}`,
		`not json`,
		`{"sid":4,"at":1794303776903,"hg":6,"stmt":"insert into t values (2);","allowed":false,"error":"Access denied"}`)
	open := spoolFile(t, dir, 1794303795, `{"sid":4,"at":1794303796000,"hg":6,"stmt":"select 3;","allowed":true}`)
	targets := []recordingTarget{
		{cfg: DBConfig{API: srv.URL, Host: "192.168.122.175", Port: "5432", DBType: "postgres"}, hostgroup: 6},
		{cfg: DBConfig{API: srv.URL, Host: "192.168.122.176", Port: "5432", DBType: "postgres"}, hostgroup: 9},
	}

	if err := shipRecordings(dir, now, targets); err != nil {
		t.Fatal(err)
	}
	if len(fb.uploads) != 2 {
		t.Fatalf("uploads: %v", fb.uploads)
	}
	byHost := map[string]map[string]any{}
	for _, u := range fb.uploads {
		byHost[u["host"].(string)] = u
	}
	a, b := byHost["192.168.122.175:5432"], byHost["192.168.122.176:5432"]
	if a == nil || b == nil || a["bucket"] != float64(1794303775) {
		t.Fatalf("uploads: %v", fb.uploads)
	}
	sa, sb := a["statements"].([]any), b["statements"].([]any)
	// Each statement carries its line in the file, so chunks and resends never collide.
	if len(sa) != 2 || len(sb) != 1 || sa[0].(map[string]any)["line"] != float64(0) || sa[1].(map[string]any)["line"] != float64(3) ||
		sb[0].(map[string]any)["line"] != float64(1) || sa[1].(map[string]any)["stmt"] != "insert into t values (2);" {
		t.Errorf("statements: %v / %v", sa, sb)
	}
	if exists(closed) {
		t.Error("a shipped file was kept")
	}
	if !exists(open) || len(fb.uploads) != 2 {
		t.Error("the bucket still being written was touched")
	}
}

func TestRecordingsKeptUntilTheBackendTakesThem(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeRecordingBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	dir := t.TempDir()
	now := time.Unix(1794303800, 0)
	f := spoolFile(t, dir, 1794303775, `{"sid":4,"at":1794303776901,"hg":3,"stmt":"select 1;","allowed":true}`)
	// One server: everything is its, whatever the hostgroup.
	targets := []recordingTarget{{cfg: DBConfig{API: srv.URL, Host: "h", Port: "5432", DBType: "postgres"}, hostgroup: 6}}

	fb.fail = true
	if err := shipRecordings(dir, now, targets); err == nil || !exists(f) {
		t.Fatalf("a failed upload: err %v, kept %v", err, exists(f))
	}
	fb.fail = false
	if err := shipRecordings(dir, now, targets); err != nil || exists(f) || len(fb.uploads) != 1 {
		t.Fatalf("retry: %v %v %v", err, exists(f), fb.uploads)
	}

	// Too old to matter any more (the backend never took it): dropped.
	stale := spoolFile(t, dir, now.Add(-8*24*time.Hour).Unix()/5*5, `{"sid":4,"at":1,"hg":6,"stmt":"x","allowed":true}`)
	fb.fail = true
	shipRecordings(dir, now, targets)
	if exists(stale) {
		t.Error("a week-old file was kept")
	}
}

func TestNoRecordingsShippedWithoutACredential(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeRecordingBackend(t)
	SetInstallCredential(srv.URL, "", "")
	dir := t.TempDir()
	f := spoolFile(t, dir, 1794303775, `{"sid":4,"at":1794303776901,"hg":6,"stmt":"select 1;","allowed":true}`)
	targets := []recordingTarget{{cfg: DBConfig{API: srv.URL, Host: "h", DBType: "postgres"}, hostgroup: 6}}
	if err := shipRecordings(dir, time.Unix(1794303800, 0), targets); err != nil || len(fb.uploads) != 0 || !exists(f) {
		t.Fatalf("shipped without a credential: %v %v", err, fb.uploads)
	}
	// No spool directory (nothing recorded yet): not an error.
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	if err := shipRecordings(filepath.Join(dir, "none"), time.Unix(1794303800, 0), targets); err != nil {
		t.Errorf("missing spool: %v", err)
	}
}

// ProxySQL numbers MySQL and PostgreSQL hostgroups separately: a MySQL and a PostgreSQL target can
// both be hostgroup 0, and each line's engine says whose it is. A line naming no engine (an older
// ProxySQL) goes by hostgroup alone, as before.
func TestRecordingsSplitByEngineAndHostgroup(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeRecordingBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	dir := t.TempDir()
	spoolFile(t, dir, 1794303775,
		`{"sid":4,"at":1794303776901,"hg":0,"engine":"mysql","stmt":"select 1","allowed":true}`,
		`{"sid":4,"at":1794303776902,"hg":0,"engine":"postgres","stmt":"select 2;","allowed":true}`,
		`{"sid":5,"at":1794303776903,"hg":0,"engine":"postgres","stmt":"select 3;","allowed":true}`)
	targets := []recordingTarget{
		{cfg: DBConfig{API: srv.URL, Host: "192.168.122.175", Port: "5432", DBType: "postgres"}, hostgroup: 0},
		{cfg: DBConfig{API: srv.URL, Host: "192.168.122.175", Port: "3306", DBType: "mysql"}, hostgroup: 0},
	}
	if err := shipRecordings(dir, time.Unix(1794303800, 0), targets); err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, u := range fb.uploads {
		got[u["host"].(string)] = len(u["statements"].([]any))
	}
	if got["192.168.122.175:3306"] != 1 || got["192.168.122.175:5432"] != 2 {
		t.Errorf("uploads per server: %v", got)
	}
}
