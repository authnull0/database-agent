package pkg

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeSessionAdmin is ProxySQL's admin interface as the sessions loop uses it.
type fakeSessionAdmin struct {
	rows   []processRow
	killed []int64
	known  map[int64]bool
	ended  map[int64]int64 // session -> new end, as set
}

func (f *fakeSessionAdmin) SetEnds(id, endsAt int64) (bool, string, error) {
	if f.ended == nil {
		f.ended = map[int64]int64{}
	}
	if !f.known[id] {
		return false, "no PgSQL session on an access grant", nil
	}
	f.ended[id] = endsAt
	return true, "session ends", nil
}

func (f *fakeSessionAdmin) List() ([]processRow, error) { return f.rows, nil }
func (f *fakeSessionAdmin) Kill(id int64) (bool, string, error) {
	f.killed = append(f.killed, id)
	if f.known[id] {
		return true, "session killed", nil
	}
	return false, "no PgSQL session", nil
}

type fakeSessionBackend struct {
	mu      sync.Mutex
	reports []map[string]any
	kill    []int64
	ends    []map[string]any
	raw     []string
}

func newFakeSessionBackend(t *testing.T) (*fakeSessionBackend, *httptest.Server) {
	fb := &fakeSessionBackend{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fb.mu.Lock()
		defer fb.mu.Unlock()
		if r.URL.Path == "/api/v1/databaseService/proxy/token" {
			json.NewEncoder(w).Encode(map[string]any{"accessToken": "dpt.2.s", "tokenType": "Bearer", "expiresIn": 900})
			return
		}
		if r.URL.Path != "/api/v1/databaseService/agent/sessions/report" {
			w.WriteHeader(404)
			return
		}
		var buf strings.Builder
		var body map[string]any
		dec := json.NewDecoder(r.Body)
		var raw json.RawMessage
		dec.Decode(&raw)
		buf.Write(raw)
		json.Unmarshal(raw, &body)
		fb.raw = append(fb.raw, buf.String())
		fb.reports = append(fb.reports, body)
		json.NewEncoder(w).Encode(map[string]any{"kill": fb.kill, "ends": fb.ends})
		fb.kill, fb.ends = nil, nil
	}))
	t.Cleanup(srv.Close)
	return fb, srv
}

func TestSessionsReportAndKill(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeSessionBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	cfg := DBConfig{API: srv.URL, Host: "192.168.122.175", Port: "5432", DBType: "postgres"}
	admin := &fakeSessionAdmin{known: map[int64]bool{7: true}, rows: []processRow{
		{ID: 7, User: "an_b4d72e627436cfb5", Database: "dbmfa_test", ClientHost: "10.0.0.5", ClientPort: 5000, Hostgroup: 6, TimeMs: 9000, Command: "Sleep"},
		{ID: 8, User: "dbmfa_writer", Database: "dbmfa_test", ClientHost: "10.0.0.6", ClientPort: 5001, Hostgroup: 9, TimeMs: 1000},
		{ID: 9, User: "dbmfa_writer", Database: "dbmfa_test", ClientHost: "10.0.0.7", ClientPort: 5002, Hostgroup: -1, TimeMs: 500},
	}}
	tracker := &sessionsTracker{}

	// This server's hostgroup only; another server's session is not this report's.
	if err := tracker.round(admin, cfg, 6, false); err != nil {
		t.Fatal(err)
	}
	ids := reportedIDs(t, fb.reports[0])
	if len(ids) != 1 || ids[0] != 7 || fb.reports[0]["host"] != "192.168.122.175:5432" {
		t.Fatalf("report: %v", fb.reports[0])
	}
	// The query text is never read, so never sent.
	if strings.Contains(fb.raw[0], "info") || strings.Contains(fb.raw[0], "select") {
		t.Errorf("the report carries query text: %s", fb.raw[0])
	}

	// An agent serving one server: a session with no hostgroup yet is that server's.
	if err := tracker.round(admin, cfg, 6, true); err != nil {
		t.Fatal(err)
	}
	if ids := reportedIDs(t, fb.reports[1]); len(ids) != 2 {
		t.Errorf("single server: %v", ids)
	}

	// Kills handed over are run; how they went is in the next report.
	fb.kill = []int64{7, 99}
	tracker.round(admin, cfg, 6, false)
	if len(admin.killed) != 2 {
		t.Fatalf("kills run: %v", admin.killed)
	}
	tracker.round(admin, cfg, 6, false)
	killed, _ := fb.reports[3]["killed"].([]any)
	if len(killed) != 2 {
		t.Fatalf("results reported: %v", fb.reports[3]["killed"])
	}
	results := map[float64]map[string]any{}
	for _, k := range killed {
		m := k.(map[string]any)
		results[m["id"].(float64)] = m
	}
	if results[7]["ok"] != true || results[99]["ok"] != false || !strings.Contains(results[99]["error"].(string), "no PgSQL session") {
		t.Errorf("results: %v", results)
	}
	// Reported once, not again.
	tracker.round(admin, cfg, 6, false)
	if k, _ := fb.reports[4]["killed"].([]any); len(k) != 0 {
		t.Errorf("results sent twice: %v", k)
	}
}

func TestNoSessionsReportWithoutACredential(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeSessionBackend(t)
	SetInstallCredential(srv.URL, "", "")
	cfg := DBConfig{API: srv.URL, Host: "h", Port: "5432", DBType: "postgres"}
	if err := (&sessionsTracker{}).round(&fakeSessionAdmin{}, cfg, 1, true); err != nil || len(fb.reports) != 0 {
		t.Fatalf("reported without a credential: %v %v", err, fb.reports)
	}
}

func reportedIDs(t *testing.T, report map[string]any) []int64 {
	t.Helper()
	list, _ := report["sessions"].([]any)
	var out []int64
	for _, s := range list {
		out = append(out, int64(s.(map[string]any)["id"].(float64)))
	}
	return out
}

// An extended grant: the backend hands back new ends for the grant's open sessions with the report's
// answer; the agent sets them in ProxySQL (PGSQL|MYSQL SET SESSION ENDS) and says how it went in the
// next report, once.
func TestSessionsNewEnds(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeSessionBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	cfg := DBConfig{API: srv.URL, Host: "192.168.122.175", Port: "5432", DBType: "postgres"}
	admin := &fakeSessionAdmin{known: map[int64]bool{7: true}, rows: []processRow{{ID: 7, Hostgroup: 6}}}
	tracker := &sessionsTracker{}

	fb.ends = []map[string]any{{"id": 7, "endsAt": 1794303775}, {"id": 99, "endsAt": 1794303775}}
	if err := tracker.round(admin, cfg, 6, false); err != nil {
		t.Fatal(err)
	}
	if admin.ended[7] != 1794303775 || len(admin.ended) != 1 {
		t.Fatalf("ends set: %v", admin.ended)
	}
	tracker.round(admin, cfg, 6, false)
	set, _ := fb.reports[1]["endsSet"].([]any)
	if len(set) != 2 {
		t.Fatalf("outcomes: %v", fb.reports[1]["endsSet"])
	}
	results := map[float64]map[string]any{}
	for _, k := range set {
		m := k.(map[string]any)
		results[m["id"].(float64)] = m
	}
	if results[7]["ok"] != true || results[99]["ok"] != false || !strings.Contains(results[99]["error"].(string), "no PgSQL session") {
		t.Errorf("outcomes: %v", results)
	}
	tracker.round(admin, cfg, 6, false)
	if k, _ := fb.reports[2]["endsSet"].([]any); len(k) != 0 {
		t.Errorf("outcomes sent twice: %v", k)
	}
}

// The admin command names the engine: ProxySQL numbers its MySQL and PostgreSQL sessions separately.
func TestSetEndsStatement(t *testing.T) {
	if got := setEndsStatement(EnginePostgres, 42, 1794303775); got != "PGSQL SET SESSION ENDS 42 1794303775" {
		t.Errorf("postgres: %q", got)
	}
	if got := setEndsStatement(EngineMySQL, 7, 1794303775); got != "MYSQL SET SESSION ENDS 7 1794303775" {
		t.Errorf("mysql: %q", got)
	}
}
