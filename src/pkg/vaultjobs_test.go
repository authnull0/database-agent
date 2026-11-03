package pkg

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestVaultCommentMarksTheRole(t *testing.T) {
	at := time.Unix(1793800000, 0)
	c := vaultComment("192.168.122.175", at)
	host, rotated, ok := parseRotationComment(c)
	if !ok || host != "192.168.122.175" || !rotated.Equal(at) || !isVaultManaged(c) {
		t.Fatalf("%q: %q %v %v vault=%v", c, host, rotated, ok, isVaultManaged(c))
	}
	for _, other := range []string{rotationComment("h", at), "", "some admin note", "authnull:vaulted;host=h;rotated=1"} {
		if isVaultManaged(other) {
			t.Errorf("%q read as vault-managed", other)
		}
	}
}

// The agent's own sweep must not touch a role whose password the vault holds: it would replace
// a password the backend knows with one nobody does.
func TestSweepLeavesVaultRolesAlone(t *testing.T) {
	fp := newFakeProxy()
	long := time.Now().Add(-30 * 24 * time.Hour)
	fp.users["vaulted"] = proxyUser{Username: "vaulted", Password: "p", DefaultHostgroup: 1, Comment: vaultComment("h", long)}
	rotated, err := RotateDuePasswords(nil, fp, "h", time.Now(), time.Hour)
	if err != nil || len(rotated) != 0 || fp.puts != 0 {
		t.Fatalf("rotated %v, err %v, writes %d", rotated, err, fp.puts)
	}
}

// fakeVaultBackend answers the agent's job calls the way authnull-service's dbvault handlers do.
type fakeVaultBackend struct {
	mu       sync.Mutex
	jobs     []map[string]any
	password string
	results  []map[string]any
	calls    []string
	auth     []string
}

func newFakeVaultBackend(t *testing.T) (*fakeVaultBackend, *httptest.Server) {
	fb := &fakeVaultBackend{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fb.mu.Lock()
		defer fb.mu.Unlock()
		if r.URL.Path == "/api/v1/databaseService/proxy/token" {
			json.NewEncoder(w).Encode(map[string]any{"accessToken": "dpt.2.vault", "tokenType": "Bearer", "expiresIn": 900})
			return
		}
		fb.calls = append(fb.calls, r.URL.Path)
		fb.auth = append(fb.auth, r.Header.Get("Authorization"))
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		switch r.URL.Path {
		case "/api/v1/databaseService/agent/jobs/claim":
			json.NewEncoder(w).Encode(map[string]any{"jobs": fb.jobs})
			fb.jobs = nil
		case "/api/v1/databaseService/agent/jobs/secret":
			w.Header().Set("Cache-Control", "no-store")
			json.NewEncoder(w).Encode(map[string]any{"password": fb.password})
		case "/api/v1/databaseService/agent/jobs/result":
			fb.results = append(fb.results, body)
			w.Write([]byte(`{"recorded":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return fb, srv
}

type fakeHostgroups map[string]int

func (f fakeHostgroups) HostgroupFor(host string) (int, bool, error) {
	hg, ok := f[host]
	return hg, ok, nil
}

// An agent without a credential cannot be given passwords: it does not ask.
func TestNoVaultJobsWithoutACredential(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeVaultBackend(t)
	SetInstallCredential(srv.URL, "", "")
	n, err := ProcessVaultJobs(nil, newFakeProxy(), fakeHostgroups{}, DBConfig{API: srv.URL, Host: "h", Port: "5432", DBType: "postgres"}, time.Now())
	if err != nil || n != 0 || len(fb.calls) != 0 {
		t.Fatalf("processed %d, err %v, calls %v", n, err, fb.calls)
	}
}

// The whole job against a real Postgres: the role's password becomes the vault's -- the role can
// log in with it -- ProxySQL gets it and the vault mark, and the result is reported. Then the
// agent's own checkout and sweep leave it alone.
func TestVaultJobAgainstPostgres(t *testing.T) {
	db, dsn := testPG(t)
	t.Cleanup(resetInstallCredentialForTest)
	role, missing := testRolePrefix+"vault_takeover", testRolePrefix+"vault_missing"
	dropRoles(t, db, role, missing)
	t.Cleanup(func() { dropRoles(t, db, role, missing) })
	if _, err := db.Exec(pgSetPasswordStatement(role, "customers-old-password", true)); err != nil {
		t.Fatal(err)
	}

	fb, srv := newFakeVaultBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	cfg := DBConfig{API: srv.URL, Host: "192.168.122.175", Port: "5432", DBType: "postgres"}
	fp := newFakeProxy()
	hg := fakeHostgroups{"192.168.122.175": 3}
	const newPassword = "VaultHeldPassword0123456789abcdef"
	fb.password = newPassword
	fb.jobs = []map[string]any{
		{"id": "job-1", "kind": "rotate_password", "host": "192.168.122.175", "port": 5432, "dbType": "postgres", "role": role},
		{"id": "job-2", "kind": "rotate_password", "host": "192.168.122.175", "port": 5432, "dbType": "postgres", "role": missing},
		{"id": "job-3", "kind": "rotate_password", "host": "10.9.9.9", "port": 5432, "dbType": "postgres", "role": role},
	}
	now := time.Unix(1793800000, 0)
	n, err := ProcessVaultJobs(db, fp, hg, cfg, now)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("%d jobs applied, want 1", n)
	}

	// The claim named this agent's host, and every call carried the token.
	if len(fb.calls) == 0 || fb.calls[0] != "/api/v1/databaseService/agent/jobs/claim" {
		t.Fatalf("calls: %v", fb.calls)
	}
	for i, a := range fb.auth {
		if a != "Bearer dpt.2.vault" {
			t.Errorf("call %s without the token: %q", fb.calls[i], a)
		}
	}

	// The database: the role logs in with the vault's password, not the old one.
	if !canLogin(t, dsn, role, newPassword) {
		t.Fatal("the role cannot log in with the vault's password")
	}
	if canLogin(t, dsn, role, "customers-old-password") {
		t.Fatal("the old password still works after the takeover")
	}
	// ProxySQL: the new password, the host's hostgroup, the vault mark.
	u, ok := fp.users[role]
	if !ok || u.Password != newPassword || u.DefaultHostgroup != 3 || !isVaultManaged(u.Comment) {
		t.Fatalf("ProxySQL row: %+v", u)
	}

	// Results: job-1 ok; job-2 failed without creating the role, and without the password in it;
	// job-3, for another host, refused rather than applied here.
	got := map[string]map[string]any{}
	for _, r := range fb.results {
		got[r["jobId"].(string)] = r
	}
	if got["job-1"]["ok"] != true {
		t.Errorf("job-1: %v", got["job-1"])
	}
	if r := got["job-2"]; r["ok"] != false || !strings.Contains(r["error"].(string), "does not exist") || strings.Contains(r["error"].(string), newPassword) {
		t.Errorf("job-2: %v", r)
	}
	if exists, _ := pgRoleExists(db, missing); exists {
		t.Error("a vault job created a role")
	}
	if r := got["job-3"]; r["ok"] != false || !strings.Contains(r["error"].(string), "not served by this agent") {
		t.Errorf("job-3: %v", r)
	}

	// Afterwards: checkout returns the vault's password without rotating, even when "due", and the
	// sweep skips the role.
	later := now.Add(90 * 24 * time.Hour)
	pw, err := EnsureRolePassword(db, fp, role, "192.168.122.175", 0, later, time.Hour)
	if err != nil || pw != newPassword {
		t.Fatalf("checkout of a vault-managed role: %v (changed: %v)", err, pw != newPassword)
	}
	if rotated, _ := RotateDuePasswords(db, fp, "192.168.122.175", later, time.Hour); len(rotated) != 0 {
		t.Errorf("the sweep rotated %v", rotated)
	}
	if !canLogin(t, dsn, role, newPassword) {
		t.Error("checkout or the sweep changed the vault's password")
	}
}

// The database took the password but ProxySQL could not be written, even on a retry: the job is
// still reported done -- with the reason -- so the vault keeps the password the database now has.
func TestVaultJobWhenProxySQLFailsAgainstPostgres(t *testing.T) {
	db, dsn := testPG(t)
	t.Cleanup(resetInstallCredentialForTest)
	role := testRolePrefix + "vault_proxy_down"
	dropRoles(t, db, role)
	t.Cleanup(func() { dropRoles(t, db, role) })
	if _, err := db.Exec(pgSetPasswordStatement(role, "old-password", true)); err != nil {
		t.Fatal(err)
	}
	fb, srv := newFakeVaultBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	const pw = "VaultHeldPasswordProxyDown0123456"
	fb.password = pw
	fb.jobs = []map[string]any{{"id": "job-p", "kind": "rotate_password", "host": "192.168.122.175", "role": role}}
	fp := newFakeProxy()
	fp.users[role] = proxyUser{Username: role, Password: "old-password", DefaultHostgroup: 2}
	fp.failPut[role] = true
	cfg := DBConfig{API: srv.URL, Host: "192.168.122.175", Port: "5432", DBType: "postgres"}
	if _, err := ProcessVaultJobs(db, fp, fakeHostgroups{}, cfg, time.Now()); err != nil {
		t.Fatal(err)
	}
	if !canLogin(t, dsn, role, pw) {
		t.Fatal("the database does not have the vault's password")
	}
	if len(fb.results) != 1 || fb.results[0]["ok"] != true || !strings.Contains(fb.results[0]["error"].(string), "ProxySQL could not be updated") {
		t.Fatalf("result: %v", fb.results)
	}
	if strings.Contains(fb.results[0]["error"].(string), pw) {
		t.Fatal("the password is in the reported error")
	}
}
