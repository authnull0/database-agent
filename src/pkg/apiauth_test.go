package pkg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

const (
	testClientID     = "dpc-2-0011223344556677"
	testClientSecret = "dps_very-secret-value"
)

// fakeBackend is authnull-service as the agent sees it: the token exchange
// (/api/v1/databaseService/proxy/token, proxyauth.TokenHandler's answer) and the agent's routes,
// recording the Authorization each call carried.
type fakeBackend struct {
	mu        sync.Mutex
	exchanges int
	tokenTTL  int
	refuse    bool // the exchange answers 401
	authOf    map[string][]string
	reject    map[string]bool // paths that answer 401 to the next call
}

func newFakeBackend(t *testing.T) (*fakeBackend, *httptest.Server) {
	fb := &fakeBackend{tokenTTL: 900, authOf: map[string][]string{}, reject: map[string]bool{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fb.mu.Lock()
		defer fb.mu.Unlock()
		if r.URL.Path == "/api/v1/databaseService/proxy/token" {
			var req struct{ ClientID, ClientSecret string }
			json.NewDecoder(r.Body).Decode(&req)
			if fb.refuse || req.ClientID != testClientID || req.ClientSecret != testClientSecret {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error":"invalid client credentials"}`))
				return
			}
			fb.exchanges++
			json.NewEncoder(w).Encode(map[string]any{
				"accessToken": fmt.Sprintf("dpt.2.token-%d", fb.exchanges), "tokenType": "Bearer",
				"expiresIn": fb.tokenTTL, "expiresAt": time.Now().Add(time.Duration(fb.tokenTTL) * time.Second),
			})
			return
		}
		fb.authOf[r.URL.Path] = append(fb.authOf[r.URL.Path], r.Header.Get("Authorization"))
		if fb.reject[r.URL.Path] {
			fb.reject[r.URL.Path] = false
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Write([]byte(`{"code":200,"status":"Success"}`))
	}))
	t.Cleanup(srv.Close)
	return fb, srv
}

func call(t *testing.T, url string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("POST", url, strings.NewReader(`{}`))
	resp, err := doAPI(&http.Client{Timeout: 5 * time.Second}, req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp
}

func TestAPICallsCarryTheInstallToken(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)

	call(t, srv.URL+"/api/v1/databaseService/getJobQueue")
	call(t, srv.URL+"/api/v1/databaseService/updateQueue")
	if fb.exchanges != 1 {
		t.Errorf("%d exchanges for two calls; the token is cached", fb.exchanges)
	}
	for _, p := range []string{"/api/v1/databaseService/getJobQueue", "/api/v1/databaseService/updateQueue"} {
		if got := fb.authOf[p]; len(got) != 1 || got[0] != "Bearer dpt.2.token-1" {
			t.Errorf("%s carried %v", p, got)
		}
	}

	// Close to expiry, the token is exchanged again before it is used.
	installToken.Lock()
	installToken.expires = time.Now().Add(30 * time.Second)
	installToken.Unlock()
	call(t, srv.URL+"/api/v1/databaseService/dbSync")
	if fb.exchanges != 2 || fb.authOf["/api/v1/databaseService/dbSync"][0] != "Bearer dpt.2.token-2" {
		t.Errorf("near expiry: %d exchanges, sent %v", fb.exchanges, fb.authOf["/api/v1/databaseService/dbSync"])
	}

	// A 401 means the token is no good (revoked, the server restarted its store): drop it, so the
	// next call exchanges again rather than repeating a refused token.
	fb.reject["/api/v1/databaseService/dbUser"] = true
	if resp := call(t, srv.URL+"/api/v1/databaseService/dbUser"); resp.StatusCode != 401 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	call(t, srv.URL+"/api/v1/databaseService/dbUser")
	if fb.exchanges != 3 || fb.authOf["/api/v1/databaseService/dbUser"][1] != "Bearer dpt.2.token-3" {
		t.Errorf("after a 401: %d exchanges, sent %v", fb.exchanges, fb.authOf["/api/v1/databaseService/dbUser"])
	}
}

// Without a credential in db.env the agent calls as before, with no header: the control plane
// lets it in while DB_AGENT_ALLOW_UNAUTHENTICATED is on.
func TestNoCredentialSendsNoHeader(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeBackend(t)
	SetInstallCredential(srv.URL, "", "")
	call(t, srv.URL+"/api/v1/databaseService/registerDbAgent")
	if fb.exchanges != 0 || fb.authOf["/api/v1/databaseService/registerDbAgent"][0] != "" {
		t.Errorf("exchanges %d, header %q", fb.exchanges, fb.authOf["/api/v1/databaseService/registerDbAgent"][0])
	}
}

// A credential the control plane refuses (revoked, mistyped) does not stop the agent calling:
// the call goes without a token, and the control plane decides.
func TestRefusedCredentialStillCalls(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	fb, srv := newFakeBackend(t)
	fb.refuse = true
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	call(t, srv.URL+"/api/v1/databaseService/updateLastActive")
	if got := fb.authOf["/api/v1/databaseService/updateLastActive"]; len(got) != 1 || got[0] != "" {
		t.Errorf("sent %v", got)
	}
}

// The secret and the token never reach the log, whatever happens.
func TestCredentialNeverLogged(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(prev) })

	fb, srv := newFakeBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)
	call(t, srv.URL+"/api/v1/databaseService/getJobQueue")
	fb.refuse = true
	installToken.Lock()
	installToken.token = ""
	installToken.Unlock()
	call(t, srv.URL+"/api/v1/databaseService/getJobQueue")
	cfg := DBConfig{ProxyClientID: testClientID, ProxyClientSecret: testClientSecret}
	log.Printf("%v %+v %#v", cfg, cfg, cfg)

	out := buf.String()
	for _, secret := range []string{testClientSecret, "dpt.2.token-1"} {
		if strings.Contains(out, secret) {
			t.Errorf("the log shows %q:\n%s", secret, out)
		}
	}
	if !strings.Contains(out, testClientID) {
		t.Errorf("the client id is not a secret and should be visible in the config line:\n%s", out)
	}
}

// Two of the agent's real calls, end to end: the last-active report and the policy lookup, which
// has no configuration of its own.
func TestRealCallSitesCarryTheToken(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	t.Cleanup(resetRotationIntervalForTest)
	fb, srv := newFakeBackend(t)
	SetInstallCredential(srv.URL, testClientID, testClientSecret)

	if err := LastActive("31", nil, "appdb", DBConfig{API: srv.URL, OrgID: "2", TenantID: "1"}, nil); err != nil {
		t.Fatal(err)
	}
	FetchPolicyDetails(srv.URL, 2, 1, uuid.New())
	for _, p := range []string{"/api/v1/databaseService/updateLastActive", "/api/v1/policyService/getPolicyDetails"} {
		if got := fb.authOf[p]; len(got) != 1 || !strings.HasPrefix(got[0], "Bearer dpt.2.") {
			t.Errorf("%s carried %v", p, got)
		}
	}
}

// resetInstallCredentialForTest puts the agent back to no credential and no token.
func resetInstallCredentialForTest() {
	installToken.Lock()
	installToken.api, installToken.clientID, installToken.clientSecret = "", "", ""
	installToken.token, installToken.expires = "", time.Time{}
	installToken.Unlock()
}
