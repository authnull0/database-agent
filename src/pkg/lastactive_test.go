package pkg

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// fakeLastActive answers updateLastActive the way authnull-service does, and records what the
// agent sent.
func fakeLastActive(t *testing.T, status int, body string) (*httptest.Server, *map[string]any) {
	t.Helper()
	got := map[string]any{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/databaseService/updateLastActive" {
			t.Errorf("path %s", r.URL.Path)
		}
		json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, &got
}

func TestLastActiveTakesTheIntervalTheConsoleSet(t *testing.T) {
	t.Cleanup(resetRotationIntervalForTest)
	resetRotationIntervalForTest()
	srv, sent := fakeLastActive(t, 200,
		`{"code":200,"status":"Success","message":"ok","passwordRotationHours":12}`)
	cfg := DBConfig{API: srv.URL, OrgID: "5", TenantID: "3", DBType: "postgres"}
	if err := LastActive("31", nil, "appdb", cfg, nil); err != nil {
		t.Fatal(err)
	}
	if got := RotationInterval(); got != 12*time.Hour {
		t.Fatalf("interval %v, want the console's 12h", got)
	}
	if (*sent)["instanceId"] != "31" || (*sent)["orgId"] != float64(5) || (*sent)["tenantId"] != float64(3) {
		t.Errorf("the report: %v", *sent)
	}
}

// Anything short of a good answer keeps the interval in use: no field (an older control
// plane, or one that could not read the setting), a value out of range, an error status, a
// body that is not JSON.
func TestLastActiveKeepsTheIntervalWithoutAGoodAnswer(t *testing.T) {
	t.Cleanup(resetRotationIntervalForTest)
	for name, c := range map[string]struct {
		status int
		body   string
	}{
		"no field":     {200, `{"code":200,"status":"Success"}`},
		"zero":         {200, `{"code":200,"passwordRotationHours":0}`},
		"over a year":  {200, `{"code":200,"passwordRotationHours":9000}`},
		"a string":     {200, `{"code":200,"passwordRotationHours":"12"}`},
		"server error": {500, `{"code":500,"passwordRotationHours":2}`},
		"not json":     {200, `<html>gateway timeout</html>`},
		"empty body":   {200, ``},
	} {
		resetRotationIntervalForTest()
		if err := SetRotationHours(48); err != nil {
			t.Fatal(err)
		}
		srv, _ := fakeLastActive(t, c.status, c.body)
		LastActive("31", nil, "appdb", DBConfig{API: srv.URL, OrgID: "5", TenantID: "3"}, nil)
		if got := RotationInterval(); got != 48*time.Hour {
			t.Errorf("%s: interval %v, want 48h kept", name, got)
		}
	}
}

// The control plane unreachable is an error to log, not a crash: the agent used to defer
// Close on a nil response here.
func TestLastActiveWithTheControlPlaneDown(t *testing.T) {
	t.Cleanup(resetRotationIntervalForTest)
	resetRotationIntervalForTest()
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	if err := LastActive("31", nil, "appdb", DBConfig{API: url, OrgID: "5", TenantID: "3"}, nil); err == nil {
		t.Error("an unreachable control plane reported success")
	}
	if got := RotationInterval(); got != DefaultPasswordRotation {
		t.Errorf("interval %v", got)
	}
}

// resetRotationIntervalForTest puts the interval back to a freshly started agent's.
func resetRotationIntervalForTest() {
	rotationInterval.Lock()
	rotationInterval.d = DefaultPasswordRotation
	rotationInterval.Unlock()
}
