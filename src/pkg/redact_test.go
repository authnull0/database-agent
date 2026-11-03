package pkg

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"testing"
)

const (
	secretPassword = "pw-9d1f3c2b7a"
	secretKey      = "key-4e8a6c0d21"
	secretMachine  = "DATAB-mk-7731"
)

func secretConfig() DBConfig {
	return DBConfig{
		OrgID: "2", TenantID: "1", DBType: "postgres", Port: "5432", API: "https://api.example",
		Host: "10.0.0.5", User: "agent", Password: secretPassword, Key: secretKey, MachineKey: secretMachine,
	}
}

// The agent printed its whole configuration -- database password, tenant key, machine key -- at
// every start (main.go) and every sync (FetchDatabaseDetails). Any format of a DBConfig must now
// leave the secrets out and keep what an operator needs to read.
func TestDBConfigNeverPrintsSecrets(t *testing.T) {
	cfg := secretConfig()
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		out := fmt.Sprintf(verb, cfg)
		for _, secret := range []string{secretPassword, secretKey, secretMachine} {
			if strings.Contains(out, secret) {
				t.Errorf("%s of DBConfig shows a secret: %s", verb, out)
			}
		}
		for _, keep := range []string{"10.0.0.5", "agent", "https://api.example"} {
			if !strings.Contains(out, keep) {
				t.Errorf("%s of DBConfig lost %q: %s", verb, keep, out)
			}
		}
	}
	// A pointer prints the same way.
	if out := fmt.Sprintf("%v", &cfg); strings.Contains(out, secretKey) {
		t.Errorf("%%v of *DBConfig shows the key: %s", out)
	}
	// The value itself is untouched: only printing is redacted.
	if cfg.Password != secretPassword || cfg.Key != secretKey {
		t.Error("redaction changed the config")
	}
}

// FetchDatabaseDetails logs the config it was given; run the log line through a captured logger.
func TestConfigLogLineHasNoSecrets(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(prev)

	log.Default().Printf("%v", secretConfig())
	log.Printf("Configuration loaded: %v", secretConfig())

	for _, secret := range []string{secretPassword, secretKey, secretMachine} {
		if strings.Contains(buf.String(), secret) {
			t.Errorf("log shows a secret: %s", buf.String())
		}
	}
}

// The registration payload carries the tenant key (as "uuid") and the machine key; the line that
// logs it must not.
func TestRegisterPayloadForLog(t *testing.T) {
	payload := map[string]interface{}{
		"uuid": secretKey, "machineKey": secretMachine, "orgId": 2, "instanceName": "web-vm-03",
	}
	out := fmt.Sprintf("%v", payloadForLog(payload))
	if strings.Contains(out, secretKey) || strings.Contains(out, secretMachine) {
		t.Errorf("payload log shows a secret: %s", out)
	}
	if !strings.Contains(out, "web-vm-03") {
		t.Errorf("payload log lost the instance name: %s", out)
	}
	if payload["uuid"] != secretKey {
		t.Error("payloadForLog changed the payload that is sent")
	}
}

// Every "Payload Sent" line logged the JSON as sent: the registration and sync payloads carry the
// tenant key as "uuid" and the machine key. Found live on the lab after the first fix, which only
// covered the map form of one payload.
func TestJSONForLog(t *testing.T) {
	body := []byte(`{"uuid":"` + secretKey + `","machineKey":"` + secretMachine + `","orgId":2,"instanceName":"web-vm-03",
		"nested":{"password":"` + secretPassword + `","host":"10.0.0.5"},"list":[{"key":"` + secretKey + `"}]}`)
	out := jsonForLog(body)
	for _, secret := range []string{secretKey, secretMachine, secretPassword} {
		if strings.Contains(out, secret) {
			t.Errorf("jsonForLog shows a secret: %s", out)
		}
	}
	for _, keep := range []string{"web-vm-03", "10.0.0.5"} {
		if !strings.Contains(out, keep) {
			t.Errorf("jsonForLog lost %q: %s", keep, out)
		}
	}
	// Not JSON: say so rather than print it.
	if out := jsonForLog([]byte("uuid=" + secretKey)); strings.Contains(out, secretKey) {
		t.Errorf("non-JSON payload printed: %s", out)
	}
}
