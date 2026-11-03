package utils

import (
	"bytes"
	"log"
	"os/exec"
	"strings"
	"testing"
)

const testKey = "m461-test-key-a9f3"

// DecryptPassword logged both the ciphertext and the key that opens it, on every start.
func TestDecryptPasswordLogsNoSecrets(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(prev)

	_, _ = DecryptPassword("ENC(U2FsdGVkX1+secretciphertext)", testKey)

	if strings.Contains(buf.String(), testKey) {
		t.Errorf("log shows the key: %s", buf.String())
	}
	if strings.Contains(buf.String(), "secretciphertext") {
		t.Errorf("log shows the ciphertext: %s", buf.String())
	}
}

// The key went to openssl as "-pass pass:<key>", readable by any local user in the process list
// for as long as openssl runs. It must not be on the command line.
func TestDecryptCommandKeepsKeyOffCommandLine(t *testing.T) {
	cmd := decryptCommand("U2FsdGVkX1+abc", testKey)
	for _, a := range cmd.Args {
		if strings.Contains(a, testKey) {
			t.Errorf("key on the command line: %v", cmd.Args)
		}
	}
}

// And decryption still works: encrypt the way run_agent.sh does, decrypt with DecryptPassword.
func TestDecryptPasswordRoundTrip(t *testing.T) {
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl not installed")
	}
	enc := exec.Command("openssl", "enc", "-aes-256-cbc", "-a", "-salt", "-pbkdf2", "-iter", "100000", "-pass", "pass:"+testKey)
	enc.Stdin = strings.NewReader("s3cret-db-pw")
	out, err := enc.Output()
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	got, err := DecryptPassword("ENC("+strings.TrimSpace(string(out))+")", testKey)
	if err != nil || got != "s3cret-db-pw" {
		t.Fatalf("DecryptPassword = %q, %v; want the original password", got, err)
	}
}
