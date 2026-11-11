package main

import (
	"os"
	"path/filepath"
	"testing"
)

// The agent's log is created 0640, and one an older agent left 0644 is tightened: it names every
// database user and the agent's addresses.
func TestOpenLogFileMode(t *testing.T) {
	dir := t.TempDir()
	fresh := filepath.Join(dir, "fresh.log")
	f, err := openLogFile(fresh)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if st, _ := os.Stat(fresh); st.Mode().Perm() != 0640 {
		t.Errorf("new log: mode %v", st.Mode().Perm())
	}

	old := filepath.Join(dir, "old.log")
	if err := os.WriteFile(old, []byte("kept\n"), 0644); err != nil {
		t.Fatal(err)
	}
	os.Chmod(old, 0644)
	f, err = openLogFile(old)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("appended\n")
	f.Close()
	if st, _ := os.Stat(old); st.Mode().Perm() != 0640 {
		t.Errorf("existing log: mode %v", st.Mode().Perm())
	}
	if b, _ := os.ReadFile(old); string(b) != "kept\nappended\n" {
		t.Errorf("existing log not appended to: %q", b)
	}
}
