package pkg

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

// captureLog sends the std logger to a buffer for the test, and sets the debug level for it.
func captureLog(t *testing.T, debug bool) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev, prevDebug := log.Writer(), debugLogs
	log.SetOutput(&buf)
	debugLogs = debug
	t.Cleanup(func() { log.SetOutput(prev); debugLogs = prevDebug })
	return &buf
}

// LOG_LEVEL=debug (any case, spaces aside) turns debug lines on; anything else leaves them off.
func TestDebugLevelFromLogLevel(t *testing.T) {
	for v, want := range map[string]bool{
		"": false, "info": false, "warn": false, "debugging": false,
		"debug": true, "DEBUG": true, " Debug ": true,
	} {
		if got := debugLevel(v); got != want {
			t.Errorf("debugLevel(%q) = %v, want %v", v, got, want)
		}
	}
}

func TestLogDebugfPrintsOnlyAtDebug(t *testing.T) {
	buf := captureLog(t, false)
	logDebugf("per-user noise %d", 1)
	log.Printf("kept at default")
	if strings.Contains(buf.String(), "per-user noise") {
		t.Fatalf("debug line printed at the default level: %q", buf.String())
	}
	if !strings.Contains(buf.String(), "kept at default") {
		t.Fatalf("default line missing: %q", buf.String())
	}

	buf = captureLog(t, true)
	logDebugf("per-user noise %d", 2)
	if !strings.Contains(buf.String(), "per-user noise 2") {
		t.Fatalf("debug line missing at debug: %q", buf.String())
	}
}

// SetLogLevel is how main applies LOG_LEVEL from db.env or the environment.
func TestSetLogLevel(t *testing.T) {
	captureLog(t, false)
	SetLogLevel("debug")
	if !debugLogs {
		t.Fatal("SetLogLevel(debug) left debug off")
	}
	SetLogLevel("")
	if debugLogs {
		t.Fatal("SetLogLevel(\"\") left debug on")
	}
}

// The debug line names its caller, not the helper, under log.Lshortfile.
func TestLogDebugfReportsTheCaller(t *testing.T) {
	buf := captureLog(t, true)
	prevFlags := log.Flags()
	log.SetFlags(log.Lshortfile)
	t.Cleanup(func() { log.SetFlags(prevFlags) })
	logDebugf("where")
	if !strings.HasPrefix(buf.String(), "logdebug_test.go:") {
		t.Fatalf("caller: %q", buf.String())
	}
}
