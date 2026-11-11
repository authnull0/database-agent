package pkg

import (
	"fmt"
	"log"
	"os"
	"strings"
)

// The agent logs at two levels. The default is what an operator reads: failures, what changed,
// one summary per database per sync. LOG_LEVEL=debug adds the per-user, per-table, per-payload
// and per-connection detail, which on a busy install is tens of thousands of lines an hour.
// Both go through the std logger, so the destination and format stay those main sets.

// debugLogs is whether debug lines print: LOG_LEVEL from the environment at start, or from db.env
// through SetLogLevel.
var debugLogs = debugLevel(os.Getenv("LOG_LEVEL"))

// debugLevel reads a LOG_LEVEL value: "debug", in any case, is debug; anything else is the default.
func debugLevel(v string) bool { return strings.EqualFold(strings.TrimSpace(v), "debug") }

// SetLogLevel applies a LOG_LEVEL value, once, at start.
func SetLogLevel(v string) { debugLogs = debugLevel(v) }

// Debugf logs like log.Printf, only at LOG_LEVEL=debug. The line names Debugf's caller.
func Debugf(format string, args ...interface{}) {
	if debugLogs {
		log.Output(2, fmt.Sprintf(format, args...))
	}
}

// logDebugf is Debugf, inside the package.
var logDebugf = Debugf
