package pkg

import (
	"database/sql"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The control plane's jobs (vaultjobs.go) on their own short poll, every JOB_POLL_SECONDS (10),
// instead of once per sync (TIME_INTERVAL minutes): a just-in-time account (authnull-service
// docs/DB-JIT-ACCOUNTS.md) is created seconds after its approval and dropped seconds after its
// grant ends, and a console role or rotation applies as soon as it is asked for.
// JOB_POLL_SECONDS=0 turns the poll off: jobs then run with the sync, as before.
//
// Jobs and the sync's rotation sweep both set role passwords and ProxySQL users, so they never run
// at the same time: each takes the job lock.

// JobTarget is one configured database and the agent's connection to it.
type JobTarget struct {
	Conn *sql.DB
	Cfg  DBConfig
}

// processJobsFor is ProcessVaultJobsForHost; swappable in tests.
var processJobsFor = ProcessVaultJobsForHost

const defaultJobPoll = 10 * time.Second

var jobMu sync.Mutex

// JobPollEvery reads JOB_POLL_SECONDS: 0 is off, a positive number of seconds is the period, and
// anything else (unset included) is the default.
func JobPollEvery(setting string) time.Duration {
	n, err := strconv.Atoi(strings.TrimSpace(setting))
	switch {
	case err != nil || n < 0:
		return defaultJobPoll
	case n == 0:
		return 0
	}
	return time.Duration(n) * time.Second
}

// WithJobLock runs fn while no job is being processed.
func WithJobLock(fn func()) {
	jobMu.Lock()
	defer jobMu.Unlock()
	fn()
}

// RunJobsLoop polls the jobs of every target every period, until stop is closed.
func RunJobsLoop(targets []JobTarget, every time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		for _, t := range targets {
			WithJobLock(func() {
				if err := processJobsFor(t.Conn, t.Cfg, time.Now()); err != nil {
					log.Printf("Jobs for %s: %v", t.Cfg.Host, err)
				}
			})
		}
	}
}
