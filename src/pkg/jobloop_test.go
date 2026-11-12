package pkg

import (
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// How often the agent asks the control plane for jobs (JOB_POLL_SECONDS): every 10 seconds unless
// db.env says otherwise; 0 turns the separate poll off (jobs then run with the sync, as before).
func TestJobPollEvery(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"":    10 * time.Second,
		"5":   5 * time.Second,
		" 3 ": 3 * time.Second,
		"0":   0,
		"x":   10 * time.Second,
		"-4":  10 * time.Second,
	} {
		if got := JobPollEvery(in); got != want {
			t.Errorf("JobPollEvery(%q) = %v, want %v", in, got, want)
		}
	}
}

// Jobs are picked up on their own short poll -- a just-in-time account is created seconds after
// its approval, not at the next sync -- for every target, until stopped; and never at the same
// time as other password work (the sync's rotation sweep), which takes the same lock.
func TestRunJobsLoop(t *testing.T) {
	prev := processJobsFor
	t.Cleanup(func() { processJobsFor = prev })
	var mu sync.Mutex
	runs := map[string]int{}
	var inside, overlapped int32
	processJobsFor = func(_ *sql.DB, cfg DBConfig, _ time.Time) error {
		if atomic.AddInt32(&inside, 1) > 1 {
			atomic.StoreInt32(&overlapped, 1)
		}
		time.Sleep(5 * time.Millisecond)
		atomic.AddInt32(&inside, -1)
		mu.Lock()
		runs[cfg.Host]++
		mu.Unlock()
		return nil
	}
	targets := []JobTarget{{Cfg: DBConfig{Host: "10.0.0.1"}}, {Cfg: DBConfig{Host: "10.0.0.2", DBType: "mysql"}}}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { RunJobsLoop(targets, 30*time.Millisecond, stop); close(done) }()
	// Other password work, competing for the lock meanwhile.
	for i := 0; i < 10; i++ {
		WithJobLock(func() {
			if atomic.AddInt32(&inside, 1) > 1 {
				atomic.StoreInt32(&overlapped, 1)
			}
			time.Sleep(3 * time.Millisecond)
			atomic.AddInt32(&inside, -1)
		})
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the loop did not stop")
	}
	mu.Lock()
	defer mu.Unlock()
	if runs["10.0.0.1"] < 3 || runs["10.0.0.2"] < 3 {
		t.Errorf("each target should be polled every interval: %v", runs)
	}
	if atomic.LoadInt32(&overlapped) != 0 {
		t.Error("job processing overlapped other password work")
	}
}
