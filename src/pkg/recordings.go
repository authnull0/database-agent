package pkg

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Session recording (authnull-service docs/DB-SESSION-RECORDING.md). ProxySQL writes each statement
// of a session whose policy asks for recording to a local spool -- one file per 5-second bucket,
// <unix>.jsonl, secrets masked, never result data. The agent uploads each file once its bucket has
// closed, per database server (the line's hostgroup), each statement carrying its line in the
// file so a resend adds nothing; it deletes the file once the backend has taken all of it.

const (
	recordingsUploadPath = "/api/v1/databaseService/agent/recordings/upload"
	recordingBucket      = 5 * time.Second
	recordingGrace       = 2 * time.Second // a write that started as the bucket closed has landed
	recordingMaxAge      = 7 * 24 * time.Hour
	recordingChunk       = 1000
)

// RecordingDir is AUTHNULL_RECORDING_DIR, as ProxySQL is given it, or its default.
func RecordingDir() string {
	if d := strings.TrimSpace(os.Getenv("AUTHNULL_RECORDING_DIR")); d != "" {
		return d
	}
	return "/var/lib/proxysql/authnull-recordings"
}

// recordingTarget is a database server the agent serves, and its ProxySQL hostgroup.
type recordingTarget struct {
	cfg       DBConfig
	hostgroup int
}

func (t recordingTarget) host() string {
	if t.cfg.Port != "" {
		return t.cfg.Host + ":" + t.cfg.Port
	}
	return t.cfg.Host
}

// shipRecordings uploads the closed spool files in dir. A file whose upload fails stays for the
// next round, unless it is older than recordingMaxAge.
func shipRecordings(dir string, now time.Time, targets []recordingTarget) error {
	if !credentialConfigured() || len(targets) == 0 {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var buckets []int64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		b, err := strconv.ParseInt(strings.TrimSuffix(name, ".jsonl"), 10, 64)
		if err != nil || b <= 0 {
			continue
		}
		if time.Unix(b, 0).Add(recordingBucket + recordingGrace).After(now) {
			continue // still being written
		}
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
	var firstErr error
	for _, b := range buckets {
		path := filepath.Join(dir, strconv.FormatInt(b, 10)+".jsonl")
		err := shipRecordingFile(path, b, targets)
		switch {
		case err == nil:
			if err := os.Remove(path); err != nil {
				log.Printf("Session recording: removing %s: %v", path, err)
			}
		case now.Sub(time.Unix(b, 0)) > recordingMaxAge:
			log.Printf("Session recording: dropping %s, not uploaded for %s: %v", path, recordingMaxAge, err)
			os.Remove(path)
		default:
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// shipRecordingFile uploads one file, each server's lines to it.
func shipRecordingFile(path string, bucket int64, targets []recordingTarget) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	perTarget := make([][]map[string]any, len(targets))
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	unplaced := 0
	for line := 0; sc.Scan(); line++ {
		var st map[string]any
		if err := json.Unmarshal(sc.Bytes(), &st); err != nil {
			continue
		}
		st["line"] = line
		i := 0
		if len(targets) > 1 {
			// ProxySQL numbers each engine's hostgroups separately, so a line is placed by its
			// engine and hostgroup; a line naming no engine (an older ProxySQL) by hostgroup alone.
			hg, _ := st["hg"].(float64)
			engine, _ := st["engine"].(string)
			i = -1
			for j, t := range targets {
				if float64(t.hostgroup) == hg && (engine == "" || Engine(engine) == Engine(t.cfg.DBType)) {
					i = j
					break
				}
			}
		}
		if i < 0 {
			unplaced++
			continue
		}
		perTarget[i] = append(perTarget[i], st)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if unplaced > 0 {
		log.Printf("Session recording: %d statement(s) in %s ran on a server this agent does not serve; not uploaded", unplaced, path)
	}
	for i, list := range perTarget {
		for from := 0; from < len(list); from += recordingChunk {
			to := from + recordingChunk
			if to > len(list) {
				to = len(list)
			}
			body := map[string]any{"host": targets[i].host(), "bucket": bucket, "statements": list[from:to]}
			if err := postJSON(strings.TrimRight(targets[i].cfg.API, "/"), recordingsUploadPath, body, nil); err != nil {
				return fmt.Errorf("uploading %s for %s: %w", filepath.Base(path), targets[i].host(), err)
			}
		}
	}
	return nil
}

// RunRecordingsLoop uploads the spool every interval until stop is closed.
func RunRecordingsLoop(targets []DBConfig, dir string, interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		if !credentialConfigured() {
			continue
		}
		list := recordingTargets(targets, func(cfg DBConfig) (int, bool, error) {
			proxyDB, err := ConnectToProxysqlDB(cfg)
			if err != nil {
				log.Printf("Session recording for %s: ProxySQL: %v", cfg.Host, err)
				return 0, false, err
			}
			defer proxyDB.Close()
			return newAdminProxy(proxyDB, cfg).HostgroupFor(cfg.Host)
		})
		if err := shipRecordings(dir, time.Now(), list); err != nil {
			log.Printf("Session recording: %v", err)
		}
	}
}

// recordingTargets are the servers whose recordings are uploaded this round: PostgreSQL and MySQL
// targets alike. A spool line names its hostgroup (hg), so with more than one server each is placed
// by its hostgroup -- looked up among its own engine's servers -- and one whose hostgroup is not
// known yet waits for a later round. A lone server takes every line.
func recordingTargets(targets []DBConfig, lookup func(DBConfig) (int, bool, error)) []recordingTarget {
	var list []recordingTarget
	for _, cfg := range targets {
		if e := Engine(cfg.DBType); e != EnginePostgres && e != EngineMySQL {
			continue
		}
		hg := -1
		if len(targets) > 1 {
			h, found, err := lookup(cfg)
			if err != nil || !found {
				continue
			}
			hg = h
		}
		list = append(list, recordingTarget{cfg: cfg, hostgroup: hg})
	}
	return list
}
