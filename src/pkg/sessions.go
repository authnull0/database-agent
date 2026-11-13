package pkg

import (
	"database/sql"
	"log"
	"strconv"
	"strings"
	"time"
)

// Live sessions (authnull-service docs/DB-LIVE-SESSIONS.md). Every few seconds the agent reports
// the client sessions open on its ProxySQL for each database server it serves, and runs the kills
// the backend hands back -- reporting how they went in the next round. It reads ProxySQL's process
// list for the session's id, user, database, client and age: never the query text (its info column).

const sessionsReportPath = "/api/v1/databaseService/agent/sessions/report"

// processRow is a row of stats_pgsql_processlist or stats_mysql_processlist, as far as the report
// needs it.
type processRow struct {
	ID         int64
	User       string
	Database   string
	ClientHost string
	ClientPort int
	Hostgroup  int
	TimeMs     int64
	Command    string
}

// sessionAdmin is ProxySQL's admin interface: its sessions, and ending one.
type sessionAdmin interface {
	List() ([]processRow, error)
	Kill(id int64) (ok bool, detail string, err error)
	SetEnds(id, endsAt int64) (ok bool, detail string, err error)
}

// setEndsStatement moves an open session's access-grant end (ProxySQL include/authnull_session_ends.h),
// naming the engine: ProxySQL numbers its MySQL and PostgreSQL sessions separately.
func setEndsStatement(engine string, id, endsAt int64) string {
	prefix := "PGSQL"
	if engine == EngineMySQL {
		prefix = "MYSQL"
	}
	return prefix + " SET SESSION ENDS " + strconv.FormatInt(id, 10) + " " + strconv.FormatInt(endsAt, 10)
}

// SetEnds sends SET SESSION ENDS, which answers with a row (updated, detail). A ProxySQL without the
// command answers with an error: the new end is reported failed with it.
func (a adminSessions) SetEnds(id, endsAt int64) (bool, string, error) {
	var updated int
	var detail string
	if err := a.db.QueryRow(setEndsStatement(a.engine, id, endsAt)).Scan(&updated, &detail); err != nil {
		return false, "", err
	}
	return updated == 1, detail, nil
}

// adminSessions is ProxySQL's sessions for one engine: MySQL's and PostgreSQL's are separate lists.
type adminSessions struct {
	db     *sql.DB
	engine string // Engine(): EngineMySQL reads stats_mysql_processlist
}

func (a adminSessions) List() ([]processRow, error) {
	// The two lists differ in one column's name: the database is "db" for MySQL.
	table, database := "stats_pgsql_processlist", "database"
	if a.engine == EngineMySQL {
		table, database = "stats_mysql_processlist", "db"
	}
	rows, err := a.db.Query(`SELECT SessionID, coalesce(user, ''), coalesce(` + database + `, ''), coalesce(cli_host, ''),
		coalesce(cli_port, 0), coalesce(hostgroup, -1), time_ms, coalesce(command, '') FROM ` + table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []processRow
	for rows.Next() {
		var r processRow
		if err := rows.Scan(&r.ID, &r.User, &r.Database, &r.ClientHost, &r.ClientPort, &r.Hostgroup, &r.TimeMs, &r.Command); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Kill sends PGSQL KILL CONNECTION, which answers with a row (killed, detail). A ProxySQL without
// the command answers with an error: the kill is reported failed with it.
//
// A MySQL session is ended with the admin interface's own KILL CONNECTION, which answers OK, or an
// error that is the failed kill's reason.
func (a adminSessions) Kill(id int64) (bool, string, error) {
	if a.engine == EngineMySQL {
		if _, err := a.db.Exec("KILL CONNECTION " + strconv.FormatInt(id, 10)); err != nil {
			return false, "", err
		}
		return true, "session killed", nil
	}
	var killed int
	var detail string
	if err := a.db.QueryRow("PGSQL KILL CONNECTION "+strconv.FormatInt(id, 10)).Scan(&killed, &detail); err != nil {
		return false, "", err
	}
	return killed == 1, detail, nil
}

type reportedSession struct {
	ID         int64  `json:"id"`
	User       string `json:"user"`
	Database   string `json:"database"`
	ClientHost string `json:"clientHost"`
	ClientPort int    `json:"clientPort"`
	Command    string `json:"command,omitempty"`
	TimeMs     int64  `json:"timeMs"`
}

type killOutcome struct {
	ID    int64  `json:"id"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// sessionsTracker is one server's state between rounds: the outcomes of the last kills and new
// session ends, not yet reported.
type sessionsTracker struct {
	pending     []killOutcome
	pendingEnds []killOutcome
}

// sessionEnd is a new end for an open session, from an extended grant (authnull-service
// docs/DB-ACCESS-REQUESTS.md, "Extending a grant").
type sessionEnd struct {
	ID     int64 `json:"id"`
	EndsAt int64 `json:"endsAt"`
}

// round reports the server's sessions -- those in its hostgroup, and, when the agent serves this
// server alone, those not yet in any -- with the last kills' outcomes, and runs the kills handed
// back.
func (t *sessionsTracker) round(admin sessionAdmin, cfg DBConfig, hostgroup int, single bool) error {
	if !credentialConfigured() {
		return nil
	}
	if e := Engine(cfg.DBType); e != EnginePostgres && e != EngineMySQL {
		return nil
	}
	rows, err := admin.List()
	if err != nil {
		return err
	}
	sessions := []reportedSession{}
	for _, r := range rows {
		if r.Hostgroup != hostgroup && !(single && r.Hostgroup < 0) {
			continue
		}
		sessions = append(sessions, reportedSession{ID: r.ID, User: r.User, Database: r.Database, ClientHost: r.ClientHost,
			ClientPort: r.ClientPort, Command: r.Command, TimeMs: r.TimeMs})
	}
	host := cfg.Host
	if cfg.Port != "" {
		host += ":" + cfg.Port
	}
	var answer struct {
		Kill []int64      `json:"kill"`
		Ends []sessionEnd `json:"ends"`
	}
	body := map[string]any{"host": host, "sessions": sessions, "killed": t.pending, "endsSet": t.pendingEnds}
	if err := postJSON(strings.TrimRight(cfg.API, "/"), sessionsReportPath, body, &answer); err != nil {
		return err // the outcomes stay pending for the next round
	}
	t.pending, t.pendingEnds = nil, nil
	for _, e := range answer.Ends {
		ok, detail, err := admin.SetEnds(e.ID, e.EndsAt)
		out := killOutcome{ID: e.ID, OK: ok && err == nil}
		switch {
		case err != nil:
			out.Error = err.Error()
		case !ok:
			out.Error = detail
		}
		if out.OK {
			log.Printf("ProxySQL session %d on %s now ends at %s (grant extended)", e.ID, cfg.Host, time.Unix(e.EndsAt, 0).UTC().Format(time.RFC3339))
		} else {
			log.Printf("Could not move the end of ProxySQL session %d on %s: %s", e.ID, cfg.Host, out.Error)
		}
		t.pendingEnds = append(t.pendingEnds, out)
	}
	for _, id := range answer.Kill {
		ok, detail, err := admin.Kill(id)
		out := killOutcome{ID: id, OK: ok && err == nil}
		switch {
		case err != nil:
			out.Error = err.Error()
		case !ok:
			out.Error = detail
		}
		if out.OK {
			log.Printf("Killed ProxySQL session %d on %s at the console's request", id, cfg.Host)
		} else {
			log.Printf("Could not kill ProxySQL session %d on %s: %s", id, cfg.Host, out.Error)
		}
		t.pending = append(t.pending, out)
	}
	return nil
}

// singleServerPerEngine says, for each target, whether it is the only one of its engine: a session
// not yet in any hostgroup is in its engine's process list, so it is that server's when the agent
// serves no other server of the engine (a MySQL target beside a PostgreSQL one is still alone).
func singleServerPerEngine(targets []DBConfig) []bool {
	count := map[string]int{}
	for _, t := range targets {
		count[Engine(t.DBType)]++
	}
	out := make([]bool, len(targets))
	for i, t := range targets {
		out[i] = count[Engine(t.DBType)] == 1
	}
	return out
}

// RunSessionsLoop reports every interval for each server until stop is closed.
func RunSessionsLoop(targets []DBConfig, interval time.Duration, stop <-chan struct{}) {
	trackers := make([]sessionsTracker, len(targets))
	single := singleServerPerEngine(targets)
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
		for i, cfg := range targets {
			proxyDB, err := ConnectToProxysqlDB(cfg)
			if err != nil {
				log.Printf("Live sessions for %s: ProxySQL: %v", cfg.Host, err)
				continue
			}
			hg, found, err := newAdminProxy(proxyDB, cfg).HostgroupFor(cfg.Host)
			if err == nil && found {
				if err := trackers[i].round(adminSessions{db: proxyDB, engine: Engine(cfg.DBType)}, cfg, hg, single[i]); err != nil {
					log.Printf("Live sessions for %s: %v", cfg.Host, err)
				}
			}
			proxyDB.Close()
		}
	}
}
